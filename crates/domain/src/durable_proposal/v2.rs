//! Frozen profile-update DTOs. Do not extend these for later command families.
//!
//! Only bounded identifiers vary in length; numeric patch intent (even invalid
//! zero values) is retained verbatim for deterministic state-machine refusal.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName,
    QueueTimeToLiveUpdate, SubscriptionConfigUpdate, SubscriptionName, Timestamp,
    TopicConfigUpdate,
};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    authority: Authority,
    namespace: Text,
    topic: Text,
    issued_at: u64,
    kind: Kind,
}

#[derive(Serialize, Deserialize)]
enum Authority {
    Unbound,
    Bound {
        namespace: Text,
        target: Text,
        owner: Text,
        kind: OwnerKind,
        generation: u64,
    },
}

#[derive(Serialize, Deserialize)]
enum OwnerKind {
    Topic,
}

#[derive(Serialize, Deserialize)]
enum Kind {
    UpdateTopic {
        default_time_to_live_millis: Option<Lifetime>,
        max_message_bytes: Option<u64>,
    },
    UpdateSubscription {
        name: Text,
        lock_duration_millis: Option<u64>,
        max_delivery_count: Option<u32>,
        default_time_to_live_millis: Option<Lifetime>,
    },
}

#[derive(Serialize, Deserialize)]
enum Lifetime {
    Finite { millis: u64 },
    Unlimited,
}

// Never trust a deserializer's size hint. Postcard first proves that the string
// lies within the bounded input slice; only then does this visitor copy it.
#[derive(Serialize)]
#[serde(transparent)]
struct Text(String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringVisitor;
        impl<'de> Visitor<'de> for StringVisitor {
            type Value = Text;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V2 identifier")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V2 identifier exceeds envelope bound"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        deserializer.deserialize_str(StringVisitor)
    }
}

impl Text {
    fn capture(value: &str) -> Result<Self, Error> {
        if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(Self(value.to_owned()))
    }

    fn canonical(self) -> Result<String, Error> {
        if self.0.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(Error::NonCanonical);
        }
        Ok(self.0)
    }

    fn namespace(self) -> Result<NamespaceName, Error> {
        NamespaceName::new(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }

    fn entity(self) -> Result<EntityPath, Error> {
        EntityPath::from_internal(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
}

impl Proposal {
    pub(super) fn capture(value: &DurableProposal) -> Result<Self, Error> {
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                validate_binding(binding, &value.command)?;
                Authority::Bound {
                    namespace: Text::capture(binding.namespace().as_str())?,
                    target: Text::capture(binding.target().as_str())?,
                    owner: Text::capture(binding.owner().as_str())?,
                    kind: OwnerKind::Topic,
                    generation: binding.generation(),
                }
            }
        };
        let kind = match &value.command.kind {
            CommandKind::UpdateTopic { update } => Kind::UpdateTopic {
                default_time_to_live_millis: update.default_time_to_live_millis.map(Into::into),
                max_message_bytes: update
                    .max_message_bytes
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| Error::IntegerOutOfRange)?,
            },
            CommandKind::UpdateSubscription { name, update } => Kind::UpdateSubscription {
                name: Text::capture(name.as_str())?,
                lock_duration_millis: update.lock_duration_millis,
                max_delivery_count: update.max_delivery_count,
                default_time_to_live_millis: update.default_time_to_live_millis.map(Into::into),
            },
            _ => return Err(Error::UnsupportedCommandVersion(2)),
        };
        Ok(Self {
            authority,
            namespace: Text::capture(value.command.namespace.as_str())?,
            topic: Text::capture(value.command.entity.as_str())?,
            issued_at: value.command.issued_at.as_millis(),
            kind,
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let kind = match self.kind {
            Kind::UpdateTopic {
                default_time_to_live_millis,
                max_message_bytes,
            } => CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                    max_message_bytes: max_message_bytes
                        .map(usize::try_from)
                        .transpose()
                        .map_err(|_| Error::IntegerOutOfRange)?,
                },
            },
            Kind::UpdateSubscription {
                name,
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
            } => CommandKind::UpdateSubscription {
                name: SubscriptionName::new(name.canonical()?)
                    .map_err(|_| Error::InvalidIdentifier)?,
                update: SubscriptionConfigUpdate {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                },
            },
        };
        let command = Command::new(
            self.namespace.namespace()?,
            self.topic.entity()?,
            Timestamp::from_millis(self.issued_at),
            kind,
        );
        let binding = match self.authority {
            Authority::Unbound => None,
            Authority::Bound {
                namespace,
                target,
                owner,
                kind: OwnerKind::Topic,
                generation,
            } => {
                let binding = EntityBinding::new(
                    namespace.namespace()?,
                    target.entity()?,
                    owner.entity()?,
                    EntityBindingKind::Topic,
                    generation,
                );
                validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

impl From<QueueTimeToLiveUpdate> for Lifetime {
    fn from(value: QueueTimeToLiveUpdate) -> Self {
        match value {
            QueueTimeToLiveUpdate::Finite { millis } => Self::Finite { millis },
            QueueTimeToLiveUpdate::Unlimited => Self::Unlimited,
        }
    }
}

impl From<Lifetime> for QueueTimeToLiveUpdate {
    fn from(value: Lifetime) -> Self {
        match value {
            Lifetime::Finite { millis } => Self::Finite { millis },
            Lifetime::Unlimited => Self::Unlimited,
        }
    }
}

pub(super) fn validate_binding(binding: &EntityBinding, command: &Command) -> Result<(), Error> {
    super::v1::validate_binding(binding, command)?;
    if binding.kind() != EntityBindingKind::Topic || binding.owner() != &command.entity {
        return Err(Error::InvalidAuthority);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal() -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate::default(),
            },
        )))
        .unwrap()
    }

    fn decode(value: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(value, 2).unwrap())
    }

    #[test]
    fn authority_and_identifier_corruption_never_rebinds() {
        for (namespace, target, owner, generation) in [
            ("n", "t", "t", 0),
            ("other", "t", "t", 1),
            ("n", "other", "other", 1),
            ("n", "t", "other", 1),
            ("n", "t", "t/subscriptions/s", 1),
        ] {
            let mut value = proposal();
            value.authority = Authority::Bound {
                namespace: Text(namespace.into()),
                target: Text(target.into()),
                owner: Text(owner.into()),
                kind: OwnerKind::Topic,
                generation,
            };
            assert_eq!(decode(&value), Err(Error::InvalidAuthority));
        }
        for bad in ["UPPER", "", "n\0x"] {
            let mut value = proposal();
            value.namespace = Text(bad.into());
            assert!(matches!(
                decode(&value),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
            let mut value = proposal();
            value.topic = Text(bad.into());
            assert!(matches!(
                decode(&value),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
            let mut value = proposal();
            value.kind = Kind::UpdateSubscription {
                name: Text(bad.into()),
                lock_duration_millis: None,
                max_delivery_count: None,
                default_time_to_live_millis: None,
            };
            assert!(matches!(
                decode(&value),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
    }

    #[test]
    fn frozen_v1_explicitly_refuses_new_commands() {
        let value = decode(&proposal()).unwrap();
        assert!(matches!(
            super::super::v1::Proposal::capture(&value),
            Err(Error::UnsupportedCommandVersion(1))
        ));
        let mut value = proposal();
        value.kind = Kind::UpdateSubscription {
            name: Text("s".into()),
            lock_duration_millis: None,
            max_delivery_count: None,
            default_time_to_live_millis: None,
        };
        assert!(matches!(
            super::super::v1::Proposal::capture(&decode(&value).unwrap()),
            Err(Error::UnsupportedCommandVersion(1))
        ));
    }

    // The V1-only reader's version fence is retained independently of the new
    // dispatch, so this fixture cannot accidentally start accepting V2 later.
    fn v1_only_reader(bytes: &[u8]) -> Result<DurableProposal, Error> {
        if bytes.len() < 12 || &bytes[..4] != b"SWDP" {
            return Err(Error::Malformed);
        }
        let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != 1 {
            return Err(Error::UnsupportedVersion(version));
        }
        let length = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        if length != bytes.len() - 12 {
            return Err(Error::Malformed);
        }
        super::super::decode_version::<super::super::v1::Proposal>(bytes, 1)?.restore()
    }

    #[test]
    fn v1_only_reader_refuses_v2_before_decoding_any_intent() {
        let encoded = super::super::encode_version(&proposal(), 2).unwrap();
        assert_eq!(v1_only_reader(&encoded), Err(Error::UnsupportedVersion(2)));
        let old = DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::ExpireLocks,
        ));
        assert_eq!(v1_only_reader(&old.encode().unwrap()).unwrap(), old);
    }
}
