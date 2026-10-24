//! Frozen nonlegacy topic-creation intent. Numeric values are not policy-checked:
//! invalid settings must survive replication for deterministic refusal.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName, Timestamp,
    TopicConfig,
};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    authority: Authority,
    namespace: Text,
    entity: Text,
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
    Queue,
    Topic,
    Subscription,
}

#[derive(Serialize, Deserialize)]
enum Kind {
    CreateTopic {
        default_time_to_live_millis: Option<u64>,
        max_message_bytes: u64,
        requires_duplicate_detection: bool,
        duplicate_detection_history_millis: u64,
    },
}

#[derive(Serialize)]
#[serde(transparent)]
struct Text(String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Text;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V3 identifier")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V3 identifier exceeds envelope bound"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        deserializer.deserialize_str(TextVisitor)
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
        let CommandKind::CreateTopic { config } = &value.command.kind else {
            return Err(Error::UnsupportedCommandVersion(3));
        };
        if config.is_legacy_representable() {
            return Err(Error::UnsupportedCommandVersion(3));
        }
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                // Creation retains exactly V1's authority rules, not V2's
                // topic-update-only restriction. No catalog lookup or rebind.
                super::v1::validate_binding(binding, &value.command)?;
                Authority::Bound {
                    namespace: Text::capture(binding.namespace().as_str())?,
                    target: Text::capture(binding.target().as_str())?,
                    owner: Text::capture(binding.owner().as_str())?,
                    kind: match binding.kind() {
                        EntityBindingKind::Queue => OwnerKind::Queue,
                        EntityBindingKind::Topic => OwnerKind::Topic,
                        EntityBindingKind::Subscription => OwnerKind::Subscription,
                    },
                    generation: binding.generation(),
                }
            }
        };
        Ok(Self {
            authority,
            namespace: Text::capture(value.command.namespace.as_str())?,
            entity: Text::capture(value.command.entity.as_str())?,
            issued_at: value.command.issued_at.as_millis(),
            kind: Kind::CreateTopic {
                default_time_to_live_millis: config.default_time_to_live_millis,
                max_message_bytes: u64::try_from(config.max_message_bytes)
                    .map_err(|_| Error::IntegerOutOfRange)?,
                requires_duplicate_detection: config.requires_duplicate_detection,
                duplicate_detection_history_millis: config.duplicate_detection_history_millis,
            },
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let Kind::CreateTopic {
            default_time_to_live_millis,
            max_message_bytes,
            requires_duplicate_detection,
            duplicate_detection_history_millis,
        } = self.kind;
        let config = TopicConfig {
            default_time_to_live_millis,
            max_message_bytes: usize::try_from(max_message_bytes)
                .map_err(|_| Error::IntegerOutOfRange)?,
            requires_duplicate_detection,
            duplicate_detection_history_millis,
        };
        if config.is_legacy_representable() {
            return Err(Error::NonCanonical);
        }
        let command = Command::new(
            self.namespace.namespace()?,
            self.entity.entity()?,
            Timestamp::from_millis(self.issued_at),
            CommandKind::CreateTopic { config },
        );
        let binding = match self.authority {
            Authority::Unbound => None,
            Authority::Bound {
                namespace,
                target,
                owner,
                kind,
                generation,
            } => {
                let binding = EntityBinding::new(
                    namespace.namespace()?,
                    target.entity()?,
                    owner.entity()?,
                    match kind {
                        OwnerKind::Queue => EntityBindingKind::Queue,
                        OwnerKind::Topic => EntityBindingKind::Topic,
                        OwnerKind::Subscription => EntityBindingKind::Subscription,
                    },
                    generation,
                );
                super::v1::validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal() -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )))
        .unwrap()
    }
    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 3).unwrap())
    }
    #[test]
    fn redundant_v3_and_lossy_v1_capture_refuse() {
        let p = decode(&proposal()).unwrap();
        assert!(matches!(
            super::super::v1::Proposal::capture(&p),
            Err(Error::UnsupportedCommandVersion(1))
        ));
        let mut p = proposal();
        let Kind::CreateTopic {
            requires_duplicate_detection,
            ..
        } = &mut p.kind;
        *requires_duplicate_detection = false;
        assert_eq!(decode(&p), Err(Error::NonCanonical));
    }
    #[test]
    fn malformed_authority_and_identity_are_never_rebound() {
        for (namespace, target, owner, generation) in [
            ("n", "t", "t", 0),
            ("other", "t", "t", 1),
            ("n", "other", "other", 1),
            ("n", "t", "other", 1),
            ("n", "t", "t/subscriptions/s", 1),
        ] {
            let mut p = proposal();
            p.authority = Authority::Bound {
                namespace: Text(namespace.into()),
                target: Text(target.into()),
                owner: Text(owner.into()),
                kind: OwnerKind::Topic,
                generation,
            };
            assert_eq!(decode(&p), Err(Error::InvalidAuthority));
        }
        for value in ["UPPER", "", "t\0bad"] {
            let mut p = proposal();
            p.entity = Text(value.into());
            assert!(matches!(
                decode(&p),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
    }
}
