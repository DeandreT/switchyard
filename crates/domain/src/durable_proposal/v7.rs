//! Frozen queue-capacity intent: finite queue creation, which no earlier
//! version represents, and queue updates that name `capacity`. Numeric values
//! are not policy-checked: invalid settings must survive replication for
//! deterministic refusal. Do not reorder, extend or reuse these DTOs for a
//! later proposal version.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName,
    QueueCapacity, QueueConfig, QueueConfigUpdate, QueueTimeToLiveUpdate, Timestamp,
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

// Creation carries only finite profiles and updates always name capacity, so
// every value has exactly one proposal version.
#[derive(Serialize, Deserialize)]
enum Kind {
    CreateQueue {
        lock_duration_millis: u64,
        max_delivery_count: u32,
        default_time_to_live_millis: Option<u64>,
        max_message_bytes: u64,
        requires_session: bool,
        requires_duplicate_detection: bool,
        duplicate_detection_history_millis: u64,
        dead_lettering_on_message_expiration: bool,
        max_size_bytes: u64,
    },
    UpdateQueue {
        lock_duration_millis: Option<u64>,
        max_delivery_count: Option<u32>,
        default_time_to_live_millis: Option<Lifetime>,
        max_message_bytes: Option<u64>,
        requires_session: Option<bool>,
        requires_duplicate_detection: Option<bool>,
        duplicate_detection_history_millis: Option<u64>,
        dead_lettering_on_message_expiration: Option<bool>,
        capacity: Capacity,
    },
}

#[derive(Serialize, Deserialize)]
enum Lifetime {
    Finite { millis: u64 },
    Unlimited,
}

#[derive(Serialize, Deserialize)]
enum Capacity {
    NonFinite,
    Finite { max_size_bytes: u64 },
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
                f.write_str("bounded V7 identifier")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V7 identifier exceeds envelope bound"));
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

/// Whether V7 is the only version that represents this command.
pub(super) fn carries(kind: &CommandKind) -> bool {
    match kind {
        CommandKind::CreateQueue { config } => config.capacity.is_finite(),
        CommandKind::UpdateQueue { update } => update.capacity.is_some(),
        _ => false,
    }
}

fn size(value: usize) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::IntegerOutOfRange)
}

fn native_size(value: u64) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::IntegerOutOfRange)
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

impl From<QueueCapacity> for Capacity {
    fn from(value: QueueCapacity) -> Self {
        match value {
            QueueCapacity::NonFinite => Self::NonFinite,
            QueueCapacity::Finite { max_size_bytes } => Self::Finite { max_size_bytes },
        }
    }
}

impl From<Capacity> for QueueCapacity {
    fn from(value: Capacity) -> Self {
        match value {
            Capacity::NonFinite => Self::NonFinite,
            Capacity::Finite { max_size_bytes } => Self::Finite { max_size_bytes },
        }
    }
}

impl Kind {
    fn capture(value: &CommandKind) -> Result<Self, Error> {
        if !carries(value) {
            return Err(Error::UnsupportedCommandVersion(7));
        }
        Ok(match value {
            CommandKind::CreateQueue { config } => {
                let QueueCapacity::Finite { max_size_bytes } = config.capacity else {
                    return Err(Error::UnsupportedCommandVersion(7));
                };
                Self::CreateQueue {
                    lock_duration_millis: config.lock_duration_millis,
                    max_delivery_count: config.max_delivery_count,
                    default_time_to_live_millis: config.default_time_to_live_millis,
                    max_message_bytes: size(config.max_message_bytes)?,
                    requires_session: config.requires_session,
                    requires_duplicate_detection: config.requires_duplicate_detection,
                    duplicate_detection_history_millis: config.duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration: config
                        .dead_lettering_on_message_expiration,
                    max_size_bytes,
                }
            }
            CommandKind::UpdateQueue { update } => Self::UpdateQueue {
                lock_duration_millis: update.lock_duration_millis,
                max_delivery_count: update.max_delivery_count,
                default_time_to_live_millis: update.default_time_to_live_millis.map(Into::into),
                max_message_bytes: update.max_message_bytes.map(size).transpose()?,
                requires_session: update.requires_session,
                requires_duplicate_detection: update.requires_duplicate_detection,
                duplicate_detection_history_millis: update.duplicate_detection_history_millis,
                dead_lettering_on_message_expiration: update.dead_lettering_on_message_expiration,
                capacity: update
                    .capacity
                    .ok_or(Error::UnsupportedCommandVersion(7))?
                    .into(),
            },
            _ => return Err(Error::UnsupportedCommandVersion(7)),
        })
    }

    fn restore(self) -> Result<CommandKind, Error> {
        let kind = match self {
            Self::CreateQueue {
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
                max_message_bytes,
                requires_session,
                requires_duplicate_detection,
                duplicate_detection_history_millis,
                dead_lettering_on_message_expiration,
                max_size_bytes,
            } => CommandKind::CreateQueue {
                config: QueueConfig {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis,
                    max_message_bytes: native_size(max_message_bytes)?,
                    requires_session,
                    requires_duplicate_detection,
                    duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration,
                    capacity: QueueCapacity::Finite { max_size_bytes },
                },
            },
            Self::UpdateQueue {
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
                max_message_bytes,
                requires_session,
                requires_duplicate_detection,
                duplicate_detection_history_millis,
                dead_lettering_on_message_expiration,
                capacity,
            } => CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                    max_message_bytes: max_message_bytes.map(native_size).transpose()?,
                    requires_session,
                    requires_duplicate_detection,
                    duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration,
                    capacity: Some(capacity.into()),
                },
            },
        };
        // Every V7 shape is V7-only intent; this guards later carrier changes.
        if !carries(&kind) {
            return Err(Error::NonCanonical);
        }
        Ok(kind)
    }
}

impl Proposal {
    pub(super) fn capture(value: &DurableProposal) -> Result<Self, Error> {
        let kind = Kind::capture(&value.command.kind)?;
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                // Queue creation and updates retain exactly V1's authority rules.
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
            kind,
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let command = Command::new(
            self.namespace.namespace()?,
            self.entity.entity()?,
            Timestamp::from_millis(self.issued_at),
            self.kind.restore()?,
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

    fn create_queue() -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    capacity: QueueCapacity::Finite {
                        max_size_bytes: 1 << 20,
                    },
                    ..QueueConfig::default()
                },
            },
        )))
        .unwrap()
    }

    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 7).unwrap())
    }

    #[test]
    fn lossy_older_captures_refuse() {
        let p = decode(&create_queue()).unwrap();
        for capture in [
            super::super::v1::Proposal::capture(&p).map(drop),
            super::super::v2::Proposal::capture(&p).map(drop),
            super::super::v3::Proposal::capture(&p).map(drop),
            super::super::v4::Proposal::capture(&p).map(drop),
            super::super::v5::Proposal::capture(&p).map(drop),
            super::super::v6::Proposal::capture(&p).map(drop),
        ] {
            assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
        }
        let update = DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(false),
                    capacity: Some(QueueCapacity::NonFinite),
                    ..QueueConfigUpdate::default()
                },
            },
        ));
        for capture in [
            super::super::v1::Proposal::capture(&update).map(drop),
            super::super::v5::Proposal::capture(&update).map(drop),
            super::super::v6::Proposal::capture(&update).map(drop),
        ] {
            assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
        }
        assert!(Proposal::capture(&update).is_ok());
    }

    #[test]
    fn non_capacity_intent_is_not_v7() {
        for kind in [
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            CommandKind::CreateQueue {
                config: QueueConfig {
                    dead_lettering_on_message_expiration: false,
                    ..QueueConfig::default()
                },
            },
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(false),
                    ..QueueConfigUpdate::default()
                },
            },
            CommandKind::ExpireMessages,
        ] {
            assert!(matches!(
                Proposal::capture(&DurableProposal::unbound(Command::new(
                    NamespaceName::new("n").unwrap(),
                    EntityPath::new("q").unwrap(),
                    Timestamp::UNIX_EPOCH,
                    kind,
                ))),
                Err(Error::UnsupportedCommandVersion(7))
            ));
        }
    }

    #[test]
    fn malformed_authority_and_identity_are_never_rebound() {
        for (namespace, target, owner, kind, generation) in [
            ("n", "q", "q", OwnerKind::Queue, 0),
            ("other", "q", "q", OwnerKind::Queue, 1),
            ("n", "other", "other", OwnerKind::Queue, 1),
            ("n", "q", "other", OwnerKind::Queue, 1),
        ] {
            let mut p = create_queue();
            p.authority = Authority::Bound {
                namespace: Text(namespace.into()),
                target: Text(target.into()),
                owner: Text(owner.into()),
                kind,
                generation,
            };
            assert_eq!(decode(&p), Err(Error::InvalidAuthority));
        }
        for value in ["UPPER", "", "q\0bad"] {
            let mut p = create_queue();
            p.entity = Text(value.into());
            assert!(matches!(
                decode(&p),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
    }
}
