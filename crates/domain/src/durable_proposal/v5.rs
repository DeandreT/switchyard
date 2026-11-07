//! Frozen queue/subscription expiry-policy intent: creation whose profile V1
//! cannot represent, and updates that name `dead_lettering_on_message_expiration`.
//! Numeric values are not policy-checked: invalid settings must survive
//! replication for deterministic refusal. Do not reorder, extend or reuse these
//! DTOs for a later proposal version.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName, QueueConfig,
    QueueConfigUpdate, QueueTimeToLiveUpdate, SubscriptionConfig, SubscriptionConfigUpdate,
    SubscriptionName, Timestamp,
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

// Creation carries only nonlegacy profiles and updates always name the policy,
// so every value has exactly one proposal version.
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
    },
    UpdateQueue {
        lock_duration_millis: Option<u64>,
        max_delivery_count: Option<u32>,
        default_time_to_live_millis: Option<Lifetime>,
        max_message_bytes: Option<u64>,
        requires_session: Option<bool>,
        requires_duplicate_detection: Option<bool>,
        duplicate_detection_history_millis: Option<u64>,
        dead_lettering_on_message_expiration: bool,
    },
    CreateSubscription {
        name: Text,
        lock_duration_millis: u64,
        max_delivery_count: u32,
        default_time_to_live_millis: Option<u64>,
        dead_lettering_on_message_expiration: bool,
    },
    UpdateSubscription {
        name: Text,
        lock_duration_millis: Option<u64>,
        max_delivery_count: Option<u32>,
        default_time_to_live_millis: Option<Lifetime>,
        dead_lettering_on_message_expiration: bool,
    },
}

#[derive(Serialize, Deserialize)]
enum Lifetime {
    Finite { millis: u64 },
    Unlimited,
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
                f.write_str("bounded V5 identifier")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V5 identifier exceeds envelope bound"));
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
    fn subscription(self) -> Result<SubscriptionName, Error> {
        SubscriptionName::new(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
}

/// Whether V5 is the only version that represents this command.
pub(super) fn carries(kind: &CommandKind) -> bool {
    match kind {
        CommandKind::CreateQueue { config } => !config.is_legacy_representable(),
        CommandKind::UpdateQueue { update } => !update.is_legacy_representable(),
        CommandKind::CreateSubscription { config, .. } => !config.is_legacy_representable(),
        CommandKind::UpdateSubscription { update, .. } => !update.is_legacy_representable(),
        _ => false,
    }
}

/// Retains each command family's existing authority rules. No catalog lookup
/// or rebind.
fn validate_binding(binding: &EntityBinding, command: &Command) -> Result<(), Error> {
    if matches!(command.kind, CommandKind::UpdateSubscription { .. }) {
        super::v2::validate_binding(binding, command)
    } else {
        super::v1::validate_binding(binding, command)
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

impl Kind {
    fn capture(value: &CommandKind) -> Result<Self, Error> {
        if !carries(value) {
            return Err(Error::UnsupportedCommandVersion(5));
        }
        Ok(match value {
            CommandKind::CreateQueue { config } => Self::CreateQueue {
                lock_duration_millis: config.lock_duration_millis,
                max_delivery_count: config.max_delivery_count,
                default_time_to_live_millis: config.default_time_to_live_millis,
                max_message_bytes: size(config.max_message_bytes)?,
                requires_session: config.requires_session,
                requires_duplicate_detection: config.requires_duplicate_detection,
                duplicate_detection_history_millis: config.duplicate_detection_history_millis,
                dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
            },
            CommandKind::UpdateQueue { update } => Self::UpdateQueue {
                lock_duration_millis: update.lock_duration_millis,
                max_delivery_count: update.max_delivery_count,
                default_time_to_live_millis: update.default_time_to_live_millis.map(Into::into),
                max_message_bytes: update.max_message_bytes.map(size).transpose()?,
                requires_session: update.requires_session,
                requires_duplicate_detection: update.requires_duplicate_detection,
                duplicate_detection_history_millis: update.duplicate_detection_history_millis,
                dead_lettering_on_message_expiration: update
                    .dead_lettering_on_message_expiration
                    .ok_or(Error::UnsupportedCommandVersion(5))?,
            },
            CommandKind::CreateSubscription { name, config } => Self::CreateSubscription {
                name: Text::capture(name.as_str())?,
                lock_duration_millis: config.lock_duration_millis,
                max_delivery_count: config.max_delivery_count,
                default_time_to_live_millis: config.default_time_to_live_millis,
                dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
            },
            CommandKind::UpdateSubscription { name, update } => Self::UpdateSubscription {
                name: Text::capture(name.as_str())?,
                lock_duration_millis: update.lock_duration_millis,
                max_delivery_count: update.max_delivery_count,
                default_time_to_live_millis: update.default_time_to_live_millis.map(Into::into),
                dead_lettering_on_message_expiration: update
                    .dead_lettering_on_message_expiration
                    .ok_or(Error::UnsupportedCommandVersion(5))?,
            },
            _ => return Err(Error::UnsupportedCommandVersion(5)),
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
            } => CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                    max_message_bytes: max_message_bytes.map(native_size).transpose()?,
                    requires_session,
                    requires_duplicate_detection,
                    duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration: Some(
                        dead_lettering_on_message_expiration,
                    ),
                },
            },
            Self::CreateSubscription {
                name,
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
                dead_lettering_on_message_expiration,
            } => CommandKind::CreateSubscription {
                name: name.subscription()?,
                config: SubscriptionConfig {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis,
                    dead_lettering_on_message_expiration,
                },
            },
            Self::UpdateSubscription {
                name,
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
                dead_lettering_on_message_expiration,
            } => CommandKind::UpdateSubscription {
                name: name.subscription()?,
                update: SubscriptionConfigUpdate {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                    dead_lettering_on_message_expiration: Some(
                        dead_lettering_on_message_expiration,
                    ),
                },
            },
        };
        // A redundant V5 record for intent an older version carries is refused.
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
                validate_binding(binding, &value.command)?;
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
                validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(entity: &str, kind: CommandKind) -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new(entity).unwrap(),
            Timestamp::UNIX_EPOCH,
            kind,
        )))
        .unwrap()
    }

    fn create_queue() -> Proposal {
        proposal(
            "q",
            CommandKind::CreateQueue {
                config: QueueConfig {
                    dead_lettering_on_message_expiration: false,
                    ..QueueConfig::default()
                },
            },
        )
    }

    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 5).unwrap())
    }

    #[test]
    fn redundant_v5_and_lossy_older_captures_refuse() {
        let p = decode(&create_queue()).unwrap();
        for capture in [
            super::super::v1::Proposal::capture(&p).map(drop),
            super::super::v2::Proposal::capture(&p).map(drop),
            super::super::v3::Proposal::capture(&p).map(drop),
            super::super::v4::Proposal::capture(&p).map(drop),
        ] {
            assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
        }
        let mut p = create_queue();
        let Kind::CreateQueue {
            dead_lettering_on_message_expiration,
            ..
        } = &mut p.kind
        else {
            unreachable!()
        };
        *dead_lettering_on_message_expiration = true;
        assert_eq!(decode(&p), Err(Error::NonCanonical));

        let mut p = create_queue();
        p.kind = Kind::CreateSubscription {
            name: Text("s".into()),
            lock_duration_millis: 1,
            max_delivery_count: 1,
            default_time_to_live_millis: None,
            dead_lettering_on_message_expiration: true,
        };
        assert_eq!(decode(&p), Err(Error::NonCanonical));
        for legacy in [
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate::default(),
            },
            CommandKind::ExpireMessages,
        ] {
            assert!(matches!(
                Proposal::capture(&DurableProposal::unbound(Command::new(
                    NamespaceName::new("n").unwrap(),
                    EntityPath::new("q").unwrap(),
                    Timestamp::UNIX_EPOCH,
                    legacy,
                ))),
                Err(Error::UnsupportedCommandVersion(5))
            ));
        }
    }

    #[test]
    fn subscription_updates_keep_topic_owned_authority() {
        let mut p = proposal(
            "t",
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                update: SubscriptionConfigUpdate {
                    dead_lettering_on_message_expiration: Some(false),
                    ..SubscriptionConfigUpdate::default()
                },
            },
        );
        p.authority = Authority::Bound {
            namespace: Text("n".into()),
            target: Text("t".into()),
            owner: Text("t".into()),
            kind: OwnerKind::Topic,
            generation: 1,
        };
        assert!(decode(&p).is_ok());
        let Authority::Bound { kind, .. } = &mut p.authority else {
            unreachable!()
        };
        *kind = OwnerKind::Queue;
        assert_eq!(decode(&p), Err(Error::InvalidAuthority));
    }

    #[test]
    fn malformed_authority_and_identity_are_never_rebound() {
        for (namespace, target, owner, generation) in [
            ("n", "q", "q", 0),
            ("other", "q", "q", 1),
            ("n", "other", "other", 1),
            ("n", "q", "other", 1),
        ] {
            let mut p = create_queue();
            p.authority = Authority::Bound {
                namespace: Text(namespace.into()),
                target: Text(target.into()),
                owner: Text(owner.into()),
                kind: OwnerKind::Queue,
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
