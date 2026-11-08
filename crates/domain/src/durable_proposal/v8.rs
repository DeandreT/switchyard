//! Frozen subscription filter-error intent: creation that discards filter-error
//! copies, which no earlier version represents, and subscription updates that
//! name `dead_lettering_on_filter_evaluation_exceptions`. Numeric values are
//! not policy-checked: invalid settings must survive replication for
//! deterministic refusal. Do not reorder, extend or reuse these DTOs for a
//! later proposal version.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName,
    QueueTimeToLiveUpdate, SubscriptionConfig, SubscriptionConfigUpdate, SubscriptionName,
    Timestamp,
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

// Creation carries only subscriptions that discard filter-error copies and
// updates always name the setting, so every value has exactly one version.
#[derive(Serialize, Deserialize)]
enum Kind {
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
        dead_lettering_on_message_expiration: Option<bool>,
        dead_lettering_on_filter_evaluation_exceptions: bool,
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
                f.write_str("bounded V8 identifier")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V8 identifier exceeds envelope bound"));
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

/// Whether V8 is the only version that represents this command.
pub(super) fn carries(kind: &CommandKind) -> bool {
    match kind {
        CommandKind::CreateSubscription { config, .. } => {
            !config.dead_lettering_on_filter_evaluation_exceptions
        }
        CommandKind::UpdateSubscription { update, .. } => update
            .dead_lettering_on_filter_evaluation_exceptions
            .is_some(),
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
            return Err(Error::UnsupportedCommandVersion(8));
        }
        Ok(match value {
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
                dead_lettering_on_message_expiration: update.dead_lettering_on_message_expiration,
                dead_lettering_on_filter_evaluation_exceptions: update
                    .dead_lettering_on_filter_evaluation_exceptions
                    .ok_or(Error::UnsupportedCommandVersion(8))?,
            },
            _ => return Err(Error::UnsupportedCommandVersion(8)),
        })
    }

    fn restore(self) -> Result<CommandKind, Error> {
        let kind = match self {
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
                    dead_lettering_on_filter_evaluation_exceptions: false,
                },
            },
            Self::UpdateSubscription {
                name,
                lock_duration_millis,
                max_delivery_count,
                default_time_to_live_millis,
                dead_lettering_on_message_expiration,
                dead_lettering_on_filter_evaluation_exceptions,
            } => CommandKind::UpdateSubscription {
                name: name.subscription()?,
                update: SubscriptionConfigUpdate {
                    lock_duration_millis,
                    max_delivery_count,
                    default_time_to_live_millis: default_time_to_live_millis.map(Into::into),
                    dead_lettering_on_message_expiration,
                    dead_lettering_on_filter_evaluation_exceptions: Some(
                        dead_lettering_on_filter_evaluation_exceptions,
                    ),
                },
            },
        };
        // Every V8 shape is V8-only intent; this guards later carrier changes.
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

    fn command(kind: CommandKind) -> DurableProposal {
        DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t").unwrap(),
            Timestamp::UNIX_EPOCH,
            kind,
        ))
    }

    fn create_subscription() -> Proposal {
        Proposal::capture(&command(CommandKind::CreateSubscription {
            name: SubscriptionName::new("s").unwrap(),
            config: SubscriptionConfig {
                dead_lettering_on_filter_evaluation_exceptions: false,
                ..SubscriptionConfig::default()
            },
        }))
        .unwrap()
    }

    fn update_subscription() -> DurableProposal {
        command(CommandKind::UpdateSubscription {
            name: SubscriptionName::new("s").unwrap(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_message_expiration: Some(false),
                dead_lettering_on_filter_evaluation_exceptions: Some(true),
                ..SubscriptionConfigUpdate::default()
            },
        })
    }

    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 8).unwrap())
    }

    fn older_captures(p: &DurableProposal) -> [Result<(), Error>; 7] {
        [
            super::super::v1::Proposal::capture(p).map(drop),
            super::super::v2::Proposal::capture(p).map(drop),
            super::super::v3::Proposal::capture(p).map(drop),
            super::super::v4::Proposal::capture(p).map(drop),
            super::super::v5::Proposal::capture(p).map(drop),
            super::super::v6::Proposal::capture(p).map(drop),
            super::super::v7::Proposal::capture(p).map(drop),
        ]
    }

    #[test]
    fn lossy_older_captures_refuse() {
        let created = decode(&create_subscription()).unwrap();
        let updated = update_subscription();
        for p in [&created, &updated] {
            for capture in older_captures(p) {
                assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
            }
        }
        assert!(Proposal::capture(&updated).is_ok());
        // An expiry-only discard is still V5 intent, not V8.
        let expiry = command(CommandKind::CreateSubscription {
            name: SubscriptionName::new("s").unwrap(),
            config: SubscriptionConfig {
                dead_lettering_on_message_expiration: false,
                ..SubscriptionConfig::default()
            },
        });
        assert!(super::super::v5::Proposal::capture(&expiry).is_ok());
        assert!(matches!(
            Proposal::capture(&expiry),
            Err(Error::UnsupportedCommandVersion(8))
        ));
    }

    #[test]
    fn subscription_updates_keep_topic_owned_authority() {
        let mut p = Proposal::capture(&update_subscription()).unwrap();
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
            ("n", "t", "t", 0),
            ("other", "t", "t", 1),
            ("n", "other", "other", 1),
            ("n", "t", "other", 1),
        ] {
            let mut p = create_subscription();
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
            let mut p = create_subscription();
            p.entity = Text(value.into());
            assert!(matches!(
                decode(&p),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
        let mut p = create_subscription();
        let Kind::CreateSubscription { name, .. } = &mut p.kind else {
            unreachable!()
        };
        *name = Text("S".into());
        assert_eq!(decode(&p), Err(Error::NonCanonical));
    }
}
