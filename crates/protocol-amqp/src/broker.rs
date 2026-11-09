//! What the protocol edge needs from the broker.
//!
//! The edge submits commands and reports what came back. It never names a
//! storage backend, holds a state machine, or decides an order — that all sits
//! behind this trait, so replacing a single node's command bus with a Raft group
//! is invisible here.

use std::future::Future;

use domain::{BrokerError, CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName};

/// Why a command did not produce an outcome.
///
/// The distinction matters on the wire: a refusal is the client's answer and
/// carries a condition it can act on, while an unavailable broker is the
/// node's problem and the client should try again elsewhere.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerRejection {
    /// The broker considered the command and refused it.
    Refused(BrokerError),
    /// The broker could not be reached, or failed for a reason of its own.
    Unavailable(String),
    /// A presented workload certificate's nonrenewable connection lease ended.
    #[cfg(feature = "workload-tls")]
    AuthorizationExpired,
}

impl std::fmt::Display for BrokerRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(formatter, "{error}"),
            Self::Unavailable(detail) => write!(formatter, "broker unavailable: {detail}"),
            #[cfg(feature = "workload-tls")]
            Self::AuthorizationExpired => {
                formatter.write_str("connection authorization has expired")
            }
        }
    }
}

impl std::error::Error for BrokerRejection {}

impl BrokerRejection {
    /// The AMQP condition to report this as.
    pub fn condition(&self) -> &'static str {
        match self {
            Self::Refused(error) => crate::condition_for(error),
            // Not the client's fault and worth retrying, which is what this
            // condition tells an SDK.
            Self::Unavailable(_) => crate::RESOURCE_LOCKED,
            #[cfg(feature = "workload-tls")]
            Self::AuthorizationExpired => "amqp:unauthorized-access",
        }
    }
}

pub trait Broker: Clone + Send + Sync + 'static {
    /// Captures immutable authority on the broker owner. Unsupported adapters
    /// refuse; resolving names locally is not an equivalent implementation.
    fn bind_entity(
        &self,
        _namespace: NamespaceName,
        _entity: EntityPath,
    ) -> impl Future<Output = Result<EntityBinding, BrokerRejection>> + Send {
        async {
            Err(BrokerRejection::Unavailable(
                "entity binding is unsupported".into(),
            ))
        }
    }

    /// Applies through captured authority, never through a name-based fallback.
    fn submit_bound(
        &self,
        _binding: EntityBinding,
        _kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        async {
            Err(BrokerRejection::Unavailable(
                "bound submission is unsupported".into(),
            ))
        }
    }

    /// Applies one command and reports what it produced.
    ///
    /// The future is required to be `Send` because a link is driven from a task
    /// the runtime may move between threads.
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send;

    /// Resolves once something on the entity may have become deliverable.
    ///
    /// "May have": a wakeup is a hint to ask again, not a claim. Spurious
    /// wakeups are allowed, a wakeup for a message another link wins is
    /// expected, and a caller must pair this with its own timeout because a
    /// wakeup can be lost when several waiters share one entity.
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send;
}

/// Authority carried by a receiving link and its retained delivery capabilities.
/// Session holds keep the authority captured before their acceptance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReceivingAuthority {
    Ordinary(EntityBinding),
    Session(EntityBinding),
}

impl ReceivingAuthority {
    pub(crate) fn binding(&self) -> &EntityBinding {
        match self {
            Self::Ordinary(binding) | Self::Session(binding) => binding,
        }
    }
    pub(crate) async fn submit<B: Broker>(
        &self,
        broker: &B,
        _namespace: NamespaceName,
        _entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        match self {
            Self::Ordinary(binding) | Self::Session(binding) => {
                broker.submit_bound(binding.clone(), kind).await
            }
        }
    }
}

/// Opaque, owner-created authority for qualified synthetic-outcome fixtures.
#[cfg(test)]
pub(crate) fn test_binding(namespace: NamespaceName, entity: EntityPath) -> EntityBinding {
    let machine = domain::StateMachine::new(storage::MemoryStore::default());
    let (owner, subscription) = match crate::parse_attachment(entity.as_str()).unwrap() {
        crate::Attachment::Queue(owner) | crate::Attachment::DeadLetter(owner) => (owner, None),
        crate::Attachment::Subscription {
            topic,
            subscription,
        }
        | crate::Attachment::SubscriptionDeadLetter {
            topic,
            subscription,
        } => (topic, Some(subscription)),
    };
    machine
        .apply(&domain::Command::new(
            namespace.clone(),
            owner.clone(),
            domain::Timestamp::from_millis(0),
            if subscription.is_some() {
                CommandKind::CreateTopic {
                    config: domain::TopicConfig::default(),
                }
            } else {
                CommandKind::CreateQueue {
                    config: domain::QueueConfig::default(),
                }
            },
        ))
        .unwrap();
    if let Some(name) = subscription {
        machine
            .apply(&domain::Command::new(
                namespace.clone(),
                owner,
                domain::Timestamp::from_millis(0),
                CommandKind::CreateSubscription {
                    name,
                    config: domain::SubscriptionConfig::default(),
                },
            ))
            .unwrap();
    }
    machine.bind_entity(&namespace, &entity).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct NameOnly;

    impl Broker for NameOnly {
        async fn submit(
            &self,
            _: NamespaceName,
            _: EntityPath,
            _: CommandKind,
        ) -> Result<CommandOutcome, BrokerRejection> {
            panic!("bound operations must never forward to names");
        }

        async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn unsupported_bound_operations_fail_closed_without_name_fallback() {
        let namespace = NamespaceName::new("tenant").unwrap();
        let entity = EntityPath::new("orders").unwrap();
        let binding = test_binding(namespace.clone(), entity.clone());
        assert!(matches!(
            NameOnly.bind_entity(namespace, entity).await,
            Err(BrokerRejection::Unavailable(_))
        ));
        assert!(matches!(
            NameOnly
                .submit_bound(binding, CommandKind::ActivateScheduled)
                .await,
            Err(BrokerRejection::Unavailable(_))
        ));
    }

    #[test]
    fn a_refusal_carries_the_condition_of_the_error_behind_it() {
        let refused = BrokerRejection::Refused(BrokerError::QueueNotFound);
        assert_eq!(refused.condition(), crate::NOT_FOUND);
        assert_eq!(refused.to_string(), BrokerError::QueueNotFound.to_string());
    }

    #[test]
    fn an_unreachable_broker_is_not_reported_as_the_clients_mistake() {
        let unavailable = BrokerRejection::Unavailable(String::from("stopped"));
        assert_eq!(unavailable.condition(), crate::RESOURCE_LOCKED);
        assert!(unavailable.to_string().contains("stopped"));
    }
}
