//! What the protocol edge needs from the broker.
//!
//! The edge submits commands and reports what came back. It never names a
//! storage backend, holds a state machine, or decides an order — that all sits
//! behind this trait, so replacing a single node's command bus with a Raft group
//! is invisible here.

use std::future::Future;

use domain::{BrokerError, CommandKind, CommandOutcome, EntityPath, NamespaceName};

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
}

impl std::fmt::Display for BrokerRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(formatter, "{error}"),
            Self::Unavailable(detail) => write!(formatter, "broker unavailable: {detail}"),
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
        }
    }
}

pub trait Broker: Clone + Send + Sync + 'static {
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

#[cfg(test)]
mod tests {
    use super::*;

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
