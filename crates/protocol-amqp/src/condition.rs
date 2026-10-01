//! The AMQP error condition a broker rejection is reported as.
//!
//! A client's SDK decides whether to retry, whether to re-acquire a lock, and
//! which exception to raise from the condition symbol alone. Mapping every
//! rejection deliberately is therefore part of behaving like Service Bus, not
//! cosmetic: reporting a lost lock as a generic internal error turns a routine
//! redelivery into an application failure.

use domain::{BrokerError, IngressBatchLimit};

pub const NOT_FOUND: &str = "amqp:not-found";
pub const INVALID_FIELD: &str = "amqp:invalid-field";
pub const NOT_ALLOWED: &str = "amqp:not-allowed";
pub const INTERNAL_ERROR: &str = "amqp:internal-error";
pub const PRECONDITION_FAILED: &str = "amqp:precondition-failed";
pub const RESOURCE_LOCKED: &str = "amqp:resource-locked";
pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";
pub const MESSAGE_SIZE_EXCEEDED: &str = "amqp:link:message-size-exceeded";
pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";

pub const MESSAGE_LOCK_LOST: &str = "com.microsoft:message-lock-lost";
pub const MESSAGE_NOT_FOUND: &str = "com.microsoft:message-not-found";
pub const SESSION_LOCK_LOST: &str = "com.microsoft:session-lock-lost";
pub const SESSION_CANNOT_BE_LOCKED: &str = "com.microsoft:session-cannot-be-locked";
pub const ENTITY_ALREADY_EXISTS: &str = "com.microsoft:entity-already-exists";
/// What Service Bus reports when no session could be granted in time.
pub const TIMEOUT: &str = "com.microsoft:timeout";

/// The condition symbol to report `error` as.
pub fn condition_for(error: &BrokerError) -> &'static str {
    match error {
        BrokerError::QueueNotFound
        | BrokerError::TopicNotFound
        | BrokerError::SubscriptionNotFound
        | BrokerError::RuleNotFound
        | BrokerError::MessageNotScheduled { .. } => NOT_FOUND,
        BrokerError::MessageNotFound { .. } | BrokerError::MessageNotDeferred { .. } => {
            MESSAGE_NOT_FOUND
        }
        BrokerError::QueueAlreadyExists
        | BrokerError::TopicAlreadyExists
        | BrokerError::SubscriptionAlreadyExists
        | BrokerError::RuleAlreadyExists
        | BrokerError::EntityPathAlreadyExists => ENTITY_ALREADY_EXISTS,
        BrokerError::QueueCounterExhausted { .. }
        | BrokerError::SubscriptionLimitExceeded { .. }
        | BrokerError::RuleLimitExceeded { .. }
        | BrokerError::EntityDeleteTooLarge { .. }
        | BrokerError::TopicRuleMatchTooLarge { .. } => RESOURCE_LIMIT_EXCEEDED,
        BrokerError::TopicDataPlaneNotImplemented => NOT_IMPLEMENTED,
        BrokerError::SqlRuleCompilation(error) => match error {
            domain::SqlCompileError::Syntax => INVALID_FIELD,
            domain::SqlCompileError::Unsupported { .. } => NOT_IMPLEMENTED,
            domain::SqlCompileError::Limit { .. } => RESOURCE_LIMIT_EXCEEDED,
        },
        BrokerError::IngressBatchLimitExceeded { limit, .. }
        | BrokerError::TopicFanoutTooLarge { limit, .. } => match limit {
            IngressBatchLimit::ContentBytes => MESSAGE_SIZE_EXCEEDED,
            IngressBatchLimit::Messages | IngressBatchLimit::ValueItems => RESOURCE_LIMIT_EXCEEDED,
        },
        BrokerError::BatchSessionMismatch => INVALID_FIELD,

        // The client's claim on the message is gone. Saying so precisely is what
        // lets an SDK stop trying to settle and wait for redelivery instead.
        BrokerError::MessageNotLocked { .. }
        | BrokerError::LockTokenMismatch { .. }
        | BrokerError::LockExpired { .. } => MESSAGE_LOCK_LOST,

        BrokerError::SessionLockNotHeld { .. } | BrokerError::SessionLockExpired { .. } => {
            SESSION_LOCK_LOST
        }
        // Someone else holds it. Distinct from a lost lock: the client should
        // wait for another session rather than reacquire this one.
        BrokerError::SessionAlreadyLocked { .. } => SESSION_CANNOT_BE_LOCKED,

        // The client used the entity in a way its configuration forbids, which
        // no retry fixes.
        BrokerError::SessionRequired
        | BrokerError::SessionNotSupported
        | BrokerError::DeadLetterQueueIsReserved
        | BrokerError::SubscriptionPathIsReserved => NOT_ALLOWED,

        BrokerError::MessageTooLarge { .. }
        | BrokerError::MessagePropertyTooLarge { .. }
        | BrokerError::MessageHeaderTooLarge { .. } => MESSAGE_SIZE_EXCEEDED,
        BrokerError::RuleTooLarge { .. } | BrokerError::RuleSetTooLarge { .. } => {
            MESSAGE_SIZE_EXCEEDED
        }
        BrokerError::MessageIdTooLong { .. }
        | BrokerError::InvalidMessageContent { .. }
        | BrokerError::InvalidRule { .. }
        | BrokerError::EntityKindMismatch
        | BrokerError::QueuePageLimitExceeded { .. }
        | BrokerError::QueueCursorNamespaceMismatch { .. }
        | BrokerError::TopicPageLimitExceeded { .. }
        | BrokerError::TopicCursorNamespaceMismatch { .. } => INVALID_FIELD,
        BrokerError::QueueConfig(_)
        | BrokerError::TopicConfig(_)
        | BrokerError::SubscriptionConfig(_)
        | BrokerError::QueuePropertyIsImmutable { .. }
        | BrokerError::TopicPropertyIsImmutable { .. }
        | BrokerError::SubscriptionPropertyIsImmutable { .. } => PRECONDITION_FAILED,

        // The node's clock disagrees with what it already applied. A client
        // retry can succeed once it settles, so this is locked rather than
        // fatal.
        BrokerError::ClockRegression { .. } => RESOURCE_LOCKED,

        // Nothing a client did. Corrupt indexes, unreadable records, and storage
        // failures are the broker's problem and are reported as its fault.
        BrokerError::DanglingIndexEntry { .. }
        | BrokerError::DanglingSubscriptionMetadata
        | BrokerError::DanglingEntityMetadata
        | BrokerError::DanglingRuleMetadata
        | BrokerError::MalformedIndexKey
        | BrokerError::Codec(_)
        | BrokerError::Identifier(_)
        | BrokerError::Storage(_) => INTERNAL_ERROR,
    }
}

/// Whether a client that waits and tries again could succeed.
pub fn is_retryable(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::SessionAlreadyLocked { .. }
            | BrokerError::ClockRegression { .. }
            | BrokerError::Storage(_)
    )
}

#[cfg(test)]
mod tests {
    use domain::{QueueCounterKind, QueueImmutableProperty, SequenceNumber, SessionId, Timestamp};

    use super::*;

    fn session() -> SessionId {
        SessionId::new("cart-1").expect("a valid session id")
    }

    #[test]
    fn exhausted_identifiers_report_a_non_retryable_resource_limit() {
        for counter in [QueueCounterKind::Sequence, QueueCounterKind::LockToken] {
            let error = BrokerError::QueueCounterExhausted { counter };
            assert_eq!(condition_for(&error), RESOURCE_LIMIT_EXCEEDED);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn atomic_ingress_limits_and_session_mismatches_are_not_retryable() {
        for (limit, condition) in [
            (IngressBatchLimit::Messages, RESOURCE_LIMIT_EXCEEDED),
            (IngressBatchLimit::ContentBytes, MESSAGE_SIZE_EXCEEDED),
            (IngressBatchLimit::ValueItems, RESOURCE_LIMIT_EXCEEDED),
        ] {
            let error = BrokerError::IngressBatchLimitExceeded {
                limit,
                actual: 2,
                maximum: 1,
            };
            assert_eq!(condition_for(&error), condition);
            assert!(!is_retryable(&error));
        }
        assert_eq!(
            condition_for(&BrokerError::BatchSessionMismatch),
            INVALID_FIELD
        );
        assert!(!is_retryable(&BrokerError::BatchSessionMismatch));
    }

    #[test]
    fn immutable_queue_property_changes_are_not_retryable() {
        for property in [
            QueueImmutableProperty::RequiresSession,
            QueueImmutableProperty::RequiresDuplicateDetection,
        ] {
            let error = BrokerError::QueuePropertyIsImmutable { property };
            assert_eq!(condition_for(&error), PRECONDITION_FAILED);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn immutable_topology_property_changes_are_not_retryable() {
        for error in [
            BrokerError::TopicPropertyIsImmutable {
                property: domain::TopicImmutableProperty::RequiresDuplicateDetection,
            },
            BrokerError::SubscriptionPropertyIsImmutable {
                property: domain::SubscriptionImmutableProperty::RequiresSession,
            },
        ] {
            assert_eq!(condition_for(&error), PRECONDITION_FAILED);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn deletion_limits_and_kind_mismatches_are_not_retryable() {
        for limit in [
            domain::EntityDeleteLimit::Keys,
            domain::EntityDeleteLimit::KeyBytes,
            domain::EntityDeleteLimit::ValueBytes,
        ] {
            let error = BrokerError::EntityDeleteTooLarge { limit, maximum: 1 };
            assert_eq!(condition_for(&error), RESOURCE_LIMIT_EXCEEDED);
            assert!(!is_retryable(&error));
        }
        assert_eq!(
            condition_for(&BrokerError::EntityKindMismatch),
            INVALID_FIELD
        );
        assert!(!is_retryable(&BrokerError::EntityKindMismatch));
    }

    #[test]
    fn retained_topic_fanout_limits_are_not_retryable() {
        for (limit, condition) in [
            (IngressBatchLimit::Messages, RESOURCE_LIMIT_EXCEEDED),
            (IngressBatchLimit::ContentBytes, MESSAGE_SIZE_EXCEEDED),
            (IngressBatchLimit::ValueItems, RESOURCE_LIMIT_EXCEEDED),
        ] {
            let error = BrokerError::TopicFanoutTooLarge { limit, maximum: 1 };
            assert_eq!(condition_for(&error), condition);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn typed_topology_failures_keep_distinct_non_retryable_conditions() {
        for (error, condition) in [
            (BrokerError::TopicNotFound, NOT_FOUND),
            (BrokerError::TopicAlreadyExists, ENTITY_ALREADY_EXISTS),
            (
                BrokerError::SubscriptionAlreadyExists,
                ENTITY_ALREADY_EXISTS,
            ),
            (BrokerError::EntityPathAlreadyExists, ENTITY_ALREADY_EXISTS),
            (BrokerError::SubscriptionPathIsReserved, NOT_ALLOWED),
            (BrokerError::TopicDataPlaneNotImplemented, NOT_IMPLEMENTED),
            (
                BrokerError::SubscriptionLimitExceeded { maximum: 32 },
                RESOURCE_LIMIT_EXCEEDED,
            ),
            (BrokerError::DanglingSubscriptionMetadata, INTERNAL_ERROR),
            (BrokerError::DanglingEntityMetadata, INTERNAL_ERROR),
        ] {
            assert_eq!(condition_for(&error), condition, "{error}");
            assert!(!is_retryable(&error), "{error}");
        }
    }

    #[test]
    fn every_way_of_losing_a_message_lock_reports_the_same_condition() {
        let sequence = SequenceNumber::new(1);
        for error in [
            BrokerError::MessageNotLocked { sequence },
            BrokerError::LockTokenMismatch { sequence },
            BrokerError::LockExpired {
                sequence,
                locked_until: Timestamp::from_millis(1),
            },
        ] {
            assert_eq!(condition_for(&error), MESSAGE_LOCK_LOST, "{error}");
        }
    }

    #[test]
    fn missing_messages_are_distinct_from_missing_entities() {
        let sequence = SequenceNumber::new(1);
        for error in [
            BrokerError::MessageNotFound { sequence },
            BrokerError::MessageNotDeferred { sequence },
        ] {
            assert_eq!(condition_for(&error), MESSAGE_NOT_FOUND, "{error}");
            assert!(!is_retryable(&error));
        }
        assert_eq!(condition_for(&BrokerError::QueueNotFound), NOT_FOUND);
        assert_eq!(
            condition_for(&BrokerError::MessageNotScheduled { sequence }),
            NOT_FOUND
        );
    }

    #[test]
    fn a_held_session_is_distinct_from_a_lost_one() {
        // An SDK waits for a different session on one and reacquires on the
        // other, so collapsing them would hang a receiver.
        assert_eq!(
            condition_for(&BrokerError::SessionAlreadyLocked {
                session_id: session()
            }),
            SESSION_CANNOT_BE_LOCKED
        );
        assert_eq!(
            condition_for(&BrokerError::SessionLockNotHeld {
                session_id: session()
            }),
            SESSION_LOCK_LOST
        );
    }

    #[test]
    fn a_broken_index_is_reported_as_the_brokers_fault() {
        assert_eq!(
            condition_for(&BrokerError::MalformedIndexKey),
            INTERNAL_ERROR
        );
        assert_eq!(
            condition_for(&BrokerError::DanglingIndexEntry {
                sequence: SequenceNumber::new(1)
            }),
            INTERNAL_ERROR
        );
    }

    #[test]
    fn invalid_message_content_is_a_non_retryable_client_error() {
        let error = BrokerError::InvalidMessageContent {
            reason: "an array has incompatible element types".to_owned(),
        };
        assert_eq!(condition_for(&error), INVALID_FIELD);
        assert!(!is_retryable(&error));
    }

    #[test]
    fn invalid_queue_page_queries_are_non_retryable_client_errors() {
        for error in [
            BrokerError::QueuePageLimitExceeded {
                limit: 1_025,
                maximum: 1_024,
            },
            BrokerError::QueueCursorNamespaceMismatch {
                namespace: domain::NamespaceName::new("tenant").expect("namespace"),
                cursor_namespace: domain::NamespaceName::new("other").expect("namespace"),
            },
        ] {
            assert_eq!(condition_for(&error), INVALID_FIELD);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn invalid_topic_page_queries_are_non_retryable_client_errors() {
        for error in [
            BrokerError::TopicPageLimitExceeded {
                limit: 1_025,
                maximum: 1_024,
            },
            BrokerError::TopicCursorNamespaceMismatch {
                namespace: domain::NamespaceName::new("tenant").expect("namespace"),
                cursor_namespace: domain::NamespaceName::new("other").expect("namespace"),
            },
        ] {
            assert_eq!(condition_for(&error), INVALID_FIELD);
            assert!(!is_retryable(&error));
        }
    }

    #[test]
    fn a_misuse_of_the_entity_is_not_retryable() {
        for error in [
            BrokerError::SessionRequired,
            BrokerError::SessionNotSupported,
        ] {
            assert_eq!(condition_for(&error), NOT_ALLOWED);
            assert!(!is_retryable(&error), "{error} should not invite a retry");
        }
        assert!(is_retryable(&BrokerError::SessionAlreadyLocked {
            session_id: session()
        }));
    }
}
