//! Broker identifiers, commands, state-machine rules, and errors.
//!
//! This crate holds the deterministic core of Switchyard. It knows nothing
//! about networking, consensus, or the AMQP wire format: it turns a replicated
//! [`Command`] into an atomic batch of storage mutations. Consensus decides the
//! order commands are applied in; this crate decides what each one means.

#![forbid(unsafe_code)]

pub mod codec;
pub mod keys;

mod command;
mod error;
mod identifier;
mod machine;
mod message;
mod queue;
mod rule;
mod session;
mod time;
mod topic;

pub use codec::CodecError;
pub use command::{Command, CommandKind, CommandOutcome, MessageInput};
pub use error::BrokerError;
pub use identifier::{
    DEAD_LETTER_QUEUE_SUFFIX, EntityPath, IdentifierError, MAX_ENTITY_PATH_BYTES,
    MAX_NAMESPACE_NAME_BYTES, MAX_PLACEMENT_GROUP_ID_BYTES, MAX_RULE_NAME_CHARACTERS,
    MAX_SESSION_ID_BYTES, MAX_SUBSCRIPTION_NAME_CHARACTERS, NamespaceName, PlacementGroupId,
    RuleName, SessionId, SubscriptionName,
};
pub use machine::{
    MAX_DEFERRED_RECEIVE_BATCH, MAX_PEEK_BATCH, MAX_PEEK_SCAN, StateMachine, TIMER_SCAN_LIMIT,
};
pub use message::{
    DeadLetterInfo, DeadLetterReason, Delivery, DeliveryGuarantee, DeliveryLock, DeliveryOrigin,
    LockToken, MAX_MESSAGE_ID_CHARACTERS, MessageEnvelope, MessageRecord, MessageState,
    ReceiveMode, SequenceNumber,
};
pub use queue::{
    DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS, DEFAULT_LOCK_DURATION_MILLIS,
    DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES, MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
    MAX_LOCK_DURATION_MILLIS, MIN_DUPLICATE_DETECTION_HISTORY_MILLIS, QueueConfig,
    QueueConfigError, QueueCounters,
};
pub use rule::{
    CorrelationFilter, CorrelationValue, DEFAULT_RULE_NAME, FilterProperties,
    MAX_CORRELATION_FILTER_BYTES, MAX_CORRELATION_VALUE_BYTES, MAX_RULE_PAGE,
    MAX_SUBSCRIPTION_RULES, RuleConfigError, RuleDefinition, RuleFilter,
};
pub use session::{AcceptedSession, SessionHold, SessionLock, SessionRecord};
pub use time::Timestamp;
pub use topic::{
    MAX_TOPIC_SUBSCRIPTIONS, SubscriptionConfig, SubscriptionConfigError, TopicConfig,
    TopicConfigError,
};
