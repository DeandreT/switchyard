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
mod entity_binding;
mod error;
mod identifier;
mod machine;
mod message;
mod queue;
mod rule;
mod session;
mod sql_filter;
mod time;
mod topic;

pub use codec::CodecError;
pub use command::{Command, CommandKind, CommandOutcome, MessageInput};
pub use entity_binding::{BoundCommand, EntityBinding, EntityBindingKind};
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
    QueueConfigError, QueueConfigUpdate, QueueCounters, QueueTimeToLiveUpdate,
};
pub use rule::{
    CorrelationFilter, CorrelationValue, DEFAULT_RULE_NAME, FilterProperties,
    MAX_CORRELATION_FILTER_BYTES, MAX_CORRELATION_VALUE_BYTES, MAX_RULE_PAGE,
    MAX_SUBSCRIPTION_RULES, RuleConfigError, RuleDefinition, RuleFilter,
};
pub use session::{AcceptedSession, SessionHold, SessionLock, SessionRecord};
pub use sql_filter::{
    MAX_SQL_COMPARISON_BYTES, MAX_SQL_COMPILE_NODES, MAX_SQL_COMPILE_SOURCE_BYTES,
    MAX_SQL_COMPILE_TOKENS, MAX_SQL_EVALUATION_WORK, MAX_SQL_EXPRESSION_BYTES,
    MAX_SQL_EXPRESSION_DEPTH, MAX_SQL_EXPRESSION_NODES, MAX_SQL_EXPRESSION_TOKENS,
    MAX_SQL_EXPRESSION_UTF16_UNITS, MAX_SQL_IN_OPERANDS, MAX_SQL_LIKE_PATTERN_BYTES,
    MAX_SQL_PARSER_DEPTH, MAX_SQL_REGEX_BYTES, SqlCompileBudget, SqlCompileError, SqlCompileLimit,
    SqlCompileUsage, SqlEvaluationBudget, SqlEvaluationError, SqlEvaluationLimit,
    SqlEvaluationUsage, SqlMessageContext, SqlProgram, SqlProgramMetrics, SqlProperty,
    SqlSystemProperty, SqlSystemValue, SqlTruth, SqlValue,
};
pub use time::Timestamp;
pub use topic::{
    MAX_TOPIC_SUBSCRIPTIONS, SubscriptionConfig, SubscriptionConfigError, TopicConfig,
    TopicConfigError,
};
