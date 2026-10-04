//! Broker identifiers, commands, state-machine rules, and errors.
//!
//! This crate holds the deterministic core of Switchyard. It knows nothing
//! about networking, consensus, or the AMQP wire format: it turns a replicated
//! [`Command`] into an atomic batch of storage mutations. Consensus decides the
//! order commands are applied in; this crate decides what each one means.

#![forbid(unsafe_code)]

pub mod codec;
pub mod keys;

mod atomic_messaging;
mod command;
mod committed;
mod entity_binding;
mod error;
mod identifier;
mod machine;
mod message;
mod message_content;
mod queue;
mod rule;
mod session;
mod sql_filter;
mod time;
mod topic;

pub use atomic_messaging::{
    AtomicMessagingApplication, AtomicMessagingCommand, AtomicMessagingInputUsage,
    AtomicMessagingLimit, MAX_ATOMIC_MESSAGING_ACTIONS, MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
    MAX_ATOMIC_MESSAGING_MESSAGES, MAX_ATOMIC_MESSAGING_MUTATION_KEY_BYTES,
    MAX_ATOMIC_MESSAGING_MUTATION_KEYS, MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES,
    MAX_ATOMIC_MESSAGING_READ_KEY_BYTES, MAX_ATOMIC_MESSAGING_READ_OPERATIONS,
    MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES, MAX_ATOMIC_MESSAGING_VALUE_ITEMS,
    validate_atomic_messaging_kinds,
};
pub use codec::CodecError;
pub use command::{
    Command, CommandKind, CommandOutcome, DeleteEntityTarget, DeliveryBudget, EntityDeleteLimit,
    IngressBatchLimit, IngressEnvelope, ScheduledEnvelope, ScheduledMessage, SettlementDisposition,
};
pub use committed::{
    CommittedApplication, CommittedApplyError, CommittedApplyResult, CommittedCheckpoint,
    CommittedCheckpointUpdate, CommittedEntryId, CommittedEntryMark, CommittedImageError,
    CommittedImageRole, CommittedImageRow, CommittedImageRows, CommittedImageValidationError,
    CommittedMembership, CommittedQueueCommand, CommittedQueueWork, CommittedSend,
    CommittedStreamId, DecodedCommittedImage, EncodedCommittedImage, MAX_COMMITTED_BODY_BYTES,
    MAX_COMMITTED_CHECKPOINT_BYTES, MAX_COMMITTED_ENTRY_BYTES, MAX_COMMITTED_IMAGE_BYTES,
    MAX_COMMITTED_IMAGE_KEY_BYTES, MAX_COMMITTED_IMAGE_ROWS, MAX_COMMITTED_IMAGE_VALUE_BYTES,
    MAX_COMMITTED_MEMBERSHIP_BYTES, ValidatedCreateSendImage,
};
pub use entity_binding::{EntityBinding, EntityIncarnation, EntityIncarnationKind, FencedCommand};
pub use error::BrokerError;
pub use identifier::{
    DEAD_LETTER_QUEUE_SUFFIX, EntityPath, IdentifierError, MAX_ENTITY_PATH_BYTES,
    MAX_NAMESPACE_NAME_BYTES, MAX_PLACEMENT_GROUP_ID_BYTES, MAX_SESSION_ID_BYTES,
    MAX_SUBSCRIPTION_NAME_BYTES, NamespaceName, PlacementGroupId, SUBSCRIPTION_PATH_SEGMENT,
    SessionId, SubscriptionName,
};
pub use machine::committed_apply::CommittedStateMachine;
pub use machine::{
    BROKER_HEADER_RESERVE_BYTES, CommandApplication, MAX_DEAD_LETTER_DETAIL_LENGTH,
    MAX_ENTITY_DELETE_KEY_BYTES, MAX_ENTITY_DELETE_KEYS, MAX_ENTITY_DELETE_VALUE_BYTES,
    MAX_INGRESS_BATCH_CONTENT_BYTES, MAX_INGRESS_BATCH_MESSAGES, MAX_INGRESS_BATCH_VALUE_ITEMS,
    MAX_QUEUE_PAGE_SIZE, MAX_TOPIC_FANOUT_CONTENT_BYTES, MAX_TOPIC_FANOUT_COPIES,
    MAX_TOPIC_FANOUT_VALUE_ITEMS, MAX_TOPIC_PAGE_SIZE, QueueCursor, QueuePage, StateMachine,
    TIMER_SCAN_LIMIT, TopicCursor, TopicPage,
};
pub use message::{
    DeadLetterInfo, DeadLetterReason, Delivery, DeliveryGuarantee, DeliveryLock, LockToken,
    MAX_MESSAGE_ID_LENGTH, MessageRecord, MessageState, MessageStatus, ReceiveMode, SequenceNumber,
};
pub use message_content::{
    AnnotationKey, MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_PROPERTY_BYTES, MAX_MESSAGE_VALUE_DEPTH,
    MAX_MESSAGE_VALUE_ITEMS, MessageBody, MessageDescriptor, MessageEnvelope, MessageHeader,
    MessageIdentifier, MessageProperties, MessageValue,
};
pub use queue::{
    DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS, DEFAULT_LOCK_DURATION_MILLIS,
    DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES, MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
    MAX_LOCK_DURATION_MILLIS, MAX_SEQUENCE_NUMBER, MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
    QueueConfig, QueueConfigError, QueueConfigUpdate, QueueCounterKind, QueueCounters,
    QueueImmutableProperty, QueueTimeToLiveUpdate,
};
pub use rule::{
    CorrelationFilter, MAX_CORRELATION_RULE_CONDITIONS, MAX_RULE_BYTES, MAX_RULE_NAME_LENGTH,
    MAX_SUBSCRIPTION_RULE_BYTES, MAX_SUBSCRIPTION_RULES, MAX_TOPIC_RULE_COMPARISON_BYTES,
    MAX_TOPIC_RULE_MATCH_WORK, RuleDefinition, RuleFilter, RuleMatchLimit, RuleName,
    SQL_ACTION_SEMANTIC_VERSION, SQL_FILTER_SEMANTIC_VERSION, SqlAction, SqlFilter,
};
pub use session::{AcceptedSession, SessionHold, SessionLock, SessionRecord};
pub use sql_filter::{
    MAX_SQL_COMPILE_NODES, MAX_SQL_COMPILE_SOURCE_BYTES, MAX_SQL_COMPILE_TOKENS,
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_DEPTH, MAX_SQL_EXPRESSION_NODES,
    MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS, MAX_SQL_IN_ITEMS,
    MAX_SQL_LIKE_PATTERN_BYTES, MAX_SQL_PARSER_DEPTH, MAX_SQL_REGEX_ENGINE_BYTES, SqlCompileBudget,
    SqlCompileError, SqlCompileLimit, SqlCompileUsage, SqlEvaluationBudget, SqlEvaluationError,
    SqlEvaluationLimit, SqlEvaluationUsage, SqlMessageContext, SqlProgram, SqlProgramMetrics,
    SqlTruth,
};
pub use time::Timestamp;
pub use topic::{
    MAX_TOPIC_SUBSCRIPTIONS, SubscriptionConfig, SubscriptionConfigUpdate, SubscriptionDefinition,
    SubscriptionImmutableProperty, TopicConfig, TopicConfigUpdate, TopicImmutableProperty,
};
