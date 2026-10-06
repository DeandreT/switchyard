use storage::StorageError;
use thiserror::Error;

use crate::EntityPath;
use crate::{
    AtomicMessagingLimit, CodecError, EntityDeleteLimit, IdentifierError, IngressBatchLimit,
    NamespaceName, QueueConfigError, QueueCounterKind, QueueImmutableProperty, RuleMatchLimit,
    SequenceNumber, SessionId, SqlCompileError, SubscriptionImmutableProperty, Timestamp,
    TopicImmutableProperty,
};

/// Every rejection the state machine can produce.
///
/// These are decided from replicated state alone, so a follower replaying a
/// command rejects it exactly where the leader did.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BrokerError {
    #[error("atomic messaging commands must have one exact scope and timestamp")]
    InvalidAtomicMessagingCommand,
    #[error(
        "atomic messaging supports only immediate sends and held settlements on a non-session primary queue"
    )]
    AtomicMessagingOperationNotSupported,
    #[error("atomic messaging {limit} exceeds the maximum of {maximum}")]
    AtomicMessagingTooLarge {
        limit: AtomicMessagingLimit,
        maximum: usize,
    },
    #[error("the entity binding has been deleted or replaced")]
    EntityBindingStale,
    #[error("the entity binding does not match the requested operation")]
    InvalidEntityBinding,
    #[error("the entity incarnation counter is exhausted")]
    EntityIncarnationExhausted,
    #[error("queue does not exist")]
    QueueNotFound,
    #[error("queue already exists")]
    QueueAlreadyExists,
    #[error("queue property {property} cannot be changed after creation")]
    QueuePropertyIsImmutable { property: QueueImmutableProperty },
    #[error("the queue's {counter} counter is exhausted")]
    QueueCounterExhausted { counter: QueueCounterKind },
    #[error("ingress batch {limit} of {actual} exceeds the maximum of {maximum}")]
    IngressBatchLimitExceeded {
        limit: IngressBatchLimit,
        actual: usize,
        maximum: usize,
    },
    #[error("topic fanout {limit} exceeds the maximum of {maximum}")]
    TopicFanoutTooLarge {
        limit: IngressBatchLimit,
        maximum: usize,
    },
    #[error("subscription does not exist")]
    SubscriptionNotFound,
    #[error("rule already exists")]
    RuleAlreadyExists,
    #[error("rule does not exist")]
    RuleNotFound,
    #[error("subscription permits at most {maximum} rules")]
    RuleLimitExceeded { maximum: usize },
    #[error("rule exceeds its {maximum_bytes}-byte stored-value limit")]
    RuleTooLarge { maximum_bytes: usize },
    #[error("subscription rules exceed their {maximum_bytes}-byte stored-value limit")]
    RuleSetTooLarge { maximum_bytes: usize },
    #[error("invalid rule: {reason}")]
    InvalidRule { reason: String },
    #[error("stored rule metadata is inconsistent")]
    DanglingRuleMetadata,
    #[error("SQL rule could not be compiled: {0}")]
    SqlRuleCompilation(#[from] SqlCompileError),
    #[error("SQL rule action compilation failed: {0}")]
    SqlActionCompilation(SqlCompileError),
    #[error("topic rule matching {limit:?} exceeds the maximum of {maximum}")]
    TopicRuleMatchTooLarge {
        limit: RuleMatchLimit,
        maximum: usize,
    },
    #[error("every message in a session queue batch must name the same session")]
    BatchSessionMismatch,
    #[error("queue page limit {limit} exceeds the maximum of {maximum}")]
    QueuePageLimitExceeded { limit: usize, maximum: usize },
    #[error(
        "queue cursor namespace {cursor_namespace} does not match requested namespace {namespace}"
    )]
    QueueCursorNamespaceMismatch {
        namespace: NamespaceName,
        cursor_namespace: NamespaceName,
    },
    #[error("message {sequence} does not exist")]
    MessageNotFound { sequence: SequenceNumber },
    #[error("message {sequence} is not locked")]
    MessageNotLocked { sequence: SequenceNumber },
    #[error("message {sequence} is not deferred")]
    MessageNotDeferred { sequence: SequenceNumber },
    #[error("message {sequence} is not scheduled")]
    MessageNotScheduled { sequence: SequenceNumber },
    #[error("lock token does not match the lock held on message {sequence}")]
    LockTokenMismatch { sequence: SequenceNumber },
    #[error("the lock on message {sequence} expired at {locked_until}")]
    LockExpired {
        sequence: SequenceNumber,
        locked_until: Timestamp,
    },
    #[error("message content of {body_bytes} bytes exceeds the queue limit of {maximum_bytes}")]
    MessageTooLarge {
        body_bytes: usize,
        maximum_bytes: usize,
    },
    #[error("message identifier length of {length} exceeds the {maximum}-character limit")]
    MessageIdTooLong { length: usize, maximum: usize },
    #[error("invalid message content: {reason}")]
    InvalidMessageContent { reason: String },
    #[error(
        "message property {property} of {property_bytes} bytes exceeds the {maximum_bytes}-byte limit"
    )]
    MessagePropertyTooLarge {
        property: String,
        property_bytes: usize,
        maximum_bytes: usize,
    },
    #[error("message header of {header_bytes} bytes exceeds the {maximum_bytes}-byte limit")]
    MessageHeaderTooLarge {
        header_bytes: usize,
        maximum_bytes: usize,
    },
    #[error("command timestamp {proposed} precedes the applied timestamp {last_applied}")]
    ClockRegression {
        last_applied: Timestamp,
        proposed: Timestamp,
    },
    #[error("invalid queue configuration: {0}")]
    QueueConfig(#[from] QueueConfigError),
    #[error("a dead-letter queue exists only as the shadow of its parent")]
    DeadLetterQueueIsReserved,
    #[error("queue requires a session and the command named none")]
    SessionRequired,
    #[error("queue does not use sessions")]
    SessionNotSupported,
    #[error("session {session_id} is held by another receiver")]
    SessionAlreadyLocked { session_id: SessionId },
    #[error("no live lock on session {session_id} matches the token presented")]
    SessionLockNotHeld { session_id: SessionId },
    #[error("the lock on session {session_id} expired at {locked_until}")]
    SessionLockExpired {
        session_id: SessionId,
        locked_until: Timestamp,
    },
    #[error("index entry references missing message {sequence}")]
    DanglingIndexEntry { sequence: SequenceNumber },
    #[error("index key is malformed")]
    MalformedIndexKey,
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// An identifier read back out of an index key failed to validate, which
    /// means the key was not one this build wrote.
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("topic does not exist")]
    TopicNotFound,
    #[error("topic already exists")]
    TopicAlreadyExists,
    #[error("subscription already exists")]
    SubscriptionAlreadyExists,
    #[error("subscription paths are reserved for topic-owned topology")]
    SubscriptionPathIsReserved,
    #[error("topic subscription count exceeds its maximum of {maximum}")]
    SubscriptionLimitExceeded { maximum: usize },
    #[error("entity path is already occupied by another entity kind")]
    EntityPathAlreadyExists,
    #[error("requested topic data-plane operation is not implemented")]
    TopicDataPlaneNotImplemented,
    #[error("invalid topic configuration: {0}")]
    TopicConfig(QueueConfigError),
    #[error("invalid subscription configuration: {0}")]
    SubscriptionConfig(QueueConfigError),
    #[error("subscription metadata has a missing or mismatched topology component")]
    DanglingSubscriptionMetadata,
    #[error("topic page limit {limit} exceeds the maximum of {maximum}")]
    TopicPageLimitExceeded { limit: usize, maximum: usize },
    #[error(
        "topic cursor namespace {cursor_namespace} does not match requested namespace {namespace}"
    )]
    TopicCursorNamespaceMismatch {
        namespace: NamespaceName,
        cursor_namespace: NamespaceName,
    },
    #[error("entity metadata has a missing or mismatched topology component")]
    DanglingEntityMetadata,
    #[error("topic property {property} cannot be changed after creation")]
    TopicPropertyIsImmutable { property: TopicImmutableProperty },
    #[error("subscription property {property} cannot be changed after creation")]
    SubscriptionPropertyIsImmutable {
        property: SubscriptionImmutableProperty,
    },
    #[error("entity kind does not match the requested operation")]
    EntityKindMismatch,
    #[error("entity deletion {limit} exceeds the maximum of {maximum}")]
    EntityDeleteTooLarge {
        limit: EntityDeleteLimit,
        maximum: usize,
    },
    #[error("session cursor identifiers are invalid")]
    InvalidSessionCursor,
    #[error(
        "session cursor scope {cursor_namespace}/{cursor_entity} does not match requested scope {namespace}/{entity}"
    )]
    SessionCursorScopeMismatch {
        namespace: NamespaceName,
        entity: EntityPath,
        cursor_namespace: NamespaceName,
        cursor_entity: EntityPath,
    },
}
