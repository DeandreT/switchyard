use storage::StorageError;
use thiserror::Error;

use crate::{
    CodecError, EntityPath, IdentifierError, QueueConfigError, SequenceNumber, SessionId,
    SubscriptionConfigError, Timestamp, TopicConfigError,
};

/// Every rejection the state machine can produce.
///
/// These are decided from replicated state alone, so a follower replaying a
/// command rejects it exactly where the leader did.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BrokerError {
    #[error("queue does not exist")]
    QueueNotFound,
    #[error("queue already exists")]
    QueueAlreadyExists,
    #[error("topic does not exist")]
    TopicNotFound,
    #[error("topic already exists")]
    TopicAlreadyExists,
    #[error("an entity of another kind already exists at this path")]
    EntityAlreadyExists,
    #[error("the entity path uses a reserved Service Bus suffix")]
    EntityPathReserved,
    #[error("subscription already exists")]
    SubscriptionAlreadyExists,
    #[error("a topic cannot have more than {maximum} subscriptions")]
    SubscriptionLimitExceeded { maximum: usize },
    #[error("topic subscription index references missing queue {entity}")]
    DanglingSubscription { entity: EntityPath },
    #[error("messages cannot be sent directly to a topic subscription")]
    SubscriptionSendNotAllowed,
    #[error("messages cannot be received directly from a topic")]
    TopicReceiveNotSupported,
    #[error("scheduled topic messages are not supported yet")]
    TopicSchedulingNotSupported,
    #[error("session-bearing topic messages are not supported yet")]
    TopicSessionNotSupported,
    #[error("message {sequence} does not exist")]
    MessageNotFound { sequence: SequenceNumber },
    #[error("message {sequence} is not locked")]
    MessageNotLocked { sequence: SequenceNumber },
    #[error("lock token does not match the lock held on message {sequence}")]
    LockTokenMismatch { sequence: SequenceNumber },
    #[error("the lock on message {sequence} expired at {locked_until}")]
    LockExpired {
        sequence: SequenceNumber,
        locked_until: Timestamp,
    },
    #[error("message body of {body_bytes} bytes exceeds the queue limit of {maximum_bytes}")]
    MessageTooLarge {
        body_bytes: usize,
        maximum_bytes: usize,
    },
    #[error(
        "message identifier has {characters} characters, exceeding the {maximum}-character limit"
    )]
    MessageIdTooLong { characters: usize, maximum: usize },
    #[error("a message batch must contain at least one message")]
    EmptyMessageBatch,
    #[error("every message in a batch sent to a session queue must use the same session")]
    MessageBatchSessionMismatch,
    #[error("peek must request at least one message")]
    EmptyPeek,
    #[error("a deferred receive must name at least one sequence number")]
    EmptyDeferredReceive,
    #[error(
        "deferred receive named {count} sequence numbers, exceeding the batch limit of {maximum}"
    )]
    DeferredReceiveBatchTooLarge { count: usize, maximum: usize },
    #[error("deferred receive named message {sequence} more than once")]
    DuplicateDeferredSequence { sequence: SequenceNumber },
    #[error("message {sequence} is not deferred")]
    MessageNotDeferred { sequence: SequenceNumber },
    #[error("scheduled cancellation must name at least one sequence number")]
    EmptyScheduledCancellation,
    #[error("scheduled cancellation named message {sequence} more than once")]
    DuplicateScheduledSequence { sequence: SequenceNumber },
    #[error("message {sequence} is not scheduled")]
    MessageNotScheduled { sequence: SequenceNumber },
    #[error("scheduled message {sequence} has no scheduled enqueue timestamp")]
    ScheduledEnqueueTimeMissing { sequence: SequenceNumber },
    #[error("deferred message {sequence} belongs to another session")]
    DeferredMessageSessionMismatch { sequence: SequenceNumber },
    #[error("command timestamp {proposed} precedes the applied timestamp {last_applied}")]
    ClockRegression {
        last_applied: Timestamp,
        proposed: Timestamp,
    },
    #[error("invalid queue configuration: {0}")]
    QueueConfig(#[from] QueueConfigError),
    #[error("invalid topic configuration: {0}")]
    TopicConfig(#[from] TopicConfigError),
    #[error("invalid subscription configuration: {0}")]
    SubscriptionConfig(#[from] SubscriptionConfigError),
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
}
