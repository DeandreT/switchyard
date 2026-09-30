use storage::StorageError;
use thiserror::Error;

use crate::{
    CodecError, IdentifierError, NamespaceName, QueueConfigError, SequenceNumber, SessionId,
    Timestamp,
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
}
