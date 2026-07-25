use storage::StorageError;
use thiserror::Error;

use crate::{CodecError, QueueConfigError, SequenceNumber, Timestamp};

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
    #[error("command timestamp {proposed} precedes the applied timestamp {last_applied}")]
    ClockRegression {
        last_applied: Timestamp,
        proposed: Timestamp,
    },
    #[error("invalid queue configuration: {0}")]
    QueueConfig(#[from] QueueConfigError),
    #[error("index entry references missing message {sequence}")]
    DanglingIndexEntry { sequence: SequenceNumber },
    #[error("index key is malformed")]
    MalformedIndexKey,
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}
