//! Isolated, bounded vote/log persistence, not a running consensus service.
//!
//! No state-machine adapter, network, snapshot, quorum acknowledgement, or
//! production activation is provided by this module.

mod budget;
mod codec;
mod owner;
mod state;
mod store;
mod types;

pub use store::{ExperimentalLogStore, ReadOnlyLogReader};
pub use types::{
    LogCodecError, LogEntry, LogId, LogProfile, LogResource, LogTypes, LogVote, MAX_APPEND_BYTES,
    MAX_APPEND_ENTRIES, MAX_LIMITED_BYTES, MAX_LIMITED_ENTRIES, MAX_LOG_BODY_BYTES,
    MAX_LOG_ENTRY_BYTES, MAX_LOG_MEMBERSHIP_BYTES, MAX_LOG_METADATA_BYTES, MAX_LOG_QUEUE_BYTES,
    MAX_RETAINED_BYTES, MAX_RETAINED_ENTRIES, QueueLogCommand,
};

pub const MAX_LOG_OWNER_JOBS: usize = 32;
pub const MAX_LOG_OWNER_BYTES: usize = 4 * 1024 * 1024;

pub(crate) const MEMBERSHIP_SCHEMA_VERSION: u16 = 1;

pub(crate) fn validated_entry_len(entry: &LogEntry) -> Result<usize, LogCodecError> {
    Ok(codec::encode_entry(entry)?.encoded_len())
}

pub(crate) fn queue_command_is_send(command: &QueueLogCommand) -> bool {
    matches!(command.0.as_ref(), types::QueueLogKind::Send { .. })
}

pub(crate) fn encode_membership(
    membership: &openraft::Membership<u64, openraft::BasicNode>,
) -> Result<Vec<u8>, LogCodecError> {
    codec::encode_membership(membership)
}

pub(crate) fn decode_membership(
    schema_version: u16,
    bytes: &[u8],
) -> Result<openraft::Membership<u64, openraft::BasicNode>, LogCodecError> {
    if schema_version != MEMBERSHIP_SCHEMA_VERSION {
        return Err(LogCodecError::UnsupportedRecord);
    }
    codec::decode_membership(bytes)
}

/// Accepted queued and in-flight work, excluding caller-held read results.
/// Byte charges include exact encoded append rows and 64 bytes per fixed-size
/// scalar/read job; they are not exact allocation or serialized response sizes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogWorkload {
    pub accepted_jobs: usize,
    pub encoded_bytes: usize,
}

/// Sanitized adapter errors. OpenRaft sees each refusal as a storage failure,
/// never as a deterministic broker refusal or proof that a write rolled back.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LogStorageError {
    #[error("the log writer is poisoned")]
    Poisoned,
    #[error("the log storage operation failed")]
    Storage,
    #[error("the log profile does not match this store")]
    InvalidProfile,
    #[error("the stored log is corrupt or unsupported")]
    Corrupt,
    #[error("the append is not a valid contiguous log extension")]
    InvalidAppend,
    #[error("a retained log entry conflicts with the append")]
    Conflict,
    #[error("the retained log capacity is exhausted")]
    Capacity,
    #[error("the log read range is invalid")]
    InvalidRange,
    #[error("the vote regresses persisted voting state")]
    VoteRegression,
    #[error("the purge boundary is invalid")]
    InvalidPurge,
    #[error("the truncation boundary is invalid")]
    InvalidTruncate,
    #[error("the log index is exhausted")]
    IndexExhausted,
    #[error("the log entry cannot be encoded within its bound")]
    Codec,
    #[error("the log owner work capacity is exhausted")]
    Busy,
    #[error("the log owner is closed")]
    Closed,
    #[error("the log owner terminated unexpectedly")]
    Panicked,
    #[error("the log owner thread could not be started")]
    ThreadStart,
}

impl From<state::LogStateError> for LogStorageError {
    fn from(error: state::LogStateError) -> Self {
        use state::LogStateError as E;
        match error {
            E::Poisoned => Self::Poisoned,
            E::Storage => Self::Storage,
            E::InvalidProfile => Self::InvalidProfile,
            E::Corrupt => Self::Corrupt,
            E::InvalidAppend => Self::InvalidAppend,
            E::Conflict => Self::Conflict,
            E::Capacity => Self::Capacity,
            E::InvalidRange => Self::InvalidRange,
            E::VoteRegression => Self::VoteRegression,
            E::InvalidPurge => Self::InvalidPurge,
            E::InvalidTruncate => Self::InvalidTruncate,
            E::IndexExhausted => Self::IndexExhausted,
        }
    }
}

pub(super) fn io_error() -> std::io::Error {
    std::io::Error::other("experimental log storage did not complete successfully")
}

pub(super) fn raft_error(
    subject: openraft::ErrorSubject<u64>,
    verb: openraft::ErrorVerb,
) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(subject, verb, io_error())
}
