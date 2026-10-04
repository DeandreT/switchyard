//! Isolated committed queue application for the pinned replication library.
//!
//! This is not a Raft node, network, client proposer, or snapshot implementation.
//! Production startup and existing standalone proposers remain unchanged.

mod budget;
mod input;
mod owner;
mod response;
mod snapshot;
mod state;
mod store;

pub use response::{LogApplication, LogQueueConfigRefusal, LogQueueRefusal};
pub use snapshot::UnsupportedSnapshotBuilder;
pub use store::ExperimentalStateMachine;
pub(crate) use store::HealthyCheckpointReader;

pub type AppliedState = (
    Option<crate::LogId>,
    openraft::StoredMembership<u64, openraft::BasicNode>,
);

pub const MAX_APPLY_ENTRIES: usize = crate::MAX_RETAINED_ENTRIES as usize;
pub const MAX_APPLY_BYTES: usize = crate::MAX_RETAINED_BYTES as usize;
pub const MAX_STATE_MACHINE_OWNER_JOBS: usize = 32;
pub const MAX_STATE_MACHINE_OWNER_BYTES: usize = MAX_APPLY_BYTES;

/// Accepted queued/in-flight work, not caller-owned inputs or result heap size.
/// Apply charges are encoded-entry bytes; scalar queries carry a 64-byte charge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateMachineWorkload {
    pub accepted_jobs: usize,
    pub encoded_bytes: usize,
}

/// Static adapter causes. Trait errors are fatal storage failures, not normal
/// queue refusals, original-result receipts, or evidence of rollback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineError {
    #[error("the committed state owner is poisoned")]
    Poisoned,
    #[error("the committed state operation failed")]
    Storage,
    #[error("the committed state is corrupt or incompatible")]
    InvalidState,
    #[error("the apply input is not a valid contiguous extension")]
    InvalidApply,
    #[error("the apply input exceeds its finite bound")]
    Capacity,
    #[error("the apply input cannot be canonically encoded")]
    Codec,
    #[error("the committed application result is incompatible")]
    UnexpectedApplication,
    #[error("the committed state work capacity is exhausted")]
    Busy,
    #[error("the committed state owner is closed")]
    Closed,
    #[error("the committed state owner terminated unexpectedly")]
    Panicked,
    #[error("the committed state owner thread could not be started")]
    ThreadStart,
    #[error("this isolated adapter does not support snapshots")]
    UnsupportedSnapshot,
}

pub(super) fn raft_error(
    subject: openraft::ErrorSubject<u64>,
    verb: openraft::ErrorVerb,
) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        subject,
        verb,
        std::io::Error::other("experimental committed state did not complete successfully"),
    )
}

pub(super) fn snapshot_error(verb: openraft::ErrorVerb) -> openraft::StorageError<u64> {
    openraft::StorageError::from_io_error(
        openraft::ErrorSubject::Snapshot(None),
        verb,
        std::io::Error::other("this isolated committed state adapter does not support snapshots"),
    )
}
