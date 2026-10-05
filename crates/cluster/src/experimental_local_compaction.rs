//! Sealed quiescent local compaction, not quorum, installation, or engine purge.

use crate::{
    ExperimentalStateMachine, experimental_log::local_compaction::ExperimentalCompactionLogStore,
};
use domain::{CommittedCheckpoint, CommittedStreamId};
use openraft::{BasicNode, SnapshotMeta};
use std::{fmt, future::Future, sync::Arc};

pub(crate) mod frontier;
mod owner;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LocalCompactionError {
    #[error("local compaction has no applied entry")]
    NoAppliedEntry,
    #[error("local compaction catalog capture is disabled")]
    DisabledSource,
    #[error("local compaction sources do not match")]
    InvalidPair,
    #[error("local compaction history is inconsistent")]
    InvalidHistory,
    #[error("local compaction exceeds its finite limit")]
    LimitExceeded,
    #[error("local compaction has exhausted its finite serial")]
    Exhausted,
    #[error("local compaction allocation failed")]
    Allocation,
    #[error("local compaction already has accepted work")]
    Busy,
    #[error("local compaction is closed")]
    Closed,
    #[error("local compaction storage owner did not complete")]
    OwnerFailure,
    #[error("local compaction requires a live execution runtime")]
    RuntimeUnavailable,
    #[error("local compaction supervisor did not complete")]
    TaskFailed,
    #[error("local compaction write decision is unknown")]
    CommitUnknown,
}
impl LocalCompactionError {
    pub(crate) fn from_catalog(error: crate::StateMachineCatalogError) -> Self {
        use crate::StateMachineCatalogError as E;
        match error {
            E::Disabled => Self::DisabledSource,
            E::Owner(_) => Self::OwnerFailure,
            E::Domain(domain::CommittedCatalogError::CommitUnknown) => Self::CommitUnknown,
            E::Domain(
                domain::CommittedCatalogError::Poisoned | domain::CommittedCatalogError::ReadFailed,
            ) => Self::OwnerFailure,
            E::Domain(domain::CommittedCatalogError::LimitExceeded)
            | E::Metadata(crate::NativeSnapshotMetadataError::LimitExceeded) => Self::LimitExceeded,
            E::Domain(domain::CommittedCatalogError::Allocation)
            | E::Metadata(crate::NativeSnapshotMetadataError::Allocation) => Self::Allocation,
            _ => Self::InvalidPair,
        }
    }
    pub(crate) fn terminal(self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::OwnerFailure
                | Self::TaskFailed
                | Self::CommitUnknown
                | Self::Exhausted
        )
    }
}

enum Details {
    Captured(frontier::Receipt),
    Cached {
        checkpoint: Box<CommittedCheckpoint>,
        projection: SnapshotMeta<u64, BasicNode>,
    },
}

/// Cached data only, never a reusable purge permit or fresh health certification.
#[derive(Clone)]
pub struct LocalCompactionProgress {
    node_id: u64,
    ordinal: u64,
    retained_entries: u64,
    retained_bytes: u64,
    details: Arc<Details>,
}
impl LocalCompactionProgress {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn stream(&self) -> CommittedStreamId {
        self.checkpoint().stream()
    }
    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        match self.details.as_ref() {
            Details::Captured(receipt) => &receipt.checkpoint,
            Details::Cached { checkpoint, .. } => checkpoint,
        }
    }
    pub fn snapshot_meta(&self) -> &SnapshotMeta<u64, BasicNode> {
        match self.details.as_ref() {
            Details::Captured(receipt) => &receipt.projection,
            Details::Cached { projection, .. } => projection,
        }
    }
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn retained_entries(&self) -> u64 {
        self.retained_entries
    }
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}
impl fmt::Debug for LocalCompactionProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCompactionProgress")
            .field("retained_entries", &self.retained_entries)
            .finish_non_exhaustive()
    }
}

pub struct LocalCompactionAdmissionError {
    reason: LocalCompactionError,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
}
impl LocalCompactionAdmissionError {
    pub fn reason(&self) -> LocalCompactionError {
        self.reason
    }
    pub fn into_sources(self) -> (ExperimentalCompactionLogStore, ExperimentalStateMachine) {
        (self.log, self.state)
    }
}
impl fmt::Debug for LocalCompactionAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCompactionAdmissionError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for LocalCompactionAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.reason, f)
    }
}
impl std::error::Error for LocalCompactionAdmissionError {}

/// One sealed pair. No adapter, apply, append, or public deletion authority escapes.
///
/// ```compile_fail
/// fn duplicate(pair: cluster::ExperimentalLocalCompactionPair) { let _ = pair.clone(); }
/// ```
pub struct ExperimentalLocalCompactionPair {
    handle: owner::Handle,
    exit: tokio::sync::watch::Receiver<Option<Result<(), LocalCompactionError>>>,
}

impl ExperimentalLocalCompactionPair {
    // Synchronous refusal returns both owned sources without another allocation.
    #[allow(clippy::result_large_err)]
    pub fn prepare(
        node_id: u64,
        log: ExperimentalCompactionLogStore,
        state: ExperimentalStateMachine,
    ) -> Result<
        impl Future<Output = Result<Self, LocalCompactionError>> + Send + 'static,
        LocalCompactionAdmissionError,
    > {
        let fallback = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                return Err(LocalCompactionAdmissionError {
                    reason: LocalCompactionError::RuntimeUnavailable,
                    log,
                    state,
                });
            }
        };
        Ok(owner::prepare(node_id, log, state, fallback))
    }

    /// Inert until first poll. Caller loss after admission cannot cancel commits.
    pub fn compact(
        &mut self,
    ) -> impl Future<Output = Result<LocalCompactionProgress, LocalCompactionError>>
    + Send
    + 'static
    + use<> {
        let handle = self.handle.clone();
        async move { handle.compact().await }
    }

    /// Synchronously close destructive admission, then wait for BOTH real joins.
    pub fn shutdown(
        self,
    ) -> impl Future<Output = Result<(), LocalCompactionError>> + Send + 'static + use<> {
        self.handle.close();
        let mut exit = self.exit.clone();
        drop(self);
        async move {
            loop {
                if let Some(result) = *exit.borrow_and_update() {
                    return result;
                }
                exit.changed()
                    .await
                    .map_err(|_| LocalCompactionError::TaskFailed)?;
            }
        }
    }
}
impl Drop for ExperimentalLocalCompactionPair {
    fn drop(&mut self) {
        self.handle.close();
    }
}
impl fmt::Debug for ExperimentalLocalCompactionPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExperimentalLocalCompactionPair")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
