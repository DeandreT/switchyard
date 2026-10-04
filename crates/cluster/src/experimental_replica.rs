//! Owned no-snapshot storage preparation, not a running consensus service.

use std::fmt;

use domain::{CommittedStreamId, Timestamp};
use openraft::{BasicNode, StoredMembership};

use crate::{ExperimentalLogStore, ExperimentalStateMachine, LogId};

mod config;
mod preflight;
mod stores;

pub(crate) use stores::RuntimeParts;

/// A count-only replication limit that also fits worst-case encoded entries.
pub const MAX_REPLICA_PAYLOAD_ENTRIES: u64 =
    (crate::MAX_APPEND_BYTES / crate::MAX_LOG_ENTRY_BYTES) as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReplicaPreparationError {
    #[error("the replica storage identities do not match")]
    ProfileMismatch,
    #[error("purged history requires snapshot support")]
    PurgedHistory,
    #[error("the retained history is not an initialized replica prefix")]
    InvalidHistory,
    #[error("applied state is ahead of retained history")]
    AppliedAhead,
    #[error("the applied checkpoint does not match retained history")]
    CheckpointMismatch,
    #[error("the applied membership does not match retained history")]
    MembershipMismatch,
    #[error("the persisted vote does not cover retained history")]
    VoteMismatch,
    #[error("replica storage validation failed")]
    Storage,
    #[error("the replica storage owners are closed")]
    Closed,
    #[error("a replica storage owner failed during cleanup")]
    OwnerFailure,
    #[error("replica preparation requires a live Tokio runtime")]
    RuntimeUnavailable,
    #[error("the replica preparation task failed")]
    TaskFailed,
    #[error("the no-snapshot replication configuration is invalid")]
    Configuration,
}

/// Frozen startup observations, not live metrics or a quorum certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaProgress {
    node_id: u64,
    stream: CommittedStreamId,
    log_tail: Option<LogId>,
    applied: Option<LogId>,
    highest_timestamp: Timestamp,
    membership: StoredMembership<u64, BasicNode>,
}

impl ReplicaProgress {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn stream(&self) -> CommittedStreamId {
        self.stream
    }
    pub fn log_tail(&self) -> Option<LogId> {
        self.log_tail
    }
    pub fn applied(&self) -> Option<LogId> {
        self.applied
    }
    pub fn highest_timestamp(&self) -> Timestamp {
        self.highest_timestamp
    }
    pub fn membership(&self) -> &StoredMembership<u64, BasicNode> {
        &self.membership
    }
}

/// Sealed, unique storage pair. No node, network, or client write is started.
///
/// ```compile_fail
/// fn duplicate(stores: cluster::ExperimentalReplicaStores) {
///     let another_writer = stores.clone();
/// }
/// ```
pub struct ExperimentalReplicaStores {
    stores: Option<stores::OwnedStores>,
    progress: ReplicaProgress,
    config: openraft::Config,
}

impl ExperimentalReplicaStores {
    pub(crate) fn pause_runtime_ticks(&mut self) {
        self.config.enable_tick = false;
        self.config.enable_heartbeat = true;
        self.config.enable_elect = true;
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), ReplicaPreparationError> {
        let stores = self
            .stores
            .as_mut()
            .ok_or(ReplicaPreparationError::Closed)?;
        let progress = preflight::validate(self.progress.node_id(), stores).await?;
        if progress != self.progress {
            return Err(ReplicaPreparationError::CheckpointMismatch);
        }
        Ok(())
    }

    pub(crate) async fn continuity_snapshot(
        &mut self,
    ) -> Result<
        (
            crate::experimental_log::FinalLogReport,
            domain::CommittedCheckpoint,
        ),
        ReplicaPreparationError,
    > {
        let (log, state) = self.runtime_adapters()?;
        let (log, checkpoint) = tokio::join!(log.healthy_retirement_report(), state.checkpoint(),);
        Ok((
            log.map_err(|_| ReplicaPreparationError::Storage)?,
            checkpoint.map_err(|_| ReplicaPreparationError::Storage)?,
        ))
    }

    pub(crate) fn runtime_adapters(
        &mut self,
    ) -> Result<(&mut ExperimentalLogStore, &mut ExperimentalStateMachine), ReplicaPreparationError>
    {
        self.stores
            .as_mut()
            .ok_or(ReplicaPreparationError::Closed)?
            .adapters()
    }

    pub(crate) async fn into_raft_parts(mut self) -> Result<RuntimeParts, ReplicaPreparationError> {
        let stores = self.stores.take().ok_or(ReplicaPreparationError::Closed)?;
        stores.into_raft_parts(self.progress, self.config).await
    }

    /// Validate an owned pair without applying, purging, or repairing history.
    /// After the first poll under Tokio, caller loss does not cancel the owned
    /// preparation. Unpolled Drop retains the adapters' drain-only behavior.
    pub async fn prepare(
        node_id: u64,
        log: ExperimentalLogStore,
        state: ExperimentalStateMachine,
    ) -> Result<Self, ReplicaPreparationError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ReplicaPreparationError::RuntimeUnavailable)?;
        let supervisor_runtime = runtime.clone();
        let task = runtime.spawn(async move {
            let mut stores = stores::OwnedStores::new(log, state, supervisor_runtime).await?;
            let result = async {
                let config = config::no_snapshot_config()?;
                let progress = preflight::validate(node_id, &mut stores).await?;
                Ok::<_, ReplicaPreparationError>((progress, config))
            }
            .await;
            match result {
                Ok((progress, config)) => Ok(Self {
                    stores: Some(stores),
                    progress,
                    config,
                }),
                Err(error) => {
                    stores.shutdown().await?;
                    Err(error)
                }
            }
        });
        task.await
            .map_err(|_| ReplicaPreparationError::TaskFailed)?
    }

    pub fn progress(&self) -> &ReplicaProgress {
        &self.progress
    }

    /// Fixed storage-compatible settings; this does not start a Raft node.
    pub fn replication_config(&self) -> &openraft::Config {
        &self.config
    }

    /// Retire both adapters and join both owners, even if one owner failed.
    /// Losing a polled waiter does not cancel the owned cleanup supervisor.
    pub async fn shutdown(mut self) -> Result<(), ReplicaPreparationError> {
        match self.stores.take() {
            Some(stores) => stores.shutdown().await,
            None => Err(ReplicaPreparationError::Closed),
        }
    }
}

impl fmt::Debug for ExperimentalReplicaStores {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExperimentalReplicaStores")
            .finish_non_exhaustive()
    }
}
