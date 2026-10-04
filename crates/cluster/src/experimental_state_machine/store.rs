use std::{fmt, thread::JoinHandle};

use domain::CommittedStreamId;
use openraft::{
    ErrorSubject, ErrorVerb, Snapshot, SnapshotMeta, StorageError, storage::RaftStateMachine,
};
use storage::CommittedStore;

use crate::experimental_owner::RetiredOwner;
use crate::{BoundedSnapshotData, LogEntry, LogTypes};

use super::{
    AppliedState, LogApplication, StateMachineError, StateMachineWorkload,
    UnsupportedSnapshotBuilder,
    input::PreparedApply,
    owner::{Handle, Operation, Reply},
    raft_error, snapshot_error,
    state::StoreState,
};

/// One unique writer for isolated committed queue state, not a consensus node.
///
/// ```compile_fail
/// fn duplicate(machine: cluster::ExperimentalStateMachine) {
///     let another_writer = machine.clone();
/// }
/// ```
pub struct ExperimentalStateMachine {
    pub(super) handle: Handle,
    thread: Option<JoinHandle<Result<(), StateMachineError>>>,
    retired: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ExperimentalStateMachine {
    pub(crate) fn enable_retirement_report(
        &self,
    ) -> Result<
        tokio::sync::oneshot::Receiver<Result<domain::CommittedCheckpoint, StateMachineError>>,
        StateMachineError,
    > {
        self.handle.enable_retirement_report()
    }

    pub(crate) fn checkpoint_reader(&self) -> HealthyCheckpointReader {
        HealthyCheckpointReader {
            handle: self.handle.clone(),
        }
    }
    /// Synchronously initialize only a pristine committed-state store.
    pub fn create<W: CommittedStore>(
        writer: W,
        stream: CommittedStreamId,
    ) -> Result<Self, StateMachineError> {
        Self::start(StoreState::create(writer, stream)?)
    }

    /// Synchronously validate exact checkpoint and membership recovery.
    pub fn open<W: CommittedStore>(
        writer: W,
        stream: CommittedStreamId,
    ) -> Result<Self, StateMachineError> {
        Self::start(StoreState::open(writer, stream)?)
    }

    pub(super) fn start<W: CommittedStore>(
        state: StoreState<W>,
    ) -> Result<Self, StateMachineError> {
        let (handle, thread) = Handle::start(state)?;
        Ok(Self {
            handle,
            thread: Some(thread),
            retired: None,
        })
    }

    pub(crate) async fn checkpoint(
        &self,
    ) -> Result<domain::CommittedCheckpoint, StateMachineError> {
        match self.handle.request(Operation::Checkpoint).await? {
            Reply::Checkpoint(checkpoint) => Ok(*checkpoint),
            _ => Err(StateMachineError::InvalidState),
        }
    }

    pub(crate) fn into_runtime_parts(
        mut self,
    ) -> Result<(Self, RetiredOwner<StateMachineError>), StateMachineError> {
        let thread = self.thread.take().ok_or(StateMachineError::Closed)?;
        let (retired, receiver) = tokio::sync::oneshot::channel();
        self.retired = Some(retired);
        Ok((self, RetiredOwner::new(receiver, thread)))
    }

    pub fn workload(&self) -> Result<StateMachineWorkload, StateMachineError> {
        self.handle.workload()
    }

    /// Stop admission, drain accepted work, and join. Losing this waiter does
    /// not cancel accepted apply entries or the blocking join task.
    pub async fn shutdown(mut self) -> Result<(), StateMachineError> {
        self.handle.close();
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .map_err(|_| StateMachineError::Panicked)?
            .map_err(|_| StateMachineError::Panicked)?
    }
}

#[derive(Clone)]
pub(crate) struct HealthyCheckpointReader {
    handle: Handle,
}

impl HealthyCheckpointReader {
    pub(crate) async fn checkpoint(
        &self,
    ) -> Result<domain::CommittedCheckpoint, StateMachineError> {
        match self.handle.request(Operation::Checkpoint).await? {
            Reply::Checkpoint(checkpoint) => Ok(*checkpoint),
            _ => Err(StateMachineError::InvalidState),
        }
    }
}

impl fmt::Debug for HealthyCheckpointReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HealthyCheckpointReader")
            .finish_non_exhaustive()
    }
}

impl Drop for ExperimentalStateMachine {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(retired) = self.retired.take() {
            let _ = retired.send(());
        }
    }
}

impl fmt::Debug for ExperimentalStateMachine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExperimentalStateMachine")
            .finish_non_exhaustive()
    }
}

impl RaftStateMachine<LogTypes> for ExperimentalStateMachine {
    type SnapshotBuilder = UnsupportedSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<AppliedState, StorageError<u64>> {
        match self.handle.request(Operation::AppliedState).await {
            Ok(Reply::AppliedState(state)) => Ok(*state),
            _ => Err(raft_error(ErrorSubject::StateMachine, ErrorVerb::Read)),
        }
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LogApplication>, StorageError<u64>>
    where
        I: IntoIterator<Item = LogEntry> + Send,
        I::IntoIter: Send,
    {
        let entries = PreparedApply::from_entries(entries)
            .map_err(|_| raft_error(ErrorSubject::StateMachine, ErrorVerb::Write))?;
        match self.handle.request(Operation::Apply(entries)).await {
            Ok(Reply::Applications(results)) => Ok(results),
            _ => Err(raft_error(ErrorSubject::StateMachine, ErrorVerb::Write)),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        UnsupportedSnapshotBuilder { _private: () }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<BoundedSnapshotData>, StorageError<u64>> {
        Err(snapshot_error(ErrorVerb::Write))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, openraft::BasicNode>,
        _snapshot: Box<BoundedSnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        Err(snapshot_error(ErrorVerb::Write))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LogTypes>>, StorageError<u64>> {
        self.applied_state().await?;
        Ok(None)
    }
}

#[cfg(test)]
mod reader_tests;
