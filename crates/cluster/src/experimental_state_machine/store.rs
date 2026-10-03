use std::{fmt, thread::JoinHandle};

use domain::CommittedStreamId;
use openraft::{
    ErrorSubject, ErrorVerb, Snapshot, SnapshotMeta, StorageError, storage::RaftStateMachine,
};
use storage::CommittedStore;

use crate::{LogEntry, LogTypes};

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
    handle: Handle,
    thread: Option<JoinHandle<Result<(), StateMachineError>>>,
}

impl ExperimentalStateMachine {
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

    fn start<W: CommittedStore>(state: StoreState<W>) -> Result<Self, StateMachineError> {
        let (handle, thread) = Handle::start(state)?;
        Ok(Self {
            handle,
            thread: Some(thread),
        })
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

impl Drop for ExperimentalStateMachine {
    fn drop(&mut self) {
        self.handle.close();
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
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        Err(snapshot_error(ErrorVerb::Write))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, openraft::BasicNode>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
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
