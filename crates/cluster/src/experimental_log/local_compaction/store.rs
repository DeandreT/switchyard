use std::{fmt, thread::JoinHandle};

use domain::CommittedCheckpoint;
use storage::CommittedStore;

use super::super::types::EncodedAppend;
use super::{
    owner::{Handle, Operation, Reply},
    state::{SealReport, StoreState},
};
use crate::{
    LogEntry, LogProfile, LogVote,
    experimental_local_compaction::{
        LocalCompactionError as Error,
        frontier::{Frontier, PairIdentity, PurgePermit, Receipt},
    },
    experimental_owner::RetiredOwner,
};

/// Unique standalone preparation log. It implements no engine storage trait.
///
/// ```compile_fail
/// fn no_engine(store: cluster::ExperimentalCompactionLogStore) {
///     fn needs<T: openraft::storage::RaftLogStorage<cluster::LogTypes>>(_: T) {}
///     needs(store);
/// }
/// ```
pub struct ExperimentalCompactionLogStore {
    handle: Handle,
    thread: Option<JoinHandle<Result<(), Error>>>,
    retired: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ExperimentalCompactionLogStore {
    pub fn create<W: CommittedStore>(writer: W, profile: LogProfile) -> Result<Self, Error> {
        Self::start(StoreState::create(writer, profile)?)
    }
    pub fn open<W: CommittedStore>(writer: W, profile: LogProfile) -> Result<Self, Error> {
        Self::start(StoreState::open(writer, profile)?)
    }
    fn start<W: CommittedStore>(state: StoreState<W>) -> Result<Self, Error> {
        let (handle, thread) = Handle::start(state)?;
        Ok(Self {
            handle,
            thread: Some(thread),
            retired: None,
        })
    }
    /// Borrowed mutation futures cannot outlive moving this facade into a pair.
    pub async fn append<I: IntoIterator<Item = LogEntry>>(
        &mut self,
        entries: I,
    ) -> Result<(), Error> {
        let packet = EncodedAppend::from_entries(entries).map_err(|_| Error::LimitExceeded)?;
        match self.handle.request(Operation::Append(packet)).await? {
            Reply::Done => Ok(()),
            _ => Err(Error::TaskFailed),
        }
    }
    pub async fn save_vote(&mut self, vote: LogVote) -> Result<(), Error> {
        match self.handle.request(Operation::SaveVote(vote)).await? {
            Reply::Done => Ok(()),
            _ => Err(Error::TaskFailed),
        }
    }
    pub async fn read_vote(&self) -> Result<Option<LogVote>, Error> {
        match self.handle.request(Operation::Vote).await? {
            Reply::Vote(vote) => Ok(vote),
            _ => Err(Error::TaskFailed),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_reader(&self) -> TestReader {
        TestReader(self.handle.clone())
    }
    /// Inclusive upper index, at most 32 rows/4 MiB; MAX is representable.
    pub async fn read_limited(&self, start: u64, through: u64) -> Result<Vec<LogEntry>, Error> {
        match self
            .handle
            .request(Operation::Read { start, through })
            .await?
        {
            Reply::Entries(entries) => Ok(entries),
            _ => Err(Error::TaskFailed),
        }
    }
    pub(crate) async fn seal(
        &self,
        identity: PairIdentity,
        checkpoint: Box<CommittedCheckpoint>,
        catalog: Option<Vec<u8>>,
    ) -> Result<SealReport, Error> {
        match self
            .handle
            .request(Operation::Seal {
                identity,
                checkpoint,
                catalog,
            })
            .await?
        {
            Reply::Seal(report) => Ok(*report),
            _ => Err(Error::TaskFailed),
        }
    }
    pub(crate) async fn permit(
        &self,
        frontier: Frontier,
        receipt: Receipt,
    ) -> Result<PurgePermit, Error> {
        match self
            .handle
            .request(Operation::Permit { frontier, receipt })
            .await?
        {
            Reply::Permit(permit) => Ok(permit),
            _ => Err(Error::TaskFailed),
        }
    }
    pub(crate) async fn compact(&self, permit: PurgePermit) -> Result<SealReport, Error> {
        match self.handle.request(Operation::Compact(permit)).await? {
            Reply::Seal(report) => Ok(*report),
            _ => Err(Error::TaskFailed),
        }
    }
    pub(crate) fn into_parts(mut self) -> (Self, Option<RetiredOwner<Error>>) {
        let token = self.thread.take().map(|thread| {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.retired = Some(sender);
            RetiredOwner::new(receiver, thread)
        });
        (self, token)
    }
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.handle.close();
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .map_err(|_| Error::TaskFailed)?
            .map_err(|_| Error::OwnerFailure)?
    }
}
#[cfg(test)]
pub(crate) struct TestReader(Handle);
#[cfg(test)]
impl TestReader {
    pub(crate) async fn read(&self, start: u64, through: u64) -> Result<Vec<LogEntry>, Error> {
        match self.0.request(Operation::Read { start, through }).await? {
            Reply::Entries(entries) => Ok(entries),
            _ => Err(Error::TaskFailed),
        }
    }
    pub(crate) async fn vote(&self) -> Result<Option<LogVote>, Error> {
        match self.0.request(Operation::Vote).await? {
            Reply::Vote(vote) => Ok(vote),
            _ => Err(Error::TaskFailed),
        }
    }
}
impl Drop for ExperimentalCompactionLogStore {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(retired) = self.retired.take() {
            let _ = retired.send(());
        }
    }
}
impl fmt::Debug for ExperimentalCompactionLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExperimentalCompactionLogStore")
            .finish_non_exhaustive()
    }
}
