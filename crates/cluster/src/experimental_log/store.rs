use std::{fmt, ops::RangeBounds, thread::JoinHandle};

use openraft::{
    ErrorSubject, ErrorVerb, LogState, RaftLogReader, StorageError,
    storage::{LogFlushed, RaftLogStorage},
};
use storage::CommittedStore;

use super::{
    LogEntry, LogId, LogProfile, LogRetention, LogStorageError, LogTypes, LogVote, LogWorkload,
    io_error,
    owner::{Handle, Operation, Reply},
    raft_error,
    state::{OwnedLogRange, StoreState},
    types::EncodedAppend,
};
use crate::experimental_owner::RetiredOwner;

/// One unique adapter and its blocking storage owner. This is not a Raft node.
///
/// ```compile_fail
/// fn duplicate(store: cluster::ExperimentalLogStore) {
///     let second_writer = store.clone();
/// }
/// ```
pub struct ExperimentalLogStore {
    handle: Handle,
    thread: Option<JoinHandle<Result<(), LogStorageError>>>,
    retired: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ExperimentalLogStore {
    /// Initialize a distinct log-role profile in a pristine replica store.
    pub fn create<W: CommittedStore>(
        writer: W,
        profile: LogProfile,
    ) -> Result<Self, LogStorageError> {
        Self::start(StoreState::create(writer, profile)?)
    }

    /// Open only complete, matching durable log state. No data is adopted.
    pub fn open<W: CommittedStore>(
        writer: W,
        profile: LogProfile,
    ) -> Result<Self, LogStorageError> {
        Self::start(StoreState::open(writer, profile)?)
    }

    fn start<W: CommittedStore>(state: StoreState<W>) -> Result<Self, LogStorageError> {
        let (handle, thread) = Handle::start(state)?;
        Ok(Self {
            handle,
            thread: Some(thread),
            retired: None,
        })
    }

    pub(crate) async fn profile(&self) -> Result<LogProfile, LogStorageError> {
        match self.handle.request(Operation::Profile, None).await? {
            Reply::Profile(profile) => Ok(profile),
            _ => Err(LogStorageError::Corrupt),
        }
    }

    pub(crate) fn enable_retirement_report(
        &self,
    ) -> Result<
        tokio::sync::oneshot::Receiver<Result<super::FinalLogReport, LogStorageError>>,
        LogStorageError,
    > {
        self.handle.enable_retirement_report()
    }

    pub(crate) async fn healthy_retirement_report(
        &self,
    ) -> Result<super::FinalLogReport, LogStorageError> {
        match self
            .handle
            .request(Operation::RetirementReport, None)
            .await?
        {
            Reply::RetirementReport(report) => Ok(*report),
            _ => Err(LogStorageError::Corrupt),
        }
    }

    pub(crate) fn into_runtime_parts(
        mut self,
    ) -> Result<(Self, RetiredOwner<LogStorageError>), LogStorageError> {
        let thread = self.thread.take().ok_or(LogStorageError::Closed)?;
        let (retired, receiver) = tokio::sync::oneshot::channel();
        self.retired = Some(retired);
        Ok((self, RetiredOwner::new(receiver, thread)))
    }

    pub fn log_reader(&self) -> ReadOnlyLogReader {
        ReadOnlyLogReader {
            handle: self.handle.clone(),
        }
    }

    pub fn workload(&self) -> Result<LogWorkload, LogStorageError> {
        self.handle.workload()
    }

    /// Stop admission, complete accepted FIFO work, and join the owner. Caller
    /// cancellation of this wait does not cancel those accepted operations.
    pub async fn shutdown(mut self) -> Result<(), LogStorageError> {
        self.handle.close();
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .map_err(|_| LogStorageError::Panicked)?
            .map_err(|_| LogStorageError::Panicked)?
    }
}

impl Drop for ExperimentalLogStore {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(retired) = self.retired.take() {
            let _ = retired.send(());
        }
    }
}

impl fmt::Debug for ExperimentalLogStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExperimentalLogStore")
            .finish_non_exhaustive()
    }
}

/// Clones can read through the owner, but cannot obtain its writer or mutate.
///
/// ```compile_fail
/// async fn mutate(mut reader: cluster::ReadOnlyLogReader) {
///     use openraft::storage::RaftLogStorage;
///     reader.save_vote(&cluster::LogVote::new(1, 1)).await;
/// }
/// ```
#[derive(Clone)]
pub struct ReadOnlyLogReader {
    handle: Handle,
}

impl ReadOnlyLogReader {
    pub(crate) async fn retention(&self) -> Result<LogRetention, LogStorageError> {
        match self.handle.request(Operation::Retention, None).await? {
            Reply::Retention(retention) => Ok(retention),
            _ => Err(LogStorageError::Corrupt),
        }
    }
}

impl fmt::Debug for ReadOnlyLogReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadOnlyLogReader")
            .finish_non_exhaustive()
    }
}

async fn read_full<R: RangeBounds<u64>>(
    handle: &Handle,
    range: R,
) -> Result<Vec<LogEntry>, StorageError<u64>> {
    match handle
        .request(Operation::ReadFull(OwnedLogRange::from_range(range)), None)
        .await
    {
        Ok(Reply::Entries(entries)) => Ok(entries),
        _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Read)),
    }
}

async fn read_limited(
    handle: &Handle,
    start: u64,
    end: u64,
) -> Result<Vec<LogEntry>, StorageError<u64>> {
    match handle
        .request(Operation::ReadLimited { start, end }, None)
        .await
    {
        Ok(Reply::Entries(entries)) => Ok(entries),
        _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Read)),
    }
}

impl RaftLogReader<LogTypes> for ExperimentalLogStore {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<LogEntry>, StorageError<u64>> {
        read_full(&self.handle, range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogEntry>, StorageError<u64>> {
        read_limited(&self.handle, start, end).await
    }
}

impl RaftLogReader<LogTypes> for ReadOnlyLogReader {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<LogEntry>, StorageError<u64>> {
        read_full(&self.handle, range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogEntry>, StorageError<u64>> {
        read_limited(&self.handle, start, end).await
    }
}

impl RaftLogStorage<LogTypes> for ExperimentalLogStore {
    type LogReader = ReadOnlyLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<LogTypes>, StorageError<u64>> {
        match self.handle.request(Operation::LogState, None).await {
            Ok(Reply::LogState(state)) => Ok(state),
            _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Read)),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.log_reader()
    }

    async fn save_vote(&mut self, vote: &LogVote) -> Result<(), StorageError<u64>> {
        match self.handle.request(Operation::SaveVote(*vote), None).await {
            Ok(Reply::Done) => Ok(()),
            _ => Err(raft_error(ErrorSubject::Vote, ErrorVerb::Write)),
        }
    }

    async fn read_vote(&mut self) -> Result<Option<LogVote>, StorageError<u64>> {
        match self.handle.request(Operation::ReadVote, None).await {
            Ok(Reply::Vote(vote)) => Ok(vote),
            _ => Err(raft_error(ErrorSubject::Vote, ErrorVerb::Read)),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<LogTypes>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = LogEntry> + Send,
        I::IntoIter: Send,
    {
        let entries = match EncodedAppend::from_entries(entries) {
            Ok(entries) => entries,
            Err(_) => {
                callback.log_io_completed(Err(io_error()));
                return Err(raft_error(ErrorSubject::Logs, ErrorVerb::Write));
            }
        };
        match self
            .handle
            .request(Operation::Append(entries), Some(callback))
            .await
        {
            Ok(Reply::Done) => Ok(()),
            _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Write)),
        }
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError<u64>> {
        match self.handle.request(Operation::Truncate(log_id), None).await {
            Ok(Reply::Done) => Ok(()),
            _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Delete)),
        }
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError<u64>> {
        match self.handle.request(Operation::Purge(log_id), None).await {
            Ok(Reply::Done) => Ok(()),
            _ => Err(raft_error(ErrorSubject::Logs, ErrorVerb::Delete)),
        }
    }
}
