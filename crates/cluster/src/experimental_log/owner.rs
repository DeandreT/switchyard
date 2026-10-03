use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use flume::{Receiver, RecvTimeoutError, Sender};
use openraft::{LogState, storage::LogFlushed};
use storage::CommittedStore;
use tokio::sync::oneshot;

use super::{
    LogEntry, LogId, LogProfile, LogStorageError, LogTypes, LogVote, LogWorkload,
    MAX_LOG_OWNER_JOBS,
    budget::{Admission, Lease},
    io_error,
    state::{OwnedLogRange, StoreState},
    types::EncodedAppend,
};

pub(super) enum Operation {
    Append(EncodedAppend),
    ReadFull(OwnedLogRange),
    ReadLimited { start: u64, end: u64 },
    Profile,
    LogState,
    SaveVote(LogVote),
    ReadVote,
    Truncate(LogId),
    Purge(LogId),
}

pub(super) enum Reply {
    Done,
    Entries(Vec<LogEntry>),
    Profile(LogProfile),
    LogState(LogState<LogTypes>),
    Vote(Option<LogVote>),
}

type Response = Result<Reply, LogStorageError>;
type PublishedResponse = (Response, oneshot::Receiver<()>);

pub(super) struct Packet {
    operation: Option<Operation>,
    response: Option<oneshot::Sender<PublishedResponse>>,
    callback: Option<LogFlushed<LogTypes>>,
    lease: Option<Lease>,
}

impl Packet {
    pub(super) fn encoded_bytes(&self) -> usize {
        match self.operation.as_ref() {
            Some(Operation::Append(entries)) => entries.encoded_bytes(),
            _ => 64,
        }
    }

    pub(super) fn set_lease(&mut self, lease: Lease) {
        self.lease = Some(lease);
    }

    pub(super) fn finish(mut self, result: Response) {
        if let Some(callback) = self.callback.take() {
            callback.log_io_completed(if result.is_ok() {
                Ok(())
            } else {
                Err(io_error())
            });
        }
        self.publish(result);
    }

    fn publish(&mut self, result: Response) {
        drop(self.operation.take());
        let Some(response) = self.response.take() else {
            drop(self.lease.take());
            return;
        };
        let (released, completion) = oneshot::channel();
        let _ = response.send((result, completion));
        // Publish while charged, but do not let a returning caller race the
        // refund when it immediately submits another near-capacity operation.
        drop(self.lease.take());
        let _ = released.send(());
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback.log_io_completed(Err(io_error()));
        }
        self.publish(Err(LogStorageError::Panicked));
    }
}

#[derive(Clone)]
pub(super) struct Handle {
    sender: Sender<Packet>,
    admission: Arc<Admission>,
}

impl Handle {
    pub(super) fn start<W: CommittedStore>(
        state: StoreState<W>,
    ) -> Result<(Self, JoinHandle<Result<(), LogStorageError>>), LogStorageError> {
        let (sender, receiver) = flume::bounded(MAX_LOG_OWNER_JOBS);
        let admission = Arc::new(Admission::default());
        let owner_admission = Arc::clone(&admission);
        let thread = thread::Builder::new()
            .name("switchyard-log-owner".into())
            .spawn(move || run(state, receiver, owner_admission))
            .map_err(|_| LogStorageError::ThreadStart)?;
        Ok((Self { sender, admission }, thread))
    }

    pub(super) async fn request(
        &self,
        operation: Operation,
        callback: Option<LogFlushed<LogTypes>>,
    ) -> Response {
        let (response, receiver) = oneshot::channel();
        self.admission.enqueue(
            &self.sender,
            Packet {
                operation: Some(operation),
                response: Some(response),
                callback,
                lease: None,
            },
        )?;
        // Dropping this waiter never removes its accepted packet or lease.
        let (result, completion) = receiver.await.map_err(|_| LogStorageError::Panicked)?;
        completion.await.map_err(|_| LogStorageError::Panicked)?;
        result
    }

    pub(super) fn close(&self) {
        self.admission.close(LogStorageError::Closed);
    }

    pub(super) fn workload(&self) -> Result<LogWorkload, LogStorageError> {
        self.admission.workload()
    }
}

fn run<W: CommittedStore>(
    mut state: StoreState<W>,
    receiver: Receiver<Packet>,
    admission: Arc<Admission>,
) -> Result<(), LogStorageError> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        loop {
            if admission.is_closed() && receiver.is_empty() {
                break;
            }
            let mut packet = match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(packet) => packet,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            let result = match packet.operation.take() {
                Some(operation) => execute(&mut state, operation),
                None => Err(LogStorageError::Panicked),
            };
            packet.finish(result);
        }
    }));
    if result.is_err() {
        admission.close(LogStorageError::Panicked);
        for packet in receiver.try_iter() {
            packet.finish(Err(LogStorageError::Panicked));
        }
        return Err(LogStorageError::Panicked);
    }
    admission.close(LogStorageError::Closed);
    Ok(())
}

fn execute<W: CommittedStore>(state: &mut StoreState<W>, operation: Operation) -> Response {
    let result = match operation {
        Operation::Append(entries) => state.append(&entries).map(|()| Reply::Done),
        Operation::ReadFull(range) => state.read_full(range).map(Reply::Entries),
        Operation::ReadLimited { start, end } => state.read_limited(start, end).map(Reply::Entries),
        Operation::Profile => state.profile().map(Reply::Profile),
        Operation::LogState => state.log_state().map(Reply::LogState),
        Operation::SaveVote(vote) => state.save_vote(vote).map(|()| Reply::Done),
        Operation::ReadVote => state.read_vote().map(Reply::Vote),
        Operation::Truncate(since) => state.truncate(since).map(|()| Reply::Done),
        Operation::Purge(through) => state.purge(through).map(|()| Reply::Done),
    };
    result.map_err(LogStorageError::from)
}
