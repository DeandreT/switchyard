use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use domain::CommittedCheckpoint;
use storage::CommittedStore;
use tokio::sync::oneshot;

use super::super::types::EncodedAppend;
use super::state::{SealReport, StoreState};
use crate::{
    LogEntry, LogVote,
    experimental_local_compaction::{
        LocalCompactionError as Error,
        frontier::{Frontier, PairIdentity, PurgePermit, Receipt},
    },
};

pub(super) enum Operation {
    Append(EncodedAppend),
    Read {
        start: u64,
        through: u64,
    },
    SaveVote(LogVote),
    Vote,
    Seal {
        identity: PairIdentity,
        checkpoint: Box<CommittedCheckpoint>,
        catalog: Option<Vec<u8>>,
    },
    Permit {
        frontier: Frontier,
        receipt: Receipt,
    },
    Compact(PurgePermit),
}
pub(super) enum Reply {
    Done,
    Entries(Vec<LogEntry>),
    Vote(Option<LogVote>),
    Seal(Box<SealReport>),
    Permit(PurgePermit),
}
type Response = Result<Reply, Error>;
type Published = (Response, oneshot::Receiver<()>);

#[derive(Default)]
struct Budget {
    closed: Option<Error>,
    jobs: usize,
    bytes: usize,
}
struct Admission(Mutex<Budget>);
struct Lease {
    admission: Arc<Admission>,
    bytes: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.admission.0.lock().unwrap_or_else(|e| e.into_inner());
        state.jobs -= 1;
        state.bytes -= self.bytes;
    }
}
struct Packet {
    operation: Option<Operation>,
    response: Option<oneshot::Sender<Published>>,
    lease: Option<Lease>,
}
impl Packet {
    fn finish(mut self, result: Response) {
        self.publish(result);
    }
    fn publish(&mut self, result: Response) {
        drop(self.operation.take());
        let (sender, receiver) = oneshot::channel();
        if let Some(response) = self.response.take() {
            let _ = response.send((result, receiver));
        }
        drop(self.lease.take());
        let _ = sender.send(());
    }
}
impl Drop for Packet {
    fn drop(&mut self) {
        self.publish(Err(Error::TaskFailed));
    }
}

#[derive(Clone)]
pub(super) struct Handle {
    sender: flume::Sender<Packet>,
    admission: Arc<Admission>,
}

impl Handle {
    pub(super) fn start<W: CommittedStore>(
        state: StoreState<W>,
    ) -> Result<(Self, JoinHandle<Result<(), Error>>), Error> {
        let (sender, receiver) = flume::bounded(crate::MAX_LOG_OWNER_JOBS);
        let admission = Arc::new(Admission(Mutex::new(Budget::default())));
        let owner_admission = admission.clone();
        let thread = thread::Builder::new()
            .name("switchyard-local-log-owner".into())
            .spawn(move || run(state, receiver, owner_admission))
            .map_err(|_| Error::OwnerFailure)?;
        Ok((Self { sender, admission }, thread))
    }
    pub(super) async fn request(&self, operation: Operation) -> Response {
        let bytes = match &operation {
            Operation::Append(packet) => packet.encoded_bytes(),
            // Small checkpoint/member/meta authority packets are bounded independently.
            Operation::Seal { .. } | Operation::Permit { .. } | Operation::Compact(_) => 32 * 1024,
            _ => 64,
        };
        let (response, receiver) = oneshot::channel();
        let mut packet = Packet {
            operation: Some(operation),
            response: Some(response),
            lease: None,
        };
        let admitted = {
            let mut state = self.admission.0.lock().map_err(|_| Error::TaskFailed)?;
            if let Some(error) = state.closed {
                Err((error, packet))
            } else if state.jobs == crate::MAX_LOG_OWNER_JOBS
                || bytes > crate::MAX_LOG_OWNER_BYTES.saturating_sub(state.bytes)
            {
                Err((Error::Busy, packet))
            } else {
                state.jobs += 1;
                state.bytes += bytes;
                packet.lease = Some(Lease {
                    admission: self.admission.clone(),
                    bytes,
                });
                self.sender
                    .try_send(packet)
                    .map_err(|error| (Error::Closed, error.into_inner()))
            }
        };
        if let Err((error, packet)) = admitted {
            packet.finish(Err(error));
            return Err(error);
        }
        let (result, completed) = receiver.await.map_err(|_| Error::TaskFailed)?;
        completed.await.map_err(|_| Error::TaskFailed)?;
        result
    }
    pub(super) fn close(&self) {
        self.fail(Error::Closed);
    }
    fn fail(&self, error: Error) {
        let mut state = self.admission.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed.is_none() {
            state.closed = Some(error);
        }
    }
}

fn run<W: CommittedStore>(
    mut state: StoreState<W>,
    receiver: flume::Receiver<Packet>,
    admission: Arc<Admission>,
) -> Result<(), Error> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        loop {
            if admission.0.lock().map_or(true, |s| s.closed.is_some()) && receiver.is_empty() {
                break;
            }
            let mut packet = match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(packet) => packet,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            };
            let response = match packet.operation.take() {
                Some(Operation::Append(packet)) => state.append(packet).map(|()| Reply::Done),
                Some(Operation::Read { start, through }) => {
                    state.read(start, through).map(Reply::Entries)
                }
                Some(Operation::SaveVote(vote)) => state.save_vote(vote).map(|()| Reply::Done),
                Some(Operation::Vote) => state.vote().map(Reply::Vote),
                Some(Operation::Seal {
                    identity,
                    checkpoint,
                    catalog,
                }) => state
                    .seal(identity, &checkpoint, catalog.as_deref())
                    .map(|report| Reply::Seal(Box::new(report))),
                Some(Operation::Permit { frontier, receipt }) => {
                    state.permit(frontier, receipt).map(Reply::Permit)
                }
                Some(Operation::Compact(permit)) => state
                    .compact(permit)
                    .map(|report| Reply::Seal(Box::new(report))),
                None => Err(Error::TaskFailed),
            };
            packet.finish(response);
        }
    }));
    let mut budget = admission.0.lock().unwrap_or_else(|e| e.into_inner());
    budget.closed = Some(if result.is_err() {
        Error::TaskFailed
    } else {
        Error::Closed
    });
    drop(budget);
    if result.is_err() {
        for packet in receiver.try_iter() {
            packet.finish(Err(Error::TaskFailed));
        }
        return Err(Error::TaskFailed);
    }
    Ok(())
}
