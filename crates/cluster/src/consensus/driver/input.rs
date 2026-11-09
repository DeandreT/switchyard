use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use protobuf::Message as _;
use raft::eraftpb::{Message, MessageType};
use tokio::sync::{Notify, mpsc, oneshot};

use super::{DriverOptions, DriverStatus};
use crate::{
    consensus::{ConsensusIdentity, MAX_CONSENSUS_CONTEXT_BYTES, MAX_CONSENSUS_PROPOSAL_BYTES},
    transport::{PeerHandler, TransportError},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionError {
    #[error("driver retired")]
    Retired,
    #[error("bounded driver input is busy")]
    Busy,
    #[error("invalid durable proposal shape/version/namespace or context")]
    InvalidProposal,
    #[error("not leader in the requested term")]
    NotLeader,
    #[error("core declined proposal admission")]
    Declined,
    #[error("driver failed; persistence may be ambiguous")]
    Failed,
    #[error("receipt already observed or original owner lost")]
    Receipt,
}

/// Admission ONLY: this index may never commit and may be overwritten. Neither
/// successful persistence nor this receipt is an application/client outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalAdmission {
    pub term: u64,
    pub raft_index: u64,
}

pub struct Receipt<T> {
    receiver: Option<oneshot::Receiver<Result<T, AdmissionError>>>,
    credit: Option<Arc<Credit>>,
}
impl<T> Receipt<T> {
    /// Cancellation preserves the receiver and its count/byte credit. Dropping
    /// the receipt cannot cancel a queued or begun original.
    pub async fn wait(&mut self) -> Result<T, AdmissionError> {
        let result = self.receiver.as_mut().ok_or(AdmissionError::Receipt)?.await;
        self.receiver = None;
        self.credit = None;
        result.map_err(|_| AdmissionError::Receipt)?
    }
}

pub(super) struct Shared {
    pub identity: ConsensusIdentity,
    pub local: u64,
    pub status: Mutex<DriverStatus>,
    pub retired: AtomicBool,
    pub stop: Notify,
    // Linearizes retirement with queue admission and claiming a queued event
    // as begun. Never held over a core/backend operation or an await.
    gate: Mutex<()>,
    count: AtomicUsize,
    bytes: AtomicUsize,
    max_count: usize,
    max_bytes: usize,
}
pub(super) struct Credit {
    shared: Arc<Shared>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.shared.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.shared.count.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Shared {
    pub fn new(identity: ConsensusIdentity, local: u64, options: &DriverOptions) -> Arc<Self> {
        Arc::new(Self {
            identity,
            local,
            status: Mutex::new(DriverStatus::default()),
            retired: AtomicBool::new(false),
            stop: Notify::new(),
            gate: Mutex::new(()),
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            max_count: options.max_events,
            max_bytes: options.max_event_bytes,
        })
    }
    pub fn retire(&self) {
        let _gate = self.gate.lock().expect("driver admission fence");
        self.retired.store(true, Ordering::Release);
        self.stop.notify_one();
    }
    pub fn begin(&self) -> bool {
        let _gate = self.gate.lock().expect("driver admission fence");
        !self.retired.load(Ordering::Acquire)
    }
    pub fn status(&self) -> DriverStatus {
        let mut status = self.status.lock().expect("driver status").clone();
        status.retired |= self.retired.load(Ordering::Acquire);
        status.accepting_proposals &= !status.retired;
        status.pending_events = self.count.load(Ordering::Acquire);
        status.pending_event_bytes = self.bytes.load(Ordering::Acquire);
        status
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<Credit>, AdmissionError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(AdmissionError::Retired);
        }
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.max_count).then_some(n + 1)
            })
            .map_err(|_| AdmissionError::Busy)?;
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|&v| v <= self.max_bytes)
            })
            .is_err()
        {
            self.count.fetch_sub(1, Ordering::AcqRel);
            return Err(AdmissionError::Busy);
        }
        Ok(Arc::new(Credit {
            shared: self.clone(),
            bytes,
        }))
    }
}

pub(super) enum Operation {
    Peer(Message, oneshot::Sender<Result<(), AdmissionError>>),
    Campaign(oneshot::Sender<Result<(), AdmissionError>>),
    Tick(oneshot::Sender<Result<(), AdmissionError>>),
    Propose {
        data: Vec<u8>,
        context: Vec<u8>,
        term: u64,
        reply: oneshot::Sender<Result<ProposalAdmission, AdmissionError>>,
    },
}
pub(super) struct Event {
    pub op: Operation,
    pub _credit: Arc<Credit>,
}
impl Event {
    pub fn reject(self, error: AdmissionError) {
        match self.op {
            Operation::Propose { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Operation::Peer(_, reply) | Operation::Tick(reply) | Operation::Campaign(reply) => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

#[derive(Clone)]
pub struct DriverHandle {
    pub(super) shared: Arc<Shared>,
    pub(super) queue: mpsc::Sender<Event>,
}
impl DriverHandle {
    pub fn status(&self) -> DriverStatus {
        self.shared.status()
    }
    pub fn retire(&self) {
        self.shared.retire();
    }
    fn enqueue<T>(
        &self,
        bytes: usize,
        make: impl FnOnce(oneshot::Sender<Result<T, AdmissionError>>) -> Operation,
    ) -> Result<Receipt<T>, AdmissionError> {
        let credit = self.shared.reserve(bytes)?;
        let (reply, receiver) = oneshot::channel();
        let _gate = self.shared.gate.lock().expect("driver admission fence");
        if self.shared.retired.load(Ordering::Acquire) {
            return Err(AdmissionError::Retired);
        }
        self.queue
            .try_send(Event {
                op: make(reply),
                _credit: credit.clone(),
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => AdmissionError::Busy,
                mpsc::error::TrySendError::Closed(_) => AdmissionError::Retired,
            })?;
        Ok(Receipt {
            receiver: Some(receiver),
            credit: Some(credit),
        })
    }
    pub fn campaign(&self) -> Result<Receipt<()>, AdmissionError> {
        self.enqueue(128, Operation::Campaign)
    }
    pub fn tick(&self) -> Result<Receipt<()>, AdmissionError> {
        self.enqueue(128, Operation::Tick)
    }
    pub fn propose(
        &self,
        data: Vec<u8>,
        context: Vec<u8>,
        expected_term: u64,
    ) -> Result<Receipt<ProposalAdmission>, AdmissionError> {
        // The existing decoder is the authoritative shape/version/control
        // fence. No domain apply, catalog lookup, binding authorization or
        // business-validity check belongs to this admission boundary.
        if data.is_empty()
            || data.len() > MAX_CONSENSUS_PROPOSAL_BYTES
            || context.len() > MAX_CONSENSUS_CONTEXT_BYTES
        {
            return Err(AdmissionError::InvalidProposal);
        }
        let proposal =
            domain::DurableProposal::decode(&data).map_err(|_| AdmissionError::InvalidProposal)?;
        if &proposal.command().namespace != self.shared.identity.namespace() {
            return Err(AdmissionError::InvalidProposal);
        }
        let bytes = data.len() + context.len() + 256;
        self.enqueue(bytes, |reply| Operation::Propose {
            data,
            context,
            term: expected_term,
            reply,
        })
    }
    pub(super) fn peer(&self, message: Message) -> Result<Receipt<()>, AdmissionError> {
        if message.to != self.shared.local
            || message.from == self.shared.local
            || !self.shared.identity.contains(message.from)
            || !matches!(
                message.get_msg_type(),
                MessageType::MsgAppend
                    | MessageType::MsgAppendResponse
                    | MessageType::MsgHeartbeat
                    | MessageType::MsgHeartbeatResponse
                    | MessageType::MsgRequestVote
                    | MessageType::MsgRequestVoteResponse
                    | MessageType::MsgRequestPreVote
                    | MessageType::MsgRequestPreVoteResponse
            )
        {
            return Err(AdmissionError::Declined);
        }
        let bytes = message.compute_size() as usize + 256 + message.entries.len() * 128;
        self.enqueue(bytes, |reply| Operation::Peer(message, reply))
    }
}
impl PeerHandler for DriverHandle {
    async fn dispatch(&self, message: Message) -> Result<(), TransportError> {
        // No queue-space await or remote response await while holding a peer
        // credit. Only this admitted original's bounded synchronous work joins.
        self.peer(message)
            .map_err(|_| TransportError::Handler("driver admission refused"))?
            .wait()
            .await
            .map_err(|_| TransportError::Handler("driver original failed"))
    }
}
