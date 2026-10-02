use std::{collections::VecDeque, time::Instant};

use amqp::{
    NativeControllerIdentity, NativeReceiverIdentity, NativeTransactionIdentity, PreparedPosting,
    SealedDischargeReceipt, TransactionId, TransactionPostingReceipt,
};
use tokio::sync::mpsc;

use super::{LinkAuthorization, QueueAdmission, WorkerClose};
use crate::AtomicTransactionController;

pub(super) struct Controller {
    pub(super) identity: NativeControllerIdentity,
    pub(super) logical: AtomicTransactionController,
    pub(super) close: mpsc::Sender<WorkerClose>,
    pub(super) closed: bool,
}

pub(super) struct Producer {
    pub(super) key: u64,
    pub(super) identity: NativeReceiverIdentity,
    pub(super) admission: QueueAdmission,
    pub(super) authorization: Option<LinkAuthorization>,
    pub(super) close: mpsc::Sender<WorkerClose>,
    pub(super) closed: bool,
}

pub(super) struct Posting {
    pub(super) producer: u64,
    pub(super) receipt: TransactionPostingReceipt,
}

pub(super) struct Group {
    pub(super) id: TransactionId,
    pub(super) controller: NativeControllerIdentity,
    pub(super) deadline: Instant,
    pub(super) native: Option<NativeTransactionIdentity>,
    pub(super) queued: VecDeque<Posting>,
    pub(super) prepared: Vec<PreparedPosting>,
    pub(super) producers: Vec<u64>,
    pub(super) sealed: Option<SealedDischargeReceipt>,
    pub(super) ready: bool,
    pub(super) waiting_ready: bool,
    pub(super) posting_busy: bool,
    pub(super) handoff_busy: bool,
    pub(super) submitted: bool,
    pub(super) refused: bool,
    pub(super) ending: bool,
    pub(super) operations: usize,
}

impl Group {
    pub(super) fn new(
        id: TransactionId,
        controller: NativeControllerIdentity,
        deadline: Instant,
    ) -> Self {
        Self {
            id,
            controller,
            deadline,
            native: None,
            queued: VecDeque::new(),
            prepared: Vec::new(),
            producers: Vec::new(),
            sealed: None,
            ready: false,
            waiting_ready: false,
            posting_busy: false,
            handoff_busy: false,
            submitted: false,
            refused: false,
            ending: false,
            operations: 0,
        }
    }
}

pub(super) fn id_key(id: &TransactionId) -> Option<u64> {
    let bytes: [u8; 8] = id.as_bytes().try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}
