use std::{collections::VecDeque, sync::Arc, time::Instant};

use amqp::{
    Error as AmqpProtocolError, NativeControllerIdentity, NativeOutgoingDeliveryIdentity,
    NativePreparedWork, NativeReceiverIdentity, NativeSenderIdentity, NativeTransactionIdentity,
    SealedDischargeReceipt, TransactionId, TransactionPostingReceipt, TransactionRetirementReceipt,
};
use domain::{LockToken, SequenceNumber};
use tokio::sync::{mpsc, oneshot};

use super::{LinkAuthorization, QueueAdmission, WorkerClose};
use crate::AtomicTransactionController;

pub(super) struct Controller {
    pub(super) identity: NativeControllerIdentity,
    pub(super) logical: AtomicTransactionController,
    pub(super) authorization: Option<Arc<crate::authorization::ConnectionAuthorization>>,
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

pub(super) struct HeldDelivery {
    pub(super) sequence: SequenceNumber,
    pub(super) token: LockToken,
    pub(super) original: NativeOutgoingDeliveryIdentity,
}

#[derive(Clone)]
pub(super) enum RetirementCompletion {
    Committed,
    Rearmed,
    Refused(AmqpProtocolError),
}

pub(super) struct Consumer {
    pub(super) key: u64,
    pub(super) identity: NativeSenderIdentity,
    pub(super) admission: QueueAdmission,
    pub(super) authorization: Option<LinkAuthorization>,
    pub(super) close: mpsc::Sender<WorkerClose>,
    pub(super) closed: bool,
    pub(super) held: Option<HeldDelivery>,
    pub(super) pending: Option<u64>,
}

pub(super) struct Retirement {
    pub(super) consumer: u64,
    pub(super) receipt: TransactionRetirementReceipt,
}

pub(super) struct RetirementWaiter {
    pub(super) consumer: u64,
    pub(super) original: NativeOutgoingDeliveryIdentity,
    pub(super) reply: oneshot::Sender<RetirementCompletion>,
}

pub(super) enum QueuedWork {
    Posting(Box<Posting>),
    Retirement(Retirement),
}

pub(super) struct Group {
    pub(super) id: TransactionId,
    pub(super) controller: NativeControllerIdentity,
    pub(super) deadline: Instant,
    pub(super) native: Option<NativeTransactionIdentity>,
    pub(super) queued: VecDeque<QueuedWork>,
    pub(super) prepared: Vec<NativePreparedWork>,
    pub(super) producers: Vec<u64>,
    pub(super) consumers: Vec<u64>,
    pub(super) retirements: Vec<RetirementWaiter>,
    pub(super) seen_retirements: Vec<NativeOutgoingDeliveryIdentity>,
    pub(super) seen_postings: Vec<NativeReceiverIdentity>,
    pub(super) rollback_origins:
        Option<Vec<(NativeSenderIdentity, NativeOutgoingDeliveryIdentity)>>,
    pub(super) rollback_postings: Option<Vec<NativeReceiverIdentity>>,
    pub(super) rollback: bool,
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
            consumers: Vec::new(),
            retirements: Vec::new(),
            seen_retirements: Vec::new(),
            seen_postings: Vec::new(),
            rollback_origins: None,
            rollback_postings: None,
            rollback: false,
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
