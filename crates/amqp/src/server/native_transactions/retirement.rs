use std::sync::atomic::{AtomicBool, Ordering};

use super::group::{Group, NativeRoute};
use super::*;

pub(in crate::server) struct NativeRetirementCandidate {
    pub(in crate::server) channel: u16,
    pub(in crate::server) handle: u32,
    pub(in crate::server) owner: LinkIdentity,
    pub(in crate::server) delivery_identity: NativeOutgoingDeliveryIdentity,
}

pub(in crate::server) struct RetirementObligation {
    pub(in crate::server) channel: u16,
    pub(in crate::server) handle: u32,
    pub(in crate::server) owner: LinkIdentity,
    pub(in crate::server) delivery_identity: NativeOutgoingDeliveryIdentity,
    prepared: AtomicBool,
}

impl RetirementObligation {
    pub(in crate::server) fn new(candidate: &NativeRetirementCandidate) -> Arc<Self> {
        Arc::new(Self {
            channel: candidate.channel,
            handle: candidate.handle,
            owner: candidate.owner.clone(),
            delivery_identity: candidate.delivery_identity.clone(),
            prepared: AtomicBool::new(false),
        })
    }

    pub(in crate::server) fn is_prepared(&self) -> bool {
        self.prepared.load(Ordering::Acquire)
    }
}

/// Metadata for one attempt, distinct from the original delivery it retires.
#[derive(Clone)]
pub(in crate::server) struct NativeRetirementAttempt {
    pub(in crate::server) group: Arc<Group>,
    pub(in crate::server) obligation: Arc<RetirementObligation>,
}

impl NativeRetirementAttempt {
    pub(in crate::server) fn same_attempt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.group, &other.group) && Arc::ptr_eq(&self.obligation, &other.obligation)
    }

    pub(in crate::server) fn delivery_identity(&self) -> &NativeOutgoingDeliveryIdentity {
        &self.obligation.delivery_identity
    }

    pub(in crate::server) fn state(&self) -> NativeTransactionState {
        self.group.state()
    }

    pub(in crate::server) fn channel(&self) -> u16 {
        self.obligation.channel
    }

    pub(in crate::server) fn handle(&self) -> u32 {
        self.obligation.handle
    }

    pub(in crate::server) fn owner(&self) -> &LinkIdentity {
        &self.obligation.owner
    }

    pub(in crate::server) fn transaction_id(&self) -> &TransactionId {
        &self.group.id
    }

    pub(in crate::server) fn fault(&self, fault: NativeFault) {
        self.group.fault(fault);
    }

    pub(in crate::server) fn receipt(
        self,
        commands: mpsc::Sender<Command>,
    ) -> TransactionRetirementReceipt {
        let route = NativeRoute {
            channel: self.channel(),
            handle: self.handle(),
            owner: self.owner().clone(),
            commands,
        };
        TransactionRetirementReceipt {
            data: RetirementData {
                attempt: self,
                route,
            },
        }
    }

    pub(in crate::server) fn prepared(&self) {
        self.obligation.prepared.store(true, Ordering::Release);
        self.group.refresh_ready();
    }
}

impl fmt::Debug for NativeRetirementAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeRetirementAttempt")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

pub(in crate::server) struct RetirementData {
    pub(in crate::server) attempt: NativeRetirementAttempt,
    pub(in crate::server) route: NativeRoute,
}

impl Drop for RetirementData {
    fn drop(&mut self) {
        self.attempt.fault(NativeFault::Dropped);
    }
}

/// An actual peer disposition for one exact original outgoing delivery.
/// It cannot be constructed or cloned into another retirement attempt.
///
/// ```compile_fail
/// fn duplicate(receipt: amqp::TransactionRetirementReceipt) { let _ = receipt.clone(); }
/// ```
pub struct TransactionRetirementReceipt {
    pub(in crate::server) data: RetirementData,
}

impl TransactionRetirementReceipt {
    pub fn delivery_identity(&self) -> &NativeOutgoingDeliveryIdentity {
        self.data.attempt.delivery_identity()
    }

    pub fn transaction_id(&self) -> &TransactionId {
        self.data.attempt.transaction_id()
    }

    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.data.attempt.group.controller
    }

    pub fn outcome(&self) -> &Outcome {
        &ACCEPTED_OUTCOME
    }

    pub fn belongs_to_sender(&self, sender: &NativeSenderIdentity) -> bool {
        self.delivery_identity().belongs_to_sender(sender)
    }

    /// Sends the captured presumptive outcome, not a commit or transport settlement.
    pub async fn provisional_accept(self) -> Result<PreparedRetirement, EngineError> {
        let data = self.data;
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::ProvisionalRetirement {
                data: Box::new(data),
                reply,
            })
        })
        .await
    }
}

/// One exact retirement attempt whose provisional response has been flushed.
///
/// ```compile_fail
/// let _ = amqp::PreparedRetirement { data: () };
/// ```
pub struct PreparedRetirement {
    pub(in crate::server) data: Box<RetirementData>,
}

impl PreparedRetirement {
    pub fn delivery_identity(&self) -> &NativeOutgoingDeliveryIdentity {
        self.data.attempt.delivery_identity()
    }

    pub fn transaction_id(&self) -> &TransactionId {
        self.data.attempt.transaction_id()
    }

    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.data.attempt.group.controller
    }

    pub fn outcome(&self) -> &Outcome {
        &ACCEPTED_OUTCOME
    }

    pub fn belongs_to_sender(&self, sender: &NativeSenderIdentity) -> bool {
        self.delivery_identity().belongs_to_sender(sender)
    }

    pub(in crate::server) fn matches(
        &self,
        group: &Group,
        obligation: &Arc<RetirementObligation>,
    ) -> bool {
        std::ptr::eq(self.data.attempt.group.as_ref(), group)
            && Arc::ptr_eq(&self.data.attempt.obligation, obligation)
            && obligation.is_prepared()
    }
}

static ACCEPTED_OUTCOME: Outcome = Outcome::Accepted(Accepted);

/// Exact mixed resources; each admitted obligation must appear once.
pub enum NativePreparedWork {
    Posting(PreparedPosting),
    Retirement(PreparedRetirement),
}

macro_rules! opaque_debug {
    ($($name:ty),+ $(,)?) => { $(impl fmt::Debug for $name {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_struct(stringify!($name)).finish_non_exhaustive()
        }
    })+ };
}
opaque_debug!(
    RetirementData,
    TransactionRetirementReceipt,
    PreparedRetirement,
    NativePreparedWork
);
