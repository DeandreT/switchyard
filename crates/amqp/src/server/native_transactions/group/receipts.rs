use super::*;

#[derive(Clone)]
pub(in crate::server) struct NativeRoute {
    pub(in crate::server) channel: u16,
    pub(in crate::server) handle: u32,
    pub(in crate::server) owner: LinkIdentity,
    pub(in crate::server) commands: mpsc::Sender<Command>,
}

pub(in crate::server) struct ControlData {
    pub(in crate::server) route: NativeRoute,
    pub(in crate::server) controller: NativeControllerIdentity,
    pub(in crate::server) delivery: RetainedDelivery,
    pub(in crate::server) group: Option<Arc<Group>>,
    pub(in crate::server) fail: bool,
    pub(in crate::server) terminal_abort: bool,
    armed: bool,
}

impl ControlData {
    pub(in crate::server) fn new(
        route: NativeRoute,
        controller: NativeControllerIdentity,
        delivery: Delivery,
        group: Option<Arc<Group>>,
        fail: bool,
    ) -> Self {
        Self {
            route,
            controller,
            delivery: RetainedDelivery::new(delivery),
            group,
            fail,
            terminal_abort: false,
            armed: true,
        }
    }

    pub(in crate::server) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ControlData {
    fn drop(&mut self) {
        if self.armed {
            if let Some(group) = &self.group {
                group.fault(NativeFault::Dropped);
            } else {
                self.controller.0.close();
            }
        }
    }
}

pub struct PendingDeclareReceipt {
    pub(in crate::server) data: ControlData,
}

impl PendingDeclareReceipt {
    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.data.controller
    }

    /// Registers only this actor-approved Declare, and replies after write/flush.
    pub async fn declared(
        self,
        id: TransactionId,
    ) -> Result<NativeTransactionIdentity, EngineError> {
        let data = self.data;
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::Declare {
                data: Box::new(data),
                id,
                reply,
            })
        })
        .await
    }

    /// Refuses only this original Declare using its negotiated outcome profile.
    /// No transaction identifier is allocated or registered.
    ///
    /// ```compile_fail
    /// async fn twice(receipt: amqp::PendingDeclareReceipt) {
    ///     receipt.refuse(amqp::NativeDeclarationRefusal::ResourceLimit).await;
    ///     receipt.refuse(amqp::NativeDeclarationRefusal::Unavailable).await;
    /// }
    /// ```
    pub async fn refuse(self, reason: NativeDeclarationRefusal) -> Result<(), EngineError> {
        let data = self.data;
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::RefuseDeclare {
                data: Box::new(data),
                reason,
                reply,
            })
        })
        .await
    }
}

pub(in crate::server) struct NativePartialPosting {
    pub(in crate::server) group: Arc<Group>,
    pub(in crate::server) obligation: Arc<Obligation>,
    armed: bool,
}

impl NativePartialPosting {
    pub(in crate::server) fn new(group: Arc<Group>, obligation: Arc<Obligation>) -> Self {
        Self {
            group,
            obligation,
            armed: true,
        }
    }

    pub(in crate::server) fn validate_continuation(
        &self,
        state: Option<&DeliveryState>,
    ) -> Result<(), NativeTransactionError> {
        if state.is_none() {
            return Ok(());
        }
        if matches!(state, Some(DeliveryState::Transactional(state)) if state.txn_id == self.group.id && state.outcome.is_none())
        {
            return Ok(());
        }
        self.group.fault(NativeFault::Continuation);
        Err(NativeTransactionError::Faulted(NativeFault::Continuation))
    }

    pub(in crate::server) fn fault(&self, fault: NativeFault) {
        self.group.fault(fault);
    }

    pub(in crate::server) fn into_post(
        mut self,
        route: NativeRoute,
        delivery: Delivery,
    ) -> TransactionPostingReceipt {
        self.armed = false;
        self.obligation.queued();
        TransactionPostingReceipt {
            data: PostData {
                group: self.group.clone(),
                obligation: self.obligation.clone(),
                route,
                delivery: RetainedDelivery::new(delivery),
            },
        }
    }
}

impl Drop for NativePartialPosting {
    fn drop(&mut self) {
        if self.armed {
            self.group.fault(NativeFault::Dropped);
        }
    }
}

pub(in crate::server) struct PostData {
    pub(in crate::server) group: Arc<Group>,
    pub(in crate::server) obligation: Arc<Obligation>,
    pub(in crate::server) route: NativeRoute,
    pub(in crate::server) delivery: RetainedDelivery,
}

impl Drop for PostData {
    fn drop(&mut self) {
        self.group.fault(NativeFault::Dropped);
    }
}

/// A unique transactional posting; it cannot become an ordinary delivery.
/// ```compile_fail
/// fn duplicate(receipt: amqp::TransactionPostingReceipt) { let _ = receipt.clone(); }
/// ```
pub struct TransactionPostingReceipt {
    pub(in crate::server) data: PostData,
}

impl TransactionPostingReceipt {
    pub fn message(&self) -> &Message {
        self.data.delivery.message()
    }

    pub fn message_format(&self) -> u32 {
        self.data.delivery.message_format()
    }
    pub fn transaction_id(&self) -> &TransactionId {
        &self.data.group.id
    }
    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.data.group.controller
    }

    /// Tests active exact receiving-link origin, not commit authority.
    pub fn belongs_to_receiver(&self, receiver: &NativeReceiverIdentity) -> bool {
        receiver.owns_delivery(&self.data.delivery.inner().identity)
    }

    pub(in crate::server) fn dequeued(&self) {
        self.data.obligation.held();
    }

    /// This is a provisional transport outcome, not a store commit decision.
    pub async fn provisional_accept(self) -> Result<PreparedPosting, EngineError> {
        let data = self.data;
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::Provisional {
                data: Box::new(data),
                reply,
            })
        })
        .await
    }

    pub fn fail(self) {
        self.data.group.fault(NativeFault::Stage);
    }
}

pub struct PreparedPosting {
    pub(in crate::server) data: PostData,
}

impl PreparedPosting {
    pub fn message(&self) -> &Message {
        self.data.delivery.message()
    }
    pub fn message_format(&self) -> u32 {
        self.data.delivery.message_format()
    }
    pub fn transaction_id(&self) -> &TransactionId {
        &self.data.group.id
    }

    /// Remains independent of numeric delivery aliases after provisional ACK.
    pub fn belongs_to_receiver(&self, receiver: &NativeReceiverIdentity) -> bool {
        receiver.owns_delivery(&self.data.delivery.inner().identity)
    }
    pub(in crate::server) fn matches(&self, group: &Group, obligation: &Arc<Obligation>) -> bool {
        std::ptr::eq(self.data.group.as_ref(), group)
            && Arc::ptr_eq(&self.data.obligation, obligation)
            && obligation.is_flushed()
    }
}

pub(in crate::server) enum DischargeStatus {
    Live(Arc<Group>),
    Terminal {
        id: TransactionId,
        state: NativeTransactionState,
    },
}

pub struct SealedDischargeReceipt {
    pub(in crate::server) data: ControlData,
    pub(in crate::server) status: DischargeStatus,
}

impl SealedDischargeReceipt {
    pub fn transaction_id(&self) -> &TransactionId {
        match &self.status {
            DischargeStatus::Live(group) => &group.id,
            DischargeStatus::Terminal { id, .. } => id,
        }
    }
    pub fn fail(&self) -> bool {
        self.data.fail
    }
    pub fn state(&self) -> NativeTransactionState {
        match &self.status {
            DischargeStatus::Live(group) => group.state(),
            DischargeStatus::Terminal { state, .. } => *state,
        }
    }

    pub async fn wait_ready(&self) -> Result<(), NativeTransactionError> {
        match &self.status {
            DischargeStatus::Live(group) => group.wait_ready().await,
            DischargeStatus::Terminal { .. } => Err(NativeTransactionError::NotReady),
        }
    }

    /// Takes the sealed control receipt and every exact provisional posting.
    pub fn prepare(
        self,
        postings: Vec<PreparedPosting>,
    ) -> Result<NativeReadySubmission, NativeTransactionError> {
        let DischargeStatus::Live(group) = &self.status else {
            return Err(NativeTransactionError::NotReady);
        };
        if self.data.fail || group.state() != NativeTransactionState::Ready {
            return Err(NativeTransactionError::NotReady);
        }
        if !group.exact_prepared(&postings) {
            return Err(NativeTransactionError::InvalidPreparedSet);
        }
        Ok(NativeReadySubmission::new(self.data, postings))
    }

    pub async fn rollback(self) -> Result<(), EngineError> {
        let data = self.data;
        if !data.fail {
            return Err(native_error(NativeTransactionError::InvalidDecision));
        }
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::Rollback {
                data: Box::new(data),
                reply,
            })
        })
        .await
    }

    /// Refuses an original commit request before an owner can start it.
    /// Caller-held postings retain their content until they are dropped.
    ///
    /// ```compile_fail
    /// async fn twice(receipt: amqp::SealedDischargeReceipt) {
    ///     receipt.refuse_staging().await;
    ///     receipt.refuse_staging().await;
    /// }
    /// ```
    pub async fn refuse_staging(self) -> Result<(), EngineError> {
        let DischargeStatus::Live(group) = &self.status else {
            return Err(native_error(NativeTransactionError::InvalidDecision));
        };
        if self.data.fail
            || self.data.terminal_abort
            || !self
                .data
                .group
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, group))
        {
            return Err(native_error(NativeTransactionError::InvalidDecision));
        }
        let data = self.data;
        let commands = data.route.commands.clone();
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::RefuseStaging {
                data: Box::new(data),
                reply,
            })
        })
        .await
    }
}

macro_rules! opaque_debug {
    ($($name:ty),+ $(,)?) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct(stringify!($name)).finish_non_exhaustive() }
    })+ };
}
opaque_debug!(
    PendingDeclareReceipt,
    NativePartialPosting,
    PostData,
    ControlData,
    TransactionPostingReceipt,
    PreparedPosting,
    SealedDischargeReceipt
);
