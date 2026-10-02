use super::group::{ControlData, NativeRoute, PostData};
use super::*;

pub enum CoordinatorRequest {
    Declare(PendingDeclareReceipt),
    Discharge(SealedDischargeReceipt),
}

pub enum TransactionalIngress {
    Ordinary(RetainedDelivery),
    Posting(TransactionPostingReceipt),
}

pub struct CoordinatorEndpoint {
    route: NativeRoute,
    controller: NativeControllerIdentity,
    requests: mpsc::Receiver<CoordinatorRequest>,
    detached: watch::Receiver<bool>,
    consumption: Arc<Consumption>,
}

impl CoordinatorEndpoint {
    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.controller
    }

    pub async fn recv(&mut self) -> Result<CoordinatorRequest, EngineError> {
        let request = self.requests.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        self.consumption.consumed();
        Ok(request)
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        request(&self.route.commands, |reply| Command::Detach {
            channel: self.route.channel,
            handle: self.route.handle,
            identity: self.route.owner.clone(),
            error: None,
            reply,
        })
        .await
    }
}

impl Drop for CoordinatorEndpoint {
    fn drop(&mut self) {
        self.controller.0.close();
    }
}

pub struct TransactionalReceiver {
    route: NativeRoute,
    deliveries: mpsc::Receiver<TransactionalIngress>,
    detached: watch::Receiver<bool>,
    consumption: Arc<Consumption>,
}

impl TransactionalReceiver {
    pub async fn recv(&mut self) -> Result<TransactionalIngress, EngineError> {
        let delivery = self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        self.consumption.consumed();
        if let TransactionalIngress::Posting(posting) = &delivery {
            posting.dequeued();
        }
        Ok(delivery)
    }

    pub async fn accept_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.settle(receipt, DeliveryState::Accepted(Accepted))
            .await
    }

    pub async fn reject_retained(
        &self,
        receipt: &RetainedDelivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.settle(receipt, DeliveryState::Rejected(crate::Rejected { error }))
            .await
    }

    pub async fn release_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.settle(receipt, DeliveryState::Released(crate::Released))
            .await
    }

    pub async fn modify_retained(
        &self,
        receipt: &RetainedDelivery,
        modified: crate::Modified,
    ) -> Result<(), EngineError> {
        self.settle(receipt, DeliveryState::Modified(modified))
            .await
    }

    async fn settle(
        &self,
        receipt: &RetainedDelivery,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        if !receipt.inner().identity.belongs_to(&self.route.owner) {
            return Err(native_error(NativeTransactionError::InvalidPreparedSet));
        }
        request(&self.route.commands, |reply| Command::Settle {
            channel: self.route.channel,
            handle: self.route.handle,
            identity: receipt.inner().identity.clone(),
            state,
            reply,
        })
        .await
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        request(&self.route.commands, |reply| Command::Detach {
            channel: self.route.channel,
            handle: self.route.handle,
            identity: self.route.owner.clone(),
            error: None,
            reply,
        })
        .await
    }
}

pub(in crate::server) struct NativeAcceptance {
    pub(in crate::server) channel: u16,
    pub(in crate::server) session: SessionIdentity,
    pub(in crate::server) attach: IncomingAttach,
    pub(in crate::server) maximum: u64,
    pub(in crate::server) sink: ReceivingSink,
    pub(in crate::server) detached: watch::Sender<bool>,
    pub(in crate::server) consumption: Arc<Consumption>,
    pub(in crate::server) commands: mpsc::Sender<Command>,
}

pub(in crate::server) enum NativeCommand {
    AcceptCoordinator {
        acceptance: Box<NativeAcceptance>,
        reply: oneshot::Sender<Result<NativeControllerIdentity, EngineError>>,
    },
    AcceptReceiver {
        acceptance: Box<NativeAcceptance>,
        reply: oneshot::Sender<Result<LinkIdentity, EngineError>>,
    },
    Declare {
        data: Box<ControlData>,
        id: TransactionId,
        reply: oneshot::Sender<Result<NativeTransactionIdentity, EngineError>>,
    },
    Provisional {
        data: Box<PostData>,
        reply: oneshot::Sender<Result<PreparedPosting, EngineError>>,
    },
    Rollback {
        data: Box<ControlData>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Finish {
        control: Box<ControlData>,
        postings: Vec<PreparedPosting>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

impl NativeCommand {
    pub(in crate::server) fn reject(self, error: EngineError) {
        match self {
            Self::AcceptCoordinator { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::AcceptReceiver { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Declare { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Rollback { reply, .. } | Self::Finish { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Provisional { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

impl ServerSession {
    pub async fn accept_coordinator(
        &self,
        attach: IncomingAttach,
        max_message_size: u64,
    ) -> Result<CoordinatorEndpoint, EngineError> {
        attach
            .validate_request(&self.identity)
            .map_err(attach_approval_error)?;
        let kind = attach.approval().kind();
        if !matches!(kind, NativeAttachKind::Coordinator(_)) {
            return Err(native_error(NativeTransactionError::InvalidAttach));
        }
        validate_accept_kind(&attach, kind)?;
        let (requests_tx, requests) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, detached) = watch::channel(false);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let owner = attach.approval().link_identity().clone();
        let handle = attach.approval().local_handle();
        let acceptance = Box::new(NativeAcceptance {
            channel: self.channel,
            session: self.identity.clone(),
            attach,
            maximum: max_message_size,
            sink: ReceivingSink::Coordinator(requests_tx),
            detached: detached_tx,
            consumption: consumption.clone(),
            commands: self.commands.clone(),
        });
        let controller = request(&self.commands, |reply| {
            Command::NativeTransactions(NativeCommand::AcceptCoordinator { acceptance, reply })
        })
        .await?;
        Ok(CoordinatorEndpoint {
            route: NativeRoute {
                channel: self.channel,
                handle,
                owner,
                commands: self.commands.clone(),
            },
            controller,
            requests,
            detached,
            consumption,
        })
    }

    pub async fn accept_transactional_receiver(
        &self,
        attach: IncomingAttach,
        max_message_size: u64,
    ) -> Result<TransactionalReceiver, EngineError> {
        attach
            .validate_request(&self.identity)
            .map_err(attach_approval_error)?;
        validate_accept_kind(&attach, NativeAttachKind::Ordinary)?;
        if attach.role != Role::Sender
            || attach
                .target
                .as_ref()
                .and_then(|target| target.as_target())
                .is_none()
            || attach.snd_settle_mode == SenderSettleMode::Settled
        {
            return Err(native_error(NativeTransactionError::InvalidAttach));
        }
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, detached) = watch::channel(false);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let handle = attach.approval().local_handle();
        let acceptance = Box::new(NativeAcceptance {
            channel: self.channel,
            session: self.identity.clone(),
            attach,
            maximum: max_message_size,
            sink: ReceivingSink::Transactional(deliveries_tx),
            detached: detached_tx,
            consumption: consumption.clone(),
            commands: self.commands.clone(),
        });
        let owner = request(&self.commands, |reply| {
            Command::NativeTransactions(NativeCommand::AcceptReceiver { acceptance, reply })
        })
        .await?;
        Ok(TransactionalReceiver {
            route: NativeRoute {
                channel: self.channel,
                handle,
                owner,
                commands: self.commands.clone(),
            },
            deliveries,
            detached,
            consumption,
        })
    }
}

macro_rules! opaque_debug {
    ($($name:ty),+ $(,)?) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct(stringify!($name)).finish_non_exhaustive() }
    })+ };
}
opaque_debug!(
    CoordinatorEndpoint,
    CoordinatorRequest,
    TransactionalReceiver,
    TransactionalIngress,
    NativeAcceptance,
    NativeCommand
);
