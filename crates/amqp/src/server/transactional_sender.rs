use std::fmt;

use super::*;
use native_transactions::{NativeRetirementAttempt, NativeRoute};

mod acceptance;
mod consumer;
mod dispositions;

pub(super) use acceptance::{NativeSenderAcceptance, accept_native_sender};
pub(super) use consumer::{ConsumerControl, ConsumerGuard};
pub(super) use dispositions::{
    apply_native_outgoing_disposition, finish_native_retirement, provisional_native_retirement,
    reconcile_native_retirements,
};

/// An explicitly accepted native sender with rearmable retirement delivery state.
pub struct TransactionalSender {
    route: NativeRoute,
    detached: watch::Receiver<bool>,
}

/// A unique, fully flushed original delivery and its bounded disposition inbox.
/// This does not retain the encoded message or grant transaction commit authority.
///
/// ```compile_fail
/// fn duplicate(sent: amqp::SentDelivery) { let _ = sent.clone(); }
/// ```
pub struct SentDelivery {
    // Revoke pending authority before destroying any queued receipts.
    _guard: ConsumerGuard,
    identity: NativeOutgoingDeliveryIdentity,
    inbox: mpsc::Receiver<TransactionalDisposition>,
}

pub enum TransactionalDisposition {
    Ordinary(PendingSettlement),
    Retirement(native_transactions::TransactionRetirementReceipt),
}

impl SentDelivery {
    pub fn delivery_identity(&self) -> &NativeOutgoingDeliveryIdentity {
        &self.identity
    }

    pub async fn next_disposition(&mut self) -> Result<TransactionalDisposition, EngineError> {
        self.inbox.recv().await.ok_or_else(|| {
            if self.identity.owner().is_retired() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })
    }
}

impl TransactionalSender {
    pub fn sender_identity(&self) -> NativeSenderIdentity {
        NativeSenderIdentity::for_accepted_sender(&self.route.owner)
    }

    /// Returns only after the complete outgoing Transfer has been written and flushed.
    /// Dropping a queued await does not undo transport work; its consumer fails closed.
    pub async fn send_with_dispositions(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<SentDelivery, EngineError> {
        let (reply, sent, mut cancellation) = TransactionalReply::new(self.route.clone());
        self.route
            .commands
            .send(Command::NativeTransactions(
                native_transactions::NativeCommand::SendTransactional {
                    route: self.route.clone(),
                    message: Box::new(message),
                    delivery_tag,
                    reply: Box::new(reply),
                },
            ))
            .await
            .map_err(|_| EngineError::Stopped)?;
        let sent = sent.await.map_err(|_| EngineError::Stopped)??;
        cancellation.disarm();
        Ok(sent)
    }

    pub async fn on_detach(&mut self) {
        wait_for_detach(&mut self.detached).await;
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        request(&self.route.commands, |reply| Command::Detach {
            channel: self.route.channel,
            handle: self.route.handle,
            identity: self.route.owner.clone(),
            error,
            reply,
        })
        .await
    }
}

impl ServerSession {
    pub async fn accept_transactional_sender(
        &self,
        attach: IncomingAttach,
        _max_message_size: u64,
    ) -> Result<TransactionalSender, EngineError> {
        acceptance::validate_sender_approval(&attach, &self.identity)?;
        let owner = attach.approval().link_identity().clone();
        let handle = attach.approval().local_handle();
        let (detached, detached_rx) = watch::channel(false);
        let acceptance = NativeSenderAcceptance {
            channel: self.channel,
            session: self.identity.clone(),
            attach,
            detached,
            commands: self.commands.clone(),
        };
        request(&self.commands, |reply| {
            Command::NativeTransactions(native_transactions::NativeCommand::AcceptSender {
                acceptance: Box::new(acceptance),
                reply,
            })
        })
        .await?;
        Ok(TransactionalSender {
            route: NativeRoute {
                channel: self.channel,
                handle,
                owner,
                commands: self.commands.clone(),
            },
            detached: detached_rx,
        })
    }
}

pub(super) enum OutgoingReply {
    Ordinary(oneshot::Sender<Result<SendOutcome, EngineError>>),
    Transactional(Box<TransactionalReply>),
}

impl From<oneshot::Sender<Result<SendOutcome, EngineError>>> for OutgoingReply {
    fn from(reply: oneshot::Sender<Result<SendOutcome, EngineError>>) -> Self {
        Self::Ordinary(reply)
    }
}

impl From<TransactionalReply> for OutgoingReply {
    fn from(reply: TransactionalReply) -> Self {
        Self::Transactional(Box::new(reply))
    }
}

impl From<Box<TransactionalReply>> for OutgoingReply {
    fn from(reply: Box<TransactionalReply>) -> Self {
        Self::Transactional(reply)
    }
}

impl OutgoingReply {
    pub(super) fn transactional(&self) -> Option<&TransactionalReply> {
        match self {
            Self::Transactional(reply) => Some(reply),
            Self::Ordinary(_) => None,
        }
    }

    pub(super) fn flushed(&mut self, identity: &NativeOutgoingDeliveryIdentity) {
        if let Self::Transactional(reply) = self {
            reply.flushed(identity);
        }
    }

    pub(super) fn send(self, result: Result<SendOutcome, EngineError>) -> Result<(), ()> {
        match self {
            Self::Ordinary(reply) => reply.send(result).map_err(|_| ()),
            Self::Transactional(reply) => match result {
                Ok(outcome) => reply.ordinary(outcome),
                Err(error) => {
                    reply.reject(error);
                    Err(())
                }
            },
        }
    }
}

pub(super) struct TransactionalReply {
    guard: ConsumerGuard,
    pub(super) route: NativeRoute,
    pub(super) events: mpsc::Sender<TransactionalDisposition>,
    receiver: Option<mpsc::Receiver<TransactionalDisposition>>,
    completion: Option<oneshot::Sender<Result<SentDelivery, EngineError>>>,
    pub(super) fully_flushed: bool,
}

impl TransactionalReply {
    fn new(
        route: NativeRoute,
    ) -> (
        Self,
        oneshot::Receiver<Result<SentDelivery, EngineError>>,
        ConsumerGuard,
    ) {
        let control = ConsumerControl::new();
        let (events, receiver) = mpsc::channel(1);
        let (completion, sent) = oneshot::channel();
        (
            Self {
                guard: ConsumerGuard::new(control.clone()),
                route,
                events,
                receiver: Some(receiver),
                completion: Some(completion),
                fully_flushed: false,
            },
            sent,
            ConsumerGuard::new(control),
        )
    }

    pub(super) fn consumer(&self) -> &Arc<ConsumerControl> {
        self.guard.control()
    }

    pub(super) fn reject(mut self, error: EngineError) {
        self.guard.close();
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(Err(error));
        }
    }

    fn flushed(&mut self, identity: &NativeOutgoingDeliveryIdentity) {
        self.fully_flushed = true;
        if let (Some(completion), Some(inbox)) = (self.completion.take(), self.receiver.take()) {
            let sent = SentDelivery {
                _guard: ConsumerGuard::new(self.guard.control().clone()),
                identity: identity.clone(),
                inbox,
            };
            self.guard.disarm();
            let _ = completion.send(Ok(sent));
        }
    }

    fn ordinary(self, outcome: SendOutcome) -> Result<(), ()> {
        let pending = PendingSettlement {
            outcome: outcome.outcome,
            identity: self.route.owner.clone(),
            delivery_identity: outcome.delivery_identity,
            acknowledgement: outcome.acknowledgement,
            channel: self.route.channel,
            handle: self.route.handle,
            commands: self.route.commands.clone(),
        };
        self.events
            .try_send(TransactionalDisposition::Ordinary(pending))
            .map_err(|_| ())
    }
}

impl fmt::Debug for SentDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentDelivery").finish_non_exhaustive()
    }
}
impl fmt::Debug for TransactionalSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionalSender")
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for TransactionalDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionalDisposition")
            .finish_non_exhaustive()
    }
}
