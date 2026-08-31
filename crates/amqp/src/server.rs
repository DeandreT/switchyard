use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
};
use tracing::trace;

use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Fields, Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode,
    Transfer, decode_message, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};

const LINK_CREDIT: u32 = 2_048;
const SESSION_WINDOW: u32 = 2_048;
const FRAME_OVERHEAD_RESERVE: usize = 512;
const OUTGOING_DELIVERY_LIMIT: usize = 256;
static NEXT_LINK_INCARNATION: AtomicU64 = AtomicU64::new(0);
static NEXT_SESSION_INCARNATION: AtomicU64 = AtomicU64::new(0);

fn next_link_incarnation() -> u64 {
    NEXT_LINK_INCARNATION.fetch_add(1, Ordering::Relaxed)
}

fn next_session_incarnation() -> u64 {
    NEXT_SESSION_INCARNATION.fetch_add(1, Ordering::Relaxed)
}

pub trait SaslAuthenticator: Send + Sync + 'static {
    fn mechanisms(&self) -> Vec<Symbol>;
    fn authenticate(&self, init: &SaslInit) -> SaslCode;
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("the remote peer closed the connection")]
    RemoteClosed,
    #[error("the remote peer detached the link")]
    RemoteDetached,
    #[error("the AMQP engine stopped")]
    Stopped,
    #[error("invalid AMQP state: {0}")]
    InvalidState(String),
    #[error("SASL authentication failed with {0:?}")]
    SaslAuthentication(SaslCode),
}

pub struct ServerConnection {
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<CleanupCommand>,
    incoming_sessions: mpsc::Receiver<IncomingSession>,
}

pub struct IncomingSession {
    channel: u16,
    incarnation: u64,
    pub begin: Begin,
}

pub struct ServerSession {
    channel: u16,
    incarnation: u64,
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<CleanupCommand>,
    incoming_attaches: mpsc::Receiver<IncomingAttach>,
    pending_attach_identities: Mutex<HashMap<u32, VecDeque<IncomingAttachIdentity>>>,
}

struct IncomingAttach {
    session_incarnation: u64,
    incarnation: u64,
    attach: Attach,
}

#[derive(Clone, Copy)]
struct IncomingAttachIdentity {
    session_incarnation: u64,
    incarnation: u64,
}

pub enum LinkEndpoint {
    Sender(Sender),
    Receiver(Receiver),
}

pub struct Sender {
    name: String,
    channel: u16,
    handle: u32,
    incarnation: u64,
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<CleanupCommand>,
    detached: watch::Receiver<bool>,
    drains: watch::Receiver<Option<DrainRequest>>,
    credits: watch::Receiver<bool>,
    send_capacity: Arc<Semaphore>,
    pending_confirmation: Option<DeliveryConfirmation>,
}

pub struct Receiver {
    channel: u16,
    handle: u32,
    incarnation: u64,
    commands: mpsc::Sender<Command>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    id: u32,
    settled: bool,
    message_format: u32,
    message: Message,
    encoded_message: Vec<u8>,
}

impl Delivery {
    pub fn message_format(&self) -> u32 {
        self.message_format
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn encoded_message(&self) -> &[u8] {
        &self.encoded_message
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DeliveryIdentity {
    channel: u16,
    handle: u32,
    delivery_id: u32,
}

impl DeliveryIdentity {
    pub fn channel(&self) -> u16 {
        self.channel
    }

    pub fn handle(&self) -> u32 {
        self.handle
    }

    pub fn delivery_id(&self) -> u32 {
        self.delivery_id
    }
}

/// An opaque, generation-bound request for a sender to return unused link
/// credit after observing that its message source is empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainRequest {
    channel: u16,
    handle: u32,
    incarnation: u64,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CreditReservationIdentity {
    channel: u16,
    handle: u32,
    incarnation: u64,
    reservation_id: u64,
}

/// One atomically reserved unit of remote link credit.
///
/// Consume this with [`Sender::send_pending_with_credit`] after obtaining an
/// application delivery, or call [`Self::release`] when the source is empty.
/// Dropping a live reservation releases it through a dedicated connection
/// control path that cannot be blocked by the bounded command queue.
#[must_use = "reserved link credit must be consumed or released"]
pub struct CreditReservation {
    identity: CreditReservationIdentity,
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<CleanupCommand>,
    active: bool,
}

impl CreditReservation {
    /// Releases this reservation without sending a delivery.
    pub async fn release(mut self) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::ReleaseCredit {
                reservation: self.identity,
                reply: Some(reply),
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        self.active = false;
        response.await.map_err(|_| EngineError::Stopped)?
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for CreditReservation {
    fn drop(&mut self) {
        if self.active {
            let _ = self.cleanup.send(CleanupCommand::ReleaseCredit {
                reservation: self.identity,
            });
        }
    }
}

/// An outbound delivery that has consumed link credit and been written to the
/// wire, but has not necessarily received its remote outcome yet.
#[must_use = "a pending delivery must be awaited to observe its remote outcome"]
pub struct PendingDelivery {
    identity: DeliveryIdentity,
    response: oneshot::Receiver<Result<RemoteOutcome, EngineError>>,
    commands: mpsc::Sender<Command>,
}

impl PendingDelivery {
    pub fn identity(&self) -> DeliveryIdentity {
        self.identity
    }
}

impl Future for PendingDelivery {
    type Output = Result<DeliveryOutcome, EngineError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.response).poll(context) {
            Poll::Ready(Ok(Ok(remote))) => Poll::Ready(Ok(DeliveryOutcome {
                identity: self.identity,
                outcome: remote.outcome,
                confirmation: remote
                    .confirmation
                    .map(|confirmation| DeliveryConfirmation {
                        identity: DeliveryIdentity {
                            channel: self.identity.channel,
                            handle: confirmation.handle,
                            delivery_id: confirmation.delivery_id,
                        },
                        incarnation: confirmation.incarnation,
                        batchable: confirmation.batchable,
                        commands: self.commands.clone(),
                        _permit: confirmation.permit,
                    }),
            })),
            Poll::Ready(Ok(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(EngineError::Stopped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct DeliveryOutcome {
    identity: DeliveryIdentity,
    outcome: Outcome,
    confirmation: Option<DeliveryConfirmation>,
}

impl DeliveryOutcome {
    pub fn identity(&self) -> DeliveryIdentity {
        self.identity
    }

    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub fn needs_confirmation(&self) -> bool {
        self.confirmation.is_some()
    }

    pub fn into_parts(self) -> (DeliveryIdentity, Outcome, Option<DeliveryConfirmation>) {
        (self.identity, self.outcome, self.confirmation)
    }

    pub async fn confirm(self, state: DeliveryState) -> Result<Outcome, EngineError> {
        if let Some(confirmation) = self.confirmation {
            confirmation.confirm(state).await?;
        }
        Ok(self.outcome)
    }
}

/// The identity-bound settlement confirmation required by receiver settle mode
/// `second` after a remote outcome is durably applied.
pub struct DeliveryConfirmation {
    identity: DeliveryIdentity,
    incarnation: u64,
    batchable: bool,
    commands: mpsc::Sender<Command>,
    _permit: OwnedSemaphorePermit,
}

impl DeliveryConfirmation {
    pub fn identity(&self) -> DeliveryIdentity {
        self.identity
    }

    pub async fn confirm(self, state: DeliveryState) -> Result<(), EngineError> {
        self.confirm_ref(state).await
    }

    async fn confirm_ref(&self, state: DeliveryState) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::Confirm {
            channel: self.identity.channel,
            handle: self.identity.handle,
            incarnation: self.incarnation,
            delivery_id: self.identity.delivery_id,
            state,
            batchable: self.batchable,
            reply,
        })
        .await
    }
}

impl ServerConnection {
    pub async fn accept<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if let Some(authenticator) = sasl {
            expect_header(&mut stream, ProtocolHeader::SASL).await?;
            write_protocol_header(&mut stream, ProtocolHeader::SASL).await?;
            write_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
                    mechanisms: authenticator.mechanisms(),
                })),
            )
            .await?;
            let init = match read_frame(&mut stream).await? {
                Frame::Sasl(SaslPerformative::Init(init)) => init,
                _ => return Err(invalid_state("expected SASL init")),
            };
            let code = authenticator.authenticate(&init);
            write_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Outcome(SaslOutcome {
                    code: code.clone(),
                    additional_data: None,
                })),
            )
            .await?;
            if code != SaslCode::Ok {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "SASL authentication failed",
                )
                .into());
            }
        }

        expect_header(&mut stream, ProtocolHeader::AMQP).await?;
        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        let remote_open = match read_frame(&mut stream).await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(open)),
                ..
            } => open,
            _ => return Err(invalid_state("expected AMQP open")),
        };
        let local_open = Open {
            max_frame_size: remote_open.max_frame_size.max(512),
            channel_max: remote_open.channel_max,
            ..Open::new(container_id)
        };
        write_frame(
            &mut stream,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(local_open)),
                payload: Vec::new(),
            },
        )
        .await?;

        let (commands, command_rx) = mpsc::channel(256);
        let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        tokio::spawn(run_connection(
            stream,
            remote_open.max_frame_size,
            command_rx,
            cleanup_rx,
            incoming_session_tx,
        ));
        Ok(Self {
            commands,
            cleanup,
            incoming_sessions,
        })
    }

    pub async fn next_incoming_session(&mut self) -> Option<IncomingSession> {
        self.incoming_sessions.recv().await
    }

    pub async fn accept_session(
        &self,
        incoming: IncomingSession,
    ) -> Result<ServerSession, EngineError> {
        let (attach_tx, incoming_attaches) = mpsc::channel(32);
        request(&self.commands, |reply| Command::AcceptSession {
            channel: incoming.channel,
            incarnation: incoming.incarnation,
            attach_tx,
            reply,
        })
        .await?;
        Ok(ServerSession {
            channel: incoming.channel,
            incarnation: incoming.incarnation,
            commands: self.commands.clone(),
            cleanup: self.cleanup.clone(),
            incoming_attaches,
            pending_attach_identities: Mutex::new(HashMap::new()),
        })
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::Close { error, reply }).await
    }
}

impl ServerSession {
    pub async fn next_incoming_attach(&mut self) -> Option<Attach> {
        let incoming = self.incoming_attaches.recv().await?;
        self.pending_attach_identities
            .lock()
            .expect("the pending-attach identity lock is not poisoned")
            .entry(incoming.attach.handle)
            .or_default()
            .push_back(IncomingAttachIdentity {
                session_incarnation: incoming.session_incarnation,
                incarnation: incoming.incarnation,
            });
        Some(incoming.attach)
    }

    pub async fn accept_attach(
        &self,
        attach: Attach,
        max_message_size: u64,
    ) -> Result<LinkEndpoint, EngineError> {
        self.accept_attach_with_properties(attach, max_message_size, None)
            .await
    }

    pub async fn accept_attach_with_properties(
        &self,
        attach: Attach,
        max_message_size: u64,
        properties: Option<Fields>,
    ) -> Result<LinkEndpoint, EngineError> {
        let identity = {
            let mut identities = self
                .pending_attach_identities
                .lock()
                .expect("the pending-attach identity lock is not poisoned");
            let identity = identities
                .get_mut(&attach.handle)
                .and_then(VecDeque::pop_front);
            if identities
                .get(&attach.handle)
                .is_some_and(VecDeque::is_empty)
            {
                identities.remove(&attach.handle);
            }
            identity
        };
        let identity =
            identity.ok_or_else(|| invalid_state("attach was not received by this session"))?;
        if identity.session_incarnation != self.incarnation {
            return Err(EngineError::RemoteDetached);
        }
        let incarnation = identity.incarnation;
        let (deliveries_tx, deliveries) = mpsc::channel(32);
        let (detached_tx, detached) = watch::channel(false);
        let (drain_tx, drains) = watch::channel(None);
        let (credit_tx, credits) = watch::channel(false);
        let role = attach.role.clone();
        let name = attach.name.clone();
        let handle = attach.handle;
        request(&self.commands, |reply| Command::AcceptLink {
            channel: self.channel,
            session_incarnation: self.incarnation,
            incarnation,
            attach: Box::new(attach),
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            drain_tx,
            credit_tx,
            reply,
        })
        .await?;

        Ok(match role {
            Role::Sender => LinkEndpoint::Receiver(Receiver {
                channel: self.channel,
                handle,
                incarnation,
                commands: self.commands.clone(),
                deliveries,
                detached,
            }),
            Role::Receiver => LinkEndpoint::Sender(Sender {
                name,
                channel: self.channel,
                handle,
                incarnation,
                commands: self.commands.clone(),
                cleanup: self.cleanup.clone(),
                detached,
                drains,
                credits,
                send_capacity: Arc::new(Semaphore::new(OUTGOING_DELIVERY_LIMIT)),
                pending_confirmation: None,
            }),
        })
    }
}

impl Sender {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn send(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        let outcome = self.send_unconfirmed(message, delivery_tag).await?;
        self.confirm(outcome_state(&outcome)).await?;
        Ok(outcome)
    }

    /// Queues an outbound delivery, waits until it consumes remote link credit
    /// and is written, then returns an independently awaitable remote outcome.
    ///
    /// Multiple returned handles may be awaited in any order. Queue admission
    /// is also bounded, so concurrent callers cannot accumulate an unbounded
    /// number of queued or unsettled deliveries behind this link.
    pub async fn send_pending(
        &self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<PendingDelivery, EngineError> {
        self.send_pending_inner(None, message, delivery_tag).await
    }

    /// Sends a delivery using credit atomically reserved by [`Self::on_credit`].
    ///
    /// This is the lossless pump API: reserve before fetching an application
    /// delivery, then consume the reservation here once that delivery exists.
    pub async fn send_pending_with_credit(
        &self,
        reservation: CreditReservation,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<PendingDelivery, EngineError> {
        if reservation.identity.channel != self.channel
            || reservation.identity.handle != self.handle
        {
            return Err(invalid_state("credit reservation belongs to another link"));
        }
        self.send_pending_inner(Some(reservation), message, delivery_tag)
            .await
    }

    async fn send_pending_inner(
        &self,
        mut reservation: Option<CreditReservation>,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<PendingDelivery, EngineError> {
        let permit = Arc::clone(&self.send_capacity)
            .acquire_owned()
            .await
            .map_err(|_| EngineError::Stopped)?;
        let (started, start) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Send {
                channel: self.channel,
                handle: self.handle,
                incarnation: self.incarnation,
                message: Box::new(message),
                message_format: 0,
                delivery_tag,
                reservation: reservation.as_ref().map(|credit| credit.identity),
                permit,
                started,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        if let Some(reservation) = &mut reservation {
            reservation.disarm();
        }
        let identity = start.await.map_err(|_| EngineError::Stopped)??;
        Ok(PendingDelivery {
            identity,
            response,
            commands: self.commands.clone(),
        })
    }

    /// Sends a delivery and returns the receiver's outcome before confirming
    /// settlement back to it.
    ///
    /// A caller that needs to commit application state before acknowledging a
    /// receiver-settle-mode `second` disposition must call [`Self::confirm`]
    /// after that commit. Only one such delivery may be pending on a sender.
    pub async fn send_unconfirmed(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        if self.pending_confirmation.is_some() {
            return Err(invalid_state(
                "send attempted before the previous outcome was confirmed",
            ));
        }
        let pending = self.send_pending(message, delivery_tag).await?;
        let outcome = pending.await?;
        let (_, outcome, confirmation) = outcome.into_parts();
        self.pending_confirmation = confirmation;
        Ok(outcome)
    }

    /// Confirms the sender's final delivery state after the receiving
    /// application has durably applied the requested outcome.
    pub async fn confirm(&mut self, state: DeliveryState) -> Result<(), EngineError> {
        let Some(confirmation) = self.pending_confirmation.as_ref() else {
            return Ok(());
        };
        let result = confirmation.confirm_ref(state).await;
        if result.is_ok() {
            self.pending_confirmation = None;
        }
        result
    }

    /// Waits for the peer to request that unused link credit be drained.
    ///
    /// The request remains immediately observable until [`Self::drained`]
    /// acknowledges this exact generation. A detach wakes the waiter with
    /// [`EngineError::RemoteDetached`].
    pub async fn on_drain(&self) -> Result<DrainRequest, EngineError> {
        let mut drains = self.drains.clone();
        let mut detached = self.detached.clone();
        loop {
            if *detached.borrow_and_update() {
                return Err(EngineError::RemoteDetached);
            }
            if let Some(request) = *drains.borrow_and_update() {
                return Ok(request);
            }
            tokio::select! {
                changed = drains.changed() => {
                    if changed.is_err() {
                        return Err(if *detached.borrow() {
                            EngineError::RemoteDetached
                        } else {
                            EngineError::Stopped
                        });
                    }
                }
                changed = detached.changed() => {
                    if changed.is_err() && !*detached.borrow() {
                        return Err(EngineError::Stopped);
                    }
                }
            }
        }
    }

    /// Waits for and atomically reserves remote credit for one delivery.
    ///
    /// Readiness is sticky, but the returned reservation—not the observation—is
    /// what protects the slot from a concurrent drain or credit update. Reserve
    /// before fetching an application message, then either consume it with
    /// [`Self::send_pending_with_credit`] or [`CreditReservation::release`] it.
    pub async fn on_credit(&self) -> Result<CreditReservation, EngineError> {
        let mut credits = self.credits.clone();
        let mut detached = self.detached.clone();
        loop {
            if *detached.borrow_and_update() {
                return Err(EngineError::RemoteDetached);
            }
            if *credits.borrow_and_update() {
                let reserved = request(&self.commands, |reply| Command::ReserveCredit {
                    channel: self.channel,
                    handle: self.handle,
                    incarnation: self.incarnation,
                    reply,
                })
                .await?;
                if let Some(identity) = reserved {
                    return Ok(CreditReservation {
                        identity,
                        commands: self.commands.clone(),
                        cleanup: self.cleanup.clone(),
                        active: true,
                    });
                }
                continue;
            }
            tokio::select! {
                changed = credits.changed() => {
                    if changed.is_err() {
                        return Err(if *detached.borrow() {
                            EngineError::RemoteDetached
                        } else {
                            EngineError::Stopped
                        });
                    }
                }
                changed = detached.changed() => {
                    if changed.is_err() && !*detached.borrow() {
                        return Err(EngineError::Stopped);
                    }
                }
            }
        }
    }

    /// Returns all unused credit for the exact drain generation supplied by
    /// [`Self::on_drain`].
    pub async fn drained(&self, drain_request: DrainRequest) -> Result<(), EngineError> {
        if drain_request.channel != self.channel || drain_request.handle != self.handle {
            return Err(invalid_state("drain request belongs to another link"));
        }
        request(&self.commands, |reply| Command::Drained {
            request: drain_request,
            reply,
        })
        .await
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
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
            error,
            reply,
        })
        .await
    }
}

fn outcome_state(outcome: &Outcome) -> DeliveryState {
    match outcome {
        Outcome::Accepted(value) => DeliveryState::Accepted(value.clone()),
        Outcome::Rejected(value) => DeliveryState::Rejected(value.clone()),
        Outcome::Released(value) => DeliveryState::Released(value.clone()),
        Outcome::Modified(value) => DeliveryState::Modified(value.clone()),
    }
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Delivery, EngineError> {
        self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })
    }

    pub async fn accept(&self, delivery: &Delivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Accepted(Accepted))
            .await
    }

    pub async fn reject(
        &self,
        delivery: &Delivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Rejected(crate::Rejected { error }))
            .await
    }

    pub async fn release(&self, delivery: &Delivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Released(crate::Released))
            .await
    }

    pub async fn modify(
        &self,
        delivery: &Delivery,
        modified: crate::Modified,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Modified(modified))
            .await
    }

    async fn settle(&self, delivery: &Delivery, state: DeliveryState) -> Result<(), EngineError> {
        if delivery.settled {
            return Ok(());
        }
        request(&self.commands, |reply| Command::Settle {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
            delivery_id: delivery.id,
            state,
            reply,
        })
        .await
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
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
            error,
            reply,
        })
        .await
    }
}

async fn expect_header<Io>(stream: &mut Io, expected: ProtocolHeader) -> Result<(), EngineError>
where
    Io: AsyncRead + Unpin,
{
    let actual = read_protocol_header(stream).await?;
    if actual != expected {
        return Err(invalid_state(format!(
            "expected protocol id {}, got {}",
            expected.protocol_id, actual.protocol_id
        )));
    }
    Ok(())
}

async fn wait_for_detach(detached: &mut watch::Receiver<bool>) {
    while !*detached.borrow_and_update() {
        if detached.changed().await.is_err() {
            break;
        }
    }
}

async fn request<T>(
    commands: &mpsc::Sender<Command>,
    make: impl FnOnce(oneshot::Sender<Result<T, EngineError>>) -> Command,
) -> Result<T, EngineError> {
    let (reply, response) = oneshot::channel();
    commands
        .send(make(reply))
        .await
        .map_err(|_| EngineError::Stopped)?;
    response.await.map_err(|_| EngineError::Stopped)?
}

mod engine;
use engine::*;

#[cfg(feature = "test-client")]
mod client;

#[cfg(feature = "test-client")]
pub use client::{ClientConnection, ClientDelivery, ClientReceiver, ClientSender, ClientSession};
