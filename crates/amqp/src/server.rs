use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::Arc,
    time::Duration,
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Notify, mpsc, oneshot, watch},
};

use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Fields, Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode,
    Transfer, decode_message, encode_message, read_frame_with_max_size, read_protocol_header,
    write_frame, write_protocol_header,
};

#[cfg(test)]
use crate::read_frame;

mod flow_control;
mod frame_writer;
mod receive_credit;

use flow_control::{LinkCredit, LinkSnapshot, SessionWindow};
use frame_writer::FrameWriter;
use receive_credit::{Consumption, ReceiveCredit};

const LINK_CREDIT: u32 = 32;
const SESSION_WINDOW: u32 = 2_048;
const DELIVERY_QUEUE_CAPACITY: usize = LINK_CREDIT as usize;
const MAX_PENDING_ATTACHES: usize = 32;
const SEND_FRAME_QUANTUM: usize = 16;
const MAX_DELIVERY_TAG_BYTES: usize = 32;
const MAX_CLOSING_HANDLES: usize = 65_536;
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_MAX_FRAME_SIZE: u32 = 262_144;
const MIN_MAX_FRAME_SIZE: u32 = 512;

fn normalized_frame_size(advertised: u32) -> Result<u32, EngineError> {
    if advertised < MIN_MAX_FRAME_SIZE {
        return Err(invalid_state(
            "maximum frame size must be at least 512 bytes",
        ));
    }
    Ok(advertised.min(crate::codec::MAX_FRAME_SIZE as u32))
}

fn checked_open_frame(open: Open) -> Result<Frame, EngineError> {
    let frame = Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Open(open)),
        payload: Vec::new(),
    };
    if crate::encode_frame(&frame)?.len() > MIN_MAX_FRAME_SIZE as usize {
        return Err(invalid_state(
            "local AMQP Open exceeds the initial 512-byte frame limit",
        ));
    }
    Ok(frame)
}

async fn notify_frame_size_error<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    error: &io::Error,
) {
    let Some(error) = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<crate::codec::FrameSizeError>())
    else {
        return;
    };
    let _ = tokio::time::timeout(
        DEFAULT_CLOSE_TIMEOUT,
        writer.write_amqp(
            0,
            Performative::Close(Close {
                error: Some(Error::new(
                    crate::ErrorCondition::Custom(Symbol::from("amqp:connection:framing-error")),
                    error.to_string(),
                    None,
                )),
            }),
            Vec::new(),
        ),
    )
    .await;
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
    #[error(
        "the encoded message has {message_bytes} bytes, exceeding the link maximum of {maximum_bytes}"
    )]
    MessageSizeExceeded {
        message_bytes: u64,
        maximum_bytes: u64,
    },
    #[error("AMQP {0} timed out")]
    Timeout(&'static str),
}

pub struct ServerConnection {
    commands: mpsc::Sender<Command>,
    incoming_sessions: mpsc::Receiver<IncomingSession>,
    lifecycle: ConnectionLifecycle,
    close_timeout: Duration,
    consumed: Arc<Notify>,
}

struct ConnectionLifecycle {
    cancellation: watch::Sender<bool>,
    terminated: watch::Receiver<bool>,
}

impl ConnectionLifecycle {
    fn new() -> (Self, watch::Receiver<bool>, watch::Sender<bool>) {
        let (cancellation, cancelled) = watch::channel(false);
        let (terminated_tx, terminated) = watch::channel(false);
        (
            Self {
                cancellation,
                terminated,
            },
            cancelled,
            terminated_tx,
        )
    }

    async fn wait_terminated(&self) {
        wait_for_detach(&mut self.terminated.clone()).await;
    }

    async fn shutdown(&self) {
        let _ = self.cancellation.send(true);
        self.wait_terminated().await;
    }

    fn close_guard(&self) -> CloseCancellation {
        CloseCancellation(Some(self.cancellation.clone()))
    }
}

impl Drop for ConnectionLifecycle {
    fn drop(&mut self) {
        let _ = self.cancellation.send(true);
    }
}

struct CloseCancellation(Option<watch::Sender<bool>>);

impl Drop for CloseCancellation {
    fn drop(&mut self) {
        if let Some(cancellation) = self.0.take() {
            let _ = cancellation.send(true);
        }
    }
}

// A driver cancellation must not leave its independent socket reader alive.
struct ConnectionReader(Option<tokio::task::JoinHandle<()>>);

impl ConnectionReader {
    async fn shutdown(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ConnectionReader {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

pub struct IncomingSession {
    channel: u16,
    pub begin: Begin,
}

pub struct ServerSession {
    channel: u16,
    commands: mpsc::Sender<Command>,
    incoming_attaches: mpsc::Receiver<Attach>,
    consumed: Arc<Notify>,
}

pub enum LinkEndpoint {
    Sender(Sender),
    Receiver(Receiver),
}

pub struct Sender {
    name: String,
    max_message_size: Option<u64>,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    detached: watch::Receiver<bool>,
}

/// A receiver's outcome whose second-mode acknowledgement is still pending.
pub struct PendingSettlement {
    outcome: Outcome,
    delivery_id: Option<u32>,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
}

impl PendingSettlement {
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub async fn accept(self) -> Result<(), EngineError> {
        self.finish(DeliveryState::Accepted(Accepted)).await
    }

    pub async fn reject(self, error: Error) -> Result<(), EngineError> {
        self.finish(DeliveryState::Rejected(crate::Rejected {
            error: Some(error),
        }))
        .await
    }

    async fn finish(self, state: DeliveryState) -> Result<(), EngineError> {
        let Some(delivery_id) = self.delivery_id else {
            return Ok(());
        };
        request(&self.commands, |reply| Command::SettleOutgoing {
            channel: self.channel,
            handle: self.handle,
            delivery_id,
            state,
            reply,
        })
        .await
    }
}

pub struct Receiver {
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
    consumption: Arc<Consumption>,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    id: u32,
    settled: bool,
    message: Message,
}

impl Delivery {
    pub fn message(&self) -> &Message {
        &self.message
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
        let local_max_frame_size = normalized_frame_size(DEFAULT_MAX_FRAME_SIZE)?;
        let mut local_open = Open {
            max_frame_size: local_max_frame_size,
            ..Open::new(container_id)
        };
        checked_open_frame(local_open.clone())?;
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
            let init = match read_frame_with_max_size(&mut stream, local_max_frame_size).await? {
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
        let remote_open = match read_frame_with_max_size(&mut stream, MIN_MAX_FRAME_SIZE).await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(open)),
                ..
            } => open,
            _ => return Err(invalid_state("expected AMQP open")),
        };
        let remote_max_frame_size = normalized_frame_size(remote_open.max_frame_size)?;
        local_open.channel_max = remote_open.channel_max;
        write_frame(&mut stream, &checked_open_frame(local_open)?).await?;

        let (commands, command_rx) = mpsc::channel(256);
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        let consumed = Arc::new(Notify::new());
        let driver_consumed = consumed.clone();
        let (lifecycle, cancellation, terminated) = ConnectionLifecycle::new();
        tokio::spawn(async move {
            run_connection(
                stream,
                remote_max_frame_size,
                local_max_frame_size,
                command_rx,
                incoming_session_tx,
                driver_consumed,
                cancellation,
            )
            .await;
            let _ = terminated.send(true);
        });
        Ok(Self {
            commands,
            incoming_sessions,
            lifecycle,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            consumed,
        })
    }

    /// Bounds graceful Close, including time waiting to enqueue or write it.
    /// A zero duration requests immediate cancellation.
    pub fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Cancels blocked driver work and waits until both socket tasks terminate.
    pub async fn shutdown(&self) {
        self.lifecycle.shutdown().await;
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
            attach_tx,
            reply,
        })
        .await?;
        Ok(ServerSession {
            channel: incoming.channel,
            commands: self.commands.clone(),
            incoming_attaches,
            consumed: self.consumed.clone(),
        })
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        if self.close_timeout.is_zero() {
            self.shutdown().await;
            return Err(EngineError::Timeout("Close acknowledgment"));
        }
        let mut guard = self.lifecycle.close_guard();
        let closing = async {
            let result = request(&self.commands, |reply| Command::Close { error, reply }).await;
            if result.is_ok() {
                self.lifecycle.wait_terminated().await;
            } else {
                self.shutdown().await;
            }
            result
        };
        let result = match tokio::time::timeout(self.close_timeout, closing).await {
            Ok(result) => result,
            Err(_) => {
                self.shutdown().await;
                Err(EngineError::Timeout("Close acknowledgment"))
            }
        };
        let _ = guard.0.take();
        result
    }
}

impl ServerSession {
    pub async fn next_incoming_attach(&mut self) -> Option<Attach> {
        self.incoming_attaches.recv().await
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
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let (detached_tx, detached) = watch::channel(false);
        let role = attach.role.clone();
        let name = attach.name.clone();
        let max_message_size_for_sender = normalized_message_size(attach.max_message_size);
        let handle = attach.handle;
        request(&self.commands, |reply| Command::AcceptLink {
            channel: self.channel,
            attach: Box::new(attach),
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            consumption: consumption.clone(),
            reply,
        })
        .await?;

        Ok(match role {
            Role::Sender => LinkEndpoint::Receiver(Receiver {
                channel: self.channel,
                handle,
                commands: self.commands.clone(),
                deliveries,
                detached,
                consumption,
            }),
            Role::Receiver => LinkEndpoint::Sender(Sender {
                name,
                max_message_size: max_message_size_for_sender,
                channel: self.channel,
                handle,
                commands: self.commands.clone(),
                detached,
            }),
        })
    }
}

impl Sender {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The peer receiver's encoded-message limit, or no advertised limit.
    pub fn max_message_size(&self) -> Option<u64> {
        self.max_message_size
    }

    pub async fn send(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        let settlement = self.send_with_settlement(message, delivery_tag).await?;
        let outcome = settlement.outcome.clone();
        let state = match outcome.clone() {
            Outcome::Accepted(value) => DeliveryState::Accepted(value),
            Outcome::Rejected(value) => DeliveryState::Rejected(value),
            Outcome::Released(value) => DeliveryState::Released(value),
            Outcome::Modified(value) => DeliveryState::Modified(value),
        };
        settlement.finish(state).await?;
        Ok(outcome)
    }

    pub async fn send_with_settlement(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<PendingSettlement, EngineError> {
        let (reply, outcome) = oneshot::channel();
        self.commands
            .send(Command::Send {
                channel: self.channel,
                handle: self.handle,
                message: Box::new(message),
                delivery_tag,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        let outcome = outcome.await.map_err(|_| EngineError::Stopped)??;
        Ok(PendingSettlement {
            outcome: outcome.outcome,
            delivery_id: outcome.delivery_id,
            channel: self.channel,
            handle: self.handle,
            commands: self.commands.clone(),
        })
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
            error,
            reply,
        })
        .await
    }
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Delivery, EngineError> {
        let delivery = self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        self.consumption.consumed();
        Ok(delivery)
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

enum Command {
    AcceptSession {
        channel: u16,
        attach_tx: mpsc::Sender<Attach>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    AcceptLink {
        channel: u16,
        attach: Box<Attach>,
        max_message_size: u64,
        properties: Option<Fields>,
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        consumption: Arc<Consumption>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        message: Box<Message>,
        delivery_tag: DeliveryTag,
        reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        delivery_id: u32,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    SettleOutgoing {
        channel: u16,
        handle: u32,
        delivery_id: u32,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Close {
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

fn reject_closed_command(command: Command) {
    match command {
        Command::Send { reply, .. } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
        Command::AcceptSession { reply, .. }
        | Command::AcceptLink { reply, .. }
        | Command::Settle { reply, .. }
        | Command::SettleOutgoing { reply, .. }
        | Command::Detach { reply, .. }
        | Command::Close { reply, .. } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
    }
}

struct SessionState {
    attach_tx: Option<mpsc::Sender<Attach>>,
    links: HashMap<u32, LinkState>,
    closing_handles: HashSet<u32>,
    pending_attaches: HashMap<u32, PendingLinkFlow>,
    cancelled_pending_attaches: HashSet<u32>,
    pending_attach_events: VecDeque<Attach>,
    flow: SessionWindow,
    next_delivery_id: u32,
    ending: bool,
}

struct PendingLinkFlow {
    peer_role: Role,
    initial_sender_count: Option<u32>,
    credit: LinkCredit,
    latest: Option<Flow>,
}

impl PendingLinkFlow {
    fn new(peer_role: Role, initial_sender_count: Option<u32>) -> Self {
        Self {
            peer_role,
            initial_sender_count,
            credit: LinkCredit::new(0),
            latest: None,
        }
    }

    fn update(&mut self, mut flow: Flow) -> Result<(), EngineError> {
        if self.peer_role == Role::Receiver {
            self.credit
                .update_peer_optional(flow.delivery_count, flow.link_credit, flow.drain)
                .map_err(|error| invalid_state(error.to_string()))?;
        } else {
            let count = flow
                .delivery_count
                .ok_or_else(|| invalid_state("sender flow has no delivery count"))?;
            if self
                .initial_sender_count
                .is_some_and(|initial| initial != count)
            {
                return Err(invalid_state(
                    "pending sender flow has an impossible delivery count",
                ));
            }
            if flow.drain {
                return Err(invalid_state("unsolicited pending sender drain"));
            }
            self.initial_sender_count = Some(count);
        }
        flow.echo |= self.latest.as_ref().is_some_and(|previous| previous.echo);
        self.latest = Some(flow);
        Ok(())
    }
}

impl SessionState {
    fn new(peer: &Begin) -> Self {
        Self {
            attach_tx: None,
            links: HashMap::new(),
            closing_handles: HashSet::new(),
            pending_attaches: HashMap::new(),
            cancelled_pending_attaches: HashSet::new(),
            pending_attach_events: VecDeque::new(),
            flow: SessionWindow::new(
                0,
                peer.next_outgoing_id,
                peer.incoming_window,
                peer.outgoing_window,
                SESSION_WINDOW,
            ),
            next_delivery_id: 0,
            ending: false,
        }
    }
}

enum LinkState {
    Sending(Box<SendingLink>),
    Receiving(ReceivingLink),
}

struct SendingLink {
    max_message_size: Option<u64>,
    receiver_settle_mode: ReceiverSettleMode,
    settle_mode: SenderSettleMode,
    credit: LinkCredit,
    queued: VecDeque<QueuedSend>,
    active: Option<ActiveSend>,
    unsettled: HashMap<u32, OutgoingDelivery>,
    pending_acknowledgements: HashSet<u32>,
    detached: watch::Sender<bool>,
}

struct QueuedSend {
    payload: Vec<u8>,
    delivery_tag: DeliveryTag,
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
}

struct ActiveSend {
    payload: Vec<u8>,
    offset: usize,
    first_frame_sent: bool,
    delivery_id: u32,
    delivery_tag: DeliveryTag,
    settled: bool,
    settled_reply: Option<oneshot::Sender<Result<SendOutcome, EngineError>>>,
}

struct OutgoingDelivery {
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    outcome: Option<(Outcome, bool)>,
}

struct SendOutcome {
    outcome: Outcome,
    delivery_id: Option<u32>,
}

struct ReceivingLink {
    max_message_size: u64,
    deliveries: mpsc::Sender<Delivery>,
    partial: Option<PartialDelivery>,
    detached: watch::Sender<bool>,
    credit: ReceiveCredit,
}

struct PartialDelivery {
    id: u32,
    tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    bytes: Vec<u8>,
}

async fn run_connection<Io>(
    stream: Io,
    remote_max_frame_size: u32,
    local_max_frame_size: u32,
    mut commands: mpsc::Receiver<Command>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    consumed: Arc<Notify>,
    mut cancellation: watch::Receiver<bool>,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let Ok(mut writer) = FrameWriter::new(writer, remote_max_frame_size) else {
        return;
    };
    let (frames_tx, mut frames) = mpsc::channel(256);
    let mut reader_task = ConnectionReader(Some(tokio::spawn(async move {
        loop {
            let frame = read_frame_with_max_size(&mut reader, local_max_frame_size).await;
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                break;
            }
        }
    })));

    let mut sessions = HashMap::<u16, SessionState>::new();
    let mut closing_replies = Vec::<oneshot::Sender<Result<(), EngineError>>>::new();
    let mut pump_ready = false;
    let mut pump_cursor = 0;
    let processing = async {
        loop {
            tokio::select! {
                frame = frames.recv() => {
                    let Some(frame) = frame else { break };
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) => {
                            if closing_replies.is_empty() {
                                notify_frame_size_error(&mut writer, &error).await;
                            }
                            break;
                        }
                    };
                    if !closing_replies.is_empty()
                        && !matches!(&frame, Frame::Amqp {
                            performative: Some(Performative::Close(_)), ..
                        })
                    {
                        continue;
                    }
                    match handle_frame(
                        frame,
                        &mut writer,
                        &incoming_sessions,
                        &mut sessions,
                        remote_max_frame_size,
                        !closing_replies.is_empty(),
                    ).await {
                        Ok(FrameAction::Continue) => pump_ready = true,
                        Ok(FrameAction::Closed) => {
                            for reply in closing_replies.drain(..) {
                                let _ = reply.send(Ok(()));
                            }
                            commands.close();
                            while let Ok(command) = commands.try_recv() {
                                if let Command::Close { reply, .. } = command {
                                    let _ = reply.send(Ok(()));
                                } else {
                                    reject_closed_command(command);
                                }
                            }
                            break;
                        }
                        Err(_) => break,
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    let command = match command {
                        Command::Close { reply, .. } if !closing_replies.is_empty() => {
                            closing_replies.push(reply);
                            continue;
                        }
                        command if !closing_replies.is_empty() => {
                            reject_closed_command(command);
                            continue;
                        }
                        command => command,
                    };
                    match handle_command(
                        command,
                        &mut writer,
                        &mut sessions,
                        remote_max_frame_size,
                    ).await {
                        Ok(CommandAction::Continue) => pump_ready = true,
                        Ok(CommandAction::Closing(reply)) => {
                            pump_ready = false;
                            closing_replies.push(reply);
                        }
                        Err(_) => break,
                    }
                }
                () = consumed.notified(), if closing_replies.is_empty() => {
                    if refresh_consumed(&mut writer, &mut sessions).await.is_err() {
                        break;
                    }
                    pump_ready = true;
                }
                () = tokio::task::yield_now(), if pump_ready && closing_replies.is_empty() => {
                    match pump_connection(&mut writer, &mut sessions, &mut pump_cursor).await {
                        Ok(ready) => pump_ready = ready,
                        Err(_) => break,
                    }
                }
            }
        }
    };
    tokio::select! {
        biased;
        () = wait_for_detach(&mut cancellation) => {}
        () = processing => {}
    }

    reader_task.shutdown().await;

    for session in sessions.values_mut() {
        for link in session.links.values_mut() {
            stop_link(link);
        }
    }
    for reply in closing_replies {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
}

enum FrameAction {
    Continue,
    Closed,
}

async fn handle_frame<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut FrameWriter<W>,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
    locally_closing: bool,
) -> Result<FrameAction, EngineError> {
    let Frame::Amqp {
        channel,
        performative,
        payload,
    } = frame
    else {
        return Err(invalid_state("SASL frame after AMQP open"));
    };
    let Some(performative) = performative else {
        return Ok(FrameAction::Continue);
    };
    if sessions.get(&channel).is_some_and(|session| session.ending)
        && !matches!(&performative, Performative::End(_) | Performative::Close(_))
    {
        return Ok(FrameAction::Continue);
    }

    match performative {
        Performative::Begin(begin) => {
            if sessions.contains_key(&channel) {
                return Err(invalid_state("duplicate AMQP begin"));
            }
            sessions.insert(channel, SessionState::new(&begin));
            if incoming_sessions
                .try_send(IncomingSession { channel, begin })
                .is_err()
            {
                writer
                    .write_amqp(
                        channel,
                        Performative::Begin(Begin {
                            remote_channel: Some(channel),
                            ..Begin::default()
                        }),
                        Vec::new(),
                    )
                    .await?;
                refuse_session(
                    channel,
                    "amqp:resource-limit-exceeded",
                    "incoming session queue is full",
                    writer,
                    sessions,
                )
                .await?;
            }
        }
        Performative::Attach(attach) => {
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("attach on an unknown session"))?;
            let handle = attach.handle;
            if session.links.contains_key(&handle)
                || session.pending_attaches.contains_key(&handle)
                || session.cancelled_pending_attaches.contains(&handle)
                || session.closing_handles.contains(&handle)
            {
                refuse_session(
                    channel,
                    "amqp:session:handle-in-use",
                    "link handle is already assigned",
                    writer,
                    sessions,
                )
                .await?;
            } else if session.pending_attaches.len() + session.cancelled_pending_attaches.len()
                == MAX_PENDING_ATTACHES
            {
                refuse_session(
                    channel,
                    "amqp:resource-limit-exceeded",
                    "pending attach limit reached",
                    writer,
                    sessions,
                )
                .await?;
            } else {
                session.pending_attaches.insert(
                    handle,
                    PendingLinkFlow::new(attach.role.clone(), attach.initial_delivery_count),
                );
                if let Some(attach_tx) = &session.attach_tx {
                    if attach_tx.try_send(*attach).is_err() {
                        refuse_session(
                            channel,
                            "amqp:resource-limit-exceeded",
                            "incoming attach queue is full",
                            writer,
                            sessions,
                        )
                        .await?;
                    }
                } else {
                    session.pending_attach_events.push_back(*attach);
                }
            }
        }
        Performative::Flow(flow) => {
            apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
        }
        Performative::Transfer(transfer) => {
            receive_transfer(channel, transfer, payload, sessions, writer).await?;
        }
        Performative::Disposition(disposition) => {
            apply_disposition(channel, disposition, writer, sessions).await?;
        }
        Performative::Detach(detach) => {
            if let Some(session) = sessions.get_mut(&channel) {
                let locally_closing = session.closing_handles.remove(&detach.handle);
                if session.pending_attaches.remove(&detach.handle).is_some() {
                    if session.attach_tx.is_some() {
                        // Approval may already be outside the driver. Do not let
                        // handle reuse make that stale approval attach a new link.
                        session.cancelled_pending_attaches.insert(detach.handle);
                    } else {
                        session
                            .pending_attach_events
                            .retain(|attach| attach.handle != detach.handle);
                    }
                    if !locally_closing {
                        writer
                            .write_amqp(
                                channel,
                                Performative::Detach(Detach {
                                    handle: detach.handle,
                                    closed: true,
                                    error: None,
                                }),
                                Vec::new(),
                            )
                            .await?;
                    }
                } else if let Some(mut link) = session.links.remove(&detach.handle) {
                    stop_link(&mut link);
                    if !locally_closing {
                        writer
                            .write_amqp(
                                channel,
                                Performative::Detach(Detach {
                                    handle: detach.handle,
                                    closed: true,
                                    error: None,
                                }),
                                Vec::new(),
                            )
                            .await?;
                    }
                }
            }
        }
        Performative::End(_) => {
            let mut acknowledge = true;
            if let Some(mut session) = sessions.remove(&channel) {
                acknowledge = !session.ending;
                for link in session.links.values_mut() {
                    stop_link(link);
                }
            }
            if acknowledge {
                writer
                    .write_amqp(channel, Performative::End(End::default()), Vec::new())
                    .await?;
            }
        }
        Performative::Close(_) => {
            if !locally_closing {
                writer
                    .write_amqp(0, Performative::Close(Close::default()), Vec::new())
                    .await?;
            }
            return Ok(FrameAction::Closed);
        }
        Performative::Open(_) => return Err(invalid_state("duplicate AMQP open")),
    }
    Ok(FrameAction::Continue)
}

enum CommandAction {
    Continue,
    Closing(oneshot::Sender<Result<(), EngineError>>),
}

async fn handle_command<W: AsyncWrite + Unpin>(
    command: Command,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::AcceptSession {
            channel,
            attach_tx,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(invalid_state("session begin is not pending")));
                return Ok(CommandAction::Continue);
            };
            if session.attach_tx.is_some() || session.ending {
                let _ = reply.send(Err(invalid_state("session channel is already open")));
                return Ok(CommandAction::Continue);
            }
            writer
                .write_amqp(
                    channel,
                    Performative::Begin(Begin {
                        remote_channel: Some(channel),
                        ..Begin::default()
                    }),
                    Vec::new(),
                )
                .await?;
            for attach in session.pending_attach_events.drain(..) {
                attach_tx
                    .try_send(attach)
                    .map_err(|_| invalid_state("pending attach queue is full"))?;
            }
            session.attach_tx = Some(attach_tx);
            let _ = reply.send(Ok(()));
        }
        Command::AcceptLink {
            channel,
            attach,
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            consumption,
            reply,
        } => {
            let attach = *attach;
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            let handle = attach.handle;
            if session.cancelled_pending_attaches.remove(&handle)
                || !session.pending_attaches.contains_key(&handle)
            {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            if session.ending
                || session.links.contains_key(&handle)
                || session.closing_handles.contains(&handle)
            {
                let _ = reply.send(Err(invalid_state(
                    "link handle is attached or awaiting detach acknowledgement",
                )));
                return Ok(CommandAction::Continue);
            }
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.max_message_size =
                (response.role == Role::Receiver).then_some(max_message_size);
            response.properties = properties;
            let response_frame = Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(Box::new(response.clone()))),
                payload: Vec::new(),
            };
            if let Err(error) = writer.encoded_frame(&response_frame) {
                response.source = None;
                response.target = None;
                response.properties = None;
                writer
                    .write_amqp(
                        channel,
                        Performative::Attach(Box::new(response)),
                        Vec::new(),
                    )
                    .await?;
                remember_closing_handle(session, handle)?;
                writer
                    .write_amqp(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: Some(Error::new(
                                crate::AmqpError::FrameSizeTooSmall,
                                "attach response exceeds the peer frame limit",
                                None,
                            )),
                        }),
                        Vec::new(),
                    )
                    .await?;
                let _ = reply.send(Err(error.into()));
                return Ok(CommandAction::Continue);
            }
            writer.write_frame(&response_frame).await?;
            match attach.role {
                Role::Sender => {
                    let Some(initial_count) = attach.initial_delivery_count else {
                        let _ = reply.send(Err(invalid_state(
                            "sender attach has no initial delivery count",
                        )));
                        refuse_session(
                            channel,
                            "amqp:invalid-field",
                            "sender attach has no initial delivery count",
                            writer,
                            sessions,
                        )
                        .await?;
                        return Ok(CommandAction::Continue);
                    };
                    session.links.insert(
                        handle,
                        LinkState::Receiving(ReceivingLink {
                            max_message_size: normalized_message_size(Some(max_message_size))
                                .unwrap_or(u64::MAX),
                            deliveries: deliveries_tx,
                            partial: None,
                            detached: detached_tx,
                            credit: ReceiveCredit::new(initial_count, LINK_CREDIT, consumption),
                        }),
                    );
                    refill_link(channel, handle, session, writer).await?;
                }
                Role::Receiver => {
                    let credit = session
                        .pending_attaches
                        .get(&handle)
                        .map(|pending| pending.credit.clone())
                        .unwrap_or_else(|| {
                            LinkCredit::new(response.initial_delivery_count.unwrap_or(0))
                        });
                    session.links.insert(
                        handle,
                        LinkState::Sending(Box::new(SendingLink {
                            max_message_size: normalized_message_size(attach.max_message_size),
                            settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                            credit,
                            queued: VecDeque::new(),
                            active: None,
                            unsettled: HashMap::new(),
                            pending_acknowledgements: HashSet::new(),
                            detached: detached_tx,
                        })),
                    );
                }
            }
            let pending_flow = session
                .pending_attaches
                .remove(&handle)
                .and_then(|pending| pending.latest);
            if let Some(flow) = pending_flow {
                apply_link_flow(channel, flow, writer, sessions).await?;
            }
            let _ = reply.send(Ok(()));
        }
        Command::Send {
            channel,
            handle,
            message,
            delivery_tag,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            queue_send(
                channel,
                handle,
                session,
                *message,
                delivery_tag,
                reply,
                writer,
                remote_max_frame_size,
            )
            .await?;
        }
        Command::Settle {
            channel,
            handle,
            delivery_id,
            state,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if !matches!(session.links.get(&handle), Some(LinkState::Receiving(_))) {
                let _ = reply.send(Err(invalid_state("settlement on an unknown link")));
                return Ok(CommandAction::Continue);
            }
            writer
                .write_amqp(
                    channel,
                    Performative::Disposition(Disposition {
                        role: Role::Receiver,
                        first: delivery_id,
                        last: None,
                        settled: true,
                        state: Some(state),
                        batchable: false,
                    }),
                    Vec::new(),
                )
                .await?;
            let _ = reply.send(Ok(()));
        }
        Command::SettleOutgoing {
            channel,
            handle,
            delivery_id,
            state,
            reply,
        } => {
            let Some(LinkState::Sending(link)) = sessions
                .get_mut(&channel)
                .and_then(|session| session.links.get_mut(&handle))
            else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if !link.pending_acknowledgements.remove(&delivery_id) {
                let _ = reply.send(Err(invalid_state("settlement is not pending")));
                return Ok(CommandAction::Continue);
            }
            writer
                .write_amqp(
                    channel,
                    Performative::Disposition(Disposition {
                        role: Role::Sender,
                        first: delivery_id,
                        last: None,
                        settled: true,
                        state: Some(state),
                        batchable: false,
                    }),
                    Vec::new(),
                )
                .await?;
            let _ = reply.send(Ok(()));
        }
        Command::Detach {
            channel,
            handle,
            error,
            reply,
        } => {
            if let Some(session) = sessions.get_mut(&channel)
                && let Some(mut link) = session.links.remove(&handle)
            {
                remember_closing_handle(session, handle)?;
                writer
                    .write_amqp(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error,
                        }),
                        Vec::new(),
                    )
                    .await?;
                stop_link(&mut link);
            }
            let _ = reply.send(Ok(()));
        }
        Command::Close { error, reply } => {
            writer
                .write_amqp(0, Performative::Close(Close { error }), Vec::new())
                .await?;
            return Ok(CommandAction::Closing(reply));
        }
    }
    Ok(CommandAction::Continue)
}

async fn receive_transfer<W: AsyncWrite + Unpin>(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("transfer on an unknown session"))?;
    if let Err(error) = session.flow.receive_transfer() {
        return refuse_session(
            channel,
            "amqp:session:window-violation",
            error.to_string(),
            writer,
            sessions,
        )
        .await;
    }
    if session.closing_handles.contains(&transfer.handle) {
        return refill_link(channel, transfer.handle, session, writer).await;
    }
    let Some(link) = session.links.get_mut(&transfer.handle) else {
        return refuse_session(
            channel,
            "amqp:session:unattached-handle",
            "transfer on an unknown link",
            writer,
            sessions,
        )
        .await;
    };
    let LinkState::Receiving(link) = link else {
        return refuse_session(
            channel,
            "amqp:session:errant-link",
            "transfer sent to a sending link",
            writer,
            sessions,
        )
        .await;
    };

    let identity_error = if transfer
        .delivery_tag
        .as_ref()
        .is_some_and(|tag| tag.len() > MAX_DELIVERY_TAG_BYTES)
    {
        Some(("amqp:invalid-field", "delivery tag exceeds 32 bytes"))
    } else if let Some(partial) = &link.partial {
        if transfer.delivery_id.is_some_and(|id| id != partial.id) {
            Some(("amqp:invalid-field", "continuation delivery id changed"))
        } else if transfer
            .delivery_tag
            .as_ref()
            .is_some_and(|tag| tag != &partial.tag)
        {
            Some(("amqp:invalid-field", "continuation delivery tag changed"))
        } else if transfer
            .message_format
            .is_some_and(|format| format != partial.message_format)
        {
            Some(("amqp:invalid-field", "continuation message format changed"))
        } else {
            None
        }
    } else if transfer.delivery_id.is_none() {
        Some(("amqp:invalid-field", "first transfer has no delivery id"))
    } else if transfer.delivery_tag.is_none() {
        Some(("amqp:invalid-field", "first transfer has no delivery tag"))
    } else if transfer.message_format.is_none() {
        Some(("amqp:invalid-field", "first transfer has no message format"))
    } else if transfer.message_format != Some(0) {
        Some(("amqp:not-implemented", "message format is not supported"))
    } else {
        None
    };
    if let Some((condition, description)) = identity_error {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            condition,
            description,
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }
    if link.partial.is_none()
        && let Err(error) = link.credit.try_begin_delivery()
    {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            "amqp:link:transfer-limit-exceeded",
            error.to_string(),
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }
    if transfer.aborted {
        link.partial = None;
        link.credit
            .abort_delivery()
            .map_err(|error| invalid_state(error.to_string()))?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }
    let message_bytes = u64::try_from(payload.len())
        .unwrap_or(u64::MAX)
        .saturating_add(
            link.partial
                .as_ref()
                .map(|partial| u64::try_from(partial.bytes.len()).unwrap_or(u64::MAX))
                .unwrap_or(0),
        );
    if message_bytes > link.max_message_size {
        let maximum_bytes = link.max_message_size;
        detach_oversized_link(
            channel,
            transfer.handle,
            session,
            writer,
            message_bytes,
            maximum_bytes,
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }
    let partial = match link.partial.take() {
        Some(mut partial) => {
            partial.bytes.extend_from_slice(&payload);
            partial.settled |= transfer.settled.unwrap_or(false);
            partial
        }
        None => PartialDelivery {
            id: transfer
                .delivery_id
                .ok_or_else(|| invalid_state("first transfer has no delivery id"))?,
            tag: transfer
                .delivery_tag
                .expect("first transfer tag was validated"),
            message_format: transfer
                .message_format
                .expect("first transfer format was validated"),
            settled: transfer.settled.unwrap_or(false),
            bytes: payload,
        },
    };
    if transfer.more {
        link.partial = Some(partial);
        return refill_link(channel, transfer.handle, session, writer).await;
    }

    let message = match decode_message(&partial.bytes) {
        Ok(message) => message,
        Err(error) => {
            detach_link_error(
                channel,
                transfer.handle,
                session,
                writer,
                "amqp:invalid-field",
                error.to_string(),
            )
            .await?;
            return refill_link(channel, transfer.handle, session, writer).await;
        }
    };
    if link
        .deliveries
        .try_send(Delivery {
            id: partial.id,
            settled: partial.settled,
            message,
        })
        .is_err()
    {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            "amqp:resource-limit-exceeded",
            "incoming delivery queue is unavailable",
        )
        .await?;
    }
    refill_link(channel, transfer.handle, session, writer).await
}

async fn refill_link<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let session_refilled = session.flow.refill_incoming();
    let link = match session.links.get_mut(&handle) {
        Some(LinkState::Receiving(link)) => link.credit.take_refill(),
        _ => None,
    };
    if let Some(link) = link {
        writer
            .write_amqp(
                channel,
                Performative::Flow(session.flow.snapshot().flow(
                    Some(handle),
                    Some(LinkSnapshot {
                        delivery_count: link.delivery_count,
                        link_credit: link.link_credit,
                        drain: false,
                    }),
                )),
                Vec::new(),
            )
            .await?;
    } else if session_refilled {
        writer
            .write_amqp(
                channel,
                Performative::Flow(session.flow.snapshot().flow(None, None)),
                Vec::new(),
            )
            .await?;
    }
    Ok(())
}

async fn refresh_consumed<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    for (&channel, session) in sessions {
        if session.ending {
            continue;
        }
        let handles: Vec<_> = session
            .links
            .iter_mut()
            .filter_map(|(&handle, link)| {
                let LinkState::Receiving(link) = link else {
                    return None;
                };
                Some(link.credit.apply_consumed().map(|_| handle))
            })
            .collect();
        for handle in handles {
            let handle = handle.map_err(|error| invalid_state(error.to_string()))?;
            refill_link(channel, handle, session, writer).await?;
        }
    }
    Ok(())
}

async fn refuse_session<W: AsyncWrite + Unpin>(
    channel: u16,
    condition: &str,
    description: impl Into<String>,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    if let Some(session) = sessions.get_mut(&channel) {
        if session.ending {
            return Ok(());
        }
        session.ending = true;
        session.attach_tx = None;
        session.pending_attaches.clear();
        session.cancelled_pending_attaches.clear();
        session.pending_attach_events.clear();
        for link in session.links.values_mut() {
            stop_link(link);
        }
        session.links.clear();
    }
    writer
        .write_amqp(
            channel,
            Performative::End(End {
                error: Some(Error::new(
                    crate::ErrorCondition::Custom(Symbol::from(condition)),
                    description,
                    None,
                )),
            }),
            Vec::new(),
        )
        .await?;
    Ok(())
}

async fn apply_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: Flow,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
    _remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&channel) else {
        return Err(invalid_state("flow on an unknown session"));
    };
    if session.ending {
        return Ok(());
    }
    if let Err(error) = session.flow.update_peer(
        flow.next_incoming_id,
        flow.incoming_window,
        flow.next_outgoing_id,
        flow.outgoing_window,
    ) {
        return refuse_session(
            channel,
            "amqp:session:window-violation",
            error.to_string(),
            writer,
            sessions,
        )
        .await;
    }
    if flow.handle.is_none() {
        if flow.delivery_count.is_some()
            || flow.link_credit.is_some()
            || flow.available.is_some()
            || flow.drain
            || flow.properties.is_some()
        {
            return refuse_session(
                channel,
                "amqp:invalid-field",
                "session-only flow has link fields",
                writer,
                sessions,
            )
            .await;
        }
        if flow.echo {
            writer
                .write_amqp(
                    channel,
                    Performative::Flow(session.flow.snapshot().flow(None, None)),
                    Vec::new(),
                )
                .await?;
        }
        return Ok(());
    }
    apply_link_flow(channel, flow, writer, sessions).await
}

async fn apply_link_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: Flow,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let Some(handle) = flow.handle else {
        return Ok(());
    };
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("flow on an unknown session"))?;
    if session.closing_handles.contains(&handle) {
        return Ok(());
    }
    let snapshot = match session.links.get_mut(&handle) {
        Some(LinkState::Sending(link)) => {
            if let Err(error) =
                link.credit
                    .update_peer_optional(flow.delivery_count, flow.link_credit, flow.drain)
            {
                return refuse_session(
                    channel,
                    "amqp:invalid-field",
                    error.to_string(),
                    writer,
                    sessions,
                )
                .await;
            }
            if link.active.is_none() && link.queued.is_empty() && link.credit.drain_requested() {
                let snapshot = match link.credit.drain_unused() {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        detach_link_error(
                            channel,
                            handle,
                            session,
                            writer,
                            "amqp:resource-limit-exceeded",
                            error.to_string(),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                if let Some(snapshot) = snapshot {
                    writer
                        .write_amqp(
                            channel,
                            Performative::Flow(
                                session.flow.snapshot().flow(Some(handle), Some(snapshot)),
                            ),
                            Vec::new(),
                        )
                        .await?;
                    return Ok(());
                }
            }
            link.credit.snapshot()
        }
        Some(LinkState::Receiving(link)) => {
            let Some(count) = flow.delivery_count else {
                return refuse_session(
                    channel,
                    "amqp:invalid-field",
                    "sender flow has no delivery count",
                    writer,
                    sessions,
                )
                .await;
            };
            if let Err(error) = link.credit.check_sender_count(count) {
                return refuse_session(
                    channel,
                    "amqp:invalid-field",
                    error.to_string(),
                    writer,
                    sessions,
                )
                .await;
            }
            if flow.drain {
                return refuse_session(
                    channel,
                    "amqp:invalid-field",
                    "unsolicited sender drain",
                    writer,
                    sessions,
                )
                .await;
            }
            let snapshot = link.credit.snapshot();
            LinkSnapshot {
                delivery_count: snapshot.delivery_count,
                link_credit: snapshot.link_credit,
                drain: false,
            }
        }
        None => {
            if let Some(pending) = session.pending_attaches.get_mut(&handle) {
                if let Err(error) = pending.update(flow) {
                    return refuse_session(
                        channel,
                        "amqp:invalid-field",
                        error.to_string(),
                        writer,
                        sessions,
                    )
                    .await;
                }
                return Ok(());
            }
            return refuse_session(
                channel,
                "amqp:session:unattached-handle",
                "flow on an unknown link",
                writer,
                sessions,
            )
            .await;
        }
    };
    if flow.echo {
        writer
            .write_amqp(
                channel,
                Performative::Flow(session.flow.snapshot().flow(Some(handle), Some(snapshot))),
                Vec::new(),
            )
            .await?;
    }
    Ok(())
}

fn normalized_message_size(maximum: Option<u64>) -> Option<u64> {
    maximum.filter(|maximum| *maximum != 0)
}

fn remember_closing_handle(session: &mut SessionState, handle: u32) -> Result<(), EngineError> {
    if session.closing_handles.len() >= MAX_CLOSING_HANDLES
        && !session.closing_handles.contains(&handle)
    {
        return Err(invalid_state(
            "too many links awaiting detach acknowledgement",
        ));
    }
    session.closing_handles.insert(handle);
    session.pending_attaches.remove(&handle);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn queue_send<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    message: Message,
    delivery_tag: DeliveryTag,
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    writer: &mut FrameWriter<W>,
    _remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let Some(LinkState::Sending(link)) = session.links.get(&handle) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    if delivery_tag.len() > MAX_DELIVERY_TAG_BYTES {
        let _ = reply.send(Err(invalid_state("delivery tag exceeds 32 bytes")));
        return Ok(());
    }
    let payload = match encode_message(&message) {
        Ok(payload) => payload,
        Err(error) => {
            let _ = reply.send(Err(error.into()));
            return Ok(());
        }
    };
    let message_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    if let Some(maximum_bytes) = link.max_message_size
        && message_bytes > maximum_bytes
    {
        detach_oversized_link(
            channel,
            handle,
            session,
            writer,
            message_bytes,
            maximum_bytes,
        )
        .await?;
        let _ = reply.send(Err(EngineError::MessageSizeExceeded {
            message_bytes,
            maximum_bytes,
        }));
        return Ok(());
    }
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        unreachable!("the validated sending link has not changed");
    };
    if link.queued.len() == DELIVERY_QUEUE_CAPACITY {
        let _ = reply.send(Err(invalid_state("outgoing delivery queue is full")));
        return Ok(());
    }
    if let Err(error) = fragment_frame(
        channel,
        handle,
        session.next_delivery_id,
        &delivery_tag,
        link.settle_mode == SenderSettleMode::Settled,
        &payload,
        0,
        false,
        writer,
    ) {
        detach_link_error(
            channel,
            handle,
            session,
            writer,
            "amqp:invalid-field",
            "delivery tag does not fit the peer frame limit",
        )
        .await?;
        let _ = reply.send(Err(error.into()));
        return Ok(());
    }
    link.queued.push_back(QueuedSend {
        payload,
        delivery_tag,
        reply,
    });
    Ok(())
}

async fn detach_oversized_link<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
    message_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), EngineError> {
    detach_link_error(channel, handle, session, writer, "amqp:link:message-size-exceeded", format!("the encoded message has {message_bytes} bytes, exceeding the link maximum of {maximum_bytes}")).await
}

async fn detach_link_error<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
    condition: &str,
    description: impl Into<String>,
) -> Result<(), EngineError> {
    let frame = Frame::Amqp {
        channel,
        performative: Some(Performative::Detach(Detach {
            handle,
            closed: true,
            error: Some(Error::new(
                crate::ErrorCondition::Custom(Symbol::from(condition)),
                description,
                None,
            )),
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&frame)?;
    remember_closing_handle(session, handle)?;
    if let Some(mut link) = session.links.remove(&handle) {
        stop_link(&mut link);
    }
    writer.write_frame(&frame).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn fragment_frame<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    delivery_id: u32,
    delivery_tag: &DeliveryTag,
    settled: bool,
    payload: &[u8],
    offset: usize,
    first_frame_sent: bool,
    writer: &FrameWriter<W>,
) -> io::Result<(Frame, usize, bool)> {
    let mut transfer = Transfer {
        handle,
        delivery_id: (!first_frame_sent).then_some(delivery_id),
        delivery_tag: (!first_frame_sent).then_some(delivery_tag.clone()),
        message_format: (!first_frame_sent).then_some(0),
        settled: (!first_frame_sent).then_some(settled),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    };
    let header = |transfer: &Transfer| Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer.clone())),
        payload: Vec::new(),
    };
    let final_header = writer.encoded_frame(&header(&transfer))?.len();
    let maximum = writer.maximum_frame_size() as usize;
    let remaining = payload.len() - offset;
    let available = maximum - final_header;
    let complete = remaining <= available;
    let length = if complete {
        remaining
    } else {
        transfer.more = true;
        maximum - writer.encoded_frame(&header(&transfer))?.len()
    };
    if length == 0 && remaining != 0 && first_frame_sent {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer frame limit cannot carry a continuation payload",
        ));
    }
    let next_offset = offset + length;
    let frame = Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer)),
        payload: payload[offset..next_offset].to_vec(),
    };
    writer.encoded_frame(&frame)?;
    Ok((frame, next_offset, complete))
}

fn delivery_id_in_use(session: &SessionState, id: u32) -> bool {
    session.links.values().any(|link| matches!(link, LinkState::Sending(link) if link.unsettled.contains_key(&id) || link.pending_acknowledgements.contains(&id) || link.active.as_ref().is_some_and(|active| active.delivery_id == id)))
}

fn can_pump(session: &SessionState, link: &SendingLink) -> bool {
    !session.ending
        && session.flow.outgoing_allowance() != 0
        && (link.active.is_some()
            || (!link.queued.is_empty()
                && link.credit.allowance() != 0
                && !delivery_id_in_use(session, session.next_delivery_id)))
}

async fn pump_connection<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
    cursor: &mut usize,
) -> Result<bool, EngineError> {
    for _ in 0..SEND_FRAME_QUANTUM {
        let mut ready: Vec<_> = sessions
            .iter()
            .flat_map(|(&channel, session)| {
                session.links.iter().filter_map(move |(&handle, link)| {
                    let LinkState::Sending(link) = link else {
                        return None;
                    };
                    can_pump(session, link).then_some((channel, handle))
                })
            })
            .collect();
        ready.sort_unstable();
        if ready.is_empty() {
            break;
        }
        let (channel, handle) = ready[*cursor % ready.len()];
        *cursor = cursor.wrapping_add(1);
        let session = sessions.get_mut(&channel).expect("ready session exists");
        send_fragment(channel, handle, session, writer).await?;
    }
    for (&channel, session) in sessions.iter_mut() {
        if session.ending {
            continue;
        }
        let handles: Vec<_> = session.links.iter().filter_map(|(&handle, link)| matches!(link, LinkState::Sending(link) if link.active.is_none() && link.queued.is_empty() && link.credit.drain_requested()).then_some(handle)).collect();
        for handle in handles {
            let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
                continue;
            };
            match link.credit.drain_unused() {
                Ok(Some(snapshot)) => {
                    writer
                        .write_amqp(
                            channel,
                            Performative::Flow(
                                session.flow.snapshot().flow(Some(handle), Some(snapshot)),
                            ),
                            Vec::new(),
                        )
                        .await?
                }
                Ok(None) => {}
                Err(error) => {
                    detach_link_error(
                        channel,
                        handle,
                        session,
                        writer,
                        "amqp:resource-limit-exceeded",
                        error.to_string(),
                    )
                    .await?
                }
            }
        }
    }
    Ok(sessions.values().any(|session| {
        session
            .links
            .values()
            .any(|link| matches!(link, LinkState::Sending(link) if can_pump(session, link)))
    }))
}

async fn send_fragment<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let Some(LinkState::Sending(link)) = session.links.get(&handle) else {
        return Ok(());
    };
    let starting = link.active.is_none();
    let (frame, offset, complete) = if let Some(active) = &link.active {
        fragment_frame(
            channel,
            handle,
            active.delivery_id,
            &active.delivery_tag,
            active.settled,
            &active.payload,
            active.offset,
            active.first_frame_sent,
            writer,
        )?
    } else {
        let Some(queued) = link.queued.front() else {
            return Ok(());
        };
        fragment_frame(
            channel,
            handle,
            session.next_delivery_id,
            &queued.delivery_tag,
            link.settle_mode == SenderSettleMode::Settled,
            &queued.payload,
            0,
            false,
            writer,
        )?
    };
    if !session
        .flow
        .try_send_transfer()
        .map_err(|error| invalid_state(error.to_string()))?
    {
        return Ok(());
    }
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        unreachable!("ready link exists");
    };
    if starting {
        if !link
            .credit
            .try_begin_delivery()
            .map_err(|error| invalid_state(error.to_string()))?
        {
            return Err(invalid_state(
                "delivery lost link credit before its first frame",
            ));
        }
        let queued = link.queued.pop_front().expect("queued delivery exists");
        let id = session.next_delivery_id;
        session.next_delivery_id = id.wrapping_add(1);
        let settled = link.settle_mode == SenderSettleMode::Settled;
        let settled_reply = if settled {
            Some(queued.reply)
        } else {
            link.unsettled.insert(
                id,
                OutgoingDelivery {
                    reply: queued.reply,
                    outcome: None,
                },
            );
            None
        };
        link.active = Some(ActiveSend {
            payload: queued.payload,
            offset: 0,
            first_frame_sent: false,
            delivery_id: id,
            delivery_tag: queued.delivery_tag,
            settled,
            settled_reply,
        });
    }
    writer.write_frame(&frame).await?;
    let active = link.active.as_mut().expect("active delivery exists");
    active.offset = offset;
    active.first_frame_sent = true;
    if complete {
        let active = link.active.take().expect("completed delivery exists");
        if let Some(reply) = active.settled_reply {
            let _ = reply.send(Ok(SendOutcome {
                outcome: Outcome::Accepted(Accepted),
                delivery_id: None,
            }));
        } else {
            resolve_outgoing(link, active.delivery_id);
        }
    }
    Ok(())
}

fn resolve_outgoing(link: &mut SendingLink, id: u32) {
    if link
        .active
        .as_ref()
        .is_some_and(|active| active.delivery_id == id)
    {
        return;
    }
    if !link
        .unsettled
        .get(&id)
        .is_some_and(|delivery| delivery.outcome.is_some())
    {
        return;
    }
    let delivery = link.unsettled.remove(&id).expect("latched outcome exists");
    let (outcome, acknowledge) = delivery.outcome.expect("latched outcome exists");
    if acknowledge {
        link.pending_acknowledgements.insert(id);
    }
    let _ = delivery.reply.send(Ok(SendOutcome {
        outcome,
        delivery_id: acknowledge.then_some(id),
    }));
}

async fn apply_disposition<W: AsyncWrite + Unpin>(
    channel: u16,
    disposition: Disposition,
    _writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    if disposition.role != Role::Receiver {
        return Ok(());
    }
    let Some(state) = disposition.state else {
        return Ok(());
    };
    let Ok(outcome) = Outcome::try_from(state) else {
        return Ok(());
    };
    let last = disposition.last.unwrap_or(disposition.first);
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    for link in session.links.values_mut() {
        let LinkState::Sending(link) = link else {
            continue;
        };
        let ids: Vec<_> = link
            .unsettled
            .keys()
            .copied()
            .filter(|id| id.wrapping_sub(disposition.first) <= last.wrapping_sub(disposition.first))
            .collect();
        for id in ids {
            if let Some(delivery) = link.unsettled.get_mut(&id) {
                let acknowledge =
                    link.receiver_settle_mode == ReceiverSettleMode::Second && !disposition.settled;
                if delivery.outcome.is_none() {
                    delivery.outcome = Some((outcome.clone(), acknowledge));
                }
                resolve_outgoing(link, id);
            }
        }
    }
    Ok(())
}

fn stop_link(link: &mut LinkState) {
    match link {
        LinkState::Sending(link) => {
            let _ = link.detached.send(true);
            for queued in link.queued.drain(..) {
                let _ = queued.reply.send(Err(EngineError::RemoteDetached));
            }
            if let Some(active) = link.active.take()
                && let Some(reply) = active.settled_reply
            {
                let _ = reply.send(Err(EngineError::RemoteDetached));
            }
            for (_, delivery) in link.unsettled.drain() {
                let _ = delivery.reply.send(Err(EngineError::RemoteDetached));
            }
        }
        LinkState::Receiving(link) => {
            let _ = link.detached.send(true);
        }
    }
}

#[cfg(test)]
async fn write_amqp<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    performative: Performative,
    payload: Vec<u8>,
) -> Result<(), EngineError> {
    write_frame(
        writer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload,
        },
    )
    .await?;
    Ok(())
}

fn invalid_state(message: impl Into<String>) -> EngineError {
    EngineError::InvalidState(message.into())
}

#[cfg(feature = "test-client")]
mod client;

#[cfg(feature = "test-client")]
pub use client::{ClientConnection, ClientDelivery, ClientReceiver, ClientSender, ClientSession};

#[cfg(test)]
mod tests {
    use serde_amqp::primitives::Binary;

    use super::*;
    use crate::{AmqpError, Source, Target};

    #[test]
    fn a_link_role_selects_the_local_endpoint() {
        assert_eq!(Role::Sender.opposite(), Role::Receiver);
        assert_eq!(Role::Receiver.opposite(), Role::Sender);
    }

    #[test]
    fn protocol_errors_remain_link_scoped_values() {
        let error = Error::new(AmqpError::InvalidField, "bad link", None);
        assert_eq!(error.condition, AmqpError::InvalidField.into());
    }

    #[test]
    fn delivery_tags_are_binary_and_exact() {
        let tag = Binary::from(vec![0, 1, 2, 3]);
        assert_eq!(tag.as_slice(), &[0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn link_credit_arriving_during_attach_is_applied_when_the_link_is_accepted() {
        let channel = 3;
        let handle = 1;
        let (attach_tx, _attaches) = mpsc::channel(1);
        let mut session = SessionState::new(&Begin::default());
        session.attach_tx = Some(attach_tx);
        session
            .pending_attaches
            .insert(handle, PendingLinkFlow::new(Role::Receiver, None));
        let mut sessions = HashMap::from([(channel, session)]);
        let (wire, _peer) = tokio::io::duplex(64 * 1024);
        let mut wire = FrameWriter::new(wire, u32::MAX).expect("frame writer");

        apply_flow(
            channel,
            Flow {
                next_incoming_id: Some(0),
                incoming_window: SESSION_WINDOW,
                outgoing_window: SESSION_WINDOW,
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(50),
                ..Flow::default()
            },
            &mut wire,
            &mut sessions,
            u32::MAX,
        )
        .await
        .expect("an early flow is buffered");
        assert!(
            sessions[&channel].pending_attaches[&handle]
                .latest
                .is_some()
        );

        let (deliveries_tx, _deliveries) = mpsc::channel(1);
        let (detached_tx, _detached) = watch::channel(false);
        let (reply, response) = oneshot::channel();
        handle_command(
            Command::AcceptLink {
                channel,
                attach: Box::new(Attach {
                    name: String::from("response"),
                    handle,
                    role: Role::Receiver,
                    snd_settle_mode: SenderSettleMode::Settled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: Some(Source::new("node")),
                    target: Some(Target::new("reply-to")),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: None,
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }),
                max_message_size: 1024,
                properties: None,
                deliveries_tx,
                detached_tx,
                consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
                reply,
            },
            &mut wire,
            &mut sessions,
            u32::MAX,
        )
        .await
        .expect("the link is accepted");
        response
            .await
            .expect("the accept reply remains live")
            .expect("the attach is valid");

        assert!(sessions[&channel].pending_attaches.is_empty());
        let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
            panic!("the remote receiver created a local sending link");
        };
        assert_eq!(link.credit.allowance(), 50);
    }
}

#[cfg(test)]
mod message_size_tests;

#[cfg(test)]
mod lifecycle_tests;

#[cfg(test)]
mod frame_limit_tests;

#[cfg(test)]
mod flow_tests;

#[cfg(test)]
mod transfer_identity_tests;
