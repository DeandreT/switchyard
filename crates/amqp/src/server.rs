use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::Arc,
    time::Duration,
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Notify, mpsc, oneshot, watch},
};

use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Fields, Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode,
    Transfer, encode_message, read_frame_with_max_size, read_protocol_header, write_frame,
    write_protocol_header,
};

#[cfg(test)]
use crate::{decode_message, read_frame};

mod flow_control;
mod format_registry;
mod frame_writer;
mod idle;
mod incoming_ledger;
mod outgoing_identity;
mod receive_credit;
mod session_identity;

use flow_control::{LinkCredit, LinkSnapshot, SessionWindow};
pub use format_registry::MessageFormatDecoders;
use frame_writer::FrameWriter;
pub use idle::ConnectionOptions;
use idle::{Activity, ActivityTimeout, validate_idle_timeout};
#[cfg(test)]
use incoming_ledger::Completion;
use incoming_ledger::{
    DeliveryIdentity, IncomingLedger, IncomingLedgerError, LinkIdentity, SettlementAction,
};
use outgoing_identity::AckIdentity;
use receive_credit::{Consumption, ReceiveCredit};
pub use session_identity::IncomingAttach;
use session_identity::{AttachApproval, AttachApprovalError, SessionIdentity};

const LINK_CREDIT: u32 = 32;
const SESSION_WINDOW: u32 = 2_048;
const DELIVERY_QUEUE_CAPACITY: usize = LINK_CREDIT as usize;
const MAX_PENDING_ATTACHES: usize = 32;
const SEND_FRAME_QUANTUM: usize = 16;
const MAX_DELIVERY_TAG_BYTES: usize = 32;
const RECOVERY_NOT_IMPLEMENTED: &str = "link recovery is not implemented";
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

async fn negotiation_header<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: ProtocolHeader,
    options: ConnectionOptions,
) -> Result<(), EngineError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(options.write_limit())
        .ok_or_else(|| invalid_state("write timeout cannot be represented by the clock"))?;
    tokio::time::timeout_at(deadline, async {
        write_protocol_header(writer, header).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| EngineError::Timeout("write"))??;
    if tokio::time::Instant::now() >= deadline {
        return Err(EngineError::Timeout("write"));
    }
    Ok(())
}

async fn negotiation_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    options: ConnectionOptions,
) -> Result<(), EngineError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(options.write_limit())
        .ok_or_else(|| invalid_state("write timeout cannot be represented by the clock"))?;
    tokio::time::timeout_at(deadline, async {
        write_frame(writer, frame).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| EngineError::Timeout("write"))??;
    if tokio::time::Instant::now() >= deadline {
        return Err(EngineError::Timeout("write"));
    }
    Ok(())
}

async fn peer_idle_timeout<W: AsyncWrite + Unpin>(
    writer: &mut W,
    advertised: Option<u32>,
    maximum_frame_size: u32,
    options: ConnectionOptions,
) -> Result<u32, EngineError> {
    let millis = advertised.unwrap_or(0);
    if let Err(error) = validate_idle_timeout(millis) {
        let mut writer = FrameWriter::new(writer, maximum_frame_size)?;
        writer.configure_activity(options, 0, Activity::new());
        let _ = writer
            .write_amqp(
                0,
                Performative::Close(Close {
                    error: Some(Error::new(
                        crate::AmqpError::InvalidField,
                        "positive idle-time-out below 1000 milliseconds is unsupported",
                        None,
                    )),
                }),
                Vec::new(),
            )
            .await;
        return Err(error);
    }
    Ok(millis)
}

fn validate_activity_frame(frame: &Frame, channel_max: u16) -> io::Result<()> {
    match frame {
        Frame::Amqp { channel, .. } if *channel > channel_max => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ChannelLimitError {
                channel: *channel,
                maximum: channel_max,
            },
        )),
        Frame::Amqp {
            channel,
            performative,
            payload,
        } if *channel <= channel_max && (performative.is_some() || payload.is_empty()) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid post-Open AMQP frame or channel",
        )),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("AMQP frame channel {channel} exceeds the advertised channel maximum {maximum}")]
struct ChannelLimitError {
    channel: u16,
    maximum: u16,
}

async fn idle_close<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    reason: ActivityTimeout,
) -> io::Result<()> {
    writer
        .write_amqp(
            0,
            Performative::Close(Close {
                error: Some(Error::new(
                    crate::ErrorCondition::Custom(Symbol::from("amqp:connection:forced")),
                    reason.description(),
                    None,
                )),
            }),
            Vec::new(),
        )
        .await
}

async fn notify_framing_error<W: AsyncWrite + Unpin>(
    writer: &mut FrameWriter<W>,
    error: &io::Error,
) {
    let Some(cause) = error.get_ref() else {
        return;
    };
    if cause
        .downcast_ref::<crate::codec::FrameSizeError>()
        .is_none()
        && cause.downcast_ref::<ChannelLimitError>().is_none()
    {
        return;
    }
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
    #[error("the remote peer settled the delivery without reporting an outcome")]
    RemoteSettledWithoutOutcome,
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

#[derive(Clone, Copy)]
struct ConnectionSettings {
    remote_max_frame_size: u32,
    local_max_frame_size: u32,
    channel_max: u16,
    remote_channel_max: u16,
    options: ConnectionOptions,
    peer_idle_millis: u32,
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
    identity: SessionIdentity,
    pub begin: Begin,
}

pub struct ServerSession {
    channel: u16,
    identity: SessionIdentity,
    commands: mpsc::Sender<Command>,
    incoming_attaches: mpsc::Receiver<IncomingAttach>,
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
    identity: LinkIdentity,
}

/// A receiver's outcome whose second-mode acknowledgement is still pending.
pub struct PendingSettlement {
    outcome: Outcome,
    identity: LinkIdentity,
    acknowledgement: Option<AckIdentity>,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
}

impl PendingSettlement {
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub async fn accept(&self) -> Result<(), EngineError> {
        self.finish(DeliveryState::Accepted(Accepted)).await
    }

    pub async fn reject(&self, error: Error) -> Result<(), EngineError> {
        self.finish(DeliveryState::Rejected(crate::Rejected {
            error: Some(error),
        }))
        .await
    }

    async fn finish(&self, state: DeliveryState) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::SettleOutgoing {
            channel: self.channel,
            handle: self.handle,
            owner: self.identity.clone(),
            identity: self.acknowledgement.clone(),
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
    identity: LinkIdentity,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    #[cfg(test)]
    id: u32,
    #[cfg(test)]
    settled: bool,
    message_format: u32,
    message: Message,
    identity: DeliveryIdentity,
}

impl Delivery {
    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn message_format(&self) -> u32 {
        self.message_format
    }
}

impl ServerConnection {
    pub async fn accept<Io>(
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::accept_with_options(stream, container_id, sasl, ConnectionOptions::default()).await
    }

    pub async fn accept_with_options<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        options.validate()?;
        let local_max_frame_size = normalized_frame_size(DEFAULT_MAX_FRAME_SIZE)?;
        let mut local_open = Open {
            max_frame_size: local_max_frame_size,
            idle_time_out: Some(options.advertised_idle_timeout()),
            ..Open::new(container_id)
        };
        checked_open_frame(local_open.clone())?;
        if let Some(authenticator) = sasl {
            expect_header(&mut stream, ProtocolHeader::SASL).await?;
            negotiation_header(&mut stream, ProtocolHeader::SASL, options).await?;
            negotiation_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
                    mechanisms: authenticator.mechanisms(),
                })),
                options,
            )
            .await?;
            let init = match read_frame_with_max_size(&mut stream, local_max_frame_size).await? {
                Frame::Sasl(SaslPerformative::Init(init)) => init,
                _ => return Err(invalid_state("expected SASL init")),
            };
            let code = authenticator.authenticate(&init);
            negotiation_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Outcome(SaslOutcome {
                    code: code.clone(),
                    additional_data: None,
                })),
                options,
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
        negotiation_header(&mut stream, ProtocolHeader::AMQP, options).await?;
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
        let channel_max = local_open.channel_max;
        negotiation_frame(&mut stream, &checked_open_frame(local_open)?, options).await?;
        let peer_idle_millis = peer_idle_timeout(
            &mut stream,
            remote_open.idle_time_out,
            remote_max_frame_size,
            options,
        )
        .await?;

        let (commands, command_rx) = mpsc::channel(256);
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        let consumed = Arc::new(Notify::new());
        let driver_consumed = consumed.clone();
        let (lifecycle, cancellation, terminated) = ConnectionLifecycle::new();
        tokio::spawn(async move {
            run_connection(
                stream,
                ConnectionSettings {
                    remote_max_frame_size,
                    local_max_frame_size,
                    channel_max,
                    remote_channel_max: remote_open.channel_max,
                    options,
                    peer_idle_millis,
                },
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
        while let Some(incoming) = self.incoming_sessions.recv().await {
            if !incoming.identity.is_retired() {
                return Some(incoming);
            }
        }
        None
    }

    pub async fn accept_session(
        &self,
        incoming: IncomingSession,
    ) -> Result<ServerSession, EngineError> {
        if incoming.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        let (attach_tx, incoming_attaches) = mpsc::channel(32);
        request(&self.commands, |reply| Command::AcceptSession {
            channel: incoming.channel,
            identity: incoming.identity.clone(),
            attach_tx,
            reply,
        })
        .await?;
        Ok(ServerSession {
            channel: incoming.channel,
            identity: incoming.identity,
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
    pub async fn next_incoming_attach(&mut self) -> Option<IncomingAttach> {
        while let Some(attach) = self.incoming_attaches.recv().await {
            if !self.identity.is_retired() && !attach.approval().link_identity().is_retired() {
                return Some(attach);
            }
        }
        None
    }

    pub async fn accept_attach(
        &self,
        attach: IncomingAttach,
        max_message_size: u64,
    ) -> Result<LinkEndpoint, EngineError> {
        self.accept_attach_with_properties(attach, max_message_size, None)
            .await
    }

    pub async fn accept_attach_with_properties(
        &self,
        attach: IncomingAttach,
        max_message_size: u64,
        properties: Option<Fields>,
    ) -> Result<LinkEndpoint, EngineError> {
        self.accept_attach_with_decoders(
            attach,
            max_message_size,
            properties,
            MessageFormatDecoders::default(),
        )
        .await
    }

    pub async fn accept_attach_with_decoders(
        &self,
        attach: IncomingAttach,
        max_message_size: u64,
        properties: Option<Fields>,
        decoders: MessageFormatDecoders,
    ) -> Result<LinkEndpoint, EngineError> {
        attach
            .validate_request(&self.identity)
            .map_err(attach_approval_error)?;
        if has_recovery_state(&attach) {
            return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
        }
        source_default_outcome(attach.source.as_ref())?;
        if attach.role == Role::Receiver && !decoders.is_default() {
            return Err(invalid_state(
                "custom message-format decoders require a local receiving endpoint",
            ));
        }
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let identity = attach.approval().link_identity().clone();
        let (detached_tx, detached) = watch::channel(false);
        let role = attach.role.clone();
        let name = attach.name.clone();
        let max_message_size_for_sender = normalized_message_size(attach.max_message_size);
        let handle = attach.handle;
        request(&self.commands, |reply| Command::AcceptLink {
            channel: self.channel,
            session: self.identity.clone(),
            attach: Box::new(attach),
            max_message_size,
            properties,
            decoders,
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
                identity,
            }),
            Role::Receiver => LinkEndpoint::Sender(Sender {
                name,
                max_message_size: max_message_size_for_sender,
                channel: self.channel,
                handle,
                commands: self.commands.clone(),
                detached,
                identity,
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
                identity: self.identity.clone(),
                message: Box::new(message),
                delivery_tag,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        let outcome = outcome.await.map_err(|_| EngineError::Stopped)??;
        Ok(PendingSettlement {
            outcome: outcome.outcome,
            identity: self.identity.clone(),
            acknowledgement: outcome.acknowledgement,
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
        if self.identity.is_retired() {
            return Ok(());
        }
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            identity: self.identity.clone(),
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
        if !delivery.identity.belongs_to(&self.identity) {
            return Err(invalid_state(
                "delivery belongs to a different receiving link generation",
            ));
        }
        request(&self.commands, |reply| Command::Settle {
            channel: self.channel,
            handle: self.handle,
            identity: delivery.identity.clone(),
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
        if self.identity.is_retired() {
            return Ok(());
        }
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            identity: self.identity.clone(),
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
        identity: SessionIdentity,
        attach_tx: mpsc::Sender<IncomingAttach>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    AcceptLink {
        channel: u16,
        session: SessionIdentity,
        attach: Box<IncomingAttach>,
        max_message_size: u64,
        properties: Option<Fields>,
        decoders: MessageFormatDecoders,
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        consumption: Arc<Consumption>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        identity: LinkIdentity,
        message: Box<Message>,
        delivery_tag: DeliveryTag,
        reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        identity: DeliveryIdentity,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    SettleOutgoing {
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        identity: Option<AckIdentity>,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        identity: LinkIdentity,
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
    identity: SessionIdentity,
    attach_tx: Option<mpsc::Sender<IncomingAttach>>,
    local_begin_sent: bool,
    links: HashMap<u32, LinkState>,
    closing_handles: HashSet<u32>,
    pending_attaches: HashMap<u32, PendingLinkFlow>,
    pending_attach_events: VecDeque<IncomingAttach>,
    flow: SessionWindow,
    next_delivery_id: u32,
    ending: bool,
    incoming: IncomingLedger,
}

struct PendingLinkFlow {
    approval: Option<Arc<AttachApproval>>,
    peer_role: Role,
    initial_sender_count: Option<u32>,
    credit: LinkCredit,
    latest: Option<Flow>,
    recovery_refusal: bool,
}

impl PendingLinkFlow {
    fn new(peer_role: Role, initial_sender_count: Option<u32>) -> Self {
        Self {
            approval: None,
            peer_role,
            initial_sender_count,
            credit: LinkCredit::new(0),
            latest: None,
            recovery_refusal: false,
        }
    }

    fn incoming(attach: &IncomingAttach) -> Self {
        let mut pending = Self::new(attach.role.clone(), attach.initial_delivery_count);
        pending.approval = Some(attach.approval().clone());
        pending
    }

    fn retire(&self) {
        if let Some(approval) = &self.approval {
            approval.retire();
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
            identity: SessionIdentity::new(),
            attach_tx: None,
            local_begin_sent: false,
            links: HashMap::new(),
            closing_handles: HashSet::new(),
            pending_attaches: HashMap::new(),
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
            incoming: IncomingLedger::new(),
        }
    }
}

enum LinkState {
    Sending(Box<SendingLink>),
    Receiving(ReceivingLink),
}

impl LinkState {
    fn identity(&self) -> &LinkIdentity {
        match self {
            Self::Sending(link) => &link.identity,
            Self::Receiving(link) => &link.identity,
        }
    }
}

struct SendingLink {
    identity: LinkIdentity,
    auto_acknowledge: bool,
    max_message_size: Option<u64>,
    receiver_settle_mode: ReceiverSettleMode,
    default_outcome: Option<Outcome>,
    settle_mode: SenderSettleMode,
    credit: LinkCredit,
    queued: VecDeque<QueuedSend>,
    active: Option<ActiveSend>,
    unsettled: HashMap<u32, OutgoingDelivery>,
    pending_acknowledgements: HashMap<u32, AckIdentity>,
    detached: watch::Sender<bool>,
}

struct QueuedSend {
    payload: Vec<u8>,
    delivery_tag: DeliveryTag,
    message_format: u32,
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
}

struct ActiveSend {
    payload: Vec<u8>,
    offset: usize,
    first_frame_sent: bool,
    delivery_id: u32,
    delivery_tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    settled_reply: Option<oneshot::Sender<Result<SendOutcome, EngineError>>>,
}

struct OutgoingDelivery {
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    outcome: Option<Outcome>,
    receiver_settled: bool,
}

struct SendOutcome {
    outcome: Outcome,
    acknowledgement: Option<AckIdentity>,
}

struct ReceivingLink {
    max_message_size: u64,
    deliveries: mpsc::Sender<Delivery>,
    partial: Option<PartialDelivery>,
    detached: watch::Sender<bool>,
    credit: ReceiveCredit,
    decoders: MessageFormatDecoders,
    identity: LinkIdentity,
    sender_settle_mode: SenderSettleMode,
    receiver_settle_mode: ReceiverSettleMode,
}

struct PartialDelivery {
    id: u32,
    tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    bytes: Vec<u8>,
    identity: DeliveryIdentity,
    forbidden_receiver_mode: bool,
    forbidden_sender_settled: bool,
}

async fn run_connection<Io>(
    stream: Io,
    settings: ConnectionSettings,
    mut commands: mpsc::Receiver<Command>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    consumed: Arc<Notify>,
    mut cancellation: watch::Receiver<bool>,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let remote_max_frame_size = settings.remote_max_frame_size;
    let activity = Activity::configured(settings.options);
    let (mut reader, writer) = tokio::io::split(stream);
    let Ok(mut writer) = FrameWriter::new(writer, remote_max_frame_size) else {
        return;
    };
    writer.configure_activity(
        settings.options,
        settings.peer_idle_millis,
        activity.clone(),
    );
    let (frames_tx, mut frames) = mpsc::channel(256);
    let reader_activity = activity.clone();
    let mut reader_task = ConnectionReader(Some(tokio::spawn(async move {
        loop {
            let frame = read_frame_with_max_size(&mut reader, settings.local_max_frame_size)
                .await
                .and_then(|frame| {
                    validate_activity_frame(&frame, settings.channel_max)?;
                    if !reader_activity.received_frame() {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "receive idle deadline expired before the complete frame",
                        ));
                    }
                    Ok(frame)
                });
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
    loop {
        let stopped = {
            let processing = async {
                loop {
                    if activity.heartbeat_is_due(settings.peer_idle_millis)
                        && writer
                            .write_frame(&Frame::Amqp {
                                channel: 0,
                                performative: None,
                                payload: Vec::new(),
                            })
                            .await
                            .is_err()
                    {
                        break;
                    }
                    tokio::select! {
                        () = activity.heartbeat_due(settings.peer_idle_millis), if !activity.is_closing() => {
                            if writer.write_frame(&Frame::Amqp {
                                channel: 0,
                                performative: None,
                                payload: Vec::new(),
                            }).await.is_err() {
                                break;
                            }
                        }
                        frame = frames.recv() => {
                            let Some(frame) = frame else { break };
                            let frame = match frame {
                                Ok(frame) => frame,
                                Err(error) => {
                                    if !activity.is_closing() {
                                        notify_framing_error(&mut writer, &error).await;
                                    }
                                    break;
                                }
                            };
                            if activity.is_closing()
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
                                activity.is_closing(),
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
                                Err(error) => {
                                    tracing::debug!(%error, "AMQP server frame handling failed");
                                    break;
                                }
                            }
                        }
                        command = commands.recv() => {
                            let Some(command) = command else { break };
                            let command = match command {
                                Command::Close { reply, .. } if activity.is_closing() => {
                                    closing_replies.push(reply);
                                    continue;
                                }
                                command if activity.is_closing() => {
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
                                Err(error) => {
                                    tracing::debug!(%error, "AMQP server command handling failed");
                                    break;
                                }
                            }
                        }
                        () = consumed.notified(), if !activity.is_closing() => {
                            if refresh_consumed(&mut writer, &mut sessions).await.is_err() {
                                break;
                            }
                            pump_ready = true;
                        }
                        () = tokio::task::yield_now(), if pump_ready && !activity.is_closing() => {
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
                () = wait_for_detach(&mut cancellation) => None,
                reason = activity.timeout(settings.options, settings.peer_idle_millis) => Some(reason),
                () = processing => None,
            }
        };
        if let Some(reason @ (ActivityTimeout::Receive | ActivityTimeout::Peer)) = stopped
            && !activity.is_tainted()
            && !activity.is_closing()
        {
            let closed = tokio::select! {
                biased;
                () = wait_for_detach(&mut cancellation) => false,
                result = idle_close(&mut writer, reason) => result.is_ok(),
            };
            if closed {
                pump_ready = false;
                continue;
            }
        }
        break;
    }

    reader_task.shutdown().await;

    for session in sessions.values_mut() {
        stop_session(session);
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
                .try_send(IncomingSession {
                    channel,
                    identity: sessions[&channel].identity.clone(),
                    begin,
                })
                .is_err()
            {
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
            } else if session.pending_attaches.len() == MAX_PENDING_ATTACHES {
                refuse_session(
                    channel,
                    "amqp:resource-limit-exceeded",
                    "pending attach limit reached",
                    writer,
                    sessions,
                )
                .await?;
            } else if has_recovery_state(&attach) {
                if session.attach_tx.is_some() {
                    refuse_recovery_attach(channel, &attach, session, writer).await?;
                } else {
                    let attach = IncomingAttach::new(*attach, session.identity.clone());
                    let mut pending = PendingLinkFlow::incoming(&attach);
                    pending.recovery_refusal = true;
                    session.pending_attaches.insert(handle, pending);
                    session.pending_attach_events.push_back(attach);
                }
            } else {
                let attach = IncomingAttach::new(*attach, session.identity.clone());
                session
                    .pending_attaches
                    .insert(handle, PendingLinkFlow::incoming(&attach));
                if let Some(attach_tx) = &session.attach_tx {
                    if attach_tx.try_send(attach).is_err() {
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
                    session.pending_attach_events.push_back(attach);
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
                if let Some(pending) = session.pending_attaches.remove(&detach.handle) {
                    pending.retire();
                    session
                        .pending_attach_events
                        .retain(|attach| attach.handle != detach.handle);
                    if !locally_closing {
                        ensure_local_begin(channel, session, writer).await?;
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
                    forget_incoming_link(&mut session.incoming, &link);
                    stop_link(&mut link);
                    if !locally_closing {
                        ensure_local_begin(channel, session, writer).await?;
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
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("end on an unknown session"))?;
            let acknowledge = !session.ending;
            if acknowledge {
                ensure_local_begin(channel, session, writer).await?;
            }
            if let Some(mut session) = sessions.remove(&channel) {
                stop_session(&mut session);
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
            identity,
            attach_tx,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if identity.is_retired() || session.ending || session.identity.is_retired() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            if !session.identity.same_session(&identity) {
                let _ = reply.send(Err(invalid_state(
                    "session approval belongs to a different generation",
                )));
                return Ok(CommandAction::Continue);
            }
            if session.attach_tx.is_some() {
                let _ = reply.send(Err(invalid_state("session channel is already open")));
                return Ok(CommandAction::Continue);
            }
            ensure_local_begin(channel, session, writer).await?;
            for attach in std::mem::take(&mut session.pending_attach_events) {
                if attach.approval().link_identity().is_retired() {
                    continue;
                }
                if has_recovery_state(&attach) {
                    refuse_recovery_attach(channel, &attach, session, writer).await?;
                } else {
                    attach_tx
                        .try_send(attach)
                        .map_err(|_| invalid_state("pending attach queue is full"))?;
                }
            }
            session.attach_tx = Some(attach_tx);
            let _ = reply.send(Ok(()));
        }
        Command::AcceptLink {
            channel,
            session: owner,
            attach,
            max_message_size,
            properties,
            decoders,
            deliveries_tx,
            detached_tx,
            consumption,
            reply,
        } => {
            let attach = *attach;
            if owner.is_retired() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if session.ending || session.identity.is_retired() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            if !session.identity.same_session(&owner) {
                let _ = reply.send(Err(invalid_state(
                    "attach approval belongs to a different session generation",
                )));
                return Ok(CommandAction::Continue);
            }
            if let Err(error) = attach.validate_request(&owner) {
                let _ = reply.send(Err(attach_approval_error(error)));
                return Ok(CommandAction::Continue);
            }
            let handle = attach.handle;
            let Some(approval) = session
                .pending_attaches
                .get(&handle)
                .and_then(|pending| pending.approval.as_ref())
            else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if let Err(error) = attach.validate(&session.identity, approval) {
                let _ = reply.send(Err(attach_approval_error(error)));
                return Ok(CommandAction::Continue);
            }
            if has_recovery_state(&attach) {
                let _ = reply.send(Err(invalid_state(RECOVERY_NOT_IMPLEMENTED)));
                return Ok(CommandAction::Continue);
            }
            if attach.role == Role::Receiver && !decoders.is_default() {
                let _ = reply.send(Err(invalid_state(
                    "custom message-format decoders require a local receiving endpoint",
                )));
                return Ok(CommandAction::Continue);
            }
            if session.pending_attaches[&handle].recovery_refusal {
                let _ = reply.send(Err(invalid_state(RECOVERY_NOT_IMPLEMENTED)));
                return Ok(CommandAction::Continue);
            }
            let (attach, approval) = attach.into_parts();
            let identity = approval.link_identity().clone();
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
            let default_outcome = match source_default_outcome(response.source.as_ref()) {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(CommandAction::Continue);
                }
            };
            let response_frame = Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(Box::new(response.clone()))),
                payload: Vec::new(),
            };
            if let Err(error) = writer.encoded_frame(&response_frame) {
                response.source = None;
                response.target = None;
                response.properties = None;
                ensure_local_begin(channel, session, writer).await?;
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
            ensure_local_begin(channel, session, writer).await?;
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
                            decoders,
                            identity,
                            sender_settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
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
                            identity,
                            auto_acknowledge: false,
                            max_message_size: normalized_message_size(attach.max_message_size),
                            settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                            default_outcome,
                            credit,
                            queued: VecDeque::new(),
                            active: None,
                            unsettled: HashMap::new(),
                            pending_acknowledgements: HashMap::new(),
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
            identity,
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
                &identity,
                *message,
                delivery_tag,
                0,
                reply,
                writer,
                remote_max_frame_size,
            )
            .await?;
        }
        Command::Settle {
            channel,
            handle,
            identity,
            state,
            reply,
        } => {
            settle_incoming(channel, handle, identity, state, reply, sessions, writer).await?;
        }
        Command::SettleOutgoing {
            channel,
            handle,
            owner,
            identity,
            state,
            reply,
        } => {
            settle_outgoing(
                channel, handle, owner, identity, state, reply, sessions, writer,
            )
            .await?;
        }
        Command::Detach {
            channel,
            handle,
            identity,
            error,
            reply,
        } => {
            if identity.is_retired() {
                let _ = reply.send(Ok(()));
                return Ok(CommandAction::Continue);
            }
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            let Some(link) = session.links.get(&handle) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if !link.identity().same_link(&identity) {
                let _ = reply.send(Err(invalid_state(
                    "close belongs to a different link generation",
                )));
                return Ok(CommandAction::Continue);
            }
            if session.ending {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            let frame = Frame::Amqp {
                channel,
                performative: Some(Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error,
                })),
                payload: Vec::new(),
            };
            if let Err(error) = writer.encoded_frame(&frame) {
                let _ = reply.send(Err(error.into()));
                return Ok(CommandAction::Continue);
            }
            ensure_local_begin(channel, session, writer).await?;
            remember_closing_handle(session, handle)?;
            let mut link = session
                .links
                .remove(&handle)
                .expect("validated close endpoint");
            forget_incoming_link(&mut session.incoming, &link);
            stop_link(&mut link);
            writer.write_frame(&frame).await?;
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

async fn settle_incoming<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    identity: DeliveryIdentity,
    state: DeliveryState,
    reply: oneshot::Sender<Result<(), EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&channel) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    let Some(LinkState::Receiving(link)) = session.links.get(&handle) else {
        let _ = reply.send(Err(invalid_state(
            "settlement on an unknown receiving link",
        )));
        return Ok(());
    };
    let owner = link.identity.clone();
    let action = match session.incoming.settlement(&owner, &identity) {
        Ok(action) => action,
        Err(error) => {
            let _ = reply.send(Err(invalid_state(error.to_string())));
            return Ok(());
        }
    };
    if let SettlementAction::SendDisposition { settled } = action {
        let frame = Frame::Amqp {
            channel,
            performative: Some(Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: identity.id(),
                last: None,
                settled,
                state: Some(state),
                batchable: false,
            })),
            payload: Vec::new(),
        };
        if let Err(error) = writer.encoded_frame(&frame) {
            let _ = reply.send(Err(error.into()));
            return Ok(());
        }
        writer.write_frame(&frame).await?;
    }
    session
        .incoming
        .commit_settlement(&owner, &identity)
        .map_err(|error| invalid_state(error.to_string()))?;
    let _ = reply.send(Ok(()));
    Ok(())
}

fn outgoing_acknowledgement(
    channel: u16,
    link: &SendingLink,
    owner: &LinkIdentity,
    identity: Option<&AckIdentity>,
    state: Option<DeliveryState>,
) -> Result<Option<Frame>, EngineError> {
    if owner.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    if !link.identity.same_link(owner) {
        return Err(invalid_state(
            "acknowledgement belongs to a different sending link generation",
        ));
    }
    let Some(identity) = identity else {
        return Ok(None);
    };
    identity
        .validate_owner(owner)
        .map_err(|error| invalid_state(error.to_string()))?;
    if identity.is_settled() {
        return Ok(None);
    }
    if !link
        .pending_acknowledgements
        .get(&identity.id())
        .is_some_and(|pending| pending.same_ack(identity))
    {
        return Err(invalid_state(
            "acknowledgement is not pending for this delivery generation",
        ));
    }
    Ok(Some(Frame::Amqp {
        channel,
        performative: Some(Performative::Disposition(Disposition {
            role: Role::Sender,
            first: identity.id(),
            last: None,
            settled: true,
            state,
            batchable: false,
        })),
        payload: Vec::new(),
    }))
}

async fn write_outgoing_acknowledgement<W: AsyncWrite + Unpin>(
    link: &mut SendingLink,
    identity: &AckIdentity,
    frame: &Frame,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    writer.write_frame(frame).await?;
    let pending = link
        .pending_acknowledgements
        .remove(&identity.id())
        .expect("the actor retains the validated acknowledgement through its write");
    debug_assert!(pending.same_ack(identity));
    identity.mark_settled();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn settle_outgoing<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    owner: LinkIdentity,
    identity: Option<AckIdentity>,
    state: DeliveryState,
    reply: oneshot::Sender<Result<(), EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if owner.is_retired() {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    }
    let Some(session) = sessions.get_mut(&channel) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    if session.ending || session.closing_handles.contains(&handle) {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    }
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    let frame =
        match outgoing_acknowledgement(channel, link, &owner, identity.as_ref(), Some(state)) {
            Ok(frame) => frame,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
    if let Some(frame) = frame {
        if let Err(error) = writer.encoded_frame(&frame) {
            let _ = reply.send(Err(error.into()));
            return Ok(());
        }
        write_outgoing_acknowledgement(
            link,
            identity
                .as_ref()
                .expect("a pending acknowledgement produced the frame"),
            &frame,
            writer,
        )
        .await?;
    }
    let _ = reply.send(Ok(()));
    Ok(())
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

    if transfer.resume {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            "amqp:not-implemented",
            RECOVERY_NOT_IMPLEMENTED,
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }

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
    } else if transfer
        .message_format
        .is_some_and(|format| link.decoders.decoder(format).is_none())
    {
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
    let identity = if let Some(partial) = &link.partial {
        partial.identity.clone()
    } else {
        let result = session.incoming.reserve(
            &link.identity,
            transfer
                .delivery_id
                .expect("first transfer id was validated"),
            transfer
                .delivery_tag
                .as_ref()
                .expect("first transfer tag was validated"),
        );
        match result {
            Ok(identity) => identity,
            Err(error) => {
                let condition = match error {
                    IncomingLedgerError::LinkLimitReached { .. }
                    | IncomingLedgerError::SessionLimitReached { .. } => {
                        "amqp:resource-limit-exceeded"
                    }
                    _ => "amqp:invalid-field",
                };
                detach_link_error(
                    channel,
                    transfer.handle,
                    session,
                    writer,
                    condition,
                    error.to_string(),
                )
                .await?;
                return refill_link(channel, transfer.handle, session, writer).await;
            }
        }
    };
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
        session
            .incoming
            .abort(&identity)
            .map_err(|error| invalid_state(error.to_string()))?;
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
            partial.forbidden_receiver_mode |= link.receiver_settle_mode
                == ReceiverSettleMode::First
                && transfer.rcv_settle_mode == Some(ReceiverSettleMode::Second);
            partial.forbidden_sender_settled |= link.sender_settle_mode
                == SenderSettleMode::Unsettled
                && transfer.settled == Some(true);
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
            identity,
            forbidden_receiver_mode: link.receiver_settle_mode == ReceiverSettleMode::First
                && transfer.rcv_settle_mode == Some(ReceiverSettleMode::Second),
            forbidden_sender_settled: link.sender_settle_mode == SenderSettleMode::Unsettled
                && transfer.settled == Some(true),
        },
    };
    if transfer.more {
        link.partial = Some(partial);
        return refill_link(channel, transfer.handle, session, writer).await;
    }

    let mode_error = if partial.forbidden_sender_settled {
        Some("settled transfer violates the negotiated unsettled sender mode")
    } else if link.sender_settle_mode == SenderSettleMode::Settled && !partial.settled {
        Some("delivery has no settled transfer on a settled sender link")
    } else if partial.forbidden_receiver_mode
        && !partial.settled
        && !session
            .incoming
            .sender_is_settled(&partial.identity)
            .map_err(|error| invalid_state(error.to_string()))?
    {
        Some("second receiver mode is not permitted on a first-mode link")
    } else {
        None
    };
    if let Some(description) = mode_error {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            "amqp:invalid-field",
            description,
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }

    let decoder = link
        .decoders
        .decoder(partial.message_format)
        .expect("first transfer format was approved");
    let message = match decoder(&partial.bytes) {
        Ok(message) => message,
        Err(error) => {
            let description = if partial.message_format == 0 {
                error.to_string()
            } else {
                String::from("registered message-format decoder rejected the payload")
            };
            detach_link_error(
                channel,
                transfer.handle,
                session,
                writer,
                "amqp:invalid-field",
                description,
            )
            .await?;
            return refill_link(channel, transfer.handle, session, writer).await;
        }
    };
    let _completion = session
        .incoming
        .complete(
            &partial.identity,
            partial.settled,
            transfer
                .rcv_settle_mode
                .unwrap_or_else(|| link.receiver_settle_mode.clone()),
        )
        .map_err(|error| invalid_state(error.to_string()))?;
    if link
        .deliveries
        .try_send(Delivery {
            #[cfg(test)]
            id: partial.id,
            #[cfg(test)]
            settled: _completion == Completion::SenderSettled,
            message_format: partial.message_format,
            message,
            identity: partial.identity,
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
        _ if session.closing_handles.contains(&handle) => None,
        Some(LinkState::Receiving(link)) => link.credit.take_refill(),
        _ => None,
    };
    if let Some(link) = link {
        ensure_local_begin(channel, session, writer).await?;
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
        ensure_local_begin(channel, session, writer).await?;
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

async fn ensure_local_begin<W: AsyncWrite + Unpin>(
    channel: u16,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if !session.local_begin_sent {
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
        session.local_begin_sent = true;
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
    let description = description.into();
    tracing::debug!(channel, condition, %description, "refusing AMQP session");
    if let Some(session) = sessions.get_mut(&channel) {
        if session.ending {
            return Ok(());
        }
        ensure_local_begin(channel, session, writer).await?;
        session.ending = true;
        stop_session(session);
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
            ensure_local_begin(channel, session, writer).await?;
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
                    ensure_local_begin(channel, session, writer).await?;
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
        ensure_local_begin(channel, session, writer).await?;
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

fn has_recovery_state(attach: &Attach) -> bool {
    attach.incomplete_unsettled
        || attach
            .unsettled
            .as_ref()
            .is_some_and(|unsettled| !unsettled.is_empty())
}

async fn refuse_recovery_attach<W: AsyncWrite + Unpin>(
    channel: u16,
    attach: &Attach,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let response = Frame::Amqp {
        channel,
        performative: Some(Performative::Attach(Box::new(attach.response(None, None)))),
        payload: Vec::new(),
    };
    let refusal = Frame::Amqp {
        channel,
        performative: Some(Performative::Detach(Detach {
            handle: attach.handle,
            closed: true,
            error: Some(Error::new(
                crate::AmqpError::NotImplemented,
                RECOVERY_NOT_IMPLEMENTED,
                None,
            )),
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&response)?;
    writer.encoded_frame(&refusal)?;
    ensure_local_begin(channel, session, writer).await?;
    remember_closing_handle(session, attach.handle)?;
    writer.write_frame(&response).await?;
    writer.write_frame(&refusal).await?;
    Ok(())
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
    if let Some(pending) = session.pending_attaches.remove(&handle) {
        pending.retire();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn queue_send<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    identity: &LinkIdentity,
    message: Message,
    delivery_tag: DeliveryTag,
    message_format: u32,
    reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    writer: &mut FrameWriter<W>,
    _remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    if identity.is_retired() || session.ending || session.closing_handles.contains(&handle) {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    }
    let Some(LinkState::Sending(link)) = session.links.get(&handle) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    if !link.identity.same_link(identity) {
        let _ = reply.send(Err(invalid_state(
            "send belongs to a different link generation",
        )));
        return Ok(());
    }
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
        message_format,
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
        message_format,
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
    ensure_local_begin(channel, session, writer).await?;
    remember_closing_handle(session, handle)?;
    if let Some(mut link) = session.links.remove(&handle) {
        forget_incoming_link(&mut session.incoming, &link);
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
    message_format: u32,
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
        message_format: (!first_frame_sent).then_some(message_format),
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
    session.links.values().any(|link| matches!(link, LinkState::Sending(link) if link.unsettled.contains_key(&id) || link.pending_acknowledgements.contains_key(&id) || link.active.as_ref().is_some_and(|active| active.delivery_id == id)))
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
            active.message_format,
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
            queued.message_format,
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
                    receiver_settled: false,
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
            message_format: queued.message_format,
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
                acknowledgement: None,
            }));
        } else {
            resolve_outgoing(channel, link, active.delivery_id, writer).await?;
        }
    }
    Ok(())
}

async fn resolve_outgoing<W: AsyncWrite + Unpin>(
    channel: u16,
    link: &mut SendingLink,
    id: u32,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if link.identity.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    if link
        .active
        .as_ref()
        .is_some_and(|active| active.delivery_id == id)
    {
        return Ok(());
    }
    if !link
        .unsettled
        .get(&id)
        .is_some_and(|delivery| delivery.outcome.is_some() || delivery.receiver_settled)
    {
        return Ok(());
    }
    let delivery = link
        .unsettled
        .remove(&id)
        .expect("resolvable delivery exists");
    let Some(outcome) = delivery.outcome.or_else(|| link.default_outcome.clone()) else {
        let _ = delivery
            .reply
            .send(Err(EngineError::RemoteSettledWithoutOutcome));
        return Ok(());
    };
    let acknowledge =
        link.receiver_settle_mode == ReceiverSettleMode::Second && !delivery.receiver_settled;
    let mut acknowledgement = acknowledge.then(|| AckIdentity::new(&link.identity, id));
    if let Some(identity) = &acknowledgement {
        link.pending_acknowledgements.insert(id, identity.clone());
        if link.auto_acknowledge {
            let frame =
                outgoing_acknowledgement(channel, link, &link.identity, Some(identity), None)?
                    .expect("a fresh pending acknowledgement requires a frame");
            writer.encoded_frame(&frame)?;
            write_outgoing_acknowledgement(link, identity, &frame, writer).await?;
            acknowledgement = None;
        }
    }
    let _ = delivery.reply.send(Ok(SendOutcome {
        outcome,
        acknowledgement,
    }));
    Ok(())
}

async fn apply_disposition<W: AsyncWrite + Unpin>(
    channel: u16,
    disposition: Disposition,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    if disposition.role == Role::Sender {
        if disposition.settled
            && let Some(session) = sessions.get_mut(&channel)
        {
            session
                .incoming
                .sender_settled_range(disposition.first, disposition.last);
        }
        return Ok(());
    }
    let outcome = disposition
        .state
        .and_then(|state| Outcome::try_from(state).ok());
    let last = disposition.last.unwrap_or(disposition.first);
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    if session.ending || session.identity.is_retired() {
        return Ok(());
    }
    for link in session.links.values_mut() {
        let LinkState::Sending(link) = link else {
            continue;
        };
        if link.identity.is_retired() {
            continue;
        }
        if disposition.settled {
            let acknowledgements: Vec<_> = link
                .pending_acknowledgements
                .iter()
                .filter(|(id, identity)| {
                    **id == identity.id()
                        && id.wrapping_sub(disposition.first)
                            <= last.wrapping_sub(disposition.first)
                        && identity.validate_owner(&link.identity).is_ok()
                })
                .map(|(&id, identity)| (id, identity.clone()))
                .collect();
            for (id, identity) in acknowledgements {
                if link
                    .pending_acknowledgements
                    .get(&id)
                    .is_some_and(|pending| pending.same_ack(&identity))
                {
                    identity.mark_settled();
                    link.pending_acknowledgements.remove(&id);
                }
            }
        }
        let ids: Vec<_> = link
            .unsettled
            .keys()
            .copied()
            .filter(|id| id.wrapping_sub(disposition.first) <= last.wrapping_sub(disposition.first))
            .collect();
        for id in ids {
            if let Some(delivery) = link.unsettled.get_mut(&id) {
                delivery.receiver_settled |= disposition.settled;
                if delivery.outcome.is_none() {
                    delivery.outcome = outcome.clone();
                }
                resolve_outgoing(channel, link, id, writer).await?;
            }
        }
    }
    Ok(())
}

fn forget_incoming_link(incoming: &mut IncomingLedger, link: &LinkState) {
    if let LinkState::Receiving(link) = link {
        incoming.remove_link(&link.identity);
    }
}

fn stop_link(link: &mut LinkState) {
    match link {
        LinkState::Sending(link) => {
            link.identity.retire();
            link.pending_acknowledgements.clear();
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
            link.identity.retire();
            let _ = link.detached.send(true);
        }
    }
}

fn stop_session(session: &mut SessionState) {
    session.identity.retire();
    session.attach_tx = None;
    for pending in session.pending_attaches.values() {
        pending.retire();
    }
    session.pending_attaches.clear();
    session.pending_attach_events.clear();
    for link in session.links.values_mut() {
        stop_link(link);
    }
    session.links.clear();
    session.incoming = IncomingLedger::new();
}

fn attach_approval_error(error: AttachApprovalError) -> EngineError {
    match error {
        AttachApprovalError::RetiredSession | AttachApprovalError::RetiredApproval => {
            EngineError::RemoteDetached
        }
        _ => invalid_state(error.to_string()),
    }
}

fn source_default_outcome(source: Option<&crate::Source>) -> Result<Option<Outcome>, EngineError> {
    source
        .and_then(|source| source.default_outcome.clone())
        .map(|state| {
            Outcome::try_from(state)
                .map_err(|_| invalid_state("source default outcome must be terminal"))
        })
        .transpose()
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
        session.local_begin_sent = true;
        session.attach_tx = Some(attach_tx);
        let receipt = IncomingAttach::new(
            Attach {
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
            },
            session.identity.clone(),
        );
        session
            .pending_attaches
            .insert(handle, PendingLinkFlow::incoming(&receipt));
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
                session: sessions[&channel].identity.clone(),
                attach: Box::new(receipt),
                max_message_size: 1024,
                properties: None,
                decoders: MessageFormatDecoders::default(),
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

#[cfg(test)]
mod idle_tests;

#[cfg(test)]
mod format_registry_tests;

#[cfg(test)]
mod incoming_settlement_tests;

#[cfg(test)]
mod recovery_tests;

#[cfg(test)]
mod session_startup_tests;

#[cfg(test)]
mod outgoing_settlement_tests;

#[cfg(test)]
mod session_provenance_tests;

#[cfg(test)]
mod outgoing_remote_settlement_tests;
