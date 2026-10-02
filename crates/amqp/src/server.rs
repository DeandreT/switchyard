use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    io,
    sync::Arc,
    time::Duration,
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Notify, mpsc, oneshot, watch},
};

use crate::codec::prepare_message;
use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Fields, Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode,
    Transfer, read_frame_with_max_size, read_protocol_header, write_frame, write_protocol_header,
};

#[cfg(test)]
use crate::{decode_message, encode_message, read_frame};

mod connection_identity;
mod content_budget;
mod error_deliveries;
mod error_links;
mod flow_control;
mod format_registry;
mod frame_writer;
mod idle;
mod incoming_ledger;
mod link_handles;
mod native_transactions;
mod outgoing_delivery_identity;
mod outgoing_identity;
mod receive_credit;
mod retained_delivery;
mod sender_identity;
mod session_channels;
mod session_identity;
mod transactional_sender;
mod transactions;

use connection_identity::ConnectionActorExit;
pub use connection_identity::NativeConnectionIdentity;
use content_budget::ContentLease;
use error_deliveries::{
    ErrorDeliveryHistory, ErrorDeliveryHistoryError, MAX_RETIRED_DELIVERIES_PER_DIRECTION,
};
use error_links::ErrorPeerHandles;
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
use link_handles::{
    HandleAlias, connection_link_name_in_use, current_alias, is_error_detached,
    local_handle_for_peer, mark_error_detached, preferred_vacant_handle,
};
pub use native_transactions::{
    CoordinatorEndpoint, CoordinatorRequest, MAX_NATIVE_TRANSACTION_CONTROL_BYTES,
    MAX_NATIVE_TRANSACTION_POSTINGS, MAX_NATIVE_TRANSACTIONS, NativeClaim,
    NativeControllerIdentity, NativeDeclarationRefusal, NativeFault, NativePreparedWork,
    NativeReadySubmission, NativeReadyTicket, NativeReceiverIdentity, NativeTransactionDecision,
    NativeTransactionError, NativeTransactionIdentity, NativeTransactionResources,
    NativeTransactionState, PendingDeclareReceipt, PreparedPosting, PreparedRetirement,
    SealedDischargeReceipt, TransactionPostingReceipt, TransactionRetirementReceipt,
    TransactionalIngress, TransactionalReceiver,
};
use native_transactions::{NativeIngressPolicy, NativeTransactionBook};
pub use outgoing_delivery_identity::NativeOutgoingDeliveryIdentity;
use outgoing_identity::AckIdentity;
use receive_credit::{Consumption, ReceiveCredit};
pub use retained_delivery::RetainedDelivery;
pub use sender_identity::NativeSenderIdentity;
use session_channels::{local_channel_for_peer, preferred_vacant_channel};
pub use session_identity::IncomingAttach;
use session_identity::{AttachApproval, AttachApprovalError, SessionIdentity};
use transactional_sender::{
    OutgoingReply, apply_native_outgoing_disposition, reconcile_native_retirements,
};
pub use transactional_sender::{SentDelivery, TransactionalDisposition, TransactionalSender};
use transactions::{
    TRANSACTIONS_NOT_IMPLEMENTED, attach_uses_transactions, refuse_transaction_flow,
    source_uses_transactions, transaction_state,
};

const LINK_CREDIT: u32 = 32;
const SESSION_WINDOW: u32 = 2_048;
const DELIVERY_QUEUE_CAPACITY: usize = LINK_CREDIT as usize;
const MAX_QUEUED_FRAMES: usize = 16;
const MAX_PENDING_ATTACHES: usize = 32;
const SEND_FRAME_QUANTUM: usize = 16;
const MAX_DELIVERY_TAG_BYTES: usize = 32;
const MAX_OUTGOING_DELIVERIES_PER_LINK: usize = 1_024;
const MAX_OUTGOING_DELIVERIES_PER_SESSION: usize = 4_096;
const MAX_SESSIONS_PER_CONNECTION: usize = 32;
const MAX_LINKS_PER_SESSION: usize = 128;
const MAX_LINKS_PER_CONNECTION: usize = 256;
const RECOVERY_NOT_IMPLEMENTED: &str = "link recovery is not implemented";
const MAX_CLOSING_HANDLES: usize = 65_536;
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_MAX_FRAME_SIZE: u32 = 262_144;
const MIN_MAX_FRAME_SIZE: u32 = 512;
const MAX_RECEIVED_MESSAGE_BYTES: u64 = crate::codec::MAX_FRAME_SIZE as u64;

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
    identity: NativeConnectionIdentity,
}

impl ConnectionLifecycle {
    fn new() -> (Self, watch::Receiver<bool>, ConnectionActorExit) {
        let (cancellation, cancelled) = watch::channel(false);
        let (terminated_tx, terminated) = watch::channel(false);
        let identity = NativeConnectionIdentity::new();
        let actor_exit = ConnectionActorExit::new(identity.clone(), terminated_tx);
        (
            Self {
                cancellation,
                terminated,
                identity,
            },
            cancelled,
            actor_exit,
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
    delivery_identity: NativeOutgoingDeliveryIdentity,
    acknowledgement: Option<AckIdentity>,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
}

impl PendingSettlement {
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Observes the original delivery generation. This may already be a
    /// transport-terminal delivery; the observer grants no settlement authority.
    pub fn delivery_identity(&self) -> &NativeOutgoingDeliveryIdentity {
        &self.delivery_identity
    }

    /// Tests active exact sending-link origin, independent of the outcome or
    /// pending acknowledgement. This is not delivery or settlement authority.
    pub fn belongs_to_sender(&self, sender: &NativeSenderIdentity) -> bool {
        sender.matches_active_origin(&self.identity)
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
    content_lease: Option<Arc<ContentLease>>,
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
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::accept_inner(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::Disabled,
        )
        .await
    }

    /// Enables the trusted native transactional-ingress API on this connection.
    /// This does not enable Service Bus transactions or a persistence adapter.
    pub async fn accept_with_transactional_ingress<Io>(
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::accept_inner(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::Posting,
        )
        .await
    }

    /// Enables trusted native transactional postings and outgoing retirements.
    /// This does not enable Service Bus transactions or a persistence adapter.
    pub async fn accept_with_transactional_work<Io>(
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::accept_inner(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::PostingAndRetirement,
        )
        .await
    }

    /// Enables native transactional work with a narrow coordinator Attach default.
    /// An omitted sender initial-delivery-count is accepted as zero only on a
    /// fresh coordinator. Other native acceptance APIs retain their strict policy.
    /// This interoperability exception does not enable SDK transaction scopes.
    pub async fn accept_with_transactional_work_defaults<Io>(
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::accept_inner(
            stream,
            container_id,
            sasl,
            options,
            NativeIngressPolicy::WorkDefaults,
        )
        .await
    }

    async fn accept_inner<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
        options: ConnectionOptions,
        native_policy: NativeIngressPolicy,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        options.validate()?;
        let local_max_frame_size = normalized_frame_size(DEFAULT_MAX_FRAME_SIZE)?;
        let local_open = Open {
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
        let (lifecycle, cancellation, actor_exit) = ConnectionLifecycle::new();
        tokio::spawn(async move {
            let exit_guard = actor_exit;
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
                exit_guard.identity(),
                native_policy,
                command_rx,
                incoming_session_tx,
                driver_consumed,
                cancellation,
            )
            .await;
            drop(exit_guard);
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

    /// Observes this connection's provenance and native actor lifetime.
    pub fn connection_identity(&self) -> &NativeConnectionIdentity {
        &self.lifecycle.identity
    }

    pub async fn next_incoming_session(&mut self) -> Option<IncomingSession> {
        while let Some(incoming) = self.incoming_sessions.recv().await {
            if !incoming.identity.is_retired() {
                return Some(incoming);
            }
        }
        None
    }

    /// Returns an owned admission future. Creating or dropping it before its
    /// first poll performs no I/O; dropping it after enqueue does not cancel
    /// native session acceptance already in progress.
    pub fn accept_session(
        &self,
        incoming: IncomingSession,
    ) -> impl Future<Output = Result<ServerSession, EngineError>> + Send + 'static + use<> {
        let commands = self.commands.clone();
        let consumed = self.consumed.clone();
        async move {
            if incoming.identity.is_retired() {
                return Err(EngineError::RemoteDetached);
            }
            let (attach_tx, incoming_attaches) = mpsc::channel(32);
            request(&commands, |reply| Command::AcceptSession {
                channel: incoming.channel,
                identity: incoming.identity.clone(),
                attach_tx,
                reply,
            })
            .await?;
            Ok(ServerSession {
                channel: incoming.channel,
                identity: incoming.identity,
                commands,
                incoming_attaches,
                consumed,
            })
        }
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
        native_transactions::validate_accept_kind(
            &attach,
            native_transactions::NativeAttachKind::Ordinary,
        )?;
        if has_recovery_state(&attach) {
            return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
        }
        if attach_uses_transactions(&attach) {
            return Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED));
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
        let handle = attach.approval().local_handle();
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

    /// Observes this accepted sending-link origin without retaining transport.
    pub fn sender_identity(&self) -> NativeSenderIdentity {
        NativeSenderIdentity::for_accepted_sender(&self.identity)
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
            Outcome::Declared(_) => return Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED)),
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
            delivery_identity: outcome.delivery_identity,
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
        let mut delivery = self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        drop(delivery.content_lease.take());
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
    NativeTransactions(native_transactions::NativeCommand),
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
        Command::NativeTransactions(command) => command.reject(EngineError::RemoteClosed),
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
    peer_channel: Option<u16>,
    remote_handle_max: u32,
    handle_aliases: HashMap<u32, HandleAlias>,
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
    error_deliveries: ErrorDeliveryHistory,
    error_peer_handles: ErrorPeerHandles,
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
    #[cfg(test)]
    fn new(peer: &Begin) -> Self {
        Self::with_identity(peer, SessionIdentity::new())
    }

    fn for_connection(peer: &Begin, connection: &NativeConnectionIdentity) -> Self {
        Self::with_identity(peer, SessionIdentity::for_connection(connection))
    }

    fn with_identity(peer: &Begin, identity: SessionIdentity) -> Self {
        Self {
            identity,
            peer_channel: None,
            remote_handle_max: peer.handle_max,
            handle_aliases: HashMap::new(),
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
            error_deliveries: ErrorDeliveryHistory::default(),
            error_peer_handles: ErrorPeerHandles::default(),
        }
    }
}

fn link_slot_count(session: &SessionState) -> usize {
    session.handle_aliases.len()
        + session
            .links
            .keys()
            .filter(|handle| !session.handle_aliases.contains_key(*handle))
            .count()
        + session
            .pending_attaches
            .keys()
            .filter(|handle| {
                !session.handle_aliases.contains_key(*handle)
                    && !session.links.contains_key(*handle)
            })
            .count()
        + session
            .closing_handles
            .iter()
            .filter(|handle| {
                !session.handle_aliases.contains_key(*handle)
                    && !session.links.contains_key(*handle)
                    && !session.pending_attaches.contains_key(*handle)
            })
            .count()
}

fn connection_link_slot_count(sessions: &HashMap<u16, SessionState>) -> usize {
    sessions.values().fold(0, |count, session| {
        count.saturating_add(link_slot_count(session))
    })
}

enum LinkState {
    Sending(Box<SendingLink>),
    Receiving(Box<ReceivingLink>),
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
    outstanding_tags: HashSet<Vec<u8>>,
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
    content_lease: ContentLease,
    delivery_tag: DeliveryTag,
    message_format: u32,
    reply: OutgoingReply,
}

struct ActiveSend {
    payload: Vec<u8>,
    content_lease: ContentLease,
    offset: usize,
    first_frame_sent: bool,
    delivery_id: u32,
    delivery_identity: NativeOutgoingDeliveryIdentity,
    delivery_tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    settled_reply: Option<OutgoingReply>,
}

struct OutgoingDelivery {
    reply: OutgoingReply,
    delivery_identity: NativeOutgoingDeliveryIdentity,
    delivery_tag: DeliveryTag,
    outcome: Option<Outcome>,
    receiver_settled: bool,
    retirement: Option<native_transactions::NativeRetirementAttempt>,
}

struct SendOutcome {
    outcome: Outcome,
    delivery_identity: NativeOutgoingDeliveryIdentity,
    acknowledgement: Option<AckIdentity>,
}

enum ReceivingSink {
    Ordinary(mpsc::Sender<Delivery>),
    Coordinator(mpsc::Sender<native_transactions::CoordinatorRequest>),
    Transactional(mpsc::Sender<native_transactions::TransactionalIngress>),
}

impl From<mpsc::Sender<Delivery>> for ReceivingSink {
    fn from(deliveries: mpsc::Sender<Delivery>) -> Self {
        Self::Ordinary(deliveries)
    }
}

struct ReceivingLink {
    max_message_size: u64,
    deliveries: ReceivingSink,
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
    content_lease: ContentLease,
    identity: DeliveryIdentity,
    native_posting: Option<native_transactions::NativePartialPosting>,
    forbidden_receiver_mode: bool,
    forbidden_sender_settled: bool,
}

#[allow(clippy::too_many_arguments)]
async fn run_connection<Io>(
    stream: Io,
    settings: ConnectionSettings,
    connection: &NativeConnectionIdentity,
    native_policy: NativeIngressPolicy,
    mut commands: mpsc::Receiver<Command>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    consumed: Arc<Notify>,
    mut cancellation: watch::Receiver<bool>,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let mut native_transactions = NativeTransactionBook::new(connection, native_policy);
    let native_cleanup = native_transactions.cleanup_notify();
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
    let (frames_tx, mut frames) = mpsc::channel(MAX_QUEUED_FRAMES);
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
                    if native_policy.supports_retirement()
                        && reconcile_native_retirements(&mut sessions, &mut writer)
                            .await
                            .is_err()
                    {
                        break;
                    }
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
                        () = native_cleanup.notified(), if native_policy.supports_retirement() => {
                            if reconcile_native_retirements(&mut sessions, &mut writer).await.is_err() {
                                break;
                            }
                        }
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
                                    native_transactions.close_all();
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
                            match handle_frame_scoped(
                                frame,
                                &mut writer,
                                &incoming_sessions,
                                &mut sessions,
                                remote_max_frame_size,
                                settings.remote_channel_max,
                                activity.is_closing(),
                                ConnectionScope::Native(connection),
                                &mut native_transactions,
                            ).await {
                                Ok(FrameAction::Continue) => pump_ready = true,
                                Ok(FrameAction::CloseSent) => pump_ready = false,
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
                            let result = match command {
                                Command::NativeTransactions(command) => native_transactions::handle_native_command(
                                    command, &mut native_transactions, &mut sessions, &mut writer,
                                ).await.map(|()| CommandAction::Continue),
                                command => {
                                    if matches!(&command, Command::Close { .. }) {
                                        native_transactions.close_all();
                                    }
                                    handle_command(command, &mut writer, &mut sessions, remote_max_frame_size).await
                                }
                            };
                            match result {
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
        native_transactions.close_all();
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
    CloseSent,
    Closed,
}

enum ConnectionScope<'a> {
    Native(&'a NativeConnectionIdentity),
    #[cfg(test)]
    Unbound,
}

#[cfg(test)]
async fn handle_frame<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut FrameWriter<W>,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
    remote_channel_max: u16,
    locally_closing: bool,
) -> Result<FrameAction, EngineError> {
    let mut native_transactions = NativeTransactionBook::disabled();
    handle_frame_scoped(
        frame,
        writer,
        incoming_sessions,
        sessions,
        remote_max_frame_size,
        remote_channel_max,
        locally_closing,
        ConnectionScope::Unbound,
        &mut native_transactions,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_frame_scoped<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut FrameWriter<W>,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
    remote_channel_max: u16,
    locally_closing: bool,
    connection: ConnectionScope<'_>,
    native_transactions: &mut NativeTransactionBook,
) -> Result<FrameAction, EngineError> {
    let Frame::Amqp {
        channel,
        performative,
        payload,
    } = frame
    else {
        return Err(invalid_state("SASL frame after AMQP open"));
    };
    let Some(mut performative) = performative else {
        return Ok(FrameAction::Continue);
    };
    let peer_channel = channel;
    let associated = local_channel_for_peer(peer_channel, sessions);
    if associated.is_some_and(|channel| sessions[&channel].ending)
        && !matches!(
            &performative,
            Performative::End(_) | Performative::Open(_) | Performative::Close(_)
        )
    {
        return Ok(FrameAction::Continue);
    }
    let channel = match &performative {
        Performative::Begin(begin) => {
            if associated.is_some() {
                refuse_connection(
                    "amqp:connection:framing-error",
                    "peer session channel is already assigned",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            if begin.remote_channel.is_some() {
                refuse_connection(
                    "amqp:connection:framing-error",
                    "unexpected response to a locally initiated session",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            if sessions.len() >= MAX_SESSIONS_PER_CONNECTION {
                refuse_connection(
                    "amqp:resource-limit-exceeded",
                    "connection session limit reached",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            let Some(channel) =
                preferred_vacant_channel(peer_channel, remote_channel_max, sessions)
            else {
                refuse_connection(
                    "amqp:resource-limit-exceeded",
                    "peer session channel limit reached",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::Continue);
            };
            channel
        }
        Performative::Open(_) | Performative::Close(_) => channel,
        _ => {
            let Some(channel) = associated else {
                refuse_connection(
                    "amqp:connection:framing-error",
                    "frame on an unassigned peer session channel",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::Continue);
            };
            channel
        }
    };

    let input_handle = match &performative {
        Performative::Flow(flow) => flow.handle,
        Performative::Transfer(transfer) => Some(transfer.handle),
        Performative::Detach(detach) => Some(detach.handle),
        _ => None,
    };
    if let Some(peer_handle) = input_handle {
        let Some(handle) = sessions
            .get(&channel)
            .and_then(|session| local_handle_for_peer(peer_handle, session))
        else {
            let historical = sessions
                .get(&channel)
                .is_some_and(|session| session.error_peer_handles.contains(peer_handle));
            if historical && matches!(&performative, Performative::Detach(_)) {
                return Ok(FrameAction::Continue);
            }
            refuse_session(
                channel,
                if historical {
                    "amqp:session:errant-link"
                } else {
                    "amqp:session:unattached-handle"
                },
                if historical {
                    "frame on an error-detached peer link handle"
                } else {
                    "frame on an unassigned peer link handle"
                },
                writer,
                sessions,
            )
            .await?;
            return Ok(FrameAction::Continue);
        };
        match &mut performative {
            Performative::Flow(flow) => flow.handle = Some(handle),
            Performative::Transfer(transfer) => transfer.handle = handle,
            Performative::Detach(detach) => detach.handle = handle,
            _ => unreachable!("only link performatives carry an input handle"),
        }
    }

    match performative {
        Performative::Begin(begin) => {
            let mut session = match connection {
                ConnectionScope::Native(connection) => {
                    SessionState::for_connection(&begin, connection)
                }
                #[cfg(test)]
                ConnectionScope::Unbound => SessionState::new(&begin),
            };
            session.peer_channel = Some(peer_channel);
            sessions.insert(channel, session);
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
            let connection_slots = connection_link_slot_count(sessions);
            let peer_handle = attach.handle;
            let peer_handle_bound = sessions.get(&channel).is_some_and(|session| {
                session
                    .handle_aliases
                    .values()
                    .any(|alias| alias.peer_handle == Some(peer_handle))
            });
            let name_in_use = !peer_handle_bound
                && connection_link_name_in_use(sessions, &attach.name, &attach.role.opposite());
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("attach on an unknown session"))?;
            if peer_handle_bound {
                if let Some(handle) = local_handle_for_peer(peer_handle, session)
                    && is_error_detached(session, handle)
                {
                    let local_role = attach.role.opposite();
                    let known_resume = session.handle_aliases.get(&handle).is_some_and(|alias| {
                        alias.name.as_ref() == attach.name && alias.role == local_role
                    }) && writer
                        .error_link_names()
                        .contains(&attach.name, &local_role)
                        && attach.unsettled.is_some();
                    refuse_session_state(
                        channel,
                        if known_resume {
                            "amqp:not-implemented"
                        } else {
                            "amqp:session:errant-link"
                        },
                        if known_resume {
                            RECOVERY_NOT_IMPLEMENTED
                        } else {
                            "attach on an error-detached peer link handle"
                        },
                        session,
                        writer,
                    )
                    .await?;
                    return Ok(FrameAction::Continue);
                }
                refuse_connection(
                    "amqp:session:handle-in-use",
                    "link handle is already assigned",
                    writer,
                    sessions,
                )
                .await?;
                return Ok(FrameAction::CloseSent);
            }
            let known_error = writer
                .error_link_names()
                .contains(&attach.name, &attach.role.opposite());
            if known_error && attach.unsettled.is_none() {
                refuse_session_state(
                    channel,
                    "amqp:session:errant-link",
                    "pipelined attach for an error-detached link",
                    session,
                    writer,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            if !known_error && name_in_use {
                refuse_session_state(
                    channel,
                    "amqp:not-implemented",
                    "reattaching an existing link name is not implemented",
                    session,
                    writer,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            let historical_recovery = known_error && attach.unsettled.is_some();
            let native_kind = if historical_recovery {
                native_transactions::NativeAttachKind::Ordinary
            } else {
                match native_transactions::classify_attach(&attach, native_transactions.policy()) {
                    Ok(kind) => kind,
                    Err(error) => {
                        refuse_session_state(
                            channel,
                            error.condition(),
                            error.description(),
                            session,
                            writer,
                        )
                        .await?;
                        return Ok(FrameAction::Continue);
                    }
                }
            };
            if !historical_recovery
                && attach_uses_transactions(&attach)
                && !matches!(
                    native_kind,
                    native_transactions::NativeAttachKind::Coordinator(_)
                )
            {
                refuse_session_state(
                    channel,
                    "amqp:not-implemented",
                    TRANSACTIONS_NOT_IMPLEMENTED,
                    session,
                    writer,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            if !historical_recovery
                && matches!(native_kind, native_transactions::NativeAttachKind::Ordinary)
                && source_default_outcome(attach.source.as_ref()).is_err()
            {
                refuse_session_state(
                    channel,
                    "amqp:invalid-field",
                    "source default outcome must be an ordinary terminal outcome",
                    session,
                    writer,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            // Ordinary approvals may normalize control-link fields; historical
            // handle reassignment must instead validate before replacing authority.
            if session.error_peer_handles.contains(peer_handle)
                && attach.role == Role::Sender
                && attach.initial_delivery_count.is_none()
            {
                refuse_session_state(
                    channel,
                    "amqp:invalid-field",
                    "sender attach has no initial delivery count",
                    session,
                    writer,
                )
                .await?;
                return Ok(FrameAction::Continue);
            }
            if link_slot_count(session) >= MAX_LINKS_PER_SESSION
                || connection_slots >= MAX_LINKS_PER_CONNECTION
            {
                refuse_session(
                    channel,
                    "amqp:resource-limit-exceeded",
                    "link lifecycle slot limit reached",
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
            } else {
                let Some(handle) =
                    preferred_vacant_handle(peer_handle, session.remote_handle_max, session)
                else {
                    refuse_session(
                        channel,
                        "amqp:resource-limit-exceeded",
                        "peer link handle limit reached",
                        writer,
                        sessions,
                    )
                    .await?;
                    return Ok(FrameAction::Continue);
                };
                let attach = IncomingAttach::new_with_kind(
                    *attach,
                    session.identity.clone(),
                    handle,
                    native_kind,
                );
                session.handle_aliases.insert(
                    handle,
                    HandleAlias {
                        identity: attach.approval().link_identity().clone(),
                        name: Arc::clone(attach.approval().name()),
                        role: attach.approval().local_role(),
                        peer_handle: Some(peer_handle),
                        own_attach_sent: false,
                        error_detached: false,
                    },
                );
                session.error_peer_handles.reassign(peer_handle);
                let mut pending = PendingLinkFlow::incoming(&attach);
                pending.recovery_refusal = has_recovery_state(&attach) || known_error;
                let recovery_refusal = pending.recovery_refusal;
                session.pending_attaches.insert(handle, pending);
                if recovery_refusal && session.attach_tx.is_some() {
                    refuse_recovery_attach(channel, &attach, session, writer).await?;
                } else if let Some(attach_tx) = &session.attach_tx {
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
            receive_transfer_with_native(
                channel,
                transfer,
                payload,
                sessions,
                writer,
                native_transactions,
            )
            .await?;
        }
        Performative::Disposition(disposition) => {
            if !apply_native_outgoing_disposition(
                channel,
                &disposition,
                sessions,
                writer,
                native_transactions,
            )
            .await?
            {
                apply_disposition(channel, disposition, writer, sessions).await?;
            }
        }
        Performative::Detach(detach) => {
            if let Some(session) = sessions.get_mut(&channel) {
                let locally_closing = session.closing_handles.contains(&detach.handle);
                if session.pending_attaches.contains_key(&detach.handle) && !locally_closing {
                    acknowledge_pending_detach(channel, detach.handle, session, writer).await?;
                } else if let Some(mut link) = session.links.remove(&detach.handle) {
                    let identity = link.identity().clone();
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
                    remove_handle_alias(session, detach.handle, &identity);
                    session.closing_handles.remove(&detach.handle);
                } else if locally_closing {
                    if let Some(alias) = session.handle_aliases.remove(&detach.handle) {
                        alias.identity.retire();
                    }
                    session.closing_handles.remove(&detach.handle);
                    if let Some(pending) = session.pending_attaches.remove(&detach.handle) {
                        pending.retire();
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
            native_transactions.close_all();
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

#[allow(clippy::too_many_arguments)]
async fn accept_native_receiving<W: AsyncWrite + Unpin>(
    channel: u16,
    owner: SessionIdentity,
    attach: IncomingAttach,
    max_message_size: u64,
    decoders: MessageFormatDecoders,
    deliveries: ReceivingSink,
    detached: watch::Sender<bool>,
    consumption: Arc<Consumption>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<LinkIdentity, EngineError> {
    if owner.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    let session = sessions
        .get_mut(&channel)
        .ok_or(EngineError::RemoteDetached)?;
    if session.ending || session.identity.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    if !session.identity.same_session(&owner) {
        return Err(invalid_state(
            "attach approval belongs to a different session generation",
        ));
    }
    attach
        .validate_request(&owner)
        .map_err(attach_approval_error)?;
    let expected = match &deliveries {
        ReceivingSink::Coordinator(_) => match attach.approval().kind() {
            kind @ native_transactions::NativeAttachKind::Coordinator(_) => kind,
            native_transactions::NativeAttachKind::Ordinary => {
                return Err(invalid_state("ordinary attach cannot become a coordinator"));
            }
        },
        ReceivingSink::Transactional(_) => native_transactions::NativeAttachKind::Ordinary,
        ReceivingSink::Ordinary(_) => {
            return Err(invalid_state("native acceptance requires a dedicated sink"));
        }
    };
    native_transactions::validate_accept_kind(&attach, expected)?;
    if matches!(
        expected,
        native_transactions::NativeAttachKind::Coordinator(_)
    ) && !decoders.is_default()
    {
        return Err(invalid_state(
            "custom message-format decoders require a transactional receiving endpoint",
        ));
    }
    if attach.role != Role::Sender {
        return Err(invalid_state(
            "native ingress requires a peer sending endpoint",
        ));
    }
    if has_recovery_state(&attach) {
        return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
    }
    let handle = attach.approval().local_handle();
    let pending = session
        .pending_attaches
        .get(&handle)
        .ok_or(EngineError::RemoteDetached)?;
    let approval = pending
        .approval
        .as_ref()
        .ok_or(EngineError::RemoteDetached)?;
    attach
        .validate(&session.identity, approval)
        .map_err(attach_approval_error)?;
    if pending.recovery_refusal {
        return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
    }
    if !session.handle_aliases.get(&handle).is_some_and(|alias| {
        alias.identity.same_link(approval.link_identity())
            && alias.peer_handle == Some(attach.handle)
    }) {
        return Err(invalid_state(
            "attach approval has no matching handle alias",
        ));
    }
    if session.links.contains_key(&handle) || session.closing_handles.contains(&handle) {
        return Err(invalid_state(
            "link handle is attached or awaiting detach acknowledgement",
        ));
    }
    let initial_count = match attach.initial_delivery_count {
        Some(count) => count,
        None if matches!(
            expected,
            native_transactions::NativeAttachKind::Coordinator(profile)
                if profile.defaults_initial_delivery_count()
        ) =>
        {
            0
        }
        None => return Err(invalid_state("sender attach has no initial delivery count")),
    };
    let receive_maximum = effective_receive_maximum(Some(max_message_size));
    let (attach, approval) = attach.into_parts();
    let identity = approval.link_identity().clone();
    let mut response = attach.response(attach.source.clone(), attach.target.clone());
    response.handle = handle;
    if matches!(
        expected,
        native_transactions::NativeAttachKind::Coordinator(_)
    ) {
        response.target = Some(
            crate::Coordinator {
                capabilities: Some(
                    vec![
                        crate::Symbol::from("amqp:local-transactions"),
                        crate::Symbol::from("amqp:multi-txns-per-ssn"),
                        crate::Symbol::from("amqp:multi-ssns-per-txn"),
                    ]
                    .into(),
                ),
            }
            .into(),
        );
    }
    response.max_message_size = Some(receive_maximum);
    let frame = Frame::Amqp {
        channel,
        performative: Some(Performative::Attach(Box::new(response))),
        payload: Vec::new(),
    };
    if let Err(error) = writer.encoded_frame(&frame) {
        close_pending_link(
            channel,
            handle,
            &approval,
            session,
            writer,
            Some(Error::new(
                crate::AmqpError::FrameSizeTooSmall,
                "attach response exceeds the peer frame limit",
                None,
            )),
            false,
        )
        .await?;
        return Err(error.into());
    }
    ensure_local_begin(channel, session, writer).await?;
    writer.write_frame(&frame).await?;
    if let Some(alias) = session.handle_aliases.get_mut(&handle) {
        alias.own_attach_sent = true;
    }
    session.links.insert(
        handle,
        LinkState::Receiving(Box::new(ReceivingLink {
            max_message_size: receive_maximum,
            deliveries,
            partial: None,
            detached,
            credit: ReceiveCredit::new(initial_count, LINK_CREDIT, consumption),
            decoders,
            identity: identity.clone(),
            sender_settle_mode: attach.snd_settle_mode,
            receiver_settle_mode: attach.rcv_settle_mode,
        })),
    );
    refill_link(channel, handle, session, writer).await?;
    let pending_flow = session
        .pending_attaches
        .remove(&handle)
        .and_then(|pending| pending.latest);
    if let Some(flow) = pending_flow {
        apply_link_flow(channel, flow, writer, sessions).await?;
    }
    if identity.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    Ok(identity)
}

async fn handle_command<W: AsyncWrite + Unpin>(
    command: Command,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::NativeTransactions(command) => {
            command.reject(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED));
        }
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
                let recovery_refusal = session
                    .pending_attaches
                    .get(&attach.approval().local_handle())
                    .is_some_and(|pending| {
                        pending.recovery_refusal
                            && pending
                                .approval
                                .as_ref()
                                .is_some_and(|approval| Arc::ptr_eq(approval, attach.approval()))
                    });
                if has_recovery_state(&attach) || recovery_refusal {
                    refuse_recovery_attach(channel, &attach, session, writer).await?;
                } else {
                    attach_tx
                        .try_send(attach)
                        .map_err(|_| invalid_state("pending attach queue is full"))?;
                }
            }
            if session.ending {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
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
            if let Err(error) = native_transactions::validate_accept_kind(
                &attach,
                native_transactions::NativeAttachKind::Ordinary,
            ) {
                let _ = reply.send(Err(error));
                return Ok(CommandAction::Continue);
            }
            let handle = attach.approval().local_handle();
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
            if !session.handle_aliases.get(&handle).is_some_and(|alias| {
                alias.identity.same_link(approval.link_identity())
                    && alias.peer_handle == Some(attach.handle)
            }) {
                let _ = reply.send(Err(invalid_state(
                    "attach approval has no matching handle alias",
                )));
                return Ok(CommandAction::Continue);
            }
            if has_recovery_state(&attach) {
                let _ = reply.send(Err(invalid_state(RECOVERY_NOT_IMPLEMENTED)));
                return Ok(CommandAction::Continue);
            }
            if attach_uses_transactions(&attach) {
                let _ = reply.send(Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED)));
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
            let receive_maximum = effective_receive_maximum(Some(max_message_size));
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.handle = handle;
            response.max_message_size =
                (response.role == Role::Receiver).then_some(receive_maximum);
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
                close_pending_link(
                    channel,
                    handle,
                    &approval,
                    session,
                    writer,
                    Some(Error::new(
                        crate::AmqpError::FrameSizeTooSmall,
                        "attach response exceeds the peer frame limit",
                        None,
                    )),
                    false,
                )
                .await?;
                let _ = reply.send(Err(error.into()));
                return Ok(CommandAction::Continue);
            }
            ensure_local_begin(channel, session, writer).await?;
            writer.write_frame(&response_frame).await?;
            session
                .handle_aliases
                .get_mut(&handle)
                .expect("validated handle alias")
                .own_attach_sent = true;
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
                        LinkState::Receiving(Box::new(ReceivingLink {
                            max_message_size: receive_maximum,
                            deliveries: deliveries_tx.into(),
                            partial: None,
                            detached: detached_tx,
                            credit: ReceiveCredit::new(initial_count, LINK_CREDIT, consumption),
                            decoders,
                            identity,
                            sender_settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                        })),
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
                            outstanding_tags: HashSet::new(),
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
            let error_detached = error.is_some();
            let error_snapshot = if error_detached {
                match snapshot_error_histories(session, handle, &identity, None, writer) {
                    Ok(snapshot) => Some(snapshot),
                    Err(error) => {
                        refuse_session_state(
                            channel,
                            "amqp:resource-limit-exceeded",
                            error.to_string(),
                            session,
                            writer,
                        )
                        .await?;
                        let _ = reply.send(Ok(()));
                        return Ok(CommandAction::Continue);
                    }
                }
            } else {
                None
            };
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
            if let Some(snapshot) = &error_snapshot {
                if !session.local_begin_sent {
                    writer.encoded_frame(&local_begin_frame(channel, session)?)?;
                }
                commit_error_histories(session, snapshot, writer)?;
            }
            ensure_local_begin(channel, session, writer).await?;
            remember_closing_handle(session, handle)?;
            if error_detached {
                mark_error_detached(session, handle, &identity);
            }
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
    if transaction_state(Some(&state)) {
        let _ = reply.send(Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED)));
        return Ok(());
    }
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
    if transaction_state(state.as_ref()) {
        return Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED));
    }
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
    link.outstanding_tags.remove(identity.tag());
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

#[cfg(any(test, feature = "test-client"))]
async fn receive_transfer<W: AsyncWrite + Unpin>(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let mut native_transactions = NativeTransactionBook::disabled();
    receive_transfer_with_native(
        channel,
        transfer,
        payload,
        sessions,
        writer,
        &mut native_transactions,
    )
    .await
}

async fn receive_transfer_with_native<W: AsyncWrite + Unpin>(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
    native_transactions: &mut NativeTransactionBook,
) -> Result<(), EngineError> {
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("transfer on an unknown session"))?;
    if is_error_detached(session, transfer.handle) {
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
        return refuse_session_state(
            channel,
            "amqp:session:errant-link",
            "transfer on an error-detached link",
            session,
            writer,
        )
        .await;
    }
    let native_transactional = matches!(session.links.get(&transfer.handle), Some(LinkState::Receiving(link))
        if matches!(&link.deliveries, ReceivingSink::Transactional(_)));
    if !session.closing_handles.contains(&transfer.handle)
        && matches!(
            session.links.get(&transfer.handle),
            Some(LinkState::Receiving(_))
        )
        && transaction_state(transfer.state.as_ref())
        && !(native_transactional
            && matches!(&transfer.state, Some(DeliveryState::Transactional(_))))
    {
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
        return refuse_session_state(
            channel,
            "amqp:not-implemented",
            TRANSACTIONS_NOT_IMPLEMENTED,
            session,
            writer,
        )
        .await;
    }
    if let Err(error) = session.flow.receive_transfer() {
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
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
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
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
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
        return refuse_session(
            channel,
            "amqp:session:errant-link",
            "transfer sent to a sending link",
            writer,
            sessions,
        )
        .await;
    };

    if let Some(partial) = &link.partial {
        if let Some(posting) = &partial.native_posting {
            if let Err(error) = posting.validate_continuation(transfer.state.as_ref()) {
                native_transactions.fault_posting(
                    transfer.state.as_ref(),
                    native_transactions::NativeFault::Continuation,
                );
                detach_link_error(
                    channel,
                    transfer.handle,
                    session,
                    writer,
                    error.condition(),
                    error.description(),
                )
                .await?;
                return refill_link(channel, transfer.handle, session, writer).await;
            }
        } else if native_transactional && transaction_state(transfer.state.as_ref()) {
            native_transactions.fault_posting(
                transfer.state.as_ref(),
                native_transactions::NativeFault::Continuation,
            );
            detach_link_error(
                channel,
                transfer.handle,
                session,
                writer,
                "amqp:invalid-field",
                "ordinary delivery cannot become a transactional posting",
            )
            .await?;
            return refill_link(channel, transfer.handle, session, writer).await;
        }
    }
    let native_posting_frame = matches!(
        transfer.state.as_ref(),
        Some(DeliveryState::Transactional(_))
    ) || link
        .partial
        .as_ref()
        .is_some_and(|partial| partial.native_posting.is_some());
    if transfer.settled == Some(true)
        && (native_posting_frame || matches!(&link.deliveries, ReceivingSink::Coordinator(_)))
    {
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
        let condition = if matches!(&link.deliveries, ReceivingSink::Coordinator(_)) {
            "amqp:illegal-state"
        } else {
            "amqp:not-allowed"
        };
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            condition,
            "sender-settled native transaction ingress is not supported",
        )
        .await?;
        return refill_link(channel, transfer.handle, session, writer).await;
    }

    if transfer.resume {
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
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
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Decode,
        );
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
                native_transactions.fault_posting(
                    transfer.state.as_ref(),
                    native_transactions::NativeFault::Stage,
                );
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
        native_transactions.fault_posting(
            transfer.state.as_ref(),
            native_transactions::NativeFault::Stage,
        );
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
    if link.partial.is_none() {
        // A fresh ID supersedes error history only after ledger and credit admission.
        session.error_deliveries.reassign_incoming(identity.id());
    }
    let native_posting = if link.partial.is_none() && native_transactional {
        if let Some(DeliveryState::Transactional(state)) = transfer.state.as_ref() {
            match native_transactions.begin_posting(&link.identity, identity.clone(), state) {
                Ok(posting) => Some(posting),
                Err(error) => {
                    detach_link_error(
                        channel,
                        transfer.handle,
                        session,
                        writer,
                        error.condition(),
                        error.description(),
                    )
                    .await?;
                    return refill_link(channel, transfer.handle, session, writer).await;
                }
            }
        } else {
            None
        }
    } else {
        None
    };
    if transfer.aborted {
        if let Some(posting) = native_posting.as_ref().or_else(|| {
            link.partial
                .as_ref()
                .and_then(|partial| partial.native_posting.as_ref())
        }) {
            posting.fault(native_transactions::NativeFault::Aborted);
        }
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
    let content_lease = match link.partial.as_mut() {
        Some(partial) => partial.content_lease.try_grow(payload.len()).map(|()| None),
        None => writer.content_budget().try_reserve(payload.len()).map(Some),
    };
    let content_lease = match content_lease {
        Ok(lease) => lease,
        Err(error) => {
            detach_link_error(
                channel,
                transfer.handle,
                session,
                writer,
                "amqp:resource-limit-exceeded",
                error.to_string(),
            )
            .await?;
            return refill_link(channel, transfer.handle, session, writer).await;
        }
    };
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
            content_lease: content_lease.expect("a first transfer reserves its content"),
            identity,
            native_posting,
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
            if let Some(posting) = &partial.native_posting {
                posting.fault(native_transactions::NativeFault::Decode);
            }
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
    let delivery = Delivery {
        #[cfg(test)]
        id: partial.id,
        #[cfg(test)]
        settled: _completion == Completion::SenderSettled,
        message_format: partial.message_format,
        message,
        identity: partial.identity,
        content_lease: Some(Arc::new(partial.content_lease)),
    };
    let publication_error = match &link.deliveries {
        ReceivingSink::Ordinary(sink) => sink.try_send(delivery).err().map(|_| {
            (
                "amqp:resource-limit-exceeded",
                "incoming delivery queue is unavailable",
            )
        }),
        ReceivingSink::Coordinator(sink) => {
            if let Err(refusal) = native_transactions.publish_control(
                channel,
                transfer.handle,
                &link.identity,
                delivery,
                sink,
            ) {
                native_transactions::handle_control_refusal(refusal, session, writer).await?;
            }
            return refill_link(channel, transfer.handle, session, writer).await;
        }
        ReceivingSink::Transactional(sink) => {
            if let Some(posting) = partial.native_posting {
                native_transactions
                    .publish_posting(posting, delivery, sink)
                    .err()
                    .map(|error| (error.condition(), error.description()))
            } else {
                sink.try_send(native_transactions::TransactionalIngress::Ordinary(
                    RetainedDelivery::new(delivery),
                ))
                .err()
                .map(|_| {
                    (
                        "amqp:resource-limit-exceeded",
                        "incoming delivery queue is unavailable",
                    )
                })
            }
        }
    };
    if let Some((condition, description)) = publication_error {
        detach_link_error(
            channel,
            transfer.handle,
            session,
            writer,
            condition,
            description,
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
            .write_frame(&local_begin_frame(channel, session)?)
            .await?;
        session.local_begin_sent = true;
    }
    Ok(())
}

fn local_begin_frame(channel: u16, session: &SessionState) -> Result<Frame, EngineError> {
    let peer_channel = session
        .peer_channel
        .ok_or_else(|| invalid_state("session has no associated peer channel"))?;
    Ok(Frame::Amqp {
        channel,
        performative: Some(Performative::Begin(Begin {
            remote_channel: Some(peer_channel),
            ..Begin::default()
        })),
        payload: Vec::new(),
    })
}

async fn refuse_connection<W: AsyncWrite + Unpin>(
    condition: &str,
    description: impl Into<String>,
    writer: &mut FrameWriter<W>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let description = description.into();
    tracing::debug!(condition, %description, "refusing AMQP connection");
    let frame = Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Close(Close {
            error: Some(Error::new(
                crate::ErrorCondition::Custom(Symbol::from(condition)),
                description,
                None,
            )),
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&frame)?;
    for session in sessions.values_mut() {
        stop_session(session);
    }
    writer.write_frame(&frame).await?;
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
        return refuse_session_state(channel, condition, description, session, writer).await;
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

async fn refuse_session_state<W: AsyncWrite + Unpin>(
    channel: u16,
    condition: &str,
    description: impl Into<String>,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if session.ending {
        return Ok(());
    }
    let description = description.into();
    tracing::debug!(channel, condition, %description, "refusing AMQP session");
    let frame = Frame::Amqp {
        channel,
        performative: Some(Performative::End(End {
            error: Some(Error::new(
                crate::ErrorCondition::Custom(Symbol::from(condition)),
                description,
                None,
            )),
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&frame)?;
    if !session.local_begin_sent {
        writer.encoded_frame(&local_begin_frame(channel, session)?)?;
    }
    ensure_local_begin(channel, session, writer).await?;
    session.ending = true;
    stop_session(session);
    writer.write_frame(&frame).await?;
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
    if flow
        .handle
        .is_some_and(|handle| is_error_detached(session, handle))
    {
        return refuse_session_state(
            channel,
            "amqp:session:errant-link",
            "flow on an error-detached link",
            session,
            writer,
        )
        .await;
    }
    if refuse_transaction_flow(channel, &flow, session, writer).await? {
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
    if session.ending {
        return Ok(());
    }
    if is_error_detached(session, handle) {
        return refuse_session_state(
            channel,
            "amqp:session:errant-link",
            "flow on an error-detached link",
            session,
            writer,
        )
        .await;
    }
    if session.closing_handles.contains(&handle) {
        return Ok(());
    }
    if refuse_transaction_flow(channel, &flow, session, writer).await? {
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

fn effective_receive_maximum(maximum: Option<u64>) -> u64 {
    normalized_message_size(maximum)
        .unwrap_or(MAX_RECEIVED_MESSAGE_BYTES)
        .min(MAX_RECEIVED_MESSAGE_BYTES)
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
    attach: &IncomingAttach,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    close_pending_link(
        channel,
        attach.approval().local_handle(),
        attach.approval(),
        session,
        writer,
        Some(Error::new(
            crate::AmqpError::NotImplemented,
            RECOVERY_NOT_IMPLEMENTED,
            None,
        )),
        false,
    )
    .await
}

async fn acknowledge_pending_detach<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let approval = session
        .pending_attaches
        .get(&handle)
        .and_then(|pending| pending.approval.as_ref())
        .ok_or_else(|| invalid_state("pending link has no attach approval"))?
        .clone();
    close_pending_link(channel, handle, &approval, session, writer, None, true).await
}

async fn close_pending_link<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    approval: &Arc<AttachApproval>,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
    error: Option<Error>,
    peer_detached: bool,
) -> Result<(), EngineError> {
    let alias = session
        .handle_aliases
        .get(&handle)
        .filter(|alias| alias.identity.same_link(approval.link_identity()))
        .ok_or_else(|| invalid_state("pending link has no matching handle alias"))?;
    let error_detached = error.is_some();
    let needs_attach = !alias.own_attach_sent;
    let error_snapshot = if error_detached {
        match snapshot_error_histories(session, handle, approval.link_identity(), None, writer) {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                return refuse_session_state(
                    channel,
                    "amqp:resource-limit-exceeded",
                    error.to_string(),
                    session,
                    writer,
                )
                .await;
            }
        }
    } else {
        None
    };
    let response = Frame::Amqp {
        channel,
        performative: Some(Performative::Attach(Box::new(approval.refusal_attach()))),
        payload: Vec::new(),
    };
    if needs_attach && writer.encoded_frame(&response).is_err() {
        return refuse_session_state(
            channel,
            "amqp:frame-size-too-small",
            "minimal attach response exceeds the peer frame limit",
            session,
            writer,
        )
        .await;
    }
    let detach = Frame::Amqp {
        channel,
        performative: Some(Performative::Detach(Detach {
            handle,
            closed: true,
            error,
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&detach)?;
    if !session.local_begin_sent {
        writer.encoded_frame(&local_begin_frame(channel, session)?)?;
    }
    if let Some(snapshot) = &error_snapshot {
        commit_error_histories(session, snapshot, writer)?;
    }
    if peer_detached {
        approval.retire();
    } else {
        remember_closing_handle(session, handle)?;
        if error_detached {
            mark_error_detached(session, handle, approval.link_identity());
        }
    }
    ensure_local_begin(channel, session, writer).await?;
    if needs_attach {
        writer.write_frame(&response).await?;
        session
            .handle_aliases
            .get_mut(&handle)
            .expect("validated pending handle alias")
            .own_attach_sent = true;
    }
    writer.write_frame(&detach).await?;
    if peer_detached {
        if session
            .pending_attaches
            .get(&handle)
            .and_then(|pending| pending.approval.as_ref())
            .is_some_and(|current| Arc::ptr_eq(current, approval))
        {
            session.pending_attaches.remove(&handle);
        }
        session
            .pending_attach_events
            .retain(|attach| !Arc::ptr_eq(attach.approval(), approval));
        remove_handle_alias(session, handle, approval.link_identity());
        session.closing_handles.remove(&handle);
    }
    Ok(())
}

fn remove_handle_alias(session: &mut SessionState, handle: u32, identity: &LinkIdentity) {
    if session
        .handle_aliases
        .get(&handle)
        .is_some_and(|alias| alias.identity.same_link(identity))
    {
        session.handle_aliases.remove(&handle);
    }
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

fn collect_error_delivery_ids(
    ids: impl IntoIterator<Item = u32>,
) -> Result<HashSet<u32>, ErrorDeliveryHistoryError> {
    let mut owned = HashSet::new();
    for id in ids {
        owned.insert(id);
        if owned.len() > MAX_RETIRED_DELIVERIES_PER_DIRECTION {
            return Err(ErrorDeliveryHistoryError::LimitReached {
                maximum: MAX_RETIRED_DELIVERIES_PER_DIRECTION,
            });
        }
    }
    Ok(owned)
}

fn snapshot_error_deliveries(
    session: &SessionState,
    handle: u32,
    owner: &LinkIdentity,
) -> Result<Option<(Role, HashSet<u32>)>, ErrorDeliveryHistoryError> {
    if session.ending || session.identity.is_retired() || owner.is_retired() {
        return Ok(None);
    }
    let Some(link) = session
        .links
        .get(&handle)
        .filter(|link| link.identity().same_link(owner))
    else {
        return Ok(None);
    };
    let (role, ids) = match link {
        LinkState::Receiving(_) => (
            Role::Sender,
            collect_error_delivery_ids(session.incoming.owned_live_ids(owner))?,
        ),
        LinkState::Sending(link) => (
            Role::Receiver,
            collect_error_delivery_ids(
                link.unsettled
                    .keys()
                    .copied()
                    .chain(link.active.as_ref().map(|active| active.delivery_id))
                    .chain(
                        link.pending_acknowledgements
                            .iter()
                            .filter_map(|(&id, token)| {
                                (id == token.id() && token.belongs_to(owner) && !token.is_settled())
                                    .then_some(id)
                            }),
                    ),
            )?,
        ),
    };
    session.error_deliveries.check_record(&role, &ids)?;
    Ok(Some((role, ids)))
}

#[derive(Debug)]
struct ErrorLinkSnapshot {
    owner: LinkIdentity,
    alias: Option<(Arc<str>, Role, Option<u32>)>,
    deliveries: Option<(Role, HashSet<u32>)>,
}

fn snapshot_error_histories<W>(
    session: &SessionState,
    handle: u32,
    owner: &LinkIdentity,
    peer_override: Option<u32>,
    writer: &FrameWriter<W>,
) -> Result<ErrorLinkSnapshot, EngineError> {
    if owner.is_retired() || session.identity.is_retired() || session.ending {
        return Ok(ErrorLinkSnapshot {
            owner: owner.clone(),
            alias: None,
            deliveries: None,
        });
    }
    let alias = current_alias(handle, session)
        .filter(|alias| alias.identity.same_link(owner))
        .map(|alias| {
            (
                Arc::clone(&alias.name),
                alias.role.clone(),
                peer_override.or(alias.peer_handle),
            )
        });
    let deliveries = snapshot_error_deliveries(session, handle, owner)
        .map_err(|error| invalid_state(error.to_string()))?;
    let snapshot = ErrorLinkSnapshot {
        owner: owner.clone(),
        alias,
        deliveries,
    };
    check_error_histories(session, &snapshot, writer)?;
    Ok(snapshot)
}

fn check_error_histories<W>(
    session: &SessionState,
    snapshot: &ErrorLinkSnapshot,
    writer: &FrameWriter<W>,
) -> Result<(), EngineError> {
    if let Some((name, role, peer_handle)) = &snapshot.alias {
        writer
            .error_link_names()
            .check_record(name, role)
            .map_err(|error| invalid_state(error.to_string()))?;
        if let Some(peer_handle) = peer_handle {
            session
                .error_peer_handles
                .check_record(*peer_handle)
                .map_err(|error| invalid_state(error.to_string()))?;
        }
    }
    if let Some((role, ids)) = &snapshot.deliveries {
        session
            .error_deliveries
            .check_record(role, ids)
            .map_err(|error| invalid_state(error.to_string()))?;
    }
    Ok(())
}

fn commit_error_histories<W>(
    session: &mut SessionState,
    snapshot: &ErrorLinkSnapshot,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    // Admission for every index precedes the first publication; no await can intervene.
    check_error_histories(session, snapshot, writer)?;
    if let Some((name, role, peer_handle)) = &snapshot.alias {
        writer
            .error_link_names_mut()
            .record(Arc::clone(name), role, &snapshot.owner)
            .map_err(|error| invalid_state(error.to_string()))?;
        if let Some(peer_handle) = peer_handle {
            session
                .error_peer_handles
                .record(*peer_handle, &snapshot.owner)
                .map_err(|error| invalid_state(error.to_string()))?;
        }
    }
    if let Some((role, ids)) = &snapshot.deliveries {
        session
            .error_deliveries
            .record(role, &snapshot.owner, ids)
            .map_err(|error| invalid_state(error.to_string()))?;
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
    reply: impl Into<OutgoingReply>,
    writer: &mut FrameWriter<W>,
    _remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let reply = reply.into();
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
    if link.outstanding_tags.contains(delivery_tag.as_ref()) {
        let _ = reply.send(Err(invalid_state(
            "outgoing delivery tag is already in use on this link",
        )));
        return Ok(());
    }
    if link.outstanding_tags.len() >= MAX_OUTGOING_DELIVERIES_PER_LINK {
        let _ = reply.send(Err(invalid_state(
            "outgoing delivery limit reached on this link",
        )));
        return Ok(());
    }
    let outstanding = session
        .links
        .values()
        .filter_map(|link| match link {
            LinkState::Sending(link) => Some(link.outstanding_tags.len()),
            _ => None,
        })
        .sum::<usize>();
    if outstanding >= MAX_OUTGOING_DELIVERIES_PER_SESSION {
        let _ = reply.send(Err(invalid_state(
            "outgoing delivery limit reached on this session",
        )));
        return Ok(());
    }
    if link.queued.len() >= DELIVERY_QUEUE_CAPACITY {
        let _ = reply.send(Err(invalid_state("outgoing delivery queue is full")));
        return Ok(());
    }
    let prepared = match prepare_message(&message) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = reply.send(Err(error.into()));
            return Ok(());
        }
    };
    let message_bytes = u64::try_from(prepared.encoded_len()).unwrap_or(u64::MAX);
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
    let content_lease = match writer.content_budget().try_reserve(prepared.encoded_len()) {
        Ok(lease) => lease,
        Err(error) => {
            let _ = reply.send(Err(invalid_state(error.to_string())));
            return Ok(());
        }
    };
    let payload = match prepared.encode() {
        Ok(payload) => payload,
        Err(error) => {
            drop(content_lease);
            let _ = reply.send(Err(error.into()));
            return Ok(());
        }
    };
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        unreachable!("the validated sending link has not changed");
    };
    if let Err(error) = fragment_frame(
        channel,
        handle,
        u32::MAX,
        &delivery_tag,
        message_format,
        link.settle_mode == SenderSettleMode::Settled,
        &payload,
        0,
        false,
        writer,
    ) {
        drop(payload);
        drop(content_lease);
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
    link.outstanding_tags.insert(delivery_tag.as_ref().to_vec());
    link.queued.push_back(QueuedSend {
        payload,
        content_lease,
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
    let owner = session
        .links
        .get(&handle)
        .map(|link| link.identity().clone());
    let error_snapshot = if let Some(owner) = &owner {
        match snapshot_error_histories(session, handle, owner, None, writer) {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                return refuse_session_state(
                    channel,
                    "amqp:resource-limit-exceeded",
                    error.to_string(),
                    session,
                    writer,
                )
                .await;
            }
        }
    } else {
        None
    };
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
    if !session.local_begin_sent {
        writer.encoded_frame(&local_begin_frame(channel, session)?)?;
    }
    if let Some(snapshot) = &error_snapshot {
        commit_error_histories(session, snapshot, writer)?;
    }
    ensure_local_begin(channel, session, writer).await?;
    remember_closing_handle(session, handle)?;
    if let Some(owner) = &owner {
        mark_error_detached(session, handle, owner);
    }
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
    session.error_deliveries.outgoing_contains(id)
        || session.links.values().any(|link| matches!(link, LinkState::Sending(link) if link.unsettled.contains_key(&id) || link.pending_acknowledgements.contains_key(&id) || link.active.as_ref().is_some_and(|active| active.delivery_id == id)))
}

fn vacant_delivery_id(session: &SessionState) -> Option<u32> {
    let start = session.next_delivery_id;
    if !delivery_id_in_use(session, start) {
        return Some(start);
    }
    let mut occupied = HashSet::new();
    for link in session.links.values() {
        let LinkState::Sending(link) = link else {
            continue;
        };
        for id in link
            .unsettled
            .keys()
            .copied()
            .chain(link.pending_acknowledgements.keys().copied())
            .chain(link.active.as_ref().map(|active| active.delivery_id))
        {
            occupied.insert(id);
            if occupied.len() > MAX_OUTGOING_DELIVERIES_PER_SESSION {
                return None;
            }
        }
    }
    occupied.extend(session.error_deliveries.outgoing_ids());
    // Independent live and retired bounds guarantee a vacancy in this union.
    (1..=MAX_OUTGOING_DELIVERIES_PER_SESSION + MAX_RETIRED_DELIVERIES_PER_DIRECTION)
        .map(|offset| start.wrapping_add(offset as u32))
        .find(|id| !occupied.contains(id))
}

fn can_pump(session: &SessionState, link: &SendingLink) -> bool {
    !session.ending
        && !link.identity.is_retired()
        && session.flow.outgoing_allowance() != 0
        && (link.active.is_some()
            || (!link.queued.is_empty()
                && link.credit.allowance() != 0
                && vacant_delivery_id(session).is_some()))
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
    if session.ending || link.identity.is_retired() || session.flow.outgoing_allowance() == 0 {
        return Ok(());
    }
    let starting = link.active.is_none();
    let delivery_id = if let Some(active) = &link.active {
        active.delivery_id
    } else {
        if link.queued.is_empty() || link.credit.allowance() == 0 {
            return Ok(());
        }
        let Some(id) = vacant_delivery_id(session) else {
            return Ok(());
        };
        id
    };
    let (frame, offset, complete) = if let Some(active) = &link.active {
        fragment_frame(
            channel,
            handle,
            delivery_id,
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
            delivery_id,
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
        let id = delivery_id;
        let delivery_identity = NativeOutgoingDeliveryIdentity::for_delivery(&link.identity, id);
        session.next_delivery_id = id.wrapping_add(1);
        let settled = link.settle_mode == SenderSettleMode::Settled;
        let settled_reply = if settled {
            Some(queued.reply)
        } else {
            link.unsettled.insert(
                id,
                OutgoingDelivery {
                    reply: queued.reply,
                    delivery_identity: delivery_identity.clone(),
                    delivery_tag: queued.delivery_tag.clone(),
                    outcome: None,
                    receiver_settled: false,
                    retirement: None,
                },
            );
            None
        };
        link.active = Some(ActiveSend {
            payload: queued.payload,
            content_lease: queued.content_lease,
            offset: 0,
            first_frame_sent: false,
            delivery_id: id,
            delivery_identity,
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
        let ActiveSend {
            payload,
            content_lease,
            delivery_id,
            delivery_identity,
            delivery_tag,
            settled_reply,
            ..
        } = link.active.take().expect("completed delivery exists");
        drop(payload);
        drop(content_lease);
        if let Some(reply) = settled_reply {
            link.outstanding_tags.remove(delivery_tag.as_ref());
            let _ = reply.send(Ok(SendOutcome {
                outcome: Outcome::Accepted(Accepted),
                delivery_identity,
                acknowledgement: None,
            }));
        } else {
            if let Some(row) = link.unsettled.get_mut(&delivery_id) {
                row.reply.flushed(&row.delivery_identity);
            }
            resolve_outgoing(channel, link, delivery_id, writer).await?;
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
        link.outstanding_tags.remove(delivery.delivery_tag.as_ref());
        let _ = delivery
            .reply
            .send(Err(EngineError::RemoteSettledWithoutOutcome));
        return Ok(());
    };
    let acknowledge =
        link.receiver_settle_mode == ReceiverSettleMode::Second && !delivery.receiver_settled;
    let mut acknowledgement = acknowledge.then(|| {
        AckIdentity::for_delivery(&delivery.delivery_identity, delivery.delivery_tag.as_ref())
    });
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
    } else {
        link.outstanding_tags.remove(delivery.delivery_tag.as_ref());
    }
    let _ = delivery.reply.send(Ok(SendOutcome {
        outcome,
        delivery_identity: delivery.delivery_identity,
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
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    if session.ending || session.identity.is_retired() {
        return Ok(());
    }
    if session.error_deliveries.contains_range(
        &disposition.role,
        disposition.first,
        disposition.last,
    ) {
        return refuse_session_state(
            channel,
            "amqp:session:errant-link",
            "disposition on an error-detached delivery",
            session,
            writer,
        )
        .await;
    }
    if transaction_state(disposition.state.as_ref()) {
        return refuse_session_state(
            channel,
            "amqp:not-implemented",
            TRANSACTIONS_NOT_IMPLEMENTED,
            session,
            writer,
        )
        .await;
    }
    if disposition.role == Role::Sender {
        if disposition.settled {
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
                    link.outstanding_tags.remove(identity.tag());
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
            link.outstanding_tags.clear();
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
            link.partial = None;
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
    for alias in session.handle_aliases.values() {
        alias.identity.retire();
    }
    session.handle_aliases.clear();
    session.incoming = IncomingLedger::new();
    session.error_deliveries.clear();
    session.error_peer_handles.clear();
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
    if source_uses_transactions(source) {
        return Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED));
    }
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
                target: Some(Target::new("reply-to").into()),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
            session.identity.clone(),
            handle,
        );
        session.handle_aliases.insert(
            handle,
            HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(handle),
                own_attach_sent: false,
                error_detached: false,
            },
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
mod owned_session_admission_tests;

#[cfg(test)]
mod outgoing_settlement_tests;

#[cfg(test)]
mod sender_provenance_tests;

#[cfg(test)]
mod native_retirement_tests;

#[cfg(test)]
mod transactional_defaults_tests;

#[cfg(test)]
mod session_provenance_tests;

#[cfg(test)]
mod outgoing_remote_settlement_tests;

#[cfg(test)]
mod outgoing_tag_tests;

#[cfg(test)]
mod outgoing_id_tests;

#[cfg(test)]
mod lifecycle_limit_tests;

#[cfg(test)]
mod receive_ceiling_tests;

#[cfg(test)]
mod content_budget_tests;

#[cfg(test)]
mod connection_identity_tests;

#[cfg(test)]
mod native_transaction_tests;

#[cfg(test)]
mod session_channel_tests;

#[cfg(test)]
mod link_handle_tests;

#[cfg(test)]
mod error_closing_tests;

#[cfg(test)]
mod error_delivery_tests;

#[cfg(test)]
mod error_link_tests;

#[cfg(test)]
mod live_name_tests;
