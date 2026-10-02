use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot, watch};
use url::Url;

use super::link_handles::vacant_handle;
use super::session_channels::vacant_channel;
use super::*;
use crate::{Source, Target, TargetTerminus};

#[path = "client_pending_attaches.rs"]
mod client_pending_attaches;

use client_pending_attaches::PendingAttaches;

pub struct ClientConnection {
    commands: mpsc::Sender<ClientCommand>,
    closed: watch::Receiver<bool>,
    lifecycle: ConnectionLifecycle,
    close_timeout: Duration,
    consumed: Arc<Notify>,
}

pub struct ClientSession {
    channel: u16,
    identity: SessionIdentity,
    commands: mpsc::Sender<ClientCommand>,
    consumed: Arc<Notify>,
}

pub struct ClientSender {
    channel: u16,
    handle: u32,
    next_tag: u64,
    commands: mpsc::Sender<ClientCommand>,
    detached: watch::Receiver<bool>,
    identity: LinkIdentity,
}

pub struct ClientReceiver {
    channel: u16,
    handle: u32,
    source: Option<Source>,
    commands: mpsc::Sender<ClientCommand>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
    consumption: Arc<Consumption>,
    identity: LinkIdentity,
}

pub type ClientDelivery = Delivery;

pub struct ClientConnectionBuilder {
    container_id: String,
    sasl: Option<SaslInit>,
    max_frame_size: u32,
    options: ConnectionOptions,
}

pub struct ClientReceiverBuilder {
    name: Option<String>,
    source: Option<Source>,
    target: Option<Target>,
    sender_settle_mode: SenderSettleMode,
    receiver_settle_mode: ReceiverSettleMode,
    max_message_size: Option<u64>,
}

impl ClientConnection {
    pub fn builder() -> ClientConnectionBuilder {
        ClientConnectionBuilder {
            container_id: String::from("amqp-client"),
            sasl: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            options: ConnectionOptions::default(),
        }
    }

    pub async fn open<Io>(
        stream: Io,
        container_id: impl Into<String>,
        sasl: Option<SaslInit>,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::open_with_max_frame_size(
            stream,
            container_id,
            sasl,
            DEFAULT_MAX_FRAME_SIZE,
            ConnectionOptions::default(),
        )
        .await
    }

    async fn open_with_max_frame_size<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<SaslInit>,
        maximum_frame_size: u32,
        options: ConnectionOptions,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        options.validate()?;
        let local_max_frame_size = normalized_frame_size(maximum_frame_size)?;
        let channel_max = u16::MAX;
        let local_open = checked_open_frame(Open {
            max_frame_size: local_max_frame_size,
            idle_time_out: Some(options.advertised_idle_timeout()),
            ..Open::new(container_id)
        })?;
        if let Some(init) = sasl {
            negotiation_header(&mut stream, ProtocolHeader::SASL, options).await?;
            expect_header(&mut stream, ProtocolHeader::SASL).await?;
            let mechanisms =
                match read_frame_with_max_size(&mut stream, local_max_frame_size).await? {
                    Frame::Sasl(SaslPerformative::Mechanisms(mechanisms)) => mechanisms,
                    _ => return Err(invalid_state("expected SASL mechanisms")),
                };
            if !mechanisms
                .mechanisms
                .iter()
                .any(|mechanism| mechanism == &init.mechanism)
            {
                return Err(invalid_state(
                    "the requested SASL mechanism was not offered",
                ));
            }
            negotiation_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Init(init)),
                options,
            )
            .await?;
            let outcome = match read_frame_with_max_size(&mut stream, local_max_frame_size).await? {
                Frame::Sasl(SaslPerformative::Outcome(outcome)) => outcome,
                _ => return Err(invalid_state("expected SASL outcome")),
            };
            if outcome.code != SaslCode::Ok {
                return Err(EngineError::SaslAuthentication(outcome.code));
            }
        }

        negotiation_header(&mut stream, ProtocolHeader::AMQP, options).await?;
        expect_header(&mut stream, ProtocolHeader::AMQP).await?;
        negotiation_frame(&mut stream, &local_open, options).await?;
        let remote_open = match read_frame_with_max_size(&mut stream, MIN_MAX_FRAME_SIZE).await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(open)),
                ..
            } => open,
            _ => return Err(invalid_state("expected AMQP open")),
        };
        let remote_max_frame_size = normalized_frame_size(remote_open.max_frame_size)?;
        let peer_idle_millis = peer_idle_timeout(
            &mut stream,
            remote_open.idle_time_out,
            remote_max_frame_size,
            options,
        )
        .await?;

        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        let consumed = Arc::new(Notify::new());
        let driver_consumed = consumed.clone();
        let (lifecycle, cancellation, actor_exit) = ConnectionLifecycle::new();
        tokio::spawn(async move {
            let exit_guard = actor_exit;
            let _ = run_client(
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
                command_rx,
                closed_tx,
                driver_consumed,
                cancellation,
            )
            .await;
            drop(exit_guard);
        });
        Ok(Self {
            commands,
            closed,
            lifecycle,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            consumed,
        })
    }

    pub async fn begin(&mut self) -> Result<ClientSession, EngineError> {
        let (channel, identity) =
            client_request(&self.commands, |reply| ClientCommand::Begin { reply }).await?;
        Ok(ClientSession {
            channel,
            identity,
            commands: self.commands.clone(),
            consumed: self.consumed.clone(),
        })
    }

    pub async fn on_close(&mut self) {
        wait_for_detach(&mut self.closed).await;
    }

    /// A zero duration requests immediate cancellation rather than waiting.
    pub fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    pub async fn shutdown(&self) {
        self.lifecycle.shutdown().await;
    }

    /// Observes this connection's provenance and native actor lifetime.
    pub fn connection_identity(&self) -> &NativeConnectionIdentity {
        &self.lifecycle.identity
    }

    #[cfg(test)]
    pub(super) async fn wait_terminated(&self) {
        self.lifecycle.wait_terminated().await;
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        if self.close_timeout.is_zero() {
            self.shutdown().await;
            return Err(EngineError::Timeout("Close acknowledgment"));
        }
        let mut guard = self.lifecycle.close_guard();
        let closing = async {
            let result =
                client_request(&self.commands, |reply| ClientCommand::Close { reply }).await;
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

impl ClientConnectionBuilder {
    pub fn container_id(mut self, container_id: impl Into<String>) -> Self {
        self.container_id = container_id.into();
        self
    }

    pub fn sasl(mut self, init: SaslInit) -> Self {
        self.sasl = Some(init);
        self
    }

    /// Advertises the incoming frame limit, capped by the codec's hard limit.
    /// Values below the AMQP minimum of 512 bytes are refused before I/O.
    pub fn max_frame_size(mut self, maximum: u32) -> Self {
        self.max_frame_size = maximum;
        self
    }

    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.options = options;
        self
    }

    pub fn idle_timeout_millis(mut self, millis: u32) -> Self {
        self.options = self.options.idle_timeout_millis(millis);
        self
    }

    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.options = self.options.write_timeout(timeout);
        self
    }

    pub async fn open(self, url: &str) -> Result<ClientConnection, EngineError> {
        self.options.validate()?;
        let max_frame_size = normalized_frame_size(self.max_frame_size)?;
        let url =
            Url::parse(url).map_err(|error| invalid_state(format!("invalid AMQP URL: {error}")))?;
        let host = url
            .host_str()
            .ok_or_else(|| invalid_state("AMQP URL has no host"))?;
        let port = url.port().unwrap_or(5672);
        let stream = tokio::net::TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        ClientConnection::open_with_max_frame_size(
            stream,
            self.container_id,
            self.sasl,
            max_frame_size,
            self.options,
        )
        .await
    }

    pub async fn open_with_stream<Io>(self, stream: Io) -> Result<ClientConnection, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        ClientConnection::open_with_max_frame_size(
            stream,
            self.container_id,
            self.sasl,
            self.max_frame_size,
            self.options,
        )
        .await
    }
}

impl ClientSession {
    pub async fn begin(connection: &mut ClientConnection) -> Result<Self, EngineError> {
        connection.begin().await
    }

    pub async fn attach_sender(
        &mut self,
        name: impl Into<String>,
        address: impl Into<String>,
    ) -> Result<ClientSender, EngineError> {
        self.attach_sender_with(name, Target::new(address)).await
    }

    pub async fn attach_sender_with(
        &mut self,
        name: impl Into<String>,
        target: Target,
    ) -> Result<ClientSender, EngineError> {
        if self.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        let name = name.into();
        let (deliveries_tx, _) = mpsc::channel(1);
        let (detached_tx, detached) = watch::channel(false);
        let identity = self.identity.new_link();
        let (handle, _) = client_request(&self.commands, |reply| ClientCommand::Attach {
            channel: self.channel,
            session: self.identity.clone(),
            request: Box::new(AttachRequest {
                name,
                role: Role::Sender,
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(target.into()),
                max_message_size: None,
            }),
            deliveries_tx,
            detached_tx,
            consumption: Arc::new(Consumption::new(self.consumed.clone())),
            identity: identity.clone(),
            reply,
        })
        .await?;
        Ok(ClientSender {
            channel: self.channel,
            handle,
            next_tag: 0,
            commands: self.commands.clone(),
            detached,
            identity,
        })
    }

    pub async fn attach_receiver(
        &mut self,
        name: impl Into<String>,
        address: impl Into<String>,
    ) -> Result<ClientReceiver, EngineError> {
        self.attach_receiver_with(
            name,
            Source::new(address),
            None,
            SenderSettleMode::Unsettled,
        )
        .await
    }

    pub async fn attach_receiver_with(
        &mut self,
        name: impl Into<String>,
        source: Source,
        target: Option<Target>,
        sender_settle_mode: SenderSettleMode,
    ) -> Result<ClientReceiver, EngineError> {
        self.attach_receiver_with_limit(
            name,
            source,
            target,
            sender_settle_mode,
            ReceiverSettleMode::First,
            None,
        )
        .await
    }

    async fn attach_receiver_with_limit(
        &mut self,
        name: impl Into<String>,
        source: Source,
        target: Option<Target>,
        sender_settle_mode: SenderSettleMode,
        receiver_settle_mode: ReceiverSettleMode,
        max_message_size: Option<u64>,
    ) -> Result<ClientReceiver, EngineError> {
        if self.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        source_default_outcome(Some(&source))?;
        let name = name.into();
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let identity = self.identity.new_link();
        let (detached_tx, detached) = watch::channel(false);
        let (handle, response) = client_request(&self.commands, |reply| ClientCommand::Attach {
            channel: self.channel,
            session: self.identity.clone(),
            request: Box::new(AttachRequest {
                name,
                role: Role::Receiver,
                sender_settle_mode,
                receiver_settle_mode,
                source: Some(source),
                target: target.map(Into::into),
                max_message_size,
            }),
            deliveries_tx,
            detached_tx,
            consumption: consumption.clone(),
            identity: identity.clone(),
            reply,
        })
        .await?;
        Ok(ClientReceiver {
            channel: self.channel,
            handle,
            source: response.source,
            commands: self.commands.clone(),
            deliveries,
            detached,
            consumption,
            identity,
        })
    }

    pub async fn end(&self) -> Result<(), EngineError> {
        if self.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        client_request(&self.commands, |reply| ClientCommand::End {
            channel: self.channel,
            identity: self.identity.clone(),
            reply,
        })
        .await
    }
}

impl ClientSender {
    pub async fn attach(
        session: &mut ClientSession,
        name: impl Into<String>,
        address: impl Into<String>,
    ) -> Result<Self, EngineError> {
        session.attach_sender(name, address).await
    }

    pub async fn send(&mut self, message: Message) -> Result<Outcome, EngineError> {
        self.send_with_message_format(message, 0).await
    }

    pub async fn send_with_message_format(
        &mut self,
        message: Message,
        message_format: u32,
    ) -> Result<Outcome, EngineError> {
        if self.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        let tag = self.next_tag.to_be_bytes().to_vec().into();
        self.next_tag = self.next_tag.wrapping_add(1);
        let (reply, outcome) = oneshot::channel();
        self.commands
            .send(ClientCommand::Send {
                channel: self.channel,
                handle: self.handle,
                identity: self.identity.clone(),
                message: Box::new(message),
                delivery_tag: tag,
                message_format,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        Ok(outcome.await.map_err(|_| EngineError::Stopped)??.outcome)
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        if self.identity.is_retired() {
            return Ok(());
        }
        client_request(&self.commands, |reply| ClientCommand::Detach {
            channel: self.channel,
            handle: self.handle,
            identity: self.identity.clone(),
            reply,
        })
        .await
    }

    pub async fn on_detach(&mut self) {
        wait_for_detach(&mut self.detached).await;
    }
}

impl ClientReceiver {
    pub fn builder() -> ClientReceiverBuilder {
        ClientReceiverBuilder {
            name: None,
            source: None,
            target: None,
            sender_settle_mode: SenderSettleMode::Unsettled,
            receiver_settle_mode: ReceiverSettleMode::First,
            max_message_size: None,
        }
    }

    pub async fn attach(
        session: &mut ClientSession,
        name: impl Into<String>,
        address: impl Into<String>,
    ) -> Result<Self, EngineError> {
        session.attach_receiver(name, address).await
    }

    pub fn source(&self) -> &Option<Source> {
        &self.source
    }

    pub async fn recv(&mut self) -> Result<ClientDelivery, EngineError> {
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

    /// Keeps the native content reservation until the returned receipt is dropped.
    /// Credit consumption still occurs exactly once at dequeue.
    pub async fn recv_retained(&mut self) -> Result<RetainedDelivery, EngineError> {
        let delivery = self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        self.consumption.consumed();
        Ok(RetainedDelivery::new(delivery))
    }

    pub async fn accept_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.accept(receipt.inner()).await
    }

    pub async fn reject_retained(
        &self,
        receipt: &RetainedDelivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.reject(receipt.inner(), error).await
    }

    pub async fn release_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.release(receipt.inner()).await
    }

    pub async fn modify_retained(
        &self,
        receipt: &RetainedDelivery,
        modified: crate::Modified,
    ) -> Result<(), EngineError> {
        self.modify(receipt.inner(), modified).await
    }

    pub async fn accept(&self, delivery: &ClientDelivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Accepted(Accepted))
            .await
    }

    pub async fn reject(
        &self,
        delivery: &ClientDelivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Rejected(crate::Rejected { error }))
            .await
    }

    pub async fn release(&self, delivery: &ClientDelivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Released(crate::Released))
            .await
    }

    pub async fn modify(
        &self,
        delivery: &ClientDelivery,
        modified: crate::Modified,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Modified(modified))
            .await
    }

    async fn settle(
        &self,
        delivery: &ClientDelivery,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        if !delivery.identity.belongs_to(&self.identity) {
            return Err(invalid_state(
                "delivery belongs to a different receiving link generation",
            ));
        }
        client_request(&self.commands, |reply| ClientCommand::Settle {
            channel: self.channel,
            handle: self.handle,
            identity: delivery.identity.clone(),
            state,
            reply,
        })
        .await
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        if self.identity.is_retired() {
            return Ok(());
        }
        client_request(&self.commands, |reply| ClientCommand::Detach {
            channel: self.channel,
            handle: self.handle,
            identity: self.identity.clone(),
            reply,
        })
        .await
    }
}

impl ClientReceiverBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn source(mut self, source: impl Into<Source>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(Target::new(target));
        self
    }

    pub fn sender_settle_mode(mut self, mode: SenderSettleMode) -> Self {
        self.sender_settle_mode = mode;
        self
    }

    pub fn receiver_settle_mode(mut self, mode: ReceiverSettleMode) -> Self {
        self.receiver_settle_mode = mode;
        self
    }

    /// Advertises the encoded-message limit, capped by the local 4 MiB ceiling.
    /// Zero or omission selects that local default.
    pub fn max_message_size(mut self, maximum: u64) -> Self {
        self.max_message_size = Some(maximum);
        self
    }

    pub async fn attach(self, session: &mut ClientSession) -> Result<ClientReceiver, EngineError> {
        session
            .attach_receiver_with_limit(
                self.name.unwrap_or_else(|| String::from("receiver")),
                self.source.unwrap_or_default(),
                self.target,
                self.sender_settle_mode,
                self.receiver_settle_mode,
                self.max_message_size,
            )
            .await
    }
}

impl From<String> for Source {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for Source {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

struct AttachRequest {
    name: String,
    role: Role,
    sender_settle_mode: SenderSettleMode,
    receiver_settle_mode: ReceiverSettleMode,
    source: Option<Source>,
    target: Option<TargetTerminus>,
    max_message_size: Option<u64>,
}

enum ClientCommand {
    Begin {
        reply: oneshot::Sender<Result<(u16, SessionIdentity), EngineError>>,
    },
    Attach {
        channel: u16,
        session: SessionIdentity,
        request: Box<AttachRequest>,
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        consumption: Arc<Consumption>,
        identity: LinkIdentity,
        reply: oneshot::Sender<Result<(u32, Attach), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        identity: LinkIdentity,
        message: Box<Message>,
        delivery_tag: DeliveryTag,
        message_format: u32,
        reply: oneshot::Sender<Result<SendOutcome, EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        identity: DeliveryIdentity,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        identity: LinkIdentity,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    End {
        channel: u16,
        identity: SessionIdentity,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Close {
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

fn reject_closed_client_command(command: ClientCommand) {
    match command {
        ClientCommand::Begin { reply } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
        ClientCommand::Attach { reply, .. } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
        ClientCommand::Send { reply, .. } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
        ClientCommand::Settle { reply, .. }
        | ClientCommand::Detach { reply, .. }
        | ClientCommand::End { reply, .. }
        | ClientCommand::Close { reply } => {
            let _ = reply.send(Err(EngineError::RemoteClosed));
        }
    }
}

struct PendingAttach {
    channel: u16,
    session: SessionIdentity,
    handle: u32,
    reply: oneshot::Sender<Result<(u32, Attach), EngineError>>,
    link: LinkState,
    consumption: Arc<Consumption>,
}

fn pending_attach_matches(
    pending: &PendingAttach,
    channel: u16,
    sessions: &HashMap<u16, SessionState>,
) -> bool {
    pending.channel == channel
        && sessions.get(&channel).is_some_and(|session| {
            !session.ending
                && !pending.session.is_retired()
                && session.identity.same_session(&pending.session)
                && session
                    .handle_aliases
                    .get(&pending.handle)
                    .is_some_and(|alias| {
                        alias.identity.same_link(pending.link.identity())
                            && alias.peer_handle.is_none()
                            && alias.own_attach_sent
                    })
        })
}

struct PendingBegin {
    identity: SessionIdentity,
    reply: oneshot::Sender<Result<(u16, SessionIdentity), EngineError>>,
}

struct PendingEnd {
    identity: SessionIdentity,
    reply: oneshot::Sender<Result<(), EngineError>>,
}

fn fail_pending_session(
    channel: u16,
    pending_begins: &mut HashMap<u16, PendingBegin>,
    pending_attaches: &mut PendingAttaches,
    pending_detaches: &mut HashMap<(u16, u32), oneshot::Sender<Result<(), EngineError>>>,
    pending_ends: &mut HashMap<u16, PendingEnd>,
) {
    if let Some(pending) = pending_begins.remove(&channel) {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::RemoteDetached));
    }
    let names: Vec<_> = pending_attaches
        .iter()
        .filter_map(|(name, role, pending)| {
            (pending.channel == channel).then_some((name.to_owned(), role))
        })
        .collect();
    for (name, role) in names {
        let mut pending = pending_attaches
            .remove(&name, &role)
            .expect("pending attach exists");
        stop_link(&mut pending.link);
        let _ = pending.reply.send(Err(EngineError::RemoteDetached));
    }
    let handles: Vec<_> = pending_detaches
        .keys()
        .copied()
        .filter(|(pending_channel, _)| *pending_channel == channel)
        .collect();
    for handle in handles {
        if let Some(reply) = pending_detaches.remove(&handle) {
            let _ = reply.send(Err(EngineError::RemoteDetached));
        }
    }
    if let Some(pending) = pending_ends.remove(&channel) {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::RemoteDetached));
    }
}

fn fail_pending_connection(
    pending_begins: &mut HashMap<u16, PendingBegin>,
    pending_attaches: &mut PendingAttaches,
    pending_detaches: &mut HashMap<(u16, u32), oneshot::Sender<Result<(), EngineError>>>,
    pending_ends: &mut HashMap<u16, PendingEnd>,
) {
    for (_, pending) in pending_begins.drain() {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::RemoteClosed));
    }
    for (_, mut pending) in pending_attaches.drain() {
        stop_link(&mut pending.link);
        let _ = pending.reply.send(Err(EngineError::RemoteClosed));
    }
    for (_, reply) in pending_detaches.drain() {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
    for (_, pending) in pending_ends.drain() {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::RemoteClosed));
    }
}

async fn client_request<T>(
    commands: &mpsc::Sender<ClientCommand>,
    make: impl FnOnce(oneshot::Sender<Result<T, EngineError>>) -> ClientCommand,
) -> Result<T, EngineError> {
    let (reply, response) = oneshot::channel();
    commands
        .send(make(reply))
        .await
        .map_err(|_| EngineError::Stopped)?;
    response.await.map_err(|_| EngineError::Stopped)?
}

#[allow(clippy::too_many_arguments)]
async fn run_client<Io>(
    stream: Io,
    settings: ConnectionSettings,
    connection: &NativeConnectionIdentity,
    mut commands: mpsc::Receiver<ClientCommand>,
    closed: watch::Sender<bool>,
    consumed: Arc<Notify>,
    mut cancellation: watch::Receiver<bool>,
) -> Result<(), EngineError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let remote_max_frame_size = settings.remote_max_frame_size;
    let activity = Activity::configured(settings.options);
    let (mut reader, writer) = tokio::io::split(stream);
    let mut writer = FrameWriter::new(writer, remote_max_frame_size)?;
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
    let mut next_channel = 0_u16;
    let mut next_handles = HashMap::<u16, u32>::new();
    let mut pending_begins = HashMap::<u16, PendingBegin>::new();
    let mut pending_ends = HashMap::<u16, PendingEnd>::new();
    let mut pending_attaches = PendingAttaches::default();
    let mut pending_detaches =
        HashMap::<(u16, u32), oneshot::Sender<Result<(), EngineError>>>::new();
    let mut closing = Vec::<oneshot::Sender<Result<(), EngineError>>>::new();
    let mut pump_ready = false;
    let mut pump_cursor = 0;

    let result = loop {
        let (result, stopped) = {
            let processing = async {
                loop {
                    if activity.heartbeat_is_due(settings.peer_idle_millis) {
                        writer
                            .write_frame(&Frame::Amqp {
                                channel: 0,
                                performative: None,
                                payload: Vec::new(),
                            })
                            .await?;
                    }
                    tokio::select! {
                        () = activity.heartbeat_due(settings.peer_idle_millis), if !activity.is_closing() => {
                            writer.write_frame(&Frame::Amqp {
                                channel: 0,
                                performative: None,
                                payload: Vec::new(),
                            }).await?;
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
                        let Frame::Amqp { channel, performative, payload } = frame else { break };
                        let Some(mut performative) = performative else { continue };
                        if activity.is_closing() && !matches!(&performative, Performative::Close(_)) {
                            continue;
                        }
                        let peer_channel = channel;
                        let associated = local_channel_for_peer(peer_channel, &sessions);
                        if associated.is_some_and(|channel| sessions[&channel].ending)
                            && !matches!(&performative, Performative::End(_) | Performative::Open(_) | Performative::Close(_))
                        { continue; }
                        let association = match &performative {
                            Performative::Begin(begin) => {
                                if associated.is_some() {
                                    Err(("amqp:connection:framing-error", "peer session channel is already assigned"))
                                } else if let Some(channel) = begin.remote_channel {
                                    let matches = pending_begins.get(&channel).is_some_and(|pending| {
                                        sessions.get(&channel).is_some_and(|session| {
                                            !session.ending && session.peer_channel.is_none()
                                                && !pending.identity.is_retired() && !session.identity.is_retired()
                                                && session.identity.same_session(&pending.identity)
                                        })
                                    });
                                    if matches {
                                        Ok(channel)
                                    } else {
                                        Err(("amqp:connection:framing-error", "begin response does not reference a pending local session"))
                                    }
                                } else {
                                    Err(("amqp:not-implemented", "peer-initiated sessions are not implemented by this client"))
                                }
                            }
                            Performative::Open(_) | Performative::Close(_) => Ok(channel),
                            _ => associated.ok_or(("amqp:connection:framing-error", "frame on an unassigned peer session channel")),
                        };
                        let channel = match association {
                            Ok(channel) => channel,
                            Err((condition, description)) => {
                                refuse_connection(condition, description, &mut writer, &mut sessions).await?;
                                fail_pending_connection(&mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                pump_ready = false;
                                continue;
                            }
                        };
                        let input_handle = match &performative {
                            Performative::Flow(flow) => flow.handle,
                            Performative::Transfer(transfer) => Some(transfer.handle),
                            Performative::Detach(detach) => Some(detach.handle),
                            _ => None,
                        };
                        if let Some(peer_handle) = input_handle {
                            let Some(handle) = sessions.get(&channel)
                                .and_then(|session| local_handle_for_peer(peer_handle, session))
                            else {
                                let historical = sessions.get(&channel).is_some_and(|session| session.error_peer_handles.contains(peer_handle));
                                if historical && matches!(&performative, Performative::Detach(_)) { continue; }
                                refuse_session(channel, if historical { "amqp:session:errant-link" } else { "amqp:session:unattached-handle" }, if historical { "frame on an error-detached peer link handle" } else { "frame on an unassigned peer link handle" }, &mut writer, &mut sessions).await?;
                                fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                continue;
                            };
                            match &mut performative {
                                Performative::Flow(flow) => flow.handle = Some(handle),
                                Performative::Transfer(transfer) => transfer.handle = handle,
                                Performative::Detach(detach) => detach.handle = handle,
                                _ => unreachable!("only link performatives carry an input handle"),
                            }
                        }
                            let result = match performative {
                                Performative::Begin(begin) => {
                                    let pending = pending_begins.remove(&channel).expect("validated pending begin");
                                    let session = sessions.get_mut(&channel).expect("validated initiated session");
                                    session.peer_channel = Some(peer_channel);
                                    session.remote_handle_max = begin.handle_max;
                                    session.flow = SessionWindow::new(0, begin.next_outgoing_id, begin.incoming_window, begin.outgoing_window, SESSION_WINDOW);
                                    let _ = pending.reply.send(Ok((channel, pending.identity)));
                                    Ok(false)
                                }
                                Performative::Attach(attach) => {
                                    let attach = *attach;
                                    let local_role = attach.role.opposite();
                                    let known_error = writer.error_link_names().contains(&attach.name, &local_role);
                                    if sessions.get(&channel).is_some_and(|session| session.handle_aliases.values().any(|alias| alias.peer_handle == Some(attach.handle))) {
                                        let error_alias = sessions.get(&channel).and_then(|session| local_handle_for_peer(attach.handle, session).filter(|handle| is_error_detached(session, *handle)).and_then(|handle| session.handle_aliases.get(&handle)));
                                        if let Some(alias) = error_alias {
                                            let known_resume = known_error && alias.name.as_ref() == attach.name && alias.role == local_role && attach.unsettled.is_some();
                                            refuse_session(channel, if known_resume { "amqp:not-implemented" } else { "amqp:session:errant-link" }, if known_resume { RECOVERY_NOT_IMPLEMENTED } else { "attach on an error-detached peer link handle" }, &mut writer, &mut sessions).await?;
                                            fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                            continue;
                                        }
                                        refuse_connection("amqp:session:handle-in-use", "peer link handle is already assigned", &mut writer, &mut sessions).await?;
                                        fail_pending_connection(&mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        pump_ready = false;
                                        continue;
                                    }
                                    if known_error && attach.unsettled.is_none() {
                                        refuse_session(channel, "amqp:session:errant-link", "pipelined attach for an error-detached link", &mut writer, &mut sessions).await?;
                                        fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        continue;
                                    }
                                    if let Some(pending) = pending_attaches.get(&attach.name, &local_role) {
                                        // A directional request elsewhere owns this response name;
                                        // never consume a current-session opposite-role request instead.
                                        if !pending_attach_matches(pending, channel, &sessions) {
                                            continue;
                                        }
                                    } else {
                                        if pending_attaches.get(&attach.name, &local_role.opposite()).is_some_and(|pending| pending_attach_matches(pending, channel, &sessions)) {
                                            refuse_session(channel, "amqp:invalid-field", "attach response has the wrong role", &mut writer, &mut sessions).await?;
                                            fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        } else if known_error {
                                            refuse_session(channel, "amqp:not-implemented", RECOVERY_NOT_IMPLEMENTED, &mut writer, &mut sessions).await?;
                                            fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        }
                                        continue;
                                    }
                                    if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                                        refuse_session(channel, "amqp:invalid-field", "sender attach has no initial delivery count", &mut writer, &mut sessions).await?;
                                        fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        continue;
                                    }
                                    let historical_recovery = known_error && attach.unsettled.is_some();
                                    if !historical_recovery && attach_uses_transactions(&attach) {
                                        refuse_session(channel, "amqp:not-implemented", TRANSACTIONS_NOT_IMPLEMENTED, &mut writer, &mut sessions).await?;
                                        fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                        continue;
                                    }
                                    let default_outcome = match if historical_recovery { Ok(None) } else { source_default_outcome(attach.source.as_ref()) } {
                                        Ok(outcome) => outcome,
                                        Err(_) => {
                                            refuse_session(channel, "amqp:invalid-field", "source default outcome must be an ordinary terminal outcome", &mut writer, &mut sessions).await?;
                                            fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                            continue;
                                        }
                                    };
                                    let recovery_reply = if has_recovery_state(&attach) || known_error {
                                        let pending = pending_attaches.get(&attach.name, &local_role).expect("validated pending attach");
                                        let session = sessions.get(&channel).expect("validated pending session");
                                        let snapshot = match snapshot_error_histories(session, pending.handle, pending.link.identity(), Some(attach.handle), &writer) {
                                            Ok(snapshot) => snapshot,
                                            Err(error) => {
                                                refuse_session(channel, "amqp:resource-limit-exceeded", error.to_string(), &mut writer, &mut sessions).await?;
                                                fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                                continue;
                                            }
                                        };
                                        let refusal = Frame::Amqp {
                                            channel,
                                            performative: Some(Performative::Detach(Detach {
                                                handle: pending.handle,
                                                closed: true,
                                                error: Some(Error::new(crate::AmqpError::NotImplemented, RECOVERY_NOT_IMPLEMENTED, None)),
                                            })),
                                            payload: Vec::new(),
                                        };
                                        writer.encoded_frame(&refusal)?;
                                        if !session.local_begin_sent { writer.encoded_frame(&local_begin_frame(channel, session)?)?; }
                                        Some((refusal, snapshot))
                                    } else { None };
                                    if let Some(pending) = pending_attaches.remove(&attach.name, &local_role) {
                                        let session = sessions.get_mut(&channel).expect("validated pending session");
                                        session.handle_aliases.get_mut(&pending.handle).expect("validated pending handle alias").peer_handle = Some(attach.handle);
                                        session.error_peer_handles.reassign(attach.handle);
                                        if let Some((_, snapshot)) = &recovery_reply { commit_error_histories(session, snapshot, &mut writer)?; }
                                        let pending_flow = sessions
                                            .get_mut(&channel)
                                            .and_then(|session| session.pending_attaches.remove(&pending.handle));
                                        let mut link = pending.link;
                                        if let Some((refusal, _)) = recovery_reply {
                                            let identity = link.identity().clone();
                                            stop_link(&mut link);
                                            let session = sessions.get_mut(&channel).ok_or_else(|| invalid_state("attach on an unknown session"))?;
                                            remember_closing_handle(session, pending.handle)?;
                                            mark_error_detached(session, pending.handle, &identity);
                                            writer.write_frame(&refusal).await?;
                                            let _ = pending.reply.send(Err(EngineError::RemoteDetached));
                                            continue;
                                        }
                                        match &mut link {
                                            LinkState::Sending(link) if attach.role == Role::Receiver => {
                                                link.max_message_size = normalized_message_size(attach.max_message_size);
                                                link.receiver_settle_mode = attach.rcv_settle_mode.clone();
                                                link.default_outcome = default_outcome;
                                                if let Some(pending_flow) = &pending_flow {
                                                    link.credit = pending_flow.credit.clone();
                                                }
                                            },
                                            LinkState::Receiving(receiving) if attach.role == Role::Sender => {
                                                let Some(initial) = attach.initial_delivery_count else {
                                                    stop_link(&mut link);
                                                    let _ = pending.reply.send(Err(invalid_state("sender attach has no initial delivery count")));
                                                    refuse_session(channel, "amqp:invalid-field", "sender attach has no initial delivery count", &mut writer, &mut sessions).await?;
                                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                                    continue;
                                                };
                                                if pending_flow.as_ref().and_then(|flow| flow.initial_sender_count).is_some_and(|count| count != initial) {
                                                    stop_link(&mut link);
                                                    let _ = pending.reply.send(Err(invalid_state("sender attach disagrees with the pending delivery count")));
                                                    refuse_session(channel, "amqp:invalid-field", "sender attach disagrees with the pending delivery count", &mut writer, &mut sessions).await?;
                                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                                    continue;
                                                }
                                                receiving.credit = ReceiveCredit::new(initial, LINK_CREDIT, pending.consumption);
                                                receiving.sender_settle_mode = attach.snd_settle_mode.clone();
                                            }
                                            _ => {
                                                stop_link(&mut link);
                                                let _ = pending.reply.send(Err(invalid_state("attach response has the wrong role")));
                                                refuse_session(channel, "amqp:invalid-field", "attach response has the wrong role", &mut writer, &mut sessions).await?;
                                                fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                                continue;
                                            }
                                        }
                                        let session = sessions.get_mut(&channel).ok_or_else(|| invalid_state("attach on an unknown session"))?;
                                        session.links.insert(pending.handle, link);
                                        refill_link(channel, pending.handle, session, &mut writer).await?;
                                        let flow = pending_flow.and_then(|pending| pending.latest);
                                        if let Some(flow) = flow { apply_link_flow(channel, flow, &mut writer, &mut sessions).await?; }
                                        let _ = pending.reply.send(Ok((pending.handle, attach)));
                                    }
                                    Ok(false)
                                }
                                Performative::Flow(flow) => {
                                    apply_flow(
                                        channel,
                                        flow,
                                        &mut writer,
                                        &mut sessions,
                                        remote_max_frame_size,
                                    ).await?;
                                    Ok(false)
                                }
                                Performative::Transfer(transfer) => {
                                    receive_transfer(channel, transfer, payload, &mut sessions, &mut writer).await?;
                                    Ok(false)
                                }
                                Performative::Disposition(disposition) => {
                                    apply_disposition(channel, disposition, &mut writer, &mut sessions).await?;
                                    Ok(false)
                                }
                                Performative::Detach(detach) => {
                                    let alias_identity = sessions.get(&channel).and_then(|session| session.handle_aliases.get(&detach.handle)).map(|alias| alias.identity.clone());
                                    let pending_name = pending_attaches.iter().find_map(|(name, role, pending)| {
                                        (pending.channel == channel && pending.handle == detach.handle).then_some((name.to_owned(), role))
                                    });
                                    if let Some((name, role)) = pending_name {
                                        let mut pending = pending_attaches.remove(&name, &role).expect("pending attach exists");
                                        stop_link(&mut pending.link);
                                        let _ = pending.reply.send(Err(EngineError::RemoteDetached));
                                        if let Some(session) = sessions.get_mut(&channel) {
                                            session.pending_attaches.remove(&detach.handle);
                                        }
                                    }
                                    let local_reply = pending_detaches.remove(&(channel, detach.handle));
                                    let locally_closing = sessions
                                        .get_mut(&channel)
                                        .is_some_and(|session| session.closing_handles.remove(&detach.handle));
                                    if let Some(session) = sessions.get_mut(&channel)
                                        && let Some(mut link) = session.links.remove(&detach.handle)
                                    {
                                        forget_incoming_link(&mut session.incoming, &link);
                                        stop_link(&mut link);
                                    }
                                    if let Some(reply) = local_reply {
                                        let _ = reply.send(Ok(()));
                                    } else if !locally_closing {
                                        writer.write_amqp(channel,
                                            Performative::Detach(Detach {
                                                handle: detach.handle,
                                                closed: true,
                                                error: None,
                                            }),
                                            Vec::new(),
                                        ).await?;
                                    }
                                    if let Some(session) = sessions.get_mut(&channel)
                                        && let Some(identity) = alias_identity
                                    {
                                        remove_handle_alias(session, detach.handle, &identity);
                                    }
                                    Ok(false)
                                }
                                Performative::End(_) => {
                                    let mut acknowledge = false;
                                    let mut completed = None;
                                    if let Some(mut session) = sessions.remove(&channel) {
                                        acknowledge = !session.ending;
                                        if pending_ends.get(&channel).is_some_and(|pending| session.identity.same_session(&pending.identity)) {
                                            completed = pending_ends.remove(&channel);
                                        }
                                        stop_session(&mut session);
                                    }
                                    next_handles.remove(&channel);
                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                    if acknowledge {
                                        writer.write_amqp(channel, Performative::End(End::default()), Vec::new()).await?;
                                    }
                                    if let Some(pending) = completed {
                                        let _ = pending.reply.send(Ok(()));
                                    }
                                    Ok(false)
                                }
                            Performative::Close(_) => {
                                if !activity.is_closing() {
                                        writer.write_amqp(0,
                                            Performative::Close(Close::default()),
                                            Vec::new(),
                                    ).await?;
                                } else {
                                    for reply in closing.drain(..) {
                                        let _ = reply.send(Ok(()));
                                    }
                                }
                                commands.close();
                                while let Ok(command) = commands.try_recv() {
                                    if let ClientCommand::Close { reply } = command {
                                        let _ = reply.send(Ok(()));
                                    } else {
                                        reject_closed_client_command(command);
                                    }
                                }
                                Ok(true)
                                }
                                Performative::Open(_) => Err(invalid_state("duplicate AMQP open")),
                            };
                            match result {
                                Ok(true) => break,
                                Ok(false) => {
                                    if sessions.get(&channel).is_some_and(|session| session.ending)
                                        && !pending_ends.contains_key(&channel)
                                    {
                                        fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                    }
                                    pump_ready = !activity.is_closing();
                                }
                                Err(_) => break,
                            }
                        }
                    command = commands.recv() => {
                        let Some(command) = command else { break };
                        let command = match command {
                            ClientCommand::Close { .. } => command,
                            command if activity.is_closing() => {
                                reject_closed_client_command(command);
                                continue;
                            }
                            command => command,
                        };
                            let result: Result<(), EngineError> = match command {
                                ClientCommand::Begin { reply } => {
                                    if sessions.len() >= MAX_SESSIONS_PER_CONNECTION {
                                        let _ = reply.send(Err(invalid_state("connection session limit reached")));
                                        continue;
                                    }
                                    let Some(channel) = vacant_channel(next_channel, settings.remote_channel_max, &sessions) else {
                                        let _ = reply.send(Err(invalid_state("peer session channel limit reached")));
                                        continue;
                                    };
                                    let frame = Frame::Amqp { channel, performative: Some(Performative::Begin(Begin::default())), payload: Vec::new() };
                                    if let Err(error) = writer.encoded_frame(&frame) {
                                        let _ = reply.send(Err(error.into()));
                                        continue;
                                    }
                                    let session = SessionState::for_connection(&Begin { incoming_window: 0, outgoing_window: 0, ..Begin::default() }, connection);
                                    let identity = session.identity.clone();
                                    sessions.insert(channel, session);
                                    next_handles.insert(channel, 0);
                                    pending_begins.insert(channel, PendingBegin { identity, reply });
                                    next_channel = if channel == settings.remote_channel_max { 0 } else { channel + 1 };
                                    writer.write_frame(&frame).await?;
                                    sessions.get_mut(&channel).expect("initiated session exists").local_begin_sent = true;
                                    Ok(())
                                }
                                ClientCommand::Attach {
                                    channel,
                                    session: owner,
                                    request,
                                    deliveries_tx,
                                    detached_tx,
                                    consumption,
                                    identity,
                                    reply,
                                } => {
                                    if owner.is_retired() {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    }
                                    let connection_slots = connection_link_slot_count(&sessions);
                                    let name_in_use = connection_link_name_in_use(&sessions, &request.name, &request.role);
                                    let Some(session) = sessions.get_mut(&channel) else {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    };
                                    if session.ending || session.identity.is_retired() {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    }
                                    if !session.identity.same_session(&owner) {
                                        let _ = reply.send(Err(invalid_state("attach belongs to a different session generation")));
                                        continue;
                                    }
                                    let request = *request;
                                    if writer.error_link_names().contains(&request.name, &request.role) {
                                        let _ = reply.send(Err(invalid_state(RECOVERY_NOT_IMPLEMENTED)));
                                        continue;
                                    }
                                    if name_in_use {
                                        let _ = reply.send(Err(invalid_state("link name is already assigned")));
                                        continue;
                                    }
                                    if request.target.as_ref().is_some_and(|target| target.as_coordinator().is_some()) {
                                        let _ = reply.send(Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED)));
                                        continue;
                                    }
                                    if let Err(error) = source_default_outcome(request.source.as_ref()) {
                                        let _ = reply.send(Err(error));
                                        continue;
                                    }
                                    let receive_maximum = effective_receive_maximum(request.max_message_size);
                                    let next_handle = next_handles
                                        .get_mut(&channel)
                                        .expect("live initiated session has a handle counter");
                                    if session.pending_attaches.len() == MAX_PENDING_ATTACHES || pending_attaches.contains_key(&request.name, &request.role) {
                                        let _ = reply.send(Err(invalid_state("pending attach limit reached or name is already assigned")));
                                        continue;
                                    }
                                    if link_slot_count(session) >= MAX_LINKS_PER_SESSION || connection_slots >= MAX_LINKS_PER_CONNECTION {
                                        let _ = reply.send(Err(invalid_state("link lifecycle slot limit reached")));
                                        continue;
                                    }
                                    let Some(handle) = vacant_handle(*next_handle, session.remote_handle_max, session) else {
                                        let _ = reply.send(Err(invalid_state("peer link handle limit reached")));
                                        continue;
                                    };
                                    let attach = Attach {
                                        name: request.name.clone(),
                                        handle,
                                        role: request.role.clone(),
                                        snd_settle_mode: request.sender_settle_mode.clone(),
                                        rcv_settle_mode: request.receiver_settle_mode.clone(),
                                        source: request.source,
                                        target: request.target,
                                        unsettled: None,
                                        incomplete_unsettled: false,
                                        initial_delivery_count: (request.role == Role::Sender).then_some(0),
                                        max_message_size: if request.role == Role::Receiver { Some(receive_maximum) } else { request.max_message_size },
                                        offered_capabilities: None,
                                        desired_capabilities: None,
                                        properties: None,
                                    };
                                    let peer_role = attach.role.opposite();
                                    let attach_frame = Frame::Amqp { channel, performative: Some(Performative::Attach(Box::new(attach))), payload: Vec::new() };
                                    if let Err(error) = writer.encoded_frame(&attach_frame) {
                                        let _ = reply.send(Err(error.into()));
                                        continue;
                                    }
                                    *next_handle = if handle == session.remote_handle_max { 0 } else { handle + 1 };
                                    session.handle_aliases.insert(handle, HandleAlias { identity: identity.clone(), name: Arc::from(request.name.as_str()), role: request.role.clone(), peer_handle: None, own_attach_sent: false, error_detached: false });
                                    let link = match request.role {
                                        Role::Sender => {
                                            LinkState::Sending(Box::new(SendingLink {
                                                identity,
                                                auto_acknowledge: true,
                                                max_message_size: None,
                                                receiver_settle_mode: request.receiver_settle_mode,
                                                default_outcome: None,
                                                outstanding_tags: HashSet::new(),
                                                settle_mode: request.sender_settle_mode,
                                                credit: LinkCredit::new(0),
                                                queued: VecDeque::new(),
                                                active: None,
                                                unsettled: HashMap::new(),
                                                pending_acknowledgements: HashMap::new(),
                                                detached: detached_tx,
                                            }))
                                        }
                                        Role::Receiver => {
                                            LinkState::Receiving(ReceivingLink {
                                                max_message_size: receive_maximum,
                                                deliveries: deliveries_tx,
                                                partial: None,
                                                detached: detached_tx,
                                                credit: ReceiveCredit::new(0, LINK_CREDIT, consumption.clone()),
                                                decoders: MessageFormatDecoders::default(),
                                                identity,
                                                sender_settle_mode: request.sender_settle_mode,
                                                receiver_settle_mode: request.receiver_settle_mode,
                                            })
                                        }
                                    };
                                    session.pending_attaches.insert(handle, PendingLinkFlow::new(peer_role, None));
                                    pending_attaches.insert(request.name, PendingAttach { channel, session: owner, handle, reply, link, consumption });
                                    writer.write_frame(&attach_frame).await?;
                                    session.handle_aliases.get_mut(&handle).expect("published pending handle alias").own_attach_sent = true;
                                    Ok(())
                                }
                                ClientCommand::Send {
                                    channel,
                                    handle,
                                    identity,
                                    message,
                                    delivery_tag,
                                    message_format,
                                    reply,
                                } => {
                                    let Some(session) = sessions.get_mut(&channel) else {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    };
                                    queue_send(
                                        channel,
                                        handle,
                                        session,
                                        &identity,
                                        *message,
                                        delivery_tag,
                                        message_format,
                                        reply,
                                        &mut writer,
                                        remote_max_frame_size,
                                    ).await
                                }
                                ClientCommand::Settle {
                                    channel,
                                    handle,
                                    identity,
                                    state,
                                    reply,
                                } => {
                                    settle_incoming(channel, handle, identity, state, reply, &mut sessions, &mut writer).await
                                }
                                ClientCommand::Detach { channel, handle, identity, reply } => {
                                    if identity.is_retired() {
                                        let _ = reply.send(Ok(()));
                                        continue;
                                    }
                                    let Some(session) = sessions.get_mut(&channel) else {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    };
                                    let Some(link) = session.links.get(&handle) else {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    };
                                    if !link.identity().same_link(&identity) {
                                        let _ = reply.send(Err(invalid_state("close belongs to a different link generation")));
                                        continue;
                                    }
                                    if session.ending {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    }
                                    let frame = Frame::Amqp {
                                        channel,
                                        performative: Some(Performative::Detach(Detach {
                                            handle,
                                            closed: true,
                                            error: None,
                                        })),
                                        payload: Vec::new(),
                                    };
                                    if let Err(error) = writer.encoded_frame(&frame) {
                                        let _ = reply.send(Err(error.into()));
                                        continue;
                                    }
                                    remember_closing_handle(session, handle)?;
                                    let mut link = session.links.remove(&handle).expect("validated close endpoint");
                                    forget_incoming_link(&mut session.incoming, &link);
                                    stop_link(&mut link);
                                    pending_detaches.insert((channel, handle), reply);
                                    writer.write_frame(&frame).await.map_err(Into::into)
                                }
                                ClientCommand::End { channel, identity, reply } => {
                                    if identity.is_retired() {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    }
                                    let Some(session) = sessions.get_mut(&channel) else {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    };
                                    if session.ending || session.identity.is_retired() {
                                        let _ = reply.send(Err(EngineError::RemoteDetached));
                                        continue;
                                    }
                                    if !session.identity.same_session(&identity) {
                                        let _ = reply.send(Err(invalid_state("end belongs to a different session generation")));
                                        continue;
                                    }
                                    let frame = Frame::Amqp { channel, performative: Some(Performative::End(End::default())), payload: Vec::new() };
                                    if let Err(error) = writer.encoded_frame(&frame) {
                                        let _ = reply.send(Err(error.into()));
                                        continue;
                                    }
                                    session.ending = true;
                                    stop_session(session);
                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches, &mut pending_ends);
                                    pending_ends.insert(channel, PendingEnd { identity, reply });
                                    writer.write_frame(&frame).await.map_err(Into::into)
                                }
                            ClientCommand::Close { reply } => {
                                let send_close = !activity.is_closing();
                                closing.push(reply);
                                if send_close {
                                    writer.write_amqp(0,
                                        Performative::Close(Close::default()),
                                        Vec::new(),
                                    ).await.map_err(Into::into)
                                } else {
                                    Ok(())
                                }
                                }
                            };
                            if result.is_err() {
                                break;
                            }
                            pump_ready = !activity.is_closing();
                        }
                        () = consumed.notified(), if !activity.is_closing() => {
                            refresh_consumed(&mut writer, &mut sessions).await?;
                            pump_ready = true;
                        }
                        () = tokio::task::yield_now(), if pump_ready && !activity.is_closing() => {
                            pump_ready = pump_connection(&mut writer, &mut sessions, &mut pump_cursor).await?;
                        }
                    }
                }
                Ok::<(), EngineError>(())
            };
            tokio::select! {
                biased;
                () = wait_for_detach(&mut cancellation) => (Ok(()), None),
                reason = activity.timeout(settings.options, settings.peer_idle_millis) => (Ok(()), Some(reason)),
                result = processing => (result, None),
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
        break result;
    };

    reader_task.shutdown().await;

    for session in sessions.values_mut() {
        stop_session(session);
    }
    for (_, pending) in pending_begins {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::Stopped));
    }
    for (_, mut pending) in pending_attaches.drain() {
        stop_link(&mut pending.link);
        let _ = pending.reply.send(Err(EngineError::Stopped));
    }
    for (_, reply) in pending_detaches {
        let _ = reply.send(Err(EngineError::Stopped));
    }
    for (_, pending) in pending_ends {
        pending.identity.retire();
        let _ = pending.reply.send(Err(EngineError::RemoteClosed));
    }
    for reply in closing {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
    let _ = closed.send(true);
    result
}

#[cfg(test)]
#[path = "client_channel_tests.rs"]
mod channel_tests;

#[cfg(test)]
#[path = "link_handle_client_tests.rs"]
mod handle_tests;

#[cfg(test)]
#[path = "error_link_client_tests.rs"]
mod error_link_tests;

#[cfg(test)]
#[path = "live_name_client_tests.rs"]
mod live_name_tests;

#[cfg(test)]
#[path = "transaction_client_tests.rs"]
mod transaction_tests;
