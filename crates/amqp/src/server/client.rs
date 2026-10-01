use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot, watch};
use url::Url;

use super::*;
use crate::{Source, Target};

pub struct ClientConnection {
    commands: mpsc::Sender<ClientCommand>,
    closed: watch::Receiver<bool>,
    lifecycle: ConnectionLifecycle,
    close_timeout: Duration,
    consumed: Arc<Notify>,
}

pub struct ClientSession {
    channel: u16,
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
        let (lifecycle, cancellation, terminated) = ConnectionLifecycle::new();
        tokio::spawn(async move {
            let _ = run_client(
                stream,
                ConnectionSettings {
                    remote_max_frame_size,
                    local_max_frame_size,
                    channel_max,
                    options,
                    peer_idle_millis,
                },
                command_rx,
                closed_tx,
                driver_consumed,
                cancellation,
            )
            .await;
            let _ = terminated.send(true);
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
        let channel =
            client_request(&self.commands, |reply| ClientCommand::Begin { reply }).await?;
        Ok(ClientSession {
            channel,
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
        let name = name.into();
        let (deliveries_tx, _) = mpsc::channel(1);
        let (detached_tx, detached) = watch::channel(false);
        let identity = LinkIdentity::new();
        let (handle, _) = client_request(&self.commands, |reply| ClientCommand::Attach {
            channel: self.channel,
            request: Box::new(AttachRequest {
                name,
                role: Role::Sender,
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(target),
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
        let name = name.into();
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let consumption = Arc::new(Consumption::new(self.consumed.clone()));
        let identity = LinkIdentity::new();
        let (detached_tx, detached) = watch::channel(false);
        let (handle, response) = client_request(&self.commands, |reply| ClientCommand::Attach {
            channel: self.channel,
            request: Box::new(AttachRequest {
                name,
                role: Role::Receiver,
                sender_settle_mode,
                receiver_settle_mode,
                source: Some(source),
                target,
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
        client_request(&self.commands, |reply| ClientCommand::End {
            channel: self.channel,
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

    /// Advertises the encoded-message limit; zero leaves the link unlimited.
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
    target: Option<Target>,
    max_message_size: Option<u64>,
}

enum ClientCommand {
    Begin {
        reply: oneshot::Sender<Result<u16, EngineError>>,
    },
    Attach {
        channel: u16,
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
    handle: u32,
    reply: oneshot::Sender<Result<(u32, Attach), EngineError>>,
    link: LinkState,
    consumption: Arc<Consumption>,
}

fn fail_pending_session(
    channel: u16,
    pending_begins: &mut HashMap<u16, oneshot::Sender<Result<u16, EngineError>>>,
    pending_attaches: &mut HashMap<String, PendingAttach>,
    pending_detaches: &mut HashMap<(u16, u32), oneshot::Sender<Result<(), EngineError>>>,
) {
    if let Some(reply) = pending_begins.remove(&channel) {
        let _ = reply.send(Err(EngineError::RemoteDetached));
    }
    let names: Vec<_> = pending_attaches
        .iter()
        .filter_map(|(name, pending)| (pending.channel == channel).then_some(name.clone()))
        .collect();
    for name in names {
        let mut pending = pending_attaches
            .remove(&name)
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

async fn run_client<Io>(
    stream: Io,
    settings: ConnectionSettings,
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
    let mut next_channel = 0_u16;
    let mut next_handles = HashMap::<u16, u32>::new();
    let mut pending_begins = HashMap::<u16, oneshot::Sender<Result<u16, EngineError>>>::new();
    let mut pending_attaches = HashMap::<String, PendingAttach>::new();
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
                        let Some(performative) = performative else { continue };
                        if activity.is_closing() && !matches!(&performative, Performative::Close(_)) {
                            continue;
                        }
                        if sessions.get(&channel).is_some_and(|session| session.ending)
                            && !matches!(&performative, Performative::End(_) | Performative::Close(_))
                        { continue; }
                            let result = match performative {
                                Performative::Begin(begin) => {
                                    if let Some(reply) = pending_begins.remove(&channel) {
                                        if let Some(session) = sessions.get_mut(&channel) {
                                            session.flow = SessionWindow::new(0, begin.next_outgoing_id, begin.incoming_window, begin.outgoing_window, SESSION_WINDOW);
                                        }
                                        let _ = reply.send(Ok(channel));
                                    }
                                    Ok(false)
                                }
                                Performative::Attach(attach) => {
                                    let attach = *attach;
                                    if let Some(pending) = pending_attaches.remove(&attach.name) {
                                        let pending_flow = sessions
                                            .get_mut(&channel)
                                            .and_then(|session| session.pending_attaches.remove(&pending.handle));
                                        let mut link = pending.link;
                                        if has_recovery_state(&attach) {
                                            stop_link(&mut link);
                                            let session = sessions.get_mut(&channel).ok_or_else(|| invalid_state("attach on an unknown session"))?;
                                            remember_closing_handle(session, pending.handle)?;
                                            writer.write_amqp(channel,
                                                Performative::Detach(Detach {
                                                    handle: pending.handle,
                                                    closed: true,
                                                    error: Some(Error::new(crate::AmqpError::NotImplemented, RECOVERY_NOT_IMPLEMENTED, None)),
                                                }),
                                                Vec::new(),
                                            ).await?;
                                            let _ = pending.reply.send(Err(EngineError::RemoteDetached));
                                            continue;
                                        }
                                        match &mut link {
                                            LinkState::Sending(link) if attach.role == Role::Receiver => {
                                                link.max_message_size = normalized_message_size(attach.max_message_size);
                                                link.receiver_settle_mode = attach.rcv_settle_mode.clone();
                                                if let Some(pending_flow) = &pending_flow {
                                                    link.credit = pending_flow.credit.clone();
                                                }
                                            },
                                            LinkState::Receiving(link) if attach.role == Role::Sender => {
                                                let Some(initial) = attach.initial_delivery_count else {
                                                    let _ = pending.reply.send(Err(invalid_state("sender attach has no initial delivery count")));
                                                    refuse_session(channel, "amqp:invalid-field", "sender attach has no initial delivery count", &mut writer, &mut sessions).await?;
                                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
                                                    continue;
                                                };
                                                if pending_flow.as_ref().and_then(|flow| flow.initial_sender_count).is_some_and(|count| count != initial) {
                                                    let _ = pending.reply.send(Err(invalid_state("sender attach disagrees with the pending delivery count")));
                                                    refuse_session(channel, "amqp:invalid-field", "sender attach disagrees with the pending delivery count", &mut writer, &mut sessions).await?;
                                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
                                                    continue;
                                                }
                                                link.credit = ReceiveCredit::new(initial, LINK_CREDIT, pending.consumption);
                                                link.sender_settle_mode = attach.snd_settle_mode.clone();
                                            }
                                            _ => {
                                                let _ = pending.reply.send(Err(invalid_state("attach response has the wrong role")));
                                                refuse_session(channel, "amqp:invalid-field", "attach response has the wrong role", &mut writer, &mut sessions).await?;
                                                fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
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
                                    let pending_name = pending_attaches.iter().find_map(|(name, pending)| {
                                        (pending.channel == channel && pending.handle == detach.handle).then_some(name.clone())
                                    });
                                    if let Some(name) = pending_name {
                                        let mut pending = pending_attaches.remove(&name).expect("pending attach exists");
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
                                    Ok(false)
                                }
                                Performative::End(_) => {
                                    let mut acknowledge = false;
                                    if let Some(mut session) = sessions.remove(&channel) {
                                        acknowledge = !session.ending;
                                        for link in session.links.values_mut() {
                                            stop_link(link);
                                        }
                                    }
                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
                                    if acknowledge {
                                        writer.write_amqp(channel, Performative::End(End::default()), Vec::new()).await?;
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
                                    if sessions.get(&channel).is_some_and(|session| session.ending) {
                                        fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
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
                                    let channel = next_channel;
                                    next_channel = next_channel.wrapping_add(1);
                                    sessions.insert(channel, SessionState::new(&Begin { incoming_window: 0, outgoing_window: 0, ..Begin::default() }));
                                    next_handles.insert(channel, 0);
                                    pending_begins.insert(channel, reply);
                                    writer.write_amqp(channel,
                                        Performative::Begin(Begin::default()),
                                        Vec::new(),
                                    ).await?;
                                    sessions.get_mut(&channel).expect("initiated session exists").local_begin_sent = true;
                                    Ok(())
                                }
                                ClientCommand::Attach {
                                    channel,
                                    request,
                                    deliveries_tx,
                                    detached_tx,
                                    consumption,
                                    identity,
                                    reply,
                                } => {
                                    let request = *request;
                                    let next_handle = next_handles
                                        .get_mut(&channel)
                                        .ok_or_else(|| invalid_state("attach on an unknown session"))?;
                                    let handle = *next_handle;
                                    *next_handle = next_handle.wrapping_add(1);
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
                                        max_message_size: request.max_message_size,
                                        offered_capabilities: None,
                                        desired_capabilities: None,
                                        properties: None,
                                    };
                                    let session = sessions
                                        .get_mut(&channel)
                                        .ok_or_else(|| invalid_state("attach on an unknown session"))?;
                                    if session.links.contains_key(&handle) || session.closing_handles.contains(&handle) {
                                        let _ = reply.send(Err(invalid_state("link handle is attached or awaiting detach acknowledgement")));
                                        continue;
                                    }
                                    if session.ending || session.pending_attaches.len() == MAX_PENDING_ATTACHES || pending_attaches.contains_key(&request.name) {
                                        let _ = reply.send(Err(invalid_state("pending attach limit reached or name is already assigned")));
                                        continue;
                                    }
                                    let peer_role = attach.role.opposite();
                                    let attach_frame = Frame::Amqp { channel, performative: Some(Performative::Attach(Box::new(attach))), payload: Vec::new() };
                                    if let Err(error) = writer.encoded_frame(&attach_frame) {
                                        let _ = reply.send(Err(error.into()));
                                        continue;
                                    }
                                    let link = match request.role {
                                        Role::Sender => {
                                            LinkState::Sending(Box::new(SendingLink {
                                                identity,
                                                auto_acknowledge: true,
                                                max_message_size: None,
                                                receiver_settle_mode: request.receiver_settle_mode,
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
                                                max_message_size: normalized_message_size(request.max_message_size)
                                                    .unwrap_or(u64::MAX),
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
                                    pending_attaches.insert(request.name, PendingAttach { channel, handle, reply, link, consumption });
                                    writer.write_frame(&attach_frame).await.map_err(Into::into)
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
                                ClientCommand::End { channel, reply } => {
                                    let result = writer.write_amqp(channel,
                                        Performative::End(End::default()),
                                        Vec::new(),
                                    ).await;
                                    if let Some(session) = sessions.get_mut(&channel) {
                                        session.ending = true;
                                        for link in session.links.values_mut() {
                                            stop_link(link);
                                        }
                                        session.links.clear();
                                        session.pending_attaches.clear();
                                    }
                                    fail_pending_session(channel, &mut pending_begins, &mut pending_attaches, &mut pending_detaches);
                                    let _ = reply.send(result.as_ref().map(|_| ()).map_err(|error| {
                                        EngineError::InvalidState(error.to_string())
                                    }));
                                    result.map_err(Into::into)
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
        for link in session.links.values_mut() {
            stop_link(link);
        }
    }
    for (_, reply) in pending_begins {
        let _ = reply.send(Err(EngineError::Stopped));
    }
    for (_, pending) in pending_attaches {
        let _ = pending.reply.send(Err(EngineError::Stopped));
    }
    for (_, reply) in pending_detaches {
        let _ = reply.send(Err(EngineError::Stopped));
    }
    for reply in closing {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
    let _ = closed.send(true);
    result
}
