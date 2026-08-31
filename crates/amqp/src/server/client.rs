use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot, watch};
use url::Url;

use super::*;
use crate::{Source, Target};

pub struct ClientConnection {
    commands: mpsc::Sender<ClientCommand>,
    closed: watch::Receiver<bool>,
}

pub struct ClientSession {
    channel: u16,
    commands: mpsc::Sender<ClientCommand>,
}

pub struct ClientSender {
    channel: u16,
    handle: u32,
    incarnation: u64,
    next_tag: u64,
    commands: mpsc::Sender<ClientCommand>,
    detached: watch::Receiver<bool>,
    send_capacity: Arc<Semaphore>,
}

pub struct ClientReceiver {
    channel: u16,
    handle: u32,
    incarnation: u64,
    source: Option<Source>,
    commands: mpsc::Sender<ClientCommand>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
}

pub type ClientDelivery = Delivery;

pub struct ClientConnectionBuilder {
    container_id: String,
    sasl: Option<SaslInit>,
}

pub struct ClientReceiverBuilder {
    name: Option<String>,
    source: Option<Source>,
    target: Option<Target>,
    sender_settle_mode: SenderSettleMode,
}

impl ClientConnection {
    pub fn builder() -> ClientConnectionBuilder {
        ClientConnectionBuilder {
            container_id: String::from("amqp-client"),
            sasl: None,
        }
    }

    pub async fn open<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<SaslInit>,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if let Some(init) = sasl {
            write_protocol_header(&mut stream, ProtocolHeader::SASL).await?;
            expect_header(&mut stream, ProtocolHeader::SASL).await?;
            let mechanisms = match read_frame(&mut stream).await? {
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
            write_frame(&mut stream, &Frame::Sasl(SaslPerformative::Init(init))).await?;
            let outcome = match read_frame(&mut stream).await? {
                Frame::Sasl(SaslPerformative::Outcome(outcome)) => outcome,
                _ => return Err(invalid_state("expected SASL outcome")),
            };
            if outcome.code != SaslCode::Ok {
                return Err(EngineError::SaslAuthentication(outcome.code));
            }
        }

        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        expect_header(&mut stream, ProtocolHeader::AMQP).await?;
        write_amqp(
            &mut stream,
            0,
            Performative::Open(Open::new(container_id)),
            Vec::new(),
        )
        .await?;
        let remote_open = match read_frame(&mut stream).await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(open)),
                ..
            } => open,
            _ => return Err(invalid_state("expected AMQP open")),
        };

        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        tokio::spawn(run_client(
            stream,
            remote_open.max_frame_size,
            command_rx,
            closed_tx,
        ));
        Ok(Self { commands, closed })
    }

    pub async fn begin(&mut self) -> Result<ClientSession, EngineError> {
        let channel =
            client_request(&self.commands, |reply| ClientCommand::Begin { reply }).await?;
        Ok(ClientSession {
            channel,
            commands: self.commands.clone(),
        })
    }

    pub async fn on_close(&mut self) {
        wait_for_detach(&mut self.closed).await;
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        client_request(&self.commands, |reply| ClientCommand::Close { reply }).await
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

    pub async fn open(self, url: &str) -> Result<ClientConnection, EngineError> {
        let url =
            Url::parse(url).map_err(|error| invalid_state(format!("invalid AMQP URL: {error}")))?;
        let host = url
            .host_str()
            .ok_or_else(|| invalid_state("AMQP URL has no host"))?;
        let port = url.port().unwrap_or(5672);
        let stream = tokio::net::TcpStream::connect((host, port)).await?;
        ClientConnection::open(stream, self.container_id, self.sasl).await
    }

    pub async fn open_with_stream<Io>(self, stream: Io) -> Result<ClientConnection, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        ClientConnection::open(stream, self.container_id, self.sasl).await
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
        let (drain_tx, _drains) = watch::channel(None);
        let (credit_tx, _credits) = watch::channel(false);
        let (handle, incarnation, _) =
            client_request(&self.commands, |reply| ClientCommand::Attach {
                channel: self.channel,
                request: Box::new(AttachRequest {
                    name,
                    role: Role::Sender,
                    sender_settle_mode: SenderSettleMode::Mixed,
                    receiver_settle_mode: ReceiverSettleMode::First,
                    source: None,
                    target: Some(target),
                }),
                deliveries_tx,
                detached_tx,
                drain_tx,
                credit_tx,
                reply,
            })
            .await?;
        Ok(ClientSender {
            channel: self.channel,
            handle,
            incarnation,
            next_tag: 0,
            commands: self.commands.clone(),
            detached,
            send_capacity: Arc::new(Semaphore::new(OUTGOING_DELIVERY_LIMIT)),
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
        let name = name.into();
        let (deliveries_tx, deliveries) = mpsc::channel(32);
        let (detached_tx, detached) = watch::channel(false);
        let (drain_tx, _drains) = watch::channel(None);
        let (credit_tx, _credits) = watch::channel(false);
        let (handle, incarnation, response) =
            client_request(&self.commands, |reply| ClientCommand::Attach {
                channel: self.channel,
                request: Box::new(AttachRequest {
                    name,
                    role: Role::Receiver,
                    sender_settle_mode,
                    receiver_settle_mode: ReceiverSettleMode::First,
                    source: Some(source),
                    target,
                }),
                deliveries_tx,
                detached_tx,
                drain_tx,
                credit_tx,
                reply,
            })
            .await?;
        Ok(ClientReceiver {
            channel: self.channel,
            handle,
            incarnation,
            source: response.source,
            commands: self.commands.clone(),
            deliveries,
            detached,
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
        self.send_with_format(message, 0).await
    }

    /// Sends a message with an explicit AMQP message-format value.
    ///
    /// This is primarily useful for interoperability tests of extension
    /// formats while retaining the ordinary decoded [`Message`] model.
    pub async fn send_with_format(
        &mut self,
        message: Message,
        message_format: u32,
    ) -> Result<Outcome, EngineError> {
        let tag = self.next_tag.to_be_bytes().to_vec().into();
        self.next_tag = self.next_tag.wrapping_add(1);
        let permit = Arc::clone(&self.send_capacity)
            .acquire_owned()
            .await
            .map_err(|_| EngineError::Stopped)?;
        let (started, start) = oneshot::channel();
        let (reply, outcome) = oneshot::channel();
        self.commands
            .send(ClientCommand::Send {
                channel: self.channel,
                handle: self.handle,
                incarnation: self.incarnation,
                message: Box::new(message),
                message_format,
                delivery_tag: tag,
                permit,
                started,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        start.await.map_err(|_| EngineError::Stopped)??;
        Ok(outcome.await.map_err(|_| EngineError::Stopped)??.outcome)
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        client_request(&self.commands, |reply| ClientCommand::Detach {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
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
        self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })
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
        if delivery.settled {
            return Ok(());
        }
        client_request(&self.commands, |reply| ClientCommand::Settle {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
            delivery_id: delivery.id,
            state,
            reply,
        })
        .await
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        client_request(&self.commands, |reply| ClientCommand::Detach {
            channel: self.channel,
            handle: self.handle,
            incarnation: self.incarnation,
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

    pub async fn attach(self, session: &mut ClientSession) -> Result<ClientReceiver, EngineError> {
        session
            .attach_receiver_with(
                self.name.unwrap_or_else(|| String::from("receiver")),
                self.source.unwrap_or_default(),
                self.target,
                self.sender_settle_mode,
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
        drain_tx: watch::Sender<Option<DrainRequest>>,
        credit_tx: watch::Sender<bool>,
        reply: oneshot::Sender<Result<(u32, u64, Attach), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        incarnation: u64,
        message: Box<Message>,
        message_format: u32,
        delivery_tag: DeliveryTag,
        permit: OwnedSemaphorePermit,
        started: oneshot::Sender<Result<DeliveryIdentity, EngineError>>,
        reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        incarnation: u64,
        delivery_id: u32,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        incarnation: u64,
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

struct PendingAttach {
    handle: u32,
    incarnation: u64,
    reply: oneshot::Sender<Result<(u32, u64, Attach), EngineError>>,
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
    remote_max_frame_size: u32,
    mut commands: mpsc::Receiver<ClientCommand>,
    closed: watch::Sender<bool>,
) -> Result<(), EngineError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (frames_tx, mut frames) = mpsc::channel(256);
    tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut reader).await;
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                break;
            }
        }
    });

    let (unused_attach_tx, _) = mpsc::channel(1);
    let mut sessions = HashMap::<u16, SessionState>::new();
    let mut next_channel = 0_u16;
    let mut next_handles = HashMap::<u16, u32>::new();
    let mut pending_begins = HashMap::<u16, oneshot::Sender<Result<u16, EngineError>>>::new();
    let mut pending_attaches = HashMap::<String, PendingAttach>::new();
    let mut pending_detaches =
        HashMap::<(u16, u32), oneshot::Sender<Result<(), EngineError>>>::new();
    let mut closing: Option<oneshot::Sender<Result<(), EngineError>>> = None;

    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(Ok(frame)) = frame else { break };
                let Frame::Amqp { channel, performative, payload } = frame else { break };
                let Some(performative) = performative else { continue };
                let result = match performative {
                    Performative::Begin(_) => {
                        if let Some(reply) = pending_begins.remove(&channel) {
                            let _ = reply.send(Ok(channel));
                        }
                        Ok(false)
                    }
                    Performative::Attach(attach) => {
                        let attach = *attach;
                        if let Some(pending) = pending_attaches.remove(&attach.name) {
                            if attach.role == Role::Sender {
                                write_amqp(
                                    &mut writer,
                                    channel,
                                    Performative::Flow(Flow {
                                        next_incoming_id: Some(0),
                                        incoming_window: SESSION_WINDOW,
                                        next_outgoing_id: 0,
                                        outgoing_window: SESSION_WINDOW,
                                        handle: Some(pending.handle),
                                        delivery_count: Some(0),
                                        link_credit: Some(LINK_CREDIT),
                                        ..Flow::default()
                                    }),
                                    Vec::new(),
                                ).await?;
                            }
                            let _ = pending.reply.send(Ok((
                                pending.handle,
                                pending.incarnation,
                                attach,
                            )));
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
                        receive_transfer(channel, transfer, payload, &mut sessions).await?;
                        Ok(false)
                    }
                    Performative::Disposition(disposition) => {
                        apply_disposition(channel, disposition, &mut sessions);
                        Ok(false)
                    }
                    Performative::Detach(detach) => {
                        let locally_initiated = pending_detaches.remove(&(channel, detach.handle));
                        if let Some(session) = sessions.get_mut(&channel)
                            && let Some(mut link) = session.links.remove(&detach.handle)
                        {
                            stop_link(&mut link);
                        }
                        if let Some(reply) = locally_initiated {
                            let _ = reply.send(Ok(()));
                        } else {
                            write_amqp(
                                &mut writer,
                                channel,
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
                        if let Some(mut session) = sessions.remove(&channel) {
                            for link in session.links.values_mut() {
                                stop_link(link);
                            }
                        }
                        Ok(false)
                    }
                    Performative::Close(_) => {
                        if let Some(reply) = closing.take() {
                            let _ = reply.send(Ok(()));
                        } else {
                            write_amqp(
                                &mut writer,
                                0,
                                Performative::Close(Close::default()),
                                Vec::new(),
                            ).await?;
                        }
                        Ok(true)
                    }
                    Performative::Open(_) => Err(invalid_state("duplicate AMQP open")),
                };
                match result {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(_) => break,
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                let result = match command {
                    ClientCommand::Begin { reply } => {
                        let channel = next_channel;
                        next_channel = next_channel.wrapping_add(1);
                        sessions.insert(channel, SessionState {
                            incarnation: next_session_incarnation(),
                            attach_tx: Some(unused_attach_tx.clone()),
                            pending_attaches: HashMap::new(),
                            links: HashMap::new(),
                            pending_flows: HashMap::new(),
                            next_outgoing_id: 0,
                        });
                        next_handles.insert(channel, 0);
                        pending_begins.insert(channel, reply);
                        write_amqp(
                            &mut writer,
                            channel,
                            Performative::Begin(Begin::default()),
                            Vec::new(),
                        ).await
                    }
                    ClientCommand::Attach {
                        channel,
                        request,
                        deliveries_tx,
                        detached_tx,
                        drain_tx,
                        credit_tx,
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
                            max_message_size: Some(usize::MAX as u64),
                            offered_capabilities: None,
                            desired_capabilities: None,
                            properties: None,
                        };
                        let session = sessions
                            .get_mut(&channel)
                            .ok_or_else(|| invalid_state("attach on an unknown session"))?;
                        let incarnation = match request.role {
                            Role::Sender => {
                                let drain = LinkDrain::new(drain_tx);
                                let incarnation = drain.incarnation;
                                session.links.insert(handle, LinkState::Sending(SendingLink {
                                    receiver_settle_mode: request.receiver_settle_mode,
                                    settle_mode: request.sender_settle_mode,
                                    delivery_count: 0,
                                    credit_limit: 0,
                                    queued: VecDeque::new(),
                                    unsettled: HashMap::new(),
                                    detached: detached_tx,
                                    drain,
                                    credit: credit_tx,
                                    credit_reservations: HashSet::new(),
                                    next_credit_reservation: 0,
                                }));
                                incarnation
                            }
                            Role::Receiver => {
                                let incarnation = next_link_incarnation();
                                session.links.insert(handle, LinkState::Receiving(ReceivingLink {
                                    incarnation,
                                    max_message_size: usize::MAX as u64,
                                    deliveries: deliveries_tx,
                                    partial: None,
                                    detached: detached_tx,
                                }));
                                incarnation
                            }
                        };
                        pending_attaches.insert(
                            request.name,
                            PendingAttach {
                                handle,
                                incarnation,
                                reply,
                            },
                        );
                        write_amqp(
                            &mut writer,
                            channel,
                            Performative::Attach(Box::new(attach)),
                            Vec::new(),
                        ).await
                    }
                    ClientCommand::Send {
                        channel,
                        handle,
                        incarnation,
                        message,
                        message_format,
                        delivery_tag,
                        permit,
                        started,
                        reply,
                    } => {
                        let Some(session) = sessions.get_mut(&channel) else {
                            let _ = started.send(Err(EngineError::RemoteDetached));
                            let _ = reply.send(Err(EngineError::RemoteDetached));
                            continue;
                        };
                        let Some(link) = session.links.get_mut(&handle) else {
                            let _ = started.send(Err(EngineError::RemoteDetached));
                            let _ = reply.send(Err(EngineError::RemoteDetached));
                            continue;
                        };
                        if link.incarnation() != incarnation {
                            let _ = started.send(Err(EngineError::RemoteDetached));
                            let _ = reply.send(Err(EngineError::RemoteDetached));
                            continue;
                        }
                        let LinkState::Sending(link) = link else {
                            let _ = started.send(Err(invalid_state("send on a receiving link")));
                            let _ = reply.send(Err(invalid_state("send on a receiving link")));
                            continue;
                        };
                        link.queued.push_back(QueuedSend {
                            message: *message,
                            message_format,
                            delivery_tag,
                            reservation: None,
                            permit,
                            started,
                            reply,
                        });
                        flush_sends(
                            channel,
                            handle,
                            session,
                            &mut writer,
                            remote_max_frame_size,
                        ).await
                    }
                    ClientCommand::Settle {
                        channel,
                        handle,
                        incarnation,
                        delivery_id,
                        state,
                        reply,
                    } => {
                        if !matches!(
                            sessions.get(&channel).and_then(|session| session.links.get(&handle)),
                            Some(LinkState::Receiving(link)) if link.incarnation == incarnation
                        ) {
                            let _ = reply.send(Err(EngineError::RemoteDetached));
                            continue;
                        }
                        let result = write_amqp(
                            &mut writer,
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
                        ).await;
                        let _ = reply.send(result.as_ref().map(|_| ()).map_err(|error| {
                            EngineError::InvalidState(error.to_string())
                        }));
                        result
                    }
                    ClientCommand::Detach {
                        channel,
                        handle,
                        incarnation,
                        reply,
                    } => {
                        let is_current = sessions
                            .get(&channel)
                            .and_then(|session| session.links.get(&handle))
                            .is_some_and(|link| link.incarnation() == incarnation);
                        if is_current {
                            pending_detaches.insert((channel, handle), reply);
                            write_amqp(
                                &mut writer,
                                channel,
                                Performative::Detach(Detach {
                                    handle,
                                    closed: true,
                                    error: None,
                                }),
                                Vec::new(),
                            ).await
                        } else {
                            let _ = reply.send(Ok(()));
                            Ok(())
                        }
                    }
                    ClientCommand::End { channel, reply } => {
                        let result = write_amqp(
                            &mut writer,
                            channel,
                            Performative::End(End::default()),
                            Vec::new(),
                        ).await;
                        if let Some(mut session) = sessions.remove(&channel) {
                            for link in session.links.values_mut() {
                                stop_link(link);
                            }
                        }
                        let _ = reply.send(result.as_ref().map(|_| ()).map_err(|error| {
                            EngineError::InvalidState(error.to_string())
                        }));
                        result
                    }
                    ClientCommand::Close { reply } => {
                        closing = Some(reply);
                        write_amqp(
                            &mut writer,
                            0,
                            Performative::Close(Close::default()),
                            Vec::new(),
                        ).await
                    }
                };
                if result.is_err() {
                    break;
                }
            }
        }
    }

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
    if let Some(reply) = closing {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
    let _ = closed.send(true);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_sender_can_send_an_explicit_message_format() {
        let message_format = 0x8001_3700;
        let message = Message::data(b"formatted-batch".to_vec());
        let encoded = encode_message(&message).expect("message encodes");
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);

        let server = tokio::spawn(async move {
            let mut connection = ServerConnection::accept(server_io, "server", None)
                .await
                .expect("server accepts the connection");
            let incoming = connection
                .next_incoming_session()
                .await
                .expect("client begins a session");
            let mut session = connection
                .accept_session(incoming)
                .await
                .expect("server accepts the session");
            let attach = session
                .next_incoming_attach()
                .await
                .expect("client attaches a sender");
            let LinkEndpoint::Receiver(mut receiver) = session
                .accept_attach(attach, 64 * 1024)
                .await
                .expect("server accepts the link")
            else {
                panic!("a client sender creates a server receiver");
            };
            let delivery = receiver.recv().await.expect("server receives the message");
            let received_format = delivery.message_format();
            let received_message = delivery.message().clone();
            let received_bytes = delivery.encoded_message().to_vec();
            receiver
                .accept(&delivery)
                .await
                .expect("server accepts the delivery");
            (received_format, received_message, received_bytes)
        });

        let mut connection = ClientConnection::open(client_io, "client", None)
            .await
            .expect("client opens the connection");
        let mut session = connection.begin().await.expect("client begins a session");
        let mut sender = session
            .attach_sender("sender", "queue")
            .await
            .expect("client attaches a sender");
        let outcome = sender
            .send_with_format(message.clone(), message_format)
            .await
            .expect("formatted delivery receives an outcome");
        assert_eq!(outcome, Outcome::Accepted(Accepted));

        let (received_format, received_message, received_bytes) =
            server.await.expect("server task joins");
        assert_eq!(received_format, message_format);
        assert_eq!(received_message, message);
        assert_eq!(received_bytes, encoded);
    }
}
