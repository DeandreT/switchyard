use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::Arc,
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
};

use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode, Role,
    SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode, Transfer,
    decode_message, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};

const LINK_CREDIT: u32 = 2_048;
const SESSION_WINDOW: u32 = 2_048;
const FRAME_OVERHEAD_RESERVE: usize = 512;

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
    incoming_sessions: mpsc::Receiver<IncomingSession>,
}

pub struct IncomingSession {
    channel: u16,
    pub begin: Begin,
}

pub struct ServerSession {
    channel: u16,
    commands: mpsc::Sender<Command>,
    incoming_attaches: mpsc::Receiver<Attach>,
}

pub enum LinkEndpoint {
    Sender(Sender),
    Receiver(Receiver),
}

pub struct Sender {
    name: String,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    detached: watch::Receiver<bool>,
}

pub struct Receiver {
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
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
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        tokio::spawn(run_connection(
            stream,
            remote_open.max_frame_size,
            command_rx,
            incoming_session_tx,
        ));
        Ok(Self {
            commands,
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
            attach_tx,
            reply,
        })
        .await?;
        Ok(ServerSession {
            channel: incoming.channel,
            commands: self.commands.clone(),
            incoming_attaches,
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
        self.incoming_attaches.recv().await
    }

    pub async fn accept_attach(
        &self,
        attach: Attach,
        max_message_size: u64,
    ) -> Result<LinkEndpoint, EngineError> {
        let (deliveries_tx, deliveries) = mpsc::channel(32);
        let (detached_tx, detached) = watch::channel(false);
        let role = attach.role.clone();
        let name = attach.name.clone();
        let handle = attach.handle;
        request(&self.commands, |reply| Command::AcceptLink {
            channel: self.channel,
            attach: Box::new(attach),
            max_message_size,
            deliveries_tx,
            detached_tx,
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
            }),
            Role::Receiver => LinkEndpoint::Sender(Sender {
                name,
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

    pub async fn send(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
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
        outcome.await.map_err(|_| EngineError::Stopped)?
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
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        message: Box<Message>,
        delivery_tag: DeliveryTag,
        reply: oneshot::Sender<Result<Outcome, EngineError>>,
    },
    Settle {
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

struct SessionState {
    attach_tx: Option<mpsc::Sender<Attach>>,
    links: HashMap<u32, LinkState>,
    next_outgoing_id: u32,
}

enum LinkState {
    Sending(SendingLink),
    Receiving(ReceivingLink),
}

struct SendingLink {
    receiver_settle_mode: ReceiverSettleMode,
    settle_mode: SenderSettleMode,
    delivery_count: u32,
    credit_limit: u64,
    queued: VecDeque<QueuedSend>,
    unsettled: HashMap<u32, oneshot::Sender<Result<Outcome, EngineError>>>,
    detached: watch::Sender<bool>,
}

struct QueuedSend {
    message: Message,
    delivery_tag: DeliveryTag,
    reply: oneshot::Sender<Result<Outcome, EngineError>>,
}

struct ReceivingLink {
    max_message_size: u64,
    deliveries: mpsc::Sender<Delivery>,
    partial: Option<PartialDelivery>,
    detached: watch::Sender<bool>,
}

struct PartialDelivery {
    id: u32,
    settled: bool,
    bytes: Vec<u8>,
}

async fn run_connection<Io>(
    stream: Io,
    remote_max_frame_size: u32,
    mut commands: mpsc::Receiver<Command>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
) where
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

    let mut sessions = HashMap::<u16, SessionState>::new();
    let mut closing_reply: Option<oneshot::Sender<Result<(), EngineError>>> = None;
    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                let Ok(frame) = frame else { break };
                match handle_frame(
                    frame,
                    &mut writer,
                    &incoming_sessions,
                    &mut sessions,
                    remote_max_frame_size,
                ).await {
                    Ok(FrameAction::Continue) => {}
                    Ok(FrameAction::Closed) => {
                        if let Some(reply) = closing_reply.take() {
                            let _ = reply.send(Ok(()));
                        }
                        break;
                    }
                    Err(_) => break,
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match handle_command(
                    command,
                    &mut writer,
                    &mut sessions,
                    remote_max_frame_size,
                ).await {
                    Ok(CommandAction::Continue) => {}
                    Ok(CommandAction::Closing(reply)) => closing_reply = Some(reply),
                    Err(_) => break,
                }
            }
        }
    }

    for session in sessions.values_mut() {
        for link in session.links.values_mut() {
            stop_link(link);
        }
    }
    if let Some(reply) = closing_reply {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
}

enum FrameAction {
    Continue,
    Closed,
}

async fn handle_frame<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut W,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
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

    match performative {
        Performative::Begin(begin) => {
            incoming_sessions
                .send(IncomingSession { channel, begin })
                .await
                .map_err(|_| EngineError::Stopped)?;
        }
        Performative::Attach(attach) => {
            let session = sessions
                .get(&channel)
                .ok_or_else(|| invalid_state("attach on an unknown session"))?;
            session
                .attach_tx
                .as_ref()
                .ok_or_else(|| invalid_state("session cannot accept remote links"))?
                .send(*attach)
                .await
                .map_err(|_| EngineError::Stopped)?;
        }
        Performative::Flow(flow) => {
            apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
        }
        Performative::Transfer(transfer) => {
            receive_transfer(channel, transfer, payload, sessions).await?;
        }
        Performative::Disposition(disposition) => {
            apply_disposition(channel, disposition, writer, sessions).await?;
        }
        Performative::Detach(detach) => {
            if let Some(session) = sessions.get_mut(&channel)
                && let Some(mut link) = session.links.remove(&detach.handle)
            {
                stop_link(&mut link);
                write_amqp(
                    writer,
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
        Performative::End(_) => {
            if let Some(mut session) = sessions.remove(&channel) {
                for link in session.links.values_mut() {
                    stop_link(link);
                }
            }
            write_amqp(
                writer,
                channel,
                Performative::End(End::default()),
                Vec::new(),
            )
            .await?;
        }
        Performative::Close(_) => {
            write_amqp(writer, 0, Performative::Close(Close::default()), Vec::new()).await?;
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
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::AcceptSession {
            channel,
            attach_tx,
            reply,
        } => {
            if sessions.contains_key(&channel) {
                let _ = reply.send(Err(invalid_state("session channel is already open")));
                return Ok(CommandAction::Continue);
            }
            write_amqp(
                writer,
                channel,
                Performative::Begin(Begin {
                    remote_channel: Some(channel),
                    ..Begin::default()
                }),
                Vec::new(),
            )
            .await?;
            sessions.insert(
                channel,
                SessionState {
                    attach_tx: Some(attach_tx),
                    links: HashMap::new(),
                    next_outgoing_id: 0,
                },
            );
            let _ = reply.send(Ok(()));
        }
        Command::AcceptLink {
            channel,
            attach,
            max_message_size,
            deliveries_tx,
            detached_tx,
            reply,
        } => {
            let attach = *attach;
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("link accepted on an unknown session"))?;
            let handle = attach.handle;
            if session.links.contains_key(&handle) {
                let _ = reply.send(Err(invalid_state("link handle is already attached")));
                return Ok(CommandAction::Continue);
            }
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.max_message_size =
                (response.role == Role::Receiver).then_some(max_message_size);
            write_amqp(
                writer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await?;
            match attach.role {
                Role::Sender => {
                    session.links.insert(
                        handle,
                        LinkState::Receiving(ReceivingLink {
                            max_message_size,
                            deliveries: deliveries_tx,
                            partial: None,
                            detached: detached_tx,
                        }),
                    );
                    write_amqp(
                        writer,
                        channel,
                        Performative::Flow(Flow {
                            next_incoming_id: Some(0),
                            incoming_window: SESSION_WINDOW,
                            next_outgoing_id: session.next_outgoing_id,
                            outgoing_window: SESSION_WINDOW,
                            handle: Some(handle),
                            delivery_count: Some(0),
                            link_credit: Some(LINK_CREDIT),
                            ..Flow::default()
                        }),
                        Vec::new(),
                    )
                    .await?;
                }
                Role::Receiver => {
                    session.links.insert(
                        handle,
                        LinkState::Sending(SendingLink {
                            settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                            delivery_count: 0,
                            credit_limit: 0,
                            queued: VecDeque::new(),
                            unsettled: HashMap::new(),
                            detached: detached_tx,
                        }),
                    );
                }
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
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("send on an unknown session"))?;
            let LinkState::Sending(link) = session
                .links
                .get_mut(&handle)
                .ok_or_else(|| invalid_state("send on an unknown link"))?
            else {
                let _ = reply.send(Err(invalid_state("send on a receiving link")));
                return Ok(CommandAction::Continue);
            };
            link.queued.push_back(QueuedSend {
                message: *message,
                delivery_tag,
                reply,
            });
            flush_sends(channel, handle, session, writer, remote_max_frame_size).await?;
        }
        Command::Settle {
            channel,
            handle,
            delivery_id,
            state,
            reply,
        } => {
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("settlement on an unknown session"))?;
            if !matches!(session.links.get(&handle), Some(LinkState::Receiving(_))) {
                let _ = reply.send(Err(invalid_state("settlement on an unknown link")));
                return Ok(CommandAction::Continue);
            }
            write_amqp(
                writer,
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
        Command::Detach {
            channel,
            handle,
            error,
            reply,
        } => {
            if let Some(session) = sessions.get_mut(&channel)
                && let Some(mut link) = session.links.remove(&handle)
            {
                write_amqp(
                    writer,
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
            write_amqp(writer, 0, Performative::Close(Close { error }), Vec::new()).await?;
            return Ok(CommandAction::Closing(reply));
        }
    }
    Ok(CommandAction::Continue)
}

async fn receive_transfer(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("transfer on an unknown session"))?;
    let Some(link) = session.links.get_mut(&transfer.handle) else {
        // A transfer can cross a link-scoped refusal on the wire. The detach is
        // authoritative; a late transfer must not escalate it to the connection.
        return Ok(());
    };
    let LinkState::Receiving(link) = link else {
        return Err(invalid_state("transfer sent to a sending link"));
    };

    if transfer.aborted {
        link.partial = None;
        return Ok(());
    }
    let partial = match link.partial.take() {
        Some(mut partial) => {
            partial.bytes.extend_from_slice(&payload);
            partial
        }
        None => PartialDelivery {
            id: transfer
                .delivery_id
                .ok_or_else(|| invalid_state("first transfer has no delivery id"))?,
            settled: transfer.settled.unwrap_or(false),
            bytes: payload,
        },
    };
    if partial.bytes.len() as u64 > link.max_message_size {
        return Err(invalid_state("message exceeds the link's maximum size"));
    }
    if transfer.more {
        link.partial = Some(partial);
        return Ok(());
    }

    let message = decode_message(&partial.bytes)?;
    link.deliveries
        .send(Delivery {
            id: partial.id,
            settled: partial.settled,
            message,
        })
        .await
        .map_err(|_| EngineError::Stopped)
}

async fn apply_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: Flow,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let Some(handle) = flow.handle else {
        return Ok(());
    };
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        return Ok(());
    };
    if let Some(credit) = flow.link_credit {
        link.credit_limit =
            u64::from(flow.delivery_count.unwrap_or(link.delivery_count)) + u64::from(credit);
    }
    flush_sends(channel, handle, session, writer, remote_max_frame_size).await
}

async fn flush_sends<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut W,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    loop {
        let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
            return Ok(());
        };
        if u64::from(link.delivery_count) >= link.credit_limit {
            return Ok(());
        }
        let Some(queued) = link.queued.pop_front() else {
            return Ok(());
        };
        let delivery_id = session.next_outgoing_id;
        session.next_outgoing_id = session.next_outgoing_id.wrapping_add(1);
        link.delivery_count = link.delivery_count.wrapping_add(1);
        let settled = link.settle_mode == SenderSettleMode::Settled;
        let payload = encode_message(&queued.message)?;
        write_transfer(
            writer,
            channel,
            handle,
            delivery_id,
            queued.delivery_tag,
            settled,
            payload,
            remote_max_frame_size,
        )
        .await?;
        if settled {
            let _ = queued.reply.send(Ok(Outcome::Accepted(Accepted)));
        } else {
            link.unsettled.insert(delivery_id, queued.reply);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_transfer<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    handle: u32,
    delivery_id: u32,
    delivery_tag: DeliveryTag,
    settled: bool,
    payload: Vec<u8>,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let maximum = usize::try_from(remote_max_frame_size)
        .unwrap_or(usize::MAX)
        .saturating_sub(FRAME_OVERHEAD_RESERVE)
        .max(1);
    let chunks = if payload.is_empty() {
        vec![&[][..]]
    } else {
        payload.chunks(maximum).collect()
    };
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.into_iter().enumerate() {
        write_amqp(
            writer,
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: (index == 0).then_some(delivery_id),
                delivery_tag: (index == 0).then_some(delivery_tag.clone()),
                message_format: (index == 0).then_some(0),
                settled: (index == 0).then_some(settled),
                more: index + 1 < chunk_count,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            chunk.to_vec(),
        )
        .await?;
    }
    Ok(())
}

async fn apply_disposition<W: AsyncWrite + Unpin>(
    channel: u16,
    disposition: Disposition,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    if disposition.role != Role::Receiver {
        return Ok(());
    }
    let Some(state) = disposition.state.clone() else {
        return Ok(());
    };
    let Ok(outcome) = Outcome::try_from(state.clone()) else {
        return Ok(());
    };
    let last = disposition.last.unwrap_or(disposition.first);
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    let mut echo = false;
    for link in session.links.values_mut() {
        let LinkState::Sending(link) = link else {
            continue;
        };
        for id in disposition.first..=last {
            if let Some(reply) = link.unsettled.remove(&id) {
                echo |=
                    link.receiver_settle_mode == ReceiverSettleMode::Second && !disposition.settled;
                let _ = reply.send(Ok(outcome.clone()));
            }
        }
    }
    if echo {
        write_amqp(
            writer,
            channel,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: disposition.first,
                last: disposition.last,
                settled: true,
                state: Some(state),
                batchable: disposition.batchable,
            }),
            Vec::new(),
        )
        .await?;
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
            for (_, reply) in link.unsettled.drain() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
            }
        }
        LinkState::Receiving(link) => {
            let _ = link.detached.send(true);
        }
    }
}

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
    use crate::AmqpError;

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
}
