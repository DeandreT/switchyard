use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ClientSender, ClientSession, Close,
    ConnectionOptions, Delivery, DeliveryState, Detach, Disposition, EngineError, Flow, Frame,
    IncomingAttach, LinkEndpoint, Message, Open, Outcome, Performative, ProtocolHeader, Receiver,
    ReceiverSettleMode, Role, Sender, SenderSettleMode, ServerConnection, ServerSession, Source,
    Target, Transfer, decode_message, encode_message, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;
pub(super) const IO_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const CASE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone, Copy)]
pub(super) struct Channels {
    pub incoming: u16,
    pub outgoing: u16,
}

#[derive(Clone, Copy)]
pub(super) struct Link {
    pub channels: Channels,
    pub peer: u32,
    pub local: u32,
}

pub(super) struct Peer {
    stream: TcpStream,
    sent_transfers: HashMap<u16, u32>,
    received_transfers: HashMap<u16, u32>,
}

impl Peer {
    pub async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(performative, Performative::Begin(_)) {
            self.sent_transfers.insert(channel, 0);
        } else if matches!(performative, Performative::Transfer(_)) {
            *self.sent_transfers.entry(channel).or_default() += 1;
        }
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    pub async fn read(&mut self) -> TestResult<Frame> {
        let frame = timeout(IO_TIMEOUT, read_frame(&mut self.stream)).await??;
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = &frame
        {
            self.received_transfers.insert(*channel, 0);
        } else if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            *self.received_transfers.entry(*channel).or_default() += 1;
        }
        Ok(frame)
    }

    pub fn flow(&self, channels: Channels) -> Flow {
        Flow {
            next_incoming_id: Some(
                self.received_transfers
                    .get(&channels.outgoing)
                    .copied()
                    .unwrap_or(0),
            ),
            incoming_window: 2_048,
            next_outgoing_id: self
                .sent_transfers
                .get(&channels.incoming)
                .copied()
                .unwrap_or(0),
            outgoing_window: 2_048,
            ..Flow::default()
        }
    }

    pub async fn barrier(&mut self, channels: Channels) -> TestResult<Vec<Frame>> {
        let mut flow = self.flow(channels);
        flow.echo = true;
        self.send(channels.incoming, Performative::Flow(flow), Vec::new())
            .await?;
        let expected = self
            .sent_transfers
            .get(&channels.incoming)
            .copied()
            .unwrap_or(0);
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel, performative: Some(Performative::Flow(flow)), ..
            } if *channel == channels.outgoing && flow.handle.is_none()
                && flow.next_incoming_id == Some(expected));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_)),
                        ..
                    }
                ),
                "connection must remain healthy: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    pub async fn own_attach(&mut self, link: Link, role: Role) -> TestResult<Attach> {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = self.read().await?
        else {
            panic!("own Attach must publish its own handle first")
        };
        assert_eq!(channel, link.channels.outgoing);
        assert_eq!(attach.handle, link.local);
        assert_eq!(attach.role, role);
        Ok(*attach)
    }

    pub async fn receiving_flow(&mut self, link: Link) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel, performative: Some(Performative::Flow(flow)), ..
        } if channel == link.channels.outgoing && flow.handle == Some(link.local)
            && flow.link_credit == Some(32)));
        Ok(())
    }

    pub async fn detach(&mut self, link: Link) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(channel, link.channels.outgoing);
                    assert_eq!(detach.handle, link.local);
                    assert!(detach.closed);
                    assert!(detach.error.is_none());
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected own mapped Detach, got {frame:?}"),
            }
        }
    }

    pub async fn error_detach(&mut self, link: Link, condition: &str) -> TestResult {
        let frame = self.read().await?;
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Detach(detach)),
            payload,
        } = &frame
        else {
            panic!("expected immediate mapped error Detach: {frame:?}")
        };
        assert_eq!(*channel, link.channels.outgoing);
        assert_eq!(detach.handle, link.local);
        assert!(detach.closed);
        assert!(payload.is_empty());
        assert_eq!(
            detach
                .error
                .as_ref()
                .expect("link refusal")
                .condition
                .as_symbol()
                .as_str(),
            condition
        );
        Ok(())
    }

    pub async fn end(&mut self, channels: Channels, condition: &str) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::End(end)),
                    ..
                } => {
                    assert_eq!(channel, channels.outgoing);
                    assert_eq!(
                        end.error
                            .expect("session refusal")
                            .condition
                            .as_symbol()
                            .as_str(),
                        condition
                    );
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected bounded mapped End, got {frame:?}"),
            }
        }
    }

    pub async fn close(&mut self, condition: &str) -> TestResult {
        let frame = self.read().await?;
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Close(close)),
            payload,
        } = &frame
        else {
            panic!("expected immediate connection Close, not session or link traffic: {frame:?}")
        };
        assert_eq!(*channel, 0);
        assert!(payload.is_empty());
        assert_eq!(
            close
                .error
                .as_ref()
                .expect("connection refusal")
                .condition
                .as_symbol()
                .as_str(),
            condition
        );
        Ok(())
    }

    pub async fn acknowledge_close_and_expect_eof(&mut self) -> TestResult {
        self.send(0, Performative::Close(Close::default()), Vec::new())
            .await?;
        let mut byte = [0];
        assert_eq!(
            timeout(IO_TIMEOUT, self.stream.read(&mut byte)).await??,
            0,
            "Close acknowledgement must finish the connection without further frames"
        );
        Ok(())
    }

    pub async fn accepted(&mut self, channels: Channels, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(value)),
                    ..
                } => {
                    assert_eq!(channel, channels.outgoing);
                    assert_eq!(value.role, Role::Receiver);
                    assert_eq!(value.first, id);
                    assert_eq!(value.last, None);
                    assert!(value.settled);
                    assert_eq!(value.state, Some(DeliveryState::Accepted(Accepted)));
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected mapped settlement, got {frame:?}"),
            }
        }
    }
}

pub(super) enum Connection {
    Server(ServerConnection),
    Client(ClientConnection),
}

pub(super) enum Session {
    Server(ServerSession),
    Client(ClientSession),
}

pub(super) enum Receiving {
    Server(Receiver),
    Client(Box<ClientReceiver>),
}

impl Receiving {
    pub async fn recv(&mut self) -> Result<Delivery, EngineError> {
        match self {
            Self::Server(receiver) => receiver.recv().await,
            Self::Client(receiver) => receiver.recv().await,
        }
    }
    pub async fn accept(&self, delivery: &Delivery) -> Result<(), EngineError> {
        match self {
            Self::Server(receiver) => receiver.accept(delivery).await,
            Self::Client(receiver) => receiver.accept(delivery).await,
        }
    }
    pub async fn close(&self) -> Result<(), EngineError> {
        match self {
            Self::Server(receiver) => receiver.close().await,
            Self::Client(receiver) => receiver.close().await,
        }
    }
}

pub(super) enum Sending {
    Server(Sender),
    Client(ClientSender),
}

impl Sending {
    pub async fn send(&mut self, message: Message) -> Result<Outcome, EngineError> {
        match self {
            Self::Server(sender) => sender.send(message, vec![3].into()).await,
            Self::Client(sender) => sender.send(message).await,
        }
    }
}

pub(super) struct Node {
    pub connection: Connection,
    pub peer: Peer,
    next_link_name: u64,
}

impl Node {
    pub async fn new(server: bool) -> TestResult<Self> {
        let listener = timeout(IO_TIMEOUT, TcpListener::bind("127.0.0.1:0")).await??;
        let client = timeout(IO_TIMEOUT, TcpStream::connect(listener.local_addr()?)).await??;
        let (socket, _) = timeout(IO_TIMEOUT, listener.accept()).await??;
        client.set_nodelay(true)?;
        socket.set_nodelay(true)?;
        let (wire, raw) = if server {
            (socket, client)
        } else {
            (client, socket)
        };
        let mut peer = Peer {
            stream: raw,
            sent_transfers: HashMap::new(),
            received_transfers: HashMap::new(),
        };
        let open = Open {
            channel_max: 3,
            max_frame_size: 512,
            idle_time_out: None,
            ..Open::new("handle-peer")
        };
        let connection = if server {
            let accepting = tokio::spawn(ServerConnection::accept_with_options(
                wire,
                "handle-server",
                None,
                ConnectionOptions::default().idle_timeout_millis(0),
            ));
            timeout(
                IO_TIMEOUT,
                write_protocol_header(&mut peer.stream, ProtocolHeader::AMQP),
            )
            .await??;
            assert_eq!(
                timeout(IO_TIMEOUT, read_protocol_header(&mut peer.stream)).await??,
                ProtocolHeader::AMQP
            );
            peer.send(0, Performative::Open(open), Vec::new()).await?;
            assert!(matches!(
                peer.read().await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            Connection::Server(timeout(IO_TIMEOUT, accepting).await???)
        } else {
            let opening = tokio::spawn(
                ClientConnection::builder()
                    .container_id("handle-client")
                    .max_frame_size(512)
                    .idle_timeout_millis(0)
                    .open_with_stream(wire),
            );
            assert_eq!(
                timeout(IO_TIMEOUT, read_protocol_header(&mut peer.stream)).await??,
                ProtocolHeader::AMQP
            );
            timeout(
                IO_TIMEOUT,
                write_protocol_header(&mut peer.stream, ProtocolHeader::AMQP),
            )
            .await??;
            assert!(matches!(
                peer.read().await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            peer.send(0, Performative::Open(open), Vec::new()).await?;
            Connection::Client(timeout(IO_TIMEOUT, opening).await???)
        };
        Ok(Self {
            connection,
            peer,
            next_link_name: 0,
        })
    }

    pub async fn session(&mut self, channels: Channels, handle_max: u32) -> TestResult<Session> {
        match &mut self.connection {
            Connection::Server(connection) => {
                self.peer
                    .send(
                        channels.incoming,
                        Performative::Begin(Begin {
                            handle_max,
                            ..Begin::default()
                        }),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, connection.next_incoming_session())
                    .await?
                    .expect("incoming session");
                let session = timeout(IO_TIMEOUT, connection.accept_session(incoming)).await??;
                assert!(matches!(self.peer.read().await?, Frame::Amqp {
                    channel, performative: Some(Performative::Begin(begin)), ..
                } if channel == channels.outgoing && begin.remote_channel == Some(channels.incoming)
                    && begin.handle_max == u32::MAX));
                Ok(Session::Server(session))
            }
            Connection::Client(connection) => {
                let (session, ()) = timeout(IO_TIMEOUT, async {
                    tokio::try_join!(
                        async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
                        async {
                            assert!(matches!(self.peer.read().await?, Frame::Amqp {
                                channel, performative: Some(Performative::Begin(begin)), ..
                            } if channel == channels.outgoing && begin.remote_channel.is_none()
                                && begin.handle_max == u32::MAX));
                            self.peer
                                .send(
                                    channels.incoming,
                                    Performative::Begin(Begin {
                                        remote_channel: Some(channels.outgoing),
                                        handle_max,
                                        ..Begin::default()
                                    }),
                                    Vec::new(),
                                )
                                .await
                        }
                    )
                })
                .await??;
                Ok(Session::Client(session))
            }
        }
    }

    pub async fn pending(
        &mut self,
        session: &mut Session,
        link: Link,
        role: Role,
    ) -> TestResult<IncomingAttach> {
        let name = format!("peer-{}-{}", link.channels.incoming, link.peer);
        self.pending_named(session, link, role, name).await
    }

    async fn pending_named(
        &mut self,
        session: &mut Session,
        link: Link,
        role: Role,
        name: String,
    ) -> TestResult<IncomingAttach> {
        let Session::Server(session) = session else {
            panic!("server approval")
        };
        let mut request = attach(link, role);
        request.name = name;
        self.peer
            .send(
                link.channels.incoming,
                Performative::Attach(Box::new(request)),
                Vec::new(),
            )
            .await?;
        let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("pending approval");
        assert_eq!(
            incoming.attach().handle,
            link.peer,
            "original peer request is preserved"
        );
        Ok(incoming)
    }

    pub async fn approve_receiver(
        &mut self,
        session: &Session,
        incoming: IncomingAttach,
        link: Link,
    ) -> TestResult<Receiving> {
        let Session::Server(session) = session else {
            panic!("server approval")
        };
        let name = incoming.attach().name.clone();
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
        else {
            panic!("receiving endpoint")
        };
        assert_eq!(self.peer.own_attach(link, Role::Receiver).await?.name, name);
        self.peer.receiving_flow(link).await?;
        Ok(Receiving::Server(receiver))
    }

    pub async fn receiver(&mut self, session: &mut Session, link: Link) -> TestResult<Receiving> {
        let name = self.fresh_name("receiver", link);
        self.receiver_named(session, link, name).await
    }

    pub async fn receiver_named(
        &mut self,
        session: &mut Session,
        link: Link,
        name: impl Into<String>,
    ) -> TestResult<Receiving> {
        let name = name.into();
        if matches!(session, Session::Server(_)) {
            let incoming = self
                .pending_named(session, link, Role::Sender, name)
                .await?;
            return self.approve_receiver(session, incoming, link).await;
        }
        let Session::Client(session) = session else {
            unreachable!()
        };
        let (receiver, ()) = tokio::try_join!(
            async {
                Ok::<_, Box<dyn Error>>(session.attach_receiver(name.clone(), "queue").await?)
            },
            async {
                let mut response = self.peer.own_attach(link, Role::Receiver).await?;
                assert_eq!(response.name, name);
                response = response.response(response.source.clone(), response.target.clone());
                response.handle = link.peer;
                self.peer
                    .send(
                        link.channels.incoming,
                        Performative::Attach(Box::new(response)),
                        Vec::new(),
                    )
                    .await
            }
        )?;
        self.peer.receiving_flow(link).await?;
        Ok(Receiving::Client(Box::new(receiver)))
    }

    pub async fn sender(&mut self, session: &mut Session, link: Link) -> TestResult<Sending> {
        let name = self.fresh_name("sender", link);
        self.sender_named(session, link, name).await
    }

    pub async fn sender_named(
        &mut self,
        session: &mut Session,
        link: Link,
        name: impl Into<String>,
    ) -> TestResult<Sending> {
        let sender = self
            .sender_without_credit_named(session, link, name.into())
            .await?;
        let mut flow = self.peer.flow(link.channels);
        flow.handle = Some(link.peer);
        flow.delivery_count = Some(0);
        flow.link_credit = Some(2);
        self.peer
            .send(link.channels.incoming, Performative::Flow(flow), Vec::new())
            .await?;
        Ok(sender)
    }

    pub async fn sender_without_credit(
        &mut self,
        session: &mut Session,
        link: Link,
    ) -> TestResult<Sending> {
        let name = self.fresh_name("sender", link);
        self.sender_without_credit_named(session, link, name).await
    }

    async fn sender_without_credit_named(
        &mut self,
        session: &mut Session,
        link: Link,
        name: String,
    ) -> TestResult<Sending> {
        let sender = if matches!(session, Session::Server(_)) {
            let incoming = self
                .pending_named(session, link, Role::Receiver, name.clone())
                .await?;
            let Session::Server(session) = session else {
                unreachable!()
            };
            let LinkEndpoint::Sender(sender) =
                timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
            else {
                panic!("sending endpoint")
            };
            assert_eq!(self.peer.own_attach(link, Role::Sender).await?.name, name);
            Sending::Server(sender)
        } else {
            let Session::Client(session) = session else {
                unreachable!()
            };
            let (sender, ()) = tokio::try_join!(
                async {
                    Ok::<_, Box<dyn Error>>(session.attach_sender(name.clone(), "queue").await?)
                },
                async {
                    let request = self.peer.own_attach(link, Role::Sender).await?;
                    assert_eq!(request.name, name);
                    let mut response =
                        request.response(request.source.clone(), request.target.clone());
                    response.handle = link.peer;
                    self.peer
                        .send(
                            link.channels.incoming,
                            Performative::Attach(Box::new(response)),
                            Vec::new(),
                        )
                        .await
                }
            )?;
            Sending::Client(sender)
        };
        Ok(sender)
    }

    fn fresh_name(&mut self, kind: &str, link: Link) -> String {
        let suffix = self.next_link_name;
        self.next_link_name += 1;
        format!("{kind}-{}-{}-{suffix}", link.channels.outgoing, link.local)
    }

    pub async fn incoming_message(
        &mut self,
        receiver: &mut Receiving,
        link: Link,
        id: u32,
        byte: u8,
    ) -> TestResult {
        let message = Message::data(vec![byte; 8]);
        self.peer
            .send(
                link.channels.incoming,
                Performative::Transfer(transfer(link.peer, id)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        timeout(IO_TIMEOUT, receiver.accept(&delivery)).await??;
        self.peer.accepted(link.channels, id).await
    }

    pub async fn outgoing_message(
        &mut self,
        sender: &mut Sending,
        link: Link,
        byte: u8,
    ) -> TestResult {
        let message = Message::data(vec![byte; 8]);
        let (outcome, ()) = tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(sender.send(message.clone()).await?) },
            async {
                loop {
                    match self.peer.read().await? {
                        Frame::Amqp {
                            channel,
                            performative: Some(Performative::Transfer(value)),
                            payload,
                        } => {
                            assert_eq!(channel, link.channels.outgoing);
                            assert_eq!(value.handle, link.local);
                            assert!(!value.more);
                            assert!(!value.settled.unwrap_or(false));
                            assert_eq!(decode_message(&payload)?, message);
                            self.peer
                                .send(
                                    link.channels.incoming,
                                    Performative::Disposition(Disposition {
                                        role: Role::Receiver,
                                        first: value.delivery_id.expect("delivery ID"),
                                        last: None,
                                        settled: true,
                                        state: Some(DeliveryState::Accepted(Accepted)),
                                        batchable: false,
                                    }),
                                    Vec::new(),
                                )
                                .await?;
                            return Ok::<_, Box<dyn Error>>(());
                        }
                        Frame::Amqp {
                            performative: Some(Performative::Flow(_)),
                            ..
                        } => {}
                        frame => panic!("expected mapped outgoing Transfer, got {frame:?}"),
                    }
                }
            }
        )?;
        assert_eq!(outcome, Outcome::Accepted(Accepted));
        Ok(())
    }

    pub async fn close_receiver(&mut self, receiver: &Receiving, link: Link) -> TestResult {
        timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(receiver.close().await?) },
                async {
                    self.peer.detach(link).await?;
                    self.peer
                        .send(
                            link.channels.incoming,
                            Performative::Detach(detach(link.peer)),
                            Vec::new(),
                        )
                        .await
                }
            )
        })
        .await??;
        Ok(())
    }

    pub async fn shutdown(&self) {
        match &self.connection {
            Connection::Server(connection) => connection.shutdown().await,
            Connection::Client(connection) => connection.shutdown().await,
        }
    }
}

pub(super) fn channels(server: bool, index: u16) -> Channels {
    Channels {
        incoming: if server { 55 + index } else { 27 + index },
        outgoing: index,
    }
}

pub(super) fn attach(link: Link, role: Role) -> Attach {
    Attach {
        name: format!("peer-{}-{}", link.channels.incoming, link.peer),
        handle: link.peer,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        role,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

pub(super) fn transfer(handle: u32, id: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(vec![id as u8].into()),
        message_format: Some(0),
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

pub(super) fn detach(handle: u32) -> Detach {
    Detach {
        handle,
        closed: true,
        error: None,
    }
}
