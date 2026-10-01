//! Session channels are independently assigned in each connection direction.

use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ClientSender, ClientSession, Close,
    ConnectionOptions, Delivery, DeliveryState, Detach, Disposition, End, EngineError, Flow, Frame,
    LinkEndpoint, Message, Open, Outcome, Performative, ProtocolHeader, Receiver,
    ReceiverSettleMode, Role, Sender, SenderSettleMode, ServerConnection, ServerSession, Source,
    Target, Transfer, decode_message, encode_message, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const CASE_TIMEOUT: Duration = Duration::from_secs(45);
const FRAMING_ERROR: &str = "amqp:connection:framing-error";

#[derive(Clone, Copy)]
struct Binding {
    incoming: u16,
    outgoing: u16,
}

struct Peer {
    stream: TcpStream,
    sent_transfers: HashMap<u16, u32>,
    received_transfers: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
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

    async fn read(&mut self) -> TestResult<Frame> {
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

    fn flow(&self, binding: Binding) -> Flow {
        Flow {
            next_incoming_id: Some(
                self.received_transfers
                    .get(&binding.outgoing)
                    .copied()
                    .unwrap_or(0),
            ),
            incoming_window: 2_048,
            next_outgoing_id: self
                .sent_transfers
                .get(&binding.incoming)
                .copied()
                .unwrap_or(0),
            outgoing_window: 2_048,
            ..Flow::default()
        }
    }

    async fn barrier(&mut self, binding: Binding) -> TestResult<Vec<Frame>> {
        let mut flow = self.flow(binding);
        flow.echo = true;
        self.send(binding.incoming, Performative::Flow(flow), Vec::new())
            .await?;
        let expected = self
            .sent_transfers
            .get(&binding.incoming)
            .copied()
            .unwrap_or(0);
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel, performative: Some(Performative::Flow(flow)), ..
            } if *channel == binding.outgoing && flow.handle.is_none()
                && flow.next_incoming_id == Some(expected));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_)),
                        ..
                    }
                ),
                "mapped sibling must remain healthy: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn begin_reply(&mut self, binding: Binding) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel, performative: Some(Performative::Begin(begin)), ..
        } if channel == binding.outgoing && begin.remote_channel == Some(binding.incoming)));
        Ok(())
    }

    async fn accepted(&mut self, binding: Binding, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Disposition(value)),
                    ..
                } => {
                    assert_eq!(channel, binding.outgoing);
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

    async fn detach(&mut self, binding: Binding, handle: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Detach(value)),
                    ..
                } => {
                    assert_eq!(channel, binding.outgoing);
                    assert_eq!(value.handle, handle);
                    assert!(value.closed);
                    assert!(value.error.is_none());
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected mapped Detach, got {frame:?}"),
            }
        }
    }

    async fn end(&mut self, binding: Binding, condition: Option<&str>) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::End(value)),
                    ..
                } => {
                    assert_eq!(channel, binding.outgoing);
                    assert_eq!(
                        value.error.as_ref().map(|error| error
                            .condition
                            .as_symbol()
                            .as_str()
                            .to_owned()),
                        condition.map(str::to_owned)
                    );
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected mapped End, got {frame:?}"),
            }
        }
    }

    async fn close(&mut self, condition: &str) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Close(value)),
                    ..
                } => {
                    assert_eq!(
                        value
                            .error
                            .expect("connection refusal")
                            .condition
                            .as_symbol()
                            .as_str(),
                        condition
                    );
                    self.send(0, Performative::Close(Close::default()), Vec::new())
                        .await?;
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected bounded Close, got {frame:?}"),
            }
        }
    }
}

enum Connection {
    Server(ServerConnection),
    Client(ClientConnection),
}

enum Session {
    Server(ServerSession),
    Client(ClientSession),
}

enum Receiving {
    Server(Receiver),
    Client(Box<ClientReceiver>),
}

impl Receiving {
    async fn recv(&mut self) -> Result<Delivery, EngineError> {
        match self {
            Self::Server(receiver) => receiver.recv().await,
            Self::Client(receiver) => receiver.recv().await,
        }
    }

    async fn accept(&self, delivery: &Delivery) -> Result<(), EngineError> {
        match self {
            Self::Server(receiver) => receiver.accept(delivery).await,
            Self::Client(receiver) => receiver.accept(delivery).await,
        }
    }

    async fn close(&self) -> Result<(), EngineError> {
        match self {
            Self::Server(receiver) => receiver.close().await,
            Self::Client(receiver) => receiver.close().await,
        }
    }
}

enum Sending {
    Server(Sender),
    Client(ClientSender),
}

impl Sending {
    async fn send(&mut self, message: Message) -> Result<Outcome, EngineError> {
        match self {
            Self::Server(sender) => sender.send(message, vec![3].into()).await,
            Self::Client(sender) => sender.send(message).await,
        }
    }
}

struct Node {
    connection: Connection,
    peer: Peer,
    next_link_name: u64,
}

impl Node {
    async fn new(server: bool, peer_channel_max: u16) -> TestResult<Self> {
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
            channel_max: peer_channel_max,
            max_frame_size: 512,
            idle_time_out: None,
            ..Open::new("asymmetric-peer")
        };
        let connection = if server {
            let accepting = tokio::spawn(ServerConnection::accept_with_options(
                wire,
                "asymmetric-server",
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
            assert!(matches!(peer.read().await?, Frame::Amqp {
                channel: 0, performative: Some(Performative::Open(open)), ..
            } if open.channel_max == u16::MAX));
            Connection::Server(timeout(IO_TIMEOUT, accepting).await???)
        } else {
            let opening = tokio::spawn(
                ClientConnection::builder()
                    .container_id("asymmetric-client")
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
            assert!(matches!(peer.read().await?, Frame::Amqp {
                channel: 0, performative: Some(Performative::Open(open)), ..
            } if open.channel_max == u16::MAX));
            peer.send(0, Performative::Open(open), Vec::new()).await?;
            Connection::Client(timeout(IO_TIMEOUT, opening).await???)
        };
        Ok(Self {
            connection,
            peer,
            next_link_name: 0,
        })
    }

    async fn session(&mut self, binding: Binding) -> TestResult<Session> {
        match &mut self.connection {
            Connection::Server(connection) => {
                self.peer
                    .send(
                        binding.incoming,
                        Performative::Begin(Begin::default()),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, connection.next_incoming_session())
                    .await?
                    .expect("incoming session");
                let session = timeout(IO_TIMEOUT, connection.accept_session(incoming)).await??;
                self.peer.begin_reply(binding).await?;
                Ok(Session::Server(session))
            }
            Connection::Client(connection) => {
                let (session, ()) = timeout(IO_TIMEOUT, async {
                    tokio::try_join!(
                        async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
                        async {
                            assert!(matches!(self.peer.read().await?, Frame::Amqp {
                                channel, performative: Some(Performative::Begin(begin)), ..
                            } if channel == binding.outgoing && begin.remote_channel.is_none()));
                            self.peer
                                .send(
                                    binding.incoming,
                                    Performative::Begin(Begin {
                                        remote_channel: Some(binding.outgoing),
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

    async fn receiver(
        &mut self,
        session: &mut Session,
        binding: Binding,
        handle: u32,
    ) -> TestResult<Receiving> {
        let name = self.fresh_name("receiver", binding, handle);
        let receiver = match session {
            Session::Server(session) => {
                let mut request = attach(handle, Role::Sender);
                request.name = name.clone();
                self.peer
                    .send(
                        binding.incoming,
                        Performative::Attach(Box::new(request)),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
                    .await?
                    .expect("sender approval");
                let LinkEndpoint::Receiver(receiver) =
                    timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
                else {
                    panic!("receiving endpoint")
                };
                assert!(matches!(self.peer.read().await?, Frame::Amqp {
                    channel, performative: Some(Performative::Attach(response)), ..
                } if channel == binding.outgoing && response.handle == handle && response.role == Role::Receiver));
                Receiving::Server(receiver)
            }
            Session::Client(session) => {
                let (receiver, ()) = tokio::try_join!(
                    async {
                        Ok::<_, Box<dyn Error>>(
                            session.attach_receiver(name.clone(), "queue").await?,
                        )
                    },
                    async {
                        let Frame::Amqp {
                            channel,
                            performative: Some(Performative::Attach(request)),
                            ..
                        } = self.peer.read().await?
                        else {
                            panic!("receiving Attach")
                        };
                        assert_eq!(channel, binding.outgoing);
                        assert_eq!(request.handle, handle);
                        assert_eq!(request.role, Role::Receiver);
                        let response =
                            request.response(request.source.clone(), request.target.clone());
                        self.peer
                            .send(
                                binding.incoming,
                                Performative::Attach(Box::new(response)),
                                Vec::new(),
                            )
                            .await
                    }
                )?;
                Receiving::Client(Box::new(receiver))
            }
        };
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel, performative: Some(Performative::Flow(flow)), ..
        } if channel == binding.outgoing && flow.handle == Some(handle) && flow.link_credit == Some(32)));
        Ok(receiver)
    }

    async fn sender(
        &mut self,
        session: &mut Session,
        binding: Binding,
        handle: u32,
    ) -> TestResult<Sending> {
        let name = self.fresh_name("sender", binding, handle);
        let sender = match session {
            Session::Server(session) => {
                let mut request = attach(handle, Role::Receiver);
                request.name = name.clone();
                self.peer
                    .send(
                        binding.incoming,
                        Performative::Attach(Box::new(request)),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
                    .await?
                    .expect("receiver approval");
                let LinkEndpoint::Sender(sender) =
                    timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
                else {
                    panic!("sending endpoint")
                };
                assert!(matches!(self.peer.read().await?, Frame::Amqp {
                    channel, performative: Some(Performative::Attach(response)), ..
                } if channel == binding.outgoing && response.handle == handle && response.role == Role::Sender));
                Sending::Server(sender)
            }
            Session::Client(session) => {
                let (sender, ()) = tokio::try_join!(
                    async {
                        Ok::<_, Box<dyn Error>>(session.attach_sender(name.clone(), "queue").await?)
                    },
                    async {
                        let Frame::Amqp {
                            channel,
                            performative: Some(Performative::Attach(request)),
                            ..
                        } = self.peer.read().await?
                        else {
                            panic!("sending Attach")
                        };
                        assert_eq!(channel, binding.outgoing);
                        assert_eq!(request.handle, handle);
                        assert_eq!(request.role, Role::Sender);
                        let response =
                            request.response(request.source.clone(), request.target.clone());
                        self.peer
                            .send(
                                binding.incoming,
                                Performative::Attach(Box::new(response)),
                                Vec::new(),
                            )
                            .await
                    }
                )?;
                Sending::Client(sender)
            }
        };
        let mut flow = self.peer.flow(binding);
        flow.handle = Some(handle);
        flow.delivery_count = Some(0);
        flow.link_credit = Some(2);
        self.peer
            .send(binding.incoming, Performative::Flow(flow), Vec::new())
            .await?;
        Ok(sender)
    }

    fn fresh_name(&mut self, kind: &str, binding: Binding, handle: u32) -> String {
        let suffix = self.next_link_name;
        self.next_link_name += 1;
        format!("{kind}-{}-{handle}-{suffix}", binding.outgoing)
    }

    async fn incoming_message(
        &mut self,
        receiver: &mut Receiving,
        binding: Binding,
        handle: u32,
        id: u32,
        byte: u8,
    ) -> TestResult {
        let message = Message::data(vec![byte; 8]);
        self.peer
            .send(
                binding.incoming,
                Performative::Transfer(transfer(handle, id)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        timeout(IO_TIMEOUT, receiver.accept(&delivery)).await??;
        self.peer.accepted(binding, id).await
    }

    async fn abandon_begin(&mut self, expected: u16) -> TestResult {
        let Connection::Client(connection) = &mut self.connection else {
            panic!("client")
        };
        let mut pending = Box::pin(connection.begin());
        timeout(IO_TIMEOUT, async {
            tokio::select! {
                _ = &mut pending => panic!("Begin completed before its acknowledgement"),
                frame = self.peer.read() => {
                    assert!(matches!(frame?, Frame::Amqp {
                        channel, performative: Some(Performative::Begin(begin)), ..
                    } if channel == expected && begin.remote_channel.is_none()));
                    Ok::<_, Box<dyn Error>>(())
                }
            }
        })
        .await??;
        drop(pending);
        Ok(())
    }

    async fn bad_client_reply(
        &mut self,
        local: u16,
        incoming: u16,
        remote_channel: u16,
    ) -> TestResult {
        let Connection::Client(connection) = &mut self.connection else {
            panic!("client")
        };
        let (result, wire) = tokio::join!(connection.begin(), async {
            assert!(matches!(self.peer.read().await?, Frame::Amqp {
                channel, performative: Some(Performative::Begin(_)), ..
            } if channel == local));
            self.peer
                .send(
                    incoming,
                    Performative::Begin(Begin {
                        remote_channel: Some(remote_channel),
                        ..Begin::default()
                    }),
                    Vec::new(),
                )
                .await?;
            self.peer.close(FRAMING_ERROR).await
        });
        wire?;
        assert!(
            result.is_err(),
            "wrong association cannot resolve a pending Begin"
        );
        Ok(())
    }

    async fn shutdown(&self) {
        match &self.connection {
            Connection::Server(connection) => connection.shutdown().await,
            Connection::Client(connection) => connection.shutdown().await,
        }
    }
}

fn attach(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("link-{handle}"),
        handle,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        role,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn transfer(handle: u32, id: u32) -> Transfer {
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

fn detach(handle: u32) -> Detach {
    Detach {
        handle,
        closed: true,
        error: None,
    }
}

async fn full_lifecycle(server: bool, incoming: u16) -> TestResult {
    let mut node = Node::new(server, 0).await?;
    let binding = Binding {
        incoming,
        outgoing: 0,
    };
    let mut session = node.session(binding).await?;
    let mut receiver = node.receiver(&mut session, binding, 0).await?;
    let mut sender = node.sender(&mut session, binding, 1).await?;
    node.incoming_message(&mut receiver, binding, 0, 0, 7)
        .await?;
    let message = Message::data(vec![9; 8]);
    let (outcome, ()) = tokio::try_join!(
        async { Ok::<_, Box<dyn Error>>(sender.send(message.clone()).await?) },
        async {
            loop {
                match node.peer.read().await? {
                    Frame::Amqp {
                        channel,
                        performative: Some(Performative::Transfer(value)),
                        payload,
                    } => {
                        assert_eq!(channel, binding.outgoing);
                        assert_eq!(value.handle, 1);
                        assert_eq!(value.delivery_id, Some(0));
                        assert!(!value.more);
                        assert!(!value.settled.unwrap_or(false));
                        assert_eq!(decode_message(&payload)?, message);
                        node.peer
                            .send(
                                binding.incoming,
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
    timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(receiver.close().await?) },
            async {
                node.peer.detach(binding, 0).await?;
                node.peer
                    .send(
                        binding.incoming,
                        Performative::Detach(detach(0)),
                        Vec::new(),
                    )
                    .await
            }
        )
    })
    .await??;
    node.peer
        .send(
            binding.incoming,
            Performative::Detach(detach(1)),
            Vec::new(),
        )
        .await?;
    node.peer.detach(binding, 1).await?;
    match &session {
        Session::Server(_) => {
            node.peer
                .send(
                    binding.incoming,
                    Performative::End(End::default()),
                    Vec::new(),
                )
                .await?;
            node.peer.end(binding, None).await?;
        }
        Session::Client(session) => {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(session.end().await?) },
                async {
                    node.peer.end(binding, None).await?;
                    node.peer
                        .send(
                            binding.incoming,
                            Performative::End(End::default()),
                            Vec::new(),
                        )
                        .await
                }
            )?;
        }
    }
    let mut replacement = node.session(binding).await?;
    let mut receiver = node.receiver(&mut replacement, binding, 0).await?;
    node.incoming_message(&mut receiver, binding, 0, 0, 11)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn server_crossed_bindings() -> TestResult {
    let mut node = Node::new(true, 1).await?;
    let first = Binding {
        incoming: 55,
        outgoing: 0,
    };
    let second = Binding {
        incoming: 0,
        outgoing: 1,
    };
    let mut a = node.session(first).await?;
    let mut b = node.session(second).await?;
    let mut receiver_a = node.receiver(&mut a, first, 0).await?;
    let mut receiver_b = node.receiver(&mut b, second, 0).await?;
    node.incoming_message(&mut receiver_b, second, 0, 0, 2)
        .await?;
    node.incoming_message(&mut receiver_a, first, 0, 0, 1)
        .await?;

    node.peer
        .send(
            first.incoming,
            Performative::Transfer(transfer(99, 1)),
            encode_message(&Message::data(vec![3]))?,
        )
        .await?;
    node.peer
        .end(first, Some("amqp:session:unattached-handle"))
        .await?;
    node.peer
        .send(
            first.incoming,
            Performative::Begin(Begin::default()),
            Vec::new(),
        )
        .await?;
    node.peer
        .send(
            first.incoming,
            Performative::Flow(node.peer.flow(first)),
            Vec::new(),
        )
        .await?;
    let frames = node.peer.barrier(second).await?;
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "ending binding must discard input without retargeting its sibling: {frames:?}"
    );
    node.peer
        .send(
            first.incoming,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await?;
    node.peer.barrier(second).await?;

    let replacement = Binding {
        incoming: 1,
        outgoing: 0,
    };
    let mut c = node.session(replacement).await?;
    let mut receiver_c = node.receiver(&mut c, replacement, 0).await?;
    node.incoming_message(&mut receiver_c, replacement, 0, 0, 3)
        .await?;
    node.incoming_message(&mut receiver_b, second, 0, 1, 4)
        .await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, receiver_a.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    node.shutdown().await;
    Ok(())
}

async fn client_crossed_replies() -> TestResult {
    let mut node = Node::new(false, 1).await?;
    node.abandon_begin(0).await?;
    let live = Binding {
        incoming: 0,
        outgoing: 1,
    };
    let abandoned = Binding {
        incoming: 1,
        outgoing: 0,
    };
    let mut session = node.session(live).await?;
    node.peer
        .send(
            abandoned.incoming,
            Performative::Begin(Begin {
                remote_channel: Some(abandoned.outgoing),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await?;
    node.peer.barrier(abandoned).await?;
    let mut receiver = node.receiver(&mut session, live, 0).await?;
    node.incoming_message(&mut receiver, live, 0, 0, 5).await?;

    node.peer
        .send(
            abandoned.incoming,
            Performative::Transfer(transfer(99, 0)),
            encode_message(&Message::data(vec![6]))?,
        )
        .await?;
    node.peer
        .end(abandoned, Some("amqp:session:unattached-handle"))
        .await?;
    node.peer
        .send(
            abandoned.incoming,
            Performative::Begin(Begin {
                remote_channel: Some(live.outgoing),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await?;
    let frames = node.peer.barrier(live).await?;
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "ending peer binding cannot steal the live pending association: {frames:?}"
    );
    node.peer
        .send(
            abandoned.incoming,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await?;
    node.peer.barrier(live).await?;

    let replacement = Binding {
        incoming: 27,
        outgoing: 0,
    };
    let mut replacement_session = node.session(replacement).await?;
    let mut replacement_receiver = node
        .receiver(&mut replacement_session, replacement, 0)
        .await?;
    node.incoming_message(&mut replacement_receiver, replacement, 0, 0, 7)
        .await?;
    node.incoming_message(&mut receiver, live, 0, 1, 8).await?;
    node.shutdown().await;
    Ok(())
}

async fn local_number_is_not_an_incoming_alias(server: bool, incoming: u16) -> TestResult {
    let mut node = Node::new(server, 0).await?;
    let binding = Binding {
        incoming,
        outgoing: 0,
    };
    let _session = node.session(binding).await?;
    let alias = Binding {
        incoming: 0,
        outgoing: 0,
    };
    node.peer
        .send(0, Performative::Flow(node.peer.flow(alias)), Vec::new())
        .await?;
    node.peer.close(FRAMING_ERROR).await?;
    node.shutdown().await;
    Ok(())
}

async fn server_wrong_response_association() -> TestResult {
    let mut node = Node::new(true, 0).await?;
    node.peer
        .send(
            55,
            Performative::Begin(Begin {
                remote_channel: Some(0),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await?;
    node.peer.close(FRAMING_ERROR).await?;
    let Connection::Server(connection) = &mut node.connection else {
        panic!("server")
    };
    assert!(
        timeout(IO_TIMEOUT, connection.next_incoming_session())
            .await?
            .is_none()
    );
    node.shutdown().await;
    Ok(())
}

async fn client_unknown_response_association() -> TestResult {
    let mut node = Node::new(false, 0).await?;
    node.bad_client_reply(0, 27, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn client_duplicate_local_association() -> TestResult {
    let mut node = Node::new(false, 1).await?;
    let _session = node
        .session(Binding {
            incoming: 27,
            outgoing: 0,
        })
        .await?;
    node.bad_client_reply(1, 28, 0).await?;
    node.shutdown().await;
    Ok(())
}

async fn client_duplicate_peer_association() -> TestResult {
    let mut node = Node::new(false, 1).await?;
    let _session = node
        .session(Binding {
            incoming: 27,
            outgoing: 0,
        })
        .await?;
    node.bad_client_reply(1, 27, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn client_unsolicited_session() -> TestResult {
    let mut node = Node::new(false, 0).await?;
    node.peer
        .send(27, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    node.peer.close("amqp:not-implemented").await?;
    node.shutdown().await;
    Ok(())
}

async fn server_outgoing_range_exhaustion() -> TestResult {
    let mut node = Node::new(true, 0).await?;
    let mut session = node
        .session(Binding {
            incoming: 55,
            outgoing: 0,
        })
        .await?;
    node.peer
        .send(56, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    node.peer.close("amqp:resource-limit-exceeded").await?;
    let Connection::Server(connection) = &mut node.connection else {
        panic!("server")
    };
    assert!(
        timeout(IO_TIMEOUT, connection.next_incoming_session())
            .await?
            .is_none()
    );
    let Session::Server(session) = &mut session else {
        panic!("server session")
    };
    assert!(
        timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .is_none()
    );
    node.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn server_channels_are_independent_through_both_link_roles_and_session_reuse() -> TestResult {
    timeout(CASE_TIMEOUT, full_lifecycle(true, 55)).await?
}

#[tokio::test]
async fn client_channels_are_independent_through_both_link_roles_and_session_reuse() -> TestResult {
    timeout(CASE_TIMEOUT, full_lifecycle(false, 27)).await?
}

#[tokio::test]
async fn server_crossed_bindings_isolate_deliveries_and_preserve_ending_sibling_until_end()
-> TestResult {
    timeout(CASE_TIMEOUT, server_crossed_bindings()).await?
}

#[tokio::test]
async fn client_out_of_order_crossed_replies_preserve_live_and_abandoned_session_owners()
-> TestResult {
    timeout(CASE_TIMEOUT, client_crossed_replies()).await?
}

#[tokio::test]
async fn server_does_not_accept_its_local_channel_as_a_peer_channel_alias() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        local_number_is_not_an_incoming_alias(true, 55),
    )
    .await?
}

#[tokio::test]
async fn client_does_not_accept_its_local_channel_as_a_peer_channel_alias() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        local_number_is_not_an_incoming_alias(false, 27),
    )
    .await?
}

#[tokio::test]
async fn server_cannot_acknowledge_a_session_it_never_initiated() -> TestResult {
    timeout(CASE_TIMEOUT, server_wrong_response_association()).await?
}

#[tokio::test]
async fn client_wrong_remote_channel_cannot_resolve_a_pending_begin() -> TestResult {
    timeout(CASE_TIMEOUT, client_unknown_response_association()).await?
}

#[tokio::test]
async fn client_live_local_channel_cannot_be_associated_with_a_second_peer_channel() -> TestResult {
    timeout(CASE_TIMEOUT, client_duplicate_local_association()).await?
}

#[tokio::test]
async fn client_live_peer_channel_cannot_be_associated_with_a_second_local_channel() -> TestResult {
    timeout(CASE_TIMEOUT, client_duplicate_peer_association()).await?
}

#[tokio::test]
async fn client_peer_initiated_session_is_explicitly_not_implemented() -> TestResult {
    timeout(CASE_TIMEOUT, client_unsolicited_session()).await?
}

#[tokio::test]
async fn server_peer_channel_max_bounds_local_output_not_peer_input() -> TestResult {
    timeout(CASE_TIMEOUT, server_outgoing_range_exhaustion()).await?
}
