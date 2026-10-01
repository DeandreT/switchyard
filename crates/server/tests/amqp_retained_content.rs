//! Retained receiving content shares a connection allowance across link and session lifetimes.

use std::{error::Error, time::Duration};

use amqp::{
    AmqpError, Attach, Begin, ClientConnection, ClientReceiver, ClientSession, Delivery, End,
    EngineError, Frame, LinkEndpoint, Message, Open, Performative, ProtocolHeader, Receiver,
    ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession, Source, Target,
    Transfer, encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const PAYLOAD_CHUNK: usize = 64 * 1024;
const MESSAGES_PER_INBOX: u32 = 8;

struct Peer(TcpStream);

impl Peer {
    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.0,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.0)).await??)
    }

    async fn control(&mut self) -> TestResult<Performative> {
        timeout(IO_TIMEOUT, async {
            loop {
                match self.read().await? {
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Flow(_)),
                        ..
                    } => {}
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(performative),
                        payload,
                    } => {
                        assert!(payload.is_empty());
                        return Ok(performative);
                    }
                    other => panic!("expected a session control frame: {other:?}"),
                }
            }
        })
        .await?
    }

    async fn credit(&mut self, handle: u32) -> TestResult {
        timeout(IO_TIMEOUT, async {
            loop {
                match self.read().await? {
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Flow(flow)),
                        ..
                    } => {
                        if flow.handle == Some(handle) {
                            assert_eq!(flow.link_credit, Some(32));
                            return Ok(());
                        }
                    }
                    other => panic!("expected receiving link credit: {other:?}"),
                }
            }
        })
        .await?
    }

    async fn delivery(&mut self, handle: u32, id: u32, payload: &[u8]) -> TestResult {
        for (index, chunk) in payload.chunks(PAYLOAD_CHUNK).enumerate() {
            let first = index == 0;
            self.send(
                Performative::Transfer(Transfer {
                    handle,
                    delivery_id: first.then_some(id),
                    delivery_tag: first.then(|| id.to_be_bytes().to_vec().into()),
                    message_format: first.then_some(0),
                    settled: first.then_some(false),
                    more: (index + 1) * PAYLOAD_CHUNK < payload.len(),
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

    async fn refused(&mut self, handle: u32) -> TestResult {
        let Performative::Detach(detach) = self.control().await? else {
            panic!("retained-content refusal must detach only its receiving link");
        };
        assert_eq!(detach.handle, handle);
        assert!(detach.closed);
        assert_eq!(
            detach.error.expect("resource refusal").condition,
            AmqpError::ResourceLimitExceeded.into()
        );
        Ok(())
    }
}

enum Endpoint {
    Server(Receiver),
    Client(Box<ClientReceiver>),
}

impl Endpoint {
    async fn recv(&mut self) -> Result<Delivery, EngineError> {
        match self {
            Self::Server(receiver) => receiver.recv().await,
            Self::Client(receiver) => receiver.recv().await,
        }
    }
}

enum Node {
    Server {
        connection: ServerConnection,
        session: ServerSession,
    },
    Client {
        connection: ClientConnection,
        session: ClientSession,
    },
}

struct Fixture {
    node: Node,
    peer: Peer,
    next_handle: u32,
    next_link_name: u64,
}

impl Fixture {
    async fn server() -> TestResult<Self> {
        let listener = timeout(IO_TIMEOUT, TcpListener::bind("127.0.0.1:0")).await??;
        let address = listener.local_addr()?;
        let mut stream = timeout(IO_TIMEOUT, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        let (socket, _) = timeout(IO_TIMEOUT, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept(
            socket,
            format!("retained-content-server-{}", address.port()),
            None,
        ));
        timeout(
            IO_TIMEOUT,
            write_protocol_header(&mut stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(IO_TIMEOUT, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        let mut peer = Peer(stream);
        peer.send(
            Performative::Open(Open {
                max_frame_size: 512,
                channel_max: 0,
                idle_time_out: None,
                ..Open::new(format!("retained-content-peer-{}", address.port()))
            }),
            Vec::new(),
        )
        .await?;
        assert!(matches!(peer.control().await?, Performative::Open(open)
            if open.max_frame_size >= PAYLOAD_CHUNK as u32 + 512));
        let mut connection = timeout(IO_TIMEOUT, accepting).await???;
        peer.send(Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(IO_TIMEOUT, connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let session = timeout(IO_TIMEOUT, connection.accept_session(incoming)).await??;
        assert!(matches!(peer.control().await?, Performative::Begin(begin)
            if begin.remote_channel == Some(0)));
        Ok(Self {
            node: Node::Server {
                connection,
                session,
            },
            peer,
            next_handle: 0,
            next_link_name: 0,
        })
    }

    async fn client() -> TestResult<Self> {
        let listener = timeout(IO_TIMEOUT, TcpListener::bind("127.0.0.1:0")).await??;
        let address = listener.local_addr()?;
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        let (mut socket, _) = timeout(IO_TIMEOUT, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let opening = tokio::spawn(
            ClientConnection::builder()
                .container_id(format!("retained-content-client-{}", address.port()))
                .idle_timeout_millis(0)
                .open_with_stream(stream),
        );
        assert_eq!(
            timeout(IO_TIMEOUT, read_protocol_header(&mut socket)).await??,
            ProtocolHeader::AMQP
        );
        timeout(
            IO_TIMEOUT,
            write_protocol_header(&mut socket, ProtocolHeader::AMQP),
        )
        .await??;
        let mut peer = Peer(socket);
        assert!(matches!(peer.control().await?, Performative::Open(open)
            if open.max_frame_size >= PAYLOAD_CHUNK as u32 + 512));
        peer.send(
            Performative::Open(Open {
                max_frame_size: 512,
                channel_max: 0,
                idle_time_out: None,
                ..Open::new(format!("retained-content-raw-server-{}", address.port()))
            }),
            Vec::new(),
        )
        .await?;
        let mut connection = timeout(IO_TIMEOUT, opening).await???;
        let session = begin_client_session(&mut connection, &mut peer).await?;
        Ok(Self {
            node: Node::Client {
                connection,
                session,
            },
            peer,
            next_handle: 0,
            next_link_name: 0,
        })
    }

    async fn receiver(&mut self) -> TestResult<(Endpoint, u32)> {
        let handle = self.next_handle;
        self.next_handle += 1;
        let name = format!("retained-link-{}", self.next_link_name);
        self.next_link_name += 1;
        let endpoint = match &mut self.node {
            Node::Server { session, .. } => {
                self.peer
                    .send(
                        Performative::Attach(Box::new(sender_attach(handle, name))),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
                    .await?
                    .expect("incoming sender attach");
                let LinkEndpoint::Receiver(receiver) = timeout(
                    IO_TIMEOUT,
                    session.accept_attach(incoming, MESSAGE_BYTES as u64),
                )
                .await??
                else {
                    panic!("server receiving endpoint");
                };
                assert!(
                    matches!(self.peer.control().await?, Performative::Attach(response)
                    if response.handle == handle && response.role == Role::Receiver
                        && response.max_message_size == Some(MESSAGE_BYTES as u64))
                );
                Endpoint::Server(receiver)
            }
            Node::Client { session, .. } => {
                let builder = ClientReceiver::builder().name(name).source("queue");
                let (receiver, ()) = timeout(IO_TIMEOUT, async {
                    tokio::try_join!(
                        async { Ok::<_, Box<dyn Error>>(builder.attach(session).await?) },
                        async {
                            let Performative::Attach(request) = self.peer.control().await? else {
                                panic!("client receiving Attach");
                            };
                            assert_eq!(request.handle, handle);
                            assert_eq!(request.role, Role::Receiver);
                            assert_eq!(request.max_message_size, Some(MESSAGE_BYTES as u64));
                            let response =
                                request.response(request.source.clone(), request.target.clone());
                            self.peer
                                .send(Performative::Attach(Box::new(response)), Vec::new())
                                .await
                        }
                    )
                })
                .await??;
                Endpoint::Client(Box::new(receiver))
            }
        };
        self.peer.credit(handle).await?;
        Ok((endpoint, handle))
    }

    async fn refused_probe(&mut self, id: u32, payload: &[u8]) -> TestResult {
        let (mut endpoint, handle) = self.receiver().await?;
        self.peer.delivery(handle, id, payload).await?;
        self.peer.refused(handle).await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, endpoint.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        Ok(())
    }

    async fn replace_session(&mut self) -> TestResult {
        self.peer
            .send(Performative::End(End::default()), Vec::new())
            .await?;
        assert!(matches!(self.peer.control().await?, Performative::End(end)
            if end.error.is_none()));
        match &mut self.node {
            Node::Server {
                connection,
                session,
            } => {
                self.peer
                    .send(Performative::Begin(Begin::default()), Vec::new())
                    .await?;
                let incoming = timeout(IO_TIMEOUT, connection.next_incoming_session())
                    .await?
                    .expect("replacement incoming session");
                *session = timeout(IO_TIMEOUT, connection.accept_session(incoming)).await??;
                assert!(
                    matches!(self.peer.control().await?, Performative::Begin(begin)
                    if begin.remote_channel == Some(0))
                );
            }
            Node::Client {
                connection,
                session,
            } => {
                *session = begin_client_session(connection, &mut self.peer).await?;
            }
        }
        self.next_handle = 0;
        Ok(())
    }

    async fn finish(self) -> TestResult {
        match self.node {
            Node::Server { connection, .. } => timeout(IO_TIMEOUT, connection.shutdown()).await?,
            Node::Client { connection, .. } => timeout(IO_TIMEOUT, connection.shutdown()).await?,
        }
        Ok(())
    }
}

async fn begin_client_session(
    connection: &mut ClientConnection,
    peer: &mut Peer,
) -> TestResult<ClientSession> {
    let (session, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
            async {
                assert!(matches!(peer.control().await?, Performative::Begin(_)));
                peer.send(
                    Performative::Begin(Begin {
                        remote_channel: Some(0),
                        ..Begin::default()
                    }),
                    Vec::new(),
                )
                .await
            }
        )
    })
    .await??;
    Ok(session)
}

fn sender_attach(handle: u32, name: String) -> Attach {
    Attach {
        name,
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn exercise(mut fixture: Fixture) -> TestResult {
    let exact = Message::data(vec![7; MESSAGE_BYTES - 8]);
    let exact_payload = encode_message(&exact)?;
    assert_eq!(exact_payload.len(), MESSAGE_BYTES);
    let small_payload = encode_message(&Message::data(vec![9]))?;
    let (mut first, first_handle) = fixture.receiver().await?;
    let (second, second_handle) = fixture.receiver().await?;

    for id in 0..MESSAGES_PER_INBOX {
        fixture
            .peer
            .delivery(first_handle, id * 2, &exact_payload)
            .await?;
        fixture
            .peer
            .delivery(second_handle, id * 2 + 1, &exact_payload)
            .await?;
    }
    fixture
        .peer
        .delivery(first_handle, 16, &small_payload)
        .await?;
    fixture.peer.refused(first_handle).await?;

    // A detached link's public inbox still owns its eight unread deliveries.
    fixture.refused_probe(17, &small_payload).await?;
    fixture.replace_session().await?;
    fixture.refused_probe(0, &small_payload).await?;

    let old_delivery = timeout(IO_TIMEOUT, first.recv()).await??;
    assert_eq!(old_delivery.message(), &exact);
    let (mut fresh, fresh_handle) = fixture.receiver().await?;
    fixture
        .peer
        .delivery(fresh_handle, 1, &exact_payload)
        .await?;
    // Receiving refunds the allowance without settling or dropping the returned value.
    fixture.refused_probe(2, &small_payload).await?;

    drop(second);
    let (mut replacement, replacement_handle) = fixture.receiver().await?;
    for id in 3..3 + MESSAGES_PER_INBOX {
        fixture
            .peer
            .delivery(replacement_handle, id, &exact_payload)
            .await?;
    }
    fixture
        .peer
        .delivery(replacement_handle, 11, &small_payload)
        .await?;
    fixture.peer.refused(replacement_handle).await?;

    let fresh_delivery = timeout(IO_TIMEOUT, fresh.recv()).await??;
    assert_eq!(fresh_delivery.message(), &exact);
    for _ in 0..MESSAGES_PER_INBOX {
        let delivery = timeout(IO_TIMEOUT, replacement.recv()).await??;
        assert_eq!(delivery.message(), &exact);
    }
    assert!(matches!(
        timeout(IO_TIMEOUT, replacement.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(old_delivery.message(), &exact);
    fixture.finish().await
}

#[tokio::test]
async fn server_retained_content_survives_detach_and_end_until_public_recv_or_inbox_drop()
-> TestResult {
    exercise(Fixture::server().await?).await
}

#[tokio::test]
async fn client_retained_content_survives_detach_and_end_until_public_recv_or_inbox_drop()
-> TestResult {
    exercise(Fixture::client().await?).await
}
