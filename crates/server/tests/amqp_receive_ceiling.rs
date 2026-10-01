//! Native receivers advertise and enforce a finite encoded-message ceiling.

use std::{error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ClientSession, Delivery,
    DeliveryState, EngineError, ErrorCondition, Frame, LinkEndpoint, Message, Open, Performative,
    ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection,
    ServerSession, Source, Target, Transfer, encode_message, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const RECEIVE_CEILING: usize = 4 * 1024 * 1024;
const PAYLOAD_CHUNK: usize = 64 * 1024;

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

    async fn delivery(
        &mut self,
        handle: u32,
        id: u32,
        payload: &[u8],
        leave_open: bool,
    ) -> TestResult {
        for (index, chunk) in payload.chunks(PAYLOAD_CHUNK).enumerate() {
            let first = index == 0;
            let more = leave_open || (index + 1) * PAYLOAD_CHUNK < payload.len();
            self.send(
                Performative::Transfer(Transfer {
                    handle,
                    delivery_id: first.then_some(id),
                    delivery_tag: first.then(|| id.to_be_bytes().to_vec().into()),
                    message_format: first.then_some(0),
                    settled: first.then_some(false),
                    more,
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

    async fn overflow_continuation(&mut self, handle: u32) -> TestResult {
        self.send(
            Performative::Transfer(Transfer {
                handle,
                delivery_id: None,
                delivery_tag: None,
                message_format: None,
                settled: None,
                more: true,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            vec![0],
        )
        .await
    }

    async fn accepted(&mut self, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert!(disposition.settled);
                    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("fitting message must be accepted without refusal: {other:?}"),
            }
        }
    }

    async fn oversized(&mut self, handle: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach.error.expect("size refusal").condition,
                        ErrorCondition::Custom("amqp:link:message-size-exceeded".into())
                    );
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("only oversized link must be detached: {other:?}"),
            }
        }
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

    async fn accept(&self, delivery: &Delivery) -> Result<(), EngineError> {
        match self {
            Self::Server(receiver) => receiver.accept(delivery).await,
            Self::Client(receiver) => receiver.accept(delivery).await,
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
            format!("receive-ceiling-server-{}", address.port()),
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
                idle_time_out: None,
                ..Open::new(format!("receive-ceiling-peer-{}", address.port()))
            }),
            Vec::new(),
        )
        .await?;
        assert!(matches!(peer.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(open)),
            ..
        } if open.max_frame_size >= PAYLOAD_CHUNK as u32 + 512));
        let mut connection = timeout(IO_TIMEOUT, accepting).await???;
        peer.send(Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(IO_TIMEOUT, connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let session = timeout(IO_TIMEOUT, connection.accept_session(incoming)).await??;
        assert!(matches!(peer.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Begin(begin)),
            ..
        } if begin.remote_channel == Some(0)));
        Ok(Self {
            node: Node::Server {
                connection,
                session,
            },
            peer,
            next_handle: 0,
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
                .container_id(format!("receive-ceiling-client-{}", address.port()))
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
        assert!(matches!(peer.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(open)),
            ..
        } if open.max_frame_size >= PAYLOAD_CHUNK as u32 + 512));
        peer.send(
            Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: None,
                ..Open::new(format!("receive-ceiling-raw-server-{}", address.port()))
            }),
            Vec::new(),
        )
        .await?;
        let mut connection = timeout(IO_TIMEOUT, opening).await???;
        let (session, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
                async {
                    assert!(matches!(
                        peer.read().await?,
                        Frame::Amqp {
                            channel: 0,
                            performative: Some(Performative::Begin(_)),
                            ..
                        }
                    ));
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
        Ok(Self {
            node: Node::Client {
                connection,
                session,
            },
            peer,
            next_handle: 0,
        })
    }

    async fn receiver(
        &mut self,
        requested: Option<u64>,
        expected: u64,
    ) -> TestResult<(Endpoint, u32)> {
        let handle = self.next_handle;
        self.next_handle += 1;
        let endpoint = match &mut self.node {
            Node::Server { session, .. } => {
                self.peer
                    .send(
                        Performative::Attach(Box::new(sender_attach(handle))),
                        Vec::new(),
                    )
                    .await?;
                let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
                    .await?
                    .expect("incoming sender attach");
                let LinkEndpoint::Receiver(receiver) = timeout(
                    IO_TIMEOUT,
                    session.accept_attach(incoming, requested.unwrap_or(0)),
                )
                .await??
                else {
                    panic!("server receiving endpoint");
                };
                assert!(matches!(self.peer.read().await?, Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(response)),
                    ..
                } if response.handle == handle && response.role == Role::Receiver
                    && response.max_message_size == Some(expected)));
                Endpoint::Server(receiver)
            }
            Node::Client { session, .. } => {
                let mut builder = ClientReceiver::builder()
                    .name(format!("client-{handle}"))
                    .source("queue");
                if let Some(requested) = requested {
                    builder = builder.max_message_size(requested);
                }
                let (receiver, ()) = timeout(IO_TIMEOUT, async {
                    tokio::try_join!(
                        async { Ok::<_, Box<dyn Error>>(builder.attach(session).await?) },
                        async {
                            let Frame::Amqp {
                                channel: 0,
                                performative: Some(Performative::Attach(request)),
                                ..
                            } = self.peer.read().await?
                            else {
                                panic!("client receiving Attach");
                            };
                            assert_eq!(request.handle, handle);
                            assert_eq!(request.role, Role::Receiver);
                            assert_eq!(request.max_message_size, Some(expected));
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
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Flow(flow)),
            ..
        } if flow.handle == Some(handle) && flow.link_credit == Some(32)));
        Ok((endpoint, handle))
    }

    async fn finish(self) -> TestResult {
        match self.node {
            Node::Server { connection, .. } => timeout(IO_TIMEOUT, connection.shutdown()).await?,
            Node::Client { connection, .. } => timeout(IO_TIMEOUT, connection.shutdown()).await?,
        }
        Ok(())
    }
}

fn sender_attach(handle: u32) -> Attach {
    Attach {
        name: format!("peer-{handle}"),
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

fn message_of_encoded_size(bytes: usize) -> Message {
    let message = Message::data(vec![7; bytes - 8]);
    assert_eq!(
        encode_message(&message)
            .expect("boundary message encodes")
            .len(),
        bytes
    );
    message
}

async fn fitting(
    fixture: &mut Fixture,
    endpoint: &mut Endpoint,
    handle: u32,
    id: u32,
    message: &Message,
) -> TestResult {
    fixture
        .peer
        .delivery(handle, id, &encode_message(message)?, false)
        .await?;
    let delivery = timeout(IO_TIMEOUT, endpoint.recv()).await??;
    assert_eq!(delivery.message(), message);
    timeout(IO_TIMEOUT, endpoint.accept(&delivery)).await??;
    fixture.peer.accepted(id).await
}

async fn exercise(mut fixture: Fixture) -> TestResult {
    let maximum = RECEIVE_CEILING as u64;
    let mut endpoints = Vec::new();
    for (requested, expected) in [
        (None, maximum),
        (Some(0), maximum),
        (Some(maximum), maximum),
        (Some(maximum + 1), maximum),
        (Some(u64::MAX), maximum),
        (Some(513), 513),
    ] {
        endpoints.push(fixture.receiver(requested, expected).await?);
    }

    let exact = message_of_encoded_size(RECEIVE_CEILING);
    fitting(&mut fixture, &mut endpoints[0].0, 0, 0, &exact).await?;
    fixture
        .peer
        .delivery(0, 1, &encode_message(&exact)?, true)
        .await?;
    fixture.peer.overflow_continuation(0).await?;
    fixture.peer.oversized(0).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, endpoints[0].0.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));

    let small = message_of_encoded_size(513);
    fitting(&mut fixture, &mut endpoints[5].0, 5, 2, &small).await?;
    fixture
        .peer
        .delivery(5, 3, &encode_message(&message_of_encoded_size(514))?, false)
        .await?;
    fixture.peer.oversized(5).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, endpoints[5].0.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    fitting(
        &mut fixture,
        &mut endpoints[1].0,
        1,
        4,
        &Message::data(vec![9; 32]),
    )
    .await?;
    fixture.finish().await
}

#[tokio::test]
async fn server_receive_ceiling_is_advertised_and_enforced_without_harming_siblings() -> TestResult
{
    exercise(Fixture::server().await?).await
}

#[tokio::test]
async fn client_receive_ceiling_clamps_omitted_zero_and_large_requests_and_stops_endless_more()
-> TestResult {
    exercise(Fixture::client().await?).await
}
