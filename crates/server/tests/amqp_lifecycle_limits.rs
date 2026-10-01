//! Connection lifecycle limits retain ownership until the peer acknowledges it.

use std::{error::Error, time::Duration};

use amqp::{
    AmqpError, Attach, Begin, ClientConnection, ClientSender, ClientSession, Close, Detach, End,
    EngineError, Flow, Frame, IncomingSession, LinkEndpoint, Open, Performative, ProtocolHeader,
    Receiver, ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession, Source,
    Target, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_LIMIT: u16 = 32;
const LINK_LIMIT: u32 = 128;

struct Peer(TcpStream);

impl Peer {
    async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.0,
                &Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.0)).await??)
    }

    async fn barrier(&mut self, channel: u16) -> TestResult<Vec<Frame>> {
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: 0,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
        )
        .await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Flow(flow)),
                ..
            } if *actual == channel && flow.handle.is_none() && flow.next_incoming_id == Some(0));
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn end_reply(&mut self, channel: u16, resource_limit: bool) -> TestResult {
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::End(end)),
            ..
        } = self.read().await?
        else {
            panic!("expected session End");
        };
        assert_eq!(actual, channel);
        if resource_limit {
            assert_eq!(
                end.error.expect("resource refusal").condition,
                AmqpError::ResourceLimitExceeded.into()
            );
        } else {
            assert!(end.error.is_none());
        }
        Ok(())
    }
}

struct ServerNode {
    connection: ServerConnection,
    peer: Peer,
}

impl ServerNode {
    async fn new() -> TestResult<Self> {
        let listener = timeout(IO_TIMEOUT, TcpListener::bind("127.0.0.1:0")).await??;
        let address = listener.local_addr()?;
        let mut stream = timeout(IO_TIMEOUT, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        let (socket, _) = timeout(IO_TIMEOUT, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept(
            socket,
            format!("lifecycle-server-{}", address.port()),
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
            0,
            Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: None,
                ..Open::new(format!("lifecycle-peer-{}", address.port()))
            }),
        )
        .await?;
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            connection: timeout(IO_TIMEOUT, accepting).await???,
            peer,
        })
    }

    async fn incoming(&mut self, channel: u16) -> TestResult<IncomingSession> {
        self.peer
            .send(channel, Performative::Begin(Begin::default()))
            .await?;
        Ok(timeout(IO_TIMEOUT, self.connection.next_incoming_session())
            .await?
            .expect("incoming session"))
    }

    async fn session(&mut self, channel: u16) -> TestResult<ServerSession> {
        let incoming = self.incoming(channel).await?;
        let session = timeout(IO_TIMEOUT, self.connection.accept_session(incoming)).await??;
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Begin(begin)),
            ..
        } if actual == channel && begin.remote_channel == Some(channel)));
        Ok(session)
    }

    async fn receiver(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
    ) -> TestResult<Receiver> {
        self.peer
            .send(
                channel,
                Performative::Attach(Box::new(attach(channel, handle))),
            )
            .await?;
        let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("incoming attach");
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(incoming, 262_144)).await??
        else {
            panic!("peer sender creates receiver");
        };
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Attach(response)),
            ..
        } if actual == channel && response.handle == handle));
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Flow(flow)),
            ..
        } if actual == channel && flow.handle == Some(handle) && flow.link_credit == Some(32)));
        Ok(receiver)
    }

    async fn finish(self) -> TestResult {
        timeout(IO_TIMEOUT, self.connection.shutdown()).await?;
        Ok(())
    }
}

struct ClientNode {
    connection: ClientConnection,
    peer: Peer,
}

impl ClientNode {
    async fn new() -> TestResult<Self> {
        let listener = timeout(IO_TIMEOUT, TcpListener::bind("127.0.0.1:0")).await??;
        let address = listener.local_addr()?;
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        let (mut socket, _) = timeout(IO_TIMEOUT, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let opening = tokio::spawn(
            ClientConnection::builder()
                .container_id(format!("lifecycle-client-{}", address.port()))
                .max_frame_size(512)
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
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer.send(
            0,
            Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: None,
                ..Open::new(format!("lifecycle-raw-server-{}", address.port()))
            }),
        )
        .await?;
        Ok(Self {
            connection: timeout(IO_TIMEOUT, opening).await???,
            peer,
        })
    }

    async fn session(&mut self, expected: u16) -> TestResult<ClientSession> {
        let (session, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(self.connection.begin().await?) },
                async {
                    assert!(matches!(self.peer.read().await?, Frame::Amqp {
                        channel,
                        performative: Some(Performative::Begin(begin)),
                        ..
                    } if channel == expected && begin.remote_channel.is_none()));
                    self.peer
                        .send(
                            expected,
                            Performative::Begin(Begin {
                                remote_channel: Some(expected),
                                ..Begin::default()
                            }),
                        )
                        .await
                }
            )
        })
        .await??;
        Ok(session)
    }

    async fn sender(
        &mut self,
        session: &mut ClientSession,
        channel: u16,
        handle: u32,
    ) -> TestResult<ClientSender> {
        let (sender, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async {
                    Ok::<_, Box<dyn Error>>(
                        session
                            .attach_sender(format!("client-link-{channel}-{handle}"), "queue")
                            .await?,
                    )
                },
                async {
                    let Frame::Amqp {
                        channel: actual,
                        performative: Some(Performative::Attach(request)),
                        ..
                    } = self.peer.read().await?
                    else {
                        panic!("client Attach request");
                    };
                    assert_eq!(actual, channel);
                    assert_eq!(request.handle, handle);
                    let response = request.response(request.source.clone(), request.target.clone());
                    self.peer
                        .send(channel, Performative::Attach(Box::new(response)))
                        .await
                }
            )
        })
        .await??;
        Ok(sender)
    }

    async fn finish(self) -> TestResult {
        timeout(IO_TIMEOUT, self.connection.shutdown()).await?;
        Ok(())
    }
}

fn attach(channel: u16, handle: u32) -> Attach {
    Attach {
        name: format!("server-link-{channel}-{handle}"),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn assert_only_barrier(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(frame, Frame::Amqp {
        performative: Some(Performative::Flow(flow)),
        ..
    } if flow.handle.is_none())),
        "local limit must not emit admission frames: {frames:?}"
    );
}

#[tokio::test]
async fn server_session_limit_closes_without_publishing_an_excess_session() -> TestResult {
    let mut node = ServerNode::new().await?;
    let _accepted = node.session(0).await?;
    let mut pending = Vec::new();
    for channel in 1..SESSION_LIMIT {
        pending.push(node.incoming(channel).await?);
    }
    node.peer
        .send(SESSION_LIMIT, Performative::Begin(Begin::default()))
        .await?;
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Close(close)),
        ..
    } = node.peer.read().await?
    else {
        panic!("excess Begin must produce Close, not Begin or End");
    };
    assert_eq!(
        close.error.expect("resource refusal").condition,
        AmqpError::ResourceLimitExceeded.into()
    );
    for incoming in pending {
        assert!(matches!(
            timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    node.peer
        .send(0, Performative::Close(Close::default()))
        .await?;
    assert!(
        timeout(IO_TIMEOUT, node.connection.next_incoming_session())
            .await?
            .is_none()
    );
    assert!(
        matches!(timeout(IO_TIMEOUT, read_frame(&mut node.peer.0)).await?,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof)
    );
    node.finish().await
}

#[tokio::test]
async fn client_session_limit_counts_pending_and_ending_sessions_until_peer_ack() -> TestResult {
    let mut node = ClientNode::new().await?;
    let mut sessions = Vec::new();
    for channel in 0..SESSION_LIMIT - 1 {
        sessions.push(node.session(channel).await?);
    }
    let mut pending = Box::pin(node.connection.begin());
    timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut pending => panic!("Begin completed without peer ACK"),
            frame = node.peer.read() => {
                assert!(matches!(frame?, Frame::Amqp {
                    channel,
                    performative: Some(Performative::Begin(_)),
                    ..
                } if channel == SESSION_LIMIT - 1));
                Ok::<_, Box<dyn Error>>(())
            }
        }
    })
    .await??;
    drop(pending);
    assert!(matches!(
        timeout(IO_TIMEOUT, node.connection.begin()).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_only_barrier(&node.peer.barrier(0).await?);
    node.peer
        .send(
            SESSION_LIMIT - 1,
            Performative::Begin(Begin {
                remote_channel: Some(SESSION_LIMIT - 1),
                ..Begin::default()
            }),
        )
        .await?;
    node.peer
        .send(SESSION_LIMIT - 1, Performative::End(End::default()))
        .await?;
    node.peer.end_reply(SESSION_LIMIT - 1, false).await?;
    let _replacement = node.session(SESSION_LIMIT).await?;

    let mut ending = Box::pin(sessions[0].end());
    timeout(IO_TIMEOUT, async {
        tokio::select! {
            result = &mut ending => panic!("End completed without peer ACK: {result:?}"),
            frame = node.peer.read() => {
                assert!(matches!(frame?, Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::End(_)),
                    ..
                }));
                Ok::<_, Box<dyn Error>>(())
            }
        }
    })
    .await??;
    assert!(matches!(
        timeout(IO_TIMEOUT, node.connection.begin()).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_only_barrier(&node.peer.barrier(1).await?);
    node.peer.send(0, Performative::End(End::default())).await?;
    timeout(IO_TIMEOUT, ending).await??;
    let _fresh = node.session(SESSION_LIMIT + 1).await?;
    node.finish().await
}

#[tokio::test]
async fn server_connection_link_limit_keeps_closing_alias_until_ack_then_reuses_slot() -> TestResult
{
    let mut node = ServerNode::new().await?;
    let mut first = node.session(0).await?;
    let mut second = node.session(1).await?;
    let mut receivers = Vec::new();
    for handle in 0..LINK_LIMIT {
        receivers.push(node.receiver(&mut first, 0, handle).await?);
        receivers.push(node.receiver(&mut second, 1, handle).await?);
    }
    let mut target = node.session(2).await?;
    timeout(IO_TIMEOUT, receivers[0].close()).await??;
    assert!(matches!(node.peer.read().await?, Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Detach(detach)),
        ..
    } if detach.handle == 0 && detach.closed && detach.error.is_none()));

    node.peer
        .send(2, Performative::Attach(Box::new(attach(2, 0))))
        .await?;
    node.peer.end_reply(2, true).await?;
    assert!(
        timeout(IO_TIMEOUT, target.next_incoming_attach())
            .await?
            .is_none()
    );
    assert_only_barrier(&node.peer.barrier(0).await?);
    node.peer.send(2, Performative::End(End::default())).await?;
    assert_only_barrier(&node.peer.barrier(0).await?);
    let mut target = node.session(2).await?;

    node.peer
        .send(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        )
        .await?;
    assert_only_barrier(&node.peer.barrier(0).await?);
    let _replacement = node.receiver(&mut target, 2, 0).await?;
    assert_only_barrier(&node.peer.barrier(1).await?);
    node.finish().await
}

#[tokio::test]
async fn client_session_link_limit_refuses_without_advancing_handle_until_detach_ack() -> TestResult
{
    let mut node = ClientNode::new().await?;
    let mut session = node.session(0).await?;
    let mut senders = Vec::new();
    for handle in 0..LINK_LIMIT {
        senders.push(node.sender(&mut session, 0, handle).await?);
    }
    let mut closing = Box::pin(senders[0].close());
    timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut closing => panic!("Detach completed without peer ACK"),
            frame = node.peer.read() => {
                assert!(matches!(frame?, Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } if detach.handle == 0 && detach.closed));
                Ok::<_, Box<dyn Error>>(())
            }
        }
    })
    .await??;
    assert!(matches!(
        timeout(IO_TIMEOUT, session.attach_sender("excess", "queue")).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_only_barrier(&node.peer.barrier(0).await?);
    node.peer
        .send(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        )
        .await?;
    timeout(IO_TIMEOUT, closing).await??;
    let _replacement = node.sender(&mut session, 0, LINK_LIMIT).await?;
    assert_only_barrier(&node.peer.barrier(0).await?);
    node.finish().await
}

#[tokio::test]
async fn client_connection_link_limit_counts_abandoned_pending_attach_and_reuses_detached_slot()
-> TestResult {
    let mut node = ClientNode::new().await?;
    let mut first = node.session(0).await?;
    let mut second = node.session(1).await?;
    let mut target = node.session(2).await?;
    let mut senders = Vec::new();
    for handle in 0..LINK_LIMIT {
        senders.push(node.sender(&mut first, 0, handle).await?);
        if handle < LINK_LIMIT - 1 {
            senders.push(node.sender(&mut second, 1, handle).await?);
        }
    }

    let mut pending = Box::pin(second.attach_sender("pending-global-last", "queue"));
    let request = timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut pending => panic!("Attach completed without peer echo"),
            frame = node.peer.read() => {
                let Frame::Amqp {
                    channel: 1,
                    performative: Some(Performative::Attach(request)),
                    ..
                } = frame? else {
                    panic!("last globally admitted Attach");
                };
                assert_eq!(request.handle, LINK_LIMIT - 1);
                Ok::<_, Box<dyn Error>>(request)
            }
        }
    })
    .await??;
    drop(pending);
    assert!(matches!(
        timeout(IO_TIMEOUT, target.attach_sender("excess-global", "queue")).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_only_barrier(&node.peer.barrier(0).await?);

    let response = request.response(request.source.clone(), request.target.clone());
    node.peer
        .send(1, Performative::Attach(Box::new(response)))
        .await?;
    assert_only_barrier(&node.peer.barrier(0).await?);
    node.peer
        .send(
            1,
            Performative::Detach(Detach {
                handle: LINK_LIMIT - 1,
                closed: true,
                error: None,
            }),
        )
        .await?;
    assert!(matches!(node.peer.read().await?, Frame::Amqp {
        channel: 1,
        performative: Some(Performative::Detach(detach)),
        ..
    } if detach.handle == LINK_LIMIT - 1 && detach.closed && detach.error.is_none()));
    let _replacement = node.sender(&mut target, 2, 0).await?;
    assert_only_barrier(&node.peer.barrier(2).await?);
    node.finish().await
}
