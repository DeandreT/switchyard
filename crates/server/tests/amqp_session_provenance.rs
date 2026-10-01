//! Fresh session and attach approvals across numeric channel/handle reuse.

use std::{
    collections::HashMap,
    error::Error,
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientSession, DeliveryState, Detach, End,
    EngineError, Flow, Frame, IncomingAttach, IncomingSession, LinkEndpoint, Message, Open,
    Performative, ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode,
    ServerConnection, ServerSession, Source, Target, Transfer, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

struct Peer {
    stream: TcpStream,
    outgoing: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if let Performative::Begin(begin) = &performative {
            self.outgoing.insert(channel, begin.next_outgoing_id);
        } else if matches!(performative, Performative::Transfer(_)) {
            let next = self.outgoing.entry(channel).or_default();
            *next = next.wrapping_add(1);
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
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.stream)).await??)
    }

    fn flow(&self, channel: u16) -> Flow {
        Flow {
            next_incoming_id: Some(0),
            incoming_window: 2_048,
            next_outgoing_id: self.outgoing.get(&channel).copied().unwrap_or(0),
            outgoing_window: 2_048,
            echo: true,
            ..Flow::default()
        }
    }

    async fn barrier(&mut self, channel: u16) -> TestResult<Vec<Frame>> {
        let expected = self.outgoing.get(&channel).copied().unwrap_or(0);
        self.send(channel, Performative::Flow(self.flow(channel)), Vec::new())
            .await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Flow(flow)),
                ..
            } if *actual == channel && flow.handle.is_none() && flow.next_incoming_id == Some(expected));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_)),
                        ..
                    }
                ),
                "a stale local operation must not close its connection: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn begin_reply(&mut self, channel: u16) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Begin(begin)),
            ..
        } if actual == channel && begin.remote_channel == Some(channel)));
        Ok(())
    }

    async fn detach_reply(&mut self, channel: u16, handle: u32) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Detach(detach)),
            ..
        } if actual == channel && detach.handle == handle && detach.closed && detach.error.is_none()));
        Ok(())
    }

    async fn end_reply(&mut self, channel: u16) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::End(end)),
            ..
        } if actual == channel && end.error.is_none()));
        Ok(())
    }
}

struct ServerNode {
    connection: ServerConnection,
    peer: Peer,
}

impl ServerNode {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept(
            socket,
            format!("provenance-server-{}", address.port()),
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
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut stream,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Open(Open::new(format!(
                        "raw-provenance-peer-{}",
                        address.port()
                    )))),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        assert!(matches!(
            timeout(IO_TIMEOUT, read_frame(&mut stream)).await??,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(Self {
            connection: timeout(IO_TIMEOUT, accepting).await???,
            peer: Peer {
                stream,
                outgoing: HashMap::new(),
            },
        })
    }

    async fn pending_session(&mut self, channel: u16) -> TestResult<IncomingSession> {
        self.peer
            .send(channel, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        Ok(timeout(IO_TIMEOUT, self.connection.next_incoming_session())
            .await?
            .expect("incoming session receipt"))
    }

    async fn session(&mut self, channel: u16) -> TestResult<ServerSession> {
        let incoming = self.pending_session(channel).await?;
        let session = timeout(IO_TIMEOUT, self.connection.accept_session(incoming)).await??;
        self.peer.begin_reply(channel).await?;
        Ok(session)
    }

    async fn request(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        request: &Attach,
    ) -> TestResult<IncomingAttach> {
        self.peer
            .send(
                channel,
                Performative::Attach(Box::new(request.clone())),
                Vec::new(),
            )
            .await?;
        let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("live incoming attach receipt");
        assert_eq!(incoming.attach(), request);
        Ok(incoming)
    }

    async fn approve(
        &mut self,
        session: &ServerSession,
        channel: u16,
        incoming: IncomingAttach,
    ) -> TestResult<Receiver> {
        let handle = incoming.handle;
        let name = incoming.name.clone();
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
        else {
            panic!("peer sender creates a receiving endpoint");
        };
        let mut attached = false;
        let mut credited = false;
        while !attached || !credited {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Attach(response)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(response.handle, handle);
                    assert_eq!(response.name, name);
                    attached = true;
                }
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if actual == channel && flow.handle == Some(handle) => {
                    assert_eq!(flow.link_credit, Some(32));
                    credited = true;
                }
                other => panic!("exact approved link response expected: {other:?}"),
            }
        }
        Ok(receiver)
    }

    async fn healthy(
        &mut self,
        receiver: &mut Receiver,
        channel: u16,
        handle: u32,
        id: u32,
    ) -> TestResult {
        let message = Message::data(id.to_be_bytes().to_vec());
        self.peer
            .send(
                channel,
                Performative::Transfer(transfer(handle, id)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        timeout(IO_TIMEOUT, receiver.accept(&delivery)).await??;
        loop {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(actual, channel);
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
                other => panic!("healthy delivery after stale refusal expected: {other:?}"),
            }
        }
    }

    async fn finish(self) {
        self.connection.shutdown().await;
    }
}

fn attach(handle: u32) -> Attach {
    Attach {
        name: format!("same-link-{handle}"),
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

fn transfer(handle: u32, id: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
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

fn assert_no_approval_frames(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(frame, Frame::Amqp {
            performative: Some(Performative::Flow(flow)),
            ..
        } if flow.handle.is_none())),
        "stale/local refusal must emit no attach, link credit, detach, or End: {frames:?}"
    );
}

#[tokio::test]
async fn exact_receipt_clone_approves_once_but_identical_wire_content_cannot_claim_reused_link()
-> TestResult {
    let mut node = ServerNode::new().await?;
    let mut session = node.session(0).await?;
    let request = attach(0);
    let receipt = node.request(&mut session, 0, &request).await?;
    let receiver = node.approve(&session, 0, receipt.clone()).await?;
    assert!(
        timeout(IO_TIMEOUT, session.accept_attach(receipt.clone(), 262_144))
            .await?
            .is_err()
    );
    assert_no_approval_frames(&node.peer.barrier(0).await?);
    timeout(IO_TIMEOUT, receiver.close()).await??;
    node.peer.detach_reply(0, 0).await?;
    node.peer
        .send(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await?;
    let replacement = node.request(&mut session, 0, &request).await?;
    assert_eq!(receipt.attach(), replacement.attach());
    assert!(matches!(
        timeout(IO_TIMEOUT, session.accept_attach(receipt, 262_144)).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_no_approval_frames(&node.peer.barrier(0).await?);
    let mut replacement = node.approve(&session, 0, replacement).await?;
    node.healthy(&mut replacement, 0, 0, 7).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn changing_handle_name_or_role_refuses_locally_without_consuming_valid_clone() -> TestResult
{
    for changed_field in 0..3 {
        let mut node = ServerNode::new().await?;
        let mut session = node.session(0).await?;
        let request = attach(0);
        let original = node.request(&mut session, 0, &request).await?;
        let mut changed = original.clone();
        match changed_field {
            0 => changed.handle = 1,
            1 => changed.name.push_str("-changed"),
            2 => changed.role = Role::Receiver,
            _ => unreachable!("three immutable identity fields"),
        }
        assert!(matches!(
            timeout(IO_TIMEOUT, session.accept_attach(changed, 262_144)).await?,
            Err(EngineError::InvalidState(_))
        ));
        assert_no_approval_frames(&node.peer.barrier(0).await?);
        let mut receiver = node.approve(&session, 0, original).await?;
        node.healthy(&mut receiver, 0, 0, changed_field).await?;
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn dispatched_pending_detach_allows_immediate_handle_reuse_before_old_approval_is_consumed()
-> TestResult {
    let mut node = ServerNode::new().await?;
    let mut session = node.session(0).await?;
    let request = attach(0);
    let old = node.request(&mut session, 0, &request).await?;
    node.peer
        .send(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await?;
    node.peer.detach_reply(0, 0).await?;
    let fresh = node.request(&mut session, 0, &request).await?;
    assert_eq!(old.attach(), fresh.attach());
    assert!(matches!(
        timeout(IO_TIMEOUT, session.accept_attach(old, 262_144)).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_no_approval_frames(&node.peer.barrier(0).await?);
    let mut receiver = node.approve(&session, 0, fresh).await?;
    node.healthy(&mut receiver, 0, 0, 11).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn old_receipt_and_server_session_cannot_approve_fresh_link_after_channel_reuse() -> TestResult
{
    let mut node = ServerNode::new().await?;
    let mut old_session = node.session(0).await?;
    let request = attach(0);
    let old_receipt = node.request(&mut old_session, 0, &request).await?;
    node.peer
        .send(0, Performative::End(End::default()), Vec::new())
        .await?;
    node.peer.end_reply(0).await?;
    let mut fresh_session = node.session(0).await?;
    let fresh_receipt = node.request(&mut fresh_session, 0, &request).await?;
    assert_eq!(old_receipt.attach(), fresh_receipt.attach());
    assert!(
        timeout(
            IO_TIMEOUT,
            fresh_session.accept_attach(old_receipt, 262_144)
        )
        .await?
        .is_err()
    );
    assert!(
        timeout(
            IO_TIMEOUT,
            old_session.accept_attach(fresh_receipt.clone(), 262_144)
        )
        .await?
        .is_err()
    );
    assert_no_approval_frames(&node.peer.barrier(0).await?);
    let mut receiver = node.approve(&fresh_session, 0, fresh_receipt).await?;
    node.healthy(&mut receiver, 0, 0, 19).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn receipts_from_another_connection_cannot_approve_identical_channel_and_link_content()
-> TestResult {
    let mut original = ServerNode::new().await?;
    let mut other = ServerNode::new().await?;
    let mut original_session = original.session(0).await?;
    let mut other_session = other.session(0).await?;
    let request = attach(0);
    let original_receipt = original.request(&mut original_session, 0, &request).await?;
    let other_receipt = other.request(&mut other_session, 0, &request).await?;
    assert_eq!(original_receipt.attach(), other_receipt.attach());
    assert!(matches!(
        timeout(
            IO_TIMEOUT,
            other_session.accept_attach(original_receipt.clone(), 262_144)
        )
        .await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_no_approval_frames(&other.peer.barrier(0).await?);
    let mut original_receiver = original
        .approve(&original_session, 0, original_receipt)
        .await?;
    let mut other_receiver = other.approve(&other_session, 0, other_receipt).await?;
    original.healthy(&mut original_receiver, 0, 0, 23).await?;
    other.healthy(&mut other_receiver, 0, 0, 24).await?;
    original.finish().await;
    other.finish().await;
    Ok(())
}

#[tokio::test]
async fn stale_incoming_session_cannot_approve_a_reused_pending_channel() -> TestResult {
    let mut node = ServerNode::new().await?;
    let _sibling = node.session(1).await?;
    let old = node.pending_session(0).await?;
    node.peer
        .send(0, Performative::End(End::default()), Vec::new())
        .await?;
    node.peer.begin_reply(0).await?;
    node.peer.end_reply(0).await?;
    let fresh = node.pending_session(0).await?;
    assert!(
        timeout(IO_TIMEOUT, node.connection.accept_session(old))
            .await?
            .is_err()
    );
    assert_no_approval_frames(&node.peer.barrier(1).await?);
    let mut session = timeout(IO_TIMEOUT, node.connection.accept_session(fresh)).await??;
    node.peer.begin_reply(0).await?;
    let receipt = node.request(&mut session, 0, &attach(0)).await?;
    let mut receiver = node.approve(&session, 0, receipt).await?;
    node.healthy(&mut receiver, 0, 0, 29).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn incoming_session_from_another_connection_cannot_consume_the_rightful_pending_event()
-> TestResult {
    let mut original = ServerNode::new().await?;
    let mut other = ServerNode::new().await?;
    let _sibling = other.session(1).await?;
    let original_incoming = original.pending_session(0).await?;
    let other_incoming = other.pending_session(0).await?;
    assert!(matches!(
        timeout(
            IO_TIMEOUT,
            other.connection.accept_session(original_incoming)
        )
        .await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_no_approval_frames(&other.peer.barrier(1).await?);
    let mut session =
        timeout(IO_TIMEOUT, other.connection.accept_session(other_incoming)).await??;
    other.peer.begin_reply(0).await?;
    let receipt = other.request(&mut session, 0, &attach(0)).await?;
    let mut receiver = other.approve(&session, 0, receipt).await?;
    other.healthy(&mut receiver, 0, 0, 31).await?;
    original.finish().await;
    other.finish().await;
    Ok(())
}

struct ClientNode {
    connection: ClientConnection,
    peer: Peer,
}

impl ClientNode {
    async fn new(channel_max: u16) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (mut socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let opening = tokio::spawn(
            ClientConnection::builder()
                .container_id(format!("provenance-client-{}", address.port()))
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
        assert!(matches!(
            timeout(IO_TIMEOUT, read_frame(&mut socket)).await??,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut socket,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Open(Open {
                        channel_max,
                        ..Open::new(format!("raw-provenance-server-{}", address.port()))
                    })),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        Ok(Self {
            connection: timeout(IO_TIMEOUT, opening).await???,
            peer: Peer {
                stream: socket,
                outgoing: HashMap::new(),
            },
        })
    }

    async fn session(&mut self, expected_channel: u16) -> TestResult<ClientSession> {
        let (session, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(self.connection.begin().await?) },
                async {
                    assert!(matches!(self.peer.read().await?, Frame::Amqp {
                        channel,
                        performative: Some(Performative::Begin(begin)),
                        ..
                    } if channel == expected_channel && begin.remote_channel.is_none()));
                    self.peer
                        .send(
                            expected_channel,
                            Performative::Begin(Begin {
                                remote_channel: Some(expected_channel),
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

    async fn finish(self) {
        self.connection.shutdown().await;
    }
}

#[tokio::test]
async fn wrong_channel_same_name_echo_cannot_consume_the_rightful_pending_attach() -> TestResult {
    let mut node = ClientNode::new(1).await?;
    let mut rightful = node.session(0).await?;
    let _other = node.session(1).await?;
    let mut attaching = Box::pin(rightful.attach_sender("same-name", "queue"));
    let request = timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut attaching => panic!("attach must wait for its echo"),
            frame = node.peer.read() => {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(request)),
                    ..
                } = frame? else {
                    panic!("rightful attach request on channel zero");
                };
                Ok::<_, Box<dyn Error>>(request)
            }
        }
    })
    .await??;
    let response = request.response(request.source.clone(), request.target.clone());
    node.peer
        .send(
            1,
            Performative::Attach(Box::new(response.clone())),
            Vec::new(),
        )
        .await?;
    let frames = node.peer.barrier(0).await?;
    for frame in frames {
        assert!(
            !matches!(frame, Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Flow(flow)),
            ..
        } if flow.handle.is_some()),
            "wrong-channel echo must not install or credit the link"
        );
    }
    poll_fn(|cx| {
        assert!(
            attaching.as_mut().poll(cx).is_pending(),
            "the rightful request must remain pending"
        );
        Poll::Ready(())
    })
    .await;
    node.peer
        .send(0, Performative::Attach(Box::new(response)), Vec::new())
        .await?;
    let mut sender = timeout(IO_TIMEOUT, attaching).await??;
    node.peer
        .send(
            0,
            Performative::Flow(Flow {
                delivery_count: Some(0),
                link_credit: Some(32),
                handle: Some(request.handle),
                echo: false,
                ..node.peer.flow(0)
            }),
            Vec::new(),
        )
        .await?;
    let (sent, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(sender.send(Message::data(vec![9])).await?) },
            async {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Transfer(transfer)),
                    ..
                } = node.peer.read().await?
                else {
                    panic!("rightfully approved sender remains usable");
                };
                node.peer
                    .send(
                        0,
                        Performative::Disposition(amqp::Disposition {
                            role: Role::Receiver,
                            first: transfer.delivery_id.expect("first transfer identity"),
                            last: None,
                            settled: true,
                            state: Some(DeliveryState::Accepted(Accepted)),
                            batchable: false,
                        }),
                        Vec::new(),
                    )
                    .await
            }
        )
    })
    .await??;
    assert_eq!(sent, amqp::Outcome::Accepted(Accepted));
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn peer_channel_cap_and_end_acknowledgement_fence_reused_client_session_ownership()
-> TestResult {
    let mut node = ClientNode::new(0).await?;
    let mut old = node.session(0).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, node.connection.begin()).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_no_approval_frames(&node.peer.barrier(0).await?);

    let mut ending = Box::pin(old.end());
    timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut ending => panic!("End must wait for its matching peer acknowledgement"),
            frame = node.peer.read() => {
                assert!(matches!(frame?, Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::End(end)),
                    ..
                } if end.error.is_none()));
                Ok::<_, Box<dyn Error>>(())
            }
        }
    })
    .await??;
    poll_fn(|cx| {
        assert!(
            ending.as_mut().poll(cx).is_pending(),
            "held peer ACK must keep End unresolved"
        );
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        timeout(IO_TIMEOUT, old.end()).await?,
        Err(EngineError::RemoteDetached)
    ));
    node.peer
        .send(0, Performative::End(End::default()), Vec::new())
        .await?;
    timeout(IO_TIMEOUT, ending).await??;

    let mut fresh = node.session(0).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, old.end()).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(
            IO_TIMEOUT,
            old.attach_sender("stale-client-session", "queue")
        )
        .await?,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(
            IO_TIMEOUT,
            old.attach_receiver("stale-client-receiver", "queue")
        )
        .await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_no_approval_frames(&node.peer.barrier(0).await?);
    let (mut sender, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async {
                Ok::<_, Box<dyn Error>>(fresh.attach_sender("fresh-client-session", "queue").await?)
            },
            async {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(request)),
                    ..
                } = node.peer.read().await?
                else {
                    panic!("fresh session remains able to attach a sender");
                };
                assert_eq!(request.name, "fresh-client-session");
                assert_eq!(
                    request.handle, 0,
                    "stale attach must not advance the new allocator"
                );
                let response = request.response(request.source.clone(), request.target.clone());
                node.peer
                    .send(0, Performative::Attach(Box::new(response)), Vec::new())
                    .await?;
                node.peer
                    .send(
                        0,
                        Performative::Flow(Flow {
                            handle: Some(0),
                            delivery_count: Some(0),
                            link_credit: Some(32),
                            echo: false,
                            ..node.peer.flow(0)
                        }),
                        Vec::new(),
                    )
                    .await
            }
        )
    })
    .await??;
    let (sent, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(sender.send(Message::data(vec![4])).await?) },
            async {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Transfer(transfer)),
                    ..
                } = node.peer.read().await?
                else {
                    panic!("fresh sender must survive stale End and Attach commands");
                };
                node.peer
                    .send(
                        0,
                        Performative::Disposition(amqp::Disposition {
                            role: Role::Receiver,
                            first: transfer.delivery_id.expect("first transfer identity"),
                            last: None,
                            settled: true,
                            state: Some(DeliveryState::Accepted(Accepted)),
                            batchable: false,
                        }),
                        Vec::new(),
                    )
                    .await
            }
        )
    })
    .await??;
    assert_eq!(sent, amqp::Outcome::Accepted(Accepted));
    node.finish().await;
    Ok(())
}
