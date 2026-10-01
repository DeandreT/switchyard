//! Lazy local Begin publication without leaking pending-session responses.

use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ConnectionOptions, DeliveryState,
    Detach, End, EngineError, Flow, Frame, IncomingSession, LinkEndpoint, Message, Open,
    OrderedMap, Performative, ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode,
    ServerConnection, ServerSession, Source, Symbol, Target, Transfer, encode_message, read_frame,
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
    transfers: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(performative, Performative::Begin(_)) {
            self.transfers.insert(channel, 0);
        } else if matches!(performative, Performative::Transfer(_)) {
            let count = self.transfers.entry(channel).or_default();
            *count = count.wrapping_add(1);
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
            next_outgoing_id: self.transfers.get(&channel).copied().unwrap_or(0),
            outgoing_window: 2_048,
            echo: true,
            ..Flow::default()
        }
    }

    // The accepted sibling session's echo orders all preceding socket input.
    async fn barrier(&mut self, channel: u16) -> TestResult<Vec<Frame>> {
        let expected = self.transfers.get(&channel).copied().unwrap_or(0);
        self.send(channel, Performative::Flow(self.flow(channel)), Vec::new())
            .await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: actual, performative: Some(Performative::Flow(flow)), ..
            } if *actual == channel && flow.handle.is_none() && flow.next_incoming_id == Some(expected));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_)),
                        ..
                    }
                ),
                "session error must not close the connection: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn begin_reply(&mut self, channel: u16) -> TestResult {
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: actual, performative: Some(Performative::Begin(begin)), ..
        } if actual == channel && begin.remote_channel == Some(channel)));
        Ok(())
    }

    async fn accepted(&mut self, channel: u16, id: u32) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(value)),
                    ..
                } => {
                    assert_eq!(actual, channel);
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
                other => panic!("healthy delivery outcome expected: {other:?}"),
            }
        }
    }

    async fn end_reply(&mut self, channel: u16, condition: Option<&str>) -> TestResult {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::End(end)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(
                        end.error.as_ref().map(|error| error.condition.as_symbol()),
                        condition.map(Symbol::from)
                    );
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("End without a duplicate Begin expected: {other:?}"),
            }
        }
    }
}

struct Node {
    connection: ServerConnection,
    peer: Peer,
}

impl Node {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut stream = TcpStream::connect(listener.local_addr()?).await?;
        stream.set_nodelay(true)?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            "startup-server",
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
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
                    performative: Some(Performative::Open(Open::new("startup-peer"))),
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
                transfers: HashMap::new(),
            },
        })
    }

    async fn pending(&mut self, channel: u16) -> TestResult<IncomingSession> {
        self.peer
            .send(channel, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        Ok(timeout(IO_TIMEOUT, self.connection.next_incoming_session())
            .await?
            .expect("incoming session"))
    }

    async fn begin(&mut self, channel: u16) -> TestResult<ServerSession> {
        let incoming = self.pending(channel).await?;
        let session = timeout(IO_TIMEOUT, self.connection.accept_session(incoming)).await??;
        self.peer.begin_reply(channel).await?;
        Ok(session)
    }

    async fn receiver(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
    ) -> TestResult<Receiver> {
        let request = attach(channel, handle);
        self.peer
            .send(
                channel,
                Performative::Attach(Box::new(request.clone())),
                Vec::new(),
            )
            .await?;
        let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("only live attach offered");
        assert_eq!(incoming.name, request.name);
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(incoming, 4 * 1024 * 1024)).await??
        else {
            panic!("peer sender creates a receiver");
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
                    attached = true;
                }
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    if actual == channel && flow.handle == Some(handle) {
                        assert_eq!(flow.link_credit, Some(32));
                        credited = true;
                    }
                }
                other => panic!("supported attach response expected: {other:?}"),
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
                Performative::Transfer(first(handle, id)),
                encode_message(&message)?,
            )
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        receiver.accept(&delivery).await?;
        self.peer.accepted(channel, id).await?;
        Ok(())
    }

    async fn finish(self) {
        self.connection.shutdown().await;
    }
}

fn attach(channel: u16, handle: u32) -> Attach {
    Attach {
        name: format!("live-{channel}-{handle}"),
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

fn first(handle: u32, id: u32) -> Transfer {
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

fn channel_frames(frames: &[Frame], channel: u16) -> Vec<&Performative> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Amqp {
                channel: actual,
                performative: Some(value),
                ..
            } if *actual == channel => Some(value),
            _ => None,
        })
        .collect()
}

fn assert_begin_then_end(frames: &[Frame], channel: u16, condition: Option<&str>) {
    let frames = channel_frames(frames, channel);
    let [Performative::Begin(begin), Performative::End(end)] = frames.as_slice() else {
        panic!("exactly Begin then End expected: {frames:?}");
    };
    assert_eq!(begin.remote_channel, Some(channel));
    assert_eq!(
        end.error.as_ref().map(|error| error.condition.as_symbol()),
        condition.map(Symbol::from)
    );
}

fn assert_no_channel(frames: &[Frame], channel: u16) {
    assert!(
        channel_frames(frames, channel).is_empty(),
        "no additional session frame expected: {frames:?}"
    );
}

#[tokio::test]
async fn pending_session_echo_publishes_begin_once_and_public_acceptance_remains_available()
-> TestResult {
    let mut node = Node::new().await?;
    let mut sibling_session = node.begin(0).await?;
    let mut sibling = node.receiver(&mut sibling_session, 0, 0).await?;
    let incoming = node.pending(1).await?;
    node.peer
        .send(1, Performative::Flow(node.peer.flow(1)), Vec::new())
        .await?;
    let frames = node.peer.barrier(0).await?;
    let responses = channel_frames(&frames, 1);
    let [Performative::Begin(begin), Performative::Flow(flow)] = responses.as_slice() else {
        panic!("pending echo must begin its outgoing session first: {responses:?}");
    };
    assert_eq!(begin.remote_channel, Some(1));
    assert!(flow.handle.is_none());
    assert_eq!(flow.next_incoming_id, Some(0));
    let mut session = timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await??;
    assert_no_channel(&node.peer.barrier(0).await?, 1);
    let mut receiver = node.receiver(&mut session, 1, 0).await?;
    node.healthy(&mut receiver, 1, 0, 5).await?;
    node.healthy(&mut sibling, 0, 0, 6).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn pending_attach_detach_ack_starts_the_session_and_removes_only_undispatched_approval()
-> TestResult {
    let mut node = Node::new().await?;
    let mut sibling_session = node.begin(0).await?;
    let mut sibling = node.receiver(&mut sibling_session, 0, 0).await?;
    let incoming = node.pending(1).await?;
    let mut pending = attach(1, 0);
    pending.name = String::from("undispatched-canceled");
    node.peer
        .send(1, Performative::Attach(Box::new(pending)), Vec::new())
        .await?;
    node.peer
        .send(
            1,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await?;
    let frames = node.peer.barrier(0).await?;
    let responses = channel_frames(&frames, 1);
    let [
        Performative::Begin(begin),
        Performative::Attach(attach),
        Performative::Detach(detach),
    ] = responses.as_slice()
    else {
        panic!(
            "pending detach must publish its session and link before cancellation: {responses:?}"
        );
    };
    assert_eq!(begin.remote_channel, Some(1));
    assert_eq!(attach.name, "undispatched-canceled");
    assert_eq!(attach.handle, 0);
    assert_eq!(attach.role, Role::Receiver);
    assert!(attach.source.is_none());
    assert!(attach.target.is_none());
    assert!(attach.initial_delivery_count.is_none());
    assert_eq!(detach.handle, 0);
    assert!(detach.closed);
    assert!(detach.error.is_none());
    let mut session = timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await??;
    assert_no_channel(&node.peer.barrier(0).await?, 1);
    let mut replacement = node.receiver(&mut session, 1, 0).await?;
    node.healthy(&mut replacement, 1, 0, 7).await?;
    node.healthy(&mut sibling, 0, 0, 8).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn remote_end_before_approval_begins_first_and_stale_acceptance_does_not_emit_frames()
-> TestResult {
    let mut node = Node::new().await?;
    let mut sibling_session = node.begin(0).await?;
    let mut sibling = node.receiver(&mut sibling_session, 0, 0).await?;
    let incoming = node.pending(1).await?;
    node.peer
        .send(1, Performative::End(End::default()), Vec::new())
        .await?;
    assert_begin_then_end(&node.peer.barrier(0).await?, 1, None);
    assert!(matches!(
        timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_no_channel(&node.peer.barrier(0).await?, 1);
    let mut replacement_session = node.begin(1).await?;
    let mut replacement = node.receiver(&mut replacement_session, 1, 0).await?;
    node.healthy(&mut replacement, 1, 0, 9).await?;
    node.healthy(&mut sibling, 0, 0, 10).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn pending_unknown_handle_flow_or_transfer_refuses_after_begin_without_harming_siblings()
-> TestResult {
    for transfer in [false, true] {
        let mut node = Node::new().await?;
        let mut sibling_session = node.begin(0).await?;
        let mut sibling = node.receiver(&mut sibling_session, 0, 0).await?;
        let incoming = node.pending(1).await?;
        if transfer {
            node.peer
                .send(
                    1,
                    Performative::Transfer(first(99, 17)),
                    encode_message(&Message::data(vec![1]))?,
                )
                .await?;
        } else {
            let mut flow = node.peer.flow(1);
            flow.handle = Some(99);
            flow.delivery_count = Some(0);
            node.peer
                .send(1, Performative::Flow(flow), Vec::new())
                .await?;
        }
        assert_begin_then_end(
            &node.peer.barrier(0).await?,
            1,
            Some("amqp:session:unattached-handle"),
        );
        assert!(matches!(
            timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await?,
            Err(EngineError::RemoteDetached)
        ));
        node.peer
            .send(1, Performative::Flow(node.peer.flow(1)), Vec::new())
            .await?;
        node.peer
            .send(1, Performative::Transfer(first(99, 18)), vec![255])
            .await?;
        assert_no_channel(&node.peer.barrier(0).await?, 1);
        node.peer
            .send(1, Performative::End(End::default()), Vec::new())
            .await?;
        assert_no_channel(&node.peer.barrier(0).await?, 1);
        let mut replacement_session = node.begin(1).await?;
        let mut replacement = node.receiver(&mut replacement_session, 1, 0).await?;
        node.healthy(&mut replacement, 1, 0, 11).await?;
        node.healthy(&mut sibling, 0, 0, 12).await?;
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn thirty_three_mixed_pending_attaches_hit_one_shared_bound_after_begin_then_allow_reuse()
-> TestResult {
    let mut node = Node::new().await?;
    let mut sibling_session = node.begin(0).await?;
    let mut sibling = node.receiver(&mut sibling_session, 0, 0).await?;
    let incoming = node.pending(1).await?;
    for handle in 0..33 {
        let mut request = attach(1, handle);
        if handle % 2 == 1 {
            request.unsettled = Some(OrderedMap::new());
            request.incomplete_unsettled = true;
        }
        node.peer
            .send(1, Performative::Attach(Box::new(request)), Vec::new())
            .await?;
    }
    assert_begin_then_end(
        &node.peer.barrier(0).await?,
        1,
        Some("amqp:resource-limit-exceeded"),
    );
    assert!(matches!(
        timeout(IO_TIMEOUT, node.connection.accept_session(incoming)).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert_no_channel(&node.peer.barrier(0).await?, 1);
    node.peer
        .send(1, Performative::End(End::default()), Vec::new())
        .await?;
    assert_no_channel(&node.peer.barrier(0).await?, 1);
    let mut replacement_session = node.begin(1).await?;
    let mut replacement = node.receiver(&mut replacement_session, 1, 0).await?;
    node.healthy(&mut replacement, 1, 0, 13).await?;
    node.healthy(&mut sibling, 0, 0, 14).await?;
    node.finish().await;
    Ok(())
}

async fn client_peer() -> TestResult<(ClientConnection, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (mut socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let opening = tokio::spawn(
        ClientConnection::builder()
            .container_id("startup-client")
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
                performative: Some(Performative::Open(Open::new("raw-startup-server"))),
                payload: Vec::new(),
            },
        ),
    )
    .await??;
    Ok((
        timeout(IO_TIMEOUT, opening).await???,
        Peer {
            stream: socket,
            transfers: HashMap::new(),
        },
    ))
}

#[tokio::test]
async fn client_mapped_session_errors_never_repeat_its_own_begin() -> TestResult {
    for refused in [false, true] {
        let (mut connection, mut peer) = client_peer().await?;
        let (mut session, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
                async {
                    assert!(matches!(peer.read().await?, Frame::Amqp { channel: 0, performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel.is_none()));
                    peer.send(0, Performative::Begin(Begin { remote_channel: Some(0), ..Begin::default() }), Vec::new()).await
                }
            )
        }).await??;
        let (mut receiver, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(ClientReceiver::builder().name("healthy-client-link").source(Source::new("queue")).attach(&mut session).await?) },
                async {
                    let Frame::Amqp { channel: 0, performative: Some(Performative::Attach(request)), .. } = peer.read().await? else { panic!("client receiver Attach"); };
                    assert_eq!(request.handle, 0);
                    let response = request.response(request.source.clone(), request.target.clone());
                    peer.send(0, Performative::Attach(Box::new(response)), Vec::new()).await?;
                    assert!(matches!(peer.read().await?, Frame::Amqp { channel: 0, performative: Some(Performative::Flow(flow)), .. } if flow.handle == Some(0) && flow.link_credit == Some(32)));
                    Ok::<_, Box<dyn Error>>(())
                }
            )
        }).await??;
        let (result, peer_result) = timeout(IO_TIMEOUT, async {
            tokio::join!(connection.begin(), async {
                assert!(matches!(peer.read().await?, Frame::Amqp { channel: 1, performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel.is_none()));
                peer.send(1, Performative::Begin(Begin { remote_channel: Some(1), ..Begin::default() }), Vec::new()).await?;
                if refused {
                    let mut flow = peer.flow(1);
                    flow.handle = Some(99);
                    flow.delivery_count = Some(0);
                    peer.send(1, Performative::Flow(flow), Vec::new()).await?;
                } else {
                    peer.send(1, Performative::End(End::default()), Vec::new()).await?;
                }
                peer.end_reply(1, refused.then_some("amqp:session:unattached-handle")).await
            })
        }).await?;
        peer_result?;
        let ended = result?;
        assert!(matches!(
            ended.end().await,
            Err(EngineError::RemoteDetached)
        ));
        if refused {
            peer.send(1, Performative::End(End::default()), Vec::new())
                .await?;
        }
        let message = Message::data(b"healthy client sibling".to_vec());
        peer.send(
            0,
            Performative::Transfer(first(0, 41)),
            encode_message(&message)?,
        )
        .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        assert_eq!(delivery.message(), &message);
        receiver.accept(&delivery).await?;
        peer.accepted(0, 41).await?;
        let mut invalid = peer.flow(0);
        invalid.handle = Some(99);
        invalid.delivery_count = Some(0);
        peer.send(0, Performative::Flow(invalid), Vec::new())
            .await?;
        peer.end_reply(0, Some("amqp:session:unattached-handle"))
            .await?;
        peer.send(0, Performative::End(End::default()), Vec::new())
            .await?;
        connection.shutdown().await;
    }
    Ok(())
}
