//! Outgoing acknowledgements and endpoint closure through real TCP streams.

use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Begin, ClientConnection, ClientReceiver, ClientSender, ClientSession, Close,
    DeliveryState, Detach, Disposition, EngineError, Flow, Frame, Message, Open, Outcome,
    Performative, ProtocolHeader, ReceiverSettleMode, Role, Transfer, decode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(10);
const WINDOW: u32 = 2_048;

struct Peer {
    stream: TcpStream,
    received: HashMap<u16, u32>,
    sent: HashMap<u16, u32>,
}

impl Peer {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            received: HashMap::new(),
            sent: HashMap::new(),
        }
    }

    async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        if matches!(&performative, Performative::Begin(_)) {
            self.received.insert(channel, 0);
            self.sent.insert(channel, 0);
        }
        if matches!(&performative, Performative::Transfer(_)) {
            let count = self.sent.entry(channel).or_default();
            *count = count.wrapping_add(1);
        }
        timeout(
            DEADLINE,
            write_frame(
                &mut self.stream,
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
        let frame = timeout(DEADLINE, read_frame(&mut self.stream)).await??;
        if let Frame::Amqp {
            channel,
            performative: Some(Performative::Transfer(_)),
            ..
        } = &frame
        {
            let count = self.received.entry(*channel).or_default();
            *count = count.wrapping_add(1);
        }
        Ok(frame)
    }

    fn flow(&self, channel: u16, incoming_window: u32) -> Flow {
        Flow {
            next_incoming_id: Some(self.received.get(&channel).copied().unwrap_or(0)),
            incoming_window,
            next_outgoing_id: self.sent.get(&channel).copied().unwrap_or(0),
            outgoing_window: WINDOW,
            ..Flow::default()
        }
    }

    async fn grant(&mut self, channel: u16, handle: u32, window: u32) -> TestResult {
        let mut flow = self.flow(channel, window);
        flow.handle = Some(handle);
        flow.delivery_count = Some(0);
        flow.link_credit = Some(8);
        self.send(channel, Performative::Flow(flow)).await
    }

    async fn allow_frame(&mut self, channel: u16) -> TestResult {
        self.send(channel, Performative::Flow(self.flow(channel, 1)))
            .await
    }

    // The echo orders preceding peer input and successful local commands. Both
    // counters count Transfer frames, not messages, so fragmentation is valid.
    async fn barrier(&mut self, channel: u16, window: u32) -> TestResult<Vec<Frame>> {
        let incoming = self.sent.get(&channel).copied().unwrap_or(0);
        let outgoing = self.received.get(&channel).copied().unwrap_or(0);
        let mut flow = self.flow(channel, window);
        flow.echo = true;
        self.send(channel, Performative::Flow(flow)).await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: actual, performative: Some(Performative::Flow(value)), ..
            } if *actual == channel && value.handle.is_none()
                && value.next_incoming_id == Some(incoming)
                && value.next_outgoing_id == outgoing);
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_) | Performative::End(_)),
                        ..
                    }
                ),
                "the session must remain usable at the barrier: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn control(&mut self) -> TestResult<(u16, Performative)> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                } if !matches!(&performative, Performative::Flow(_)) => {
                    assert!(payload.is_empty(), "a control frame has no payload");
                    return Ok((channel, performative));
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("a control frame was expected: {other:?}"),
            }
        }
    }

    async fn transfer(&mut self, channel: u16, handle: u32) -> TestResult<(Transfer, Vec<u8>)> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(transfer.handle, handle);
                    return Ok((transfer, payload));
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("an outgoing Transfer was expected: {other:?}"),
            }
        }
    }

    async fn delivery(&mut self, channel: u16, handle: u32) -> TestResult<(Transfer, Vec<u8>)> {
        let (first, mut bytes) = self.transfer(channel, handle).await?;
        assert!(first.delivery_id.is_some());
        assert!(first.delivery_tag.is_some());
        assert_eq!(first.message_format, Some(0));
        let mut more = first.more;
        while more {
            let (continuation, payload) = self.transfer(channel, handle).await?;
            assert_eq!(continuation.delivery_id, None);
            assert_eq!(continuation.delivery_tag, None);
            assert_eq!(continuation.message_format, None);
            bytes.extend(payload);
            more = continuation.more;
        }
        Ok((first, bytes))
    }

    async fn accepted(&mut self, channel: u16, id: u32, settled: bool) -> TestResult {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        )
        .await
    }

    async fn acknowledgement(
        &mut self,
        channel: u16,
        id: u32,
        state: Option<DeliveryState>,
    ) -> TestResult {
        let (actual, performative) = self.control().await?;
        let Performative::Disposition(value) = performative else {
            panic!("Sender acknowledgement was expected: {performative:?}");
        };
        assert_eq!(actual, channel);
        assert_eq!(value.role, Role::Sender);
        assert_eq!(value.first, id);
        assert_eq!(value.last, None);
        assert!(value.settled);
        assert_eq!(value.state, state);
        Ok(())
    }

    async fn detached(&mut self, channel: u16, handle: u32) -> TestResult {
        let (actual, performative) = self.control().await?;
        let Performative::Detach(value) = performative else {
            panic!("closed Detach was expected: {performative:?}");
        };
        assert_eq!(actual, channel);
        assert_eq!(value.handle, handle);
        assert!(value.closed);
        assert_eq!(value.error, None);
        Ok(())
    }
}

fn assert_no_dispositions(frames: &[Frame]) {
    assert!(
        !frames.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Disposition(_)),
                ..
            }
        )),
        "no acknowledgement was permitted: {frames:?}"
    );
}

fn assert_no_detaches(frames: &[Frame]) {
    assert!(
        !frames.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Detach(_)),
                ..
            }
        )),
        "no additional Detach was permitted: {frames:?}"
    );
}

struct ClientNode {
    connection: ClientConnection,
    peer: Peer,
}

impl ClientNode {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let socket = TcpStream::connect(listener.local_addr()?).await?;
        socket.set_nodelay(true)?;
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let opening = tokio::spawn(
            ClientConnection::builder()
                .container_id("outgoing-client")
                .idle_timeout_millis(0)
                .open_with_stream(socket),
        );
        let mut peer = Peer::new(stream);
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut peer.stream)).await??,
            ProtocolHeader::AMQP
        );
        timeout(
            DEADLINE,
            write_protocol_header(&mut peer.stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert!(matches!(peer.control().await?, (0, Performative::Open(_))));
        peer.send(
            0,
            Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: Some(0),
                ..Open::new("outgoing-raw-peer")
            }),
        )
        .await?;
        Ok(Self {
            connection: timeout(DEADLINE, opening).await???,
            peer,
        })
    }

    async fn begin(&mut self, window: u32) -> TestResult<(ClientSession, u16)> {
        let connection = &mut self.connection;
        let peer = &mut self.peer;
        let (session, channel) = timeout(DEADLINE, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
                async {
                    let (channel, performative) = peer.control().await?;
                    assert!(matches!(performative, Performative::Begin(_)));
                    peer.send(
                        channel,
                        Performative::Begin(Begin {
                            remote_channel: Some(channel),
                            incoming_window: window,
                            ..Begin::default()
                        }),
                    )
                    .await?;
                    Ok::<_, Box<dyn Error>>(channel)
                }
            )
        })
        .await??;
        Ok((session, channel))
    }

    async fn sender(
        &mut self,
        session: &mut ClientSession,
        mode: ReceiverSettleMode,
        window: u32,
    ) -> TestResult<(ClientSender, u16, u32)> {
        let peer = &mut self.peer;
        let (sender, (channel, handle)) = timeout(DEADLINE, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn Error>>(session.attach_sender("sender", "queue").await?) },
                async {
                    let (channel, performative) = peer.control().await?;
                    let Performative::Attach(attach) = performative else {
                        panic!("client sender Attach");
                    };
                    assert_eq!(attach.role, Role::Sender);
                    let handle = attach.handle;
                    let mut response =
                        attach.response(attach.source.clone(), attach.target.clone());
                    response.rcv_settle_mode = mode;
                    response.max_message_size = Some(4 * 1024 * 1024);
                    peer.send(channel, Performative::Attach(Box::new(response)))
                        .await?;
                    peer.grant(channel, handle, window).await?;
                    Ok::<_, Box<dyn Error>>((channel, handle))
                }
            )
        })
        .await??;
        Ok((sender, channel, handle))
    }

    async fn receiver(
        &mut self,
        session: &mut ClientSession,
    ) -> TestResult<(ClientReceiver, u16, u32)> {
        let peer = &mut self.peer;
        let (receiver, (channel, handle)) = timeout(DEADLINE, async {
            tokio::try_join!(
                async {
                    Ok::<_, Box<dyn Error>>(session.attach_receiver("receiver", "queue").await?)
                },
                async {
                    let (channel, performative) = peer.control().await?;
                    let Performative::Attach(attach) = performative else {
                        panic!("client receiver Attach");
                    };
                    assert_eq!(attach.role, Role::Receiver);
                    let handle = attach.handle;
                    let mut response = attach.response(attach.source.clone(), attach.target.clone());
                    response.initial_delivery_count = Some(0);
                    peer.send(channel, Performative::Attach(Box::new(response)))
                        .await?;
                    assert!(matches!(peer.read().await?, Frame::Amqp {
                        channel: actual, performative: Some(Performative::Flow(flow)), ..
                    } if actual == channel && flow.handle == Some(handle) && flow.link_credit == Some(32)));
                    Ok::<_, Box<dyn Error>>((channel, handle))
                }
            )
        })
        .await??;
        Ok((receiver, channel, handle))
    }

    async fn finish(self) -> TestResult {
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}

async fn client_send(
    sender: &mut ClientSender,
    peer: &mut Peer,
    channel: u16,
    handle: u32,
    message: Message,
    settled: bool,
) -> TestResult<u32> {
    let expected = message.clone();
    let (outcome, id) = timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(sender.send(message).await?) },
            async {
                let (first, bytes) = peer.delivery(channel, handle).await?;
                assert_eq!(decode_message(&bytes)?, expected);
                assert_ne!(first.settled, Some(true));
                let id = first.delivery_id.expect("outgoing identity");
                peer.accepted(channel, id, settled).await?;
                Ok::<_, Box<dyn Error>>(id)
            }
        )
    })
    .await??;
    assert_eq!(outcome, Outcome::Accepted(Accepted));
    Ok(id)
}

#[tokio::test]
async fn client_second_mode_acknowledges_without_state_before_a_following_close() -> TestResult {
    let mut node = ClientNode::new().await?;
    let (mut session, _) = node.begin(WINDOW).await?;
    let (mut sender, channel, handle) = node
        .sender(&mut session, ReceiverSettleMode::Second, WINDOW)
        .await?;
    let id = client_send(
        &mut sender,
        &mut node.peer,
        channel,
        handle,
        Message::data(b"automatic acknowledgement".to_vec()),
        false,
    )
    .await?;

    let connection = &node.connection;
    let peer = &mut node.peer;
    timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.close().await?) },
            async {
                peer.acknowledgement(channel, id, None).await?;
                let (_, performative) = peer.control().await?;
                assert!(matches!(performative, Performative::Close(_)));
                peer.send(0, Performative::Close(Close::default())).await
            }
        )
    })
    .await??;
    node.finish().await
}

#[tokio::test]
async fn first_mode_and_receiver_settled_outcomes_do_not_emit_sender_acknowledgements() -> TestResult
{
    for (mode, settled) in [
        (ReceiverSettleMode::First, false),
        (ReceiverSettleMode::First, true),
        (ReceiverSettleMode::Second, true),
    ] {
        let mut node = ClientNode::new().await?;
        let (mut session, _) = node.begin(WINDOW).await?;
        let (mut sender, channel, handle) = node.sender(&mut session, mode, WINDOW).await?;
        client_send(
            &mut sender,
            &mut node.peer,
            channel,
            handle,
            Message::data(vec![1, 2, 3]),
            settled,
        )
        .await?;
        assert_no_dispositions(&node.peer.barrier(channel, WINDOW).await?);
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn early_second_mode_outcome_waits_for_the_final_fragment_before_acknowledgement()
-> TestResult {
    let mut node = ClientNode::new().await?;
    let (mut session, _) = node.begin(1).await?;
    let (mut sender, channel, handle) = node
        .sender(&mut session, ReceiverSettleMode::Second, 1)
        .await?;
    let message = Message::data(vec![7; 1_600]);
    let mut sending = Box::pin(sender.send(message.clone()));
    let (first, mut bytes) = timeout(DEADLINE, async {
        tokio::select! {
            result = &mut sending => panic!("send completed before any receiver outcome: {result:?}"),
            frame = node.peer.transfer(channel, handle) => frame,
        }
    })
    .await??;
    assert!(first.more, "the 512-byte frame limit forces fragmentation");
    let id = first.delivery_id.expect("first fragment identity");
    node.peer.accepted(channel, id, false).await?;
    let before = timeout(DEADLINE, async {
        tokio::select! {
            biased;
            result = &mut sending => panic!("early outcome completed a partial send: {result:?}"),
            frames = node.peer.barrier(channel, 0) => frames,
        }
    })
    .await??;
    assert_no_dispositions(&before);

    let mut fragments = 1;
    loop {
        node.peer.allow_frame(channel).await?;
        let (transfer, payload) = node.peer.transfer(channel, handle).await?;
        assert_eq!(transfer.delivery_id, None);
        assert_eq!(transfer.delivery_tag, None);
        assert_eq!(transfer.message_format, None);
        bytes.extend(payload);
        fragments += 1;
        if !transfer.more {
            break;
        }
        let before = timeout(DEADLINE, async {
            tokio::select! {
                biased;
                result = &mut sending => panic!("send completed before its final Transfer: {result:?}"),
                frames = node.peer.barrier(channel, 0) => frames,
            }
        })
        .await??;
        assert_no_dispositions(&before);
    }
    assert!(fragments > 2);
    assert_eq!(decode_message(&bytes)?, message);
    node.peer.acknowledgement(channel, id, None).await?;
    assert_eq!(
        timeout(DEADLINE, sending).await??,
        Outcome::Accepted(Accepted)
    );
    assert_no_dispositions(&node.peer.barrier(channel, WINDOW).await?);
    node.finish().await
}

#[tokio::test]
async fn dropping_a_client_send_future_does_not_drop_its_second_mode_acknowledgement() -> TestResult
{
    let mut node = ClientNode::new().await?;
    let (mut session, _) = node.begin(WINDOW).await?;
    let (mut sender, channel, handle) = node
        .sender(&mut session, ReceiverSettleMode::Second, WINDOW)
        .await?;
    let message = Message::data(b"dropped caller".to_vec());
    let mut sending = Box::pin(sender.send(message.clone()));
    let (first, bytes) = timeout(DEADLINE, async {
        tokio::select! {
            result = &mut sending => panic!("send completed before its receiver outcome: {result:?}"),
            delivery = node.peer.delivery(channel, handle) => delivery,
        }
    })
    .await??;
    assert_eq!(decode_message(&bytes)?, message);
    let id = first.delivery_id.expect("outgoing identity");
    drop(sending);
    node.peer.accepted(channel, id, false).await?;
    node.peer.acknowledgement(channel, id, None).await?;
    assert_no_dispositions(&node.peer.barrier(channel, WINDOW).await?);

    let next = client_send(
        &mut sender,
        &mut node.peer,
        channel,
        handle,
        Message::data(b"healthy next caller".to_vec()),
        false,
    )
    .await?;
    assert_eq!(next, id.wrapping_add(1));
    node.peer.acknowledgement(channel, next, None).await?;
    assert_no_dispositions(&node.peer.barrier(channel, WINDOW).await?);
    node.finish().await
}

#[tokio::test]
async fn a_receiver_outcome_after_client_close_never_emits_an_acknowledgement() -> TestResult {
    let mut node = ClientNode::new().await?;
    let (mut session, _) = node.begin(1).await?;
    let (mut sender, channel, handle) = node
        .sender(&mut session, ReceiverSettleMode::Second, 1)
        .await?;
    let mut sending = Box::pin(sender.send(Message::data(vec![9; 1_600])));
    let (first, _) = timeout(DEADLINE, async {
        tokio::select! {
            result = &mut sending => panic!("send completed before its receiver outcome: {result:?}"),
            frame = node.peer.transfer(channel, handle) => frame,
        }
    })
    .await??;
    assert!(first.more);
    let id = first.delivery_id.expect("partial outgoing identity");
    assert_no_dispositions(&node.peer.barrier(channel, 0).await?);

    let connection = &node.connection;
    let peer = &mut node.peer;
    timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.close().await?) },
            async {
                let (_, performative) = peer.control().await?;
                assert!(matches!(performative, Performative::Close(_)));
                peer.accepted(channel, id, false).await?;
                peer.send(0, Performative::Close(Close::default())).await?;
                let mut after_close = Vec::new();
                timeout(DEADLINE, peer.stream.read_to_end(&mut after_close)).await??;
                assert!(
                    after_close.is_empty(),
                    "no Transfer, acknowledgement, or second Close follows local Close"
                );
                Ok::<_, Box<dyn Error>>(())
            }
        )
    })
    .await??;
    assert!(timeout(DEADLINE, sending).await?.is_err());
    node.finish().await
}

enum ClientEndpoint {
    Sender(ClientSender),
    Receiver(Box<ClientReceiver>),
}

impl ClientEndpoint {
    async fn close(&self) -> Result<(), EngineError> {
        match self {
            Self::Sender(sender) => sender.close().await,
            Self::Receiver(receiver) => receiver.close().await,
        }
    }
}

async fn endpoint(
    node: &mut ClientNode,
    session: &mut ClientSession,
    sender: bool,
) -> TestResult<(ClientEndpoint, u16, u32)> {
    if sender {
        let (endpoint, channel, handle) = node
            .sender(session, ReceiverSettleMode::First, WINDOW)
            .await?;
        Ok((ClientEndpoint::Sender(endpoint), channel, handle))
    } else {
        let (endpoint, channel, handle) = node.receiver(session).await?;
        Ok((
            ClientEndpoint::Receiver(Box::new(endpoint)),
            channel,
            handle,
        ))
    }
}

#[tokio::test]
async fn already_peer_detached_client_endpoints_close_locally_without_another_detach() -> TestResult
{
    for sender in [false, true] {
        let mut node = ClientNode::new().await?;
        let (mut session, _) = node.begin(WINDOW).await?;
        let (endpoint, channel, handle) = endpoint(&mut node, &mut session, sender).await?;
        node.peer
            .send(
                channel,
                Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error: None,
                }),
            )
            .await?;
        node.peer.detached(channel, handle).await?;
        assert_no_detaches(&node.peer.barrier(channel, WINDOW).await?);
        for _ in 0..2 {
            timeout(DEADLINE, endpoint.close()).await??;
            assert_no_detaches(&node.peer.barrier(channel, WINDOW).await?);
        }
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn repeated_client_close_does_not_wait_for_or_duplicate_the_first_detach_acknowledgement()
-> TestResult {
    for sender in [false, true] {
        let mut node = ClientNode::new().await?;
        let (mut session, _) = node.begin(WINDOW).await?;
        let (endpoint, channel, handle) = endpoint(&mut node, &mut session, sender).await?;
        let mut closing = Box::pin(endpoint.close());
        timeout(DEADLINE, async {
            tokio::select! {
                result = &mut closing => panic!("the first close must await its peer ACK: {result:?}"),
                detached = node.peer.detached(channel, handle) => detached,
            }
        })
        .await??;

        timeout(DEADLINE, endpoint.close()).await??;
        let frames = timeout(DEADLINE, async {
            tokio::select! {
                biased;
                result = &mut closing => panic!("first close completed without its peer ACK: {result:?}"),
                frames = node.peer.barrier(channel, WINDOW) => frames,
            }
        })
        .await??;
        assert_no_detaches(&frames);
        node.peer
            .send(
                channel,
                Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error: None,
                }),
            )
            .await?;
        timeout(DEADLINE, closing).await??;
        timeout(DEADLINE, endpoint.close()).await??;
        assert_no_detaches(&node.peer.barrier(channel, WINDOW).await?);
        node.finish().await?;
    }
    Ok(())
}
