//! Receiver settlement and selected Source outcomes through real TCP streams.

use std::{error::Error as StdError, future::Future, time::Duration};

use amqp::{
    Accepted, AmqpError, Attach, Begin, ClientConnection, ClientSender, ClientSession,
    ConnectionOptions, DeliveryState, Disposition, EngineError, Error, Flow, Frame, IncomingAttach,
    LinkEndpoint, Message, Open, Outcome, PendingSettlement, Performative, ProtocolHeader,
    ReceiverSettleMode, Rejected, Released, Role, Sender, SenderSettleMode, ServerConnection,
    ServerSession, Source, Target, decode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;
const DEADLINE: Duration = Duration::from_secs(10);
const WINDOW: u32 = 2_048;
const CHANNEL: u16 = 0;

struct Peer {
    stream: TcpStream,
    received_transfers: u32,
}

impl Peer {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            received_transfers: 0,
        }
    }

    async fn send(&mut self, performative: Performative) -> TestResult {
        timeout(
            DEADLINE,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel: CHANNEL,
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
        if matches!(
            &frame,
            Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Transfer(_)),
                ..
            }
        ) {
            self.received_transfers = self.received_transfers.wrapping_add(1);
        }
        Ok(frame)
    }

    async fn control(&mut self) -> TestResult<Performative> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: CHANNEL,
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                Frame::Amqp {
                    channel: CHANNEL,
                    performative: Some(performative),
                    payload,
                } => {
                    assert!(payload.is_empty());
                    return Ok(performative);
                }
                other => panic!("a control frame was expected: {other:?}"),
            }
        }
    }

    fn flow(&self, incoming_window: u32) -> Flow {
        Flow {
            next_incoming_id: Some(self.received_transfers),
            incoming_window,
            next_outgoing_id: 0,
            outgoing_window: WINDOW,
            ..Flow::default()
        }
    }

    async fn grant(&mut self, handle: u32, incoming_window: u32) -> TestResult {
        let mut flow = self.flow(incoming_window);
        flow.handle = Some(handle);
        flow.delivery_count = Some(0);
        flow.link_credit = Some(8);
        self.send(Performative::Flow(flow)).await
    }

    // Neither raw peer sends Transfer frames. The counters still count every
    // received fragment so these fences do not grant accidental extra credit.
    async fn barrier(&mut self, window: u32) -> TestResult<Vec<Frame>> {
        let outgoing = self.received_transfers;
        let mut flow = self.flow(window);
        flow.echo = true;
        self.send(Performative::Flow(flow)).await?;
        let mut frames = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: CHANNEL, performative: Some(Performative::Flow(value)), ..
            } if value.handle.is_none() && value.next_incoming_id == Some(0)
                && value.next_outgoing_id == outgoing);
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_) | Performative::End(_)),
                        ..
                    }
                ),
                "the link must not destroy its session: {frame:?}"
            );
            frames.push(frame);
            if done {
                return Ok(frames);
            }
        }
    }

    async fn pending_barrier<F: Future + Unpin>(
        &mut self,
        sending: &mut F,
        window: u32,
    ) -> TestResult {
        let frames = timeout(DEADLINE, async {
            tokio::select! {
                biased;
                _ = sending => panic!("send completed before the required settlement or final Transfer"),
                frames = self.barrier(window) => frames,
            }
        })
        .await??;
        assert_flow_only(&frames);
        Ok(())
    }

    async fn transfer(&mut self, handle: u32) -> TestResult<(amqp::Transfer, Vec<u8>)> {
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: CHANNEL,
                    performative: Some(Performative::Transfer(value)),
                    payload,
                } => {
                    assert_eq!(value.handle, handle);
                    return Ok((value, payload));
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("an outgoing Transfer was expected: {other:?}"),
            }
        }
    }

    async fn delivery(&mut self, handle: u32, message: &Message) -> TestResult<u32> {
        let (first, mut bytes) = self.transfer(handle).await?;
        let id = first.delivery_id.expect("first fragment identity");
        assert!(first.delivery_tag.is_some());
        assert_eq!(first.message_format, Some(0));
        assert_ne!(first.settled, Some(true));
        let mut more = first.more;
        while more {
            let (fragment, payload) = self.transfer(handle).await?;
            assert_eq!(fragment.delivery_id, None);
            assert_eq!(fragment.delivery_tag, None);
            assert_eq!(fragment.message_format, None);
            bytes.extend(payload);
            more = fragment.more;
        }
        assert_eq!(&decode_message(&bytes)?, message);
        Ok(id)
    }

    async fn disposition(
        &mut self,
        id: u32,
        state: Option<DeliveryState>,
        settled: bool,
    ) -> TestResult {
        self.send(Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: id,
            last: None,
            settled,
            state,
            batchable: false,
        }))
        .await
    }

    async fn acknowledgement(&mut self, id: u32, state: Option<DeliveryState>) -> TestResult {
        let performative = self.control().await?;
        let Performative::Disposition(value) = performative else {
            panic!("one Sender acknowledgement was expected: {performative:?}");
        };
        assert_eq!(value.role, Role::Sender);
        assert_eq!(value.first, id);
        assert_eq!(value.last, None);
        assert!(value.settled);
        assert_eq!(value.state, state);
        Ok(())
    }
}

fn assert_flow_only(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(frame, Frame::Amqp {
            channel: CHANNEL, performative: Some(Performative::Flow(flow)), payload,
        } if flow.handle.is_none() && payload.is_empty())),
        "no acknowledgement, attach refusal, or link credit was permitted: {frames:?}"
    );
}

fn received() -> DeliveryState {
    DeliveryState::Received {
        section_number: 0,
        section_offset: 0,
    }
}

fn accepted() -> DeliveryState {
    DeliveryState::Accepted(Accepted)
}

fn released_source() -> Source {
    let mut source = Source::new("queue");
    source.default_outcome = Some(DeliveryState::Released(Released));
    source
}

fn rejected_source() -> Source {
    let mut source = Source::new("queue");
    source.default_outcome = Some(DeliveryState::Rejected(Rejected {
        error: Some(Error::new(
            AmqpError::InternalError,
            "receiver default rejection",
            None,
        )),
    }));
    source
}

struct ClientNode {
    connection: ClientConnection,
    peer: Peer,
}

impl ClientNode {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let socket = timeout(DEADLINE, TcpStream::connect(address)).await??;
        socket.set_nodelay(true)?;
        let (stream, _) = timeout(DEADLINE, listener.accept()).await??;
        stream.set_nodelay(true)?;
        let opening = tokio::spawn(
            ClientConnection::builder()
                .container_id(format!("remote-settlement-client-{}", address.port()))
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
        assert!(matches!(peer.control().await?, Performative::Open(_)));
        peer.send(Performative::Open(Open {
            max_frame_size: 512,
            idle_time_out: Some(0),
            ..Open::new(format!("remote-settlement-server-peer-{}", address.port()))
        }))
        .await?;
        Ok(Self {
            connection: timeout(DEADLINE, opening).await???,
            peer,
        })
    }

    async fn begin(&mut self, window: u32) -> TestResult<ClientSession> {
        let connection = &mut self.connection;
        let peer = &mut self.peer;
        let (session, ()) = timeout(DEADLINE, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn StdError>>(connection.begin().await?) },
                async {
                    assert!(matches!(peer.control().await?, Performative::Begin(_)));
                    peer.send(Performative::Begin(Begin {
                        remote_channel: Some(CHANNEL),
                        incoming_window: window,
                        ..Begin::default()
                    }))
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
        source: Option<Source>,
        window: u32,
    ) -> TestResult<(ClientSender, u32)> {
        let peer = &mut self.peer;
        let (sender, handle) = timeout(DEADLINE, async {
            tokio::try_join!(
                async {
                    Ok::<_, Box<dyn StdError>>(session.attach_sender("sender", "queue").await?)
                },
                async {
                    let performative = peer.control().await?;
                    let Performative::Attach(attach) = performative else {
                        panic!("client sender Attach was expected: {performative:?}");
                    };
                    assert_eq!(attach.role, Role::Sender);
                    assert_eq!(attach.source, None);
                    let handle = attach.handle;
                    let mut response = attach.response(source, attach.target.clone());
                    response.rcv_settle_mode = ReceiverSettleMode::Second;
                    response.max_message_size = Some(4 * 1024 * 1024);
                    peer.send(Performative::Attach(Box::new(response))).await?;
                    peer.grant(handle, window).await?;
                    Ok::<_, Box<dyn StdError>>(handle)
                }
            )
        })
        .await??;
        Ok((sender, handle))
    }

    async fn healthy(&mut self, sender: &mut ClientSender, handle: u32) -> TestResult {
        let message = Message::data(b"healthy next send".to_vec());
        let peer = &mut self.peer;
        let (outcome, ()) = timeout(DEADLINE, async {
            tokio::try_join!(
                async { Ok::<_, Box<dyn StdError>>(sender.send(message.clone()).await?) },
                async {
                    let id = peer.delivery(handle, &message).await?;
                    peer.disposition(id, Some(accepted()), true).await
                }
            )
        })
        .await??;
        assert_eq!(outcome, Outcome::Accepted(Accepted));
        assert_flow_only(&self.peer.barrier(WINDOW).await?);
        Ok(())
    }

    async fn finish(self) -> TestResult {
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}

#[tokio::test]
async fn early_terminal_then_stateless_settlement_waits_for_final_transfer_without_an_ack()
-> TestResult {
    for final_state in [None, Some(received())] {
        let mut node = ClientNode::new().await?;
        let mut session = node.begin(1).await?;
        let (mut sender, handle) = node
            .sender(&mut session, Some(released_source()), 1)
            .await?;
        let message = Message::data(vec![7; 1_600]);
        let mut sending = Box::pin(sender.send(message.clone()));
        let (first, mut bytes) = timeout(DEADLINE, async {
            tokio::select! {
                _ = &mut sending => panic!("send completed before the receiver saw a fragment"),
                frame = node.peer.transfer(handle) => frame,
            }
        })
        .await??;
        assert!(first.more, "the 512-byte peer limit forces fragmentation");
        let id = first.delivery_id.expect("first fragment identity");
        node.peer.disposition(id, Some(accepted()), false).await?;
        node.peer.pending_barrier(&mut sending, 0).await?;
        node.peer.disposition(id, final_state, true).await?;
        node.peer.pending_barrier(&mut sending, 0).await?;

        let mut fragments = 1;
        loop {
            node.peer
                .send(Performative::Flow(node.peer.flow(1)))
                .await?;
            let (fragment, payload) = node.peer.transfer(handle).await?;
            assert_eq!(fragment.delivery_id, None);
            assert_eq!(fragment.delivery_tag, None);
            assert_eq!(fragment.message_format, None);
            bytes.extend(payload);
            fragments += 1;
            if !fragment.more {
                break;
            }
            node.peer.pending_barrier(&mut sending, 0).await?;
        }
        assert!(fragments > 2);
        assert_eq!(decode_message(&bytes)?, message);
        assert_eq!(
            timeout(DEADLINE, sending).await??,
            Outcome::Accepted(Accepted)
        );
        assert_flow_only(&node.peer.barrier(WINDOW).await?);
        node.healthy(&mut sender, handle).await?;
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stateless_client_settlement_uses_echoed_source_default_or_reports_no_outcome() -> TestResult
{
    let released = released_source();
    let rejected = rejected_source();
    for (source, expected) in [
        (Some(released.clone()), Some(Outcome::Released(Released))),
        (
            Some(rejected.clone()),
            Some(Outcome::try_from(rejected.default_outcome.unwrap()).unwrap()),
        ),
        (None, None),
        (Some(Source::new("queue")), None),
    ] {
        for state in [None, Some(received())] {
            let mut node = ClientNode::new().await?;
            let mut session = node.begin(WINDOW).await?;
            let (mut sender, handle) = node.sender(&mut session, source.clone(), WINDOW).await?;
            let message = Message::data(b"state-less receiver settlement".to_vec());
            let peer = &mut node.peer;
            let (result, peer_result) = timeout(DEADLINE, async {
                tokio::join!(sender.send(message.clone()), async {
                    let id = peer.delivery(handle, &message).await?;
                    peer.disposition(id, state, true).await
                })
            })
            .await?;
            peer_result?;
            match &expected {
                Some(outcome) => assert_eq!(&result?, outcome),
                None => assert!(matches!(
                    result,
                    Err(EngineError::RemoteSettledWithoutOutcome)
                )),
            }
            assert_flow_only(&node.peer.barrier(WINDOW).await?);
            node.healthy(&mut sender, handle).await?;
            node.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn nonterminal_unsettled_states_do_not_complete_or_apply_the_source_default() -> TestResult {
    let mut node = ClientNode::new().await?;
    let mut session = node.begin(WINDOW).await?;
    let (mut sender, handle) = node
        .sender(&mut session, Some(released_source()), WINDOW)
        .await?;
    let message = Message::data(b"await a terminal outcome".to_vec());
    let mut sending = Box::pin(sender.send(message.clone()));
    let id = timeout(DEADLINE, async {
        tokio::select! {
            _ = &mut sending => panic!("send completed before a receiver disposition"),
            id = node.peer.delivery(handle, &message) => id,
        }
    })
    .await??;
    for state in [None, Some(received())] {
        node.peer.disposition(id, state, false).await?;
        node.peer.pending_barrier(&mut sending, WINDOW).await?;
    }
    node.peer.disposition(id, Some(accepted()), false).await?;
    node.peer.acknowledgement(id, None).await?;
    assert_eq!(
        timeout(DEADLINE, sending).await??,
        Outcome::Accepted(Accepted)
    );
    assert_flow_only(&node.peer.barrier(WINDOW).await?);
    node.healthy(&mut sender, handle).await?;
    node.finish().await
}

struct ServerNode {
    connection: ServerConnection,
    peer: Peer,
}

impl ServerNode {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        let (socket, _) = timeout(DEADLINE, listener.accept()).await??;
        socket.set_nodelay(true)?;
        let accepting = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            format!("remote-settlement-server-{}", address.port()),
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
        ));
        let mut peer = Peer::new(stream);
        timeout(
            DEADLINE,
            write_protocol_header(&mut peer.stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut peer.stream)).await??,
            ProtocolHeader::AMQP
        );
        peer.send(Performative::Open(Open {
            max_frame_size: 512,
            idle_time_out: Some(0),
            ..Open::new(format!("remote-settlement-client-peer-{}", address.port()))
        }))
        .await?;
        assert!(matches!(peer.control().await?, Performative::Open(_)));
        Ok(Self {
            connection: timeout(DEADLINE, accepting).await???,
            peer,
        })
    }

    async fn begin(&mut self) -> TestResult<ServerSession> {
        self.peer
            .send(Performative::Begin(Begin::default()))
            .await?;
        let incoming = timeout(DEADLINE, self.connection.next_incoming_session())
            .await?
            .expect("incoming Begin");
        let session = timeout(DEADLINE, self.connection.accept_session(incoming)).await??;
        let performative = self.peer.control().await?;
        let Performative::Begin(begin) = performative else {
            panic!("server Begin was expected: {performative:?}");
        };
        assert_eq!(begin.remote_channel, Some(CHANNEL));
        Ok(session)
    }

    async fn request(
        &mut self,
        session: &mut ServerSession,
        source: Source,
    ) -> TestResult<IncomingAttach> {
        let attach = Attach {
            name: String::from("server-sender"),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(source),
            target: Some(Target::new("queue")),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(4 * 1024 * 1024),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.peer
            .send(Performative::Attach(Box::new(attach.clone())))
            .await?;
        let incoming = timeout(DEADLINE, session.next_incoming_attach())
            .await?
            .expect("incoming receiver Attach");
        assert_eq!(incoming.attach(), &attach);
        Ok(incoming)
    }

    async fn approve(
        &mut self,
        session: &ServerSession,
        incoming: IncomingAttach,
    ) -> TestResult<Sender> {
        let expected_source = incoming.source.clone();
        let endpoint = timeout(DEADLINE, session.accept_attach(incoming, 256 * 1024)).await??;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("local Sender was expected");
        };
        let performative = self.peer.control().await?;
        let Performative::Attach(attach) = performative else {
            panic!("server sender Attach was expected: {performative:?}");
        };
        assert_eq!(attach.role, Role::Sender);
        assert_eq!(attach.handle, 0);
        assert_eq!(attach.source, expected_source);
        assert_eq!(attach.initial_delivery_count, Some(0));
        self.peer.grant(0, WINDOW).await?;
        assert_flow_only(&self.peer.barrier(WINDOW).await?);
        Ok(sender)
    }

    async fn send(
        &mut self,
        sender: &mut Sender,
        tag: u8,
        state: Option<DeliveryState>,
        settled: bool,
    ) -> TestResult<(PendingSettlement, u32)> {
        let message = Message::data(vec![tag]);
        let peer = &mut self.peer;
        let (result, id) = timeout(DEADLINE, async {
            tokio::join!(
                sender.send_with_settlement(message.clone(), vec![tag].into()),
                async {
                    let id = peer.delivery(0, &message).await?;
                    peer.disposition(id, state, settled).await?;
                    Ok::<_, Box<dyn StdError>>(id)
                }
            )
        })
        .await?;
        Ok((result?, id?))
    }

    async fn healthy(&mut self, sender: &mut Sender) -> TestResult {
        let (receipt, id) = self.send(sender, 2, Some(accepted()), false).await?;
        assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
        timeout(DEADLINE, receipt.accept()).await??;
        self.peer.acknowledgement(id, Some(accepted())).await?;
        assert_flow_only(&self.peer.barrier(WINDOW).await?);
        Ok(())
    }

    async fn finish(self) -> TestResult {
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}

fn oversized_rejection() -> Error {
    Error::new(AmqpError::InternalError, "x".repeat(1024), None)
}

#[tokio::test]
async fn receiver_settled_held_server_receipts_repeat_locally_even_with_oversized_rejections()
-> TestResult {
    for state in [None, Some(received())] {
        let mut node = ServerNode::new().await?;
        let mut session = node.begin().await?;
        let incoming = node.request(&mut session, released_source()).await?;
        let mut sender = node.approve(&session, incoming).await?;
        let (receipt, id) = node.send(&mut sender, 1, Some(accepted()), false).await?;
        assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
        assert_flow_only(&node.peer.barrier(WINDOW).await?);

        node.peer.disposition(id, state, true).await?;
        assert_flow_only(&node.peer.barrier(WINDOW).await?);
        for _ in 0..2 {
            timeout(DEADLINE, receipt.accept()).await??;
            timeout(DEADLINE, receipt.reject(oversized_rejection())).await??;
            assert_flow_only(&node.peer.barrier(WINDOW).await?);
        }

        let (next, next_id) = node.send(&mut sender, 2, Some(accepted()), false).await?;
        assert_eq!(next_id, id.wrapping_add(1));
        timeout(DEADLINE, receipt.reject(oversized_rejection())).await??;
        assert_flow_only(&node.peer.barrier(WINDOW).await?);
        timeout(DEADLINE, next.accept()).await??;
        node.peer.acknowledgement(next_id, Some(accepted())).await?;
        assert_flow_only(&node.peer.barrier(WINDOW).await?);
        node.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn server_stateless_settlement_follows_the_approved_not_requested_source_default()
-> TestResult {
    for (requested, approved) in [
        (rejected_source(), released_source()),
        (released_source(), rejected_source()),
    ] {
        for state in [None, Some(received())] {
            let mut node = ServerNode::new().await?;
            let mut session = node.begin().await?;
            let mut incoming = node.request(&mut session, requested.clone()).await?;
            incoming.source = Some(approved.clone());
            let expected = Outcome::try_from(approved.default_outcome.clone().unwrap()).unwrap();
            let mut sender = node.approve(&session, incoming).await?;
            let (receipt, _) = node.send(&mut sender, 1, state, true).await?;
            assert_eq!(receipt.outcome(), &expected);
            timeout(DEADLINE, receipt.accept()).await??;
            assert_flow_only(&node.peer.barrier(WINDOW).await?);
            node.healthy(&mut sender).await?;
            node.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn nonterminal_source_default_is_refused_without_wire_or_consuming_a_valid_approval()
-> TestResult {
    let mut node = ServerNode::new().await?;
    let mut session = node.begin().await?;
    let original = node.request(&mut session, released_source()).await?;
    let mut invalid = original.clone();
    invalid
        .source
        .as_mut()
        .expect("selected Source")
        .default_outcome = Some(received());
    assert!(matches!(
        timeout(DEADLINE, session.accept_attach(invalid, 256 * 1024)).await?,
        Err(EngineError::InvalidState(_))
    ));
    assert_flow_only(&node.peer.barrier(WINDOW).await?);

    let mut sender = node.approve(&session, original).await?;
    let (receipt, _) = node.send(&mut sender, 1, None, true).await?;
    assert_eq!(receipt.outcome(), &Outcome::Released(Released));
    timeout(DEADLINE, receipt.accept()).await??;
    assert_flow_only(&node.peer.barrier(WINDOW).await?);
    node.healthy(&mut sender).await?;
    node.finish().await
}

#[tokio::test]
async fn cleared_approved_default_does_not_reuse_the_requested_default() -> TestResult {
    for state in [None, Some(received())] {
        let mut node = ServerNode::new().await?;
        let mut session = node.begin().await?;
        let mut incoming = node.request(&mut session, released_source()).await?;
        incoming
            .source
            .as_mut()
            .expect("source request")
            .default_outcome = None;
        let mut sender = node.approve(&session, incoming).await?;
        let message = Message::data(vec![1]);
        let peer = &mut node.peer;
        let (result, observed) = timeout(DEADLINE, async {
            tokio::join!(
                sender.send_with_settlement(message.clone(), vec![1].into()),
                async {
                    let id = peer.delivery(0, &message).await?;
                    peer.disposition(id, state, true).await?;
                    Ok::<_, Box<dyn StdError>>(())
                }
            )
        })
        .await?;
        observed?;
        assert!(matches!(
            result,
            Err(EngineError::RemoteSettledWithoutOutcome)
        ));
        assert_flow_only(&node.peer.barrier(WINDOW).await?);
        node.healthy(&mut sender).await?;
        node.finish().await?;
    }
    Ok(())
}
