use tokio::{io::DuplexStream, time::timeout};

use super::*;

const CUSTOM_FORMAT: u32 = 0xf123_4567;

fn raw_decoder(bytes: &[u8]) -> io::Result<Message> {
    Ok(Message::data(bytes.to_vec()))
}

fn marker_decoder(_: &[u8]) -> io::Result<Message> {
    Ok(Message::builder()
        .body(crate::Body::Value(crate::Value::Uint(42)))
        .build())
}

fn failing_decoder(_: &[u8]) -> io::Result<Message> {
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "custom decoder refused payload",
    ))
}

fn huge_error_decoder(_: &[u8]) -> io::Result<Message> {
    Err(io::Error::other("x".repeat(10_000)))
}

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(Duration::from_secs(2), read_frame(peer))
        .await
        .expect("engine response")
        .expect("valid frame")
}

fn first(handle: u32, id: u32, format: u32, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(vec![0, 255, id as u8].into()),
        message_format: Some(format),
        settled: Some(true),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn continuation(handle: u32, more: bool) -> Transfer {
    Transfer {
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        ..first(handle, 0, 0, more)
    }
}

struct Harness {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<DuplexStream>,
    peer: DuplexStream,
    received: [mpsc::Receiver<Delivery>; 2],
}

impl Harness {
    fn new(decoders: MessageFormatDecoders) -> Self {
        let (wire, peer) = tokio::io::duplex(64 * 1024);
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        let mut receivers = Vec::new();
        for handle in 0..2 {
            let (deliveries, receiver) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let (detached, _) = watch::channel(false);
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            session.links.insert(
                handle,
                LinkState::Receiving(ReceivingLink {
                    max_message_size: 1024 * 1024,
                    deliveries,
                    partial: None,
                    detached,
                    credit,
                    decoders: if handle == 0 {
                        decoders.clone()
                    } else {
                        MessageFormatDecoders::default()
                    },
                    identity: LinkIdentity::new(),
                    sender_settle_mode: SenderSettleMode::Mixed,
                    receiver_settle_mode: ReceiverSettleMode::First,
                }),
            );
            receivers.push(receiver);
        }
        Self {
            sessions: HashMap::from([(0, session)]),
            writer: FrameWriter::new(wire, 512).expect("writer"),
            peer,
            received: receivers.try_into().expect("two receiver queues"),
        }
    }

    async fn transfer(&mut self, transfer: Transfer, bytes: &[u8]) {
        receive_transfer(
            0,
            transfer,
            bytes.to_vec(),
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("link-scoped transfer handling");
    }

    async fn assert_detach(&mut self, handle: u32, condition: &str) {
        let Frame::Amqp {
            performative: Some(Performative::Detach(detach)),
            ..
        } = next_frame(&mut self.peer).await
        else {
            panic!("link refusal");
        };
        assert_eq!(detach.handle, handle);
        assert_eq!(
            detach.error.expect("refusal reason").condition.as_symbol(),
            Symbol::from(condition)
        );
    }

    async fn assert_healthy(&mut self) {
        let expected = Message::data(b"healthy".to_vec());
        self.transfer(
            first(1, 99, 0, false),
            &encode_message(&expected).expect("ordinary bytes"),
        )
        .await;
        let delivery = self.received[1]
            .try_recv()
            .expect("other same-session link still delivers");
        assert_eq!(delivery.message(), &expected);
        assert_eq!(delivery.message_format(), 0);
    }
}

#[tokio::test]
async fn unknown_format_is_refused_before_zero_credit_or_first_frame_abort() {
    for aborted in [false, true] {
        let mut harness = Harness::new(MessageFormatDecoders::default());
        let LinkState::Receiving(link) = harness
            .sessions
            .get_mut(&0)
            .expect("session")
            .links
            .get_mut(&0)
            .expect("link")
        else {
            unreachable!()
        };
        // No published grant: a recognized format would fail transfer credit.
        link.credit = ReceiveCredit::new(
            0,
            LINK_CREDIT,
            Arc::new(Consumption::new(Arc::new(Notify::new()))),
        );
        let mut transfer = first(0, 17, CUSTOM_FORMAT, true);
        transfer.aborted = aborted;
        harness
            .transfer(transfer, b"garbage, even for an aborted transfer")
            .await;
        harness.assert_detach(0, "amqp:not-implemented").await;
        assert!(harness.received[0].try_recv().is_err());
        assert_eq!(
            harness.sessions[&0].flow.snapshot().next_incoming_id,
            1,
            "session accounting still counts the frame"
        );
        harness.assert_healthy().await;
    }
}

#[tokio::test]
async fn custom_registry_is_link_local_and_deliveries_retain_real_format_and_message() {
    let registry = MessageFormatDecoders::default()
        .with_decoder(CUSTOM_FORMAT, marker_decoder)
        .expect("registry");
    let mut harness = Harness::new(registry);
    harness
        .transfer(first(0, 7, CUSTOM_FORMAT, false), b"custom payload")
        .await;
    let delivery = harness.received[0]
        .try_recv()
        .expect("registered custom format delivered");
    assert_eq!(delivery.message_format(), CUSTOM_FORMAT);
    assert_eq!(
        delivery.message(),
        &marker_decoder(b"").expect("selected real decoder")
    );
    harness
        .transfer(
            first(1, 8, CUSTOM_FORMAT, false),
            b"same format, different link",
        )
        .await;
    harness.assert_detach(1, "amqp:not-implemented").await;
    assert!(harness.received[1].try_recv().is_err());
    harness
        .transfer(
            first(0, 9, 0, false),
            &encode_message(&Message::data(b"ordinary".to_vec())).expect("ordinary bytes"),
        )
        .await;
    let delivery = harness.received[0]
        .try_recv()
        .expect("built-in decoder still available");
    assert_eq!(delivery.message_format(), 0);
    assert_eq!(delivery.message(), &Message::data(b"ordinary".to_vec()));
}

#[tokio::test]
async fn fragmented_custom_payload_uses_one_approved_decoder_and_original_identity() {
    for repeat in [false, true] {
        let registry = MessageFormatDecoders::default()
            .with_decoder(u32::MAX, raw_decoder)
            .expect("registry");
        let mut harness = Harness::new(registry);
        let initial = first(0, 5, u32::MAX, true);
        harness.transfer(initial.clone(), b"one").await;
        assert!(harness.received[0].try_recv().is_err());
        let mut middle = continuation(0, true);
        let mut final_frame = continuation(0, false);
        if repeat {
            middle.delivery_id = initial.delivery_id;
            middle.delivery_tag = initial.delivery_tag.clone();
            middle.message_format = initial.message_format;
            final_frame.delivery_id = initial.delivery_id;
            final_frame.delivery_tag = initial.delivery_tag;
            final_frame.message_format = initial.message_format;
        }
        harness.transfer(middle, b"two").await;
        harness.transfer(final_frame, b"three").await;
        let delivery = harness.received[0].try_recv().expect("completed delivery");
        assert_eq!(delivery.id, 5);
        assert!(delivery.settled, "settled remains sticky");
        assert_eq!(delivery.message_format(), u32::MAX);
        assert_eq!(delivery.message(), &Message::data(b"onetwothree".to_vec()));
        let LinkState::Receiving(link) = &harness.sessions[&0].links[&0] else {
            unreachable!()
        };
        assert_eq!(link.credit.delivery_count(), 1);
        assert_eq!(link.credit.occupied(), 1);
        assert_eq!(harness.sessions[&0].flow.snapshot().next_incoming_id, 3);
    }
}

#[tokio::test]
async fn custom_handler_failure_and_changed_continuation_format_detach_only_their_link() {
    for changed in [false, true] {
        let decoder: fn(&[u8]) -> io::Result<Message> = if changed {
            raw_decoder
        } else {
            failing_decoder
        };
        let registry = MessageFormatDecoders::default()
            .with_decoder(CUSTOM_FORMAT, decoder)
            .expect("registry");
        let mut harness = Harness::new(registry);
        harness
            .transfer(first(0, 4, CUSTOM_FORMAT, true), b"first")
            .await;
        let mut final_frame = continuation(0, false);
        if changed {
            final_frame.message_format = Some(0);
        }
        harness.transfer(final_frame, b"last").await;
        harness.assert_detach(0, "amqp:invalid-field").await;
        assert!(harness.received[0].try_recv().is_err());
        harness.assert_healthy().await;
    }
}

#[tokio::test]
async fn custom_handler_error_text_cannot_expand_a_link_refusal_beyond_the_peer_frame_cap() {
    let registry = MessageFormatDecoders::default()
        .with_decoder(CUSTOM_FORMAT, huge_error_decoder)
        .expect("registry");
    let mut harness = Harness::new(registry);
    harness
        .transfer(first(0, 3, CUSTOM_FORMAT, false), b"payload")
        .await;
    harness.assert_detach(0, "amqp:invalid-field").await;
    harness.assert_healthy().await;
}

#[tokio::test]
async fn recognized_aborted_formats_never_call_the_handler_and_free_one_slot() {
    let registry = MessageFormatDecoders::default()
        .with_decoder(CUSTOM_FORMAT, failing_decoder)
        .expect("registry");
    let mut harness = Harness::new(registry);
    let mut aborted = first(0, 8, CUSTOM_FORMAT, true);
    aborted.aborted = true;
    harness.transfer(aborted, b"not decoded").await;
    let LinkState::Receiving(link) = &harness.sessions[&0].links[&0] else {
        unreachable!()
    };
    assert_eq!(link.credit.delivery_count(), 1);
    assert_eq!(link.credit.occupied(), 0);
    assert!(harness.received[0].try_recv().is_err());
    let expected = Message::data(b"retry".to_vec());
    harness
        .transfer(
            first(0, 9, 0, false),
            &encode_message(&expected).expect("ordinary bytes"),
        )
        .await;
    assert_eq!(
        harness.received[0]
            .try_recv()
            .expect("link stays usable")
            .message(),
        &expected
    );
}

#[tokio::test]
async fn custom_registry_cannot_be_attached_to_a_local_sender_or_enqueue_a_command() {
    let (commands, mut queued) = mpsc::channel(1);
    let (_, incoming_attaches) = mpsc::channel(1);
    let session = ServerSession {
        channel: 0,
        identity: SessionIdentity::new(),
        commands,
        incoming_attaches,
        consumed: Arc::new(Notify::new()),
    };
    let attach = Attach {
        name: String::from("remote-receiver"),
        handle: 1,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    let registry = MessageFormatDecoders::default()
        .with_decoder(CUSTOM_FORMAT, raw_decoder)
        .expect("registry");
    let attach = IncomingAttach::new(attach, session.identity.clone());
    assert!(matches!(
        session
            .accept_attach_with_decoders(attach, 1024, None, registry)
            .await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(
        matches!(queued.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "role mismatch fails before any acceptance command"
    );
}

#[test]
fn first_fragment_preflight_includes_the_selected_format_encoding_width() {
    let (wire, _) = tokio::io::duplex(4096);
    let writer = FrameWriter::new(wire, 512).expect("writer");
    let bytes = vec![1; 1024];
    let tag = vec![1, 2].into();
    let (ordinary, ordinary_offset, _) =
        fragment_frame(0, 0, 0, &tag, 0, true, &bytes, 0, false, &writer)
            .expect("ordinary fragment");
    let (custom, custom_offset, _) =
        fragment_frame(0, 0, 0, &tag, u32::MAX, true, &bytes, 0, false, &writer)
            .expect("custom fragment");
    assert_eq!(
        crate::encode_frame(&ordinary)
            .expect("ordinary frame")
            .len(),
        512
    );
    assert_eq!(
        crate::encode_frame(&custom).expect("custom frame").len(),
        512
    );
    assert_eq!(
        ordinary_offset - custom_offset,
        4,
        "full uint is wider than uint0"
    );
    assert!(matches!(
        custom,
        Frame::Amqp {
            performative: Some(Performative::Transfer(Transfer {
                message_format: Some(u32::MAX),
                ..
            })),
            ..
        }
    ));
}

#[tokio::test]
async fn channel_above_our_advertised_maximum_gets_a_bounded_framing_error_close() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                channel_max: 2,
                ..Open::new("channel-peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        let Frame::Amqp {
            performative: Some(Performative::Open(open)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("server Open");
        };
        assert_eq!(open.channel_max, 2);
        peer
    };
    let (connection, mut peer) = tokio::join!(
        ServerConnection::accept(wire, "channel-server", None),
        opening
    );
    let connection = connection.expect("connection");
    write_frame(
        &mut peer,
        &Frame::Amqp {
            channel: 3,
            performative: None,
            payload: Vec::new(),
        },
    )
    .await
    .expect("out-of-range heartbeat");
    let Frame::Amqp {
        performative: Some(Performative::Close(close)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("framing error Close");
    };
    assert_eq!(
        close.error.expect("channel limit error").condition,
        crate::ErrorCondition::Custom(Symbol::from("amqp:connection:framing-error"))
    );
    connection.lifecycle.wait_terminated().await;
    let mut remaining = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut peer, &mut remaining)
        .await
        .expect("reader joined without waiting for ack");
    assert!(remaining.is_empty());
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_custom_send_fragments_keep_the_format_and_ordinary_send_stays_zero() {
    let (server_wire, client_wire) = tokio::io::duplex(64 * 1024);
    let custom_message = Message::data(vec![7; 600 * 1024]);
    let ordinary_message = Message::data(b"ordinary-after-custom".to_vec());
    let server = async {
        let mut connection = ServerConnection::accept(server_wire, "format-server", None)
            .await
            .expect("server connection");
        let incoming = connection.next_incoming_session().await.expect("session");
        let mut session = connection
            .accept_session(incoming)
            .await
            .expect("accept session");
        let attach = session.next_incoming_attach().await.expect("sender attach");
        let registry = MessageFormatDecoders::default()
            .with_decoder(CUSTOM_FORMAT, decode_message)
            .expect("format registry");
        let LinkEndpoint::Receiver(mut receiver) = session
            .accept_attach_with_decoders(attach, 1024 * 1024, None, registry)
            .await
            .expect("custom receiver")
        else {
            panic!("receiving endpoint");
        };
        for (id, format, expected) in [
            (0, CUSTOM_FORMAT, &custom_message),
            (1, 0, &ordinary_message),
        ] {
            let delivery = receiver.recv().await.expect("delivery");
            assert_eq!(delivery.id, id);
            assert_eq!(delivery.message_format(), format);
            assert_eq!(delivery.message(), expected);
            receiver.accept(&delivery).await.expect("settle delivery");
        }
        assert!(
            connection.next_incoming_session().await.is_none(),
            "client closes after both outcomes"
        );
        connection.shutdown().await;
    };
    let client = async {
        let mut connection = ClientConnection::open(client_wire, "format-client", None)
            .await
            .expect("client connection");
        let mut session = connection.begin().await.expect("client session");
        let mut sender = session
            .attach_sender("format-sender", "queue")
            .await
            .expect("sender");
        assert!(matches!(
            sender
                .send_with_message_format(custom_message.clone(), CUSTOM_FORMAT)
                .await
                .expect("custom send"),
            Outcome::Accepted(_)
        ));
        assert!(matches!(
            sender
                .send(ordinary_message.clone())
                .await
                .expect("ordinary send"),
            Outcome::Accepted(_)
        ));
        connection.close().await.expect("graceful client close");
    };
    timeout(Duration::from_secs(4), async {
        tokio::join!(server, client);
    })
    .await
    .expect("fragmented custom delivery completes");
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_custom_send_emits_first_format_once_and_respects_exact_small_peer_frame_limit() {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let message = Message::data(vec![3; 4096]);
    let observing = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                max_frame_size: 512,
                ..Open::new("small-frame-peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("Begin");
        };
        write_amqp(
            &mut peer,
            channel,
            Performative::Begin(Begin {
                remote_channel: Some(channel),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
        let Frame::Amqp {
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("sender Attach");
        };
        let handle = attach.handle;
        let response = attach.response(attach.source.clone(), attach.target.clone());
        write_amqp(
            &mut peer,
            channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("receiver Attach");
        write_amqp(
            &mut peer,
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: SESSION_WINDOW,
                next_outgoing_id: 0,
                outgoing_window: SESSION_WINDOW,
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(1),
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await
        .expect("one delivery credit");
        let mut assembled = Vec::new();
        let mut frames = 0;
        let mut first_tag = None;
        loop {
            let frame = next_frame(&mut peer).await;
            assert!(crate::encode_frame(&frame).expect("wire frame").len() <= 512);
            let Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                payload,
                ..
            } = frame
            else {
                panic!("delivery fragment");
            };
            if frames == 0 {
                assert_eq!(transfer.delivery_id, Some(0));
                assert_eq!(transfer.message_format, Some(u32::MAX));
                first_tag = transfer.delivery_tag.clone();
                assert!(first_tag.is_some());
            } else {
                assert_eq!(transfer.delivery_id, None);
                assert_eq!(transfer.delivery_tag, None);
                assert_eq!(transfer.message_format, None);
            }
            assembled.extend(payload);
            frames += 1;
            if !transfer.more {
                break;
            }
        }
        assert!(
            frames > 8,
            "custom delivery is genuinely fragmented under one link credit"
        );
        assert_eq!(
            decode_message(&assembled).expect("reassembled standard payload"),
            message
        );
        assert_eq!(first_tag.expect("automatic client tag").len(), 8);
        write_amqp(
            &mut peer,
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: 0,
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
            Vec::new(),
        )
        .await
        .expect("delivery Accepted");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("Close ack");
    };
    let sending = async {
        let mut connection = ClientConnection::open(wire, "custom-format-client", None)
            .await
            .expect("client");
        let mut session = connection.begin().await.expect("session");
        let mut sender = session
            .attach_sender("custom", "queue")
            .await
            .expect("sender");
        assert!(matches!(
            sender
                .send_with_message_format(message.clone(), u32::MAX)
                .await
                .expect("custom send"),
            Outcome::Accepted(_)
        ));
        connection.close().await.expect("Close");
    };
    timeout(Duration::from_secs(4), async {
        tokio::join!(observing, sending);
    })
    .await
    .expect("small-frame custom delivery completes");
}
