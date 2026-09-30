use tokio::{io::DuplexStream, time::timeout};

use super::*;

struct Harness {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<DuplexStream>,
    peer: DuplexStream,
    affected: mpsc::Receiver<Delivery>,
    healthy: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
}

impl Harness {
    fn new() -> Self {
        let (wire, peer) = tokio::io::duplex(64 * 1024);
        let mut session = SessionState::new(&Begin::default());
        let mut receivers = Vec::new();
        let mut affected_detached = None;
        for handle in 0..2 {
            let (deliveries, receiver) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let (detached, observed_detached) = watch::channel(false);
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            session.links.insert(
                handle,
                LinkState::Receiving(ReceivingLink {
                    max_message_size: 4 * 1024 * 1024,
                    deliveries,
                    partial: None,
                    detached,
                    credit,
                    decoders: MessageFormatDecoders::default(),
                }),
            );
            receivers.push(receiver);
            if handle == 0 {
                affected_detached = Some(observed_detached);
            }
        }
        let healthy = receivers.pop().expect("healthy queue");
        let affected = receivers.pop().expect("affected queue");
        Self {
            sessions: HashMap::from([(0, session)]),
            writer: FrameWriter::new(wire, 512).expect("frame writer"),
            peer,
            affected,
            healthy,
            detached: affected_detached.expect("affected detach signal"),
        }
    }

    async fn receive(&mut self, transfer: Transfer, payload: Vec<u8>) {
        receive_transfer(0, transfer, payload, &mut self.sessions, &mut self.writer)
            .await
            .expect("invalid known-link metadata is not a connection error");
    }

    async fn assert_refusal_and_healthy_link(&mut self, condition: &str) {
        let frame = timeout(Duration::from_secs(2), read_frame(&mut self.peer))
            .await
            .expect("prompt link refusal")
            .expect("valid refusal frame");
        let Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Detach(detach)),
            ..
        } = frame
        else {
            panic!("metadata errors detach only the affected link");
        };
        assert_eq!(detach.handle, 0);
        assert_eq!(
            detach
                .error
                .expect("refusal condition")
                .condition
                .as_symbol(),
            Symbol::from(condition)
        );
        assert!(*self.detached.borrow());
        assert!(!self.sessions[&0].ending);
        assert!(!self.sessions[&0].links.contains_key(&0));
        assert!(self.sessions[&0].links.contains_key(&1));
        self.receive(first(1, 100, Vec::new(), false), encoded())
            .await;
        assert_eq!(
            self.healthy
                .recv()
                .await
                .expect("unrelated link delivery")
                .message,
            message()
        );
    }
}

fn message() -> Message {
    Message::data(b"identity preserved".to_vec())
}

fn encoded() -> Vec<u8> {
    encode_message(&message()).expect("encoded message")
}

fn continuation(handle: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn first(handle: u32, id: u32, tag: Vec<u8>, more: bool) -> Transfer {
    Transfer {
        delivery_id: Some(id),
        delivery_tag: Some(tag.into()),
        message_format: Some(0),
        more,
        ..continuation(handle)
    }
}

#[tokio::test]
async fn first_transfer_requires_identity_and_refuses_unsupported_formats_without_storing_content()
{
    for (case, more) in (0..7).flat_map(|case| [false, true].map(|more| (case, more))) {
        let mut harness = Harness::new();
        let mut transfer = first(0, 17, vec![1], more);
        let condition = match case {
            0 => {
                transfer.delivery_id = None;
                "amqp:invalid-field"
            }
            1 => {
                transfer.delivery_tag = None;
                "amqp:invalid-field"
            }
            2 => {
                transfer.message_format = None;
                "amqp:invalid-field"
            }
            3 => {
                transfer.delivery_tag = Some(vec![1; 33].into());
                "amqp:invalid-field"
            }
            4 => {
                transfer.message_format = Some(1);
                "amqp:not-implemented"
            }
            5 => {
                transfer.message_format = Some(u32::MAX);
                "amqp:not-implemented"
            }
            6 => {
                transfer.message_format = Some(0x8001_3700);
                "amqp:not-implemented"
            }
            _ => unreachable!(),
        };
        harness.receive(transfer, encoded()).await;
        assert_eq!(harness.sessions[&0].flow.snapshot().next_incoming_id, 1);
        assert!(
            harness.affected.recv().await.is_none(),
            "case {case} must not deliver its body"
        );
        harness.assert_refusal_and_healthy_link(condition).await;
    }
}

#[tokio::test]
async fn continuation_identity_changes_refuse_only_the_partial_delivery_link() {
    for (case, aborted) in (0..4).flat_map(|case| [false, true].map(|aborted| (case, aborted))) {
        let mut harness = Harness::new();
        harness
            .receive(first(0, 17, vec![1], true), encoded()[..2].to_vec())
            .await;
        let mut transfer = continuation(0);
        transfer.aborted = aborted;
        match case {
            0 => transfer.delivery_id = Some(18),
            1 => transfer.delivery_tag = Some(vec![2].into()),
            2 => transfer.message_format = Some(1),
            3 => transfer.delivery_tag = Some(vec![1; 33].into()),
            _ => unreachable!(),
        }
        harness.receive(transfer, encoded()[2..].to_vec()).await;
        assert_eq!(harness.sessions[&0].flow.snapshot().next_incoming_id, 2);
        assert!(
            harness.affected.recv().await.is_none(),
            "case {case} discards its partial body"
        );
        harness
            .assert_refusal_and_healthy_link("amqp:invalid-field")
            .await;
    }
}

#[tokio::test]
async fn empty_and_maximum_tags_and_repeated_or_omitted_continuation_fields_are_valid() {
    for tag in [
        Vec::new(),
        vec![5; MAX_DELIVERY_TAG_BYTES],
        vec![0, 255, 128, 1],
    ] {
        for repeated in 0..8 {
            let mut harness = Harness::new();
            let bytes = encoded();
            let mut initial = first(0, 17, tag.clone(), true);
            initial.settled = Some(false);
            initial.rcv_settle_mode = Some(ReceiverSettleMode::First);
            harness.receive(initial, bytes[..2].to_vec()).await;
            let mut middle = continuation(0);
            middle.more = true;
            middle.settled = Some(true);
            middle.rcv_settle_mode = Some(ReceiverSettleMode::Second);
            if repeated & 1 != 0 {
                middle.delivery_id = Some(17);
            }
            if repeated & 2 != 0 {
                middle.delivery_tag = Some(tag.clone().into());
            }
            if repeated & 4 != 0 {
                middle.message_format = Some(0);
            }
            harness.receive(middle, bytes[2..3].to_vec()).await;
            let mut final_frame = continuation(0);
            final_frame.settled = Some(false);
            harness.receive(final_frame, bytes[3..].to_vec()).await;
            let delivery = harness
                .affected
                .recv()
                .await
                .expect("valid fragmented delivery");
            assert_eq!(delivery.id, 17);
            assert!(
                delivery.settled,
                "settled remains sticky across continuations"
            );
            assert_eq!(delivery.message, message());
            let LinkState::Receiving(link) = &harness.sessions[&0].links[&0] else {
                panic!("receiving link");
            };
            assert!(link.partial.is_none());
            assert_eq!(link.credit.delivery_count(), 1);
            assert_eq!(link.credit.occupied(), 1);
            assert!(!*harness.detached.borrow());
        }
    }
}

#[tokio::test]
async fn malformed_first_abort_cannot_release_an_already_queued_delivery_slot() {
    let mut harness = Harness::new();
    harness
        .receive(first(0, 17, vec![1], false), encoded())
        .await;
    let mut aborted = first(0, 18, vec![2], false);
    aborted.aborted = true;
    aborted.more = true;
    aborted.delivery_tag = None;
    harness.receive(aborted, Vec::new()).await;
    assert_eq!(
        harness
            .affected
            .recv()
            .await
            .expect("previous queued delivery is retained")
            .id,
        17
    );
    assert!(harness.affected.recv().await.is_none());
    harness
        .assert_refusal_and_healthy_link("amqp:invalid-field")
        .await;
}

#[tokio::test]
async fn valid_abort_counts_the_delivery_once_and_frees_its_reservation() {
    let mut harness = Harness::new();
    harness
        .receive(first(0, 17, vec![1], true), encoded()[..2].to_vec())
        .await;
    let mut aborted = continuation(0);
    aborted.aborted = true;
    aborted.more = true;
    aborted.delivery_id = Some(17);
    aborted.delivery_tag = Some(vec![1].into());
    aborted.message_format = Some(0);
    harness.receive(aborted, vec![255]).await;
    let Frame::Amqp {
        performative: Some(Performative::Flow(flow)),
        ..
    } = read_frame(&mut harness.peer).await.expect("abort refill")
    else {
        panic!("abort releases the slot");
    };
    assert_eq!(flow.delivery_count, Some(1));
    assert_eq!(flow.link_credit, Some(LINK_CREDIT));
    harness
        .receive(first(0, 18, Vec::new(), false), encoded())
        .await;
    assert_eq!(
        harness
            .affected
            .recv()
            .await
            .expect("next valid delivery")
            .id,
        18
    );
    let LinkState::Receiving(link) = &harness.sessions[&0].links[&0] else {
        panic!("receiving link");
    };
    assert_eq!(link.credit.delivery_count(), 2);
    assert_eq!(link.credit.occupied(), 1);
}

#[tokio::test]
async fn oversized_outbound_tag_is_refused_before_credit_and_delivery_or_frame_id_mutation() {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let mut writer = FrameWriter::new(wire, 512).expect("frame writer");
    let mut session = SessionState::new(&Begin::default());
    let mut credit = LinkCredit::new(0);
    credit
        .update_peer(Some(0), 1, false)
        .expect("one delivery grant");
    let (detached, detached_rx) = watch::channel(false);
    session.links.insert(
        0,
        LinkState::Sending(Box::new(SendingLink {
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::First,
            settle_mode: SenderSettleMode::Settled,
            credit,
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashSet::new(),
            detached,
        })),
    );
    let mut sessions = HashMap::from([(0, session)]);
    for length in [33, 1024] {
        let (reply, response) = oneshot::channel();
        handle_command(
            Command::Send {
                channel: 0,
                handle: 0,
                message: Box::new(message()),
                delivery_tag: vec![3; length].into(),
                reply,
            },
            &mut writer,
            &mut sessions,
            512,
        )
        .await
        .expect("local refusal keeps driver alive");
        assert!(matches!(
            response.await.expect("send reply"),
            Err(EngineError::InvalidState(_))
        ));
        assert_eq!(sessions[&0].next_delivery_id, 0);
        assert_eq!(sessions[&0].flow.snapshot().next_outgoing_id, 0);
        let LinkState::Sending(link) = &sessions[&0].links[&0] else {
            panic!("sender remains attached");
        };
        assert_eq!(link.credit.allowance(), 1);
        assert_eq!(link.credit.delivery_count(), 0);
        assert!(link.queued.is_empty());
        assert!(link.active.is_none());
        assert!(link.unsettled.is_empty());
        assert!(!*detached_rx.borrow());
    }
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::Send {
            channel: 0,
            handle: 0,
            message: Box::new(message()),
            delivery_tag: vec![3; 32].into(),
            reply,
        },
        &mut writer,
        &mut sessions,
        512,
    )
    .await
    .expect("maximum legal tag accepted");
    let mut cursor = 0;
    pump_connection(&mut writer, &mut sessions, &mut cursor)
        .await
        .expect("legal send pump");
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = read_frame(&mut peer).await.expect("legal first transfer")
    else {
        panic!("oversized requests emitted neither Transfer nor Detach before the legal send");
    };
    assert_eq!(transfer.delivery_id, Some(0));
    assert_eq!(transfer.delivery_tag.expect("tag").len(), 32);
    assert!(matches!(
        response
            .await
            .expect("legal send reply")
            .expect("legal send"),
        SendOutcome {
            outcome: Outcome::Accepted(_),
            ..
        }
    ));
}

#[cfg(feature = "test-client")]
async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(Duration::from_secs(2), read_frame(peer))
        .await
        .expect("client control progress")
        .expect("valid raw frame")
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, ClientSession, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let opening = async {
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
            Performative::Open(Open::new("identity-peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, peer) = tokio::join!(ClientConnection::open(wire, "client", None), opening);
    let mut connection = connection.expect("client connection");
    let mut peer = peer;
    let accepting = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("client Begin");
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
    };
    let (session, ()) = tokio::join!(connection.begin(), accepting);
    (connection, session.expect("client session"), peer)
}

#[cfg(feature = "test-client")]
async fn client_receiver(
    session: &mut ClientSession,
    peer: &mut DuplexStream,
    name: &str,
) -> (ClientReceiver, u32) {
    let accepting = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(peer).await
        else {
            panic!("receiver Attach");
        };
        let handle = attach.handle;
        let response = attach.response(attach.source.clone(), attach.target.clone());
        write_amqp(
            peer,
            channel,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("sender Attach response");
        assert!(matches!(
            next_frame(peer).await,
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            }
        ));
        handle
    };
    let (receiver, handle) = tokio::join!(session.attach_receiver(name, "queue"), accepting);
    (receiver.expect("client receiver"), handle)
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_invalid_first_and_continuation_identity_refuses_only_the_affected_link() {
    for case in 0..9 {
        let (connection, mut session, mut peer) = client_pair().await;
        let (mut affected, handle) = client_receiver(&mut session, &mut peer, "affected").await;
        let (mut healthy, healthy_handle) =
            client_receiver(&mut session, &mut peer, "healthy").await;
        let mut transfer = first(handle, 17, vec![1], false);
        let payload = if case >= 5 {
            write_amqp(
                &mut peer,
                0,
                Performative::Transfer(first(handle, 17, vec![1], true)),
                encoded()[..2].to_vec(),
            )
            .await
            .expect("valid first fragment");
            transfer = continuation(handle);
            encoded()[2..].to_vec()
        } else {
            encoded()
        };
        match case {
            0 => transfer.delivery_id = None,
            1 => transfer.delivery_tag = None,
            2 => transfer.message_format = None,
            3 => transfer.delivery_tag = Some(vec![1; 33].into()),
            4 => transfer.message_format = Some(0x8001_3700),
            5 => transfer.delivery_id = Some(18),
            6 => transfer.delivery_tag = Some(vec![2].into()),
            7 => transfer.message_format = Some(1),
            8 => {
                transfer.delivery_tag = Some(vec![2].into());
                transfer.aborted = true;
            }
            _ => unreachable!(),
        }
        write_amqp(&mut peer, 0, Performative::Transfer(transfer), payload)
            .await
            .expect("invalid identity transfer");
        let Frame::Amqp {
            performative: Some(Performative::Detach(detach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("link-scoped identity refusal");
        };
        assert_eq!(detach.handle, handle);
        let condition = if case == 4 {
            "amqp:not-implemented"
        } else {
            "amqp:invalid-field"
        };
        assert_eq!(
            detach.error.expect("condition").condition.as_symbol(),
            Symbol::from(condition)
        );
        assert!(matches!(
            affected.recv().await,
            Err(EngineError::RemoteDetached)
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Transfer(first(healthy_handle, 99, Vec::new(), false)),
            encoded(),
        )
        .await
        .expect("healthy link send");
        assert_eq!(
            healthy
                .recv()
                .await
                .expect("same-session healthy delivery")
                .message,
            message()
        );
        connection.shutdown().await;
    }
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_matching_abort_with_more_and_garbage_then_omitted_identity_is_valid() {
    let (connection, mut session, mut peer) = client_pair().await;
    let (mut receiver, handle) = client_receiver(&mut session, &mut peer, "receiver").await;
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(first(handle, 17, vec![0, 255], true)),
        encoded()[..2].to_vec(),
    )
    .await
    .expect("first partial frame");
    let mut abort = first(handle, 17, vec![0, 255], true);
    abort.aborted = true;
    write_amqp(&mut peer, 0, Performative::Transfer(abort), vec![255; 3])
        .await
        .expect("matching abort ignores more and payload");
    let Frame::Amqp {
        performative: Some(Performative::Flow(refill)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("abort releases its slot");
    };
    assert_eq!(refill.delivery_count, Some(1));
    assert_eq!(refill.link_credit, Some(LINK_CREDIT));
    let mut abort = first(handle, 18, Vec::new(), false);
    abort.aborted = true;
    write_amqp(&mut peer, 0, Performative::Transfer(abort), vec![255])
        .await
        .expect("valid standalone first abort");
    let Frame::Amqp {
        performative: Some(Performative::Flow(refill)),
        ..
    } = next_frame(&mut peer).await
    else {
        panic!("standalone abort releases its slot");
    };
    assert_eq!(refill.delivery_count, Some(2));
    assert_eq!(refill.link_credit, Some(LINK_CREDIT));
    let bytes = encoded();
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(first(handle, 19, vec![1; 32], true)),
        bytes[..2].to_vec(),
    )
    .await
    .expect("maximum tag first frame");
    let mut final_frame = continuation(handle);
    final_frame.settled = Some(true);
    final_frame.rcv_settle_mode = Some(ReceiverSettleMode::Second);
    write_amqp(
        &mut peer,
        0,
        Performative::Transfer(final_frame),
        bytes[2..].to_vec(),
    )
    .await
    .expect("omitted continuation identity");
    let delivery = receiver
        .recv()
        .await
        .expect("abort did not emit a delivery or consume extra slots");
    assert_eq!(delivery.id, 19);
    assert_eq!(delivery.message, message());
    assert!(delivery.settled);
    connection.shutdown().await;
}
