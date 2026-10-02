use super::*;

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<tokio::io::DuplexStream>,
    peer: tokio::io::DuplexStream,
    received: [mpsc::Receiver<Delivery>; 2],
    consumed: [Arc<Consumption>; 2],
}

impl Fixture {
    fn new(sender_mode: SenderSettleMode, receiver_mode: ReceiverSettleMode) -> Self {
        let (wire, peer) = tokio::io::duplex(1024 * 1024);
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        let consumed: [Arc<Consumption>; 2] =
            std::array::from_fn(|_| Arc::new(Consumption::new(Arc::new(Notify::new()))));
        let mut received = Vec::new();
        for (handle, consumption) in consumed.iter().enumerate() {
            let (deliveries, receiver) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let (detached, _) = watch::channel(false);
            let mut credit = ReceiveCredit::new(0, LINK_CREDIT, consumption.clone());
            credit.take_refill().expect("initial grant");
            session.links.insert(
                handle as u32,
                LinkState::Receiving(Box::new(ReceivingLink {
                    max_message_size: u64::MAX,
                    deliveries: deliveries.into(),
                    partial: None,
                    detached,
                    credit,
                    decoders: MessageFormatDecoders::default(),
                    identity: LinkIdentity::new(),
                    sender_settle_mode: sender_mode.clone(),
                    receiver_settle_mode: receiver_mode.clone(),
                })),
            );
            received.push(receiver);
        }
        Self {
            sessions: HashMap::from([(0, session)]),
            writer: FrameWriter::new(wire, 512).expect("writer"),
            peer,
            received: received.try_into().expect("two receiving links"),
            consumed,
        }
    }

    async fn transfer(&mut self, transfer: Transfer, payload: Vec<u8>) {
        receive_transfer(0, transfer, payload, &mut self.sessions, &mut self.writer)
            .await
            .expect("bounded transfer processing");
    }

    fn delivery(&mut self, handle: usize) -> Delivery {
        self.received[handle]
            .try_recv()
            .expect("delivery published")
    }

    async fn settle(
        &mut self,
        handle: u32,
        delivery: &Delivery,
        state: DeliveryState,
    ) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        settle_incoming(
            0,
            handle,
            delivery.identity.clone(),
            state,
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("local settlement leaves the driver usable");
        response.await.expect("settlement reply")
    }

    async fn sender_ack(&mut self, first: u32, last: Option<u32>) {
        apply_disposition(
            0,
            Disposition {
                role: Role::Sender,
                first,
                last,
                settled: true,
                state: None,
                batchable: false,
            },
            &mut self.writer,
            &mut self.sessions,
        )
        .await
        .expect("sender settlement");
    }

    async fn control(&mut self) -> Performative {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut self.peer))
                .await
                .expect("bounded control read")
                .expect("control frame");
            if let Frame::Amqp {
                performative: Some(performative),
                ..
            } = frame
                && !matches!(performative, Performative::Flow(_))
            {
                return performative;
            }
        }
    }

    async fn disposition(&mut self, id: u32, settled: bool) {
        let Performative::Disposition(disposition) = self.control().await else {
            panic!("expected disposition");
        };
        assert_eq!(disposition.role, Role::Receiver);
        assert_eq!(disposition.first, id);
        assert_eq!(disposition.last, None);
        assert_eq!(disposition.settled, settled);
    }

    async fn detached(&mut self, handle: u32, condition: &str) {
        let Performative::Detach(detach) = self.control().await else {
            panic!("expected link refusal");
        };
        assert_eq!(detach.handle, handle);
        assert_eq!(
            detach.error.expect("refusal error").condition.as_symbol(),
            Symbol::from(condition)
        );
        assert!(self.received[handle as usize].try_recv().is_err());
        assert!(self.sessions[&0].links.contains_key(&(1 - handle)));
    }

    async fn no_control(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(5), read_frame(&mut self.peer))
                .await
                .is_err()
        );
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

fn continuation() -> Transfer {
    Transfer {
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        ..first(0, 0)
    }
}

fn encoded() -> Vec<u8> {
    encode_message(&Message::data(vec![1, 2, 3])).expect("message")
}

fn accepted() -> DeliveryState {
    DeliveryState::Accepted(Accepted)
}

#[tokio::test]
async fn ownership_and_settled_tokens_cannot_settle_a_reused_numeric_alias() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    fixture.transfer(first(0, 7), encoded()).await;
    let old = fixture.delivery(0);
    assert!(matches!(
        fixture.settle(1, &old, accepted()).await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.no_control().await;
    fixture
        .settle(0, &old, accepted())
        .await
        .expect("first outcome");
    fixture.disposition(7, true).await;

    fixture.transfer(first(0, 7), encoded()).await;
    let new = fixture.delivery(0);
    fixture
        .settle(0, &old, accepted())
        .await
        .expect("owned repeat");
    fixture.no_control().await;
    fixture
        .settle(0, &new, accepted())
        .await
        .expect("new outcome");
    fixture.disposition(7, true).await;
}

#[tokio::test]
async fn sender_presettled_tokens_still_validate_the_current_link_generation() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    let mut transfer = first(0, 0);
    transfer.settled = Some(true);
    fixture.transfer(transfer, encoded()).await;
    let old = fixture.delivery(0);
    assert!(old.settled);
    assert!(matches!(
        fixture.settle(1, &old, accepted()).await,
        Err(EngineError::InvalidState(_))
    ));
    fixture
        .settle(0, &old, accepted())
        .await
        .expect("owned no-op");
    fixture.no_control().await;

    let session = fixture.sessions.get_mut(&0).expect("session");
    let LinkState::Receiving(mut link) = session.links.remove(&0).expect("old link") else {
        unreachable!();
    };
    session.incoming.remove_link(&link.identity);
    link.identity = LinkIdentity::new();
    session.links.insert(0, LinkState::Receiving(link));
    assert!(matches!(
        fixture.settle(0, &old, accepted()).await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.no_control().await;
}

#[tokio::test]
async fn partial_id_collision_refuses_only_the_intruding_link_and_retains_the_original() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    let bytes = encoded();
    let mut initial = first(0, 42);
    initial.more = true;
    fixture.transfer(initial, bytes[..2].to_vec()).await;
    let mut intruder = first(1, 42);
    intruder.aborted = true;
    fixture.transfer(intruder, Vec::new()).await;
    fixture.detached(1, "amqp:invalid-field").await;
    fixture.transfer(continuation(), bytes[2..].to_vec()).await;
    let original = fixture.delivery(0);
    fixture
        .settle(0, &original, accepted())
        .await
        .expect("original outcome");
    fixture.disposition(42, true).await;
}

#[tokio::test]
async fn a_live_tag_collision_is_checked_before_first_abort_accounting() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    fixture.transfer(first(0, 1), encoded()).await;
    let original = fixture.delivery(0);
    let mut intruder = first(0, 2);
    intruder.delivery_tag = Some(1u32.to_be_bytes().to_vec().into());
    intruder.aborted = true;
    fixture.transfer(intruder, Vec::new()).await;
    fixture.detached(0, "amqp:invalid-field").await;
    assert!(matches!(
        fixture.settle(0, &original, accepted()).await,
        Err(EngineError::InvalidState(_))
    ));
    fixture.transfer(first(1, 1), encoded()).await;
    let healthy = fixture.delivery(1);
    fixture
        .settle(1, &healthy, accepted())
        .await
        .expect("healthy link");
    fixture.disposition(1, true).await;
}

#[tokio::test]
async fn second_mode_retains_aliases_until_the_wrapping_sender_ack() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::Second);
    for id in [u32::MAX, 0] {
        fixture.transfer(first(0, id), encoded()).await;
        let delivery = fixture.delivery(0);
        fixture
            .settle(0, &delivery, accepted())
            .await
            .expect("second outcome");
        fixture.disposition(id, false).await;
        fixture
            .settle(0, &delivery, accepted())
            .await
            .expect("pending repeat");
        fixture.no_control().await;
    }
    fixture.sender_ack(u32::MAX, Some(0)).await;
    fixture.no_control().await;
    fixture.transfer(first(0, 0), encoded()).await;
    let reused = fixture.delivery(0);
    fixture
        .settle(0, &reused, accepted())
        .await
        .expect("released alias");
    fixture.disposition(0, false).await;
}

#[tokio::test]
async fn early_sender_ack_on_a_partial_suppresses_the_completed_delivery_outcome() {
    let mut fixture = Fixture::new(SenderSettleMode::Unsettled, ReceiverSettleMode::Second);
    let bytes = encoded();
    let mut initial = first(0, 0);
    initial.more = true;
    fixture.transfer(initial, bytes[..2].to_vec()).await;
    fixture.sender_ack(0, None).await;
    fixture.transfer(continuation(), bytes[2..].to_vec()).await;
    let old = fixture.delivery(0);
    assert!(old.settled);
    fixture
        .settle(0, &old, accepted())
        .await
        .expect("sender forgot");
    fixture.no_control().await;
    fixture.transfer(first(0, 0), encoded()).await;
    let reused = fixture.delivery(0);
    fixture
        .settle(0, &reused, accepted())
        .await
        .expect("reused alias");
    fixture.disposition(0, false).await;
}

#[tokio::test]
async fn completing_frame_receiver_mode_is_not_sticky_fragment_identity() {
    for (first_mode, final_mode, settled) in [
        (Some(ReceiverSettleMode::First), None, false),
        (None, Some(ReceiverSettleMode::First), true),
    ] {
        let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::Second);
        let bytes = encoded();
        let mut initial = first(0, 0);
        initial.more = true;
        initial.rcv_settle_mode = first_mode;
        fixture.transfer(initial, bytes[..2].to_vec()).await;
        let mut final_frame = continuation();
        final_frame.rcv_settle_mode = final_mode;
        fixture.transfer(final_frame, bytes[2..].to_vec()).await;
        let delivery = fixture.delivery(0);
        fixture
            .settle(0, &delivery, accepted())
            .await
            .expect("effective mode");
        fixture.disposition(0, settled).await;
    }
}

#[tokio::test]
async fn forbidden_receiver_override_is_ignored_only_when_transfer_settlement_or_abort_wins() {
    for (settled, aborted) in [(true, false), (false, true), (false, false)] {
        let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
        let bytes = encoded();
        let mut initial = first(0, 0);
        initial.more = true;
        initial.rcv_settle_mode = Some(ReceiverSettleMode::Second);
        fixture.transfer(initial, bytes[..2].to_vec()).await;
        let mut final_frame = continuation();
        final_frame.settled = Some(settled);
        final_frame.aborted = aborted;
        fixture.transfer(final_frame, bytes[2..].to_vec()).await;
        if settled {
            let delivery = fixture.delivery(0);
            fixture
                .settle(0, &delivery, accepted())
                .await
                .expect("presettled");
            fixture.no_control().await;
        } else if aborted {
            assert!(fixture.received[0].try_recv().is_err());
            fixture.transfer(first(0, 0), encoded()).await;
            let retry = fixture.delivery(0);
            fixture
                .settle(0, &retry, accepted())
                .await
                .expect("abort released aliases");
            fixture.disposition(0, true).await;
        } else {
            fixture.detached(0, "amqp:invalid-field").await;
        }
    }
}

#[tokio::test]
async fn negotiated_sender_modes_are_validated_independently_of_early_remote_ack() {
    for mode in [SenderSettleMode::Unsettled, SenderSettleMode::Settled] {
        let mut fixture = Fixture::new(mode.clone(), ReceiverSettleMode::First);
        let bytes = encoded();
        let mut initial = first(0, 0);
        initial.more = true;
        initial.settled = Some(mode == SenderSettleMode::Unsettled);
        fixture.transfer(initial, bytes[..2].to_vec()).await;
        fixture.sender_ack(0, None).await;
        fixture.transfer(continuation(), bytes[2..].to_vec()).await;
        fixture.detached(0, "amqp:invalid-field").await;
    }
}

#[tokio::test]
async fn outcome_preflight_refusal_does_not_consume_the_delivery_alias() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    fixture.transfer(first(0, 0), encoded()).await;
    let delivery = fixture.delivery(0);
    let oversized = DeliveryState::Rejected(crate::Rejected {
        error: Some(Error::new(
            crate::AmqpError::InvalidField,
            "x".repeat(1024),
            None,
        )),
    });
    assert!(matches!(
        fixture.settle(0, &delivery, oversized).await,
        Err(EngineError::Io(_))
    ));
    fixture.no_control().await;
    fixture
        .settle(0, &delivery, accepted())
        .await
        .expect("retry outcome");
    fixture.disposition(0, true).await;
}

#[tokio::test]
async fn receiver_consumption_does_not_make_the_unsettled_ledger_unbounded() {
    let mut fixture = Fixture::new(SenderSettleMode::Mixed, ReceiverSettleMode::First);
    for id in 0..incoming_ledger::MAX_INCOMING_DELIVERIES_PER_LINK as u32 {
        fixture.transfer(first(0, id), encoded()).await;
        let _ = fixture.delivery(0);
        fixture.consumed[0].consumed();
        refresh_consumed(&mut fixture.writer, &mut fixture.sessions)
            .await
            .expect("bounded replenishment");
    }
    fixture
        .transfer(
            first(0, incoming_ledger::MAX_INCOMING_DELIVERIES_PER_LINK as u32),
            encoded(),
        )
        .await;
    fixture.detached(0, "amqp:resource-limit-exceeded").await;
    fixture.transfer(first(1, 0), encoded()).await;
    let healthy = fixture.delivery(1);
    fixture
        .settle(1, &healthy, accepted())
        .await
        .expect("other link remains live");
    fixture.disposition(0, true).await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_receiver_builder_second_mode_emits_unsettled_outcomes_and_accepts_sender_acks() {
    async fn next(peer: &mut tokio::io::DuplexStream) -> Performative {
        loop {
            match read_frame(peer).await.expect("client control frame") {
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                Frame::Amqp {
                    performative: Some(performative),
                    ..
                } => return performative,
                frame => panic!("unexpected frame: {frame:?}"),
            }
        }
    }

    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let raw = tokio::spawn(async move {
        assert_eq!(
            read_protocol_header(&mut peer).await.expect("header"),
            ProtocolHeader::AMQP
        );
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(next(&mut peer).await, Performative::Open(_)));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        assert!(matches!(next(&mut peer).await, Performative::Begin(_)));
        write_amqp(
            &mut peer,
            0,
            Performative::Begin(Begin {
                remote_channel: Some(0),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
        let mut handle = None;
        for _ in 0..2 {
            let Performative::Attach(attach) = next(&mut peer).await else {
                panic!("receiver Attach");
            };
            assert_eq!(attach.rcv_settle_mode, ReceiverSettleMode::Second);
            handle.get_or_insert(attach.handle);
            let response = attach.response(attach.source.clone(), attach.target.clone());
            write_amqp(
                &mut peer,
                0,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await
            .expect("sender Attach");
        }
        let handle = handle.expect("first receiving link");
        for _ in 0..2 {
            write_amqp(
                &mut peer,
                0,
                Performative::Transfer(first(handle, 0)),
                encoded(),
            )
            .await
            .expect("delivery");
            let Performative::Disposition(outcome) = next(&mut peer).await else {
                panic!("second-mode outcome");
            };
            assert_eq!(outcome.role, Role::Receiver);
            assert_eq!(outcome.first, 0);
            assert!(!outcome.settled);
            write_amqp(
                &mut peer,
                0,
                Performative::Disposition(Disposition {
                    role: Role::Sender,
                    first: 0,
                    last: None,
                    settled: true,
                    state: None,
                    batchable: false,
                }),
                Vec::new(),
            )
            .await
            .expect("sender ACK releases the reused alias");
        }
        let Performative::Detach(detach) = next(&mut peer).await else {
            panic!("receiver Detach");
        };
        assert_eq!(detach.handle, handle);
        write_amqp(&mut peer, 0, Performative::Detach(detach), Vec::new())
            .await
            .expect("peer Detach");
        assert!(matches!(next(&mut peer).await, Performative::Close(_)));
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("peer Close");
    });
    let run = async {
        let mut connection = ClientConnection::open(wire, "client", None)
            .await
            .expect("client connection");
        let mut session = connection.begin().await.expect("client session");
        let mut receiver = ClientReceiver::builder()
            .name("receiver")
            .source("queue")
            .receiver_settle_mode(ReceiverSettleMode::Second)
            .attach(&mut session)
            .await
            .expect("Second receiving link");
        let other = ClientReceiver::builder()
            .name("other")
            .source("queue")
            .receiver_settle_mode(ReceiverSettleMode::Second)
            .attach(&mut session)
            .await
            .expect("other receiving link");
        let mut last = None;
        for _ in 0..2 {
            let delivery = receiver.recv().await.expect("delivery");
            assert!(matches!(
                other.accept(&delivery).await,
                Err(EngineError::InvalidState(_))
            ));
            receiver.accept(&delivery).await.expect("Second outcome");
            last = Some(delivery);
        }
        receiver.close().await.expect("receiver Detach");
        assert!(matches!(
            receiver.accept(&last.expect("last delivery")).await,
            Err(EngineError::InvalidState(_))
        ));
        connection.close().await.expect("client Close");
        raw.await.expect("raw peer");
    };
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("bounded Second exchange");
}
