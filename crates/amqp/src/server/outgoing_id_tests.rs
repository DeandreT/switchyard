use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::*;

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;

#[derive(Clone, Default)]
struct Writer(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .lock()
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn sending() -> SendingLink {
    let (detached, _) = watch::channel(false);
    let mut credit = LinkCredit::new(0);
    credit
        .update_peer(Some(0), 10_000, false)
        .expect("link credit");
    SendingLink {
        identity: LinkIdentity::new(),
        auto_acknowledge: false,
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        default_outcome: None,
        outstanding_tags: HashSet::new(),
        settle_mode: SenderSettleMode::Unsettled,
        credit,
        queued: VecDeque::new(),
        active: None,
        unsettled: HashMap::new(),
        pending_acknowledgements: HashMap::new(),
        detached,
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    output: Writer,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new() -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        session
            .links
            .insert(HANDLE, LinkState::Sending(Box::new(sending())));
        let output = Writer::default();
        Self {
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(output.clone(), 512).expect("frame writer"),
            output,
        }
    }

    fn session(&self) -> &SessionState {
        &self.sessions[&CHANNEL]
    }

    fn session_mut(&mut self) -> &mut SessionState {
        self.sessions.get_mut(&CHANNEL).expect("session")
    }

    fn link(&self, handle: u32) -> &SendingLink {
        let LinkState::Sending(link) = &self.session().links[&handle] else {
            panic!("sending link")
        };
        link
    }

    fn link_mut(&mut self, handle: u32) -> &mut SendingLink {
        let LinkState::Sending(link) = self
            .session_mut()
            .links
            .get_mut(&handle)
            .expect("sending handle")
        else {
            panic!("sending link")
        };
        link
    }

    fn add_link(&mut self, handle: u32) {
        assert!(
            self.session_mut()
                .links
                .insert(handle, LinkState::Sending(Box::new(sending())))
                .is_none()
        );
    }

    async fn queue(
        &mut self,
        handle: u32,
        message: Message,
        tag: &[u8],
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let owner = self.link(handle).identity.clone();
        let (reply, result) = oneshot::channel();
        queue_send(
            CHANNEL,
            handle,
            self.sessions.get_mut(&CHANNEL).expect("session"),
            &owner,
            message,
            tag.to_vec().into(),
            0,
            reply,
            &mut self.writer,
            512,
        )
        .await
        .expect("enqueue");
        result
    }

    async fn fragment(&mut self, handle: u32) {
        send_fragment(
            CHANNEL,
            handle,
            self.sessions.get_mut(&CHANNEL).expect("session"),
            &mut self.writer,
        )
        .await
        .expect("outgoing fragment");
    }

    async fn finish(&mut self, handle: u32) {
        self.fragment(handle).await;
        while self.link(handle).active.is_some() {
            self.fragment(handle).await;
        }
    }

    fn unsettled(
        &mut self,
        handle: u32,
        id: u32,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        let tag = tag(handle, id);
        let link = self.link_mut(handle);
        assert!(link.outstanding_tags.insert(tag.clone()));
        assert!(
            link.unsettled
                .insert(
                    id,
                    OutgoingDelivery {
                        reply: reply.into(),
                        delivery_identity: NativeOutgoingDeliveryIdentity::for_delivery(
                            &link.identity,
                            id
                        ),
                        delivery_tag: tag.into(),
                        outcome: None,
                        receiver_settled: false,
                        retirement: None,
                    }
                )
                .is_none()
        );
        result
    }

    fn pending(&mut self, handle: u32, id: u32) -> AckIdentity {
        let tag = tag(handle, id);
        let link = self.link_mut(handle);
        assert!(link.outstanding_tags.insert(tag.clone()));
        let token = AckIdentity::new(&link.identity, id, &tag);
        assert!(
            link.pending_acknowledgements
                .insert(id, token.clone())
                .is_none()
        );
        token
    }

    fn active_presettled(
        &mut self,
        handle: u32,
        id: u32,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        let tag = tag(handle, id);
        let content_lease = self
            .writer
            .content_budget()
            .try_reserve(1024)
            .expect("active content");
        let link = self.link_mut(handle);
        assert!(link.outstanding_tags.insert(tag.clone()));
        assert!(link.active.is_none());
        link.active = Some(ActiveSend {
            payload: vec![1; 1024],
            content_lease,
            offset: 1,
            first_frame_sent: true,
            delivery_id: id,
            delivery_identity: NativeOutgoingDeliveryIdentity::for_delivery(&link.identity, id),
            delivery_tag: tag.into(),
            message_format: 0,
            settled: true,
            settled_reply: Some(reply.into()),
        });
        result
    }

    async fn update(&mut self, id: u32, settled: bool) {
        apply_disposition(
            CHANNEL,
            Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            },
            &mut self.writer,
            &mut self.sessions,
        )
        .await
        .expect("receiver outcome");
    }

    async fn settle_as(
        &mut self,
        handle: u32,
        owner: &LinkIdentity,
        token: &AckIdentity,
    ) -> Result<(), EngineError> {
        let (reply, result) = oneshot::channel();
        settle_outgoing(
            CHANNEL,
            handle,
            owner.clone(),
            Some(token.clone()),
            DeliveryState::Accepted(Accepted),
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("settlement command");
        result.await.expect("settlement result")
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.output.0.lock().expect("captured bytes").clone();
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame = read_frame(&mut input).await.expect("complete frame");
            assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }
}

fn tag(handle: u32, id: u32) -> Vec<u8> {
    [handle.to_be_bytes(), id.to_be_bytes()].concat()
}

fn first_id(frame: &Frame) -> u32 {
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = frame
    else {
        panic!("Transfer")
    };
    transfer.delivery_id.expect("first delivery ID")
}

#[test]
fn free_cursor_is_selected_without_reserving_or_advancing_it() {
    let mut fixture = Fixture::new();
    for id in [0, 255, 256, u32::MAX] {
        fixture.session_mut().next_delivery_id = id;
        let flow = fixture.session().flow.snapshot();
        assert_eq!(vacant_delivery_id(fixture.session()), Some(id));
        assert_eq!(vacant_delivery_id(fixture.session()), Some(id));
        assert_eq!(fixture.session().next_delivery_id, id);
        assert_eq!(fixture.session().flow.snapshot(), flow);
        assert!(fixture.link(HANDLE).outstanding_tags.is_empty());
    }
}

#[tokio::test]
async fn wrapping_allocation_skips_unsettled_manual_ack_and_active_presettled_ids() {
    let mut fixture = Fixture::new();
    fixture.add_link(HANDLE + 1);
    fixture.add_link(HANDLE + 2);
    let mut old = fixture.unsettled(HANDLE, u32::MAX);
    let token = fixture.pending(HANDLE + 1, 0);
    let mut active = fixture.active_presettled(HANDLE + 2, 1);
    fixture.session_mut().next_delivery_id = u32::MAX;
    let mut fresh = fixture
        .queue(HANDLE, Message::data(vec![2]), b"fresh")
        .await;
    assert_eq!(vacant_delivery_id(fixture.session()), Some(2));
    assert!(can_pump(fixture.session(), fixture.link(HANDLE)));
    fixture.fragment(HANDLE).await;
    assert_eq!(first_id(&fixture.frames().await[0]), 2);
    assert_eq!(fixture.session().next_delivery_id, 3);
    assert!(fixture.link(HANDLE).unsettled.contains_key(&u32::MAX));
    assert!(fixture.link(HANDLE).unsettled.contains_key(&2));
    assert!(fixture.link(HANDLE + 1).pending_acknowledgements[&0].same_ack(&token));
    assert!(!token.is_settled());
    assert_eq!(
        fixture
            .link(HANDLE + 2)
            .active
            .as_ref()
            .expect("old active send")
            .delivery_id,
        1
    );
    for result in [&mut old, &mut active, &mut fresh] {
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    fixture.update(2, false).await;
    let fresh_token = fresh
        .await
        .expect("fresh reply")
        .expect("fresh outcome")
        .acknowledgement
        .expect("fresh ACK");
    let owner = fixture.link(HANDLE).identity.clone();
    fixture
        .settle_as(HANDLE, &owner, &fresh_token)
        .await
        .expect("fresh ACK flush");
    assert!(
        fixture
            .link(HANDLE)
            .outstanding_tags
            .contains(tag(HANDLE, u32::MAX).as_slice())
    );
    assert!(!token.is_settled());
    assert!(matches!(
        old.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn each_live_alias_kind_blocks_reuse_on_the_same_or_a_different_link() {
    for same_link in [false, true] {
        for kind in 0..3 {
            let mut fixture = Fixture::new();
            fixture.add_link(HANDLE + 1);
            let owner_handle = if same_link { HANDLE } else { HANDLE + 1 };
            let old_reply = match kind {
                0 => Some(fixture.unsettled(owner_handle, 0)),
                1 => {
                    fixture.pending(owner_handle, 0);
                    None
                }
                2 => Some(fixture.active_presettled(owner_handle, 0)),
                _ => unreachable!(),
            };
            if same_link && kind == 2 {
                assert_eq!(vacant_delivery_id(fixture.session()), Some(1));
                assert_eq!(
                    fixture
                        .link(HANDLE)
                        .active
                        .as_ref()
                        .expect("active")
                        .delivery_id,
                    0
                );
                continue;
            }
            drop(fixture.queue(HANDLE, Message::data(vec![3]), b"new").await);
            fixture.fragment(HANDLE).await;
            assert_eq!(first_id(&fixture.frames().await[0]), 1);
            assert!(delivery_id_in_use(fixture.session(), 0));
            assert!(delivery_id_in_use(fixture.session(), 1));
            if let Some(mut reply) = old_reply {
                assert!(matches!(
                    reply.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
            }
        }
    }
}

#[tokio::test]
async fn continuations_keep_their_captured_id_while_another_link_skips_it() {
    let mut fixture = Fixture::new();
    fixture.add_link(HANDLE + 1);
    fixture.session_mut().next_delivery_id = u32::MAX;
    let message = Message::data(vec![7; 2048]);
    drop(fixture.queue(HANDLE, message.clone(), b"fragmented").await);
    fixture.fragment(HANDLE).await;
    assert_eq!(
        fixture
            .link(HANDLE)
            .active
            .as_ref()
            .expect("active")
            .delivery_id,
        u32::MAX
    );
    fixture.session_mut().next_delivery_id = u32::MAX;
    drop(
        fixture
            .queue(HANDLE + 1, Message::data(vec![8]), b"other")
            .await,
    );
    fixture.fragment(HANDLE + 1).await;
    assert_eq!(fixture.session().next_delivery_id, 1);
    while fixture.link(HANDLE).active.is_some() {
        fixture.fragment(HANDLE).await;
    }
    let frames = fixture.frames().await;
    assert_eq!(first_id(&frames[0]), u32::MAX);
    assert_eq!(first_id(&frames[1]), 0);
    let mut bytes = Vec::new();
    let mut continuations = 0;
    for frame in &frames {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            payload,
            ..
        } = frame
        else {
            panic!("Transfer")
        };
        if transfer.handle != HANDLE {
            continue;
        }
        if continuations != 0 {
            assert!(transfer.delivery_id.is_none());
            assert!(transfer.delivery_tag.is_none());
        }
        continuations += 1;
        bytes.extend_from_slice(payload);
    }
    assert!(continuations > 1);
    assert_eq!(decode_message(&bytes).expect("fragmented message"), message);
    assert_eq!(fixture.session().next_delivery_id, 1);
    assert_eq!(
        fixture.session().flow.snapshot().next_outgoing_id as usize,
        frames.len()
    );
    assert_eq!(fixture.link(HANDLE).credit.snapshot().delivery_count, 1);
    assert_eq!(fixture.link(HANDLE + 1).credit.snapshot().delivery_count, 1);
}

#[tokio::test]
async fn opposite_direction_and_other_session_ids_do_not_reserve_outgoing_aliases() {
    let mut fixture = Fixture::new();
    let incoming_owner = LinkIdentity::new();
    let incoming = fixture
        .session_mut()
        .incoming
        .reserve(&incoming_owner, 0, b"incoming")
        .expect("incoming reservation");
    let mut other = SessionState::new(&Begin::default());
    let mut link = sending();
    let other_token = AckIdentity::new(&link.identity, 0, b"other-session");
    link.outstanding_tags.insert(b"other-session".to_vec());
    link.pending_acknowledgements.insert(0, other_token.clone());
    other
        .links
        .insert(HANDLE, LinkState::Sending(Box::new(link)));
    fixture.sessions.insert(CHANNEL + 1, other);
    assert_eq!(vacant_delivery_id(fixture.session()), Some(0));
    drop(
        fixture
            .queue(HANDLE, Message::data(vec![1]), b"outgoing")
            .await,
    );
    fixture.fragment(HANDLE).await;
    assert_eq!(first_id(&fixture.frames().await[0]), 0);
    assert_eq!(
        fixture
            .session_mut()
            .incoming
            .complete(&incoming, false, ReceiverSettleMode::Second)
            .expect("incoming delivery remains live"),
        Completion::Unsettled
    );
    assert!(matches!(
        fixture
            .session()
            .incoming
            .settlement(&incoming_owner, &incoming),
        Ok(SettlementAction::SendDisposition { settled: false })
    ));
    assert!(!other_token.is_settled());
}

#[tokio::test]
async fn no_link_or_session_allowance_preserves_cursor_credit_and_every_alias() {
    for no_link_credit in [false, true] {
        let mut fixture = Fixture::new();
        fixture.add_link(HANDLE + 1);
        let token = fixture.pending(HANDLE + 1, 0);
        let mut result = fixture
            .queue(HANDLE, Message::data(vec![1]), b"waiting")
            .await;
        if no_link_credit {
            fixture.link_mut(HANDLE).credit = LinkCredit::new(0);
        } else {
            fixture.session_mut().flow =
                SessionWindow::new(0, 0, 0, SESSION_WINDOW, SESSION_WINDOW);
        }
        let flow = fixture.session().flow.snapshot();
        let credit = fixture.link(HANDLE).credit.snapshot();
        let tags = fixture.link(HANDLE).outstanding_tags.clone();
        assert!(!can_pump(fixture.session(), fixture.link(HANDLE)));
        fixture.fragment(HANDLE).await;
        assert!(fixture.frames().await.is_empty());
        assert_eq!(fixture.session().next_delivery_id, 0);
        assert_eq!(fixture.session().flow.snapshot(), flow);
        assert_eq!(fixture.link(HANDLE).credit.snapshot(), credit);
        assert_eq!(fixture.link(HANDLE).outstanding_tags, tags);
        assert_eq!(fixture.link(HANDLE).queued.len(), 1);
        assert!(fixture.link(HANDLE + 1).pending_acknowledgements[&0].same_ack(&token));
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        fixture
            .link_mut(HANDLE)
            .credit
            .update_peer(Some(0), 1, false)
            .expect("credit restored");
        fixture
            .session_mut()
            .flow
            .update_peer(Some(0), 1, 0, SESSION_WINDOW)
            .expect("session window restored");
        fixture.fragment(HANDLE).await;
        assert_eq!(first_id(&fixture.frames().await[0]), 1);
        assert_eq!(fixture.session().next_delivery_id, 2);
    }
}

#[tokio::test]
async fn queued_message_uses_selected_wider_id_and_still_obeys_the_frame_cap() {
    let mut fixture = Fixture::new();
    fixture.session_mut().next_delivery_id = 255;
    let tag = [17; 32];
    let message = (0..512)
        .map(|length| Message::data(vec![5; length]))
        .find(|message| {
            let payload = encode_message(message).expect("message encoding");
            let (_, _, small_complete) = fragment_frame(
                CHANNEL,
                HANDLE,
                255,
                &tag.to_vec().into(),
                0,
                false,
                &payload,
                0,
                false,
                &fixture.writer,
            )
            .expect("small ID frame");
            let (_, _, wide_complete) = fragment_frame(
                CHANNEL,
                HANDLE,
                256,
                &tag.to_vec().into(),
                0,
                false,
                &payload,
                0,
                false,
                &fixture.writer,
            )
            .expect("wide ID frame");
            small_complete && !wide_complete
        })
        .expect("payload across the integer-width boundary");
    drop(fixture.queue(HANDLE, message.clone(), &tag).await);
    fixture.add_link(HANDLE + 1);
    let token = fixture.pending(HANDLE + 1, 255);
    fixture.finish(HANDLE).await;
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 2);
    assert_eq!(first_id(&frames[0]), 256);
    let mut bytes = Vec::new();
    for frame in &frames {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(_)),
            payload,
            ..
        } = frame
        else {
            panic!("Transfer")
        };
        bytes.extend_from_slice(payload);
    }
    assert_eq!(decode_message(&bytes).expect("message"), message);
    assert_eq!(fixture.session().next_delivery_id, 257);
    assert!(!token.is_settled());
}

#[test]
fn maximum_live_id_set_is_skipped_across_wrap_without_mutation() {
    let mut fixture = Fixture::new();
    let start = u32::MAX - 5;
    fixture.session_mut().next_delivery_id = start;
    for index in 0..4u32 {
        let handle = HANDLE + index;
        if index != 0 {
            fixture.add_link(handle);
        }
        for offset in 0..MAX_OUTGOING_DELIVERIES_PER_LINK as u32 {
            fixture.pending(
                handle,
                start.wrapping_add(index * MAX_OUTGOING_DELIVERIES_PER_LINK as u32 + offset),
            );
        }
    }
    assert_eq!(
        vacant_delivery_id(fixture.session()),
        Some(start.wrapping_add(MAX_OUTGOING_DELIVERIES_PER_SESSION as u32))
    );
    assert_eq!(fixture.session().next_delivery_id, start);
    assert!(fixture.output.0.lock().expect("captured bytes").is_empty());
}

#[tokio::test]
async fn defensive_overcapacity_scan_does_not_advance_or_overwrite_an_alias() {
    let mut fixture = Fixture::new();
    for id in 0..=MAX_OUTGOING_DELIVERIES_PER_SESSION as u32 {
        fixture.pending(HANDLE, id);
    }
    // Seed the impossible overcapacity state directly to exercise the bounded fallback.
    let (reply, mut result) = oneshot::channel();
    let payload = encode_message(&Message::data(vec![1])).expect("payload");
    let content_lease = fixture
        .writer
        .content_budget()
        .try_reserve(payload.len())
        .expect("queued content");
    let link = fixture.link_mut(HANDLE);
    link.outstanding_tags.insert(b"queued".to_vec());
    link.queued.push_back(QueuedSend {
        payload,
        content_lease,
        delivery_tag: b"queued".to_vec().into(),
        message_format: 0,
        reply: reply.into(),
    });
    let flow = fixture.session().flow.snapshot();
    let credit = fixture.link(HANDLE).credit.snapshot();
    assert_eq!(vacant_delivery_id(fixture.session()), None);
    assert!(!can_pump(fixture.session(), fixture.link(HANDLE)));
    fixture.fragment(HANDLE).await;
    assert_eq!(fixture.session().next_delivery_id, 0);
    assert_eq!(fixture.session().flow.snapshot(), flow);
    assert_eq!(fixture.link(HANDLE).credit.snapshot(), credit);
    assert_eq!(
        fixture.link(HANDLE).pending_acknowledgements.len(),
        MAX_OUTGOING_DELIVERIES_PER_SESSION + 1
    );
    assert_eq!(fixture.link(HANDLE).queued.len(), 1);
    assert!(fixture.frames().await.is_empty());
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn teardown_releases_numeric_alias_but_old_owner_cannot_settle_its_replacement() {
    let mut fixture = Fixture::new();
    let old = fixture.pending(HANDLE, 0);
    let old_owner = fixture.link(HANDLE).identity.clone();
    stop_link(fixture.session_mut().links.get_mut(&HANDLE).expect("link"));
    fixture
        .session_mut()
        .links
        .insert(HANDLE, LinkState::Sending(Box::new(sending())));
    assert_eq!(vacant_delivery_id(fixture.session()), Some(0));
    let result = fixture
        .queue(HANDLE, Message::data(vec![1]), b"replacement")
        .await;
    fixture.fragment(HANDLE).await;
    assert_eq!(first_id(&fixture.frames().await[0]), 0);
    fixture.update(0, false).await;
    let fresh = result
        .await
        .expect("send reply")
        .expect("outcome")
        .acknowledgement
        .expect("fresh ACK");
    let bytes = fixture.output.0.lock().expect("captured bytes").len();
    assert!(matches!(
        fixture.settle_as(HANDLE, &old_owner, &old).await,
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(
        fixture.output.0.lock().expect("captured bytes").len(),
        bytes
    );
    assert!(fixture.link(HANDLE).pending_acknowledgements[&0].same_ack(&fresh));
    assert!(!fresh.is_settled());
    assert!(
        fixture
            .link(HANDLE)
            .outstanding_tags
            .contains(b"replacement".as_slice())
    );
    let owner = fixture.link(HANDLE).identity.clone();
    fixture
        .settle_as(HANDLE, &owner, &fresh)
        .await
        .expect("fresh ACK");
    assert!(!delivery_id_in_use(fixture.session(), 0));
}
