use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use tokio::time::timeout;

use super::content_budget::ContentBudget;
use super::link_handles::{HandleAlias, is_error_detached, mark_error_detached};
use super::*;
use crate::{Source, Target};

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    fail_flush: AtomicBool,
    block_flush: AtomicBool,
}

struct Writer(Arc<Output>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .bytes
            .lock()
            .expect("output")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.fail_flush.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "error detach flush",
            )))
        } else if self.0.block_flush.load(Ordering::Acquire) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    budget: ContentBudget,
    output: Arc<Output>,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new() -> Self {
        let output = Arc::new(Output::default());
        let budget = ContentBudget::new(256);
        let mut sessions = HashMap::new();
        for channel in [0, 1] {
            let mut session = SessionState::new(&Begin::default());
            session.peer_channel = Some(17 + channel);
            session.local_begin_sent = true;
            sessions.insert(channel, session);
        }
        Self {
            sessions,
            writer: FrameWriter::new_with_content_budget(
                Writer(output.clone()),
                512,
                budget.clone(),
            )
            .expect("frame writer"),
            budget,
            output,
        }
    }

    fn link(
        &mut self,
        channel: u16,
        handle: u32,
        peer_handle: u32,
        role: Role,
    ) -> (
        LinkIdentity,
        mpsc::Receiver<Delivery>,
        watch::Receiver<bool>,
    ) {
        let identity = LinkIdentity::new();
        let (deliveries, inbox) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached, observed) = watch::channel(false);
        let link = if role == Role::Sender {
            LinkState::Sending(Box::new(SendingLink {
                identity: identity.clone(),
                auto_acknowledge: true,
                max_message_size: None,
                receiver_settle_mode: ReceiverSettleMode::First,
                default_outcome: None,
                outstanding_tags: HashSet::new(),
                settle_mode: SenderSettleMode::Mixed,
                credit: LinkCredit::new(0),
                reservations: Default::default(),
                queued: VecDeque::new(),
                active: None,
                unsettled: HashMap::new(),
                pending_acknowledgements: HashMap::new(),
                detached,
            }))
        } else {
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            LinkState::Receiving(Box::new(ReceivingLink {
                max_message_size: u64::MAX,
                deliveries: deliveries.into(),
                partial: None,
                detached,
                credit,
                decoders: MessageFormatDecoders::default(),
                identity: identity.clone(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
            }))
        };
        let session = self.sessions.get_mut(&channel).expect("session");
        session.links.insert(handle, link);
        session.handle_aliases.insert(
            handle,
            HandleAlias {
                identity: identity.clone(),
                name: format!("closing-{channel}-{handle}").into(),
                role,
                peer_handle: Some(peer_handle),
                own_attach_sent: true,
                error_detached: false,
            },
        );
        (identity, inbox, observed)
    }

    fn pending(&mut self, peer_role: Role) -> IncomingAttach {
        let session = self.sessions.get_mut(&0).expect("session");
        let receipt = IncomingAttach::new(request(peer_role), session.identity.clone(), 0);
        session.handle_aliases.insert(
            0,
            HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(42),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        session
            .pending_attaches
            .insert(0, PendingLinkFlow::incoming(&receipt));
        receipt
    }

    async fn error_detach(&mut self) {
        detach_link_error(
            0,
            0,
            self.sessions.get_mut(&0).expect("session"),
            &mut self.writer,
            "amqp:invalid-field",
            "injected link refusal",
        )
        .await
        .expect("bounded error detach");
        self.assert_detach("amqp:invalid-field").await;
        assert!(is_error_detached(&self.sessions[&0], 0));
    }

    async fn input(&mut self, channel: u16, performative: Performative, payload: Vec<u8>) {
        let (incoming, _) = mpsc::channel(1);
        assert!(matches!(
            handle_frame(
                Frame::Amqp {
                    channel: 17 + channel,
                    performative: Some(performative),
                    payload
                },
                &mut self.writer,
                &incoming,
                &mut self.sessions,
                512,
                u16::MAX,
                false,
            )
            .await
            .expect("mapped session traffic"),
            FrameAction::Continue
        ));
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.bytes.lock().expect("output"));
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame = read_frame(&mut input).await.expect("captured frame");
            assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }

    async fn assert_detach(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Detach(detach)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one error detach: {frames:?}");
        };
        assert_eq!(detach.handle, 0);
        assert!(detach.closed && payload.is_empty());
        assert_eq!(
            detach
                .error
                .as_ref()
                .expect("detach condition")
                .condition
                .as_symbol(),
            Symbol::from(condition)
        );
    }

    async fn assert_end(&self) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::End(end)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one errant-link End: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(
            end.error
                .as_ref()
                .expect("End condition")
                .condition
                .as_symbol(),
            Symbol::from("amqp:session:errant-link")
        );
    }

    async fn assert_sibling_live(
        &mut self,
        inbox: &mut mpsc::Receiver<Delivery>,
        owner: &LinkIdentity,
    ) {
        let message = Message::data(b"healthy sibling".to_vec());
        self.input(
            1,
            Performative::Transfer(transfer(77, Some(91), false)),
            encode_message(&message).expect("message"),
        )
        .await;
        let delivery = inbox.try_recv().expect("sibling delivery");
        assert_eq!(delivery.message, message);
        assert!(delivery.identity.belongs_to(owner));
        drop(delivery);
        assert!(!self.sessions[&1].ending && !owner.is_retired());
        assert!(self.frames().await.is_empty());
        assert_eq!(self.budget.retained_bytes(), 0);
    }
}

fn request(role: Role) -> Attach {
    Attach {
        name: "error-pending".into(),
        handle: 42,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn flow(handle: u32) -> Flow {
    Flow {
        handle: Some(handle),
        next_incoming_id: Some(0),
        incoming_window: 0,
        outgoing_window: 0,
        delivery_count: Some(0),
        link_credit: Some(u32::MAX),
        drain: true,
        echo: true,
        ..Flow::default()
    }
}

fn transfer(handle: u32, id: Option<u32>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: id,
        delivery_tag: id.map(|id| id.to_be_bytes().to_vec().into()),
        message_format: id.map(|_| 0),
        settled: Some(true),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

#[test]
fn marker_requires_the_exact_current_closing_generation() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let (original, _, _) = fixture.link(0, 0, 42, role.clone());
        let session = fixture.sessions.get_mut(&0).expect("session");
        assert!(!mark_error_detached(session, 0, &original));
        assert!(!session.handle_aliases[&0].error_detached);
        remember_closing_handle(session, 0).expect("closing slot");
        assert!(!is_error_detached(session, 0));
        assert!(!mark_error_detached(session, 0, &LinkIdentity::new()));
        assert!(mark_error_detached(session, 0, &original));
        assert!(is_error_detached(session, 0));
        let mut previous = session.links.remove(&0).expect("original link");
        stop_link(&mut previous);
        assert!(original.is_retired());
        assert!(
            is_error_detached(session, 0),
            "a retired retained alias still identifies its peer"
        );
        assert_eq!(local_handle_for_peer(42, session), Some(0));
        let retained = session.handle_aliases.remove(&0).expect("retained alias");
        let (replacement, _, _) = fixture.link(0, 0, 42, role);
        let session = fixture.sessions.get_mut(&0).expect("session");
        session.handle_aliases.insert(0, retained);
        assert!(!is_error_detached(session, 0));
        assert!(!mark_error_detached(session, 0, &original));
        assert!(!mark_error_detached(session, 0, &replacement));
        assert_eq!(local_handle_for_peer(42, session), None);
        session.links.remove(&0);
        let foreign = IncomingAttach::new(request(Role::Sender), session.identity.clone(), 0);
        session
            .pending_attaches
            .insert(0, PendingLinkFlow::incoming(&foreign));
        assert!(!is_error_detached(session, 0));
        assert!(!mark_error_detached(session, 0, &original));
        session.pending_attaches.clear();
        assert!(is_error_detached(session, 0));
        session.closing_handles.clear();
        assert!(!is_error_detached(session, 0));
        assert!(!mark_error_detached(session, 0, &original));
    }
}

#[tokio::test]
async fn mapped_error_closing_flow_and_transfer_end_only_the_affected_session_before_accounting() {
    for role in [Role::Sender, Role::Receiver] {
        for incoming_flow in [false, true] {
            let mut fixture = Fixture::new();
            let (affected, _, detached) = fixture.link(0, 0, 42, role.clone());
            let (same_session, _, _) = fixture.link(0, 1, 43, Role::Receiver);
            let (sibling, mut inbox, _) = fixture.link(1, 0, 77, Role::Receiver);
            fixture.error_detach().await;
            assert!(affected.is_retired() && *detached.borrow());
            let before = fixture.sessions[&0].flow.clone();
            let allowance = before.outgoing_allowance();
            if incoming_flow {
                fixture
                    .input(0, Performative::Flow(flow(42)), Vec::new())
                    .await;
            } else {
                fixture
                    .input(
                        0,
                        Performative::Transfer(transfer(42, Some(7), true)),
                        vec![9; 257],
                    )
                    .await;
            }
            fixture.assert_end().await;
            assert_eq!(fixture.sessions[&0].flow, before);
            assert_eq!(fixture.sessions[&0].flow.outgoing_allowance(), allowance);
            assert_eq!(fixture.sessions[&0].next_delivery_id, 0);
            assert_eq!(fixture.budget.retained_bytes(), 0);
            assert!(fixture.sessions[&0].ending && same_session.is_retired());
            fixture.assert_sibling_live(&mut inbox, &sibling).await;
        }
    }
}

#[tokio::test]
async fn direct_helpers_reject_error_closing_links_before_window_or_content_mutation() {
    for direct in 0..3 {
        let mut fixture = Fixture::new();
        fixture.link(0, 0, 42, Role::Receiver);
        fixture.error_detach().await;
        let before = fixture.sessions[&0].flow.clone();
        match direct {
            0 => apply_flow(0, flow(0), &mut fixture.writer, &mut fixture.sessions, 512).await,
            1 => apply_link_flow(0, flow(0), &mut fixture.writer, &mut fixture.sessions).await,
            _ => {
                receive_transfer(
                    0,
                    transfer(0, Some(7), true),
                    vec![0; 257],
                    &mut fixture.sessions,
                    &mut fixture.writer,
                )
                .await
            }
        }
        .expect("direct errant-link refusal");
        fixture.assert_end().await;
        assert_eq!(fixture.sessions[&0].flow, before);
        assert_eq!(fixture.budget.retained_bytes(), 0);
        assert!(fixture.sessions[&0].ending);
    }
}

#[tokio::test]
async fn size_refusal_then_in_flight_continuation_does_not_spend_another_frame_or_content_lease() {
    let mut fixture = Fixture::new();
    let (affected, mut inbox, detached) = fixture.link(0, 0, 42, Role::Receiver);
    let LinkState::Receiving(link) = fixture
        .sessions
        .get_mut(&0)
        .expect("session")
        .links
        .get_mut(&0)
        .expect("receiver")
    else {
        panic!("receiving link");
    };
    link.max_message_size = 8;
    fixture
        .input(
            0,
            Performative::Transfer(transfer(42, Some(7), true)),
            vec![0; 4],
        )
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 4);
    fixture
        .input(
            0,
            Performative::Transfer(transfer(42, None, false)),
            vec![0; 5],
        )
        .await;
    fixture
        .assert_detach("amqp:link:message-size-exceeded")
        .await;
    assert!(is_error_detached(&fixture.sessions[&0], 0));
    assert!(affected.is_retired() && *detached.borrow());
    assert!(inbox.recv().await.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let before = fixture.sessions[&0].flow.clone();
    fixture
        .input(
            0,
            Performative::Transfer(transfer(42, None, false)),
            vec![0; 257],
        )
        .await;
    fixture.assert_end().await;
    assert_eq!(fixture.sessions[&0].flow, before);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn ordinary_closing_crossings_remain_discarded_and_acknowledgements_release_both_kinds() {
    for error in [false, true] {
        let mut fixture = Fixture::new();
        let (owner, _, _) = fixture.link(0, 0, 42, Role::Receiver);
        let (reply, result) = oneshot::channel();
        handle_command(
            Command::Detach {
                channel: 0,
                handle: 0,
                identity: owner.clone(),
                error: error.then(|| {
                    Error::new(
                        crate::AmqpError::NotImplemented,
                        "explicit endpoint refusal",
                        None,
                    )
                }),
                reply,
            },
            &mut fixture.writer,
            &mut fixture.sessions,
            512,
        )
        .await
        .expect("endpoint detach");
        result
            .await
            .expect("detach reply")
            .expect("detach committed");
        if error {
            fixture.assert_detach("amqp:not-implemented").await;
            assert!(is_error_detached(&fixture.sessions[&0], 0));
        } else {
            let frames = fixture.frames().await;
            assert!(matches!(
                frames.as_slice(),
                [Frame::Amqp {
                    performative: Some(Performative::Detach(Detach {
                        handle: 0,
                        error: None,
                        ..
                    })),
                    ..
                }]
            ));
            assert!(!is_error_detached(&fixture.sessions[&0], 0));
            let incoming = fixture.sessions[&0].flow.snapshot().next_incoming_id;
            fixture
                .input(0, Performative::Flow(flow(42)), Vec::new())
                .await;
            assert_eq!(fixture.sessions[&0].flow.outgoing_allowance(), 0);
            fixture
                .input(
                    0,
                    Performative::Transfer(transfer(42, None, false)),
                    vec![0; 257],
                )
                .await;
            assert_eq!(
                fixture.sessions[&0].flow.snapshot().next_incoming_id,
                incoming + 1
            );
            assert!(!fixture.sessions[&0].ending);
            assert!(fixture.frames().await.is_empty());
            assert_eq!(fixture.budget.retained_bytes(), 0);
        }
        fixture
            .input(
                0,
                Performative::Detach(Detach {
                    handle: 42,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await;
        assert!(fixture.frames().await.is_empty());
        assert!(!fixture.sessions[&0].closing_handles.contains(&0));
        assert!(!fixture.sessions[&0].handle_aliases.contains_key(&0));
        let (replacement, _, _) = fixture.link(0, 0, 42, Role::Receiver);
        assert!(!replacement.same_link(&owner));
        assert!(!is_error_detached(&fixture.sessions[&0], 0));
        assert!(!mark_error_detached(
            fixture.sessions.get_mut(&0).expect("session"),
            0,
            &owner
        ));
    }
}

#[tokio::test]
async fn error_detach_preflight_preserves_the_installed_owner_and_unmarked_alias() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let (owner, _, detached) = fixture.link(0, 0, 42, role);
        let result = detach_link_error(
            0,
            0,
            fixture.sessions.get_mut(&0).expect("session"),
            &mut fixture.writer,
            "amqp:invalid-field",
            "x".repeat(2048),
        )
        .await;
        let Err(EngineError::Io(error)) = result else {
            panic!("oversize error detach is a typed preflight failure");
        };
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<super::frame_writer::FrameWriteError>())
                .is_some()
        );
        assert!(!owner.is_retired() && !*detached.borrow());
        assert!(fixture.sessions[&0].links[&0].identity().same_link(&owner));
        assert!(!fixture.sessions[&0].closing_handles.contains(&0));
        assert!(!fixture.sessions[&0].handle_aliases[&0].error_detached);
        assert!(fixture.frames().await.is_empty());
    }
}

#[tokio::test]
async fn failed_or_cancelled_error_detach_flush_keeps_the_exact_marker_and_reserved_alias() {
    for role in [Role::Sender, Role::Receiver] {
        for failed in [false, true] {
            let mut fixture = Fixture::new();
            let (owner, _, detached) = fixture.link(0, 0, 42, role.clone());
            fixture.output.fail_flush.store(failed, Ordering::Release);
            fixture.output.block_flush.store(!failed, Ordering::Release);
            let detaching = detach_link_error(
                0,
                0,
                fixture.sessions.get_mut(&0).expect("session"),
                &mut fixture.writer,
                "amqp:invalid-field",
                "uncertain error detach",
            );
            if failed {
                assert!(matches!(detaching.await, Err(EngineError::Io(_))));
            } else {
                assert!(timeout(Duration::from_millis(10), detaching).await.is_err());
            }
            assert!(owner.is_retired() && *detached.borrow());
            assert!(!fixture.sessions[&0].links.contains_key(&0));
            assert!(is_error_detached(&fixture.sessions[&0], 0));
            assert!(
                fixture.sessions[&0].handle_aliases[&0]
                    .identity
                    .same_link(&owner)
            );
            assert_eq!(local_handle_for_peer(42, &fixture.sessions[&0]), Some(0));
            assert_eq!(fixture.budget.retained_bytes(), 0);
            fixture.assert_detach("amqp:invalid-field").await;
        }
    }
}

#[tokio::test]
async fn pending_refusal_publishes_own_attach_then_marks_the_exact_alias_before_errant_traffic() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let pending = fixture.pending(role.clone());
        close_pending_link(
            0,
            0,
            pending.approval(),
            fixture.sessions.get_mut(&0).expect("session"),
            &mut fixture.writer,
            Some(Error::new(
                crate::AmqpError::NotImplemented,
                "pending recovery refusal",
                None,
            )),
            false,
        )
        .await
        .expect("pending error refusal");
        let frames = fixture.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Attach(attach)),
                ..
            },
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Detach(detach)),
                ..
            },
        ] = frames.as_slice()
        else {
            panic!("own Attach precedes pending error Detach: {frames:?}");
        };
        assert_eq!(attach.handle, 0);
        assert_ne!(attach.role, role);
        assert_eq!(detach.handle, 0);
        assert!(detach.error.is_some());
        assert!(fixture.sessions[&0].handle_aliases[&0].own_attach_sent);
        assert!(is_error_detached(&fixture.sessions[&0], 0));
        let before = fixture.sessions[&0].flow.clone();
        fixture
            .input(0, Performative::Flow(flow(42)), Vec::new())
            .await;
        fixture.assert_end().await;
        assert_eq!(fixture.sessions[&0].flow, before);
        assert!(pending.approval().link_identity().is_retired());
    }
}

#[tokio::test]
async fn pending_error_detach_preflight_does_not_publish_or_mark_a_refusal() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let pending = fixture.pending(role);
        let result = close_pending_link(
            0,
            0,
            pending.approval(),
            fixture.sessions.get_mut(&0).expect("session"),
            &mut fixture.writer,
            Some(Error::new(
                crate::AmqpError::NotImplemented,
                "x".repeat(2048),
                None,
            )),
            false,
        )
        .await;
        assert!(matches!(result, Err(EngineError::Io(_))));
        assert!(!pending.approval().link_identity().is_retired());
        assert!(!fixture.sessions[&0].closing_handles.contains(&0));
        assert!(!fixture.sessions[&0].handle_aliases[&0].own_attach_sent);
        assert!(!fixture.sessions[&0].handle_aliases[&0].error_detached);
        assert!(Arc::ptr_eq(
            fixture.sessions[&0].pending_attaches[&0]
                .approval
                .as_ref()
                .expect("pending approval"),
            pending.approval()
        ));
        assert!(fixture.frames().await.is_empty());
    }
}

#[tokio::test]
async fn failed_or_cancelled_pending_refusal_attach_flush_keeps_its_exact_error_marker() {
    for role in [Role::Sender, Role::Receiver] {
        for failed in [false, true] {
            let mut fixture = Fixture::new();
            let pending = fixture.pending(role.clone());
            fixture.output.fail_flush.store(failed, Ordering::Release);
            fixture.output.block_flush.store(!failed, Ordering::Release);
            let refusing = close_pending_link(
                0,
                0,
                pending.approval(),
                fixture.sessions.get_mut(&0).expect("session"),
                &mut fixture.writer,
                Some(Error::new(
                    crate::AmqpError::NotImplemented,
                    "uncertain pending refusal",
                    None,
                )),
                false,
            );
            if failed {
                assert!(matches!(refusing.await, Err(EngineError::Io(_))));
            } else {
                assert!(timeout(Duration::from_millis(10), refusing).await.is_err());
            }
            assert!(is_error_detached(&fixture.sessions[&0], 0));
            assert!(!fixture.sessions[&0].handle_aliases[&0].own_attach_sent);
            assert!(
                fixture.sessions[&0].handle_aliases[&0]
                    .identity
                    .same_link(pending.approval().link_identity())
            );
            assert_eq!(local_handle_for_peer(42, &fixture.sessions[&0]), Some(0));
            assert!(!fixture.sessions[&0].pending_attaches.contains_key(&0));
            assert!(pending.approval().link_identity().is_retired());
            assert!(fixture.sessions[&0].closing_handles.contains(&0));
            assert_eq!(link_slot_count(&fixture.sessions[&0]), 1);
            let frames = fixture.frames().await;
            assert!(
                matches!(frames.as_slice(), [Frame::Amqp { channel: 0, performative: Some(Performative::Attach(attach)), .. }] if attach.handle == 0 && attach.role != role)
            );
        }
    }
}
