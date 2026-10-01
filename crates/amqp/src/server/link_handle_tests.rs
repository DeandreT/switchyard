use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use tokio::time::timeout;

use super::link_handles::{
    HandleAlias, local_handle_for_peer, preferred_vacant_handle, vacant_handle,
};
use super::*;
use crate::{Source, Target};

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    flushes: AtomicUsize,
    stop_at: AtomicUsize,
    fail: AtomicUsize,
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
            .expect("captured bytes")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let current = self.0.flushes.load(Ordering::Acquire) + 1;
        if current == self.0.stop_at.load(Ordering::Acquire) {
            if self.0.fail.load(Ordering::Acquire) != 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected handle reply flush failure",
                )));
            }
            return Poll::Pending;
        }
        self.0.flushes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn request(peer_handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("handle-{peer_handle}"),
        handle: peer_handle,
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

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<Writer>,
    output: Arc<Output>,
    incoming: mpsc::Sender<IncomingSession>,
    attaches: mpsc::Receiver<IncomingAttach>,
}

impl Fixture {
    fn new(maximum: u32) -> Self {
        let output = Arc::new(Output::default());
        let (attach_tx, attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
        let mut session = SessionState::new(&Begin {
            handle_max: maximum,
            ..Begin::default()
        });
        session.peer_channel = Some(17);
        session.local_begin_sent = true;
        session.attach_tx = Some(attach_tx);
        let (incoming, _) = mpsc::channel(1);
        Self {
            sessions: HashMap::from([(0, session)]),
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("frame writer"),
            output,
            incoming,
            attaches,
        }
    }

    async fn input_result(
        &mut self,
        performative: Performative,
        payload: Vec<u8>,
    ) -> Result<FrameAction, EngineError> {
        handle_frame(
            Frame::Amqp {
                channel: 17,
                performative: Some(performative),
                payload,
            },
            &mut self.writer,
            &self.incoming,
            &mut self.sessions,
            512,
            u16::MAX,
            false,
        )
        .await
    }

    async fn input(&mut self, performative: Performative, payload: Vec<u8>) {
        assert!(matches!(
            self.input_result(performative, payload)
                .await
                .expect("link routing response"),
            FrameAction::Continue
        ));
    }

    async fn attach(&mut self, peer: u32, role: Role) -> IncomingAttach {
        self.input(
            Performative::Attach(Box::new(request(peer, role))),
            Vec::new(),
        )
        .await;
        self.attaches.try_recv().expect("pending link receipt")
    }

    fn seed_pending(&mut self, peer: u32, local: u32, role: Role) -> IncomingAttach {
        let session = self.sessions.get_mut(&0).expect("session");
        let receipt = IncomingAttach::new(request(peer, role), session.identity.clone(), local);
        session.handle_aliases.insert(
            local,
            HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(peer),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        session
            .pending_attaches
            .insert(local, PendingLinkFlow::incoming(&receipt));
        receipt
    }

    fn seed_sibling(&mut self) -> IncomingAttach {
        let mut session = SessionState::new(&Begin {
            handle_max: 0,
            ..Begin::default()
        });
        session.peer_channel = Some(18);
        session.local_begin_sent = true;
        let receipt = IncomingAttach::new(request(88, Role::Receiver), session.identity.clone(), 0);
        session.handle_aliases.insert(
            0,
            HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(88),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        session
            .pending_attaches
            .insert(0, PendingLinkFlow::incoming(&receipt));
        session.pending_attach_events.push_back(receipt.clone());
        assert!(self.sessions.insert(1, session).is_none());
        receipt
    }

    async fn approve(
        &mut self,
        receipt: IncomingAttach,
    ) -> (Result<(), EngineError>, mpsc::Receiver<Delivery>) {
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, _) = watch::channel(false);
        let (reply, result) = oneshot::channel();
        handle_command(
            Command::AcceptLink {
                channel: 0,
                session: self.sessions[&0].identity.clone(),
                attach: Box::new(receipt),
                max_message_size: 1024,
                properties: None,
                decoders: MessageFormatDecoders::default(),
                deliveries_tx,
                detached_tx,
                consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
                reply,
            },
            &mut self.writer,
            &mut self.sessions,
            512,
        )
        .await
        .expect("approval is local");
        (result.await.expect("approval result"), deliveries)
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.bytes.lock().expect("captured bytes"));
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame = read_frame(&mut input).await.expect("complete frame");
            assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }

    async fn assert_end(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::End(end)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one bounded session End: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(
            end.error.as_ref().expect("End error").condition.as_symbol(),
            Symbol::from(condition)
        );
    }

    async fn assert_close(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Close(close)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one bounded connection Close: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(
            close
                .error
                .as_ref()
                .expect("Close error")
                .condition
                .as_symbol(),
            Symbol::from(condition)
        );
    }
}

fn detach(peer: u32) -> Performative {
    Performative::Detach(Detach {
        handle: peer,
        closed: true,
        error: None,
    })
}

fn flow(peer: u32, credit: Option<u32>, echo: bool) -> Performative {
    Performative::Flow(Flow {
        handle: Some(peer),
        incoming_window: SESSION_WINDOW,
        outgoing_window: SESSION_WINDOW,
        delivery_count: Some(0),
        link_credit: credit,
        echo,
        ..Flow::default()
    })
}

fn transfer(peer: u32, id: u32) -> Performative {
    Performative::Transfer(Transfer {
        handle: peer,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        settled: Some(true),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    })
}

#[test]
fn local_allocator_counts_the_lifecycle_union_and_wraps_the_full_u32_range() {
    let mut session = SessionState::new(&Begin::default());
    assert_eq!(vacant_handle(0, 0, &session), Some(0));
    session.handle_aliases.insert(
        0,
        HandleAlias {
            identity: LinkIdentity::new(),
            name: Arc::from("allocator-zero"),
            role: Role::Sender,
            peer_handle: None,
            own_attach_sent: false,
            error_detached: false,
        },
    );
    session
        .pending_attaches
        .insert(0, PendingLinkFlow::new(Role::Sender, None));
    session.closing_handles.insert(0);
    assert_eq!(link_slot_count(&session), 1);
    assert_eq!(vacant_handle(0, 0, &session), None);
    assert_eq!(preferred_vacant_handle(7, 1, &session), Some(1));
    assert_eq!(preferred_vacant_handle(0, 1, &session), Some(1));
    session.closing_handles.insert(1);
    assert_eq!(vacant_handle(1, 1, &session), None);
    session.handle_aliases.insert(
        u32::MAX,
        HandleAlias {
            identity: LinkIdentity::new(),
            name: Arc::from("allocator-maximum"),
            role: Role::Sender,
            peer_handle: Some(42),
            own_attach_sent: true,
            error_detached: false,
        },
    );
    assert_eq!(vacant_handle(u32::MAX, u32::MAX, &session), Some(2));
    assert_eq!(
        preferred_vacant_handle(u32::MAX, u32::MAX, &session),
        Some(2)
    );
    session = SessionState::new(&Begin::default());
    for handle in 0..MAX_LINKS_PER_SESSION as u32 {
        session.closing_handles.insert(handle);
    }
    assert_eq!(
        vacant_handle(0, u32::MAX, &session),
        Some(MAX_LINKS_PER_SESSION as u32)
    );
}

#[test]
fn peer_alias_lookup_requires_exact_live_generation_or_retained_closing_owner() {
    let mut fixture = Fixture::new(1);
    let first = fixture.seed_pending(1, 0, Role::Sender);
    let second = fixture.seed_pending(0, 1, Role::Receiver);
    assert_eq!(local_handle_for_peer(1, &fixture.sessions[&0]), Some(0));
    assert_eq!(local_handle_for_peer(0, &fixture.sessions[&0]), Some(1));
    let session = fixture.sessions.get_mut(&0).expect("session");
    session.handle_aliases.get_mut(&0).expect("alias").identity =
        second.approval().link_identity().clone();
    assert_eq!(local_handle_for_peer(1, session), None);
    session.handle_aliases.get_mut(&0).expect("alias").identity =
        first.approval().link_identity().clone();
    session.pending_attaches.remove(&0);
    assert_eq!(local_handle_for_peer(1, session), None);
    session.closing_handles.insert(0);
    first.approval().retire();
    assert_eq!(local_handle_for_peer(1, session), Some(0));
    session
        .handle_aliases
        .get_mut(&1)
        .expect("pending alias")
        .peer_handle = None;
    assert_eq!(local_handle_for_peer(0, session), None);
}

#[tokio::test]
async fn incoming_raw_peer_handle_and_assigned_authority_are_independent_for_both_roles() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new(0);
        let receipt = fixture.attach(42, role.clone()).await;
        assert_eq!(receipt.handle, 42);
        assert_eq!(receipt.approval().local_handle(), 0);
        let mut changed = receipt.clone();
        changed.handle = 0;
        assert!(matches!(
            changed.validate_request(&fixture.sessions[&0].identity),
            Err(AttachApprovalError::ChangedHandle)
        ));
        assert_eq!(changed.approval().local_handle(), 0);
        assert!(fixture.frames().await.is_empty());
        let (result, _inbox) = fixture.approve(receipt.clone()).await;
        result.expect("valid approval");
        let frames = fixture.frames().await;
        assert!(
            matches!(&frames[0], Frame::Amqp { channel: 0, performative: Some(Performative::Attach(attach)), .. }
            if attach.handle == 0 && attach.role == role.opposite())
        );
        assert_eq!(receipt.handle, 42);
        assert!(fixture.sessions[&0].handle_aliases[&0].own_attach_sent);
        assert_eq!(local_handle_for_peer(42, &fixture.sessions[&0]), Some(0));
    }
}

#[tokio::test]
async fn stale_alias_generation_cannot_route_to_an_installed_replacement_link() {
    let mut fixture = Fixture::new(0);
    let receipt = fixture.attach(7, Role::Receiver).await;
    fixture.approve(receipt).await.0.expect("installed sender");
    fixture.frames().await;
    let session = fixture.sessions.get_mut(&0).expect("session");
    session.handle_aliases.get_mut(&0).expect("alias").identity = LinkIdentity::new();
    assert_eq!(local_handle_for_peer(7, session), None);
    fixture.input(flow(7, Some(1), false), Vec::new()).await;
    fixture.assert_end("amqp:session:unattached-handle").await;
}

#[tokio::test]
async fn crossed_receiving_aliases_route_transfers_to_local_delivery_authority() {
    let mut fixture = Fixture::new(1);
    let first = fixture.seed_pending(1, 0, Role::Sender);
    let second = fixture.seed_pending(0, 1, Role::Sender);
    let (result, mut first_inbox) = fixture.approve(first).await;
    result.expect("first receiver");
    let (result, mut second_inbox) = fixture.approve(second).await;
    result.expect("second receiver");
    fixture.frames().await;
    fixture
        .input(
            transfer(1, 0),
            encode_message(&Message::data(vec![10])).expect("message"),
        )
        .await;
    fixture
        .input(
            transfer(0, 1),
            encode_message(&Message::data(vec![20])).expect("message"),
        )
        .await;
    let first = first_inbox.try_recv().expect("first delivery");
    let second = second_inbox.try_recv().expect("second delivery");
    assert_eq!(first.identity.id(), 0);
    assert_eq!(second.identity.id(), 1);
    assert!(
        first
            .identity
            .belongs_to(fixture.sessions[&0].links[&0].identity())
    );
    assert!(
        !first
            .identity
            .belongs_to(fixture.sessions[&0].links[&1].identity())
    );
    assert!(
        second
            .identity
            .belongs_to(fixture.sessions[&0].links[&1].identity())
    );
    assert!(
        !second
            .identity
            .belongs_to(fixture.sessions[&0].links[&0].identity())
    );
    assert_eq!(first.message, Message::data(vec![10]));
    assert_eq!(second.message, Message::data(vec![20]));
    fixture.input(detach(1), Vec::new()).await;
    assert!(!fixture.sessions[&0].links.contains_key(&0));
    assert!(fixture.sessions[&0].links.contains_key(&1));
    assert!(matches!(
        fixture.frames().await.as_slice(),
        [Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Detach(Detach { handle: 0, .. })),
            ..
        }]
    ));
}

#[tokio::test]
async fn crossed_sending_aliases_route_credit_and_outgoing_transfers_to_local_handles() {
    let mut fixture = Fixture::new(1);
    let first = fixture.seed_pending(1, 0, Role::Receiver);
    let second = fixture.seed_pending(0, 1, Role::Receiver);
    let first_owner = first.approval().link_identity().clone();
    let second_owner = second.approval().link_identity().clone();
    fixture.approve(first).await.0.expect("first sender");
    fixture.approve(second).await.0.expect("second sender");
    fixture.frames().await;
    for (peer, local, owner) in [(1, 0, first_owner), (0, 1, second_owner)] {
        fixture.input(flow(peer, Some(1), true), Vec::new()).await;
        let frames = fixture.frames().await;
        assert!(matches!(frames.as_slice(), [Frame::Amqp {
            channel: 0, performative: Some(Performative::Flow(flow)), ..
        }] if flow.handle == Some(local)));
        let (reply, _result) = oneshot::channel();
        handle_command(
            Command::Send {
                channel: 0,
                handle: local,
                identity: owner,
                delivery_tag: vec![local as u8].into(),
                message: Box::new(Message::data(vec![local as u8])),
                reply,
            },
            &mut fixture.writer,
            &mut fixture.sessions,
            512,
        )
        .await
        .expect("send command");
        let mut cursor = 0;
        pump_connection(&mut fixture.writer, &mut fixture.sessions, &mut cursor)
            .await
            .expect("outgoing pump");
        assert!(matches!(fixture.frames().await.as_slice(), [Frame::Amqp {
            channel: 0, performative: Some(Performative::Transfer(transfer)), ..
        }] if transfer.handle == local));
    }
}

#[tokio::test]
async fn pending_link_echo_waits_for_own_attach_and_uses_the_assigned_handle() {
    let mut fixture = Fixture::new(0);
    let receipt = fixture.attach(7, Role::Receiver).await;
    fixture.input(flow(7, Some(3), true), Vec::new()).await;
    assert!(fixture.frames().await.is_empty());
    assert!(!fixture.sessions[&0].handle_aliases[&0].own_attach_sent);
    fixture.approve(receipt).await.0.expect("approved sender");
    assert!(matches!(fixture.frames().await.as_slice(), [
        Frame::Amqp { channel: 0, performative: Some(Performative::Attach(attach)), .. },
        Frame::Amqp { channel: 0, performative: Some(Performative::Flow(flow)), .. },
    ] if attach.handle == 0 && flow.handle == Some(0) && flow.link_credit == Some(3)));
}

#[tokio::test]
async fn pending_cancellation_publishes_own_attach_then_detach_before_exact_slot_reuse() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new(0);
        let old = fixture.attach(7, role.clone()).await;
        fixture.input(detach(7), Vec::new()).await;
        assert!(matches!(fixture.frames().await.as_slice(), [
            Frame::Amqp { channel: 0, performative: Some(Performative::Attach(attach)), .. },
            Frame::Amqp { channel: 0, performative: Some(Performative::Detach(detach)), .. },
        ] if attach.handle == 0 && attach.role == role.opposite() && attach.source.is_none() && attach.target.is_none()
            && detach.handle == 0 && detach.closed));
        assert!(old.approval().link_identity().is_retired());
        assert!(fixture.sessions[&0].handle_aliases.is_empty());
        assert!(fixture.sessions[&0].pending_attaches.is_empty());
        let fresh = fixture.attach(7, role).await;
        assert_eq!(fresh.approval().local_handle(), 0);
        assert!(
            !fresh
                .approval()
                .link_identity()
                .same_link(old.approval().link_identity())
        );
        assert!(matches!(
            fixture.approve(old).await.0,
            Err(EngineError::RemoteDetached)
        ));
        assert!(fixture.frames().await.is_empty());
        fixture.approve(fresh).await.0.expect("fresh approval");
    }
}

#[tokio::test]
async fn failed_or_cancelled_pending_detach_flush_keeps_its_exact_alias_and_slot() {
    for fail in [false, true] {
        let mut fixture = Fixture::new(0);
        let old = fixture.attach(7, Role::Receiver).await;
        fixture.output.stop_at.store(2, Ordering::Release);
        fixture
            .output
            .fail
            .store(usize::from(fail), Ordering::Release);
        let result = timeout(
            Duration::from_millis(20),
            fixture.input_result(detach(7), Vec::new()),
        )
        .await;
        if fail {
            assert!(matches!(result, Ok(Err(EngineError::Io(_)))));
        } else {
            assert!(result.is_err());
        }
        let session = &fixture.sessions[&0];
        let alias = &session.handle_aliases[&0];
        assert!(alias.identity.same_link(old.approval().link_identity()));
        assert_eq!(alias.peer_handle, Some(7));
        assert!(alias.own_attach_sent);
        assert_eq!(local_handle_for_peer(7, session), Some(0));
        assert_eq!(vacant_handle(0, 0, session), None);
    }
}

#[tokio::test]
async fn oversized_minimum_cancellation_attach_refuses_the_session_without_a_partial_reply() {
    let mut fixture = Fixture::new(0);
    let mut attach = request(7, Role::Receiver);
    attach.name = "x".repeat(2048);
    fixture
        .input(Performative::Attach(Box::new(attach)), Vec::new())
        .await;
    let receipt = fixture
        .attaches
        .try_recv()
        .expect("pending oversized-name receipt");
    fixture.input(detach(7), Vec::new()).await;
    fixture.assert_end("amqp:frame-size-too-small").await;
    assert!(fixture.sessions[&0].ending);
    assert!(receipt.approval().link_identity().is_retired());
}

#[tokio::test]
async fn peer_handle_collision_has_priority_over_the_exhausted_local_output_range() {
    for (peer, condition) in [
        (7, "amqp:session:handle-in-use"),
        (8, "amqp:resource-limit-exceeded"),
    ] {
        let mut fixture = Fixture::new(0);
        let receipt = fixture.attach(7, Role::Receiver).await;
        let sibling = fixture.seed_sibling();
        let action = fixture
            .input_result(
                Performative::Attach(Box::new(request(peer, Role::Receiver))),
                Vec::new(),
            )
            .await
            .expect("Attach refusal");
        if peer == 7 {
            assert!(matches!(action, FrameAction::CloseSent));
            fixture.assert_close(condition).await;
            assert!(fixture.sessions[&1].identity.is_retired());
            assert!(sibling.approval().link_identity().is_retired());
        } else {
            assert!(matches!(action, FrameAction::Continue));
            fixture.assert_end(condition).await;
            assert!(!fixture.sessions[&1].identity.is_retired());
            assert!(!sibling.approval().link_identity().is_retired());
        }
        assert!(fixture.attaches.try_recv().is_err());
        assert!(receipt.approval().link_identity().is_retired());
        assert!(fixture.sessions[&0].identity.is_retired());
    }
}

#[tokio::test]
async fn duplicate_pending_installed_or_closing_alias_closes_and_retires_every_owner() {
    for state in 0..3 {
        let mut fixture = Fixture::new(0);
        let receipt = fixture.attach(7, Role::Receiver).await;
        let owner = receipt.approval().link_identity().clone();
        if state > 0 {
            fixture
                .approve(receipt.clone())
                .await
                .0
                .expect("installed link");
            fixture.frames().await;
        }
        if state == 2 {
            let (reply, result) = oneshot::channel();
            handle_command(
                Command::Detach {
                    channel: 0,
                    handle: 0,
                    identity: owner.clone(),
                    error: None,
                    reply,
                },
                &mut fixture.writer,
                &mut fixture.sessions,
                512,
            )
            .await
            .expect("local Detach");
            result.await.expect("Detach reply").expect("Detach success");
            fixture.frames().await;
            assert!(fixture.sessions[&0].closing_handles.contains(&0));
            assert!(owner.is_retired());
            assert_eq!(local_handle_for_peer(7, &fixture.sessions[&0]), Some(0));
        }
        let sibling = fixture.seed_sibling();
        let sessions = [
            fixture.sessions[&0].identity.clone(),
            fixture.sessions[&1].identity.clone(),
        ];
        let mut duplicate = request(7, Role::Sender);
        duplicate.name = "different-name".to_owned();
        assert!(matches!(
            fixture
                .input_result(Performative::Attach(Box::new(duplicate)), Vec::new())
                .await
                .expect("duplicate response"),
            FrameAction::CloseSent
        ));
        fixture.assert_close("amqp:session:handle-in-use").await;
        assert!(owner.is_retired());
        assert!(sibling.approval().link_identity().is_retired());
        assert!(sessions.iter().all(SessionIdentity::is_retired));
        assert!(fixture.attaches.try_recv().is_err());
        for session in fixture.sessions.values() {
            assert!(session.links.is_empty());
            assert!(session.pending_attaches.is_empty());
            assert!(session.pending_attach_events.is_empty());
            assert!(session.handle_aliases.is_empty());
        }
    }
}

#[tokio::test]
async fn oversized_close_preflight_preserves_all_exact_owners_without_writing() {
    let mut fixture = Fixture::new(0);
    let receipt = fixture.attach(7, Role::Receiver).await;
    let sibling = fixture.seed_sibling();
    let result = refuse_connection(
        "amqp:session:handle-in-use",
        "x".repeat(2048),
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await;
    let Err(EngineError::Io(error)) = result else {
        panic!("oversized Close preflight must fail");
    };
    let limit = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<super::frame_writer::FrameWriteError>())
        .expect("typed frame limit");
    assert_eq!(limit.maximum, 512);
    assert!(limit.actual > limit.maximum);
    assert!(fixture.frames().await.is_empty());
    assert_eq!(fixture.output.flushes.load(Ordering::Acquire), 0);
    for (local, pending) in [(0, &receipt), (1, &sibling)] {
        let session = &fixture.sessions[&local];
        assert!(!session.identity.is_retired());
        assert!(!pending.approval().link_identity().is_retired());
        assert!(
            session.handle_aliases[&0]
                .identity
                .same_link(pending.approval().link_identity())
        );
        assert!(Arc::ptr_eq(
            session.pending_attaches[&0]
                .approval
                .as_ref()
                .expect("pending approval"),
            pending.approval()
        ));
    }
}

#[tokio::test]
async fn failed_or_cancelled_duplicate_close_flush_retires_all_connection_owners() {
    for fail in [false, true] {
        let mut fixture = Fixture::new(0);
        let receipt = fixture.attach(7, Role::Receiver).await;
        let sibling = fixture.seed_sibling();
        let sessions = [
            fixture.sessions[&0].identity.clone(),
            fixture.sessions[&1].identity.clone(),
        ];
        fixture.output.stop_at.store(1, Ordering::Release);
        fixture
            .output
            .fail
            .store(usize::from(fail), Ordering::Release);
        let result = timeout(
            Duration::from_millis(20),
            fixture.input_result(
                Performative::Attach(Box::new(request(7, Role::Sender))),
                Vec::new(),
            ),
        )
        .await;
        if fail {
            assert!(matches!(result, Ok(Err(EngineError::Io(_)))));
        } else {
            assert!(result.is_err());
        }
        fixture.assert_close("amqp:session:handle-in-use").await;
        assert!(sessions.iter().all(SessionIdentity::is_retired));
        assert!(receipt.approval().link_identity().is_retired());
        assert!(sibling.approval().link_identity().is_retired());
        assert!(fixture.attaches.try_recv().is_err());
        assert!(fixture.sessions.values().all(
            |session| session.handle_aliases.is_empty() && session.pending_attaches.is_empty()
        ));
    }
}
