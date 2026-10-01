use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::link_handles::connection_link_name_in_use;
use super::*;
use crate::{Source, Target};

fn request(name: &str, local_role: &Role, peer: u32) -> Attach {
    let role = local_role.opposite();
    Attach {
        name: name.into(),
        handle: peer,
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

fn endpoint(role: &Role, owner: LinkIdentity) -> LinkState {
    let (detached, _) = watch::channel(false);
    if role == &Role::Sender {
        LinkState::Sending(Box::new(SendingLink {
            identity: owner,
            auto_acknowledge: true,
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::First,
            default_outcome: None,
            outstanding_tags: HashSet::new(),
            settle_mode: SenderSettleMode::Mixed,
            credit: LinkCredit::new(0),
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashMap::new(),
            detached,
        }))
    } else {
        let (deliveries, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        LinkState::Receiving(ReceivingLink {
            identity: owner,
            max_message_size: 1024,
            deliveries,
            partial: None,
            detached,
            credit: ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            ),
            decoders: MessageFormatDecoders::default(),
            sender_settle_mode: SenderSettleMode::Mixed,
            receiver_settle_mode: ReceiverSettleMode::First,
        })
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Pending,
    Installed,
    Closing,
    Orphan,
}

fn seed(session: &mut SessionState, name: &str, role: &Role, phase: Phase) -> LinkIdentity {
    let receipt = IncomingAttach::new(request(name, role, 42), session.identity.clone(), 0);
    let owner = receipt.approval().link_identity().clone();
    session.handle_aliases.insert(
        0,
        HandleAlias {
            identity: owner.clone(),
            name: Arc::clone(receipt.approval().name()),
            role: role.clone(),
            peer_handle: Some(42),
            own_attach_sent: !matches!(phase, Phase::Pending | Phase::Orphan),
            error_detached: false,
        },
    );
    match phase {
        Phase::Pending => {
            session
                .pending_attaches
                .insert(0, PendingLinkFlow::incoming(&receipt));
        }
        Phase::Installed => {
            session.links.insert(0, endpoint(role, owner.clone()));
        }
        Phase::Closing => {
            session.closing_handles.insert(0);
            owner.retire();
        }
        Phase::Orphan => {}
    }
    owner
}

#[test]
fn name_reservations_require_exact_current_authority_and_canonical_direction() {
    for role in [Role::Sender, Role::Receiver] {
        for phase in [Phase::Pending, Phase::Installed, Phase::Closing] {
            let mut session = SessionState::new(&Begin::default());
            let owner = seed(&mut session, "Held", &role, phase);
            let mut sessions = HashMap::from([(0, session)]);
            assert!(connection_link_name_in_use(&sessions, "Held", &role));
            assert!(!connection_link_name_in_use(&sessions, "held", &role));
            assert!(!connection_link_name_in_use(
                &sessions,
                "Held",
                &role.opposite()
            ));
            if matches!(phase, Phase::Closing) {
                assert!(owner.is_retired());
            }
            sessions.get_mut(&0).expect("session").ending = true;
            assert!(!connection_link_name_in_use(&sessions, "Held", &role));
            sessions.get_mut(&0).expect("session").ending = false;
            sessions
                .get_mut(&0)
                .expect("session")
                .handle_aliases
                .get_mut(&0)
                .expect("alias")
                .error_detached = true;
            assert!(!connection_link_name_in_use(&sessions, "Held", &role));
            if !matches!(phase, Phase::Closing) {
                sessions
                    .get_mut(&0)
                    .expect("session")
                    .handle_aliases
                    .get_mut(&0)
                    .expect("alias")
                    .error_detached = false;
                owner.retire();
                assert!(!connection_link_name_in_use(&sessions, "Held", &role));
            }
        }
        let mut orphan = SessionState::new(&Begin::default());
        seed(&mut orphan, "Held", &role, Phase::Orphan);
        assert!(!connection_link_name_in_use(
            &HashMap::from([(0, orphan)]),
            "Held",
            &role
        ));
        for phase in [Phase::Pending, Phase::Installed] {
            let mut session = SessionState::new(&Begin::default());
            seed(&mut session, "Held", &role, phase);
            let replacement =
                IncomingAttach::new(request("Held", &role, 42), session.identity.clone(), 0);
            if matches!(phase, Phase::Pending) {
                session
                    .pending_attaches
                    .insert(0, PendingLinkFlow::incoming(&replacement));
            } else {
                session.links.insert(
                    0,
                    endpoint(&role, replacement.approval().link_identity().clone()),
                );
            }
            assert!(!connection_link_name_in_use(
                &HashMap::from([(0, session)]),
                "Held",
                &role
            ));
        }
        let mut retired_session = SessionState::new(&Begin::default());
        seed(&mut retired_session, "Held", &role, Phase::Installed);
        retired_session.identity.retire();
        assert!(!connection_link_name_in_use(
            &HashMap::from([(0, retired_session)]),
            "Held",
            &role
        ));
    }
}

struct Writer(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().expect("output").extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<Writer>,
    output: Arc<Mutex<Vec<u8>>>,
    attaches: mpsc::Receiver<IncomingAttach>,
}

impl Fixture {
    fn new() -> Self {
        let output = Arc::new(Mutex::new(Vec::new()));
        let (attach_tx, attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
        let mut sessions = HashMap::new();
        for local in 0..2 {
            let mut session = SessionState::new(&Begin {
                handle_max: 7,
                ..Begin::default()
            });
            session.peer_channel = Some(17 + local);
            session.local_begin_sent = true;
            session.attach_tx = Some(attach_tx.clone());
            sessions.insert(local, session);
        }
        Self {
            sessions,
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("writer"),
            output,
            attaches,
        }
    }

    async fn input(&mut self, local: u16, performative: Performative) -> FrameAction {
        let (incoming, _) = mpsc::channel(1);
        let peer = self.sessions[&local].peer_channel.expect("mapped session");
        handle_frame(
            Frame::Amqp {
                channel: peer,
                performative: Some(performative),
                payload: Vec::new(),
            },
            &mut self.writer,
            &incoming,
            &mut self.sessions,
            512,
            1,
            false,
        )
        .await
        .expect("input")
    }

    async fn pending(&mut self, local: u16, name: &str, role: &Role, peer: u32) -> IncomingAttach {
        assert!(matches!(
            self.input(
                local,
                Performative::Attach(Box::new(request(name, role, peer)))
            )
            .await,
            FrameAction::Continue
        ));
        let receipt = self.attaches.try_recv().expect("ordinary approval event");
        assert_eq!(receipt.approval().local_role(), *role);
        receipt
    }

    async fn approve(&mut self, local: u16, receipt: IncomingAttach) {
        let (deliveries_tx, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, _) = watch::channel(false);
        let (reply, result) = oneshot::channel();
        handle_command(
            Command::AcceptLink {
                channel: local,
                session: self.sessions[&local].identity.clone(),
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
        .expect("approval command");
        result
            .await
            .expect("approval reply")
            .expect("approved link");
    }

    async fn close(&mut self, local: u16, owner: &LinkIdentity) {
        let (reply, result) = oneshot::channel();
        handle_command(
            Command::Detach {
                channel: local,
                handle: 0,
                identity: owner.clone(),
                error: None,
                reply,
            },
            &mut self.writer,
            &mut self.sessions,
            512,
        )
        .await
        .expect("normal close command");
        result
            .await
            .expect("normal close reply")
            .expect("normal close");
        assert!(owner.is_retired());
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.lock().expect("output"));
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(read_frame(&mut input).await.expect("captured frame"));
        }
        frames
    }
}

#[tokio::test]
async fn pending_installed_and_normal_closing_names_refuse_same_or_other_session_before_admission()
{
    for role in [Role::Sender, Role::Receiver] {
        for phase in [Phase::Pending, Phase::Installed, Phase::Closing] {
            for target in [0, 1] {
                let mut fixture = Fixture::new();
                let receipt = fixture.pending(0, "held", &role, 42).await;
                let owner = receipt.approval().link_identity().clone();
                if !matches!(phase, Phase::Pending) {
                    fixture.approve(0, receipt).await;
                }
                if matches!(phase, Phase::Closing) {
                    fixture.close(0, &owner).await;
                }
                drop(fixture.frames().await);
                let original_session = fixture.sessions[&0].identity.clone();
                let target_session = fixture.sessions[&target].identity.clone();
                assert!(connection_link_name_in_use(
                    &fixture.sessions,
                    "held",
                    &role
                ));
                assert!(matches!(
                    fixture
                        .input(
                            target,
                            Performative::Attach(Box::new(request("held", &role, 43)))
                        )
                        .await,
                    FrameAction::Continue
                ));
                let frames = fixture.frames().await;
                assert!(
                    matches!(frames.as_slice(), [Frame::Amqp { channel, performative: Some(Performative::End(end)), payload }]
                    if *channel == target && payload.is_empty() && end.error.as_ref().expect("name refusal").condition.as_symbol() == Symbol::from("amqp:not-implemented"))
                );
                assert!(target_session.is_retired() && fixture.sessions[&target].ending);
                assert!(fixture.attaches.try_recv().is_err());
                assert!(fixture.writer.error_link_names().len() == 0);
                assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
                if target == 1 {
                    assert!(!original_session.is_retired());
                    assert!(
                        fixture.sessions[&0].handle_aliases[&0]
                            .identity
                            .same_link(&owner)
                    );
                    assert!(connection_link_name_in_use(
                        &fixture.sessions,
                        "held",
                        &role
                    ));
                    assert_eq!(owner.is_retired(), matches!(phase, Phase::Closing));
                } else {
                    assert!(owner.is_retired());
                }
            }
        }
    }
}

#[tokio::test]
async fn opposite_direction_and_case_distinct_pending_names_are_independent() {
    let mut fixture = Fixture::new();
    let first = fixture.pending(0, "held", &Role::Sender, 42).await;
    let opposite = fixture.pending(1, "held", &Role::Receiver, 43).await;
    let distinct = fixture.pending(1, "Held", &Role::Sender, 44).await;
    assert_eq!(first.approval().local_handle(), 0);
    assert_eq!(opposite.approval().local_handle(), 0);
    assert_eq!(distinct.approval().local_handle(), 1);
    assert!(fixture.frames().await.is_empty());
    for (name, role) in [
        ("held", Role::Sender),
        ("held", Role::Receiver),
        ("Held", Role::Sender),
    ] {
        assert!(connection_link_name_in_use(&fixture.sessions, name, &role));
    }
    fixture.approve(1, opposite).await;
    fixture.approve(0, first).await;
    fixture.approve(1, distinct).await;
    assert!(fixture.frames().await.iter().all(|frame| !matches!(
        frame,
        Frame::Amqp {
            performative: Some(
                Performative::End(_) | Performative::Close(_) | Performative::Detach(_)
            ),
            ..
        }
    )));
    assert!(
        fixture
            .sessions
            .values()
            .all(|session| !session.identity.is_retired())
    );
}

#[tokio::test]
async fn normal_ack_and_session_end_release_names_without_creating_error_history() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let first = fixture.pending(0, "held", &role, 42).await;
        let owner = first.approval().link_identity().clone();
        fixture.approve(0, first).await;
        fixture.close(0, &owner).await;
        drop(fixture.frames().await);
        assert!(connection_link_name_in_use(
            &fixture.sessions,
            "held",
            &role
        ));
        assert!(matches!(
            fixture
                .input(
                    0,
                    Performative::Detach(Detach {
                        handle: 42,
                        closed: true,
                        error: None
                    })
                )
                .await,
            FrameAction::Continue
        ));
        assert!(fixture.frames().await.is_empty());
        assert!(!connection_link_name_in_use(
            &fixture.sessions,
            "held",
            &role
        ));
        let replacement = fixture.pending(1, "held", &role, 43).await;
        let replacement_owner = replacement.approval().link_identity().clone();
        assert!(matches!(
            fixture.input(1, Performative::End(End::default())).await,
            FrameAction::Continue
        ));
        assert!(replacement_owner.is_retired() && !fixture.sessions.contains_key(&1));
        assert!(!connection_link_name_in_use(
            &fixture.sessions,
            "held",
            &role
        ));
        assert!(matches!(
            fixture.frames().await.as_slice(),
            [Frame::Amqp {
                channel: 1,
                performative: Some(Performative::End(End { error: None })),
                ..
            }]
        ));
        let final_link = fixture.pending(0, "held", &role, 44).await;
        assert!(!final_link.approval().link_identity().is_retired());
        assert!(fixture.writer.error_link_names().len() == 0);
        assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
    }
}

#[tokio::test]
async fn occupied_normal_peer_handle_retains_connection_close_priority_over_name_refusal() {
    let mut fixture = Fixture::new();
    let original = fixture.pending(0, "held", &Role::Sender, 42).await;
    assert!(matches!(
        fixture
            .input(
                0,
                Performative::Attach(Box::new(request("held", &Role::Sender, 42)))
            )
            .await,
        FrameAction::CloseSent
    ));
    assert!(
        matches!(fixture.frames().await.as_slice(), [Frame::Amqp { channel: 0, performative: Some(Performative::Close(close)), payload }]
        if payload.is_empty() && close.error.as_ref().expect("duplicate handle").condition.as_symbol() == Symbol::from("amqp:session:handle-in-use"))
    );
    assert!(original.approval().link_identity().is_retired());
    assert!(
        fixture
            .sessions
            .values()
            .all(|session| session.identity.is_retired())
    );
    assert!(fixture.attaches.try_recv().is_err());
    assert!(fixture.writer.error_link_names().len() == 0);
}
