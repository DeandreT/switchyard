use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use tokio::time::timeout;

use super::error_links::{
    ErrorLinkNames, ErrorPeerHandles, MAX_ERROR_LINK_NAME_BYTES, MAX_ERROR_LINK_NAMES,
    MAX_ERROR_PEER_HANDLES,
};
use super::*;
use crate::{Source, Target};

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    fail: AtomicBool,
    block: AtomicBool,
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
        if self.0.fail.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "error link flush",
            )))
        } else if self.0.block.load(Ordering::Acquire) {
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
    output: Arc<Output>,
    writer: FrameWriter<Writer>,
    attach_tx: mpsc::Sender<IncomingAttach>,
    attaches: mpsc::Receiver<IncomingAttach>,
}

impl Fixture {
    fn new() -> Self {
        let output = Arc::new(Output::default());
        let (attach_tx, attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
        let mut fixture = Self {
            sessions: HashMap::new(),
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("writer"),
            output,
            attach_tx,
            attaches,
        };
        fixture.session(0, 17);
        fixture.session(1, 18);
        fixture
    }

    fn session(&mut self, local: u16, peer: u16) {
        let mut session = SessionState::new(&Begin {
            handle_max: 0,
            ..Begin::default()
        });
        session.peer_channel = Some(peer);
        session.local_begin_sent = true;
        session.attach_tx = Some(self.attach_tx.clone());
        assert!(self.sessions.insert(local, session).is_none());
    }

    async fn input_result(
        &mut self,
        local: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> Result<FrameAction, EngineError> {
        let (incoming, _) = mpsc::channel(1);
        let peer = self.sessions[&local].peer_channel.expect("mapped peer");
        handle_frame(
            Frame::Amqp {
                channel: peer,
                performative: Some(performative),
                payload,
            },
            &mut self.writer,
            &incoming,
            &mut self.sessions,
            512,
            1,
            false,
        )
        .await
    }

    async fn input(&mut self, local: u16, performative: Performative, payload: Vec<u8>) {
        assert!(matches!(
            self.input_result(local, performative, payload)
                .await
                .expect("mapped input"),
            FrameAction::Continue
        ));
    }

    async fn approve(&mut self, local: u16, receipt: IncomingAttach) -> LinkIdentity {
        let owner = receipt.approval().link_identity().clone();
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
        owner
    }

    async fn link(&mut self, name: &str, local_role: Role) -> LinkIdentity {
        self.input(
            0,
            Performative::Attach(Box::new(request(name, opposite(&local_role), 42))),
            Vec::new(),
        )
        .await;
        let receipt = self.attaches.try_recv().expect("pending link");
        assert_eq!(receipt.approval().local_handle(), 0);
        assert_eq!(receipt.approval().local_role(), local_role);
        assert_eq!(receipt.approval().name().as_ref(), name);
        let owner = self.approve(0, receipt).await;
        drop(self.frames().await);
        owner
    }

    fn known_delivery(&mut self, role: &Role, owner: &LinkIdentity) {
        let session = self.sessions.get_mut(&0).expect("session");
        if role == &Role::Receiver {
            session
                .incoming
                .reserve(owner, 7, &[7])
                .expect("owned incoming delivery");
        } else {
            let LinkState::Sending(link) = session.links.get_mut(&0).expect("link") else {
                panic!("sender")
            };
            let (reply, result) = oneshot::channel();
            drop(result);
            link.unsettled.insert(
                7,
                OutgoingDelivery {
                    reply,
                    delivery_tag: vec![7].into(),
                    outcome: None,
                    receiver_settled: false,
                },
            );
        }
    }

    async fn error(&mut self, role: Role) -> LinkIdentity {
        let owner = self.link("dead", role.clone()).await;
        self.known_delivery(&role, &owner);
        detach_link_error(
            0,
            0,
            self.sessions.get_mut(&0).expect("session"),
            &mut self.writer,
            "amqp:invalid-field",
            "failed link",
        )
        .await
        .expect("error detach");
        let frames = self.frames().await;
        assert!(
            matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if detach.handle == 0 && detach.error.is_some())
        );
        assert!(
            self.writer
                .error_link_names()
                .owner("dead", &role)
                .expect("name history")
                .same_link(&owner)
        );
        assert!(
            self.sessions[&0]
                .error_peer_handles
                .owner(42)
                .expect("handle history")
                .same_link(&owner)
        );
        owner
    }

    async fn ack(&mut self) {
        self.input(
            0,
            Performative::Detach(Detach {
                handle: 42,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
        assert!(self.frames().await.is_empty());
        assert!(!self.sessions[&0].handle_aliases.contains_key(&0));
        assert!(self.sessions[&0].error_peer_handles.contains(42));
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.bytes.lock().expect("output"));
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame = read_frame(&mut input).await.expect("captured frame");
            assert!(crate::encode_frame(&frame).expect("encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }

    async fn end(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::End(end)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one scoped End: {frames:?}")
        };
        assert!(payload.is_empty());
        assert_eq!(
            end.error.as_ref().expect("End error").condition.as_symbol(),
            Symbol::from(condition)
        );
    }
}

fn opposite(role: &Role) -> Role {
    if role == &Role::Sender {
        Role::Receiver
    } else {
        Role::Sender
    }
}

fn request(name: &str, role: Role, peer: u32) -> Attach {
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

fn flow(peer: u32) -> Flow {
    Flow {
        handle: Some(peer),
        incoming_window: SESSION_WINDOW,
        outgoing_window: SESSION_WINDOW,
        delivery_count: Some(0),
        link_credit: Some(1),
        echo: true,
        ..Flow::default()
    }
}

fn transfer(peer: u32) -> Transfer {
    Transfer {
        handle: peer,
        delivery_id: Some(8),
        delivery_tag: Some(vec![8].into()),
        message_format: Some(0),
        settled: Some(false),
        more: true,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

#[tokio::test]
async fn queued_known_empty_resume_is_refused_on_session_approval_without_publishing_an_event() {
    for overflow in [false, true] {
        let mut fixture = Fixture::new();
        let original = fixture.error(Role::Sender).await;
        fixture.ack().await;
        let session = fixture.sessions.get_mut(&1).expect("unapproved session");
        session.attach_tx = None;
        session.local_begin_sent = false;
        if overflow {
            for peer in 1000..1000 + MAX_ERROR_PEER_HANDLES as u32 {
                session
                    .error_peer_handles
                    .record(peer, &original)
                    .expect("history capacity");
            }
        }
        let mut incoming = request("dead", Role::Receiver, 43);
        incoming.unsettled = Some(Default::default());
        assert!(
            !has_recovery_state(&incoming),
            "empty unsettled alone is fresh state"
        );
        fixture
            .input(1, Performative::Attach(Box::new(incoming)), Vec::new())
            .await;
        assert!(fixture.frames().await.is_empty());
        assert!(fixture.attaches.try_recv().is_err());
        let session = &fixture.sessions[&1];
        let queued = session
            .pending_attach_events
            .front()
            .expect("queued request");
        assert_eq!(session.pending_attach_events.len(), 1);
        let owner = queued.approval().link_identity().clone();
        let pending = &session.pending_attaches[&0];
        assert!(pending.recovery_refusal);
        assert!(Arc::ptr_eq(
            pending.approval.as_ref().expect("approval authority"),
            queued.approval()
        ));
        let session_owner = session.identity.clone();
        let (attach_tx, mut approvals) = mpsc::channel(MAX_PENDING_ATTACHES);
        let (reply, result) = oneshot::channel();
        assert!(matches!(
            handle_command(
                Command::AcceptSession {
                    channel: 1,
                    identity: session_owner.clone(),
                    attach_tx,
                    reply,
                },
                &mut fixture.writer,
                &mut fixture.sessions,
                512,
            )
            .await
            .expect("drain pending recovery request"),
            CommandAction::Continue
        ));
        let result = result.await.expect("session approval reply");
        let frames = fixture.frames().await;
        assert!(matches!(frames.first(), Some(Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Begin(begin)),
            payload,
        }) if begin.remote_channel == Some(18) && payload.is_empty()));
        assert!(
            approvals.try_recv().is_err(),
            "a recovery refusal is never an ordinary approval"
        );
        assert!(owner.is_retired());
        assert!(fixture.sessions[&1].pending_attach_events.is_empty());
        assert!(fixture.sessions[&1].pending_attaches.is_empty());
        assert!(!fixture.sessions[&0].identity.is_retired());
        assert!(
            fixture
                .writer
                .error_link_names()
                .owner("dead", &Role::Sender)
                .expect("original name history")
                .same_link(&original)
        );
        if overflow {
            assert!(matches!(result, Err(EngineError::RemoteDetached)));
            assert!(matches!(frames.as_slice(), [_, Frame::Amqp {
                channel: 1,
                performative: Some(Performative::End(end)),
                payload,
            }] if payload.is_empty() && end.error.as_ref().expect("capacity End").condition.as_symbol() == Symbol::from("amqp:resource-limit-exceeded")));
            assert!(session_owner.is_retired() && fixture.sessions[&1].ending);
            assert!(fixture.sessions[&1].attach_tx.is_none());
            assert!(fixture.sessions[&1].handle_aliases.is_empty());
        } else {
            result.expect("live session approval");
            assert!(matches!(frames.as_slice(), [_, Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Attach(attach)),
                payload: attach_payload,
            }, Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Detach(detach)),
                payload: detach_payload,
            }] if attach.name == "dead" && attach.handle == 0 && attach.role == Role::Sender
                && attach_payload.is_empty() && detach_payload.is_empty() && detach.handle == 0
                && detach.error.as_ref().expect("resume refusal").condition.as_symbol() == Symbol::from("amqp:not-implemented")));
            assert!(!session_owner.is_retired() && !fixture.sessions[&1].ending);
            assert!(fixture.sessions[&1].attach_tx.is_some());
            let alias = &fixture.sessions[&1].handle_aliases[&0];
            assert!(
                alias.identity.same_link(&owner) && alias.own_attach_sent && alias.error_detached
            );
            assert!(fixture.sessions[&1].closing_handles.contains(&0));
            assert!(
                fixture.sessions[&1]
                    .error_peer_handles
                    .owner(43)
                    .expect("pending peer history")
                    .same_link(&owner)
            );
        }
    }
}

#[test]
fn name_and_peer_handle_registries_preserve_exact_direction_generation_and_bounded_admission() {
    let mut names = ErrorLinkNames::default();
    let original = LinkIdentity::new();
    let foreign = LinkIdentity::new();
    names
        .record(Arc::from("Queue"), &Role::Sender, &original)
        .expect("sending name");
    assert!(!names.contains("queue", &Role::Sender));
    assert!(!names.contains("Queue", &Role::Receiver));
    names
        .record(Arc::from("Queue"), &Role::Sender, &foreign)
        .expect("owned repeat");
    assert!(
        names
            .owner("Queue", &Role::Sender)
            .expect("original owner")
            .same_link(&original)
    );
    names
        .record(Arc::from("Queue"), &Role::Receiver, &foreign)
        .expect("opposite local role");
    assert_eq!(names.len(), 2);
    assert_eq!(names.name_bytes(), 10);
    for index in 2..MAX_ERROR_LINK_NAMES {
        names
            .record(format!("name-{index}").into(), &Role::Sender, &original)
            .expect("bounded key");
    }
    let bytes = names.name_bytes();
    assert!(names.check_record("extra", &Role::Sender).is_err());
    assert!(
        names
            .record(Arc::from("extra"), &Role::Sender, &foreign)
            .is_err()
    );
    assert_eq!(names.len(), MAX_ERROR_LINK_NAMES);
    assert_eq!(names.name_bytes(), bytes);
    assert!(!names.contains("extra", &Role::Sender));
    let mut unicode = ErrorLinkNames::default();
    let maximum: Arc<str> = "\u{e9}".repeat(MAX_ERROR_LINK_NAME_BYTES / 2).into();
    unicode
        .record(maximum.clone(), &Role::Sender, &original)
        .expect("exact UTF8 byte cap");
    assert_eq!(unicode.name_bytes(), MAX_ERROR_LINK_NAME_BYTES);
    assert!(
        unicode
            .record(Arc::from("a"), &Role::Receiver, &foreign)
            .is_err()
    );
    assert_eq!(unicode.len(), 1);
    unicode
        .record(maximum, &Role::Sender, &foreign)
        .expect("repeat at byte cap");
    let mut handles = ErrorPeerHandles::default();
    for peer in 0..MAX_ERROR_PEER_HANDLES as u32 {
        handles
            .record(peer, &original)
            .expect("bounded peer handle");
    }
    assert!(handles.record(u32::MAX, &foreign).is_err());
    handles.record(0, &foreign).expect("repeat spends no slot");
    assert!(
        handles
            .owner(0)
            .expect("original peer owner")
            .same_link(&original)
    );
    assert!(handles.reassign(0));
    assert!(!handles.reassign(0));
    handles
        .record(u32::MAX, &foreign)
        .expect("vacant bounded peer slot");
    assert!(
        handles
            .owner(u32::MAX)
            .expect("new peer owner")
            .same_link(&foreign)
    );
    handles.clear();
    assert!(!handles.contains(u32::MAX));
}

#[tokio::test]
async fn occupied_error_alias_classifies_attach_without_allocating_a_replacement() {
    for kind in 0..3 {
        let mut fixture = Fixture::new();
        fixture.error(Role::Sender).await;
        let sibling = fixture.sessions[&1].identity.clone();
        let mut attach = request(
            if kind == 1 { "unrelated" } else { "dead" },
            Role::Receiver,
            42,
        );
        if kind == 2 {
            attach.unsettled = Some(Default::default());
        }
        fixture
            .input(0, Performative::Attach(Box::new(attach)), Vec::new())
            .await;
        fixture
            .end(if kind == 2 {
                "amqp:not-implemented"
            } else {
                "amqp:session:errant-link"
            })
            .await;
        assert!(fixture.attaches.try_recv().is_err());
        assert!(fixture.sessions[&0].ending);
        assert!(!sibling.is_retired());
    }
}

#[tokio::test]
async fn normal_occupied_alias_keeps_connection_close_priority_over_a_known_error_name() {
    let mut fixture = Fixture::new();
    fixture.link("held", Role::Sender).await;
    let previous = LinkIdentity::new();
    previous.retire();
    fixture
        .writer
        .error_link_names_mut()
        .record(Arc::from("dead"), &Role::Sender, &previous)
        .expect("older error name");
    assert!(matches!(
        fixture
            .input_result(
                0,
                Performative::Attach(Box::new(request("dead", Role::Receiver, 42))),
                Vec::new()
            )
            .await
            .expect("duplicate refusal"),
        FrameAction::CloseSent
    ));
    let frames = fixture.frames().await;
    assert!(
        matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Close(close)), .. }] if close.error.as_ref().expect("Close condition").condition.as_symbol() == Symbol::from("amqp:session:handle-in-use"))
    );
    assert!(
        fixture
            .sessions
            .values()
            .all(|session| session.identity.is_retired())
    );
}

#[tokio::test]
async fn known_names_survive_ack_and_session_end_and_distinguish_null_from_empty_resume() {
    for new_session in [false, true] {
        for empty in [false, true] {
            let mut fixture = Fixture::new();
            let owner = fixture.error(Role::Sender).await;
            fixture.ack().await;
            if new_session {
                fixture
                    .input(0, Performative::End(End::default()), Vec::new())
                    .await;
                let frames = fixture.frames().await;
                assert!(matches!(
                    frames.as_slice(),
                    [Frame::Amqp {
                        performative: Some(Performative::End(End { error: None })),
                        ..
                    }]
                ));
                fixture.session(0, 19);
                assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
            }
            let mut attach = request("dead", Role::Receiver, 43);
            if empty {
                attach.unsettled = Some(Default::default());
            }
            fixture
                .input(0, Performative::Attach(Box::new(attach)), Vec::new())
                .await;
            if empty {
                let frames = fixture.frames().await;
                assert!(
                    matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Attach(attach)), .. }, Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if attach.handle == 0 && detach.handle == 0 && detach.error.as_ref().expect("resume refusal").condition.as_symbol() == Symbol::from("amqp:not-implemented"))
                );
                assert!(!fixture.sessions[&0].ending);
                assert!(
                    fixture.sessions[&0]
                        .error_peer_handles
                        .owner(43)
                        .expect("pending refusal peer history")
                        .same_link(&fixture.sessions[&0].handle_aliases[&0].identity)
                );
            } else {
                fixture.end("amqp:session:errant-link").await;
            }
            assert!(
                fixture
                    .writer
                    .error_link_names()
                    .owner("dead", &Role::Sender)
                    .expect("retained exact name")
                    .same_link(&owner)
            );
            assert!(fixture.attaches.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn fresh_empty_unsettled_and_opposite_direction_names_remain_approvable() {
    for opposite_role in [false, true] {
        let mut fixture = Fixture::new();
        fixture.error(Role::Sender).await;
        fixture.ack().await;
        let mut attach = request(
            if opposite_role { "dead" } else { "fresh" },
            if opposite_role {
                Role::Sender
            } else {
                Role::Receiver
            },
            42,
        );
        attach.unsettled = Some(Default::default());
        fixture
            .input(0, Performative::Attach(Box::new(attach)), Vec::new())
            .await;
        let receipt = fixture.attaches.try_recv().expect("fresh approval");
        assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
        let owner = fixture.approve(0, receipt).await;
        assert!(!owner.is_retired() && !fixture.sessions[&0].ending);
        let frames = fixture.frames().await;
        assert!(frames.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Attach(_)),
                ..
            }
        )));
        assert!(!frames.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(
                    Performative::End(_) | Performative::Detach(_) | Performative::Close(_)
                ),
                ..
            }
        )));
    }
}

#[tokio::test]
async fn historical_peer_detach_is_ignored_but_flow_and_transfer_end_before_accounting() {
    for incoming_flow in [false, true] {
        let mut fixture = Fixture::new();
        fixture.error(Role::Receiver).await;
        fixture.ack().await;
        let before = fixture.sessions[&0].flow.clone();
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
        assert_eq!(fixture.sessions[&0].flow, before);
        if incoming_flow {
            let mut invalid_window = flow(42);
            invalid_window.next_outgoing_id = 99;
            fixture
                .input(0, Performative::Flow(invalid_window), Vec::new())
                .await;
        } else {
            fixture
                .input(0, Performative::Transfer(transfer(42)), vec![1; 257])
                .await;
        }
        fixture.end("amqp:session:errant-link").await;
        assert_eq!(fixture.sessions[&0].flow, before);
        assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
        assert!(!fixture.sessions[&1].identity.is_retired());
    }
}

#[tokio::test]
async fn exact_fresh_binding_wins_over_the_old_numeric_marker_and_delays_echo_until_approval() {
    let mut fixture = Fixture::new();
    let original = fixture.error(Role::Sender).await;
    fixture.ack().await;
    fixture
        .input(
            0,
            Performative::Attach(Box::new(request("replacement", Role::Receiver, 42))),
            Vec::new(),
        )
        .await;
    let receipt = fixture.attaches.try_recv().expect("replacement receipt");
    assert!(!receipt.approval().link_identity().same_link(&original));
    assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
    fixture
        .input(0, Performative::Flow(flow(42)), Vec::new())
        .await;
    assert!(fixture.frames().await.is_empty());
    let replacement = fixture.approve(0, receipt).await;
    let frames = fixture.frames().await;
    assert!(
        matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Attach(attach)), .. }, Frame::Amqp { performative: Some(Performative::Flow(flow)), .. }] if attach.handle == 0 && flow.handle == Some(0))
    );
    assert!(!replacement.is_retired());
    assert!(
        fixture
            .writer
            .error_link_names()
            .contains("dead", &Role::Sender)
    );
}

#[tokio::test]
async fn historical_detach_does_not_reply_or_mutate_a_fresh_different_peer_binding() {
    let mut fixture = Fixture::new();
    fixture.error(Role::Sender).await;
    fixture.ack().await;
    fixture
        .input(
            0,
            Performative::Attach(Box::new(request("replacement", Role::Receiver, 43))),
            Vec::new(),
        )
        .await;
    let receipt = fixture.attaches.try_recv().expect("replacement receipt");
    let current = fixture.approve(0, receipt).await;
    drop(fixture.frames().await);
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
    assert!(
        fixture.sessions[&0].links[&0]
            .identity()
            .same_link(&current)
    );
    assert!(!current.is_retired() && !fixture.sessions[&0].ending);
    assert!(fixture.sessions[&0].error_peer_handles.contains(42));
    fixture
        .input(0, Performative::Flow(flow(43)), Vec::new())
        .await;
    assert!(
        matches!(fixture.frames().await.as_slice(), [Frame::Amqp { performative: Some(Performative::Flow(flow)), .. }] if flow.handle == Some(0))
    );
}

#[tokio::test]
async fn malformed_fresh_sender_never_publishes_a_binding_or_consumes_an_approval() {
    let mut fixture = Fixture::new();
    fixture.error(Role::Sender).await;
    fixture.ack().await;
    let mut malformed = request("replacement", Role::Sender, 42);
    malformed.initial_delivery_count = None;
    fixture
        .input(0, Performative::Attach(Box::new(malformed)), Vec::new())
        .await;
    fixture.end("amqp:invalid-field").await;
    assert!(fixture.attaches.try_recv().is_err());
    assert!(fixture.sessions[&0].handle_aliases.is_empty());
    assert!(
        fixture
            .writer
            .error_link_names()
            .contains("dead", &Role::Sender)
    );
    assert!(
        !fixture
            .writer
            .error_link_names()
            .contains("replacement", &Role::Receiver)
    );
    assert!(!fixture.sessions[&1].identity.is_retired());
}

#[tokio::test]
async fn ordinary_fresh_sender_retains_approval_time_control_field_normalization() {
    let mut fixture = Fixture::new();
    let mut attach = request("control-compatible", Role::Sender, 42);
    attach.initial_delivery_count = None;
    fixture
        .input(0, Performative::Attach(Box::new(attach)), Vec::new())
        .await;
    assert!(fixture.frames().await.is_empty());
    let mut receipt = fixture.attaches.try_recv().expect("ordinary approval");
    assert_eq!(receipt.initial_delivery_count, None);
    receipt.initial_delivery_count = Some(0);
    let owner = fixture.approve(0, receipt).await;
    assert!(!owner.is_retired() && !fixture.sessions[&0].ending);
    assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
    assert!(
        !fixture
            .writer
            .error_link_names()
            .contains("control-compatible", &Role::Receiver)
    );
    let frames = fixture.frames().await;
    assert!(matches!(
        frames.as_slice(),
        [Frame::Amqp { performative: Some(Performative::Attach(attach)), .. },
         Frame::Amqp { performative: Some(Performative::Flow(flow)), .. }]
        if attach.handle == 0 && flow.handle == Some(0)
    ));
}

#[tokio::test]
async fn every_history_cap_and_error_frame_preflight_preserve_all_authority_before_any_commit() {
    for failure in 0..5 {
        let mut fixture = Fixture::new();
        let owner = fixture.link("dead", Role::Sender).await;
        fixture.known_delivery(&Role::Sender, &owner);
        let old = LinkIdentity::new();
        old.retire();
        match failure {
            0 => {
                for index in 0..MAX_ERROR_LINK_NAMES {
                    fixture
                        .writer
                        .error_link_names_mut()
                        .record(format!("full-{index}").into(), &Role::Sender, &old)
                        .expect("name cap");
                }
            }
            1 => {
                fixture
                    .writer
                    .error_link_names_mut()
                    .record(
                        "x".repeat(MAX_ERROR_LINK_NAME_BYTES).into(),
                        &Role::Receiver,
                        &old,
                    )
                    .expect("byte cap");
            }
            2 => {
                for peer in 1000..1000 + MAX_ERROR_PEER_HANDLES as u32 {
                    fixture
                        .sessions
                        .get_mut(&0)
                        .expect("session")
                        .error_peer_handles
                        .record(peer, &old)
                        .expect("peer cap");
                }
            }
            3 => {
                let ids: HashSet<_> = (1000..1000
                    + super::error_deliveries::MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32)
                    .collect();
                fixture
                    .sessions
                    .get_mut(&0)
                    .expect("session")
                    .error_deliveries
                    .record(&Role::Receiver, &old, &ids)
                    .expect("delivery cap");
            }
            _ => {}
        }
        let names = (
            fixture.writer.error_link_names().len(),
            fixture.writer.error_link_names().name_bytes(),
        );
        if failure < 4 {
            // A missing required Begin makes the bounded cap refusal fail its own preflight.
            let session = fixture.sessions.get_mut(&0).expect("session");
            session.local_begin_sent = false;
            session.peer_channel = None;
        }
        let result = detach_link_error(
            0,
            0,
            fixture.sessions.get_mut(&0).expect("session"),
            &mut fixture.writer,
            "amqp:invalid-field",
            if failure == 4 {
                "x".repeat(2048)
            } else {
                "atomic history refusal".into()
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!owner.is_retired() && !fixture.sessions[&0].ending);
        assert!(fixture.sessions[&0].links[&0].identity().same_link(&owner));
        assert!(!fixture.sessions[&0].closing_handles.contains(&0));
        assert_eq!(
            (
                fixture.writer.error_link_names().len(),
                fixture.writer.error_link_names().name_bytes()
            ),
            names
        );
        assert!(
            !fixture
                .writer
                .error_link_names()
                .contains("dead", &Role::Sender)
        );
        assert!(!fixture.sessions[&0].error_peer_handles.contains(42));
        assert!(
            fixture.sessions[&0]
                .error_deliveries
                .owner(&Role::Receiver, 7)
                .is_none()
        );
        assert!(fixture.frames().await.is_empty());
    }
}

#[tokio::test]
async fn failed_or_cancelled_error_detach_flush_retains_all_three_committed_histories() {
    for role in [Role::Sender, Role::Receiver] {
        for failed in [false, true] {
            let mut fixture = Fixture::new();
            let owner = fixture.link("dead", role.clone()).await;
            fixture.known_delivery(&role, &owner);
            fixture.output.fail.store(failed, Ordering::Release);
            fixture.output.block.store(!failed, Ordering::Release);
            let future = detach_link_error(
                0,
                0,
                fixture.sessions.get_mut(&0).expect("session"),
                &mut fixture.writer,
                "amqp:invalid-field",
                "uncertain error link",
            );
            if failed {
                assert!(future.await.is_err());
            } else {
                assert!(timeout(Duration::from_millis(10), future).await.is_err());
            }
            assert!(owner.is_retired());
            assert!(
                fixture
                    .writer
                    .error_link_names()
                    .owner("dead", &role)
                    .expect("retained name")
                    .same_link(&owner)
            );
            assert!(
                fixture.sessions[&0]
                    .error_peer_handles
                    .owner(42)
                    .expect("retained peer")
                    .same_link(&owner)
            );
            assert!(
                fixture.sessions[&0]
                    .error_deliveries
                    .owner(&opposite(&role), 7)
                    .expect("retained delivery")
                    .same_link(&owner)
            );
            assert!(fixture.sessions[&0].handle_aliases[&0].error_detached);
            let frames = fixture.frames().await;
            assert!(
                matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if detach.error.is_some())
            );
        }
    }
}
