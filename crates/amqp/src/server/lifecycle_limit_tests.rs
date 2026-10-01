use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::*;
use crate::{Source, Target};

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

fn sending() -> LinkState {
    let (detached, _) = watch::channel(false);
    LinkState::Sending(Box::new(SendingLink {
        identity: LinkIdentity::new(),
        auto_acknowledge: false,
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        default_outcome: None,
        outstanding_tags: HashSet::new(),
        settle_mode: SenderSettleMode::Unsettled,
        credit: LinkCredit::new(0),
        queued: VecDeque::new(),
        active: None,
        unsettled: HashMap::new(),
        pending_acknowledgements: HashMap::new(),
        detached,
    }))
}

fn request(handle: u32) -> Attach {
    Attach {
        name: format!("limit-{handle}"),
        handle,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    output: Writer,
    writer: FrameWriter<Writer>,
    incoming_tx: mpsc::Sender<IncomingSession>,
    incoming: mpsc::Receiver<IncomingSession>,
}

impl Fixture {
    fn new() -> Self {
        let output = Writer::default();
        let (incoming_tx, incoming) = mpsc::channel(MAX_SESSIONS_PER_CONNECTION + 1);
        Self {
            sessions: HashMap::new(),
            writer: FrameWriter::new(output.clone(), 512).expect("writer"),
            output,
            incoming_tx,
            incoming,
        }
    }

    fn seed_session(&mut self, channel: u16, count: usize, begun: bool) {
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(channel);
        session.local_begin_sent = begun;
        for handle in 0..count as u32 {
            session.links.insert(handle, sending());
            session.handle_aliases.insert(
                handle,
                super::link_handles::HandleAlias {
                    identity: session.links[&handle].identity().clone(),
                    peer_handle: Some(handle),
                    own_attach_sent: true,
                },
            );
        }
        assert!(self.sessions.insert(channel, session).is_none());
    }

    async fn input(
        &mut self,
        channel: u16,
        performative: Performative,
    ) -> Result<FrameAction, EngineError> {
        handle_frame(
            Frame::Amqp {
                channel,
                performative: Some(performative),
                payload: Vec::new(),
            },
            &mut self.writer,
            &self.incoming_tx,
            &mut self.sessions,
            512,
            u16::MAX,
            false,
        )
        .await
    }

    async fn attach(&mut self, channel: u16, handle: u32) {
        assert!(matches!(
            self.input(channel, Performative::Attach(Box::new(request(handle))))
                .await
                .expect("Attach admission"),
            FrameAction::Continue
        ));
    }

    async fn accept(&mut self, channel: u16, receipt: IncomingAttach) -> Result<(), EngineError> {
        let (deliveries_tx, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, _) = watch::channel(false);
        let (reply, result) = oneshot::channel();
        let action = handle_command(
            Command::AcceptLink {
                channel,
                session: self.sessions[&channel].identity.clone(),
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
        assert!(matches!(action, CommandAction::Continue));
        result.await.expect("approval reply")
    }

    async fn detach(&mut self, channel: u16, handle: u32) {
        let identity = self.sessions[&channel].links[&handle].identity().clone();
        let (reply, result) = oneshot::channel();
        handle_command(
            Command::Detach {
                channel,
                handle,
                identity,
                error: None,
                reply,
            },
            &mut self.writer,
            &mut self.sessions,
            512,
        )
        .await
        .expect("local Detach");
        result.await.expect("detach reply").expect("detach success");
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

fn assert_resource_end(frame: &Frame, expected_channel: u16) {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::End(end)),
        payload,
    } = frame
    else {
        panic!("resource End")
    };
    assert_eq!(*channel, expected_channel);
    assert!(payload.is_empty());
    assert_eq!(
        end.error.as_ref().expect("End error").condition.as_symbol(),
        Symbol::from("amqp:resource-limit-exceeded")
    );
}

#[test]
fn slot_count_is_an_exact_union_of_live_pending_and_closing_handles() {
    let mut fixture = Fixture::new();
    fixture.seed_session(0, 2, true);
    let session = fixture.sessions.get_mut(&0).expect("session");
    session
        .pending_attaches
        .insert(0, PendingLinkFlow::new(Role::Receiver, None));
    session.closing_handles.extend([0, 1]);
    assert_eq!(link_slot_count(session), 2);
    session
        .pending_attaches
        .insert(2, PendingLinkFlow::new(Role::Receiver, None));
    session.closing_handles.extend([2, 3]);
    assert_eq!(link_slot_count(session), 4);
    assert_eq!(connection_link_slot_count(&fixture.sessions), 4);
    fixture.seed_session(1, 1, false);
    assert_eq!(connection_link_slot_count(&fixture.sessions), 5);
    fixture
        .sessions
        .get_mut(&0)
        .expect("session")
        .links
        .get(&0)
        .expect("link")
        .identity()
        .retire();
    assert_eq!(connection_link_slot_count(&fixture.sessions), 5);
}

#[tokio::test]
async fn excess_begin_publishes_no_new_session_and_retires_existing_owners_before_close() {
    let mut fixture = Fixture::new();
    let mut owners = Vec::new();
    for channel in 0..MAX_SESSIONS_PER_CONNECTION as u16 {
        fixture
            .input(channel, Performative::Begin(Begin::default()))
            .await
            .expect("allowed Begin");
        owners.push(fixture.sessions[&channel].identity.clone());
        let session = fixture.sessions.get_mut(&channel).expect("session");
        session.local_begin_sent = channel % 2 == 0;
        session.ending = channel % 3 == 0;
    }
    assert!(fixture.frames().await.is_empty());
    let pending = IncomingAttach::new(request(19), fixture.sessions[&1].identity.clone(), 19);
    fixture
        .sessions
        .get_mut(&1)
        .expect("session")
        .pending_attaches
        .insert(19, PendingLinkFlow::incoming(&pending));
    let link = sending();
    let owner = link.identity().clone();
    fixture
        .sessions
        .get_mut(&2)
        .expect("session")
        .links
        .insert(0, link);
    assert!(matches!(
        fixture
            .input(u16::MAX, Performative::Begin(Begin::default()))
            .await
            .expect("resource Close"),
        FrameAction::Continue
    ));
    assert_eq!(fixture.sessions.len(), MAX_SESSIONS_PER_CONNECTION);
    assert!(!fixture.sessions.contains_key(&u16::MAX));
    assert_eq!(fixture.incoming.len(), MAX_SESSIONS_PER_CONNECTION);
    assert!(owners.iter().all(SessionIdentity::is_retired));
    assert!(owner.is_retired());
    assert!(pending.approval().link_identity().is_retired());
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 1);
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Close(close)),
        payload,
    } = &frames[0]
    else {
        panic!("bounded connection Close")
    };
    assert!(payload.is_empty());
    assert_eq!(
        close
            .error
            .as_ref()
            .expect("Close error")
            .condition
            .as_symbol(),
        Symbol::from("amqp:resource-limit-exceeded")
    );
}

#[tokio::test]
async fn duplicate_begin_retains_framing_error_priority_at_session_capacity() {
    let mut fixture = Fixture::new();
    for channel in 0..MAX_SESSIONS_PER_CONNECTION as u16 {
        fixture.seed_session(channel, 0, true);
    }
    let owner = fixture.sessions[&0].identity.clone();
    assert!(matches!(
        fixture
            .input(0, Performative::Begin(Begin::default()))
            .await
            .expect("duplicate Begin refusal"),
        FrameAction::Continue
    ));
    let frames = fixture.frames().await;
    let [
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(close)),
            payload,
        },
    ] = frames.as_slice()
    else {
        panic!("duplicate association gets one connection Close");
    };
    assert!(payload.is_empty());
    assert_eq!(
        close
            .error
            .as_ref()
            .expect("collision error")
            .condition
            .as_symbol(),
        Symbol::from("amqp:connection:framing-error")
    );
    assert!(owner.is_retired());
    assert!(fixture.incoming.try_recv().is_err());
    assert_eq!(fixture.sessions.len(), MAX_SESSIONS_PER_CONNECTION);
}

#[tokio::test]
async fn ending_session_slot_is_reused_only_after_matching_peer_end() {
    let mut fixture = Fixture::new();
    for channel in 0..MAX_SESSIONS_PER_CONNECTION as u16 {
        fixture.seed_session(channel, 0, true);
        let session = fixture.sessions.get_mut(&channel).expect("session");
        session.ending = true;
        session.identity.retire();
    }
    fixture
        .input(7, Performative::End(End::default()))
        .await
        .expect("matching End ACK");
    assert_eq!(fixture.sessions.len(), MAX_SESSIONS_PER_CONNECTION - 1);
    assert!(fixture.frames().await.is_empty());
    fixture
        .input(u16::MAX, Performative::Begin(Begin::default()))
        .await
        .expect("freed session slot");
    assert_eq!(fixture.sessions.len(), MAX_SESSIONS_PER_CONNECTION);
    assert_eq!(
        fixture
            .incoming
            .try_recv()
            .expect("new session event")
            .channel,
        u16::MAX
    );
    assert!(!fixture.sessions[&u16::MAX].identity.is_retired());
    assert!(fixture.frames().await.is_empty());
}

#[tokio::test]
async fn exact_session_link_limit_admits_last_receipt_then_ends_without_an_overflow_alias() {
    let mut fixture = Fixture::new();
    fixture.seed_session(3, MAX_LINKS_PER_SESSION - 1, false);
    fixture.attach(3, u32::MAX - 1).await;
    assert_eq!(
        link_slot_count(&fixture.sessions[&3]),
        MAX_LINKS_PER_SESSION
    );
    let receipt = fixture.sessions[&3]
        .pending_attach_events
        .front()
        .expect("last pending receipt")
        .clone();
    assert!(!receipt.approval().link_identity().is_retired());
    assert!(fixture.frames().await.is_empty());
    fixture.attach(3, u32::MAX).await;
    let session = &fixture.sessions[&3];
    assert!(session.ending);
    assert!(session.identity.is_retired());
    assert!(!session.pending_attaches.contains_key(&u32::MAX));
    assert!(!session.closing_handles.contains(&u32::MAX));
    assert!(session.pending_attach_events.is_empty());
    assert!(receipt.approval().link_identity().is_retired());
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 2);
    assert!(
        matches!(&frames[0], Frame::Amqp { channel: 3, performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(3))
    );
    assert_resource_end(&frames[1], 3);
}

#[tokio::test]
async fn connection_link_limit_ends_only_the_target_and_approval_does_not_double_charge() {
    let mut fixture = Fixture::new();
    fixture.seed_session(1, MAX_LINKS_PER_SESSION, true);
    fixture.seed_session(2, MAX_LINKS_PER_SESSION - 1, true);
    fixture.seed_session(3, 0, false);
    fixture.attach(2, u32::MAX).await;
    let receipt = fixture
        .sessions
        .get_mut(&2)
        .expect("session")
        .pending_attach_events
        .pop_front()
        .expect("last receipt");
    assert_eq!(
        connection_link_slot_count(&fixture.sessions),
        MAX_LINKS_PER_CONNECTION
    );
    fixture
        .accept(2, receipt.clone())
        .await
        .expect("same-slot approval at capacity");
    assert_eq!(
        connection_link_slot_count(&fixture.sessions),
        MAX_LINKS_PER_CONNECTION
    );
    assert!(
        fixture.sessions[&2].links[&u32::MAX]
            .identity()
            .same_link(receipt.approval().link_identity())
    );
    assert!(!receipt.approval().link_identity().is_retired());
    fixture.attach(3, 0).await;
    assert!(fixture.sessions[&3].ending);
    assert!(!fixture.sessions[&1].identity.is_retired());
    assert!(!fixture.sessions[&2].identity.is_retired());
    assert_eq!(
        connection_link_slot_count(&fixture.sessions),
        MAX_LINKS_PER_CONNECTION
    );
    assert!(fixture.sessions[&3].pending_attaches.is_empty());
    assert!(fixture.sessions[&3].closing_handles.is_empty());
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 3);
    assert!(matches!(
        &frames[0],
        Frame::Amqp {
            channel: 2,
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    assert!(matches!(
        &frames[1],
        Frame::Amqp {
            channel: 3,
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    assert_resource_end(&frames[2], 3);
}

#[tokio::test]
async fn local_detach_keeps_its_slot_until_peer_ack_then_new_handle_can_use_it() {
    let mut fixture = Fixture::new();
    fixture.seed_session(0, MAX_LINKS_PER_SESSION, true);
    fixture.detach(0, 0).await;
    assert_eq!(
        link_slot_count(&fixture.sessions[&0]),
        MAX_LINKS_PER_SESSION
    );
    assert!(!fixture.sessions[&0].links.contains_key(&0));
    assert!(fixture.sessions[&0].closing_handles.contains(&0));
    fixture
        .input(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        )
        .await
        .expect("peer Detach ACK");
    assert_eq!(
        link_slot_count(&fixture.sessions[&0]),
        MAX_LINKS_PER_SESSION - 1
    );
    fixture.attach(0, u32::MAX).await;
    assert_eq!(
        link_slot_count(&fixture.sessions[&0]),
        MAX_LINKS_PER_SESSION
    );
    assert!(!fixture.sessions[&0].ending);
    assert!(
        fixture.sessions[&0]
            .pending_attaches
            .contains_key(&u32::MAX)
    );
    assert_eq!(fixture.frames().await.len(), 1);
}

#[tokio::test]
async fn connection_capacity_recovers_after_peer_detach_without_disturbing_other_sessions() {
    let mut fixture = Fixture::new();
    fixture.seed_session(0, MAX_LINKS_PER_SESSION, true);
    fixture.seed_session(1, MAX_LINKS_PER_SESSION, true);
    fixture.seed_session(2, 0, true);
    let other = fixture.sessions[&1].identity.clone();
    fixture
        .input(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        )
        .await
        .expect("peer Detach");
    assert_eq!(
        connection_link_slot_count(&fixture.sessions),
        MAX_LINKS_PER_CONNECTION - 1
    );
    fixture.attach(2, 0).await;
    assert_eq!(
        connection_link_slot_count(&fixture.sessions),
        MAX_LINKS_PER_CONNECTION
    );
    assert!(!fixture.sessions[&2].ending);
    assert!(!other.is_retired());
    assert!(fixture.sessions[&2].pending_attaches.contains_key(&0));
    assert_eq!(fixture.frames().await.len(), 1);
}

#[tokio::test]
async fn recovery_refusal_owns_one_closing_slot_and_capacity_precedes_another_refusal() {
    let mut fixture = Fixture::new();
    fixture.seed_session(0, MAX_LINKS_PER_SESSION - 1, true);
    let (attach_tx, mut attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
    fixture.sessions.get_mut(&0).expect("session").attach_tx = Some(attach_tx);
    let mut attach = request(u32::MAX - 1);
    attach.incomplete_unsettled = true;
    fixture
        .input(0, Performative::Attach(Box::new(attach)))
        .await
        .expect("unsupported recovery refusal");
    assert_eq!(
        link_slot_count(&fixture.sessions[&0]),
        MAX_LINKS_PER_SESSION
    );
    assert!(
        fixture.sessions[&0]
            .closing_handles
            .contains(&(u32::MAX - 1))
    );
    assert!(!fixture.sessions[&0].ending);
    assert!(matches!(
        attaches.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let mut excess = request(u32::MAX);
    excess.incomplete_unsettled = true;
    fixture
        .input(0, Performative::Attach(Box::new(excess)))
        .await
        .expect("capacity refusal before recovery");
    assert!(fixture.sessions[&0].ending);
    assert!(!fixture.sessions[&0].closing_handles.contains(&u32::MAX));
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 3);
    assert!(
        matches!(&frames[0], Frame::Amqp { performative: Some(Performative::Attach(attach)), .. } if attach.source.is_none() && attach.target.is_none())
    );
    assert!(
        matches!(&frames[1], Frame::Amqp { performative: Some(Performative::Detach(detach)), .. } if detach.handle == u32::MAX - 1)
    );
    assert_resource_end(&frames[2], 0);
}

#[tokio::test]
async fn existing_handle_error_has_priority_over_capacity_and_does_not_publish_a_receipt() {
    let mut fixture = Fixture::new();
    fixture.seed_session(0, MAX_LINKS_PER_SESSION, true);
    fixture.attach(0, 0).await;
    let frames = fixture.frames().await;
    assert_eq!(frames.len(), 1);
    let Frame::Amqp {
        performative: Some(Performative::End(end)),
        ..
    } = &frames[0]
    else {
        panic!("handle-in-use End")
    };
    assert_eq!(
        end.error.as_ref().expect("End error").condition.as_symbol(),
        Symbol::from("amqp:session:handle-in-use")
    );
    assert!(fixture.sessions[&0].pending_attaches.is_empty());
    assert!(fixture.sessions[&0].pending_attach_events.is_empty());
}
