use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use super::session_identity::{IncomingAttach, SessionIdentity};
use super::*;
use crate::{Source, Target};

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    fail_flush: AtomicBool,
}

impl Output {
    fn assert_silent(&self) {
        assert!(self.bytes.lock().expect("captured output").is_empty());
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("captured output").clone();
        let mut remaining = bytes.as_slice();
        let mut frames = Vec::new();
        while !remaining.is_empty() {
            frames.push(read_frame(&mut remaining).await.expect("complete frame"));
        }
        frames
    }

    fn clear(&self) {
        self.bytes.lock().expect("captured output").clear();
    }
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
            .expect("captured output")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.fail_flush.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected approval response flush failure",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn request(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("approval-{handle}"),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue")),
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
    output: Arc<Output>,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new() -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(CHANNEL);
        session.local_begin_sent = true;
        let output = Arc::new(Output::default());
        Self {
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(Writer(output.clone()), 512).expect("frame writer"),
            output,
        }
    }

    fn session(&self) -> &SessionState {
        &self.sessions[&CHANNEL]
    }

    fn session_mut(&mut self) -> &mut SessionState {
        self.sessions.get_mut(&CHANNEL).expect("current session")
    }

    fn pending(&mut self, handle: u32, role: Role) -> IncomingAttach {
        let receipt = IncomingAttach::new(
            request(handle, role),
            self.session().identity.clone(),
            handle,
        );
        self.session_mut().handle_aliases.insert(
            handle,
            super::link_handles::HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(handle),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        self.session_mut()
            .pending_attaches
            .insert(handle, PendingLinkFlow::incoming(&receipt));
        receipt
    }

    fn assert_pending(&self, receipt: &IncomingAttach) {
        let approval = self.session().pending_attaches[&receipt.handle]
            .approval
            .as_ref()
            .expect("incoming approval");
        assert!(Arc::ptr_eq(approval, receipt.approval()));
        assert!(!receipt.approval().link_identity().is_retired());
        assert!(!self.session().links.contains_key(&receipt.handle));
        assert!(self.session().closing_handles.is_empty());
    }

    fn accept_command(
        session: SessionIdentity,
        receipt: IncomingAttach,
    ) -> (Command, oneshot::Receiver<Result<(), EngineError>>) {
        let (deliveries_tx, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, _) = watch::channel(false);
        let (reply, result) = oneshot::channel();
        (
            Command::AcceptLink {
                channel: CHANNEL,
                session,
                attach: Box::new(receipt),
                max_message_size: 1024 * 1024,
                properties: None,
                decoders: MessageFormatDecoders::default(),
                deliveries_tx,
                detached_tx,
                consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
                reply,
            },
            result,
        )
    }

    async fn process(&mut self, command: Command) -> Result<(), EngineError> {
        let action = handle_command(command, &mut self.writer, &mut self.sessions, 512).await?;
        assert!(matches!(action, CommandAction::Continue));
        Ok(())
    }

    async fn accept(
        &mut self,
        session: &SessionIdentity,
        receipt: IncomingAttach,
    ) -> Result<(), EngineError> {
        let (command, result) = Self::accept_command(session.clone(), receipt);
        self.process(command)
            .await
            .expect("approval refusal leaves the actor usable");
        result.await.expect("actor approval result")
    }

    async fn assert_approved(&self, receipt: &IncomingAttach) {
        let link = &self.session().links[&receipt.handle];
        assert!(
            link.identity()
                .same_link(receipt.approval().link_identity())
        );
        assert!(!link.identity().is_retired());
        assert!(
            !self
                .session()
                .pending_attaches
                .contains_key(&receipt.handle)
        );
        let frames = self.output.frames().await;
        let Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Attach(response)),
            payload,
        } = &frames[0]
        else {
            panic!("approved Attach response: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(response.handle, receipt.handle);
        assert_eq!(response.name, receipt.name);
        assert_eq!(response.role, receipt.role.opposite());
        assert_eq!(frames.len(), 1 + usize::from(receipt.role == Role::Sender));
    }

    fn session_command(
        identity: SessionIdentity,
    ) -> (
        Command,
        oneshot::Receiver<Result<(), EngineError>>,
        mpsc::Receiver<IncomingAttach>,
    ) {
        let (attach_tx, attaches) = mpsc::channel(MAX_PENDING_ATTACHES);
        let (reply, result) = oneshot::channel();
        (
            Command::AcceptSession {
                channel: CHANNEL,
                identity,
                attach_tx,
                reply,
            },
            result,
            attaches,
        )
    }

    async fn input(&mut self, performative: Performative) {
        let (incoming, _received) = mpsc::channel(1);
        let action = handle_frame(
            Frame::Amqp {
                channel: CHANNEL,
                performative: Some(performative),
                payload: Vec::new(),
            },
            &mut self.writer,
            &incoming,
            &mut self.sessions,
            512,
            u16::MAX,
            false,
        )
        .await
        .expect("peer lifecycle event leaves the connection usable");
        assert!(matches!(action, FrameAction::Continue));
    }
}

#[tokio::test]
async fn mutated_handle_name_or_role_refuses_before_pending_lookup_and_preserves_original() {
    for role in [Role::Sender, Role::Receiver] {
        for field in 0..3 {
            let mut fixture = Fixture::new();
            let original = fixture.pending(HANDLE, role.clone());
            let sibling = fixture.pending(HANDLE + 1, role.clone());
            let session = fixture.session().identity.clone();
            let mut changed = original.clone();
            match field {
                0 => changed.handle = sibling.handle,
                1 => changed.name = sibling.name.clone(),
                2 => changed.role = changed.role.opposite(),
                _ => unreachable!("three immutable fields"),
            }
            assert!(matches!(
                fixture.accept(&session, changed).await,
                Err(EngineError::InvalidState(_))
            ));
            fixture.output.assert_silent();
            fixture.assert_pending(&original);
            fixture.assert_pending(&sibling);
            fixture
                .accept(&session, original.clone())
                .await
                .expect("unchanged cloned receipt remains valid");
            fixture.assert_approved(&original).await;
            fixture.assert_pending(&sibling);
        }
    }
}

#[tokio::test]
async fn cloned_approval_is_admitted_once_and_reuses_the_captured_endpoint_identity() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let receipt = fixture.pending(HANDLE, role);
        let session = fixture.session().identity.clone();
        fixture
            .accept(&session, receipt.clone())
            .await
            .expect("first approval");
        fixture.assert_approved(&receipt).await;
        fixture.output.clear();
        assert!(fixture.accept(&session, receipt.clone()).await.is_err());
        fixture.output.assert_silent();
        assert!(
            fixture.session().links[&HANDLE]
                .identity()
                .same_link(receipt.approval().link_identity())
        );
        assert!(!receipt.approval().link_identity().is_retired());
    }
}

#[tokio::test]
async fn invalid_actor_source_default_preserves_the_exact_pending_approval_without_wire() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let original = fixture.pending(HANDLE, role);
        let session = fixture.session().identity.clone();
        let mut invalid = original.clone();
        invalid
            .source
            .as_mut()
            .expect("source fixture")
            .default_outcome = Some(DeliveryState::Received {
            section_number: 0,
            section_offset: 0,
        });
        assert!(matches!(
            fixture.accept(&session, invalid).await,
            Err(EngineError::InvalidState(reason)) if reason == "source default outcome must be terminal"
        ));
        fixture.output.assert_silent();
        fixture.assert_pending(&original);
        fixture
            .accept(&session, original.clone())
            .await
            .expect("the original approval can retry without a default");
        fixture.assert_approved(&original).await;
    }
}

#[tokio::test]
async fn stale_or_retired_receipt_cannot_consume_a_fresh_same_handle_approval() {
    for role in [Role::Sender, Role::Receiver] {
        for retire_old in [false, true] {
            let mut fixture = Fixture::new();
            let old = fixture.pending(HANDLE, role.clone());
            let session = fixture.session().identity.clone();
            if retire_old {
                old.approval().retire();
            }
            let fresh = fixture.pending(HANDLE, role.clone());
            assert_eq!(old.attach(), fresh.attach());
            assert!(fixture.accept(&session, old).await.is_err());
            fixture.output.assert_silent();
            fixture.assert_pending(&fresh);
            fixture
                .accept(&session, fresh.clone())
                .await
                .expect("fresh exact approval survives stale receipt");
            fixture.assert_approved(&fresh).await;
        }
    }
}

#[tokio::test]
async fn old_caller_and_receipt_cannot_claim_a_replacement_session_with_identical_numbers() {
    for retire_old in [false, true] {
        let mut fixture = Fixture::new();
        let old = fixture.pending(HANDLE, Role::Sender);
        let old_session = fixture.session().identity.clone();
        if retire_old {
            old_session.retire();
        }
        let mut replacement = SessionState::new(&Begin::default());
        replacement.peer_channel = Some(CHANNEL);
        replacement.local_begin_sent = true;
        fixture.sessions.insert(CHANNEL, replacement);
        let fresh = fixture.pending(HANDLE, Role::Sender);
        let session = fixture.session().identity.clone();
        assert_eq!(old.attach(), fresh.attach());
        for (caller, receipt) in [
            (old_session.clone(), old.clone()),
            (session.clone(), old.clone()),
            (old_session.clone(), fresh.clone()),
        ] {
            assert!(fixture.accept(&caller, receipt).await.is_err());
            fixture.output.assert_silent();
            fixture.assert_pending(&fresh);
        }
        fixture
            .accept(&session, fresh.clone())
            .await
            .expect("only current caller and receipt approve the replacement");
        fixture.assert_approved(&fresh).await;
    }
}

#[tokio::test]
async fn foreign_connection_receipts_and_callers_cannot_consume_identical_pending_content() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let mut other = Fixture::new();
        let original = fixture.pending(HANDLE, role.clone());
        let foreign = other.pending(HANDLE, role);
        let session = fixture.session().identity.clone();
        let foreign_session = other.session().identity.clone();
        assert_eq!(original.attach(), foreign.attach());
        for (caller, receipt) in [
            (session.clone(), foreign.clone()),
            (foreign_session.clone(), original.clone()),
            (foreign_session.clone(), foreign.clone()),
        ] {
            assert!(fixture.accept(&caller, receipt).await.is_err());
            fixture.output.assert_silent();
            other.output.assert_silent();
            fixture.assert_pending(&original);
            other.assert_pending(&foreign);
        }
        fixture
            .accept(&session, original.clone())
            .await
            .expect("original connection remains independently approveable");
        other
            .accept(&foreign_session, foreign.clone())
            .await
            .expect("foreign connection remains independently approveable");
        fixture.assert_approved(&original).await;
        other.assert_approved(&foreign).await;
    }
}

#[tokio::test]
async fn peer_pending_detach_retires_the_exact_receipt_without_tombstoning_replacement_handle() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let old = fixture.pending(HANDLE, role.clone());
        let session = fixture.session().identity.clone();
        fixture
            .input(Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }))
            .await;
        assert!(old.approval().link_identity().is_retired());
        assert!(!fixture.session().pending_attaches.contains_key(&HANDLE));
        assert!(fixture.session().closing_handles.is_empty());
        let frames = fixture.output.frames().await;
        assert!(matches!(frames.as_slice(), [Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Attach(response)),
            payload: attach_payload,
        }, Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Detach(detach)),
            payload,
        }] if response.handle == HANDLE && response.role == role.opposite() && response.source.is_none()
            && response.target.is_none() && attach_payload.is_empty()
            && detach.handle == HANDLE && detach.closed && detach.error.is_none() && payload.is_empty()));
        fixture.output.clear();

        let fresh = fixture.pending(HANDLE, role);
        assert_eq!(old.attach(), fresh.attach());
        assert!(matches!(
            fixture.accept(&session, old).await,
            Err(EngineError::RemoteDetached)
        ));
        fixture.output.assert_silent();
        fixture.assert_pending(&fresh);
        fixture
            .accept(&session, fresh.clone())
            .await
            .expect("peer-acknowledged numeric handle is reusable immediately");
        fixture.assert_approved(&fresh).await;
    }
}

#[tokio::test]
async fn old_incoming_session_cannot_publish_begin_or_consume_fresh_pending_session_events() {
    for retire_old in [false, true] {
        let mut fixture = Fixture::new();
        let old = IncomingSession {
            channel: CHANNEL,
            identity: fixture.session().identity.clone(),
            begin: Begin::default(),
        };
        if retire_old {
            old.identity.retire();
        }
        fixture
            .sessions
            .insert(CHANNEL, SessionState::new(&Begin::default()));
        fixture.session_mut().peer_channel = Some(CHANNEL);
        let fresh = fixture.pending(HANDLE, Role::Sender);
        fixture
            .session_mut()
            .pending_attach_events
            .push_back(fresh.clone());
        let session = fixture.session().identity.clone();
        let (command, result, mut rejected) = Fixture::session_command(old.identity);
        fixture
            .process(command)
            .await
            .expect("old session rejection is local");
        assert!(result.await.expect("session rejection reply").is_err());
        assert!(rejected.try_recv().is_err());
        fixture.output.assert_silent();
        fixture.assert_pending(&fresh);
        assert!(!fixture.session().local_begin_sent);
        assert!(fixture.session().attach_tx.is_none());
        assert_eq!(fixture.session().pending_attach_events.len(), 1);
        assert!(Arc::ptr_eq(
            fixture.session().pending_attach_events[0].approval(),
            fresh.approval()
        ));

        let (command, result, mut accepted) = Fixture::session_command(session.clone());
        fixture
            .process(command)
            .await
            .expect("current session approval");
        result
            .await
            .expect("session approval reply")
            .expect("current session accepted");
        let published = accepted.try_recv().expect("fresh pending attach published");
        assert!(Arc::ptr_eq(published.approval(), fresh.approval()));
        assert!(fixture.session().pending_attach_events.is_empty());
        assert!(fixture.session().local_begin_sent);
        assert!(fixture.session().attach_tx.is_some());
        let frames = fixture.output.frames().await;
        assert!(matches!(frames.as_slice(), [Frame::Amqp {
            channel: CHANNEL, performative: Some(Performative::Begin(begin)), payload,
        }] if begin.remote_channel == Some(CHANNEL) && payload.is_empty()));
        fixture.output.clear();

        let (command, result, mut duplicate) = Fixture::session_command(session);
        fixture
            .process(command)
            .await
            .expect("repeated session approval is local");
        assert!(result.await.expect("duplicate session reply").is_err());
        assert!(duplicate.try_recv().is_err());
        assert!(accepted.try_recv().is_err());
        fixture.output.assert_silent();
        fixture.assert_pending(&fresh);
    }
}

#[tokio::test]
async fn failed_attach_response_flush_never_installs_or_reports_a_successful_endpoint() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let receipt = fixture.pending(HANDLE, role);
        let session = fixture.session().identity.clone();
        fixture.output.fail_flush.store(true, Ordering::Release);
        let (command, mut result) = Fixture::accept_command(session, receipt.clone());
        assert!(matches!(
            fixture.process(command).await,
            Err(EngineError::Io(_))
        ));
        assert!(!matches!(result.try_recv(), Ok(Ok(()))));
        fixture.assert_pending(&receipt);
        let frames = fixture.output.frames().await;
        assert!(matches!(frames.as_slice(), [Frame::Amqp {
            channel: CHANNEL, performative: Some(Performative::Attach(response)), payload,
        }] if response.handle == HANDLE && payload.is_empty()));
    }
}

#[tokio::test]
async fn failed_begin_flush_does_not_publish_attach_events_or_accept_the_session() {
    let mut fixture = Fixture::new();
    fixture.session_mut().local_begin_sent = false;
    let receipt = fixture.pending(HANDLE, Role::Sender);
    fixture
        .session_mut()
        .pending_attach_events
        .push_back(receipt.clone());
    let session = fixture.session().identity.clone();
    fixture.output.fail_flush.store(true, Ordering::Release);
    let (command, mut result, mut published) = Fixture::session_command(session);
    assert!(matches!(
        fixture.process(command).await,
        Err(EngineError::Io(_))
    ));
    assert!(!matches!(result.try_recv(), Ok(Ok(()))));
    assert!(published.try_recv().is_err());
    assert!(!fixture.session().local_begin_sent);
    assert!(fixture.session().attach_tx.is_none());
    assert_eq!(fixture.session().pending_attach_events.len(), 1);
    fixture.assert_pending(&receipt);
    let frames = fixture.output.frames().await;
    assert!(matches!(frames.as_slice(), [Frame::Amqp {
        channel: CHANNEL, performative: Some(Performative::Begin(begin)), payload,
    }] if begin.remote_channel == Some(CHANNEL) && payload.is_empty()));
}
