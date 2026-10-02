use std::{
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::*;
use crate::server::{
    CommandAction, NativeConnectionIdentity, flow_control::LinkCredit, frame_writer::FrameWriter,
    handle_command,
};
use crate::{Begin, ReceiverSettleMode, SenderSettleMode};
use tokio::sync::watch;

const CHANNEL: u16 = 3;
const HANDLE: u32 = 7;

struct Fixture {
    sender: Sender,
    incoming: mpsc::Receiver<Command>,
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<tokio::io::Sink>,
}

impl Fixture {
    fn new() -> Self {
        let connection = NativeConnectionIdentity::new();
        let identity = LinkIdentity::for_connection(&connection);
        let mut session = SessionState::for_connection(&Begin::default(), &connection);
        let (commands, incoming) = mpsc::channel(1);
        let (detached, detached_rx) = watch::channel(false);
        let mut credit = LinkCredit::new(0);
        credit.update_peer(Some(0), 32, false).expect("credit");
        session.links.insert(
            HANDLE,
            LinkState::Sending(Box::new(SendingLink {
                identity: identity.clone(),
                auto_acknowledge: false,
                max_message_size: None,
                receiver_settle_mode: ReceiverSettleMode::Second,
                default_outcome: None,
                outstanding_tags: Default::default(),
                settle_mode: SenderSettleMode::Unsettled,
                credit,
                reservations: OutgoingReservations::default(),
                queued: VecDeque::new(),
                active: None,
                unsettled: Default::default(),
                pending_acknowledgements: Default::default(),
                detached,
            })),
        );
        Self {
            sender: Sender {
                name: "owned-send".into(),
                max_message_size: None,
                channel: CHANNEL,
                handle: HANDLE,
                commands,
                detached: detached_rx,
                identity,
            },
            incoming,
            sessions: HashMap::from([(CHANNEL, session)]),
            writer: FrameWriter::new(tokio::io::sink(), 262_144).expect("writer"),
        }
    }

    fn claim(&mut self) -> (ClaimedOutgoingSendReservation, Arc<ReservationControl>) {
        let control = ReservationControl::new(self.sender.identity.clone());
        let (reply, mut response) = oneshot::channel();
        handle_reserve(
            ReservationRequest {
                channel: CHANNEL,
                handle: HANDLE,
                control: Arc::clone(&control),
                reply,
            },
            &mut self.sessions,
            self.writer.reservation_cleanup(),
        );
        let claimed = response
            .try_recv()
            .expect("reservation reply")
            .expect("reservation")
            .try_claim()
            .expect("claim");
        (claimed, control)
    }

    fn link(&self) -> &SendingLink {
        let LinkState::Sending(link) = &self.sessions[&CHANNEL].links[&HANDLE] else {
            panic!("sending link");
        };
        link
    }

    fn fill_command_channel(&self) -> Arc<ReservationControl> {
        let control = ReservationControl::new(self.sender.identity.clone());
        let (reply, _response) = oneshot::channel();
        assert!(
            self.sender
                .commands
                .try_send(Command::ReserveSend(ReservationRequest {
                    channel: CHANNEL,
                    handle: HANDLE,
                    control: Arc::clone(&control),
                    reply,
                }))
                .is_ok()
        );
        control
    }

    async fn process(&mut self, command: Command) {
        assert!(matches!(
            handle_command(command, &mut self.writer, &mut self.sessions, 262_144)
                .await
                .expect("command"),
            CommandAction::Continue
        ));
    }
}

fn poll_pending<F: Future>(future: Pin<&mut F>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

fn owned<F: Future<Output = Result<PendingSettlement, EngineError>> + Send + 'static>(
    future: F,
) -> F {
    future
}

#[test]
fn factory_is_owned_static_and_unpolled_drop_refunds_without_enqueue() {
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let future = owned({
        let endpoint = &fixture.sender;
        endpoint.send_reserved_with_settlement_owned(
            claimed,
            Message::data([1, 2, 3]),
            vec![1].into(),
        )
    });
    assert_eq!(control.phase(), CLAIMED);
    assert_eq!(fixture.link().reservations.count(), 1);
    drop(fixture.sender.on_detach());
    drop(future);
    assert_eq!(control.phase(), CANCELLED);
    assert_eq!(fixture.link().reservations.count(), 0);
    assert!(matches!(
        fixture.incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
    assert_eq!(fixture.link().credit.delivery_count(), 0);
}

#[tokio::test]
async fn full_command_queue_cancellation_refunds_without_eviction_or_io() {
    let mut fixture = Fixture::new();
    let queued = fixture.fill_command_channel();
    let (claimed, control) = fixture.claim();
    let mut future = Box::pin(fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([1]),
        vec![1].into(),
    ));
    poll_pending(future.as_mut());
    assert_eq!(control.phase(), CLAIMED);
    drop(future);
    assert_eq!(control.phase(), CANCELLED);
    let command = fixture
        .incoming
        .try_recv()
        .expect("existing queued command");
    let Command::ReserveSend(request) = command else {
        panic!("no reserved send entered full channel");
    };
    assert!(Arc::ptr_eq(&queued, &request.control));
    assert!(matches!(
        fixture.incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.link().reservations.count(), 0);
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
}

#[tokio::test]
async fn dropped_queued_operation_is_refused_before_encoding_or_alias_admission() {
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let mut future = Box::pin(fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([1, 2, 3]),
        vec![1].into(),
    ));
    poll_pending(future.as_mut());
    let command = fixture.incoming.try_recv().expect("reserved send queued");
    drop(future);
    assert_eq!(control.phase(), CANCELLED);
    fixture.process(command).await;
    assert!(fixture.link().queued.is_empty());
    assert!(fixture.link().outstanding_tags.is_empty());
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
    assert_eq!(fixture.link().credit.delivery_count(), 0);
}

#[tokio::test]
async fn actor_consumed_enqueue_is_not_reversed_by_future_drop() {
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let mut future = Box::pin(fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([1, 2, 3]),
        vec![1].into(),
    ));
    poll_pending(future.as_mut());
    let command = fixture.incoming.try_recv().expect("reserved send queued");
    fixture.process(command).await;
    assert_eq!(control.phase(), CONSUMED);
    let retained = fixture.writer.content_budget().retained_bytes();
    assert!(retained > 0);
    drop(future);
    assert_eq!(control.phase(), CONSUMED);
    assert_eq!(fixture.link().queued.len(), 1);
    assert!(fixture.link().outstanding_tags.contains([1].as_slice()));
    assert_eq!(fixture.writer.content_budget().retained_bytes(), retained);
    assert_eq!(fixture.link().credit.delivery_count(), 0);
    crate::server::stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
}

#[tokio::test]
async fn foreign_origin_is_refused_locally_before_command_publication() {
    let mut fixture = Fixture::new();
    let foreign = Fixture::new();
    let (claimed, control) = fixture.claim();
    let result = foreign
        .sender
        .send_reserved_with_settlement_owned(claimed, Message::data([9]), vec![9].into())
        .await;
    assert!(matches!(result, Err(EngineError::SendReservationRevoked)));
    assert_eq!(control.phase(), CANCELLED);
    assert!(matches!(
        fixture.incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.link().reservations.count(), 0);
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
}

#[tokio::test]
async fn retirement_after_factory_creation_refuses_without_enqueue() {
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let future = fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([9]),
        vec![9].into(),
    );
    crate::server::stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    assert!(matches!(
        future.await,
        Err(EngineError::SendReservationRevoked)
    ));
    assert_eq!(control.phase(), REVOKED);
    assert!(matches!(
        fixture.incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
}

#[tokio::test]
async fn missing_actor_route_and_stopped_command_channel_refund_local_admission() {
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let mut future = Box::pin(fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([1]),
        vec![1].into(),
    ));
    poll_pending(future.as_mut());
    let command = fixture.incoming.try_recv().expect("command");
    let mut empty_sessions = HashMap::new();
    handle_command(command, &mut fixture.writer, &mut empty_sessions, 262_144)
        .await
        .expect("missing route handled locally");
    let result = tokio::time::timeout(Duration::from_millis(100), future)
        .await
        .expect("reply");
    assert!(matches!(result, Err(EngineError::SendReservationRevoked)));
    assert_eq!(control.phase(), CANCELLED);
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);

    let (claimed, control) = fixture.claim();
    fixture.incoming.close();
    assert!(matches!(
        fixture
            .sender
            .send_reserved_with_settlement_owned(claimed, Message::data([2]), vec![2].into())
            .await,
        Err(EngineError::Stopped)
    ));
    assert_eq!(control.phase(), CANCELLED);
}

#[test]
fn unpolled_packet_cancellation_wakes_with_terminal_state_before_returning() {
    struct CancellationWake {
        control: Arc<ReservationControl>,
        count: AtomicUsize,
    }
    impl Wake for CancellationWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert_eq!(self.control.phase(), CANCELLED);
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut fixture = Fixture::new();
    let (claimed, control) = fixture.claim();
    let wake = Arc::new(CancellationWake {
        control: Arc::clone(&control),
        count: AtomicUsize::new(0),
    });
    let cleanup = fixture.writer.reservation_cleanup();
    let mut notified = Box::pin(cleanup.notified());
    let waker = Waker::from(Arc::clone(&wake));
    assert!(matches!(
        notified.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
    let future = fixture.sender.send_reserved_with_settlement_owned(
        claimed,
        Message::data([1, 2, 3]),
        vec![1].into(),
    );
    drop(future);
    assert_eq!(wake.count.load(Ordering::Relaxed), 1);
    assert_eq!(control.phase(), CANCELLED);
    assert_eq!(fixture.writer.content_budget().retained_bytes(), 0);
    assert!(matches!(
        fixture.incoming.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
