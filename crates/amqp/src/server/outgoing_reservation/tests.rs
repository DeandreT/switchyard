use std::sync::{Barrier, atomic::AtomicUsize};

use super::*;
use crate::server::{
    NativeConnectionIdentity, QueuedSend, SendingLink, connection_identity::ConnectionActorExit,
    content_budget::ContentBudget, flow_control::LinkCredit,
};
use crate::{Begin, ReceiverSettleMode, SenderSettleMode};
use tokio::sync::watch;

fn owner() -> LinkIdentity {
    LinkIdentity::for_connection(&NativeConnectionIdentity::new())
}

fn entry(
    reservations: &mut OutgoingReservations,
    owner: &LinkIdentity,
) -> (
    Arc<ReservationControl>,
    oneshot::Receiver<Result<OutgoingSendReservation, EngineError>>,
) {
    let control = ReservationControl::new(owner.clone());
    let (reply, response) = oneshot::channel();
    reservations.entries.push_back(ReservationEntry {
        control: Arc::clone(&control),
        reply: Some(reply),
    });
    (control, response)
}

fn token(reservations: &mut OutgoingReservations, owner: &LinkIdentity) -> OutgoingSendReservation {
    let (_, mut response) = entry(reservations, owner);
    reservations.refresh(reservations.count() as u32, 0, 0);
    response
        .try_recv()
        .expect("published reservation")
        .expect("admitted")
}

fn sending(owner: LinkIdentity) -> SendingLink {
    let (detached, _) = watch::channel(false);
    SendingLink {
        identity: owner,
        auto_acknowledge: false,
        max_message_size: None,
        receiver_settle_mode: ReceiverSettleMode::Second,
        default_outcome: None,
        outstanding_tags: Default::default(),
        settle_mode: SenderSettleMode::Unsettled,
        credit: LinkCredit::new(0),
        reservations: OutgoingReservations::default(),
        queued: VecDeque::new(),
        active: None,
        unsettled: Default::default(),
        pending_acknowledgements: Default::default(),
        detached,
    }
}

fn queued(reserved: bool) -> QueuedSend {
    let (reply, _) = oneshot::channel();
    QueuedSend {
        credit_reserved: reserved,
        payload: Vec::new(),
        content_lease: ContentBudget::default().try_reserve(0).expect("zero lease"),
        delivery_tag: vec![0].into(),
        message_format: 0,
        reply: reply.into(),
    }
}

#[test]
fn reserve_claim_and_drop_do_not_modify_wire_credit() {
    let mut link = sending(owner());
    link.credit.update_peer(Some(0), 1, false).expect("Flow");
    let reservation = token(&mut link.reservations, &link.identity);
    assert_eq!(link.credit.delivery_count(), 0);
    assert_eq!(link.credit.allowance(), 1);
    assert!(link.reservations.blocks_drain());
    let claimed = reservation.try_claim().expect("claim");
    assert_eq!(link.reservations.count(), 1);
    drop(claimed);
    refresh_link(&mut link);
    assert_eq!(link.reservations.count(), 0);
    assert!(!link.reservations.blocks_drain());
    assert_eq!(link.credit.delivery_count(), 0);
    assert_eq!(link.credit.allowance(), 1);
}

#[test]
fn unbound_and_inactive_origins_fail_closed() {
    let mut reservations = OutgoingReservations::default();
    let unbound = token(&mut reservations, &LinkIdentity::new());
    assert!(matches!(
        unbound.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    let connection = NativeConnectionIdentity::new();
    let owner = LinkIdentity::for_connection(&connection);
    let bound = token(&mut reservations, &owner);
    let (terminated, _) = watch::channel(false);
    drop(ConnectionActorExit::new(connection, terminated));
    assert!(matches!(
        bound.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
}

#[test]
fn retraction_never_revives_a_revoked_token() {
    let owner = owner();
    let mut reservations = OutgoingReservations::default();
    let revoked = token(&mut reservations, &owner);
    reservations.refresh(0, 0, 0);
    reservations.refresh(1, 0, 0);
    assert!(matches!(
        revoked.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    assert_eq!(reservations.count(), 0);
    let replacement = token(&mut reservations, &owner)
        .try_claim()
        .expect("fresh claim");
    drop(replacement);
}

#[test]
fn retraction_retains_claimed_but_revokes_other_lookup_slots() {
    let owner = owner();
    let mut reservations = OutgoingReservations::default();
    let first = token(&mut reservations, &owner)
        .try_claim()
        .expect("first claim");
    let second = token(&mut reservations, &owner);
    reservations.refresh(1, 0, 0);
    assert!(matches!(
        second.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    assert!(reservations.validates(&first, &owner));
    reservations.refresh(0, 0, 0);
    assert!(reservations.validates(&first, &owner));
    drop(first);
    reservations.refresh(0, 0, 0);
    assert_eq!(reservations.count(), 0);
}

#[test]
fn raced_claim_cannot_leave_an_extra_unclaimed_slot_after_shrink() {
    for _ in 0..128 {
        let owner = owner();
        let mut reservations = OutgoingReservations::default();
        let first = token(&mut reservations, &owner);
        let second = token(&mut reservations, &owner);
        let barrier = Arc::new(Barrier::new(2));
        let thread_barrier = Arc::clone(&barrier);
        let claimant = std::thread::spawn(move || {
            thread_barrier.wait();
            second.try_claim()
        });
        barrier.wait();
        reservations.refresh(1, 0, 0);
        let second = claimant.join().expect("claim thread");
        let first = first.try_claim();
        assert!(usize::from(first.is_ok()) + usize::from(second.is_ok()) <= 1);
    }
}

#[tokio::test]
async fn cancellation_wakes_cleanup_without_a_command_send() {
    let mut reservations = OutgoingReservations::default();
    let (control, mut response) = entry(&mut reservations, &owner());
    let cleanup = Arc::new(Notify::new());
    control
        .cleanup
        .set(Arc::clone(&cleanup))
        .expect("install wake");
    reservations.refresh(1, 0, 0);
    let reservation = response.try_recv().expect("reply").expect("reservation");
    drop(reservation);
    tokio::time::timeout(std::time::Duration::from_millis(100), cleanup.notified())
        .await
        .expect("cleanup notification");
    assert_eq!(control.phase(), CANCELLED);
    reservations.refresh(1, 0, 0);
    assert!(reservations.entries.is_empty());
}

#[test]
fn lost_reply_and_waiting_guard_cancellation_are_reaped() {
    let mut reservations = OutgoingReservations::default();
    let (control, response) = entry(&mut reservations, &owner());
    drop(response);
    reservations.refresh(1, 0, 0);
    assert_eq!(control.phase(), CANCELLED);
    assert!(reservations.entries.is_empty());
    let (waiting, _response) = entry(&mut reservations, &owner());
    let guard = ReservationGuard::new(Arc::clone(&waiting));
    reservations.refresh(0, 0, 0);
    drop(guard);
    reservations.refresh(0, 0, 0);
    assert_eq!(waiting.phase(), CANCELLED);
    assert!(reservations.entries.is_empty());
}

#[test]
fn actor_set_drop_revokes_reserved_and_claimed_tokens() {
    let owner = owner();
    let mut reservations = OutgoingReservations::default();
    let reserved = token(&mut reservations, &owner);
    let claimed = token(&mut reservations, &owner).try_claim().expect("claim");
    drop(reservations);
    assert!(matches!(
        reserved.try_claim(),
        Err(EngineError::SendReservationRevoked)
    ));
    assert!(!claimed.belongs_to(&owner));
}

#[test]
fn exact_membership_and_single_consumption_precede_send() {
    let owner = owner();
    let foreign = owner.new_child();
    let mut reservations = OutgoingReservations::default();
    let mut unrelated = OutgoingReservations::default();
    let claimed = token(&mut reservations, &owner).try_claim().expect("claim");
    let other = token(&mut unrelated, &owner)
        .try_claim()
        .expect("other claim");
    assert!(!reservations.validates(&claimed, &foreign));
    assert!(!reservations.validates(&other, &owner));
    assert!(!reservations.consume(&other));
    assert!(reservations.validates(&claimed, &owner));
    assert!(reservations.consume(&claimed));
    assert!(!reservations.consume(&claimed));
    assert_eq!(reservations.count(), 0);
}

#[test]
fn legacy_queue_cannot_steal_reserved_credit_but_reserved_send_can_progress() {
    let mut link = sending(owner());
    link.credit.update_peer(Some(0), 1, false).expect("Flow");
    let claimed = token(&mut link.reservations, &link.identity)
        .try_claim()
        .expect("claim");
    link.queued.push_back(queued(false));
    refresh_link(&mut link);
    assert_eq!(queued_candidate(&link), None);
    assert!(link.reservations.consume(&claimed));
    link.queued.push_back(queued(true));
    assert_eq!(queued_candidate(&link), Some(1));
    let other = token(&mut link.reservations, &link.identity)
        .try_claim()
        .expect("other claim");
    assert_eq!(queued_candidate(&link), Some(1));
    link.credit
        .update_peer(Some(0), 2, false)
        .expect("two credits");
    assert_eq!(queued_candidate(&link), Some(1));
    link.credit
        .update_peer(Some(0), 3, false)
        .expect("three credits");
    assert_eq!(queued_candidate(&link), Some(0));
    drop(other);
}

#[test]
fn new_grants_account_for_all_queued_rows_without_revoking_existing_slots() {
    let owner = owner();
    let mut reservations = OutgoingReservations::default();
    let first = token(&mut reservations, &owner);
    let (_, mut waiting) = entry(&mut reservations, &owner);
    reservations.refresh(1, 1, 0);
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(first.try_claim().is_ok());
    reservations.refresh(2, 0, 0);
    assert!(waiting.try_recv().expect("second reply").is_ok());
}

#[test]
fn reservation_caps_include_queue_link_and_session_slots() {
    let connection = NativeConnectionIdentity::new();
    let mut session = SessionState::for_connection(&Begin::default(), &connection);
    session
        .links
        .insert(0, LinkState::Sending(Box::new(sending(owner()))));
    let mut sessions = HashMap::from([(0, session)]);
    let cleanup = Arc::new(Notify::new());
    let mut responses = Vec::new();
    for _ in 0..DELIVERY_QUEUE_CAPACITY {
        let owner = sessions[&0].links[&0].identity().clone();
        let (reply, response) = oneshot::channel();
        handle_reserve(
            ReservationRequest {
                channel: 0,
                handle: 0,
                control: ReservationControl::new(owner),
                reply,
            },
            &mut sessions,
            Arc::clone(&cleanup),
        );
        responses.push(response);
    }
    let request = |owner: LinkIdentity| {
        let (reply, response) = oneshot::channel();
        (
            ReservationRequest {
                channel: 0,
                handle: 0,
                control: ReservationControl::new(owner),
                reply,
            },
            response,
        )
    };
    let (extra, mut rejected) = request(sessions[&0].links[&0].identity().clone());
    handle_reserve(extra, &mut sessions, Arc::clone(&cleanup));
    assert!(matches!(
        rejected.try_recv(),
        Ok(Err(EngineError::InvalidState(_)))
    ));
    drop(responses);
    // Waiting replies are still armed by their caller guard in the public API;
    // retire here to model connection cleanup before testing independent caps.
    crate::server::stop_session(sessions.get_mut(&0).expect("session"));
    let session = SessionState::for_connection(&Begin::default(), &connection);
    sessions.insert(0, session);
    let session = sessions.get_mut(&0).expect("new session");
    let mut link = sending(owner());
    link.outstanding_tags = (0..MAX_OUTGOING_DELIVERIES_PER_LINK)
        .map(|id| id.to_be_bytes().to_vec())
        .collect();
    session.links.insert(0, LinkState::Sending(Box::new(link)));
    let (extra, mut rejected) = request(session.links[&0].identity().clone());
    session.ending = false;
    handle_reserve(extra, &mut sessions, Arc::clone(&cleanup));
    assert!(matches!(
        rejected.try_recv(),
        Ok(Err(EngineError::InvalidState(_)))
    ));
    let session = sessions.get_mut(&0).expect("session");
    session
        .links
        .insert(0, LinkState::Sending(Box::new(sending(owner()))));
    for handle in 1..=4 {
        let mut link = sending(owner());
        link.outstanding_tags = (0..MAX_OUTGOING_DELIVERIES_PER_LINK)
            .map(|id| id.to_be_bytes().to_vec())
            .collect();
        session
            .links
            .insert(handle, LinkState::Sending(Box::new(link)));
    }
    let (extra, mut rejected) = request(session.links[&0].identity().clone());
    handle_reserve(extra, &mut sessions, cleanup);
    assert!(matches!(
        rejected.try_recv(),
        Ok(Err(EngineError::InvalidState(_)))
    ));
}

#[test]
fn debug_omits_link_and_route_metadata() {
    let mut reservations = OutgoingReservations::default();
    let reservation = token(&mut reservations, &owner());
    assert_eq!(
        format!("{reservation:?}"),
        "OutgoingSendReservation { revoked: false, .. }"
    );
    let claimed = reservation.try_claim().expect("claim");
    assert_eq!(
        format!("{claimed:?}"),
        "ClaimedOutgoingSendReservation { revoked: false, .. }"
    );
}

#[test]
fn owned_unpolled_factory_retains_full_guard_and_performs_no_enqueue() {
    let (commands, mut incoming) = tokio::sync::mpsc::channel(1);
    let (_, detached) = watch::channel(false);
    let sender = Sender {
        name: "private".into(),
        max_message_size: None,
        channel: 0,
        handle: 0,
        commands,
        detached,
        identity: owner(),
    };
    fn owned<F: Future<Output = Result<OutgoingSendReservation, EngineError>> + Send + 'static>(
        value: F,
    ) -> F {
        value
    }
    let future = owned({
        let endpoint = &sender;
        endpoint.reserve_send()
    });
    drop(future);
    assert!(matches!(
        incoming.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[test]
fn reentrant_cancel_wake_has_no_registry_lock() {
    struct CancelWake(Arc<ReservationControl>, AtomicUsize);
    impl std::task::Wake for CancelWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.1.fetch_add(1, Ordering::Relaxed);
            self.0.cancel();
        }
    }
    let mut reservations = OutgoingReservations::default();
    let (control, mut response) = entry(&mut reservations, &owner());
    let wake = Arc::new(CancelWake(Arc::clone(&control), AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(
        std::pin::Pin::new(&mut response)
            .poll(&mut context)
            .is_pending()
    );
    reservations.refresh(1, 0, 0);
    assert_eq!(wake.1.load(Ordering::Relaxed), 1);
    assert_eq!(control.phase(), CANCELLED);
    reservations.refresh(1, 0, 0);
    assert!(reservations.entries.is_empty());
}

#[tokio::test]
async fn full_command_queue_cannot_block_waiter_cancellation_or_slot_refund() {
    let owner = owner();
    let (commands, mut incoming) = tokio::sync::mpsc::channel(1);
    let (_, detached) = watch::channel(false);
    let sender = Sender {
        name: "full-queue".into(),
        max_message_size: None,
        channel: 0,
        handle: 0,
        commands: commands.clone(),
        detached,
        identity: owner.clone(),
    };
    let (reply, _response) = oneshot::channel();
    commands
        .try_send(Command::ReserveSend(ReservationRequest {
            channel: 0,
            handle: 0,
            control: ReservationControl::new(owner.clone()),
            reply,
        }))
        .expect("fill the single command slot");

    let mut waiting = Box::pin(sender.reserve_send());
    std::future::poll_fn(|context| {
        assert!(waiting.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert_eq!(commands.capacity(), 0);

    let mut reservations = OutgoingReservations::default();
    let (control, mut response) = entry(&mut reservations, &owner);
    let cleanup = Arc::new(Notify::new());
    control
        .cleanup
        .set(Arc::clone(&cleanup))
        .expect("install cleanup wake");
    reservations.refresh(1, 0, 0);
    let claimed = response
        .try_recv()
        .expect("published token")
        .expect("admitted")
        .try_claim()
        .expect("claimed slot");
    drop(claimed);
    assert_eq!(control.phase(), CANCELLED);
    assert_eq!(reservations.count(), 0);
    tokio::time::timeout(std::time::Duration::from_secs(1), cleanup.notified())
        .await
        .expect("shared cleanup wake despite full commands");
    assert_eq!(commands.capacity(), 0);
    assert!(matches!(incoming.try_recv(), Ok(Command::ReserveSend(_))));
    assert!(matches!(
        incoming.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    reservations.refresh(1, 0, 0);
    assert!(reservations.entries.is_empty());
}
