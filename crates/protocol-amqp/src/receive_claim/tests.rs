use std::time::Duration;

use super::*;

fn epoch(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

#[test]
fn exact_epoch_boundary_refuses_and_retains_its_reason() {
    for now in [epoch(100), epoch(101)] {
        let (permit, ticket) = ReceiveClaimPermit::new(100);
        assert_eq!(
            ticket.try_claim_at(now),
            Err(ReceiveClaimError::AuthorizationExpired)
        );
        assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
        assert!(!permit.cancel());
        assert_eq!(permit.error(), ReceiveClaimError::AuthorizationExpired);
    }
    let (permit, ticket) = ReceiveClaimPermit::new(100);
    assert_eq!(ticket.try_claim_at(epoch(99)), Ok(()));
    assert_eq!(permit.state(), ReceiveClaimState::Started);
}

#[test]
fn zero_is_expired_and_maximum_is_a_numeric_snapshot() {
    let (_, ticket) = ReceiveClaimPermit::new(0);
    assert_eq!(ticket.claim_expiry_epoch_seconds(), 0);
    assert_eq!(
        ticket.try_claim_at(UNIX_EPOCH),
        Err(ReceiveClaimError::AuthorizationExpired)
    );
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    assert_eq!(ticket.claim_expiry_epoch_seconds(), u64::MAX);
    assert_eq!(ticket.try_claim_at(epoch(1)), Ok(()));
    assert_eq!(permit.state(), ReceiveClaimState::Started);
}

#[test]
fn unavailable_epoch_fails_closed_without_turning_into_expiry() {
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    assert_eq!(
        ticket.try_claim_at(UNIX_EPOCH - Duration::from_secs(1)),
        Err(ReceiveClaimError::ClaimClockUnavailable)
    );
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert_eq!(permit.error(), ReceiveClaimError::ClaimClockUnavailable);
}

#[test]
fn first_pending_cancellation_reason_wins() {
    let (permit, ticket) = ReceiveClaimPermit::new(0);
    assert!(permit.cancel());
    assert_eq!(
        ticket.try_claim_at(UNIX_EPOCH),
        Err(ReceiveClaimError::Cancelled)
    );
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    permit.cancel();
    assert_eq!(
        ticket.try_claim_at(UNIX_EPOCH - Duration::from_secs(1)),
        Err(ReceiveClaimError::Cancelled)
    );
}

#[test]
fn observer_drop_is_inert_ticket_and_guard_drop_cancel_pending_only() {
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    drop(permit.clone());
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    drop(ticket);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    drop(permit.abort_on_drop());
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert_eq!(
        ticket.try_claim_at(epoch(1)),
        Err(ReceiveClaimError::Cancelled)
    );
}

#[test]
fn started_admission_survives_every_pending_cleanup_handle() {
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    let guard = permit.abort_on_drop();
    assert_eq!(ticket.try_claim_at(epoch(1)), Ok(()));
    assert!(!permit.cancel());
    drop(guard);
    assert_eq!(permit.state(), ReceiveClaimState::Started);
}

#[test]
fn cancellation_and_claim_have_exactly_one_winning_cas() {
    for _ in 0..100 {
        let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
        let cancel = permit.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let other = Arc::clone(&barrier);
        let thread = std::thread::spawn(move || {
            other.wait();
            cancel.cancel()
        });
        barrier.wait();
        let claimed = ticket.try_claim_at(epoch(1));
        let cancelled = thread.join().expect("cancel thread");
        assert_eq!(claimed.is_ok(), !cancelled);
        assert_eq!(
            permit.state(),
            if cancelled {
                ReceiveClaimState::Cancelled
            } else {
                ReceiveClaimState::Started
            }
        );
        if cancelled {
            assert_eq!(claimed, Err(ReceiveClaimError::Cancelled));
        }
    }
}

#[test]
fn debug_does_not_expose_the_numeric_horizon() {
    let (permit, ticket) = ReceiveClaimPermit::new(123456789);
    let debug = format!("{permit:?} {ticket:?} {:?}", permit.abort_on_drop());
    assert!(debug.contains("Pending"));
    assert!(!debug.contains("123456789"));
}
