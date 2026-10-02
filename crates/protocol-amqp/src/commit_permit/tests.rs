use std::{
    future::Future,
    pin::pin,
    sync::Barrier,
    task::{Context, Poll, Waker},
    time::Duration,
};

use super::*;

mod expiry;

fn pending() -> (Instant, AtomicCommitPermit, AtomicCommitTicket) {
    let now = Instant::now();
    let deadline = now.checked_add(Duration::from_secs(120)).expect("deadline");
    let (permit, ticket) = AtomicCommitPermit::new(deadline);
    (now, permit, ticket)
}

#[test]
fn dropping_observers_never_revokes_ticket_authority() {
    let (now, permit, ticket) = pending();
    let observer = permit.clone();
    assert_eq!(ticket.permit().state(), AtomicCommitState::Pending);
    drop(permit);
    drop(observer.clone());
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    let claim = ticket.try_claim_at(now).expect("live owner claim");
    assert_eq!(observer.state(), AtomicCommitState::Started);
    claim.finish(AtomicCommitDecision::Committed);
    assert_eq!(observer.state(), AtomicCommitState::Committed);
}

#[test]
fn dropping_an_unclaimed_ticket_aborts_once() {
    let (_, permit, ticket) = pending();
    drop(ticket);
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert!(!permit.abort());
    drop(permit.abort_on_drop());
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
}

#[test]
fn explicit_abort_wins_before_owner_claim() {
    let (now, permit, ticket) = pending();
    assert!(permit.abort());
    assert!(!permit.abort());
    assert_eq!(
        ticket.try_claim_at(now).expect_err("revoked authority"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
}

#[test]
fn deadline_equality_and_expiry_revoke_pending_authority() {
    let deadline = Instant::now();
    for sampled in [
        deadline,
        deadline
            .checked_add(Duration::from_nanos(1))
            .expect("later"),
    ] {
        let (permit, ticket) = AtomicCommitPermit::new(deadline);
        assert_eq!(
            ticket.try_claim_at(sampled).expect_err("deadline reached"),
            AtomicCommitClaimError::Aborted
        );
        assert_eq!(permit.state(), AtomicCommitState::Aborted);
    }
    let (permit, ticket) = AtomicCommitPermit::new(deadline);
    assert_eq!(
        ticket.try_claim().expect_err("past real deadline"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
}

#[test]
fn claim_before_deadline_survives_later_abort_and_deadline() {
    let (now, permit, ticket) = pending();
    let cancellation = permit.abort_on_drop();
    let claim = ticket.try_claim_at(now).expect("claim before deadline");
    assert!(!permit.abort());
    drop(cancellation);
    assert_eq!(permit.state(), AtomicCommitState::Started);
    claim.finish(AtomicCommitDecision::Committed);
    assert_eq!(permit.state(), AtomicCommitState::Committed);
}

#[test]
fn abort_and_owner_claim_have_one_atomic_winner() {
    for _ in 0..64 {
        let (now, permit, ticket) = pending();
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            let barrier = &start;
            let owner = scope.spawn(move || {
                barrier.wait();
                match ticket.try_claim_at(now) {
                    Ok(claim) => {
                        claim.finish(AtomicCommitDecision::Committed);
                        true
                    }
                    Err(AtomicCommitClaimError::Aborted) => false,
                    Err(other) => panic!("unexpected claim error: {other}"),
                }
            });
            start.wait();
            let aborted = permit.abort();
            let claimed = owner.join().expect("owner thread");
            assert_ne!(aborted, claimed);
            assert_eq!(
                permit.state(),
                if claimed {
                    AtomicCommitState::Committed
                } else {
                    AtomicCommitState::Aborted
                }
            );
        });
    }
}

async fn cancelable_future(guard: AtomicCommitAbortGuard) {
    let _guard = guard;
    std::future::pending::<()>().await;
}

#[test]
fn cancellation_guard_is_owned_even_by_an_unpolled_future() {
    let (now, permit, ticket) = pending();
    let future = cancelable_future(permit.abort_on_drop());
    drop(future);
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(
        ticket.try_claim_at(now).expect_err("unpolled cancellation"),
        AtomicCommitClaimError::Aborted
    );
}

#[test]
fn cancellation_guard_also_revokes_a_polled_waiting_future() {
    let (now, permit, ticket) = pending();
    let mut context = Context::from_waker(Waker::noop());
    {
        let future = cancelable_future(permit.abort_on_drop());
        let mut future = pin!(future);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(
        ticket.try_claim_at(now).expect_err("waiting cancellation"),
        AtomicCommitClaimError::Aborted
    );
}

#[test]
fn dropping_or_unwinding_an_unfinished_owner_claim_is_indeterminate() {
    let (now, permit, ticket) = pending();
    drop(ticket.try_claim_at(now).expect("claim"));
    assert_eq!(permit.state(), AtomicCommitState::Indeterminate);
    assert!(!permit.abort());

    let (now, permit, ticket) = pending();
    let unwound = std::panic::catch_unwind(move || {
        let _claim = ticket.try_claim_at(now).expect("claim");
        panic!("owner unwound");
    });
    assert!(unwound.is_err());
    assert_eq!(permit.state(), AtomicCommitState::Indeterminate);
}

#[test]
fn all_final_decisions_are_immutable_under_late_cancellation() {
    for (decision, expected) in [
        (
            AtomicCommitDecision::Committed,
            AtomicCommitState::Committed,
        ),
        (AtomicCommitDecision::Rejected, AtomicCommitState::Rejected),
        (
            AtomicCommitDecision::Indeterminate,
            AtomicCommitState::Indeterminate,
        ),
    ] {
        let (now, permit, ticket) = pending();
        let cancellation = permit.abort_on_drop();
        let observer = permit.clone();
        let claim = ticket.try_claim_at(now).expect("claim");
        claim.finish(decision);
        assert_eq!(observer.state(), expected);
        drop(cancellation);
        assert!(!permit.abort());
        drop(permit);
        assert_eq!(observer.state(), expected);
    }
}

#[test]
fn defensive_duplicate_ticket_cannot_start_or_overwrite_a_known_decision() {
    let (now, permit, ticket) = pending();
    // Public constructors never issue this duplicate; the CAS still rejects it.
    let duplicate = AtomicCommitTicket {
        permit: permit.clone(),
        claim_expiry_epoch_seconds: None,
    };
    let claim = ticket.try_claim_at(now).expect("only execution authority");
    assert_eq!(
        duplicate.try_claim_at(now).expect_err("already claimed"),
        AtomicCommitClaimError::Unavailable
    );
    assert_eq!(permit.state(), AtomicCommitState::Started);
    claim.finish(AtomicCommitDecision::Committed);
    let expired_duplicate = AtomicCommitTicket {
        permit: permit.clone(),
        claim_expiry_epoch_seconds: None,
    };
    assert_eq!(
        expired_duplicate
            .try_claim_at(permit.0.deadline)
            .expect_err("terminal authority"),
        AtomicCommitClaimError::Unavailable
    );
    assert_eq!(permit.state(), AtomicCommitState::Committed);
}
