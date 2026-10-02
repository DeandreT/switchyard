use super::*;

fn epoch(seconds: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))
        .expect("test epoch")
}

#[test]
fn unrestricted_tickets_ignore_missing_and_invalid_epoch_samples() {
    let invalid = UNIX_EPOCH
        .checked_sub(Duration::from_secs(1))
        .expect("before epoch");
    for sampled in [None, Some(invalid)] {
        let (now, permit, ticket) = pending();
        assert_eq!(ticket.claim_expiry_epoch_seconds, None);
        let claim = ticket
            .try_claim_with_samples(now, sampled)
            .expect("no epoch restriction");
        claim.finish(AtomicCommitDecision::Committed);
        assert_eq!(permit.state(), AtomicCommitState::Committed);
    }
}

#[test]
fn restrictions_only_tighten_without_sampling_or_revoking_pending_work() {
    let (now, permit, mut ticket) = pending();
    for (restriction, expected) in [(200, 200), (300, 200), (100, 100), (150, 100), (0, 0)] {
        ticket.restrict_claim_expiry_epoch_seconds(restriction);
        assert_eq!(ticket.claim_expiry_epoch_seconds, Some(expected));
        assert_eq!(permit.state(), AtomicCommitState::Pending);
    }
    assert_eq!(
        ticket
            .try_claim_with_samples(now, Some(UNIX_EPOCH))
            .expect_err("tightest horizon reached"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
}

#[test]
fn claim_is_allowed_only_strictly_before_the_epoch_horizon() {
    for (sampled, allowed) in [
        (
            epoch(99)
                .checked_add(Duration::from_nanos(999_999_999))
                .expect("just before horizon"),
            true,
        ),
        (epoch(100), false),
        (epoch(101), false),
    ] {
        let (now, permit, mut ticket) = pending();
        ticket.restrict_claim_expiry_epoch_seconds(100);
        match ticket.try_claim_with_samples(now, Some(sampled)) {
            Ok(claim) => {
                assert!(allowed);
                assert_eq!(permit.state(), AtomicCommitState::Started);
                claim.finish(AtomicCommitDecision::Committed);
                assert_eq!(permit.state(), AtomicCommitState::Committed);
            }
            Err(error) => {
                assert!(!allowed);
                assert_eq!(error, AtomicCommitClaimError::Aborted);
                assert_eq!(permit.state(), AtomicCommitState::Aborted);
            }
        }
    }
}

#[test]
fn restricted_tickets_fail_closed_for_unrepresentable_or_missing_epoch() {
    let invalid = UNIX_EPOCH
        .checked_sub(Duration::from_secs(1))
        .expect("before epoch");
    for sampled in [None, Some(invalid)] {
        let (now, permit, mut ticket) = pending();
        ticket.restrict_claim_expiry_epoch_seconds(u64::MAX);
        assert_eq!(
            ticket
                .try_claim_with_samples(now, sampled)
                .expect_err("no valid epoch"),
            AtomicCommitClaimError::Aborted
        );
        assert_eq!(permit.state(), AtomicCommitState::Aborted);
    }
}

#[test]
fn monotonic_deadline_remains_an_independent_claim_limit() {
    let (now, permit, mut ticket) = pending();
    ticket.restrict_claim_expiry_epoch_seconds(100);
    assert_eq!(
        ticket
            .try_claim_with_samples(permit.0.deadline, Some(epoch(99)))
            .expect_err("monotonic deadline reached"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);

    let (permit, mut ticket) =
        AtomicCommitPermit::new(now.checked_add(Duration::from_secs(120)).expect("deadline"));
    ticket.restrict_claim_expiry_epoch_seconds(100);
    assert_eq!(
        ticket
            .try_claim_with_samples(now, Some(epoch(100)))
            .expect_err("epoch reached despite monotonic time remaining"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
}

#[test]
fn late_epoch_expiry_cannot_abort_started_or_final_owner_authority() {
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
        let (now, permit, mut ticket) = pending();
        ticket.restrict_claim_expiry_epoch_seconds(100);
        let claim = ticket
            .try_claim_with_samples(now, Some(epoch(99)))
            .expect("claim before both limits");
        let duplicate = AtomicCommitTicket {
            permit: permit.clone(),
            claim_expiry_epoch_seconds: Some(100),
        };
        assert_eq!(
            duplicate
                .try_claim_with_samples(now, Some(epoch(100)))
                .expect_err("expiry cannot revoke started authority"),
            AtomicCommitClaimError::Unavailable
        );
        assert_eq!(permit.state(), AtomicCommitState::Started);
        assert!(!permit.abort());
        claim.finish(decision);
        assert_eq!(permit.state(), expected);
        drop(permit.abort_on_drop());
        assert_eq!(permit.state(), expected);
    }
}

#[test]
fn public_claim_samples_a_present_expiry_and_preserves_default_claim_behavior() {
    let (_, permit, mut ticket) = pending();
    ticket.restrict_claim_expiry_epoch_seconds(0);
    assert_eq!(
        ticket.try_claim().expect_err("epoch zero has expired"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);

    let (_, permit, ticket) = pending();
    let claim = ticket
        .try_claim()
        .expect("default has no epoch restriction");
    claim.finish(AtomicCommitDecision::Committed);
    assert_eq!(permit.state(), AtomicCommitState::Committed);
}
