use super::*;
use crate::AtomicCommitClaimError;

#[test]
fn bound_submission_preserves_tightening_and_reservation_through_owner_parts() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("bound stage");
    stage.try_push(send(43)).expect("retained payload");
    let usage = stage.usage();
    let charged = budget.usage();
    let (permit, ticket) = pending();
    let mut submission = stage.into_submission(ticket);
    submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
    submission.restrict_claim_expiry_epoch_seconds(0);
    submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
    assert_eq!(submission.permit().state(), AtomicCommitState::Pending);
    assert_eq!(budget.usage(), charged);

    let (owner_binding, ticket, mut work) = submission.into_owner_parts();
    assert_eq!(owner_binding, binding());
    assert_eq!(work.usage, usage);
    assert_eq!(budget.usage(), charged);
    assert_eq!(
        ticket
            .try_claim()
            .expect_err("tightest horizon moved to owner"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(budget.usage(), charged);
    work.with_commands(|commands| {
        assert_eq!(commands.len(), 1);
        assert!(matches!(&commands[0], CommandKind::Send { body, .. } if body.len() == 43));
        assert_eq!(budget.usage(), charged);
    })
    .expect("owner work remains unique");
    assert_eq!(budget.usage(), charged);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn empty_submission_preserves_tightening_without_early_refund_or_state_change() {
    let budget = AtomicMessagingWorkBudget::new();
    let stage = budget.stage_unbound().expect("empty slot");
    let charged = budget.usage();
    let (permit, ticket) = pending();
    let mut submission = stage.into_empty_submission(ticket).expect("empty handoff");
    submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
    submission.restrict_claim_expiry_epoch_seconds(0);
    submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
    assert_eq!(permit.state(), AtomicCommitState::Pending);
    assert_eq!(budget.usage(), charged);

    let (ticket, mut work) = submission.into_owner_parts();
    assert_eq!(
        ticket
            .try_claim()
            .expect_err("empty horizon moved to owner"),
        AtomicCommitClaimError::Aborted
    );
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(budget.usage(), charged);
    work.with_commands(|commands| {
        assert!(commands.is_empty());
        assert_eq!(budget.usage(), charged);
    })
    .expect("empty owner callback");
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn ticket_restrictions_before_handoff_are_not_replaced_by_submission_restrictions() {
    for empty in [false, true] {
        let budget = AtomicMessagingWorkBudget::new();
        let (permit, mut ticket) = pending();
        ticket.restrict_claim_expiry_epoch_seconds(0);
        let (ticket, work) = if empty {
            let mut submission = budget
                .stage_unbound()
                .expect("empty stage")
                .into_empty_submission(ticket)
                .expect("empty handoff");
            submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
            submission.into_owner_parts()
        } else {
            let mut submission = budget
                .stage(binding())
                .expect("bound stage")
                .into_submission(ticket);
            submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
            let (_, ticket, work) = submission.into_owner_parts();
            (ticket, work)
        };
        assert_eq!(permit.state(), AtomicCommitState::Pending);
        assert_eq!(budget.usage().groups(), 1);
        assert_eq!(
            ticket
                .try_claim()
                .expect_err("original restriction retained"),
            AtomicCommitClaimError::Aborted
        );
        assert_eq!(budget.usage().groups(), 1);
        drop(work);
        assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    }
}
