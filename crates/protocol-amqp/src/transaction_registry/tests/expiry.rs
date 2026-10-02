use super::*;
use crate::AtomicCommitClaimError;

#[test]
fn both_discharge_handoffs_tighten_the_same_unique_ticket() -> TestResult {
    for bound in [false, true] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        if bound {
            registry.try_stage(
                &controller,
                &id,
                binding("tenant", "orders", 7),
                send("retained"),
            )?;
        }
        let charged = handle.work_usage();
        let mut submission = take_submission(registry.discharge(&controller, &id, false)?);
        assert_eq!(
            matches!(&submission, AtomicTransactionSubmission::Bound(_)),
            bound
        );
        submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
        submission.restrict_claim_expiry_epoch_seconds(0);
        submission.restrict_claim_expiry_epoch_seconds(u64::MAX);
        assert_eq!(submission.permit().state(), AtomicCommitState::Pending);
        assert_eq!(
            registry.state(&controller, &id)?,
            AtomicCommitState::Pending
        );
        assert_eq!(handle.work_usage(), charged);
        let (ticket, mut work) = match submission {
            AtomicTransactionSubmission::Bound(submission) => {
                let (owner_binding, ticket, work) = submission.into_owner_parts();
                assert_eq!(owner_binding, binding("tenant", "orders", 7));
                (ticket, work)
            }
            AtomicTransactionSubmission::Empty(submission) => submission.into_owner_parts(),
        };
        assert_eq!(
            ticket
                .try_claim()
                .expect_err("enum restriction survives handoff"),
            AtomicCommitClaimError::Aborted
        );
        assert_eq!(
            registry.state(&controller, &id)?,
            AtomicCommitState::Aborted
        );
        assert_state(
            registry.discharge(&controller, &id, false)?,
            AtomicCommitState::Aborted,
        );
        assert_eq!(handle.work_usage(), charged);
        work.with_commands(|commands| {
            assert_eq!(commands.len(), usize::from(bound));
            assert_eq!(handle.work_usage(), charged);
        })?;
        assert_eq!(handle.work_usage(), charged);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn unfenced_bound_and_empty_handoffs_keep_existing_owner_claim_behavior() -> TestResult {
    for bound in [false, true] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        if bound {
            registry.try_stage(
                &controller,
                &id,
                binding("tenant", "orders", 1),
                send("unfenced"),
            )?;
        }
        let submission = take_submission(registry.discharge(&controller, &id, false)?);
        let (ticket, work) = match submission {
            AtomicTransactionSubmission::Bound(submission) => {
                let (_, ticket, work) = submission.into_owner_parts();
                (ticket, work)
            }
            AtomicTransactionSubmission::Empty(submission) => submission.into_owner_parts(),
        };
        let claim = ticket.try_claim()?;
        assert_eq!(
            registry.state(&controller, &id)?,
            AtomicCommitState::Started
        );
        claim.finish(AtomicCommitDecision::Committed);
        assert_eq!(
            registry.state(&controller, &id)?,
            AtomicCommitState::Committed
        );
        assert_state(
            registry.discharge(&controller, &id, false)?,
            AtomicCommitState::Committed,
        );
        assert_eq!(handle.work_usage().groups(), 1);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}
