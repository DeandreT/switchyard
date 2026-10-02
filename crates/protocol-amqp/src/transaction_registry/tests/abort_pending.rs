use std::sync::Barrier;

use super::*;
use crate::{AtomicCommitClaimError, AtomicMessagingOwnerWork};

fn terminal<'a>(registry: &'a AtomicTransactionRegistry, id: &TransactionId) -> &'a Terminal {
    let id = id_number(id);
    registry
        .terminals
        .iter()
        .find(|terminal| terminal.id == id)
        .expect("retained terminal identity")
}

fn parts(
    submission: AtomicTransactionSubmission,
) -> (AtomicCommitTicket, AtomicMessagingOwnerWork) {
    match submission {
        AtomicTransactionSubmission::Bound(submission) => {
            let (_, ticket, work) = submission.into_owner_parts();
            (ticket, work)
        }
        AtomicTransactionSubmission::Empty(submission) => submission.into_owner_parts(),
    }
}

#[test]
fn targeted_refusal_leaves_the_first_actual_discharge_flag_unset() -> TestResult {
    for fail in [false, true] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let refused = registry.declare(&controller)?;
        let sibling = registry.declare(&controller)?;
        registry.try_stage(
            &controller,
            &sibling,
            binding("tenant", "sibling", 1),
            send("healthy"),
        )?;
        let mut invalid = send("not-staged");
        if let CommandKind::Send { session_id, .. } = &mut invalid {
            *session_id = Some(SessionId::new("unsupported")?);
        }
        assert!(
            registry
                .try_stage(
                    &controller,
                    &refused,
                    binding("tenant", "refused", 1),
                    invalid
                )
                .is_err()
        );
        let before = handle.work_usage();
        assert_eq!(
            registry.abort_pending(&controller, &refused)?,
            AtomicCommitState::Aborted
        );
        assert_eq!(terminal(&registry, &refused).first_fail, None);
        assert_eq!(
            terminal(&registry, &refused).abort_cause,
            Some(AbortCause::StagingRefused)
        );
        assert_eq!(
            registry.abort_pending(&controller, &refused)?,
            AtomicCommitState::Aborted
        );
        assert_eq!(terminal(&registry, &refused).first_fail, None);
        assert!(controller.is_active());
        assert_eq!(
            registry.state(&controller, &sibling)?,
            AtomicCommitState::Pending
        );
        assert_eq!(handle.work_usage().groups(), before.groups() - 1);
        assert_eq!(handle.work_usage().content_bytes(), before.content_bytes());
        assert_state(
            registry.discharge(&controller, &refused, fail)?,
            AtomicCommitState::Aborted,
        );
        assert_eq!(terminal(&registry, &refused).first_fail, Some(fail));
        assert_state(
            registry.discharge(&controller, &refused, fail)?,
            AtomicCommitState::Aborted,
        );
        assert!(matches!(
            registry.discharge(&controller, &refused, !fail),
            Err(AtomicTransactionRegistryError::DischargeConflict)
        ));
        assert_eq!(
            registry.abort_pending(&controller, &refused)?,
            AtomicCommitState::Aborted
        );
        assert_eq!(terminal(&registry, &refused).first_fail, Some(fail));
        registry.abort_pending(&controller, &sibling)?;
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn staging_payload_refunds_without_retaining_binding_or_private_debug_values() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    registry.try_stage(
        &controller,
        &id,
        binding("private-namespace", "private-target", 9),
        send("private-producer-value"),
    )?;
    assert!(handle.work_usage().content_bytes() > 0);
    assert_eq!(
        registry.abort_pending(&controller, &id)?,
        AtomicCommitState::Aborted
    );
    no_work(&handle);
    assert!(registry.entries.is_empty());
    assert_eq!(registry.terminals.len(), 1);
    assert_eq!(terminal(&registry, &id).first_fail, None);
    for debug in [format!("{registry:?}"), format!("{handle:?}")] {
        for private in [
            "private-namespace",
            "private-target",
            "private-producer-value",
        ] {
            assert!(!debug.contains(private));
        }
    }
    Ok(())
}

#[test]
fn provenance_is_exact_and_closed_cleanup_remains_observational() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let other = registry.controller()?;
    let id = registry.declare(&controller)?;
    let (mut foreign, _) = fresh();
    let foreign_controller = foreign.controller()?;
    assert!(matches!(
        registry.abort_pending(&foreign_controller, &id),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    assert!(matches!(
        registry.abort_pending(&other, &id),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    for bytes in [
        vec![],
        vec![1],
        vec![1; 7],
        vec![1; 9],
        vec![1; 32],
        vec![0; 8],
        99_u64.to_be_bytes().to_vec(),
    ] {
        assert!(matches!(
            registry.abort_pending(&controller, &TransactionId::new(bytes)?),
            Err(AtomicTransactionRegistryError::UnknownId)
        ));
    }
    assert_eq!(
        registry.state(&controller, &id)?,
        AtomicCommitState::Pending
    );
    assert!(registry.terminals.is_empty());
    assert_eq!(handle.work_usage().groups(), 1);
    registry.close_controller(&controller)?;
    assert!(!controller.is_active());
    assert_eq!(
        registry.abort_pending(&controller, &id)?,
        AtomicCommitState::Aborted
    );
    assert_eq!(
        terminal(&registry, &id).abort_cause,
        Some(AbortCause::ControllerClosed)
    );
    assert_eq!(terminal(&registry, &id).first_fail, None);
    let connection_id = registry.declare(&other)?;
    registry.close();
    assert!(!handle.is_active());
    assert_eq!(
        registry.abort_pending(&other, &connection_id)?,
        AtomicCommitState::Aborted
    );
    assert_eq!(
        terminal(&registry, &connection_id).abort_cause,
        Some(AbortCause::ConnectionClosed)
    );
    assert!(matches!(
        registry.abort_pending(&foreign_controller, &connection_id),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    no_work(&handle);
    Ok(())
}

#[test]
fn queued_aborts_preserve_real_commit_flag_and_outside_leases_through_eviction() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let mut queued = Vec::new();
    let mut ids = Vec::new();
    for index in 0..MAX_ATOMIC_WORK_GROUPS {
        let id = registry.declare(&controller)?;
        registry.try_stage(
            &controller,
            &id,
            binding("tenant", "queue", 1),
            send(&format!("queued-{index}")),
        )?;
        let submission = take_submission(registry.discharge(&controller, &id, false)?);
        let before = handle.work_usage();
        assert_eq!(
            registry.abort_pending(&controller, &id)?,
            AtomicCommitState::Aborted
        );
        assert_eq!(submission.permit().state(), AtomicCommitState::Aborted);
        assert_eq!(
            handle.work_usage(),
            before,
            "outside owner retains the aborted reservation"
        );
        assert_eq!(terminal(&registry, &id).first_fail, Some(false));
        assert_state(
            registry.discharge(&controller, &id, false)?,
            AtomicCommitState::Aborted,
        );
        assert!(matches!(
            registry.discharge(&controller, &id, true),
            Err(AtomicTransactionRegistryError::DischargeConflict)
        ));
        queued.push(submission);
        ids.push(id);
    }
    assert!(registry.entries.is_empty());
    assert_eq!(handle.work_usage().groups(), MAX_ATOMIC_WORK_GROUPS);
    assert!(matches!(
        registry.declare(&controller),
        Err(AtomicTransactionRegistryError::Work(
            AtomicMessagingWorkError::Group {
                maximum: MAX_ATOMIC_WORK_GROUPS
            }
        ))
    ));
    drop(queued.pop().expect("one outside owner"));
    assert_eq!(handle.work_usage().groups(), MAX_ATOMIC_WORK_GROUPS - 1);
    for _ in 0..=MAX_ATOMIC_TRANSACTION_TERMINALS {
        let id = registry.declare(&controller)?;
        registry.abort_pending(&controller, &id)?;
        assert!(registry.terminals.len() <= MAX_ATOMIC_TRANSACTION_TERMINALS);
    }
    assert!(matches!(
        registry.abort_pending(&controller, &ids[0]),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert_eq!(
        handle.work_usage().groups(),
        MAX_ATOMIC_WORK_GROUPS - 1,
        "eviction does not refund an outside job"
    );
    let last = registry.declare(&controller)?;
    assert!(matches!(
        registry.declare(&controller),
        Err(AtomicTransactionRegistryError::Work(
            AtomicMessagingWorkError::Group {
                maximum: MAX_ATOMIC_WORK_GROUPS
            }
        ))
    ));
    drop(queued);
    assert_eq!(handle.work_usage().groups(), 1);
    registry.abort_pending(&controller, &last)?;
    no_work(&handle);
    Ok(())
}

#[test]
fn queued_abort_is_safe_inside_the_owned_payload_callback() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    registry.try_stage(
        &controller,
        &id,
        binding("tenant", "queue", 1),
        send("callback-private-value"),
    )?;
    let (ticket, mut work) = parts(take_submission(registry.discharge(
        &controller,
        &id,
        false,
    )?));
    let before = handle.work_usage();
    work.with_commands(|commands| {
        assert_eq!(
            registry
                .abort_pending(&controller, &id)
                .expect("targeted callback cleanup"),
            AtomicCommitState::Aborted
        );
        assert_eq!(commands, vec![send("callback-private-value")]);
        assert_eq!(
            handle.work_usage(),
            before,
            "callbacks and payload destruction run outside the budget mutex"
        );
        assert!(!format!("{registry:?}").contains("callback-private-value"));
        drop(commands);
        assert_eq!(handle.work_usage(), before);
    })?;
    assert!(matches!(
        ticket.try_claim(),
        Err(AtomicCommitClaimError::Aborted)
    ));
    drop(work);
    no_work(&handle);
    Ok(())
}

#[test]
fn claimed_and_final_groups_cannot_be_reversed_or_given_a_refusal_cause() -> TestResult {
    for decision in [
        None,
        Some(AtomicCommitDecision::Committed),
        Some(AtomicCommitDecision::Rejected),
        Some(AtomicCommitDecision::Indeterminate),
    ] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        let (ticket, work) = parts(take_submission(registry.discharge(
            &controller,
            &id,
            false,
        )?));
        let claim = ticket.try_claim()?;
        assert_eq!(
            registry.abort_pending(&controller, &id)?,
            AtomicCommitState::Started
        );
        assert_eq!(registry.entries[&id_number(&id)].first_fail, Some(false));
        assert_eq!(registry.entries[&id_number(&id)].abort_cause, None);
        let expected = match decision {
            Some(AtomicCommitDecision::Committed) => AtomicCommitState::Committed,
            Some(AtomicCommitDecision::Rejected) => AtomicCommitState::Rejected,
            Some(AtomicCommitDecision::Indeterminate) | None => AtomicCommitState::Indeterminate,
        };
        if let Some(decision) = decision {
            claim.finish(decision);
        } else {
            drop(claim);
        }
        assert_eq!(registry.abort_pending(&controller, &id)?, expected);
        assert_eq!(terminal(&registry, &id).first_fail, Some(false));
        assert_eq!(terminal(&registry, &id).abort_cause, None);
        assert_eq!(registry.abort_pending(&controller, &id)?, expected);
        assert_state(registry.discharge(&controller, &id, false)?, expected);
        assert!(matches!(
            registry.discharge(&controller, &id, true),
            Err(AtomicTransactionRegistryError::DischargeConflict)
        ));
        assert_eq!(handle.work_usage().groups(), 1);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn pending_abort_and_owner_claim_have_exactly_one_winner() -> TestResult {
    for _ in 0..32 {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        let (ticket, work) = parts(take_submission(registry.discharge(
            &controller,
            &id,
            false,
        )?));
        let barrier = Arc::new(Barrier::new(2));
        let claiming = barrier.clone();
        let (state, claim) = std::thread::scope(|scope| {
            let owner = scope.spawn(move || {
                claiming.wait();
                ticket.try_claim()
            });
            barrier.wait();
            let state = registry.abort_pending(&controller, &id);
            (state, owner.join().expect("owner thread"))
        });
        match claim {
            Ok(claim) => {
                assert_eq!(state?, AtomicCommitState::Started);
                assert_eq!(registry.entries[&id_number(&id)].abort_cause, None);
                claim.finish(AtomicCommitDecision::Committed);
                assert_eq!(
                    registry.abort_pending(&controller, &id)?,
                    AtomicCommitState::Committed
                );
            }
            Err(error) => {
                assert_eq!(error, AtomicCommitClaimError::Aborted);
                assert_eq!(state?, AtomicCommitState::Aborted);
                assert_eq!(
                    terminal(&registry, &id).abort_cause,
                    Some(AbortCause::StagingRefused)
                );
            }
        }
        assert_eq!(terminal(&registry, &id).first_fail, Some(false));
        assert_eq!(handle.work_usage().groups(), 1);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn deadline_and_prior_cancellation_causes_win_without_deadline_extension() -> TestResult {
    for queued in [false, true] {
        for at_deadline in [false, true] {
            let (mut registry, handle) = fresh();
            let controller = registry.controller()?;
            let now = Instant::now();
            let id = registry.declare_at(&controller, now)?;
            let deadline = now + ATOMIC_TRANSACTION_TIMEOUT;
            let submission = if queued {
                Some(take_submission(registry.discharge_at(
                    &controller,
                    &id,
                    false,
                    now,
                )?))
            } else {
                None
            };
            assert_eq!(registry.entries[&id_number(&id)].deadline, deadline);
            let target = if at_deadline {
                deadline
            } else {
                deadline
                    .checked_sub(Duration::from_nanos(1))
                    .expect("before deadline")
            };
            assert_eq!(
                registry.abort_pending_at(&controller, &id, target)?,
                AtomicCommitState::Aborted
            );
            let expected = if at_deadline {
                AbortCause::TimedOut
            } else {
                AbortCause::StagingRefused
            };
            assert_eq!(terminal(&registry, &id).abort_cause, Some(expected));
            assert_eq!(terminal(&registry, &id).first_fail, queued.then_some(false));
            assert_eq!(
                registry.abort_pending_at(&controller, &id, deadline + Duration::from_secs(1))?,
                AtomicCommitState::Aborted
            );
            assert_eq!(terminal(&registry, &id).abort_cause, Some(expected));
            assert_eq!(handle.work_usage().groups(), usize::from(queued));
            drop(submission);
            no_work(&handle);
        }
    }
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let now = Instant::now();
    let id = registry.declare_at(&controller, now)?;
    let submission = take_submission(registry.discharge_at(&controller, &id, false, now)?);
    drop(submission);
    assert_eq!(
        registry.abort_pending_at(&controller, &id, now + ATOMIC_TRANSACTION_TIMEOUT)?,
        AtomicCommitState::Aborted
    );
    assert_eq!(
        terminal(&registry, &id).abort_cause,
        Some(AbortCause::Canceled)
    );
    assert_eq!(terminal(&registry, &id).first_fail, Some(false));
    no_work(&handle);
    Ok(())
}
