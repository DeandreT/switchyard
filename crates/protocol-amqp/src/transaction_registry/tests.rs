use super::*;
use crate::{AtomicCommitDecision, AtomicMessagingWorkUsage, MAX_ATOMIC_WORK_GROUPS};
use domain::{
    AtomicMessagingLimit, BrokerError, EntityPath, LockToken, MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
    MAX_ATOMIC_MESSAGING_VALUE_ITEMS, MessageBody, MessageEnvelope, MessageValue, NamespaceName,
    SequenceNumber, SessionId, SubscriptionName,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn fresh() -> (AtomicTransactionRegistry, AtomicTransactionRegistryHandle) {
    AtomicTransactionRegistry::with_ids(Arc::new(AtomicU64::new(0)))
}

fn binding(namespace: &str, entity: &str, generation: u64) -> EntityBinding {
    let namespace = NamespaceName::new(namespace).expect("namespace");
    let entity = EntityPath::new(entity).expect("queue path");
    EntityBinding::new(
        namespace,
        entity.clone(),
        entity,
        EntityIncarnationKind::Queue,
        generation,
    )
    .expect("primary queue binding")
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn raw(body: Vec<u8>) -> CommandKind {
    CommandKind::Send {
        message_id: String::new(),
        body,
        time_to_live_millis: None,
        session_id: None,
    }
}

fn take_submission(discharge: AtomicTransactionDischarge) -> AtomicTransactionSubmission {
    let AtomicTransactionDischarge::Submit(submission) = discharge else {
        panic!("first commit discharge must move unique work")
    };
    submission
}

fn assert_state(discharge: AtomicTransactionDischarge, expected: AtomicCommitState) {
    assert!(matches!(discharge, AtomicTransactionDischarge::State(state) if state == expected));
}

fn id_number(id: &TransactionId) -> u64 {
    AtomicTransactionRegistry::id_key(id).expect("issued eight-byte identifier")
}

fn no_work(handle: &AtomicTransactionRegistryHandle) {
    assert_eq!(handle.work_usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn declaration_reserves_one_unbound_slot_and_inert_clones_do_not_close_it() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let clone = controller.clone();
    let observed = handle.clone();
    let id = registry.declare(&controller)?;
    assert_eq!(id.as_bytes(), 1_u64.to_be_bytes());
    assert_eq!(registry.state(&clone, &id)?, AtomicCommitState::Pending);
    assert_eq!(handle.work_usage().groups(), 1);
    assert_eq!(handle.work_usage().content_bytes(), 0);
    assert_eq!(handle.work_usage().value_items(), 0);
    drop(clone);
    drop(observed);
    assert!(controller.is_active());
    assert!(handle.is_active());
    assert_eq!(
        registry.state(&controller, &id)?,
        AtomicCommitState::Pending
    );
    assert_state(
        registry.discharge(&controller, &id, true)?,
        AtomicCommitState::Aborted,
    );
    no_work(&handle);
    Ok(())
}

#[test]
fn foreign_connection_controller_and_same_connection_wrong_controller_cannot_use_ids() -> TestResult
{
    let ids = Arc::new(AtomicU64::new(0));
    let (mut first, _) = AtomicTransactionRegistry::with_ids(Arc::clone(&ids));
    let (mut second, _) = AtomicTransactionRegistry::with_ids(ids);
    let first_controller = first.controller()?;
    let other_controller = first.controller()?;
    let second_controller = second.controller()?;
    let first_id = first.declare(&first_controller)?;
    let second_id = second.declare(&second_controller)?;
    assert_ne!(
        first_id, second_id,
        "shared process allocator does not reuse IDs"
    );
    assert!(matches!(
        first.state(&second_controller, &first_id),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    assert!(matches!(
        first.declare(&second_controller),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    assert!(matches!(
        first.close_controller(&second_controller),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    assert!(matches!(
        first.state(&other_controller, &first_id),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert!(matches!(
        first.discharge(&other_controller, &first_id, true),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert!(matches!(
        first.state(&first_controller, &second_id),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert!(matches!(
        first.try_stage(
            &other_controller,
            &first_id,
            binding("tenant", "orders", 1),
            send("secret")
        ),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert_eq!(
        first.state(&first_controller, &first_id)?,
        AtomicCommitState::Pending
    );
    Ok(())
}

#[test]
fn unissued_binary_widths_and_zero_are_unknown_not_implicit_declarations() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    for bytes in [
        vec![],
        vec![1],
        vec![1; 7],
        vec![1; 9],
        vec![1; 32],
        vec![0; 8],
        99_u64.to_be_bytes().to_vec(),
    ] {
        let id = TransactionId::new(bytes)?;
        assert!(matches!(
            registry.state(&controller, &id),
            Err(AtomicTransactionRegistryError::UnknownId)
        ));
        assert!(matches!(
            registry.discharge(&controller, &id, false),
            Err(AtomicTransactionRegistryError::UnknownId)
        ));
        assert!(matches!(
            registry.try_stage(
                &controller,
                &id,
                binding("tenant", "orders", 1),
                send("ignored")
            ),
            Err(AtomicTransactionRegistryError::UnknownId)
        ));
    }
    assert!(registry.entries.is_empty());
    assert!(registry.terminals.is_empty());
    no_work(&handle);
    Ok(())
}

#[test]
fn failed_first_input_stays_unbound_and_a_healthy_different_binding_can_follow() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let reserved = handle.work_usage();
    let mut unsupported = send("rejected");
    if let CommandKind::Send { session_id, .. } = &mut unsupported {
        *session_id = Some(SessionId::new("session")?);
    }
    assert!(matches!(
        registry.try_stage(
            &controller,
            &id,
            binding("tenant", "rejected", 1),
            unsupported
        ),
        Err(AtomicTransactionRegistryError::Work(
            AtomicMessagingWorkError::Input(BrokerError::AtomicMessagingOperationNotSupported)
        ))
    ));
    assert_eq!(handle.work_usage(), reserved);
    let Phase::Staging {
        binding: current, ..
    } = &registry.entries[&id_number(&id)].phase
    else {
        panic!("staged entry")
    };
    assert!(current.is_none());
    let selected = binding("other", "healthy", 9);
    registry.try_stage(&controller, &id, selected.clone(), send("retained"))?;
    assert!(matches!(
        registry.try_stage(
            &controller,
            &id,
            binding("tenant", "rejected", 1),
            send("second")
        ),
        Err(AtomicTransactionRegistryError::BindingMismatch)
    ));
    let AtomicTransactionSubmission::Bound(submission) =
        take_submission(registry.discharge(&controller, &id, false)?)
    else {
        panic!("successful first action binds")
    };
    let (actual, ticket, mut work) = submission.into_owner_parts();
    assert_eq!(actual, selected);
    assert_eq!(
        work.with_commands(|commands| commands)?,
        vec![send("retained")]
    );
    drop(ticket);
    drop(work);
    no_work(&handle);
    Ok(())
}

#[test]
fn established_binding_requires_exact_namespace_path_case_owner_and_generation() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let selected = binding("tenant", "Orders", 7);
    registry.try_stage(&controller, &id, selected.clone(), send("first"))?;
    let before = handle.work_usage();
    for candidate in [
        binding("other", "Orders", 7),
        binding("tenant", "orders", 7),
        binding("tenant", "elsewhere", 7),
        binding("tenant", "Orders", 8),
    ] {
        assert!(matches!(
            registry.try_stage(&controller, &id, candidate, send("rejected")),
            Err(AtomicTransactionRegistryError::BindingMismatch)
        ));
        assert_eq!(handle.work_usage(), before);
    }
    registry.try_stage(&controller, &id, selected.clone(), send("second"))?;
    let AtomicTransactionSubmission::Bound(submission) =
        take_submission(registry.discharge(&controller, &id, false)?)
    else {
        panic!("bound work")
    };
    let (actual, ticket, mut work) = submission.into_owner_parts();
    assert_eq!(actual, selected);
    assert_eq!(
        work.with_commands(|commands| commands)?,
        vec![send("first"), send("second")]
    );
    drop(ticket);
    drop(work);
    no_work(&handle);
    Ok(())
}

#[test]
fn topic_subscription_and_deadletter_targets_refuse_without_binding_or_charge() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let namespace = NamespaceName::new("tenant")?;
    let parent = EntityPath::new("orders")?;
    let subscription = parent.subscription(&SubscriptionName::new("Alpha")?)?;
    let targets = [
        EntityBinding::new(
            namespace.clone(),
            parent.clone(),
            parent.clone(),
            EntityIncarnationKind::Topic,
            1,
        )?,
        EntityBinding::new(
            namespace.clone(),
            subscription.clone(),
            subscription,
            EntityIncarnationKind::Subscription,
            1,
        )?,
        EntityBinding::new(
            namespace,
            parent.dead_letter_queue()?,
            parent,
            EntityIncarnationKind::Queue,
            1,
        )?,
    ];
    let before = handle.work_usage();
    for target in targets {
        assert!(matches!(
            registry.try_stage(&controller, &id, target, send("ignored")),
            Err(AtomicTransactionRegistryError::UnsupportedTarget)
        ));
        assert_eq!(handle.work_usage(), before);
    }
    registry.try_stage(
        &controller,
        &id,
        binding("tenant", "healthy", 1),
        send("retained"),
    )?;
    Ok(())
}

#[test]
fn successful_empty_batch_and_settlement_are_bound_actions_not_unbound_empty_work() -> TestResult {
    for kind in [
        CommandKind::SendBatch { messages: vec![] },
        CommandKind::Complete {
            sequence: SequenceNumber::new(99),
            lock_token: LockToken::new(1),
        },
    ] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        let selected = binding("tenant", "orders", 1);
        registry.try_stage(&controller, &id, selected.clone(), kind.clone())?;
        assert_eq!(handle.work_usage().groups(), 1);
        assert_eq!(handle.work_usage().content_bytes(), 0);
        assert_eq!(handle.work_usage().value_items(), 0);
        let AtomicTransactionSubmission::Bound(submission) =
            take_submission(registry.discharge(&controller, &id, false)?)
        else {
            panic!("even a zero-content accepted action is bound")
        };
        let (actual, ticket, mut work) = submission.into_owner_parts();
        assert_eq!(actual, selected);
        assert_eq!(work.with_commands(|commands| commands)?, vec![kind]);
        drop(ticket);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn zero_action_commit_is_an_empty_unique_handoff_and_repeat_never_replays() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let AtomicTransactionSubmission::Empty(submission) =
        take_submission(registry.discharge(&controller, &id, false)?)
    else {
        panic!("zero staged actions must remain entity-free")
    };
    assert_state(
        registry.discharge(&controller, &id, false)?,
        AtomicCommitState::Pending,
    );
    assert!(matches!(
        registry.discharge(&controller, &id, true),
        Err(AtomicTransactionRegistryError::DischargeConflict)
    ));
    assert!(matches!(
        registry.try_stage(&controller, &id, binding("tenant", "late", 1), send("late")),
        Err(AtomicTransactionRegistryError::Unavailable)
    ));
    let (ticket, work) = submission.into_owner_parts();
    let claim = ticket.try_claim()?;
    assert_state(
        registry.discharge(&controller, &id, false)?,
        AtomicCommitState::Started,
    );
    claim.finish(AtomicCommitDecision::Committed);
    assert_state(
        registry.discharge(&controller, &id, false)?,
        AtomicCommitState::Committed,
    );
    assert_eq!(
        handle.work_usage().groups(),
        1,
        "decision does not destroy owner work"
    );
    drop(work);
    no_work(&handle);
    assert_state(
        registry.discharge(&controller, &id, false)?,
        AtomicCommitState::Committed,
    );
    assert!(matches!(
        registry.discharge(&controller, &id, true),
        Err(AtomicTransactionRegistryError::DischargeConflict)
    ));
    Ok(())
}

#[test]
fn abort_repeats_are_terminal_and_contradicting_fail_flag_is_refused() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    registry.try_stage(
        &controller,
        &id,
        binding("tenant", "orders", 1),
        send("discarded"),
    )?;
    assert_state(
        registry.discharge(&controller, &id, true)?,
        AtomicCommitState::Aborted,
    );
    no_work(&handle);
    assert_state(
        registry.discharge(&controller, &id, true)?,
        AtomicCommitState::Aborted,
    );
    assert!(matches!(
        registry.discharge(&controller, &id, false),
        Err(AtomicTransactionRegistryError::DischargeConflict)
    ));
    assert_eq!(
        registry
            .terminals
            .back()
            .expect("terminal decision")
            .abort_cause,
        Some(AbortCause::Requested)
    );
    Ok(())
}

#[test]
fn every_owner_terminal_state_replays_only_state_with_the_original_fail_flag() -> TestResult {
    for finish in 0..5 {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let id = registry.declare(&controller)?;
        let AtomicTransactionSubmission::Empty(submission) =
            take_submission(registry.discharge(&controller, &id, false)?)
        else {
            panic!("empty handoff")
        };
        let (ticket, work) = submission.into_owner_parts();
        let observer = ticket.permit().clone();
        let expected = if finish == 4 {
            drop(ticket);
            AtomicCommitState::Aborted
        } else {
            let claim = ticket.try_claim()?;
            match finish {
                0 => {
                    claim.finish(AtomicCommitDecision::Committed);
                    AtomicCommitState::Committed
                }
                1 => {
                    claim.finish(AtomicCommitDecision::Rejected);
                    AtomicCommitState::Rejected
                }
                2 => {
                    claim.finish(AtomicCommitDecision::Indeterminate);
                    AtomicCommitState::Indeterminate
                }
                3 => {
                    drop(claim);
                    AtomicCommitState::Indeterminate
                }
                _ => unreachable!(),
            }
        };
        assert_eq!(observer.state(), expected);
        assert_state(registry.discharge(&controller, &id, false)?, expected);
        assert_state(registry.discharge(&controller, &id, false)?, expected);
        assert!(matches!(
            registry.discharge(&controller, &id, true),
            Err(AtomicTransactionRegistryError::DischargeConflict)
        ));
        assert!(matches!(
            registry.try_stage(&controller, &id, binding("tenant", "late", 1), send("late")),
            Err(AtomicTransactionRegistryError::UnknownId)
        ));
        assert_eq!(registry.terminals.len(), 1);
        assert_eq!(registry.terminals[0].state, expected);
        assert_eq!(registry.terminals[0].first_fail, Some(false));
        assert_eq!(
            handle.work_usage().groups(),
            1,
            "terminal observation does not refund owner work"
        );
        drop(work);
        no_work(&handle);
        assert_state(registry.discharge(&controller, &id, false)?, expected);
    }
    Ok(())
}

#[test]
fn declaration_deadline_is_not_renewed_by_staging_and_equality_expires() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let now = Instant::now();
    let id = registry.declare_at(&controller, now)?;
    let deadline = now + ATOMIC_TRANSACTION_TIMEOUT;
    assert_eq!(registry.entries[&id_number(&id)].deadline, deadline);
    registry.try_stage_at(
        &controller,
        &id,
        binding("tenant", "orders", 1),
        send("near-deadline"),
        deadline - Duration::from_nanos(1),
    )?;
    assert_eq!(registry.entries[&id_number(&id)].deadline, deadline);
    assert_eq!(
        registry.state_at(&controller, &id, deadline - Duration::from_nanos(1))?,
        AtomicCommitState::Pending
    );
    assert_eq!(registry.reap_at(deadline), 1);
    assert_eq!(
        registry.state_at(&controller, &id, deadline)?,
        AtomicCommitState::Aborted
    );
    no_work(&handle);
    assert_eq!(
        registry
            .terminals
            .back()
            .expect("expired decision")
            .abort_cause,
        Some(AbortCause::TimedOut)
    );
    assert_state(
        registry.discharge_at(&controller, &id, false, deadline)?,
        AtomicCommitState::Aborted,
    );
    assert!(matches!(
        registry.discharge_at(&controller, &id, true, deadline),
        Err(AtomicTransactionRegistryError::DischargeConflict)
    ));
    Ok(())
}

#[test]
fn pending_queued_deadline_reaps_observation_but_does_not_refund_owned_work() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let now = Instant::now();
    let id = registry.declare_at(&controller, now)?;
    let submission = take_submission(registry.discharge_at(&controller, &id, false, now)?);
    let observer = submission.permit().clone();
    assert_eq!(registry.reap_at(now + ATOMIC_TRANSACTION_TIMEOUT), 1);
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
    assert_eq!(handle.work_usage().groups(), 1);
    assert!(registry.entries.is_empty());
    assert_eq!(registry.terminals.len(), 1);
    drop(submission);
    no_work(&handle);
    Ok(())
}

#[test]
fn close_and_deadline_cannot_revoke_started_owner_or_refund_its_lease() -> TestResult {
    for close_connection in [false, true] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let now = Instant::now();
        let id = registry.declare_at(&controller, now)?;
        let AtomicTransactionSubmission::Empty(submission) =
            take_submission(registry.discharge_at(&controller, &id, false, now)?)
        else {
            panic!("empty handoff")
        };
        let (ticket, work) = submission.into_owner_parts();
        let observer = ticket.permit().clone();
        let claim = ticket.try_claim()?;
        assert_eq!(registry.reap_at(now + ATOMIC_TRANSACTION_TIMEOUT), 0);
        assert_eq!(observer.state(), AtomicCommitState::Started);
        if close_connection {
            registry.close_at(now + ATOMIC_TRANSACTION_TIMEOUT);
            assert!(!handle.is_active());
        } else {
            registry.close_controller_at(&controller, now + ATOMIC_TRANSACTION_TIMEOUT)?;
            assert!(handle.is_active());
        }
        assert!(!controller.is_active());
        assert_eq!(observer.state(), AtomicCommitState::Started);
        assert!(!observer.abort());
        assert_eq!(handle.work_usage().groups(), 1);
        claim.finish(AtomicCommitDecision::Committed);
        assert_eq!(
            registry.state_at(&controller, &id, now + ATOMIC_TRANSACTION_TIMEOUT)?,
            AtomicCommitState::Committed
        );
        assert_eq!(handle.work_usage().groups(), 1);
        drop(work);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn closing_one_controller_aborts_only_its_pending_entries_and_allows_diagnostics() -> TestResult {
    let (mut registry, handle) = fresh();
    let closed = registry.controller()?;
    let healthy = registry.controller()?;
    let first = registry.declare(&closed)?;
    let second = registry.declare(&healthy)?;
    registry.close_controller(&closed)?;
    assert!(!closed.is_active());
    assert!(healthy.is_active());
    assert_eq!(registry.state(&closed, &first)?, AtomicCommitState::Aborted);
    assert_eq!(
        registry.state(&healthy, &second)?,
        AtomicCommitState::Pending
    );
    assert_eq!(handle.work_usage().groups(), 1);
    assert!(matches!(
        registry.declare(&closed),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    assert!(matches!(
        registry.discharge(&closed, &first, false),
        Err(AtomicTransactionRegistryError::InvalidController)
    ));
    registry.close_controller(&closed)?;
    let replacement = registry.controller()?;
    assert!(replacement.is_active());
    assert!(matches!(
        registry.state(&replacement, &first),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    registry.close();
    assert_eq!(
        registry.state(&healthy, &second)?,
        AtomicCommitState::Aborted
    );
    assert!(matches!(
        registry.controller(),
        Err(AtomicTransactionRegistryError::Closed)
    ));
    no_work(&handle);
    Ok(())
}

#[test]
fn live_slot_cap_includes_aborted_queued_work_until_its_owner_drops() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let mut queued = Vec::new();
    for _ in 0..MAX_ATOMIC_WORK_GROUPS {
        let id = registry.declare(&controller)?;
        let submission = take_submission(registry.discharge(&controller, &id, false)?);
        assert!(submission.permit().abort());
        queued.push(submission);
    }
    registry.expire();
    assert!(registry.entries.is_empty());
    assert_eq!(registry.terminals.len(), MAX_ATOMIC_TRANSACTION_TERMINALS);
    assert_eq!(handle.work_usage().groups(), MAX_ATOMIC_WORK_GROUPS);
    assert!(
        matches!(registry.declare(&controller), Err(AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::Group { maximum })) if maximum == MAX_ATOMIC_WORK_GROUPS)
    );
    drop(queued.pop().expect("one queued owner"));
    assert_eq!(handle.work_usage().groups(), MAX_ATOMIC_WORK_GROUPS - 1);
    let id = registry.declare(&controller)?;
    assert_eq!(handle.work_usage().groups(), MAX_ATOMIC_WORK_GROUPS);
    assert_state(
        registry.discharge(&controller, &id, true)?,
        AtomicCommitState::Aborted,
    );
    drop(queued);
    no_work(&handle);
    Ok(())
}

#[test]
fn live_metadata_stays_bounded_when_trusted_owner_work_is_dropped_early() -> TestResult {
    for started in [false, true] {
        let (mut registry, handle) = fresh();
        let controller = registry.controller()?;
        let mut tickets = Vec::new();
        let mut claims = Vec::new();
        for _ in 0..MAX_ATOMIC_WORK_GROUPS {
            let id = registry.declare(&controller)?;
            let AtomicTransactionSubmission::Empty(submission) =
                take_submission(registry.discharge(&controller, &id, false)?)
            else {
                panic!("empty handoff")
            };
            let (ticket, work) = submission.into_owner_parts();
            drop(work);
            if started {
                claims.push(ticket.try_claim()?);
            } else {
                tickets.push(ticket);
            }
        }
        no_work(&handle);
        assert_eq!(registry.entries.len(), MAX_ATOMIC_WORK_GROUPS);
        assert!(
            matches!(registry.declare(&controller), Err(AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::Group { maximum })) if maximum == MAX_ATOMIC_WORK_GROUPS)
        );
        if started {
            drop(claims.pop().expect("one started claim"));
        } else {
            drop(tickets.pop().expect("one pending ticket"));
        }
        assert_eq!(registry.expire(), 1);
        let replacement = registry.declare(&controller)?;
        assert_eq!(registry.entries.len(), MAX_ATOMIC_WORK_GROUPS);
        assert_eq!(handle.work_usage().groups(), 1);
        assert_state(
            registry.discharge(&controller, &replacement, true)?,
            AtomicCommitState::Aborted,
        );
        drop(tickets);
        drop(claims);
        registry.expire();
        assert!(registry.entries.is_empty());
        assert!(registry.terminals.len() <= MAX_ATOMIC_TRANSACTION_TERMINALS);
        no_work(&handle);
    }
    Ok(())
}

#[test]
fn terminal_cache_is_bounded_and_evicted_ids_never_replay() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let mut completed = Vec::new();
    for _ in 0..MAX_ATOMIC_TRANSACTION_TERMINALS + 1 {
        let id = registry.declare(&controller)?;
        assert_state(
            registry.discharge(&controller, &id, true)?,
            AtomicCommitState::Aborted,
        );
        completed.push(id);
        assert!(registry.terminals.len() <= MAX_ATOMIC_TRANSACTION_TERMINALS);
        no_work(&handle);
    }
    assert_eq!(registry.terminals.len(), MAX_ATOMIC_TRANSACTION_TERMINALS);
    assert!(matches!(
        registry.discharge(&controller, &completed[0], true),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert!(matches!(
        registry.discharge(&controller, &completed[0], false),
        Err(AtomicTransactionRegistryError::UnknownId)
    ));
    assert_state(
        registry.discharge(
            &controller,
            completed.last().expect("newest retained ID"),
            true,
        )?,
        AtomicCommitState::Aborted,
    );
    no_work(&handle);
    Ok(())
}

#[test]
fn shared_content_refusal_does_not_pin_the_refused_group_binding() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let first = registry.declare(&controller)?;
    let second = registry.declare(&controller)?;
    let third = registry.declare(&controller)?;
    registry.try_stage(
        &controller,
        &first,
        binding("tenant", "first", 1),
        raw(vec![1; MAX_ATOMIC_MESSAGING_CONTENT_BYTES]),
    )?;
    registry.try_stage(
        &controller,
        &second,
        binding("tenant", "second", 1),
        raw(vec![2; MAX_ATOMIC_MESSAGING_CONTENT_BYTES]),
    )?;
    let before = handle.work_usage();
    assert!(
        matches!(registry.try_stage(&controller, &third, binding("tenant", "refused", 1), raw(vec![3])), Err(AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::Content { maximum_bytes })) if maximum_bytes == crate::MAX_ATOMIC_WORK_CONTENT_BYTES)
    );
    assert_eq!(handle.work_usage(), before);
    assert_state(
        registry.discharge(&controller, &first, true)?,
        AtomicCommitState::Aborted,
    );
    registry.try_stage(
        &controller,
        &third,
        binding("other", "replacement", 8),
        raw(vec![3]),
    )?;
    assert_eq!(handle.work_usage().groups(), 2);
    assert_eq!(
        handle.work_usage().content_bytes(),
        MAX_ATOMIC_MESSAGING_CONTENT_BYTES + 1
    );
    Ok(())
}

#[test]
fn oversized_input_does_not_bind_or_charge_and_later_empty_commit_is_entity_free() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    let before = handle.work_usage();
    assert!(
        matches!(registry.try_stage(&controller, &id, binding("tenant", "refused", 1), raw(vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES + 1])), Err(AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::Input(BrokerError::AtomicMessagingTooLarge { limit: AtomicMessagingLimit::ContentBytes, maximum }))) if maximum == MAX_ATOMIC_MESSAGING_CONTENT_BYTES)
    );
    assert_eq!(handle.work_usage(), before);
    let AtomicTransactionSubmission::Empty(submission) =
        take_submission(registry.discharge(&controller, &id, false)?)
    else {
        panic!("refusal did not create a bound action")
    };
    drop(submission);
    no_work(&handle);
    Ok(())
}

#[test]
fn shared_value_limit_and_input_failure_keep_existing_reservations_exact() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(registry.declare(&controller)?);
    }
    let values = || CommandKind::SendEnvelope {
        message_id: String::new(),
        body: vec![],
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(MessageEnvelope {
            body: MessageBody::Value(MessageValue::Array(vec![
                MessageValue::Null;
                MAX_ATOMIC_MESSAGING_VALUE_ITEMS
                    - 1
            ])),
            ..MessageEnvelope::default()
        }),
    };
    for id in &ids[..2] {
        registry.try_stage(&controller, id, binding("tenant", "orders", 1), values())?;
    }
    assert_eq!(
        handle.work_usage().value_items(),
        crate::MAX_ATOMIC_WORK_VALUE_ITEMS
    );
    let before = handle.work_usage();
    let one = CommandKind::SendEnvelope {
        message_id: String::new(),
        body: vec![],
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(MessageEnvelope {
            body: MessageBody::Value(MessageValue::Null),
            ..MessageEnvelope::default()
        }),
    };
    assert!(
        matches!(registry.try_stage(&controller, &ids[2], binding("tenant", "refused", 1), one), Err(AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::ValueItem { maximum })) if maximum == crate::MAX_ATOMIC_WORK_VALUE_ITEMS)
    );
    assert_eq!(handle.work_usage(), before);
    registry.try_stage(
        &controller,
        &ids[2],
        binding("other", "healthy", 7),
        CommandKind::SendBatch { messages: vec![] },
    )?;
    assert_eq!(
        handle.work_usage(),
        before,
        "zero-content action consumes its already reserved group only"
    );
    Ok(())
}

#[test]
fn checked_transaction_and_controller_ids_never_wrap_or_leak_a_slot() -> TestResult {
    let allocator = Arc::new(AtomicU64::new(u64::MAX - 1));
    let (mut registry, handle) = AtomicTransactionRegistry::with_ids(Arc::clone(&allocator));
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    assert_eq!(id_number(&id), u64::MAX);
    let before = handle.work_usage();
    assert!(matches!(
        registry.declare(&controller),
        Err(AtomicTransactionRegistryError::IdExhausted)
    ));
    assert_eq!(allocator.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(handle.work_usage(), before);
    assert_eq!(registry.entries.len(), 1);
    registry.last_controller = u64::MAX - 1;
    let last = registry.controller()?;
    assert_eq!(last.inner.generation, u64::MAX);
    assert!(matches!(
        registry.controller(),
        Err(AtomicTransactionRegistryError::IdExhausted)
    ));
    assert!(last.is_active());
    assert_eq!(handle.work_usage(), before);
    Ok(())
}

#[test]
fn debug_snapshots_do_not_print_payload_binding_or_identifier_bytes() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let id = registry.declare(&controller)?;
    registry.try_stage(
        &controller,
        &id,
        binding("private-namespace", "private-entity", 42),
        send("private-message-content"),
    )?;
    let staged_debug = format!("{registry:?} {handle:?} {controller:?}");
    for secret in [
        "private-namespace",
        "private-entity",
        "private-message-content",
    ] {
        assert!(!staged_debug.contains(secret));
    }
    let discharge = registry.discharge(&controller, &id, false)?;
    let queued_debug = format!("{discharge:?} {registry:?} {handle:?}");
    for secret in [
        "private-namespace",
        "private-entity",
        "private-message-content",
    ] {
        assert!(!queued_debug.contains(secret));
    }
    assert!(!queued_debug.contains(&format!("{:?}", id.as_bytes())));
    drop(discharge);
    no_work(&handle);
    Ok(())
}

#[test]
fn dropping_registry_aborts_pending_but_started_owner_can_finish() -> TestResult {
    let (mut registry, handle) = fresh();
    let controller = registry.controller()?;
    let pending = registry.declare(&controller)?;
    let pending_submission = take_submission(registry.discharge(&controller, &pending, false)?);
    let pending_observer = pending_submission.permit().clone();
    let started = registry.declare(&controller)?;
    let AtomicTransactionSubmission::Empty(started_submission) =
        take_submission(registry.discharge(&controller, &started, false)?)
    else {
        panic!("empty handoff")
    };
    let (ticket, work) = started_submission.into_owner_parts();
    let started_observer = ticket.permit().clone();
    let claim = ticket.try_claim()?;
    drop(registry);
    assert!(!handle.is_active());
    assert!(!controller.is_active());
    assert_eq!(pending_observer.state(), AtomicCommitState::Aborted);
    assert_eq!(started_observer.state(), AtomicCommitState::Started);
    assert_eq!(handle.work_usage().groups(), 2);
    drop(pending_submission);
    assert_eq!(handle.work_usage().groups(), 1);
    claim.finish(AtomicCommitDecision::Rejected);
    assert_eq!(started_observer.state(), AtomicCommitState::Rejected);
    drop(work);
    no_work(&handle);
    Ok(())
}
