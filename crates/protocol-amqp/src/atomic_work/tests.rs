use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Barrier,
    time::{Duration, Instant},
};

use domain::{
    AtomicMessagingLimit, EntityIncarnationKind, EntityPath, LockToken,
    MAX_ATOMIC_MESSAGING_ACTIONS, MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
    MAX_ATOMIC_MESSAGING_VALUE_ITEMS, MessageBody, MessageEnvelope, MessageValue, NamespaceName,
    SequenceNumber,
};

use super::*;
use crate::{AtomicCommitDecision, AtomicCommitState};

mod expiry;

fn binding() -> EntityBinding {
    let entity = EntityPath::new("orders").expect("entity");
    EntityBinding::new(
        NamespaceName::new("tests").expect("namespace"),
        entity.clone(),
        entity,
        EntityIncarnationKind::Queue,
        1,
    )
    .expect("binding")
}

fn pending() -> (AtomicCommitPermit, AtomicCommitTicket) {
    AtomicCommitPermit::new(
        Instant::now()
            .checked_add(Duration::from_secs(120))
            .expect("deadline"),
    )
}

fn send(bytes: usize) -> CommandKind {
    CommandKind::Send {
        message_id: String::new(),
        body: vec![0; bytes],
        time_to_live_millis: None,
        session_id: None,
    }
}

fn values(items: usize) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: String::new(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(MessageEnvelope {
            body: MessageBody::Sequence(vec![vec![MessageValue::Null; items]]),
            ..MessageEnvelope::default()
        }),
    }
}

fn complete() -> CommandKind {
    CommandKind::Complete {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(2),
    }
}

#[test]
fn empty_groups_reserve_all_slots_and_release_one_at_a_time() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut groups = (0..MAX_ATOMIC_WORK_GROUPS)
        .map(|_| budget.stage(binding()).expect("empty group"))
        .collect::<Vec<_>>();
    assert_eq!(budget.usage().groups(), MAX_ATOMIC_WORK_GROUPS);
    assert_eq!(budget.usage().content_bytes(), 0);
    assert_eq!(budget.usage().value_items(), 0);
    assert!(groups.iter().all(|group| group.usage().actions() == 0));
    let before = budget.usage();
    assert!(matches!(
        budget.stage(binding()),
        Err(AtomicMessagingWorkError::Group {
            maximum: MAX_ATOMIC_WORK_GROUPS
        })
    ));
    assert_eq!(budget.usage(), before);
    drop(groups.pop());
    assert_eq!(budget.usage().groups(), MAX_ATOMIC_WORK_GROUPS - 1);
    groups.push(budget.stage(binding()).expect("refunded slot"));
    assert_eq!(budget.usage(), before);
    drop(groups);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn competing_stagers_cannot_both_take_the_last_slot() {
    let budget = AtomicMessagingWorkBudget::default();
    let groups = (0..MAX_ATOMIC_WORK_GROUPS - 1)
        .map(|_| budget.stage(binding()).expect("reserved slot"))
        .collect::<Vec<_>>();
    let start = Barrier::new(3);
    let results = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            start.wait();
            budget.stage(binding())
        });
        let second = scope.spawn(|| {
            start.wait();
            budget.stage(binding())
        });
        start.wait();
        [first.join().expect("first"), second.join().expect("second")]
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .any(|result| matches!(result, Err(AtomicMessagingWorkError::Group { .. })))
    );
    assert_eq!(budget.usage().groups(), MAX_ATOMIC_WORK_GROUPS);
    drop(results);
    drop(groups);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn content_cap_is_shared_across_groups_and_refusal_is_unchanged() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut first = budget.stage(binding()).expect("first");
    let mut second = budget.stage(binding()).expect("second");
    let mut third = budget.stage(binding()).expect("third");
    first
        .try_push(send(MAX_ATOMIC_MESSAGING_CONTENT_BYTES))
        .expect("first full group");
    second
        .try_push(send(MAX_ATOMIC_MESSAGING_CONTENT_BYTES))
        .expect("second full group");
    assert_eq!(
        budget.usage().content_bytes(),
        MAX_ATOMIC_WORK_CONTENT_BYTES
    );
    let shared_before = budget.usage();
    let staged_before = third.usage();
    assert!(matches!(
        third.try_push(send(1)),
        Err(AtomicMessagingWorkError::Content {
            maximum_bytes: MAX_ATOMIC_WORK_CONTENT_BYTES
        })
    ));
    assert_eq!(budget.usage(), shared_before);
    assert_eq!(third.usage(), staged_before);
    assert!(third.commands.is_empty());
    drop(first);
    third.try_push(send(1)).expect("retry after refund");
    assert_eq!(third.usage().content_bytes(), 1);
    assert_eq!(
        budget.usage().content_bytes(),
        MAX_ATOMIC_MESSAGING_CONTENT_BYTES + 1
    );
    drop(second);
    drop(third);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn value_item_cap_refuses_without_charging_candidate_bytes() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut first = budget.stage(binding()).expect("first");
    let mut second = budget.stage(binding()).expect("second");
    let mut third = budget.stage(binding()).expect("third");
    first
        .try_push(values(MAX_ATOMIC_MESSAGING_VALUE_ITEMS))
        .expect("first full values");
    second
        .try_push(values(MAX_ATOMIC_MESSAGING_VALUE_ITEMS))
        .expect("second full values");
    assert_eq!(budget.usage().value_items(), MAX_ATOMIC_WORK_VALUE_ITEMS);
    let shared_before = budget.usage();
    let staged_before = third.usage();
    assert!(matches!(
        third.try_push(values(1)),
        Err(AtomicMessagingWorkError::ValueItem {
            maximum: MAX_ATOMIC_WORK_VALUE_ITEMS
        })
    ));
    assert_eq!(budget.usage(), shared_before);
    assert_eq!(third.usage(), staged_before);
    assert!(third.commands.is_empty());
    drop(second);
    third.try_push(values(1)).expect("refunded values");
    assert_eq!(
        budget.usage().value_items(),
        MAX_ATOMIC_MESSAGING_VALUE_ITEMS + 1
    );
    drop(first);
    drop(third);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn input_refusals_preserve_prior_commands_and_all_charges() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage.try_push(send(7)).expect("retained send");
    let staged_before = stage.usage();
    let shared_before = budget.usage();
    for candidate in [
        CommandKind::ExpireLocks,
        send(MAX_ATOMIC_MESSAGING_CONTENT_BYTES + 1),
        values(MAX_ATOMIC_MESSAGING_VALUE_ITEMS + 1),
    ] {
        assert!(matches!(
            stage.try_push(candidate),
            Err(AtomicMessagingWorkError::Input(_))
        ));
        assert_eq!(stage.usage(), staged_before);
        assert_eq!(budget.usage(), shared_before);
        assert_eq!(stage.commands.len(), 1);
        assert!(matches!(&stage.commands[0], CommandKind::Send { body, .. } if body.len() == 7));
    }
    drop(stage);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn zero_content_actions_still_obey_each_groups_action_cap() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    for _ in 0..MAX_ATOMIC_MESSAGING_ACTIONS {
        stage.try_push(complete()).expect("supported action");
    }
    let before = stage.usage();
    assert!(matches!(
        stage.try_push(complete()),
        Err(AtomicMessagingWorkError::Input(
            BrokerError::AtomicMessagingTooLarge {
                limit: AtomicMessagingLimit::Actions,
                maximum: MAX_ATOMIC_MESSAGING_ACTIONS
            }
        ))
    ));
    assert_eq!(stage.usage(), before);
    assert_eq!(stage.commands.len(), MAX_ATOMIC_MESSAGING_ACTIONS);
    assert_eq!(budget.usage().groups(), 1);
    assert_eq!(budget.usage().content_bytes(), 0);
    assert_eq!(budget.usage().value_items(), 0);
}

#[test]
fn stage_records_binding_without_claiming_queue_admission() {
    let budget = AtomicMessagingWorkBudget::new();
    let entity = EntityPath::new("not-created-topic").expect("entity");
    let expected = EntityBinding::new(
        NamespaceName::new("unopened").expect("namespace"),
        entity.clone(),
        entity,
        EntityIncarnationKind::Topic,
        83,
    )
    .expect("well-shaped binding");
    let stage = budget.stage(expected.clone()).expect("work admission only");
    assert_eq!(stage.binding(), &expected);
    assert_eq!(budget.usage().groups(), 1);
}

#[test]
fn observers_do_not_own_or_refund_staged_commands() {
    let original = AtomicMessagingWorkBudget::new();
    let observer = original.clone();
    let mut stage = original.stage(binding()).expect("stage");
    stage.try_push(send(11)).expect("send");
    let charged = observer.usage();
    drop(original);
    drop(observer.clone());
    assert_eq!(observer.usage(), charged);
    drop(stage);
    assert_eq!(observer.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn queued_work_drops_payload_and_refunds_even_while_observers_remain() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage.try_push(send(19)).expect("send");
    let (permit, ticket) = pending();
    let submission = stage.into_submission(ticket);
    let permit_observer = submission.permit().clone();
    assert_eq!(permit_observer.state(), AtomicCommitState::Pending);
    assert_eq!(budget.usage().groups(), 1);
    assert_eq!(budget.usage().content_bytes(), 19);
    drop(submission);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(permit_observer.state(), AtomicCommitState::Aborted);
}

#[test]
fn moves_and_callback_keep_the_charge_until_owner_scope_ends() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage.try_push(send(23)).expect("send");
    let staged_usage = stage.usage();
    let charged = budget.usage();
    let (permit, ticket) = pending();
    let submission = stage.into_submission(ticket);
    assert_eq!(budget.usage(), charged);
    let (received_binding, ticket, mut work) = submission.into_owner_parts();
    assert_eq!(received_binding, binding());
    assert_eq!(work.usage, staged_usage);
    assert_eq!(budget.usage(), charged);
    let claim = ticket.try_claim().expect("owner claim");
    let count = work
        .with_commands(|commands| {
            assert_eq!(budget.usage(), charged);
            let count = commands.len();
            drop(commands);
            assert_eq!(budget.usage(), charged);
            count
        })
        .expect("take once");
    assert_eq!(count, 1);
    assert_eq!(budget.usage(), charged);
    assert!(work.commands.is_empty());
    claim.finish(AtomicCommitDecision::Committed);
    assert_eq!(permit.state(), AtomicCommitState::Committed);
    assert_eq!(budget.usage(), charged);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    assert_eq!(permit.state(), AtomicCommitState::Committed);
}

#[test]
fn consumed_owner_work_refuses_a_second_callback_without_refunding() {
    let budget = AtomicMessagingWorkBudget::new();
    let stage = budget.stage(binding()).expect("empty stage");
    let (_, ticket) = pending();
    let (_, ticket, mut work) = stage.into_submission(ticket).into_owner_parts();
    work.with_commands(|commands| assert!(commands.is_empty()))
        .expect("first callback");
    let before = budget.usage();
    assert!(matches!(
        work.with_commands(|_| panic!("a second callback must not run")),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert_eq!(budget.usage(), before);
    drop(ticket);
    assert_eq!(budget.usage(), before);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn callbacks_can_reserve_other_work_without_holding_the_budget_lock() {
    let budget = AtomicMessagingWorkBudget::new();
    let stage = budget.stage(binding()).expect("stage");
    let (_, ticket) = pending();
    let (_, ticket, mut work) = stage.into_submission(ticket).into_owner_parts();
    work.with_commands(|commands| {
        let mut nested = budget.stage(binding()).expect("reentrant reservation");
        nested.try_push(send(29)).expect("reentrant charge");
        assert_eq!(budget.usage().groups(), 2);
        drop(commands);
        drop(nested);
        assert_eq!(budget.usage().groups(), 1);
    })
    .expect("owner callback");
    drop(ticket);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn callback_unwind_retains_the_lease_and_marks_commands_taken() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage.try_push(send(31)).expect("send");
    let (_, ticket) = pending();
    let (_, ticket, mut work) = stage.into_submission(ticket).into_owner_parts();
    let charged = budget.usage();
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _ = work.with_commands(|commands| {
            drop(commands);
            assert_eq!(budget.usage(), charged);
            panic!("owner callback unwound");
        });
    }));
    assert!(unwound.is_err());
    assert_eq!(budget.usage(), charged);
    assert!(work.commands.is_empty());
    assert!(matches!(
        work.with_commands(|_| panic!("commands cannot be taken again")),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    drop(ticket);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn poisoned_admission_fails_closed_but_existing_leases_still_refund() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage.try_push(send(37)).expect("send");
    let staged_before = stage.usage();
    let shared_before = budget.usage();
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _guard = budget.usage.lock().expect("not yet poisoned");
        panic!("poison admission mutex");
    }));
    assert!(unwound.is_err());
    assert!(matches!(
        budget.stage(binding()),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert!(matches!(
        stage.try_push(send(1)),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert_eq!(stage.usage(), staged_before);
    assert_eq!(budget.usage(), shared_before);
    assert_eq!(stage.commands.len(), 1);
    drop(stage);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    assert!(matches!(
        budget.stage(binding()),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
}

#[test]
fn debug_output_never_formats_command_payloads_or_bindings() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut stage = budget.stage(binding()).expect("stage");
    stage
        .try_push(CommandKind::Send {
            message_id: "private-identifier".into(),
            body: b"private-command-payload".to_vec(),
            time_to_live_millis: None,
            session_id: None,
        })
        .expect("send");
    let staged_debug = format!("{stage:?}");
    let (_, ticket) = pending();
    let submission = stage.into_submission(ticket);
    let queued_debug = format!("{submission:?}");
    let (_, ticket, work) = submission.into_owner_parts();
    for output in [
        format!("{budget:?}"),
        staged_debug,
        queued_debug,
        format!("{work:?}"),
    ] {
        assert!(!output.contains("private-identifier"));
        assert!(!output.contains("private-command-payload"));
        assert!(!output.contains("orders"));
        assert!(!output.contains("CommandKind"));
    }
    drop(ticket);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn unbound_groups_and_empty_submissions_share_the_existing_slot_cap() {
    let budget = AtomicMessagingWorkBudget::new();
    let bound = (0..MAX_ATOMIC_WORK_GROUPS - 1)
        .map(|_| budget.stage(binding()).expect("bound slot"))
        .collect::<Vec<_>>();
    let unbound = budget.stage_unbound().expect("last unbound slot");
    let charged = budget.usage();
    assert_eq!(charged.groups(), MAX_ATOMIC_WORK_GROUPS);
    assert_eq!(unbound.usage(), AtomicMessagingInputUsage::default());
    assert!(matches!(
        budget.stage_unbound(),
        Err(AtomicMessagingWorkError::Group { .. })
    ));
    assert!(matches!(
        budget.stage(binding()),
        Err(AtomicMessagingWorkError::Group { .. })
    ));
    let (permit, ticket) = pending();
    let empty = unbound
        .into_empty_submission(ticket)
        .expect("empty handoff");
    assert_eq!(empty.permit().state(), AtomicCommitState::Pending);
    assert_eq!(budget.usage(), charged);
    drop(empty);
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(budget.usage().groups(), MAX_ATOMIC_WORK_GROUPS - 1);
    drop(bound);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn failed_first_push_can_still_use_the_checked_empty_handoff() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut unbound = budget.stage_unbound().expect("unbound group");
    let charged = budget.usage();
    assert!(matches!(
        unbound.try_push(CommandKind::ExpireLocks),
        Err(AtomicMessagingWorkError::Input(
            BrokerError::AtomicMessagingOperationNotSupported
        ))
    ));
    assert_eq!(unbound.usage(), AtomicMessagingInputUsage::default());
    assert!(unbound.commands.is_empty());
    assert_eq!(budget.usage(), charged);
    let (permit, ticket) = pending();
    let empty = unbound.into_empty_submission(ticket).expect("still empty");
    let (ticket, work) = empty.into_owner_parts();
    assert_eq!(work.usage, AtomicMessagingInputUsage::default());
    assert!(work.commands.is_empty());
    let claim = ticket.try_claim().expect("empty owner claim");
    claim.finish(AtomicCommitDecision::Committed);
    assert_eq!(permit.state(), AtomicCommitState::Committed);
    assert_eq!(budget.usage(), charged);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn unbound_push_and_bound_push_use_one_aggregate_content_budget() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut bound = budget.stage(binding()).expect("bound group");
    let mut unbound = budget.stage_unbound().expect("unbound group");
    let mut refused = budget.stage_unbound().expect("refused group");
    bound
        .try_push(send(MAX_ATOMIC_MESSAGING_CONTENT_BYTES))
        .expect("bound content");
    unbound
        .try_push(send(MAX_ATOMIC_MESSAGING_CONTENT_BYTES))
        .expect("unbound content");
    assert_eq!(bound.usage(), unbound.usage());
    assert_eq!(
        budget.usage().content_bytes(),
        MAX_ATOMIC_WORK_CONTENT_BYTES
    );
    let charged = budget.usage();
    assert!(matches!(
        refused.try_push(send(1)),
        Err(AtomicMessagingWorkError::Content { .. })
    ));
    assert_eq!(refused.usage(), AtomicMessagingInputUsage::default());
    assert!(refused.commands.is_empty());
    assert!(refused.commands.capacity() >= 1);
    assert_eq!(budget.usage(), charged);
    let (permit, ticket) = pending();
    let empty = refused
        .into_empty_submission(ticket)
        .expect("no accepted action");
    assert_eq!(budget.usage(), charged);
    drop(empty);
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(budget.usage().groups(), 2);
    assert_eq!(
        budget.usage().content_bytes(),
        MAX_ATOMIC_WORK_CONTENT_BYTES
    );
    drop(bound);
    drop(unbound);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn unbound_and_bound_value_items_cannot_bypass_the_shared_cap() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut bound = budget.stage(binding()).expect("bound group");
    let mut unbound = budget.stage_unbound().expect("unbound group");
    let mut refused = budget.stage_unbound().expect("refused group");
    bound
        .try_push(values(MAX_ATOMIC_MESSAGING_VALUE_ITEMS))
        .expect("bound values");
    unbound
        .try_push(values(MAX_ATOMIC_MESSAGING_VALUE_ITEMS))
        .expect("unbound values");
    assert_eq!(bound.usage(), unbound.usage());
    let charged = budget.usage();
    assert_eq!(charged.value_items(), MAX_ATOMIC_WORK_VALUE_ITEMS);
    assert!(matches!(
        refused.try_push(values(1)),
        Err(AtomicMessagingWorkError::ValueItem { .. })
    ));
    assert_eq!(budget.usage(), charged);
    assert_eq!(refused.usage(), AtomicMessagingInputUsage::default());
    drop(bound);
    refused.try_push(values(1)).expect("refunded capacity");
    assert_eq!(
        budget.usage().value_items(),
        MAX_ATOMIC_MESSAGING_VALUE_ITEMS + 1
    );
    drop(unbound);
    drop(refused);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn checked_empty_handoff_rejects_even_zero_content_actions() {
    for action in [
        send(1),
        complete(),
        CommandKind::SendBatch {
            messages: Vec::new(),
        },
    ] {
        let budget = AtomicMessagingWorkBudget::new();
        let mut unbound = budget.stage_unbound().expect("unbound group");
        unbound.try_push(action).expect("accepted action");
        assert_eq!(unbound.usage().actions(), 1);
        let (permit, ticket) = pending();
        assert!(matches!(
            unbound.into_empty_submission(ticket),
            Err(AtomicMessagingWorkError::Unavailable)
        ));
        assert_eq!(permit.state(), AtomicCommitState::Aborted);
        assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    }
}

#[test]
fn checked_empty_handoff_also_rejects_nondefault_usage_without_commands() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut unbound = budget.stage_unbound().expect("unbound group");
    unbound.try_push(complete()).expect("zero-content action");
    // A defensive private misuse cannot disguise accepted actions as empty.
    unbound.commands.clear();
    let (permit, ticket) = pending();
    assert!(matches!(
        unbound.into_empty_submission(ticket),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert_eq!(permit.state(), AtomicCommitState::Aborted);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn binding_handoff_moves_existing_payloads_and_retains_the_same_lease() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut unbound = budget.stage_unbound().expect("unbound group");
    let body = vec![0; 41];
    let original = body.as_ptr();
    unbound
        .try_push(CommandKind::Send {
            message_id: String::new(),
            body,
            time_to_live_millis: None,
            session_id: None,
        })
        .expect("send");
    unbound.try_push(complete()).expect("zero-content action");
    let input = unbound.usage();
    let charged = budget.usage();
    let (permit, ticket) = pending();
    let bound = unbound.into_bound_submission(binding(), ticket);
    assert_eq!(budget.usage(), charged);
    let (received_binding, ticket, mut work) = bound.into_owner_parts();
    assert_eq!(received_binding, binding());
    assert_eq!(work.usage, input);
    let claim = ticket.try_claim().expect("owner claim");
    work.with_commands(|commands| {
        assert_eq!(commands.len(), 2);
        match &commands[0] {
            CommandKind::Send { body, .. } => assert_eq!(body.as_ptr(), original),
            _ => panic!("send is first"),
        }
        assert_eq!(budget.usage(), charged);
        drop(commands);
        assert_eq!(budget.usage(), charged);
    })
    .expect("one owner callback");
    claim.finish(AtomicCommitDecision::Rejected);
    assert_eq!(permit.state(), AtomicCommitState::Rejected);
    assert_eq!(budget.usage(), charged);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn empty_work_retains_its_slot_after_ticket_abort_and_owner_claim_failure() {
    let budget = AtomicMessagingWorkBudget::new();
    let unbound = budget.stage_unbound().expect("unbound group");
    let (permit, ticket) = pending();
    let empty = unbound.into_empty_submission(ticket).expect("empty work");
    let observer = empty.permit().clone();
    drop(observer.clone());
    assert_eq!(observer.state(), AtomicCommitState::Pending);
    assert_eq!(budget.usage().groups(), 1);
    let (ticket, work) = empty.into_owner_parts();
    assert!(permit.abort());
    assert!(matches!(
        ticket.try_claim(),
        Err(crate::AtomicCommitClaimError::Aborted)
    ));
    assert_eq!(budget.usage().groups(), 1);
    drop(work);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
    assert_eq!(observer.state(), AtomicCommitState::Aborted);
}

#[test]
fn empty_owner_unwind_releases_its_slot_and_is_indeterminate() {
    let budget = AtomicMessagingWorkBudget::new();
    let unbound = budget.stage_unbound().expect("unbound group");
    let (permit, ticket) = pending();
    let empty = unbound.into_empty_submission(ticket).expect("empty work");
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let (ticket, _work) = empty.into_owner_parts();
        let _claim = ticket.try_claim().expect("owner claim");
        assert_eq!(budget.usage().groups(), 1);
        panic!("empty owner unwound");
    }));
    assert!(unwound.is_err());
    assert_eq!(permit.state(), AtomicCommitState::Indeterminate);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn poisoned_unbound_admission_fails_closed_without_changing_existing_work() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut unbound = budget.stage_unbound().expect("unbound group");
    let charged = budget.usage();
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _guard = budget.usage.lock().expect("not yet poisoned");
        panic!("poison admission mutex");
    }));
    assert!(unwound.is_err());
    assert!(matches!(
        budget.stage_unbound(),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert!(matches!(
        unbound.try_push(send(1)),
        Err(AtomicMessagingWorkError::Unavailable)
    ));
    assert_eq!(unbound.usage(), AtomicMessagingInputUsage::default());
    assert_eq!(budget.usage(), charged);
    drop(unbound);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}

#[test]
fn bridge_debug_output_retains_only_usage_and_permit_state() {
    let budget = AtomicMessagingWorkBudget::new();
    let mut unbound = budget.stage_unbound().expect("unbound group");
    unbound
        .try_push(CommandKind::Send {
            message_id: "private-unbound-id".into(),
            body: b"private-unbound-body".to_vec(),
            time_to_live_millis: None,
            session_id: None,
        })
        .expect("send");
    let output = format!("{unbound:?}");
    assert!(!output.contains("private-unbound"));
    assert!(!output.contains("CommandKind"));
    drop(unbound);
    let unbound = budget.stage_unbound().expect("empty group");
    let (_, ticket) = pending();
    let empty = unbound.into_empty_submission(ticket).expect("empty work");
    let output = format!("{empty:?}");
    assert!(!output.contains("commands"));
    assert!(!output.contains("EntityBinding"));
    drop(empty);
    assert_eq!(budget.usage(), AtomicMessagingWorkUsage::default());
}
