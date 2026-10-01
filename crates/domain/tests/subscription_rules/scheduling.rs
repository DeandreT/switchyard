use super::*;

fn changed_rules_preserve_existing_active_locked_and_deferred_copies_but_select_future_messages<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(50),
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            default_time_to_live_millis: Some(30),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    publish(
        &fixture,
        1,
        vec![member("deferred"), member("locked"), member("active")],
    )?;
    let CommandOutcome::Received(Some(first)) = at(
        &fixture,
        &child,
        2,
        CommandKind::Receive {
            mode: domain::ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("first delivery")
    };
    let lock = first.lock.expect("deferred lock");
    assert_eq!(
        at(
            &fixture,
            &child,
            3,
            CommandKind::Defer {
                sequence: first.sequence,
                lock_token: lock.token
            }
        )?,
        CommandOutcome::Deferred
    );
    let CommandOutcome::Received(Some(second)) = at(
        &fixture,
        &child,
        4,
        CommandKind::Receive {
            mode: domain::ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("second delivery")
    };
    assert_eq!(second.sequence, SequenceNumber::new(2));
    let original = (1..=3)
        .map(|sequence| record(&fixture, &child, sequence))
        .collect::<TestResult<Vec<_>>>()?;
    remove(&fixture, "child", "$Default", 5)?;
    add(&fixture, "child", "false", RuleFilter::False, 6)?;
    for sequence in 1..=3 {
        assert_eq!(
            record(&fixture, &child, sequence)?,
            original[sequence as usize - 1]
        );
    }
    let mut blue = member("future-blue");
    blue.envelope
        .application_properties
        .insert("color".into(), MessageValue::String("Blue".into()));
    let admission = apply(
        &fixture,
        7,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("future-red"), 100), scheduled(blue, 100)],
        },
    )?;
    assert_eq!(
        admission.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(4), SequenceNumber::new(5)]
        }
    );
    effects(&admission, &[]);
    for sequence in [4, 5] {
        assert!(matches!(
            record(&fixture, &fixture.entity, sequence)?
                .expect("unmatched scheduled parent retained")
                .state,
            MessageState::Scheduled { .. }
        ));
    }
    remove(&fixture, "child", "false", 8)?;
    add(
        &fixture,
        "child",
        "red",
        correlation([("color".into(), MessageValue::String("Red".into()))]),
        9,
    )?;
    for sequence in 1..=3 {
        assert_eq!(
            record(&fixture, &child, sequence)?,
            original[sequence as usize - 1]
        );
    }
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    effects(&activation, std::slice::from_ref(&child));
    for sequence in [4, 5] {
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
    }
    let selected = record(&fixture, &child, 6)?.expect("current red rule matches");
    assert_eq!(selected.message_id, "future-red");
    assert_eq!(selected.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(selected.expires_at, Some(Timestamp::from_millis(130)));
    assert_eq!(
        selected.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert!(record(&fixture, &child, 7)?.is_none());
    remove(&fixture, "child", "red", 101)?;
    assert_eq!(record(&fixture, &child, 6)?, Some(selected));
    // Rule changes do not settle or expire earlier stored copies.
    for sequence in 1..=3 {
        assert_eq!(
            record(&fixture, &child, sequence)?,
            original[sequence as usize - 1]
        );
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("handles and activation sequences")
            .next_sequence,
        8
    );
    Ok(())
}

fn unmatched_and_cancelled_admission_still_deduplicates_once_and_activation_never_refreshes_history<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "child", "$Default", 0)?;
    effects(&publish(&fixture, 1, vec![member("known")])?, &[]);
    let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "known");
    assert_eq!(
        fixture.machine.store().get(&history_key)?,
        Some(codec::encode(&Timestamp::from_millis(20_001))?)
    );
    let discarded = apply(
        &fixture,
        2,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("known"), 100)],
        },
    )?;
    assert_eq!(
        discarded.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(2)]
        }
    );
    assert!(record(&fixture, &fixture.entity, 2)?.is_none());
    apply(
        &fixture,
        3,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("cancel"), 100)],
        },
    )?;
    let cancel_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "cancel");
    let cancel_history = fixture.machine.store().get(&cancel_key)?;
    effects(
        &apply(
            &fixture,
            4,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(3)],
            },
        )?,
        &[],
    );
    assert_eq!(fixture.machine.store().get(&cancel_key)?, cancel_history);
    add(&fixture, "child", "true", RuleFilter::True, 5)?;
    effects(&publish(&fixture, 6, vec![member("known")])?, &[]);
    effects(&publish(&fixture, 7, vec![member("cancel")])?, &[]);
    let anonymous = publish(&fixture, 8, vec![member(""), member("")])?;
    assert_eq!(
        anonymous.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(6), SequenceNumber::new(7)]
        }
    );
    effects(&anonymous, std::slice::from_ref(&child));
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::duplicate_history(
                &fixture.namespace,
                &fixture.entity,
                ""
            ))?
            .is_none()
    );
    effects(
        &publish(&fixture, 20_001, vec![member("known")])?,
        std::slice::from_ref(&child),
    );
    assert_eq!(
        fixture.machine.store().get(&history_key)?,
        Some(codec::encode(&Timestamp::from_millis(40_001))?)
    );
    apply(
        &fixture,
        20_002,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("future"), 30_000)],
        },
    )?;
    let future_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "future");
    let future_history = fixture.machine.store().get(&future_key)?;
    remove(&fixture, "child", "true", 20_003)?;
    let activation = apply(&fixture, 30_000, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    effects(&activation, &[]);
    assert_eq!(fixture.machine.store().get(&future_key)?, future_history);
    assert!(record(&fixture, &child, 10)?.is_none());
    assert!(record(&fixture, &fixture.entity, 9)?.is_none());
    add(&fixture, "child", "restored", RuleFilter::True, 30_001)?;
    effects(
        &apply(
            &fixture,
            30_002,
            CommandKind::ScheduleEnvelopes {
                messages: vec![scheduled(member("future"), 40_000)],
            },
        )?,
        &[],
    );
    assert!(record(&fixture, &fixture.entity, 11)?.is_none());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all acknowledged sequences")
            .next_sequence,
        12
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    changed_rules_preserve_existing_active_locked_and_deferred_copies_but_select_future_messages,
    unmatched_and_cancelled_admission_still_deduplicates_once_and_activation_never_refreshes_history,
}
