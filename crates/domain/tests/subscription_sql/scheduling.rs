use super::*;

fn activation_reevaluates_current_sql_without_retroactive_settlement_or_expiration<
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
    let healthy = subscribe(&fixture, "healthy", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "child", "$Default", 0)?;
    add(&fixture, "child", "select", sql("TRUE")?, 0)?;
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
        panic!("deferred delivery")
    };
    assert_eq!(
        at(
            &fixture,
            &child,
            3,
            CommandKind::Defer {
                sequence: first.sequence,
                lock_token: first.lock.expect("lock").token
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
        panic!("locked delivery")
    };
    assert_eq!(second.sequence, SequenceNumber::new(2));
    let originals = (1..=3)
        .map(|sequence| record(&fixture, &child, sequence))
        .collect::<TestResult<Vec<_>>>()?;
    remove(&fixture, "child", "select", 5)?;
    add(&fixture, "child", "select", sql("FALSE")?, 5)?;
    let mut future = member("future-one");
    future.session_id = Some(SessionId::new("session")?);
    future.time_to_live_millis = Some(10);
    let admission = apply(
        &fixture,
        6,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(future.clone(), 100),
                scheduled(member("future-two"), 100),
            ],
        },
    )?;
    assert_eq!(
        admission.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(4), SequenceNumber::new(5)]
        }
    );
    effects(&admission, &[]);
    remove(&fixture, "child", "select", 7)?;
    add(&fixture, "child", "select", sql("1 / 0 = 1")?, 7)?;
    for sequence in 1..=3 {
        assert_eq!(
            record(&fixture, &child, sequence)?,
            originals[sequence as usize - 1]
        );
    }
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    let shadow = child.dead_letter_queue()?;
    effects(&activation, &[shadow.clone(), healthy.clone()]);
    for (handle, sequence) in [(4, 6), (5, 7)] {
        assert!(record(&fixture, &fixture.entity, handle)?.is_none());
        assert!(record(&fixture, &child, sequence)?.is_none());
        let copy = record(&fixture, &shadow, sequence)?.expect("new current-rule SDLQ copy");
        assert_eq!(copy.sequence, SequenceNumber::new(sequence));
        assert_eq!(
            copy.scheduled_enqueue_time,
            Some(Timestamp::from_millis(100))
        );
        assert_eq!(copy.enqueued_at, Timestamp::from_millis(100));
        assert_eq!(copy.session_id, None);
        assert_eq!(copy.expires_at, None);
        assert_eq!(
            copy.dead_letter.expect("filter reason").reason.as_str(),
            SQL_ERROR_REASON
        );
        assert_eq!(
            record(&fixture, &healthy, sequence)?
                .expect("shared active sibling")
                .sequence,
            copy.sequence
        );
    }
    assert_eq!(
        record(&fixture, &shadow, 6)?
            .expect("preserved scheduled envelope")
            .envelope
            .as_deref(),
        Some(&future.envelope)
    );
    assert_eq!(
        record(&fixture, &healthy, 6)?
            .expect("activation lifetime")
            .expires_at,
        Some(Timestamp::from_millis(110))
    );
    remove(&fixture, "child", "select", 101)?;
    for sequence in 1..=3 {
        assert_eq!(
            record(&fixture, &child, sequence)?,
            originals[sequence as usize - 1]
        );
    }
    assert!(record(&fixture, &shadow, 6)?.is_some());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("handles and active sequences")
            .next_sequence,
        8
    );
    Ok(())
}

fn unmatched_future_admission_and_cancellation_do_not_recheck_or_refresh_deduplication<
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
    add(&fixture, "child", "select", sql("FALSE")?, 0)?;
    effects(&publish(&fixture, 1, vec![member("known")])?, &[]);
    let known_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "known");
    let known_history = fixture.machine.store().get(&known_key)?;
    let duplicate = apply(
        &fixture,
        2,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("known"), 100)],
        },
    )?;
    assert_eq!(
        duplicate.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(2)]
        }
    );
    assert!(record(&fixture, &fixture.entity, 2)?.is_none());
    effects(
        &apply(
            &fixture,
            3,
            CommandKind::ScheduleEnvelopes {
                messages: vec![scheduled(member("cancelled"), 100)],
            },
        )?,
        &[],
    );
    let cancelled_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "cancelled");
    let cancelled_history = fixture.machine.store().get(&cancelled_key)?;
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
    assert_eq!(
        fixture.machine.store().get(&cancelled_key)?,
        cancelled_history
    );
    remove(&fixture, "child", "select", 5)?;
    add(&fixture, "child", "select", sql("TRUE")?, 5)?;
    effects(&publish(&fixture, 6, vec![member("cancelled")])?, &[]);
    let anonymous = apply(
        &fixture,
        7,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member(""), 100), scheduled(member(""), 100)],
        },
    )?;
    assert_eq!(
        anonymous.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(5), SequenceNumber::new(6)]
        }
    );
    remove(&fixture, "child", "select", 8)?;
    add(&fixture, "child", "select", sql("1 / 0 = 1")?, 8)?;
    let activated = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activated.outcome,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    effects(
        &activated,
        std::slice::from_ref(&child.dead_letter_queue()?),
    );
    assert_eq!(
        peek(&fixture, &child.dead_letter_queue()?, 100)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![SequenceNumber::new(7), SequenceNumber::new(8)]
    );
    assert_eq!(fixture.machine.store().get(&known_key)?, known_history);
    assert_eq!(
        fixture.machine.store().get(&cancelled_key)?,
        cancelled_history
    );
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
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("single ingress/due sequence allocation")
            .next_sequence,
        9
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
    activation_reevaluates_current_sql_without_retroactive_settlement_or_expiration,
    unmatched_future_admission_and_cancellation_do_not_recheck_or_refresh_deduplication,
}
