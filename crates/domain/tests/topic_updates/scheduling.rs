use super::*;

fn captured_topic_lifetime_never_widens_and_activation_uses_current_child_ceiling<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(100),
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(
        &fixture,
        "Alpha",
        SubscriptionConfig {
            default_time_to_live_millis: Some(30),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let active = send(&fixture, 10, "active", None, None)?;
    let old = schedule(&fixture, 11, "old-future", Some(500), 100)?;
    let old_record = record(&fixture, &fixture.entity, old)?.expect("scheduled parent");
    assert!(matches!(
        old_record.state,
        MessageState::Scheduled {
            time_to_live_millis: Some(100),
            ..
        }
    ));
    let original_active = record(&fixture, &alpha, active)?;
    let unchanged = retained(&fixture, &["Alpha"])?;
    topic_update(
        &fixture,
        20,
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 200 }),
            ..TopicConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "Alpha",
        20,
        SubscriptionConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 150 }),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(retained(&fixture, &["Alpha"])?, unchanged);
    assert_eq!(record(&fixture, &alpha, active)?, original_active);
    assert_eq!(record(&fixture, &fixture.entity, old)?, Some(old_record));
    let later = schedule(&fixture, 21, "new-future", Some(500), 100)?;
    assert_eq!(later, SequenceNumber::new(3));
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    let old_copy = record(&fixture, &alpha, SequenceNumber::new(4))?.expect("old activated copy");
    let new_copy = record(&fixture, &alpha, SequenceNumber::new(5))?.expect("new activated copy");
    assert_eq!(old_copy.message_id, "old-future");
    assert_eq!(old_copy.expires_at, Some(Timestamp::from_millis(200)));
    assert_eq!(new_copy.message_id, "new-future");
    assert_eq!(new_copy.expires_at, Some(Timestamp::from_millis(250)));
    assert_eq!(
        old_copy.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(
        new_copy.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(record(&fixture, &alpha, active)?, original_active);
    let lower = schedule(&fixture, 101, "lower-future", Some(500), 200)?;
    let pending = record(&fixture, &fixture.entity, lower)?;
    topic_update(
        &fixture,
        102,
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
            ..TopicConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "Alpha",
        102,
        SubscriptionConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 20 }),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, lower)?, pending);
    assert_eq!(
        fixture.at(200, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let clamped = record(&fixture, &alpha, SequenceNumber::new(7))?.expect("clamped copy");
    assert_eq!(clamped.expires_at, Some(Timestamp::from_millis(220)));
    topic_update(
        &fixture,
        201,
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
            ..TopicConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "Alpha",
        201,
        SubscriptionConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(
        record(&fixture, &alpha, SequenceNumber::new(7))?,
        Some(clamped)
    );
    let unlimited = send(&fixture, 202, "unlimited", None, None)?;
    let explicit = send(&fixture, 202, "explicit", Some(7), None)?;
    assert_eq!(
        record(&fixture, &alpha, unlimited)?
            .expect("unlimited")
            .expires_at,
        None
    );
    assert_eq!(
        record(&fixture, &alpha, explicit)?
            .expect("explicit")
            .expires_at,
        Some(Timestamp::from_millis(209))
    );
    subscription_update(
        &fixture,
        "Alpha",
        203,
        SubscriptionConfigUpdate {
            max_delivery_count: Some(3),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(topic_config(&fixture)?.default_time_to_live_millis, None);
    assert_eq!(
        subscription_config(&fixture, "Alpha")?.default_time_to_live_millis,
        None
    );
    Ok(())
}

fn reduced_current_size_limits_leave_scheduled_head_atomic_and_cancelable<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: 64,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(
        &fixture,
        "Alpha",
        SubscriptionConfig {
            max_message_bytes: 64,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let beta = subscribe(
        &fixture,
        "beta",
        SubscriptionConfig {
            max_message_bytes: 64,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let handle = schedule(&fixture, 11, "topic-ceiling", None, 100)?;
    let pending = record(&fixture, &fixture.entity, handle)?;
    topic_update(
        &fixture,
        20,
        TopicConfigUpdate {
            max_message_bytes: Some(2),
            ..TopicConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, handle)?, pending);
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::MessageTooLarge {
            body_bytes: 7,
            maximum_bytes: 2,
        },
    )?;
    assert!(record(&fixture, &alpha, SequenceNumber::new(2))?.is_none());
    assert!(record(&fixture, &beta, SequenceNumber::new(2))?.is_none());
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![handle]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    topic_update(
        &fixture,
        102,
        TopicConfigUpdate {
            max_message_bytes: Some(64),
            ..TopicConfigUpdate::default()
        },
    )?;
    let handle = schedule(&fixture, 103, "child-ceiling", None, 200)?;
    let pending = record(&fixture, &fixture.entity, handle)?;
    subscription_update(
        &fixture,
        "Alpha",
        104,
        SubscriptionConfigUpdate {
            max_message_bytes: Some(2),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, handle)?, pending);
    reject(
        &fixture,
        200,
        CommandKind::ActivateScheduled,
        BrokerError::MessageTooLarge {
            body_bytes: 7,
            maximum_bytes: 2,
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, handle)?, pending);
    assert_eq!(counters(&fixture, &alpha)?, None);
    assert_eq!(counters(&fixture, &beta)?, None);
    subscription_update(
        &fixture,
        "Alpha",
        201,
        SubscriptionConfigUpdate {
            max_message_bytes: Some(8),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    let activated = apply(&fixture, 201, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activated.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let mut targets = vec![alpha.clone(), beta.clone()];
    targets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    assert_eq!(activated.subscription_enqueues, Some(targets));
    assert!(!activated.dead_letters_enqueued);
    for entity in [&alpha, &beta] {
        let copy = record(&fixture, entity, SequenceNumber::new(3))?.expect("retry activated copy");
        assert_eq!(copy.message_id, "child-ceiling");
        assert_eq!(
            copy.scheduled_enqueue_time,
            Some(Timestamp::from_millis(200))
        );
    }
    Ok(())
}

fn sql_error_policy_changes_apply_to_current_activation_not_existing_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(500),
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let shadow = alpha.dead_letter_queue()?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let gamma = subscribe(
        &fixture,
        "gamma",
        SubscriptionConfig {
            requires_session: true,
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let mut originals = Vec::new();
    for id in ["locked", "deferred", "active"] {
        originals.push(send(&fixture, 10, id, None, None)?);
    }
    let locked = receive(&fixture, &alpha, 11, None)?;
    let deferred = receive(&fixture, &alpha, 11, None)?;
    at(
        &fixture,
        &alpha,
        11,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.expect("lock").token,
        },
    )?;
    let retained_copies = originals
        .iter()
        .map(|sequence| record(&fixture, &alpha, *sequence))
        .collect::<TestResult<Vec<_>>>()?;
    fixture.at(
        12,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("error")?,
            filter: RuleFilter::Sql(SqlFilter::new("1 / 0 = 1")?),
        },
    )?;
    let future_false = schedule(&fixture, 13, "future-drop", None, 100)?;
    let parent = record(&fixture, &fixture.entity, future_false)?;
    let projections = (
        fixture.machine.queue_config(&fixture.namespace, &alpha)?,
        fixture.machine.queue_config(&fixture.namespace, &shadow)?,
    );
    subscription_update(
        &fixture,
        "Alpha",
        14,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, future_false)?, parent);
    assert_eq!(
        (
            fixture.machine.queue_config(&fixture.namespace, &alpha)?,
            fixture.machine.queue_config(&fixture.namespace, &shadow)?
        ),
        projections
    );
    let dropped = send(&fixture, 15, "new-drop", None, None)?;
    assert!(record(&fixture, &alpha, dropped)?.is_none());
    assert!(record(&fixture, &shadow, dropped)?.is_none());
    assert!(record(&fixture, &beta, dropped)?.is_some());
    let missing_session = record(&fixture, &gamma.dead_letter_queue()?, dropped)?
        .expect("missing session still dead letters");
    assert_eq!(
        missing_session.dead_letter.expect("reason").reason.as_str(),
        "Session ID is null"
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert!(record(&fixture, &alpha, SequenceNumber::new(6))?.is_none());
    assert!(record(&fixture, &shadow, SequenceNumber::new(6))?.is_none());
    assert_eq!(
        record(&fixture, &beta, SequenceNumber::new(6))?
            .expect("healthy activation")
            .message_id,
        "future-drop"
    );
    let future_true = schedule(&fixture, 101, "future-error", None, 200)?;
    let parent = record(&fixture, &fixture.entity, future_true)?;
    subscription_update(
        &fixture,
        "Alpha",
        102,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &fixture.entity, future_true)?, parent);
    let errored = send(&fixture, 103, "new-error", None, None)?;
    let original_dead = record(&fixture, &shadow, errored)?.expect("SQL error copy");
    assert_eq!(
        original_dead
            .dead_letter
            .as_ref()
            .expect("reason")
            .reason
            .as_str(),
        "SwitchyardSqlFilterError"
    );
    subscription_update(
        &fixture,
        "Alpha",
        104,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(record(&fixture, &shadow, errored)?, Some(original_dead));
    subscription_update(
        &fixture,
        "Alpha",
        199,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture.at(200, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let copy = record(&fixture, &shadow, SequenceNumber::new(9))?.expect("current true policy");
    assert_eq!(copy.message_id, "future-error");
    assert_eq!(
        copy.dead_letter.expect("reason").reason.as_str(),
        "SwitchyardSqlFilterError"
    );
    assert_eq!(copy.expires_at, None);
    assert_eq!(copy.session_id, None);
    assert_eq!(
        copy.scheduled_enqueue_time,
        Some(Timestamp::from_millis(200))
    );
    for (sequence, original) in originals.into_iter().zip(retained_copies) {
        assert_eq!(record(&fixture, &alpha, sequence)?, original);
    }
    assert_eq!(
        at(
            &fixture,
            &alpha,
            201,
            CommandKind::Complete {
                sequence: locked.sequence,
                lock_token: locked.lock.expect("original lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    captured_topic_lifetime_never_widens_and_activation_uses_current_child_ceiling,
    reduced_current_size_limits_leave_scheduled_head_atomic_and_cancelable,
    sql_error_policy_changes_apply_to_current_activation_not_existing_copies,
}
