use super::*;

fn partial_updates_commit_only_exact_projections_and_survive_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let original_topic = TopicConfig {
        default_time_to_live_millis: Some(500),
        max_message_bytes: 128,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 40_000,
    };
    let original = SubscriptionConfig {
        lock_duration_millis: 100,
        max_delivery_count: 7,
        default_time_to_live_millis: Some(300),
        max_message_bytes: 128,
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    };
    let mut fixture = observed(provider, original_topic)?;
    let alpha = subscribe(&fixture, "Alpha", original, 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    send(&fixture, 10, "retained", None, None)?;
    let unchanged = retained(&fixture, &["Alpha"])?;
    let beta_config = subscription_config(&fixture, "beta")?;
    reset(&fixture);
    topic_update(
        &fixture,
        20,
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 200 }),
            max_message_bytes: Some(256),
            duplicate_detection_history_time_window_millis: Some(20_000),
            ..TopicConfigUpdate::default()
        },
    )?;
    assert_eq!(
        topic_config(&fixture)?,
        TopicConfig {
            default_time_to_live_millis: Some(200),
            max_message_bytes: 256,
            duplicate_detection_history_time_window_millis: 20_000,
            ..original_topic
        }
    );
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(observations.commits, 1);
        assert_eq!(
            observations.puts,
            vec![
                keys::topic_config(&fixture.namespace, &fixture.entity),
                keys::clock(),
            ]
        );
        assert!(observations.scans.iter().any(|(prefix, limit)| *prefix
            == keys::subscription_prefix(&fixture.namespace, &fixture.entity)
            && *limit == MAX_TOPIC_SUBSCRIPTIONS + 1));
    }
    reset(&fixture);
    subscription_update(
        &fixture,
        "Alpha",
        21,
        SubscriptionConfigUpdate {
            lock_duration_millis: Some(25),
            max_delivery_count: Some(3),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
            max_message_bytes: Some(256),
            dead_lettering_on_message_expiration: Some(false),
            dead_lettering_on_filter_evaluation_exceptions: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(
        subscription_config(&fixture, "Alpha")?,
        SubscriptionConfig {
            lock_duration_millis: 25,
            max_delivery_count: 3,
            default_time_to_live_millis: None,
            max_message_bytes: 256,
            dead_lettering_on_message_expiration: false,
            dead_lettering_on_filter_evaluation_exceptions: true,
            ..original
        }
    );
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(observations.commits, 1);
        let mut actual = observations.puts.clone();
        actual.sort();
        let mut expected = vec![
            keys::subscription(
                &fixture.namespace,
                &fixture.entity,
                &SubscriptionName::new("Alpha")?,
            ),
            keys::queue_config(&fixture.namespace, &alpha),
            keys::queue_config(&fixture.namespace, &alpha.dead_letter_queue()?),
            keys::clock(),
        ];
        expected.sort();
        assert_eq!(actual, expected);
    }
    assert_eq!(retained(&fixture, &["Alpha"])?, unchanged);
    assert_eq!(subscription_config(&fixture, "beta")?, beta_config);
    assert_eq!(
        record(&fixture, &alpha, SequenceNumber::new(1))?
            .expect("copy")
            .expires_at,
        Some(Timestamp::from_millis(310))
    );
    assert_eq!(
        record(&fixture, &beta, SequenceNumber::new(1))?
            .expect("sibling")
            .expires_at,
        Some(Timestamp::from_millis(510))
    );
    assert_projections(&fixture, "Alpha")?;
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

fn no_op_updates_validate_without_apply_or_clock_advance<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let topic_original = TopicConfig {
        default_time_to_live_millis: Some(500),
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    };
    let sub_original = SubscriptionConfig {
        requires_session: true,
        default_time_to_live_millis: Some(300),
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    };
    let mut fixture = observed(provider, topic_original)?;
    subscribe(&fixture, "Alpha", sub_original, 10)?;
    let snapshot = fixture.machine.store().snapshot()?;
    reset(&fixture);
    for patch in [
        TopicConfigUpdate::default(),
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 500 }),
            max_message_bytes: Some(topic_original.max_message_bytes),
            requires_duplicate_detection: Some(true),
            duplicate_detection_history_time_window_millis: Some(
                topic_original.duplicate_detection_history_time_window_millis,
            ),
        },
    ] {
        topic_update(&fixture, 100, patch)?;
    }
    for patch in [
        SubscriptionConfigUpdate::default(),
        SubscriptionConfigUpdate {
            lock_duration_millis: Some(sub_original.lock_duration_millis),
            max_delivery_count: Some(sub_original.max_delivery_count),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 300 }),
            max_message_bytes: Some(sub_original.max_message_bytes),
            requires_session: Some(true),
            dead_lettering_on_message_expiration: Some(false),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
        },
    ] {
        subscription_update(&fixture, "Alpha", 100, patch)?;
    }
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(10)
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    subscription_update(
        &fixture,
        "Alpha",
        11,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(11)
    );
    assert!(subscription_config(&fixture, "Alpha")?.dead_lettering_on_filter_evaluation_exceptions);
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

fn retained_live_states_and_session_authorities_use_new_defaults_only_on_later_operations<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(1_000),
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(
        &fixture,
        "Alpha",
        SubscriptionConfig {
            requires_session: true,
            lock_duration_millis: 100,
            max_delivery_count: 7,
            default_time_to_live_millis: Some(1_000),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let session = SessionId::new("cart")?;
    let mut sequences = Vec::new();
    for id in ["locked", "deferred", "dead", "ready"] {
        sequences.push(send(&fixture, 10, id, None, Some(&session))?);
    }
    let scheduled = schedule(&fixture, 10, "future", None, 200)?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = at(
        &fixture,
        &alpha,
        11,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("session")
    };
    let hold = accepted.hold();
    let locked = receive(&fixture, &alpha, 12, Some(&hold))?;
    let deferred = receive(&fixture, &alpha, 12, Some(&hold))?;
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.expect("lock").token,
        },
    )?;
    let dead = receive(&fixture, &alpha, 12, Some(&hold))?;
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::DeadLetter {
            sequence: dead.sequence,
            lock_token: dead.lock.expect("lock").token,
            reason: "retained".into(),
            description: "original rejection".into(),
        },
    )?;
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: b"retained state".to_vec(),
        },
    )?;
    let unchanged = retained(&fixture, &["Alpha"])?;
    let original_locked = record(&fixture, &alpha, locked.sequence)?;
    let original_deferred = record(&fixture, &alpha, deferred.sequence)?;
    let original_schedule = record(&fixture, &fixture.entity, scheduled)?;
    let beta_config = subscription_config(&fixture, "beta")?;
    topic_update(
        &fixture,
        20,
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
            max_message_bytes: Some(2),
            ..TopicConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "Alpha",
        20,
        SubscriptionConfigUpdate {
            lock_duration_millis: Some(20),
            max_delivery_count: Some(1),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
            max_message_bytes: Some(2),
            requires_session: Some(true),
            dead_lettering_on_message_expiration: Some(false),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
        },
    )?;
    assert_eq!(retained(&fixture, &["Alpha"])?, unchanged);
    assert_eq!(subscription_config(&fixture, "beta")?, beta_config);
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(record(&fixture, &alpha, locked.sequence)?, original_locked);
    assert_eq!(
        record(&fixture, &alpha, deferred.sequence)?,
        original_deferred
    );
    assert_eq!(
        record(&fixture, &fixture.entity, scheduled)?,
        original_schedule
    );
    assert_eq!(
        at(
            &fixture,
            &alpha,
            21,
            CommandKind::GetSessionState {
                session: hold.clone()
            }
        )?,
        CommandOutcome::SessionState(b"retained state".to_vec())
    );
    assert_eq!(
        at(
            &fixture,
            &alpha,
            21,
            CommandKind::RenewLock {
                sequence: locked.sequence,
                lock_token: locked.lock.expect("lock").token,
                lock_duration_millis: None,
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(41)
        }
    );
    assert_eq!(
        at(
            &fixture,
            &alpha,
            21,
            CommandKind::RenewSessionLock {
                session: hold.clone(),
                lock_duration_millis: None,
            }
        )?,
        CommandOutcome::SessionLockRenewed {
            locked_until: Timestamp::from_millis(41)
        }
    );
    assert_eq!(
        at(
            &fixture,
            &alpha,
            22,
            CommandKind::Complete {
                sequence: locked.sequence,
                lock_token: locked.lock.expect("lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    let ready = receive(&fixture, &alpha, 22, Some(&hold))?;
    assert_eq!(ready.sequence, sequences[3]);
    assert_eq!(ready.body, b"payload");
    assert_eq!(ready.expires_at, Some(Timestamp::from_millis(1_010)));
    assert_eq!(
        ready.lock.expect("new lock").locked_until,
        Timestamp::from_millis(42)
    );
    assert_eq!(
        at(
            &fixture,
            &alpha,
            23,
            CommandKind::Abandon {
                sequence: ready.sequence,
                lock_token: ready.lock.expect("lock").token,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false
        }
    );
    assert_eq!(
        record(&fixture, &beta, sequences[3])?
            .expect("sibling")
            .delivery_count,
        0
    );
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

fn expiration_policy_changes_use_old_deadlines_and_leave_siblings_independent<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(
        &fixture,
        "beta",
        SubscriptionConfig {
            dead_lettering_on_message_expiration: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let sequence = send(&fixture, 10, "expires", Some(5), None)?;
    let unchanged = retained(&fixture, &["Alpha", "beta"])?;
    subscription_update(
        &fixture,
        "Alpha",
        12,
        SubscriptionConfigUpdate {
            dead_lettering_on_message_expiration: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "beta",
        12,
        SubscriptionConfigUpdate {
            dead_lettering_on_message_expiration: Some(false),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(retained(&fixture, &["Alpha", "beta"])?, unchanged);
    for (entity, enabled) in [(&alpha, true), (&beta, false)] {
        let application = fixture.machine.apply_with_effects(&Command::new(
            fixture.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(15),
            CommandKind::ExpireMessages,
        ))?;
        assert_eq!(
            application.outcome,
            CommandOutcome::MessagesExpired {
                processed: 1,
                dead_lettered: u32::from(enabled),
                dropped: u32::from(!enabled),
            }
        );
        assert_eq!(application.dead_letters_enqueued, enabled);
        assert_eq!(application.subscription_enqueues, None);
        assert_eq!(
            record(&fixture, &entity.dead_letter_queue()?, sequence)?.is_some(),
            enabled
        );
    }
    Ok(())
}

fn topic_duplicate_window_changes_preserve_old_and_cancelled_history<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 40_000,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    send(&fixture, 10, "old", None, None)?;
    let scheduled = schedule(&fixture, 10, "cancelled", None, 100_000)?;
    fixture.at(
        11,
        CommandKind::CancelScheduled {
            sequences: vec![scheduled],
        },
    )?;
    let unchanged = retained(&fixture, &[])?;
    let old = keys::duplicate_history(&fixture.namespace, &fixture.entity, "old");
    let cancelled = keys::duplicate_history(&fixture.namespace, &fixture.entity, "cancelled");
    let deadline = fixture.machine.store().get(&old)?.expect("old history");
    assert_eq!(
        fixture.machine.store().get(&cancelled)?,
        Some(deadline.clone())
    );
    topic_update(
        &fixture,
        20,
        TopicConfigUpdate {
            duplicate_detection_history_time_window_millis: Some(20_000),
            ..TopicConfigUpdate::default()
        },
    )?;
    assert_eq!(retained(&fixture, &[])?, unchanged);
    send(&fixture, 20, "new", None, None)?;
    let new = keys::duplicate_history(&fixture.namespace, &fixture.entity, "new");
    assert_eq!(
        codec::decode::<Timestamp>(&fixture.machine.store().get(&new)?.expect("new history"))?,
        Timestamp::from_millis(20_020)
    );
    let discarded = send(&fixture, 30, "old", None, None)?;
    assert!(record(&fixture, &alpha, discarded)?.is_none());
    assert_eq!(
        fixture.at(20_020, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(fixture.machine.store().get(&old)?, Some(deadline.clone()));
    assert_eq!(fixture.machine.store().get(&cancelled)?, Some(deadline));
    let accepted = send(&fixture, 40_010, "old", None, None)?;
    assert!(record(&fixture, &alpha, accepted)?.is_some());
    assert_eq!(
        codec::decode::<Timestamp>(&fixture.machine.store().get(&old)?.expect("replacement"))?,
        Timestamp::from_millis(60_010)
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_prefix(&fixture.namespace, &alpha),
                1
            )?
            .is_empty()
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
    partial_updates_commit_only_exact_projections_and_survive_reopen,
    no_op_updates_validate_without_apply_or_clock_advance,
    retained_live_states_and_session_authorities_use_new_defaults_only_on_later_operations,
    expiration_policy_changes_use_old_deadlines_and_leave_siblings_independent,
    topic_duplicate_window_changes_preserve_old_and_cancelled_history,
}
