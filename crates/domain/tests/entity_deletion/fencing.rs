use super::*;

fn recreated_queue_cannot_reissue_stale_delivery_session_or_schedule_authorities<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let config = QueueConfig {
        requires_session: true,
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    let mut fixture = QueueFixture::new(provider, "tenant", "orders", config)?;
    let parent = fixture.entity.clone();
    let shadow = parent.dead_letter_queue()?;
    let session = SessionId::new("cart")?;
    let old_sequence = send(&fixture, 10, "old", Some(&session))?;
    let old_schedule = schedule(&fixture, 10, "future", Some(&session))?;
    let old_hold = accept(&fixture, &parent, 11, &session)?;
    let old_delivery = receive(&fixture, &parent, 12, Some(&old_hold))?;
    let dead_sequence = send(&fixture, 12, "dead", Some(&session))?;
    let dead = receive(&fixture, &parent, 12, Some(&old_hold))?;
    assert_eq!(dead.sequence, dead_sequence);
    at(
        &fixture,
        &parent,
        12,
        CommandKind::DeadLetter {
            sequence: dead.sequence,
            lock_token: dead.lock.expect("lock").token,
            reason: "old".into(),
            description: "old dead letter".into(),
        },
    )?;
    let old_shadow = receive(&fixture, &shadow, 13, None)?;
    at(
        &fixture,
        &parent,
        13,
        CommandKind::SetSessionState {
            session: old_hold.clone(),
            state: b"old state".to_vec(),
        },
    )?;
    let source_counter = counter_bytes(&fixture, &parent)?;
    let local_shadow_counter = counter_bytes(&fixture, &shadow)?;
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Auto,
        CommandOutcome::QueueDeleted,
        &[parent.clone(), shadow.clone()],
    )?;
    assert_eq!(counter_bytes(&fixture, &parent)?, source_counter);
    assert_eq!(counter_bytes(&fixture, &shadow)?, local_shadow_counter);
    let deleted = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, deleted);
    reject(
        &fixture,
        &parent,
        20,
        CommandKind::Complete {
            sequence: old_sequence,
            lock_token: old_delivery.lock.expect("lock").token,
        },
        BrokerError::MessageNotFound {
            sequence: old_sequence,
        },
    )?;
    fixture.at(21, CommandKind::CreateQueue { config })?;
    let fresh_sequence = send(&fixture, 22, "old", Some(&session))?;
    let fresh_schedule = schedule(&fixture, 22, "future", Some(&session))?;
    assert!(fresh_sequence.as_u64() > dead_sequence.as_u64());
    assert!(fresh_schedule.as_u64() > old_schedule.as_u64());
    let fresh_hold = accept(&fixture, &parent, 23, &session)?;
    assert_ne!(fresh_hold.token, old_hold.token);
    let fresh_delivery = receive(&fixture, &parent, 24, Some(&fresh_hold))?;
    assert_eq!(fresh_delivery.sequence, fresh_sequence);
    assert_ne!(
        fresh_delivery.lock.expect("lock").token,
        old_delivery.lock.expect("lock").token
    );
    assert_eq!(
        at(
            &fixture,
            &parent,
            24,
            CommandKind::GetSessionState {
                session: fresh_hold.clone()
            }
        )?,
        CommandOutcome::SessionState(Vec::new())
    );
    reject(
        &fixture,
        &parent,
        24,
        CommandKind::Complete {
            sequence: old_sequence,
            lock_token: old_delivery.lock.expect("lock").token,
        },
        BrokerError::MessageNotFound {
            sequence: old_sequence,
        },
    )?;
    reject(
        &fixture,
        &parent,
        24,
        CommandKind::Complete {
            sequence: fresh_sequence,
            lock_token: old_delivery.lock.expect("lock").token,
        },
        BrokerError::LockTokenMismatch {
            sequence: fresh_sequence,
        },
    )?;
    reject(
        &fixture,
        &parent,
        24,
        CommandKind::SetSessionState {
            session: old_hold,
            state: b"stale".to_vec(),
        },
        BrokerError::SessionLockNotHeld {
            session_id: session.clone(),
        },
    )?;
    reject(
        &fixture,
        &parent,
        24,
        CommandKind::CancelScheduled {
            sequences: vec![old_schedule],
        },
        BrokerError::MessageNotFound {
            sequence: old_schedule,
        },
    )?;
    let new_dead = send(&fixture, 25, "dead", Some(&session))?;
    let delivery = receive(&fixture, &parent, 26, Some(&fresh_hold))?;
    assert_eq!(delivery.sequence, new_dead);
    at(
        &fixture,
        &parent,
        26,
        CommandKind::DeadLetter {
            sequence: new_dead,
            lock_token: delivery.lock.expect("lock").token,
            reason: "new".into(),
            description: "new dead letter".into(),
        },
    )?;
    let fresh_shadow = receive(&fixture, &shadow, 27, None)?;
    assert_ne!(
        fresh_shadow.lock.expect("lock").token,
        old_shadow.lock.expect("lock").token
    );
    reject(
        &fixture,
        &shadow,
        27,
        CommandKind::Complete {
            sequence: fresh_shadow.sequence,
            lock_token: old_shadow.lock.expect("lock").token,
        },
        BrokerError::LockTokenMismatch {
            sequence: fresh_shadow.sequence,
        },
    )?;
    assert_eq!(
        at(
            &fixture,
            &parent,
            28,
            CommandKind::Complete {
                sequence: fresh_sequence,
                lock_token: fresh_delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn subscription_and_topic_recreation_retain_source_and_local_allocation_fences<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let config = SubscriptionConfig {
        requires_session: true,
        ..SubscriptionConfig::default()
    };
    let alpha = subscribe(&fixture, "Alpha", config, 0)?;
    let shadow = alpha.dead_letter_queue()?;
    let session = SessionId::new("cart")?;
    let old = send(&fixture, 10, "old", Some(&session))?;
    let old_schedule = schedule(&fixture, 10, "future", None)?;
    let old_hold = accept(&fixture, &alpha, 11, &session)?;
    let old_delivery = receive(&fixture, &alpha, 12, Some(&old_hold))?;
    send(&fixture, 12, "missing-session", None)?;
    receive(&fixture, &shadow, 13, None)?;
    let source_before = counter_bytes(&fixture, &fixture.entity)?;
    let local_before = counter_bytes(&fixture, &alpha)?;
    let shadow_before = counter_bytes(&fixture, &shadow)?;
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Subscription {
            name: SubscriptionName::new("Alpha")?,
        },
        CommandOutcome::SubscriptionDeleted,
        &[alpha.clone(), shadow.clone()],
    )?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, source_before);
    assert_eq!(counter_bytes(&fixture, &alpha)?, local_before);
    assert_eq!(counter_bytes(&fixture, &shadow)?, shadow_before);
    subscribe(&fixture, "Alpha", config, 21)?;
    let discarded = send(&fixture, 22, "old", Some(&session))?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &alpha, discarded)?
            .is_none()
    );
    let fresh = send(&fixture, 22, "fresh", Some(&session))?;
    let hold = accept(&fixture, &alpha, 23, &session)?;
    let delivery = receive(&fixture, &alpha, 24, Some(&hold))?;
    assert_ne!(hold.token, old_hold.token);
    reject(
        &fixture,
        &alpha,
        24,
        CommandKind::GetSessionState { session: old_hold },
        BrokerError::SessionLockNotHeld {
            session_id: session.clone(),
        },
    )?;
    reject(
        &fixture,
        &alpha,
        24,
        CommandKind::Complete {
            sequence: old,
            lock_token: old_delivery.lock.expect("lock").token,
        },
        BrokerError::MessageNotFound { sequence: old },
    )?;
    reject(
        &fixture,
        &alpha,
        24,
        CommandKind::Complete {
            sequence: fresh,
            lock_token: old_delivery.lock.expect("lock").token,
        },
        BrokerError::LockTokenMismatch { sequence: fresh },
    )?;
    let all = vec![fixture.entity.clone(), alpha.clone(), shadow];
    let source_before = counter_bytes(&fixture, &fixture.entity)?;
    let local_before = counter_bytes(&fixture, &alpha)?;
    delete(
        &fixture,
        30,
        DeleteEntityTarget::Topic,
        CommandOutcome::TopicDeleted,
        &all,
    )?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, source_before);
    assert_eq!(counter_bytes(&fixture, &alpha)?, local_before);
    fixture.at(
        31,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                ..TopicConfig::default()
            },
        },
    )?;
    subscribe(&fixture, "Alpha", config, 32)?;
    let newest = send(&fixture, 33, "old", Some(&session))?;
    let newest_schedule = schedule(&fixture, 33, "future", None)?;
    assert!(newest.as_u64() > fresh.as_u64());
    assert!(newest_schedule.as_u64() > old_schedule.as_u64());
    let newest_hold = accept(&fixture, &alpha, 34, &session)?;
    let newest_delivery = receive(&fixture, &alpha, 35, Some(&newest_hold))?;
    assert_ne!(newest_hold.token, hold.token);
    reject(
        &fixture,
        &fixture.entity,
        35,
        CommandKind::CancelScheduled {
            sequences: vec![old_schedule],
        },
        BrokerError::MessageNotFound {
            sequence: old_schedule,
        },
    )?;
    reject(
        &fixture,
        &alpha,
        35,
        CommandKind::ReleaseSession { session: hold },
        BrokerError::SessionLockNotHeld {
            session_id: session,
        },
    )?;
    reject(
        &fixture,
        &alpha,
        35,
        CommandKind::Complete {
            sequence: newest,
            lock_token: delivery.lock.expect("lock").token,
        },
        BrokerError::LockTokenMismatch { sequence: newest },
    )?;
    let rules = fixture.machine.rules(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("Alpha")?,
    )?;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].name.as_str(), "$Default");
    assert_eq!(rules[0].filter, RuleFilter::True);
    assert_eq!(rules[0].created_at, Timestamp::from_millis(32));
    assert_eq!(
        at(
            &fixture,
            &alpha,
            36,
            CommandKind::Complete {
                sequence: newest,
                lock_token: newest_delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn exhausted_counter_tombstones_remain_exhausted_after_recreation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let config = QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    };
    let fixture = QueueFixture::new(provider, "tenant", "orders", config)?;
    let exhausted = QueueCounters {
        next_sequence: MAX_SEQUENCE_NUMBER + 1,
        next_lock_token: u64::MAX,
    };
    let bytes = codec::encode(&exhausted)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        bytes.clone(),
    ))?;
    delete(
        &fixture,
        10,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &[fixture.entity.clone(), fixture.entity.dead_letter_queue()?],
    )?;
    fixture.at(11, CommandKind::CreateQueue { config })?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, Some(bytes));
    let session = SessionId::new("cart")?;
    reject(
        &fixture,
        &fixture.entity,
        12,
        CommandKind::Send {
            message_id: "new".into(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: Some(session.clone()),
        },
        BrokerError::QueueCounterExhausted {
            counter: domain::QueueCounterKind::Sequence,
        },
    )?;
    reject(
        &fixture,
        &fixture.entity,
        12,
        CommandKind::AcceptSession {
            session_id: Some(session),
            lock_duration_millis: None,
        },
        BrokerError::QueueCounterExhausted {
            counter: domain::QueueCounterKind::LockToken,
        },
    )?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    recreated_queue_cannot_reissue_stale_delivery_session_or_schedule_authorities,
    subscription_and_topic_recreation_retain_source_and_local_allocation_fences,
    exhausted_counter_tombstones_remain_exhausted_after_recreation,
}
