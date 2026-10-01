use super::*;

fn all_ingress_shapes_keep_future_work_at_the_parent_and_allocate_once_when_due<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let legacy = apply(
        &fixture,
        1,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: "legacy".into(),
                body: b"legacy".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                enqueue_at: Timestamp::from_millis(50),
            }],
        },
    )?;
    assert_eq!(
        legacy.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(1)]
        }
    );
    effects(&legacy, &[]);
    let original = member("rich", None);
    let rich = apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(original.clone(), 40)],
        },
    )?;
    assert_eq!(
        rich.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(2)]
        }
    );
    effects(&rich, &[]);
    assert_eq!(
        pending(&fixture, 2, 40)?.envelope,
        Some(Box::new(original.envelope.clone()))
    );
    let past_due = apply(&fixture, 1, schedule("past", 0))?;
    assert_eq!(
        past_due.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(3)]
        }
    );
    effects(&past_due, std::slice::from_ref(&alpha));
    assert!(record(&fixture, &fixture.entity, 3)?.is_none());
    assert_eq!(
        record(&fixture, &alpha, 3)?
            .expect("past-due copy")
            .enqueued_at,
        Timestamp::from_millis(1)
    );
    let batch = apply(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: vec![
                member("batch-future", Some(100)),
                member("batch-now", Some(2)),
                member("ordinary", None),
            ],
        },
    )?;
    assert_eq!(
        batch.outcome,
        CommandOutcome::BatchSent {
            sequences: (4..=6).map(SequenceNumber::new).collect()
        }
    );
    effects(&batch, std::slice::from_ref(&alpha));
    for (sequence, due) in [(1, 50), (2, 40), (4, 100)] {
        pending(&fixture, sequence, due)?;
        assert!(record(&fixture, &alpha, sequence)?.is_none());
    }
    for sequence in [3, 5, 6] {
        assert!(record(&fixture, &alpha, sequence)?.is_some());
    }
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 20)?;
    let before = fixture.machine.store().snapshot()?;
    activate(&fixture, 39, 0, &[])?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    // Membership is resolved when activation commits, not at admission.
    activate(&fixture, 50, 2, &[alpha.clone(), beta.clone()])?;
    for target in [&alpha, &beta] {
        let rich = record(&fixture, target, 7)?.expect("earlier deadline gets first new sequence");
        assert_eq!(rich.message_id, "rich");
        assert_eq!(rich.envelope, Some(Box::new(original.envelope.clone())));
        assert_eq!(
            rich.scheduled_enqueue_time,
            Some(Timestamp::from_millis(40))
        );
        assert_eq!(rich.enqueued_at, Timestamp::from_millis(50));
        assert_eq!(
            record(&fixture, target, 8)?
                .expect("legacy copy")
                .message_id,
            "legacy"
        );
        assert_eq!(counters(&fixture, target)?, None);
    }
    for sequence in [1, 2] {
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
    }
    activate(&fixture, 100, 1, &[alpha.clone(), beta.clone()])?;
    assert_eq!(
        record(&fixture, &beta, 9)?
            .expect("later scheduled copy")
            .message_id,
        "batch-future"
    );
    let late = subscribe(&fixture, "late", SubscriptionConfig::default(), 101)?;
    assert!(peek(&fixture, &late, 101, 0, 100, None)?.is_empty());
    effects(
        &apply(&fixture, 102, immediate("after"))?,
        &[alpha, beta, late.clone()],
    );
    assert_eq!(
        record(&fixture, &late, 10)?
            .expect("no historical replay")
            .message_id,
        "after"
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counters")
            .next_sequence,
        11
    );
    Ok(())
}

fn activation_starts_ttl_and_routes_session_null_copies_with_independent_ready_indexes<
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
    let plain = subscribe(
        &fixture,
        "plain",
        SubscriptionConfig {
            default_time_to_live_millis: Some(30),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let session = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            default_time_to_live_millis: Some(20),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let shadow = session.dead_letter_queue()?;
    let mut with_id = member("session", None);
    with_id.time_to_live_millis = Some(80);
    with_id.session_id = Some(SessionId::new("cart")?);
    let mut null = member("null", None);
    null.time_to_live_millis = Some(80);
    let original_null = null.envelope.clone();
    let admission = apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(with_id, 100), scheduled(null, 100)],
        },
    )?;
    effects(&admission, &[]);
    for sequence in [1, 2] {
        assert!(matches!(
            pending(&fixture, sequence, 100)?.state,
            MessageState::Scheduled {
                time_to_live_millis: Some(50),
                ..
            }
        ));
        for entity in [&plain, &session, &shadow] {
            assert!(record(&fixture, entity, sequence)?.is_none());
        }
    }
    assert_eq!(
        peek(&fixture, &fixture.entity, 1_000, 0, 10, None)?.len(),
        2
    );
    activate(
        &fixture,
        150,
        2,
        &[plain.clone(), session.clone(), shadow.clone()],
    )?;
    let id = SessionId::new("cart")?;
    for sequence in [3, 4] {
        let ordinary = record(&fixture, &plain, sequence)?.expect("ordinary active copy");
        assert_eq!(ordinary.enqueued_at, Timestamp::from_millis(150));
        assert_eq!(ordinary.expires_at, Some(Timestamp::from_millis(180)));
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::ready(
                    &fixture.namespace,
                    &plain,
                    SequenceNumber::new(sequence)
                ))?
                .is_some()
        );
    }
    let required = record(&fixture, &session, 3)?.expect("session active copy");
    assert_eq!(required.session_id, Some(id.clone()));
    assert_eq!(required.expires_at, Some(Timestamp::from_millis(170)));
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::session_ready(
                &fixture.namespace,
                &session,
                &id,
                SequenceNumber::new(3)
            ))?
            .is_some()
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::ready(
                &fixture.namespace,
                &session,
                SequenceNumber::new(3)
            ))?
            .is_none()
    );
    assert!(record(&fixture, &session, 4)?.is_none());
    let null = record(&fixture, &shadow, 4)?.expect("null session goes to its child shadow only");
    assert_eq!(null.enqueued_at, Timestamp::from_millis(150));
    assert_eq!(null.expires_at, None);
    assert_eq!(null.session_id, None);
    assert_eq!(null.envelope, Some(Box::new(original_null)));
    let info = null.dead_letter.expect("typed missing-session metadata");
    assert_eq!(info.reason, DeadLetterReason::MissingSessionId);
    assert_eq!(info.reason.as_str(), "Session ID is null");
    assert_eq!(info.description, MISSING_SESSION_DESCRIPTION);
    assert_eq!(info.dead_lettered_at, Timestamp::from_millis(150));
    for entity in [&plain, &session, &shadow] {
        assert_eq!(counters(&fixture, entity)?, None);
    }
    let CommandOutcome::Received(Some(ordinary)) = at(
        &fixture,
        &plain,
        150,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("ordinary receive")
    };
    assert_eq!(ordinary.sequence, SequenceNumber::new(3));
    assert_eq!(ordinary.session_id, Some(id.clone()));
    assert!(record(&fixture, &session, 3)?.is_some());
    let CommandOutcome::SessionAccepted(Some(accepted)) = at(
        &fixture,
        &session,
        150,
        CommandKind::AcceptSession {
            session_id: Some(id),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("session acceptance")
    };
    let CommandOutcome::Received(Some(required)) = at(
        &fixture,
        &session,
        150,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(accepted.hold()),
        },
    )?
    else {
        panic!("session receive")
    };
    assert_eq!(required.sequence, SequenceNumber::new(3));
    assert!(record(&fixture, &plain, 4)?.is_some());
    assert!(record(&fixture, &shadow, 4)?.is_some());
    Ok(())
}

fn duplicate_detection_runs_only_at_admission_and_cancel_keeps_history<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let config = TopicConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        ..TopicConfig::default()
    };
    let fixture = topic(provider, config)?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    effects(&apply(&fixture, 1, schedule("repeat", 20_100))?, &[]);
    effects(&apply(&fixture, 2, immediate("repeat"))?, &[]);
    let duplicate = apply(&fixture, 3, schedule("repeat", 20_100))?;
    assert_eq!(
        duplicate.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(3)]
        }
    );
    effects(&duplicate, &[]);
    assert!(record(&fixture, &fixture.entity, 3)?.is_none());
    let anonymous = apply(
        &fixture,
        3,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("", None), 100),
                scheduled(member("", None), 100),
            ],
        },
    )?;
    assert_eq!(
        anonymous.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(4), SequenceNumber::new(5)]
        }
    );
    let cancelled = apply(
        &fixture,
        4,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(4), SequenceNumber::new(4)],
        },
    )?;
    assert_eq!(
        cancelled.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    effects(&cancelled, &[]);
    activate(&fixture, 100, 1, std::slice::from_ref(&alpha))?;
    assert_eq!(
        record(&fixture, &alpha, 6)?
            .expect("anonymous retained")
            .message_id,
        ""
    );
    let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "repeat");
    assert_eq!(
        codec::decode::<Timestamp>(
            &fixture
                .machine
                .store()
                .get(&history_key)?
                .expect("initial history")
        )?,
        Timestamp::from_millis(20_001)
    );
    effects(
        &apply(&fixture, 20_001, immediate("repeat"))?,
        std::slice::from_ref(&alpha),
    );
    let history = fixture
        .machine
        .store()
        .get(&history_key)?
        .expect("new immediate history");
    activate(&fixture, 20_100, 1, std::slice::from_ref(&alpha))?;
    assert_eq!(
        record(&fixture, &alpha, 8)?
            .expect("admitted schedule must not dedup again")
            .message_id,
        "repeat"
    );
    assert_eq!(fixture.machine.store().get(&history_key)?, Some(history));
    effects(
        &apply(&fixture, 20_101, schedule("cancelled", 30_000))?,
        &[],
    );
    let cancel_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "cancelled");
    let retained = fixture.machine.store().get(&cancel_key)?;
    let cancelled = apply(
        &fixture,
        30_001,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(9)],
        },
    )?;
    assert_eq!(
        cancelled.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    effects(&cancelled, &[]);
    assert_eq!(fixture.machine.store().get(&cancel_key)?, retained);
    let dropped = apply(&fixture, 30_002, schedule("cancelled", 40_000))?;
    assert_eq!(
        dropped.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(10)]
        }
    );
    effects(&dropped, &[]);
    assert!(record(&fixture, &fixture.entity, 10)?.is_none());
    reject(
        &fixture,
        30_003,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(1)],
        },
        BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(1),
        },
    )?;
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        11
    );
    Ok(())
}

fn topic_peek_browses_only_pending_handles_without_acquiring_or_advancing_anything<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig::default(),
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("first", None), 100),
                scheduled(member("second", None), 50),
                scheduled(member("third", None), 75),
            ],
        },
    )?;
    apply(&fixture, 2, immediate("not-retained-at-topic"))?;
    *observations.lock().expect("observations") = Observations::default();
    let all = peek(&fixture, &fixture.entity, 1_000, 0, 100, None)?;
    assert_eq!(
        all.iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        all.iter()
            .all(|delivery| delivery.status == domain::MessageStatus::Scheduled)
    );
    assert_eq!(
        all[0].scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(
        peek(&fixture, &fixture.entity, 1_000, 2, 1, None)?[0].message_id,
        "second"
    );
    assert!(peek(&fixture, &fixture.entity, 1_000, 0, 0, None)?.is_empty());
    assert_eq!(peek(&fixture, &child, 1_000, 0, 100, None)?.len(), 1);
    let first = pending(&fixture, 1, 100)?;
    let exact = first.delivery_size_upper_bound() + 7;
    let bounded = peek(
        &fixture,
        &fixture.entity,
        1_000,
        0,
        100,
        Some(DeliveryBudget {
            max_bytes: exact,
            per_message_overhead_bytes: 7,
        }),
    )?;
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].sequence, SequenceNumber::new(1));
    let before = fixture.machine.store().snapshot()?;
    assert!(matches!(
        at(
            &fixture,
            &fixture.entity,
            1_000,
            CommandKind::PeekBounded {
                from_sequence: SequenceNumber::new(1),
                max_messages: 1,
                session_id: None,
                budget: DeliveryBudget {
                    max_bytes: exact - 1,
                    per_message_overhead_bytes: 7
                }
            }
        ),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(observations.lock().expect("observations").commits, 0);
    let prefix = keys::message_prefix(&fixture.namespace, &fixture.entity);
    assert!(
        observations
            .lock()
            .expect("observations")
            .scans
            .iter()
            .filter(|(seen, _, _)| *seen == prefix)
            .all(|(_, limit, _)| *limit <= TIMER_SCAN_LIMIT)
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
    all_ingress_shapes_keep_future_work_at_the_parent_and_allocate_once_when_due,
    activation_starts_ttl_and_routes_session_null_copies_with_independent_ready_indexes,
    duplicate_detection_runs_only_at_admission_and_cancel_keeps_history,
    topic_peek_browses_only_pending_handles_without_acquiring_or_advancing_anything,
}
