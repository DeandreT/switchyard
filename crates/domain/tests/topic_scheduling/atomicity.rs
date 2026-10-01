use super::*;

fn cancellation_is_atomic_after_due_and_never_refunds_sequences_or_duplicate_history<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("first", None), 100),
                scheduled(member("second", None), 100),
            ],
        },
    )?;
    let first_history = fixture.machine.store().get(&keys::duplicate_history(
        &fixture.namespace,
        &fixture.entity,
        "first",
    ))?;
    reject(
        &fixture,
        101,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(999)],
        },
        BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999),
        },
    )?;
    pending(&fixture, 1, 100)?;
    pending(&fixture, 2, 100)?;
    let cancellation = apply(
        &fixture,
        101,
        CommandKind::CancelScheduled {
            sequences: vec![
                SequenceNumber::new(2),
                SequenceNumber::new(1),
                SequenceNumber::new(2),
            ],
        },
    )?;
    assert_eq!(
        cancellation.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 2 }
    );
    effects(&cancellation, &[]);
    assert_eq!(
        fixture.machine.store().get(&keys::duplicate_history(
            &fixture.namespace,
            &fixture.entity,
            "first"
        ))?,
        first_history
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("allocated handles")
            .next_sequence,
        3
    );
    for sequence in [1, 2] {
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
        assert!(record(&fixture, &child, sequence)?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::scheduled(
                    &fixture.namespace,
                    &fixture.entity,
                    Timestamp::from_millis(100),
                    SequenceNumber::new(sequence)
                ))?
                .is_none()
        );
    }
    let before = fixture.machine.store().snapshot()?;
    activate(&fixture, 102, 0, &[])?;
    let empty = apply(
        &fixture,
        102,
        CommandKind::CancelScheduled { sequences: vec![] },
    )?;
    assert_eq!(
        empty.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 0 }
    );
    effects(&empty, &[]);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let duplicate = apply(&fixture, 102, schedule("first", 200))?;
    assert_eq!(
        duplicate.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(3)]
        }
    );
    effects(&duplicate, &[]);
    assert!(record(&fixture, &fixture.entity, 3)?.is_none());
    Ok(())
}

fn late_input_topology_and_counter_failures_roll_back_every_staged_record<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let zulu = subscribe(&fixture, "zulu", SubscriptionConfig::default(), 0)?;
    effects(&apply(&fixture, 1, schedule("known", 100))?, &[]);
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![
                member("valid", Some(100)),
                member(&"x".repeat(129), Some(100)),
            ],
        },
        BrokerError::MessageIdTooLong {
            length: 129,
            maximum: 128,
        },
    )?;
    let mut invalid_duplicate = member("known", Some(100));
    invalid_duplicate
        .envelope
        .application_properties
        .insert("invalid".into(), MessageValue::List(vec![]));
    let before = fixture.machine.store().snapshot()?;
    assert!(matches!(
        apply(
            &fixture,
            10,
            CommandKind::SendBatch {
                messages: vec![member("valid", Some(100)), invalid_duplicate]
            }
        ),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::duplicate_history(
                &fixture.namespace,
                &fixture.entity,
                "valid"
            ))?
            .is_none()
    );
    let shadow = zulu.dead_letter_queue()?;
    let shadow_key = keys::queue_config(&fixture.namespace, &shadow);
    let original = fixture
        .machine
        .store()
        .get(&shadow_key)?
        .expect("canonical child shadow config");
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_key.clone()))?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::DanglingSubscriptionMetadata,
    )?;
    pending(&fixture, 1, 100)?;
    assert!(record(&fixture, &alpha, 2)?.is_none());
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(shadow_key, original))?;
    apply(&fixture, 2, schedule("second", 100))?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        })?,
    ))?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    for sequence in [1, 2] {
        pending(&fixture, sequence, 100)?;
    }
    for entity in [&alpha, &zulu] {
        assert!(record(&fixture, entity, MAX_SEQUENCE_NUMBER)?.is_none());
        assert_eq!(counters(&fixture, entity)?, None);
    }
    Ok(())
}

fn a_malformed_late_due_record_cannot_commit_an_earlier_activation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("first", None), 100),
                scheduled(member("second", None), 100),
            ],
        },
    )?;
    let mut second = pending(&fixture, 2, 100)?;
    second.state = MessageState::Ready;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message(&fixture.namespace, &fixture.entity, SequenceNumber::new(2)),
        codec::encode(&second)?,
    ))?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::MalformedIndexKey,
    )?;
    pending(&fixture, 1, 100)?;
    assert!(record(&fixture, &child, 3)?.is_none());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("admission counter")
            .next_sequence,
        3
    );
    Ok(())
}

fn mismatched_scheduled_annotations_cannot_publish_copies_and_original_handles_remain_cancelable<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    for variant in 0..3_u64 {
        let due = 100 + variant * 100;
        let id = format!("annotation-{variant}");
        let sequence = variant + 1;
        effects(
            &apply(&fixture, 1 + variant * 100, schedule(&id, due))?,
            &[],
        );
        let mut damaged = pending(&fixture, sequence, due)?;
        let annotation = match variant {
            0 => Some(Timestamp::from_millis(due + 50)),
            1 => None,
            _ => Some(Timestamp::from_millis(due - 1)),
        };
        damaged.scheduled_enqueue_time = annotation;
        fixture.machine.store().apply(WriteBatch::default().put(
            keys::message(
                &fixture.namespace,
                &fixture.entity,
                SequenceNumber::new(sequence),
            ),
            codec::encode(&damaged)?,
        ))?;
        let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, &id);
        let history = fixture
            .machine
            .store()
            .get(&history_key)?
            .expect("admission duplicate history");
        let allocated = counters(&fixture, &fixture.entity)?;
        reject(
            &fixture,
            due,
            CommandKind::ActivateScheduled,
            BrokerError::MalformedIndexKey,
        )?;
        assert_eq!(
            pending(&fixture, sequence, due)?.scheduled_enqueue_time,
            annotation
        );
        assert_eq!(
            fixture.machine.store().get(&history_key)?,
            Some(history.clone())
        );
        assert_eq!(counters(&fixture, &fixture.entity)?, allocated);
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::message_prefix(&fixture.namespace, &child), 1)?
                .is_empty()
        );
        assert_eq!(counters(&fixture, &child)?, None);
        let cancelled = apply(
            &fixture,
            due,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(sequence)],
            },
        )?;
        assert_eq!(
            cancelled.outcome,
            CommandOutcome::ScheduledCancelled { cancelled: 1 }
        );
        effects(&cancelled, &[]);
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::scheduled(
                    &fixture.namespace,
                    &fixture.entity,
                    Timestamp::from_millis(due),
                    SequenceNumber::new(sequence)
                ))?
                .is_none()
        );
        assert_eq!(fixture.machine.store().get(&history_key)?, Some(history));
        assert_eq!(counters(&fixture, &fixture.entity)?, allocated);
    }
    Ok(())
}

fn admission_and_activation_each_commit_once_and_failed_commit_survives_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
        },
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let command = CommandKind::SendBatch {
        messages: vec![
            member("same", Some(100)),
            member("same", Some(100)),
            member("now", None),
        ],
    };
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(
        &fixture,
        1,
        command.clone(),
        BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected scheduling failure".into(),
        }),
    )?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    let application = apply(&fixture, 1, command)?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: (1..=3).map(SequenceNumber::new).collect()
        }
    );
    effects(&application, &[alpha.clone(), beta.clone()]);
    pending(&fixture, 1, 100)?;
    assert!(record(&fixture, &fixture.entity, 2)?.is_none());
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|key| **key == keys::clock())
                .count(),
            1
        );
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|key| **key == keys::queue_counters(&fixture.namespace, &fixture.entity))
                .count(),
            1
        );
        for entity in [&alpha, &beta] {
            assert!(
                !observed
                    .puts
                    .contains(&keys::queue_counters(&fixture.namespace, entity))
            );
        }
    }
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected scheduling failure".into(),
        }),
    )?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    activate(&fixture, 100, 1, &[alpha.clone(), beta.clone()])?;
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|key| **key == keys::clock())
                .count(),
            1
        );
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|key| **key == keys::queue_counters(&fixture.namespace, &fixture.entity))
                .count(),
            1
        );
        for entity in [&alpha, &beta] {
            assert_eq!(
                observed
                    .puts
                    .iter()
                    .filter(|key| **key
                        == keys::message(&fixture.namespace, entity, SequenceNumber::new(4)))
                    .count(),
                1
            );
            assert!(
                !observed
                    .puts
                    .contains(&keys::queue_counters(&fixture.namespace, entity))
            );
        }
    }
    assert!(record(&fixture, &fixture.entity, 1)?.is_none());
    assert_eq!(record(&fixture, &alpha, 4)?, record(&fixture, &beta, 4)?);
    let snapshot = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn canonical_snapshot<P: StoreProvider>(provider: P) -> TestResult<StoreSnapshot> {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let session = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("repeat", None), 100),
                scheduled(member("repeat", None), 100),
            ],
        },
    )?;
    let fixture = fixture.restart()?;
    activate(&fixture, 100, 1, &[alpha, session.dead_letter_queue()?])?;
    Ok(fixture.machine.store().snapshot()?)
}

#[test]
fn memory_and_durable_reopened_activation_have_byte_identical_snapshots() -> TestResult {
    assert_eq!(
        canonical_snapshot(testkit::MemoryProvider::new())?,
        canonical_snapshot(testkit::DurableProvider::temporary()?)?
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
    cancellation_is_atomic_after_due_and_never_refunds_sequences_or_duplicate_history,
    late_input_topology_and_counter_failures_roll_back_every_staged_record,
    a_malformed_late_due_record_cannot_commit_an_earlier_activation,
    mismatched_scheduled_annotations_cannot_publish_copies_and_original_handles_remain_cancelable,
    admission_and_activation_each_commit_once_and_failed_commit_survives_reopen,
}
