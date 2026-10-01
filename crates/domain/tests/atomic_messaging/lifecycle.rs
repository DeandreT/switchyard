use super::*;

pub(super) fn ordered_sends_share_dedup_and_counters<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        },
    )?;
    let envelope = atomic(
        &fixture,
        1,
        vec![
            send("shared"),
            CommandKind::SendBatch {
                messages: vec![member("shared"), member("second")],
            },
            send("second"),
            send(""),
        ],
    )?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations")
        .expected_before_commit = Some(before);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(
        application.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            },
            CommandOutcome::BatchSent {
                sequences: vec![SequenceNumber::new(2), SequenceNumber::new(3)]
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(4)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(5)
            },
        ]
    );
    assert_eq!(application.enqueue_targets, vec![fixture.entity.clone()]);
    let observations = observed(&fixture);
    assert_eq!(observations.commits, 1);
    assert_eq!((observations.scans, observations.snapshots), (0, 0));
    assert!(observations.reads.len() <= MAX_ATOMIC_MESSAGING_READ_OPERATIONS);
    assert!(
        observations.reads.iter().map(Vec::len).sum::<usize>()
            <= MAX_ATOMIC_MESSAGING_READ_KEY_BYTES
    );
    for key in [
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        keys::clock(),
    ] {
        assert_eq!(observations.mutations.iter().filter(|mutation| matches!(mutation, Mutation::Put { key: written, .. } if written == &key)).count(), 1);
    }
    assert_eq!(counters(&fixture)?.next_sequence, 6);
    for sequence in [1, 3, 5] {
        assert!(record(&fixture, &fixture.entity, SequenceNumber::new(sequence))?.is_some());
    }
    for sequence in [2, 4] {
        assert!(record(&fixture, &fixture.entity, SequenceNumber::new(sequence))?.is_none());
    }
    for id in ["shared", "second"] {
        let deadline: Timestamp = codec::decode(
            &fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    id,
                ))?
                .expect("committed history"),
        )?;
        assert_eq!(deadline, Timestamp::from_millis(20_001));
    }
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
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(1)
    );
    Ok(())
}

pub(super) fn settlements_preserve_order_and_final_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    fixture.at(
        1,
        CommandKind::SendBatch {
            messages: (1..=5).map(|id| member(&id.to_string())).collect(),
        },
    )?;
    let held = (0..5)
        .map(|_| hold(&fixture, 2))
        .collect::<TestResult<Vec<_>>>()?;
    let mut patch = BTreeMap::new();
    patch.insert("patched".into(), MessageValue::Bool(true));
    let envelope = atomic(
        &fixture,
        3,
        vec![
            CommandKind::Complete {
                sequence: held[0].0,
                lock_token: held[0].1,
            },
            CommandKind::Abandon {
                sequence: held[1].0,
                lock_token: held[1].1,
            },
            CommandKind::Defer {
                sequence: held[2].0,
                lock_token: held[2].1,
            },
            CommandKind::DeadLetter {
                sequence: held[3].0,
                lock_token: held[3].1,
                reason: "application".into(),
                description: "explicit".into(),
            },
            CommandKind::Settle {
                sequence: held[4].0,
                lock_token: held[4].1,
                disposition: SettlementDisposition::Abandon,
                properties_to_modify: patch.clone(),
            },
            send("new"),
        ],
    )?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(
        application.outcomes,
        vec![
            CommandOutcome::Completed,
            CommandOutcome::Abandoned {
                dead_lettered: false,
                dropped: false
            },
            CommandOutcome::Deferred,
            CommandOutcome::DeadLettered,
            CommandOutcome::Abandoned {
                dead_lettered: false,
                dropped: false
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(6)
            },
        ]
    );
    let shadow = fixture.entity.dead_letter_queue()?;
    assert_eq!(
        application.enqueue_targets,
        vec![fixture.entity.clone(), shadow.clone()]
    );
    assert_eq!(observed(&fixture).commits, 1);
    assert!(record(&fixture, &fixture.entity, held[0].0)?.is_none());
    assert_eq!(
        record(&fixture, &fixture.entity, held[1].0)?
            .expect("abandoned")
            .state,
        MessageState::Ready
    );
    assert_eq!(
        record(&fixture, &fixture.entity, held[2].0)?
            .expect("deferred")
            .state,
        MessageState::Deferred
    );
    assert!(record(&fixture, &fixture.entity, held[3].0)?.is_none());
    let dead = record(&fixture, &shadow, held[3].0)?.expect("shadow copy");
    assert_eq!(dead.dead_letter.expect("reason").description, "explicit");
    let changed = record(&fixture, &fixture.entity, held[4].0)?.expect("patched");
    assert_eq!(
        changed
            .envelope
            .expect("retained envelope")
            .application_properties,
        patch
    );
    assert_eq!(counters(&fixture)?.next_sequence, 7);
    assert_eq!(counters(&fixture)?.next_lock_token, 6);
    Ok(())
}

pub(super) fn empty_sections_and_settlement_only_have_exact_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    fixture.at(1, send("held"))?;
    let (sequence, lock_token) = hold(&fixture, 2)?;
    let before = fixture.machine.store().snapshot()?;
    let envelope = atomic(
        &fixture,
        50,
        vec![CommandKind::SendBatch { messages: vec![] }; 3],
    )?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(
        application.outcomes,
        vec![CommandOutcome::BatchSent { sequences: vec![] }; 3]
    );
    assert!(application.enqueue_targets.is_empty());
    assert_eq!(observed(&fixture).commits, 0);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(2)
    );
    let envelope = atomic(
        &fixture,
        3,
        vec![CommandKind::Settle {
            sequence,
            lock_token,
            disposition: SettlementDisposition::Defer,
            properties_to_modify: BTreeMap::from([("updated".into(), MessageValue::Int(7))]),
        }],
    )?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(application.outcomes, vec![CommandOutcome::Deferred]);
    assert!(application.enqueue_targets.is_empty());
    assert_eq!(observed(&fixture).commits, 1);
    assert_eq!(
        record(&fixture, &fixture.entity, sequence)?
            .expect("deferred")
            .state,
        MessageState::Deferred
    );
    Ok(())
}
