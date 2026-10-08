use super::*;

pub(super) fn late_settlement_failures_rollback_every_staged_change<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        },
    )?;
    fixture.at(1, send("held"))?;
    let (sequence, lock_token) = hold(&fixture, 2)?;
    let patch = BTreeMap::from([("changed".into(), MessageValue::Bool(true))]);
    for disposition in [
        SettlementDisposition::Complete,
        SettlementDisposition::Abandon,
        SettlementDisposition::Defer,
        SettlementDisposition::DeadLetter {
            reason: "staged".into(),
            description: "must roll back".into(),
        },
    ] {
        let missing = matches!(
            disposition,
            SettlementDisposition::Complete | SettlementDisposition::DeadLetter { .. }
        );
        let envelope = atomic(
            &fixture,
            3,
            vec![
                send("new-history"),
                CommandKind::Settle {
                    sequence,
                    lock_token,
                    disposition,
                    properties_to_modify: patch.clone(),
                },
                CommandKind::Complete {
                    sequence,
                    lock_token,
                },
            ],
        )?;
        expect_health_refusal(
            &fixture,
            &envelope,
            if missing {
                BrokerError::MessageNotFound { sequence }
            } else {
                BrokerError::MessageNotLocked { sequence }
            },
        )?;
        let original = record(&fixture, &fixture.entity, sequence)?.expect("original still held");
        assert!(
            matches!(original.state, MessageState::Locked { token, .. } if token == lock_token)
        );
        assert!(original.envelope.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    "new-history"
                ))?
                .is_none()
        );
    }
    let envelope = atomic(
        &fixture,
        3,
        vec![
            send("new-history"),
            CommandKind::Complete {
                sequence,
                lock_token: LockToken::new(lock_token.as_u64() + 1),
            },
        ],
    )?;
    expect_health_refusal(
        &fixture,
        &envelope,
        BrokerError::LockTokenMismatch { sequence },
    )?;
    let original = record(&fixture, &fixture.entity, sequence)?.expect("held");
    let MessageState::Locked { locked_until, .. } = original.state else {
        panic!("lock")
    };
    let envelope = atomic(
        &fixture,
        locked_until.as_millis(),
        vec![
            send("new-history"),
            CommandKind::Complete {
                sequence,
                lock_token,
            },
        ],
    )?;
    expect_health_refusal(
        &fixture,
        &envelope,
        BrokerError::LockExpired {
            sequence,
            locked_until,
        },
    )?;
    Ok(())
}

pub(super) fn late_content_counter_and_stored_record_failures_are_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    fixture.at(1, send("held"))?;
    let (sequence, lock_token) = hold(&fixture, 2)?;
    let malformed = MessageEnvelope {
        body: MessageBody::Value(MessageValue::Array(vec![
            MessageValue::Bool(true),
            MessageValue::Int(1),
        ])),
        ..MessageEnvelope::default()
    };
    let error = malformed.validate().expect_err("heterogeneous AMQP array");
    let envelope = atomic(
        &fixture,
        3,
        vec![
            send("earlier"),
            CommandKind::SendEnvelope {
                message_id: "bad".into(),
                body: vec![],
                time_to_live_millis: None,
                session_id: None,
                envelope: Box::new(malformed),
            },
        ],
    )?;
    expect_health_refusal(&fixture, &envelope, error)?;
    let envelope = atomic(
        &fixture,
        3,
        vec![
            send("earlier"),
            CommandKind::Settle {
                sequence,
                lock_token,
                disposition: SettlementDisposition::Abandon,
                properties_to_modify: BTreeMap::from([("bad".into(), MessageValue::List(vec![]))]),
            },
        ],
    )?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    assert!(matches!(
        fixture.machine.apply_atomic_messaging(&envelope),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(observed(&fixture).commits, 0);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(2)
    );

    let old_counters = counters(&fixture)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            ..old_counters
        })?,
    ))?;
    let envelope = atomic(&fixture, 3, vec![send("last-legal"), send("exhausted")])?;
    expect_health_refusal(
        &fixture,
        &envelope,
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&old_counters)?,
    ))?;

    let bad_bytes = vec![255];
    let error =
        BrokerError::Codec(codec::decode::<MessageRecord>(&bad_bytes).expect_err("bad record"));
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message(&fixture.namespace, &fixture.entity, sequence),
        bad_bytes,
    ))?;
    let envelope = atomic(
        &fixture,
        3,
        vec![
            send("earlier"),
            CommandKind::Complete {
                sequence,
                lock_token,
            },
        ],
    )?;
    expect_health_refusal(&fixture, &envelope, error)?;
    Ok(())
}

pub(super) fn failed_commit_reopens_and_retries_the_same_serialized_envelope<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        },
    )?;
    fixture.at(1, send("held"))?;
    let (sequence, lock_token) = hold(&fixture, 2)?;
    let envelope = atomic(
        &fixture,
        3,
        vec![
            send("new"),
            CommandKind::SendBatch {
                messages: vec![member("new"), member("batch")],
            },
            CommandKind::Complete {
                sequence,
                lock_token,
            },
        ],
    )?;
    let bytes = codec::encode(&envelope)?;
    let decoded: AtomicMessagingCommand = codec::decode(&bytes)?;
    assert_eq!(decoded, envelope);
    for (original, replay) in envelope.commands.iter().zip(&decoded.commands) {
        assert_eq!(codec::encode(original)?, codec::encode(replay)?);
    }
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations")
        .expected_before_commit = Some(before.clone());
    fixture
        .machine
        .store()
        .fail_next
        .store(true, Ordering::Relaxed);
    assert_eq!(
        fixture.machine.apply_atomic_messaging(&decoded),
        Err(BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected atomic messaging failure".into(),
        }))
    );
    assert_eq!(observed(&fixture).commits, 1);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(2)
    );
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&decoded)?;
    assert_eq!(
        application.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            },
            CommandOutcome::BatchSent {
                sequences: vec![SequenceNumber::new(3), SequenceNumber::new(4)]
            },
            CommandOutcome::Completed,
        ]
    );
    assert_eq!(application.enqueue_targets, vec![fixture.entity.clone()]);
    assert_eq!(observed(&fixture).commits, 1);
    assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
    assert!(record(&fixture, &fixture.entity, SequenceNumber::new(3))?.is_none());
    assert_eq!(counters(&fixture)?.next_sequence, 5);
    let committed = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, committed);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(3)
    );
    Ok(())
}

pub(super) fn scope_stamp_and_stale_guards_precede_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    fixture.at(50, send("clock"))?;
    let original = atomic(&fixture, 0, vec![send("not-staged")])?;
    for change in 0..3 {
        let mut envelope = original.clone();
        match change {
            0 => envelope.commands[0].namespace = NamespaceName::new("other")?,
            1 => envelope.commands[0].entity = EntityPath::new("Orders")?,
            _ => envelope.commands[0].issued_at = Timestamp::from_millis(1),
        }
        expect_refusal(
            &fixture,
            &envelope,
            BrokerError::InvalidAtomicMessagingCommand,
        )?;
        assert!(!observed(&fixture).reads.contains(&keys::clock()));
    }
    let shadow = fixture.entity.dead_letter_queue()?;
    let mut envelope = original.clone();
    envelope.binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &shadow,
            &fixture.entity,
            EntityIncarnationKind::Queue,
        )?
        .expect("shadow binding");
    envelope.commands[0].entity = shadow;
    expect_refusal(
        &fixture,
        &envelope,
        BrokerError::AtomicMessagingOperationNotSupported,
    )?;
    assert!(!observed(&fixture).reads.contains(&keys::clock()));
    fixture.at(
        51,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    fixture.at(
        52,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let sequence = SequenceNumber::new(1);
    let lock_token = LockToken::new(1);
    for kinds in [
        vec![send("stale")],
        vec![CommandKind::SendEnvelope {
            message_id: "stale".into(),
            body: vec![],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(MessageEnvelope::default()),
        }],
        vec![CommandKind::SendBatch {
            messages: vec![member("stale")],
        }],
        vec![CommandKind::Complete {
            sequence,
            lock_token,
        }],
        vec![CommandKind::Abandon {
            sequence,
            lock_token,
        }],
        vec![CommandKind::Defer {
            sequence,
            lock_token,
        }],
        vec![CommandKind::DeadLetter {
            sequence,
            lock_token,
            reason: "stale".into(),
            description: String::new(),
        }],
        vec![CommandKind::Settle {
            sequence,
            lock_token,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        }],
        vec![],
    ] {
        let stale = AtomicMessagingCommand {
            binding: original.binding.clone(),
            issued_at: Timestamp::from_millis(0),
            commands: kinds
                .into_iter()
                .map(|kind| fixture.command(0, kind))
                .collect(),
        };
        expect_refusal(&fixture, &stale, BrokerError::EntityBindingStale)?;
        assert!(!observed(&fixture).reads.contains(&keys::clock()));
    }
    let current = atomic(&fixture, 0, vec![send("still-regressed")])?;
    assert_eq!(fixture.machine.validate_atomic_messaging(&current), Ok(()));
    expect_health_refusal(
        &fixture,
        &current,
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(52),
            proposed: Timestamp::from_millis(0),
        },
    )?;
    Ok(())
}

pub(super) fn empty_envelope_validates_identity_without_clock_or_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    fixture.at(100, send("clock"))?;
    let empty_envelope = atomic(&fixture, 0, vec![])?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    assert_eq!(
        fixture.machine.validate_atomic_messaging(&empty_envelope),
        Ok(())
    );
    let application = fixture.machine.apply_atomic_messaging(&empty_envelope)?;
    assert!(application.outcomes.is_empty());
    assert!(application.enqueue_targets.is_empty());
    let observations = observed(&fixture);
    assert_eq!(observations.commits, 0);
    assert_health_probes(&empty_envelope.binding, &observations);
    assert_eq!(observations.scans.len(), 2);
    assert!(!observations.reads.contains(&keys::clock()));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(100)
    );

    let unsupported = vec![
        CommandKind::RenewLock {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
            lock_duration_millis: None,
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
        CommandKind::Schedule { messages: vec![] },
        CommandKind::ScheduleEnvelopes { messages: vec![] },
        CommandKind::ActivateScheduled,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate::default(),
        },
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(1),
            max_messages: 1,
            session_id: None,
        },
        CommandKind::ExpireLocks,
    ];
    for kind in unsupported {
        assert_eq!(
            validate_atomic_messaging_kinds(std::slice::from_ref(&kind)),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        let envelope = atomic(&fixture, 0, vec![kind])?;
        expect_refusal(
            &fixture,
            &envelope,
            BrokerError::AtomicMessagingOperationNotSupported,
        )?;
        assert!(!observed(&fixture).reads.contains(&keys::clock()));
    }
    let mut scheduled = member("scheduled");
    for timestamp in [0, 100, 200] {
        scheduled.scheduled_enqueue_time = Some(Timestamp::from_millis(timestamp));
        let envelope = atomic(
            &fixture,
            0,
            vec![CommandKind::SendBatch {
                messages: vec![scheduled.clone()],
            }],
        )?;
        expect_refusal(
            &fixture,
            &envelope,
            BrokerError::AtomicMessagingOperationNotSupported,
        )?;
    }
    let mut session = member("session");
    session.session_id = Some(SessionId::new("A")?);
    let envelope = atomic(
        &fixture,
        0,
        vec![CommandKind::SendBatch {
            messages: vec![session],
        }],
    )?;
    expect_refusal(
        &fixture,
        &envelope,
        BrokerError::AtomicMessagingOperationNotSupported,
    )?;

    let session_entity = EntityPath::new("session")?;
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        session_entity.clone(),
        Timestamp::from_millis(101),
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    ))?;
    let binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &session_entity,
            &session_entity,
            EntityIncarnationKind::Queue,
        )?
        .expect("session binding");
    let empty = AtomicMessagingCommand {
        binding,
        issued_at: Timestamp::from_millis(0),
        commands: vec![],
    };
    expect_refusal(
        &fixture,
        &empty,
        BrokerError::AtomicMessagingOperationNotSupported,
    )?;
    let topic = EntityPath::new("topic")?;
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        topic.clone(),
        Timestamp::from_millis(102),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    ))?;
    let binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &topic,
            &topic,
            EntityIncarnationKind::Topic,
        )?
        .expect("topic binding");
    expect_refusal(
        &fixture,
        &AtomicMessagingCommand {
            binding,
            issued_at: Timestamp::from_millis(0),
            commands: vec![],
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    )?;
    let name = SubscriptionName::new("child")?;
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        topic.clone(),
        Timestamp::from_millis(103),
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    ))?;
    let child = topic.subscription(&name)?;
    let binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &child,
            &child,
            EntityIncarnationKind::Subscription,
        )?
        .expect("subscription binding");
    expect_refusal(
        &fixture,
        &AtomicMessagingCommand {
            binding,
            issued_at: Timestamp::from_millis(0),
            commands: vec![],
        },
        BrokerError::AtomicMessagingOperationNotSupported,
    )?;

    let shadow = fixture.entity.dead_letter_queue()?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(keys::queue_config(&fixture.namespace, &shadow)))?;
    expect_refusal(
        &fixture,
        &empty_envelope,
        BrokerError::DanglingEntityMetadata,
    )?;
    Ok(())
}
