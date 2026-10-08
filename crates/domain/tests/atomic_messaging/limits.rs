use super::*;

pub(super) fn aggregate_actions_and_messages_include_duplicates<P: StoreProvider>(
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
    let empty = CommandKind::SendBatch { messages: vec![] };
    let envelope = atomic(
        &fixture,
        1,
        vec![empty.clone(); MAX_ATOMIC_MESSAGING_ACTIONS],
    )?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(application.outcomes.len(), MAX_ATOMIC_MESSAGING_ACTIONS);
    assert!(application.enqueue_targets.is_empty());
    assert_eq!(observed(&fixture).commits, 0);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(0)
    );
    let envelope = atomic(&fixture, 1, vec![empty; MAX_ATOMIC_MESSAGING_ACTIONS + 1])?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(AtomicMessagingLimit::Actions, MAX_ATOMIC_MESSAGING_ACTIONS),
    )?;
    assert!(observed(&fixture).reads.is_empty());

    let envelope = atomic(
        &fixture,
        1,
        vec![
            CommandKind::SendBatch {
                messages: vec![member("same"); 50],
            },
            CommandKind::SendBatch {
                messages: vec![member("same"); 50],
            },
        ],
    )?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert_eq!(
        application.outcomes,
        vec![
            CommandOutcome::BatchSent {
                sequences: (1..=50).map(SequenceNumber::new).collect()
            },
            CommandOutcome::BatchSent {
                sequences: (51..=100).map(SequenceNumber::new).collect()
            },
        ]
    );
    assert_eq!(application.enqueue_targets, vec![fixture.entity.clone()]);
    assert_eq!(counters(&fixture)?.next_sequence, 101);
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::message_prefix(&fixture.namespace, &fixture.entity),
                101
            )?
            .len(),
        1
    );
    let envelope = atomic(
        &fixture,
        2,
        vec![
            CommandKind::SendBatch {
                messages: vec![member("same"); 50],
            },
            CommandKind::SendBatch {
                messages: vec![member("same"); 51],
            },
        ],
    )?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::Messages,
            MAX_ATOMIC_MESSAGING_MESSAGES,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());
    let envelope = atomic(
        &fixture,
        2,
        vec![CommandKind::SendBatch {
            messages: vec![member("same"); 100],
        }],
    )?;
    let application = fixture.machine.apply_atomic_messaging(&envelope)?;
    assert!(
        application.enqueue_targets.is_empty(),
        "discarded duplicates do not wake receivers"
    );
    assert_eq!(counters(&fixture)?.next_sequence, 201);
    Ok(())
}

fn raw_send(body: Vec<u8>) -> CommandKind {
    CommandKind::Send {
        message_id: String::new(),
        body,
        time_to_live_millis: None,
        session_id: None,
    }
}

fn value_send(children: usize) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: String::new(),
        body: vec![],
        time_to_live_millis: None,
        session_id: None,
        envelope: Box::new(MessageEnvelope {
            body: MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children])),
            ..MessageEnvelope::default()
        }),
    }
}

pub(super) fn aggregate_content_and_value_boundaries<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            max_message_bytes: 5 * 1024 * 1024,
            ..QueueConfig::default()
        },
    )?;
    let half = MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2;
    let envelope = atomic(
        &fixture,
        1,
        vec![raw_send(vec![7; half]), raw_send(vec![9; half])],
    )?;
    reset(&fixture);
    assert_eq!(
        fixture.machine.apply_atomic_messaging(&envelope)?.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            },
        ]
    );
    assert_eq!(observed(&fixture).commits, 1);
    let envelope = atomic(
        &fixture,
        2,
        vec![raw_send(vec![7; half]), raw_send(vec![9; half + 1])],
    )?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());
    let half_nodes = MAX_ATOMIC_MESSAGING_VALUE_ITEMS / 2;
    let envelope = atomic(
        &fixture,
        2,
        vec![value_send(half_nodes - 1), value_send(half_nodes - 1)],
    )?;
    assert_eq!(
        fixture.machine.apply_atomic_messaging(&envelope)?.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(3)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(4)
            },
        ]
    );
    let envelope = atomic(
        &fixture,
        3,
        vec![value_send(half_nodes - 1), value_send(half_nodes)],
    )?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ValueItems,
            MAX_ATOMIC_MESSAGING_VALUE_ITEMS,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());
    Ok(())
}

pub(super) fn borrowed_patch_detail_and_empty_section_budgets<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, QueueConfig::default())?;
    let mut patch = CommandKind::Settle {
        sequence: SequenceNumber::new(99),
        lock_token: LockToken::new(1),
        disposition: SettlementDisposition::Abandon,
        properties_to_modify: BTreeMap::from([(
            "k".into(),
            MessageValue::Binary(vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES - 6]),
        )]),
    };
    // Binary content includes its five-byte AMQP width overhead; the key adds one.
    assert_eq!(
        validate_atomic_messaging_kinds(std::slice::from_ref(&patch)),
        Ok(())
    );
    if let CommandKind::Settle {
        properties_to_modify,
        ..
    } = &mut patch
    {
        properties_to_modify.insert(
            "k".into(),
            MessageValue::Binary(vec![0; MAX_ATOMIC_MESSAGING_CONTENT_BYTES - 5]),
        );
    }
    let envelope = atomic(&fixture, 1, vec![patch])?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());
    let mut detail = CommandKind::DeadLetter {
        sequence: SequenceNumber::new(99),
        lock_token: LockToken::new(1),
        reason: "r".repeat(MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2),
        description: "d".repeat(MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 2),
    };
    assert_eq!(
        validate_atomic_messaging_kinds(std::slice::from_ref(&detail)),
        Ok(())
    );
    if let CommandKind::DeadLetter { description, .. } = &mut detail {
        description.push('d');
    }
    let envelope = atomic(&fixture, 1, vec![detail])?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());

    let sections = MAX_ATOMIC_MESSAGING_CONTENT_BYTES / 15;
    let mut content = MessageEnvelope {
        body: MessageBody::Data(vec![vec![]; sections]),
        ..MessageEnvelope::default()
    };
    assert_eq!(
        content.content_size() - MessageEnvelope::default().content_size(),
        sections * 15
    );
    if let MessageBody::Data(sections) = &mut content.body {
        sections.push(vec![]);
    }
    let envelope = atomic(
        &fixture,
        1,
        vec![CommandKind::SendEnvelope {
            message_id: String::new(),
            body: vec![],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(content),
        }],
    )?;
    expect_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ContentBytes,
            MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
        ),
    )?;
    assert!(observed(&fixture).reads.is_empty());
    Ok(())
}

fn seed_held<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    count: usize,
    body_bytes: usize,
) -> TestResult<Vec<(SequenceNumber, LockToken)>> {
    fixture.at(1, raw_send(vec![1; body_bytes]))?;
    let (first, first_token) = hold(fixture, 2)?;
    let sample = record(fixture, &fixture.entity, first)?.expect("legitimate held sample");
    assert!(sample.envelope.is_none());
    let MessageState::Locked { locked_until, .. } = sample.state else {
        panic!("locked sample")
    };
    let mut batch = WriteBatch::default();
    let mut held = vec![(first, first_token)];
    for id in 2..=count as u64 {
        let sequence = SequenceNumber::new(id);
        let token = LockToken::new(id);
        let mut message = sample.clone();
        message.sequence = sequence;
        message.state = MessageState::Locked {
            token,
            locked_until,
        };
        batch.push_put(
            keys::message(&fixture.namespace, &fixture.entity, sequence),
            codec::encode(&message)?,
        );
        batch.push_put(
            keys::lock(&fixture.namespace, &fixture.entity, locked_until, sequence),
            vec![],
        );
        held.push((sequence, token));
    }
    batch.push_put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: count as u64 + 1,
            next_lock_token: count as u64 + 1,
        })?,
    );
    fixture.machine.store().apply(batch)?;
    Ok(held)
}

pub(super) fn repeated_reads_charge_values_before_decode<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            max_message_bytes: 1024 * 1024,
            ..QueueConfig::default()
        },
    )?;
    let held = seed_held(&fixture, 100, 170 * 1024)?;
    let envelope = atomic(
        &fixture,
        3,
        held.iter()
            .map(|&(sequence, lock_token)| CommandKind::Complete {
                sequence,
                lock_token,
            })
            .collect(),
    )?;
    expect_health_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ReadValueBytes,
            MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES,
        ),
    )?;
    let observations = observed(&fixture);
    assert!(observations.read_value_bytes > MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES);
    let prefix = atomic(
        &fixture,
        3,
        held[..40]
            .iter()
            .map(|&(sequence, lock_token)| CommandKind::Complete {
                sequence,
                lock_token,
            })
            .collect(),
    )?;
    reset(&fixture);
    assert_eq!(
        fixture.machine.apply_atomic_messaging(&prefix)?.outcomes,
        vec![CommandOutcome::Completed; 40]
    );
    assert_eq!(observed(&fixture).commits, 1);
    assert!(record(&fixture, &fixture.entity, held[39].0)?.is_none());
    assert!(record(&fixture, &fixture.entity, held[40].0)?.is_some());

    let &(sequence, lock_token) = held.last().expect("last held");
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message(&fixture.namespace, &fixture.entity, sequence),
        vec![255; MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES + 1],
    ))?;
    let envelope = atomic(
        &fixture,
        4,
        vec![CommandKind::Complete {
            sequence,
            lock_token,
        }],
    )?;
    expect_health_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::ReadValueBytes,
            MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES,
        ),
    )?;
    Ok(())
}

pub(super) fn generated_put_bytes_are_cumulative_not_final_size<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(
        provider,
        QueueConfig {
            max_message_bytes: 1024 * 1024,
            ..QueueConfig::default()
        },
    )?;
    let held = seed_held(&fixture, 100, 164 * 1024)?;
    let patch = BTreeMap::from([("p".into(), MessageValue::Bool(true))]);
    let kinds: Vec<_> = held
        .iter()
        .map(|&(sequence, lock_token)| CommandKind::Settle {
            sequence,
            lock_token,
            disposition: SettlementDisposition::Abandon,
            properties_to_modify: patch.clone(),
        })
        .collect();
    let envelope = atomic(&fixture, 3, kinds.clone())?;
    expect_health_refusal(
        &fixture,
        &envelope,
        limit(
            AtomicMessagingLimit::MutationValueBytes,
            MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES,
        ),
    )?;
    assert!(observed(&fixture).read_value_bytes < MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES);
    let prefix = atomic(&fixture, 3, kinds[..40].to_vec())?;
    reset(&fixture);
    let application = fixture.machine.apply_atomic_messaging(&prefix)?;
    assert_eq!(
        application.outcomes,
        vec![
            CommandOutcome::Abandoned {
                dead_lettered: false,
                dropped: false
            };
            40
        ]
    );
    assert_eq!(application.enqueue_targets, vec![fixture.entity.clone()]);
    let observations = observed(&fixture);
    assert_eq!(observations.commits, 1);
    assert!(
        observations
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                Mutation::Put { value, .. } => Some(value.len()),
                _ => None,
            })
            .sum::<usize>()
            < MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES
    );
    let changed = record(&fixture, &fixture.entity, held[0].0)?.expect("abandoned");
    assert_eq!(changed.state, MessageState::Ready);
    assert_eq!(
        changed
            .envelope
            .expect("legacy content materialized")
            .application_properties,
        patch
    );
    assert!(matches!(
        record(&fixture, &fixture.entity, held[40].0)?
            .expect("unmodified tail")
            .state,
        MessageState::Locked { .. }
    ));
    Ok(())
}
