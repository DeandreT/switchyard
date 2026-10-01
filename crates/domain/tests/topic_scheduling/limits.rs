use super::*;

fn sized(id: &str, content_bytes: usize) -> IngressEnvelope {
    let mut message = member(id, None);
    message.envelope = MessageEnvelope::default();
    message.body =
        vec![0; content_bytes - message.message_id.len() - message.envelope.content_size()];
    message
}

fn values(id: &str, nodes: usize) -> IngressEnvelope {
    let mut message = member(id, None);
    message.body.clear();
    message.envelope = MessageEnvelope {
        body: MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; nodes - 1])),
        ..MessageEnvelope::default()
    };
    message
        .envelope
        .validate()
        .expect("homogeneous array is valid");
    message
}

fn copy_budget_commits_a_due_prefix_without_consuming_the_next_handle<P: StoreProvider>(
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
    let mut children = Vec::new();
    for index in 0..32 {
        children.push(subscribe(
            &fixture,
            &format!("sub{index:02}"),
            SubscriptionConfig::default(),
            0,
        )?);
    }
    let admission = apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: (0..33)
                .map(|index| scheduled(member(&format!("id{index:02}"), None), 100))
                .collect(),
        },
    )?;
    assert_eq!(
        admission.outcome,
        CommandOutcome::Scheduled {
            sequences: (1..=33).map(SequenceNumber::new).collect()
        }
    );
    effects(&admission, &[]);
    *observations.lock().expect("observations") = Observations::default();
    activate(
        &fixture,
        100,
        (MAX_TOPIC_FANOUT_COPIES / children.len()) as u32,
        &children,
    )?;
    pending(&fixture, 33, 100)?;
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("active topic counter")
            .next_sequence,
        66
    );
    for sequence in 1..=32 {
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
    }
    for child in &children {
        let copies = fixture
            .machine
            .store()
            .scan_prefix(&keys::message_prefix(&fixture.namespace, child), 34)?;
        assert_eq!(copies.len(), 32);
        assert_eq!(
            record(&fixture, child, 34)?
                .expect("first active sequence")
                .message_id,
            "id00"
        );
        assert_eq!(
            record(&fixture, child, 65)?
                .expect("last fitting active sequence")
                .message_id,
            "id31"
        );
        assert!(record(&fixture, child, 66)?.is_none());
        assert_eq!(counters(&fixture, child)?, None);
    }
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        let prefix = keys::scheduled_prefix(&fixture.namespace, &fixture.entity);
        let scans: Vec<_> = observed
            .scans
            .iter()
            .filter(|(seen, _, _)| *seen == prefix)
            .collect();
        assert_eq!(scans.len(), 1);
        assert!(scans[0].1 <= TIMER_SCAN_LIMIT);
        assert_eq!(scans[0].2, 33);
    }
    activate(&fixture, 100, 1, &children)?;
    assert!(record(&fixture, &fixture.entity, 33)?.is_none());
    for child in &children {
        assert_eq!(
            record(&fixture, child, 66)?
                .expect("remaining due input")
                .message_id,
            "id32"
        );
    }
    let before = fixture.machine.store().snapshot()?;
    activate(&fixture, 100, 0, &[])?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn bytes_and_values_each_stop_at_the_exact_retained_boundary<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let config = SubscriptionConfig {
        max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        ..SubscriptionConfig::default()
    };
    let alpha = subscribe(&fixture, "alpha", config, 0)?;
    let beta = subscribe(&fixture, "beta", config, 0)?;
    let per_input = MAX_TOPIC_FANOUT_CONTENT_BYTES / 2;
    let first = sized("first", per_input);
    let second = sized("second", per_input);
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(first.clone(), 100),
                scheduled(second.clone(), 100),
            ],
        },
    )?;
    activate(&fixture, 100, 1, &[alpha.clone(), beta.clone()])?;
    pending(&fixture, 2, 100)?;
    assert_eq!(
        record(&fixture, &alpha, 3)?
            .expect("exact byte boundary")
            .body,
        first.body
    );
    assert!(record(&fixture, &alpha, 4)?.is_none());
    activate(&fixture, 100, 1, &[alpha.clone(), beta.clone()])?;
    assert_eq!(
        record(&fixture, &beta, 4)?.expect("byte remainder").body,
        second.body
    );
    let per_input = MAX_TOPIC_FANOUT_VALUE_ITEMS / 2;
    apply(
        &fixture,
        101,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(values("array-one", per_input), 200),
                scheduled(values("array-two", per_input), 200),
            ],
        },
    )?;
    activate(&fixture, 200, 1, &[alpha.clone(), beta.clone()])?;
    pending(&fixture, 6, 200)?;
    for entity in [&alpha, &beta] {
        let copy = record(&fixture, entity, 7)?.expect("exact projected value boundary");
        let MessageBody::Value(MessageValue::Array(items)) =
            copy.envelope.expect("typed array").body
        else {
            panic!("array copy")
        };
        assert_eq!(items.len() + 1, per_input);
        assert!(record(&fixture, entity, 8)?.is_none());
        assert_eq!(counters(&fixture, entity)?, None);
    }
    activate(&fixture, 200, 1, &[alpha.clone(), beta.clone()])?;
    assert!(record(&fixture, &fixture.entity, 6)?.is_none());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("sequences only allocated for consumed inputs")
            .next_sequence,
        9
    );
    Ok(())
}

fn later_membership_can_make_a_head_unfit_but_never_mutates_it_and_cancellation_remains_available<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let config = SubscriptionConfig {
        max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        ..SubscriptionConfig::default()
    };
    let alpha = subscribe(&fixture, "alpha", config, 0)?;
    let large = sized("large", MAX_TOPIC_FANOUT_CONTENT_BYTES / 2 + 1);
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(large.clone(), 100)],
        },
    )?;
    let beta = subscribe(&fixture, "beta", config, 2)?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    assert_eq!(pending(&fixture, 1, 100)?.body, large.body);
    for child in [&alpha, &beta] {
        assert!(record(&fixture, child, 2)?.is_none());
    }
    let cancelled = apply(
        &fixture,
        101,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(1)],
        },
    )?;
    assert_eq!(
        cancelled.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    effects(&cancelled, &[]);
    reject(
        &fixture,
        102,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(large, 200)],
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    // Three copies need 65,535 original values; the null-session projection's
    // two metadata values are what takes this formerly valid input over budget.
    let nodes = MAX_TOPIC_FANOUT_VALUE_ITEMS / 3;
    assert_eq!(nodes * 3, MAX_TOPIC_FANOUT_VALUE_ITEMS - 1);
    apply(
        &fixture,
        103,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(values("null", nodes), 200)],
        },
    )?;
    let session = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..SubscriptionConfig::default()
        },
        104,
    )?;
    reject(
        &fixture,
        200,
        CommandKind::ActivateScheduled,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ValueItems,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS,
        },
    )?;
    pending(&fixture, 2, 200)?;
    assert!(record(&fixture, &session.dead_letter_queue()?, 3)?.is_none());
    for child in [&alpha, &beta, &session] {
        assert_eq!(counters(&fixture, child)?, None);
    }
    let cancelled = apply(
        &fixture,
        201,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(2)],
        },
    )?;
    assert_eq!(
        cancelled.outcome,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    effects(&cancelled, &[]);
    Ok(())
}

fn zero_subscribers_still_enforce_input_limits_and_scan_only_256_due_entries<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig {
            max_message_bytes: MAX_INGRESS_BATCH_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::ScheduleEnvelopes {
            messages: (0..=MAX_INGRESS_BATCH_MESSAGES)
                .map(|_| scheduled(member("input", None), 100))
                .collect(),
        },
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::Messages,
            actual: MAX_INGRESS_BATCH_MESSAGES + 1,
            maximum: MAX_INGRESS_BATCH_MESSAGES,
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(
                sized("large", MAX_INGRESS_BATCH_CONTENT_BYTES + 1),
                100,
            )],
        },
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ContentBytes,
            actual: MAX_INGRESS_BATCH_CONTENT_BYTES + 1,
            maximum: MAX_INGRESS_BATCH_CONTENT_BYTES,
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(values("one", MAX_TOPIC_FANOUT_VALUE_ITEMS / 2), 100),
                scheduled(values("two", MAX_TOPIC_FANOUT_VALUE_ITEMS / 2 + 1), 100),
            ],
        },
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ValueItems,
            actual: MAX_TOPIC_FANOUT_VALUE_ITEMS + 1,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS,
        },
    )?;
    assert_eq!(counters(&fixture, &fixture.entity)?, None);
    effects(
        &apply(
            &fixture,
            1,
            CommandKind::ScheduleEnvelopes {
                messages: (0..=TIMER_SCAN_LIMIT)
                    .map(|index| scheduled(member(&format!("id{index}"), None), 100))
                    .collect(),
            },
        )?,
        &[],
    );
    *observations.lock().expect("observations") = Observations::default();
    activate(&fixture, 100, TIMER_SCAN_LIMIT as u32, &[])?;
    pending(&fixture, TIMER_SCAN_LIMIT as u64 + 1, 100)?;
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("zero-destination activation still allocates shared positions")
            .next_sequence,
        (TIMER_SCAN_LIMIT * 2 + 2) as u64
    );
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        let prefix = keys::scheduled_prefix(&fixture.namespace, &fixture.entity);
        let scans: Vec<_> = observed
            .scans
            .iter()
            .filter(|(seen, _, _)| *seen == prefix)
            .collect();
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].1, TIMER_SCAN_LIMIT);
        assert_eq!(scans[0].2, TIMER_SCAN_LIMIT);
    }
    activate(&fixture, 100, 1, &[])?;
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::message_prefix(&fixture.namespace, &fixture.entity),
                1
            )?
            .is_empty()
    );
    let child = subscribe(&fixture, "late", SubscriptionConfig::default(), 101)?;
    assert!(peek(&fixture, &child, 101, 0, 100, None)?.is_empty());
    let before = fixture.machine.store().snapshot()?;
    activate(&fixture, 101, 0, &[])?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn zero_subscriber_activation_also_limits_original_decoded_bytes_and_values<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig {
            max_message_bytes: MAX_INGRESS_BATCH_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    // Separate admissions are individually valid but do not fit one activation.
    for (millis, id) in [(1, "first"), (2, "second")] {
        effects(
            &apply(
                &fixture,
                millis,
                CommandKind::ScheduleEnvelopes {
                    messages: vec![scheduled(sized(id, MAX_INGRESS_BATCH_CONTENT_BYTES), 100)],
                },
            )?,
            &[],
        );
    }
    *observations.lock().expect("observations") = Observations::default();
    activate(&fixture, 100, 1, &[])?;
    assert!(record(&fixture, &fixture.entity, 1)?.is_none());
    pending(&fixture, 2, 100)?;
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("only first original content fits")
            .next_sequence,
        4
    );
    assert_eq!(observations.lock().expect("observations").commits, 1);
    activate(&fixture, 100, 1, &[])?;
    for (millis, id) in [(101, "array-one"), (102, "array-two")] {
        effects(
            &apply(
                &fixture,
                millis,
                CommandKind::ScheduleEnvelopes {
                    messages: vec![scheduled(values(id, MAX_TOPIC_FANOUT_VALUE_ITEMS), 200)],
                },
            )?,
            &[],
        );
    }
    *observations.lock().expect("observations") = Observations::default();
    activate(&fixture, 200, 1, &[])?;
    assert!(record(&fixture, &fixture.entity, 5)?.is_none());
    pending(&fixture, 6, 200)?;
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("only first original value tree fits")
            .next_sequence,
        8
    );
    assert_eq!(observations.lock().expect("observations").commits, 1);
    activate(&fixture, 200, 1, &[])?;
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::message_prefix(&fixture.namespace, &fixture.entity),
                1
            )?
            .is_empty()
    );
    let before = fixture.machine.store().snapshot()?;
    activate(&fixture, 200, 0, &[])?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    copy_budget_commits_a_due_prefix_without_consuming_the_next_handle,
    bytes_and_values_each_stop_at_the_exact_retained_boundary,
    later_membership_can_make_a_head_unfit_but_never_mutates_it_and_cancellation_remains_available,
    zero_subscribers_still_enforce_input_limits_and_scan_only_256_due_entries,
    zero_subscriber_activation_also_limits_original_decoded_bytes_and_values,
}
