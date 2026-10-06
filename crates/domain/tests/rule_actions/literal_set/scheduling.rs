use super::*;

pub(super) fn literal_set_current_actions_are_evaluated_at_scheduled_activation<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    add(
        &fixture,
        "child",
        "old",
        RuleFilter::True,
        Some("SET number=9"),
        0,
    )?;
    publish(&fixture, 1, vec![member("active")])?;
    let old = records(&fixture, &child)?;
    let admission = apply(
        &fixture,
        2,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("future"), 100)],
        },
    )?;
    assert_eq!(
        admission.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(3)]
        }
    );
    let source = record(&fixture, &fixture.entity, 3)?.expect("unmutated scheduled source");
    assert_eq!(source.envelope.as_deref(), Some(&member("future").envelope));
    remove(&fixture, "child", "old", 3)?;
    add(
        &fixture,
        "child",
        "a-current",
        RuleFilter::True,
        Some("SET number=11;SET added=TRUE"),
        4,
    )?;
    add(
        &fixture,
        "child",
        "b-current-failure",
        RuleFilter::True,
        Some("REMOVE color;SET number='bad'"),
        4,
    )?;
    assert_eq!(records(&fixture, &child)?, old);
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    effects(&activation, &[child.clone(), child.dead_letter_queue()?]);
    assert!(record(&fixture, &fixture.entity, 3)?.is_none());
    let current = record(&fixture, &child, 5)?.expect("current SET action");
    assert_eq!(properties(&current)["number"], MessageValue::Int(11));
    assert_eq!(properties(&current)["added"], MessageValue::Bool(true));
    let failed =
        record(&fixture, &child.dead_letter_queue()?, 6)?.expect("current conversion failure");
    assert_eq!(
        properties(&failed)["color"],
        MessageValue::String("Red".into())
    );
    assert_eq!(properties(&failed)["number"], MessageValue::Int(7));
    assert_eq!(
        failed.dead_letter.as_ref().expect("action reason").reason,
        DeadLetterReason::Application("SwitchyardSqlActionError".into())
    );
    for copy in [record(&fixture, &child, 4)?.expect("base"), current, failed] {
        assert_eq!(
            copy.scheduled_enqueue_time,
            Some(Timestamp::from_millis(100))
        );
        assert_eq!(copy.enqueued_at, Timestamp::from_millis(100));
    }
    assert_eq!(record(&fixture, &child, 1)?, Some(old[0].clone()));
    assert_eq!(record(&fixture, &child, 2)?, Some(old[1].clone()));
    Ok(())
}

pub(super) fn literal_set_unfit_activation_head_remains_pending_and_cancelable<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let refuse_activation =
        |fixture: &QueueFixture<ObservedProvider<P>>, millis: u64| -> TestResult<BrokerError> {
            let applies = observations.lock().expect("observations").commits;
            let error = refuses(fixture, millis, CommandKind::ActivateScheduled)?;
            assert_eq!(observations.lock().expect("observations").commits, applies);
            Ok(error)
        };
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..SubscriptionConfig::default()
        },
    )?;
    let shadow = child.dead_letter_queue()?;
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: (0..33)
                .map(|index| scheduled(member(&format!("future-{index}")), 100))
                .collect(),
        },
    )?;
    for index in 0..31 {
        let source = if index == 30 {
            "SET number='incompatible'"
        } else {
            "SET number=9"
        };
        add(
            &fixture,
            "child",
            &format!("a-{index:02}"),
            RuleFilter::True,
            Some(source),
            2,
        )?;
    }
    let scheduled_sources = (1..=33)
        .map(|sequence| {
            record(&fixture, &fixture.entity, sequence)?
                .ok_or_else(|| "scheduled source absent".into())
        })
        .collect::<TestResult<Vec<_>>>()?;
    let suffix_index = keys::scheduled(
        &fixture.namespace,
        &fixture.entity,
        Timestamp::from_millis(100),
        SequenceNumber::new(33),
    );
    let suffix_index_value = fixture
        .machine
        .store()
        .get(&suffix_index)?
        .expect("pending suffix index");
    let child_counters = counters(&fixture, &child)?;
    let shadow_counters = counters(&fixture, &shadow)?;
    let applies = observations.lock().expect("observations").commits;
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 32 }
    );
    effects(&activation, &[child.clone(), shadow.clone()]);
    assert_eq!(
        observations.lock().expect("observations").commits,
        applies + 1
    );
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(100)
    );
    let active = records(&fixture, &child)?
        .into_iter()
        .map(|record| (record.sequence, record))
        .collect::<BTreeMap<_, _>>();
    let dead = records(&fixture, &shadow)?
        .into_iter()
        .map(|record| (record.sequence, record))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(active.len(), 32 * 31);
    assert_eq!(dead.len(), 32);
    assert_eq!(active.len() + dead.len(), MAX_TOPIC_FANOUT_COPIES);
    for (index, original) in scheduled_sources[..32].iter().enumerate() {
        assert!(record(&fixture, &fixture.entity, original.sequence.as_u64())?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::scheduled(
                    &fixture.namespace,
                    &fixture.entity,
                    Timestamp::from_millis(100),
                    original.sequence
                ))?
                .is_none()
        );
        let base = &active[&SequenceNumber::new(34 + index as u64)];
        assert_eq!(base.message_id, original.message_id);
        assert_eq!(base.envelope, original.envelope);
        assert_eq!(base.body, original.body);
        assert_eq!(base.state, MessageState::Ready);
        assert_eq!(base.enqueued_at, Timestamp::from_millis(100));
        assert_eq!(
            base.scheduled_enqueue_time,
            Some(Timestamp::from_millis(100))
        );
        for action in 0..31 {
            let sequence = SequenceNumber::new(66 + index as u64 * 31 + action);
            let copy = if action == 30 {
                &dead[&sequence]
            } else {
                &active[&sequence]
            };
            assert_eq!(copy.message_id, original.message_id);
            let mut expected = original.envelope.as_deref().expect("typed source").clone();
            if action != 30 {
                expected
                    .application_properties
                    .insert("number".into(), MessageValue::Int(9));
            }
            expected.application_properties.insert(
                "RuleName".into(),
                MessageValue::String(format!("a-{action:02}")),
            );
            assert_eq!(copy.envelope.as_deref(), Some(&expected));
            assert_eq!(copy.body, original.body);
            assert_eq!(copy.state, MessageState::Ready);
            assert_eq!(copy.enqueued_at, Timestamp::from_millis(100));
            assert_eq!(
                copy.scheduled_enqueue_time,
                Some(Timestamp::from_millis(100))
            );
            if action == 30 {
                let detail = copy
                    .dead_letter
                    .as_ref()
                    .expect("one original failure copy");
                assert_eq!(
                    detail.reason,
                    DeadLetterReason::Application("SwitchyardSqlActionError".into())
                );
                assert_eq!(detail.description, "TypeMismatch");
            } else {
                assert_eq!(copy.dead_letter, None);
            }
        }
    }
    assert_eq!(
        record(&fixture, &fixture.entity, 33)?,
        Some(scheduled_sources[32].clone())
    );
    assert_eq!(
        fixture.machine.store().get(&suffix_index)?,
        Some(suffix_index_value)
    );
    let parent_counters = counters(&fixture, &fixture.entity)?.expect("prefix parent counters");
    assert_eq!(parent_counters.next_sequence, 1058);
    assert_eq!(parent_counters.next_lock_token, 1);
    assert_eq!(counters(&fixture, &child)?, child_counters);
    assert_eq!(counters(&fixture, &shadow)?, shadow_counters);
    apply(
        &fixture,
        101,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(33)],
        },
    )?;
    assert!(record(&fixture, &fixture.entity, 33)?.is_none());
    assert!(fixture.machine.store().get(&suffix_index)?.is_none());
    for index in 0..31 {
        remove(&fixture, "child", &format!("a-{index:02}"), 102)?;
    }
    let mut large = member("large");
    large.body = vec![7; MAX_TOPIC_FANOUT_CONTENT_BYTES / 4 + 1000];
    large.envelope.body = MessageBody::Data(vec![large.body.clone()]);
    let admission = apply(
        &fixture,
        200,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(large, 300)],
        },
    )?;
    let CommandOutcome::Scheduled { sequences } = admission.outcome else {
        panic!("scheduled head")
    };
    add(
        &fixture,
        "child",
        "growth",
        RuleFilter::True,
        Some("SET added=TRUE"),
        201,
    )?;
    assert!(matches!(
        refuse_activation(&fixture, 300)?,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            ..
        }
    ));
    assert!(record(&fixture, &fixture.entity, sequences[0].as_u64())?.is_some());
    apply(&fixture, 301, CommandKind::CancelScheduled { sequences })?;

    // Unlike those three aggregate limits, later selected per-copy/shape errors
    // refuse the complete activation, including the earlier fitting candidate.
    fixture.entity = EntityPath::new("per-copy")?;
    fixture.at(
        400,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe_at(&fixture, "child", SubscriptionConfig::default(), 400)?;
    apply(
        &fixture,
        400,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("small"), 500),
                scheduled(full_header("header"), 500),
            ],
        },
    )?;
    add(
        &fixture,
        "child",
        "growth",
        RuleFilter::True,
        Some("SET added='growth'"),
        401,
    )?;
    assert!(matches!(
        refuse_activation(&fixture, 500)?,
        BrokerError::MessageHeaderTooLarge { .. }
    ));
    assert!(record(&fixture, &fixture.entity, 1)?.is_some());
    assert!(record(&fixture, &fixture.entity, 2)?.is_some());
    assert!(records(&fixture, &child)?.is_empty());
    assert!(records(&fixture, &child.dead_letter_queue()?)?.is_empty());
    remove(&fixture, "child", "growth", 402)?;
    let original = record(&fixture, &fixture.entity, 2)?.expect("later source remains pending");
    for damage in 0..3 {
        let mut corrupt = original.clone();
        match damage {
            0 => {
                corrupt
                    .envelope
                    .as_mut()
                    .expect("typed")
                    .application_properties
                    .insert(
                        "oversize".into(),
                        MessageValue::String("x".repeat(domain::MAX_MESSAGE_PROPERTY_BYTES)),
                    );
            }
            1 => {
                corrupt.envelope.as_mut().expect("typed").body =
                    MessageBody::Value(MessageValue::Map(vec![
                        (MessageValue::Float(0), MessageValue::Null),
                        (MessageValue::Float(0x8000_0000), MessageValue::Null),
                    ]));
            }
            _ => {
                corrupt.scheduled_enqueue_time = None;
            }
        }
        let mut injection = WriteBatch::default();
        injection.push_put(
            keys::message(&fixture.namespace, &fixture.entity, corrupt.sequence),
            codec::encode(&corrupt)?,
        );
        fixture.machine.store().apply(injection)?;
        let error = refuse_activation(&fixture, 500)?;
        match damage {
            0 => assert!(matches!(error, BrokerError::MessagePropertyTooLarge { .. })),
            1 => assert!(matches!(error, BrokerError::InvalidMessageContent { .. })),
            _ => assert_eq!(error, BrokerError::MalformedIndexKey),
        }
        assert!(record(&fixture, &fixture.entity, 1)?.is_some());
        assert!(record(&fixture, &fixture.entity, 2)?.is_some());
        assert!(records(&fixture, &child)?.is_empty());
        assert!(records(&fixture, &child.dead_letter_queue()?)?.is_empty());
    }

    fixture.entity = EntityPath::new("per-message")?;
    fixture.at(
        403,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe_at(
        &fixture,
        "child",
        SubscriptionConfig {
            max_message_bytes: 1024,
            ..SubscriptionConfig::default()
        },
        403,
    )?;
    let mut later = member("later");
    later.body = vec![7; 600];
    later.envelope.body = MessageBody::Data(vec![later.body.clone()]);
    apply(
        &fixture,
        404,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(member("small"), 600), scheduled(later, 600)],
        },
    )?;
    add(
        &fixture,
        "child",
        "growth",
        RuleFilter::True,
        Some(&format!("SET added='{}'", "x".repeat(400))),
        405,
    )?;
    assert!(matches!(
        refuse_activation(&fixture, 600)?,
        BrokerError::MessageTooLarge {
            maximum_bytes: 1024,
            ..
        }
    ));
    assert!(record(&fixture, &fixture.entity, 1)?.is_some());
    assert!(record(&fixture, &fixture.entity, 2)?.is_some());
    assert!(records(&fixture, &child)?.is_empty());
    assert!(records(&fixture, &child.dead_letter_queue()?)?.is_empty());

    fixture.entity = EntityPath::new("per-value")?;
    fixture.at(
        406,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe_at(&fixture, "child", SubscriptionConfig::default(), 406)?;
    let mut earlier = member("small-value-count");
    earlier.envelope.application_properties.clear();
    let mut later = member("full-value-count");
    later.envelope.application_properties.clear();
    later.envelope.body = MessageBody::Sequence(vec![vec![
        MessageValue::Null;
        domain::MAX_MESSAGE_VALUE_ITEMS - 1
    ]]);
    apply(
        &fixture,
        407,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(earlier, 700), scheduled(later, 700)],
        },
    )?;
    remove(&fixture, "child", "$Default", 408)?;
    add(
        &fixture,
        "child",
        "growth",
        RuleFilter::True,
        Some("SET added=TRUE"),
        408,
    )?;
    // The later check reaches 65536 nodes; final RuleName would add node65537.
    // Both original inputs fit together, and the earlier action copy fits.
    assert!(matches!(
        refuse_activation(&fixture, 700)?,
        BrokerError::InvalidMessageContent { .. }
    ));
    assert!(record(&fixture, &fixture.entity, 1)?.is_some());
    assert!(record(&fixture, &fixture.entity, 2)?.is_some());
    assert!(records(&fixture, &child)?.is_empty());
    assert!(records(&fixture, &child.dead_letter_queue()?)?.is_empty());
    Ok(())
}
