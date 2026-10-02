use super::*;

fn sized_rule(name: &str, bytes: usize) -> TestResult<RuleDefinition> {
    let mut definition = RuleDefinition {
        name: RuleName::new(name)?,
        filter: correlation([("value".into(), MessageValue::Binary(vec![0; bytes - 100]))]),
        created_at: Timestamp::from_millis(1),
        action: None,
    };
    let actual = codec::encode(&definition)?.len();
    let RuleFilter::Correlation(filter) = &mut definition.filter else {
        unreachable!()
    };
    let MessageValue::Binary(value) = filter
        .properties
        .get_mut("value")
        .expect("binary condition")
    else {
        unreachable!()
    };
    value.resize(value.len() + bytes - actual, 0);
    assert_eq!(codec::encode(&definition)?.len(), bytes);
    Ok(definition)
}

fn rule_count_and_exact_encoded_byte_caps_are_atomic_and_persisted_damage_is_not_truncated<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig::default(),
    )?;
    subscribe(&fixture, "count", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "count", "$Default", 0)?;
    for index in 0..domain::MAX_SUBSCRIPTION_RULES {
        add(
            &fixture,
            "count",
            &format!("r{index:02}"),
            RuleFilter::False,
            1,
        )?;
    }
    assert_eq!(
        read_rules(&fixture, "count")?.len(),
        domain::MAX_SUBSCRIPTION_RULES
    );
    reject(
        &fixture,
        2,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("count")?,
            name: RuleName::new("overflow")?,
            filter: RuleFilter::True,
        },
        BrokerError::RuleLimitExceeded {
            maximum: domain::MAX_SUBSCRIPTION_RULES,
        },
    )?;
    let subscription = SubscriptionName::new("count")?;
    let extra = RuleDefinition {
        name: RuleName::new("r-extra")?,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(1),
        action: None,
    };
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::rule(
            &fixture.namespace,
            &fixture.entity,
            &subscription,
            &extra.name,
        ),
        codec::encode(&extra)?,
    ))?;
    let before = fixture.machine.store().snapshot()?;
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        fixture
            .machine
            .rules(&fixture.namespace, &fixture.entity, &subscription),
        Err(BrokerError::RuleLimitExceeded {
            maximum: domain::MAX_SUBSCRIPTION_RULES
        })
    );
    let observed = observations.lock().expect("observations");
    let prefix = keys::rule_prefix(&fixture.namespace, &fixture.entity, &subscription);
    assert_eq!(
        observed
            .scans
            .iter()
            .filter(|(actual, _, _)| *actual == prefix)
            .map(|(_, limit, rows)| (*limit, *rows))
            .collect::<Vec<_>>(),
        vec![(
            domain::MAX_SUBSCRIPTION_RULES + 1,
            domain::MAX_SUBSCRIPTION_RULES + 1
        )]
    );
    assert_eq!(observed.commits, 0);
    drop(observed);
    reject(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: vec![member("no-truncation")],
        },
        BrokerError::RuleLimitExceeded {
            maximum: domain::MAX_SUBSCRIPTION_RULES,
        },
    )?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.entity = EntityPath::new("byte-caps")?;
    fixture.at(
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    subscribe(&fixture, "bytes", SubscriptionConfig::default(), 1)?;
    remove(&fixture, "bytes", "$Default", 1)?;
    let exact = sized_rule("r0", domain::MAX_RULE_BYTES)?;
    assert_eq!(exact.encoded_size()?, domain::MAX_RULE_BYTES);
    let oversized = sized_rule("r0", domain::MAX_RULE_BYTES + 1)?;
    reject(
        &fixture,
        1,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("bytes")?,
            name: oversized.name,
            filter: oversized.filter,
        },
        BrokerError::RuleTooLarge {
            maximum_bytes: domain::MAX_RULE_BYTES,
        },
    )?;
    for index in 0..4 {
        let definition = sized_rule(&format!("r{index}"), domain::MAX_RULE_BYTES)?;
        add(
            &fixture,
            "bytes",
            definition.name.as_str(),
            definition.filter,
            1,
        )?;
    }
    let definitions = read_rules(&fixture, "bytes")?;
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.encoded_size())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>(),
        domain::MAX_SUBSCRIPTION_RULE_BYTES
    );
    reject(
        &fixture,
        2,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("bytes")?,
            name: RuleName::new("small")?,
            filter: RuleFilter::True,
        },
        BrokerError::RuleSetTooLarge {
            maximum_bytes: domain::MAX_SUBSCRIPTION_RULE_BYTES,
        },
    )?;
    let extra = RuleDefinition {
        name: RuleName::new("small")?,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(1),
        action: None,
    };
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::rule(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("bytes")?,
            &extra.name,
        ),
        codec::encode(&extra)?,
    ))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.machine.rules(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("bytes")?
        ),
        Err(BrokerError::DanglingRuleMetadata)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn work_limits_include_nonmatches_duplicates_and_all_custom_key_candidates_before_bounded_activation<
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
    for index in 0..4 {
        let name = format!("s{index}");
        subscribe(&fixture, &name, SubscriptionConfig::default(), 0)?;
        remove(&fixture, &name, "$Default", 0)?;
    }
    let mut large = member("late-unfit");
    large.envelope.application_properties = (0..300)
        .map(|index| (format!("p{index:03}"), MessageValue::Bool(false)))
        .collect();
    apply(
        &fixture,
        1,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(large, 50)],
        },
    )?;
    for subscription in 0..4 {
        for index in 0..domain::MAX_SUBSCRIPTION_RULES {
            add(
                &fixture,
                &format!("s{subscription}"),
                &format!("r{index:02}"),
                correlation(
                    (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
                        .map(|condition| (format!("c{condition:02}"), MessageValue::Null)),
                ),
                1,
            )?;
        }
    }
    let error = BrokerError::TopicRuleMatchTooLarge {
        limit: domain::RuleMatchLimit::WorkUnits,
        maximum: domain::MAX_TOPIC_RULE_MATCH_WORK,
    };
    effects(&publish(&fixture, 2, vec![member("known")])?, &[]);
    reject(&fixture, 50, CommandKind::ActivateScheduled, error.clone())?;
    assert!(record(&fixture, &fixture.entity, 1)?.is_some());
    effects(
        &apply(
            &fixture,
            3,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1)],
            },
        )?,
        &[],
    );
    let property_count = member("known").envelope.application_properties.len();
    let per_message = 4
        * domain::MAX_SUBSCRIPTION_RULES
        * (1 + domain::MAX_CORRELATION_RULE_CONDITIONS * (property_count + 1));
    let fitting = domain::MAX_TOPIC_RULE_MATCH_WORK / per_message;
    assert_eq!(fitting, 84);
    assert!(fitting * per_message <= domain::MAX_TOPIC_RULE_MATCH_WORK);
    assert!((fitting + 1) * per_message > domain::MAX_TOPIC_RULE_MATCH_WORK);
    // Already-known IDs and zero matching subscriptions still charge all work.
    reject(
        &fixture,
        3,
        CommandKind::SendBatch {
            messages: vec![member("known"); fitting + 1],
        },
        error,
    )?;
    let accepted = publish(&fixture, 3, vec![member("known"); fitting])?;
    effects(&accepted, &[]);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("discarded duplicate sequences")
            .next_sequence,
        fitting as u64 + 3
    );
    let first_handle = fitting as u64 + 3;
    for range in [0..42, 42..85] {
        effects(
            &apply(
                &fixture,
                4,
                CommandKind::ScheduleEnvelopes {
                    messages: range
                        .map(|index| scheduled(member(&format!("future-{index}")), 100))
                        .collect(),
                },
            )?,
            &[],
        );
    }
    let first = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        first.outcome,
        CommandOutcome::ScheduledActivated {
            activated: fitting as u32
        }
    );
    effects(&first, &[]);
    assert!(record(&fixture, &fixture.entity, first_handle)?.is_none());
    assert!(record(&fixture, &fixture.entity, first_handle + fitting as u64)?.is_some());
    let second = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        second.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    effects(&second, &[]);
    assert!(record(&fixture, &fixture.entity, first_handle + fitting as u64)?.is_none());
    for subscription in 0..4 {
        assert!(
            peek(
                &fixture,
                &fixture
                    .entity
                    .subscription(&SubscriptionName::new(format!("s{subscription}"))?)?,
                100
            )?
            .is_empty()
        );
    }
    Ok(())
}

fn comparison_bytes_charge_after_or_match_and_duplicate_drop_then_stop_at_an_exact_scheduled_prefix<
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
    let expected = "x".repeat(16_384);
    let mut targets = Vec::new();
    for subscription in 0..4 {
        let name = format!("s{subscription}");
        targets.push(subscribe(
            &fixture,
            &name,
            SubscriptionConfig::default(),
            1,
        )?);
        remove(&fixture, &name, "$Default", 1)?;
        for index in 0..8 {
            add(
                &fixture,
                &name,
                &format!("r{index}"),
                RuleFilter::Correlation(CorrelationFilter {
                    subject: Some(expected.clone()),
                    ..CorrelationFilter::default()
                }),
                1,
            )?;
        }
    }
    let mut message = member("known");
    message.envelope.properties.subject = Some(expected.clone());
    let per_message = 4 * 8 * expected.len() * 2;
    let fitting = domain::MAX_TOPIC_RULE_COMPARISON_BYTES / per_message;
    assert_eq!(fitting, 32);
    assert_eq!(
        fitting * per_message,
        domain::MAX_TOPIC_RULE_COMPARISON_BYTES
    );
    // The first matching rule cannot hide the later matching comparisons.
    effects(
        &publish(&fixture, 2, vec![message.clone(); fitting])?,
        &targets,
    );
    let mut overflow = vec![message.clone(); fitting];
    overflow[0]
        .envelope
        .properties
        .subject
        .as_mut()
        .expect("subject")
        .push('x');
    reject(
        &fixture,
        3,
        CommandKind::SendBatch { messages: overflow },
        BrokerError::TopicRuleMatchTooLarge {
            limit: domain::RuleMatchLimit::ComparisonBytes,
            maximum: domain::MAX_TOPIC_RULE_COMPARISON_BYTES,
        },
    )?;
    let mut nonmatching = message.clone();
    nonmatching.envelope.properties.subject = Some("y".repeat(expected.len()));
    effects(&publish(&fixture, 3, vec![nonmatching; fitting])?, &[]);
    for range in [0..32, 32..64] {
        effects(
            &apply(
                &fixture,
                4,
                CommandKind::ScheduleEnvelopes {
                    messages: range
                        .map(|index| {
                            let mut next = member(&format!("future-{index}"));
                            next.envelope.properties.subject = Some(expected.clone());
                            scheduled(next, 100)
                        })
                        .collect(),
                },
            )?,
            &[],
        );
    }
    let first = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        first.outcome,
        CommandOutcome::ScheduledActivated { activated: 32 }
    );
    effects(&first, &targets);
    assert!(record(&fixture, &fixture.entity, 96)?.is_none());
    assert!(record(&fixture, &fixture.entity, 97)?.is_some());
    let second = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        second.outcome,
        CommandOutcome::ScheduledActivated { activated: 32 }
    );
    effects(&second, &targets);
    assert!(record(&fixture, &fixture.entity, 97)?.is_none());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("shared activation IDs")
            .next_sequence,
        193
    );
    for target in &targets {
        assert_eq!(peek(&fixture, target, 100)?.len(), 65);
    }
    Ok(())
}

fn only_matching_targets_consume_retained_copy_budgets_and_late_rules_can_leave_a_cancelable_head<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let config = TopicConfig {
        max_message_bytes: domain::MAX_INGRESS_BATCH_CONTENT_BYTES,
        ..TopicConfig::default()
    };
    let fixture = topic(provider, config)?;
    let subscription_config = SubscriptionConfig {
        max_message_bytes: config.max_message_bytes,
        ..SubscriptionConfig::default()
    };
    let alpha = subscribe(&fixture, "Alpha", subscription_config, 0)?;
    let beta = subscribe(&fixture, "beta", subscription_config, 0)?;
    remove(&fixture, "beta", "$Default", 0)?;
    add(&fixture, "beta", "false", RuleFilter::False, 0)?;
    let body = vec![0; domain::MAX_TOPIC_FANOUT_CONTENT_BYTES / 2 + 1];
    let scheduled_command = CommandKind::Schedule {
        messages: vec![domain::ScheduledMessage {
            message_id: "large".into(),
            body,
            time_to_live_millis: None,
            session_id: None,
            enqueue_at: Timestamp::from_millis(100),
        }],
    };
    effects(&apply(&fixture, 1, scheduled_command)?, &[]);
    add(&fixture, "beta", "true", RuleFilter::True, 2)?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::TopicFanoutTooLarge {
            limit: domain::IngressBatchLimit::ContentBytes,
            maximum: domain::MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    assert!(record(&fixture, &fixture.entity, 1)?.is_some());
    assert!(record(&fixture, &alpha, 2)?.is_none());
    assert!(record(&fixture, &beta, 2)?.is_none());
    remove(&fixture, "beta", "true", 3)?;
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    effects(&activation, std::slice::from_ref(&alpha));
    assert!(record(&fixture, &alpha, 2)?.is_some());
    assert!(record(&fixture, &beta, 2)?.is_none());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    rule_count_and_exact_encoded_byte_caps_are_atomic_and_persisted_damage_is_not_truncated,
    work_limits_include_nonmatches_duplicates_and_all_custom_key_candidates_before_bounded_activation,
    comparison_bytes_charge_after_or_match_and_duplicate_drop_then_stop_at_an_exact_scheduled_prefix,
    only_matching_targets_consume_retained_copy_budgets_and_late_rules_can_leave_a_cancelable_head,
}
