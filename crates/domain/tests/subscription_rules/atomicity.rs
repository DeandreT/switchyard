use super::*;

fn failed_default_and_rule_mutations_leave_no_partial_metadata_and_retry_after_reopen<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
        },
        TopicConfig::default(),
    )?;
    let subscription = SubscriptionName::new("child")?;
    let definition = RuleName::new("rule")?;
    for (millis, kind, expected) in [
        (
            1,
            CommandKind::CreateSubscription {
                name: subscription.clone(),
                config: SubscriptionConfig::default(),
            },
            CommandOutcome::SubscriptionCreated,
        ),
        (
            2,
            CommandKind::CreateRule {
                subscription: subscription.clone(),
                name: definition.clone(),
                filter: RuleFilter::False,
            },
            CommandOutcome::RuleCreated,
        ),
        (
            3,
            CommandKind::DeleteRule {
                subscription: subscription.clone(),
                name: definition.clone(),
            },
            CommandOutcome::RuleDeleted,
        ),
    ] {
        *observations.lock().expect("observations") = Observations::default();
        fail_next.store(true, Ordering::Relaxed);
        reject(
            &fixture,
            millis,
            kind.clone(),
            BrokerError::Storage(StorageError::Backend {
                operation: "commit",
                detail: "injected rule failure".into(),
            }),
        )?;
        assert_eq!(observations.lock().expect("observations").commits, 1);
        let before = fixture.machine.store().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        *observations.lock().expect("observations") = Observations::default();
        assert_eq!(apply(&fixture, millis, kind)?.outcome, expected);
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
        drop(observed);
        assert_eq!(
            fixture.machine.last_applied_time()?,
            Timestamp::from_millis(millis)
        );
    }
    assert_eq!(read_rules(&fixture, "child")?.len(), 1);
    reject(
        &fixture,
        2,
        CommandKind::CreateRule {
            subscription,
            name: RuleName::new("late")?,
            filter: RuleFilter::True,
        },
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(3),
            proposed: Timestamp::from_millis(2),
        },
    )?;
    Ok(())
}

fn complete_rule_reads_reject_corrupt_late_entries_before_any_publication<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    for damage in 0..7 {
        fixture.entity = EntityPath::new(format!("damage-{damage}"))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        let healthy = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 10)?;
        let late = subscribe(&fixture, "zulu", SubscriptionConfig::default(), 10)?;
        let subscription = SubscriptionName::new("zulu")?;
        let name = RuleName::new("z-rule")?;
        add(&fixture, "zulu", name.as_str(), RuleFilter::True, 10)?;
        let key = keys::rule(&fixture.namespace, &fixture.entity, &subscription, &name);
        let mut definition = RuleDefinition {
            name,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(10),
        };
        let expected =
            match damage {
                0 => {
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(key, vec![255]))?;
                    None
                }
                1 => {
                    definition.name = RuleName::new("different")?;
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(key, codec::encode(&definition)?))?;
                    Some(BrokerError::DanglingRuleMetadata)
                }
                2 => {
                    definition.filter =
                        correlation([("invalid".into(), MessageValue::List(vec![]))]);
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(key, codec::encode(&definition)?))?;
                    None
                }
                3 => {
                    definition.filter = RuleFilter::Correlation(CorrelationFilter {
                        subject: Some("subject".into()),
                        properties: (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
                            .map(|index| (format!("p{index}"), MessageValue::Null))
                            .collect(),
                        ..CorrelationFilter::default()
                    });
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(key, codec::encode(&definition)?))?;
                    Some(BrokerError::DanglingRuleMetadata)
                }
                4 => {
                    let mut malformed = key;
                    malformed.push(b'x');
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(malformed, codec::encode(&definition)?))?;
                    Some(BrokerError::MalformedIndexKey)
                }
                5 => {
                    fixture.machine.store().apply(WriteBatch::default().delete(
                        keys::queue_config(&fixture.namespace, &late.dead_letter_queue()?),
                    ))?;
                    Some(BrokerError::DanglingSubscriptionMetadata)
                }
                6 => {
                    definition.filter = correlation([(
                        "value".into(),
                        MessageValue::Binary(vec![0; domain::MAX_RULE_BYTES]),
                    )]);
                    fixture
                        .machine
                        .store()
                        .apply(WriteBatch::default().put(key, codec::encode(&definition)?))?;
                    Some(BrokerError::DanglingRuleMetadata)
                }
                _ => unreachable!(),
            };
        let before = fixture.machine.store().snapshot()?;
        let applied = fixture.machine.last_applied_time()?;
        let read = fixture
            .machine
            .rules(&fixture.namespace, &fixture.entity, &subscription);
        if let Some(expected) = &expected {
            assert_eq!(read, Err(expected.clone()));
        } else {
            assert!(matches!(read, Err(BrokerError::Codec(_))));
        }
        let publication = publish(&fixture, 100, vec![member("would-stage-first")]);
        if let Some(expected) = expected {
            assert_eq!(publication, Err(expected));
        } else {
            assert!(matches!(publication, Err(BrokerError::Codec(_))));
        }
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
        assert!(record(&fixture, &healthy, 1)?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    "would-stage-first"
                ))?
                .is_none()
        );
    }
    Ok(())
}

fn missing_wrong_parent_orphan_and_invalid_rule_commands_never_mutate_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    let subscription = SubscriptionName::new("missing")?;
    reject(
        &fixture,
        1,
        CommandKind::CreateRule {
            subscription: subscription.clone(),
            name: RuleName::new("rule")?,
            filter: RuleFilter::True,
        },
        BrokerError::SubscriptionNotFound,
    )?;
    reject(
        &fixture,
        1,
        CommandKind::DeleteRule {
            subscription,
            name: RuleName::new("rule")?,
        },
        BrokerError::SubscriptionNotFound,
    )?;
    let before = fixture.machine.store().snapshot()?;
    for filter in [
        correlation([("value".into(), MessageValue::List(vec![]))]),
        correlation([("value".into(), MessageValue::Map(vec![]))]),
        correlation([("value".into(), MessageValue::Array(vec![]))]),
        correlation(
            (0..=domain::MAX_CORRELATION_RULE_CONDITIONS)
                .map(|index| (format!("p{index}"), MessageValue::Null)),
        ),
    ] {
        assert!(matches!(
            apply(
                &fixture,
                10,
                CommandKind::CreateRule {
                    subscription: SubscriptionName::new("child")?,
                    name: RuleName::new("invalid")?,
                    filter,
                }
            ),
            Err(BrokerError::InvalidRule { .. })
        ));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let original = fixture.entity.clone();
    for entity in [
        EntityPath::new("missing-topic")?,
        EntityPath::new("anchor")?,
    ] {
        let command = Command::new(
            fixture.namespace.clone(),
            entity,
            Timestamp::from_millis(10),
            CommandKind::CreateRule {
                subscription: SubscriptionName::new("child")?,
                name: RuleName::new("rule")?,
                filter: RuleFilter::True,
            },
        );
        assert_eq!(
            fixture.machine.apply(&command),
            Err(BrokerError::TopicNotFound)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let orphan = SubscriptionName::new("orphan")?;
    let name = RuleName::new("leftover")?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::rule(&fixture.namespace, &original, &orphan, &name),
        codec::encode(&RuleDefinition {
            name,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(0),
        })?,
    ))?;
    reject(
        &fixture,
        10,
        CommandKind::CreateSubscription {
            name: orphan,
            config: SubscriptionConfig::default(),
        },
        BrokerError::DanglingRuleMetadata,
    )?;
    assert!(record(&fixture, &child, 1)?.is_none());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

fn ambiguous_primary_types_are_rejected_with_or_without_members_before_ingress_or_rule_reads<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for populated in [false, true] {
        fixture.entity = EntityPath::new(if populated { "populated" } else { "empty" })?;
        fixture.at(
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        if populated {
            subscribe(&fixture, "existing", SubscriptionConfig::default(), 0)?;
        }
        let missing = SubscriptionName::new("missing")?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &missing),
            Err(BrokerError::SubscriptionNotFound)
        );
        fixture.machine.store().apply(WriteBatch::default().put(
            keys::queue_config(&fixture.namespace, &fixture.entity),
            codec::encode(&domain::QueueConfig::default())?,
        ))?;
        for kind in [
            CommandKind::Send {
                message_id: "must-not-enqueue".into(),
                body: b"payload".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
            CommandKind::SendBatch { messages: vec![] },
            CommandKind::Schedule {
                messages: vec![domain::ScheduledMessage {
                    message_id: "must-not-schedule".into(),
                    body: b"payload".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(100),
                }],
            },
            CommandKind::Schedule { messages: vec![] },
            CommandKind::ScheduleEnvelopes {
                messages: vec![scheduled(member("rich"), 100)],
            },
            CommandKind::ScheduleEnvelopes { messages: vec![] },
            CommandKind::ActivateScheduled,
        ] {
            reject(&fixture, 10, kind, BrokerError::DanglingEntityMetadata)?;
        }
        let before = fixture.machine.store().snapshot()?;
        let applied = fixture.machine.last_applied_time()?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &missing),
            Err(BrokerError::DanglingEntityMetadata)
        );
        if populated {
            assert_eq!(
                fixture.machine.rules(
                    &fixture.namespace,
                    &fixture.entity,
                    &SubscriptionName::new("existing")?
                ),
                Err(BrokerError::DanglingEntityMetadata)
            );
        }
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
    }
    Ok(())
}

fn orphan_rule_prefixes_are_not_clean_absence_even_without_the_parent_topic<P: StoreProvider>(
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
    for parent_exists in [false, true] {
        fixture.entity = EntityPath::new(if parent_exists {
            "parent-present"
        } else {
            "parent-absent"
        })?;
        if parent_exists {
            fixture.at(
                10,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
        }
        let subscription = SubscriptionName::new("missing")?;
        let missing = if parent_exists {
            BrokerError::SubscriptionNotFound
        } else {
            BrokerError::TopicNotFound
        };
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &subscription),
            Err(missing.clone())
        );
        let definition = RuleDefinition {
            name: RuleName::new("orphan")?,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(10),
        };
        let key = keys::rule(
            &fixture.namespace,
            &fixture.entity,
            &subscription,
            &definition.name,
        );
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().put(key.clone(), codec::encode(&definition)?))?;
        let before = fixture.machine.store().snapshot()?;
        let applied = fixture.machine.last_applied_time()?;
        *observations.lock().expect("observations") = Observations::default();
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &subscription),
            Err(BrokerError::DanglingRuleMetadata)
        );
        let prefix = keys::rule_prefix(&fixture.namespace, &fixture.entity, &subscription);
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 0);
        assert_eq!(
            observed
                .scans
                .iter()
                .filter(|(actual, _, _)| *actual == prefix)
                .map(|(_, limit, rows)| (*limit, *rows))
                .collect::<Vec<_>>(),
            vec![(1, 1)]
        );
        drop(observed);
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().delete(key))?;
        let before = fixture.machine.store().snapshot()?;
        *observations.lock().expect("observations") = Observations::default();
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &subscription),
            Err(missing)
        );
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 0);
        assert_eq!(
            observed
                .scans
                .iter()
                .filter(|(actual, _, _)| *actual == prefix)
                .map(|(_, limit, rows)| (*limit, *rows))
                .collect::<Vec<_>>(),
            vec![(1, 0)]
        );
        drop(observed);
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
    }
    Ok(())
}

struct FlatNestedValue {
    header: Vec<u8>,
    terminal: Vec<u8>,
    depth: usize,
}

impl serde::Serialize for FlatNestedValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tuple =
            serializer.serialize_tuple(self.header.len() * self.depth + self.terminal.len())?;
        for _ in 0..self.depth {
            for byte in &self.header {
                tuple.serialize_element(byte)?;
            }
        }
        for byte in &self.terminal {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

#[derive(serde::Serialize)]
struct StoredRuleProbe<'a> {
    name: &'a RuleName,
    filter: StoredFilterProbe,
    created_at: Timestamp,
}

#[derive(serde::Serialize)]
#[allow(dead_code, clippy::large_enum_variant)]
enum StoredFilterProbe {
    True,
    False,
    Correlation(StoredCorrelationProbe),
}

#[derive(Default, serde::Serialize)]
struct StoredCorrelationProbe {
    correlation_id: Option<String>,
    message_id: Option<String>,
    to: Option<String>,
    reply_to: Option<String>,
    subject: Option<String>,
    session_id: Option<String>,
    reply_to_session_id: Option<String>,
    content_type: Option<String>,
    properties: BTreeMap<String, FlatNestedValue>,
}

fn compound_or_deeply_nested_stored_conditions_return_scoped_codec_errors_without_recursive_values<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    let name = RuleName::new("poison")?;
    let key = keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("child")?,
        &name,
    );
    let mut payloads = Vec::new();
    for value in [
        MessageValue::List(vec![]),
        MessageValue::Map(vec![]),
        MessageValue::Array(vec![MessageValue::Null]),
        MessageValue::Described {
            descriptor: domain::MessageDescriptor::Code(1),
            value: Box::new(MessageValue::Null),
        },
        MessageValue::List(vec![MessageValue::List(vec![MessageValue::Null])]),
    ] {
        payloads.push(codec::encode(&RuleDefinition {
            name: name.clone(),
            filter: correlation([("value".into(), value)]),
            created_at: Timestamp::from_millis(0),
        })?);
    }
    let terminal = postcard::to_stdvec(&MessageValue::Null)?;
    let shallow = postcard::to_stdvec(&MessageValue::List(vec![MessageValue::Null]))?;
    assert!(shallow.ends_with(&terminal));
    // Postcard tuples flatten this bounded byte stream; no recursive Rust tree
    // is created, encoded, decoded by the fixture, or dropped.
    let nested = codec::encode(&StoredRuleProbe {
        name: &name,
        filter: StoredFilterProbe::Correlation(StoredCorrelationProbe {
            properties: BTreeMap::from([(
                "value".into(),
                FlatNestedValue {
                    header: shallow[..shallow.len() - terminal.len()].to_vec(),
                    terminal,
                    depth: 8_192,
                },
            )]),
            ..StoredCorrelationProbe::default()
        }),
        created_at: Timestamp::from_millis(0),
    })?;
    assert!(nested.len() < domain::MAX_RULE_BYTES);
    payloads.push(nested);
    for payload in payloads {
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().put(key.clone(), payload))?;
        let before = fixture.machine.store().snapshot()?;
        let applied = fixture.machine.last_applied_time()?;
        assert_eq!(
            fixture.machine.rules(
                &fixture.namespace,
                &fixture.entity,
                &SubscriptionName::new("child")?
            ),
            Err(BrokerError::Codec(domain::CodecError::Decode))
        );
        reject(
            &fixture,
            10,
            CommandKind::SendBatch {
                messages: vec![member("must-not-publish")],
            },
            BrokerError::Codec(domain::CodecError::Decode),
        )?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
        assert!(record(&fixture, &child, 1)?.is_none());
    }
    Ok(())
}

for_each_backend! {
    failed_default_and_rule_mutations_leave_no_partial_metadata_and_retry_after_reopen,
    complete_rule_reads_reject_corrupt_late_entries_before_any_publication,
    missing_wrong_parent_orphan_and_invalid_rule_commands_never_mutate_metadata,
    ambiguous_primary_types_are_rejected_with_or_without_members_before_ingress_or_rule_reads,
    orphan_rule_prefixes_are_not_clean_absence_even_without_the_parent_topic,
    compound_or_deeply_nested_stored_conditions_return_scoped_codec_errors_without_recursive_values,
}
