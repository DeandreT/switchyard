use super::*;

use domain::{CorrelationFilter, MessageValue, RuleDefinition, SessionId, SqlAction, SqlFilter};
use server::{AtomRuleDefinition, AtomRuleOwnerError};

fn definition(name: &str, filter: RuleFilter) -> AtomRuleDefinition {
    AtomRuleDefinition {
        name: RuleName::new(name).unwrap(),
        filter,
    }
}

fn correlation_filter() -> CorrelationFilter {
    CorrelationFilter {
        correlation_id: Some(String::new()),
        message_id: Some("Message-CaSe".into()),
        to: Some("https://example.invalid/?a=1&b=<x>".into()),
        reply_to: Some(" reply \u{03BB} ".into()),
        subject: Some("Subject\r\n&<>".into()),
        session_id: Some(String::new()),
        reply_to_session_id: Some("Reply-Session".into()),
        content_type: Some("application/CaseSensitive".into()),
        properties: [
            ("boolean".into(), MessageValue::Bool(true)),
            (
                "double".into(),
                MessageValue::Double(1.234_567_890_123_456_7_f64.to_bits()),
            ),
            ("integer".into(), MessageValue::Int(i32::MIN)),
            ("long".into(), MessageValue::Long(i64::MAX)),
            (
                "negative-infinity".into(),
                MessageValue::Double(f64::NEG_INFINITY.to_bits()),
            ),
            (
                "negative-zero".into(),
                MessageValue::Double((-0.0_f64).to_bits()),
            ),
            (
                "positive-infinity".into(),
                MessageValue::Double(f64::INFINITY.to_bits()),
            ),
            (
                "string-\u{03BB}&<".into(),
                MessageValue::String(" Red & <\u{03BB}> \r\n".into()),
            ),
            ("timestamp".into(), MessageValue::Timestamp(123)),
        ]
        .into_iter()
        .collect(),
    }
}

fn correlation_value(value: MessageValue) -> CorrelationFilter {
    CorrelationFilter {
        properties: [("value".into(), value)].into_iter().collect(),
        ..CorrelationFilter::default()
    }
}

const COLLIDING_CORRELATION_KEYS: &[(&str, &str)] = &[
    ("a", "A"),
    ("\u{e9}", "\u{c9}"),
    ("\u{b5}", "\u{39c}"),
    ("\u{250}", "\u{2c6f}"),
    ("\u{3c2}", "\u{3a3}"),
    ("\u{1c8a}", "\u{1c89}"),
    ("\u{10428}", "\u{10400}"),
    ("\u{16e60}", "\u{16e40}"),
    (" Key \u{e9}", " KEY \u{c9}"),
];

const DISTINCT_CORRELATION_KEYS: &[(&str, &str)] = &[
    ("\u{131}", "I"),
    ("\u{17f}", "S"),
    ("\u{130}", "i"),
    ("\u{212a}", "K"),
    ("\u{df}", "\u{1e9e}"),
    ("\u{df}", "SS"),
    ("\u{fb00}", "ff"),
    ("\u{e9}", "e\u{301}"),
    ("\u{10d70}", "\u{10d50}"),
    ("\u{16ebb}", "\u{16ea0}"),
    (" Key ", "Key"),
    ("", " "),
];

fn correlation_keys(left: &str, right: &str) -> CorrelationFilter {
    assert_ne!(left, right, "fixture keys must remain ordinally distinct");
    CorrelationFilter {
        properties: [
            (left.into(), MessageValue::Int(1)),
            (right.into(), MessageValue::Long(2)),
        ]
        .into_iter()
        .collect(),
        ..CorrelationFilter::default()
    }
}

fn rule_error(error: BrokerError) -> AtomRuleOwnerError {
    AtomRuleOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(error)))
}

fn create_rule<S: StateStore>(
    fixture: &Fixture<S>,
    name: &str,
    filter: RuleFilter,
) -> Result<AtomRuleDefinition, AtomRuleOwnerError> {
    fixture.handle().create_atom_rule_blocking(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        fixture.name.clone(),
        definition(name, filter),
    )
}

fn get_rule<S: StateStore>(
    fixture: &Fixture<S>,
    name: &str,
) -> Result<Option<AtomRuleDefinition>, AtomRuleOwnerError> {
    fixture.handle().get_atom_rule_blocking(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        fixture.name.clone(),
        RuleName::new(name).unwrap(),
    )
}

fn list_rules<S: StateStore>(
    fixture: &Fixture<S>,
    skip: usize,
    top: usize,
) -> Result<Vec<AtomRuleDefinition>, AtomRuleOwnerError> {
    fixture.handle().list_atom_rules_blocking(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        fixture.name.clone(),
        skip,
        top,
    )
}

fn delete_rule<S: StateStore>(
    fixture: &Fixture<S>,
    name: &str,
) -> Result<CommandOutcome, AtomRuleOwnerError> {
    fixture.handle().delete_atom_rule_blocking(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        fixture.name.clone(),
        RuleName::new(name).unwrap(),
    )
}

fn rule_key<S: StateStore>(fixture: &Fixture<S>, name: &str) -> Vec<u8> {
    keys::rule(
        &fixture.namespace,
        &fixture.topic,
        &fixture.name,
        &RuleName::new(name).unwrap(),
    )
}

fn expected_put<S: StateStore>(
    fixture: &Fixture<S>,
    name: &str,
    filter: RuleFilter,
    millis: u64,
) -> TestResult<Vec<Mutation>> {
    Ok(vec![
        Mutation::Put {
            key: rule_key(fixture, name),
            value: codec::encode(&RuleDefinition {
                name: RuleName::new(name)?,
                filter,
                created_at: Timestamp::from_millis(millis),
                action: None,
            })?,
        },
        Mutation::Put {
            key: keys::clock(),
            value: codec::encode(&Timestamp::from_millis(millis))?,
        },
    ])
}

fn expected_delete<S: StateStore>(
    fixture: &Fixture<S>,
    name: &str,
    millis: u64,
) -> TestResult<Vec<Mutation>> {
    Ok(vec![
        Mutation::Delete {
            key: rule_key(fixture, name),
        },
        Mutation::Put {
            key: keys::clock(),
            value: codec::encode(&Timestamp::from_millis(millis))?,
        },
    ])
}

fn exact_image(
    before: &StoreSnapshot,
    after: &StoreSnapshot,
    mutations: &[Mutation],
) -> TestResult {
    let expected = storage::MemoryStore::default();
    expected.apply(
        before
            .entries()
            .iter()
            .fold(WriteBatch::default(), |batch, (key, value)| {
                batch.put(key.clone(), value.clone())
            }),
    )?;
    expected.apply(mutations.iter().fold(
        WriteBatch::default(),
        |batch, mutation| match mutation {
            Mutation::Put { key, value } => batch.put(key.clone(), value.clone()),
            Mutation::Delete { key } => batch.delete(key.clone()),
        },
    ))?;
    assert_eq!(
        after,
        &expected.snapshot()?,
        "whole raw expected mutation projection, not an independent domain oracle"
    );
    Ok(())
}

fn stored<S: StateStore>(fixture: &Fixture<S>, name: &str) -> TestResult<Option<RuleDefinition>> {
    Ok(match fixture.store.inner.get(&rule_key(fixture, name))? {
        Some(bytes) => Some(RuleDefinition::decode(&bytes)?),
        None => None,
    })
}

fn native_opaque_rules<S: StateStore>(fixture: &Fixture<S>) -> TestResult {
    assert_eq!(
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateRule {
                subscription: fixture.name.clone(),
                name: RuleName::new("native-sql")?,
                filter: RuleFilter::Sql(SqlFilter::new("1=0")?),
            }
        )?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateRuleWithAction {
                subscription: fixture.name.clone(),
                name: RuleName::new("native-action")?,
                filter: RuleFilter::False,
                action: SqlAction::new("SET user.marker = 'native';")?,
            }
        )?,
        CommandOutcome::RuleCreated
    );
    Ok(())
}

async fn rule_mutations_preserve_retained_state_and_exact_timestamps<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig {
        default_time_to_live_millis: Some(120_000),
        ..SubscriptionConfig::default()
    })?;
    native_opaque_rules(&fixture)?;
    let sibling = SubscriptionName::new("sibling")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: sibling,
            config: SubscriptionConfig::default(),
        },
    )?;
    for sequence in 1..=3 {
        fixture.submit(
            &fixture.topic,
            CommandKind::Send {
                message_id: format!("retained-{sequence}"),
                body: vec![sequence as u8; 32],
                time_to_live_millis: None,
                session_id: Some(SessionId::new("metadata")?),
            },
        )?;
        if sequence < 3 {
            let CommandOutcome::Received(Some(delivery)) = fixture.submit(
                &fixture.child()?,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("retained rule seed must be deliverable");
            };
            if sequence == 1 {
                fixture.submit(
                    &fixture.child()?,
                    CommandKind::DeadLetter {
                        sequence: delivery.sequence,
                        lock_token: delivery.lock.unwrap().token,
                        reason: "rule-retention".into(),
                        description: "trusted seed".into(),
                    },
                )?;
            }
        }
    }
    let before = fixture.store.snapshot()?;
    let original_default = stored(&fixture, "$Default")?.unwrap();
    fixture.clock.manual.set(1_500);
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "owned", RuleFilter::False)?,
        definition("owned", RuleFilter::False)
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let expected = expected_put(&fixture, "owned", RuleFilter::False, 1_500)?;
    assert_eq!(observed.mutations, expected);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
    exact_image(&before, &fixture.store.snapshot()?, &expected)?;
    assert_eq!(stored(&fixture, "$Default")?, Some(original_default));
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false);
    assert_eq!(delete_rule(&fixture, "owned")?, CommandOutcome::RuleDeleted);
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let expected = expected_delete(&fixture, "owned", 2_000)?;
    assert_eq!(observed.mutations, expected);
    exact_image(&before, &fixture.store.snapshot()?, &expected)?;
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

async fn prepared_rule_returns_and_preapply_failures_have_no_postcommit_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    for create in [true, false] {
        let before = fixture.store.snapshot()?;
        fixture.store.arm(false);
        fixture.store.observation.lock().unwrap().fail_next = true;
        let result = if create {
            create_rule(&fixture, "owned", RuleFilter::True).map(|_| CommandOutcome::RuleCreated)
        } else {
            delete_rule(&fixture, "owned")
        };
        assert!(matches!(
            result,
            Err(AtomRuleOwnerError::Submit(SubmitError::Propose(
                ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
            )))
        ));
        let attempt = fixture.store.disarm();
        assert_owner(&attempt, 1);
        assert!(
            !attempt.committed,
            "preapply attempt is not a successful commit"
        );
        assert_eq!(fixture.store.snapshot()?, before);
        fixture.store.arm(false);
        if create {
            assert_eq!(
                create_rule(&fixture, "owned", RuleFilter::True)?,
                definition("owned", RuleFilter::True)
            );
        } else {
            assert_eq!(delete_rule(&fixture, "owned")?, CommandOutcome::RuleDeleted);
        }
        let committed = fixture.store.disarm();
        assert_owner(&committed, 1);
        assert!(committed.committed);
        assert_eq!(attempt.mutations, committed.mutations);
        exact_image(&before, &fixture.store.snapshot()?, &committed.mutations)?;
        let image = fixture.store.snapshot()?;
        // Physical Fjall reopening is checked only after the owner is dropped.
        assert_eq!(fixture.store.inner.snapshot()?, image);
    }
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

async fn duplicate_and_absent_rule_refusals_stamp_without_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    for create in [true, false] {
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        let result = if create {
            create_rule(&fixture, "$Default", RuleFilter::False)
                .map(|_| CommandOutcome::RuleCreated)
        } else {
            delete_rule(&fixture, "absent")
        };
        assert_eq!(
            result,
            Err(rule_error(if create {
                BrokerError::RuleAlreadyExists
            } else {
                BrokerError::RuleNotFound
            }))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(get_rule(&fixture, "absent")?, None);
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

async fn missing_child_is_bind_first_not_an_orphan_rule_health_proof<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture
        .store
        .inner
        .apply(WriteBatch::default().put(rule_key(&fixture, "orphan"), vec![255]))?;
    let direct = StateMachine::new(fixture.store.inner.clone()).rules(
        &fixture.namespace,
        &fixture.topic,
        &fixture.name,
    );
    assert_eq!(direct, Err(BrokerError::DanglingRuleMetadata));
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for operation in 0..4 {
        fixture.store.arm(true);
        let result = match operation {
            0 => create_rule(&fixture, "orphan", RuleFilter::True).map(|_| ()),
            1 => get_rule(&fixture, "orphan").map(|_| ()),
            2 => list_rules(&fixture, 0, 100).map(|_| ()),
            _ => delete_rule(&fixture, "orphan").map(|_| ()),
        };
        assert_eq!(result, Err(rule_error(BrokerError::SubscriptionNotFound)));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    Ok(())
}

async fn reads_validate_complete_rule_health_before_lookup_or_page<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let corrupt = rule_key(&fixture, "corrupt");
    for bytes in [
        vec![255],
        codec::encode(&RuleDefinition {
            name: RuleName::new("different")?,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(1_000),
            action: None,
        })?,
    ] {
        fixture
            .store
            .inner
            .apply(WriteBatch::default().put(corrupt.clone(), bytes))?;
        let expected = StateMachine::new(fixture.store.inner.clone())
            .rules(&fixture.namespace, &fixture.topic, &fixture.name)
            .unwrap_err();
        let before = fixture.store.snapshot()?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        for operation in 0..3 {
            fixture.store.arm(true);
            let result = match operation {
                0 => get_rule(&fixture, "$Default").map(|_| ()),
                1 => get_rule(&fixture, "absent").map(|_| ()),
                _ => list_rules(&fixture, 1_000, 1).map(|_| ()),
            };
            assert_eq!(result, Err(rule_error(expected.clone())));
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.store.snapshot()?, before);
        }
    }
    Ok(())
}

async fn unsupported_projection_is_not_action_stripping_or_page_hiding<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    native_opaque_rules(&fixture)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "$Default")?,
        Some(definition("$Default", RuleFilter::True))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "native-sql")?,
        Some(definition(
            "native-sql",
            RuleFilter::Sql(SqlFilter::new("1=0")?)
        ))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "native-action"),
        Err(AtomRuleOwnerError::UnsupportedDefinition)
    );
    assert_owner(&fixture.store.disarm(), 0);
    for (skip, top) in [(0, 1), (0, 100), (1_000, 1)] {
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, skip, top),
            Err(AtomRuleOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "owned", RuleFilter::False)?,
        definition("owned", RuleFilter::False)
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    exact_image(
        &before,
        &fixture.store.snapshot()?,
        &expected_put(&fixture, "owned", RuleFilter::False, 1_000)?,
    )?;
    for name in ["native-sql", "native-action"] {
        let before = fixture.store.snapshot()?;
        fixture.store.arm(false);
        assert_eq!(delete_rule(&fixture, name)?, CommandOutcome::RuleDeleted);
        let observed = fixture.store.disarm();
        assert_owner(&observed, 1);
        assert_eq!(observed.mutations, expected_delete(&fixture, name, 1_000)?);
        exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
    }
    Ok(())
}

async fn admission_corruption_keeps_original_priority_before_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let child = fixture.child()?;
    let parent_key = keys::entity_incarnation(&fixture.namespace, &fixture.topic);
    let identity =
        codec::decode::<EntityIncarnation>(&fixture.store.inner.get(&parent_key)?.unwrap())?;
    let faults = [
        (parent_key.clone(), None),
        (
            parent_key.clone(),
            Some(codec::encode(&EntityIncarnation::new(
                identity.generation(),
                identity.kind(),
                true,
            )?)?),
        ),
        (
            parent_key,
            Some(codec::encode(&EntityIncarnation::new(
                identity.generation(),
                domain::EntityIncarnationKind::Queue,
                false,
            )?)?),
        ),
        (keys::entity_incarnation(&fixture.namespace, &child), None),
        (
            keys::subscription(&fixture.namespace, &fixture.topic, &fixture.name),
            None,
        ),
        (
            keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
            None,
        ),
        (
            keys::queue_capacity_mode(&fixture.namespace, &fixture.topic),
            Some(vec![255]),
        ),
        (
            keys::queue_capacity_usage(&fixture.namespace, &child),
            Some(vec![255]),
        ),
    ];
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for (key, value) in faults {
        let original = fixture.store.inner.get(&key)?;
        fixture.store.inner.apply(match value {
            Some(value) => WriteBatch::default().put(key.clone(), value),
            None => WriteBatch::default().delete(key.clone()),
        })?;
        let expected = match fixture.get().unwrap_err() {
            AtomSubscriptionOwnerError::Submit(error) => AtomRuleOwnerError::Submit(error),
            AtomSubscriptionOwnerError::UnsupportedDefinition => {
                panic!("fault must retain original corruption")
            }
        };
        let before = fixture.store.snapshot()?;
        for operation in 0..4 {
            fixture.store.arm(true);
            let result = match operation {
                0 => create_rule(&fixture, "safe", RuleFilter::False).map(|_| ()),
                1 => get_rule(&fixture, "$Default").map(|_| ()),
                2 => list_rules(&fixture, 0, 100).map(|_| ()),
                _ => delete_rule(&fixture, "$Default").map(|_| ()),
            };
            assert_eq!(result, Err(expected.clone()), "admission key: {key:?}");
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.store.snapshot()?, before);
        }
        fixture.store.inner.apply(match original {
            Some(value) => WriteBatch::default().put(key, value),
            None => WriteBatch::default().delete(key),
        })?;
    }
    Ok(())
}

async fn incompatible_subscription_and_desired_profiles_refuse_without_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    for desired in [
        definition(
            "correlation",
            RuleFilter::Correlation(correlation_value(MessageValue::Uuid([0; 16]))),
        ),
        definition(".", RuleFilter::True),
        definition("bad\u{FFFE}", RuleFilter::False),
    ] {
        let before = fixture.store.snapshot()?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().create_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                desired,
            ),
            Err(AtomRuleOwnerError::UnsupportedDefinition)
        );
        assert_eq!(fixture.store.disarm().commits, 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    for (label, config) in [
        (
            "session",
            SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        ),
        (
            "hidden",
            SubscriptionConfig {
                max_message_bytes: 4_096,
                ..SubscriptionConfig::default()
            },
        ),
        (
            "lock",
            SubscriptionConfig {
                lock_duration_millis: 4_999,
                ..SubscriptionConfig::default()
            },
        ),
    ] {
        let subscription = SubscriptionName::new(label)?;
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateSubscription {
                name: subscription.clone(),
                config,
            },
        )?;
        let before = fixture.store.snapshot()?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        for operation in 0..4 {
            fixture.store.arm(true);
            let handle = fixture.handle();
            let result = match operation {
                0 => handle
                    .create_atom_rule_blocking(
                        fixture.namespace.clone(),
                        fixture.topic.clone(),
                        subscription.clone(),
                        definition("safe", RuleFilter::True),
                    )
                    .map(|_| ()),
                1 => handle
                    .get_atom_rule_blocking(
                        fixture.namespace.clone(),
                        fixture.topic.clone(),
                        subscription.clone(),
                        RuleName::new("$Default")?,
                    )
                    .map(|_| ()),
                2 => handle
                    .list_atom_rules_blocking(
                        fixture.namespace.clone(),
                        fixture.topic.clone(),
                        subscription.clone(),
                        0,
                        100,
                    )
                    .map(|_| ()),
                _ => handle
                    .delete_atom_rule_blocking(
                        fixture.namespace.clone(),
                        fixture.topic.clone(),
                        subscription.clone(),
                        RuleName::new("$Default")?,
                    )
                    .map(|_| ()),
            };
            assert_eq!(result, Err(AtomRuleOwnerError::UnsupportedDefinition));
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.store.snapshot()?, before);
        }
        let mode = keys::queue_capacity_mode(
            &fixture.namespace,
            &fixture.topic.subscription(&subscription)?,
        );
        fixture
            .store
            .inner
            .apply(WriteBatch::default().put(mode.clone(), vec![255]))?;
        let corrupt = fixture.store.snapshot()?;
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().get_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                subscription.clone(),
                RuleName::new("$Default")?,
            ),
            Err(rule_error(BrokerError::QueueCapacityCorrupt))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(
            fixture.store.snapshot()?,
            corrupt,
            "stored corruption precedes scalar profile refusal"
        );
        fixture
            .store
            .inner
            .apply(WriteBatch::default().delete(mode))?;
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
    }
    Ok(())
}

async fn bounded_complete_pages_and_rule_limit_keep_native_semantics<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let mut expected = vec![definition("$Default", RuleFilter::True)];
    for index in 0..domain::MAX_SUBSCRIPTION_RULES - 1 {
        let name = format!("rule{index:02}");
        expected.push(create_rule(&fixture, &name, RuleFilter::False)?);
    }
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "overflow", RuleFilter::True),
        Err(rule_error(BrokerError::RuleLimitExceeded {
            maximum: domain::MAX_SUBSCRIPTION_RULES
        }))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for (skip, top) in [(0, 100), (0, 1), (1, 2), (32, 100), (1_000, 1)] {
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, skip, top)?,
            expected
                .iter()
                .skip(skip)
                .take(top)
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    for (skip, top) in [(1_001, 1), (0, 0), (0, 101)] {
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, skip, top),
            Err(AtomRuleOwnerError::InvalidPageBounds)
        );
        let observed = fixture.store.disarm();
        assert_eq!(observed.commits, 0);
        assert!(
            observed.threads.is_empty(),
            "invalid bounds unexpectedly read the store"
        );
        assert_eq!(fixture.store.snapshot()?, before);
    }
    Ok(())
}

async fn malformed_rule_mutators_preserve_after_stamp_planner_errors<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    fixture
        .store
        .inner
        .apply(WriteBatch::default().put(rule_key(&fixture, "broken"), vec![255]))?;
    let expected = StateMachine::new(fixture.store.inner.clone())
        .rules(&fixture.namespace, &fixture.topic, &fixture.name)
        .unwrap_err();
    let before = fixture.store.snapshot()?;
    for create in [true, false] {
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        let result = if create {
            create_rule(&fixture, "safe", RuleFilter::True).map(|_| CommandOutcome::RuleCreated)
        } else {
            delete_rule(&fixture, "$Default")
        };
        assert_eq!(result, Err(rule_error(expected.clone())));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    Ok(())
}

async fn default_rule_delete_and_recreate_preserve_empty_set_meaning<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    fixture.store.arm(false);
    assert_eq!(
        delete_rule(&fixture, "$Default")?,
        CommandOutcome::RuleDeleted
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert_eq!(
        observed.mutations,
        expected_delete(&fixture, "$Default", 1_000)?
    );
    exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
    assert!(list_rules(&fixture, 0, 100)?.is_empty());
    fixture.submit(
        &fixture.topic,
        CommandKind::Send {
            message_id: "empty-set".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert!(
        StateMachine::new(fixture.store.inner.clone())
            .message(
                &fixture.namespace,
                &fixture.child()?,
                SequenceNumber::new(1)
            )?
            .is_none()
    );
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(1_500);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "$Default", RuleFilter::False)?,
        definition("$Default", RuleFilter::False)
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert_eq!(
        observed.mutations,
        expected_put(&fixture, "$Default", RuleFilter::False, 1_500)?
    );
    exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
    fixture.submit(
        &fixture.topic,
        CommandKind::Send {
            message_id: "false-set".into(),
            body: vec![2],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert!(
        StateMachine::new(fixture.store.inner.clone())
            .message(
                &fixture.namespace,
                &fixture.child()?,
                SequenceNumber::new(2)
            )?
            .is_none()
    );
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false);
    assert_eq!(
        delete_rule(&fixture, "$Default")?,
        CommandOutcome::RuleDeleted
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert_eq!(
        observed.mutations,
        expected_delete(&fixture, "$Default", 2_000)?
    );
    exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_500);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "$Default", RuleFilter::True)?,
        definition("$Default", RuleFilter::True)
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert_eq!(
        observed.mutations,
        expected_put(&fixture, "$Default", RuleFilter::True, 2_500)?
    );
    exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
    assert_eq!(
        stored(&fixture, "$Default")?.unwrap().created_at,
        Timestamp::from_millis(2_500)
    );
    fixture.submit(
        &fixture.topic,
        CommandKind::Send {
            message_id: "recreated-default".into(),
            body: vec![3],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert_eq!(
        StateMachine::new(fixture.store.inner.clone())
            .message(
                &fixture.namespace,
                &fixture.child()?,
                SequenceNumber::new(3)
            )?
            .unwrap()
            .body,
        vec![3]
    );
    Ok(())
}

async fn asynchronous_rule_jobs_publish_no_delivery_wakeups<P: StoreProvider>(
    provider: P,
) -> TestResult {
    use protocol_amqp::Broker as _;
    use std::{future::poll_fn, task::Poll};
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let handle = fixture.handle();
    let child = fixture.child()?;
    let shadow = child.dead_letter_queue()?;
    let mut child_wait = Box::pin(handle.deliverable(&fixture.namespace, &child));
    let mut shadow_wait = Box::pin(handle.deliverable(&fixture.namespace, &shadow));
    fixture.store.arm(false);
    assert_eq!(
        bounded(
            "async rule create",
            handle.create_atom_rule(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                definition("async", RuleFilter::False),
            )
        )
        .await?,
        definition("async", RuleFilter::False)
    );
    assert_owner(&fixture.store.disarm(), 1);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        bounded(
            "async rule get",
            handle.get_atom_rule(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                RuleName::new("async")?,
            )
        )
        .await?,
        Some(definition("async", RuleFilter::False))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        bounded(
            "async rule list",
            handle.list_atom_rules(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                1,
                1,
            )
        )
        .await?,
        vec![definition("async", RuleFilter::False)]
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        bounded(
            "async rule delete",
            handle.delete_atom_rule(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                RuleName::new("async")?,
            )
        )
        .await?,
        CommandOutcome::RuleDeleted
    );
    assert_owner(&fixture.store.disarm(), 1);
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(child_wait);
    drop(shadow_wait);
    drop(handle);
    Ok(())
}

async fn stale_child_rule_fences_refuse_before_clock_and_by_name_uses_current<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let target = AdminTarget::Subscription {
        topic: fixture.topic.clone(),
        name: fixture.name.clone(),
    };
    let old = bounded(
        "old rule child",
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target),
    )
    .await?
    .unwrap()
    .binding;
    fixture.delete()?;
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for kind in [
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("stale")?,
            filter: RuleFilter::True,
        },
        CommandKind::DeleteRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("$Default")?,
        },
    ] {
        fixture.store.arm(true);
        assert_eq!(
            fixture
                .handle()
                .submit_fenced_blocking(old.clone(), fixture.topic.clone(), kind),
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::EntityBindingStale
            )))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.store.arm(true);
    assert_eq!(
        fixture
            .handle()
            .rules_fenced_blocking(old, fixture.topic.clone(), fixture.name.clone()),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::EntityBindingStale
        )))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "current", RuleFilter::False)?,
        definition("current", RuleFilter::False)
    );
    assert_owner(&fixture.store.disarm(), 1);
    exact_image(
        &before,
        &fixture.store.snapshot()?,
        &expected_put(&fixture, "current", RuleFilter::False, 1_000)?,
    )?;
    Ok(())
}

async fn sql_rule_prepared_mutations_preserve_retained_raw_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig {
        default_time_to_live_millis: Some(120_000),
        ..SubscriptionConfig::default()
    })?;
    native_opaque_rules(&fixture)?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("sibling")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    for sequence in 1..=3 {
        fixture.submit(
            &fixture.topic,
            CommandKind::Send {
                message_id: format!("sql-retained-{sequence}"),
                body: vec![sequence as u8; 32],
                time_to_live_millis: None,
                session_id: None,
            },
        )?;
        if sequence < 3 {
            let CommandOutcome::Received(Some(delivery)) = fixture.submit(
                &fixture.child()?,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("trusted SQL retention seed must be deliverable");
            };
            if sequence == 1 {
                fixture.submit(
                    &fixture.child()?,
                    CommandKind::DeadLetter {
                        sequence: delivery.sequence,
                        lock_token: delivery.lock.unwrap().token,
                        reason: "sql-retention".into(),
                        description: "trusted seed".into(),
                    },
                )?;
            }
        }
    }
    {
        let machine = StateMachine::new(fixture.store.inner.clone());
        let child = fixture.child()?;
        let shadow = child.dead_letter_queue()?;
        let ready = machine
            .message(&fixture.namespace, &child, SequenceNumber::new(3))?
            .unwrap();
        assert_eq!(ready.state, domain::MessageState::Ready);
        assert_eq!(ready.body, vec![3; 32]);
        assert_eq!(ready.expires_at, Some(Timestamp::from_millis(121_000)));
        let locked = machine
            .message(&fixture.namespace, &child, SequenceNumber::new(2))?
            .unwrap();
        assert!(matches!(locked.state, domain::MessageState::Locked { .. }));
        assert_eq!(locked.body, vec![2; 32]);
        let dead = machine
            .message(&fixture.namespace, &shadow, SequenceNumber::new(1))?
            .unwrap();
        assert_eq!(dead.body, vec![1; 32]);
        assert_eq!(
            dead.dead_letter.unwrap().reason,
            domain::DeadLetterReason::Application("sql-retention".into())
        );
    }
    let source = " \r\nuser.colour = 'Red & <x>' OR sys.Label IS NULL\r\n ";
    let filter = RuleFilter::Sql(SqlFilter::new(source)?);
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(1_500);
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        create_rule(&fixture, "sql", filter.clone()),
        Err(AtomRuleOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    let failed = fixture.store.disarm();
    assert_owner(&failed, 1);
    assert!(!failed.committed);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "sql", filter.clone())?,
        definition("sql", filter.clone())
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert!(observed.committed);
    let expected = expected_put(&fixture, "sql", filter.clone(), 1_500)?;
    assert_eq!(failed.mutations, expected);
    assert_eq!(observed.mutations, expected);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 2);
    exact_image(&before, &fixture.store.snapshot()?, &expected)?;
    let retained = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "sql")?,
        Some(definition("sql", filter.clone()))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        list_rules(&fixture, 1_000, 1),
        Err(AtomRuleOwnerError::UnsupportedDefinition),
        "a supported SQL row does not hide the other native action"
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, retained);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false);
    assert_eq!(delete_rule(&fixture, "sql")?, CommandOutcome::RuleDeleted);
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let expected = expected_delete(&fixture, "sql", 2_000)?;
    assert_eq!(observed.mutations, expected);
    exact_image(&retained, &fixture.store.snapshot()?, &expected)?;
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

async fn invalid_sql_dto_priority_precedes_admission_and_command_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let session = SubscriptionName::new("session")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: session.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    let token_limit = format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS));
    let sources = ["broken =", "lower(name)", token_limit.as_str()];
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for source in sources {
        let filter = domain::codec::decode::<SqlFilter>(&domain::codec::encode(&(1_u32, source))?)?;
        assert!(domain::SqlProgram::compile(filter.expression()).is_err());
        for subscription in [
            fixture.name.clone(),
            SubscriptionName::new("absent")?,
            session.clone(),
        ] {
            fixture.store.arm(true);
            assert_eq!(
                fixture.handle().create_atom_rule_blocking(
                    fixture.namespace.clone(),
                    fixture.topic.clone(),
                    subscription,
                    definition("$Default", RuleFilter::Sql(filter.clone())),
                ),
                Err(AtomRuleOwnerError::UnsupportedDefinition),
                "DTO validation is inside the owner turn, before admission/stamp"
            );
            let observed = fixture.store.disarm();
            assert_eq!(observed.commits, 0);
            assert!(observed.threads.is_empty(), "invalid DTO read stored state");
            assert_eq!(fixture.store.snapshot()?, before);
        }
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    let valid = definition("$Default", RuleFilter::Sql(SqlFilter::new("1=1")?));
    for (subscription, expected) in [
        (
            SubscriptionName::new("absent")?,
            rule_error(BrokerError::SubscriptionNotFound),
        ),
        (session, AtomRuleOwnerError::UnsupportedDefinition),
    ] {
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().create_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                subscription,
                valid.clone(),
            ),
            Err(expected)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "$Default", valid.filter),
        Err(rule_error(BrokerError::RuleAlreadyExists))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

async fn stored_sql_health_precedes_projection_lookup_and_paging<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let corrupt = rule_key(&fixture, "corrupt");
    let token_limit = format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS));
    for source in ["secret =", "lower(name)", token_limit.as_str()] {
        let filter = codec::decode::<SqlFilter>(&codec::encode(&(1_u32, source))?)?;
        fixture.store.inner.apply(WriteBatch::default().put(
            corrupt.clone(),
            codec::encode(&RuleDefinition {
                name: RuleName::new("corrupt")?,
                filter: RuleFilter::Sql(filter),
                created_at: Timestamp::from_millis(1_000),
                action: None,
            })?,
        ))?;
        let expected = StateMachine::new(fixture.store.inner.clone())
            .rules(&fixture.namespace, &fixture.topic, &fixture.name)
            .unwrap_err();
        let before = fixture.store.snapshot()?;
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        for operation in 0..3 {
            fixture.store.arm(true);
            let result = match operation {
                0 => get_rule(&fixture, "$Default").map(|_| ()),
                1 => get_rule(&fixture, "absent").map(|_| ()),
                _ => list_rules(&fixture, 1_000, 1).map(|_| ()),
            };
            assert_eq!(result, Err(rule_error(expected.clone())));
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.store.snapshot()?, before);
        }
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
        for create in [true, false] {
            let clocks = fixture.clock.reads.load(Ordering::SeqCst);
            fixture.store.arm(false);
            let result = if create {
                create_rule(&fixture, "safe", RuleFilter::Sql(SqlFilter::new("1=1")?))
                    .map(|_| CommandOutcome::RuleCreated)
            } else {
                delete_rule(&fixture, "$Default")
            };
            assert_eq!(result, Err(rule_error(expected.clone())));
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
            assert_eq!(fixture.store.snapshot()?, before);
        }
    }
    fixture
        .store
        .inner
        .apply(WriteBatch::default().delete(corrupt))?;
    let illegal = SqlFilter::new("'\u{FFFE}' = 'x'")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("xml-illegal")?,
            filter: RuleFilter::Sql(illegal),
        },
    )?;
    assert!(
        StateMachine::new(fixture.store.inner.clone())
            .rules(&fixture.namespace, &fixture.topic, &fixture.name)
            .is_ok()
    );
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "$Default")?,
        Some(definition("$Default", RuleFilter::True))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "xml-illegal"),
        Err(AtomRuleOwnerError::UnsupportedDefinition)
    );
    assert_owner(&fixture.store.disarm(), 0);
    for (skip, top) in [(0, 1), (1_000, 1)] {
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, skip, top),
            Err(AtomRuleOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

async fn correlation_rule_prepared_mutations_preserve_retained_raw_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    use protocol_amqp::Broker as _;
    use std::{future::poll_fn, task::Poll};

    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig {
        default_time_to_live_millis: Some(120_000),
        ..SubscriptionConfig::default()
    })?;
    native_opaque_rules(&fixture)?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("sibling")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    for sequence in 1..=3 {
        fixture.submit(
            &fixture.topic,
            CommandKind::Send {
                message_id: format!("correlation-retained-{sequence}"),
                body: vec![sequence as u8; 32],
                time_to_live_millis: None,
                session_id: None,
            },
        )?;
        if sequence < 3 {
            let CommandOutcome::Received(Some(delivery)) = fixture.submit(
                &fixture.child()?,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("trusted correlation retention seed must be deliverable");
            };
            if sequence == 1 {
                fixture.submit(
                    &fixture.child()?,
                    CommandKind::DeadLetter {
                        sequence: delivery.sequence,
                        lock_token: delivery.lock.unwrap().token,
                        reason: "correlation-retention".into(),
                        description: "trusted seed".into(),
                    },
                )?;
            }
        }
    }
    let child = fixture.child()?;
    let shadow = child.dead_letter_queue()?;
    {
        let machine = StateMachine::new(fixture.store.inner.clone());
        let ready = machine
            .message(&fixture.namespace, &child, SequenceNumber::new(3))?
            .unwrap();
        assert_eq!(ready.state, domain::MessageState::Ready);
        assert_eq!(ready.body, vec![3; 32]);
        assert_eq!(ready.expires_at, Some(Timestamp::from_millis(121_000)));
        let locked = machine
            .message(&fixture.namespace, &child, SequenceNumber::new(2))?
            .unwrap();
        assert!(matches!(locked.state, domain::MessageState::Locked { .. }));
        let dead = machine
            .message(&fixture.namespace, &shadow, SequenceNumber::new(1))?
            .unwrap();
        assert_eq!(dead.body, vec![1; 32]);
        assert_eq!(
            dead.dead_letter.unwrap().reason,
            domain::DeadLetterReason::Application("correlation-retention".into())
        );
    }
    let handle = fixture.handle();
    let mut child_wait = Box::pin(handle.deliverable(&fixture.namespace, &child));
    let mut shadow_wait = Box::pin(handle.deliverable(&fixture.namespace, &shadow));
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let filter = RuleFilter::Correlation(correlation_filter());
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.manual.set(1_500);
    fixture.store.arm(false);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        create_rule(&fixture, "correlation", filter.clone()),
        Err(AtomRuleOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    let failed = fixture.store.disarm();
    assert_owner(&failed, 1);
    assert!(!failed.committed);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "correlation", filter.clone())?,
        definition("correlation", filter.clone())
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert!(observed.committed);
    let expected = expected_put(&fixture, "correlation", filter.clone(), 1_500)?;
    assert_eq!(failed.mutations, expected);
    assert_eq!(observed.mutations, expected);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 2);
    exact_image(&before, &fixture.store.snapshot()?, &expected)?;
    let retained = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        get_rule(&fixture, "correlation")?,
        Some(definition("correlation", filter.clone()))
    );
    assert_owner(&fixture.store.disarm(), 0);
    fixture.store.arm(true);
    assert_eq!(
        list_rules(&fixture, 1_000, 1),
        Err(AtomRuleOwnerError::UnsupportedDefinition)
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, retained);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "correlation", filter),
        Err(rule_error(BrokerError::RuleAlreadyExists))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 3);
    assert_eq!(fixture.store.snapshot()?, retained);
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false);
    assert_eq!(
        delete_rule(&fixture, "correlation")?,
        CommandOutcome::RuleDeleted
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert_eq!(
        observed.mutations,
        expected_delete(&fixture, "correlation", 2_000)?
    );
    exact_image(&retained, &fixture.store.snapshot()?, &observed.mutations)?;
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(child_wait);
    drop(shadow_wait);
    drop(handle);
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

async fn invalid_correlation_dto_priority_precedes_admission_and_command_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let session = SubscriptionName::new("session")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: session.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    let mut invalid: Vec<CorrelationFilter> = [
        MessageValue::Null,
        MessageValue::Ubyte(1),
        MessageValue::Ushort(1),
        MessageValue::Uint(1),
        MessageValue::Ulong(1),
        MessageValue::Byte(-1),
        MessageValue::Short(-1),
        MessageValue::Float(1.0_f32.to_bits()),
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::Char('x'),
        MessageValue::Uuid([0; 16]),
        MessageValue::Binary(vec![1]),
        MessageValue::Symbol("ascii".into()),
        MessageValue::Double(0x7ff8_0000_0000_0001),
        MessageValue::Timestamp(-62_135_596_800_001),
        MessageValue::Timestamp(253_402_300_800_000),
        MessageValue::String("secret\u{FFFE}".into()),
        MessageValue::List(Vec::new()),
        MessageValue::Map(Vec::new()),
        MessageValue::Array(Vec::new()),
        MessageValue::Described {
            descriptor: domain::MessageDescriptor::Code(1),
            value: Box::new(MessageValue::Null),
        },
    ]
    .into_iter()
    .map(correlation_value)
    .collect();
    invalid.push(CorrelationFilter {
        correlation_id: Some("secret\u{FFFE}".into()),
        ..CorrelationFilter::default()
    });
    invalid.push(CorrelationFilter {
        properties: [("secret\u{FFFE}".into(), MessageValue::Bool(true))]
            .into_iter()
            .collect(),
        ..CorrelationFilter::default()
    });
    invalid.push(CorrelationFilter {
        correlation_id: Some("system-counts-too".into()),
        properties: (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
            .map(|index| (format!("property-{index}"), MessageValue::Int(index as i32)))
            .collect(),
        ..CorrelationFilter::default()
    });
    invalid.extend(
        COLLIDING_CORRELATION_KEYS
            .iter()
            .map(|&(left, right)| correlation_keys(left, right)),
    );
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for filter in invalid {
        for subscription in [
            fixture.name.clone(),
            SubscriptionName::new("absent")?,
            session.clone(),
        ] {
            fixture.store.arm(true);
            assert_eq!(
                fixture.handle().create_atom_rule_blocking(
                    fixture.namespace.clone(),
                    fixture.topic.clone(),
                    subscription,
                    definition("$Default", RuleFilter::Correlation(filter.clone())),
                ),
                Err(AtomRuleOwnerError::UnsupportedDefinition)
            );
            let observed = fixture.store.disarm();
            assert_eq!(observed.commits, 0);
            assert!(
                observed.threads.is_empty(),
                "invalid correlation DTO read stored state"
            );
            assert_eq!(fixture.store.snapshot()?, before);
        }
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    let valid = definition(
        "$Default",
        RuleFilter::Correlation(CorrelationFilter::default()),
    );
    for (subscription, expected) in [
        (
            SubscriptionName::new("absent")?,
            rule_error(BrokerError::SubscriptionNotFound),
        ),
        (session, AtomRuleOwnerError::UnsupportedDefinition),
    ] {
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().create_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                subscription,
                valid.clone(),
            ),
            Err(expected)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        create_rule(&fixture, "$Default", valid.filter),
        Err(rule_error(BrokerError::RuleAlreadyExists))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
    assert_eq!(fixture.store.snapshot()?, before);

    let oversized = RuleFilter::Correlation(correlation_value(MessageValue::String(
        "x".repeat(domain::MAX_RULE_BYTES + 1),
    )));
    for (name, expected) in [
        (
            "oversized",
            BrokerError::RuleTooLarge {
                maximum_bytes: domain::MAX_RULE_BYTES,
            },
        ),
        ("$Default", BrokerError::RuleAlreadyExists),
    ] {
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        assert_eq!(
            create_rule(&fixture, name, oversized.clone()),
            Err(rule_error(expected))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
        assert_eq!(
            fixture.store.snapshot()?,
            before,
            "native size admission must preserve durable Clock"
        );
    }

    for (index, &(left, right)) in DISTINCT_CORRELATION_KEYS.iter().enumerate() {
        let name = format!("distinct-{index}");
        let filter = RuleFilter::Correlation(correlation_keys(left, right));
        let desired = definition(&name, filter.clone());
        let before = fixture.store.snapshot()?;
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        assert_eq!(create_rule(&fixture, &name, filter.clone())?, desired);
        let observed = fixture.store.disarm();
        assert_owner(&observed, 1);
        assert_eq!(
            observed.mutations,
            expected_put(&fixture, &name, filter.clone(), 1_000)?
        );
        exact_image(&before, &fixture.store.snapshot()?, &observed.mutations)?;
        assert_eq!(stored(&fixture, &name)?.unwrap().filter, filter);
        let retained = fixture.store.snapshot()?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        fixture.store.arm(true);
        assert_eq!(get_rule(&fixture, &name)?, Some(desired.clone()));
        assert_owner(&fixture.store.disarm(), 0);
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, 0, 100)?,
            vec![definition("$Default", RuleFilter::True), desired]
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, retained);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
        fixture.store.arm(false);
        assert_eq!(delete_rule(&fixture, &name)?, CommandOutcome::RuleDeleted);
        let observed = fixture.store.disarm();
        assert_owner(&observed, 1);
        assert_eq!(observed.mutations, expected_delete(&fixture, &name, 1_000)?);
        exact_image(&retained, &fixture.store.snapshot()?, &observed.mutations)?;
        assert_eq!(fixture.store.snapshot()?, before);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 2);
    }
    Ok(())
}

async fn stored_correlation_health_precedes_projection_lookup_and_paging<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("projectable")?,
            filter: RuleFilter::Correlation(correlation_filter()),
        },
    )?;
    for (name, filter) in [
        (
            "native-uuid",
            correlation_value(MessageValue::Uuid([1; 16])),
        ),
        (
            "native-nan",
            correlation_value(MessageValue::Double(0x7ff8_0000_0000_0001)),
        ),
        (
            "xml-illegal",
            correlation_value(MessageValue::String("secret\u{FFFE}".into())),
        ),
    ] {
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateRule {
                subscription: fixture.name.clone(),
                name: RuleName::new(name)?,
                filter: RuleFilter::Correlation(filter),
            },
        )?;
    }
    for (index, &(left, right)) in COLLIDING_CORRELATION_KEYS.iter().enumerate() {
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateRule {
                subscription: fixture.name.clone(),
                name: RuleName::new(format!("collision-{index}"))?,
                filter: RuleFilter::Correlation(correlation_keys(left, right)),
            },
        )?;
    }
    assert!(
        StateMachine::new(fixture.store.inner.clone())
            .rules(&fixture.namespace, &fixture.topic, &fixture.name)
            .is_ok()
    );
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for (name, expected) in [
        ("$Default", Some(definition("$Default", RuleFilter::True))),
        (
            "projectable",
            Some(definition(
                "projectable",
                RuleFilter::Correlation(correlation_filter()),
            )),
        ),
        ("absent", None),
    ] {
        fixture.store.arm(true);
        assert_eq!(get_rule(&fixture, name)?, expected);
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    for name in ["native-uuid", "native-nan", "xml-illegal"]
        .into_iter()
        .map(String::from)
        .chain((0..COLLIDING_CORRELATION_KEYS.len()).map(|index| format!("collision-{index}")))
    {
        fixture.store.arm(true);
        assert_eq!(
            get_rule(&fixture, &name),
            Err(AtomRuleOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
    }
    for (skip, top) in [(0, 1), (1_000, 1)] {
        fixture.store.arm(true);
        assert_eq!(
            list_rules(&fixture, skip, top),
            Err(AtomRuleOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    for create in [true, false] {
        let before = fixture.store.snapshot()?;
        fixture.store.arm(false);
        if create {
            assert_eq!(
                create_rule(
                    &fixture,
                    "transient",
                    RuleFilter::Correlation(CorrelationFilter::default())
                )?,
                definition(
                    "transient",
                    RuleFilter::Correlation(CorrelationFilter::default())
                )
            );
        } else {
            assert_eq!(
                delete_rule(&fixture, "transient")?,
                CommandOutcome::RuleDeleted
            );
        }
        let observed = fixture.store.disarm();
        assert_owner(&observed, 1);
        let expected = if create {
            expected_put(
                &fixture,
                "transient",
                RuleFilter::Correlation(CorrelationFilter::default()),
                1_000,
            )?
        } else {
            expected_delete(&fixture, "transient", 1_000)?
        };
        assert_eq!(observed.mutations, expected);
        exact_image(&before, &fixture.store.snapshot()?, &expected)?;
    }
    let corrupt = rule_key(&fixture, "corrupt");
    let invalid = CorrelationFilter {
        correlation_id: Some("system-counts-too".into()),
        properties: (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
            .map(|index| (format!("property-{index}"), MessageValue::Int(index as i32)))
            .collect(),
        ..CorrelationFilter::default()
    };
    fixture.store.inner.apply(WriteBatch::default().put(
        corrupt,
        codec::encode(&RuleDefinition {
            name: RuleName::new("corrupt")?,
            filter: RuleFilter::Correlation(invalid),
            created_at: Timestamp::from_millis(1_000),
            action: None,
        })?,
    ))?;
    let expected = StateMachine::new(fixture.store.inner.clone())
        .rules(&fixture.namespace, &fixture.topic, &fixture.name)
        .unwrap_err();
    let before = fixture.store.snapshot()?;
    let clocks = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for operation in 0..3 {
        fixture.store.arm(true);
        let result = match operation {
            0 => get_rule(&fixture, "projectable").map(|_| ()),
            1 => get_rule(&fixture, "absent").map(|_| ()),
            _ => list_rules(&fixture, 1_000, 1).map(|_| ()),
        };
        assert_eq!(result, Err(rule_error(expected.clone())));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    for create in [true, false] {
        let clocks = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        let result = if create {
            create_rule(
                &fixture,
                "safe",
                RuleFilter::Correlation(CorrelationFilter::default()),
            )
            .map(|_| CommandOutcome::RuleCreated)
        } else {
            delete_rule(&fixture, "$Default")
        };
        assert_eq!(result, Err(rule_error(expected.clone())));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), clocks + 1);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    let image = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, image);
    Ok(())
}

for_each_subscription_backend! {
    rule_mutations_preserve_retained_state_and_exact_timestamps,
    prepared_rule_returns_and_preapply_failures_have_no_postcommit_reads,
    duplicate_and_absent_rule_refusals_stamp_without_mutation,
    missing_child_is_bind_first_not_an_orphan_rule_health_proof,
    reads_validate_complete_rule_health_before_lookup_or_page,
    unsupported_projection_is_not_action_stripping_or_page_hiding,
    admission_corruption_keeps_original_priority_before_clock,
    incompatible_subscription_and_desired_profiles_refuse_without_clock,
    bounded_complete_pages_and_rule_limit_keep_native_semantics,
    malformed_rule_mutators_preserve_after_stamp_planner_errors,
    default_rule_delete_and_recreate_preserve_empty_set_meaning,
    asynchronous_rule_jobs_publish_no_delivery_wakeups,
    stale_child_rule_fences_refuse_before_clock_and_by_name_uses_current,
    sql_rule_prepared_mutations_preserve_retained_raw_state,
    invalid_sql_dto_priority_precedes_admission_and_command_clock,
    stored_sql_health_precedes_projection_lookup_and_paging,
    correlation_rule_prepared_mutations_preserve_retained_raw_state,
    invalid_correlation_dto_priority_precedes_admission_and_command_clock,
    stored_correlation_health_precedes_projection_lookup_and_paging,
}
