use super::*;

fn exact_action_source_and_copies_survive_reopen_and_clock_free_listing<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    remove(&fixture, "child", "$Default", 0)?;
    let source = "  ReMoVe user.[color] /* exact source */ ; ";
    add(
        &fixture,
        "child",
        "named action",
        RuleFilter::True,
        Some(source),
        1,
    )?;
    let expected = RuleDefinition {
        name: RuleName::new("named action")?,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(1),
        action: Some(SqlAction::new(source)?),
    };
    assert_eq!(rules(&fixture, "child")?, vec![expected.clone()]);
    let key = keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("child")?,
        &expected.name,
    );
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(codec::encode(&expected)?)
    );
    publish(&fixture, 2, vec![member("persisted")])?;
    let copies = records(&fixture, &child)?;
    let before = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(rules(&fixture, "child")?, vec![expected]);
    assert_eq!(records(&fixture, &child)?, copies);
    assert_eq!(peek(&fixture, &child, 2)?.len(), 1);
    assert_eq!(copies[0].sequence, SequenceNumber::new(2));
    assert_eq!(
        copies[0]
            .envelope
            .as_ref()
            .expect("retained action")
            .application_properties
            .get("RuleName"),
        Some(&MessageValue::String("named action".into()))
    );
    Ok(())
}

#[derive(serde::Serialize)]
struct StoredAction {
    semantic_version: u32,
    expression: String,
}

#[derive(serde::Serialize)]
struct StoredRule {
    name: RuleName,
    filter: RuleFilter,
    created_at: Timestamp,
    action: Option<StoredAction>,
}

fn malformed_replayed_and_stored_actions_refuse_before_metadata_or_fanout_is_staged<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    subscribe(&fixture, "child", SubscriptionConfig::default())?;
    let malformed: SqlAction = codec::decode(&codec::encode(&StoredAction {
        semantic_version: 1,
        expression: "REMOVE".into(),
    })?)?;
    reject(
        &fixture,
        1,
        CommandKind::CreateRuleWithAction {
            subscription: SubscriptionName::new("child")?,
            name: RuleName::new("replayed")?,
            filter: RuleFilter::True,
            action: malformed,
        },
        BrokerError::SqlActionCompilation(SqlCompileError::Syntax),
    )?;
    for damage in 0..5 {
        fixture.entity = EntityPath::new(format!("damage-{damage}"))?;
        fixture.at(
            1,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        fixture.at(
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Alpha")?,
                config: SubscriptionConfig::default(),
            },
        )?;
        fixture.at(
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("zulu")?,
                config: SubscriptionConfig::default(),
            },
        )?;
        let healthy = fixture
            .entity
            .subscription(&SubscriptionName::new("Alpha")?)?;
        add(
            &fixture,
            "zulu",
            "action",
            RuleFilter::True,
            Some("REMOVE missing"),
            1,
        )?;
        publish(&fixture, 1, vec![member("known")])?;
        let (version, source, expected) = match damage {
            0 => (
                3,
                "REMOVE missing".into(),
                BrokerError::Codec(codec::CodecError::Decode),
            ),
            1 => (1, "REMOVE".into(), BrokerError::DanglingRuleMetadata),
            2 => (
                1,
                "SET color = 'Blue'".into(),
                BrokerError::DanglingRuleMetadata,
            ),
            3 => (
                1,
                "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
                BrokerError::Codec(codec::CodecError::Decode),
            ),
            4 => (1, "REMOVE x;".repeat(33), BrokerError::DanglingRuleMetadata),
            _ => unreachable!(),
        };
        let name = RuleName::new("action")?;
        let sub = SubscriptionName::new("zulu")?;
        fixture.machine.store().apply(WriteBatch::default().put(
            keys::rule(&fixture.namespace, &fixture.entity, &sub, &name),
            codec::encode(&StoredRule {
                name,
                filter: RuleFilter::True,
                created_at: Timestamp::from_millis(1),
                action: Some(StoredAction {
                    semantic_version: version,
                    expression: source,
                }),
            })?,
        ))?;
        let before = fixture.machine.store().snapshot()?;
        let time = fixture.machine.last_applied_time()?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &sub),
            Err(expected.clone())
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, time);
        for kind in [
            CommandKind::SendBatch {
                messages: vec![member("fresh")],
            },
            CommandKind::SendBatch {
                messages: vec![member("known")],
            },
            CommandKind::SendBatch { messages: vec![] },
            CommandKind::ScheduleEnvelopes {
                messages: vec![scheduled(member("future"), 100)],
            },
            CommandKind::ActivateScheduled,
        ] {
            reject(&fixture, 2, kind, expected.clone())?;
        }
        assert_eq!(records(&fixture, &healthy)?.len(), 1);
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    "fresh"
                ))?
                .is_none()
        );
    }
    Ok(())
}

fn failed_rule_creation_and_fanout_commit_retry_after_reopen_without_partial_effects<
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
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    let sibling = subscribe(&fixture, "sibling", SubscriptionConfig::default())?;
    let create = CommandKind::CreateRuleWithAction {
        subscription: SubscriptionName::new("child")?,
        name: RuleName::new("action")?,
        filter: RuleFilter::True,
        action: SqlAction::new("REMOVE color")?,
    };
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    fail_next.store(true, Ordering::SeqCst);
    assert!(matches!(
        apply(&fixture, 1, create.clone()),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        apply(&fixture, 1, create)?.outcome,
        CommandOutcome::RuleCreated
    );
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    observations.lock().expect("observations").commits = 0;
    fail_next.store(true, Ordering::SeqCst);
    assert!(matches!(
        publish(&fixture, 2, vec![member("one"), member("one")]),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    assert_eq!(observations.lock().expect("observations").commits, 1);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    observations.lock().expect("observations").commits = 0;
    let result = publish(&fixture, 2, vec![member("one"), member("one")])?;
    effects(&result, &[child.clone(), sibling.clone()]);
    assert_eq!(observations.lock().expect("observations").commits, 1);
    assert_eq!(
        records(&fixture, &child)?
            .iter()
            .map(|copy| copy.sequence)
            .collect::<Vec<_>>(),
        vec![SequenceNumber::new(1), SequenceNumber::new(3)]
    );
    assert_eq!(records(&fixture, &sibling)?.len(), 1);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("dedup base plus one action")
            .next_sequence,
        4
    );
    let before = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(rules(&fixture, "child")?.len(), 2);
    effects(&publish(&fixture, 3, vec![member("one")])?, &[]);
    assert_eq!(records(&fixture, &child)?.len(), 2);
    Ok(())
}

fn action_command_fences_the_child_incarnation_before_clock_or_compilation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    let sibling = subscribe(&fixture, "sibling", SubscriptionConfig::default())?;
    let binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &child,
            &child,
            EntityIncarnationKind::Subscription,
        )?
        .expect("child binding");
    let parent = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &fixture.entity,
            &fixture.entity,
            EntityIncarnationKind::Topic,
        )?
        .expect("parent binding");
    let kind = CommandKind::CreateRuleWithAction {
        subscription: SubscriptionName::new("child")?,
        name: RuleName::new("fenced")?,
        filter: RuleFilter::True,
        action: SqlAction::new("REMOVE missing")?,
    };
    let command = fixture.command(0, kind);
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.machine.apply_fenced(&FencedCommand {
            binding: parent,
            command: command.clone()
        }),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.machine.validate_fenced_intent(
        &binding,
        &fixture.namespace,
        &fixture.entity,
        &command.kind,
    )?;
    assert_eq!(
        fixture.machine.apply_fenced(&FencedCommand {
            binding: binding.clone(),
            command: command.clone()
        })?,
        CommandOutcome::RuleCreated
    );
    fixture.at(
        1,
        CommandKind::DeleteEntity {
            target: domain::DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("child")?,
            },
        },
    )?;
    fixture.at(
        2,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("child")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    assert_eq!(
        fixture.machine.apply_fenced(&FencedCommand {
            binding,
            command: command.clone()
        }),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    assert_eq!(rules(&fixture, "child")?.len(), 1);
    assert_eq!(rules(&fixture, "sibling")?.len(), 1);
    let fresh = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &child,
            &child,
            EntityIncarnationKind::Subscription,
        )?
        .expect("replacement binding");
    let mut fresh_command = command;
    fresh_command.issued_at = Timestamp::from_millis(2);
    assert_eq!(
        fixture.machine.apply_fenced(&FencedCommand {
            binding: fresh,
            command: fresh_command
        })?,
        CommandOutcome::RuleCreated
    );
    assert!(records(&fixture, &sibling)?.is_empty());
    Ok(())
}

for_each_backend! {
    exact_action_source_and_copies_survive_reopen_and_clock_free_listing,
    malformed_replayed_and_stored_actions_refuse_before_metadata_or_fanout_is_staged,
    failed_rule_creation_and_fanout_commit_retry_after_reopen_without_partial_effects,
    action_command_fences_the_child_incarnation_before_clock_or_compilation,
}
