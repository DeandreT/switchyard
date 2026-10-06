use super::*;

const SQL_ACTION_CODE: u64 = 0x0000013700000006;

impl RulesClient {
    async fn add_action(&mut self, name: &str, filter: &str, action: Value) -> TestResult<Message> {
        self.request(
            name,
            protocol_amqp::ADD_RULE_OPERATION,
            map([
                ("rule-name", Value::String(name.into())),
                (
                    "rule-description",
                    Value::Map(map([
                        ("rule-name", Value::String(name.into())),
                        ("sql-filter", sql(filter)),
                        ("sql-rule-action", action),
                    ])),
                ),
            ]),
        )
        .await
    }
}

fn action(source: &str) -> Value {
    Value::Map(map([("expression", Value::String(source.into()))]))
}

fn action_rules(response: &Message) -> Vec<(String, Value, Value, i64)> {
    status(response, 200);
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("enumeration body")
    };
    let Some(Value::List(rules)) = body.get(&Value::String("rules".into())) else {
        panic!("rule list")
    };
    rules
        .iter()
        .map(|rule| {
            let Value::Map(entry) = rule else {
                panic!("rule entry")
            };
            assert_eq!(entry.len(), 1);
            let fields = described(
                entry
                    .get(&Value::String("rule-description".into()))
                    .expect("description"),
                RULE_DESCRIPTION_CODE,
            );
            let [
                filter,
                action,
                Value::String(name),
                Value::Timestamp(created),
            ] = fields
            else {
                panic!("rule fields")
            };
            (
                name.clone(),
                filter.clone(),
                action.clone(),
                created.milliseconds(),
            )
        })
        .collect()
}

fn condition(response: &Message, expected: &str) {
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from(expected)))
    );
}

fn clock<P: StoreProvider>(node: &Node<P>) -> TestResult<Timestamp> {
    Ok(StateMachine::new(node.store.as_ref().expect("store").clone()).last_applied_time()?)
}

async fn create_list_delete_and_three_private_copies_preserve_original_and_legacy_bodies<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut rules = RulesClient::attach(
        &mut session,
        "action-rules",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    let first_source = "  ReMoVe member; REMOVE [RuleName]; REMOVE marker; ";
    let second_source = "REMOVE nullable; REMOVE missing";
    for (name, source) in [("a-remove", first_source), ("b-remove", second_source)] {
        status(
            &rules
                .add_action(name, "member=7 OR sys.MessageId='legacy'", action(source))
                .await?,
            200,
        );
    }
    status(
        &rules
            .add(
                "overlap",
                "sql-filter",
                sql("member=7 OR sys.MessageId='legacy'"),
            )
            .await?,
        200,
    );
    let before = node.snapshot()?;
    let applied = clock(&node)?;
    let listed = action_rules(&rules.list(100, 0).await?);
    assert_eq!(
        listed
            .iter()
            .map(|rule| rule.0.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "a-remove", "b-remove", "overlap"]
    );
    for (index, source) in [(1, first_source), (2, second_source)] {
        assert_eq!(
            described(&listed[index].2, SQL_ACTION_CODE),
            [Value::String(source.into()), Value::Int(20)]
        );
        assert_eq!(listed[index].3, 1_000);
    }
    for index in [0, 3] {
        assert!(described(&listed[index].2, EMPTY_ACTION_CODE).is_empty());
    }
    assert_eq!(
        action_rules(&rules.list(1, 1).await?),
        vec![listed[1].clone()]
    );
    assert_eq!(
        action_rules(&rules.list(1, 2).await?),
        vec![listed[2].clone()]
    );
    let duplicate = rules
        .add_action("a-remove", "1=1", action("REMOVE replacement"))
        .await?;
    status(&duplicate, 409);
    condition(&duplicate, protocol_amqp::ENTITY_ALREADY_EXISTS);
    assert_eq!(action_rules(&rules.list(100, 0).await?), listed);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(clock(&node)?, applied);
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "action-publisher", "Orders"),
    )
    .await??;
    let mut original = rich(7);
    let properties = &mut original
        .application_properties
        .as_mut()
        .expect("application properties")
        .0;
    properties.insert("RuleName".into(), Value::String("producer-name".into()));
    properties.insert("marker".into(), Value::String("lower".into()));
    properties.insert("Marker".into(), Value::String("capital".into()));
    accepted(timeout(DEADLINE, publisher.send(original.clone())).await??);
    assert_eq!(
        node.peek(&alpha)
            .await?
            .iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "private-action-copies", alpha.as_str()),
    )
    .await??;
    let base = recv(&mut receiver).await?;
    let first = recv(&mut receiver).await?;
    let second = recv(&mut receiver).await?;
    for (delivery, expected_sequence, name, removed) in [
        (&base, 1, None, &[][..]),
        (
            &first,
            2,
            Some("a-remove"),
            &["member", "RuleName", "marker"][..],
        ),
        (&second, 3, Some("b-remove"), &["nullable", "missing"][..]),
    ] {
        assert_eq!(sequence(delivery.message()), expected_sequence);
        let mut expected = original.clone();
        let properties = &mut expected
            .application_properties
            .as_mut()
            .expect("application properties")
            .0;
        for key in removed {
            properties.shift_remove(*key);
        }
        if let Some(name) = name {
            properties.insert("RuleName".into(), Value::String(name.into()));
        }
        // Stored user-property maps are canonicalized by key, not insertion order.
        let sorted: std::collections::BTreeMap<_, _> = properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        *properties = sorted.into_iter().collect();
        content(delivery.message(), &expected);
        assert_eq!(
            delivery
                .message()
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get("Marker")),
            Some(&Value::String("capital".into()))
        );
    }
    // Settle the second transported copy before the first and third.
    timeout(DEADLINE, receiver.accept(&first)).await??;
    node.wait_len(&alpha, 2).await?;
    assert_eq!(
        node.peek(&alpha)
            .await?
            .iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [1, 3]
    );
    timeout(DEADLINE, receiver.accept(&base)).await??;
    node.wait_len(&alpha, 1).await?;
    timeout(DEADLINE, receiver.accept(&second)).await??;
    node.wait_len(&alpha, 0).await?;
    assert_eq!(
        node.submit(
            &node.topic,
            CommandKind::Send {
                message_id: "legacy".into(),
                body: b"legacy-body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            }
        )
        .await?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(4)
        }
    );
    for (expected_sequence, name) in [(4, None), (5, Some("a-remove")), (6, Some("b-remove"))] {
        let delivery = recv(&mut receiver).await?;
        assert_eq!(sequence(delivery.message()), expected_sequence);
        assert_eq!(
            delivery.message().body,
            Body::Data(vec![b"legacy-body".to_vec().into()])
        );
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            Some(MessageId::String("legacy".into()))
        );
        assert_eq!(
            delivery
                .message()
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get("RuleName")),
            name.map(|name| Value::String(name.into())).as_ref()
        );
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
    }
    node.wait_len(&alpha, 0).await?;
    let counters: QueueCounters = codec::decode(
        &node
            .store
            .as_ref()
            .expect("store")
            .get(&keys::queue_counters(&node.namespace, &node.topic))?
            .expect("parent counter"),
    )?;
    assert_eq!(counters.next_sequence, 7);
    timeout(DEADLINE, connection.close()).await??;
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut rules = RulesClient::attach(
        &mut session,
        "reopened-action-rules",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    assert_eq!(action_rules(&rules.list(100, 0).await?), listed);
    for name in ["a-remove", "b-remove", "overlap"] {
        status(&rules.remove(name).await?, 200);
    }
    let remaining = action_rules(&rules.list(100, 0).await?);
    assert_eq!(
        remaining
            .iter()
            .map(|rule| rule.0.as_str())
            .collect::<Vec<_>>(),
        ["$Default"]
    );
    assert!(described(&remaining[0].2, EMPTY_ACTION_CODE).is_empty());
    let set_source =
        " /* wire literal */ SET member=11;SET nullable=TRUE;SET added=23;SET RuleName='ignored'; ";
    for (name, source) in [
        ("a-set", set_source),
        ("b-fail", "REMOVE marker;SET member='bad'"),
    ] {
        status(
            &rules.add_action(name, "member=7", action(source)).await?,
            200,
        );
    }
    let listed = action_rules(&rules.list(100, 0).await?);
    assert_eq!(
        described(&listed[1].2, SQL_ACTION_CODE),
        [Value::String(set_source.into()), Value::Int(20)]
    );
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "literal-publisher", "Orders"),
    )
    .await??;
    accepted(timeout(DEADLINE, publisher.send(original.clone())).await??);
    let shadow = alpha.dead_letter_queue()?;
    assert_eq!(
        node.peek(&alpha)
            .await?
            .iter()
            .map(|copy| copy.sequence.as_u64())
            .collect::<Vec<_>>(),
        [7, 8]
    );
    let dead = node.peek(&shadow).await?;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].sequence.as_u64(), 9);
    assert_eq!(
        dead[0]
            .dead_letter
            .as_ref()
            .expect("conversion details")
            .reason,
        domain::DeadLetterReason::Application("SwitchyardSqlActionError".into())
    );
    assert_eq!(
        dead[0]
            .dead_letter
            .as_ref()
            .expect("conversion details")
            .description,
        "TypeMismatch"
    );
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "literal-private-copies", alpha.as_str()),
    )
    .await??;
    let base = recv(&mut receiver).await?;
    let changed = recv(&mut receiver).await?;
    assert_eq!(sequence(base.message()), 7);
    assert_eq!(sequence(changed.message()), 8);
    for (delivery, is_action) in [(&base, false), (&changed, true)] {
        let mut expected = original.clone();
        let values = &mut expected
            .application_properties
            .as_mut()
            .expect("properties")
            .0;
        if is_action {
            values.insert("member".into(), Value::Int(11));
            values.insert("nullable".into(), Value::Bool(true));
            values.insert("added".into(), Value::Long(23));
            values.insert("RuleName".into(), Value::String("a-set".into()));
        }
        let sorted: std::collections::BTreeMap<_, _> = values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        *values = sorted.into_iter().collect();
        content(delivery.message(), &expected);
    }
    timeout(DEADLINE, receiver.accept(&changed)).await??;
    node.wait_len(&alpha, 1).await?;
    assert_eq!(node.peek(&alpha).await?[0].sequence.as_u64(), 7);
    assert_eq!(node.peek(&shadow).await?.len(), 1);
    timeout(DEADLINE, receiver.accept(&base)).await??;
    node.wait_len(&alpha, 0).await?;
    let mut dead_receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "literal-dead-copies", shadow.as_str()),
    )
    .await??;
    let failure = recv(&mut dead_receiver).await?;
    assert_eq!(sequence(failure.message()), 9);
    assert_eq!(failure.message().body, original.body);
    assert_eq!(failure.message().footer, original.footer);
    let values = failure
        .message()
        .application_properties
        .as_ref()
        .expect("failure properties");
    assert_eq!(values.get("member"), Some(&Value::Int(7)));
    assert_eq!(values.get("marker"), Some(&Value::String("lower".into())));
    assert_eq!(
        values.get("RuleName"),
        Some(&Value::String("b-fail".into()))
    );
    assert_eq!(
        values.get("DeadLetterReason"),
        Some(&Value::String("SwitchyardSqlActionError".into()))
    );
    assert_eq!(
        values.get("DeadLetterErrorDescription"),
        Some(&Value::String("TypeMismatch".into()))
    );
    timeout(DEADLINE, dead_receiver.accept(&failure)).await??;
    node.wait_len(&shadow, 0).await?;
    for name in ["a-set", "b-fail"] {
        status(&rules.remove(name).await?, 200);
    }
    assert_eq!(action_rules(&rules.list(100, 0).await?), remaining);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn unsupported_action_refuses_without_clock_or_snapshot_changes_and_recovers<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut rules = RulesClient::attach(
        &mut session,
        "action-refusals",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    node.clock.set(2_000);
    for (action, expected, error) in [
        (
            action("SET sys.Subject = 'sensitive-source'"),
            501,
            protocol_amqp::NOT_IMPLEMENTED,
        ),
        (action("REMOVE"), 400, protocol_amqp::INVALID_FIELD),
        (
            Value::Map(map([("expression", Value::Int(1))])),
            400,
            protocol_amqp::INVALID_FIELD,
        ),
    ] {
        let before = node.snapshot()?;
        let applied = clock(&node)?;
        let response = rules.add_action("refused", "1=1", action).await?;
        status(&response, expected);
        condition(&response, error);
        assert_eq!(response.body, Body::Value(Value::Null));
        assert!(!format!("{response:?}").contains("sensitive-source"));
        assert_eq!(node.snapshot()?, before);
        assert_eq!(clock(&node)?, applied);
    }
    status(
        &rules
            .add_action("healthy", "1=1", action("REMOVE absent"))
            .await?,
        200,
    );
    assert_eq!(clock(&node)?, Timestamp::from_millis(2_000));
    let listed = action_rules(&rules.list(100, 0).await?);
    assert_eq!(listed.len(), 2);
    assert_eq!(
        described(&listed[1].2, SQL_ACTION_CODE),
        [Value::String("REMOVE absent".into()), Value::Int(20)]
    );
    status(&rules.remove("healthy").await?, 200);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn stale_management_pairs_cannot_create_read_or_delete_replacement_action_rules<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let address = "Orders/subscriptions/Alpha/$management";
    let mut old = RulesClient::attach(&mut session, "old-action-rules", address, 16_384).await?;
    status(
        &old.add_action("old", "1=1", action("REMOVE old")).await?,
        200,
    );
    node.submit(
        &node.topic,
        CommandKind::DeleteEntity {
            target: domain::DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
        },
    )
    .await?;
    node.subscription("Alpha").await?;
    let mut fresh = RulesClient::attach(&mut session, "new-action-rules", address, 16_384).await?;
    status(
        &fresh
            .add_action("replacement", "1=1", action("REMOVE current"))
            .await?,
        200,
    );
    let before = node.snapshot()?;
    let applied = clock(&node)?;
    for response in [
        old.add_action("wrong", "1=1", action("REMOVE current"))
            .await?,
        old.list(100, 0).await?,
        old.remove("replacement").await?,
    ] {
        status(&response, 404);
        condition(&response, protocol_amqp::NOT_FOUND);
        assert_eq!(response.body, Body::Value(Value::Null));
        assert_eq!(node.snapshot()?, before);
        assert_eq!(clock(&node)?, applied);
    }
    let current = action_rules(&fresh.list(100, 0).await?);
    assert_eq!(
        current
            .iter()
            .map(|rule| rule.0.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "replacement"]
    );
    status(&fresh.remove("replacement").await?, 200);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

for_each_backend! {
    create_list_delete_and_three_private_copies_preserve_original_and_legacy_bodies,
    unsupported_action_refuses_without_clock_or_snapshot_changes_and_recovers,
    stale_management_pairs_cannot_create_read_or_delete_replacement_action_rules,
}
