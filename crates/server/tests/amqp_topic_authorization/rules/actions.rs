use super::*;

const SQL_ACTION_CODE: u64 = 0x0000013700000006;
const RULE_DESCRIPTION_CODE: u64 = 0x0000013700000004;

fn add_action(name: &str, source: &str) -> OrderedMap<Value, Value> {
    map([
        ("rule-name", Value::String(name.into())),
        (
            "rule-description",
            Value::Map(map([
                ("rule-name", Value::String(name.into())),
                (
                    "sql-filter",
                    Value::Map(map([(
                        "expression",
                        Value::String("sys.MessageId IS NOT NULL".into()),
                    )])),
                ),
                (
                    "sql-rule-action",
                    Value::Map(map([("expression", Value::String(source.into()))])),
                ),
            ])),
        ),
    ])
}

fn list_body() -> OrderedMap<Value, Value> {
    map([("top", Value::Int(100)), ("skip", Value::Int(0))])
}

fn remove_body(name: &str) -> OrderedMap<Value, Value> {
    map([("rule-name", Value::String(name.into()))])
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

fn listed_action(response: &Message, expected_name: &str, source: &str) {
    status(response, 200);
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("enumeration body")
    };
    let Some(Value::List(rules)) = body.get(&Value::String("rules".into())) else {
        panic!("rule list")
    };
    let action = rules
        .iter()
        .find_map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("rule entry")
            };
            let Some(Value::Described(description)) =
                entry.get(&Value::String("rule-description".into()))
            else {
                panic!("description")
            };
            assert_eq!(
                description.descriptor,
                amqp::Descriptor::Code(RULE_DESCRIPTION_CODE)
            );
            let Value::List(fields) = &description.value else {
                panic!("description fields")
            };
            let [_, action, Value::String(name), Value::Timestamp(created)] = fields.as_slice()
            else {
                panic!("rule fields")
            };
            assert_eq!(created.milliseconds(), 1_000);
            (name == expected_name).then_some(action)
        })
        .expect("action rule enumerated");
    let Value::Described(action) = action else {
        panic!("typed SQL action")
    };
    assert_eq!(action.descriptor, amqp::Descriptor::Code(SQL_ACTION_CODE));
    assert_eq!(
        action.value,
        Value::List(vec![Value::String(source.into()), Value::Int(20)])
    );
}

impl RuleClient {
    async fn attach_named(session: &mut Session, name: &str, address: &str) -> TestResult<Self> {
        let reply_to = format!("{name}-replies");
        let receiver = timeout(
            DEADLINE,
            Receiver::builder()
                .name(format!("{name}-responses"))
                .source(address)
                .target(reply_to.clone())
                .attach(session),
        )
        .await??;
        let sender = timeout(
            DEADLINE,
            Sender::attach(session, format!("{name}-requests"), address),
        )
        .await??;
        Ok(Self {
            sender,
            receiver,
            reply_to,
        })
    }
}

fn request_to_reply(body: OrderedMap<Value, Value>, reply_to: &str) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some("foreign-action-reply".into()),
            reply_to: Some(reply_to.into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::ADD_RULE_OPERATION,
                )
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build()
}

async fn exact_listen_grant_creates_lists_and_deletes_actions_on_the_canonical_child<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::LISTEN,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/Subscriptions/Alpha/$Management")
        .await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/SUBSCRIPTIONS/Alpha/$MANAGEMENT").await?;
    let source = "  ReMoVe user.[marker]; REMOVE missing; ";
    node.clear();
    status(
        &client
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                add_action("scoped-action", source),
                Some("foreign-associated-name"),
            )
            .await?,
        200,
    );
    let definitions = node.rules("Alpha")?;
    let stored = definitions
        .iter()
        .find(|rule| rule.name.as_str() == "scoped-action")
        .expect("stored action");
    assert_eq!(
        stored.action.as_ref().expect("action source").expression(),
        source
    );
    assert_eq!(
        stored
            .action
            .as_ref()
            .expect("action version")
            .semantic_version(),
        1
    );
    let before = node.store.snapshot()?;
    listed_action(
        &client
            .request(protocol_amqp::ENUMERATE_RULES_OPERATION, list_body(), None)
            .await?,
        "scoped-action",
        source,
    );
    assert_eq!(node.store.snapshot()?, before);
    status(
        &client
            .request(
                protocol_amqp::REMOVE_RULE_OPERATION,
                remove_body("scoped-action"),
                None,
            )
            .await?,
        200,
    );
    {
        let calls = node.calls.lock().expect("calls");
        assert_eq!(calls.metadata, 0);
        assert_eq!(
            calls.rules,
            [(EntityPath::new("Orders")?, SubscriptionName::new("Alpha")?)]
        );
        assert!(matches!(&calls.submits[..], [
            (entity, CommandKind::CreateRuleWithAction { subscription, action, .. }),
            (_, CommandKind::DeleteRule { .. }),
        ] if entity.as_str() == "Orders" && subscription.as_str() == "Alpha" && action.expression() == source));
    }
    assert_eq!(node.rules("Alpha")?.len(), 1);
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn send_only_grant_denies_action_and_rule_operations_before_parsing_or_owner_calls<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::SEND,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/subscriptions/Alpha/$management")
        .await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/subscriptions/Alpha/$management").await?;
    node.clear();
    let before = node.store.snapshot()?;
    let clock = StateMachine::new(node.store.clone()).last_applied_time()?;
    for (operation, body) in [
        (
            protocol_amqp::ADD_RULE_OPERATION,
            add_action("denied", "REMOVE missing"),
        ),
        (
            protocol_amqp::ADD_RULE_OPERATION,
            add_action("unsupported", "SET secret = 'not-visible'"),
        ),
        (protocol_amqp::ENUMERATE_RULES_OPERATION, list_body()),
        (
            protocol_amqp::REMOVE_RULE_OPERATION,
            remove_body("$Default"),
        ),
    ] {
        let response = client
            .request(operation, body, Some("foreign-associated-link"))
            .await?;
        status(&response, 401);
        condition(&response, "amqp:unauthorized-access");
        assert_eq!(response.body, Body::Value(Value::Null));
        assert!(!format!("{response:?}").contains("not-visible"));
        node.no_calls();
        assert_eq!(node.store.snapshot()?, before);
        assert_eq!(
            StateMachine::new(node.store.clone()).last_applied_time()?,
            clock
        );
    }
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn wrong_scope_and_literal_case_refuse_management_links_before_topology<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::LISTEN,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/subscriptions/Alpha/$management")
        .await?;
    let before = node.store.snapshot()?;
    for (index, address) in [
        "Orders/subscriptions/Billing/$management",
        "Orders/subscriptions/alpha/$management",
        "orders/subscriptions/Alpha/$management",
        "Orders/subscriptions/Missing/$management",
    ]
    .into_iter()
    .enumerate()
    {
        let mut session = timeout(DEADLINE, connection.begin()).await??;
        node.clear();
        match timeout(
            DEADLINE,
            Receiver::builder()
                .name(format!("denied-action-response-{index}"))
                .source(address)
                .target(format!("denied-action-reply-{index}"))
                .attach(&mut session),
        )
        .await?
        {
            Err(error) => assert!(matches!(
                error,
                amqp::EngineError::RemoteDetached | amqp::EngineError::RemoteClosed
            )),
            Ok(mut receiver) => {
                let error = timeout(DEADLINE, receiver.recv())
                    .await?
                    .expect_err("unauthorized receiver was usable");
                assert!(matches!(
                    error,
                    amqp::EngineError::RemoteDetached | amqp::EngineError::RemoteClosed
                ));
            }
        }
        node.no_calls();
        assert_eq!(node.store.snapshot()?, before);
        let _ = timeout(DEADLINE, session.end()).await?;
    }
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut healthy =
        RuleClient::attach(&mut session, "Orders/subscriptions/Alpha/$management").await?;
    status(
        &healthy
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                add_action("healthy", "REMOVE absent"),
                None,
            )
            .await?,
        200,
    );
    listed_action(
        &healthy
            .request(protocol_amqp::ENUMERATE_RULES_OPERATION, list_body(), None)
            .await?,
        "healthy",
        "REMOVE absent",
    );
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn foreign_live_links_and_old_reply_incarnations_cannot_redirect_action_work<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(provider, "", PermissionSet::LISTEN).await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "").await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let address = "Orders/subscriptions/Alpha/$management";
    let mut old = RuleClient::attach_named(&mut session, "alpha-action", address).await?;
    let mut foreign = RuleClient::attach_named(
        &mut session,
        "billing-action",
        "Orders/subscriptions/Billing/$management",
    )
    .await?;
    node.clear();
    status(
        &old.request(
            protocol_amqp::ADD_RULE_OPERATION,
            add_action("canonical", "REMOVE missing"),
            Some("billing-action-responses"),
        )
        .await?,
        200,
    );
    assert_eq!(node.rules("Alpha")?.len(), 2);
    assert_eq!(node.rules("Billing")?.len(), 1);
    {
        let calls = node.calls.lock().expect("calls");
        assert!(
            matches!(&calls.submits[..], [(entity, CommandKind::CreateRuleWithAction { subscription, .. })] if entity.as_str() == "Orders" && subscription.as_str() == "Alpha")
        );
    }
    node.clear();
    let before = node.store.snapshot()?;
    let Outcome::Rejected(rejected) = timeout(
        DEADLINE,
        old.sender.send(request_to_reply(
            add_action("foreign", "REMOVE missing"),
            &foreign.reply_to,
        )),
    )
    .await??
    else {
        panic!("foreign reply route accepted")
    };
    assert_eq!(
        rejected.error.expect("binding error").condition.as_symbol(),
        Symbol::from(protocol_amqp::NOT_FOUND)
    );
    node.no_calls();
    assert_eq!(node.store.snapshot()?, before);
    status(
        &foreign
            .request(protocol_amqp::ENUMERATE_RULES_OPERATION, list_body(), None)
            .await?,
        200,
    );
    node.broker.handle().submit_blocking(
        NamespaceName::new("tenant")?,
        EntityPath::new("Orders")?,
        CommandKind::DeleteEntity {
            target: domain::DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
        },
    )?;
    node.broker.handle().submit_blocking(
        NamespaceName::new("tenant")?,
        EntityPath::new("Orders")?,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("Alpha")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    let mut fresh = RuleClient::attach_named(&mut session, "fresh-alpha-action", address).await?;
    node.clear();
    let before = node.store.snapshot()?;
    let clock = StateMachine::new(node.store.clone()).last_applied_time()?;
    let Outcome::Rejected(rejected) = timeout(
        DEADLINE,
        fresh.sender.send(request_to_reply(
            add_action("stale-reply", "REMOVE missing"),
            &old.reply_to,
        )),
    )
    .await??
    else {
        panic!("old incarnation reply route accepted")
    };
    assert_eq!(
        rejected
            .error
            .expect("stale route error")
            .condition
            .as_symbol(),
        Symbol::from(protocol_amqp::NOT_FOUND)
    );
    node.no_calls();
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(
        StateMachine::new(node.store.clone()).last_applied_time()?,
        clock
    );
    status(
        &fresh
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                add_action("replacement", "REMOVE current"),
                Some("billing-action-responses"),
            )
            .await?,
        200,
    );
    listed_action(
        &fresh
            .request(protocol_amqp::ENUMERATE_RULES_OPERATION, list_body(), None)
            .await?,
        "replacement",
        "REMOVE current",
    );
    assert_eq!(
        node.rules("Alpha")?
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "replacement"]
    );
    assert_eq!(node.rules("Billing")?.len(), 1);
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

for_each_backend! {
    exact_listen_grant_creates_lists_and_deletes_actions_on_the_canonical_child,
    send_only_grant_denies_action_and_rule_operations_before_parsing_or_owner_calls,
    wrong_scope_and_literal_case_refuse_management_links_before_topology,
    foreign_live_links_and_old_reply_incarnations_cannot_redirect_action_work,
}
