use amqp::{
    ApplicationProperties, Body, ClientReceiver, ClientSender, ClientSession, Message, Outcome,
    Properties, Value,
};

use super::{
    actions::{create_action, list_actions},
    *,
};

fn message() -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some("native-action-message".into()),
            subject: Some("preserved subject".into()),
            content_type: Some("application/octet-stream".into()),
            ..Default::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("member", Value::Int(7))
                .insert("nullable", Value::Null)
                .insert("RuleName", "producer-name")
                .insert("audit", "lower")
                .insert("Audit", "capital")
                .build(),
        )
        .body(Body::Data(vec![vec![0, 255, 7].into()]))
        .build()
}

fn expected(original: &Message, name: Option<&str>) -> Message {
    let mut expected = original.clone();
    let properties = &mut expected.application_properties.as_mut().unwrap().0;
    match name {
        Some("a-remove") => {
            properties.shift_remove("member");
            properties.shift_remove("RuleName");
            properties.shift_remove("audit");
        }
        Some("b-remove") => {
            properties.shift_remove("nullable");
        }
        None => {}
        Some(_) => unreachable!("known native rule names"),
    }
    if let Some(name) = name {
        properties.insert("RuleName".into(), Value::String(name.into()));
    }
    // Stored application properties use canonical key order, not producer order.
    let mut ordered: Vec<_> = properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    *properties = ordered.into_iter().collect();
    expected
}

pub(super) async fn round_trip<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(
                CHILD,
                "overlap",
                Filter::SqlFilter(SqlRuleFilter {
                    expression: "member = 7".into(),
                    semantic_version: None,
                }),
            ),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    let sources = [
        (
            "a-remove",
            " /* exact */ REMOVE member; REMOVE [RuleName]; REMOVE audit; ",
        ),
        ("b-remove", "REMOVE nullable; REMOVE missing"),
    ];
    for (name, source) in sources {
        let filter = if name == "a-remove" {
            Filter::SqlFilter(SqlRuleFilter {
                expression: "member = 7".into(),
                semantic_version: None,
            })
        } else {
            Filter::CorrelationFilter(CorrelationRuleFilter {
                properties: vec![CorrelationProperty {
                    name: "member".into(),
                    value: Some(RuleScalarValue {
                        value: Some(Scalar::IntValue(7)),
                    }),
                }],
                ..Default::default()
            })
        };
        timeout(
            DEADLINE,
            rules.create_rule_with_action(request(
                create_action(CHILD, name, filter, source, None),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await??;
    }
    let persisted = timeout(
        DEADLINE,
        rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage")))),
    )
    .await??
    .into_inner();
    assert_eq!(
        persisted
            .rules
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "a-remove", "b-remove", "overlap"]
    );
    for (name, source) in sources {
        assert_eq!(
            persisted
                .rules
                .iter()
                .find(|rule| rule.name == name)
                .unwrap()
                .action,
            Some(SqlRuleAction {
                expression: source.into(),
                semantic_version: Some(1)
            })
        );
    }
    let mut connection = node.connect_amqp().await?;
    let mut session = timeout(DEADLINE, ClientSession::begin(&mut connection)).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "native-action-publisher", "Orders"),
    )
    .await??;
    let original = message();
    assert!(matches!(
        timeout(DEADLINE, sender.send(original.clone())).await??,
        Outcome::Accepted(_)
    ));
    let copies = node.peek(CHILD).await?;
    assert_eq!(
        copies
            .iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        copies
            .iter()
            .map(|delivery| delivery.body.as_slice())
            .collect::<Vec<_>>(),
        [b"\0\xff\x07".as_slice(); 3]
    );
    let beta = "Orders/subscriptions/Beta";
    let untouched = node.peek(beta).await?;
    assert_eq!(untouched.len(), 1);
    assert_eq!(untouched[0].sequence.as_u64(), 1);
    assert_eq!(
        untouched[0]
            .envelope
            .as_ref()
            .unwrap()
            .application_properties
            .get("member"),
        Some(&domain::MessageValue::Int(7))
    );
    assert_eq!(
        untouched[0]
            .envelope
            .as_ref()
            .unwrap()
            .application_properties
            .get("RuleName"),
        Some(&domain::MessageValue::String("producer-name".into()))
    );

    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "native-action-base", CHILD),
    )
    .await??;
    let base = timeout(DEADLINE, receiver.recv()).await??;
    let first = timeout(DEADLINE, receiver.recv()).await??;
    let second = timeout(DEADLINE, receiver.recv()).await??;
    for (delivery, name) in [
        (&base, None),
        (&first, Some("a-remove")),
        (&second, Some("b-remove")),
    ] {
        let expected = expected(&original, name);
        assert_eq!(
            delivery.message().application_properties,
            expected.application_properties
        );
        assert_eq!(delivery.message().properties, expected.properties);
        assert_eq!(delivery.message().body, original.body);
        assert_eq!(delivery.message().footer, original.footer);
    }
    // Settle the second transported copy before the first and third.
    timeout(DEADLINE, receiver.accept(&first)).await??;
    timeout(DEADLINE, async {
        loop {
            let remaining = node.peek(CHILD).await?;
            if remaining.len() == 2 {
                assert_eq!(
                    remaining
                        .iter()
                        .map(|delivery| delivery.sequence.as_u64())
                        .collect::<Vec<_>>(),
                    [1, 3]
                );
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(
        base.message().application_properties,
        expected(&original, None).application_properties
    );
    assert_eq!(
        second.message().application_properties,
        expected(&original, Some("b-remove")).application_properties
    );
    timeout(DEADLINE, receiver.accept(&base)).await??;
    timeout(DEADLINE, receiver.accept(&second)).await??;
    node.wait_empty(CHILD).await?;
    timeout(DEADLINE, receiver.close()).await??;
    timeout(DEADLINE, sender.close()).await??;
    timeout(DEADLINE, session.end()).await??;
    timeout(DEADLINE, connection.close()).await??;
    drop(rules);
    drop(entities);
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let (_, mut rules) = node.clients().await?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner(),
        persisted
    );
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await?
        .expect_err("reopened actions still require opt-in")
        .code(),
        Code::Unimplemented
    );
    let mut connection = node.connect_amqp().await?;
    let mut session = timeout(DEADLINE, ClientSession::begin(&mut connection)).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "reopened-action-publisher", "Orders"),
    )
    .await??;
    assert!(matches!(
        timeout(DEADLINE, sender.send(original)).await??,
        Outcome::Accepted(_)
    ));
    assert_eq!(
        node.peek(CHILD)
            .await?
            .iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [4, 5, 6]
    );
    timeout(DEADLINE, sender.close()).await??;
    timeout(DEADLINE, session.end()).await??;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}
