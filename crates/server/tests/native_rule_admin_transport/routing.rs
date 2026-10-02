use amqp::{
    ApplicationProperties, Body, ClientReceiver, ClientSender, ClientSession, Message, Outcome,
    Properties,
};

use super::*;

fn message(color: &str) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some(color.to_owned().into()),
            subject: Some("route".into()),
            ..Default::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("color", color.to_owned())
                .build(),
        )
        .body(Body::Data(vec![color.as_bytes().to_vec().into()]))
        .build()
}

pub(super) async fn round_trip<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let beta = "Orders/subscriptions/Beta";
    for path in [CHILD, beta] {
        timeout(
            DEADLINE,
            rules.delete_rule(request(delete(path, "$Default"), Some(sas("", "manage")))),
        )
        .await??;
    }
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(
                CHILD,
                "Red",
                Filter::SqlFilter(SqlRuleFilter {
                    expression: "color = 'red'".into(),
                    semantic_version: Some(domain::SQL_FILTER_SEMANTIC_VERSION),
                }),
            ),
            Some(sas("", "manage")),
        )),
    )
    .await??;
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(
                beta,
                "Blue",
                Filter::CorrelationFilter(CorrelationRuleFilter {
                    subject: Some("route".into()),
                    properties: vec![CorrelationProperty {
                        name: "color".into(),
                        value: Some(RuleScalarValue {
                            value: Some(Scalar::StringValue("blue".into())),
                        }),
                    }],
                    ..Default::default()
                }),
            ),
            Some(sas("", "manage")),
        )),
    )
    .await??;

    let mut connection = node.connect_amqp().await?;
    let mut session = timeout(DEADLINE, ClientSession::begin(&mut connection)).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "native-rule-publisher", "Orders"),
    )
    .await??;
    for color in ["red", "blue", "green"] {
        assert!(matches!(
            timeout(DEADLINE, sender.send(message(color))).await??,
            Outcome::Accepted(_)
        ));
    }
    let red = node.peek(CHILD).await?;
    let blue = node.peek(beta).await?;
    assert_eq!(red.len(), 1);
    assert_eq!(blue.len(), 1);
    assert_eq!(red[0].message_id, "red");
    assert_eq!(blue[0].message_id, "blue");
    assert_eq!(red[0].body, b"red");
    assert_eq!(blue[0].body, b"blue");
    assert_ne!(red[0].sequence, blue[0].sequence);

    let mut red_receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "native-red", CHILD),
    )
    .await??;
    let mut blue_receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "native-blue", beta),
    )
    .await??;
    for (receiver, color) in [(&mut red_receiver, "red"), (&mut blue_receiver, "blue")] {
        let delivery = timeout(DEADLINE, receiver.recv()).await??;
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            Some(color.to_owned().into())
        );
        assert_eq!(delivery.message().body, message(color).body);
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
    }
    node.wait_empty(CHILD).await?;
    node.wait_empty(beta).await?;
    for (path, name) in [(CHILD, "Red"), (beta, "Blue")] {
        timeout(
            DEADLINE,
            rules.delete_rule(request(delete(path, name), Some(sas("", "manage")))),
        )
        .await??;
        assert!(
            timeout(
                DEADLINE,
                rules.list_rules(request(list(path), Some(sas("", "manage"))))
            )
            .await??
            .into_inner()
            .rules
            .is_empty()
        );
    }
    assert!(matches!(
        timeout(DEADLINE, sender.send(message("red"))).await??,
        Outcome::Accepted(_)
    ));
    assert!(node.peek(CHILD).await?.is_empty());
    assert!(node.peek(beta).await?.is_empty());
    let mut persisted = Vec::new();
    for (path, filter) in [
        (
            CHILD,
            Filter::SqlFilter(SqlRuleFilter {
                expression: "color = 'green'".into(),
                semantic_version: None,
            }),
        ),
        (
            beta,
            Filter::CorrelationFilter(CorrelationRuleFilter {
                subject: Some("persisted".into()),
                properties: vec![CorrelationProperty {
                    name: "binary".into(),
                    value: Some(RuleScalarValue {
                        value: Some(Scalar::BinaryValue(vec![0, 255])),
                    }),
                }],
                ..Default::default()
            }),
        ),
    ] {
        timeout(
            DEADLINE,
            rules.create_rule(request(
                create(path, "Persisted", filter),
                Some(sas("", "manage")),
            )),
        )
        .await??;
        persisted.push(
            timeout(
                DEADLINE,
                rules.get_rule(request(get(path, "Persisted"), Some(sas("", "manage")))),
            )
            .await??
            .into_inner(),
        );
    }
    timeout(DEADLINE, red_receiver.close()).await??;
    timeout(DEADLINE, blue_receiver.close()).await??;
    timeout(DEADLINE, sender.close()).await??;
    timeout(DEADLINE, session.end()).await??;
    timeout(DEADLINE, connection.close()).await??;
    drop(rules);
    drop(entities);
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let (mut entities, mut rules) = node.clients().await?;
    for (path, expected) in [CHILD, beta].into_iter().zip(persisted) {
        let entity = timeout(
            DEADLINE,
            entities.get_entity(request(
                GetEntityRequest {
                    namespace: "tenant".into(),
                    path: path.into(),
                },
                Some(sas("", "manage")),
            )),
        )
        .await??
        .into_inner();
        assert_eq!(entity.kind, EntityKind::Subscription as i32);
        assert_eq!(
            timeout(
                DEADLINE,
                rules.get_rule(request(get(path, "Persisted"), Some(sas("", "manage"))))
            )
            .await??
            .into_inner(),
            expected
        );
        assert_eq!(
            timeout(
                DEADLINE,
                rules.list_rules(request(list(path), Some(sas("", "manage"))))
            )
            .await??
            .into_inner()
            .rules,
            vec![expected]
        );
        assert!(node.peek(path).await?.is_empty());
    }
    Ok(())
}
