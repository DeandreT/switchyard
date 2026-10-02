use super::*;

pub(super) async fn exact_scope<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let before = node.snapshot()?;
    for token in [
        None,
        Some("not-a-token".into()),
        Some(sas("", "manage").replace("sig=", "sig=forged")),
    ] {
        let error = timeout(DEADLINE, rules.list_rules(request(list(CHILD), token)))
            .await?
            .expect_err("a valid management token is required");
        assert_eq!(error.code(), Code::Unauthenticated);
        assert!(!error.message().contains(KEY));
    }
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas("", "send"))))
        )
        .await?
        .expect_err("Send cannot administer rules")
        .code(),
        Code::PermissionDenied
    );
    for path in [
        "Orders/subscriptions/Beta",
        "orders/subscriptions/Alpha",
        "Orders/subscriptions/alpha",
    ] {
        assert_eq!(
            timeout(
                DEADLINE,
                rules.get_rule(request(get(path, "$Default"), Some(sas(CHILD, "manage"))))
            )
            .await?
            .expect_err("an exact child token cannot discover other children")
            .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            timeout(
                DEADLINE,
                rules.create_rule(request(
                    create(path, "Denied", Filter::TrueFilter(TrueRuleFilter {})),
                    Some(sas(CHILD, "manage"))
                ))
            )
            .await?
            .expect_err("an exact child token cannot create on another child")
            .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            timeout(
                DEADLINE,
                rules.delete_rule(request(
                    delete(path, "$Default"),
                    Some(sas(CHILD, "manage"))
                ))
            )
            .await?
            .expect_err("an exact child token cannot delete on another child")
            .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(
                list(CHILD),
                Some(sas(&format!("{CHILD}/$management"), "manage"))
            ))
        )
        .await?
        .expect_err("control endpoint scope does not administer the base child")
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(node.snapshot()?, before, "refusals must not mutate rules");
    let response = timeout(
        DEADLINE,
        rules.list_rules(request(
            list("Orders/SUBSCRIPTIONS/Alpha"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].subscription_path, CHILD);
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(CHILD, "Allowed", Filter::TrueFilter(TrueRuleFilter {})),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    timeout(
        DEADLINE,
        rules.delete_rule(request(
            delete(CHILD, "Allowed"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    let literal_parent = "Orders/$Management";
    let literal_child = "Orders/$Management/subscriptions/Subscriptions";
    for (path, kind) in [
        (literal_parent, EntityKind::Topic),
        (literal_child, EntityKind::Subscription),
    ] {
        timeout(
            DEADLINE,
            entities.create_entity(request(
                CreateEntityRequest {
                    namespace: "tenant".into(),
                    path: path.into(),
                    kind: kind as i32,
                    ..Default::default()
                },
                Some(sas("", "manage")),
            )),
        )
        .await??;
    }
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(
                literal_child,
                "Literal",
                Filter::FalseFilter(FalseRuleFilter {}),
            ),
            Some(sas(literal_child, "manage")),
        )),
    )
    .await??;
    let literal = timeout(
        DEADLINE,
        rules.get_rule(request(
            get(literal_child, "Literal"),
            Some(sas(literal_child, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(literal.subscription_path, literal_child);
    assert_eq!(literal.name, "Literal");
    assert_eq!(
        literal.filter,
        Some(RuleFilter {
            filter: Some(Filter::FalseFilter(FalseRuleFilter {}))
        })
    );
    let listed = timeout(
        DEADLINE,
        rules.list_rules(request(
            list(literal_child),
            Some(sas(literal_child, "manage")),
        )),
    )
    .await??
    .into_inner()
    .rules;
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[1], literal);
    assert!(
        listed
            .iter()
            .all(|rule| rule.subscription_path == literal_child)
    );
    let before = node.snapshot()?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(
                list(literal_child),
                Some(sas(
                    "Orders/$management/subscriptions/Subscriptions",
                    "manage"
                ))
            ))
        )
        .await?
        .expect_err("a normalized control-name grant cannot alias a literal native name")
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(
                list("/Orders/$Management/subscriptions/Subscriptions"),
                Some(sas("", "manage"))
            ))
        )
        .await?
        .expect_err("authenticated resource scopes cannot represent a leading empty segment")
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(node.snapshot()?, before);
    timeout(
        DEADLINE,
        rules.delete_rule(request(
            delete(literal_child, "Literal"),
            Some(sas(literal_child, "manage")),
        )),
    )
    .await??;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(
                list(literal_child),
                Some(sas(literal_child, "manage"))
            ))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        1
    );
    Ok(())
}

pub(super) async fn limits<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let before = node.snapshot()?;
    let oversized = create(
        CHILD,
        "Oversized",
        Filter::CorrelationFilter(CorrelationRuleFilter {
            properties: vec![CorrelationProperty {
                name: "large".into(),
                value: Some(RuleScalarValue {
                    value: Some(Scalar::BinaryValue(vec![
                        7;
                        server::NATIVE_ADMIN_REQUEST_LIMIT
                            + 1
                    ])),
                }),
            }],
            ..Default::default()
        }),
    );
    assert!(oversized.encoded_len() > server::NATIVE_ADMIN_REQUEST_LIMIT);
    let error = timeout(
        DEADLINE,
        rules.create_rule(request(oversized, Some(sas("", "manage")))),
    )
    .await?
    .expect_err("the shared native transport request cap is enforced");
    assert_eq!(error.code(), Code::OutOfRange);
    assert!(!error.message().contains(KEY));
    assert_eq!(node.snapshot()?, before);
    let channel = node.channel().await?;
    let mut small_response = RuleServiceClient::new(channel).max_decoding_message_size(1);
    let error = timeout(
        DEADLINE,
        small_response.list_rules(request(list(CHILD), Some(sas("", "manage")))),
    )
    .await?
    .expect_err("the client applies its deliberately smaller response cap");
    assert_eq!(error.code(), Code::OutOfRange);
    timeout(
        DEADLINE,
        rules.create_rule(request(
            create(CHILD, "Healthy", Filter::TrueFilter(TrueRuleFilter {})),
            Some(sas("", "manage")),
        )),
    )
    .await??;
    for index in 0..30 {
        timeout(
            DEADLINE,
            rules.create_rule(request(
                create(
                    CHILD,
                    &format!("Bounded{index:02}"),
                    Filter::FalseFilter(FalseRuleFilter {}),
                ),
                Some(sas("", "manage")),
            )),
        )
        .await??;
    }
    let full = node.snapshot()?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.create_rule(request(
                create(CHILD, "TooMany", Filter::TrueFilter(TrueRuleFilter {})),
                Some(sas("", "manage"))
            ))
        )
        .await?
        .expect_err("a complete rule set is bounded at 32")
        .code(),
        Code::ResourceExhausted
    );
    assert_eq!(node.snapshot()?, full);
    let response = timeout(
        DEADLINE,
        rules.list_rules(request(list(CHILD), Some(sas("", "manage")))),
    )
    .await??
    .into_inner();
    assert_eq!(response.rules.len(), domain::MAX_SUBSCRIPTION_RULES);
    assert!(response.encoded_len() <= server::NATIVE_ADMIN_RESPONSE_LIMIT);
    let entity = timeout(
        DEADLINE,
        entities.get_entity(request(
            GetEntityRequest {
                namespace: "tenant".into(),
                path: "Orders".into(),
            },
            Some(sas("", "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(entity.kind, EntityKind::Topic as i32);
    Ok(())
}
