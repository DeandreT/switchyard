use super::*;

pub(super) fn create_action(
    path: &str,
    name: &str,
    filter: Filter,
    expression: &str,
    semantic_version: Option<u32>,
) -> CreateRuleWithActionRequest {
    CreateRuleWithActionRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
        filter: Some(RuleFilter {
            filter: Some(filter),
        }),
        action: Some(SqlRuleAction {
            expression: expression.into(),
            semantic_version,
        }),
    }
}

pub(super) fn get_actions(path: &str, name: &str) -> GetRuleRequest {
    GetRuleRequest {
        include_actions: true,
        ..get(path, name)
    }
}

pub(super) fn list_actions(path: &str) -> ListRulesRequest {
    ListRulesRequest {
        include_actions: true,
        ..list(path)
    }
}

pub(super) async fn round_trip<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let source = " /* preserve source */ REMOVE user.[audit]; REMOVE missing; ";
    let filters = [
        ("ActionTrue", Filter::TrueFilter(TrueRuleFilter {})),
        ("ActionFalse", Filter::FalseFilter(FalseRuleFilter {})),
        (
            "ActionCorrelation",
            Filter::CorrelationFilter(crud::all_scalars()),
        ),
        (
            "ActionSql",
            Filter::SqlFilter(SqlRuleFilter {
                expression: "member = 7".into(),
                semantic_version: None,
            }),
        ),
    ];
    let plain = timeout(
        DEADLINE,
        rules.get_rule(request(get(CHILD, "$Default"), Some(sas(CHILD, "manage")))),
    )
    .await??
    .into_inner();
    assert!(plain.action.is_none());
    let mut expected = vec![plain.clone()];
    for (index, (name, filter)) in filters.into_iter().enumerate() {
        timeout(
            DEADLINE,
            rules.create_rule_with_action(request(
                create_action(
                    "Orders/SUBSCRIPTIONS/Alpha",
                    name,
                    filter.clone(),
                    source,
                    (index % 2 == 0).then_some(1),
                ),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await??;
        let output = timeout(
            DEADLINE,
            rules.get_rule(request(
                get_actions(CHILD, name),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await??
        .into_inner();
        assert_eq!(output.namespace, "tenant");
        assert_eq!(output.subscription_path, CHILD);
        assert_eq!(output.name, name);
        assert_eq!(output.created_at_unix_millis, 10_000);
        assert_eq!(
            output.action,
            Some(SqlRuleAction {
                expression: source.into(),
                semantic_version: Some(if index % 2 == 0 { 1 } else { 2 })
            })
        );
        let filter = match filter {
            Filter::SqlFilter(filter) => Filter::SqlFilter(SqlRuleFilter {
                semantic_version: Some(1),
                ..filter
            }),
            filter => filter,
        };
        assert_eq!(
            output.filter,
            Some(RuleFilter {
                filter: Some(filter)
            })
        );
        expected.push(output);
    }
    expected.sort_by(|left, right| left.name.cmp(&right.name));
    node.clock.set(0);
    let before = node.snapshot()?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.get_rule(request(get(CHILD, "$Default"), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner(),
        plain
    );
    for error in [
        timeout(
            DEADLINE,
            rules.get_rule(request(
                get(CHILD, "ActionTrue"),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await?
        .expect_err("action read requires opt-in"),
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas(CHILD, "manage")))),
        )
        .await?
        .expect_err("default list cannot hide action rules"),
    ] {
        assert_eq!(error.code(), Code::Unimplemented);
        assert!(!error.message().contains(source));
    }
    let response = timeout(
        DEADLINE,
        rules.list_rules(request(
            list_actions("Orders/Subscriptions/Alpha"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(response.rules, expected);
    assert!(response.encoded_len() <= server::NATIVE_ADMIN_RESPONSE_LIMIT);
    assert_eq!(node.snapshot()?, before, "all read modes are clock-free");
    drop(rules);
    drop(entities);
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let (_, mut rules) = node.clients().await?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules,
        expected
    );
    node.clock.set(10_001);
    let before = node.snapshot()?;
    let error = timeout(
        DEADLINE,
        rules.create_rule_with_action(request(
            create_action(
                CHILD,
                "ActionTrue",
                Filter::FalseFilter(FalseRuleFilter {}),
                "REMOVE other",
                None,
            ),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await?
    .expect_err("creation cannot replace an action");
    assert_eq!(error.code(), Code::AlreadyExists);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(
        timeout(
            DEADLINE,
            rules.get_rule(request(
                get_actions(CHILD, "actiontrue"),
                Some(sas(CHILD, "manage"))
            ))
        )
        .await?
        .expect_err("rule name case is exact")
        .code(),
        Code::NotFound
    );
    for rule in expected.iter().filter(|rule| rule.action.is_some()) {
        timeout(
            DEADLINE,
            rules.delete_rule(request(
                delete(CHILD, &rule.name),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await??;
    }
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules,
        vec![plain]
    );
    timeout(
        DEADLINE,
        rules.delete_rule(request(
            delete(CHILD, "$Default"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    assert!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .is_empty()
    );
    let literal = " /* wire */ SET member=11;SET nullable=TRUE;SET added='literal'; ";
    timeout(
        DEADLINE,
        rules.create_rule_with_action(request(
            create_action(
                CHILD,
                "LiteralSet",
                Filter::TrueFilter(TrueRuleFilter {}),
                literal,
                None,
            ),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    let output = timeout(
        DEADLINE,
        rules.get_rule(request(
            get_actions(CHILD, "LiteralSet"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(
        output.action,
        Some(SqlRuleAction {
            expression: literal.into(),
            semantic_version: Some(2)
        })
    );
    let before = node.snapshot()?;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.create_rule_with_action(request(
                create_action(
                    CHILD,
                    "V1Literal",
                    Filter::TrueFilter(TrueRuleFilter {}),
                    literal,
                    Some(1)
                ),
                Some(sas(CHILD, "manage"))
            ))
        )
        .await?
        .expect_err("explicit v1 remains REMOVE-only")
        .code(),
        Code::Unimplemented
    );
    assert_eq!(node.snapshot()?, before);
    timeout(
        DEADLINE,
        rules.delete_rule(request(
            delete(CHILD, "LiteralSet"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    assert!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .is_empty()
    );
    Ok(())
}

pub(super) async fn refusals<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let valid = create_action(
        CHILD,
        "Refused",
        Filter::TrueFilter(TrueRuleFilter {}),
        "REMOVE audit",
        None,
    );
    let cases = [
        (
            CreateRuleWithActionRequest {
                action: None,
                ..valid.clone()
            },
            Code::InvalidArgument,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "",
                None,
            ),
            Code::InvalidArgument,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "REMOVE [private-source",
                None,
            ),
            Code::InvalidArgument,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "SET sys.Subject = 7",
                None,
            ),
            Code::Unimplemented,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "REMOVE sys.Subject",
                None,
            ),
            Code::Unimplemented,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "invalid private-source",
                Some(0),
            ),
            Code::Unimplemented,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                "invalid private-source",
                Some(3),
            ),
            Code::Unimplemented,
        ),
        (
            create_action(
                CHILD,
                "Refused",
                Filter::TrueFilter(TrueRuleFilter {}),
                &"x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
                None,
            ),
            Code::ResourceExhausted,
        ),
    ];
    let before = node.snapshot()?;
    node.clock.set(20_000);
    for (input, expected) in cases {
        let error = timeout(
            DEADLINE,
            rules.create_rule_with_action(request(input, Some(sas(CHILD, "manage")))),
        )
        .await?
        .expect_err("invalid action must not mutate the store");
        assert_eq!(error.code(), expected);
        assert!(!error.message().contains("private-source"));
        assert!(!error.message().contains("private_source"));
        assert!(!error.message().contains(KEY));
        assert_eq!(node.snapshot()?, before);
    }
    let mut oversized = valid.clone();
    oversized.action.as_mut().unwrap().expression =
        "x".repeat(server::NATIVE_ADMIN_REQUEST_LIMIT + 1);
    assert!(oversized.encoded_len() > server::NATIVE_ADMIN_REQUEST_LIMIT);
    assert_eq!(
        timeout(
            DEADLINE,
            rules.create_rule_with_action(request(oversized, Some(sas(CHILD, "manage"))))
        )
        .await?
        .expect_err("native request cap includes the action")
        .code(),
        Code::OutOfRange
    );
    assert_eq!(node.snapshot()?, before);
    timeout(
        DEADLINE,
        rules.create_rule_with_action(request(valid, Some(sas(CHILD, "manage")))),
    )
    .await??;
    let healthy = timeout(
        DEADLINE,
        rules.get_rule(request(
            get_actions(CHILD, "Refused"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(healthy.created_at_unix_millis, 20_000);
    assert_eq!(healthy.action.unwrap().expression, "REMOVE audit");
    let mut small = RuleServiceClient::new(node.channel().await?).max_decoding_message_size(1);
    assert_eq!(
        timeout(
            DEADLINE,
            small.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await?
        .expect_err("client response cap applies to action-aware lists")
        .code(),
        Code::OutOfRange
    );
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        2
    );
    Ok(())
}
