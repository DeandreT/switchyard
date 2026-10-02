use super::*;

pub(super) async fn action_crud_is_explicit_clock_free_and_create_only<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    node.topology("Literal/$Management", "Alpha").await?;
    node.create(PATH, "plain", false_filter()).await?;
    assert!(node.get(PATH, "plain").await?.action.is_none());
    let sources = [
        (
            "a-action",
            " /* exact source */ ReMoVe user.[colour]; REMOVE [RuleName]; ",
            None,
        ),
        ("z-action", "\nREMOVE [audit]; REMOVE missing;\n", Some(1)),
    ];
    for (name, source, version) in sources {
        node.create_action(PATH, name, true_filter(), sql_action(source, version))
            .await?;
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    node.clock.manual.set(0);
    assert!(node.get(PATH, "$Default").await?.action.is_none());
    assert_eq!(node.get(PATH, "plain").await?.filter, false_filter());
    for (name, source, _) in sources {
        let error = code(node.get(PATH, name).await, Code::Unimplemented);
        assert_eq!(error.message(), "rule action metadata was not requested");
        assert!(!error.message().contains(source));
        let rule = node.get_actions(PATH, name).await?;
        assert_eq!(rule.namespace, "tenant");
        assert_eq!(rule.subscription_path, PATH);
        assert_eq!(rule.name, name);
        assert_eq!(rule.filter, true_filter());
        assert_eq!(rule.created_at_unix_millis, 1_000);
        assert_eq!(rule.action, sql_action(source, Some(1)));
    }
    let error = code(node.list(PATH).await, Code::Unimplemented);
    assert_eq!(error.message(), "rule action metadata was not requested");
    let listed = node.list_actions(PATH).await?;
    assert_eq!(
        listed
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "a-action", "plain", "z-action"]
    );
    assert!(listed[0].action.is_none() && listed[2].action.is_none());
    assert_eq!(listed[1].action, sql_action(sources[0].1, Some(1)));
    assert_eq!(listed[3].action, sql_action(sources[1].1, Some(1)));
    node.unchanged(&before, writes, clocks)?;
    node.clock.manual.set(2_000);
    code(
        node.create_action(
            PATH,
            "a-action",
            false_filter(),
            sql_action("REMOVE replacement", None),
        )
        .await,
        Code::AlreadyExists,
    );
    code(
        node.create(PATH, "z-action", false_filter()).await,
        Code::AlreadyExists,
    );
    code(
        node.create_action(
            PATH,
            "plain",
            true_filter(),
            sql_action("REMOVE replacement", None),
        )
        .await,
        Code::AlreadyExists,
    );
    assert_eq!(node.list_actions(PATH).await?, listed);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);

    let literal = "Literal/$Management/subscriptions/Alpha";
    node.create_action(
        literal,
        " Literal Action ",
        false_filter(),
        sql_action("REMOVE [audit]", None),
    )
    .await?;
    let literal_rule = node.get_actions(literal, " Literal Action ").await?;
    assert_eq!(literal_rule.subscription_path, literal);
    assert_eq!(literal_rule.name, " Literal Action ");
    assert_eq!(literal_rule.action, sql_action("REMOVE [audit]", Some(1)));
    let before = node.snapshot()?;
    let node = node.reopen()?;
    node.clock.manual.set(0);
    assert_eq!(node.list_actions(PATH).await?, listed);
    assert_eq!(
        node.get_actions(literal, " Literal Action ").await?,
        literal_rule
    );
    node.unchanged(&before, 0, 0)?;
    node.clock.manual.set(3_000);
    node.delete(PATH, "a-action").await?;
    code(node.get_actions(PATH, "a-action").await, Code::NotFound);
    code(node.list(PATH).await, Code::Unimplemented);
    node.delete(PATH, "z-action").await?;
    let remaining = node.list(PATH).await?;
    assert_eq!(
        remaining
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "plain"]
    );
    assert!(remaining.iter().all(|rule| rule.action.is_none()));
    node.delete(literal, " Literal Action ").await?;
    assert_eq!(node.list(literal).await?.len(), 1);
    let before = node.snapshot()?;
    let node = node.reopen()?;
    assert_eq!(node.list(PATH).await?, remaining);
    node.unchanged(&before, 0, 0)?;
    Ok(())
}

pub(super) async fn action_validation_and_combined_limits_precede_owner<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let before = node.snapshot()?;
    let reads = node.reads();
    let writes = node.writes();
    let clocks = node.clocks();
    code(
        node.create_action(PATH, "missing", true_filter(), None)
            .await,
        Code::InvalidArgument,
    );
    code(
        node.create_action(PATH, "missing-filter", None, sql_action("REMOVE x", None))
            .await,
        Code::InvalidArgument,
    );
    for (source, expected) in [
        ("", Code::InvalidArgument),
        ("REMOVE", Code::InvalidArgument),
        ("REMOVE [", Code::InvalidArgument),
        (
            "SET private-secret = 'sensitive-action'",
            Code::Unimplemented,
        ),
        ("REMOVE sys.MessageId", Code::Unimplemented),
    ] {
        let error = code(
            node.create_action(PATH, "invalid", true_filter(), sql_action(source, None))
                .await,
            expected,
        );
        assert!(!error.message().contains("sensitive-action"));
        assert!(!error.message().contains("private-secret"));
    }
    for version in [0, 2, u32::MAX] {
        code(
            node.create_action(
                PATH,
                "version",
                true_filter(),
                sql_action("broken-private-source", Some(version)),
            )
            .await,
            Code::Unimplemented,
        );
    }
    for source in [
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
        "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
        (0..33)
            .map(|index| format!("REMOVE [p{index}]"))
            .collect::<Vec<_>>()
            .join(";"),
    ] {
        let error = code(
            node.create_action(PATH, "limit", true_filter(), sql_action(&source, None))
                .await,
            Code::ResourceExhausted,
        );
        assert!(!error.message().contains(&source));
    }
    let oversized = correlation(CorrelationRuleFilter {
        properties: vec![property(
            "large",
            rule_scalar_value::Value::StringValue("x".repeat(domain::MAX_RULE_BYTES)),
        )],
        ..Default::default()
    });
    code(
        node.create_action(
            PATH,
            "combined",
            oversized,
            sql_action("REMOVE [large]", None),
        )
        .await,
        Code::ResourceExhausted,
    );
    let payload = "x".repeat(domain::MAX_RULE_BYTES - 128);
    let source = format!("/*{}*/REMOVE[large]", "x".repeat(400));
    let mut definition = RuleDefinition {
        name: RuleName::new("combined-action")?,
        filter: domain::RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([("large".into(), MessageValue::String(payload.clone()))]),
            ..Default::default()
        }),
        created_at: Timestamp::UNIX_EPOCH,
        action: None,
    };
    assert!(definition.encoded_size()? <= domain::MAX_RULE_BYTES);
    definition.action = Some(domain::SqlAction::new(source.clone())?);
    assert!(matches!(
        definition.encoded_size(),
        Err(domain::BrokerError::RuleTooLarge { .. })
    ));
    let filter = correlation(CorrelationRuleFilter {
        properties: vec![property(
            "large",
            rule_scalar_value::Value::StringValue(payload),
        )],
        ..Default::default()
    });
    code(
        node.create_action(PATH, "combined-action", filter, sql_action(&source, None))
            .await,
        Code::ResourceExhausted,
    );
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    node.create_action(
        PATH,
        "healthy",
        true_filter(),
        sql_action("REMOVE [missing]", None),
    )
    .await?;
    assert_eq!(
        node.get_actions(PATH, "healthy").await?.action,
        sql_action("REMOVE [missing]", Some(1))
    );
    node.delete(PATH, "healthy").await?;
    Ok(())
}
