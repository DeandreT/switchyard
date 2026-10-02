use super::*;

pub(super) fn all_scalars() -> CorrelationRuleFilter {
    let values = vec![
        Scalar::NullValue(RuleNullValue {}),
        Scalar::BoolValue(true),
        Scalar::UbyteValue(255),
        Scalar::UshortValue(65_535),
        Scalar::UintValue(u32::MAX),
        Scalar::UlongValue(u64::MAX),
        Scalar::ByteValue(-128),
        Scalar::ShortValue(-32_768),
        Scalar::IntValue(i32::MIN),
        Scalar::LongValue(i64::MIN),
        Scalar::FloatBits(1.5_f32.to_bits()),
        Scalar::DoubleBits((-2.25_f64).to_bits()),
        Scalar::Decimal32Bytes(vec![0, 1, 2, 3]),
        Scalar::Decimal64Bytes((0..8).collect()),
        Scalar::Decimal128Bytes((0..16).collect()),
        Scalar::CharCodepoint(0x3bb),
        Scalar::TimestampMillis(-1_234),
        Scalar::UuidBytes((0..16).collect()),
        Scalar::BinaryValue(vec![0, 255, 1]),
        Scalar::StringValue("text".into()),
        Scalar::SymbolValue("ascii-symbol".into()),
    ];
    CorrelationRuleFilter {
        correlation_id: Some("correlation".into()),
        message_id: Some("message".into()),
        to: Some("destination".into()),
        reply_to: Some("reply".into()),
        subject: Some("subject".into()),
        session_id: Some("session".into()),
        reply_to_session_id: Some("reply-session".into()),
        content_type: Some("text/plain".into()),
        properties: values
            .into_iter()
            .enumerate()
            .map(|(index, value)| CorrelationProperty {
                name: format!("p{index:02}"),
                value: Some(RuleScalarValue { value: Some(value) }),
            })
            .collect(),
    }
}

pub(super) async fn round_trip<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let default = timeout(
        DEADLINE,
        rules.get_rule(request(get(CHILD, "$Default"), Some(sas("", "manage")))),
    )
    .await??
    .into_inner();
    assert_eq!(default.subscription_path, CHILD);
    assert_eq!(default.name, "$Default");
    assert_eq!(default.created_at_unix_millis, 10_000);
    assert_eq!(
        default.filter,
        Some(RuleFilter {
            filter: Some(Filter::TrueFilter(TrueRuleFilter {}))
        })
    );
    let filters = [
        ("Always", Filter::TrueFilter(TrueRuleFilter {})),
        ("Correlated", Filter::CorrelationFilter(all_scalars())),
        ("Never", Filter::FalseFilter(FalseRuleFilter {})),
        (
            "Sql",
            Filter::SqlFilter(SqlRuleFilter {
                expression: "color = 'red'".into(),
                semantic_version: None,
            }),
        ),
    ];
    let mut expected: Vec<Rule> = vec![default];
    for (name, filter) in filters {
        timeout(
            DEADLINE,
            rules.create_rule(request(
                create("Orders/SUBSCRIPTIONS/Alpha", name, filter.clone()),
                Some(sas("", "manage")),
            )),
        )
        .await??;
        let output = timeout(
            DEADLINE,
            rules.get_rule(request(
                get("Orders/Subscriptions/Alpha", name),
                Some(sas("", "manage")),
            )),
        )
        .await??
        .into_inner();
        assert_eq!(output.namespace, "tenant");
        assert_eq!(output.subscription_path, CHILD);
        assert_eq!(output.name, name);
        assert_eq!(output.created_at_unix_millis, 10_000);
        let expected_filter = match filter {
            Filter::SqlFilter(sql) => Filter::SqlFilter(SqlRuleFilter {
                semantic_version: Some(domain::SQL_FILTER_SEMANTIC_VERSION),
                ..sql
            }),
            other => other,
        };
        assert_eq!(
            output.filter,
            Some(RuleFilter {
                filter: Some(expected_filter)
            })
        );
        expected.push(output);
    }
    node.clock.set(0);
    let before = node.snapshot()?;
    let output = timeout(
        DEADLINE,
        rules.list_rules(request(
            list("Orders/SUBSCRIPTIONS/Alpha"),
            Some(sas("", "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(output.rules, expected);
    assert!(output.encoded_len() <= server::NATIVE_ADMIN_RESPONSE_LIMIT);
    assert_eq!(
        node.snapshot()?,
        before,
        "Get/List must not stamp the regressed clock"
    );
    node.clock.set(10_001);
    assert_eq!(
        timeout(
            DEADLINE,
            rules.create_rule(request(
                create(CHILD, "Always", Filter::FalseFilter(FalseRuleFilter {})),
                Some(sas("", "manage"))
            ))
        )
        .await?
        .expect_err("creation is not an update")
        .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        timeout(
            DEADLINE,
            rules.get_rule(request(get(CHILD, "always"), Some(sas("", "manage"))))
        )
        .await?
        .expect_err("rule names preserve case")
        .code(),
        Code::NotFound
    );
    for version in [0, domain::SQL_FILTER_SEMANTIC_VERSION + 1] {
        assert_eq!(
            timeout(
                DEADLINE,
                rules.create_rule(request(
                    create(
                        CHILD,
                        "Future",
                        Filter::SqlFilter(SqlRuleFilter {
                            expression: "not valid SQL".into(),
                            semantic_version: Some(version),
                        })
                    ),
                    Some(sas("", "manage"))
                ))
            )
            .await?
            .expect_err("unsupported version is not compiled")
            .code(),
            Code::Unimplemented
        );
    }
    for rule in expected {
        timeout(
            DEADLINE,
            rules.delete_rule(request(delete(CHILD, &rule.name), Some(sas("", "manage")))),
        )
        .await??;
    }
    assert!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas("", "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .is_empty()
    );
    assert_eq!(
        timeout(
            DEADLINE,
            rules.delete_rule(request(delete(CHILD, "Always"), Some(sas("", "manage"))))
        )
        .await?
        .expect_err("missing rule is not a successful deletion")
        .code(),
        Code::NotFound
    );
    assert_eq!(
        timeout(
            DEADLINE,
            entities.get_entity(request(
                GetEntityRequest {
                    namespace: "tenant".into(),
                    path: CHILD.into()
                },
                Some(sas("", "manage"))
            ))
        )
        .await??
        .into_inner()
        .kind,
        EntityKind::Subscription as i32
    );
    Ok(())
}
