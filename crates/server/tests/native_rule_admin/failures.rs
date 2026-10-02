use super::*;

pub(super) async fn invalid_filters_and_scalar_widths_never_reach_owner<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    let reads = node.reads();
    for name in ["", "   ", "a/b", "a\\b", "a@b", "a?b", "a#b", "a*b", "a\nb"] {
        code(
            node.create(PATH, name, true_filter()).await,
            Code::InvalidArgument,
        );
        code(node.get(PATH, name).await, Code::InvalidArgument);
        code(node.delete(PATH, name).await, Code::InvalidArgument);
    }
    for name in ["a".repeat(51), "\u{1f600}".repeat(26)] {
        code(
            node.create(PATH, &name, true_filter()).await,
            Code::InvalidArgument,
        );
    }
    for missing in [None, Some(RuleFilter::default())] {
        code(
            node.create(PATH, "missing", missing).await,
            Code::InvalidArgument,
        );
    }
    for expression in ["", "broken =", "priority >>> 3"] {
        code(
            node.create(PATH, "syntax", sql(expression, None)).await,
            Code::InvalidArgument,
        );
    }
    for version in [0, 2, u32::MAX] {
        code(
            node.create(PATH, "version", sql("broken =", Some(version)))
                .await,
            Code::Unimplemented,
        );
    }
    use rule_scalar_value::Value as S;
    let bad = vec![
        S::UbyteValue(256),
        S::UshortValue(65_536),
        S::ByteValue(-129),
        S::ByteValue(128),
        S::ShortValue(-32_769),
        S::ShortValue(32_768),
        S::Decimal32Bytes(vec![0; 3]),
        S::Decimal32Bytes(vec![0; 5]),
        S::Decimal64Bytes(vec![0; 7]),
        S::Decimal64Bytes(vec![0; 9]),
        S::Decimal128Bytes(vec![0; 15]),
        S::Decimal128Bytes(vec![0; 17]),
        S::UuidBytes(vec![0; 15]),
        S::UuidBytes(vec![0; 17]),
        S::CharCodepoint(0xd800),
        S::CharCodepoint(0xdfff),
        S::CharCodepoint(0x11_0000),
        S::SymbolValue("non-ascii-\u{e9}".into()),
    ];
    for value in bad {
        code(
            node.create(
                PATH,
                "invalid-scalar",
                correlation(CorrelationRuleFilter {
                    properties: vec![property("p", value)],
                    ..Default::default()
                }),
            )
            .await,
            Code::InvalidArgument,
        );
    }
    for properties in [
        vec![CorrelationProperty {
            name: "p".into(),
            value: None,
        }],
        vec![CorrelationProperty {
            name: "p".into(),
            value: Some(RuleScalarValue::default()),
        }],
        vec![
            property("duplicate", S::NullValue(RuleNullValue {})),
            property("duplicate", S::BoolValue(true)),
        ],
    ] {
        code(
            node.create(
                PATH,
                "invalid-property",
                correlation(CorrelationRuleFilter {
                    properties,
                    ..Default::default()
                }),
            )
            .await,
            Code::InvalidArgument,
        );
    }
    for expression in [
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
        "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
    ] {
        code(
            node.create(PATH, "large-sql", sql(&expression, None)).await,
            Code::ResourceExhausted,
        );
    }
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    node.create(PATH, &"\u{1f600}".repeat(25), true_filter())
        .await?;
    node.create(
        PATH,
        "null",
        correlation(CorrelationRuleFilter {
            properties: vec![property("p", S::NullValue(RuleNullValue {}))],
            ..Default::default()
        }),
    )
    .await?;
    assert_eq!(
        node.get(PATH, "null").await?.filter,
        correlation(CorrelationRuleFilter {
            properties: vec![property("p", S::NullValue(RuleNullValue {}))],
            ..Default::default()
        })
    );
    Ok(())
}

fn large_rule(name: &str, wanted: usize) -> TestResult<(RuleDefinition, Option<RuleFilter>)> {
    let definition = |length: usize| -> TestResult<RuleDefinition> {
        Ok(RuleDefinition {
            name: RuleName::new(name)?,
            filter: domain::RuleFilter::Correlation(CorrelationFilter {
                properties: BTreeMap::from([(
                    "large".into(),
                    MessageValue::String("x".repeat(length)),
                )]),
                ..Default::default()
            }),
            created_at: Timestamp::from_millis(1_000),
        })
    };
    let mut low = 0;
    let mut high = wanted;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if definition(middle)?
            .encoded_size()
            .is_ok_and(|size| size <= wanted)
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let result = definition(low)?;
    assert_eq!(result.encoded_size()?, wanted);
    let input = correlation(CorrelationRuleFilter {
        properties: vec![property(
            "large",
            rule_scalar_value::Value::StringValue("x".repeat(low)),
        )],
        ..Default::default()
    });
    Ok((result, input))
}

pub(super) async fn rule_count_condition_and_encoded_byte_limits_are_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    node.delete(PATH, "$Default").await?;
    let mut conditions = CorrelationRuleFilter {
        correlation_id: Some("a".into()),
        message_id: Some("b".into()),
        to: Some("c".into()),
        reply_to: Some("d".into()),
        subject: Some("e".into()),
        session_id: Some("f".into()),
        reply_to_session_id: Some("g".into()),
        content_type: Some("h".into()),
        properties: (0..24)
            .map(|index| {
                property(
                    &format!("p{index:02}"),
                    rule_scalar_value::Value::UintValue(index),
                )
            })
            .collect(),
    };
    node.create(PATH, "boundary", correlation(conditions.clone()))
        .await?;
    assert_eq!(
        node.get(PATH, "boundary").await?.filter,
        correlation(conditions.clone())
    );
    conditions.properties.push(property(
        "overflow",
        rule_scalar_value::Value::NullValue(RuleNullValue {}),
    ));
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    let reads = node.reads();
    code(
        node.create(PATH, "overflow", correlation(conditions)).await,
        Code::ResourceExhausted,
    );
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    for index in 1..domain::MAX_SUBSCRIPTION_RULES {
        node.create(PATH, &format!("rule{index:02}"), true_filter())
            .await?;
    }
    assert_eq!(node.list(PATH).await?.len(), domain::MAX_SUBSCRIPTION_RULES);
    let before = node.snapshot()?;
    let writes = node.writes();
    code(
        node.create(PATH, "thirty-third", true_filter()).await,
        Code::ResourceExhausted,
    );
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);
    node.delete(PATH, "rule31").await?;
    node.create(PATH, "healthy-after-count-refusal", false_filter())
        .await?;

    node.topology("Bytes", "Alpha").await?;
    let path = "Bytes/subscriptions/Alpha";
    node.delete(path, "$Default").await?;
    let (_, boundary) = large_rule("big0", domain::MAX_RULE_BYTES)?;
    let mut oversized = boundary.clone().expect("filter");
    let Some(rule_filter::Filter::CorrelationFilter(filter)) = oversized.filter.as_mut() else {
        panic!("correlation");
    };
    let Some(rule_scalar_value::Value::StringValue(value)) = filter.properties[0]
        .value
        .as_mut()
        .expect("scalar")
        .value
        .as_mut()
    else {
        panic!("string");
    };
    value.push('x');
    let before = node.snapshot()?;
    let writes = node.writes();
    code(
        node.create(path, "big0", Some(oversized)).await,
        Code::ResourceExhausted,
    );
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);
    for index in 0..4 {
        let name = format!("big{index}");
        let (definition, filter) = large_rule(&name, domain::MAX_RULE_BYTES)?;
        node.create(path, &name, filter).await?;
        let bytes = node
            .store
            .inner
            .get(&keys::rule(
                &namespace()?,
                &EntityPath::new("Bytes")?,
                &SubscriptionName::new("Alpha")?,
                &definition.name,
            ))?
            .expect("exact stored rule");
        assert_eq!(bytes.len(), domain::MAX_RULE_BYTES);
    }
    assert_eq!(node.list(path).await?.len(), 4);
    let before = node.snapshot()?;
    let writes = node.writes();
    code(
        node.create(path, "extra", true_filter()).await,
        Code::ResourceExhausted,
    );
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);
    node.delete(path, "big3").await?;
    node.create(path, "extra", true_filter()).await?;
    Ok(())
}
