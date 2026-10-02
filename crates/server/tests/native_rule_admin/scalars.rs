use super::*;

pub(super) fn scalar_cases() -> Vec<(rule_scalar_value::Value, MessageValue)> {
    use rule_scalar_value::Value as P;
    vec![
        (P::NullValue(RuleNullValue {}), MessageValue::Null),
        (P::BoolValue(false), MessageValue::Bool(false)),
        (P::UbyteValue(255), MessageValue::Ubyte(255)),
        (P::UshortValue(65_535), MessageValue::Ushort(65_535)),
        (P::UintValue(u32::MAX), MessageValue::Uint(u32::MAX)),
        (P::UlongValue(u64::MAX), MessageValue::Ulong(u64::MAX)),
        (P::ByteValue(-128), MessageValue::Byte(-128)),
        (P::ShortValue(-32_768), MessageValue::Short(-32_768)),
        (P::IntValue(i32::MIN), MessageValue::Int(i32::MIN)),
        (P::LongValue(i64::MIN), MessageValue::Long(i64::MIN)),
        (P::FloatBits(0x7fc0_0001), MessageValue::Float(0x7fc0_0001)),
        (
            P::DoubleBits(0x8000_0000_0000_0000),
            MessageValue::Double(0x8000_0000_0000_0000),
        ),
        (
            P::Decimal32Bytes(vec![0, 1, 254, 255]),
            MessageValue::Decimal32([0, 1, 254, 255]),
        ),
        (
            P::Decimal64Bytes(vec![128; 8]),
            MessageValue::Decimal64([128; 8]),
        ),
        (
            P::Decimal128Bytes(vec![254; 16]),
            MessageValue::Decimal128([254; 16]),
        ),
        (P::CharCodepoint(0x1f600), MessageValue::Char('\u{1f600}')),
        (P::TimestampMillis(-1), MessageValue::Timestamp(-1)),
        (P::UuidBytes(vec![255; 16]), MessageValue::Uuid([255; 16])),
        (
            P::BinaryValue(vec![0, 255, 128]),
            MessageValue::Binary(vec![0, 255, 128]),
        ),
        (
            P::StringValue("literal \u{1f600}".into()),
            MessageValue::String("literal \u{1f600}".into()),
        ),
        (
            P::SymbolValue("ASCII.symbol".into()),
            MessageValue::Symbol("ASCII.symbol".into()),
        ),
    ]
}

pub(super) async fn all_scalar_constructors_round_trip_without_coercion<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let properties = scalar_cases()
        .iter()
        .enumerate()
        .map(|(index, (value, _))| property(&format!("value{index:02}"), value.clone()))
        .collect::<Vec<_>>();
    let mut input = CorrelationRuleFilter {
        correlation_id: Some(String::new()),
        message_id: Some("id".into()),
        to: Some("address".into()),
        reply_to: Some("reply".into()),
        subject: Some("subject".into()),
        session_id: Some("session".into()),
        reply_to_session_id: Some("reply-session".into()),
        content_type: Some("application/test".into()),
        properties,
    };
    assert_eq!(input.properties.len() + 8, 29);
    node.create(PATH, "all-scalars", correlation(input.clone()))
        .await?;
    let saved = node.get(PATH, "all-scalars").await?;
    assert_eq!(saved.filter, correlation(input.clone()));
    let rules = StateMachine::new(node.store.clone()).rules(
        &namespace()?,
        &EntityPath::new("Orders")?,
        &SubscriptionName::new("Alpha")?,
    )?;
    let actual = rules
        .iter()
        .find(|rule| rule.name.as_str() == "all-scalars")
        .expect("stored typed rule");
    let domain::RuleFilter::Correlation(actual) = &actual.filter else {
        panic!("correlation filter");
    };
    assert_eq!(
        actual.properties,
        scalar_cases()
            .into_iter()
            .enumerate()
            .map(|(index, (_, value))| (format!("value{index:02}"), value))
            .collect::<BTreeMap<_, _>>()
    );
    assert_eq!(actual.correlation_id, Some(String::new()));
    input.properties = vec![
        property(
            "case",
            rule_scalar_value::Value::StringValue("upper".into()),
        ),
        property(
            "Case",
            rule_scalar_value::Value::StringValue("lower".into()),
        ),
        property(
            "float-minus-zero",
            rule_scalar_value::Value::FloatBits(0x8000_0000),
        ),
        property(
            "double-nan",
            rule_scalar_value::Value::DoubleBits(0x7ff8_0000_0000_0001),
        ),
        property(
            "empty-binary",
            rule_scalar_value::Value::BinaryValue(Vec::new()),
        ),
        property(
            "empty-symbol",
            rule_scalar_value::Value::SymbolValue(String::new()),
        ),
    ];
    node.create(PATH, "bits-and-case", correlation(input.clone()))
        .await?;
    input
        .properties
        .sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(
        node.get(PATH, "bits-and-case").await?.filter,
        correlation(input)
    );
    let before = node.snapshot()?;
    let rules = node.list(PATH).await?;
    let node = node.reopen()?;
    node.clock.manual.set(0);
    assert_eq!(node.list(PATH).await?, rules);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.clocks(), 0);
    assert_eq!(node.writes(), 0);
    Ok(())
}
