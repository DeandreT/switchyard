use super::*;

pub(super) async fn action_bearing_reads_refuse_without_omitting_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    node.create(PATH, "plain", false_filter()).await?;
    let action = domain::SqlAction::new("REMOVE [private-action]")?;
    tokio::time::timeout(
        DEADLINE,
        node.broker.handle().submit(
            namespace()?,
            EntityPath::new("Orders")?,
            CommandKind::CreateRuleWithAction {
                subscription: SubscriptionName::new("Alpha")?,
                name: RuleName::new("annotated")?,
                filter: domain::RuleFilter::True,
                action: action.clone(),
            },
        ),
    )
    .await??;
    let before = node.snapshot()?;
    let node = node.reopen()?;
    let writes = node.writes();
    let clocks = node.clocks();
    node.clock.manual.set(0);
    assert_eq!(node.get(PATH, "plain").await?.filter, false_filter());
    for error in [
        code(node.get(PATH, "annotated").await, Code::Unimplemented),
        code(node.list(PATH).await, Code::Unimplemented),
    ] {
        assert!(!error.message().contains("private-action"));
        assert!(error.message().contains("cannot represent SQL actions"));
    }
    let definitions = tokio::time::timeout(
        DEADLINE,
        node.broker.handle().rules(
            namespace()?,
            EntityPath::new("Orders")?,
            SubscriptionName::new("Alpha")?,
        ),
    )
    .await??;
    assert_eq!(
        definitions
            .into_iter()
            .find(|rule| rule.name.as_str() == "annotated")
            .expect("action rule")
            .action,
        Some(action),
    );
    node.unchanged(&before, writes, clocks)?;
    node.clock.manual.set(2_000);
    node.delete(PATH, "annotated").await?;
    assert_eq!(node.list(PATH).await?.len(), 2);
    Ok(())
}

fn legacy_rule(name: &RuleName, length: usize) -> TestResult<Vec<u8>> {
    let filter = domain::RuleFilter::Correlation(CorrelationFilter {
        properties: BTreeMap::from([("p".into(), MessageValue::Binary(vec![0; length]))]),
        ..Default::default()
    });
    let mut bytes = codec::encode(&(name, filter, Timestamp::from_millis(1_000)))?;
    bytes[0] = codec::VALUE_FORMAT_V10;
    Ok(bytes)
}

pub(super) async fn exact_limit_legacy_rules_remain_readable<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let name = RuleName::new("legacy-limit")?;
    let mut low = 0_usize;
    let mut high = domain::MAX_RULE_BYTES;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if legacy_rule(&name, middle)?.len() <= domain::MAX_RULE_BYTES {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let bytes = legacy_rule(&name, low)?;
    assert_eq!(bytes.len(), domain::MAX_RULE_BYTES);
    let decoded = RuleDefinition::decode(&bytes)?;
    assert!(decoded.action.is_none());
    assert!(decoded.encoded_size().is_err());
    let mut batch = WriteBatch::default();
    batch.push_put(
        keys::rule(
            &namespace()?,
            &EntityPath::new("Orders")?,
            &SubscriptionName::new("Alpha")?,
            &name,
        ),
        bytes,
    );
    node.store.apply(batch)?;
    let before = node.snapshot()?;
    let node = node.reopen()?;
    let writes = node.writes();
    let clocks = node.clocks();
    node.clock.manual.set(0);
    let expected = correlation(CorrelationRuleFilter {
        properties: vec![property(
            "p",
            rule_scalar_value::Value::BinaryValue(vec![0; low]),
        )],
        ..Default::default()
    });
    assert_eq!(node.get(PATH, name.as_str()).await?.filter, expected);
    assert_eq!(node.list(PATH).await?.len(), 2);
    node.unchanged(&before, writes, clocks)?;
    node.clock.manual.set(1_000);
    node.create(PATH, "new-format", false_filter()).await?;
    assert_eq!(node.get(PATH, name.as_str()).await?.filter, expected);
    node.delete(PATH, name.as_str()).await?;
    assert_eq!(node.list(PATH).await?.len(), 2);
    Ok(())
}
