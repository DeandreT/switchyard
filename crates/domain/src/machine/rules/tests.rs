use super::*;
use storage::MemoryStore;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn fixture() -> Result<
    (
        StateMachine<MemoryStore>,
        NamespaceName,
        EntityPath,
        SubscriptionName,
    ),
    BrokerError,
> {
    let machine = StateMachine::new(MemoryStore::default());
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("orders")?;
    let subscription = SubscriptionName::new("child")?;
    for kind in [
        CommandKind::CreateTopic {
            config: crate::TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: subscription.clone(),
            config: crate::SubscriptionConfig::default(),
        },
        CommandKind::DeleteRule {
            subscription: subscription.clone(),
            name: RuleName::new("$Default")?,
        },
    ] {
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::UNIX_EPOCH,
            kind,
        ))?;
    }
    Ok((machine, namespace, topic, subscription))
}

fn legacy(name: &RuleName, size: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut filter = CorrelationFilter {
        properties: BTreeMap::from([(
            "value".to_owned(),
            MessageValue::Binary(vec![0; size - 100]),
        )]),
        ..CorrelationFilter::default()
    };
    let encode = |filter: &CorrelationFilter| -> Result<Vec<u8>, postcard::Error> {
        let mut bytes = vec![codec::VALUE_FORMAT_V10];
        bytes.extend_from_slice(&postcard::to_stdvec(&(
            name,
            RuleFilter::Correlation(filter.clone()),
            Timestamp::UNIX_EPOCH,
        ))?);
        Ok(bytes)
    };
    let length = encode(&filter)?.len();
    let MessageValue::Binary(value) = filter.properties.get_mut("value").expect("binary") else {
        unreachable!("binary condition")
    };
    value.resize(value.len() + size - length, 0);
    let bytes = encode(&filter)?;
    assert_eq!(bytes.len(), size);
    Ok(bytes)
}

#[test]
fn exact_limit_legacy_rule_remains_readable_and_allows_create_then_delete() -> TestResult {
    let (machine, namespace, topic, subscription) = fixture()?;
    let name = RuleName::new("legacy")?;
    let bytes = legacy(&name, MAX_RULE_BYTES)?;
    let decoded = RuleDefinition::decode(&bytes)?;
    assert!(decoded.validate().is_ok());
    assert!(matches!(
        decoded.encoded_size(),
        Err(BrokerError::RuleTooLarge { .. })
    ));
    let key = keys::rule(&namespace, &topic, &subscription, &name);
    machine
        .store()
        .apply(WriteBatch::default().put(key.clone(), bytes.clone()))?;
    let snapshot = machine.store().snapshot()?;
    assert_eq!(
        machine.rules(&namespace, &topic, &subscription)?,
        vec![decoded]
    );
    assert_eq!(machine.store().snapshot()?, snapshot);
    assert_eq!(
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1),
            CommandKind::CreateRule {
                subscription: subscription.clone(),
                name: RuleName::new("new")?,
                filter: RuleFilter::True
            }
        ))?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(machine.store().get(&key)?, Some(bytes));
    assert_eq!(
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(2),
            CommandKind::DeleteRule {
                subscription: subscription.clone(),
                name
            }
        ))?,
        CommandOutcome::RuleDeleted
    );
    assert_eq!(machine.rules(&namespace, &topic, &subscription)?.len(), 1);
    Ok(())
}

#[test]
fn aggregate_rule_admission_counts_actual_legacy_bytes_not_v11_reencodings() -> TestResult {
    let (machine, namespace, topic, subscription) = fixture()?;
    let name = RuleName::new("new")?;
    let new_size = rule_definition_size(&name, &RuleFilter::True, Timestamp::from_millis(1), None)?;
    let mut batch = WriteBatch::default();
    for index in 0..4 {
        let legacy_name = RuleName::new(format!("legacy{index}"))?;
        let size = MAX_RULE_BYTES - if index == 3 { new_size } else { 0 };
        batch.push_put(
            keys::rule(&namespace, &topic, &subscription, &legacy_name),
            legacy(&legacy_name, size)?,
        );
    }
    machine.store().apply(batch)?;
    assert_eq!(
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1),
            CommandKind::CreateRule {
                subscription: subscription.clone(),
                name,
                filter: RuleFilter::True
            }
        ))?,
        CommandOutcome::RuleCreated
    );
    let records = machine.store().scan_prefix(
        &keys::rule_prefix(&namespace, &topic, &subscription),
        MAX_SUBSCRIPTION_RULES + 1,
    )?;
    assert_eq!(
        records.iter().map(|(_, bytes)| bytes.len()).sum::<usize>(),
        MAX_SUBSCRIPTION_RULE_BYTES
    );
    assert_eq!(machine.rules(&namespace, &topic, &subscription)?.len(), 5);
    let before = machine.store().snapshot()?;
    assert!(matches!(
        machine.apply(&Command::new(
            namespace,
            topic,
            Timestamp::from_millis(2),
            CommandKind::CreateRule {
                subscription,
                name: RuleName::new("overflow")?,
                filter: RuleFilter::True
            }
        )),
        Err(BrokerError::RuleSetTooLarge { .. })
    ));
    assert_eq!(machine.store().snapshot()?, before);
    Ok(())
}
