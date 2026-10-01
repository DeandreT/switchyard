use domain::{MAX_SUBSCRIPTION_RULES, RuleDefinition, RuleFilter, RuleName};

use super::*;

async fn complete_rules_are_clock_free_and_subscription_scoped<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    for name in ["Alpha", "beta"] {
        node.subscription("events", name, SubscriptionConfig::default())?;
    }
    node.submit(
        "events",
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("never")?,
            filter: RuleFilter::False,
        },
    )?;
    let parent = EntityPath::new("events")?;
    let alpha = SubscriptionName::new("Alpha")?;
    let expected = vec![
        RuleDefinition {
            name: RuleName::new("$Default")?,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(1_000),
        },
        RuleDefinition {
            name: RuleName::new("never")?,
            filter: RuleFilter::False,
            created_at: Timestamp::from_millis(1_000),
        },
    ];
    node.store
        .observed
        .rule_limits
        .lock()
        .expect("rule scan limits")
        .clear();
    let snapshot = node.store.snapshot()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.inner.set(0);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let handle = node.broker.handle();
    assert_eq!(
        handle.rules_blocking(namespace(), parent.clone(), alpha.clone())?,
        expected
    );
    assert_eq!(
        handle
            .rules(namespace(), parent.clone(), alpha.clone())
            .await?,
        expected
    );
    assert_eq!(
        protocol_amqp::Broker::rules(&handle, namespace(), parent.clone(), alpha.clone()).await?,
        expected
    );
    assert_eq!(
        handle
            .rules(namespace(), parent.clone(), SubscriptionName::new("beta")?)
            .await?,
        expected[..1]
    );
    {
        let mut limits = node
            .store
            .observed
            .rule_limits
            .lock()
            .expect("rule scan limits");
        assert_eq!(limits.as_slice(), &[MAX_SUBSCRIPTION_RULES + 1; 4]);
        limits.clear();
    }
    assert!(matches!(
        handle
            .rules(
                NamespaceName::new("another")?,
                parent.clone(),
                alpha.clone()
            )
            .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::TopicNotFound
        )))
    ));
    assert!(matches!(
        protocol_amqp::Broker::rules(
            &handle,
            namespace(),
            parent.clone(),
            SubscriptionName::new("ALPHA")?,
        )
        .await,
        Err(protocol_amqp::BrokerRejection::Refused(
            BrokerError::SubscriptionNotFound
        ))
    ));
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    assert_eq!(node.store.snapshot()?, snapshot);
    {
        let limits = node
            .store
            .observed
            .rule_limits
            .lock()
            .expect("rule scan limits");
        assert_eq!(limits.as_slice(), &[1, 1]);
    }
    drop(node.broker);
    assert_eq!(
        handle.rules_blocking(namespace(), parent.clone(), alpha.clone()),
        Err(SubmitError::BrokerStopped)
    );
    assert_eq!(
        handle
            .rules(namespace(), parent.clone(), alpha.clone())
            .await,
        Err(SubmitError::BrokerStopped)
    );
    assert!(matches!(
        protocol_amqp::Broker::rules(&handle, namespace(), parent, alpha).await,
        Err(protocol_amqp::BrokerRejection::Unavailable(_))
    ));
    Ok(())
}

async fn rule_reads_reject_reserved_targets_and_report_storage_failure<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    node.subscription("events", "Alpha", SubscriptionConfig::default())?;
    let handle = node.broker.handle();
    let alpha = SubscriptionName::new("Alpha")?;
    let snapshot = node.store.snapshot()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    for parent in ["events/$deadletterqueue", "events/subscriptions/Alpha"] {
        node.store.observed.reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            handle.rules_blocking(namespace(), EntityPath::new(parent)?, alpha.clone()),
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::DeadLetterQueueIsReserved | BrokerError::SubscriptionPathIsReserved
            )))
        ));
        assert_eq!(node.store.observed.reads.load(Ordering::SeqCst), 0);
    }
    let parent = EntityPath::new("events")?;
    *node.store.observed.fail_get.lock().expect("read failure") =
        Some(keys::topic_config(&namespace(), &parent));
    assert!(matches!(
        handle
            .rules(namespace(), parent.clone(), alpha.clone())
            .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Storage(StorageError::Backend { .. })
        )))
    ));
    assert_eq!(handle.rules(namespace(), parent, alpha).await?.len(), 1);
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    assert_eq!(node.store.snapshot()?, snapshot);
    Ok(())
}

#[tokio::test]
async fn memory_rules_are_clock_free() -> TestResult {
    complete_rules_are_clock_free_and_subscription_scoped(testkit::MemoryProvider::new()).await
}

#[tokio::test]
async fn durable_rules_are_clock_free() -> TestResult {
    complete_rules_are_clock_free_and_subscription_scoped(testkit::DurableProvider::temporary()?)
        .await
}

#[tokio::test]
async fn memory_rule_read_failures_are_atomic() -> TestResult {
    rule_reads_reject_reserved_targets_and_report_storage_failure(testkit::MemoryProvider::new())
        .await
}

#[tokio::test]
async fn durable_rule_read_failures_are_atomic() -> TestResult {
    rule_reads_reject_reserved_targets_and_report_storage_failure(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
