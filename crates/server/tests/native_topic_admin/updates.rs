use std::collections::BTreeSet;

use super::*;

fn topic_patch(path: &str, config: TopicConfiguration) -> UpdateEntityRequest {
    UpdateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        topic_config: Some(config),
        ..Default::default()
    }
}

fn subscription_patch(path: &str, config: SubscriptionConfiguration) -> UpdateEntityRequest {
    UpdateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        subscription_config: Some(config),
        ..Default::default()
    }
}

fn puts<P: StoreProvider>(node: &Node<P>) -> Vec<Key> {
    node.store.observations.puts.lock().expect("puts").clone()
}

fn changed_keys<P: StoreProvider>(node: &Node<P>, start: usize, expected: Vec<Key>) {
    let actual = puts(node);
    assert_eq!(actual.len() - start, expected.len());
    assert_eq!(
        actual[start..].iter().cloned().collect::<BTreeSet<_>>(),
        expected.into_iter().collect::<BTreeSet<_>>()
    );
}

async fn partial_updates_preserve_presence_projections_rules_and_read_only_queries<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(topic(
        "Orders",
        Some(TopicConfiguration {
            default_time_to_live: Some(topic_configuration::DefaultTimeToLive::DefaultTtlMillis(
                30_000,
            )),
            requires_duplicate_detection: Some(true),
            duplicate_detection_history_time_window_millis: Some(60_000),
            max_message_bytes: Some(4_096),
        }),
    ))
    .await?;
    node.create(subscription(
        "Orders",
        "Alpha",
        Some(SubscriptionConfiguration {
            lock_duration_millis: Some(17_000),
            max_delivery_count: Some(3),
            default_time_to_live: Some(
                subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(10_000),
            ),
            max_message_bytes: Some(2_048),
            requires_session: Some(true),
            dead_lettering_on_message_expiration: Some(true),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
        }),
    )?)
    .await?;
    let namespace = NamespaceName::new("tenant")?;
    let parent = EntityPath::new("Orders")?;
    let member = SubscriptionName::new("Alpha")?;
    let child = parent.subscription(&member)?;
    let shadow = child.dead_letter_queue()?;
    let rule_key = keys::rule(
        &namespace,
        &parent,
        &member,
        &domain::RuleName::new("$Default")?,
    );
    let original_rule = node.store.get(&rule_key)?;
    node.clock.set(2_000);
    let writes = node.writes();
    let start = puts(&node).len();
    let updated_topic = node
        .update(topic_patch(
            "Orders",
            TopicConfiguration {
                default_time_to_live: Some(
                    topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                        UnlimitedTimeToLive {},
                    ),
                ),
                max_message_bytes: Some(8_192),
                ..Default::default()
            },
        ))
        .await?;
    kind(&updated_topic, EntityKind::Topic);
    assert_eq!(node.writes(), writes + 1);
    changed_keys(
        &node,
        start,
        vec![keys::topic_config(&namespace, &parent), keys::clock()],
    );
    let config = updated_topic.topic_config.as_ref().expect("topic");
    assert_eq!(config.requires_duplicate_detection, Some(true));
    assert_eq!(
        config.duplicate_detection_history_time_window_millis,
        Some(60_000)
    );
    assert_eq!(config.max_message_bytes, Some(8_192));
    assert!(matches!(
        config.default_time_to_live,
        Some(topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
            _
        ))
    ));
    let start = puts(&node).len();
    let updated_child = node
        .update(subscription_patch(
            "Orders/SUBSCRIPTIONS/Alpha",
            SubscriptionConfiguration {
                lock_duration_millis: Some(23_000),
                dead_lettering_on_message_expiration: Some(false),
                ..Default::default()
            },
        ))
        .await?;
    kind(&updated_child, EntityKind::Subscription);
    assert_eq!(updated_child.path, child.as_str());
    assert_eq!(node.writes(), writes + 2);
    changed_keys(
        &node,
        start,
        vec![
            keys::subscription(&namespace, &parent, &member),
            keys::queue_config(&namespace, &child),
            keys::queue_config(&namespace, &shadow),
            keys::clock(),
        ],
    );
    let config = updated_child
        .subscription_config
        .as_ref()
        .expect("subscription");
    assert_eq!(config.lock_duration_millis, Some(23_000));
    assert_eq!(config.max_delivery_count, Some(3));
    assert_eq!(config.max_message_bytes, Some(2_048));
    assert_eq!(config.requires_session, Some(true));
    assert_eq!(config.dead_lettering_on_message_expiration, Some(false));
    assert_eq!(
        config.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(10_000))
    ));
    assert_eq!(node.store.get(&rule_key)?, original_rule);
    let machine = StateMachine::new(node.store.clone());
    let stored = machine
        .subscription_config(&namespace, &parent, &member)?
        .expect("membership");
    assert_eq!(
        machine.queue_config(&namespace, &child)?,
        Some(stored.to_queue_config())
    );
    assert_eq!(
        machine.queue_config(&namespace, &shadow)?,
        Some(stored.to_queue_config().dead_letter_shadow())
    );

    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(9_000);
    for request in [
        topic_patch("Orders", TopicConfiguration::default()),
        topic_patch("Orders", updated_topic.topic_config.expect("topic")),
        subscription_patch(child.as_str(), SubscriptionConfiguration::default()),
        subscription_patch(
            child.as_str(),
            updated_child.subscription_config.expect("subscription"),
        ),
    ] {
        node.update(request).await?;
        node.unchanged(&before, writes)?;
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    }
    node.clock.set(0);
    assert_eq!(node.get("Orders").await?, updated_topic);
    assert_eq!(node.get(child.as_str()).await?, updated_child);
    let listed =
        super::paging::page(&node, list(EntityKind::Subscription, "Orders", 10, "")).await?;
    assert_eq!(listed.entities, vec![updated_child.clone()]);
    node.unchanged(&before, writes)?;
    node.clock.set(2_001);
    let unlimited_child = node
        .update(subscription_patch(
            child.as_str(),
            SubscriptionConfiguration {
                default_time_to_live: Some(
                    subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                        UnlimitedTimeToLive {},
                    ),
                ),
                ..Default::default()
            },
        ))
        .await?;
    assert_eq!(
        unlimited_child
            .subscription_config
            .as_ref()
            .expect("subscription")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    let before = node.snapshot()?;
    drop(machine);
    let node = node.reopen()?;
    node.clock.set(0);
    assert_eq!(node.get("Orders").await?, updated_topic);
    assert_eq!(node.get(child.as_str()).await?, unlimited_child);
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

async fn invalid_patch_families_immutability_and_regressed_clock_leave_state_unchanged<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(topic("Orders", None)).await?;
    node.create(subscription("Orders", "Alpha", None)?).await?;
    node.create(queue("work")).await?;
    node.clock.set(2_000);
    let before = node.snapshot()?;
    let writes = node.writes();
    let mut mixed = topic_patch("Orders", TopicConfiguration::default());
    mixed.queue_config = Some(QueueConfiguration::default());
    let mut mixed_sub = subscription_patch(
        "Orders/subscriptions/Alpha",
        SubscriptionConfiguration::default(),
    );
    mixed_sub.topic_config = Some(TopicConfiguration::default());
    for request in [
        UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders".into(),
            ..Default::default()
        },
        mixed,
        mixed_sub,
        topic_patch("Orders/subscriptions/Alpha", TopicConfiguration::default()),
        subscription_patch("Orders", SubscriptionConfiguration::default()),
        topic_patch("Orders/$deadletterqueue", TopicConfiguration::default()),
        subscription_patch(
            "Orders/subscriptions/Alpha/$deadletterqueue",
            SubscriptionConfiguration::default(),
        ),
    ] {
        let reads = node.reads();
        code(node.update(request).await, Code::InvalidArgument);
        assert_eq!(
            node.reads(),
            reads,
            "structurally invalid patches must not reach the owner"
        );
        node.unchanged(&before, writes)?;
    }
    for request in [
        topic_patch("work", TopicConfiguration::default()),
        subscription_patch(
            "Orders/subscriptions/Alpha",
            SubscriptionConfiguration {
                max_delivery_count: Some(0),
                ..Default::default()
            },
        ),
        topic_patch(
            "Orders",
            TopicConfiguration {
                default_time_to_live: Some(
                    topic_configuration::DefaultTimeToLive::DefaultTtlMillis(0),
                ),
                ..Default::default()
            },
        ),
    ] {
        code(node.update(request).await, Code::InvalidArgument);
        node.unchanged(&before, writes)?;
    }
    for request in [
        topic_patch(
            "Orders",
            TopicConfiguration {
                requires_duplicate_detection: Some(true),
                max_message_bytes: Some(4_096),
                ..Default::default()
            },
        ),
        subscription_patch(
            "Orders/subscriptions/Alpha",
            SubscriptionConfiguration {
                requires_session: Some(true),
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..Default::default()
            },
        ),
    ] {
        code(node.update(request).await, Code::FailedPrecondition);
        node.unchanged(&before, writes)?;
    }
    for request in [
        topic_patch("missing", TopicConfiguration::default()),
        subscription_patch(
            "Orders/subscriptions/missing",
            SubscriptionConfiguration::default(),
        ),
    ] {
        code(node.update(request).await, Code::NotFound);
        node.unchanged(&before, writes)?;
    }
    node.clock.set(0);
    for request in [
        topic_patch(
            "Orders",
            TopicConfiguration {
                max_message_bytes: Some(4_096),
                ..Default::default()
            },
        ),
        subscription_patch(
            "Orders/subscriptions/Alpha",
            SubscriptionConfiguration {
                max_delivery_count: Some(7),
                ..Default::default()
            },
        ),
    ] {
        code(node.update(request).await, Code::Unavailable);
        node.unchanged(&before, writes)?;
    }
    node.get("Orders").await?;
    node.get("Orders/subscriptions/Alpha").await?;
    node.unchanged(&before, writes)?;
    Ok(())
}

async fn failed_topic_and_subscription_updates_retry_after_reopen_without_partial_configs<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    node.create(topic("Orders", None)).await?;
    node.create(subscription("Orders", "Alpha", None)?).await?;
    for (index, request) in [
        topic_patch(
            "Orders",
            TopicConfiguration {
                max_message_bytes: Some(4_096),
                ..Default::default()
            },
        ),
        subscription_patch(
            "Orders/subscriptions/Alpha",
            SubscriptionConfiguration {
                max_delivery_count: Some(7),
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..Default::default()
            },
        ),
    ]
    .into_iter()
    .enumerate()
    {
        node.clock.set(2_000 + index as u64);
        let before = node.snapshot()?;
        let writes = node.writes();
        let applied = node.broker.handle().last_applied_blocking()?;
        let old = node.get(&request.path).await?;
        node.store
            .observations
            .fail_next
            .store(true, Ordering::SeqCst);
        code(node.update(request.clone()).await, Code::Internal);
        assert_eq!(node.writes(), writes + 1);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
        assert_eq!(node.get(&request.path).await?, old);
        node = node.reopen()?;
        assert_eq!(node.snapshot()?, before);
        node.clock.set(2_000 + index as u64);
        let updated = node.update(request).await?;
        assert_ne!(updated, old);
        assert_eq!(node.writes(), 1);
        assert_eq!(node.get(&updated.path).await?, updated);
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::MemoryProvider::new())).await?
            })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::DurableProvider::temporary()?)).await?
            })+ }
    };
}

for_each_backend! {
    partial_updates_preserve_presence_projections_rules_and_read_only_queries,
    invalid_patch_families_immutability_and_regressed_clock_leave_state_unchanged,
    failed_topic_and_subscription_updates_retry_after_reopen_without_partial_configs,
}
