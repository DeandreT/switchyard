use super::*;

async fn local_validation_missing_parents_and_occupancy_never_mutate_topology<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(topic("Orders", None)).await?;
    node.create(queue("work")).await?;
    node.create(subscription("Orders", "Alpha", None)?).await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.set(2_000);
    let mut bad = vec![
        topic(
            "invalid-topic",
            Some(TopicConfiguration {
                max_message_bytes: Some(0),
                ..TopicConfiguration::default()
            }),
        ),
        topic(
            "invalid-topic",
            Some(TopicConfiguration {
                default_time_to_live: Some(
                    topic_configuration::DefaultTimeToLive::DefaultTtlMillis(0),
                ),
                ..TopicConfiguration::default()
            }),
        ),
        topic(
            "invalid-topic",
            Some(TopicConfiguration {
                duplicate_detection_history_time_window_millis: Some(0),
                ..TopicConfiguration::default()
            }),
        ),
        subscription(
            "Orders",
            "badlock",
            Some(SubscriptionConfiguration {
                lock_duration_millis: Some(0),
                ..SubscriptionConfiguration::default()
            }),
        )?,
        subscription(
            "Orders",
            "baddelivery",
            Some(SubscriptionConfiguration {
                max_delivery_count: Some(0),
                ..SubscriptionConfiguration::default()
            }),
        )?,
        subscription(
            "Orders",
            "badttl",
            Some(SubscriptionConfiguration {
                default_time_to_live: Some(
                    subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(0),
                ),
                ..SubscriptionConfiguration::default()
            }),
        )?,
        subscription(
            "Orders",
            "badsize",
            Some(SubscriptionConfiguration {
                max_message_bytes: Some(0),
                ..SubscriptionConfiguration::default()
            }),
        )?,
        topic("Orders/subscriptions/child", None),
        queue("Orders/subscriptions/child"),
        topic("Orders/$DeadLetterQueue", None),
    ];
    for kind in [EntityKind::Topic, EntityKind::Subscription] {
        let mut request = if kind == EntityKind::Topic {
            topic("mixed", None)
        } else {
            subscription("Orders", "mixed", None)?
        };
        request.queue_config = Some(QueueConfiguration::default());
        bad.push(request);
        for field in ["lock", "ttl", "delivery", "session"] {
            let mut request = if kind == EntityKind::Topic {
                topic("legacy", None)
            } else {
                subscription("Orders", "legacy", None)?
            };
            match field {
                "lock" => request.lock_duration_millis = 30_000,
                "ttl" => request.default_ttl_millis = 20_000,
                "delivery" => request.max_delivery_count = 7,
                _ => request.requires_session = true,
            }
            bad.push(request);
        }
    }
    let mut wrong = queue("mixed");
    wrong.topic_config = Some(TopicConfiguration::default());
    bad.push(wrong);
    let mut wrong = topic("mixed", None);
    wrong.subscription_config = Some(SubscriptionConfiguration::default());
    bad.push(wrong);
    let mut wrong = subscription("Orders", "mixed", None)?;
    wrong.topic_config = Some(TopicConfiguration::default());
    bad.push(wrong);
    let mut wrong = subscription("Orders", "missing", None)?;
    wrong.path = "Orders".into();
    bad.push(wrong);
    for path in [
        "Orders/subscriptions/-bad",
        "Orders/subscriptions/Alpha/extra",
        "Orders/subscriptions/Alpha/subscriptions/child",
        "Orders/subscriptions/Alpha/$deadletterqueue",
    ] {
        let mut request = subscription("Orders", "unused", None)?;
        request.path = path.into();
        bad.push(request);
    }
    for request in bad {
        let reads = node.reads();
        code(node.create(request).await, Code::InvalidArgument);
        assert_eq!(
            node.reads(),
            reads,
            "invalid input must not reach the owner"
        );
        node.unchanged(&before, writes)?;
    }
    for path in [
        "Orders/subscriptions/-bad",
        "Orders/subscriptions/Alpha/extra",
        "Orders/subscriptions/Alpha/$DeadLetterQueue",
    ] {
        let reads = node.reads();
        code(node.get(path).await, Code::InvalidArgument);
        assert_eq!(node.reads(), reads);
        node.unchanged(&before, writes)?;
    }
    code(
        node.create(subscription("missing", "Alpha", None)?).await,
        Code::NotFound,
    );
    code(
        node.get("Orders/subscriptions/missing").await,
        Code::NotFound,
    );
    code(node.get("missing").await, Code::NotFound);
    for request in [
        topic("Orders", None),
        queue("Orders"),
        topic("work", None),
        subscription("Orders", "Alpha", None)?,
    ] {
        code(node.create(request).await, Code::AlreadyExists);
        node.unchanged(&before, writes)?;
    }
    for path in ["Orders", "Orders/subscriptions/Alpha"] {
        code(
            tokio::time::timeout(
                DEADLINE,
                node.service
                    .update_entity(Request::new(UpdateEntityRequest {
                        namespace: "tenant".into(),
                        path: path.into(),
                        queue_config: Some(QueueConfiguration::default()),
                        ..Default::default()
                    })),
            )
            .await?,
            Code::InvalidArgument,
        );
        code(
            tokio::time::timeout(
                DEADLINE,
                node.service
                    .delete_entity(Request::new(admin_api::v1::DeleteEntityRequest {
                        namespace: "tenant".into(),
                        path: path.into(),
                        kind: EntityKind::Queue as i32,
                    })),
            )
            .await?,
            Code::InvalidArgument,
        );
        node.unchanged(&before, writes)?;
    }
    node.unchanged(&before, writes)?;
    Ok(())
}

async fn membership_and_composed_path_limits_leave_byte_identical_failures<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(topic("full", None)).await?;
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        node.create(subscription("full", &format!("member{index:02}"), None)?)
            .await?;
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.set(2_000);
    code(
        node.create(subscription("full", "overflow", None)?).await,
        Code::ResourceExhausted,
    );
    node.unchanged(&before, writes)?;
    let length = domain::MAX_ENTITY_PATH_BYTES
        - domain::SUBSCRIPTION_PATH_SEGMENT.len()
        - 1
        - domain::DEAD_LETTER_QUEUE_SUFFIX.len();
    let boundary = "a".repeat(length);
    node.create(topic(&boundary, None)).await?;
    let created = node.create(subscription(&boundary, "b", None)?).await?;
    assert_eq!(
        format!("{}{}", created.path, domain::DEAD_LETTER_QUEUE_SUFFIX).len(),
        domain::MAX_ENTITY_PATH_BYTES
    );
    let too_long = "c".repeat(length + 1);
    node.create(topic(&too_long, None)).await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    code(
        node.create(subscription(&too_long, "b", None)?).await,
        Code::InvalidArgument,
    );
    node.unchanged(&before, writes)?;
    Ok(())
}

async fn one_commit_failure_retry_and_reopen_create_no_partial_subscription<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let before = node.snapshot()?;
    let writes = node.writes();
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    code(node.create(topic("Orders", None)).await, Code::Internal);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let created = node.create(topic("Orders", None)).await?;
    kind(&created, EntityKind::Topic);
    let before = node.snapshot()?;
    let writes = node.writes();
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    code(
        node.create(subscription("Orders", "Alpha", None)?).await,
        Code::Internal,
    );
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    node.store.observations.puts.lock().expect("puts").clear();
    let writes = node.writes();
    let created = node.create(subscription("Orders", "Alpha", None)?).await?;
    kind(&created, EntityKind::Subscription);
    assert_eq!(node.writes(), writes + 1);
    let namespace = NamespaceName::new("tenant")?;
    let parent = EntityPath::new("Orders")?;
    let name = SubscriptionName::new("Alpha")?;
    let child = parent.subscription(&name)?;
    {
        let puts = node.store.observations.puts.lock().expect("puts");
        for key in [
            keys::subscription(&namespace, &parent, &name),
            keys::queue_config(&namespace, &child),
            keys::queue_config(&namespace, &child.dead_letter_queue()?),
        ] {
            assert_eq!(puts.iter().filter(|put| **put == key).count(), 1);
        }
        assert!(!puts.contains(&keys::queue_counters(&namespace, &child)));
    }
    assert_eq!(node.get(&created.path).await?, created);
    Ok(())
}

async fn late_topology_corruption_fails_whole_reads_without_partial_pages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(topic("Orders", None)).await?;
    for name in ["Alpha", "zulu"] {
        node.create(subscription("Orders", name, None)?).await?;
    }
    let namespace = NamespaceName::new("tenant")?;
    let parent = EntityPath::new("Orders")?;
    let zulu = parent.subscription(&SubscriptionName::new("zulu")?)?;
    let shadow_key = keys::queue_config(&namespace, &zulu.dead_letter_queue()?);
    let shadow = node.store.get(&shadow_key)?.expect("shadow config");
    node.store
        .apply(WriteBatch::default().delete(shadow_key.clone()))?;
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.set(0);
    code(
        super::paging::page(&node, list(EntityKind::Subscription, "Orders", 1, "")).await,
        Code::Internal,
    );
    code(node.get("Orders").await, Code::Internal);
    node.unchanged(&before, writes)?;
    node.store
        .apply(WriteBatch::default().put(shadow_key, shadow))?;
    let mut malformed = keys::subscription(&namespace, &parent, &SubscriptionName::new("zulu")?);
    malformed.push(b'x');
    node.store.apply(WriteBatch::default().put(
        malformed.clone(),
        codec::encode(&SubscriptionConfig::default())?,
    ))?;
    let before = node.snapshot()?;
    let writes = node.writes();
    code(
        super::paging::page(&node, list(EntityKind::Subscription, "Orders", 1, "")).await,
        Code::Internal,
    );
    node.unchanged(&before, writes)?;
    node.store
        .apply(WriteBatch::default().delete(malformed).put(
            keys::topic_config(&namespace, &zulu),
            codec::encode(&TopicConfig::default())?,
        ))?;
    let before = node.snapshot()?;
    let writes = node.writes();
    code(node.get(zulu.as_str()).await, Code::Internal);
    node.unchanged(&before, writes)?;
    node.store.apply(
        WriteBatch::default()
            .delete(keys::topic_config(&namespace, &zulu))
            .put(
                keys::topic_config(&namespace, &parent),
                codec::encode(&TopicConfig {
                    max_message_bytes: 0,
                    ..TopicConfig::default()
                })?,
            ),
    )?;
    let before = node.snapshot()?;
    let writes = node.writes();
    code(node.get("Orders").await, Code::Internal);
    node.unchanged(&before, writes)?;
    node.store
        .apply(WriteBatch::default().put(keys::topic_config(&namespace, &parent), vec![255]))?;
    let before = node.snapshot()?;
    let writes = node.writes();
    code(node.get("Orders").await, Code::Internal);
    node.unchanged(&before, writes)?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    local_validation_missing_parents_and_occupancy_never_mutate_topology,
    membership_and_composed_path_limits_leave_byte_identical_failures,
    one_commit_failure_retry_and_reopen_create_no_partial_subscription,
    late_topology_corruption_fails_whole_reads_without_partial_pages,
}
