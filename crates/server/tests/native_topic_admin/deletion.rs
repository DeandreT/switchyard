use admin_api::v1::{DeleteEntityRequest, Operation};
use domain::{CommandKind, QueueCounters, SequenceNumber};

use super::*;

fn request(path: &str, kind: EntityKind) -> DeleteEntityRequest {
    DeleteEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
    }
}

async fn remove<P: StoreProvider>(
    node: &Node<P>,
    path: &str,
    kind: EntityKind,
) -> Result<Operation, tonic::Status> {
    Ok(tokio::time::timeout(
        DEADLINE,
        node.service
            .delete_entity(Request::new(request(path, kind))),
    )
    .await
    .expect("bounded deletion")?
    .into_inner())
}

fn completed(operation: Operation) {
    assert_eq!(operation.state, "completed");
    assert!(
        operation.operation_id.is_empty(),
        "no asynchronous job is advertised"
    );
    assert!(operation.error.is_empty());
}

async fn synchronous_deletion_cascades_and_recreates_without_resetting_counters<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(queue("work")).await?;
    node.create(topic("Orders", None)).await?;
    for name in ["Accounting", "accounting"] {
        node.create(subscription("Orders", name, None)?).await?;
    }
    let namespace = NamespaceName::new("tenant")?;
    let work = EntityPath::new("work")?;
    node.broker
        .handle()
        .submit(
            namespace.clone(),
            work.clone(),
            CommandKind::Send {
                message_id: "old".into(),
                body: b"old".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )
        .await?;
    let counter_key = keys::queue_counters(&namespace, &work);
    let counters = node.store.get(&counter_key)?;
    assert!(counters.is_some());
    node.clock.set(2_000);
    let writes = node.writes();
    completed(remove(&node, "work", EntityKind::Unspecified).await?);
    assert_eq!(node.writes(), writes + 1);
    assert_eq!(node.store.get(&counter_key)?, counters);
    code(node.get("work").await, Code::NotFound);
    assert!(
        node.store
            .get(&keys::queue_config(&namespace, &work.dead_letter_queue()?))?
            .is_none()
    );
    node.create(queue("work")).await?;
    assert_eq!(node.store.get(&counter_key)?, counters);
    assert_eq!(
        node.broker
            .handle()
            .submit(
                namespace.clone(),
                work,
                CommandKind::Send {
                    message_id: "new".into(),
                    body: b"new".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                }
            )
            .await?,
        domain::CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );
    node.clock.set(2_001);
    let writes = node.writes();
    completed(
        remove(
            &node,
            "Orders/SuBsCrIpTiOnS/Accounting",
            EntityKind::Subscription,
        )
        .await?,
    );
    assert_eq!(node.writes(), writes + 1);
    code(
        node.get("Orders/subscriptions/Accounting").await,
        Code::NotFound,
    );
    node.get("Orders/subscriptions/accounting").await?;
    node.get("Orders").await?;
    node.create(subscription("Orders", "Accounting", None)?)
        .await?;
    node.clock.set(2_002);
    let writes = node.writes();
    completed(remove(&node, "Orders", EntityKind::Topic).await?);
    assert_eq!(node.writes(), writes + 1);
    for path in [
        "Orders",
        "Orders/subscriptions/Accounting",
        "Orders/subscriptions/accounting",
    ] {
        code(node.get(path).await, Code::NotFound);
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.set(0);
    node.get("work").await?;
    node.unchanged(&before, writes)?;
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    node.clock.set(0);
    node.get("work").await?;
    code(node.get("Orders").await, Code::NotFound);
    node.unchanged(&before, 0)?;
    node.clock.set(2_003);
    node.create(topic("Orders", None)).await?;
    node.create(subscription("Orders", "Accounting", None)?)
        .await?;
    Ok(())
}

async fn deletion_validates_shape_and_kind_without_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(queue("work")).await?;
    node.create(topic("Orders", None)).await?;
    node.create(subscription("Orders", "Accounting", None)?)
        .await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    for (path, kind) in [
        ("Orders", EntityKind::Subscription),
        ("Orders/subscriptions/Accounting", EntityKind::Queue),
        ("Orders/$deadletterqueue", EntityKind::Unspecified),
        (
            "Orders/subscriptions/Accounting/extra",
            EntityKind::Subscription,
        ),
    ] {
        let reads = node.reads();
        code(remove(&node, path, kind).await, Code::InvalidArgument);
        assert_eq!(node.reads(), reads, "shape errors remain local");
        node.unchanged(&before, writes)?;
    }
    let reads = node.reads();
    let mut invalid = request("Orders", EntityKind::Unspecified);
    invalid.kind = 99;
    code(
        node.service.delete_entity(Request::new(invalid)).await,
        Code::InvalidArgument,
    );
    assert_eq!(node.reads(), reads);
    for (path, kind, expected) in [
        ("Orders", EntityKind::Queue, Code::InvalidArgument),
        ("work", EntityKind::Topic, Code::InvalidArgument),
        ("missing", EntityKind::Queue, Code::NotFound),
        ("missing", EntityKind::Topic, Code::NotFound),
        (
            "Orders/subscriptions/missing",
            EntityKind::Unspecified,
            Code::NotFound,
        ),
        ("orders", EntityKind::Unspecified, Code::NotFound),
    ] {
        code(remove(&node, path, kind).await, expected);
        node.unchanged(&before, writes)?;
    }
    node.clock.set(0);
    code(
        remove(&node, "work", EntityKind::Queue).await,
        Code::Unavailable,
    );
    node.unchanged(&before, writes)?;
    node.get("work").await?;
    node.get("Orders").await?;
    node.clock.set(1_001);
    completed(
        remove(
            &node,
            "Orders/subscriptions/Accounting",
            EntityKind::Unspecified,
        )
        .await?,
    );
    let after = node.snapshot()?;
    let writes = node.writes();
    code(
        remove(
            &node,
            "Orders/subscriptions/Accounting",
            EntityKind::Subscription,
        )
        .await,
        Code::NotFound,
    );
    node.unchanged(&after, writes)?;
    Ok(())
}

async fn failed_deletion_commits_reopen_and_retry_atomically<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    for (index, (path, expected)) in [
        ("work", EntityKind::Queue),
        ("Orders/subscriptions/Accounting", EntityKind::Subscription),
        ("Orders", EntityKind::Topic),
    ]
    .into_iter()
    .enumerate()
    {
        match expected {
            EntityKind::Queue => {
                node.create(queue(path)).await?;
            }
            EntityKind::Subscription => {
                node.create(topic("Orders", None)).await?;
                node.create(subscription("Orders", "Accounting", None)?)
                    .await?;
            }
            _ => {
                node.create(subscription("Orders", "Remaining", None)?)
                    .await?;
            }
        }
        let before = node.snapshot()?;
        let writes = node.writes();
        let applied = node.broker.handle().last_applied_blocking()?;
        let stamp = 2_000 + index as u64;
        node.clock.set(stamp);
        node.store
            .observations
            .fail_next
            .store(true, Ordering::SeqCst);
        code(remove(&node, path, expected).await, Code::Internal);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.writes(), writes + 1);
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
        node.get(path).await?;
        node = node.reopen()?;
        assert_eq!(node.snapshot()?, before);
        node.clock.set(stamp);
        completed(remove(&node, path, expected).await?);
        assert_eq!(node.writes(), 1);
        assert_eq!(
            node.broker.handle().last_applied_blocking()?.as_millis(),
            stamp
        );
        code(node.get(path).await, Code::NotFound);
    }
    Ok(())
}

async fn oversized_and_dangling_deletions_are_typed_atomic_refusals<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create(queue("large")).await?;
    node.create(queue("broken")).await?;
    node.create(queue("bad-numeric-queue")).await?;
    node.create(topic("bad-numeric-topic", None)).await?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("large")?;
    let mut batch = WriteBatch::default();
    for sequence in 1..=domain::MAX_ENTITY_DELETE_KEYS as u64 {
        batch.push_put(
            keys::message(&namespace, &entity, SequenceNumber::new(sequence)),
            vec![0xFF],
        );
    }
    batch.push_put(
        keys::queue_counters(&namespace, &entity),
        codec::encode(&QueueCounters {
            next_sequence: domain::MAX_ENTITY_DELETE_KEYS as u64 + 1,
            next_lock_token: 1,
        })?,
    );
    let broken = EntityPath::new("broken")?;
    batch.push_delete(keys::queue_config(&namespace, &broken.dead_letter_queue()?));
    batch.push_put(
        keys::queue_config(&namespace, &EntityPath::new("bad-numeric-queue")?),
        codec::encode(&domain::QueueConfig {
            max_delivery_count: 0,
            ..Default::default()
        })?,
    );
    batch.push_put(
        keys::topic_config(&namespace, &EntityPath::new("bad-numeric-topic")?),
        codec::encode(&TopicConfig {
            max_message_bytes: 0,
            ..Default::default()
        })?,
    );
    node.store.inner.apply(batch)?;
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.set(2_000);
    code(
        remove(&node, "large", EntityKind::Queue).await,
        Code::ResourceExhausted,
    );
    node.unchanged(&before, writes)?;
    node.get("large").await?;
    code(
        remove(&node, "broken", EntityKind::Queue).await,
        Code::Internal,
    );
    node.unchanged(&before, writes)?;
    for (path, kind) in [
        ("bad-numeric-queue", EntityKind::Queue),
        ("bad-numeric-topic", EntityKind::Topic),
    ] {
        code(remove(&node, path, kind).await, Code::Internal);
        node.unchanged(&before, writes)?;
    }
    {
        let scans = node.store.observations.scans.lock().expect("scans");
        assert!(
            scans
                .iter()
                .filter(|(prefix, _)| prefix.starts_with(&keys::message_prefix(&namespace, &entity)))
                .all(|(_, limit)| *limit == 1)
        );
    }
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    node.clock.set(2_000);
    code(
        remove(&node, "large", EntityKind::Unspecified).await,
        Code::ResourceExhausted,
    );
    node.unchanged(&before, 0)?;
    Ok(())
}

async fn maximum_length_topics_and_missing_primary_paths_do_not_require_shadow_capacity<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let name = "t".repeat(260);
    let path = EntityPath::new(&name)?;
    assert!(path.dead_letter_queue().is_err());
    node.create(topic(&name, None)).await?;
    let namespace = NamespaceName::new("tenant")?;
    node.broker
        .handle()
        .submit(
            namespace.clone(),
            path.clone(),
            CommandKind::Schedule {
                messages: vec![domain::ScheduledMessage {
                    message_id: "future".into(),
                    body: b"retained".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    enqueue_at: domain::Timestamp::from_millis(10_000),
                }],
            },
        )
        .await?;
    let counter_key = keys::queue_counters(&namespace, &path);
    let counter = node.store.get(&counter_key)?;
    assert!(counter.is_some());
    node.clock.set(2_000);
    completed(remove(&node, &name, EntityKind::Topic).await?);
    assert_eq!(node.store.get(&counter_key)?, counter);
    assert!(
        node.store
            .scan_prefix(&keys::message_prefix(&namespace, &path), 1)?
            .is_empty()
    );
    let before = node.snapshot()?;
    let writes = node.writes();
    for kind in [
        EntityKind::Unspecified,
        EntityKind::Queue,
        EntityKind::Topic,
    ] {
        code(remove(&node, &name, kind).await, Code::NotFound);
        node.unchanged(&before, writes)?;
    }
    Ok(())
}

macro_rules! backends {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> TestResult {
            tokio::time::timeout(DEADLINE * 12, super::$case(testkit::MemoryProvider::new())).await?
        })+ use super::*; }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> TestResult {
            tokio::time::timeout(DEADLINE * 12, super::$case(testkit::DurableProvider::temporary()?)).await?
        })+ use super::*; }
    };
}

backends!(
    synchronous_deletion_cascades_and_recreates_without_resetting_counters,
    deletion_validates_shape_and_kind_without_mutation,
    failed_deletion_commits_reopen_and_retry_atomically,
    oversized_and_dangling_deletions_are_typed_atomic_refusals,
    maximum_length_topics_and_missing_primary_paths_do_not_require_shadow_capacity,
);
