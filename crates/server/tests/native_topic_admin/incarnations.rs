use domain::{EntityIncarnation, EntityIncarnationKind};

use super::*;

async fn exhausted_incarnations_refuse_native_creation_without_partial_topology<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    node.create(topic("Events", None)).await?;
    let namespace = NamespaceName::new("tenant")?;
    let owner = EntityPath::new("Events")?.subscription(&SubscriptionName::new("Alpha")?)?;
    let mut batch = WriteBatch::default();
    for (owner, kind) in [
        (EntityPath::new("work")?, EntityIncarnationKind::Queue),
        (
            EntityPath::new("publications")?,
            EntityIncarnationKind::Topic,
        ),
        (owner, EntityIncarnationKind::Subscription),
    ] {
        batch.push_put(
            keys::entity_incarnation(&namespace, &owner),
            codec::encode(&EntityIncarnation::new(u64::MAX, kind, true)?)?,
        );
    }
    node.store.apply(batch)?;
    for reopen in [false, true] {
        if reopen {
            node = node.reopen()?;
        }
        let before = node.snapshot()?;
        let writes = node.writes();
        for request in [
            queue("work"),
            topic("publications", None),
            subscription("Events", "Alpha", None)?,
        ] {
            code(node.create(request).await, Code::ResourceExhausted);
            node.unchanged(&before, writes)?;
        }
        code(node.get("work").await, Code::NotFound);
        code(node.get("publications").await, Code::NotFound);
        code(node.get("Events/subscriptions/Alpha").await, Code::NotFound);
        node.unchanged(&before, writes)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_exhausted_incarnations_refuse_native_creation() -> TestResult {
    exhausted_incarnations_refuse_native_creation_without_partial_topology(
        testkit::MemoryProvider::new(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_exhausted_incarnations_refuse_native_creation() -> TestResult {
    exhausted_incarnations_refuse_native_creation_without_partial_topology(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
