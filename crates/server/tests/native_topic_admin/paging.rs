use super::*;
use base64::{
    Engine,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct TestCursor {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(enumeration = "EntityKind", tag = "2")]
    kind: i32,
    #[prost(string, tag = "3")]
    parent_topic: String,
    #[prost(string, tag = "4")]
    after: String,
}

#[derive(Clone, PartialEq, Message)]
struct UnknownCursorField {
    #[prost(bool, tag = "5")]
    ignored: bool,
}

pub(super) async fn page<P: StoreProvider>(
    node: &Node<P>,
    request: ListEntitiesRequest,
) -> Result<admin_api::v1::ListEntitiesResponse, tonic::Status> {
    Ok(
        tokio::time::timeout(DEADLINE, node.service.list_entities(Request::new(request)))
            .await
            .expect("bounded list")?
            .into_inner(),
    )
}

async fn kind_filtered_pages_are_sorted_bounded_and_keep_legacy_queue_tokens<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for name in ["omega", "alpha"] {
        node.create(queue(name)).await?;
    }
    let expected: Vec<_> = (0..105).map(|index| format!("t{index:03}")).collect();
    for name in expected.iter().rev() {
        node.create(topic(name, None)).await?;
    }
    let names: Vec<_> = (0..domain::MAX_TOPIC_SUBSCRIPTIONS)
        .map(|index| format!("Member{index:02}"))
        .collect();
    for name in names.iter().rev() {
        node.create(subscription("t050", name, None)?).await?;
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    let first = page(&node, list(EntityKind::Unspecified, "", 1, "")).await?;
    assert_eq!(first.entities.len(), 1);
    assert_eq!(first.entities[0].path, "alpha");
    kind(&first.entities[0], EntityKind::Queue);
    assert!(first.next_page_token.starts_with("v1."));
    assert_eq!(
        page(&node, list(EntityKind::Queue, "", 1, "")).await?,
        first
    );
    let last = page(
        &node,
        list(EntityKind::Unspecified, "", 1, &first.next_page_token),
    )
    .await?;
    assert_eq!(last.entities.len(), 1);
    assert_eq!(last.entities[0].path, "omega");
    assert!(last.next_page_token.starts_with("queue.scan.v1."));
    let mut token = last.next_page_token;
    for _ in 0..4 {
        let result = page(&node, list(EntityKind::Unspecified, "", 1, &token)).await?;
        assert!(result.entities.is_empty());
        if result.next_page_token.is_empty() {
            token.clear();
            break;
        }
        assert!(result.next_page_token.starts_with("queue.scan.v1."));
        assert_ne!(result.next_page_token, token);
        token = result.next_page_token;
    }
    assert!(token.is_empty(), "hidden rows must reach exact exhaustion");
    let first = page(&node, list(EntityKind::Topic, "", 0, "")).await?;
    assert_eq!(first.entities.len(), 100);
    assert!(first.next_page_token.starts_with("topic.v1."));
    assert_eq!(
        first
            .entities
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>(),
        expected[..100]
    );
    for entry in &first.entities {
        kind(entry, EntityKind::Topic);
    }
    let last = page(
        &node,
        list(EntityKind::Topic, "", 0, &first.next_page_token),
    )
    .await?;
    assert_eq!(
        last.entities
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>(),
        expected[100..]
    );
    assert!(last.next_page_token.is_empty());
    let mut token = String::new();
    let mut paths = Vec::new();
    loop {
        let result = page(&node, list(EntityKind::Topic, "", 7, &token)).await?;
        assert!(!result.entities.is_empty());
        assert!(result.entities.len() <= 7);
        paths.extend(result.entities.into_iter().map(|entry| entry.path));
        token = result.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(paths, expected);
    let mut token = String::new();
    let mut members = Vec::new();
    loop {
        let result = page(&node, list(EntityKind::Subscription, "t050", 7, &token)).await?;
        assert!(!result.entities.is_empty());
        assert!(result.entities.len() <= 7);
        for entry in result.entities {
            kind(&entry, EntityKind::Subscription);
            members.push(entry.path);
        }
        token = result.next_page_token;
        if token.is_empty() {
            break;
        }
        assert!(token.starts_with("subscription.v1."));
    }
    assert_eq!(
        members,
        names
            .iter()
            .map(|name| format!("t050/subscriptions/{name}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        page(&node, list(EntityKind::Subscription, "t050", 0, ""))
            .await?
            .entities
            .len(),
        domain::MAX_TOPIC_SUBSCRIPTIONS
    );
    assert_eq!(
        page(&node, list(EntityKind::Subscription, "t050", 1_024, ""))
            .await?
            .entities
            .len(),
        domain::MAX_TOPIC_SUBSCRIPTIONS
    );
    assert_eq!(
        page(&node, list(EntityKind::Topic, "", 1_024, ""))
            .await?
            .entities
            .len(),
        expected.len()
    );
    assert!(
        page(&node, list(EntityKind::Subscription, "t051", 1, ""))
            .await?
            .entities
            .is_empty()
    );
    let membership_tag =
        keys::subscription_prefix(&NamespaceName::new("tenant")?, &EntityPath::new("t050")?)[0];
    let scans = node.store.observations.scans.lock().expect("scans");
    assert!(
        scans
            .iter()
            .any(|(prefix, limit)| prefix.first() == Some(&membership_tag)
                && *limit == domain::MAX_TOPIC_SUBSCRIPTIONS + 1)
    );
    assert!(
        scans
            .iter()
            .filter(|(prefix, _)| prefix.first() == Some(&membership_tag))
            .all(|(_, limit)| *limit <= domain::MAX_TOPIC_SUBSCRIPTIONS + 1)
    );
    drop(scans);
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

async fn cursors_bind_kind_parent_and_namespace_and_bad_filters_never_read_storage<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for name in ["alpha", "omega"] {
        node.create(queue(name)).await?;
    }
    for name in ["Orders", "Orders-extra"] {
        node.create(topic(name, None)).await?;
        for member in ["Alpha", "beta"] {
            node.create(subscription(name, member, None)?).await?;
        }
    }
    let queue_token = page(&node, list(EntityKind::Queue, "", 1, ""))
        .await?
        .next_page_token;
    let topic_token = page(&node, list(EntityKind::Topic, "", 1, ""))
        .await?
        .next_page_token;
    let child_token = page(&node, list(EntityKind::Subscription, "Orders", 1, ""))
        .await?
        .next_page_token;
    assert!(!queue_token.is_empty());
    assert!(!topic_token.is_empty());
    assert!(!child_token.is_empty());
    let before = node.snapshot()?;
    let writes = node.writes();
    let mut requests = vec![
        list(EntityKind::Topic, "", 1, &queue_token),
        list(EntityKind::Queue, "", 1, &topic_token),
        list(EntityKind::Subscription, "Orders", 1, &topic_token),
        list(EntityKind::Topic, "", 1, &child_token),
        list(EntityKind::Subscription, "Orders-extra", 1, &child_token),
        list(EntityKind::Subscription, "orders", 1, &child_token),
        list(EntityKind::Subscription, "", 1, ""),
        list(EntityKind::Queue, "Orders", 1, ""),
        list(EntityKind::Topic, "Orders", 1, ""),
        list(
            EntityKind::Subscription,
            "Orders/subscriptions/Alpha",
            1,
            "",
        ),
        list(EntityKind::Topic, "", 1_025, ""),
        list(EntityKind::Topic, "", 1, "topic.v1.!"),
        list(EntityKind::Subscription, "Orders", 1, "subscription.v2.AA"),
        list(EntityKind::Topic, "", 1, &"x".repeat(513)),
    ];
    let mut unknown = list(EntityKind::Topic, "", 1, "");
    unknown.kind = 99;
    requests.push(unknown);
    for request in requests {
        let reads = node.reads();
        code(page(&node, request).await, Code::InvalidArgument);
        assert_eq!(
            node.reads(),
            reads,
            "invalid filters/cursors must not reach the owner"
        );
        node.unchanged(&before, writes)?;
    }
    let neighbor = NativeAdminService::new(node.broker.handle(), NamespaceName::new("neighbor")?);
    for (kind, parent, token) in [
        (EntityKind::Topic, "", &topic_token),
        (EntityKind::Subscription, "Orders", &child_token),
    ] {
        let mut request = list(kind, parent, 1, token);
        request.namespace = "neighbor".into();
        let reads = node.reads();
        code(
            tokio::time::timeout(DEADLINE, neighbor.list_entities(Request::new(request))).await?,
            Code::InvalidArgument,
        );
        assert_eq!(node.reads(), reads);
        node.unchanged(&before, writes)?;
    }
    let reads = node.reads();
    let mut request = list(EntityKind::Topic, "", 1, "");
    request.namespace = "neighbor".into();
    code(page(&node, request).await, Code::PermissionDenied);
    assert_eq!(node.reads(), reads);
    Ok(())
}

async fn noncanonical_cursor_encodings_are_refused_before_owner_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for name in ["OrdersA", "OrdersZ"] {
        node.create(topic(name, None)).await?;
    }
    for name in ["Alpha", "beta"] {
        node.create(subscription("OrdersA", name, None)?).await?;
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);

    for (kind, parent, prefix, first_path, last_path) in [
        (EntityKind::Topic, "", "topic.v1.", "OrdersA", "OrdersZ"),
        (
            EntityKind::Subscription,
            "OrdersA",
            "subscription.v1.",
            "OrdersA/subscriptions/Alpha",
            "OrdersA/subscriptions/beta",
        ),
    ] {
        let first = page(&node, list(kind, parent, 1, "")).await?;
        assert_eq!(first.entities.len(), 1);
        assert_eq!(first.entities[0].path, first_path);
        let encoded = first
            .next_page_token
            .strip_prefix(prefix)
            .expect("cursor family");
        let canonical = URL_SAFE_NO_PAD.decode(encoded)?;
        let cursor = TestCursor::decode(canonical.as_slice())?;
        assert_eq!(cursor.encode_to_vec(), canonical);

        let mut duplicate = canonical.clone();
        duplicate.extend(
            TestCursor {
                namespace: cursor.namespace.clone(),
                ..TestCursor::default()
            }
            .encode_to_vec(),
        );
        let mut unknown = canonical.clone();
        unknown.extend(UnknownCursorField { ignored: true }.encode_to_vec());
        let mut reordered = TestCursor {
            after: cursor.after.clone(),
            ..TestCursor::default()
        }
        .encode_to_vec();
        reordered.extend(
            TestCursor {
                after: String::new(),
                ..cursor.clone()
            }
            .encode_to_vec(),
        );

        let mut forged = Vec::new();
        for (label, bytes) in [
            ("duplicate protobuf field", duplicate),
            ("unknown protobuf field", unknown),
            ("reordered protobuf fields", reordered),
        ] {
            assert_ne!(bytes, canonical, "{label}");
            assert_eq!(TestCursor::decode(bytes.as_slice())?, cursor, "{label}");
            forged.push((label, format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes))));
        }
        let padded = URL_SAFE.encode(&canonical);
        assert_ne!(padded, encoded, "fixture requires base64 padding");
        assert_eq!(URL_SAFE.decode(&padded)?, canonical);
        forged.push(("noncanonical padded base64", format!("{prefix}{padded}")));

        for (label, token) in forged {
            let reads = node.reads();
            code(
                page(&node, list(kind, parent, 1, &token)).await,
                Code::InvalidArgument,
            );
            assert_eq!(node.reads(), reads, "{label} must not reach the owner");
            node.unchanged(&before, writes)?;
            assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
        }
        let last = page(&node, list(kind, parent, 1, &first.next_page_token)).await?;
        assert_eq!(last.entities.len(), 1);
        assert_eq!(last.entities[0].path, last_path);
        assert!(last.next_page_token.is_empty());
        node.unchanged(&before, writes)?;
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    kind_filtered_pages_are_sorted_bounded_and_keep_legacy_queue_tokens,
    cursors_bind_kind_parent_and_namespace_and_bad_filters_never_read_storage,
    noncanonical_cursor_encodings_are_refused_before_owner_reads,
}
