use super::*;

use domain::{DeleteEntityTarget, ReceiveMode, keys};
use storage::WriteBatch;

const RULE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><Name>mode-check</Name><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><Parameters /></Filter></RuleDescription></content></entry>"#;
const UPDATE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" /></content></entry>"#;

fn restore<S: StateStore>(store: &S, key: Vec<u8>, value: Option<Vec<u8>>) -> TestResult {
    store.apply(match value {
        Some(value) => WriteBatch::default().put(key, value),
        None => WriteBatch::default().delete(key),
    })?;
    Ok(())
}

async fn stale_mode<P: StoreProvider>(
    node: &Node<P>,
    namespace: &NamespaceName,
) -> TestResult<Vec<u8>> {
    let topic = EntityPath::new("zz-mode-generation-two")?;
    seed_topic(node, topic.as_str()).await?;
    assert_eq!(
        timeout(
            DEADLINE,
            node.handle().submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Topic,
                }
            )
        )
        .await??,
        CommandOutcome::TopicDeleted,
    );
    seed_topic(node, topic.as_str()).await?;
    Ok(node
        .store
        .as_ref()
        .unwrap()
        .inner
        .get(&keys::topic_mode(namespace, &topic))?
        .unwrap())
}

async fn seed_retained<P: StoreProvider>(
    node: &Node<P>,
    namespace: &NamespaceName,
    topic: &EntityPath,
    child: &EntityPath,
) -> TestResult {
    for index in 0..3 {
        timeout(
            DEADLINE,
            node.handle().submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::Send {
                    message_id: format!("tls-mode-retained-{index}"),
                    body: vec![index; 19],
                    time_to_live_millis: Some(60_000),
                    session_id: None,
                },
            ),
        )
        .await??;
        if index < 2 {
            let CommandOutcome::Received(Some(delivery)) = timeout(
                DEADLINE,
                node.handle().submit(
                    namespace.clone(),
                    child.clone(),
                    CommandKind::Receive {
                        mode: ReceiveMode::PeekLock,
                        lock_duration_millis: None,
                        session: None,
                    },
                ),
            )
            .await??
            else {
                panic!("retained TLS mode fixture delivery");
            };
            if index == 0 {
                timeout(
                    DEADLINE,
                    node.handle().submit(
                        namespace.clone(),
                        child.clone(),
                        CommandKind::DeadLetter {
                            sequence: delivery.sequence,
                            lock_token: delivery.lock.expect("held delivery").token,
                            reason: "tls-mode-retained".into(),
                            description: "unchanged".into(),
                        },
                    ),
                )
                .await??;
            }
        }
    }
    Ok(())
}

async fn tls_parent_mode_corruption_refuses_subscription_and_rule_operations<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let child = topic.subscription(&SubscriptionName::new("worker")?)?;
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let (status, original_reply) = exchange(
            &node,
            Method::PUT,
            path,
            Some(management_token()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        seed_retained(&node, &namespace, &topic, &child).await?;
        let mode_key = keys::topic_mode(&namespace, &topic);
        let store = &node.store.as_ref().unwrap().inner;
        let valid = store.get(&mode_key)?.expect("created topic mode");
        let stale = stale_mode(&node, &namespace).await?;
        assert_ne!(stale, valid);
        let ghost = topic.subscription(&SubscriptionName::new("absent")?)?;
        let faults = [
            (mode_key.clone(), None),
            (mode_key, Some(vec![255])),
            (keys::topic_mode(&namespace, &topic), Some(stale)),
            (keys::topic_mode(&namespace, &child), Some(valid.clone())),
            (
                keys::topic_mode(&namespace, &child.dead_letter_queue()?),
                Some(valid.clone()),
            ),
            (
                keys::topic_mode(&namespace, &topic.dead_letter_queue()?),
                Some(valid.clone()),
            ),
            (keys::topic_mode(&namespace, &ghost), Some(valid)),
        ];
        for (key, value) in faults {
            let original = store.get(&key)?;
            restore(store, key.clone(), value)?;
            let before = node.snapshot()?;
            let effects = node.effects();
            let (status, body) = exchange(
                &node,
                Method::GET,
                path,
                Some(token(&format!("https://{HOST}"), "send", epoch() + 300)),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(!std::str::from_utf8(&body)?.contains("mode"));
            assert_eq!(
                node.effects(),
                effects,
                "Manage refusal precedes corrupt metadata reads"
            );
            assert_eq!(node.snapshot()?, before);
            for (method, target, body, update) in [
                (Method::GET, path, b"".as_slice(), false),
                (
                    Method::PUT,
                    "/orders/Subscriptions/new-child?api-version=2024-05",
                    DEFINITION,
                    false,
                ),
                (Method::PUT, path, UPDATE, true),
                (Method::DELETE, path, b"".as_slice(), false),
                (
                    Method::GET,
                    "/orders/Subscriptions/worker/Rules/$Default?api-version=2024-05",
                    b"".as_slice(),
                    false,
                ),
                (
                    Method::GET,
                    "/orders/Subscriptions/worker/Rules?api-version=2024-05&$skip=1000&$top=1",
                    b"".as_slice(),
                    false,
                ),
                (
                    Method::PUT,
                    "/orders/Subscriptions/worker/Rules/mode-check?api-version=2024-05",
                    RULE,
                    false,
                ),
                (
                    Method::DELETE,
                    "/orders/Subscriptions/worker/Rules/$Default?api-version=2024-05",
                    b"".as_slice(),
                    false,
                ),
            ] {
                let clock = node.clock.0.load(Ordering::SeqCst);
                let (status, body) = exchange(
                    &node,
                    method,
                    target,
                    Some(management_token()),
                    body,
                    update,
                )
                .await?;
                assert_eq!(
                    status,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "mode fault key {key:?}; target {target}"
                );
                let text = std::str::from_utf8(&body)?;
                assert!(text.contains("InternalError"));
                for secret in [
                    KEY,
                    "TopicCapacityCorrupt",
                    "mode-generation-two",
                    "tls-mode-retained",
                ] {
                    assert!(!text.contains(secret));
                }
                assert_eq!(
                    node.clock.0.load(Ordering::SeqCst),
                    clock,
                    "metadata refusal must not stamp"
                );
                assert_eq!(
                    node.snapshot()?,
                    before,
                    "corrupt metadata must not rewrite retained state or Clock"
                );
            }
            restore(store, key, original)?;
            let healthy = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, reply) = exchange(
                &node,
                Method::GET,
                path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(reply, original_reply);
            for target in [
                "/orders/Subscriptions/worker/Rules/$Default?api-version=2024-05",
                "/orders/Subscriptions/worker/Rules?api-version=2024-05&$skip=1000&$top=1",
            ] {
                let (status, _) = exchange(
                    &node,
                    Method::GET,
                    target,
                    Some(management_token()),
                    b"",
                    false,
                )
                .await?;
                assert_eq!(status, StatusCode::OK);
            }
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
            assert_eq!(node.snapshot()?, healthy);
        }
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn tls_absent_child_mode_scope_preserves_missing_topology_semantics<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let store = &node.store.as_ref().unwrap().inner;
        let key = keys::topic_mode(&namespace, &topic);
        let valid = store.get(&key)?.unwrap();
        store.apply(WriteBatch::default().delete(key.clone()))?;
        for parent in ["orders", "missing-parent"] {
            let before = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            for suffix in ["", "/Rules/$Default", "/Rules"] {
                let path = format!("/{parent}/Subscriptions/worker{suffix}?api-version=2024-05");
                let (status, _) = exchange(
                    &node,
                    Method::GET,
                    &path,
                    Some(management_token()),
                    b"",
                    false,
                )
                .await?;
                assert_eq!(
                    status,
                    StatusCode::NOT_FOUND,
                    "absent child must not validate an unrelated parent mode"
                );
                assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
                assert_eq!(node.snapshot()?, before);
            }
        }
        restore(store, key, Some(valid.clone()))?;
        let missing_parent = EntityPath::new("missing-parent")?;
        let orphan = keys::topic_mode(
            &namespace,
            &missing_parent.subscription(&SubscriptionName::new("worker")?)?,
        );
        store.apply(WriteBatch::default().put(orphan.clone(), valid))?;
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        for suffix in ["", "/Rules/$Default", "/Rules"] {
            let path = format!("/missing-parent/Subscriptions/worker{suffix}?api-version=2024-05");
            let (status, _) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "the missing child still refuses its own misplaced mode"
            );
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
            assert_eq!(node.snapshot()?, before);
        }
        store.apply(WriteBatch::default().delete(orphan))?;
        let before = node.snapshot()?;
        let (status, _) = exchange(
            &node,
            Method::GET,
            "/missing-parent/Subscriptions/worker?api-version=2024-05",
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        assert_eq!(node.snapshot()?, before);
        expected = Some(before);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

subscription_transport_backends! {
    tls_parent_mode_corruption_refuses_subscription_and_rule_operations,
    tls_absent_child_mode_scope_preserves_missing_topology_semantics,
}
