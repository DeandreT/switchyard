use domain::{CommandKind, CommandOutcome, EntityPath, SequenceNumber, SubscriptionName};

use super::*;

const TOPIC: &str = "Orders/$Management";
const MEMBER: &str = "Subscriptions";
const SIBLING: &str = "Beta";
const QUEUE: &str = "Orders/$management";

async fn send(node: &Node, entity: &str, id: &str) -> TestResult<SequenceNumber> {
    let outcome = node
        .broker
        .handle()
        .submit(
            NamespaceName::new("tenant")?,
            EntityPath::new(entity)?,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: id.as_bytes().to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )
        .await?;
    let CommandOutcome::Sent { sequence } = outcome else {
        panic!("ordinary send")
    };
    Ok(sequence)
}

async fn completed(node: &Node, command: &[&str]) -> TestResult {
    assert_eq!(
        node.json(command).await?,
        serde_json::json!({
            "operation_id": "", "state": "completed", "error": "",
        })
    );
    Ok(())
}

async fn refused(node: &Node, command: &[&str], status: &str) -> TestResult {
    let before = node.store.snapshot()?;
    let time = StateMachine::new(node.store.clone()).last_applied_time()?;
    let output = node.run(command).await?;
    failed(&output);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(status),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains(KEY));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("SharedAccessSignature"));
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(
        StateMachine::new(node.store.clone()).last_applied_time()?,
        time
    );
    Ok(())
}

fn scoped_token(path: &str) -> String {
    let audience = format!("amqps://{HOST}/{path}");
    let resource = byte_serialize(audience.as_bytes()).collect::<String>();
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

async fn deletion_roundtrip(tls: bool) -> TestResult {
    let node = Node::start(tls).await?;
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new(TOPIC)?;
    let member = SubscriptionName::new(MEMBER)?;
    let alpha = topic.subscription(&member)?;
    let beta = topic.subscription(&SubscriptionName::new(SIBLING)?)?;
    let queue = EntityPath::new(QUEUE)?;
    let machine = StateMachine::new(node.store.clone());
    let compatibility = node.json(&["compatibility"]).await?;
    for family in [
        "queue_operations",
        "topic_operations",
        "subscription_operations",
    ] {
        assert_eq!(
            compatibility[family],
            serde_json::json!(["create", "get", "list", "update", "delete"])
        );
    }
    node.json(&["topic", "create", TOPIC]).await?;
    node.json(&["subscription", "create", TOPIC, MEMBER])
        .await?;
    node.json(&["subscription", "create", TOPIC, SIBLING])
        .await?;
    node.json(&["queue", "create", QUEUE]).await?;
    let first = send(&node, TOPIC, "old-topic-copy").await?;
    let queue_first = send(&node, QUEUE, "old-queue-message").await?;
    assert!(machine.message(&namespace, &alpha, first)?.is_some());
    let sibling_message = machine.message(&namespace, &beta, first)?.unwrap();
    refused(&node, &["queue", "delete", TOPIC], "InvalidArgument").await?;
    refused(&node, &["topic", "delete", QUEUE], "InvalidArgument").await?;
    if tls {
        std::fs::write(
            node.credentials.path().join("token"),
            scoped_token(alpha.as_str()),
        )?;
        refused(&node, &["topic", "delete", TOPIC], "PermissionDenied").await?;
        refused(
            &node,
            &["subscription", "delete", TOPIC, SIBLING],
            "PermissionDenied",
        )
        .await?;
        refused(&node, &["queue", "delete", QUEUE], "PermissionDenied").await?;
        refused(&node, &["subscription", "list", TOPIC], "PermissionDenied").await?;
    }
    completed(&node, &["subscription", "delete", TOPIC, MEMBER]).await?;
    assert!(machine.message(&namespace, &alpha, first)?.is_none());
    assert!(machine.queue_config(&namespace, &alpha)?.is_none());
    assert!(
        machine
            .queue_config(&namespace, &alpha.dead_letter_queue()?)?
            .is_none()
    );
    assert_eq!(
        machine.message(&namespace, &beta, first)?,
        Some(sibling_message)
    );
    assert!(machine.message(&namespace, &queue, queue_first)?.is_some());
    refused(&node, &["subscription", "get", TOPIC, MEMBER], "NotFound").await?;
    refused(
        &node,
        &["subscription", "delete", TOPIC, MEMBER],
        "NotFound",
    )
    .await?;
    if tls {
        std::fs::write(node.credentials.path().join("token"), token())?;
    }
    assert_eq!(node.json(&["topic", "get", TOPIC]).await?["path"], TOPIC);
    let listed = node.json(&["subscription", "list", TOPIC]).await?;
    assert_eq!(listed["entities"].as_array().unwrap().len(), 1);
    assert_eq!(listed["entities"][0]["path"], beta.as_str());
    node.json(&["subscription", "create", TOPIC, MEMBER])
        .await?;
    let second = send(&node, TOPIC, "new-child-copy").await?;
    assert!(second > first);
    assert!(machine.message(&namespace, &alpha, first)?.is_none());
    assert!(machine.message(&namespace, &alpha, second)?.is_some());
    assert!(machine.message(&namespace, &beta, first)?.is_some());
    assert!(machine.message(&namespace, &beta, second)?.is_some());
    completed(&node, &["topic", "delete", TOPIC]).await?;
    assert!(machine.message(&namespace, &alpha, second)?.is_none());
    assert!(machine.message(&namespace, &beta, first)?.is_none());
    assert!(machine.message(&namespace, &beta, second)?.is_none());
    for child in [&alpha, &beta] {
        assert!(machine.queue_config(&namespace, child)?.is_none());
        assert!(
            machine
                .queue_config(&namespace, &child.dead_letter_queue()?)?
                .is_none()
        );
    }
    refused(&node, &["topic", "get", TOPIC], "NotFound").await?;
    refused(&node, &["subscription", "get", TOPIC, SIBLING], "NotFound").await?;
    assert!(
        node.json(&["topic", "list"]).await?["entities"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(node.json(&["queue", "get", QUEUE]).await?["kind"], "queue");
    completed(&node, &["queue", "delete", QUEUE]).await?;
    assert!(machine.message(&namespace, &queue, queue_first)?.is_none());
    assert!(machine.queue_config(&namespace, &queue)?.is_none());
    assert!(
        machine
            .queue_config(&namespace, &queue.dead_letter_queue()?)?
            .is_none()
    );
    assert!(
        node.json(&["queue", "list"]).await?["entities"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    refused(&node, &["queue", "delete", QUEUE], "NotFound").await?;
    node.json(&["queue", "create", QUEUE]).await?;
    let queue_second = send(&node, QUEUE, "new-queue-message").await?;
    assert!(queue_second > queue_first);
    assert!(machine.message(&namespace, &queue, queue_first)?.is_none());
    assert!(machine.message(&namespace, &queue, queue_second)?.is_some());
    node.json(&["topic", "create", TOPIC]).await?;
    node.json(&["subscription", "create", TOPIC, MEMBER])
        .await?;
    let third = send(&node, TOPIC, "new-topic-copy").await?;
    assert!(third > second);
    assert!(machine.message(&namespace, &alpha, third)?.is_some());
    assert_eq!(
        node.json(&["subscription", "get", TOPIC, MEMBER]).await?["kind"],
        "subscription"
    );
    completed(&node, &["topic", "delete", TOPIC]).await?;
    completed(&node, &["queue", "delete", QUEUE]).await?;
    assert!(
        node.json(&["queue", "list"]).await?["entities"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        node.json(&["topic", "list"]).await?["entities"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_deletion_purges_and_recreates_each_typed_family() -> TestResult {
    timeout(DEADLINE * 4, deletion_roundtrip(false)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_deletion_uses_exact_literal_scopes_and_recreates_each_typed_family() -> TestResult {
    timeout(DEADLINE * 4, deletion_roundtrip(true)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_delete_arguments_fail_without_connecting() -> TestResult {
    timeout(DEADLINE, async {
        for command in [
            vec!["queue", "delete", "Orders", "--max-delivery-count", "2"],
            vec![
                "topic",
                "delete",
                "Orders",
                "--requires-duplicate-detection",
            ],
            vec![
                "subscription",
                "delete",
                "Orders",
                "Alpha",
                "--requires-session",
            ],
            vec!["subscription", "delete", "Orders", "bad/name"],
            vec!["topic", "delete", "Orders/$deadletterqueue"],
            vec!["queue", "delete", "Orders/Subscriptions/Alpha"],
            vec![
                "subscription",
                "delete",
                "Orders/Subscriptions/Alpha",
                "Beta",
            ],
        ] {
            let mut argv = vec![
                "--allow-insecure".into(),
                "--endpoint".into(),
                "http://127.0.0.1:1".into(),
            ];
            argv.extend(command.into_iter().map(str::to_owned));
            let output = run(argv).await?;
            failed(&output);
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(
                !error.contains("could not establish the administration connection"),
                "{error}"
            );
        }
        Ok(())
    })
    .await?
}
