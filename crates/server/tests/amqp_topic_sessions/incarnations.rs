use domain::{DeleteEntityTarget, QueueConfig};

use super::*;

fn peek_body() -> OrderedMap<Value, Value> {
    map([
        (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
        (protocol_amqp::MESSAGE_COUNT, Value::Int(1)),
    ])
}

fn stale_transfer(outcome: Outcome) {
    let Outcome::Rejected(rejected) = outcome else {
        panic!("stale endpoint accepted: {outcome:?}")
    };
    assert_eq!(
        rejected.error.expect("stale error").condition.as_symbol(),
        Symbol::from(protocol_amqp::NOT_FOUND)
    );
}

async fn open_producers_cannot_cross_primary_kind_recreation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let path = EntityPath::new("healthy")?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut old_queue = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "old-queue", path.as_str()),
    )
    .await??;
    node.delete_entity(&path, DeleteEntityTarget::Queue).await?;
    node.submit_entity(
        &path,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    let before = node.snapshot()?;
    stale_transfer(timeout(DEADLINE, old_queue.send(message("wrong-new-topic", None))).await??);
    assert_eq!(node.snapshot()?, before);
    let mut old_topic = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "old-topic", path.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, old_topic.send(message("current-topic", None))).await??);
    node.delete_entity(&path, DeleteEntityTarget::Topic).await?;
    node.submit_entity(
        &path,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    let before = node.snapshot()?;
    stale_transfer(timeout(DEADLINE, old_topic.send(message("wrong-new-queue", None))).await??);
    stale_transfer(timeout(DEADLINE, old_queue.send(message("still-old-queue", None))).await??);
    assert_eq!(node.snapshot()?, before);
    let mut current = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "current-queue", path.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, current.send(message("current-queue", None))).await??);
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "current-consumer", path.as_str()),
    )
    .await??;
    let delivery = recv(&mut receiver).await?;
    assert_eq!(body(delivery.message()), b"current-queue");
    assert_eq!(
        sequence(delivery.message()),
        2,
        "topic and queue share the retained source fence"
    );
    timeout(DEADLINE, receiver.accept(&delivery)).await??;
    node.wait_removed(&path, 2).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn old_management_pair_cannot_peek_or_read_replacement_rules<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let address = format!("{}/$management", node.ordinary);
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut old = Management::attach(&mut session, "old-member", &address).await?;
    node.delete_entity(
        &node.topic,
        DeleteEntityTarget::Subscription {
            name: SubscriptionName::new("ordinary")?,
        },
    )
    .await?;
    node.submit_entity(
        &node.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("ordinary")?,
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "replacement-publisher", node.topic.as_str()),
    )
    .await??;
    accepted(
        timeout(
            DEADLINE,
            publisher.send(message("replacement-only", Some("A"))),
        )
        .await??,
    );
    let before = node.snapshot()?;
    for (id, operation, body) in [
        (
            "old-peek",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
        ),
        (
            "old-rules",
            protocol_amqp::ENUMERATE_RULES_OPERATION,
            map([("top", Value::Int(1)), ("skip", Value::Int(0))]),
        ),
    ] {
        let response = old.request_unassociated(id, operation, body).await?;
        status(&response, 404);
        assert_eq!(response.body, Body::Value(Value::Null));
        assert_eq!(node.snapshot()?, before);
    }
    let mut current = Management::attach(&mut session, "current-member", &address).await?;
    let response = current
        .request_unassociated(
            "current-peek",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
        )
        .await?;
    status(&response, 200);
    assert_eq!(body(&decoded(&response)?), b"replacement-only");
    assert_eq!(
        node.snapshot()?,
        before,
        "peek remains clock-free in storage"
    );
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn new_management_requests_refuse_old_reply_routes_before_owner_work<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let path = EntityPath::new("healthy")?;
    let address = "healthy/$management";
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut old_responses = timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name("old-route")
            .source(address)
            .target("old-route-address")
            .attach(&mut session),
    )
    .await??;
    node.delete_entity(&path, DeleteEntityTarget::Queue).await?;
    node.submit_entity(
        &path,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    let mut producer = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "new-route-data", path.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, producer.send(message("new-route-secret", None))).await??);
    let mut requests = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "new-route-requests", address),
    )
    .await??;
    let before = node.snapshot()?;
    let submits = node.submissions();
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("wrong-route".into()),
            reply_to: Some("old-route-address".into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                )
                .build(),
        )
        .body(Body::Value(Value::Map(peek_body())))
        .build();
    stale_transfer(timeout(DEADLINE, requests.send(request)).await??);
    assert_eq!(
        node.submissions(),
        submits,
        "route mismatch must not ask the owner"
    );
    assert_eq!(node.snapshot()?, before);
    assert!(
        timeout(Duration::from_millis(100), old_responses.recv())
            .await
            .is_err(),
        "replacement data reached an old route"
    );
    let mut current = Management::attach(&mut session, "good-route", address).await?;
    let response = current
        .request_unassociated(
            "current-route",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
        )
        .await?;
    status(&response, 200);
    assert_eq!(body(&decoded(&response)?), b"new-route-secret");
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn fresh_management_rejects_associated_receivers_from_old_subscriptions<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "identity-publisher", node.topic.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, publisher.send(message("old-held", Some("A")))).await??);
    let mut old = receiving(
        &mut session,
        "old-associated",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    let _delivery = recv(&mut old).await?;
    node.delete_entity(
        &node.topic,
        DeleteEntityTarget::Subscription {
            name: SubscriptionName::new("Alpha")?,
        },
    )
    .await?;
    node.submit_entity(
        &node.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("Alpha")?,
            config: SubscriptionConfig {
                requires_session: true,
                lock_duration_millis: 30_000,
                ..SubscriptionConfig::default()
            },
        },
    )
    .await?;
    let mut management = Management::attach(
        &mut session,
        "fresh-associated",
        &format!("{}/$management", node.alpha),
    )
    .await?;
    let before = node.snapshot()?;
    let submits = node.submissions();
    for (id, operation) in [
        (
            "old-session-state",
            protocol_amqp::GET_SESSION_STATE_OPERATION,
        ),
        (
            "old-session-renew",
            protocol_amqp::RENEW_SESSION_LOCK_OPERATION,
        ),
    ] {
        let response = management
            .request(id, operation, "old-associated", session_body("A"))
            .await?;
        status(&response, 410);
        assert_eq!(
            node.submissions(),
            submits,
            "old associated identity must be refused before owner work"
        );
        assert_eq!(node.snapshot()?, before);
    }
    accepted(timeout(DEADLINE, publisher.send(message("new-held", Some("A")))).await??);
    let mut current = receiving(
        &mut session,
        "current-associated",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    let hold = node.session(&node.alpha, "A")?.lock.expect("current hold");
    let delivery = recv(&mut current).await?;
    assert_eq!(body(delivery.message()), b"new-held");
    timeout(DEADLINE, old.close()).await??;
    assert_eq!(
        node.session(&node.alpha, "A")?.lock,
        Some(hold),
        "old cleanup changed the current hold"
    );
    let response = management
        .request(
            "current-session",
            protocol_amqp::GET_SESSION_STATE_OPERATION,
            "current-associated",
            session_body("A"),
        )
        .await?;
    status(&response, 200);
    timeout(DEADLINE, current.accept(&delivery)).await??;
    node.wait_removed(&node.alpha, 2).await?;
    timeout(DEADLINE, current.close()).await??;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

for_each_backend!(
    open_producers_cannot_cross_primary_kind_recreation,
    old_management_pair_cannot_peek_or_read_replacement_rules,
    new_management_requests_refuse_old_reply_routes_before_owner_work,
    fresh_management_rejects_associated_receivers_from_old_subscriptions,
);
