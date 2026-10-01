use domain::DeleteEntityTarget;

use super::*;

async fn detached_promptly(receiver: &mut ClientReceiver) -> TestResult {
    let result = timeout(Duration::from_secs(1), receiver.recv()).await?;
    assert!(
        matches!(result, Err(amqp::EngineError::RemoteDetached)),
        "{result:?}"
    );
    Ok(())
}

async fn healthy_connection(session: &mut ClientSession) -> TestResult {
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(session, "healthy-producer", "healthy"),
    )
    .await??;
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(session, "healthy-consumer", "healthy"),
    )
    .await??;
    accepted(timeout(DEADLINE, sender.send(message("still-open", None))).await??);
    let delivery = recv(&mut receiver).await?;
    assert_eq!(body(delivery.message()), b"still-open");
    timeout(DEADLINE, receiver.accept(&delivery)).await??;
    timeout(DEADLINE, receiver.close()).await??;
    timeout(DEADLINE, sender.close()).await??;
    Ok(())
}

async fn deleting_one_subscription_closes_waiting_receivers_without_disturbing_siblings<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "deleted-ordinary", node.ordinary.as_str()),
    )
    .await??;
    let shadow = node.ordinary.dead_letter_queue()?;
    let mut dead_letters = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "deleted-shadow", shadow.as_str()),
    )
    .await??;
    let mut sibling =
        receiving(&mut session, "healthy-beta", node.beta.as_str(), Some("B")).await?;
    node.wait_waiting(&node.ordinary).await?;
    node.wait_waiting(&shadow).await?;
    node.wait_waiting(&node.beta).await?;
    node.clock.set(2_000);
    assert_eq!(
        node.delete_entity(
            &node.topic,
            DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("ordinary")?
            }
        )
        .await?,
        CommandOutcome::SubscriptionDeleted
    );
    detached_promptly(&mut receiver).await?;
    detached_promptly(&mut dead_letters).await?;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "remaining-topic", node.topic.as_str()),
    )
    .await??;
    accepted(timeout(DEADLINE, sender.send(message("for-sibling", Some("B")))).await??);
    let delivery = recv(&mut sibling).await?;
    assert_eq!(body(delivery.message()), b"for-sibling");
    timeout(DEADLINE, sibling.accept(&delivery)).await??;
    node.wait_removed(&node.beta, 1).await?;
    timeout(DEADLINE, sibling.close()).await??;
    timeout(DEADLINE, sender.close()).await??;
    healthy_connection(&mut session).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn topic_cascade_closes_idle_session_and_ordinary_receivers<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut alpha = receiving(
        &mut session,
        "deleted-alpha",
        node.alpha.as_str(),
        Some("A"),
    )
    .await?;
    let mut beta = receiving(&mut session, "deleted-beta", node.beta.as_str(), Some("B")).await?;
    let mut ordinary = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "deleted-ordinary", node.ordinary.as_str()),
    )
    .await??;
    let shadow = node.alpha.dead_letter_queue()?;
    let mut dead_letters = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "deleted-alpha-shadow", shadow.as_str()),
    )
    .await??;
    for entity in [&node.alpha, &node.beta, &node.ordinary, &shadow] {
        node.wait_waiting(entity).await?;
    }
    let alpha_counter = node
        .store
        .get(&keys::queue_counters(&node.namespace, &node.alpha))?;
    let beta_counter = node
        .store
        .get(&keys::queue_counters(&node.namespace, &node.beta))?;
    node.clock.set(2_000);
    assert_eq!(
        node.delete_entity(&node.topic, DeleteEntityTarget::Topic)
            .await?,
        CommandOutcome::TopicDeleted
    );
    for receiver in [&mut alpha, &mut beta, &mut ordinary, &mut dead_letters] {
        detached_promptly(receiver).await?;
    }
    assert_eq!(
        node.store
            .get(&keys::queue_counters(&node.namespace, &node.alpha))?,
        alpha_counter
    );
    assert_eq!(
        node.store
            .get(&keys::queue_counters(&node.namespace, &node.beta))?,
        beta_counter
    );
    for (entity, id) in [(&node.alpha, "A"), (&node.beta, "B")] {
        assert!(
            node.store
                .get(&keys::session(
                    &node.namespace,
                    entity,
                    &domain::SessionId::new(id)?
                ))?
                .is_none()
        );
    }
    healthy_connection(&mut session).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn session_queue_deletion_closes_both_live_owner_and_dlq_waiters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start_for_peek(provider, true).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let entity = EntityPath::new("Sessions")?;
    let shadow = entity.dead_letter_queue()?;
    let mut owner = receiving(
        &mut session,
        "deleted-session-owner",
        entity.as_str(),
        Some("A"),
    )
    .await?;
    let mut dead_letters = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "deleted-session-shadow", shadow.as_str()),
    )
    .await??;
    node.wait_waiting(&entity).await?;
    node.wait_waiting(&shadow).await?;
    let counter = node
        .store
        .get(&keys::queue_counters(&node.namespace, &entity))?;
    node.clock.set(2_000);
    assert_eq!(
        node.delete_entity(&entity, DeleteEntityTarget::Queue)
            .await?,
        CommandOutcome::QueueDeleted
    );
    detached_promptly(&mut owner).await?;
    detached_promptly(&mut dead_letters).await?;
    assert_eq!(
        node.store
            .get(&keys::queue_counters(&node.namespace, &entity))?,
        counter
    );
    assert!(
        node.store
            .get(&keys::session(
                &node.namespace,
                &entity,
                &domain::SessionId::new("A")?
            ))?
            .is_none()
    );
    healthy_connection(&mut session).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

for_each_backend!(
    deleting_one_subscription_closes_waiting_receivers_without_disturbing_siblings,
    topic_cascade_closes_idle_session_and_ordinary_receivers,
    session_queue_deletion_closes_both_live_owner_and_dlq_waiters,
);
