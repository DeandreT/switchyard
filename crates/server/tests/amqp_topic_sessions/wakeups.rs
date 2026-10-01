use super::*;

const PROMPT_DELIVERY: Duration = Duration::from_secs(1);
const IDLE_PROBE: Duration = Duration::from_millis(100);

async fn publish_promptly(
    sender: &mut ClientSender,
    receiver: &mut ClientReceiver,
    text: &str,
    id: &str,
) -> TestResult<ClientDelivery> {
    timeout(PROMPT_DELIVERY, async {
        accepted(sender.send(message(text, Some(id))).await?);
        Ok::<_, Box<dyn Error>>(receiver.recv().await?)
    })
    .await?
}

async fn idle_session_waiters_both_observe_publication<P: StoreProvider>(
    provider: P,
    subscription: bool,
) -> TestResult {
    let node = Node::start_for_peek(provider, !subscription).await?;
    let entity = if subscription {
        node.alpha.clone()
    } else {
        EntityPath::new("Sessions")?
    };
    let publishing_entity = if subscription { &node.topic } else { &entity };
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "wakeup-publisher", publishing_entity.as_str()),
    )
    .await??;
    let mut a = receiving(&mut session, "waiting-A", entity.as_str(), Some("A")).await?;
    node.wait_waiter_count(&entity, 1).await?;
    let a_hold = node.session(&entity, "A")?.lock.expect("A hold");
    let mut b = receiving(&mut session, "waiting-B", entity.as_str(), Some("B")).await?;
    node.wait_waiter_count(&entity, 2).await?;
    let b_hold = node.session(&entity, "B")?.lock.expect("B hold");
    assert_eq!(granted(&a), Some("A"));
    assert_eq!(granted(&b), Some("B"));

    // A registered first, but only B can consume the committed publication.
    let delivery = publish_promptly(&mut sender, &mut b, "for-B", "B").await?;
    assert_eq!(body(delivery.message()), b"for-B");
    assert_eq!(group(delivery.message()), Some("B"));
    assert_eq!(sequence(delivery.message()), 1);
    assert!(timeout(IDLE_PROBE, a.recv()).await.is_err());
    assert_eq!(node.session(&entity, "A")?.lock, Some(a_hold));
    assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));
    timeout(DEADLINE, b.accept(&delivery)).await??;
    node.wait_removed(&entity, 1).await?;
    node.wait_waiter_count(&entity, 2).await?;

    let delivery = publish_promptly(&mut sender, &mut a, "for-A", "A").await?;
    assert_eq!(body(delivery.message()), b"for-A");
    assert_eq!(group(delivery.message()), Some("A"));
    assert_eq!(sequence(delivery.message()), 2);
    assert!(timeout(IDLE_PROBE, b.recv()).await.is_err());
    assert_eq!(node.session(&entity, "A")?.lock, Some(a_hold));
    assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));
    timeout(DEADLINE, a.accept(&delivery)).await??;
    node.wait_removed(&entity, 2).await?;
    node.wait_waiter_count(&entity, 2).await?;
    timeout(DEADLINE, a.close()).await??;
    node.wait_released(&entity, "A").await?;
    timeout(DEADLINE, b.close()).await??;
    node.wait_released(&entity, "B").await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn required_queue_session_waiters_receive_promptly<P: StoreProvider>(
    provider: P,
) -> TestResult {
    idle_session_waiters_both_observe_publication(provider, false).await
}

async fn required_subscription_session_waiters_receive_promptly<P: StoreProvider>(
    provider: P,
) -> TestResult {
    idle_session_waiters_both_observe_publication(provider, true).await
}

async fn mixed_topic_batch_wakes_both_idle_session_waiters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "batch-wakeup-publisher", node.topic.as_str()),
    )
    .await??;
    let mut a = receiving(&mut session, "batch-A", node.alpha.as_str(), Some("A")).await?;
    node.wait_waiter_count(&node.alpha, 1).await?;
    let mut b = receiving(&mut session, "batch-B", node.alpha.as_str(), Some("B")).await?;
    node.wait_waiter_count(&node.alpha, 2).await?;
    let a_hold = node.session(&node.alpha, "A")?.lock.expect("A hold");
    let b_hold = node.session(&node.alpha, "B")?.lock.expect("B hold");
    let messages = [message("batch-A", Some("A")), message("batch-B", Some("B"))];
    let batch = Message {
        body: Body::Data(
            messages
                .iter()
                .map(encode_message)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(Into::into)
                .collect(),
        ),
        ..Message::default()
    };
    let (first, second) = timeout(PROMPT_DELIVERY, async {
        accepted(
            sender
                .send_with_message_format(batch, protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT)
                .await?,
        );
        Ok::<_, Box<dyn Error>>(tokio::try_join!(a.recv(), b.recv())?)
    })
    .await??;
    for (delivery, text, id, number) in [
        (&first, b"batch-A".as_slice(), "A", 1),
        (&second, b"batch-B".as_slice(), "B", 2),
    ] {
        assert_eq!(body(delivery.message()), text);
        assert_eq!(group(delivery.message()), Some(id));
        assert_eq!(sequence(delivery.message()), number);
    }
    assert_eq!(node.session(&node.alpha, "A")?.lock, Some(a_hold));
    assert_eq!(node.session(&node.alpha, "B")?.lock, Some(b_hold));
    timeout(DEADLINE, a.accept(&first)).await??;
    timeout(DEADLINE, b.accept(&second)).await??;
    node.wait_removed(&node.alpha, 1).await?;
    node.wait_removed(&node.alpha, 2).await?;
    node.wait_waiter_count(&node.alpha, 2).await?;
    timeout(DEADLINE, a.close()).await??;
    node.wait_released(&node.alpha, "A").await?;
    timeout(DEADLINE, b.close()).await??;
    node.wait_released(&node.alpha, "B").await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

for_each_backend!(
    required_queue_session_waiters_receive_promptly,
    required_subscription_session_waiters_receive_promptly,
    mixed_topic_batch_wakes_both_idle_session_waiters
);
