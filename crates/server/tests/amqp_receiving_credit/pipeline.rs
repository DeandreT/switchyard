use super::*;

fn held<P: StoreProvider>(node: &Node<P>, index: usize, id: &str) -> TestResult {
    let delivery = node.controls.delivery(index);
    let sequence = index as u64 + 1;
    assert_eq!(delivery.sequence.as_u64(), sequence);
    assert_eq!(delivery.message_id, id);
    assert_eq!(delivery.body, id.as_bytes());
    assert_eq!(delivery.delivery_count, 1);
    let lock = delivery.lock.ok_or("canonical lock is missing")?;
    let record = node
        .record("orders", sequence)?
        .ok_or("held original is missing")?;
    assert_eq!(record.message_id, id);
    assert_eq!(record.body, id.as_bytes());
    assert_eq!(record.delivery_count, 1);
    assert_eq!(
        record.state,
        MessageState::Locked {
            token: lock.token,
            locked_until: lock.locked_until,
        }
    );
    Ok(())
}

pub(super) async fn a_started_receive_survives_another_jobs_final_ack<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    for id in ["first-job", "paused-reply", "last-job"] {
        node.seed("orders", id).await?;
    }
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("retained-intake", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let gate = node.controls.pause_receive_response(2);
    peer.grant(0, 3, false).await?;
    let first = peer.delivery().await?;
    assert_eq!(first.id, 0);
    assert_body(&first, "first-job");
    gate.wait().await?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 2);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
    held(&node, 0, "first-job")?;
    held(&node, 1, "paused-reply")?;
    node.ready("orders", 3, "last-job")?;
    let paused = node
        .record("orders", 2)?
        .ok_or("paused canonical original missing")?;
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 2);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 2);

    // The final ACK must finish while the already-committed second Receive
    // remains pinned. Dropping and resubmitting that receive loses seq2.
    peer.complete(&first).await?;
    peer.grant(1, 2, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert!(node.record("orders", 1)?.is_none());
    assert_eq!(node.record("orders", 2)?, Some(paused));
    node.ready("orders", 3, "last-job")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 2);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 3);
    gate.release();
    let second = peer.delivery().await?;
    let third = peer.delivery().await?;
    assert_eq!((second.id, third.id), (1, 2));
    assert_body(&second, "paused-reply");
    assert_body(&third, "last-job");
    node.controls.wait_completed(3).await?;
    assert_eq!(node.controls.delivery(1).sequence.as_u64(), 2);
    assert_eq!(node.controls.delivery(2).sequence.as_u64(), 3);
    peer.grant(3, 0, false).await?;
    peer.complete(&third).await?;
    peer.complete(&second).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    for sequence in 1..=3 {
        assert!(node.record("orders", sequence)?.is_none());
    }
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 6);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 6);
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn three_held_originals_settle_in_reverse_without_replayed_credit<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    for id in ["first", "second", "third", "uncredited"] {
        node.seed("orders", id).await?;
    }
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("three-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 3, false).await?;
    let mut deliveries = Vec::new();
    for (id, body) in ["first", "second", "third"].into_iter().enumerate() {
        let delivery = peer.delivery().await?;
        assert_eq!(delivery.id, id as u32);
        assert!(!delivery.settled);
        assert_body(&delivery, body);
        deliveries.push(delivery);
    }
    node.controls.wait_completed(3).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    for (index, id) in ["first", "second", "third"].into_iter().enumerate() {
        held(&node, index, id)?;
    }
    node.ready("orders", 4, "uncredited")?;
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 3);
    let before = node.snapshot()?;
    peer.grant(0, 3, false).await?;
    peer.barrier(CHANNEL).await?;
    peer.grant(3, 0, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    assert_eq!(node.snapshot()?, before);

    peer.settle(&deliveries[2], DeliveryState::Released(amqp::Released))
        .await?;
    peer.settle(
        &deliveries[1],
        DeliveryState::Modified(amqp::Modified {
            delivery_failed: Some(false),
            undeliverable_here: Some(true),
            message_annotations: None,
        }),
    )
    .await?;
    peer.complete(&deliveries[0]).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert!(node.record("orders", 1)?.is_none());
    for (sequence, id, state) in [
        (2, "second", MessageState::Deferred),
        (3, "third", MessageState::Ready),
    ] {
        let record = node
            .record("orders", sequence)?
            .ok_or("settled canonical original missing")?;
        assert_eq!(record.message_id, id);
        assert_eq!(record.body, id.as_bytes());
        assert_eq!(record.state, state);
        assert_eq!(record.delivery_count, 1);
    }
    node.ready("orders", 4, "uncredited")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 6);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 6);
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn thirty_two_held_jobs_bound_a_thirty_three_credit_grant<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    for sequence in 1..=34 {
        node.seed("orders", &format!("original-{sequence}")).await?;
    }
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("bounded-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 33, false).await?;
    let mut deliveries = Vec::new();
    for index in 0..32 {
        let delivery = peer.delivery().await?;
        assert_eq!(delivery.id, index);
        assert!(!delivery.settled);
        assert_body(&delivery, &format!("original-{}", index + 1));
        deliveries.push(delivery);
    }
    node.controls.wait_completed(32).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    for index in 0..32 {
        held(&node, index, &format!("original-{}", index + 1))?;
    }
    node.ready("orders", 33, "original-33")?;
    node.ready("orders", 34, "original-34")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 32);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 32);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 32);
    let before = node.snapshot()?;
    peer.grant(0, 33, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 32);

    // Finishing one job frees the local slot; the original grant has one
    // remaining wire credit, without any new grant or hidden 33rd lookup.
    peer.complete(&deliveries[31]).await?;
    let next = peer.delivery().await?;
    assert_eq!(next.id, 32);
    assert_body(&next, "original-33");
    node.controls.wait_completed(33).await?;
    assert_eq!(node.controls.delivery(32).sequence.as_u64(), 33);
    assert!(node.record("orders", 32)?.is_none());
    peer.grant(33, 0, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.ready("orders", 34, "original-34")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 33);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 34);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 34);
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn an_empty_lookup_drains_unused_credit_while_jobs_remain_held<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    node.seed("orders", "held-a").await?;
    node.seed("orders", "held-b").await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("drain-with-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 3, false).await?;
    let first = peer.delivery().await?;
    let second = peer.delivery().await?;
    assert_eq!((first.id, second.id), (0, 1));
    assert_body(&first, "held-a");
    assert_body(&second, "held-b");
    node.controls.wait_completed(3).await?;
    held(&node, 0, "held-a")?;
    held(&node, 1, "held-b")?;
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 2);
    peer.grant(0, 3, true).await?;
    peer.drained(3).await?;
    peer.complete(&second).await?;
    peer.complete(&first).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert!(node.record("orders", 1)?.is_none());
    assert!(node.record("orders", 2)?.is_none());
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 5);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 4);
    node.seed("orders", "after-drain").await?;
    node.controls.reset();
    let before = node.snapshot()?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.inert(&before)?;
    node.ready("orders", 3, "after-drain")?;
    peer.grant(3, 1, false).await?;
    let next = peer.delivery().await?;
    assert_eq!(next.id, 2);
    assert_body(&next, "after-drain");
    node.controls.wait_completed(1).await?;
    assert_eq!(node.controls.delivery(0).sequence.as_u64(), 3);
    peer.grant(4, 0, false).await?;
    peer.complete(&next).await?;
    node.fence().await?;
    assert!(node.record("orders", 3)?.is_none());
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn detach_preserves_held_locks_until_explicit_expiry_and_redelivery<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    for id in ["retained-a", "retained-b", "retained-c", "uncredited"] {
        node.seed("orders", id).await?;
    }
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("detached-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 3, false).await?;
    for (index, id) in ["retained-a", "retained-b", "retained-c"]
        .into_iter()
        .enumerate()
    {
        let delivery = peer.delivery().await?;
        assert_eq!(delivery.id, index as u32);
        assert_body(&delivery, id);
    }
    node.controls.wait_completed(3).await?;
    peer.grant(3, 0, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    let before = node.snapshot()?;
    peer.detach().await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    // The native Detach ACK is not an adapter-cleanup fence. These checks
    // concern persisted locks only; no automatic Abandon is promised.
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    for (index, id) in ["retained-a", "retained-b", "retained-c"]
        .into_iter()
        .enumerate()
    {
        held(&node, index, id)?;
    }
    node.ready("orders", 4, "uncredited")?;
    let old_tokens: Vec<_> = (0..3)
        .map(|index| node.controls.delivery(index).lock.unwrap().token)
        .collect();
    node.expire_locks(3).await?;
    for sequence in 1..=3 {
        let record = node
            .record("orders", sequence)?
            .ok_or("expired original is missing")?;
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.delivery_count, 1);
    }
    peer.attach_receiver("replacement-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let expired = node.snapshot()?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.inert(&expired)?;
    peer.grant(0, 3, false).await?;
    let mut repeated = Vec::new();
    for (index, id) in ["retained-a", "retained-b", "retained-c"]
        .into_iter()
        .enumerate()
    {
        let delivery = peer.delivery().await?;
        assert_eq!(delivery.id, index as u32 + 3);
        assert_body(&delivery, id);
        repeated.push(delivery);
    }
    node.controls.wait_completed(3).await?;
    for (index, old_token) in old_tokens.into_iter().enumerate() {
        let delivery = node.controls.delivery(index);
        assert_eq!(delivery.sequence.as_u64(), index as u64 + 1);
        assert_eq!(delivery.delivery_count, 2);
        assert_ne!(delivery.lock.expect("redelivery lock").token, old_token);
    }
    peer.grant(3, 0, false).await?;
    for delivery in repeated.iter().rev() {
        peer.complete(delivery).await?;
    }
    node.fence().await?;
    for sequence in 1..=3 {
        assert!(node.record("orders", sequence)?.is_none());
    }
    node.ready("orders", 4, "uncredited")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}
