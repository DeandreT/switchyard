use super::*;

pub(super) async fn zero_credit_never_claims_or_deletes_queue_or_subscription_messages<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    node.seed("orders", "queue-a").await?;
    node.seed("orders", "queue-b").await?;
    node.seed("topic", "child-a").await?;
    node.seed("topic", "child-b").await?;
    for (address, ids) in [
        ("orders", ["queue-a", "queue-b"]),
        ("topic/subscriptions/Alpha", ["child-a", "child-b"]),
    ] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            let mut peer = Peer::connect(node.address).await?;
            peer.begin(CHANNEL, 0, 0).await?;
            peer.attach_receiver("no-credit", address, mode).await?;
            node.controls.reset();
            let before = node.snapshot()?;
            peer.barrier(CHANNEL).await?;
            node.fence().await?;
            node.inert(&before)?;
            peer.grant(0, 0, false).await?;
            peer.barrier(CHANNEL).await?;
            node.fence().await?;
            node.inert(&before)?;
            for (sequence, id) in ids.into_iter().enumerate() {
                node.ready(address, sequence as u64 + 1, id)?;
            }
            peer.detach().await?;
            peer.barrier(CHANNEL).await?;
            node.fence().await?;
            node.inert(&before)?;
            peer.close().await?;
        }
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn one_credit_claims_one_original_and_replayed_flow_cannot_refresh_it<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    for (address, mode) in [
        ("orders", ReceiveMode::PeekLock),
        ("empty", ReceiveMode::ReceiveAndDelete),
    ] {
        node.seed(address, "first").await?;
        node.seed(address, "second").await?;
        let mut peer = Peer::connect(node.address).await?;
        peer.begin(CHANNEL, 0, 0).await?;
        peer.attach_receiver("one-credit", address, mode).await?;
        node.controls.reset();
        peer.grant(0, 1, false).await?;
        let first = peer.delivery().await?;
        assert_eq!(first.id, 0);
        assert_body(&first, "first");
        assert_eq!(first.settled, mode == ReceiveMode::ReceiveAndDelete);
        node.controls.wait_completed(1).await?;
        let canonical = node.controls.delivery(0);
        assert_eq!(canonical.sequence.as_u64(), 1);
        assert_ne!(u64::from(first.id), canonical.sequence.as_u64());
        match mode {
            ReceiveMode::PeekLock => {
                let record = node
                    .record(address, 1)?
                    .ok_or("held first original missing")?;
                let lock = canonical.lock.ok_or("canonical PeekLock token missing")?;
                assert_eq!(
                    record.state,
                    MessageState::Locked {
                        token: lock.token,
                        locked_until: lock.locked_until
                    }
                );
                assert_eq!(record.delivery_count, 1);
            }
            ReceiveMode::ReceiveAndDelete => assert!(node.record(address, 1)?.is_none()),
        }
        node.ready(address, 2, "second")?;
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);

        // Replaying the original absolute grant does not add a second credit.
        peer.grant(0, 1, false).await?;
        peer.barrier(CHANNEL).await?;
        node.fence().await?;
        assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
        let before = node.snapshot()?;
        peer.grant(1, 0, false).await?;
        peer.complete(&first).await?;
        peer.barrier(CHANNEL).await?;
        node.fence().await?;
        assert!(node.record(address, 1)?.is_none());
        node.ready(address, 2, "second")?;
        assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.controls.clocks.load(Ordering::SeqCst),
            if mode == ReceiveMode::PeekLock { 2 } else { 1 }
        );
        assert_eq!(
            node.controls.writes.load(Ordering::SeqCst),
            if mode == ReceiveMode::PeekLock { 2 } else { 1 }
        );
        if mode == ReceiveMode::ReceiveAndDelete {
            assert_eq!(node.snapshot()?, before);
        }

        peer.grant(1, 1, false).await?;
        let second = peer.delivery().await?;
        assert_eq!(second.id, 1);
        assert_body(&second, "second");
        node.controls.wait_completed(2).await?;
        assert_eq!(node.controls.delivery(1).sequence.as_u64(), 2);
        peer.grant(2, 0, false).await?;
        peer.complete(&second).await?;
        peer.barrier(CHANNEL).await?;
        node.fence().await?;
        assert!(node.record(address, 2)?.is_none());
        assert_eq!(node.controls.receives.load(Ordering::SeqCst), 2);
        peer.detach().await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn empty_receive_returns_credit_before_drain_and_later_enqueue<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("empty-credit", "empty", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 1, false).await?;
    node.controls.wait_completed(1).await?;
    peer.grant(0, 1, true).await?;
    peer.drained(1).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);

    node.seed("empty", "after-empty").await?;
    let before = node.snapshot()?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
    node.ready("empty", 1, "after-empty")?;
    peer.grant(1, 1, false).await?;
    let delivery = peer.delivery().await?;
    assert_body(&delivery, "after-empty");
    node.controls.wait_completed(2).await?;
    peer.grant(2, 0, false).await?;
    peer.complete(&delivery).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert!(node.record("empty", 1)?.is_none());
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 2);
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn detached_receiver_credit_cannot_be_spent_by_its_replacement<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    node.seed("orders", "replacement-original").await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CHANNEL, 0, 0).await?;
    peer.attach_receiver("old-receiver", "empty", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let before = node.snapshot()?;
    peer.detach().await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.inert(&before)?;

    peer.attach_receiver("old-with-unused-credit", "empty", ReceiveMode::PeekLock)
        .await?;
    peer.grant(0, 1, false).await?;
    node.controls.wait_completed(1).await?;
    peer.detach().await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    peer.attach_receiver("new-receiver", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let before = node.snapshot()?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    node.inert(&before)?;
    node.ready("orders", 1, "replacement-original")?;
    peer.grant(0, 1, false).await?;
    let delivery = peer.delivery().await?;
    assert_eq!(delivery.id, 0);
    assert_body(&delivery, "replacement-original");
    node.controls.wait_completed(1).await?;
    peer.grant(1, 0, false).await?;
    peer.complete(&delivery).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    assert!(node.record("orders", 1)?.is_none());
    peer.detach().await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}
