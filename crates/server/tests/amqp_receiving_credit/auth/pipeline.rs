use super::*;

pub(crate) async fn authorization_loss_preserves_several_locks_until_expiry<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new()?;
    let node = Node::start_secure(
        provider,
        Some((security.tls.clone(), security.authentication.clone())),
    )
    .await?;
    for id in ["authorized-a", "authorized-b", "authorized-c", "uncredited"] {
        node.seed("orders", id).await?;
    }
    let mut peer = security.connect(node.address).await?;
    open_cbs(&mut peer).await?;
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(4)
        .ok_or("SAS expiry overflow")?;
    put_token(&mut peer, token(expiry)?).await?;
    peer.begin(CHANNEL, 1, 0).await?;
    peer.attach_receiver("expires-with-held", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    peer.grant(0, 3, false).await?;
    let mut original_tokens = Vec::new();
    for (index, id) in ["authorized-a", "authorized-b", "authorized-c"]
        .into_iter()
        .enumerate()
    {
        let delivery = peer.delivery().await?;
        assert_eq!(delivery.id, index as u32);
        assert!(!delivery.settled);
        assert_body(&delivery, id);
    }
    node.controls.wait_completed(3).await?;
    for index in 0..3 {
        let delivery = node.controls.delivery(index);
        assert_eq!(delivery.sequence.as_u64(), index as u64 + 1);
        assert_eq!(delivery.delivery_count, 1);
        original_tokens.push(delivery.lock.ok_or("canonical held lock missing")?.token);
    }
    peer.grant(3, 0, false).await?;
    peer.barrier(CHANNEL).await?;
    node.fence().await?;
    let before = node.snapshot()?;
    // The adapter's explicit authorization Detach is sent after its work and
    // registration cleanup. It does not claim to Abandon committed locks.
    peer.detached(Some("amqp:unauthorized-access")).await?;
    peer.barrier(CHANNEL).await?;
    peer.barrier(CBS).await?;
    node.fence().await?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 3);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 3);
    for (index, token) in original_tokens.iter().enumerate() {
        let record = node
            .record("orders", index as u64 + 1)?
            .ok_or("held original missing")?;
        assert!(
            matches!(record.state, MessageState::Locked { token: actual, .. } if actual == *token)
        );
        assert_eq!(record.delivery_count, 1);
    }
    node.ready("orders", 4, "uncredited")?;
    peer.close().await?;
    node.expire_locks(3).await?;
    for sequence in 1..=3 {
        let record = node
            .record("orders", sequence)?
            .ok_or("expired original missing")?;
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.delivery_count, 1);
    }

    let mut fresh = security.connect(node.address).await?;
    open_cbs(&mut fresh).await?;
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(60)
        .ok_or("SAS expiry overflow")?;
    put_token(&mut fresh, token(expiry)?).await?;
    fresh.begin(CHANNEL, 1, 0).await?;
    fresh
        .attach_receiver("fresh-authorized", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    fresh.grant(0, 3, false).await?;
    let mut deliveries = Vec::new();
    for (index, id) in ["authorized-a", "authorized-b", "authorized-c"]
        .into_iter()
        .enumerate()
    {
        let delivery = fresh.delivery().await?;
        assert_eq!(delivery.id, index as u32);
        assert_body(&delivery, id);
        deliveries.push(delivery);
    }
    node.controls.wait_completed(3).await?;
    for (index, token) in original_tokens.into_iter().enumerate() {
        let delivery = node.controls.delivery(index);
        assert_eq!(delivery.sequence.as_u64(), index as u64 + 1);
        assert_eq!(delivery.delivery_count, 2);
        assert_ne!(delivery.lock.expect("new lock").token, token);
    }
    fresh.grant(3, 0, false).await?;
    for delivery in deliveries.iter().rev() {
        fresh.complete(delivery).await?;
    }
    node.fence().await?;
    for sequence in 1..=3 {
        assert!(node.record("orders", sequence)?.is_none());
    }
    node.ready("orders", 4, "uncredited")?;
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 3);
    fresh.detach().await?;
    fresh.close().await?;
    node.stop().await;
    Ok(())
}
