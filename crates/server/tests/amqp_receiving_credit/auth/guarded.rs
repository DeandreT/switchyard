use super::*;

async fn authenticated_receiver<P: StoreProvider>(
    node: &Node<P>,
    security: &Security,
    mode: ReceiveMode,
    expiry: u64,
) -> TestResult<Peer> {
    let mut peer = security.connect(node.address).await?;
    open_cbs(&mut peer).await?;
    put_token(&mut peer, token(expiry)?).await?;
    peer.begin(CHANNEL, 1, 0).await?;
    peer.attach_receiver("guarded-receiver", "orders", mode)
        .await?;
    Ok(peer)
}

fn horizon(seconds: u64) -> TestResult<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(seconds)
        .ok_or("SAS expiry overflow")?)
}

async fn secure_node<P: StoreProvider>(provider: P) -> TestResult<(Node<P>, Security)> {
    let security = Security::new()?;
    let node = Node::start_secure(
        provider,
        Some((security.tls.clone(), security.authentication.clone())),
    )
    .await?;
    Ok((node, security))
}

async fn queued_cancellation<P: StoreProvider>(provider: P, mode: ReceiveMode) -> TestResult {
    let (node, security) = secure_node(provider).await?;
    node.seed("orders", "queued-original").await?;
    node.seed("orders", "queued-sibling").await?;
    let expiry = horizon(600)?;
    let mut peer = authenticated_receiver(&node, &security, mode, expiry).await?;
    node.controls.reset();
    let before = node.snapshot()?;
    let (release, parked) = node.park_owner().await?;
    let base_reads = node.controls.reads.load(Ordering::SeqCst);
    assert_eq!(base_reads, 1, "only the parked configuration GET entered");
    peer.grant(0, 1, false).await?;
    peer.barrier(CHANNEL).await?;
    node.controls.wait_guarded_polled(1).await?;
    let permit = node.controls.permit(0);
    assert_eq!(node.controls.horizon(0), expiry);
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), base_reads);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);

    peer.detach().await?;
    // The wire ACK alone is not an adapter cleanup fence.
    node.controls.wait_cancelled(1).await?;
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert_eq!(node.snapshot()?, before);
    release.release();
    timeout(DEADLINE, parked).await??;
    node.fence().await?;
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), base_reads + 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    node.ready("orders", 1, "queued-original")?;
    node.ready("orders", 2, "queued-sibling")?;
    peer.barrier(CHANNEL).await?;
    peer.barrier(CBS).await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(crate) async fn queued_peek_lock_cancellation_precedes_all_receive_owner_work<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    queued_cancellation(provider, ReceiveMode::PeekLock).await
}

pub(crate) async fn queued_receive_and_delete_cancellation_preserves_the_original<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    queued_cancellation(provider, ReceiveMode::ReceiveAndDelete).await
}

pub(crate) async fn queued_receive_expiry_is_an_exact_wire_refusal_without_owner_work<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let (node, security) = secure_node(provider).await?;
    node.seed("orders", "expiry-original").await?;
    let mut peer = security.connect(node.address).await?;
    open_cbs(&mut peer).await?;
    let expiry = horizon(4)?;
    put_token(&mut peer, token(expiry)?).await?;
    peer.begin(CHANNEL, 1, 0).await?;
    peer.attach_receiver("guarded-expiry", "orders", ReceiveMode::PeekLock)
        .await?;
    node.controls.reset();
    let before = node.snapshot()?;
    let (release, parked) = node.park_owner().await?;
    let base_reads = node.controls.reads.load(Ordering::SeqCst);
    peer.grant(0, 1, false).await?;
    peer.barrier(CHANNEL).await?;
    node.controls.wait_guarded_polled(1).await?;
    let permit = node.controls.permit(0);
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    assert_eq!(
        node.controls.horizon(0),
        expiry,
        "the exact Listen grant bounds this actual owner ticket"
    );
    peer.detached(Some("amqp:unauthorized-access")).await?;
    node.controls.wait_cancelled(1).await?;
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), base_reads);
    assert_eq!(node.snapshot()?, before);
    release.release();
    timeout(DEADLINE, parked).await??;
    node.fence().await?;
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), base_reads + 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    node.ready("orders", 1, "expiry-original")?;
    assert_eq!(node.snapshot()?, before);
    peer.barrier(CHANNEL).await?;
    peer.barrier(CBS).await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(crate) async fn a_stale_receive_binding_refuses_before_clock_and_cannot_touch_replacement<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let (node, security) = secure_node(provider).await?;
    node.seed("orders", "old-incarnation").await?;
    let mut peer =
        authenticated_receiver(&node, &security, ReceiveMode::PeekLock, horizon(600)?).await?;
    node.controls.reset();
    let gate = node.controls.pause_before_receive_queue(1);
    peer.grant(0, 1, false).await?;
    gate.wait().await?;
    let permit = node.controls.permit(0);
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    assert_eq!(node.controls.guarded_polled.load(Ordering::SeqCst), 0);
    let handle = node.broker.handle();
    timeout(
        DEADLINE,
        handle.submit(
            node.namespace.clone(),
            EntityPath::new("orders")?,
            CommandKind::DeleteEntity {
                target: domain::DeleteEntityTarget::Queue,
            },
        ),
    )
    .await??;
    timeout(
        DEADLINE,
        handle.submit(
            node.namespace.clone(),
            EntityPath::new("orders")?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
    )
    .await??;
    node.seed("orders", "replacement-original").await?;
    node.controls.reset_owner_io();
    let before = node.snapshot()?;
    gate.release();
    peer.detached(Some(protocol_amqp::NOT_FOUND)).await?;
    node.controls.wait_completed(1).await?;
    assert_eq!(
        permit.state(),
        ReceiveClaimState::Started,
        "admission is not proof of a receive result"
    );
    assert!(
        node.controls.reads.load(Ordering::SeqCst) > 0,
        "the exact incarnation was checked"
    );
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.snapshot()?, before);
    assert!(
        node.record("orders", 1)?.is_none(),
        "the old original was deleted with its incarnation"
    );
    node.ready("orders", 2, "replacement-original")?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(crate) async fn started_receive_commits_once_despite_connection_and_reply_loss<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let (node, security) = secure_node(provider).await?;
    for id in ["started-lock", "started-delete", "untouched-sibling"] {
        node.seed("orders", id).await?;
    }
    let mut peer =
        authenticated_receiver(&node, &security, ReceiveMode::PeekLock, horizon(600)?).await?;
    node.controls.reset();
    let (entered, release) = node.controls.pause_commit();
    peer.grant(0, 1, false).await?;
    timeout(DEADLINE, entered.recv_async()).await??;
    let permit = node.controls.permit(0);
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    node.ready("orders", 1, "started-lock")?;
    peer.detach().await?;
    node.controls.wait_cancelled(1).await?;
    peer.close().await?;
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    release.release();
    node.fence().await?;
    let locked = node
        .record("orders", 1)?
        .ok_or("started original missing")?;
    assert!(matches!(locked.state, MessageState::Locked { .. }));
    assert_eq!(locked.delivery_count, 1);
    assert_eq!(locked.message_id, "started-lock");
    assert_eq!(locked.body, b"started-lock");
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 0);
    node.ready("orders", 2, "started-delete")?;
    node.ready("orders", 3, "untouched-sibling")?;

    let mut peer = authenticated_receiver(
        &node,
        &security,
        ReceiveMode::ReceiveAndDelete,
        horizon(600)?,
    )
    .await?;
    node.controls.reset();
    let response = node.controls.pause_receive_response(1);
    peer.grant(0, 1, false).await?;
    response.wait().await?;
    let permit = node.controls.permit(0);
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    assert_eq!(node.controls.delivery(0).sequence.as_u64(), 2);
    assert!(node.record("orders", 2)?.is_none());
    peer.detach().await?;
    node.controls.wait_cancelled(1).await?;
    response.release();
    node.fence().await?;
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        node.record("orders", 1)?.expect("unabandoned original"),
        locked
    );
    node.ready("orders", 3, "untouched-sibling")?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(crate) async fn valid_guarded_receive_preserves_canonical_delivery_and_settlement<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let (node, security) = secure_node(provider).await?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        node.seed("orders", "valid-original").await?;
        node.seed("orders", "valid-sibling").await?;
        let expiry = horizon(600)?;
        let mut peer = authenticated_receiver(&node, &security, mode, expiry).await?;
        node.controls.reset();
        peer.grant(0, 1, false).await?;
        let original = peer.delivery().await?;
        assert_body(&original, "valid-original");
        assert_eq!(original.id, 0);
        assert_eq!(original.settled, mode == ReceiveMode::ReceiveAndDelete);
        node.controls.wait_completed(1).await?;
        assert_eq!(node.controls.permit(0).state(), ReceiveClaimState::Started);
        assert_eq!(node.controls.horizon(0), expiry);
        let sequence = index as u64 * 2 + 1;
        let canonical = node.controls.delivery(0);
        assert_eq!(canonical.sequence.as_u64(), sequence);
        assert_ne!(canonical.sequence.as_u64(), u64::from(original.id));
        if mode == ReceiveMode::PeekLock {
            let lock = canonical.lock.ok_or("canonical lock missing")?;
            let record = node
                .record("orders", sequence)?
                .ok_or("locked original missing")?;
            assert_eq!(
                record.state,
                MessageState::Locked {
                    token: lock.token,
                    locked_until: lock.locked_until
                }
            );
            assert_eq!(record.delivery_count, 1);
        } else {
            assert!(node.record("orders", sequence)?.is_none());
        }
        node.ready("orders", sequence + 1, "valid-sibling")?;
        peer.grant(1, 0, false).await?;
        peer.complete(&original).await?;
        peer.barrier(CHANNEL).await?;
        node.fence().await?;
        assert!(node.record("orders", sequence)?.is_none());
        assert_eq!(node.controls.receives.load(Ordering::SeqCst), 1);
        node.ready("orders", sequence + 1, "valid-sibling")?;
        peer.grant(1, 1, false).await?;
        let sibling = peer.delivery().await?;
        assert_eq!(sibling.id, 1);
        assert_body(&sibling, "valid-sibling");
        node.controls.wait_completed(2).await?;
        assert_eq!(node.controls.delivery(1).sequence.as_u64(), sequence + 1);
        peer.grant(2, 0, false).await?;
        peer.complete(&sibling).await?;
        peer.barrier(CHANNEL).await?;
        node.fence().await?;
        assert!(node.record("orders", sequence + 1)?.is_none());
        assert_eq!(node.controls.receives.load(Ordering::SeqCst), 2);
        peer.detach().await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}
