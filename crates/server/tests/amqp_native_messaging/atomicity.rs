use super::*;

pub(super) async fn expired_lock_rejects_entire_mixed_group<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    let original = peer.setup().await?;
    lifecycle::assert_canonical_hold(&node, &original)?;
    let (transaction, post) = lifecycle::stage_mixed(&mut peer, &original).await?;
    let before = node.snapshot()?;
    let held = node.controls.held();
    node.controls.now.store(
        held.lock
            .expect("actual lock deadline")
            .locked_until
            .as_millis()
            + 1,
        Ordering::SeqCst,
    );
    node.controls.reset_io();
    let control = peer.discharge(&transaction, false).await?;
    peer.rejected_mixed(post, control, "com.microsoft:message-lock-lost")
        .await?;
    node.controls.wait_completed(1).await?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.states(), [AtomicCommitState::Rejected]);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.record()?.expect("old held message preserved").state,
        MessageState::Locked {
            token: held.lock.expect("actual token").token,
            locked_until: held.lock.expect("actual deadline").locked_until,
        }
    );
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn replacement_incarnation_rejects_old_canonical_delivery<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    let original = peer.setup().await?;
    lifecycle::assert_canonical_hold(&node, &original)?;
    let (transaction, post) = lifecycle::stage_mixed(&mut peer, &original).await?;
    let (entered, release) = node.controls.pause_handoff();
    let control = peer.discharge(&transaction, false).await?;
    let permit = timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(permit.state(), AtomicCommitState::Pending);
    let handle = node.broker.handle();
    handle
        .submit(
            node.namespace.clone(),
            EntityPath::new("orders")?,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        )
        .await?;
    handle
        .submit(
            node.namespace.clone(),
            EntityPath::new("orders")?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )
        .await?;
    let replacement = node.snapshot()?;
    node.controls.reset_io();
    release.release();
    peer.rejected_mixed(post, control, "amqp:not-found").await?;
    node.controls.wait_completed(1).await?;
    assert_eq!(permit.state(), AtomicCommitState::Rejected);
    node.unchanged(&replacement)?;
    assert!(node.messages()?.is_empty());
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn physical_errors_close_without_retry_and_reopen_whole_commit<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    for after in [false, true] {
        if after {
            // A different client explicitly reacquires after the old lock deadline; this is not a retry.
            node.expire_held().await?;
        }
        let mut peer = Peer::connect(node.address).await?;
        let original = peer.setup().await?;
        lifecycle::assert_canonical_hold(&node, &original)?;
        let (transaction, _post) = lifecycle::stage_mixed(&mut peer, &original).await?;
        let before = node.snapshot()?;
        node.controls.reset_io();
        if after {
            node.controls.fail_after.store(true, Ordering::SeqCst);
        } else {
            node.controls.fail_before.store(true, Ordering::SeqCst);
        }
        peer.discharge(&transaction, false).await?;
        peer.unknown_mixed().await?;
        node.controls.wait_completed(1).await?;
        assert_eq!(node.controls.states(), [AtomicCommitState::Indeterminate]);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.controls.writes.load(Ordering::SeqCst),
            1,
            "indeterminate commits are never automatically retried"
        );
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.controls.receive_starts.load(Ordering::SeqCst),
            1,
            "unknown cannot rearm or fetch another delivery"
        );
        assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
        if after {
            assert!(node.record()?.is_none());
            assert_eq!(
                node.messages()?
                    .iter()
                    .map(|delivery| delivery.message_id.as_str())
                    .collect::<Vec<_>>(),
                ["mixed-a", "mixed-b"]
            );
            assert_ne!(node.snapshot()?, before);
        } else {
            assert_eq!(node.snapshot()?, before);
            assert!(node.record()?.is_some());
            assert_eq!(node.messages()?.len(), 1);
        }
        peer.close().await?;
    }
    let committed = node.snapshot()?;
    let (_provider, reopened, namespace) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, committed);
    assert_eq!(peek(&reopened, &namespace)?.len(), 2);
    Ok(())
}

pub(super) async fn source_or_controller_close_aborts_queued_handoff<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    for controller in [false, true] {
        if controller {
            node.expire_held().await?;
        }
        let mut peer = Peer::connect(node.address).await?;
        let original = peer.setup().await?;
        lifecycle::assert_canonical_hold(&node, &original)?;
        let (transaction, _post) = lifecycle::stage_mixed(&mut peer, &original).await?;
        let before = node.snapshot()?;
        node.controls.reset_io();
        let (entered, release) = node.controls.pause_handoff();
        peer.discharge(&transaction, false).await?;
        let permit = timeout(DEADLINE, entered.recv_async()).await??;
        assert_eq!(permit.state(), AtomicCommitState::Pending);
        let route = if controller {
            (CONTROL, CONTROL_HANDLE)
        } else {
            (RECEIVE, RECEIVE_HANDLE)
        };
        peer.request_detach(route.0, route.1).await?;
        peer.detached(route.0, route.1, None, false).await?;
        peer.close_after_scoped_cleanup().await?;
        node.controls.wait_cancelled().await?;
        assert_eq!(permit.state(), AtomicCommitState::Aborted);
        node.unchanged(&before)?;
        assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.completed.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
        release.release();
    }
    node.stop().await;
    Ok(())
}
