use super::*;

pub(super) async fn stale_queue_incarnation_cannot_commit_to_its_replacement<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    for topic in [false, true] {
        let mut peer = Peer::connect(node.address).await?;
        peer.setup(ReceiverSettleMode::First).await?;
        let transaction = peer.declare(ReceiverSettleMode::First).await?;
        let posting = peer
            .transfer(
                POST,
                POST_HANDLE,
                Some(&transaction),
                0,
                &message("stale", b"old incarnation payload"),
            )
            .await?;
        peer.provisional(POST, posting, &transaction).await?;
        let (entered, release) = node.controls.pause_handoff();
        let control_id = peer.discharge(&transaction, false).await?;
        let permit = timeout(DEADLINE, entered.recv_async()).await??;
        assert_eq!(permit.state(), AtomicCommitState::Pending);
        peer.barrier(POST).await?;
        node.broker
            .handle()
            .submit(
                node.namespace.clone(),
                EntityPath::new("orders")?,
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue,
                },
            )
            .await?;
        let create = if topic {
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            }
        } else {
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            }
        };
        node.broker
            .handle()
            .submit(node.namespace.clone(), EntityPath::new("orders")?, create)
            .await?;
        let replacement = node.snapshot()?;
        node.controls.reset();
        release.release();
        peer.final_outcome(POST, posting, ReceiverSettleMode::First, false)
            .await?;
        let disposition = peer.disposition(CONTROL, control_id).await?;
        assert!(disposition.settled);
        let Some(DeliveryState::Rejected(amqp::Rejected { error: Some(error) })) =
            disposition.state
        else {
            return Err("old incarnation must reject, not commit".into());
        };
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:transaction:rollback"
        );
        assert_eq!(permit.state(), AtomicCommitState::Rejected);
        node.unchanged(&replacement)?;
        if topic {
            assert!(
                StateMachine::new(node.store.clone())
                    .topic_config(&node.namespace, &EntityPath::new("orders")?)?
                    .is_some()
            );
        } else {
            assert!(node.messages("orders")?.is_empty());
        }
        peer.healthy().await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn physical_commit_errors_are_indeterminate_without_retry_or_detail_leak<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    for after in [false, true] {
        let mut peer = Peer::connect(node.address).await?;
        peer.setup(ReceiverSettleMode::First).await?;
        let transaction = peer.declare(ReceiverSettleMode::First).await?;
        let wrapped = batch(&[message("error-a", b"one"), message("error-b", b"two")])?;
        let posting = peer
            .transfer(
                POST,
                POST_HANDLE,
                Some(&transaction),
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                &wrapped,
            )
            .await?;
        peer.provisional(POST, posting, &transaction).await?;
        let before = node.snapshot()?;
        node.controls.reset();
        if after {
            node.controls.fail_after.store(true, Ordering::SeqCst);
        } else {
            node.controls.fail_before.store(true, Ordering::SeqCst);
        }
        peer.discharge(&transaction, false).await?;
        peer.detached(CONTROL, CONTROL_HANDLE, "amqp:internal-error")
            .await?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.controls.writes.load(Ordering::SeqCst),
            1,
            "no automatic owner retry"
        );
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.controls.states(),
            vec![AtomicCommitState::Indeterminate]
        );
        if after {
            assert_ne!(node.snapshot()?, before);
            let retained = node.messages("orders")?;
            assert_eq!(retained.len(), 2);
            assert_eq!(retained[0].message_id, "error-a");
            assert_eq!(retained[1].message_id, "error-b");
        } else {
            assert_eq!(node.snapshot()?, before);
            assert!(node.messages("orders")?.is_empty());
        }
        peer.close().await?;
    }
    let namespace = node.namespace.clone();
    let committed = node.snapshot()?;
    let (_provider, reopened) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, committed);
    assert_eq!(peek(&reopened, &namespace, "orders")?.len(), 2);
    Ok(())
}
