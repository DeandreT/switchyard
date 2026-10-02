use super::*;

pub(super) async fn single_batch_and_cross_session_postings_commit_once<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let before_count = node.messages("orders")?.len();
        let mut peer = Peer::connect(node.address).await?;
        peer.setup(mode.clone()).await?;
        peer.producer(SECOND, "orders", mode.clone()).await?;
        let transaction = peer.declare(mode.clone()).await?;
        node.controls.reset();
        let before = node.snapshot()?;
        let first = message("single", b"single retained payload");
        let first_id = peer
            .transfer(POST, POST_HANDLE, Some(&transaction), 0, &first)
            .await?;
        peer.provisional(POST, first_id, &transaction).await?;
        let wrapped = batch(&[message("batch-a", b"one"), message("batch-b", b"two")])?;
        let batch_id = peer
            .transfer(
                SECOND,
                POST_HANDLE,
                Some(&transaction),
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                &wrapped,
            )
            .await?;
        peer.provisional(SECOND, batch_id, &transaction).await?;
        peer.barrier(POST).await?;
        peer.barrier(SECOND).await?;
        node.unchanged(&before)?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        let control_id = peer.discharge(&transaction, false).await?;
        peer.final_outcome(POST, first_id, mode.clone(), true)
            .await?;
        peer.final_outcome(SECOND, batch_id, mode.clone(), true)
            .await?;
        peer.final_outcome(CONTROL, control_id, mode.clone(), true)
            .await?;
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.states(), vec![AtomicCommitState::Committed]);
        let messages = node.messages("orders")?;
        assert_eq!(messages.len(), before_count + 3);
        for (offset, (retained, (expected_id, expected_body))) in messages[before_count..]
            .iter()
            .zip([
                ("single", b"single retained payload".as_slice()),
                ("batch-a", b"one".as_slice()),
                ("batch-b", b"two".as_slice()),
            ])
            .enumerate()
        {
            assert_eq!(
                retained.sequence,
                SequenceNumber::new((before_count + offset + 1) as u64)
            );
            assert_eq!(retained.message_id, expected_id);
            assert_eq!(retained.body, expected_body);
            assert_eq!(
                retained
                    .envelope
                    .as_deref()
                    .unwrap()
                    .properties
                    .subject
                    .as_deref(),
                Some("rich subject")
            );
            assert!(retained.lock.is_none());
        }
        if mode == ReceiverSettleMode::Second {
            peer.sender_ack(POST, first_id).await?;
            peer.sender_ack(SECOND, batch_id).await?;
            peer.sender_ack(CONTROL, control_id).await?;
        }
        peer.close().await?;
    }
    let namespace = node.namespace.clone();
    let before = node.snapshot()?;
    let (_provider, reopened) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, before);
    assert_eq!(peek(&reopened, &namespace, "orders")?.len(), 6);
    Ok(())
}

pub(super) async fn abort_and_empty_discharge_never_touch_broker_storage<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    for empty in [false, true] {
        for fail in [false, true] {
            if !empty && !fail {
                continue;
            }
            let mut peer = Peer::connect(node.address).await?;
            peer.begin(CONTROL).await?;
            peer.attach(CONTROL, CONTROL_HANDLE, "", true, ReceiverSettleMode::First)
                .await?;
            peer.admitted(CONTROL, true).await?;
            if !empty {
                peer.producer(POST, "orders", ReceiverSettleMode::First)
                    .await?;
            }
            let transaction = peer.declare(ReceiverSettleMode::First).await?;
            node.controls.reset();
            let before = node.snapshot()?;
            let posting = if empty {
                None
            } else {
                let id = peer
                    .transfer(
                        POST,
                        POST_HANDLE,
                        Some(&transaction),
                        0,
                        &message("aborted", b"must not retain"),
                    )
                    .await?;
                peer.provisional(POST, id, &transaction).await?;
                Some(id)
            };
            let control_id = peer.discharge(&transaction, fail).await?;
            if let Some(posting) = posting {
                peer.final_outcome(POST, posting, ReceiverSettleMode::First, false)
                    .await?;
            }
            peer.final_outcome(CONTROL, control_id, ReceiverSettleMode::First, true)
                .await?;
            node.unchanged(&before)?;
            assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
            assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
            assert_eq!(
                node.controls.handoffs.load(Ordering::SeqCst),
                usize::from(!fail)
            );
            if !fail {
                assert_eq!(node.controls.states(), vec![AtomicCommitState::Committed]);
            }
            assert!(node.messages("orders")?.is_empty());
            peer.close().await?;
        }
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn default_listener_still_refuses_coordinator_without_admission<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false).await?;
    let before = node.snapshot()?;
    node.controls.reset();
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(CONTROL).await?;
    peer.attach(CONTROL, CONTROL_HANDLE, "", true, ReceiverSettleMode::First)
        .await?;
    peer.ended(CONTROL, "amqp:not-implemented").await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    peer.healthy().await?;
    assert_eq!(node.messages("healthy")?.len(), 1);
    peer.close().await?;
    node.stop().await;
    Ok(())
}
