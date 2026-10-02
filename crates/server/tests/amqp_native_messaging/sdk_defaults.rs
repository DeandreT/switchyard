use super::*;

#[path = "sdk_defaults/authorization.rs"]
mod authorization;
pub(super) use authorization::mixed_receiver_still_requires_listen_before_bind;

pub(super) async fn sdk_defaults_rearm_the_same_canonical_original_before_mixed_commit<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.coordinator_with_sdk_defaults().await?;
    peer.producer(POST, "orders").await?;
    let original = peer
        .consumer_with_sender_mode(SenderSettleMode::Mixed)
        .await?;
    lifecycle::assert_canonical_hold(&node, &original)?;
    let canonical = node.controls.held();
    let before = node.snapshot()?;
    node.controls.reset_io();

    let rolled_back = peer.declare().await?;
    peer.retire(&original, &rolled_back).await?;
    peer.provisional(RECEIVE, Role::Sender, original.id, &rolled_back)
        .await?;
    let control = peer.discharge(&rolled_back, true).await?;
    peer.rollback_control(&original, &rolled_back, control)
        .await?;
    peer.barrier(RECEIVE).await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.held(), canonical);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);

    let (committed, posting) = lifecycle::stage_mixed(&mut peer, &original).await?;
    let control = peer.discharge(&committed, false).await?;
    peer.committed(&original, posting, control).await?;
    node.controls.wait_receive_starts(2).await?;
    assert!(node.record()?.is_none());
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.states(), [AtomicCommitState::Committed]);
    let retained = node.messages()?;
    assert_eq!(
        retained
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        ["mixed-a", "mixed-b"]
    );
    assert_eq!(
        retained
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        [3, 4]
    );
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn negotiated_unsettled_original_still_refuses_settled_retirement<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.coordinator_with_sdk_defaults().await?;
    let original = peer
        .consumer_with_sender_mode(SenderSettleMode::Mixed)
        .await?;
    lifecycle::assert_canonical_hold(&node, &original)?;
    let transaction = peer.declare().await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.send(
        RECEIVE,
        Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: original.id,
            last: None,
            settled: true,
            state: Some(DeliveryState::Transactional(TransactionalState {
                txn_id: transaction,
                outcome: Some(Outcome::Accepted(amqp::Accepted)),
            })),
            batchable: false,
        }),
        vec![],
    )
    .await?;
    peer.detached(RECEIVE, RECEIVE_HANDLE, Some("amqp:not-implemented"), true)
        .await?;
    node.unchanged(&before)?;
    assert!(matches!(
        node.record()?.expect("held original").state,
        MessageState::Locked { .. }
    ));
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
    peer.producer(HEALTHY, "healthy").await?;
    peer.barrier(HEALTHY).await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

struct ExtraListener(Option<JoinHandle<()>>);

impl ExtraListener {
    async fn stop(mut self) -> TestResult {
        let task = self.0.take().expect("extra listener");
        task.abort();
        let _ = timeout(DEADLINE, task).await?;
        Ok(())
    }
}

impl Drop for ExtraListener {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

pub(super) async fn coordinator_default_is_confined_to_messaging_admission<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let (posting_address, posting) = node.additional_listener(ListenerMode::Posting).await?;
    let posting = ExtraListener(Some(posting));
    let ordinary_socket = TcpListener::bind("127.0.0.1:0").await?;
    let ordinary_address = ordinary_socket.local_addr()?;
    let service = protocol_amqp::AmqpListener::new(node.broker.handle(), node.namespace.clone())
        .with_idle_timeout_millis(0);
    let ordinary = ExtraListener(Some(tokio::spawn(async move {
        let _ = service.serve(ordinary_socket).await;
    })));
    for (address, condition) in [
        (posting_address, "amqp:invalid-field"),
        (ordinary_address, "amqp:not-implemented"),
    ] {
        let mut peer = Peer::connect(address).await?;
        peer.begin(CONTROL).await?;
        let mut request = Peer::attach_request(CONTROL, CONTROL_HANDLE, "", Role::Sender);
        request.target = Some(Coordinator::default().into());
        request.initial_delivery_count = None;
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.attach(CONTROL, request).await?;
        let (channel, performative) = peer.control().await?;
        assert_eq!(channel, peer.local(CONTROL));
        let Performative::End(end) = performative else {
            return Err("strict coordinator admission must end its session".into());
        };
        assert_eq!(
            end.error
                .expect("strict refusal")
                .condition
                .as_symbol()
                .as_str(),
            condition
        );
        node.unchanged(&before)?;
        assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        peer.acknowledge_end(CONTROL).await?;
        peer.producer(HEALTHY, "healthy").await?;
        peer.barrier(HEALTHY).await?;
        peer.close().await?;
    }
    let mut peer = Peer::connect(posting_address).await?;
    peer.begin(RECEIVE).await?;
    let mut request = Peer::attach_request(RECEIVE, RECEIVE_HANDLE, "orders", Role::Receiver);
    request.snd_settle_mode = SenderSettleMode::Mixed;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.attach(RECEIVE, request).await?;
    peer.detached(RECEIVE, RECEIVE_HANDLE, Some("amqp:not-implemented"), true)
        .await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
    peer.producer(HEALTHY, "healthy").await?;
    peer.barrier(HEALTHY).await?;
    peer.close().await?;
    posting.stop().await?;
    ordinary.stop().await?;
    node.stop().await;
    Ok(())
}
