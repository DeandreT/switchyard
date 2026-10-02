use super::*;

pub(super) fn assert_canonical_hold<P: StoreProvider>(
    node: &Node<P>,
    original: &WireDelivery,
) -> TestResult {
    let held = node.controls.held();
    assert_eq!(held.message_id, "held-original");
    assert_eq!(held.body, b"held-body");
    assert_eq!(held.sequence, SequenceNumber::new(2));
    assert_ne!(held.sequence.as_u64(), u64::from(original.id));
    assert!(!original.tag.is_empty());
    assert_eq!(
        original
            .message
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.clone()),
        Some("held-original".into())
    );
    assert!(
        matches!(&original.message.body, Body::Data(parts) if parts.len() == 1 && parts[0].as_ref() == b"held-body")
    );
    let lock = held.lock.expect("actual PeekLock authority");
    let record = node.record()?.expect("canonical held row");
    assert_eq!(record.sequence, held.sequence);
    assert!(
        matches!(record.state, MessageState::Locked { token, locked_until } if token == lock.token && locked_until == lock.locked_until)
    );
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    Ok(())
}

pub(super) async fn stage_mixed(
    peer: &mut Peer,
    original: &WireDelivery,
) -> TestResult<(TransactionId, u32)> {
    let transaction = peer.declare().await?;
    peer.retire(original, &transaction).await?;
    peer.provisional(RECEIVE, Role::Sender, original.id, &transaction)
        .await?;
    let wrapped = batch(&[message("mixed-a", b"new-a"), message("mixed-b", b"new-b")])?;
    let post = peer
        .transfer(
            POST,
            POST_HANDLE,
            Some(&transaction),
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            &wrapped,
        )
        .await?;
    peer.provisional(POST, Role::Receiver, post, &transaction)
        .await?;
    Ok((transaction, post))
}

pub(super) async fn mixed_batch_and_canonical_complete_commit_once<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.coordinator().await?;
    peer.producer(POST, "orders").await?;
    let original = peer.consumer_with_distribution(Some("move")).await?;
    assert_canonical_hold(&node, &original)?;
    let (transaction, post) = stage_mixed(&mut peer, &original).await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    let (entered, release) = node.controls.pause_handoff();
    let control = peer.discharge(&transaction, false).await?;
    let permit = timeout(DEADLINE, entered.recv_async()).await??;
    assert_eq!(permit.state(), AtomicCommitState::Pending);
    peer.barrier(RECEIVE).await?;
    peer.barrier(POST).await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    release.release();
    peer.committed(&original, post, control).await?;
    node.controls.wait_receive_starts(2).await?;
    assert_eq!(permit.state(), AtomicCommitState::Committed);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    assert!(
        node.record()?.is_none(),
        "Complete targeted actual sequence/token, not alias zero"
    );
    let retained = node.messages()?;
    assert_eq!(
        retained
            .iter()
            .map(|delivery| delivery.message_id.as_str())
            .collect::<Vec<_>>(),
        ["mixed-a", "mixed-b"]
    );
    assert_eq!(
        retained
            .iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [3, 4]
    );
    peer.close().await?;
    let committed = node.snapshot()?;
    let (_provider, reopened, namespace) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, committed);
    assert_eq!(peek(&reopened, &namespace)?.len(), 2);
    Ok(())
}

pub(super) async fn explicit_rollback_rearms_original_without_resend<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    let original = peer.setup().await?;
    assert_canonical_hold(&node, &original)?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    for await_provisional in [true, false] {
        let transaction = peer.declare().await?;
        peer.retire(&original, &transaction).await?;
        if await_provisional {
            peer.provisional(RECEIVE, Role::Sender, original.id, &transaction)
                .await?;
        }
        let control = peer.discharge(&transaction, true).await?;
        peer.rollback_control(&original, &transaction, control)
            .await?;
        peer.barrier(RECEIVE).await?;
        node.unchanged(&before)?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 1);
    }
    let (transaction, post) = stage_mixed(&mut peer, &original).await?;
    let control = peer.discharge(&transaction, false).await?;
    peer.committed(&original, post, control).await?;
    node.controls.wait_receive_starts(2).await?;
    assert!(node.record()?.is_none());
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    peer.close().await?;
    node.stop().await;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum OrdinaryOutcome {
    Complete,
    Abandon,
    Defer,
    DeadLetter,
}

pub(super) async fn ordinary_outcome<P: StoreProvider>(
    provider: P,
    expected: OrdinaryOutcome,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    let mut peer = Peer::connect(node.address).await?;
    let original = peer.consumer().await?;
    assert_canonical_hold(&node, &original)?;
    node.controls.reset_io();
    let state = match expected {
        OrdinaryOutcome::Complete => DeliveryState::Accepted(amqp::Accepted),
        OrdinaryOutcome::Abandon => DeliveryState::Released(amqp::Released),
        OrdinaryOutcome::Defer => DeliveryState::Modified(amqp::Modified {
            undeliverable_here: Some(true),
            ..amqp::Modified::default()
        }),
        OrdinaryOutcome::DeadLetter => {
            let mut info = amqp::Fields::new();
            info.insert(
                Symbol::from("DeadLetterReason"),
                Value::String("wire-rejection-reason".into()),
            );
            info.insert(
                Symbol::from("DeadLetterErrorDescription"),
                Value::String("wire-rejection-description".into()),
            );
            DeliveryState::Rejected(amqp::Rejected {
                error: Some(amqp::Error {
                    condition: amqp::ErrorCondition::Custom(Symbol::from(
                        "com.microsoft:dead-letter",
                    )),
                    description: Some("unused fallback".into()),
                    info: Some(info),
                }),
            })
        }
    };
    peer.outcome(original.id, state).await?;
    let disposition = peer.disposition(RECEIVE, Role::Sender, original.id).await?;
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    node.controls.wait_receive_starts(2).await?;
    match expected {
        OrdinaryOutcome::Complete => assert!(node.record()?.is_none()),
        OrdinaryOutcome::Abandon => {
            let record = node.record()?.expect("released original remains available");
            assert_eq!(record.state, MessageState::Ready);
            assert_eq!(record.delivery_count, 1);
            // Reacquire a copied snapshot without changing the real listener's exact IO counts.
            let copied = storage::MemoryStore::default();
            let mut batch = WriteBatch::default();
            for (key, value) in node.snapshot()?.entries() {
                batch.push_put(key.clone(), value.clone());
            }
            copied.apply(batch)?;
            let outcome = StateMachine::new(copied).apply(&Command::new(
                node.namespace.clone(),
                EntityPath::new("orders")?,
                Timestamp::from_millis(2_000),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            ))?;
            let CommandOutcome::Received(Some(delivery)) = outcome else {
                return Err("released original must be reacquirable".into());
            };
            assert_eq!(delivery.sequence, node.controls.held().sequence);
            assert_eq!(delivery.delivery_count, 2);
        }
        OrdinaryOutcome::Defer => {
            let record = node.record()?.expect("deferred original remains stored");
            assert_eq!(record.state, MessageState::Deferred);
            assert_eq!(record.sequence, node.controls.held().sequence);
        }
        OrdinaryOutcome::DeadLetter => {
            assert!(node.record()?.is_none());
            let record = StateMachine::new(node.store.clone())
                .dead_lettered_message(
                    &node.namespace,
                    &EntityPath::new("orders")?,
                    node.controls.held().sequence,
                )?
                .expect("canonical shadow original");
            assert_eq!(record.body, b"held-body");
            assert_eq!(record.state, MessageState::Ready);
            let info = record.dead_letter.expect("wire dead-letter detail");
            assert_eq!(
                info.reason,
                domain::DeadLetterReason::Application("wire-rejection-reason".into())
            );
            assert_eq!(info.description, "wire-rejection-description");
        }
    }
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.receive_applied.load(Ordering::SeqCst), 1);
    peer.close().await?;
    node.stop().await;
    Ok(())
}
