use super::*;

pub(super) async fn unpolled_and_capacity_cancel<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::new(provider)?;
    node.seed("untouched", None)?;
    let before = node.store.snapshot()?;
    node.controls.reset();
    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    let future = assert_owned(node.handle().receive_fenced_owned(submission));
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    assert!(node.handle().requests.is_empty());
    drop(permit.clone());
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    drop(future);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    node.assert_no_work(&before)?;

    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    let future = assert_owned(protocol_amqp::Broker::receive_fenced_owned(
        &node.handle(),
        submission,
    ));
    drop(future);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    node.assert_no_work(&before)?;

    let (requests, incoming) = super::super::super::request_queue::bounded(1);
    let fake = BrokerHandle {
        requests,
        watchers: Arc::new(Watchers::default()),
    };
    let (reply, _response) = flume::bounded(1);
    fake.requests.send(Request::LastApplied { reply })?;
    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    let mut future = Box::pin(fake.receive_fenced_owned(submission));
    poll_pending(future.as_mut());
    assert_eq!(
        fake.requests.len(),
        1,
        "the full slot retains only its original request"
    );
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    drop(future);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert!(matches!(incoming.recv()?, Request::LastApplied { .. }));
    assert!(fake.requests.is_empty());
    node.assert_no_work(&before)?;

    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    let mut future = Box::pin(fake.receive_fenced_owned(submission));
    poll_pending(future.as_mut());
    assert_eq!(fake.requests.len(), 1);
    drop(incoming);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert_eq!(
        timeout(DEADLINE, future).await?,
        Err(ReceiveSubmitError::OwnerUnavailable(
            ReceiveOwnerUnavailableCause::ResponseUnavailable,
        ))
    );
    node.assert_no_work(&before)?;

    let retained = node.handle();
    node.stop();
    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    assert_eq!(
        timeout(DEADLINE, retained.receive_fenced_owned(submission)).await?,
        Err(ReceiveSubmitError::OwnerUnavailable(
            ReceiveOwnerUnavailableCause::Stopped
        ))
    );
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    node.assert_no_work(&before)?;
    Ok(())
}

pub(super) async fn queued_cancel<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.seed("untouched", None)?;
    let before = node.store.snapshot()?;
    let (release, parked) = node.park_owner().await?;
    node.controls.reset();
    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    let mut future = Box::pin(node.handle().receive_fenced_owned(submission));
    poll_pending(future.as_mut());
    assert_eq!(node.handle().requests.len(), 1);
    drop(future);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    release.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    node.fence().await?;
    assert_eq!(
        node.controls.reads.load(Ordering::SeqCst),
        1,
        "only the pure owner fence may read"
    );
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.store.snapshot()?, before);
    assert!(matches!(
        node.record(1)?.unwrap().state,
        MessageState::Ready
    ));
    Ok(())
}

pub(super) async fn queued_expiry<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.seed("untouched", None)?;
    let before = node.store.snapshot()?;
    let (release, parked) = node.park_owner().await?;
    node.controls.reset();
    // A deliberately invalid owner horizon needs no wall-clock sleep. Even a
    // regressed domain clock must not be consulted by this denied admission.
    node.controls.now.store(0, Ordering::SeqCst);
    let (permit, submission) = node.submission(ReceiveMode::ReceiveAndDelete, 0);
    let mut future = Box::pin(node.handle().receive_fenced_owned(submission));
    poll_pending(future.as_mut());
    assert_eq!(node.handle().requests.len(), 1);
    release.release();
    timeout(DEADLINE, parked.recv_async()).await???;
    assert_eq!(
        timeout(DEADLINE, future).await?,
        Err(ReceiveSubmitError::Claim(
            ReceiveClaimError::AuthorizationExpired
        ))
    );
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert!(!permit.cancel(), "owner expiry won the Pending transition");
    node.assert_no_work(&before)?;
    Ok(())
}

pub(super) async fn stale_and_wrong_routes<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.seed("original", None)?;
    let handle = node.handle();
    handle.submit_blocking(
        node.binding.namespace().clone(),
        node.binding.target().clone(),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    handle.submit_blocking(
        node.binding.namespace().clone(),
        node.binding.target().clone(),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let replacement = node.seed("replacement", None)?;
    assert_eq!(
        replacement,
        SequenceNumber::new(2),
        "deletion retains the sequence counter"
    );
    let before = node.store.snapshot()?;
    node.controls.reset();
    node.controls.now.store(0, Ordering::SeqCst);
    let (permit, submission) = node.submission(ReceiveMode::ReceiveAndDelete, u64::MAX);
    assert_eq!(
        timeout(DEADLINE, handle.receive_fenced_owned(submission)).await?,
        Err(ReceiveSubmitError::Refused(BrokerError::EntityBindingStale))
    );
    assert_eq!(
        permit.state(),
        ReceiveClaimState::Started,
        "Started is only admission, not application"
    );
    assert!(
        node.controls.reads.load(Ordering::SeqCst) > 0,
        "stale validation may read incarnation metadata"
    );
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 0);
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(
        node.record(replacement.as_u64())?.unwrap().message_id,
        "replacement"
    );

    let binding = StateMachine::new(node.store.clone())
        .bind_entity(
            node.binding.namespace(),
            node.binding.target(),
            node.binding.owner(),
            EntityIncarnationKind::Queue,
        )?
        .ok_or("replacement binding")?;
    node.controls.reset();
    let (permit, ticket) = ReceiveClaimPermit::new(u64::MAX);
    let submission = OwnedReceiveSubmission::new(
        binding,
        EntityPath::new("other")?,
        ReceiveMode::ReceiveAndDelete,
        None,
        ticket,
    );
    assert_eq!(
        timeout(DEADLINE, handle.receive_fenced_owned(submission)).await?,
        Err(ReceiveSubmitError::Refused(
            BrokerError::InvalidEntityBinding
        ))
    );
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    node.assert_no_work(&before)?;
    Ok(())
}
