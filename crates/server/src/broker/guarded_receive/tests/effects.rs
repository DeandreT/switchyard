use super::*;

pub(super) async fn started_response_loss<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let first = node.seed("peeklock", None)?;
    let second = node.seed("delete", None)?;
    for (mode, sequence) in [
        (ReceiveMode::PeekLock, first),
        (ReceiveMode::ReceiveAndDelete, second),
    ] {
        node.controls.reset();
        let (observed, release) = node.controls.pause_apply();
        let (permit, submission) = node.submission(mode, u64::MAX);
        let mut future = Box::pin(node.handle().receive_fenced_owned(submission));
        poll_pending(future.as_mut());
        timeout(DEADLINE, observed.recv_async()).await??;
        assert_eq!(permit.state(), ReceiveClaimState::Started);
        assert!(
            !permit.cancel(),
            "transport cancellation cannot revoke Started admission"
        );
        drop(future);
        assert!(matches!(
            node.record(sequence.as_u64())?.unwrap().state,
            MessageState::Ready
        ));
        release.release();
        node.fence().await?;
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        match mode {
            ReceiveMode::PeekLock => {
                let original = node.record(sequence.as_u64())?.unwrap();
                assert_eq!(original.message_id, "peeklock");
                assert_eq!(original.body, b"peeklock");
                assert_eq!(original.delivery_count, 1);
                assert!(matches!(original.state, MessageState::Locked { .. }));
                assert_eq!(
                    node.store
                        .scan_prefix(
                            &keys::lock_prefix(node.binding.namespace(), node.binding.target(),),
                            2
                        )?
                        .len(),
                    1
                );
            }
            ReceiveMode::ReceiveAndDelete => assert!(node.record(sequence.as_u64())?.is_none()),
        }
        assert_eq!(
            permit.state(),
            ReceiveClaimState::Started,
            "the observer cannot report commit or rollback"
        );
    }
    let counters = node.counters()?;
    assert_eq!(counters.next_sequence, 3);
    assert_eq!(counters.next_lock_token, 2);
    assert!(matches!(
        node.record(first.as_u64())?.unwrap().state,
        MessageState::Locked { .. }
    ));
    assert!(node.record(second.as_u64())?.is_none());
    Ok(())
}

pub(super) async fn storage_unknown<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.seed("lock", None)?;
    node.seed("delete", None)?;
    for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
        let sequence = if mode == ReceiveMode::PeekLock { 1 } else { 2 };
        for committed in [false, true] {
            let before = node.store.snapshot()?;
            node.controls.reset();
            if committed {
                node.controls.fail_after.store(true, Ordering::SeqCst);
            } else {
                node.controls.fail_before.store(true, Ordering::SeqCst);
            }
            let (permit, submission) = node.submission(mode, u64::MAX);
            let result = timeout(DEADLINE, node.handle().receive_fenced_owned(submission)).await?;
            assert_eq!(
                result,
                Err(ReceiveSubmitError::OwnerUnavailable(
                    ReceiveOwnerUnavailableCause::Storage
                ))
            );
            assert_eq!(permit.state(), ReceiveClaimState::Started);
            assert!(!permit.cancel());
            assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
            assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
            let error = result.unwrap_err();
            assert!(!format!("{error:?} {error}").contains("private"));
            if committed {
                assert_ne!(
                    node.store.snapshot()?,
                    before,
                    "physical error may follow a complete batch"
                );
                match mode {
                    ReceiveMode::PeekLock => assert!(matches!(
                        node.record(sequence)?.unwrap().state,
                        MessageState::Locked { .. }
                    )),
                    ReceiveMode::ReceiveAndDelete => assert!(node.record(sequence)?.is_none()),
                }
            } else {
                assert_eq!(node.store.snapshot()?, before);
                assert!(matches!(
                    node.record(sequence)?.unwrap().state,
                    MessageState::Ready
                ));
            }
        }
    }
    let counters = node.counters()?;
    assert_eq!(counters.next_sequence, 3);
    assert_eq!(counters.next_lock_token, 2);
    Ok(())
}

pub(super) async fn empty_and_deadletter_effects<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    let (permit, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    assert_eq!(
        timeout(DEADLINE, node.handle().receive_fenced_owned(submission)).await??,
        None
    );
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    node.seed("short", Some(1))?;
    node.controls.now.store(2_002, Ordering::SeqCst);
    let shadow = node.binding.target().dead_letter_queue()?;
    let handle = node.handle();
    let watch = handle.watchers.watch(node.binding.namespace(), &shadow);
    let mut wake = Box::pin(watch.wait());
    poll_pending(wake.as_mut());
    let primary_watch = handle
        .watchers
        .watch(node.binding.namespace(), node.binding.target());
    let mut primary_wake = Box::pin(primary_watch.wait());
    poll_pending(primary_wake.as_mut());
    let (_, submission) = node.submission(ReceiveMode::PeekLock, u64::MAX);
    assert_eq!(
        timeout(DEADLINE, handle.receive_fenced_owned(submission)).await??,
        None
    );
    timeout(DEADLINE, wake).await?;
    poll_pending(primary_wake.as_mut());
    assert!(node.record(1)?.is_none());
    assert_eq!(
        node.store
            .scan_prefix(&keys::message_prefix(node.binding.namespace(), &shadow), 2)?
            .len(),
        1
    );

    let binding = timeout(
        DEADLINE,
        handle.bind(
            node.binding.namespace().clone(),
            Attachment::DeadLetter(node.binding.target().clone()),
        ),
    )
    .await??
    .ok_or("admitted shadow route")?
    .binding;
    assert_eq!(binding.target(), &shadow);
    assert_eq!(binding.owner(), node.binding.owner());
    node.controls.reset();
    let before = node.store.snapshot()?;
    let (_, ticket) = ReceiveClaimPermit::new(u64::MAX);
    let wrong = OwnedReceiveSubmission::new(
        binding.clone(),
        binding.owner().clone(),
        ReceiveMode::ReceiveAndDelete,
        None,
        ticket,
    );
    assert_eq!(
        timeout(DEADLINE, handle.receive_fenced_owned(wrong)).await?,
        Err(ReceiveSubmitError::Refused(
            BrokerError::InvalidEntityBinding
        ))
    );
    node.assert_no_work(&before)?;
    let (_, ticket) = ReceiveClaimPermit::new(u64::MAX);
    let correct =
        OwnedReceiveSubmission::new(binding, shadow, ReceiveMode::ReceiveAndDelete, None, ticket);
    let actual = timeout(DEADLINE, handle.receive_fenced_owned(correct))
        .await??
        .ok_or("the exact shadow original was not received")?;
    assert_eq!(actual.message_id, "short");
    assert_eq!(actual.body, b"short");
    assert!(actual.dead_letter.is_some());
    Ok(())
}
