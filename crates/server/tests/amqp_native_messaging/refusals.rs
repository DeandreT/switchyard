use super::*;

#[path = "refusals/authorization.rs"]
mod authorization;
pub(super) use authorization::listen_and_send_permissions_precede_target_binding;

pub(super) async fn restricted_profiles_and_posting_only_policy_refuse_before_bind<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    for variant in 0..7 {
        let mut peer = Peer::connect(node.address).await?;
        peer.begin(RECEIVE).await?;
        let mut request = Peer::attach_request(RECEIVE, RECEIVE_HANDLE, "orders", Role::Receiver);
        match variant {
            0 => request.rcv_settle_mode = ReceiverSettleMode::First,
            1 => request.snd_settle_mode = SenderSettleMode::Settled,
            2 => {
                request.source.as_mut().expect("Source").filter = Some(
                    [(Symbol::from("selector"), Value::String("true".into()))]
                        .into_iter()
                        .collect(),
                )
            }
            3 => request.source.as_mut().expect("Source").dynamic = true,
            4 => request.source.as_mut().expect("Source").durable = 1,
            5 => {
                request.source.as_mut().expect("Source").distribution_mode =
                    Some(Symbol::from("copy"))
            }
            6 => {
                request.source.as_mut().expect("Source").distribution_mode =
                    Some(Symbol::from("unknown-mode"))
            }
            _ => unreachable!(),
        }
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.attach(RECEIVE, request).await?;
        peer.detached(RECEIVE, RECEIVE_HANDLE, Some("amqp:not-allowed"), true)
            .await?;
        node.unchanged(&before)?;
        assert_eq!(
            node.controls.binds.load(Ordering::SeqCst),
            0,
            "unsupported profile {variant} must not probe topology"
        );
        assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        // The refused receiver cannot prevent an independent native posting link from working.
        peer.producer(HEALTHY, "healthy").await?;
        peer.barrier(HEALTHY).await?;
        peer.close().await?;
    }
    let (address, listener) = node.additional_listener(ListenerMode::Posting).await?;
    let mut peer = Peer::connect(address).await?;
    peer.begin(RECEIVE).await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.attach(
        RECEIVE,
        Peer::attach_request(RECEIVE, RECEIVE_HANDLE, "orders", Role::Receiver),
    )
    .await?;
    peer.detached(RECEIVE, RECEIVE_HANDLE, Some("amqp:not-implemented"), true)
        .await?;
    node.unchanged(&before)?;
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
    peer.producer(HEALTHY, "healthy").await?;
    peer.barrier(HEALTHY).await?;
    peer.close().await?;
    listener.abort();
    let _ = listener.await;
    node.stop().await;
    Ok(())
}

pub(super) async fn unsupported_sources_do_not_acquire_messages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, ListenerMode::Messaging).await?;
    for (entity, bind_count) in [
        ("topic", 1),
        ("session", 1),
        ("topic/Subscriptions/Alpha", 0),
        ("orders/$DeadLetterQueue", 0),
    ] {
        let mut peer = Peer::connect(node.address).await?;
        peer.begin(RECEIVE).await?;
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.attach(
            RECEIVE,
            Peer::attach_request(RECEIVE, RECEIVE_HANDLE, entity, Role::Receiver),
        )
        .await?;
        peer.detached(RECEIVE, RECEIVE_HANDLE, Some("amqp:not-allowed"), true)
            .await?;
        node.unchanged(&before)?;
        assert_eq!(
            node.controls.binds.load(Ordering::SeqCst),
            bind_count,
            "{entity}"
        );
        assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        peer.producer(HEALTHY, "healthy").await?;
        peer.barrier(HEALTHY).await?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}
