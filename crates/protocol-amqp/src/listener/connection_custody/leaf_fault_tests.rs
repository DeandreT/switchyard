//! Concrete original parent observation, separate from captured leaf controls.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn leaf_notice_latches_on_original_parent_observer_and_keeps_connection_identity() {
    let (old, _old_peer, writes) = opened().await;
    let (independent, mut peer, _independent_writes) = opened().await;
    writes.hold();
    let mut original_close = Box::pin(old.close_owned());
    timeout(LIMIT, async {
        tokio::select! {
            result = original_close.as_mut() => panic!("original native write held: {result:?}"),
            () = writes.reached() => {},
        }
    })
    .await
    .unwrap();
    let mut queued = Vec::new();
    for _ in 0..256 {
        let mut original = Box::pin(tokio::task::unconstrained(old.close_owned()));
        pending_once(original.as_mut()).await;
        queued.push(original);
    }
    let mut overflow = Box::pin(tokio::task::unconstrained(old.close_owned()));
    pending_once(overflow.as_mut()).await;
    let mut parent = ConnectionCustody::new(old);
    let request = parent.request_handle();
    for _ in 0..2 {
        let mut borrowed = Box::pin(parent.retirement_observer());
        pending_once(borrowed.as_mut()).await;
    }
    assert!(!request.is_requested());
    request.request();
    request.clone().request();
    // Dropping an already-completed observer cannot consume the watch latch.
    for _ in 0..2 {
        timeout(LIMIT, parent.retirement_observer()).await.unwrap();
    }
    assert!(parent.is_retired() && request.is_requested());
    timeout(LIMIT, original_close.as_mut())
        .await
        .unwrap()
        .unwrap_err();
    for original in queued {
        assert!(original.await.is_err());
    }
    assert!(overflow.await.is_err());
    assert!(writes.state.lock().unwrap().held);
    parent.record_primary(Ok(Ok(())));
    timeout(LIMIT, parent.finish()).await.unwrap();
    let shutdown = parent.shutdown.clone();
    timeout(LIMIT, parent.finish()).await.unwrap();
    assert_eq!(parent.shutdown, shutdown);
    parent.finish_result().unwrap();
    request.request();
    // Same container/session numbers on a distinct connection remain usable.
    let mut independent = ConnectionCustody::new(independent);
    begin(&mut peer, 1).await;
    let incoming = timeout(LIMIT, independent.connection.next_incoming_session())
        .await
        .unwrap()
        .unwrap();
    let session = timeout(LIMIT, independent.connection.accept_session(incoming))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        timeout(LIMIT, read_frame(&mut peer))
            .await
            .unwrap()
            .unwrap(),
        Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    assert!(!independent.is_retired());
    assert!(!session.is_ended());
    independent.record_primary(Ok(Ok(())));
    timeout(LIMIT, independent.finish()).await.unwrap();
    independent.finish_result().unwrap();
}

#[derive(Clone)]
struct FaultBroker {
    sends: bool,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Broker for FaultBroker {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert!(if self.sends {
            matches!(kind, CommandKind::Send { .. })
        } else {
            matches!(kind, CommandKind::Receive { .. })
        });
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        panic!("controlled admitted data-leaf original fault")
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

async fn while_parent<F: Future + ?Sized>(
    parent: Pin<&mut F>,
    peer: &mut DuplexStream,
) -> Performative {
    timeout(LIMIT, async {
        tokio::select! {
            _ = parent => panic!("parent remains active during admission"),
            frame = read_frame(peer) => match frame.unwrap() {
                Frame::Amqp { performative: Some(performative), .. } => performative,
                _ => panic!("actual admission response"),
            },
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn adopted_data_leaf_faults_publish_the_original_connection_request() {
    // Qualified original-poll poison: integration plumbing, not a committed-store control.
    for sends in [false, true] {
        let (connection, mut peer, _writes) = opened().await;
        let mut parent = ConnectionCustody::new(connection);
        let request = parent.request_handle();
        let broker = FaultBroker {
            sends,
            calls: Arc::default(),
        };
        let mut original = Box::pin(
            AssertUnwindSafe(crate::listener::serve_open_connection(
                &mut parent,
                NamespaceName::new("tenant").unwrap(),
                broker.clone(),
                None,
            ))
            .catch_unwind(),
        );
        begin(&mut peer, 1).await;
        assert!(matches!(
            while_parent(original.as_mut(), &mut peer).await,
            Performative::Begin(_)
        ));
        let attach = amqp::Attach {
            name: "owned-data-notice".to_owned(),
            handle: 1,
            role: if sends {
                amqp::Role::Sender
            } else {
                amqp::Role::Receiver
            },
            snd_settle_mode: amqp::SenderSettleMode::Unsettled,
            rcv_settle_mode: amqp::ReceiverSettleMode::First,
            source: (!sends).then(|| amqp::Source::new("orders")),
            target: sends.then(|| amqp::Target::new("orders")),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: sends.then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        write_frame(&mut peer, &frame(1, Performative::Attach(Box::new(attach))))
            .await
            .unwrap();
        assert!(matches!(
            while_parent(original.as_mut(), &mut peer).await,
            Performative::Attach(_)
        ));
        assert!(!request.is_requested());
        if sends {
            assert!(matches!(
                while_parent(original.as_mut(), &mut peer).await,
                Performative::Flow(_)
            ));
            let mut transfer = frame(
                1,
                Performative::Transfer(amqp::Transfer {
                    handle: 1,
                    delivery_id: Some(0),
                    delivery_tag: Some(vec![0].into()),
                    message_format: Some(0),
                    settled: Some(false),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                }),
            );
            let Frame::Amqp { payload, .. } = &mut transfer else {
                unreachable!()
            };
            *payload = amqp::encode_message(&amqp::Message::default()).unwrap();
            write_frame(&mut peer, &transfer).await.unwrap();
        } else {
            write_frame(
                &mut peer,
                &frame(
                    1,
                    Performative::Flow(amqp::Flow {
                        next_incoming_id: Some(0),
                        incoming_window: 100,
                        next_outgoing_id: 0,
                        outgoing_window: 100,
                        handle: Some(1),
                        delivery_count: Some(0),
                        link_credit: Some(1),
                        available: None,
                        drain: false,
                        echo: false,
                        properties: None,
                    }),
                ),
            )
            .await
            .unwrap();
        }
        let primary = timeout(LIMIT, original.as_mut()).await.unwrap();
        drop(original);
        assert!(request.is_requested());
        assert_eq!(broker.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        parent.record_primary(primary);
        timeout(LIMIT, parent.retirement_observer()).await.unwrap();
        timeout(LIMIT, parent.finish()).await.unwrap();
        parent.finish_result().unwrap();
    }
}
