use super::fixture::*;
use super::*;

#[tokio::test]
async fn null_target_sender_returns_only_a_retired_handle_after_real_detach() {
    let mut fixture = Fixture::new().await;
    let observed = caught(async {
        let caller = fixture.start(Role::Sender, "denied-sender");
        let request = fixture.request().await;
        fixture.null_response(&request, 17).await;
        fixture.barrier().await;
        assert!(fixture.pending(caller));
        fixture.no_activity(request.handle).await;
        fixture.gate.arm();
        fixture.detach(&request, 17).await;
        fixture.gate.entered().await;
        assert!(fixture.pending(caller));
        fixture.gate.release();
        fixture.complete(caller).await;
        let Endpoint::Sender(sender) = fixture.endpoint(caller) else {
            panic!("sender");
        };
        assert!(sender.identity.is_retired());
        assert!(matches!(
            bounded(sender.send(Message::data("denied"))).await,
            Err(EngineError::RemoteDetached)
        ));
        bounded(sender.on_detach()).await;
        bounded(sender.close()).await.expect("terminal close");
        fixture.barrier().await;
        fixture.no_activity(request.handle).await;
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    rethrow(observed);
}

#[tokio::test]
async fn null_source_receiver_never_refills_and_closes_original_delivery_channel() {
    let mut fixture = Fixture::new().await;
    let observed = caught(async {
        let caller = fixture.start(Role::Receiver, "denied-receiver");
        let request = fixture.request().await;
        fixture.null_response(&request, 17).await;
        fixture.barrier().await;
        assert!(fixture.pending(caller));
        fixture.no_activity(request.handle).await;
        fixture.gate.arm();
        fixture.detach(&request, 17).await;
        fixture.gate.entered().await;
        assert!(fixture.pending(caller));
        fixture.gate.release();
        fixture.complete(caller).await;
        let Endpoint::Receiver(receiver) = fixture.endpoint(caller) else {
            panic!("receiver");
        };
        assert!(receiver.identity.is_retired());
        assert!(receiver.source().is_none());
        assert!(*receiver.detached.borrow());
        assert!(matches!(
            bounded(receiver.recv()).await,
            Err(EngineError::RemoteDetached)
        ));
        assert!(matches!(
            bounded(receiver.recv_retained()).await,
            Err(EngineError::RemoteDetached)
        ));
        bounded(receiver.close()).await.expect("terminal close");
        fixture.barrier().await;
        fixture.no_activity(request.handle).await;
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    rethrow(observed);
}

#[tokio::test]
async fn refused_pending_name_and_alias_remain_owned_until_peer_detach() {
    let mut fixture = Fixture::new().await;
    let observed = caught(async {
        let first = fixture.start(Role::Sender, "held-0");
        let original = fixture.request().await;
        fixture.null_response(&original, 100).await;
        fixture.barrier().await;
        let duplicate = fixture.start(Role::Sender, "held-0");
        fixture.complete(duplicate).await;
        assert!(fixture.failed(duplicate));
        let other_session_duplicate = fixture.start_other(Role::Sender, "held-0").await;
        fixture.complete(other_session_duplicate).await;
        assert!(fixture.failed(other_session_duplicate));
        assert!(fixture.pending(first));
        let opposite = fixture.start(Role::Receiver, "held-0");
        let request = fixture.request().await;
        fixture.null_response(&request, 200).await;
        fixture.barrier().await;
        fixture.detach(&request, 200).await;
        fixture.complete(opposite).await;
        let mut pending = vec![(first, original, 100)];
        for index in 1..MAX_PENDING_ATTACHES {
            let caller = fixture.start(Role::Sender, format!("held-{index}"));
            let request = fixture.request().await;
            let peer_handle = 100 + index as u32;
            fixture.null_response(&request, peer_handle).await;
            fixture.barrier().await;
            assert!(fixture.pending(caller));
            pending.push((caller, request, peer_handle));
        }
        let excess = fixture.start(Role::Sender, "over-limit");
        fixture.complete(excess).await;
        assert!(fixture.failed(excess));
        assert!(!fixture.captured().await.iter().any(|frame| matches!(frame,
            Frame::Amqp { performative: Some(Performative::Attach(attach)), .. } if attach.name == "over-limit")));
        for (caller, request, peer_handle) in pending {
            fixture.no_activity(request.handle).await;
            fixture.detach(&request, peer_handle).await;
            fixture.complete(caller).await;
        }
        let reuse = fixture.start(Role::Sender, "held-0");
        let request = fixture.request().await;
        fixture.null_response(&request, 300).await;
        fixture.detach(&request, 300).await;
        fixture.complete(reuse).await;
    }).await;
    fixture.finish().await;
    fixture.joined();
    rethrow(observed);
}

#[tokio::test]
async fn cancelling_attach_waiter_does_not_cancel_refusal_cleanup() {
    let mut fixture = Fixture::new().await;
    let observed = caught(async {
        let caller = fixture.start(Role::Sender, "cancelled");
        let request = fixture.request().await;
        fixture.null_response(&request, 17).await;
        fixture.barrier().await;
        fixture.cancel_caller(caller).await;
        assert!(fixture.cancelled(caller));
        let duplicate = fixture.start(Role::Sender, "cancelled");
        fixture.complete(duplicate).await;
        assert!(fixture.failed(duplicate));
        fixture.detach(&request, 17).await;
        fixture.barrier().await;
        let reuse = fixture.start(Role::Sender, "cancelled");
        let request = fixture.request().await;
        fixture.null_response(&request, 18).await;
        fixture.detach(&request, 18).await;
        fixture.complete(reuse).await;
        fixture.no_activity(request.handle).await;
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    rethrow(observed);
}

#[tokio::test]
async fn missing_or_misrouted_detach_never_manufactures_refusal_success() {
    for case in 0..4 {
        let mut fixture = Fixture::new().await;
        let observed = caught(async {
            let caller = fixture.start(Role::Receiver, "waiting");
            let request = fixture.request().await;
            fixture.null_response(&request, 17).await;
            fixture.barrier().await;
            assert!(fixture.pending(caller));
            if case == 3 {
                let other = fixture.begin_other().await;
                fixture
                    .write_on(
                        other,
                        Performative::Detach(Detach {
                            handle: 17,
                            closed: true,
                            error: None,
                        }),
                    )
                    .await;
                assert!(matches!(fixture.read().await, Frame::Amqp {
                    channel, performative: Some(Performative::End(End { error: Some(_), .. })), ..
                } if channel != fixture.session.as_ref().expect("original session").channel));
                fixture.barrier().await;
                assert!(fixture.pending(caller));
                fixture.detach(&request, 17).await;
                fixture.complete(caller).await;
                let Endpoint::Receiver(receiver) = fixture.endpoint(caller) else {
                    panic!("original receiver");
                };
                assert!(receiver.identity.is_retired());
                fixture.no_activity(request.handle).await;
                return;
            }
            match case {
                0 => fixture.write(Performative::End(End::default())).await,
                1 => {
                    fixture
                        .write_on(
                            77,
                            Performative::Detach(Detach {
                                handle: 17,
                                closed: true,
                                error: None,
                            }),
                        )
                        .await
                }
                _ => {
                    fixture
                        .write(Performative::Detach(Detach {
                            handle: 991,
                            closed: true,
                            error: None,
                        }))
                        .await
                }
            }
            let frame = fixture.read().await;
            if case == 1 {
                assert!(matches!(
                    frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(Close { error: Some(_), .. })),
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    frame,
                    Frame::Amqp {
                        performative: Some(Performative::End(_)),
                        ..
                    }
                ));
            }
            fixture.complete(caller).await;
            assert!(fixture.failed(caller));
            fixture.no_activity(request.handle).await;
        })
        .await;
        fixture.finish().await;
        fixture.joined();
        rethrow(observed);
    }
}
