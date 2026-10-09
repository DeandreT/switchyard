//! Native phases and captured route cleanup under an actual outer-pump fault.

#[path = "leaf_fault_tests.rs"]
mod leaf_fault_tests;

use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::custody::{PUMP_FAULT, PumpFault};
use super::*;

struct PollWitness<F> {
    actual: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F: Future> Future for PollWitness<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.actual.as_mut().poll(context)
    }
}

fn witness<F: Future>(actual: F, polls: &Arc<AtomicUsize>) -> PollWitness<F> {
    PollWitness {
        actual: Box::pin(actual),
        polls: Arc::clone(polls),
    }
}

fn assert_outer_panic(result: std::thread::Result<Result<(), ManagementError>>) {
    assert_eq!(
        result
            .expect_err("outer pump panic resumes")
            .downcast_ref::<&str>(),
        Some(&"controlled outer management pump panic")
    );
}

async fn transfer(wire: &mut Wire) -> Transfer {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer)),
        payload,
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("original native reply transfer")
    };
    assert_eq!(channel, CHANNEL);
    assert_eq!(
        decode_message(&payload)
            .unwrap()
            .properties
            .unwrap()
            .correlation_id,
        Some(MessageId::Ulong(42))
    );
    transfer
}

async fn accept(wire: &mut Wire, id: u32) {
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .unwrap();
}

async fn replacement_survives(
    management: &ConnectionManagement,
    responses: &mut mpsc::Receiver<ManagementResponse>,
) {
    assert!(matches!(
        responses.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    management
        .route_response(ADDRESS, response(999))
        .await
        .unwrap();
    assert_eq!(
        responses.recv().await.unwrap().correlation_id,
        MessageId::Ulong(999)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_retains_actual_begun_ack_or_reject_until_native_stop_or_error() {
    for reject in [false, true] {
        for fail in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("request receiver")
            };
            let management = ConnectionManagement::new();
            let (_route, mut responses) = management.register_reply_route(ADDRESS.to_owned()).await;
            if reject {
                wire.request_message(&Message::default()).await;
            } else {
                wire.request().await;
            }
            wire.barrier().await;
            wire.writes.arm(false);
            let fault = PumpFault::new(PumpPoint::RequestNative);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests(
                        receiver,
                        NamespaceName::new("tenant").unwrap(),
                        EntityPath::new("orders").unwrap(),
                        NoBroker,
                        management,
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
            }
            wire.no_frame_yet().await;
            if fail {
                wire.writes.fail_held_write();
            } else {
                wire.stop().await;
            }
            assert!(wire.writes.state.lock().unwrap().held);
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_keeps_cached_response_and_retires_full_or_missing_route_without_retry()
{
    for point in [PumpPoint::RequestResponse, PumpPoint::RequestRouting] {
        for full in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("request receiver")
            };
            let management = ConnectionManagement::new();
            let old = if full {
                let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
                for id in 0..REPLY_BUFFER as u64 {
                    route.send(response(id)).await.unwrap();
                }
                Some((route, responses))
            } else {
                None
            };
            wire.request().await;
            wire.barrier().await;
            let fault = PumpFault::new(point);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests(
                        receiver,
                        NamespaceName::new("tenant").unwrap(),
                        EntityPath::new("orders").unwrap(),
                        NoBroker,
                        Arc::clone(&management),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            if point == PumpPoint::RequestRouting {
                let Performative::Disposition(disposition) = wire.control(CHANNEL).await else {
                    panic!("original ACK")
                };
                assert_eq!(disposition.first, 1);
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                wire.barrier().await;
                pending_once(serving.as_mut()).await;
            }
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            let (_replacement, mut replacement_responses) =
                management.register_reply_route(ADDRESS.to_owned()).await;
            fault.trigger.notify_one();
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            replacement_survives(&management, &mut replacement_responses).await;
            if let Some((route, mut responses)) = old {
                assert_eq!(route.capacity(), 0);
                for id in 0..REPLY_BUFFER as u64 {
                    assert_eq!(
                        responses.try_recv().unwrap().correlation_id,
                        MessageId::Ulong(id)
                    );
                }
                assert!(matches!(
                    responses.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            }
            wire.barrier().await;
            wire.no_frame_yet().await;
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_before_native_start_closes_only_the_captured_route() {
    for point in [PumpPoint::ReplyPrepared, PumpPoint::ReplyIdle] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("reply sender")
        };
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        route.send(response(42)).await.unwrap();
        let fault = PumpFault::new(point);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_management_replies(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&management),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        pending_once(serving.as_mut()).await;
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        fault.trigger.notify_one();
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert!(route.is_closed());
        replacement_survives(&management, &mut replacement_responses).await;
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_retains_actual_begun_write_and_second_confirmation_before_route_cleanup()
{
    for (settle, confirmation) in [
        (ReceiverSettleMode::First, false),
        (ReceiverSettleMode::Second, false),
        (ReceiverSettleMode::Second, true),
    ] {
        for fail in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle.clone()).await;
            let LinkEndpoint::Sender(sender) = endpoint else {
                panic!("reply sender")
            };
            let management = ConnectionManagement::new();
            let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
            route.send(response(42)).await.unwrap();
            if !confirmation {
                wire.writes.arm(false);
            }
            let fault = PumpFault::new(PumpPoint::ReplyNative);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_replies(
                        sender,
                        ADDRESS.to_owned(),
                        route.clone(),
                        responses,
                        Arc::clone(&management),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            if confirmation {
                let transfer = transfer(&mut wire).await;
                accept(&mut wire, transfer.delivery_id.unwrap()).await;
                wire.barrier().await;
                wire.writes.arm(false);
                pending_once(serving.as_mut()).await;
            }
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            let (_replacement, mut replacement_responses) =
                management.register_reply_route(ADDRESS.to_owned()).await;
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
                assert!(route.is_closed());
            }
            wire.no_frame_yet().await;
            if fail {
                wire.writes.fail_held_write();
            } else {
                wire.stop().await;
            }
            assert!(wire.writes.state.lock().unwrap().held);
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            replacement_survives(&management, &mut replacement_responses).await;
            wire.stop().await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_reply_panic_keeps_ready_outcome_without_starting_a_new_second_confirmation() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::Second).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("reply sender")
    };
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    route.send(response(42)).await.unwrap();
    let fault = PumpFault::new(PumpPoint::ReplyNative);
    let mut serving = Box::pin(
        PUMP_FAULT.scope(
            Arc::clone(&fault),
            AssertUnwindSafe(serve_management_replies(
                sender,
                ADDRESS.to_owned(),
                route.clone(),
                responses,
                Arc::clone(&management),
                None,
            ))
            .catch_unwind(),
        ),
    );
    pending_once(serving.as_mut()).await;
    let transfer = transfer(&mut wire).await;
    accept(&mut wire, transfer.delivery_id.unwrap()).await;
    wire.barrier().await;
    fault.trigger.notify_one();
    assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
    assert!(route.is_closed());
    assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
    wire.barrier().await;
    wire.no_frame_yet().await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reply_owner_retains_cached_native_packet_across_cancelled_conditional_route_cleanup() {
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle.clone()).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("reply sender")
        };
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            send_management_response(&sender, response(42), control.clone()),
            control,
        ));
        pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
        let transfer = transfer(&mut wire).await;
        accept(&mut wire, transfer.delivery_id.unwrap()).await;
        wire.barrier().await;
        if settle == ReceiverSettleMode::Second {
            pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
            let Performative::Disposition(confirmation) = wire.control(CHANNEL).await else {
                panic!("original confirmation")
            };
            assert_eq!(confirmation.first, transfer.delivery_id.unwrap());
            assert!(confirmation.settled);
        }
        assert!(matches!(
            timeout(WAIT, custody.original.as_mut().unwrap().observe())
                .await
                .unwrap(),
            Some(Ok(Outcome::Accepted(_)))
        ));
        custody.capture_packet();
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        let held = management.routes.lock().await;
        for _ in 0..2 {
            pending_once(Box::pin(custody.finish(sender.on_detach_owned())).as_mut()).await;
            assert!(route.is_closed() && custody.original.is_none());
            let packet = custody.packet.as_ref().unwrap();
            assert!(packet.started && !packet.panicked);
            assert!(matches!(
                packet.result.as_ref(),
                Some(Ok(Outcome::Accepted(_)))
            ));
        }
        drop(held);
        timeout(WAIT, custody.finish(sender.on_detach_owned()))
            .await
            .unwrap();
        timeout(WAIT, custody.finish(sender.on_detach_owned()))
            .await
            .unwrap();
        assert!(matches!(
            custody.packet.as_ref().unwrap().result.as_ref(),
            Some(Ok(Outcome::Accepted(_)))
        ));
        replacement_survives(&management, &mut replacement_responses).await;
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_concurrent_finish_keeps_completed_or_poisoned_branch_without_repoll() {
    for poison in [false, true] {
        for reply_first in [false, true] {
            let management = ConnectionManagement::new();
            let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
            let mut custody =
                ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
            let released = Arc::new(Notify::new());
            let reply_polls = Arc::new(AtomicUsize::new(0));
            let close_polls = Arc::new(AtomicUsize::new(0));
            let payload = Arc::new(if reply_first {
                "original reply poison"
            } else {
                "original close poison"
            });
            let reply_control = OperationControl::new();
            let close_control = OperationControl::new();
            assert!(reply_control.begin() && close_control.begin());
            let reply_released = Arc::clone(&released);
            let reply_payload = Arc::clone(&payload);
            custody.original = Some(PendingOperation::new(
                witness(
                    async move {
                        if reply_first {
                            if poison {
                                std::panic::panic_any(reply_payload);
                            }
                        } else {
                            reply_released.notified().await;
                        }
                        Ok(Outcome::Accepted(Accepted))
                    },
                    &reply_polls,
                ),
                reply_control,
            ));
            let close_released = Arc::clone(&released);
            let close_payload = Arc::clone(&payload);
            custody.close = Some(PendingOperation::new(
                witness(
                    async move {
                        if !reply_first {
                            if poison {
                                std::panic::panic_any(close_payload);
                            }
                        } else {
                            close_released.notified().await;
                        }
                        Ok(())
                    },
                    &close_polls,
                ),
                close_control,
            ));
            let completed_polls = if reply_first {
                &reply_polls
            } else {
                &close_polls
            };
            let held_polls = if reply_first {
                &close_polls
            } else {
                &reply_polls
            };
            for _ in 0..2 {
                pending_once(Box::pin(custody.finish(std::future::pending())).as_mut()).await;
                assert_eq!(completed_polls.load(Ordering::SeqCst), 1);
                assert!(held_polls.load(Ordering::SeqCst) >= 1);
                assert!(route.is_closed());
            }
            released.notify_one();
            timeout(WAIT, custody.finish(std::future::pending()))
                .await
                .unwrap();
            let held_final_polls = held_polls.load(Ordering::SeqCst);
            timeout(WAIT, custody.finish(std::future::pending()))
                .await
                .unwrap();
            assert_eq!(completed_polls.load(Ordering::SeqCst), 1);
            assert_eq!(held_polls.load(Ordering::SeqCst), held_final_polls);
            let reply = custody.packet.as_ref().unwrap();
            let close = custody.close_packet.as_ref().unwrap();
            assert_eq!(reply.panicked, poison && reply_first);
            assert_eq!(close.panicked, poison && !reply_first);
            assert_eq!(reply.result.is_some(), !(poison && reply_first));
            assert_eq!(close.result.is_some(), !(poison && !reply_first));
            if poison {
                let retained = custody.take_cleanup_panic().unwrap();
                assert!(Arc::ptr_eq(
                    retained.downcast_ref::<Arc<&str>>().unwrap(),
                    &payload
                ));
            } else {
                assert!(custody.take_cleanup_panic().is_none());
            }
            assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
        }
    }
}

#[test]
fn primary_panic_error_and_original_native_error_precede_secondary_cleanup_panic() {
    let primary = catch_unwind(AssertUnwindSafe(|| {
        finish_pump::<()>(
            Err(Box::new("primary pump panic")),
            Some(Box::new("secondary panic")),
            Some(EngineError::Stopped),
        )
    }));
    assert_eq!(
        primary.unwrap_err().downcast_ref::<&str>(),
        Some(&"primary pump panic")
    );
    let error = finish_pump::<()>(
        Ok(Err(
            EngineError::InvalidState("primary error".to_owned()).into()
        )),
        Some(Box::new("secondary panic")),
        Some(EngineError::Stopped),
    )
    .unwrap_err();
    assert!(
        matches!(error.downcast_ref::<EngineError>(), Some(EngineError::InvalidState(value)) if value == "primary error")
    );
    let error = finish_pump(
        Ok(Ok(())),
        Some(Box::new("secondary panic")),
        Some(EngineError::RemoteDetached),
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::RemoteDetached)
    ));
    let secondary = catch_unwind(AssertUnwindSafe(|| {
        finish_pump::<()>(Ok(Ok(())), Some(Box::new("secondary panic")), None)
    }));
    assert_eq!(
        secondary.unwrap_err().downcast_ref::<&str>(),
        Some(&"secondary panic")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_no_credit_native_error_precedes_secondary_diagnostic_panic_after_route_cleanup() {
    struct PanicDiagnostics(Arc<AtomicUsize>);
    impl tracing::Subscriber for PanicDiagnostics {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target().ends_with("::management::custody")
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any("controlled secondary management diagnostic panic");
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for stop in [false, true] {
            let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, settle.clone()).await;
            let LinkEndpoint::Sender(sender) = endpoint else {
                panic!("reply sender")
            };
            let management = ConnectionManagement::new();
            let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
            route.send(response(42)).await.unwrap();
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                .join()
                .unwrap();
            let mut serving = Box::pin(
                AssertUnwindSafe(serve_management_replies(
                    sender,
                    ADDRESS.to_owned(),
                    route.clone(),
                    responses,
                    Arc::clone(&management),
                    None,
                ))
                .catch_unwind(),
            );
            let mut observed = Box::pin(poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || serving.as_mut().poll(context))
            }));
            pending_once(observed.as_mut()).await;
            wire.barrier().await;
            let (_replacement, mut replacement_responses) =
                management.register_reply_route(ADDRESS.to_owned()).await;
            if stop {
                wire.stop().await;
            } else {
                wire.detach().await;
            }
            let error = timeout(WAIT, observed.as_mut())
                .await
                .unwrap()
                .expect("secondary diagnostic panic does not mask original native error")
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::RemoteDetached)
            ));
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
            assert!(route.is_closed());
            replacement_survives(&management, &mut replacement_responses).await;
            wire.stop().await;
        }
    }
}
