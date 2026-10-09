//! Concrete native fronts and qualified original-poison custody controls.

use super::*;
use crate::management::custody::{OperationPacket, PanicPayload};

struct OriginalFault {
    trigger: Notify,
    payload: Arc<str>,
}

impl OriginalFault {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            trigger: Notify::new(),
            payload: Arc::from("controlled original management cleanup fault"),
        })
    }
}

async fn original_poison<T>(control: OperationControl, fault: Arc<OriginalFault>) -> T {
    assert!(control.begin());
    fault.trigger.notified().await;
    std::panic::panic_any(Arc::clone(&fault.payload))
}

fn raw_error<T>(packet: &OperationPacket<Result<T, EngineError>>) -> &EngineError {
    match packet.result.as_ref().unwrap() {
        Err(error) => error,
        Ok(_) => panic!("the actual native original failed"),
    }
}

async fn peer_detach(wire: &mut Wire) {
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Detach(amqp::Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
        ),
    )
    .await
    .unwrap();
}

struct PanicReport {
    reached: Arc<AtomicUsize>,
    payload: Arc<str>,
}

impl tracing::Subscriber for PanicReport {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::management::custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn report_fault(custody: &ReplyCustody<'_>) -> PanicPayload {
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let reached = Arc::new(AtomicUsize::new(0));
    let payload: Arc<str> = Arc::from("controlled secondary management report fault");
    let dispatch = tracing::Dispatch::new(PanicReport {
        reached: Arc::clone(&reached),
        payload: Arc::clone(&payload),
    });
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let caught = catch_unwind(AssertUnwindSafe(|| {
        tracing::dispatcher::with_default(&dispatch, || custody.report());
    }))
    .unwrap_err();
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<str>>().unwrap(),
        &payload
    ));
    caught
}

#[tokio::test(flavor = "current_thread")]
async fn reply_actual_error_notifies_before_qualified_opposite_wait_and_cancelled_join_retry() {
    let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("actual no-credit reply sender")
    };
    let notice = wire.retirement_request();
    let management = ConnectionManagement::new();
    let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
    let mut custody = ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
    let control = OperationControl::new();
    let reply_polls = Arc::new(AtomicUsize::new(0));
    custody.original = Some(PendingOperation::new(
        witness(
            send_management_response(&sender, response(42), control.clone()),
            &reply_polls,
        ),
        control.clone(),
    ));
    pending_once(
        Box::pin(tokio::task::unconstrained(
            custody.original.as_mut().unwrap().observe(),
        ))
        .as_mut(),
    )
    .await;
    assert!(control.started());
    wire.barrier().await;
    let close_release = Arc::new(Notify::new());
    let close_control = OperationControl::new();
    let close_polls = Arc::new(AtomicUsize::new(0));
    let begun = close_control.clone();
    let release = Arc::clone(&close_release);
    custody.close = Some(PendingOperation::new(
        witness(
            async move {
                assert!(begun.begin());
                release.notified().await;
                Ok(())
            },
            &close_polls,
        ),
        close_control.clone(),
    ));
    pending_once(
        Box::pin(tokio::task::unconstrained(
            custody.close.as_mut().unwrap().observe(),
        ))
        .as_mut(),
    )
    .await;
    assert!(close_control.started());
    peer_detach(&mut wire).await;
    timeout(WAIT, sender.on_detach_owned()).await.unwrap();
    let (_replacement, mut replacement_responses) =
        management.register_reply_route(ADDRESS.to_owned()).await;
    let held = management.routes.lock().await;
    assert!(!notice.is_requested());
    let mut finish =
        Box::pin(custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)));
    timeout(WAIT, async {
        tokio::select! {
            () = finish.as_mut() => panic!("qualified original Close remains held"),
            () = notice.observer() => {},
        }
    })
    .await
    .unwrap();
    drop(finish);
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
    let cached = timeout(WAIT, custody.original.as_mut().unwrap().observe())
        .await
        .unwrap()
        .unwrap();
    let Err(retained) = cached else {
        panic!("actual reply result is retained")
    };
    assert!(matches!(retained, EngineError::RemoteDetached));
    let original_pointer = retained as *const EngineError;
    let final_polls = reply_polls.load(Ordering::SeqCst);
    for _ in 0..2 {
        pending_once(
            Box::pin(tokio::task::unconstrained(
                custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
            ))
            .as_mut(),
        )
        .await;
        assert!(custody.packet.is_none() && custody.close_packet.is_none());
        let cached = custody.original.as_mut().unwrap().observe().await.unwrap();
        let Err(retained) = cached else {
            panic!("same actual reply error")
        };
        assert_eq!(retained as *const EngineError, original_pointer);
        assert_eq!(reply_polls.load(Ordering::SeqCst), final_polls);
        notice.request();
    }
    close_release.notify_one();
    pending_once(
        Box::pin(tokio::task::unconstrained(
            custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
        ))
        .as_mut(),
    )
    .await;
    assert!(
        custody
            .close_packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .is_ok()
    );
    let pointer = raw_error(custody.packet.as_ref().unwrap()) as *const EngineError;
    let final_close_polls = close_polls.load(Ordering::SeqCst);
    drop(held);
    timeout(
        WAIT,
        custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
    )
    .await
    .unwrap();
    assert_eq!(reply_polls.load(Ordering::SeqCst), final_polls);
    assert_eq!(close_polls.load(Ordering::SeqCst), final_close_polls);
    assert_eq!(
        raw_error(custody.packet.as_ref().unwrap()) as *const EngineError,
        pointer
    );
    assert!(custody.take_cleanup_panic().is_none());
    let diagnostic = report_fault(&custody);
    let error = finish_pump(Ok(Ok(())), Some(diagnostic), custody.take_native_error()).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<EngineError>(),
        Some(EngineError::RemoteDetached)
    ));
    assert!(route.is_closed());
    replacement_survives(&management, &mut replacement_responses).await;
}

#[tokio::test(flavor = "current_thread")]
async fn reply_cleanup_poison_notifies_before_opposite_actual_write_and_native_error_wins() {
    for reply_poison in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual reply sender")
        };
        let notice = wire.retirement_request();
        let (mut independent, _) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        independent.barrier().await;
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
        let fault = OriginalFault::new();
        let reply_polls = Arc::new(AtomicUsize::new(0));
        let close_polls = Arc::new(AtomicUsize::new(0));
        let reply_control = OperationControl::new();
        let close_control = OperationControl::new();
        if reply_poison {
            custody.original = Some(PendingOperation::new(
                witness(
                    original_poison(reply_control.clone(), Arc::clone(&fault)),
                    &reply_polls,
                ),
                reply_control.clone(),
            ));
            let begun = close_control.clone();
            let endpoint = &sender;
            custody.close = Some(PendingOperation::new(
                witness(
                    async move {
                        assert!(begun.begin());
                        endpoint
                            .close_with_error(unauthorized_error("original held management Close"))
                            .await
                    },
                    &close_polls,
                ),
                close_control.clone(),
            ));
            pending_once(
                Box::pin(tokio::task::unconstrained(
                    custody.original.as_mut().unwrap().observe(),
                ))
                .as_mut(),
            )
            .await;
        } else {
            custody.original = Some(PendingOperation::new(
                witness(
                    send_management_response(&sender, response(42), reply_control.clone()),
                    &reply_polls,
                ),
                reply_control.clone(),
            ));
            custody.close = Some(PendingOperation::new(
                witness(
                    original_poison(close_control.clone(), Arc::clone(&fault)),
                    &close_polls,
                ),
                close_control.clone(),
            ));
            pending_once(
                Box::pin(tokio::task::unconstrained(
                    custody.close.as_mut().unwrap().observe(),
                ))
                .as_mut(),
            )
            .await;
        }
        wire.writes.arm(false);
        if reply_poison {
            let mut actual = Box::pin(custody.close.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = actual.as_mut() => { let _ = result; panic!("actual Close write held") },
                () = wire.writes.reached() => {},
            }}).await.unwrap();
        } else {
            let mut actual = Box::pin(custody.original.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = actual.as_mut() => { let _ = result; panic!("actual reply write held") },
                () = wire.writes.reached() => {},
            }}).await.unwrap();
        }
        assert!(reply_control.started() && close_control.started());
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        let held = management.routes.lock().await;
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        let mut finish =
            Box::pin(custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)));
        timeout(WAIT, async {
            tokio::select! {
                () = finish.as_mut() => panic!("conditional route removal is still held"),
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
        drop(finish);
        assert!(wire.writes.state.lock().unwrap().held);
        // Only the captured notice stops these original native tasks; no release.
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        let poison_polls = if reply_poison {
            &reply_polls
        } else {
            &close_polls
        };
        let opposite_polls = if reply_poison {
            &close_polls
        } else {
            &reply_polls
        };
        assert_eq!(poison_polls.load(Ordering::SeqCst), 2);
        for _ in 0..2 {
            pending_once(
                Box::pin(tokio::task::unconstrained(
                    custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
                ))
                .as_mut(),
            )
            .await;
            assert_eq!(poison_polls.load(Ordering::SeqCst), 2);
            notice.request();
        }
        let native = if reply_poison {
            raw_error(custody.close_packet.as_ref().unwrap())
        } else {
            raw_error(custody.packet.as_ref().unwrap())
        };
        let pointer = native as *const EngineError;
        let description = native.to_string();
        let native_kind = std::mem::discriminant(native);
        let final_opposite_polls = opposite_polls.load(Ordering::SeqCst);
        pending_once(
            Box::pin(custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)))
                .as_mut(),
        )
        .await;
        let cached = if reply_poison {
            raw_error(custody.close_packet.as_ref().unwrap())
        } else {
            raw_error(custody.packet.as_ref().unwrap())
        };
        assert_eq!(cached as *const EngineError, pointer);
        assert_eq!(opposite_polls.load(Ordering::SeqCst), final_opposite_polls);
        drop(held);
        timeout(
            WAIT,
            custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
        )
        .await
        .unwrap();
        assert_eq!(poison_polls.load(Ordering::SeqCst), 2);
        let packet = if reply_poison {
            custody
                .packet
                .as_ref()
                .map(|packet| (packet.panicked, packet.result.is_none()))
        } else {
            custody
                .close_packet
                .as_ref()
                .map(|packet| (packet.panicked, packet.result.is_none()))
        };
        assert_eq!(packet, Some((true, true)));
        let cleanup = custody.take_cleanup_panic().unwrap();
        assert!(Arc::ptr_eq(
            cleanup.downcast_ref::<Arc<str>>().unwrap(),
            &fault.payload
        ));
        let diagnostic = report_fault(&custody);
        let native = custody.take_native_error().unwrap();
        assert_eq!(std::mem::discriminant(&native), native_kind);
        drop(diagnostic);
        let error = finish_pump(Ok(Ok(())), Some(cleanup), Some(native)).unwrap_err();
        assert_eq!(
            error.downcast_ref::<EngineError>().unwrap().to_string(),
            description
        );
        assert!(route.is_closed());
        replacement_survives(&management, &mut replacement_responses).await;
        notice.request();
        independent.barrier().await;
        independent.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reply_actual_cleanup_error_notifies_before_cancelled_conditional_route_cleanup() {
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, settle).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual no-credit sender")
        };
        let notice = wire.retirement_request();
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
        let control = OperationControl::new();
        let polls = Arc::new(AtomicUsize::new(0));
        custody.original = Some(PendingOperation::new(
            witness(
                send_management_response(&sender, response(42), control.clone()),
                &polls,
            ),
            control.clone(),
        ));
        pending_once(
            Box::pin(tokio::task::unconstrained(
                custody.original.as_mut().unwrap().observe(),
            ))
            .as_mut(),
        )
        .await;
        assert!(control.started());
        wire.barrier().await;
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        let held = management.routes.lock().await;
        peer_detach(&mut wire).await;
        timeout(WAIT, sender.on_detach_owned()).await.unwrap();
        assert!(!notice.is_requested());
        let mut finish =
            Box::pin(custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)));
        timeout(WAIT, async {
            tokio::select! {
                () = finish.as_mut() => panic!("actual route lock held"),
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
        drop(finish);
        let retained = raw_error(custody.packet.as_ref().unwrap());
        assert!(matches!(retained, EngineError::RemoteDetached));
        let pointer = retained as *const EngineError;
        let final_polls = polls.load(Ordering::SeqCst);
        for _ in 0..2 {
            pending_once(
                Box::pin(custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)))
                    .as_mut(),
            )
            .await;
            assert_eq!(
                raw_error(custody.packet.as_ref().unwrap()) as *const EngineError,
                pointer
            );
            assert_eq!(polls.load(Ordering::SeqCst), final_polls);
        }
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        drop(held);
        timeout(
            WAIT,
            custody.finish_with_retirement(sender.on_detach_owned(), Some(&notice)),
        )
        .await
        .unwrap();
        let diagnostic = report_fault(&custody);
        let error =
            finish_pump(Ok(Ok(())), Some(diagnostic), custody.take_native_error()).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<EngineError>(),
            Some(EngineError::RemoteDetached)
        ));
        assert!(route.is_closed());
        replacement_survives(&management, &mut replacement_responses).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn request_actual_ack_or_reject_cleanup_error_notifies_before_later_close_wait() {
    for reject in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(mut receiver) = endpoint else {
            panic!("actual request receiver")
        };
        wire.request().await;
        wire.barrier().await;
        let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
        let notice = wire.retirement_request();
        let native_control = OperationControl::new();
        let native_polls = Arc::new(AtomicUsize::new(0));
        let mut custody = RequestCustody::default();
        custody.native = Some(PendingOperation::new(
            witness(
                async {
                    assert!(native_control.begin());
                    if reject {
                        receiver.reject(&delivery, None).await
                    } else {
                        receiver.accept(&delivery).await
                    }
                },
                &native_polls,
            ),
            native_control.clone(),
        ));
        wire.writes.arm(false);
        let mut actual = Box::pin(custody.native.as_mut().unwrap().observe());
        timeout(WAIT, async { tokio::select! {
            result = actual.as_mut() => { let _ = result; panic!("original request disposition write held") },
            () = wire.writes.reached() => {},
        }}).await.unwrap();
        drop(actual);
        let close_release = Arc::new(Notify::new());
        let close_control = OperationControl::new();
        let close_polls = Arc::new(AtomicUsize::new(0));
        let release = Arc::clone(&close_release);
        let begun = close_control.clone();
        custody.close = Some(PendingOperation::new(
            witness(
                async move {
                    assert!(begun.begin());
                    release.notified().await;
                    Ok(())
                },
                &close_polls,
            ),
            close_control.clone(),
        ));
        assert!(!notice.is_requested());
        // Real writer failure, not Stop, produces the first retained native Err.
        wire.writes.fail_held_write();
        let mut finish =
            Box::pin(custody.finish_with_retirement(std::future::pending(), Some(&notice)));
        timeout(WAIT, async {
            tokio::select! {
                () = finish.as_mut() => panic!("qualified subsequent Close remains held"),
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
        drop(finish);
        assert!(wire.writes.state.lock().unwrap().held);
        let pointer = raw_error(custody.native_packet.as_ref().unwrap()) as *const EngineError;
        let description = raw_error(custody.native_packet.as_ref().unwrap()).to_string();
        let final_polls = native_polls.load(Ordering::SeqCst);
        for _ in 0..2 {
            pending_once(
                Box::pin(custody.finish_with_retirement(std::future::pending(), Some(&notice)))
                    .as_mut(),
            )
            .await;
            assert_eq!(
                raw_error(custody.native_packet.as_ref().unwrap()) as *const EngineError,
                pointer
            );
            assert_eq!(native_polls.load(Ordering::SeqCst), final_polls);
        }
        assert!(close_control.started());
        close_release.notify_one();
        timeout(
            WAIT,
            custody.finish_with_retirement(std::future::pending(), Some(&notice)),
        )
        .await
        .unwrap();
        let error = finish_pump(
            Ok(Ok(())),
            custody.take_cleanup_panic(),
            custody.take_native_error(),
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<EngineError>().unwrap().to_string(),
            description
        );
        assert!(
            custody
                .close_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .is_ok()
        );
        let _ = timeout(WAIT, wire.connection.shutdown()).await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn management_idle_detach_and_authorization_close_do_not_publish_cleanup_fault() {
    for unauthorized in [false, true] {
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual idle reply sender")
        };
        let notice = wire.retirement_request();
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let auth = unauthorized.then(authorization);
        if let Some(auth) = auth.as_ref() {
            expire(auth).await;
        }
        let mut original = Box::pin(serve_management_replies_with_retirement(
            sender,
            ADDRESS.to_owned(),
            route.clone(),
            responses,
            Arc::clone(&management),
            auth,
            Some(notice.clone()),
        ));
        if unauthorized {
            let (result, close) = timeout(WAIT, async {
                tokio::join!(original.as_mut(), wire.control(CHANNEL))
            })
            .await
            .unwrap();
            result.unwrap();
            let Performative::Detach(close) = close else {
                panic!("original Unauthorized Close")
            };
            assert!(
                matches!(close.error, Some(error) if error.condition == AmqpError::UnauthorizedAccess.into())
            );
        } else {
            pending_once(Box::pin(tokio::task::unconstrained(original.as_mut())).as_mut()).await;
            wire.detach().await;
            timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
        }
        drop(original);
        assert!(!notice.is_requested());
        assert!(route.is_closed());
        assert!(!management.routes.lock().await.senders.contains_key(ADDRESS));
        wire.barrier().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reply_cached_actual_success_and_report_or_unstarted_work_do_not_notify() {
    for settle in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let confirmation = settle == ReceiverSettleMode::Second;
        let (mut wire, endpoint) = Wire::new(Role::Receiver, 1, settle).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("actual reply sender")
        };
        let notice = wire.retirement_request();
        let management = ConnectionManagement::new();
        let (route, responses) = management.register_reply_route(ADDRESS.to_owned()).await;
        let mut custody =
            ReplyCustody::new(responses, ADDRESS.to_owned(), route.clone(), &management);
        let control = OperationControl::new();
        custody.original = Some(PendingOperation::new(
            send_management_response(&sender, response(42), control.clone()),
            control,
        ));
        let mut observation = Box::pin(custody.original.as_mut().unwrap().observe());
        let transfer = timeout(WAIT, async { tokio::select! {
            result = observation.as_mut() => { let _ = result; panic!("reply waits for actual disposition") },
            transfer = super::transfer(&mut wire) => transfer,
        }}).await.unwrap();
        super::accept(&mut wire, transfer.delivery_id.unwrap()).await;
        assert!(matches!(
            timeout(WAIT, observation.as_mut()).await.unwrap(),
            Some(Ok(Outcome::Accepted(_)))
        ));
        drop(observation);
        if confirmation {
            let Performative::Disposition(confirmed) = wire.control(CHANNEL).await else {
                panic!("actual original second confirmation")
            };
            assert_eq!(confirmed.first, transfer.delivery_id.unwrap());
            assert!(confirmed.settled);
        }
        wire.barrier().await;
        let invoked = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&invoked);
        let close_control = OperationControl::new();
        custody.close = Some(PendingOperation::new(
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            close_control,
        ));
        let (_replacement, mut replacement_responses) =
            management.register_reply_route(ADDRESS.to_owned()).await;
        timeout(
            WAIT,
            custody.finish_with_retirement(std::future::ready(()), Some(&notice)),
        )
        .await
        .unwrap();
        assert!(!notice.is_requested());
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert!(matches!(
            custody.packet.as_ref().unwrap().result.as_ref(),
            Some(Ok(Outcome::Accepted(_)))
        ));
        assert!(custody.close_packet.as_ref().unwrap().result.is_none());
        let diagnostic = report_fault(&custody);
        let caught = catch_unwind(AssertUnwindSafe(|| {
            finish_pump(Ok(Ok(())), Some(diagnostic), custody.take_native_error())
        }));
        assert!(caught.unwrap_err().downcast_ref::<Arc<str>>().is_some());
        assert!(!notice.is_requested());
        replacement_survives(&management, &mut replacement_responses).await;
        wire.barrier().await;
        wire.stop().await;
    }
}
