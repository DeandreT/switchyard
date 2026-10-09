//! Outer-pump faults keep the actual broker command and its post-result work.

#[path = "request_leaf_fault_tests.rs"]
mod leaf_fault_tests;

#[path = "request_cleanup_notice_tests.rs"]
mod cleanup_notice_tests;

use super::super::custody::{PUMP_FAULT, PumpFault};
use super::*;

fn assert_outer_panic(result: std::thread::Result<Result<(), ManagementError>>) {
    assert_eq!(
        result
            .expect_err("outer pump panic resumes")
            .downcast_ref::<&str>(),
        Some(&"controlled outer management pump panic")
    );
}

fn correlated(mut message: Message) -> Message {
    message.properties = Some(Properties {
        message_id: Some(MessageId::Ulong(87)),
        reply_to: Some("panic-reply".to_owned()),
        ..Default::default()
    });
    message
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_drains_actual_session_mutation_before_and_after_apply() {
    for durable in [false, true] {
        for after in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold.clone()).await;
            let (_route, mut responses) = management
                .register_reply_route("panic-reply".to_owned())
                .await;
            actor.gate.arm(
                domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
                true,
            );
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("request receiver")
            };
            wire.request_message(&correlated(state_request())).await;
            wire.barrier().await;
            let fault = PumpFault::new(PumpPoint::RequestBroker);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests(
                        receiver,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        actor.actual.as_ref().unwrap().clone(),
                        Arc::clone(&management),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            actor.gate.reached(false).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            if after {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
            }
            wire.barrier().await;
            wire.no_frame_yet().await;
            if !after {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
            actor.gate.release(true);
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            wire.barrier().await;
            wire.no_frame_yet().await;
            assert!(matches!(
                responses.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            actor.assert_one(
                CommandKind::SetSessionState {
                    session: hold.clone(),
                    state: STATE.to_vec(),
                },
                Ok(CommandOutcome::SessionStateSet),
            );
            let committed = actor.snapshot();
            drop(serving);
            wire.stop().await;
            actor.reopen(&committed);
            assert_eq!(actor.session().state, STATE);
            assert_eq!(actor.session().lock.unwrap().token, hold.token);
        }
    }
}

async fn raw_while_serving<F: Future + ?Sized>(
    mut serving: Pin<&mut F>,
    actor: &Actor,
) -> RawResult {
    timeout(
        WAIT,
        poll_fn(|context| {
            assert!(serving.as_mut().poll(context).is_pending());
            let submissions = actor.submissions.lock().unwrap();
            assert!(submissions.len() <= 1);
            submissions
                .first()
                .and_then(|submission| submission.result.clone())
                .map_or(Poll::Pending, Poll::Ready)
        }),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_drains_actual_registry_refresh_and_removals_without_replacing_owners()
{
    for durable in [false, true] {
        for mode in 0..3 {
            let mut actor = Actor::new(durable, false);
            let delivery = actor.receive();
            let lock = delivery.lock.unwrap();
            let management = ConnectionManagement::new();
            register_lock(&management, &actor, &delivery).await;
            actor.clock.set(2_000);
            if mode == 2 {
                actor.clock.set(lock.locked_until.as_millis());
                assert_eq!(
                    actor.intent(CommandKind::ExpireLocks),
                    CommandOutcome::LocksExpired {
                        returned_to_ready: 1,
                        dead_lettered: 0
                    }
                );
            }
            let message = correlated(if mode == 1 {
                request(
                    UPDATE_DISPOSITION_OPERATION,
                    true,
                    [
                        (LOCK_TOKENS, token_value(lock.token)),
                        ("disposition-status", Value::String("completed".to_owned())),
                    ],
                )
            } else {
                request(
                    RENEW_LOCK_OPERATION,
                    true,
                    [(LOCK_TOKENS, token_value(lock.token))],
                )
            });
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("request receiver")
            };
            wire.request_message(&message).await;
            wire.barrier().await;
            actor.result_gate.arm();
            let fault = PumpFault::new(PumpPoint::RequestBroker);
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests(
                        receiver,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        actor.actual.as_ref().unwrap().clone(),
                        Arc::clone(&management),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            let raw = raw_while_serving(serving.as_mut(), &actor).await;
            assert!(actor.result_gate.entered.load(Ordering::SeqCst));
            match mode {
                0 => assert!(matches!(raw, Ok(CommandOutcome::LockRenewed { .. }))),
                1 => assert_eq!(raw, Ok(CommandOutcome::Completed)),
                _ => assert!(matches!(
                    raw,
                    Err(BrokerRejection::Refused(
                        domain::BrokerError::MessageNotLocked { .. }
                    ))
                )),
            }
            let (ordinary, rr) = register_lock(&management, &actor, &delivery).await;
            let held = management.request_response_deliveries.write().await;
            let replacement_row = held.get(&rr.key).unwrap().clone();
            actor.result_gate.release();
            pending_once(serving.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(serving.as_mut()).await;
            }
            assert_eq!(held.get(&rr.key), Some(&replacement_row));
            assert_eq!(actor.submissions.lock().unwrap().len(), 1);
            drop(held);
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            assert_eq!(
                management.deliveries.read().await.get(&ordinary.key),
                Some(&ordinary)
            );
            assert_eq!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .get(&rr.key),
                Some(&replacement_row)
            );
            actor.assert_one(
                if mode == 1 {
                    CommandKind::Complete {
                        sequence: delivery.sequence,
                        lock_token: lock.token,
                    }
                } else {
                    CommandKind::RenewLock {
                        sequence: delivery.sequence,
                        lock_token: lock.token,
                        lock_duration_millis: None,
                    }
                },
                raw,
            );
            wire.barrier().await;
            wire.no_frame_yet().await;
            let committed = actor.snapshot();
            drop(serving);
            wire.stop().await;
            actor.reopen(&committed);
            match mode {
                0 => assert!(matches!(actor.message(delivery.sequence).unwrap().state,
                    MessageState::Locked { locked_until, .. } if locked_until > lock.locked_until)),
                1 => assert!(actor.message(delivery.sequence).is_none()),
                _ => assert_eq!(
                    actor.message(delivery.sequence).unwrap().state,
                    MessageState::Ready
                ),
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_retires_unstarted_and_registry_locked_preparation_without_broker_or_ack()
 {
    for durable in [false, true] {
        for prepared in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold).await;
            let held = management.sessions.write().await;
            let before = actor.snapshot();
            let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
            let LinkEndpoint::Receiver(receiver) = endpoint else {
                panic!("request receiver")
            };
            wire.request_message(&correlated(state_request())).await;
            wire.barrier().await;
            let fault = PumpFault::new(if prepared {
                PumpPoint::RequestPrepared
            } else {
                PumpPoint::RequestBroker
            });
            let mut serving = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests(
                        receiver,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        actor.actual.as_ref().unwrap().clone(),
                        Arc::clone(&management),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(serving.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            assert!(actor.submissions.lock().unwrap().is_empty());
            fault.trigger.notify_one();
            assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
            assert!(actor.submissions.lock().unwrap().is_empty());
            assert_eq!(actor.snapshot(), before);
            drop(held);
            wire.barrier().await;
            wire.no_frame_yet().await;
            drop(serving);
            wire.stop().await;
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn request_owner_retains_actual_raw_result_and_registry_work_across_cancelled_finish() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let delivery = actor.receive();
        let lock = delivery.lock.unwrap();
        let management = ConnectionManagement::new();
        register_lock(&management, &actor, &delivery).await;
        actor.clock.set(2_000);
        let broker = actor.request_broker();
        let message = request(
            RENEW_LOCK_OPERATION,
            true,
            [(LOCK_TOKENS, token_value(lock.token))],
        );
        let mut custody = RequestCustody::default();
        custody.request = Some(PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        ));
        let raw = paused_broker_result(custody.request.as_mut().unwrap(), &actor).await;
        let held = management.request_response_deliveries.write().await;
        actor.result_gate.release();
        for _ in 0..2 {
            pending_once(Box::pin(custody.finish(std::future::pending())).as_mut()).await;
            assert!(custody.request.is_some() && custody.request_packet.is_none());
            assert_eq!(
                actor.submissions.lock().unwrap()[0].result,
                Some(raw.clone())
            );
        }
        drop(held);
        timeout(WAIT, custody.finish(std::future::pending()))
            .await
            .unwrap();
        let packet = custody.request_packet.as_ref().unwrap();
        assert!(packet.started && packet.retired && !packet.panicked);
        let pointer = packet.result.as_ref().unwrap() as *const ManagementResponse;
        assert_eq!(packet.result.as_ref().unwrap().status_code, 200);
        timeout(WAIT, custody.finish(std::future::pending()))
            .await
            .unwrap();
        assert_eq!(
            custody
                .request_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap() as *const ManagementResponse,
            pointer
        );
        actor.assert_one(
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: lock.token,
                lock_duration_millis: None,
            },
            raw,
        );
        let committed = actor.snapshot();
        drop(custody);
        drop(broker);
        actor.reopen(&committed);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_request_panic_drains_actual_deferred_receive_registration_after_result() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let original = actor.receive();
        assert_eq!(
            actor.intent(CommandKind::Defer {
                sequence: original.sequence,
                lock_token: original.lock.unwrap().token,
                replacement_envelope: None
            }),
            CommandOutcome::Deferred
        );
        let management = ConnectionManagement::new();
        let held = management.request_response_deliveries.write().await;
        let message = correlated(request(
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            false,
            [
                (
                    "sequence-numbers",
                    Value::Array(Array::from(vec![Value::Long(
                        i64::try_from(original.sequence.as_u64()).unwrap(),
                    )])),
                ),
                ("receiver-settle-mode", Value::Uint(1)),
            ],
        ));
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("request receiver")
        };
        wire.request_message(&message).await;
        wire.barrier().await;
        let fault = PumpFault::new(PumpPoint::RequestBroker);
        let mut serving = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(serve_management_requests(
                    receiver,
                    actor.namespace.clone(),
                    actor.entity.clone(),
                    actor.actual.as_ref().unwrap().clone(),
                    Arc::clone(&management),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        let raw = raw_while_serving(serving.as_mut(), &actor).await;
        let Ok(CommandOutcome::DeferredReceived(deliveries)) = &raw else {
            panic!("actual deferred delivery")
        };
        let delivery = deliveries.first().unwrap().clone();
        assert_eq!(deliveries.len(), 1);
        assert_ne!(delivery.lock.unwrap().token, original.lock.unwrap().token);
        assert!(held.is_empty());
        timeout(WAIT, fault.reached.notified()).await.unwrap();
        fault.trigger.notify_one();
        for _ in 0..2 {
            pending_once(serving.as_mut()).await;
        }
        drop(held);
        assert_outer_panic(timeout(WAIT, serving.as_mut()).await.unwrap());
        assert_eq!(
            management
                .request_response_delivery(&actor.entity, delivery.lock.unwrap().token)
                .await
                .unwrap()
                .delivery,
            Some(delivery.clone())
        );
        actor.assert_one(
            CommandKind::ReceiveDeferred {
                sequences: vec![original.sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
            raw,
        );
        wire.barrier().await;
        wire.no_frame_yet().await;
        let committed = actor.snapshot();
        drop(serving);
        wire.stop().await;
        actor.reopen(&committed);
        assert!(matches!(actor.message(delivery.sequence).unwrap().state,
            MessageState::Locked { token, .. } if token == delivery.lock.unwrap().token));
    }
}
