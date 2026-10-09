//! Real sending leaves publish before broker drain; native Stop is not gate release.

use super::*;
use crate::listener::{
    ConnectionRetirementRequest, finish_send_with_retirement, serve_sending_client_with_retirement,
};

#[tokio::test(flavor = "current_thread")]
async fn send_fault_notice_precedes_original_commit_drain_on_both_backends() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, false);
                    actor.clock.set(2_000);
                    actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                    let mut wire = IngressWire::new().await;
                    let notice = ConnectionRetirementRequest::capture(&wire.connection);
                    let receiver = wire.receiver.take().unwrap();
                    let (message, format) = messages(batch);
                    wire.send(0, message, format).await;
                    wire.no_ack_barrier().await;
                    let fault = PumpFault::new(PumpPoint::Send);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client_with_retirement(receiver,
                            actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(),
                            None, Some(notice.clone()))).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("held original Send") },
                        () = actor.gate.reached(false) => {},
                    }}).await.unwrap();
                    timeout(WAIT, fault.reached.notified()).await.unwrap();
                    if after { actor.gate.release(false); actor.gate.reached(true).await; }
                    assert!(!notice.is_requested());
                    fault.trigger.notify_one();
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("notice precedes held commit drain") },
                        () = notice.observer() => {},
                    }}).await.unwrap();
                    assert!(notice.is_requested());
                    pending_once(original.as_mut()).await;
                    // No explicit Stop: the leaf's captured request interrupts native ownership.
                    timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                    {
                        let log = actor.log.lock().unwrap();
                        assert_eq!(log.len(), 1, "the original command is still held");
                        assert!(!log[0].returned);
                        assert_eq!(matches!(log[0].kind, CommandKind::SendBatch { .. }), batch);
                    }
                    actor.gate.release_all();
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    assert_one_submit(&actor, batch);
                    assert_stored(&actor, if batch { 2 } else { 1 });
                    assert_eq!(StateMachine::new(actor.store().clone()).last_applied_time().unwrap().as_millis(), 2_000);
                    let committed = actor.store().snapshot().unwrap();
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), committed);
                }
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn send_and_authorization_close_fault_notices_interrupt_begun_native_writes() {
    tokio::spawn(async {
        for durable in [false, true] {
            // Accept, broker Reject, conversion Reject, terminal auth Close.
            for case in 0..4 {
                let mut actor = Actor::new(durable, case == 1);
                let (mut wire, writes) = gated_wire().await;
                let _release = NativeRelease(Arc::clone(&writes));
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let receiver = wire.receiver.take().unwrap();
                let authorization = if case == 3 {
                    let authorization = authorization();
                    expire(&authorization).await;
                    Some(authorization)
                } else { None };
                if case != 3 {
                    let (message, format) = messages(false);
                    wire.send(0, message, if case == 2 { 7 } else { format }).await;
                    wire.no_ack_barrier().await;
                }
                writes.hold();
                let fault = PumpFault::new(if case == 3 { PumpPoint::Close } else { PumpPoint::Native });
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(serve_sending_client_with_retirement(receiver,
                        actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(),
                        authorization, Some(notice.clone()))).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("original native write is held") },
                    () = writes.reached() => {},
                }}).await.unwrap();
                timeout(WAIT, fault.reached.notified()).await.unwrap();
                assert!(!notice.is_requested());
                fault.trigger.notify_one();
                let ((), result) = timeout(WAIT, async { tokio::join!(notice.observer(), original.as_mut()) }).await.unwrap();
                assert_outer(result, &fault);
                drop(original);
                assert!(notice.is_requested());
                assert!(writes.held.load(Ordering::SeqCst));
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                if case < 2 { assert_one_submit(&actor, false); }
                else { assert!(actor.log.lock().unwrap().is_empty()); }
                assert_stored(&actor, u64::from(case == 0));
                let snapshot = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_sends_refusals_conversion_rejects_and_detach_do_not_publish_fault() {
    tokio::spawn(async {
        for durable in [false, true] {
            for case in 0..4 {
                let mut actor = Actor::new(durable, case == 1);
                let mut wire = IngressWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let receiver = wire.receiver.take().unwrap();
                if case == 3 {
                    let authorization = authorization();
                    expire(&authorization).await;
                    timeout(WAIT, serve_sending_client_with_retirement(receiver,
                        actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(),
                        Some(authorization), Some(notice.clone()))).await.unwrap().unwrap();
                    let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else { panic!("authorization Detach") };
                    assert_eq!(detach.error.unwrap().condition, AmqpError::UnauthorizedAccess.into());
                    assert!(!notice.is_requested());
                    wire.no_ack_barrier().await;
                    wire.stop().await;
                    actor.reopen();
                    continue;
                }
                let (message, format) = messages(false);
                wire.send(0, message, if case == 2 { 7 } else { format }).await;
                let mut original = Box::pin(serve_sending_client_with_retirement(receiver,
                    actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(),
                    None, Some(notice.clone())));
                let disposition = timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("healthy link remains open") },
                    disposition = wire.disposition(0) => disposition,
                }}).await.unwrap();
                assert_eq!(matches!(disposition.state, Some(DeliveryState::Accepted(_))), case == 0);
                assert!(!notice.is_requested());
                wire.no_ack_barrier().await;
                wire.detach().await;
                timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
                drop(original);
                assert!(!notice.is_requested());
                wire.stop().await;
                actor.reopen();
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn active_send_native_error_requests_notice_without_reclassifying_raw_error() {
    // Qualified production delivery helper: pending retirement makes Stopped an active error.
    tokio::spawn(async {
        let mut actor = Actor::new(false, false);
        let (mut wire, writes) = gated_wire().await;
        let _release = NativeRelease(Arc::clone(&writes));
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let mut receiver = wire.receiver.take().unwrap();
        let (message, format) = messages(false);
        wire.send(0, message, format).await;
        let delivery = receiver.recv().await.unwrap();
        writes.hold();
        let mut custody = SendCustody::default();
        let retirement = std::future::pending::<SendRetirement>();
        tokio::pin!(retirement);
        let mut original = Box::pin(
            AssertUnwindSafe(send_delivery_pump(
                &receiver,
                &delivery,
                &actor.namespace,
                &actor.entity,
                actor.broker.as_ref().unwrap(),
                &mut custody,
                retirement.as_mut(),
            ))
            .catch_unwind(),
        );
        timeout(WAIT, async { tokio::select! {
            result = original.as_mut() => { let _ = result; panic!("original ACK writer held") },
            () = writes.reached() => {},
        }}).await.unwrap();
        wire.stop().await;
        let primary = timeout(WAIT, original.as_mut()).await.unwrap();
        drop(original);
        assert!(matches!(&primary, Ok(Err(amqp::EngineError::Stopped))));
        assert!(!notice.is_requested());
        let error = finish_send_with_retirement(&mut custody, primary, Some(&notice))
            .await
            .err()
            .unwrap();
        assert!(matches!(error, amqp::EngineError::Stopped));
        assert!(notice.is_requested());
        drop(custody);
        assert_one_submit(&actor, false);
        actor.reopen();
    })
    .await
    .expect("original observer joined");
}
