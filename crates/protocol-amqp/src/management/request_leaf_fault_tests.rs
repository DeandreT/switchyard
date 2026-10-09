//! A real command remains held after notice; Stop does not promise broker latency.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn management_request_fault_notice_precedes_original_session_commit_drain() {
    tokio::spawn(async {
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                let hold = actor.accept();
                let management = ConnectionManagement::new();
                install_hold(&management, &actor, hold.clone()).await;
                let (_route, mut responses) = management.register_reply_route("panic-reply".to_owned()).await;
                actor.gate.arm(domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id), true);
                let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
                let LinkEndpoint::Receiver(receiver) = endpoint else { panic!("request receiver") };
                let notice = wire.retirement_request();
                wire.request_message(&correlated(state_request())).await;
                wire.barrier().await;
                let fault = PumpFault::new(PumpPoint::RequestBroker);
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(serve_management_requests_with_retirement(receiver,
                        actor.namespace.clone(), actor.entity.clone(), actor.actual.as_ref().unwrap().clone(),
                        Arc::clone(&management), None, Some(notice.clone()))).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("original management command held") },
                    () = actor.gate.reached(false) => {},
                }}).await.unwrap();
                timeout(WAIT, fault.reached.notified()).await.unwrap();
                if after { actor.gate.release(false); actor.gate.reached(true).await; }
                assert!(!notice.is_requested());
                fault.trigger.notify_one();
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("notice precedes command drain") },
                    () = notice.observer() => {},
                }}).await.unwrap();
                pending_once(original.as_mut()).await;
                assert!(notice.is_requested());
                actor.gate.release_all();
                assert_outer_panic(timeout(WAIT, original.as_mut()).await.unwrap());
                drop(original);
                actor.assert_one(CommandKind::SetSessionState { session: hold.clone(), state: STATE.to_vec() },
                    Ok(CommandOutcome::SessionStateSet));
                assert!(matches!(responses.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
                let committed = actor.snapshot();
                actor.reopen(&committed);
                assert_eq!(actor.session().state, STATE);
                assert_eq!(actor.session().lock.unwrap().token, hold.token);
            }
        }
    }).await.expect("original observer joined");
}
