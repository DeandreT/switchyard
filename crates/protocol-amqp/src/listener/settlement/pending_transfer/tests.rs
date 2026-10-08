//! Exact native-start custody on real links; broker mutations use the shared actor.

use std::{pin::Pin, sync::Arc};

use amqp::{
    EngineError, Flow, Frame, PendingDelivery, Performative, ReceiverSettleMode, read_frame,
    write_frame,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, MessageState, ReceiveMode, SessionId, StateMachine,
};
use storage::StateStore;
use tokio::time::timeout;

use super::*;
use crate::listener::{
    ReceivingLinkProtocol,
    settlement::{
        lock_delivery_tag, serve_receiving_client, settle_started_delivery, test_support::*,
        workers::SettlementWorkers,
    },
};
use crate::management::{ConnectionManagement, DeliveryRegistration};

async fn reserve(wire: &Wire) -> amqp::CreditReservation {
    timeout(WAIT, wire.sender.as_ref().unwrap().on_credit())
        .await
        .unwrap()
        .unwrap()
}

async fn transfer(wire: &mut Wire, delivery: &Delivery) -> u32 {
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        payload,
        ..
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("original native Transfer");
    };
    assert!(!payload.is_empty());
    assert_eq!(
        transfer.delivery_tag.as_ref(),
        Some(&lock_delivery_tag(delivery.lock.unwrap().token))
    );
    transfer.delivery_id.unwrap()
}

async fn request_zero_credit(wire: &mut Wire, count: u32) {
    write_frame(
        &mut wire.peer,
        &frame(Performative::Flow(Flow {
            handle: Some(HANDLE),
            delivery_count: Some(count),
            link_credit: Some(0),
            drain: true,
            incoming_window: 2048,
            outgoing_window: 2048,
            ..Flow::default()
        })),
    )
    .await
    .unwrap();
}

async fn drain_response(wire: &mut Wire, count: u32) {
    let Performative::Flow(flow) = performative(&mut wire.peer).await else {
        panic!("positive native drain response, without a late confirmation");
    };
    assert_eq!(flow.handle, Some(HANDLE));
    assert_eq!(flow.delivery_count, Some(count));
    assert_eq!(flow.link_credit, Some(0));
    assert!(flow.drain);
}

async fn registered(
    actor: &Actor,
    delivery: &Delivery,
) -> (Arc<ConnectionManagement>, DeliveryRegistration) {
    let management = ConnectionManagement::new();
    let registration = management
        .register_delivery(
            LINK,
            actor.entity.clone(),
            delivery.sequence,
            delivery.lock.unwrap().token,
        )
        .await;
    (management, registration)
}

async fn assert_cached(owner: &mut PendingTransfer<'_>, id: amqp::DeliveryIdentity) {
    let first = owner.finish().await.unwrap().as_ref().unwrap() as *const PendingDelivery;
    let mut observer = Box::pin(async {
        assert_eq!(
            owner.observe().await.unwrap().as_ref().unwrap().identity(),
            id
        );
        std::future::pending::<()>().await;
    });
    pending_once(observer.as_mut()).await;
    drop(observer);
    let second = owner.finish().await.unwrap().as_ref().unwrap() as *const PendingDelivery;
    assert_eq!(
        first, second,
        "same cached native object, not a replacement"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn original_native_start_survives_lowered_credit_write_gate_and_cancelled_observers() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("owned-native", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let before = actor.store().snapshot().unwrap();
            let (management, registration) = registered(&actor, &delivery).await;
            let mut wire = Wire::new_with_credit(mode, 1).await;
            let reservation = reserve(&wire).await;
            request_zero_credit(&mut wire, 0).await;
            let request = timeout(WAIT, wire.sender.as_ref().unwrap().on_drain())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                wire.sender.as_ref().unwrap().drained(request).await,
                Err(EngineError::InvalidState(_))
            ));
            // The actual processed lowered Flow did not revoke the held reservation.
            let sender = wire.sender.take().unwrap();
            let message = crate::message::write_delivery_from(&delivery, None).unwrap();
            wire.writes.block();
            let _release = wire.writes.release_on_drop();
            let mut owner = PendingTransfer::new(
                delivery.clone(),
                sender.send_pending_with_credit(reservation, message, lock_delivery_tag(token)),
            );
            let mut observer = Box::pin(owner.observe());
            pending_once(observer.as_mut()).await;
            wire.writes.reached().await;
            drop(observer);
            assert!(owner.started());
            assert!(management.delivery(LINK, token).await.is_some());
            owner.retire();
            let mut observer = Box::pin(owner.finish());
            pending_once(observer.as_mut()).await;
            drop(observer);
            assert!(owner.take_packet().is_none());
            wire.writes.release();
            let wire_id = transfer(&mut wire, &delivery).await;
            let id = timeout(WAIT, owner.finish())
                .await
                .unwrap()
                .unwrap()
                .as_ref()
                .unwrap()
                .identity();
            assert_eq!(id.delivery_id(), wire_id);
            assert_cached(&mut owner, id).await;
            let packet = owner.take_packet().unwrap();
            assert!(packet.started && packet.retired);
            assert_eq!(packet.delivery.sequence, delivery.sequence);
            assert_eq!(packet.delivery.lock.unwrap().token, token);
            assert!(owner.take_packet().is_none());
            let mut workers = SettlementWorkers::new();
            workers.retire();
            let retirement = workers.subscribe();
            let worker_id = workers.adopt_retired(
                Some(registration.clone()),
                settle_started_delivery(
                    packet.result.unwrap().unwrap(),
                    packet.delivery,
                    Some(registration.clone()),
                    actor.context(Arc::clone(&management)),
                    retirement,
                ),
            );
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_eq!(workers.finished()[0].id, worker_id);
            assert_eq!(workers.finished()[0].lock_token, Some(token));
            assert_eq!(
                workers.finished()[0].registration.as_ref(),
                Some(&registration)
            );
            assert!(
                workers.finished()[0]
                    .result
                    .as_ref()
                    .unwrap()
                    .result
                    .is_ok()
            );
            assert!(management.delivery(LINK, token).await.is_none());
            assert_eq!(actor.complete_count(), 0);
            assert_eq!(actor.store().snapshot().unwrap(), before);
            request_zero_credit(&mut wire, 1).await;
            drain_response(&mut wire, 1).await;
            drop(workers);
            drop(owner);
            drop(sender);
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn late_original_pending_is_adopted_before_finish_and_drains_ready_accepted() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, false);
            actor.send("late-accepted", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let key = actor.key(delivery.sequence);
            let before = actor.store().snapshot().unwrap();
            actor.gate.arm(key.clone());
            let (management, registration) = registered(&actor, &delivery).await;
            let mut wire = Wire::new_with_credit(mode, 1).await;
            let reservation = reserve(&wire).await;
            let sender = wire.sender.take().unwrap();
            let message = crate::message::write_delivery_from(&delivery, None).unwrap();
            wire.writes.block();
            let _release = wire.writes.release_on_drop();
            let mut owner = PendingTransfer::new(
                delivery.clone(),
                sender.send_pending_with_credit(reservation, message, lock_delivery_tag(token)),
            );
            let mut observer = Box::pin(owner.observe());
            pending_once(observer.as_mut()).await;
            wire.writes.reached().await;
            drop(observer);
            let mut workers = SettlementWorkers::new();
            owner.retire();
            workers.retire();
            wire.writes.release();
            let wire_id = transfer(&mut wire, &delivery).await;
            timeout(WAIT, owner.finish())
                .await
                .unwrap()
                .unwrap()
                .as_ref()
                .unwrap();
            wire.accepted(wire_id).await;
            request_zero_credit(&mut wire, 1).await;
            drain_response(&mut wire, 1).await;
            // The peer disposition preceded this processed Flow, not just a command FIFO.
            let packet = owner.take_packet().unwrap();
            let retirement = workers.subscribe();
            let worker_id = workers.adopt_retired(
                Some(registration.clone()),
                settle_started_delivery(
                    packet.result.unwrap().unwrap(),
                    packet.delivery,
                    Some(registration.clone()),
                    actor.context(Arc::clone(&management)),
                    retirement,
                ),
            );
            actor.gate.reached(false).await;
            if durable {
                actor.gate.release(false);
                actor.gate.reached(true).await;
                assert!(actor.store().get(&key).unwrap().is_none());
            } else {
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
            let mut observer = Box::pin(workers.finish());
            pending_once(observer.as_mut()).await;
            drop(observer);
            assert!(workers.finished().is_empty());
            assert_eq!(workers.len(), 1);
            assert!(management.delivery(LINK, token).await.is_none());
            actor.gate.release_all();
            timeout(WAIT, workers.finish()).await.unwrap();
            let joined = &workers.finished()[0];
            assert_eq!(joined.id, worker_id);
            assert_eq!(joined.lock_token, Some(token));
            assert!(joined.result.as_ref().unwrap().result.is_ok());
            assert_eq!(actor.complete_count(), 1);
            assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
            {
                let log = actor.log.lock().unwrap();
                assert_eq!(log.len(), 2);
                assert!(log.iter().all(|entry| entry.worker == worker_id));
                assert!(!log[0].returned && log[1].returned);
            }
            request_zero_credit(&mut wire, 1).await;
            drain_response(&mut wire, 1).await;
            let committed = actor.store().snapshot().unwrap();
            assert!(actor.store().get(&key).unwrap().is_none());
            timeout(WAIT, workers.finish()).await.unwrap();
            assert_eq!(workers.finished().len(), 1);
            drop(workers);
            drop(owner);
            drop(sender);
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retired_unpolled_native_start_refunds_only_original_credit_and_keeps_raw_errors() {
    let actor = Actor::new(false, false);
    actor.send("unpolled", None);
    let delivery = actor.receive();
    let token = delivery.lock.unwrap().token;
    let before = actor.store().snapshot().unwrap();
    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
    let reservation = reserve(&wire).await;
    let old = wire.sender.take().unwrap();
    let message = crate::message::write_delivery_from(&delivery, None).unwrap();
    let mut owner = PendingTransfer::new(
        delivery.clone(),
        old.send_pending_with_credit(reservation, message, lock_delivery_tag(token)),
    );
    assert!(!owner.started());
    owner.retire();
    assert!(owner.finish().await.is_none());
    let packet = owner.take_packet().unwrap();
    assert!(!packet.started && packet.retired && packet.result.is_none());
    request_zero_credit(&mut wire, 0).await;
    drain_response(&mut wire, 0).await;
    drop(packet);
    drop(owner);
    drop(old);
    wire.detach().await;
    wire.sender = Some(wire.reattach(1).await);
    let reservation = reserve(&wire).await;
    let old = wire.sender.take().unwrap();
    wire.detach().await;
    let message = crate::message::write_delivery_from(&delivery, None).unwrap();
    let mut owner = PendingTransfer::new(
        delivery.clone(),
        old.send_pending_with_credit(reservation, message, lock_delivery_tag(token)),
    );
    assert!(matches!(
        owner.observe().await,
        Some(Err(EngineError::RemoteDetached))
    ));
    let first = match owner.finish().await.unwrap() {
        Err(error) => error as *const EngineError,
        Ok(_) => panic!("original detached start error"),
    };
    let second = match owner.observe().await.unwrap() {
        Err(error) => error as *const EngineError,
        Ok(_) => panic!("same cached error"),
    };
    assert_eq!(first, second);
    let packet = owner.take_packet().unwrap();
    assert!(packet.started && packet.retired);
    assert!(matches!(
        packet.result,
        Some(Err(EngineError::RemoteDetached))
    ));
    assert_eq!(packet.delivery.lock.unwrap().token, token);
    assert!(owner.take_packet().is_none());
    assert_eq!(actor.store().snapshot().unwrap(), before);
    drop(owner);
    drop(old);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_old_unpolled_start_cannot_reclaim_replacement_reservation() {
    let actor = Actor::new(false, false);
    actor.send("replacement", None);
    let delivery = actor.receive();
    let token = delivery.lock.unwrap().token;
    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
    let reservation = reserve(&wire).await;
    let old = wire.sender.take().unwrap();
    let message = crate::message::write_delivery_from(&delivery, None).unwrap();
    let mut owner = PendingTransfer::new(
        delivery,
        old.send_pending_with_credit(reservation, message, lock_delivery_tag(token)),
    );
    wire.detach().await;
    wire.sender = Some(wire.reattach(1).await);
    let replacement = reserve(&wire).await;
    request_zero_credit(&mut wire, 0).await;
    let request = wire.sender.as_ref().unwrap().on_drain().await.unwrap();
    assert!(matches!(
        wire.sender.as_ref().unwrap().drained(request).await,
        Err(EngineError::InvalidState(_))
    ));
    owner.retire();
    assert!(owner.finish().await.is_none());
    drop(owner.take_packet());
    assert!(matches!(
        wire.sender.as_ref().unwrap().drained(request).await,
        Err(EngineError::InvalidState(_))
    ));
    replacement.release().await.unwrap();
    drain_response(&mut wire, 0).await;
    drop(owner);
    drop(old);
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_blocked_transfer_requires_native_stop_join_before_session_cleanup() {
    for durable in [false, true] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            let mut actor = Actor::new(durable, true);
            let session_id = SessionId::new("blocked-start").unwrap();
            let sequence = actor.send("blocked-start", Some(session_id.clone()));
            let CommandOutcome::SessionAccepted(Some(accepted)) =
                actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id.clone()),
                    lock_duration_millis: None,
                })
            else {
                panic!("actual session grant");
            };
            let hold = accepted.hold();
            let management = ConnectionManagement::new();
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
            wire.writes.block();
            let _release = wire.writes.release_on_drop();
            let sender = wire.sender.take().unwrap();
            let mut pump = tokio::spawn(serve_receiving_client(
                sender,
                actor.namespace.clone(),
                actor.entity.clone(),
                actor.broker.as_ref().unwrap().clone(),
                mode,
                Some(hold.clone()),
                ReceivingLinkProtocol {
                    authorization: None,
                    management: Arc::clone(&management),
                },
            ));
            wire.writes.reached().await;
            let record = StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap();
            let token = match mode {
                ReceiveMode::PeekLock => {
                    let MessageState::Locked { token, .. } = record.unwrap().state else {
                        panic!("actual committed lease before Transfer");
                    };
                    assert!(management.delivery(LINK, token).await.is_some());
                    Some(token)
                }
                ReceiveMode::ReceiveAndDelete => {
                    assert!(record.is_none());
                    None
                }
            };
            assert_eq!(
                StateMachine::new(actor.store().clone())
                    .session(&actor.namespace, &actor.entity, &session_id)
                    .unwrap()
                    .unwrap()
                    .lock,
                Some(accepted.lock)
            );
            pending_once(Pin::new(&mut pump)).await;
            {
                let log = actor.log.lock().unwrap();
                assert_eq!(log.len(), 2);
                assert!(log.iter().all(|entry| matches!(&entry.kind, CommandKind::Receive { session: Some(session), .. } if *session == hold)));
                assert!(!log[0].returned && log[1].returned);
            }
            // This is explicit stop/join unblocking, not a prompt End behind blocked IO.
            wire.stop().await;
            timeout(WAIT, &mut pump).await.unwrap().unwrap().unwrap();
            if let Some(token) = token {
                assert!(management.delivery(LINK, token).await.is_none());
            }
            {
                let log = actor.log.lock().unwrap();
                assert_eq!(log.len(), 4);
                assert!(
                    matches!(&log[2].kind, CommandKind::ReleaseSession { session } if *session == hold)
                );
                assert!(!log[2].returned && log[3].returned);
            }
            assert_eq!(actor.receive_count(), 1);
            assert_eq!(actor.complete_count(), 0);
            assert!(
                StateMachine::new(actor.store().clone())
                    .session(&actor.namespace, &actor.entity, &session_id)
                    .unwrap()
                    .unwrap()
                    .lock
                    .is_none()
            );
            let committed = actor.store().snapshot().unwrap();
            actor.restart();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
            let record = StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap();
            match mode {
                ReceiveMode::PeekLock => {
                    let MessageState::Locked { locked_until, .. } = record.unwrap().state else {
                        panic!("late native failure retains lease");
                    };
                    actor.clock.set(locked_until.as_millis());
                    assert_eq!(
                        actor.intent(CommandKind::ExpireLocks),
                        CommandOutcome::LocksExpired {
                            returned_to_ready: 1,
                            dead_lettered: 0,
                        }
                    );
                    assert!(matches!(
                        StateMachine::new(actor.store().clone())
                            .message(&actor.namespace, &actor.entity, sequence)
                            .unwrap()
                            .unwrap()
                            .state,
                        MessageState::Ready
                    ));
                }
                ReceiveMode::ReceiveAndDelete => assert!(record.is_none()),
            }
            let final_state = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), final_state);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_pump_adopts_cached_pending_with_ready_accepted_before_session_cleanup() {
    // Only this observer is spawned for the adapter's task-local submission log.
    // The original pump future itself remains pinned across borrowed observers.
    tokio::spawn(async {
        for durable in [false, true] {
            for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
                let mut actor = Actor::new(durable, true);
                let session_id = SessionId::new("late-pump").unwrap();
                let sequence = actor.send("late-pump", Some(session_id.clone()));
                let CommandOutcome::SessionAccepted(Some(accepted)) = actor.intent(CommandKind::AcceptSession { session_id: Some(session_id.clone()), lock_duration_millis: None }) else { panic!("actual accepted session"); };
                let hold = accepted.hold();
                actor.gate.arm(actor.key(sequence));
                let management = ConnectionManagement::new();
                let mut wire = Wire::new_with_credit(mode, 1).await;
                wire.writes.block();
                let _release = wire.writes.release_on_drop();
                let sender = wire.sender.take().unwrap();
                let mut pump = Box::pin(serve_receiving_client(sender, actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(), ReceiveMode::PeekLock, Some(hold.clone()), ReceivingLinkProtocol { authorization: None, management: Arc::clone(&management) }));
                timeout(WAIT, async {
                    tokio::select! {
                        biased;
                        () = wire.writes.reached() => {},
                        result = &mut pump => panic!("original pump ended before blocked start: {result:?}"),
                    }
                }).await.unwrap();
                let MessageState::Locked { token, .. } = StateMachine::new(actor.store().clone()).message(&actor.namespace, &actor.entity, sequence).unwrap().unwrap().state else { panic!("original committed lease"); };
                assert!(management.delivery(LINK, token).await.is_some());
                // Do not poll the same retained pump while the native engine caches
                // its success and the remote Accepted in their original oneshots.
                wire.writes.release();
                let Frame::Amqp { performative: Some(Performative::Transfer(transfer)), payload, .. } = timeout(WAIT, read_frame(&mut wire.peer)).await.unwrap().unwrap() else { panic!("actual original Transfer"); };
                assert!(!payload.is_empty());
                assert_eq!(transfer.delivery_tag.as_ref(), Some(&lock_delivery_tag(token)));
                let wire_id = transfer.delivery_id.unwrap();
                wire.accepted(wire_id).await;
                request_zero_credit(&mut wire, 1).await;
                drain_response(&mut wire, 1).await;
                // This positive response proves the peer Accepted was processed
                // before Detach; removing the link cannot overwrite that reply.
                wire.detach().await;
                assert_eq!(actor.complete_count(), 0);
                timeout(WAIT, async {
                    tokio::select! {
                        biased;
                        () = actor.gate.reached(false) => {},
                        result = &mut pump => panic!("late original settlement did not reach actual apply: {result:?}"),
                    }
                }).await.unwrap();
                assert_eq!(actor.complete_count(), 1);
                assert!(management.delivery(LINK, token).await.is_none());
                assert_eq!(StateMachine::new(actor.store().clone()).session(&actor.namespace, &actor.entity, &session_id).unwrap().unwrap().lock, Some(accepted.lock));
                if durable {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                    assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                }
                let mut observer = Box::pin(async { (&mut pump).await });
                pending_once(observer.as_mut()).await;
                drop(observer);
                wire.sender = Some(wire.reattach(1).await);
                let mut replacement_watch = Box::pin(wire.sender.as_ref().unwrap().on_detach_owned());
                pending_once(replacement_watch.as_mut()).await;
                actor.gate.release_all();
                timeout(WAIT, &mut pump).await.unwrap().unwrap();
                drop(pump);
                {
                    let log = actor.log.lock().unwrap();
                    assert_eq!(log.len(), 6);
                    assert!(matches!(&log[0].kind, CommandKind::Receive { session: Some(session), .. } if *session == hold));
                    assert!(!log[0].returned && log[1].returned);
                    assert!(matches!(&log[2].kind, CommandKind::Complete { sequence: settled, lock_token } if *settled == sequence && *lock_token == token));
                    assert!(!log[2].returned && log[3].returned);
                    assert!(matches!(&log[4].kind, CommandKind::ReleaseSession { session } if *session == hold));
                    assert!(!log[4].returned && log[5].returned);
                    assert_ne!(log[0].worker, log[2].worker, "late settlement is owned by its original adopted worker");
                    assert_eq!(log[2].worker, log[3].worker);
                }
                let replacement_credit = reserve(&wire).await;
                pending_once(replacement_watch.as_mut()).await;
                replacement_credit.release().await.unwrap();
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(actor.complete_count(), 1);
                assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                assert!(StateMachine::new(actor.store().clone()).session(&actor.namespace, &actor.entity, &session_id).unwrap().unwrap().lock.is_none());
                let committed = actor.store().snapshot().unwrap();
                drop(replacement_watch);
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
        }
    }).await.expect("original fixture observer joined");
}

use std::time::{SystemTime, UNIX_EPOCH};

use auth::{
    Permission, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::SharedAccessAuthentication;
use crate::authorization::ConnectionAuthorization;
use crate::listener::LinkAuthorization;

fn native_start_authorization() -> LinkAuthorization {
    let rule = SharedAccessRule::new(
        "listen",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::LISTEN,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = policy.authenticate_plain("listen", "secret").unwrap();
    LinkAuthorization {
        connection: ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net").unwrap(),
            Some(grant),
        ),
        resource: ResourceScope::entity("tenant.servicebus.windows.net", "orders").unwrap(),
        permission: Permission::Listen,
    }
}

async fn expire_native_start_authorization(authorization: &LinkAuthorization) {
    // Refresh only after the real original native write is positively held.
    let encoded = "amqps%3A%2F%2Ftenant.servicebus.windows.net";
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 2;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    let token =
        format!("SharedAccessSignature sr={encoded}&sig={signature}&se={expiry}&skn=listen");
    authorization
        .connection
        .validate_and_add(&token, "amqps://tenant.servicebus.windows.net")
        .await
        .unwrap();
    assert!(authorization.ensure().await.is_ok());
    timeout(WAIT, authorization.wait_until_unauthorized())
        .await
        .expect("actual SAS expiry");
    assert!(authorization.ensure().await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_auth_retirement_retains_blocked_native_start_until_release_or_joined_stop() {
    for durable in [false, true] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            for joined_stop in [false, true] {
                let mut actor = Actor::new(durable, true);
                let session_id = SessionId::new("auth-start").unwrap();
                let sequence = actor.send("auth-start", Some(session_id.clone()));
                let CommandOutcome::SessionAccepted(Some(accepted)) =
                    actor.intent(CommandKind::AcceptSession {
                        session_id: Some(session_id.clone()),
                        lock_duration_millis: None,
                    })
                else {
                    panic!("actual session grant");
                };
                let hold = accepted.hold();
                let authorization = native_start_authorization();
                assert!(authorization.ensure().await.is_ok());
                let management = ConnectionManagement::new();
                let retired = Arc::new(tokio::sync::Notify::new());
                let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                wire.writes.block();
                let _release = wire.writes.release_on_drop();
                let sender = wire.sender.take().unwrap();
                let mut pump = tokio::spawn(TRANSFER_RETIREMENT.scope(
                    Arc::clone(&retired),
                    serve_receiving_client(
                        sender,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        actor.broker.as_ref().unwrap().clone(),
                        mode,
                        Some(hold.clone()),
                        ReceivingLinkProtocol {
                            authorization: Some(authorization.clone()),
                            management: Arc::clone(&management),
                        },
                    ),
                ));
                wire.writes.reached().await;
                let token = match mode {
                    ReceiveMode::PeekLock => {
                        let MessageState::Locked { token, .. } =
                            StateMachine::new(actor.store().clone())
                                .message(&actor.namespace, &actor.entity, sequence)
                                .unwrap()
                                .unwrap()
                                .state
                        else {
                            panic!("actual original lease");
                        };
                        assert!(management.delivery(LINK, token).await.is_some());
                        Some(token)
                    }
                    ReceiveMode::ReceiveAndDelete => {
                        assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                        None
                    }
                };
                let blocked = actor.store().snapshot().unwrap();
                expire_native_start_authorization(&authorization).await;
                // This is the real pump's first sticky transfer retirement, not
                // an independent expiry waiter or a Pending JoinHandle poll.
                timeout(WAIT, retired.notified())
                    .await
                    .expect("actual pump retired its original blocked native start");
                assert!(authorization.ensure().await.is_err());
                let mut observer = Box::pin(async { (&mut pump).await });
                pending_once(observer.as_mut()).await;
                drop(observer);
                assert_eq!(actor.store().snapshot().unwrap(), blocked);
                assert_eq!(
                    StateMachine::new(actor.store().clone())
                        .session(&actor.namespace, &actor.entity, &session_id)
                        .unwrap()
                        .unwrap()
                        .lock,
                    Some(accepted.lock)
                );
                if let Some(token) = token {
                    assert!(management.delivery(LINK, token).await.is_some());
                }
                {
                    let log = actor.log.lock().unwrap();
                    assert_eq!(
                        log.len(),
                        2,
                        "no early session release while native result remains held"
                    );
                    assert!(log.iter().all(|entry| matches!(&entry.kind, CommandKind::Receive { session: Some(session), .. } if *session == hold)));
                    assert!(!log[0].returned && log[1].returned);
                }
                if joined_stop {
                    // Only explicit stop/join unblocks this held native IO.
                    wire.stop().await;
                } else {
                    wire.writes.release();
                    let Frame::Amqp {
                        performative: Some(Performative::Transfer(transfer)),
                        payload,
                        ..
                    } = timeout(WAIT, read_frame(&mut wire.peer))
                        .await
                        .unwrap()
                        .unwrap()
                    else {
                        panic!("already-started original Transfer");
                    };
                    assert!(!payload.is_empty());
                    let expected = match token {
                        Some(token) => lock_delivery_tag(token),
                        None => crate::listener::settlement::sequence_delivery_tag(sequence),
                    };
                    assert_eq!(transfer.delivery_tag.as_ref(), Some(&expected));
                    let Performative::Detach(detach) = performative(&mut wire.peer).await else {
                        panic!("auth close after original native result drain");
                    };
                    assert_eq!(
                        detach.error.unwrap().condition,
                        amqp::AmqpError::UnauthorizedAccess.into()
                    );
                }
                timeout(WAIT, &mut pump).await.unwrap().unwrap().unwrap();
                if let Some(token) = token {
                    assert!(management.delivery(LINK, token).await.is_none());
                }
                {
                    let log = actor.log.lock().unwrap();
                    assert_eq!(log.len(), 4);
                    assert!(
                        matches!(&log[2].kind, CommandKind::ReleaseSession { session } if *session == hold)
                    );
                    assert!(!log[2].returned && log[3].returned);
                    assert_eq!(
                        log[0].worker, log[2].worker,
                        "same original pump observes before exact release"
                    );
                }
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(actor.complete_count(), 0);
                assert!(
                    StateMachine::new(actor.store().clone())
                        .session(&actor.namespace, &actor.entity, &session_id)
                        .unwrap()
                        .unwrap()
                        .lock
                        .is_none()
                );
                let committed = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
                let record = StateMachine::new(actor.store().clone())
                    .message(&actor.namespace, &actor.entity, sequence)
                    .unwrap();
                match mode {
                    ReceiveMode::PeekLock => assert!(
                        matches!(record.unwrap().state, MessageState::Locked { token: persisted, .. } if Some(persisted) == token)
                    ),
                    ReceiveMode::ReceiveAndDelete => assert!(record.is_none()),
                }
            }
        }
    }
}
