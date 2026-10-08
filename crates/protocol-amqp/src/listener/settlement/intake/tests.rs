//! One original Receive future and credit reservation across natural retirement.
//! Each case has one real test-observer task for shared submission logging;
//! ReceiveIntake itself owns a pinned future, not another spawned task.

use std::{
    future::Future,
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};

use amqp::{
    AmqpError, CreditReservation, EngineError, Flow, Frame, Performative, ReceiverSettleMode,
    read_frame, write_frame,
};
use auth::{
    Permission, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, CommandOutcome, Delivery, MessageState, ReceiveMode, SequenceNumber, SessionId,
    StateMachine,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use storage::StateStore;
use tokio::time::timeout;

use super::*;
use crate::authorization::ConnectionAuthorization;
use crate::listener::{
    LinkAuthorization, ReceivingLinkProtocol,
    settlement::{serve_receiving_client, test_support::*},
};
use crate::management::ConnectionManagement;
use crate::{Broker, BrokerRejection, SharedAccessAuthentication};

fn submission(
    actor: &Actor,
    mode: ReceiveMode,
) -> impl Future<Output = RawReceiveResult> + Send + 'static {
    let broker = actor.broker.as_ref().unwrap().clone();
    let namespace = actor.namespace.clone();
    let entity = actor.entity.clone();
    async move {
        broker
            .submit(
                namespace,
                entity,
                CommandKind::Receive {
                    mode,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .await
    }
}

async fn reserve(wire: &Wire) -> CreditReservation {
    timeout(WAIT, wire.sender.as_ref().unwrap().on_credit())
        .await
        .unwrap()
        .unwrap()
}

fn arm_receive(actor: &Actor, mode: ReceiveMode, sequence: SequenceNumber) {
    match mode {
        ReceiveMode::PeekLock => actor.gate.arm_put(actor.key(sequence)),
        ReceiveMode::ReceiveAndDelete => actor.gate.arm(actor.key(sequence)),
    }
}

fn delivered(result: &RawReceiveResult) -> &Delivery {
    let Ok(CommandOutcome::Received(Some(delivery))) = result else {
        panic!("original actual Receive result");
    };
    delivery
}

#[derive(Clone, Copy)]
enum Terminal {
    Detach,
    End,
    Stop,
}

async fn terminate(wire: &mut Wire, terminal: Terminal) {
    match terminal {
        Terminal::Detach => wire.detach().await,
        Terminal::End => wire.end().await,
        Terminal::Stop => wire.stop().await,
    }
}

async fn native_drained(wire: &mut Wire, count: u32) {
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
    let Performative::Flow(response) = performative(&mut wire.peer).await else {
        panic!("positive drain response, not a late Transfer");
    };
    assert_eq!(response.handle, Some(HANDLE));
    assert_eq!(response.delivery_count, Some(count));
    assert_eq!(response.link_credit, Some(0));
    assert!(response.drain);
}

#[tokio::test(flavor = "current_thread")]
async fn retired_unpolled_receive_never_enters_submit_and_returns_original_credit_once() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                let actor = Actor::new(durable, false);
                actor.send("never-polled", None);
                let before = actor.store().snapshot().unwrap();
                let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                let mut intake = ReceiveIntake::new(reserve(&wire).await, submission(&actor, mode));
                assert!(!intake.started());
                assert!(intake.take_packet().is_none());
                intake.retire();
                assert!(intake.finish().await.is_none());
                assert!(!intake.started());
                assert_eq!(actor.receive_count(), 0);
                assert_eq!(actor.store().snapshot().unwrap(), before);
                let packet = intake.take_packet().unwrap();
                assert!(packet.result.is_none());
                assert!(intake.take_packet().is_none());
                timeout(WAIT, packet.reservation.release())
                    .await
                    .unwrap()
                    .unwrap();
                let returned = reserve(&wire).await;
                timeout(WAIT, returned.release()).await.unwrap().unwrap();
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn started_receive_keeps_its_hidden_result_through_detach_end_and_stop() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                for after_commit in [false, true] {
                    for terminal in [Terminal::Detach, Terminal::End, Terminal::Stop] {
                        let mut actor = Actor::new(durable, false);
                        let sequence = actor.send("hidden-result", None);
                        let before = actor.store().snapshot().unwrap();
                        arm_receive(&actor, mode, sequence);
                        let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                        let mut intake =
                            ReceiveIntake::new(reserve(&wire).await, submission(&actor, mode));
                        let mut observer = Box::pin(intake.observe());
                        pending_once(observer.as_mut()).await;
                        drop(observer);
                        assert!(intake.started());
                        actor.gate.reached(false).await;
                        if after_commit {
                            actor.gate.release(false);
                            actor.gate.reached(true).await;
                            assert_ne!(actor.store().snapshot().unwrap(), before);
                        } else {
                            assert_eq!(actor.store().snapshot().unwrap(), before);
                        }
                        terminate(&mut wire, terminal).await;
                        intake.retire();
                        let mut observer = Box::pin(intake.finish());
                        pending_once(observer.as_mut()).await;
                        drop(observer);
                        assert!(intake.take_packet().is_none());
                        assert_eq!(actor.receive_count(), 1);
                        if !after_commit {
                            actor.gate.release(false);
                            actor.gate.reached(true).await;
                        }
                        actor.gate.release(true);
                        let result = timeout(WAIT, intake.finish())
                            .await
                            .unwrap()
                            .cloned()
                            .unwrap();
                        assert_eq!(timeout(WAIT, intake.finish()).await.unwrap(), Some(&result));
                        let delivery = delivered(&result).clone();
                        assert_eq!(delivery.sequence, sequence);
                        assert_eq!(delivery.delivery_count, 1);
                        assert_eq!(actor.receive_count(), 1);
                        assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                        let packet = intake.take_packet().unwrap();
                        assert_eq!(packet.result, Some(result));
                        assert!(intake.take_packet().is_none());
                        drop(packet.reservation);
                        let committed = actor.store().snapshot().unwrap();
                        wire.stop().await;
                        drop(intake);
                        actor.restart();
                        assert_eq!(actor.store().snapshot().unwrap(), committed);
                        match mode {
                            ReceiveMode::PeekLock => {
                                let lock = delivery.lock.unwrap();
                                let record = StateMachine::new(actor.store().clone())
                                    .message(&actor.namespace, &actor.entity, sequence)
                                    .unwrap()
                                    .unwrap();
                                assert!(
                                    matches!(record.state, MessageState::Locked { token, .. } if token == lock.token)
                                );
                                actor.clock.set(lock.locked_until.as_millis());
                                assert_eq!(
                                    actor.intent(CommandKind::ExpireLocks),
                                    CommandOutcome::LocksExpired {
                                        returned_to_ready: 1,
                                        dead_lettered: 0
                                    }
                                );
                                let recovered = actor.receive();
                                assert_eq!(recovered.sequence, sequence);
                                assert_eq!(recovered.delivery_count, 2);
                            }
                            ReceiveMode::ReceiveAndDelete => {
                                assert!(delivery.lock.is_none());
                                assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                                assert_eq!(
                                    actor.intent(CommandKind::Receive {
                                        mode,
                                        lock_duration_millis: None,
                                        session: None
                                    }),
                                    CommandOutcome::Received(None)
                                );
                            }
                        }
                        assert_eq!(actor.complete_count(), 0);
                        assert!(
                            !actor
                                .log
                                .lock()
                                .unwrap()
                                .iter()
                                .any(|entry| matches!(entry.kind, CommandKind::Abandon { .. }))
                        );
                    }
                }
            }
        }
    }).await.expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn empty_and_refused_actual_results_are_cached_without_inventing_a_delivery() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for refused in [false, true] {
                let actor = Actor::new(durable, refused);
                let before = actor.store().snapshot().unwrap();
                let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                let mut intake = ReceiveIntake::new(
                    reserve(&wire).await,
                    submission(&actor, ReceiveMode::PeekLock),
                );
                let result = timeout(WAIT, intake.observe())
                    .await
                    .unwrap()
                    .cloned()
                    .unwrap();
                assert!(intake.started());
                if refused {
                    assert!(matches!(&result, Err(BrokerRejection::Refused(_))));
                } else {
                    assert_eq!(result, Ok(CommandOutcome::Received(None)));
                }
                let mut observer = Box::pin(async {
                    assert_eq!(intake.observe().await, Some(&result));
                    std::future::pending::<()>().await;
                });
                pending_once(observer.as_mut()).await;
                drop(observer);
                assert_eq!(timeout(WAIT, intake.finish()).await.unwrap(), Some(&result));
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(actor.store().snapshot().unwrap(), before);
                let packet = intake.take_packet().unwrap();
                assert_eq!(packet.result, Some(result));
                assert!(intake.take_packet().is_none());
                packet.reservation.release().await.unwrap();
                let returned = reserve(&wire).await;
                returned.release().await.unwrap();
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn test_local_unexpected_outcome_remains_raw_until_parent_mapping() {
    tokio::spawn(async move {
        let actor = Actor::new(false, false);
        let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
        let actual = submission(&actor, ReceiveMode::PeekLock);
        // Deliberately corrupt only the test-local return shape after an actual
        // empty Receive. This is not an outcome the real broker normally emits.
        let unexpected = async move {
            assert_eq!(actual.await, Ok(CommandOutcome::Received(None)));
            Ok(CommandOutcome::Completed)
        };
        let mut intake = ReceiveIntake::new(reserve(&wire).await, unexpected);
        assert_eq!(intake.observe().await, Some(&Ok(CommandOutcome::Completed)));
        assert_eq!(intake.finish().await, Some(&Ok(CommandOutcome::Completed)));
        let packet = intake.take_packet().unwrap();
        let raw = packet.result.unwrap();
        assert_eq!(raw, Ok(CommandOutcome::Completed));
        assert!(matches!(
            super::super::received_delivery(raw),
            Err(BrokerRejection::Unavailable(_))
        ));
        packet.reservation.release().await.unwrap();
        assert_eq!(actor.receive_count(), 1);
        wire.stop().await;
    })
    .await
    .expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn old_receive_credit_cannot_release_a_replacement_links_held_reservation() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let actor = Actor::new(durable, false);
            let sequence = actor.send("old-credit", None);
            actor.gate.arm_put(actor.key(sequence));
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
            let mut intake = ReceiveIntake::new(
                reserve(&wire).await,
                submission(&actor, ReceiveMode::PeekLock),
            );
            let mut observer = Box::pin(intake.observe());
            pending_once(observer.as_mut()).await;
            drop(observer);
            actor.gate.reached(false).await;
            wire.detach().await;
            let replacement = wire.reattach(1).await;
            let held = timeout(WAIT, replacement.on_credit())
                .await
                .unwrap()
                .unwrap();
            write_frame(
                &mut wire.peer,
                &frame(Performative::Flow(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(0),
                    link_credit: Some(1),
                    drain: true,
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Flow::default()
                })),
            )
            .await
            .unwrap();
            let request = timeout(WAIT, replacement.on_drain())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                replacement.drained(request).await,
                Err(EngineError::InvalidState(_))
            ));
            intake.retire();
            actor.gate.release_all();
            assert!(
                timeout(WAIT, intake.finish())
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            let packet = intake.take_packet().unwrap();
            drop(packet.reservation);
            // Current native cleanup is biased before commands. This actual
            // Drained reply therefore witnesses the queued old Drop cleanup too.
            assert!(matches!(
                replacement.drained(request).await,
                Err(EngineError::InvalidState(_))
            ));
            held.release().await.unwrap();
            replacement.drained(request).await.unwrap();
            let Performative::Flow(response) = performative(&mut wire.peer).await else {
                panic!("replacement's own drain response");
            };
            assert_eq!(response.delivery_count, Some(1));
            assert_eq!(response.link_credit, Some(0));
            assert!(response.drain);
            assert_eq!(actor.receive_count(), 1);
            wire.stop().await;
        }
    })
    .await
    .expect("original test observer task joined");
}

fn authorization() -> LinkAuthorization {
    let scope = ResourceScope::namespace("tenant.servicebus.windows.net").unwrap();
    let rule = SharedAccessRule::new(
        "listen",
        scope,
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

async fn expire_authorization(authorization: &LinkAuthorization) {
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
        .expect("positive actual authorization expiry");
    assert!(authorization.ensure().await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn natural_authorization_retirement_drains_the_original_receive_without_transfer() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, false);
            let sequence = actor.send("auth-retirement", None);
            actor.gate.arm_put(actor.key(sequence));
            let authorization = authorization();
            assert!(authorization.ensure().await.is_ok());
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
            let sender = wire.sender.take().unwrap();
            let protocol = ReceivingLinkProtocol {
                authorization: Some(authorization.clone()),
                management: ConnectionManagement::new(),
            };
            let mut pump = tokio::spawn(serve_receiving_client(
                sender,
                actor.namespace.clone(),
                actor.entity.clone(),
                actor.broker.as_ref().unwrap().clone(),
                ReceiveMode::PeekLock,
                None,
                protocol,
            ));
            actor.gate.reached(false).await;
            expire_authorization(&authorization).await;
            pending_once(Pin::new(&mut pump)).await;
            assert_eq!(actor.receive_count(), 1);
            actor.gate.release_all();
            let Performative::Detach(detach) = performative(&mut wire.peer).await else {
                panic!("authorization close without a late Transfer");
            };
            assert_eq!(
                detach.error.unwrap().condition,
                AmqpError::UnauthorizedAccess.into()
            );
            timeout(WAIT, &mut pump).await.unwrap().unwrap().unwrap();
            assert_eq!(actor.receive_count(), 1);
            assert_eq!(actor.complete_count(), 0);
            let committed = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.restart();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
            let record = StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap()
                .unwrap();
            assert!(matches!(record.state, MessageState::Locked { .. }));
        }
    })
    .await
    .expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_receive_authorization_preparation_retires_with_grant_row_still_held() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                for terminal in [Terminal::End, Terminal::Stop] {
                  for unpolled in [false, true] {
                    let actor = std::sync::Arc::new(Actor::new(durable, true));
                    let session_id = SessionId::new("held-receive-authorization").unwrap();
                    let sequence = actor.send("held-receive-authorization", Some(session_id.clone()));
                    let CommandOutcome::SessionAccepted(Some(accepted)) = actor.intent(CommandKind::AcceptSession {
                        session_id: Some(session_id),
                        lock_duration_millis: None,
                    }) else { panic!("original captured session hold"); };
                    let hold = accepted.hold();
                    let before = acquisition_counters(&actor);
                    let broker = EagerAcquisitionBroker::new(std::sync::Arc::clone(&actor), EagerTarget::Receive);
                    let _release = broker.guard();
                    broker.release_invocation();
                    broker.release_result();
                    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                    let _flow_processed = wire.session_barrier(2).await;
                    let authorization = authorization();
                    let grants = authorization.connection.grant_write_lock().await;
                    let mut original = Box::pin(serve_receiving_client(
                        wire.sender.take().unwrap(), actor.namespace.clone(), actor.entity.clone(),
                        broker.clone(), mode, Some(hold.clone()),
                        ReceivingLinkProtocol {
                            authorization: Some(authorization.clone()),
                            management: ConnectionManagement::new(),
                        },
                    ));
                    if !unpolled {
                        pending_once(original.as_mut()).await;
                        // The original ReserveCredit reply precedes this native FIFO
                        // barrier. The next poll must reach the held authorization row.
                        let _credit_reserved = wire.session_barrier(3).await;
                        pending_once(original.as_mut()).await;
                        assert!(broker.events().is_empty());
                        tokio::select! {
                            biased;
                            result = original.as_mut() => panic!("actual grant row must hold Receive preparation: {}", result.is_ok()),
                            () = std::future::ready(()) => {},
                        }
                    }
                    terminate(&mut wire, terminal).await;
                    timeout(WAIT, original.as_mut()).await.unwrap().unwrap();
                    drop(original);
                    let events = broker.events();
                    let [EagerEvent::Invoked(release), EagerEvent::Completed(completed, result)] = events.as_slice() else {
                        panic!("only exact captured-session cleanup is allowed: {events:?}");
                    };
                    assert_eq!(release, &CommandKind::ReleaseSession { session: hold.clone() });
                    assert_eq!(completed.as_ref(), release);
                    assert_eq!(result, &Ok(CommandOutcome::SessionReleased));
                    assert_eq!(acquisition_counters(&actor), before);
                    let machine = StateMachine::new(actor.store().clone());
                    assert_eq!(machine.message(&actor.namespace, &actor.entity, sequence)
                        .unwrap().unwrap().state, MessageState::Ready);
                    assert!(machine.session(&actor.namespace, &actor.entity, &hold.session_id)
                        .unwrap().unwrap().lock.is_none());
                    // No row release is needed to finish the original helper.
                    drop(grants);
                    wire.stop().await;
                  }
                }
            }
        }
    }).await.expect("original receiving preparation observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn older_original_worker_failure_drains_receive_and_drops_only_its_credit() {
    tokio::spawn(async move {
        for durable in [false, true] {
            let actor = Actor::new(durable, false);
            actor.send("older-worker", None);
            let late = actor.send("late-receive", None);
            actor.gate.arm_put(actor.key(late));
            actor.broker.as_ref().unwrap().fail_complete();
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 2).await;
            let sender = wire.sender.take().unwrap();
            let protocol = ReceivingLinkProtocol {
                authorization: None,
                management: ConnectionManagement::new(),
            };
            let mut pump = tokio::spawn(serve_receiving_client(
                sender,
                actor.namespace.clone(),
                actor.entity.clone(),
                actor.broker.as_ref().unwrap().clone(),
                ReceiveMode::PeekLock,
                None,
                protocol,
            ));
            let Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                ..
            } = timeout(WAIT, read_frame(&mut wire.peer))
                .await
                .unwrap()
                .unwrap()
            else {
                panic!("first actual Transfer");
            };
            actor.gate.reached(false).await;
            wire.accepted(transfer.delivery_id.unwrap()).await;
            timeout(WAIT, async {
                while actor.complete_count() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("older original worker reached its synchronous panic");
            pending_once(Pin::new(&mut pump)).await;
            actor.gate.release_all();
            let result = timeout(WAIT, &mut pump).await.unwrap().unwrap();
            let error = result.expect_err("original worker failure remains observable");
            let error = error
                .downcast::<tokio::task::JoinError>()
                .expect("original raw worker JoinError");
            assert!(error.is_panic());
            assert_eq!(actor.receive_count(), 2);
            assert!(actor.store().get(&actor.key(late)).unwrap().is_some());
            native_drained(&mut wire, 1).await;
            wire.stop().await;
        }
    })
    .await
    .expect("original test observer task joined");
}

#[tokio::test(flavor = "current_thread")]
async fn natural_terminal_drains_receive_before_releasing_an_already_held_session() {
    tokio::spawn(async move {
        for durable in [false, true] {
            for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                for terminal in [Terminal::Detach, Terminal::End, Terminal::Stop] {
                    let mut actor = Actor::new(durable, true);
                    let session_id = SessionId::new("held-session").unwrap();
                    let sequence = actor.send("natural-receive", Some(session_id.clone()));
                    let CommandOutcome::SessionAccepted(Some(accepted)) = actor.intent(CommandKind::AcceptSession {
                        session_id: Some(session_id), lock_duration_millis: None,
                    }) else { panic!("fixture already owns the accepted session"); };
                    let hold = accepted.hold();
                    arm_receive(&actor, mode, sequence);
                    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
                    let sender = wire.sender.take().unwrap();
                    let protocol = ReceivingLinkProtocol { authorization: None, management: ConnectionManagement::new() };
                    let mut pump = tokio::spawn(serve_receiving_client(
                        sender, actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(), mode, Some(hold.clone()), protocol,
                    ));
                    actor.gate.reached(false).await;
                    terminate(&mut wire, terminal).await;
                    pending_once(Pin::new(&mut pump)).await;
                    actor.gate.release_all();
                    timeout(WAIT, &mut pump).await.expect("original natural pump drained").unwrap().unwrap();
                    {
                        let log = actor.log.lock().unwrap();
                        let receives: Vec<_> = log.iter().enumerate().filter(|(_, entry)| matches!(entry.kind, CommandKind::Receive { .. })).collect();
                        let releases: Vec<_> = log.iter().enumerate().filter(|(_, entry)| matches!(entry.kind, CommandKind::ReleaseSession { .. })).collect();
                        assert_eq!(receives.len(), 2);
                        assert!(!receives[0].1.returned);
                        assert!(receives[1].1.returned);
                        assert!(receives.iter().all(|(_, entry)| matches!(&entry.kind, CommandKind::Receive { session: Some(session), mode: actual_mode, .. } if session == &hold && *actual_mode == mode)));
                        assert_eq!(releases.len(), 2);
                        assert!(!releases[0].1.returned);
                        assert!(releases[1].1.returned);
                        assert!(releases.iter().all(|(_, entry)| matches!(&entry.kind, CommandKind::ReleaseSession { session } if session == &hold)));
                        assert!(receives[1].0 < releases[0].0);
                        assert!(!log.iter().any(|entry| matches!(entry.kind, CommandKind::Complete { .. } | CommandKind::Abandon { .. } | CommandKind::Defer { .. } | CommandKind::DeadLetter { .. })));
                    }
                    match mode {
                        ReceiveMode::PeekLock => {
                            let record = StateMachine::new(actor.store().clone()).message(&actor.namespace, &actor.entity, sequence).unwrap().unwrap();
                            assert!(matches!(record.state, MessageState::Locked { .. }));
                        }
                        ReceiveMode::ReceiveAndDelete => assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none()),
                    }
                    let committed = actor.store().snapshot().unwrap();
                    wire.stop().await;
                    actor.restart();
                    assert_eq!(actor.store().snapshot().unwrap(), committed);
                }
            }
        }
    }).await.expect("original test observer task joined");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_actual_receive_returns_before_exact_session_cleanup_and_reopen() {
    use crate::listener::settlement::test_support::{
        EagerAcquisitionBroker, EagerEvent, EagerTarget, acquisition_counters, assert_eager_release,
    };
    use std::sync::Arc;

    for durable in [false, true] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            let actor = Actor::new(durable, true);
            let session_id = SessionId::new("eager-receive").unwrap();
            let sequence = actor.send("eager-receive", Some(session_id.clone()));
            let CommandOutcome::SessionAccepted(Some(accepted)) =
                actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id),
                    lock_duration_millis: None,
                })
            else {
                panic!("actual fixture session grant");
            };
            let hold = accepted.hold();
            let before_counters = acquisition_counters(&actor);
            actor.clock.set(2_000);
            arm_receive(&actor, mode, sequence);
            let actor = Arc::new(actor);
            let broker = EagerAcquisitionBroker::new(Arc::clone(&actor), EagerTarget::Receive);
            let guard = broker.guard();
            let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
            let sender = wire.sender.take().unwrap();
            let (borrowed_done, borrowed) = tokio::sync::oneshot::channel();
            let (resume, resumed) = tokio::sync::oneshot::channel();
            let mut original = {
                let actor = Arc::clone(&actor);
                let broker = broker.clone();
                let hold = hold.clone();
                tokio::spawn(async move {
                    let mut helper = Box::pin(serve_receiving_client(
                        sender,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        broker.clone(),
                        mode,
                        Some(hold),
                        ReceivingLinkProtocol {
                            authorization: None,
                            management: ConnectionManagement::new(),
                        },
                    ));
                    tokio::select! {
                        () = broker.first_poll() => {}
                        _ = helper.as_mut() => panic!("helper cleaned up before observing its raw Receive"),
                    }
                    let _ = borrowed_done.send(());
                    let _ = resumed.await;
                    helper.await
                })
            };
            actor.gate.reached(false).await;
            actor.gate.release(false);
            actor.gate.reached(true).await;
            assert_eq!(
                StateMachine::new(actor.store().clone())
                    .last_applied_time()
                    .unwrap()
                    .as_millis(),
                2_000
            );
            wire.detach().await;
            actor.gate.release_all();
            let raw = broker.captured().await;
            let CommandOutcome::Received(Some(delivery)) = &raw else {
                panic!("actual eager Receive");
            };
            assert_eq!(delivery.sequence, sequence);
            broker.release_invocation();
            tokio::select! {
                () = broker.first_poll() => {}
                result = &mut original => panic!(
                    "helper finished before the returned-future poll: failed={}, events={:?}",
                    result.is_err(), broker.events()
                ),
            }
            timeout(WAIT, borrowed).await.unwrap().unwrap();
            assert_eq!(broker.events().len(), 3);
            broker.release_result();
            resume.send(()).unwrap();
            timeout(WAIT, original).await.unwrap().unwrap().unwrap();
            assert!(matches!(
                assert_eager_release(&broker.events(), &raw, &hold),
                CommandKind::Receive { mode: actual, session: Some(session), .. }
                    if actual == mode && session == hold
            ));
            let machine = StateMachine::new(actor.store().clone());
            match mode {
                ReceiveMode::PeekLock => {
                    let record = machine
                        .message(&actor.namespace, &actor.entity, sequence)
                        .unwrap()
                        .unwrap();
                    assert!(matches!(record.state, MessageState::Locked { token, .. }
                        if Some(token) == delivery.lock.map(|lock| lock.token)));
                }
                ReceiveMode::ReceiveAndDelete => assert!(
                    machine
                        .message(&actor.namespace, &actor.entity, sequence)
                        .unwrap()
                        .is_none()
                ),
            }
            assert!(
                machine
                    .session(&actor.namespace, &actor.entity, &hold.session_id)
                    .unwrap()
                    .unwrap()
                    .lock
                    .is_none()
            );
            let after_counters = acquisition_counters(&actor);
            assert_eq!(after_counters.next_sequence, before_counters.next_sequence);
            assert_eq!(
                after_counters.next_lock_token,
                before_counters.next_lock_token + u64::from(mode == ReceiveMode::PeekLock)
            );
            // The replacement Attach response on this session rejects any earlier late Transfer.
            let replacement = wire.reattach(1).await;
            drop(replacement);
            wire.stop().await;
            let committed = actor.store().snapshot().unwrap();
            drop(machine);
            drop(guard);
            drop(broker);
            let mut actor = Arc::try_unwrap(actor)
                .unwrap_or_else(|_| panic!("all eager observer references released"));
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
            assert_eq!(
                StateMachine::new(actor.store().clone())
                    .last_applied_time()
                    .unwrap()
                    .as_millis(),
                2_000
            );
        }
    }

    // Original-link retirement before helper polling invokes no eager Receive method/future.
    let actor = Actor::new(false, true);
    let session_id = SessionId::new("unpolled-receive").unwrap();
    let sequence = actor.send("unpolled-receive", Some(session_id.clone()));
    let CommandOutcome::SessionAccepted(Some(accepted)) =
        actor.intent(CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: None,
        })
    else {
        panic!("actual fixture session grant");
    };
    let hold = accepted.hold();
    let before_counters = acquisition_counters(&actor);
    let actor = Arc::new(actor);
    let broker = EagerAcquisitionBroker::new(Arc::clone(&actor), EagerTarget::Receive);
    let _guard = broker.guard();
    let mut wire = Wire::new_with_credit(ReceiverSettleMode::First, 1).await;
    let sender = wire.sender.take().unwrap();
    wire.detach().await;
    let original = tokio::spawn(serve_receiving_client(
        sender,
        actor.namespace.clone(),
        actor.entity.clone(),
        broker.clone(),
        ReceiveMode::PeekLock,
        Some(hold.clone()),
        ReceivingLinkProtocol {
            authorization: None,
            management: ConnectionManagement::new(),
        },
    ));
    timeout(WAIT, original).await.unwrap().unwrap().unwrap();
    let events = broker.events();
    let [
        EagerEvent::Invoked(invoked),
        EagerEvent::Completed(completed, result),
    ] = events.as_slice()
    else {
        panic!("only held-session cleanup is allowed: {events:?}");
    };
    assert_eq!(
        invoked,
        &CommandKind::ReleaseSession {
            session: hold.clone()
        }
    );
    assert_eq!(completed.as_ref(), invoked);
    assert_eq!(result, &Ok(CommandOutcome::SessionReleased));
    assert_eq!(acquisition_counters(&actor), before_counters);
    assert_eq!(
        StateMachine::new(actor.store().clone())
            .message(&actor.namespace, &actor.entity, sequence)
            .unwrap()
            .unwrap()
            .state,
        MessageState::Ready
    );
    wire.stop().await;
}
