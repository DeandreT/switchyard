//! Actual receiver-pump panic and borrowed cleanup boundaries.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use amqp::{Frame, Performative, ReceiverSettleMode, Transfer, read_frame};
use domain::{
    AcceptedSession, CommandKind, CommandOutcome, Delivery, MessageState, ReceiveMode, SessionHold,
    SessionId, StateMachine,
};
use storage::StateStore;
use tokio::{
    sync::Notify,
    task::{JoinError, JoinHandle},
    time::timeout,
};

use super::{
    SettlementContext,
    custody::{PUMP_PANIC, PanicFrontier, PumpPanic, ReceivingCustody},
    intake::ReceiveIntake,
    pending_transfer::PendingTransfer,
    serve_receiving_client,
    test_support::*,
};
use crate::listener::ReceivingLinkProtocol;
use crate::{
    Broker, BrokerRejection,
    management::{ConnectionManagement, SessionRegistration},
};

type PumpTask = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

fn accept(actor: &Actor, id: &SessionId) -> AcceptedSession {
    let CommandOutcome::SessionAccepted(Some(accepted)) =
        actor.intent(CommandKind::AcceptSession {
            session_id: Some(id.clone()),
            lock_duration_millis: None,
        })
    else {
        panic!("actual session grant");
    };
    accepted
}

async fn register(
    management: &Arc<ConnectionManagement>,
    actor: &Actor,
    hold: &SessionHold,
) -> SessionRegistration {
    let claim = management.claim_session(LINK, actor.entity.clone());
    management
        .install_session(&claim, hold.clone(), || true)
        .await
        .unwrap()
}

fn start<B: Broker>(
    wire: &mut Wire,
    actor: &Actor,
    broker: B,
    mode: ReceiveMode,
    hold: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
    control: Arc<PumpPanic>,
) -> PumpTask {
    tokio::spawn(PUMP_PANIC.scope(
        control,
        serve_receiving_client(
            wire.sender.take().unwrap(),
            actor.namespace.clone(),
            actor.entity.clone(),
            broker,
            mode,
            hold,
            protocol,
        ),
    ))
}

async fn transfer(wire: &mut Wire) -> Transfer {
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = timeout(WAIT, read_frame(&mut wire.peer))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("one actual native Transfer");
    };
    transfer
}

fn primary(error: JoinError) {
    assert!(error.is_panic());
    assert_eq!(
        error.into_panic().downcast_ref::<&str>(),
        Some(&"receiving-primary-panic")
    );
}

fn releases(actor: &Actor) -> usize {
    actor
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| !entry.returned && matches!(entry.kind, CommandKind::ReleaseSession { .. }))
        .count()
}

fn context<B: Broker>(
    actor: &Actor,
    broker: B,
    management: Arc<ConnectionManagement>,
) -> SettlementContext<B> {
    SettlementContext {
        namespace: actor.namespace.clone(),
        entity: actor.entity.clone(),
        broker,
        authorization: None,
        management,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_primary_pump_panic_drains_one_complete_before_and_after_apply() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("panic-commit").unwrap();
                let first = actor.send("panic-commit", Some(id.clone()));
                let unanswered = actor.send("panic-unanswered", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let management = ConnectionManagement::new();
                let registration = register(&management, &actor, &hold).await;
                actor.gate.arm(actor.key(first));
                let mut wire = Wire::new_with_credit(mode.clone(), 2).await;
                let control = PumpPanic::new(PanicFrontier::Waiting);
                let mut pump = start(
                    &mut wire,
                    &actor,
                    actor.broker.as_ref().unwrap().clone(),
                    ReceiveMode::PeekLock,
                    Some(hold.clone()),
                    ReceivingLinkProtocol {
                        authorization: None,
                        management: Arc::clone(&management),
                        session_registration: Some(registration.clone()),
                    },
                    Arc::clone(&control),
                );
                let original = transfer(&mut wire).await;
                let _unanswered = transfer(&mut wire).await;
                wire.accepted(original.delivery_id.unwrap()).await;
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                control.request();
                timeout(WAIT, control.fired()).await.unwrap();
                pending_once(Pin::new(&mut pump)).await;
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(releases(&actor), 0);
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(registration)
                );
                // A real native FIFO barrier would encounter any early fake
                // confirmation before the expected Begin response.
                let _barrier = wire.session_barrier(2).await;
                if !after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                    pending_once(Pin::new(&mut pump)).await;
                    assert_eq!(releases(&actor), 0);
                }
                actor.gate.release(true);
                primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                assert_eq!(releases(&actor), 1);
                assert!(management.registered_session_owner(LINK).await.is_none());
                assert!(
                    StateMachine::new(actor.store().clone())
                        .message(&actor.namespace, &actor.entity, first)
                        .unwrap()
                        .is_none()
                );
                assert!(matches!(
                    StateMachine::new(actor.store().clone())
                        .message(&actor.namespace, &actor.entity, unanswered)
                        .unwrap()
                        .unwrap()
                        .state,
                    MessageState::Locked { .. }
                ));
                let _barrier = wire.session_barrier(3).await;
                let snapshot = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_pump_panic_retains_a_begun_receive_without_late_transfer() {
    for durable in [false, true] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("panic-intake").unwrap();
                let sequence = actor.send("panic-intake", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let management = ConnectionManagement::new();
                let registration = register(&management, &actor, &hold).await;
                match mode {
                    ReceiveMode::PeekLock => actor.gate.arm_put(actor.key(sequence)),
                    ReceiveMode::ReceiveAndDelete => actor.gate.arm(actor.key(sequence)),
                }
                let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 1).await;
                let control = PumpPanic::new(PanicFrontier::Waiting);
                let mut pump = start(
                    &mut wire,
                    &actor,
                    actor.broker.as_ref().unwrap().clone(),
                    mode,
                    Some(hold),
                    ReceivingLinkProtocol {
                        authorization: None,
                        management: Arc::clone(&management),
                        session_registration: Some(registration),
                    },
                    Arc::clone(&control),
                );
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                control.request();
                timeout(WAIT, control.fired()).await.unwrap();
                pending_once(Pin::new(&mut pump)).await;
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(releases(&actor), 0);
                let _barrier = wire.session_barrier(2).await;
                actor.gate.release_all();
                primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(releases(&actor), 1);
                assert!(management.registered_session_owner(LINK).await.is_none());
                let message = StateMachine::new(actor.store().clone())
                    .message(&actor.namespace, &actor.entity, sequence)
                    .unwrap();
                match mode {
                    ReceiveMode::PeekLock => assert!(matches!(
                        message.unwrap().state,
                        MessageState::Locked { .. }
                    )),
                    ReceiveMode::ReceiveAndDelete => assert!(message.is_none()),
                }
                let _barrier = wire.session_barrier(3).await;
                let snapshot = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_completed_packets_survive_primary_panic_before_successor_adoption() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for frontier in [PanicFrontier::ReceivePacket, PanicFrontier::TransferPacket] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("panic-packet").unwrap();
                let sequence = actor.send("panic-packet", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let management = ConnectionManagement::new();
                let registration = register(&management, &actor, &hold).await;
                let mut wire = Wire::new_with_credit(mode.clone(), 1).await;
                let control = PumpPanic::new(frontier);
                control.request();
                let mut pump = start(
                    &mut wire,
                    &actor,
                    actor.broker.as_ref().unwrap().clone(),
                    ReceiveMode::PeekLock,
                    Some(hold),
                    ReceivingLinkProtocol {
                        authorization: None,
                        management: Arc::clone(&management),
                        session_registration: Some(registration),
                    },
                    Arc::clone(&control),
                );
                timeout(WAIT, control.fired()).await.unwrap();
                primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
                if frontier == PanicFrontier::TransferPacket {
                    let _ = transfer(&mut wire).await;
                }
                let _barrier = wire.session_barrier(2).await;
                assert_eq!(actor.receive_count(), 1);
                assert_eq!(actor.complete_count(), 0);
                assert_eq!(releases(&actor), 1);
                assert!(management.registered_session_owner(LINK).await.is_none());
                let MessageState::Locked { token, .. } = StateMachine::new(actor.store().clone())
                    .message(&actor.namespace, &actor.entity, sequence)
                    .unwrap()
                    .unwrap()
                    .state
                else {
                    panic!("original packet still has its durable lock");
                };
                assert!(management.delivery(LINK, token).await.is_none());
                let snapshot = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }
}

#[derive(Default)]
struct ReleaseGate {
    raw: Mutex<Option<Result<CommandOutcome, BrokerRejection>>>,
    allowed: std::sync::atomic::AtomicBool,
    changed: Notify,
}

impl ReleaseGate {
    async fn captured(&self) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.raw.lock().unwrap().is_some() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
    }
    fn allow(&self) {
        self.allowed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[derive(Clone)]
struct ReleaseGateBroker {
    actual: ActualBroker,
    gate: Arc<ReleaseGate>,
    panic_complete: bool,
}

impl Broker for ReleaseGateBroker {
    async fn submit(
        &self,
        namespace: domain::NamespaceName,
        entity: domain::EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let release = matches!(kind, CommandKind::ReleaseSession { .. });
        let complete = matches!(kind, CommandKind::Complete { .. });
        let result = self.actual.submit(namespace, entity, kind).await;
        if complete && self.panic_complete {
            panic!("secondary-worker-panic");
        }
        if release {
            *self.gate.raw.lock().unwrap() = Some(result.clone());
            self.gate.changed.notify_waiters();
            loop {
                let notified = self.gate.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.gate.allowed.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
        result
    }
    fn deliverable(
        &self,
        namespace: &domain::NamespaceName,
        entity: &domain::EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

fn receive_session(actor: &Actor, hold: &SessionHold) -> Delivery {
    let CommandOutcome::Received(Some(delivery)) = actor.intent(CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: Some(hold.clone()),
    }) else {
        panic!("actual locked session delivery");
    };
    delivery
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_receiving_cleanup_retains_rows_and_one_original_session_release() {
    tokio::spawn(async {
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("cancelled-receiver-cleanup").unwrap();
                actor.send("cancelled-receiver-cleanup", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let delivery = receive_session(&actor, &hold);
                let token = delivery.lock.unwrap().token;
                let management = ConnectionManagement::new();
                let old_session = register(&management, &actor, &hold).await;
                let new_session = register(&management, &actor, &hold).await;
                assert_ne!(old_session, new_session);
                let old_delivery = management
                    .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                    .await;
                let new_delivery = management
                    .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                    .await;
                assert_ne!(old_delivery, new_delivery);
                let release_gate = Arc::new(ReleaseGate::default());
                let broker = ReleaseGateBroker {
                    actual: actor.broker.as_ref().unwrap().clone(),
                    gate: Arc::clone(&release_gate),
                    panic_complete: false,
                };
                let context = context(&actor, broker, Arc::clone(&management));
                let mut custody =
                    ReceivingCustody::new(&context, Some(hold.clone()), Some(old_session.clone()));
                custody.registrations.push(old_delivery.clone());
                assert_eq!(releases(&actor), 0, "release invocation is lazy");

                let row = management.delivery_write_lock().await;
                let mut borrowed = Box::pin(custody.finish(&context));
                pending_once(borrowed.as_mut()).await;
                drop(borrowed);
                assert_eq!(custody.registrations, vec![old_delivery]);
                assert_eq!(custody.session_registration, Some(old_session.clone()));
                assert_eq!(releases(&actor), 0);
                drop(row);
                actor
                    .gate
                    .arm_put(domain::keys::session(&actor.namespace, &actor.entity, &id));
                let session_row = management.session_write_lock().await;
                let mut borrowed = Box::pin(custody.finish(&context));
                pending_once(borrowed.as_mut()).await;
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                drop(borrowed);
                assert!(custody.registrations.is_empty());
                assert_eq!(custody.session_registration, Some(old_session.clone()));
                assert_eq!(releases(&actor), 1);
                let mut retry = Box::pin(custody.finish(&context));
                pending_once(retry.as_mut()).await;
                drop(retry);
                assert_eq!(releases(&actor), 1);
                actor.gate.release_all();
                let mut retry = Box::pin(custody.finish(&context));
                timeout(WAIT, async {
                    tokio::select! {
                        biased;
                        () = release_gate.captured() => {},
                        _ = retry.as_mut() => panic!("raw release result remains held"),
                    }
                })
                .await
                .unwrap();
                drop(retry);
                assert!(custody.release_result().is_none());
                assert!(matches!(
                    release_gate.raw.lock().unwrap().as_ref(),
                    Some(Ok(CommandOutcome::SessionReleased))
                ));
                release_gate.allow();
                let mut retry = Box::pin(custody.finish(&context));
                pending_once(retry.as_mut()).await;
                drop(retry);
                assert!(matches!(
                    custody.release_result(),
                    Some(Ok(CommandOutcome::SessionReleased))
                ));
                assert_eq!(custody.session_registration, Some(old_session));
                assert_eq!(releases(&actor), 1);
                drop(session_row);
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                assert!(custody.session_registration.is_none());
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(new_session)
                );
                assert!(management.delivery(LINK, token).await.is_some());
                management.unregister_delivery(&new_delivery).await;
                assert!(management.delivery(LINK, token).await.is_none());
                assert_eq!(releases(&actor), 1);
                let snapshot = actor.store().snapshot().unwrap();
                drop(custody);
                drop(context);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_primary_pump_panic_keeps_a_blocked_native_start_until_its_result() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, true);
            let id = SessionId::new("panic-native").unwrap();
            let sequence = actor.send("panic-native", Some(id.clone()));
            let accepted = accept(&actor, &id);
            let hold = accepted.hold();
            let management = ConnectionManagement::new();
            let registration = register(&management, &actor, &hold).await;
            let mut wire = Wire::new_with_credit(mode, 1).await;
            wire.writes.block();
            let _release = wire.writes.release_on_drop();
            let control = PumpPanic::new(PanicFrontier::Waiting);
            let mut pump = start(
                &mut wire,
                &actor,
                actor.broker.as_ref().unwrap().clone(),
                ReceiveMode::PeekLock,
                Some(hold),
                ReceivingLinkProtocol {
                    authorization: None,
                    management: Arc::clone(&management),
                    session_registration: Some(registration.clone()),
                },
                Arc::clone(&control),
            );
            wire.writes.reached().await;
            let MessageState::Locked { token, .. } = StateMachine::new(actor.store().clone())
                .message(&actor.namespace, &actor.entity, sequence)
                .unwrap()
                .unwrap()
                .state
            else {
                panic!("native start owns the original committed lock");
            };
            assert!(management.delivery(LINK, token).await.is_some());
            control.request();
            timeout(WAIT, control.fired()).await.unwrap();
            pending_once(Pin::new(&mut pump)).await;
            pending_once(Pin::new(&mut pump)).await;
            assert_eq!(actor.receive_count(), 1);
            assert_eq!(actor.complete_count(), 0);
            assert_eq!(releases(&actor), 0);
            assert_eq!(
                management.registered_session_owner(LINK).await,
                Some(registration)
            );
            wire.writes.release();
            let original = transfer(&mut wire).await;
            let bytes: &[u8] = original.delivery_tag.as_ref().unwrap().as_ref();
            assert_eq!(&bytes[8..], &token.as_u64().to_be_bytes());
            primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
            assert_eq!(actor.receive_count(), 1);
            assert_eq!(actor.complete_count(), 0);
            assert_eq!(releases(&actor), 1);
            assert!(management.delivery(LINK, token).await.is_none());
            assert!(management.registered_session_owner(LINK).await.is_none());
            let _barrier = wire.session_barrier(2).await;
            let snapshot = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_outer_cleanup_adopts_the_same_ready_native_packet_and_original_worker() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, false);
                let sequence = actor.send("late-owner-adoption", None);
                let delivery = actor.receive();
                let token = delivery.lock.unwrap().token;
                let management = ConnectionManagement::new();
                let registration = management
                    .register_delivery(LINK, actor.entity.clone(), sequence, token)
                    .await;
                let context = actor.context(Arc::clone(&management));
                let mut wire = Wire::new_with_credit(mode.clone(), 1).await;
                let sender = wire.sender.take().unwrap();
                let reservation = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
                let message = crate::message::write_delivery_from(&delivery, None).unwrap();
                wire.writes.block();
                let _release = wire.writes.release_on_drop();
                let mut custody = ReceivingCustody::new(&context, None, None);
                custody.registrations.push(registration.clone());
                custody.transfer_registration = Some(registration.clone());
                custody.transfer = Some(PendingTransfer::new(
                    delivery,
                    sender.send_pending_with_credit(
                        reservation,
                        message,
                        super::lock_delivery_tag(token),
                    ),
                ));
                let mut observe = Box::pin(custody.transfer.as_mut().unwrap().observe());
                pending_once(observe.as_mut()).await;
                wire.writes.reached().await;
                drop(observe);
                custody.record_primary(Box::new("receiving-primary-panic"));
                let mut borrowed = Box::pin(custody.finish(&context));
                pending_once(borrowed.as_mut()).await;
                drop(borrowed);
                assert!(custody.transfer.as_ref().unwrap().started());
                assert!(custody.workers.is_empty());
                assert_eq!(custody.transfer_registration, Some(registration.clone()));
                wire.writes.release();
                let original = transfer(&mut wire).await;
                wire.accepted(original.delivery_id.unwrap()).await;
                // Outcome is actually processed while the finish observer is
                // absent, so late adoption must prefer the ready raw outcome.
                let _barrier = wire.session_barrier(2).await;
                actor.gate.arm(actor.key(sequence));
                let mut borrowed = Box::pin(custody.finish(&context));
                timeout(WAIT, async {
                    tokio::select! {
                        biased;
                        () = actor.gate.reached(false) => {},
                        _ = borrowed.as_mut() => panic!("late original worker remains held"),
                    }
                })
                .await
                .unwrap();
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                drop(borrowed);
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(custody.workers.len(), 1);
                assert!(custody.transfer_registration.is_none());
                let mut retry = Box::pin(custody.finish(&context));
                pending_once(retry.as_mut()).await;
                drop(retry);
                assert_eq!(actor.complete_count(), 1);
                actor.gate.release_all();
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                assert_eq!(custody.workers.finished().len(), 1);
                let joined = &custody.workers.finished()[0];
                assert_eq!(joined.registration, Some(registration));
                assert!(joined.result.as_ref().unwrap().result.is_ok());
                let submissions: Vec<_> = actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|entry| {
                        !entry.returned && matches!(entry.kind, CommandKind::Complete { .. })
                    })
                    .cloned()
                    .collect();
                assert_eq!(submissions.len(), 1);
                assert_eq!(submissions[0].worker, joined.id);
                assert_eq!(
                    custody.take_panic().unwrap().downcast_ref::<&str>(),
                    Some(&"receiving-primary-panic")
                );
                assert!(management.delivery(LINK, token).await.is_none());
                assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                let _barrier = wire.session_barrier(3).await;
                let snapshot = actor.store().snapshot().unwrap();
                drop(custody);
                drop(context);
                drop(sender);
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_primary_panic_wins_secondary_worker_panic_and_stale_session_release() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut actor = Actor::new(durable, true);
            let id = SessionId::new("primary-precedence").unwrap();
            let sequence = actor.send("primary-precedence", Some(id.clone()));
            actor.send("primary-unanswered", Some(id.clone()));
            let accepted = accept(&actor, &id);
            let old = accepted.hold();
            let management = ConnectionManagement::new();
            let old_registration = register(&management, &actor, &old).await;
            let release_gate = Arc::new(ReleaseGate::default());
            let broker = ReleaseGateBroker {
                actual: actor.broker.as_ref().unwrap().clone(),
                gate: Arc::clone(&release_gate),
                panic_complete: true,
            };
            actor.gate.arm(actor.key(sequence));
            let mut wire = Wire::new_with_credit(mode, 2).await;
            let control = PumpPanic::new(PanicFrontier::Waiting);
            let mut pump = start(
                &mut wire,
                &actor,
                broker,
                ReceiveMode::PeekLock,
                Some(old.clone()),
                ReceivingLinkProtocol {
                    authorization: None,
                    management: Arc::clone(&management),
                    session_registration: Some(old_registration),
                },
                Arc::clone(&control),
            );
            let original = transfer(&mut wire).await;
            let unanswered = transfer(&mut wire).await;
            wire.accepted(original.delivery_id.unwrap()).await;
            actor.gate.reached(false).await;
            // The unanswered worker and residual cleanup cannot pass this
            // actual row while the primary panic and committed worker drain.
            let row = management.delivery_write_lock().await;
            control.request();
            timeout(WAIT, control.fired()).await.unwrap();
            actor.gate.release_all();
            timeout(WAIT, async {
                loop {
                    if actor.log.lock().unwrap().iter().any(|entry| {
                        entry.returned && matches!(entry.kind, CommandKind::Complete { .. })
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            pending_once(Pin::new(&mut pump)).await;
            assert_eq!(releases(&actor), 0);
            actor.clock.set(accepted.lock.locked_until.as_millis());
            let replacement = accept(&actor, &id);
            assert_ne!(old.token, replacement.hold().token);
            let new_registration = register(&management, &actor, &replacement.hold()).await;
            drop(row);
            release_gate.captured().await;
            pending_once(Pin::new(&mut pump)).await;
            assert!(matches!(
                release_gate.raw.lock().unwrap().as_ref(),
                Some(Err(BrokerRejection::Refused(
                    domain::BrokerError::SessionLockNotHeld { .. }
                )))
            ));
            release_gate.allow();
            primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
            assert_eq!(actor.complete_count(), 1);
            assert_eq!(releases(&actor), 1);
            assert_eq!(
                management.registered_session_owner(LINK).await,
                Some(new_registration)
            );
            let session = StateMachine::new(actor.store().clone())
                .session(&actor.namespace, &actor.entity, &id)
                .unwrap()
                .unwrap();
            assert_eq!(session.lock.unwrap(), replacement.lock);
            let bytes: &[u8] = unanswered.delivery_tag.as_ref().unwrap().as_ref();
            let token = domain::LockToken::new(u64::from_be_bytes(bytes[8..].try_into().unwrap()));
            assert!(management.delivery(LINK, token).await.is_none());
            let _barrier = wire.session_barrier(2).await;
            let snapshot = actor.store().snapshot().unwrap();
            wire.stop().await;
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            assert_eq!(
                StateMachine::new(actor.store().clone())
                    .session(&actor.namespace, &actor.entity, &id)
                    .unwrap()
                    .unwrap()
                    .lock
                    .unwrap(),
                replacement.lock
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn poisoned_originals_keep_panic_and_metadata_without_repoll_or_fabricated_result() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        let sequence = actor.send("poison-state", None);
        let delivery = actor.receive();
        let token = delivery.lock.unwrap().token;
        let management = ConnectionManagement::new();
        let registration = management
            .register_delivery(LINK, actor.entity.clone(), sequence, token)
            .await;
        let context = actor.context(Arc::clone(&management));
        let mut wire = Wire::new_with_credit(ReceiverSettleMode::Second, 1).await;
        let sender = wire.sender.take().unwrap();
        let reservation = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
        let receive_polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let native_polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut custody = ReceivingCustody::new(&context, None, None);
        custody.registrations.push(registration.clone());
        custody.transfer_registration = Some(registration);
        let counter = Arc::clone(&receive_polls);
        // These terminal-state doubles do not claim recovery or certification
        // of a broker/native operation whose own future unwound.
        custody.intake = Some(ReceiveIntake::new(reservation, async move {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("poisoned-receive-original");
        }));
        let counter = Arc::clone(&native_polls);
        custody.transfer = Some(PendingTransfer::new(delivery, async move {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("poisoned-native-original");
        }));
        // Start originals through their normal borrowed observer frontiers.
        use futures_util::FutureExt;
        let received = std::panic::AssertUnwindSafe(custody.intake.as_mut().unwrap().observe())
            .catch_unwind()
            .await
            .unwrap_err();
        custody.record_primary(received);
        let transferred =
            std::panic::AssertUnwindSafe(custody.transfer.as_mut().unwrap().observe())
                .catch_unwind()
                .await;
        let secondary = match transferred {
            Err(payload) => payload,
            Ok(_) => panic!("original native panic"),
        };
        // The primary wins even when another supported cleanup original later
        // panics. Save that raw payload as secondary evidence in the holder.
        custody.record_cleanup_panic(secondary);
        timeout(WAIT, custody.finish(&context)).await.unwrap();
        timeout(WAIT, custody.finish(&context)).await.unwrap();
        assert_eq!(receive_polls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(native_polls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(custody.receive_poisoned);
        assert!(custody.retired_receive.is_none());
        let packet = custody.transferred.as_ref().unwrap();
        assert!(packet.panicked && packet.started && packet.retired);
        assert!(packet.result.is_none());
        assert_eq!(packet.delivery.sequence, sequence);
        assert_eq!(packet.delivery.lock.unwrap().token, token);
        assert_eq!(custody.secondary_panics().len(), 1);
        assert_eq!(
            custody.secondary_panics()[0].downcast_ref::<&str>(),
            Some(&"poisoned-native-original")
        );
        assert_eq!(
            custody.take_panic().unwrap().downcast_ref::<&str>(),
            Some(&"poisoned-receive-original")
        );
        assert!(management.delivery(LINK, token).await.is_none());
        assert_eq!(actor.receive_count(), 0);
        assert_eq!(actor.complete_count(), 0);
        let snapshot = actor.store().snapshot().unwrap();
        drop(custody);
        drop(context);
        drop(sender);
        wire.stop().await;
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), snapshot);
    }
}
