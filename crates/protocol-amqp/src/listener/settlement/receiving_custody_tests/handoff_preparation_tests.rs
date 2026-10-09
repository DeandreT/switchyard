//! User-clone panic is preparation, not a native/result recovery promise.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;

struct CloneControl {
    clones: AtomicUsize,
    armed: AtomicBool,
    panics: AtomicUsize,
    payload: Arc<str>,
    received: Mutex<Option<Result<CommandOutcome, BrokerRejection>>>,
    completed: Mutex<Option<Result<CommandOutcome, BrokerRejection>>>,
    changed: Notify,
}

impl CloneControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            clones: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
            panics: AtomicUsize::new(0),
            payload: Arc::from("original settlement-context preparation clone"),
            received: Mutex::new(None),
            completed: Mutex::new(None),
            changed: Notify::new(),
        })
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn complete_returned(&self) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.completed.lock().unwrap().is_some() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
    }
}

struct CloneProbe<B> {
    actual: B,
    control: Arc<CloneControl>,
    arm_after_receive: bool,
}

impl<B: Clone> Clone for CloneProbe<B> {
    fn clone(&self) -> Self {
        self.control.clones.fetch_add(1, Ordering::SeqCst);
        if self.control.armed.load(Ordering::SeqCst) {
            self.control.panics.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(Arc::clone(&self.control.payload));
        }
        Self {
            actual: self.actual.clone(),
            control: Arc::clone(&self.control),
            arm_after_receive: self.arm_after_receive,
        }
    }
}

impl<B: Broker> Broker for CloneProbe<B> {
    async fn submit(
        &self,
        namespace: domain::NamespaceName,
        entity: domain::EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let receive = matches!(kind, CommandKind::Receive { .. });
        let complete = matches!(kind, CommandKind::Complete { .. });
        let result = self.actual.submit(namespace, entity, kind).await;
        if receive {
            *self.control.received.lock().unwrap() = Some(result.clone());
            if self.arm_after_receive && matches!(result, Ok(CommandOutcome::Received(Some(_)))) {
                self.control.arm();
            }
        }
        if complete {
            *self.control.completed.lock().unwrap() = Some(result.clone());
            self.control.changed.notify_waiters();
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

fn assert_primary(payload: Box<dyn std::any::Any + Send>, expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_receive_retains_its_guarantee_when_pre_native_context_clone_panics() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for guarantee in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    let id = SessionId::new("failed-context-preparation").unwrap();
                    let sequence = actor.send("failed-context-preparation", Some(id.clone()));
                    let accepted = accept(&actor, &id);
                    let hold = accepted.hold();
                    let management = ConnectionManagement::new();
                    let old = register(&management, &actor, &hold).await;
                    let replacement = register(&management, &actor, &hold).await;
                    assert_ne!(old, replacement);
                    let release_gate = Arc::new(ReleaseGate::default());
                    let clones = CloneControl::new();
                    let broker = CloneProbe {
                        actual: ReleaseGateBroker {
                            actual: actor.broker.as_ref().unwrap().clone(),
                            gate: Arc::clone(&release_gate),
                            panic_complete: false,
                        },
                        control: Arc::clone(&clones),
                        arm_after_receive: true,
                    };
                    actor
                        .gate
                        .arm_put(domain::keys::session(&actor.namespace, &actor.entity, &id));
                    let session_row = management.session_write_lock().await;
                    let mut wire = Wire::new_with_credit(mode.clone(), 1).await;
                    let mut pump = start(
                        &mut wire,
                        &actor,
                        broker,
                        guarantee,
                        Some(hold),
                        ReceivingLinkProtocol {
                            authorization: None,
                            management: Arc::clone(&management),
                            session_registration: Some(old),
                        },
                        PumpPanic::new(PanicFrontier::Waiting),
                    );
                    actor.gate.reached(false).await;
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    pending_once(Pin::new(&mut pump)).await;
                    assert_eq!(clones.clones.load(Ordering::SeqCst), 3);
                    assert_eq!(clones.panics.load(Ordering::SeqCst), 1);
                    assert_eq!(actor.receive_count(), 1);
                    assert_eq!(actor.complete_count(), 0);
                    assert_eq!(releases(&actor), 1);
                    let Some(Ok(CommandOutcome::Received(Some(delivery)))) =
                        clones.received.lock().unwrap().clone()
                    else {
                        panic!("same original actual Receive result");
                    };
                    assert_eq!(delivery.sequence, sequence);
                    match guarantee {
                        ReceiveMode::PeekLock => {
                            let token = delivery.lock.unwrap().token;
                            let record = StateMachine::new(actor.store().clone())
                                .message(&actor.namespace, &actor.entity, sequence)
                                .unwrap()
                                .unwrap();
                            assert!(
                                matches!(record.state, MessageState::Locked { token: retained, .. } if retained == token)
                            );
                            assert!(management.delivery(LINK, token).await.is_none());
                        }
                        ReceiveMode::ReceiveAndDelete => {
                            assert!(delivery.lock.is_none());
                            assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                        }
                    }
                    // A positive native FIFO boundary rejects any Transfer or
                    // new acknowledgement before this original Begin response.
                    let _barrier = wire.session_barrier(2).await;
                    actor.gate.release_all();
                    release_gate.captured().await;
                    assert!(matches!(
                        release_gate.raw.lock().unwrap().as_ref(),
                        Some(Ok(CommandOutcome::SessionReleased))
                    ));
                    pending_once(Pin::new(&mut pump)).await;
                    assert_eq!(clones.panics.load(Ordering::SeqCst), 1);
                    release_gate.allow();
                    pending_once(Pin::new(&mut pump)).await;
                    drop(session_row);
                    let error = timeout(WAIT, &mut pump).await.unwrap().unwrap_err();
                    assert!(error.is_panic());
                    assert_primary(error.into_panic(), &clones.payload);
                    assert_eq!(clones.clones.load(Ordering::SeqCst), 3);
                    assert_eq!(clones.panics.load(Ordering::SeqCst), 1);
                    assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                    assert_eq!(releases(&actor), 1);
                    assert_eq!(
                        management.registered_session_owner(LINK).await,
                        Some(replacement.clone())
                    );
                    management.unregister_session(&replacement).await;
                    let snapshot = actor.store().snapshot().unwrap();
                    wire.stop().await;
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), snapshot);
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_active_native_success_moves_prepared_context_without_a_second_clone() {
    for durable in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("active-prepared-context").unwrap();
                let sequence = actor.send("active-prepared-context", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let management = ConnectionManagement::new();
                let old_session = register(&management, &actor, &hold).await;
                let clones = CloneControl::new();
                let broker = CloneProbe {
                    actual: actor.broker.as_ref().unwrap().clone(),
                    control: Arc::clone(&clones),
                    arm_after_receive: false,
                };
                actor.gate.arm(actor.key(sequence));
                let mut wire = Wire::new_with_credit(mode.clone(), 1).await;
                wire.writes.block();
                let _release = wire.writes.release_on_drop();
                let control = PumpPanic::new(PanicFrontier::Waiting);
                let mut pump = start(
                    &mut wire,
                    &actor,
                    broker,
                    ReceiveMode::PeekLock,
                    Some(hold.clone()),
                    ReceivingLinkProtocol {
                        authorization: None,
                        management: Arc::clone(&management),
                        session_registration: Some(old_session),
                    },
                    Arc::clone(&control),
                );
                wire.writes.reached().await;
                assert_eq!(clones.clones.load(Ordering::SeqCst), 3);
                // The original native start is positively held after context
                // preparation; any old post-ready clone would now panic.
                clones.arm();
                wire.writes.release();
                let original = transfer(&mut wire).await;
                wire.accepted(original.delivery_id.unwrap()).await;
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                let Some(Ok(CommandOutcome::Received(Some(delivery)))) =
                    clones.received.lock().unwrap().clone()
                else {
                    panic!("one actual Receive result");
                };
                let token = delivery.lock.unwrap().token;
                let replacement_delivery = management
                    .register_delivery(LINK, actor.entity.clone(), sequence, token)
                    .await;
                let replacement_session = register(&management, &actor, &hold).await;
                control.request();
                timeout(WAIT, control.fired()).await.unwrap();
                pending_once(Pin::new(&mut pump)).await;
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(releases(&actor), 0);
                assert_eq!(clones.panics.load(Ordering::SeqCst), 0);
                actor.gate.release_all();
                primary(timeout(WAIT, &mut pump).await.unwrap().unwrap_err());
                assert_eq!(actor.complete_count(), 1);
                assert_eq!(actor.gate.state.lock().unwrap().commits, 1);
                assert_eq!(clones.clones.load(Ordering::SeqCst), 3);
                assert_eq!(clones.panics.load(Ordering::SeqCst), 0);
                assert_eq!(releases(&actor), 1);
                let submissions: Vec<_> = actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|entry| matches!(entry.kind, CommandKind::Complete { .. }))
                    .cloned()
                    .collect();
                assert_eq!(submissions.len(), 2);
                assert!(!submissions[0].returned && submissions[1].returned);
                assert_eq!(submissions[0].worker, submissions[1].worker);
                assert!(
                    matches!(submissions[0].kind, CommandKind::Complete { sequence: retained, lock_token } if retained == sequence && lock_token == token)
                );
                assert!(management.delivery(LINK, token).await.is_some());
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(replacement_session.clone())
                );
                let _barrier = wire.session_barrier(2).await;
                management.unregister_delivery(&replacement_delivery).await;
                management.unregister_session(&replacement_session).await;
                assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                let snapshot = actor.store().snapshot().unwrap();
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_late_native_success_moves_one_context_across_cancelled_cleanup() {
    // ActualBroker records the executing Tokio task's identity even during
    // the borrowed owner's lazy ReleaseSession invocation.
    tokio::spawn(async {
        for durable in [false, true] {
            for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    let id = SessionId::new("late-prepared-context").unwrap();
                    let sequence = actor.send("late-prepared-context", Some(id.clone()));
                    let accepted = accept(&actor, &id);
                    let hold = accepted.hold();
                    let delivery = receive_session(&actor, &hold);
                    let token = delivery.lock.unwrap().token;
                    let management = ConnectionManagement::new();
                    let old_session = register(&management, &actor, &hold).await;
                    let replacement_session = register(&management, &actor, &hold).await;
                    let old_delivery = management.register_delivery(LINK, actor.entity.clone(), sequence, token).await;
                    let replacement_delivery = management.register_delivery(LINK, actor.entity.clone(), sequence, token).await;
                    assert_ne!(old_delivery, replacement_delivery);
                    assert_ne!(old_session, replacement_session);
                    let release_gate = Arc::new(ReleaseGate::default());
                    let clones = CloneControl::new();
                    let context = context(&actor, CloneProbe {
                        actual: ReleaseGateBroker {
                            actual: actor.broker.as_ref().unwrap().clone(),
                            gate: Arc::clone(&release_gate), panic_complete: false,
                        },
                        control: Arc::clone(&clones), arm_after_receive: false,
                    }, Arc::clone(&management));
                    let mut wire = Wire::new_with_credit(mode.clone(), 1).await;
                    let sender = wire.sender.take().unwrap();
                    let mut custody = ReceivingCustody::new(&context, Some(hold), Some(old_session.clone()));
                    custody.registrations.push(old_delivery.clone());
                    custody.transfer_registration = Some(old_delivery.clone());
                    custody.transfer_context = Some(context.clone());
                    assert_eq!(clones.clones.load(Ordering::SeqCst), 2);
                    let reservation = timeout(WAIT, sender.on_credit()).await.unwrap().unwrap();
                    let message = crate::message::write_delivery_from(&delivery, None).unwrap();
                    wire.writes.block();
                    let _release = wire.writes.release_on_drop();
                    custody.transfer = Some(PendingTransfer::new(delivery, sender.send_pending_with_credit(
                        reservation, message, super::super::lock_delivery_tag(token),
                    )));
                    let mut observed = Box::pin(custody.transfer.as_mut().unwrap().observe());
                    pending_once(observed.as_mut()).await;
                    wire.writes.reached().await;
                    drop(observed);
                    clones.arm();
                    // This direct owner control retains a synthetic outer
                    // primary payload; the native start and outcomes are real.
                    let primary_payload: Arc<str> = Arc::from("captured receiving outer panic");
                    custody.record_primary(Box::new(Arc::clone(&primary_payload)));
                    for _ in 0..2 {
                        let mut borrowed = Box::pin(custody.finish(&context));
                        pending_once(borrowed.as_mut()).await;
                        drop(borrowed);
                        assert!(custody.transfer_context.is_some());
                        assert_eq!(custody.transfer_registration, Some(old_delivery.clone()));
                        assert!(custody.workers.is_empty());
                        assert_eq!(clones.clones.load(Ordering::SeqCst), 2);
                    }
                    wire.writes.release();
                    let original = transfer(&mut wire).await;
                    wire.accepted(original.delivery_id.unwrap()).await;
                    let _barrier = wire.session_barrier(2).await;
                    let delivery_row = management.delivery_write_lock().await;
                    for _ in 0..2 {
                        let mut borrowed = Box::pin(custody.finish(&context));
                        pending_once(borrowed.as_mut()).await;
                        drop(borrowed);
                        assert!(custody.transfer_context.is_none());
                        assert!(custody.transfer_registration.is_none());
                        assert!(custody.transferred.is_none());
                        assert_eq!(custody.workers.len(), 1);
                        assert_eq!(custody.registrations, vec![old_delivery.clone()]);
                        assert_eq!(actor.complete_count(), 0);
                        assert_eq!(clones.clones.load(Ordering::SeqCst), 2);
                    }
                    actor.gate.arm(actor.key(sequence));
                    drop(delivery_row);
                    let mut borrowed = Box::pin(custody.finish(&context));
                    timeout(WAIT, async {
                        tokio::select! {
                            biased;
                            () = actor.gate.reached(false) => {},
                            _ = borrowed.as_mut() => panic!("same original Complete is held"),
                        }
                    }).await.unwrap();
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    drop(borrowed);
                    assert_eq!(actor.complete_count(), 1);
                    assert_eq!(releases(&actor), 0);
                    let worker = actor.log.lock().unwrap().iter()
                        .find(|entry| !entry.returned && matches!(entry.kind, CommandKind::Complete { .. }))
                        .unwrap().worker;
                    let mut borrowed = Box::pin(custody.finish(&context));
                    pending_once(borrowed.as_mut()).await;
                    drop(borrowed);
                    assert_eq!(actor.complete_count(), 1);
                    actor.gate.release_all();
                    // The actual delegated Complete has returned before the
                    // same store gate is rearmed. No finish observer can start
                    // the lazy ReleaseSession during this handoff.
                    clones.complete_returned().await;
                    assert!(matches!(clones.completed.lock().unwrap().as_ref(), Some(Ok(CommandOutcome::Completed))));
                    actor.gate.arm_put(domain::keys::session(&actor.namespace, &actor.entity, &id));
                    let session_row = management.session_write_lock().await;
                    let mut borrowed = Box::pin(custody.finish(&context));
                    timeout(WAIT, async {
                        tokio::select! {
                            biased;
                            () = actor.gate.reached(false) => {},
                            _ = borrowed.as_mut() => panic!("one original ReleaseSession is held"),
                        }
                    }).await.unwrap();
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    drop(borrowed);
                    assert_eq!(custody.workers.finished().len(), 1);
                    let joined = &custody.workers.finished()[0];
                    assert_eq!(joined.id, worker);
                    assert_eq!(joined.registration, Some(old_delivery.clone()));
                    assert_eq!(joined.lock_token, Some(token));
                    assert!(joined.result.as_ref().unwrap().result.is_ok());
                    assert!(custody.registrations.is_empty());
                    assert_eq!(releases(&actor), 1);
                    assert_eq!(clones.clones.load(Ordering::SeqCst), 2);
                    actor.gate.release_all();
                    let mut borrowed = Box::pin(custody.finish(&context));
                    timeout(WAIT, async {
                        tokio::select! {
                            biased;
                            () = release_gate.captured() => {},
                            _ = borrowed.as_mut() => panic!("same original raw release result remains held"),
                        }
                    }).await.unwrap();
                    drop(borrowed);
                    assert!(custody.release_result().is_none());
                    assert!(matches!(release_gate.raw.lock().unwrap().as_ref(), Some(Ok(CommandOutcome::SessionReleased))));
                    release_gate.allow();
                    for _ in 0..2 {
                        let mut borrowed = Box::pin(custody.finish(&context));
                        pending_once(borrowed.as_mut()).await;
                        drop(borrowed);
                        assert!(matches!(custody.release_result(), Some(Ok(CommandOutcome::SessionReleased))));
                        assert_eq!(custody.session_registration, Some(old_session.clone()));
                        assert_eq!(releases(&actor), 1);
                    }
                    drop(session_row);
                    timeout(WAIT, custody.finish(&context)).await.unwrap();
                    timeout(WAIT, custody.finish(&context)).await.unwrap();
                    assert_eq!(custody.workers.finished().len(), 1);
                    assert!(custody.transfer_context.is_none());
                    assert_eq!(clones.clones.load(Ordering::SeqCst), 2);
                    assert_eq!(clones.panics.load(Ordering::SeqCst), 0);
                    assert_primary(custody.take_panic().unwrap(), &primary_payload);
                    assert_eq!(management.registered_session_owner(LINK).await, Some(replacement_session.clone()));
                    assert!(management.delivery(LINK, token).await.is_some());
                    assert!(actor.store().get(&actor.key(sequence)).unwrap().is_none());
                    let _barrier = wire.session_barrier(3).await;
                    management.unregister_delivery(&replacement_delivery).await;
                    management.unregister_session(&replacement_session).await;
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
    }).await.unwrap();
}
