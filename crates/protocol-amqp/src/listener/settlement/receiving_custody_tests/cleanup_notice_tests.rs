//! New faults discovered by receiving cleanup, not pump/family recovery.
//! Manually composed slots and poll-poison doubles are qualified separately
//! from their real broker, native IO and original worker evidence.

use super::*;
use crate::listener::{
    ConnectionRetirementRequest,
    settlement::{
        SettlementContext, SettlementFailure,
        custody::{OriginalCleanup, ReceivingCustody},
        intake::ReceiveIntake,
        pending_transfer::PendingTransfer,
        settle_started_delivery,
        workers::SettlementWorkers,
    },
};
use crate::management::{DeliveryRegistration, SessionRegistration};
use domain::{AcceptedSession, SessionHold};
use std::sync::atomic::AtomicUsize;

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

fn accept(actor: &Actor, id: &SessionId) -> AcceptedSession {
    let CommandOutcome::SessionAccepted(Some(accepted)) =
        actor.intent(CommandKind::AcceptSession {
            session_id: Some(id.clone()),
            lock_duration_millis: None,
        })
    else {
        panic!("actual session grant")
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

fn assert_payload(payload: &(dyn std::any::Any + Send), expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}

async fn start_worker<B: Broker>(
    workers: &mut SettlementWorkers,
    wire: &mut Wire,
    actor: &Actor,
    delivery: Delivery,
    context: SettlementContext<B>,
) -> (Id, u32, DeliveryRegistration) {
    let registration = context
        .management
        .register_delivery(
            LINK,
            actor.entity.clone(),
            delivery.sequence,
            delivery.lock.unwrap().token,
        )
        .await;
    let (pending, wire_id) = wire.start(&delivery).await;
    let retired = workers.subscribe();
    let id = workers.spawn(
        Some(registration.clone()),
        settle_started_delivery(
            pending,
            delivery,
            Some(registration.clone()),
            context,
            retired,
        ),
    );
    (id, wire_id, registration)
}

#[tokio::test(flavor = "current_thread")]
async fn new_worker_fault_is_cached_before_notice_and_held_actual_native_drain() {
    for durable in [false, true] {
        for fault_kind in 0..3 {
            let mut actor = Actor::new(durable, false);
            actor.send("native-opposite", None);
            let delivery = actor.receive();
            let snapshot = actor.store().snapshot().unwrap();
            let mut wire = Wire::new(ReceiverSettleMode::Second).await;
            let _barrier = wire.session_barrier(2).await;
            let _release = wire.writes.release_on_drop();
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let sender = wire.sender.take().unwrap();
            let mut workers = SettlementWorkers::new();
            let fault = Arc::new(Notify::new());
            let payload: Arc<str> = Arc::from("new original worker panic");
            let task_fault = Arc::clone(&fault);
            let task_payload = Arc::clone(&payload);
            let first = workers.spawn(None, async move {
                task_fault.notified().await;
                // The panic is an actual raw JoinError. The other two rows are
                // qualified typed-completion classification doubles, not codec IO.
                match fault_kind {
                    0 => std::panic::panic_any(task_payload),
                    1 => Err(SettlementFailure::Protocol(
                        crate::ProtocolError::InvalidEnvelope {
                            detail: "worker classification double".to_owned(),
                        },
                    )),
                    _ => Err(SettlementFailure::Engine(amqp::EngineError::InvalidState(
                        "worker classification double".to_owned(),
                    ))),
                }
            });
            wire.writes.block();
            let returned = Arc::new(Notify::new());
            let retained = Arc::new(Notify::new());
            let task_returned = Arc::clone(&returned);
            let task_retained = Arc::clone(&retained);
            let message = crate::message::write_delivery_from(&delivery, None).unwrap();
            let token = delivery.lock.unwrap().token;
            // Qualified composition: after the actual native future returns, this
            // original task deliberately retains that result behind a second gate.
            let second = workers.spawn(None, async move {
                let raw = sender.send_pending(message, lock_delivery_tag(token)).await;
                task_returned.notify_one();
                task_retained.notified().await;
                match raw {
                    Ok(_) => panic!("writer was never released"),
                    Err(error) => Err(SettlementFailure::Engine(error)),
                }
            });
            wire.writes.reached().await;
            assert!(!notice.is_requested());
            fault.notify_one();
            let mut borrowed = Box::pin(workers.finish_with_retirement(Some(&notice)));
            timeout(WAIT, async {
                tokio::select! {
                    _ = borrowed.as_mut() => panic!("opposite original is still retained"),
                    () = notice.observer() => {},
                }
            })
            .await
            .unwrap();
            drop(borrowed);
            assert_eq!(workers.finished().len(), 1);
            assert_eq!(workers.finished()[0].id, first);
            if fault_kind == 0 {
                let Err(error) = &workers.finished()[0].result else {
                    panic!("raw original join failure")
                };
                assert!(error.is_panic());
            } else {
                let raw = &workers.finished()[0]
                    .result
                    .as_ref()
                    .expect("typed original completion")
                    .result;
                if fault_kind == 1 {
                    assert!(matches!(raw, Err(SettlementFailure::Protocol(_))));
                } else {
                    assert!(matches!(
                        raw,
                        Err(SettlementFailure::Engine(amqp::EngineError::InvalidState(
                            _
                        )))
                    ));
                }
            }
            assert!(
                workers.failures().is_empty(),
                "no next() synthetic Stopped path"
            );
            notice.request();
            notice.request();
            timeout(WAIT, returned.notified()).await.unwrap();
            timeout(WAIT, wire.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
            assert!(wire.writes.state.lock().unwrap().blocked);
            let mut retry = Box::pin(workers.finish_with_retirement(Some(&notice)));
            pending_once(retry.as_mut()).await;
            drop(retry);
            assert_eq!(workers.finished()[0].id, first);
            retained.notify_one();
            timeout(WAIT, workers.finish_with_retirement(Some(&notice)))
                .await
                .unwrap();
            assert_eq!(workers.finished().len(), 2);
            assert_eq!(workers.finished()[1].id, second);
            timeout(WAIT, workers.finish_with_retirement(Some(&notice)))
                .await
                .unwrap();
            let error = workers.into_join_error();
            if fault_kind == 0 {
                assert_payload(error.unwrap().into_panic().as_ref(), &payload);
            } else {
                assert!(error.is_none());
            }
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn new_original_poll_poison_notifies_before_held_actual_native_drain() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        actor.send("intake-poison-opposite", None);
        let delivery = actor.receive();
        let snapshot = actor.store().snapshot().unwrap();
        let management = ConnectionManagement::new();
        let context = actor.context(Arc::clone(&management));
        let mut wire = Wire::new(ReceiverSettleMode::Second).await;
        let _barrier = wire.session_barrier(2).await;
        let _release = wire.writes.release_on_drop();
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let sender = wire.sender.take().unwrap();
        let reservation = sender.on_credit().await.unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(false));
        let payload: Arc<str> = Arc::from("original intake poll panic");
        let polled = Arc::clone(&polls);
        let allowed = Arc::clone(&ready);
        let original_payload = Arc::clone(&payload);
        let mut custody =
            ReceivingCustody::new(&context, None, None).with_retirement(Some(&notice));
        custody.intake = Some(ReceiveIntake::new(
            reservation,
            poll_fn(move |_| {
                polled.fetch_add(1, Ordering::SeqCst);
                if allowed.load(Ordering::SeqCst) {
                    std::panic::panic_any(Arc::clone(&original_payload));
                }
                Poll::Pending
            }),
        ));
        let mut intake = Box::pin(custody.intake.as_mut().unwrap().observe());
        pending_once(intake.as_mut()).await;
        drop(intake);
        let message = crate::message::write_delivery_from(&delivery, None).unwrap();
        let token = delivery.lock.unwrap().token;
        custody.transfer = Some(PendingTransfer::new(
            delivery,
            sender.send_pending(message, lock_delivery_tag(token)),
        ));
        wire.writes.block();
        let mut start = Box::pin(custody.transfer.as_mut().unwrap().observe());
        timeout(WAIT, async {
            tokio::select! {
                _ = start.as_mut() => panic!("actual original Transfer is held"),
                () = wire.writes.reached() => {},
            }
        })
        .await
        .unwrap();
        drop(start);
        assert!(!notice.is_requested());
        ready.store(true, Ordering::SeqCst);
        let mut finish = Box::pin(custody.finish(&context));
        timeout(WAIT, async {
            tokio::select! {
                _ = finish.as_mut() => {},
                () = notice.observer() => {},
            }
        })
        .await
        .unwrap();
        drop(finish);
        assert!(notice.is_requested());
        assert_eq!(custody.secondary_panics().len(), 1);
        assert_payload(custody.secondary_panics()[0].as_ref(), &payload);
        timeout(WAIT, custody.finish(&context)).await.unwrap();
        timeout(WAIT, custody.finish(&context)).await.unwrap();
        assert_eq!(
            polls.load(Ordering::SeqCst),
            2,
            "poisoned original is never re-polled"
        );
        assert!(custody.receive_poisoned && custody.retired_receive.is_none());
        let packet = custody.transferred.as_ref().unwrap();
        assert!(packet.started && packet.retired && !packet.panicked);
        assert!(matches!(
            packet.result,
            Some(Err(amqp::EngineError::Stopped))
        ));
        assert!(wire.writes.state.lock().unwrap().blocked);
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_payload(custody.take_panic().unwrap().as_ref(), &payload);
        assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        assert_eq!(
            actor.receive_count(),
            0,
            "intake poison is a terminal-state double"
        );
        drop(custody);
        drop(context);
        drop(sender);
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        for credit in [false, true] {
            let mut actor = Actor::new(durable, false);
            actor.send("other-poison-slot", None);
            let delivery = actor.receive();
            let snapshot = actor.store().snapshot().unwrap();
            let context = actor.context(ConnectionManagement::new());
            let mut wire = Wire::new(ReceiverSettleMode::Second).await;
            let _barrier = wire.session_barrier(2).await;
            let _release = wire.writes.release_on_drop();
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let sender = wire.sender.take().unwrap();
            let message = crate::message::write_delivery_from(&delivery, None).unwrap();
            let token = delivery.lock.unwrap().token;
            let mut custody =
                ReceivingCustody::new(&context, None, None).with_retirement(Some(&notice));
            let ready = Arc::new(AtomicBool::new(false));
            let polls = Arc::new(AtomicUsize::new(0));
            let payload: Arc<str> = Arc::from("new credit or native original poll panic");
            let allowed = Arc::clone(&ready);
            let counted = Arc::clone(&polls);
            let raw_payload = Arc::clone(&payload);
            // These two rows are qualified terminal-state slot compositions.
            // The opposite worker nevertheless polls a real held native Send.
            if credit {
                custody.credit_release = Some(OriginalCleanup::new(poll_fn(move |_| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    if allowed.load(Ordering::SeqCst) {
                        std::panic::panic_any(Arc::clone(&raw_payload));
                    }
                    Poll::Pending
                })));
                let mut original = Box::pin(custody.credit_release.as_mut().unwrap().finish());
                pending_once(original.as_mut()).await;
                drop(original);
            } else {
                custody.transfer = Some(PendingTransfer::new(
                    delivery,
                    poll_fn(move |_| {
                        counted.fetch_add(1, Ordering::SeqCst);
                        if allowed.load(Ordering::SeqCst) {
                            std::panic::panic_any(Arc::clone(&raw_payload));
                        }
                        Poll::Pending
                    }),
                ));
                let mut original = Box::pin(custody.transfer.as_mut().unwrap().observe());
                pending_once(original.as_mut()).await;
                drop(original);
            }
            wire.writes.block();
            let opposite = custody.workers.spawn(None, async move {
                sender
                    .send_pending(message, lock_delivery_tag(token))
                    .await
                    .map(|_| ())
                    .map_err(SettlementFailure::Engine)
            });
            wire.writes.reached().await;
            ready.store(true, Ordering::SeqCst);
            let mut finish = Box::pin(custody.finish(&context));
            timeout(WAIT, async {
                tokio::select! {
                    _ = finish.as_mut() => {},
                    () = notice.observer() => {},
                }
            })
            .await
            .unwrap();
            drop(finish);
            assert!(notice.is_requested());
            assert_payload(custody.secondary_panics()[0].as_ref(), &payload);
            timeout(WAIT, custody.finish(&context)).await.unwrap();
            timeout(WAIT, custody.finish(&context)).await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 2);
            assert_eq!(custody.workers.finished()[0].id, opposite);
            assert!(matches!(
                custody.workers.finished()[0]
                    .result
                    .as_ref()
                    .unwrap()
                    .result,
                Err(SettlementFailure::Engine(amqp::EngineError::Stopped))
            ));
            if !credit {
                let packet = custody.transferred.as_ref().unwrap();
                assert!(
                    packet.started && packet.retired && packet.panicked && packet.result.is_none()
                );
            }
            assert!(wire.writes.state.lock().unwrap().blocked);
            timeout(WAIT, wire.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
            assert_payload(custody.take_panic().unwrap().as_ref(), &payload);
            drop(custody);
            drop(context);
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        }
    }
}

async fn second_sender(wire: &mut Wire) -> Sender {
    let mut attach = Attach {
        name: "other-credit-owner".to_owned(),
        handle: 2,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("orders")),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    write_frame(
        &mut wire.peer,
        &frame(Performative::Attach(Box::new(attach.clone()))),
    )
    .await
    .unwrap();
    attach = wire._session.next_incoming_attach().await.unwrap();
    let LinkEndpoint::Sender(sender) = wire
        ._session
        .accept_attach(attach, 1024 * 1024)
        .await
        .unwrap()
    else {
        panic!("second actual sender")
    };
    assert!(matches!(
        performative(&mut wire.peer).await,
        Performative::Attach(_)
    ));
    write_frame(
        &mut wire.peer,
        &frame(Performative::Flow(Flow {
            handle: Some(2),
            delivery_count: Some(0),
            link_credit: Some(1),
            incoming_window: 2048,
            outgoing_window: 2048,
            ..Flow::default()
        })),
    )
    .await
    .unwrap();
    sender
}

#[tokio::test(flavor = "current_thread")]
async fn new_actual_native_error_notifies_before_conditional_row_wait_but_cached_error_does_not() {
    for durable in [false, true] {
        for cached in [false, true] {
            let mut actor = Actor::new(durable, false);
            actor.send("native-error", None);
            let delivery = actor.receive();
            let snapshot = actor.store().snapshot().unwrap();
            let management = ConnectionManagement::new();
            let token = delivery.lock.unwrap().token;
            let old = management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            let replacement = management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            assert_ne!(old, replacement);
            let context = actor.context(Arc::clone(&management));
            let mut wire = Wire::new(ReceiverSettleMode::Second).await;
            let other = second_sender(&mut wire).await;
            let reservation = other.on_credit().await.unwrap();
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let sender = wire.sender.take().unwrap();
            let allow = Arc::new(Notify::new());
            let original_allow = Arc::clone(&allow);
            let message = crate::message::write_delivery_from(&delivery, None).unwrap();
            let mut custody =
                ReceivingCustody::new(&context, None, None).with_retirement(Some(&notice));
            custody.registrations.push(old.clone());
            // Actual API error: a real reservation from handle 2 is presented
            // to handle 1. The pre-result Notify is qualified slot composition.
            custody.transfer = Some(PendingTransfer::new(delivery, async {
                original_allow.notified().await;
                sender
                    .send_pending_with_credit(reservation, message, lock_delivery_tag(token))
                    .await
            }));
            let mut original = Box::pin(custody.transfer.as_mut().unwrap().observe());
            pending_once(original.as_mut()).await;
            drop(original);
            allow.notify_one();
            if cached {
                let raw = custody.transfer.as_mut().unwrap().observe().await.unwrap();
                assert!(matches!(raw, Err(amqp::EngineError::InvalidState(reason))
                    if reason == "credit reservation belongs to another link"));
            }
            let row = management.delivery_write_lock().await;
            let mut finish = Box::pin(custody.finish(&context));
            if cached {
                pending_once(finish.as_mut()).await;
            } else {
                timeout(WAIT, async {
                    tokio::select! {
                        _ = finish.as_mut() => panic!("captured row remains held"),
                        () = notice.observer() => {},
                    }
                })
                .await
                .unwrap();
            }
            drop(finish);
            assert_eq!(notice.is_requested(), !cached);
            assert_eq!(custody.registrations, vec![old]);
            let packet = custody.transferred.as_ref().unwrap() as *const _;
            assert!(matches!(
                custody.transferred.as_ref().unwrap().result,
                Some(Err(amqp::EngineError::InvalidState(_)))
            ));
            let mut retry = Box::pin(custody.finish(&context));
            pending_once(retry.as_mut()).await;
            drop(retry);
            assert_eq!(custody.transferred.as_ref().unwrap() as *const _, packet);
            assert_eq!(notice.is_requested(), !cached);
            drop(row);
            timeout(WAIT, custody.finish(&context)).await.unwrap();
            assert!(management.delivery(LINK, token).await.is_some());
            management.unregister_delivery(&replacement).await;
            assert!(management.delivery(LINK, token).await.is_none());
            assert!(custody.take_panic().is_none());
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            let mut independent = Wire::new(ReceiverSettleMode::First).await;
            let _positive = independent.session_barrier(2).await;
            independent.stop().await;
            wire.stop().await;
            drop(custody);
            drop(context);
            drop(sender);
            drop(other);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        }
    }
}

#[derive(Clone)]
struct CompletePanicBroker {
    actual: ActualBroker,
    first: SequenceNumber,
    reached: Arc<Notify>,
    allow: Arc<Notify>,
    payload: Arc<str>,
}

impl Broker for CompletePanicBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let panic =
            matches!(kind, CommandKind::Complete { sequence, .. } if sequence == self.first);
        let raw = self.actual.submit(namespace, entity, kind).await;
        if panic {
            self.reached.notify_one();
            self.allow.notified().await;
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        raw
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_worker_raw_join_fault_precedes_another_held_broker_commit() {
    for durable in [false, true] {
        for after in [false, true] {
            let mut actor = Actor::new(durable, false);
            let first_sequence = actor.send("complete-poison", None);
            let second_sequence = actor.send("complete-held", None);
            let first_delivery = actor.receive();
            let second_delivery = actor.receive();
            assert_eq!(first_delivery.sequence, first_sequence);
            assert_eq!(second_delivery.sequence, second_sequence);
            let management = ConnectionManagement::new();
            let payload: Arc<str> = Arc::from("delegated Complete original panic");
            let broker = CompletePanicBroker {
                actual: actor.broker.as_ref().unwrap().clone(),
                first: first_sequence,
                reached: Arc::new(Notify::new()),
                allow: Arc::new(Notify::new()),
                payload: Arc::clone(&payload),
            };
            let mut wire = Wire::new(ReceiverSettleMode::Second).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let mut workers = SettlementWorkers::new();
            let (first, first_wire, first_registration) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                first_delivery,
                context(&actor, broker.clone(), Arc::clone(&management)),
            )
            .await;
            wire.accepted(first_wire).await;
            timeout(WAIT, broker.reached.notified()).await.unwrap();
            assert!(
                actor
                    .store()
                    .get(&actor.key(first_sequence))
                    .unwrap()
                    .is_none()
            );
            actor.gate.arm(actor.key(second_sequence));
            let (second, second_wire, second_registration) = start_worker(
                &mut workers,
                &mut wire,
                &actor,
                second_delivery,
                context(&actor, broker.clone(), Arc::clone(&management)),
            )
            .await;
            wire.accepted(second_wire).await;
            actor.gate.reached(false).await;
            if after {
                actor.gate.release(false);
                actor.gate.reached(true).await;
            }
            assert_eq!(actor.complete_count(), 2);
            assert!(!notice.is_requested());
            broker.allow.notify_one();
            let mut finish = Box::pin(workers.finish_with_retirement(Some(&notice)));
            timeout(WAIT, async {
                tokio::select! {
                    _ = finish.as_mut() => panic!("second actual Complete remains held"),
                    () = notice.observer() => {},
                }
            })
            .await
            .unwrap();
            drop(finish);
            assert_eq!(workers.finished().len(), 1);
            assert_eq!(workers.finished()[0].id, first);
            assert_eq!(workers.finished()[0].registration, Some(first_registration));
            let Err(error) = &workers.finished()[0].result else {
                panic!("raw delegated worker panic")
            };
            assert!(error.is_panic());
            let mut retry = Box::pin(workers.finish_with_retirement(Some(&notice)));
            pending_once(retry.as_mut()).await;
            drop(retry);
            assert_eq!(actor.complete_count(), 2);
            actor.gate.release_all();
            timeout(WAIT, workers.finish_with_retirement(Some(&notice)))
                .await
                .unwrap();
            assert_eq!(workers.finished().len(), 2);
            assert_eq!(workers.finished()[1].id, second);
            assert_eq!(
                workers.finished()[1].registration,
                Some(second_registration)
            );
            assert!(
                workers.finished()[1]
                    .result
                    .as_ref()
                    .unwrap()
                    .result
                    .is_ok()
            );
            assert!(workers.failures().is_empty());
            assert_payload(
                workers.into_join_error().unwrap().into_panic().as_ref(),
                &payload,
            );
            assert_eq!(actor.complete_count(), 2);
            assert!(
                actor
                    .store()
                    .get(&actor.key(second_sequence))
                    .unwrap()
                    .is_none()
            );
            {
                let log = actor.log.lock().unwrap();
                assert_eq!(log.len(), 4);
                assert!(
                    log.iter()
                        .all(|entry| entry.worker == first || entry.worker == second)
                );
                assert_eq!(log.iter().filter(|entry| entry.returned).count(), 2);
            }
            wire.stop().await;
            drop(broker);
            let snapshot = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), snapshot);
        }
    }
}

#[derive(Clone)]
struct ReleasePanicBroker {
    actual: ActualBroker,
    delegate: bool,
    reached: Arc<Notify>,
    allow: Arc<Notify>,
    payload: Arc<str>,
}

impl Broker for ReleasePanicBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(kind, CommandKind::ReleaseSession { .. }) {
            if self.delegate {
                let raw = self.actual.submit(namespace, entity, kind).await;
                assert!(matches!(raw, Ok(CommandOutcome::SessionReleased)));
            }
            self.reached.notify_one();
            self.allow.notified().await;
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        self.actual.submit(namespace, entity, kind).await
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_release_panic_notifies_before_conditional_session_cleanup_and_keeps_primary() {
    tokio::spawn(async {
        for durable in [false, true] {
            for delegate in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("release-poison").unwrap();
                actor.send("release-poison", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let hold = accepted.hold();
                let management = ConnectionManagement::new();
                let old = register(&management, &actor, &hold).await;
                let replacement = register(&management, &actor, &hold).await;
                assert_ne!(old, replacement);
                let payload: Arc<str> = Arc::from("original session release panic");
                let primary: Arc<str> = Arc::from("retained primary pump panic");
                let broker = ReleasePanicBroker {
                    actual: actor.broker.as_ref().unwrap().clone(),
                    delegate,
                    reached: Arc::new(Notify::new()),
                    allow: Arc::new(Notify::new()),
                    payload: Arc::clone(&payload),
                };
                let context = context(&actor, broker.clone(), Arc::clone(&management));
                let mut wire = Wire::new(ReceiverSettleMode::Second).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let mut custody = ReceivingCustody::new(&context, Some(hold), Some(old.clone()))
                    .with_retirement(Some(&notice));
                // Qualified priority composition: this raw primary is seeded,
                // not a claim that an ordinary #139 primary lacked its notice.
                custody.record_primary(Box::new(Arc::clone(&primary)));
                let row = management.session_write_lock().await;
                let mut finish = Box::pin(custody.finish(&context));
                timeout(WAIT, async {
                    tokio::select! {
                        _ = finish.as_mut() => panic!("original release is held"),
                        () = broker.reached.notified() => {},
                    }
                })
                .await
                .unwrap();
                drop(finish);
                assert!(!notice.is_requested());
                broker.allow.notify_one();
                let mut retry = Box::pin(custody.finish(&context));
                timeout(WAIT, async {
                    tokio::select! {
                        _ = retry.as_mut() => panic!("captured session row remains held"),
                        () = notice.observer() => {},
                    }
                })
                .await
                .unwrap();
                drop(retry);
                assert_eq!(custody.session_registration, Some(old));
                assert_payload(custody.secondary_panics()[0].as_ref(), &payload);
                assert!(
                    custody.release_result().is_none(),
                    "poison is not a fabricated release result"
                );
                let mut cancelled = Box::pin(custody.finish(&context));
                pending_once(cancelled.as_mut()).await;
                drop(cancelled);
                assert_eq!(custody.secondary_panics().len(), 1);
                drop(row);
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(replacement)
                );
                assert_payload(custody.take_panic().unwrap().as_ref(), &primary);
                assert_payload(custody.take_panic().unwrap().as_ref(), &payload);
                let releases = actor
                    .log
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|entry| {
                        !entry.returned && matches!(entry.kind, CommandKind::ReleaseSession { .. })
                    })
                    .count();
                assert_eq!(releases, usize::from(delegate));
                let stored = StateMachine::new(actor.store().clone())
                    .session(&actor.namespace, &actor.entity, &id)
                    .unwrap()
                    .unwrap();
                assert_eq!(stored.lock.is_none(), delegate);
                wire.stop().await;
                drop(custody);
                drop(context);
                drop(broker);
                let snapshot = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    })
    .await
    .expect("actual broker calls stay inside their original Tokio task");
}

#[derive(Clone)]
struct RefusedReleaseBroker {
    actual: ActualBroker,
    unavailable: bool,
}
impl Broker for RefusedReleaseBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if self.unavailable && matches!(kind, CommandKind::ReleaseSession { .. }) {
            return Err(BrokerRejection::Unavailable(
                "controlled unavailable release".to_owned(),
            ));
        }
        self.actual.submit(namespace, entity, kind).await
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

struct ReportPanic(Arc<AtomicUsize>);
impl tracing::Subscriber for ReportPanic {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().module_path() == Some("protocol_amqp::listener::settlement::custody") {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(Arc::<str>::from("report-only receiving diagnostic"));
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn benign_cached_unstarted_refusal_and_report_only_results_never_notify() {
    tokio::spawn(async {
        for durable in [false, true] {
            for unavailable in [false, true] {
                let mut actor = Actor::new(durable, true);
                let id = SessionId::new("nonfatal-old-session").unwrap();
                actor.send("nonfatal-old-session", Some(id.clone()));
                let accepted = accept(&actor, &id);
                let old_hold = accepted.hold();
                actor.clock.set(accepted.lock.locked_until.as_millis());
                let replacement = accept(&actor, &id);
                let management = ConnectionManagement::new();
                let old_registration = register(&management, &actor, &old_hold).await;
                let replacement_registration =
                    register(&management, &actor, &replacement.hold()).await;
                let broker = RefusedReleaseBroker {
                    actual: actor.broker.as_ref().unwrap().clone(),
                    unavailable,
                };
                let context = context(&actor, broker, Arc::clone(&management));
                let mut wire = Wire::new(ReceiverSettleMode::Second).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let sender = wire.sender.take().unwrap();
                let reservation = sender.on_credit().await.unwrap();
                let delivery = actor.intent(CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: Some(replacement.hold()),
                });
                let CommandOutcome::Received(Some(delivery)) = delivery else {
                    panic!("current session delivery")
                };
                let before = actor.store().snapshot().unwrap();
                let mut custody =
                    ReceivingCustody::new(&context, Some(old_hold), Some(old_registration))
                        .with_retirement(Some(&notice));
                let unstarted = Arc::new(AtomicUsize::new(0));
                let invocations = Arc::clone(&unstarted);
                custody.intake = Some(ReceiveIntake::new(reservation, async move {
                    invocations.fetch_add(1, Ordering::SeqCst);
                    Err(BrokerRejection::Unavailable(
                        "unstarted intake double".to_owned(),
                    ))
                }));
                let native_invocations = Arc::clone(&unstarted);
                custody.transfer = Some(PendingTransfer::new(delivery, async move {
                    native_invocations.fetch_add(1, Ordering::SeqCst);
                    Err(amqp::EngineError::InvalidState(
                        "unstarted double".to_owned(),
                    ))
                }));
                custody
                    .workers
                    .spawn(None, async { Err(SettlementFailure::Unauthorized) });
                custody.workers.spawn(None, async {
                    Err(SettlementFailure::Engine(amqp::EngineError::RemoteDetached))
                });
                custody.workers.spawn(None, async {
                    Err(SettlementFailure::Engine(amqp::EngineError::RemoteClosed))
                });
                custody.workers.spawn(None, async {
                    Err(SettlementFailure::Engine(amqp::EngineError::Stopped))
                });
                custody.credit_release = Some(OriginalCleanup::new(async {
                    Err(amqp::EngineError::Stopped)
                }));
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let _other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
                let dispatch = tracing::Dispatch::new(ReportPanic(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                    .join()
                    .unwrap();
                let mut finish = Box::pin(custody.finish(&context));
                timeout(
                    WAIT,
                    poll_fn(|cx| {
                        tracing::dispatcher::with_default(&dispatch, || finish.as_mut().poll(cx))
                    }),
                )
                .await
                .unwrap();
                drop(finish);
                assert!(!notice.is_requested());
                assert_eq!(unstarted.load(Ordering::SeqCst), 0);
                assert_eq!(
                    diagnostics.load(Ordering::SeqCst),
                    1,
                    "report callback actually reached"
                );
                assert_eq!(custody.secondary_panics().len(), 1);
                assert_eq!(
                    custody.secondary_panics()[0]
                        .downcast_ref::<Arc<str>>()
                        .unwrap()
                        .as_ref(),
                    "report-only receiving diagnostic"
                );
                if unavailable {
                    assert!(matches!(
                        custody.release_result(),
                        Some(Err(BrokerRejection::Unavailable(_)))
                    ));
                } else {
                    assert!(matches!(
                        custody.release_result(),
                        Some(Err(BrokerRejection::Refused(
                            domain::BrokerError::SessionLockNotHeld { .. }
                        )))
                    ));
                }
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(replacement_registration)
                );
                assert_eq!(
                    StateMachine::new(actor.store().clone())
                        .session(&actor.namespace, &actor.entity, &id)
                        .unwrap()
                        .unwrap()
                        .lock,
                    Some(replacement.lock)
                );
                timeout(WAIT, custody.finish(&context)).await.unwrap();
                assert!(!notice.is_requested());
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);

                // This second owner retains a real, begun old-hold Receive
                // refusal after its raw broker return. Its gate is a qualified
                // result-delivery composition, not a new production frontier.
                let reservation = sender.on_credit().await.unwrap();
                let raw_seen = Arc::new(Notify::new());
                let allow_raw = Arc::new(Notify::new());
                let actual = actor.broker.as_ref().unwrap().clone();
                let namespace = actor.namespace.clone();
                let entity = actor.entity.clone();
                let rejected_hold = accepted.hold();
                let seen = Arc::clone(&raw_seen);
                let allowed = Arc::clone(&allow_raw);
                let mut refused =
                    ReceivingCustody::new(&context, None, None).with_retirement(Some(&notice));
                refused.intake = Some(ReceiveIntake::new(reservation, async move {
                    let raw = actual
                        .submit(
                            namespace,
                            entity,
                            CommandKind::Receive {
                                mode: ReceiveMode::PeekLock,
                                lock_duration_millis: None,
                                session: Some(rejected_hold),
                            },
                        )
                        .await;
                    seen.notify_one();
                    allowed.notified().await;
                    raw
                }));
                let mut observer = Box::pin(refused.intake.as_mut().unwrap().observe());
                timeout(WAIT, async {
                    tokio::select! {
                        _ = observer.as_mut() => panic!("raw refusal remains held"),
                        () = raw_seen.notified() => {},
                    }
                })
                .await
                .unwrap();
                drop(observer);
                allow_raw.notify_one();
                timeout(WAIT, refused.finish(&context)).await.unwrap();
                assert!(matches!(
                    refused.retired_receive,
                    Some(Err(BrokerRejection::Refused(
                        domain::BrokerError::SessionLockNotHeld { .. }
                    )))
                ));
                assert!(!notice.is_requested());
                assert!(refused.take_panic().is_none());
                assert_eq!(actor.store().snapshot().unwrap(), before);
                drop(refused);

                let mut cached_credit = OriginalCleanup::new(async {
                    Err(amqp::EngineError::InvalidState(
                        "previously consumed credit cleanup double".to_owned(),
                    ))
                });
                assert!(cached_credit.finish().await);
                let mut old_packet =
                    ReceivingCustody::new(&context, None, None).with_retirement(Some(&notice));
                old_packet.credit_release = Some(cached_credit);
                timeout(WAIT, old_packet.finish(&context)).await.unwrap();
                assert!(
                    !notice.is_requested(),
                    "a prior terminal credit packet is not new cleanup evidence"
                );
                drop(old_packet);

                // An error consumed through next() was already known by the
                // pump. Finishing cannot reinterpret its fabricated Stopped
                // completion or the retained raw JoinError as a new fault.
                let mut cached = SettlementWorkers::new();
                let old_payload: Arc<str> = Arc::from("previously captured worker panic");
                let task_payload = Arc::clone(&old_payload);
                cached.spawn(None, async move { std::panic::panic_any(task_payload) });
                let completed = cached.next().await.unwrap();
                assert!(matches!(
                    completed.result,
                    Err(SettlementFailure::Engine(amqp::EngineError::Stopped))
                ));
                assert_eq!(cached.failures().len(), 1);
                timeout(WAIT, cached.finish_with_retirement(Some(&notice)))
                    .await
                    .unwrap();
                assert!(!notice.is_requested());
                assert_payload(
                    cached.into_join_error().unwrap().into_panic().as_ref(),
                    &old_payload,
                );
                let _positive = wire.session_barrier(2).await;
                let snapshot = actor.store().snapshot().unwrap();
                assert_eq!(
                    snapshot, before,
                    "retired cleanup performs no implicit settlement or old-hold mutation"
                );
                wire.stop().await;
                drop(custody);
                drop(context);
                drop(sender);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), snapshot);
            }
        }
    })
    .await
    .expect("actual rejection evidence uses the original Tokio task");
}
