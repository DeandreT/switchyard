//! Injected owner compositions are distinct from actual whole-attachment paths.

use super::*;
use crate::listener::ConnectionRetirementRequest;
use crate::listener::attachments::serve_entity_attachment_with_retirement;

fn assert_cleanup_payload(custody: &AttachmentCustody<'_>, expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        custody
            .cleanup_payload_for_test()
            .unwrap()
            .downcast_ref::<Arc<str>>()
            .unwrap(),
        expected,
    ));
}

async fn ready_packet(
    actor: &Actor,
    broker: &impl Broker,
    wire: &mut PendingWire,
    session: &mut ServerSession,
    management: &Arc<ConnectionManagement>,
) -> crate::listener::attachments::EntityLink {
    let attach = wire.offer(session, Some(filter(false))).await;
    let ready = accept_entity_link(
        session,
        broker,
        &actor.namespace,
        "orders",
        attach,
        None,
        management,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        control(&mut wire.peer, CHANNEL).await,
        Performative::Attach(_)
    ));
    ready
}

#[tokio::test(flavor = "current_thread")]
async fn injected_attachment_cleanup_poison_is_cached_before_actual_native_and_registry_waits() {
    tokio::spawn(async {
        for durable in [false, true] {
            for captured in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-phase-poison", Some(session_id()));
                let accepted = accepted(&actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id()),
                    lock_duration_millis: None,
                }))
                .clone();
                let (mut wire, mut session, writes) = held_native_wire().await;
                let _release = NativeWriteRelease(Arc::clone(&writes));
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let endpoint = session
                    .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                    .await
                    .unwrap();
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let claim = management.claim_session(LINK, actor.entity.clone());
                let registration = management
                    .install_session(&claim, accepted.hold(), || true)
                    .await
                    .unwrap();
                let broker = ReleaseWitness::new(&actor);
                let payload = Arc::<str>::from("injected original attachment phase cleanup poison");
                let trigger = Arc::new(Notify::new());
                let polls = Arc::new(AtomicUsize::new(0));
                let mut custody =
                    AttachmentCustody::new(&session).with_retirement(captured.then_some(&notice));
                custody.grant = Some(Ok(CommandOutcome::SessionAccepted(Some(accepted.clone()))));
                custody.entity = Some(actor.entity.clone());
                custody.registration = Some(registration.clone());
                let original_trigger = Arc::clone(&trigger);
                let original_payload = Arc::clone(&payload);
                custody.handoff.begin_grant(PollWitness {
                    actual: Box::pin(async move {
                        original_trigger.notified().await;
                        std::panic::panic_any(original_payload)
                    }),
                    polls: Arc::clone(&polls),
                });
                pending_once(Box::pin(custody.handoff.observe()).as_mut()).await;
                assert_eq!(polls.load(Ordering::SeqCst), 1);
                writes.hold();
                let mut refusal = Box::pin(custody.start_refusal_for_test(
                    endpoint,
                    crate::listener::error_for(
                        amqp::AmqpError::IllegalState,
                        String::from("injected owner refusal"),
                    ),
                ));
                timeout(WAIT, async {
                    tokio::select! {
                        () = refusal.as_mut() => panic!("actual original refusal write is held"),
                        () = writes.reached() => {},
                    }
                })
                .await
                .unwrap();
                drop(refusal);
                let held = management.session_write_lock().await;
                let before = actor.store().snapshot().unwrap();
                trigger.notify_one();
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                assert_cleanup_payload(&custody, &payload);
                let packet = custody.packet.as_ref().unwrap();
                assert!(
                    packet.started && packet.retired && packet.panicked && packet.step.is_none()
                );
                assert_eq!(notice.is_requested(), captured);
                if captured {
                    timeout(WAIT, notice.observer()).await.unwrap();
                    notice.request();
                } else {
                    pending_once(Box::pin(notice.observer()).as_mut()).await;
                }
                for _ in 0..2 {
                    pending_once(
                        Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                    )
                    .await;
                    assert_cleanup_payload(&custody, &payload);
                    assert_eq!(
                        polls.load(Ordering::SeqCst),
                        2,
                        "poisoned original is never repolled"
                    );
                    assert_eq!(held.get(LINK), Some(&registration));
                    assert!(release_holds(&actor).is_empty());
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                    assert!(writes.held.load(Ordering::SeqCst));
                }
                if !captured {
                    wire.connection.stop();
                }
                timeout(WAIT, wire.connection.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                actor.gate.arm_put(domain::keys::session(
                    &actor.namespace,
                    &actor.entity,
                    &session_id(),
                ));
                drop(held);
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                actor.gate.reached(false).await;
                for _ in 0..2 {
                    pending_once(
                        Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                    )
                    .await;
                    assert_cleanup_payload(&custody, &payload);
                    assert_eq!(release_holds(&actor), vec![accepted.hold()]);
                }
                actor.gate.release_all();
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                assert_cleanup_payload(&custody, &payload);
                assert_eq!(polls.load(Ordering::SeqCst), 2);
                assert_eq!(
                    *broker.results.lock().unwrap(),
                    vec![(accepted.hold(), Ok(()))]
                );
                assert!(management.registered_session_owner(LINK).await.is_none());
                drop(custody);
                drop(broker);
                assert_reopened_release(&mut actor, &accepted.hold());
                let (mut replacement, _) = PendingWire::new().await;
                notice.request();
                let independent = replacement.begin(2).await;
                drop(independent);
                replacement.stop().await;
            }
        }
    })
    .await
    .expect("qualified owner observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_attachment_native_cleanup_error_notifies_before_held_registry_and_release() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("cleanup-native-stopped", Some(session_id()));
            let accepted = accepted(&actor.intent(CommandKind::AcceptSession {
                session_id: Some(session_id()), lock_duration_millis: None,
            })).clone();
            let (mut wire, mut session, writes) = held_native_wire().await;
            let _release = NativeWriteRelease(Arc::clone(&writes));
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let claim = management.claim_session(LINK, actor.entity.clone());
            let registration = management.install_session(&claim, accepted.hold(), || true).await.unwrap();
            let broker = ReleaseWitness::new(&actor);
            let mut custody = AttachmentCustody::new(&session).with_retirement(Some(&notice));
            custody.grant = Some(Ok(CommandOutcome::SessionAccepted(Some(accepted.clone()))));
            custody.entity = Some(actor.entity.clone());
            custody.registration = Some(registration.clone());
            custody.handoff.begin_accept(session.accept_attach(attach,
                crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64));
            writes.hold();
            let mut observed = Box::pin(custody.handoff.observe());
            timeout(WAIT, async { tokio::select! {
                result = observed.as_mut() => { let _ = result; panic!("actual acceptance write is held") },
                () = writes.reached() => {},
            }}).await.unwrap();
            drop(observed);
            let held = management.session_write_lock().await;
            let before = actor.store().snapshot().unwrap();
            assert!(!notice.is_requested());
            wire.connection.stop();
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            pending_once(Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut()).await;
            timeout(WAIT, notice.observer()).await.unwrap();
            assert!(matches!(custody.packet.as_ref().unwrap().step, Some(HandoffStep::Native(Err(EngineError::Stopped)))));
            let packet = custody.packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && !packet.panicked);
            assert_eq!(packet.phase, HandoffPhase::Native);
            assert!(matches!(custody.grant.as_ref(), Some(Ok(CommandOutcome::SessionAccepted(Some(actual)))) if actual.hold() == accepted.hold()));
            assert!(custody.cleanup_payload_for_test().is_none());
            for _ in 0..2 {
                pending_once(Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut()).await;
                assert!(writes.held.load(Ordering::SeqCst));
                assert_eq!(held.get(LINK), Some(&registration));
                assert!(release_holds(&actor).is_empty());
                assert_eq!(actor.store().snapshot().unwrap(), before);
                notice.request();
            }
            actor.gate.arm_put(domain::keys::session(&actor.namespace, &actor.entity, &session_id()));
            drop(held);
            pending_once(Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut()).await;
            actor.gate.reached(false).await;
            actor.gate.release_all();
            timeout(WAIT, custody.finish(&broker, &actor.namespace, &management)).await.unwrap();
            timeout(WAIT, custody.finish(&broker, &actor.namespace, &management)).await.unwrap();
            assert_eq!(release_holds(&actor), vec![accepted.hold()]);
            assert_eq!(*broker.results.lock().unwrap(), vec![(accepted.hold(), Ok(()))]);
            assert!(management.registered_session_owner(LINK).await.is_none());
            assert!(matches!(custody.packet.as_ref().unwrap().step, Some(HandoffStep::Native(Err(EngineError::Stopped)))));
            drop(custody);
            drop(broker);
            assert_reopened_release(&mut actor, &accepted.hold());
        }
    }).await.expect("actual native-result owner observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn injected_attachment_unregister_poison_keeps_ready_hold_and_replacement_across_retry() {
    tokio::spawn(async {
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-unregister-poison", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseWitness::new(&actor);
                let ready =
                    ready_packet(&actor, &broker, &mut wire, &mut session, &management).await;
                let accepted = ready.accepted.as_ref().unwrap().clone();
                let old_registration = ready.registration.clone().unwrap();
                let claim = management.claim_session(LINK, actor.entity.clone());
                let replacement = management
                    .install_session(&claim, accepted.hold(), || true)
                    .await
                    .unwrap();
                assert_ne!(old_registration, replacement);
                let held = management.session_write_lock().await;
                let payload = Arc::<str>::from("injected original conditional unregister poison");
                let polls = Arc::new(AtomicUsize::new(0));
                let original_management = Arc::clone(&management);
                let original_registration = old_registration.clone();
                let original_payload = Arc::clone(&payload);
                let mut custody = AttachmentCustody::new(&session).with_retirement(Some(&notice));
                custody.ready = Some(ready);
                custody.seed_unregister_for_test(PollWitness {
                    actual: Box::pin(async move {
                        if after {
                            original_management
                                .unregister_session(&original_registration)
                                .await;
                        }
                        std::panic::panic_any(original_payload)
                    }),
                    polls: Arc::clone(&polls),
                });
                actor.gate.arm_put(domain::keys::session(
                    &actor.namespace,
                    &actor.entity,
                    &session_id(),
                ));
                let before = actor.store().snapshot().unwrap();
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                if after {
                    assert!(!notice.is_requested());
                    assert!(custody.cleanup_payload_for_test().is_none());
                    assert!(release_holds(&actor).is_empty());
                    assert_eq!(held.get(LINK), Some(&replacement));
                } else {
                    timeout(WAIT, notice.observer()).await.unwrap();
                    assert_cleanup_payload(&custody, &payload);
                }
                drop(held);
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                actor.gate.reached(false).await;
                timeout(WAIT, notice.observer()).await.unwrap();
                assert_cleanup_payload(&custody, &payload);
                let terminal_polls = polls.load(Ordering::SeqCst);
                for _ in 0..2 {
                    pending_once(
                        Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                    )
                    .await;
                    assert_cleanup_payload(&custody, &payload);
                    assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
                    assert_eq!(
                        custody
                            .ready
                            .as_ref()
                            .unwrap()
                            .accepted
                            .as_ref()
                            .unwrap()
                            .hold(),
                        accepted.hold()
                    );
                    assert_eq!(
                        management.registered_session_owner(LINK).await,
                        Some(replacement.clone())
                    );
                    assert_eq!(release_holds(&actor), vec![accepted.hold()]);
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                    notice.request();
                }
                actor.gate.release(false);
                actor.gate.reached(true).await;
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                assert_cleanup_payload(&custody, &payload);
                actor.gate.release(true);
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
                assert_eq!(
                    *broker.results.lock().unwrap(),
                    vec![(accepted.hold(), Ok(()))]
                );
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(replacement.clone())
                );
                management.unregister_session(&old_registration).await;
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(replacement)
                );
                drop(custody);
                drop(broker);
                timeout(WAIT, wire.connection.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                assert_reopened_release(&mut actor, &accepted.hold());
            }
        }
    })
    .await
    .expect("qualified unregister owner observer joined");
}

#[derive(Clone)]
struct ReleaseFaultBroker {
    actual: ReleaseWitness,
    attempts: Arc<AtomicUsize>,
    payload: Arc<str>,
    after: bool,
}

impl Broker for ReleaseFaultBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(&kind, CommandKind::ReleaseSession { .. }) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if !self.after {
                std::panic::panic_any(Arc::clone(&self.payload));
            }
            let _raw = self.actual.submit(namespace, entity, kind).await;
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
async fn attachment_release_original_poison_notifies_without_retry_or_installed_hold_rollback() {
    tokio::spawn(async {
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-release-poison", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseFaultBroker {
                    actual: ReleaseWitness::new(&actor),
                    attempts: Arc::new(AtomicUsize::new(0)),
                    payload: Arc::from("injected original release submit poison"),
                    after,
                };
                let ready =
                    ready_packet(&actor, &broker, &mut wire, &mut session, &management).await;
                let accepted = ready.accepted.as_ref().unwrap().clone();
                let before = actor.store().snapshot().unwrap();
                let mut custody = AttachmentCustody::new(&session).with_retirement(Some(&notice));
                custody.ready = Some(ready);
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                timeout(WAIT, notice.observer()).await.unwrap();
                assert_cleanup_payload(&custody, &broker.payload);
                assert!(management.registered_session_owner(LINK).await.is_none());
                for _ in 0..2 {
                    timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                        .await
                        .unwrap();
                    assert_cleanup_payload(&custody, &broker.payload);
                    assert_eq!(broker.attempts.load(Ordering::SeqCst), 1);
                    notice.request();
                }
                if after {
                    assert_eq!(
                        *broker.actual.results.lock().unwrap(),
                        vec![(accepted.hold(), Ok(()))]
                    );
                    assert_eq!(release_holds(&actor), vec![accepted.hold()]);
                } else {
                    assert!(broker.actual.results.lock().unwrap().is_empty());
                    assert!(release_holds(&actor).is_empty());
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                    assert_eq!(stored_grant(&actor), accepted);
                }
                drop(custody);
                drop(broker);
                timeout(WAIT, wire.connection.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                if after {
                    assert_reopened_release(&mut actor, &accepted.hold());
                } else {
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                    assert_eq!(stored_grant(&actor), accepted);
                }
            }
        }
    })
    .await
    .expect("original-submit poison observer joined");
}

#[derive(Clone)]
struct UnavailableReleaseBroker {
    actual: ReleaseWitness,
    attempts: Arc<AtomicUsize>,
}

impl Broker for UnavailableReleaseBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        if matches!(&kind, CommandKind::ReleaseSession { .. }) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(BrokerRejection::Unavailable(String::from(
                "injected original release unavailable",
            )));
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
async fn captured_attachment_release_rejections_do_not_notify_or_destroy_replacements() {
    tokio::spawn(async {
        for durable in [false, true] {
            for unavailable in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-release-rejection", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let actual = ReleaseWitness::new(&actor);
                let ready = ready_packet(&actor, &actual, &mut wire, &mut session, &management).await;
                let old = ready.accepted.as_ref().unwrap().clone();
                let old_registration = ready.registration.clone().unwrap();
                actor.clock.set(old.lock.locked_until.as_millis());
                let replacement = accepted(&actor.intent(CommandKind::AcceptSession {
                    session_id: Some(session_id()), lock_duration_millis: None,
                })).clone();
                let claim = management.claim_session(LINK, actor.entity.clone());
                let registration = management.install_session(&claim, replacement.hold(), || true).await.unwrap();
                let before = actor.store().snapshot().unwrap();
                let broker = UnavailableReleaseBroker { actual: actual.clone(), attempts: Arc::new(AtomicUsize::new(0)) };
                let mut custody = AttachmentCustody::new(&session).with_retirement(Some(&notice));
                custody.ready = Some(ready);
                if unavailable {
                    timeout(WAIT, custody.finish(&broker, &actor.namespace, &management)).await.unwrap();
                    timeout(WAIT, custody.finish(&broker, &actor.namespace, &management)).await.unwrap();
                    assert_eq!(broker.attempts.load(Ordering::SeqCst), 1);
                    assert!(actual.results.lock().unwrap().is_empty());
                } else {
                    timeout(WAIT, custody.finish(&actual, &actor.namespace, &management)).await.unwrap();
                    timeout(WAIT, custody.finish(&actual, &actor.namespace, &management)).await.unwrap();
                    let results = actual.results.lock().unwrap().clone();
                    assert_eq!(results.len(), 1);
                    assert_eq!(results[0].0, old.hold());
                    assert!(matches!(&results[0].1, Err(BrokerRejection::Refused(BrokerError::SessionLockNotHeld { session_id: actual })) if actual == &session_id()));
                }
                assert!(custody.cleanup_payload_for_test().is_none());
                assert!(!notice.is_requested());
                pending_once(Box::pin(notice.observer()).as_mut()).await;
                management.unregister_session(&old_registration).await;
                assert_eq!(management.registered_session_owner(LINK).await, Some(registration));
                assert_eq!(actor.store().snapshot().unwrap(), before);
                let independent = wire.begin(2).await;
                drop(independent);
                drop(custody);
                drop(broker);
                drop(actual);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
                assert_eq!(stored_grant(&actor), replacement);
                wire.stop().await;
            }
        }
    }).await.expect("nonfatal original release result observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_native_and_primary_priority_survive_cleanup_poison_and_report_only_faults() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-native-priority", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                wire.detach().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseFaultBroker {
                    actual: ReleaseWitness::new(&actor), attempts: Arc::new(AtomicUsize::new(0)),
                    payload: Arc::from("injected secondary original release poison"), after,
                };
                assert!(matches!(timeout(WAIT, AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach, None, &management, Some(&notice)))
                    .catch_unwind()).await.unwrap(), Ok(Ok(()))), "benign actual detached result keeps priority over cleanup poison");
                timeout(WAIT, notice.observer()).await.unwrap();
                assert_eq!(broker.attempts.load(Ordering::SeqCst), 1);
                assert!(management.registered_session_owner(LINK).await.is_none());
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                drop(broker);
                if after { let hold = release_holds(&actor)[0].clone(); assert_reopened_release(&mut actor, &hold); }
                else { let before = actor.store().snapshot().unwrap(); actor.reopen(); assert_eq!(actor.store().snapshot().unwrap(), before); }
            }
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-stopped-priority", Some(session_id()));
                let (mut wire, mut session, writes) = held_native_wire().await;
                let _release = NativeWriteRelease(Arc::clone(&writes));
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                writes.hold();
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseFaultBroker {
                    actual: ReleaseWitness::new(&actor), attempts: Arc::new(AtomicUsize::new(0)),
                    payload: Arc::from("injected release poison behind actual stopped result"), after,
                };
                let mut original = Box::pin(AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach, None, &management, Some(&notice))).catch_unwind());
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("actual original native write is held") },
                    () = writes.reached() => {},
                }}).await.unwrap();
                assert!(!notice.is_requested());
                wire.connection.stop();
                assert!(matches!(timeout(WAIT, original.as_mut()).await.unwrap(), Ok(Err(EngineError::Stopped))),
                    "actual original native error precedes secondary release poison");
                assert!(notice.is_requested());
                assert_eq!(broker.attempts.load(Ordering::SeqCst), 1);
                assert!(writes.held.load(Ordering::SeqCst));
                drop(original);
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                let before = actor.store().snapshot().unwrap();
                drop(broker);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
            for after in [false, true] {
                let mut actor = Actor::new(durable, true);
                actor.send("cleanup-primary-priority", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseFaultBroker {
                    actual: ReleaseWitness::new(&actor), attempts: Arc::new(AtomicUsize::new(0)),
                    payload: Arc::from("injected release poison behind raw primary"), after,
                };
                let fault = PumpFault::new(PumpPoint::GrantResult);
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(serve_entity_attachment_with_retirement(&session, &broker,
                        &actor.namespace, "orders", attach, None, &management, Some(&notice))).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("actual grant packet is retained") },
                    () = fault.reached.notified() => {},
                }}).await.unwrap();
                grant_started_and_returned(&actor);
                assert!(!notice.is_requested());
                fault.trigger.notify_one();
                assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                assert!(notice.is_requested());
                assert_eq!(broker.attempts.load(Ordering::SeqCst), 1);
                drop(original);
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                let before = actor.store().snapshot().unwrap();
                drop(broker);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
            let mut actor = Actor::new(durable, true);
            actor.send("cleanup-report-only", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            wire.detach().await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
            let mut original = Box::pin(AssertUnwindSafe(serve_entity_attachment_with_retirement(
                &session, &broker, &actor.namespace, "orders", attach, None, &management, Some(&notice))).catch_unwind());
            let mut observed = Box::pin(std::future::poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
            }));
            assert!(matches!(timeout(WAIT, observed.as_mut()).await.unwrap(), Ok(Ok(()))));
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1, "caught report callback was actually reached");
            assert!(!notice.is_requested());
            pending_once(Box::pin(notice.observer()).as_mut()).await;
            drop(observed);
            drop(original);
            let independent = wire.begin(2).await;
            drop(independent);
            let hold = release_holds(&actor)[0].clone();
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;
        }
    }).await.expect("actual priority and report-only observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_cached_attachment_native_error_is_not_new_cleanup_notice() {
    tokio::spawn(async {
        for cached in [false, true] {
            for captured in [false, true] {
                for remote_detached in [false, true] {
                    let actor = Actor::new(false, true);
                    let broker = ReleaseWitness::new(&actor);
                    let management = ConnectionManagement::new();
                    let (mut wire, mut session, writes) = held_native_wire().await;
                    let _release = NativeWriteRelease(Arc::clone(&writes));
                    let attach = wire.offer(&mut session, None).await;
                    let notice = ConnectionRetirementRequest::capture(&wire.connection);
                    if remote_detached {
                        wire.detach().await;
                    } else {
                        writes.hold();
                    }
                    let polls = Arc::new(AtomicUsize::new(0));
                    let mut custody = AttachmentCustody::new(&session)
                        .with_retirement(captured.then_some(&notice));
                    custody.handoff.begin_accept(PollWitness {
                        actual: Box::pin(session.accept_attach(
                            attach,
                            crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                        )),
                        polls: Arc::clone(&polls),
                    });
                    let mut observed = Box::pin(custody.handoff.observe());
                    if remote_detached {
                        pending_once(observed.as_mut()).await;
                    } else {
                        timeout(WAIT, async {
                            tokio::select! {
                                result = observed.as_mut() => {
                                    let _ = result;
                                    panic!("the original acceptance write is held")
                                },
                                () = writes.reached() => {},
                            }
                        })
                        .await
                        .unwrap();
                    }
                    drop(observed);
                    let barrier = if remote_detached {
                        Some(wire.native_fifo_after_accept(false).await)
                    } else {
                        wire.connection.stop();
                        timeout(WAIT, wire.connection.shutdown())
                            .await
                            .unwrap()
                            .unwrap();
                        None
                    };
                    let expected_error = |step: &HandoffStep| match step {
                        HandoffStep::Native(Err(EngineError::RemoteDetached)) => remote_detached,
                        HandoffStep::Native(Err(EngineError::Stopped)) => !remote_detached,
                        _ => false,
                    };
                    if cached {
                        // This qualified composition omits the pump's synchronous capture.
                        let step = timeout(WAIT, custody.handoff.observe())
                            .await
                            .unwrap()
                            .unwrap();
                        assert!(expected_error(step));
                        let address = std::ptr::from_ref(step) as usize;
                        let terminal_polls = polls.load(Ordering::SeqCst);
                        for _ in 0..2 {
                            let old = timeout(WAIT, custody.handoff.observe())
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(std::ptr::from_ref(old) as usize, address);
                            assert!(expected_error(old));
                            assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
                        }
                    }
                    assert!(!notice.is_requested());
                    let before_finish = polls.load(Ordering::SeqCst);
                    timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                        .await
                        .unwrap();
                    let terminal_polls = polls.load(Ordering::SeqCst);
                    assert_eq!(terminal_polls, before_finish + usize::from(!cached));
                    let packet = custody.packet.as_ref().unwrap();
                    assert!(packet.started && packet.retired && !packet.panicked);
                    assert_eq!(packet.phase, HandoffPhase::Native);
                    let step = packet.step.as_ref().unwrap();
                    assert!(expected_error(step));
                    let address = std::ptr::from_ref(step) as usize;
                    let notified = captured && !cached && !remote_detached;
                    assert_eq!(notice.is_requested(), notified);
                    if notified {
                        timeout(WAIT, notice.observer()).await.unwrap();
                        notice.request();
                    } else {
                        pending_once(Box::pin(notice.observer()).as_mut()).await;
                    }
                    for _ in 0..2 {
                        timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                            .await
                            .unwrap();
                        let step = custody.packet.as_ref().unwrap().step.as_ref().unwrap();
                        assert_eq!(std::ptr::from_ref(step) as usize, address);
                        assert!(expected_error(step));
                        assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
                        assert_eq!(notice.is_requested(), notified);
                    }
                    assert!(custody.cleanup_payload_for_test().is_none());
                    assert!(release_holds(&actor).is_empty());
                    assert!(management.registered_session_owner(LINK).await.is_none());
                    drop(custody);
                    drop(broker);
                    if remote_detached {
                        let independent = wire.begin(3).await;
                        drop(independent);
                        drop(barrier);
                        wire.stop().await;
                    }
                }
            }
        }
        for captured in [false, true] {
            let actor = Actor::new(false, true);
            let broker = ReleaseWitness::new(&actor);
            let management = ConnectionManagement::new();
            let (mut wire, session) = PendingWire::new().await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let payload = Arc::<str>::from("qualified previously poisoned native original");
            let original_payload = Arc::clone(&payload);
            let polls = Arc::new(AtomicUsize::new(0));
            let mut custody =
                AttachmentCustody::new(&session).with_retirement(captured.then_some(&notice));
            // This is injected original-poll poison, not a native engine panic.
            custody.handoff.begin_accept(PollWitness {
                actual: Box::pin(async move { std::panic::panic_any(original_payload) }),
                polls: Arc::clone(&polls),
            });
            let original_panic = match AssertUnwindSafe(custody.handoff.observe())
                .catch_unwind()
                .await
            {
                Err(payload) => payload,
                Ok(_) => panic!("the original observer caught its poison"),
            };
            assert!(Arc::ptr_eq(
                original_panic.downcast_ref::<Arc<str>>().unwrap(),
                &payload,
            ));
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            for _ in 0..3 {
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                let packet = custody.packet.as_ref().unwrap();
                assert!(
                    packet.started && packet.retired && packet.panicked && packet.step.is_none()
                );
                assert_eq!(packet.phase, HandoffPhase::Native);
                assert!(custody.cleanup_payload_for_test().is_none());
                assert_eq!(polls.load(Ordering::SeqCst), 1);
                assert!(!notice.is_requested());
                pending_once(Box::pin(notice.observer()).as_mut()).await;
                assert!(Arc::ptr_eq(
                    original_panic.downcast_ref::<Arc<str>>().unwrap(),
                    &payload,
                ));
            }
            assert!(release_holds(&actor).is_empty());
            drop(custody);
            drop(broker);
            let independent = wire.begin(2).await;
            drop(independent);
            wire.stop().await;
        }
    })
    .await
    .expect("qualified cached native-result observer joined");
}
