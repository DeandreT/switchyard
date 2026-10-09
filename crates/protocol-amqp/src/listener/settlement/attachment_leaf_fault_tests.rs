//! Captured attachment notice and native Stop, not parent/family join evidence.

use super::*;
use crate::listener::ConnectionRetirementRequest;
use crate::listener::attachments::serve_entity_attachment_with_retirement;
use crate::listener::settlement::test_support::CommitGate;

#[tokio::test(flavor = "current_thread")]
async fn attachment_notice_precedes_hidden_original_grant_before_and_after_apply() {
    tokio::spawn(async {
        for durable in [false, true] {
            for next in [false, true] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    actor.send("notice-hidden-grant", Some(session_id()));
                    actor.gate.arm_put(domain::keys::session(
                        &actor.namespace, &actor.entity, &session_id()));
                    let (mut wire, mut session) = PendingWire::new().await;
                    let attach = wire.offer(&mut session, Some(filter(next))).await;
                    let notice = ConnectionRetirementRequest::capture(&wire.connection);
                    let management = ConnectionManagement::new();
                    let broker = ReleaseWitness::new(&actor);
                    let fault = PumpFault::new(PumpPoint::Grant);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_entity_attachment_with_retirement(
                            &session, &broker, &actor.namespace, "orders", attach,
                            None, &management, Some(&notice))).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("original grant is held") },
                        () = actor.gate.reached(false) => {},
                    }}).await.unwrap();
                    timeout(WAIT, fault.reached.notified()).await.unwrap();
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    assert!(!notice.is_requested());
                    pending_once(Box::pin(notice.observer()).as_mut()).await;
                    fault.trigger.notify_one();
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("notice precedes original grant drain") },
                        () = notice.observer() => {},
                    }}).await.unwrap();
                    assert!(notice.is_requested());
                    assert_eq!(grant_count(&actor), 1);
                    assert!(release_holds(&actor).is_empty());
                    timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                    notice.request();
                    timeout(WAIT, notice.observer()).await.unwrap();
                    if !after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    let hold = stored_grant(&actor).hold();
                    actor.gate.release(true);
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    grant_started_and_returned(&actor);
                    assert_eq!(release_holds(&actor), vec![hold.clone()]);
                    assert_eq!(*broker.results.lock().unwrap(), vec![(hold.clone(), Ok(()))]);
                    assert!(management.registered_session_owner(LINK).await.is_none());
                    let (mut independent, _) = PendingWire::new().await;
                    notice.request();
                    let independent_session = independent.begin(2).await;
                    drop(independent_session);
                    independent.stop().await;
                    drop(broker);
                    assert_reopened_release(&mut actor, &hold);
                }
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_notice_interrupts_begun_native_acceptance_without_releasing_writer() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("notice-native-grant", Some(session_id()));
            let (mut wire, mut session, writes) = held_native_wire().await;
            let _release = NativeWriteRelease(Arc::clone(&writes));
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            writes.hold();
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Native);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach,
                    None, &management, Some(&notice))).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("original native acceptance is held") },
                () = writes.reached() => {},
            }}).await.unwrap();
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            let hold = stored_grant(&actor).hold();
            assert!(!notice.is_requested());
            fault.trigger.notify_one();
            // The notice itself is the only Stop. No endpoint is fabricated
            // from the original native Err(Stopped) produced during drain.
            let result = timeout(WAIT, original.as_mut()).await.unwrap();
            timeout(WAIT, notice.observer()).await.unwrap();
            assert_outer(result, &fault);
            assert!(writes.held.load(Ordering::SeqCst));
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            drop(original);
            grant_started_and_returned(&actor);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            assert!(management.registered_session_owner(LINK).await.is_none());
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_notice_keeps_cached_packets_through_held_original_release() {
    tokio::spawn(async {
        for durable in [false, true] {
            for point in [PumpPoint::GrantResult, PumpPoint::NativeResult, PumpPoint::Registry,
                PumpPoint::Installed, PumpPoint::Ready, PumpPoint::EntryPrepared] {
                let mut actor = Actor::new(durable, true);
                actor.send("notice-cached-grant", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let held_registry = if point == PumpPoint::Registry {
                    Some(management.session_write_lock().await)
                } else {
                    None
                };
                let broker = ReleaseWitness::new(&actor);
                let fault = PumpFault::new(point);
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(serve_entity_attachment_with_retirement(
                        &session, &broker, &actor.namespace, "orders", attach,
                        None, &management, Some(&notice))).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("original packet is held") },
                    () = fault.reached.notified() => {},
                }}).await.unwrap();
                let hold = stored_grant(&actor).hold();
                if point != PumpPoint::GrantResult {
                    let Performative::Attach(echo) = control(&mut wire.peer, CHANNEL).await else {
                        panic!("original native attach")
                    };
                    assert_eq!(read_session_filter(echo.source.as_ref()).unwrap(),
                        SessionRequest::Named(hold.session_id.clone()));
                }
                if matches!(point, PumpPoint::Installed | PumpPoint::Ready | PumpPoint::EntryPrepared) {
                    assert_eq!(management.registered_session(LINK).await,
                        Some((actor.entity.clone(), hold.clone())));
                }
                actor.gate.arm_put(domain::keys::session(
                    &actor.namespace, &actor.entity, &session_id()));
                assert!(!notice.is_requested());
                fault.trigger.notify_one();
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("original release is held") },
                    () = actor.gate.reached(false) => {},
                }}).await.unwrap();
                timeout(WAIT, notice.observer()).await.unwrap();
                assert_eq!(release_holds(&actor), vec![hold.clone()]);
                if let Some(held) = &held_registry {
                    assert!(held.is_empty());
                }
                drop(held_registry);
                assert!(management.registered_session_owner(LINK).await.is_none());
                for _ in 0..2 {
                    pending_once(original.as_mut()).await;
                    timeout(WAIT, notice.observer()).await.unwrap();
                    assert_eq!(release_holds(&actor), vec![hold.clone()]);
                }
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                actor.gate.release_all();
                assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                drop(original);
                grant_started_and_returned(&actor);
                assert_eq!(*broker.results.lock().unwrap(), vec![(hold.clone(), Ok(()))]);
                drop(broker);
                assert_reopened_release(&mut actor, &hold);
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_notice_precedes_conditional_unregister_and_keeps_replacement_hold() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("notice-replacement-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Ready);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach,
                    None, &management, Some(&notice))).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("original ready packet is held") },
                () = fault.reached.notified() => {},
            }}).await.unwrap();
            assert!(matches!(control(&mut wire.peer, CHANNEL).await, Performative::Attach(_)));
            let old = stored_grant(&actor);
            let old_registration = management.registered_session_owner(LINK).await.unwrap();
            actor.clock.set(old.lock.locked_until.as_millis());
            let replacement = accepted(&actor.intent(CommandKind::AcceptSession {
                session_id: Some(session_id()), lock_duration_millis: None,
            })).clone();
            assert_ne!(old.hold().token, replacement.hold().token);
            let claim = management.claim_session(LINK, actor.entity.clone());
            let registration = management.install_session(&claim, replacement.hold(), || true)
                .await.unwrap();
            let held = management.session_write_lock().await;
            let before = actor.store().snapshot().unwrap();
            fault.trigger.notify_one();
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("captured unregister is held") },
                () = notice.observer() => {},
            }}).await.unwrap();
            for _ in 0..2 {
                // Drop borrowed observations, never the original helper owner.
                pending_once(original.as_mut()).await;
                timeout(WAIT, notice.observer()).await.unwrap();
                assert!(release_holds(&actor).is_empty());
                assert_eq!(held.get(LINK), Some(&registration));
            }
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            drop(held);
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            drop(original);
            assert_eq!(release_holds(&actor), vec![old.hold()]);
            let results = broker.results.lock().unwrap().clone();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].0, old.hold());
            assert!(matches!(&results[0].1,
                Err(BrokerRejection::Refused(BrokerError::SessionLockNotHeld { session_id: actual }))
                    if actual == &session_id()));
            management.unregister_session(&old_registration).await;
            assert_eq!(management.registered_session_owner(LINK).await, Some(registration));
            assert_eq!(actor.store().snapshot().unwrap(), before);
            grant_started_and_returned(&actor);
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
            assert_eq!(stored_grant(&actor), replacement);
        }
    }).await.expect("original observer joined");
}

struct GrantCleanupBroker {
    actual: ReleaseWitness,
    gate: Arc<CommitGate>,
    key: Vec<u8>,
    clones: Arc<AtomicUsize>,
    ordinal: Option<usize>,
    payload: Arc<str>,
}

impl GrantCleanupBroker {
    fn new(actor: &Actor, ordinal: Option<usize>) -> Self {
        Self {
            actual: ReleaseWitness::new(actor),
            gate: Arc::clone(&actor.gate),
            key: domain::keys::session(&actor.namespace, &actor.entity, &session_id()),
            clones: Arc::new(AtomicUsize::new(0)),
            ordinal,
            payload: Arc::from("original receiving-entry clone fault"),
        }
    }
}

impl Clone for GrantCleanupBroker {
    fn clone(&self) -> Self {
        let ordinal = self.clones.fetch_add(1, Ordering::SeqCst) + 1;
        if self.ordinal == Some(ordinal) {
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        Self {
            actual: self.actual.clone(),
            gate: Arc::clone(&self.gate),
            key: self.key.clone(),
            clones: Arc::clone(&self.clones),
            ordinal: self.ordinal,
            payload: Arc::clone(&self.payload),
        }
    }
}

impl Broker for GrantCleanupBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let grant = matches!(&kind, CommandKind::AcceptSession { .. });
        let result = self.actual.submit(namespace, entity, kind).await;
        if grant && matches!(&result, Ok(CommandOutcome::SessionAccepted(Some(_)))) {
            // The actual grant has returned. Gate only its original cleanup.
            self.gate.arm_put(self.key.clone());
        }
        result
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
async fn attachment_notice_catches_all_receiving_entry_clones_before_original_release() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            for ordinal in 1..=3 {
                let mut actor = Actor::new(durable, true);
                actor.send("notice-construction-grant", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = GrantCleanupBroker::new(&actor, Some(ordinal));
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
                let mut original = Box::pin(AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach,
                    None, &management, Some(&notice))).catch_unwind());
                let mut observed = Box::pin(std::future::poll_fn(|context| {
                    tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
                }));
                timeout(WAIT, async { tokio::select! {
                    result = observed.as_mut() => { let _ = result; panic!("original clone-fault cleanup is held") },
                    () = actor.gate.reached(false) => {},
                }}).await.unwrap();
                timeout(WAIT, notice.observer()).await.unwrap();
                let hold = stored_grant(&actor).hold();
                assert_eq!(broker.clones.load(Ordering::SeqCst), ordinal);
                assert_eq!(release_holds(&actor), vec![hold.clone()]);
                assert!(management.registered_session_owner(LINK).await.is_none());
                assert_eq!(diagnostics.load(Ordering::SeqCst), 0);
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                for _ in 0..2 {
                    pending_once(observed.as_mut()).await;
                    assert_eq!(release_holds(&actor), vec![hold.clone()]);
                }
                actor.gate.release_all();
                let result = timeout(WAIT, observed.as_mut()).await.unwrap();
                let payload = result.expect_err("original entry clone panic resumes");
                assert!(Arc::ptr_eq(payload.downcast_ref::<Arc<str>>().unwrap(), &broker.payload));
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
                drop(observed);
                drop(original);
                grant_started_and_returned(&actor);
                assert_eq!(*broker.actual.results.lock().unwrap(), vec![(hold.clone(), Ok(()))]);
                drop(broker);
                assert_reopened_release(&mut actor, &hold);
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_active_native_error_notifies_before_release_and_precedes_diagnostics() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("notice-invalid-native-identity", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let mut attach = wire.offer(&mut session, Some(filter(false))).await;
            // Controlled internal misuse of the real native API, not ordinary
            // wire admission or evidence of native writer admission.
            attach.handle = HANDLE + 99;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = GrantCleanupBroker::new(&actor, None);
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
            let mut original = Box::pin(AssertUnwindSafe(serve_entity_attachment_with_retirement(
                &session, &broker, &actor.namespace, "orders", attach,
                None, &management, Some(&notice))).catch_unwind());
            let mut observed = Box::pin(std::future::poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
            }));
            timeout(WAIT, async { tokio::select! {
                result = observed.as_mut() => { let _ = result; panic!("original active-error cleanup is held") },
                () = actor.gate.reached(false) => {},
            }}).await.unwrap();
            timeout(WAIT, notice.observer()).await.unwrap();
            let hold = stored_grant(&actor).hold();
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            assert_eq!(diagnostics.load(Ordering::SeqCst), 0);
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            actor.gate.release_all();
            let result = timeout(WAIT, observed.as_mut()).await.unwrap();
            let error = match result {
                Ok(Err(error)) => error,
                _ => panic!("captured active error precedes secondary diagnostics"),
            };
            assert!(matches!(error, EngineError::InvalidState(ref cause)
                if cause == "attach was not received by this session"));
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
            drop(observed);
            drop(original);
            grant_started_and_returned(&actor);
            assert_eq!(*broker.actual.results.lock().unwrap(), vec![(hold.clone(), Ok(()))]);
            assert!(management.registered_session_owner(LINK).await.is_none());
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_notice_before_first_grant_poll_starts_no_retired_work() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("notice-unstarted-grant", Some(session_id()));
            let before = actor.store().snapshot().unwrap();
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Prepared);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session, &broker, &actor.namespace, "orders", attach,
                    None, &management, Some(&notice))).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("configured unstarted grant is held") },
                () = fault.reached.notified() => {},
            }}).await.unwrap();
            assert!(!notice.is_requested());
            fault.trigger.notify_one();
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            timeout(WAIT, notice.observer()).await.unwrap();
            timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
            drop(original);
            assert!(actor.log.lock().unwrap().is_empty());
            assert!(broker.results.lock().unwrap().is_empty());
            assert!(management.registered_session_owner(LINK).await.is_none());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }).await.expect("original observer joined");
}

type SubmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CommandOutcome, BrokerRejection>> + Send + 'a>>;

struct GrantPollPoison<'a> {
    actual: SubmissionFuture<'a>,
    poison: Option<Arc<str>>,
    polls: Arc<AtomicUsize>,
}

impl Future for GrantPollPoison<'_> {
    type Output = Result<CommandOutcome, BrokerRejection>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(payload) = &self.poison {
            self.polls.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(Arc::clone(payload));
        }
        self.actual.as_mut().poll(context)
    }
}

#[derive(Clone)]
struct GrantPoisonBroker {
    actual: ReleaseWitness,
    polls: Arc<AtomicUsize>,
    payload: Arc<str>,
}

impl Broker for GrantPoisonBroker {
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        let poison = matches!(&kind, CommandKind::AcceptSession { .. });
        let actual: SubmissionFuture<'_> = if poison {
            Box::pin(std::future::pending())
        } else {
            Box::pin(self.actual.submit(namespace, entity, kind))
        };
        GrantPollPoison {
            actual,
            poison: poison.then(|| Arc::clone(&self.payload)),
            polls: Arc::clone(&self.polls),
        }
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
async fn attachment_original_poll_poison_notifies_once_without_repoll_or_fabricated_grant() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("notice-poison-primitive", Some(session_id()));
            let before = actor.store().snapshot().unwrap();
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = GrantPoisonBroker {
                actual: ReleaseWitness::new(&actor),
                polls: Arc::new(AtomicUsize::new(0)),
                payload: Arc::from("original grant-poll poison primitive"),
            };
            // Qualified original-poll poison; no healthy actual grant admission.
            let result = timeout(
                WAIT,
                AssertUnwindSafe(serve_entity_attachment_with_retirement(
                    &session,
                    &broker,
                    &actor.namespace,
                    "orders",
                    attach,
                    None,
                    &management,
                    Some(&notice),
                ))
                .catch_unwind(),
            )
            .await
            .unwrap();
            let payload = result.expect_err("original poison resumes");
            assert!(Arc::ptr_eq(
                payload.downcast_ref::<Arc<str>>().unwrap(),
                &broker.payload
            ));
            timeout(WAIT, notice.observer()).await.unwrap();
            timeout(WAIT, wire.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
            notice.request();
            timeout(WAIT, notice.observer()).await.unwrap();
            assert_eq!(broker.polls.load(Ordering::SeqCst), 1);
            assert!(actor.log.lock().unwrap().is_empty());
            assert!(broker.actual.results.lock().unwrap().is_empty());
            assert!(management.registered_session_owner(LINK).await.is_none());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    })
    .await
    .expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_healthy_refused_and_ended_exits_do_not_request_stop() {
    tokio::spawn(async {
        for durable in [false, true] {
            for case in 0..6 {
                let requires_session = case == 3;
                let actor = Actor::new(durable, requires_session);
                let (mut wire, mut session) = PendingWire::new().await;
                let offered_filter = match case {
                    3 => Some(filter(true)),
                    4 => Some(filter(false)),
                    _ => None,
                };
                let attach = wire.offer(&mut session, offered_filter).await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let authorization = (case == 2).then(wrong_permission);
                if case == 5 {
                    wire.end().await;
                    assert!(session.is_ended());
                }
                let result = timeout(WAIT, serve_entity_attachment_with_retirement(
                    &session, actor.broker.as_ref().unwrap(), &actor.namespace,
                    if case == 1 { "" } else { "orders" }, attach,
                    authorization.as_ref(), &management, Some(&notice))).await.unwrap();
                assert!(result.is_ok());
                if case != 5 {
                    assert!(matches!(control(&mut wire.peer, CHANNEL).await, Performative::Attach(_)));
                    if case == 0 {
                        // Actual adopted receiving leaf has no credit. This is
                        // not a claim that a parent joined its successor.
                        wire.detach().await;
                    } else {
                        let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
                            panic!("original protocol refusal")
                        };
                        assert!(detach.closed && detach.error.is_some());
                    }
                }
                assert!(!notice.is_requested());
                assert!(release_holds(&actor).is_empty());
                assert_eq!(grant_count(&actor), usize::from(matches!(case, 3 | 4)));
                assert!(!actor.log.lock().unwrap().iter().any(|entry| matches!(entry.kind,
                    CommandKind::Receive { .. } | CommandKind::Complete { .. }
                    | CommandKind::Abandon { .. } | CommandKind::Defer { .. }
                    | CommandKind::DeadLetter { .. })));
                let independent = wire.begin(2).await;
                assert!(!notice.is_requested());
                drop(independent);
                wire.stop().await;
            }

            let mut actor = Actor::new(durable, true);
            actor.send("notice-ended-hidden-grant", Some(session_id()));
            actor.gate.arm_put(domain::keys::session(
                &actor.namespace, &actor.entity, &session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let mut original = Box::pin(serve_entity_attachment_with_retirement(
                &session, &broker, &actor.namespace, "orders", attach,
                None, &management, Some(&notice)));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("original End-racing grant is held") },
                () = actor.gate.reached(false) => {},
            }}).await.unwrap();
            wire.end().await;
            assert!(session.is_ended());
            pending_once(original.as_mut()).await;
            assert!(!notice.is_requested());
            assert!(release_holds(&actor).is_empty());
            actor.gate.release(false);
            actor.gate.reached(true).await;
            let hold = stored_grant(&actor).hold();
            actor.gate.release(true);
            assert!(timeout(WAIT, original.as_mut()).await.unwrap().is_ok());
            drop(original);
            assert!(!notice.is_requested());
            grant_started_and_returned(&actor);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            let independent = wire.begin(2).await;
            assert!(!notice.is_requested());
            drop(independent);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;

            let mut actor = Actor::new(durable, true);
            actor.send("notice-superseded-claim", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let notice = ConnectionRetirementRequest::capture(&wire.connection);
            let management = ConnectionManagement::new();
            let held = management.session_write_lock().await;
            let broker = ReleaseWitness::new(&actor);
            let mut original = Box::pin(serve_entity_attachment_with_retirement(
                &session, &broker, &actor.namespace, "orders", attach,
                None, &management, Some(&notice)));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("original registry installation is held") },
                performative = control(&mut wire.peer, CHANNEL) => {
                    assert!(matches!(performative, Performative::Attach(_)));
                },
            }}).await.unwrap();
            let hold = stored_grant(&actor).hold();
            let _replacement_claim = management.claim_session(LINK, actor.entity.clone());
            pending_once(original.as_mut()).await;
            assert!(held.is_empty());
            assert!(!notice.is_requested());
            drop(held);
            assert!(timeout(WAIT, original.as_mut()).await.unwrap().is_ok());
            drop(original);
            let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
                panic!("superseded attachment is an ordinary refusal")
            };
            assert!(detach.closed && detach.error.is_some());
            assert!(!notice.is_requested());
            assert!(management.registered_session_owner(LINK).await.is_none());
            grant_started_and_returned(&actor);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            let independent = wire.begin(2).await;
            assert!(!notice.is_requested());
            drop(independent);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_remote_detach_and_report_only_faults_remain_non_notifying() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            for remote_detach in [false, true] {
                let mut actor = Actor::new(durable, remote_detach);
                if remote_detach {
                    actor.send("notice-benign-detach", Some(session_id()));
                }
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire
                    .offer(&mut session, remote_detach.then(|| filter(false)))
                    .await;
                if remote_detach {
                    wire.detach().await;
                    assert!(!session.is_ended());
                }
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let management = ConnectionManagement::new();
                let broker = ReleaseWitness::new(&actor);
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                    .join()
                    .unwrap();
                let mut original = Box::pin(
                    AssertUnwindSafe(serve_entity_attachment_with_retirement(
                        &session,
                        &broker,
                        &actor.namespace,
                        if remote_detach { "orders" } else { "" },
                        attach,
                        None,
                        &management,
                        Some(&notice),
                    ))
                    .catch_unwind(),
                );
                let mut observed = Box::pin(std::future::poll_fn(|context| {
                    tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
                }));
                let result = timeout(WAIT, observed.as_mut()).await.unwrap();
                if remote_detach {
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "normalized RemoteDetached remains benign"
                    );
                    grant_started_and_returned(&actor);
                } else {
                    assert_eq!(
                        result
                            .expect_err("report-only fault remains raw")
                            .downcast_ref::<&str>(),
                        Some(&"secondary attachment diagnostics")
                    );
                    assert!(matches!(
                        control(&mut wire.peer, CHANNEL).await,
                        Performative::Attach(_)
                    ));
                    assert!(matches!(
                        control(&mut wire.peer, CHANNEL).await,
                        Performative::Detach(_)
                    ));
                    assert_eq!(grant_count(&actor), 0);
                }
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
                assert!(
                    !notice.is_requested(),
                    "cleanup/report-first notice belongs to a later increment"
                );
                drop(observed);
                drop(original);
                let independent = wire.begin(2).await;
                assert!(!notice.is_requested());
                drop(independent);
                assert!(management.registered_session_owner(LINK).await.is_none());
                let holds = release_holds(&actor);
                assert_eq!(holds.len(), usize::from(remote_detach));
                drop(broker);
                if let Some(hold) = holds.first() {
                    assert_reopened_release(&mut actor, hold);
                }
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original observer joined");
}
