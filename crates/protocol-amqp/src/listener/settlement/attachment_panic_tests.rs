//! Actual attachment fronts are distinct from original-poll poison primitives.

use std::{
    io,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

use futures_util::FutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Notify,
};

use super::*;
use crate::listener::attachments::custody::{AttachmentCustody, PUMP_FAULT, PumpFault, PumpPoint};
use crate::listener::attachments::{prepare_and_adopt, serve_entity_attachment};

#[path = "attachment_leaf_fault_tests.rs"]
mod leaf_fault_tests;

fn assert_outer<T>(result: std::thread::Result<T>, fault: &PumpFault) {
    let payload = match result {
        Err(payload) => payload,
        Ok(_) => panic!("original outer fault resumes"),
    };
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        &fault.payload
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn outer_attachment_panic_drains_actual_hidden_grant_before_and_after_apply() {
    tokio::spawn(async {
        for durable in [false, true] {
            for next in [false, true] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, true);
                    actor.send("panic-hidden-grant", Some(session_id()));
                    actor.gate.arm_put(domain::keys::session(
                        &actor.namespace,
                        &actor.entity,
                        &session_id(),
                    ));
                    let (mut wire, mut session) = PendingWire::new().await;
                    let attach = wire.offer(&mut session, Some(filter(next))).await;
                    let management = ConnectionManagement::new();
                    let broker = ReleaseWitness::new(&actor);
                    let fault = PumpFault::new(PumpPoint::Grant);
                    let mut original = Box::pin(
                        PUMP_FAULT.scope(
                            Arc::clone(&fault),
                            AssertUnwindSafe(serve_entity_attachment(
                                &session,
                                &broker,
                                &actor.namespace,
                                "orders",
                                attach,
                                None,
                                &management,
                            ))
                            .catch_unwind(),
                        ),
                    );
                    pending_once(original.as_mut()).await;
                    actor.gate.reached(false).await;
                    timeout(WAIT, fault.reached.notified()).await.unwrap();
                    if after {
                        actor.gate.release(false);
                        actor.gate.reached(true).await;
                    }
                    fault.trigger.notify_one();
                    for _ in 0..2 {
                        pending_once(original.as_mut()).await;
                    }
                    assert_eq!(grant_count(&actor), 1);
                    assert!(release_holds(&actor).is_empty());
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
                    assert_eq!(
                        *broker.results.lock().unwrap(),
                        vec![(hold.clone(), Ok(()))]
                    );
                    assert!(management.registered_session(LINK).await.is_none());
                    let independent = wire.begin(2).await;
                    let mut next_frame = Box::pin(read_frame(&mut wire.peer));
                    pending_once(next_frame.as_mut()).await;
                    drop(next_frame);
                    drop(independent);
                    drop(broker);
                    assert_reopened_release(&mut actor, &hold);
                    wire.stop().await;
                }
            }
        }
    })
    .await
    .expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_attachment_panic_keeps_cached_grant_native_and_installed_packets() {
    tokio::spawn(async {
        for durable in [false, true] {
            for point in [PumpPoint::GrantResult, PumpPoint::NativeResult, PumpPoint::Installed,
                PumpPoint::Ready, PumpPoint::EntryPrepared] {
                let mut actor = Actor::new(durable, true);
                actor.send("panic-cached-grant", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let management = ConnectionManagement::new();
                let broker = ReleaseWitness::new(&actor);
                let fault = PumpFault::new(point);
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(serve_entity_attachment(&session, &broker, &actor.namespace,
                        "orders", attach, None, &management)).catch_unwind()));
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("held checkpoint owns original") },
                    () = fault.reached.notified() => {},
                }}).await.unwrap();
                let hold = stored_grant(&actor).hold();
                if point != PumpPoint::GrantResult {
                    let Performative::Attach(echo) = control(&mut wire.peer, CHANNEL).await else { panic!("original native attach") };
                    assert_eq!(read_session_filter(echo.source.as_ref()).unwrap(), SessionRequest::Named(hold.session_id.clone()));
                }
                if matches!(point, PumpPoint::Installed | PumpPoint::Ready | PumpPoint::EntryPrepared) {
                    assert_eq!(management.registered_session(LINK).await, Some((actor.entity.clone(), hold.clone())));
                }
                fault.trigger.notify_one();
                assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                drop(original);
                grant_started_and_returned(&actor);
                assert_eq!(release_holds(&actor), vec![hold.clone()]);
                assert!(management.registered_session(LINK).await.is_none());
                let independent = wire.begin(2).await;
                let mut next_frame = Box::pin(read_frame(&mut wire.peer));
                pending_once(next_frame.as_mut()).await;
                drop(next_frame);
                drop(independent);
                drop(broker);
                assert_reopened_release(&mut actor, &hold);
                wire.stop().await;
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_attachment_panic_before_first_grant_poll_submits_nothing() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("unstarted-panic-grant", Some(session_id()));
            let before = actor.store().snapshot().unwrap();
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Prepared);
            let mut original = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_entity_attachment(
                        &session,
                        &broker,
                        &actor.namespace,
                        "orders",
                        attach,
                        None,
                        &management,
                    ))
                    .catch_unwind(),
                ),
            );
            pending_once(original.as_mut()).await;
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            fault.trigger.notify_one();
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            drop(original);
            assert_eq!(grant_count(&actor), 0);
            assert!(release_holds(&actor).is_empty());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            let independent = wire.begin(2).await;
            let mut next_frame = Box::pin(read_frame(&mut wire.peer));
            pending_once(next_frame.as_mut()).await;
            drop(next_frame);
            drop(independent);
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
            wire.stop().await;
        }
    })
    .await
    .expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_attachment_panic_at_registry_lock_never_installs_or_removes_a_replacement() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("registry-panic-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let held = management.session_write_lock().await;
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Registry);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment(&session, &broker, &actor.namespace,
                    "orders", attach, None, &management)).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("registry checkpoint owns original") },
                () = fault.reached.notified() => {},
            }}).await.unwrap();
            assert!(matches!(control(&mut wire.peer, CHANNEL).await, Performative::Attach(_)));
            let hold = stored_grant(&actor).hold();
            assert!(held.is_empty());
            let replacement_claim = management.claim_session(LINK, actor.entity.clone());
            fault.trigger.notify_one();
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            drop(original);
            assert!(held.is_empty());
            drop(held);
            let replacement = accepted(&actor.intent(CommandKind::AcceptSession {
                session_id: Some(session_id()), lock_duration_millis: None,
            })).clone();
            let registration = management.install_session(&replacement_claim, replacement.hold(), || true)
                .await.unwrap();
            assert_eq!(management.registered_session_owner(LINK).await, Some(registration));
            assert_eq!(release_holds(&actor), vec![hold]);
            let before = actor.store().snapshot().unwrap();
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
            assert_eq!(stored_grant(&actor), replacement);
            wire.stop().await;
        }
    }).await.expect("original observer joined");
}

struct CloneFaultBroker {
    actual: ReleaseWitness,
    clones: Arc<AtomicUsize>,
    ordinal: usize,
    payload: Arc<str>,
}

impl Clone for CloneFaultBroker {
    fn clone(&self) -> Self {
        if self.clones.fetch_add(1, Ordering::SeqCst) + 1 == self.ordinal {
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        Self {
            actual: self.actual.clone(),
            clones: Arc::clone(&self.clones),
            ordinal: self.ordinal,
            payload: Arc::clone(&self.payload),
        }
    }
}

impl Broker for CloneFaultBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
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
async fn actual_ready_packet_retains_exact_hold_when_receiving_entry_clone_panics() {
    tokio::spawn(async {
        for durable in [false, true] {
            for ordinal in 1..=3 {
                let mut actor = Actor::new(durable, true);
                actor.send("construction-panic-grant", Some(session_id()));
                let (mut wire, mut session) = PendingWire::new().await;
                let attach = wire.offer(&mut session, Some(filter(false))).await;
                let management = ConnectionManagement::new();
                let actual = ReleaseWitness::new(&actor);
                let ready = accept_entity_link(
                    &session,
                    &actual,
                    &actor.namespace,
                    "orders",
                    attach,
                    None,
                    &management,
                )
                .await
                .unwrap()
                .unwrap();
                assert!(matches!(
                    control(&mut wire.peer, CHANNEL).await,
                    Performative::Attach(_)
                ));
                let hold = ready.accepted.as_ref().unwrap().hold();
                let registration = ready.registration.clone().unwrap();
                let broker = CloneFaultBroker {
                    actual,
                    clones: Arc::new(AtomicUsize::new(0)),
                    ordinal,
                    payload: Arc::from("original receiving entry construction panic"),
                };
                let mut custody = AttachmentCustody::new(&session);
                custody.ready = Some(ready);
                let result = AssertUnwindSafe(prepare_and_adopt(
                    &session,
                    &broker,
                    &actor.namespace,
                    "orders",
                    &management,
                    &mut custody,
                ))
                .catch_unwind()
                .await;
                let payload = result.unwrap_err();
                assert!(Arc::ptr_eq(
                    payload.downcast_ref::<Arc<str>>().unwrap(),
                    &broker.payload
                ));
                assert_eq!(broker.clones.load(Ordering::SeqCst), ordinal);
                assert_eq!(
                    custody
                        .ready
                        .as_ref()
                        .unwrap()
                        .accepted
                        .as_ref()
                        .unwrap()
                        .hold(),
                    hold
                );
                assert_eq!(
                    management.registered_session_owner(LINK).await,
                    Some(registration)
                );
                assert!(release_holds(&actor).is_empty());
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                    .await
                    .unwrap();
                assert_eq!(release_holds(&actor), vec![hold.clone()]);
                assert!(management.registered_session_owner(LINK).await.is_none());
                grant_started_and_returned(&actor);
                drop(custody);
                drop(broker);
                assert_reopened_release(&mut actor, &hold);
                wire.stop().await;
            }
        }
    })
    .await
    .expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_attachment_finish_retains_captured_unregister_and_one_lazy_actual_release() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("cancelled-cleanup-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let ready = accept_entity_link(
                &session,
                &broker,
                &actor.namespace,
                "orders",
                attach,
                None,
                &management,
            )
            .await
            .unwrap()
            .unwrap();
            assert!(matches!(
                control(&mut wire.peer, CHANNEL).await,
                Performative::Attach(_)
            ));
            let hold = ready.accepted.as_ref().unwrap().hold();
            let claim = management.claim_session(LINK, actor.entity.clone());
            let replacement = management
                .install_session(&claim, hold.clone(), || true)
                .await
                .unwrap();
            let held = management.session_write_lock().await;
            let mut custody = AttachmentCustody::new(&session);
            custody.ready = Some(ready);
            for _ in 0..2 {
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                assert!(custody.ready.is_some());
                assert!(release_holds(&actor).is_empty());
                assert_eq!(held.get(LINK), Some(&replacement));
            }
            actor.gate.arm_put(domain::keys::session(
                &actor.namespace,
                &actor.entity,
                &session_id(),
            ));
            drop(held);
            pending_once(Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut())
                .await;
            actor.gate.reached(false).await;
            for _ in 0..2 {
                pending_once(
                    Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut(),
                )
                .await;
                assert_eq!(release_holds(&actor), vec![hold.clone()]);
            }
            actor.gate.release(false);
            actor.gate.reached(true).await;
            pending_once(Box::pin(custody.finish(&broker, &actor.namespace, &management)).as_mut())
                .await;
            actor.gate.release(true);
            timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                .await
                .unwrap();
            timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                .await
                .unwrap();
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            assert_eq!(
                management.registered_session_owner(LINK).await,
                Some(replacement)
            );
            assert_eq!(
                *broker.results.lock().unwrap(),
                vec![(hold.clone(), Ok(()))]
            );
            drop(custody);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;
        }
    })
    .await
    .expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_ready_packet_panic_keeps_same_named_replacement_lock_and_registration() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("replacement-panic-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Ready);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment(&session, &broker, &actor.namespace,
                    "orders", attach, None, &management)).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("ready packet owns original") },
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
            let registration = management.install_session(&claim, replacement.hold(), || true).await.unwrap();
            let before = actor.store().snapshot().unwrap();
            fault.trigger.notify_one();
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
            assert_eq!(stored_grant(&actor), replacement);
            grant_started_and_returned(&actor);
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
            assert_eq!(stored_grant(&actor), replacement);
            wire.stop().await;
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn ended_ready_packet_never_adopts_or_starts_a_receiving_successor() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("ended-ready-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let ready = accept_entity_link(
                &session,
                &broker,
                &actor.namespace,
                "orders",
                attach,
                None,
                &management,
            )
            .await
            .unwrap()
            .unwrap();
            assert!(matches!(
                control(&mut wire.peer, CHANNEL).await,
                Performative::Attach(_)
            ));
            let hold = ready.accepted.as_ref().unwrap().hold();
            wire.end().await;
            assert!(session.is_ended());
            let mut custody = AttachmentCustody::new(&session);
            custody.ready = Some(ready);
            prepare_and_adopt(
                &session,
                &broker,
                &actor.namespace,
                "orders",
                &management,
                &mut custody,
            )
            .await;
            assert!(
                custody.ready.is_some(),
                "ended ready packet remains in its original cleanup owner"
            );
            timeout(WAIT, custody.finish(&broker, &actor.namespace, &management))
                .await
                .unwrap();
            grant_started_and_returned(&actor);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            assert!(management.registered_session(LINK).await.is_none());
            drop(custody);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;
        }
    })
    .await
    .expect("original observer joined");
}

struct PollWitness<F> {
    actual: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}
impl<F: Future> Future for PollWitness<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.actual.as_mut().poll(context)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn poisoned_original_attachment_phase_is_terminal_without_raw_result_or_repoll() {
    let (mut wire, session) = PendingWire::new().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let payload = Arc::<str>::from("original attachment poll poison");
    let original_payload = Arc::clone(&payload);
    let mut handoff = AttachmentHandoff::new(&session);
    handoff.begin_grant(PollWitness {
        actual: Box::pin(async move { std::panic::panic_any(original_payload) }),
        polls: Arc::clone(&polls),
    });
    let caught = match AssertUnwindSafe(handoff.observe()).catch_unwind().await {
        Err(payload) => payload,
        Ok(_) => panic!("original poll poison is caught"),
    };
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<str>>().unwrap(),
        &payload
    ));
    for _ in 0..2 {
        assert!(handoff.finish().await.is_none());
    }
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let packet = handoff.take_packet().unwrap();
    assert!(packet.started && packet.retired && packet.panicked && packet.step.is_none());
    assert!(packet.accepted.is_none());
    wire.stop().await;
}

#[derive(Default)]
struct NativeWriteGate {
    held: AtomicBool,
    entered: AtomicBool,
    waker: Mutex<Option<Waker>>,
    changed: Notify,
}

impl NativeWriteGate {
    fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    async fn reached(&self) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.entered.load(Ordering::SeqCst) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}

struct NativeWriteRelease(Arc<NativeWriteGate>);
impl Drop for NativeWriteRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct HeldNativeIo {
    inner: DuplexStream,
    gate: Arc<NativeWriteGate>,
}
impl AsyncRead for HeldNativeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, bytes)
    }
}
impl AsyncWrite for HeldNativeIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.gate.held.load(Ordering::SeqCst) {
            self.gate.entered.store(true, Ordering::SeqCst);
            *self.gate.waker.lock().unwrap() = Some(context.waker().clone());
            self.gate.changed.notify_waiters();
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(context, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

async fn held_native_wire() -> (PendingWire, ServerSession, Arc<NativeWriteGate>) {
    let (stream, mut peer) = duplex(64 * 1024);
    let gate = Arc::new(NativeWriteGate::default());
    let (connection, ()) = timeout(WAIT, async {
        tokio::join!(
            ServerConnection::accept(
                HeldNativeIo {
                    inner: stream,
                    gate: Arc::clone(&gate)
                },
                "held-attachment-server",
                None
            ),
            async {
                write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                    .await
                    .unwrap();
                assert_eq!(
                    read_protocol_header(&mut peer).await.unwrap(),
                    ProtocolHeader::AMQP
                );
                write_frame(
                    &mut peer,
                    &frame(0, Performative::Open(Open::new("held-attachment-peer"))),
                )
                .await
                .unwrap();
                assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
            }
        )
    })
    .await
    .unwrap();
    let mut wire = PendingWire {
        connection: connection.unwrap(),
        peer,
    };
    let session = wire.begin(CHANNEL).await;
    (wire, session, gate)
}

#[tokio::test(flavor = "current_thread")]
async fn outer_attachment_panic_keeps_actual_begun_native_accept_until_original_stop() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("native-panic-grant", Some(session_id()));
            let (mut wire, mut session, writes) = held_native_wire().await;
            let _release = NativeWriteRelease(Arc::clone(&writes));
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            writes.hold();
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let fault = PumpFault::new(PumpPoint::Native);
            let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                AssertUnwindSafe(serve_entity_attachment(&session, &broker, &actor.namespace,
                    "orders", attach, None, &management)).catch_unwind()));
            timeout(WAIT, async { tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("held original acceptance") },
                () = writes.reached() => {},
            }}).await.unwrap();
            timeout(WAIT, fault.reached.notified()).await.unwrap();
            let hold = stored_grant(&actor).hold();
            fault.trigger.notify_one();
            for _ in 0..2 {
                pending_once(original.as_mut()).await;
                assert!(release_holds(&actor).is_empty());
                assert!(writes.held.load(Ordering::SeqCst));
            }
            wire.stop().await;
            assert!(writes.held.load(Ordering::SeqCst), "original Stop interrupts the writer without releasing its barrier");
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            drop(original);
            grant_started_and_returned(&actor);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            assert!(management.registered_session(LINK).await.is_none());
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
        }
    }).await.expect("original observer joined");
}

struct PanicDiagnostics(Arc<AtomicUsize>);
impl tracing::Subscriber for PanicDiagnostics {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::attachments::custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any("secondary attachment diagnostics");
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_accept_error_precedes_reached_secondary_diagnostic_panic() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("diagnostic-native-grant", Some(session_id()));
            let (mut wire, mut session, writes) = held_native_wire().await;
            let _release = NativeWriteRelease(Arc::clone(&writes));
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            writes.hold();
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
            let mut original = Box::pin(AssertUnwindSafe(serve_entity_attachment(&session, &broker,
                &actor.namespace, "orders", attach, None, &management)).catch_unwind());
            let mut observed = Box::pin(std::future::poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
            }));
            timeout(WAIT, async { tokio::select! {
                result = observed.as_mut() => { let _ = result; panic!("held original acceptance") },
                () = writes.reached() => {},
            }}).await.unwrap();
            let hold = stored_grant(&actor).hold();
            wire.stop().await;
            let result = timeout(WAIT, observed.as_mut()).await.unwrap();
            let error = match result { Ok(Err(error)) => error, _ => panic!("native error precedes diagnostics") };
            assert!(matches!(error, EngineError::Stopped));
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
            assert_eq!(release_holds(&actor), vec![hold.clone()]);
            drop(observed);
            drop(original);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_remote_detach_precedes_reached_secondary_diagnostic_panic() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            let mut actor = Actor::new(durable, true);
            actor.send("diagnostic-detached-grant", Some(session_id()));
            let (mut wire, mut session) = PendingWire::new().await;
            let attach = wire.offer(&mut session, Some(filter(false))).await;
            wire.detach().await;
            assert!(!session.is_ended());
            let management = ConnectionManagement::new();
            let broker = ReleaseWitness::new(&actor);
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache)
                .join()
                .unwrap();
            let mut original = Box::pin(
                AssertUnwindSafe(serve_entity_attachment(
                    &session,
                    &broker,
                    &actor.namespace,
                    "orders",
                    attach,
                    None,
                    &management,
                ))
                .catch_unwind(),
            );
            let mut observed = Box::pin(std::future::poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
            }));
            assert!(
                matches!(timeout(WAIT, observed.as_mut()).await.unwrap(), Ok(Ok(()))),
                "an original detached offer remains benign ahead of diagnostics"
            );
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
            grant_started_and_returned(&actor);
            let holds = release_holds(&actor);
            assert_eq!(holds.len(), 1);
            let hold = holds[0].clone();
            assert_eq!(
                *broker.results.lock().unwrap(),
                vec![(hold.clone(), Ok(()))]
            );
            assert!(management.registered_session(LINK).await.is_none());
            drop(observed);
            drop(original);
            let independent = wire.begin(2).await;
            let mut next_frame = Box::pin(read_frame(&mut wire.peer));
            pending_once(next_frame.as_mut()).await;
            drop(next_frame);
            drop(independent);
            drop(broker);
            assert_reopened_release(&mut actor, &hold);
            wire.stop().await;
        }
    })
    .await
    .expect("original observer joined");
}
