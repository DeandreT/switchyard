//! Actual pump fronts are distinct from typed custody and poison controls.

#[path = "ingress_leaf_fault_tests.rs"]
mod leaf_fault_tests;

use std::{
    io,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::atomic::AtomicBool,
    task::{Context, Poll, Waker},
};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::*;
use crate::listener::ingress::custody::{
    NATIVE_POLLED, NativeSend, PUMP_FAULT, PumpFault, PumpPoint, SendCustody,
};
use crate::listener::{SendOutcome, SendRetirement, finish_send, send_delivery_pump};

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
async fn outer_send_and_batch_panic_drains_one_actual_commit_before_and_after_apply() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                for after in [false, true] {
                    let mut actor = Actor::new(durable, false);
                    let before = actor.store().snapshot().unwrap();
                    actor.clock.set(2_000);
                    actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                    let mut wire = IngressWire::new().await;
                    let receiver = wire.receiver.take().unwrap();
                    let (message, format) = messages(batch);
                    wire.send(0, message, format).await;
                    wire.no_ack_barrier().await;
                    let fault = PumpFault::new(PumpPoint::Send);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client(receiver, actor.namespace.clone(), actor.entity.clone(),
                            actor.broker.as_ref().unwrap().clone(), None)).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("held actual Send commit") },
                        () = actor.gate.reached(false) => {},
                    }}).await.unwrap();
                    timeout(WAIT, fault.reached.notified()).await.unwrap();
                    if after { actor.gate.release(false); actor.gate.reached(true).await; }
                    assert_eq!(actor.store().snapshot().unwrap() == before, !after);
                    fault.trigger.notify_one();
                    for _ in 0..2 { pending_once(original.as_mut()).await; }
                    assert_eq!(actor.log.lock().unwrap().len(), 1);
                    wire.no_ack_barrier().await;
                    actor.gate.release_all();
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    assert_one_submit(&actor, batch);
                    assert_stored(&actor, if batch { 2 } else { 1 });
                    assert_eq!(StateMachine::new(actor.store().clone()).last_applied_time().unwrap().as_millis(), 2_000);
                    wire.no_ack_barrier().await;
                    let committed = actor.store().snapshot().unwrap();
                    wire.stop().await;
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), committed);
                    assert_stored(&actor, if batch { 2 } else { 1 });
                }
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_send_packet_and_unstarted_ack_panics_never_create_a_late_acknowledgement() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                for point in [PumpPoint::Packet, PumpPoint::NativePrepared, PumpPoint::NativePacket] {
                    let mut actor = Actor::new(durable, false);
                    let mut wire = IngressWire::new().await;
                    let receiver = wire.receiver.take().unwrap();
                    let (message, format) = messages(batch);
                    wire.send(0, message, format).await;
                    let fault = PumpFault::new(point);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client(receiver, actor.namespace.clone(), actor.entity.clone(),
                            actor.broker.as_ref().unwrap().clone(), None)).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("held terminal packet") },
                        () = fault.reached.notified() => {},
                    }}).await.unwrap();
                    if point == PumpPoint::NativePacket {
                        assert!(matches!(wire.disposition(0).await.state, Some(DeliveryState::Accepted(_))));
                    }
                    assert_one_submit(&actor, batch);
                    assert_stored(&actor, if batch { 2 } else { 1 });
                    fault.trigger.notify_one();
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    assert_one_submit(&actor, batch);
                    wire.no_ack_barrier().await;
                    let before = actor.store().snapshot().unwrap();
                    wire.stop().await;
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                }
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_unstarted_send_panic_invokes_neither_eager_method_nor_actual_submission() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                let mut actor = Actor::new(durable, false);
                let before = actor.store().snapshot().unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let bodies = Arc::new(AtomicUsize::new(0));
                let broker = EagerBroker {
                    actual: actor.broker.as_ref().unwrap().clone(),
                    calls: Arc::clone(&calls),
                    bodies: Arc::clone(&bodies),
                };
                let mut wire = IngressWire::new().await;
                let receiver = wire.receiver.take().unwrap();
                let (message, format) = messages(batch);
                wire.send(0, message, format).await;
                let fault = PumpFault::new(PumpPoint::Prepared);
                let mut original = Box::pin(
                    PUMP_FAULT.scope(
                        Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client(
                            receiver,
                            actor.namespace.clone(),
                            actor.entity.clone(),
                            broker,
                            None,
                        ))
                        .catch_unwind(),
                    ),
                );
                timeout(WAIT, async { tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("held unstarted Send") },
                    () = fault.reached.notified() => {},
                }}).await.unwrap();
                fault.trigger.notify_one();
                assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                drop(original);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert_eq!(bodies.load(Ordering::SeqCst), 0);
                assert!(actor.log.lock().unwrap().is_empty());
                assert_eq!(actor.store().snapshot().unwrap(), before);
                wire.no_ack_barrier().await;
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
        }
    })
    .await
    .expect("original observer joined");
}

#[derive(Default)]
struct NativeWrites {
    held: AtomicBool,
    entered: AtomicBool,
    waker: std::sync::Mutex<Option<Waker>>,
    changed: Notify,
}
impl NativeWrites {
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
struct NativeRelease(Arc<NativeWrites>);
impl Drop for NativeRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct NativeIo {
    inner: DuplexStream,
    writes: Arc<NativeWrites>,
}
impl AsyncRead for NativeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, bytes)
    }
}
impl AsyncWrite for NativeIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.writes.held.load(Ordering::SeqCst) {
            self.writes.entered.store(true, Ordering::SeqCst);
            *self.writes.waker.lock().unwrap() = Some(cx.waker().clone());
            self.writes.changed.notify_waiters();
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn gated_wire() -> (IngressWire, Arc<NativeWrites>) {
    let (inner, mut peer) = duplex(64 * 1024);
    let writes = Arc::new(NativeWrites::default());
    let (connection, ()) = timeout(WAIT, async {
        tokio::join!(
            ServerConnection::accept(
                NativeIo {
                    inner,
                    writes: Arc::clone(&writes)
                },
                "held-ingress",
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
                    &frame(0, Performative::Open(Open::new("held-peer")), Vec::new()),
                )
                .await
                .unwrap();
                assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
            }
        )
    })
    .await
    .unwrap();
    let mut connection = connection.unwrap();
    write_frame(
        &mut peer,
        &frame(CHANNEL, Performative::Begin(Begin::default()), Vec::new()),
    )
    .await
    .unwrap();
    let incoming = connection.next_incoming_session().await.unwrap();
    let mut session = connection.accept_session(incoming).await.unwrap();
    assert!(matches!(
        control(&mut peer, CHANNEL).await,
        Performative::Begin(_)
    ));
    write_frame(
        &mut peer,
        &frame(
            CHANNEL,
            Performative::Attach(Box::new(Attach {
                name: "owned-ingress".to_owned(),
                handle: HANDLE,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(Target::new("orders")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    let attach = session.next_incoming_attach().await.unwrap();
    let LinkEndpoint::Receiver(receiver) =
        session.accept_attach(attach, 1024 * 1024).await.unwrap()
    else {
        panic!("actual incoming native Receiver")
    };
    assert!(matches!(
        control(&mut peer, CHANNEL).await,
        Performative::Attach(_)
    ));
    assert!(matches!(
        control(&mut peer, CHANNEL).await,
        Performative::Flow(_)
    ));
    (
        IngressWire {
            connection,
            _session: session,
            peer,
            receiver: Some(receiver),
            next_channel: 2,
        },
        writes,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn outer_panic_keeps_begun_actual_accept_and_both_reject_fronts_until_release_or_stop() {
    tokio::spawn(async {
        for durable in [false, true] {
            // Healthy single/batch, real broker refusal, conversion-only refusal.
            for case in 0..4 {
                for stop in [false, true] {
                    let mut actor = Actor::new(durable, case == 2);
                    let before = actor.store().snapshot().unwrap();
                    let (mut wire, writes) = gated_wire().await;
                    let _release = NativeRelease(Arc::clone(&writes));
                    let receiver = wire.receiver.take().unwrap();
                    let batch = case == 1;
                    let (message, format) = messages(batch);
                    wire.send(0, message, if case == 3 { 7 } else { format }).await;
                    writes.hold();
                    let fault = PumpFault::new(PumpPoint::Native);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client(receiver, actor.namespace.clone(), actor.entity.clone(),
                            actor.broker.as_ref().unwrap().clone(), None)).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("held original native acknowledgement") },
                        () = writes.reached() => {},
                    }}).await.unwrap();
                    timeout(WAIT, fault.reached.notified()).await.unwrap();
                    fault.trigger.notify_one();
                    for _ in 0..2 { pending_once(original.as_mut()).await; }
                    assert!(writes.held.load(Ordering::SeqCst));
                    if stop { wire.stop().await; }
                    else { writes.release(); }
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    if !stop {
                        let disposition = wire.disposition(0).await;
                        match case {
                            0 | 1 => assert!(matches!(disposition.state, Some(DeliveryState::Accepted(_)))),
                            _ => {
                                let Some(DeliveryState::Rejected(rejected)) = disposition.state else { panic!("exact original rejection") };
                                assert_eq!(rejected.error.unwrap().condition,
                                    if case == 2 { AmqpError::NotAllowed.into() } else { AmqpError::InvalidField.into() });
                            }
                        }
                        wire.no_ack_barrier().await;
                        wire.stop().await;
                    }
                    if case == 3 { assert!(actor.log.lock().unwrap().is_empty()); }
                    else { assert_one_submit(&actor, batch); }
                    if case >= 2 { assert_eq!(actor.store().snapshot().unwrap(), before); }
                    assert_stored(&actor, if case == 0 { 1 } else if batch { 2 } else { 0 });
                    let committed = actor.store().snapshot().unwrap();
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), committed);
                }
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_send_custody_finish_keeps_exact_actual_result_and_native_packet() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                let mut actor = Actor::new(durable, false);
                actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                let mut custody = SendCustody::default();
                custody.expected = Some(if batch { SendOutcome::Batch(1) } else { SendOutcome::Single });
                custody.original = Some(SendIntake::new(submission(&actor, batch)));
                let mut observed = Box::pin(custody.original.as_mut().unwrap().observe());
                timeout(WAIT, async { tokio::select! {
                    result = observed.as_mut() => { let _ = result; panic!("held actual Send commit") },
                    () = actor.gate.reached(false) => {},
                }}).await.unwrap();
                drop(observed);
                for _ in 0..2 {
                    pending_once(Box::pin(custody.finish()).as_mut()).await;
                    assert!(custody.packet.is_none());
                }
                actor.gate.release_all();
                timeout(WAIT, custody.finish()).await.unwrap();
                let packet = custody.packet.as_ref().unwrap();
                let address = packet as *const _ as usize;
                assert!(packet.started && packet.retired && !packet.panicked);
                let expected = if batch { CommandOutcome::BatchSent { sequences: vec![SequenceNumber::new(1)], stored: 1 } }
                    else { CommandOutcome::Sent { sequence: SequenceNumber::new(1) } };
                assert_eq!(packet.result, Some(Ok(expected)));
                timeout(WAIT, custody.finish()).await.unwrap();
                assert_eq!(custody.packet.as_ref().unwrap() as *const _ as usize, address);
                assert_one_submit(&actor, batch);
                drop(custody);
                let committed = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
            // Qualified native custody fronts: one actual ACK or auth Close.
            for close in [false, true] {
                let (mut wire, writes) = gated_wire().await;
                let _release = NativeRelease(Arc::clone(&writes));
                let mut receiver = wire.receiver.take().unwrap();
                let (message, format) = messages(false);
                wire.send(0, message, format).await;
                let delivery = receiver.recv().await.unwrap();
                writes.hold();
                let mut custody = SendCustody::default();
                if close { custody.close = Some(NativeSend::new(receiver.close_with_error(
                    crate::listener::unauthorized_error("expired")))); }
                else { custody.native = Some(NativeSend::new(receiver.accept(&delivery))); }
                let mut observe = Box::pin(async {
                    if close { custody.close.as_mut().unwrap().observe().await; }
                    else { custody.native.as_mut().unwrap().observe().await; }
                });
                timeout(WAIT, async { tokio::select! {
                    () = observe.as_mut() => panic!("actual original native write held"),
                    () = writes.reached() => {},
                }}).await.unwrap();
                drop(observe);
                for _ in 0..2 { pending_once(Box::pin(custody.finish()).as_mut()).await; }
                wire.stop().await;
                timeout(WAIT, custody.finish()).await.unwrap();
                let packet = if close { custody.close_packet.as_ref() } else { custody.native_packet.as_ref() }.unwrap();
                assert!(packet.started && packet.retired && !packet.panicked);
                assert!(matches!(packet.result, Some(Err(amqp::EngineError::Stopped))));
                let address = packet.result.as_ref().unwrap() as *const _ as usize;
                timeout(WAIT, custody.finish()).await.unwrap();
                let packet = if close { custody.close_packet.as_ref() } else { custody.native_packet.as_ref() }.unwrap();
                assert_eq!(packet.result.as_ref().unwrap() as *const _ as usize, address);
            }
        }
    }).await.expect("original observer joined");
}

struct PollWitness<F> {
    actual: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}
impl<F: Future> Future for PollWitness<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.actual.as_mut().poll(cx)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn poisoned_original_send_and_native_polls_are_terminal_without_repoll_or_fake_result() {
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    for native in [false, true] {
        let polls = Arc::new(AtomicUsize::new(0));
        let payload: Arc<str> = Arc::from("original Send poll poison");
        let original_payload = Arc::clone(&payload);
        let mut custody = SendCustody::default();
        if native {
            custody.native = Some(NativeSend::new(PollWitness {
                actual: Box::pin(async move { std::panic::panic_any(original_payload) }),
                polls: Arc::clone(&polls),
            }));
        } else {
            custody.original = Some(SendIntake::new(PollWitness {
                actual: Box::pin(async move { std::panic::panic_any(original_payload) }),
                polls: Arc::clone(&polls),
            }));
        }
        let primary = AssertUnwindSafe(async {
            if native {
                custody.native.as_mut().unwrap().observe().await;
            } else {
                custody.original.as_mut().unwrap().observe().await;
            }
        })
        .catch_unwind()
        .await;
        let payload_caught = primary.unwrap_err();
        assert!(Arc::ptr_eq(
            payload_caught.downcast_ref::<Arc<str>>().unwrap(),
            &payload
        ));
        for _ in 0..2 {
            custody.finish().await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        if native {
            let packet = custody.native_packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        } else {
            let packet = custody.packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        }
    }
    // Qualified primitive: a selected exit must not conceal a newly poisoned
    // original drain, even if its terminal reporting callback also panics.
    for native in [false, true] {
        let polls = Arc::new(AtomicUsize::new(0));
        let payload: Arc<str> = Arc::from("newly poisoned original Send drain");
        let original_payload = Arc::clone(&payload);
        let release = Arc::new(Notify::new());
        let original_release = Arc::clone(&release);
        let mut custody = SendCustody::default();
        if native {
            custody.native = Some(NativeSend::new(PollWitness {
                actual: Box::pin(async move {
                    original_release.notified().await;
                    std::panic::panic_any(original_payload)
                }),
                polls: Arc::clone(&polls),
            }));
        } else {
            custody.original = Some(SendIntake::new(PollWitness {
                actual: Box::pin(async move {
                    original_release.notified().await;
                    std::panic::panic_any(original_payload)
                }),
                polls: Arc::clone(&polls),
            }));
        }
        if native {
            pending_once(
                Box::pin(tokio::task::unconstrained(
                    custody.native.as_mut().unwrap().observe(),
                ))
                .as_mut(),
            )
            .await;
        } else {
            pending_once(
                Box::pin(tokio::task::unconstrained(
                    custody.original.as_mut().unwrap().observe(),
                ))
                .as_mut(),
            )
            .await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        release.notify_one();
        let diagnostics = Arc::new(AtomicUsize::new(0));
        let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
        std::thread::spawn(tracing::callsite::rebuild_interest_cache)
            .join()
            .unwrap();
        let mut original = Box::pin(
            AssertUnwindSafe(tokio::task::unconstrained(finish_send(
                &mut custody,
                Ok(Ok(Some(SendRetirement::Detached))),
            )))
            .catch_unwind(),
        );
        let caught = match timeout(
            WAIT,
            std::future::poll_fn(|cx| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(cx))
            }),
        )
        .await
        .unwrap()
        {
            Err(payload) => payload,
            Ok(_) => panic!("new original drain poison remains a fault"),
        };
        assert!(Arc::ptr_eq(
            caught.downcast_ref::<Arc<str>>().unwrap(),
            &payload
        ));
        assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
        drop(original);
        for _ in 0..2 {
            custody.finish().await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        if native {
            let packet = custody.native_packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        } else {
            let packet = custody.packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_authorization_close_panics_keep_begun_original_and_suppress_unstarted_close() {
    tokio::spawn(async {
        for durable in [false, true] {
            let authorization = authorization();
            expire(&authorization).await;
            for point in [PumpPoint::ClosePrepared, PumpPoint::Close] {
                for stop in [false, true] {
                    if stop && point != PumpPoint::Close { continue; }
                    let mut actor = Actor::new(durable, false);
                    let before = actor.store().snapshot().unwrap();
                    let (mut wire, writes) = gated_wire().await;
                    let _release = NativeRelease(Arc::clone(&writes));
                    let receiver = wire.receiver.take().unwrap();
                    if point == PumpPoint::Close { writes.hold(); }
                    let fault = PumpFault::new(point);
                    let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                        AssertUnwindSafe(serve_sending_client(receiver, actor.namespace.clone(), actor.entity.clone(),
                            actor.broker.as_ref().unwrap().clone(), Some(authorization.clone()))).catch_unwind()));
                    timeout(WAIT, async { tokio::select! {
                        result = original.as_mut() => { let _ = result; panic!("held original auth Close") },
                        () = async {
                            if point == PumpPoint::Close { writes.reached().await; }
                            else { fault.reached.notified().await; }
                        } => {},
                    }}).await.unwrap();
                    if point == PumpPoint::Close { timeout(WAIT, fault.reached.notified()).await.unwrap(); }
                    fault.trigger.notify_one();
                    if point == PumpPoint::Close {
                        for _ in 0..2 { pending_once(original.as_mut()).await; }
                        if stop { wire.stop().await; } else { writes.release(); }
                    }
                    assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
                    drop(original);
                    if point == PumpPoint::Close && !stop {
                        let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else { panic!("one retained auth Detach") };
                        assert_eq!(detach.error.unwrap().condition, AmqpError::UnauthorizedAccess.into());
                    }
                    if !stop { wire.no_ack_barrier().await; wire.stop().await; }
                    assert!(actor.log.lock().unwrap().is_empty());
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                    actor.reopen();
                    assert_eq!(actor.store().snapshot().unwrap(), before);
                }
            }
        }
    }).await.expect("original observer joined");
}

async fn replacement_receiver(wire: &mut IngressWire) -> Receiver {
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Attach(Box::new(Attach {
                name: "owned-ingress".to_owned(),
                handle: HANDLE,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(Target::new("orders")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    let attach = wire._session.next_incoming_attach().await.unwrap();
    let LinkEndpoint::Receiver(receiver) = wire
        ._session
        .accept_attach(attach, 1024 * 1024)
        .await
        .unwrap()
    else {
        panic!("same-name replacement Receiver")
    };
    assert!(matches!(
        control(&mut wire.peer, CHANNEL).await,
        Performative::Attach(_)
    ));
    assert!(matches!(
        control(&mut wire.peer, CHANNEL).await,
        Performative::Flow(_)
    ));
    receiver
}

#[tokio::test(flavor = "current_thread")]
async fn outer_cached_native_packet_panic_cannot_ack_or_close_same_name_handle_replacement() {
    tokio::spawn(async {
        for durable in [false, true] {
            let mut actor = Actor::new(durable, false);
            let mut wire = IngressWire::new().await;
            let receiver = wire.receiver.take().unwrap();
            let (message, format) = messages(false);
            wire.send(0, message, format).await;
            let fault = PumpFault::new(PumpPoint::NativePacket);
            let mut original = Box::pin(
                PUMP_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_sending_client(
                        receiver,
                        actor.namespace.clone(),
                        actor.entity.clone(),
                        actor.broker.as_ref().unwrap().clone(),
                        None,
                    ))
                    .catch_unwind(),
                ),
            );
            timeout(WAIT, async {
                tokio::select! {
                    result = original.as_mut() => { let _ = result; panic!("cached original ACK") },
                    () = fault.reached.notified() => {},
                }
            })
            .await
            .unwrap();
            assert!(matches!(
                wire.disposition(0).await.state,
                Some(DeliveryState::Accepted(_))
            ));
            wire.detach().await;
            let mut replacement = replacement_receiver(&mut wire).await;
            let (message, format) = messages(false);
            wire.send(0, message, format).await;
            let delivery = timeout(WAIT, replacement.recv()).await.unwrap().unwrap();
            assert_eq!(
                delivery.message().properties.as_ref().unwrap().message_id,
                Some("ingress-0".to_owned().into())
            );
            fault.trigger.notify_one();
            assert_outer(timeout(WAIT, original.as_mut()).await.unwrap(), &fault);
            drop(original);
            assert_one_submit(&actor, false);
            assert_stored(&actor, 1);
            wire.no_ack_barrier().await;
            replacement.accept(&delivery).await.unwrap();
            assert!(matches!(
                wire.disposition(0).await.state,
                Some(DeliveryState::Accepted(_))
            ));
            wire.detach().await;
            wire.stop().await;
            let before = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    })
    .await
    .expect("original observer joined");
}

struct PanicDiagnostics(Arc<AtomicUsize>);
impl tracing::Subscriber for PanicDiagnostics {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::ingress::custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any("secondary inbound diagnostics");
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn actual_send_primary_panic_and_native_error_precede_reached_secondary_diagnostics() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            for native_error in [false, true] {
                let mut actor = Actor::new(durable, false);
                let (mut wire, writes) = gated_wire().await;
                let _release = NativeRelease(Arc::clone(&writes));
                let mut receiver = wire.receiver.take().unwrap();
                let (message, format) = messages(false);
                wire.send(0, message, format).await;
                if native_error { writes.hold(); }
                let fault = PumpFault::new(if native_error { PumpPoint::Send } else { PumpPoint::Packet });
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
                let mut original = Box::pin(PUMP_FAULT.scope(Arc::clone(&fault),
                    AssertUnwindSafe(async {
                        if native_error {
                            // Qualified actual delivery-helper control: a pending
                            // retirement observer allows the original native error
                            // to reach its active path, rather than selecting Stop.
                            let delivery = receiver.recv().await.unwrap();
                            let mut custody = SendCustody::default();
                            let retirement = std::future::pending::<SendRetirement>();
                            tokio::pin!(retirement);
                            let primary = AssertUnwindSafe(send_delivery_pump(&receiver, &delivery,
                                &actor.namespace, &actor.entity, actor.broker.as_ref().unwrap(),
                                &mut custody, retirement.as_mut())).catch_unwind().await;
                            finish_send(&mut custody, primary).await?;
                            Ok(())
                        } else {
                            serve_sending_client(receiver, actor.namespace.clone(), actor.entity.clone(),
                                actor.broker.as_ref().unwrap().clone(), None).await
                        }
                    }).catch_unwind()));
                let mut observed = Box::pin(std::future::poll_fn(|cx| {
                    tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(cx))
                }));
                timeout(WAIT, async { tokio::select! {
                    result = observed.as_mut() => { let _ = result; panic!("held actual primary frontier") },
                    () = async {
                        if native_error { writes.reached().await; }
                        else { fault.reached.notified().await; }
                    } => {},
                }}).await.unwrap();
                if native_error { wire.stop().await; }
                else { fault.trigger.notify_one(); }
                let result = timeout(WAIT, observed.as_mut()).await.unwrap();
                if native_error {
                    let error = match result { Ok(Err(error)) => error, _ => panic!("original native error wins") };
                    assert!(matches!(error.downcast_ref::<amqp::EngineError>(), Some(amqp::EngineError::Stopped)));
                } else { assert_outer(result, &fault); }
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
                drop(observed); drop(original);
                assert_one_submit(&actor, false);
                assert_stored(&actor, 1);
                if !native_error { wire.no_ack_barrier().await; wire.stop().await; }
                let before = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
        }
    }).await.expect("original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn actual_retired_native_error_stays_benign_ahead_of_reached_secondary_diagnostics() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            let expired = authorization();
            expire(&expired).await;
        for close in [false, true] {
            let mut actor = Actor::new(durable, false);
            let (mut wire, writes) = gated_wire().await;
            let _release = NativeRelease(Arc::clone(&writes));
            let receiver = wire.receiver.take().unwrap();
            if !close {
                let (message, format) = messages(false);
                wire.send(0, message, format).await;
            }
            writes.hold();
            let diagnostics = Arc::new(AtomicUsize::new(0));
            let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
            std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
            let mut original = Box::pin(AssertUnwindSafe(serve_sending_client(receiver,
                actor.namespace.clone(), actor.entity.clone(), actor.broker.as_ref().unwrap().clone(),
                close.then(|| expired.clone()))).catch_unwind());
            let mut observed = Box::pin(std::future::poll_fn(|cx| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(cx))
            }));
            timeout(WAIT, async { tokio::select! {
                result = observed.as_mut() => { let _ = result; panic!("actual original native write held") },
                () = writes.reached() => {},
            }}).await.unwrap();
            wire.stop().await;
            let result = timeout(WAIT, observed.as_mut()).await.unwrap();
            assert!(matches!(result, Ok(Ok(()))));
            assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
            drop(observed); drop(original);
            if close { assert!(actor.log.lock().unwrap().is_empty()); }
            else { assert_one_submit(&actor, false); assert_stored(&actor, 1); }
            let before = actor.store().snapshot().unwrap();
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
            for late_refusal in [false, true] {
                qualified_native_result_after_selected_detach(durable, late_refusal).await;
            }
            for conversion in [false, true] {
                qualified_unstarted_send_or_reject_after_selected_detach(durable, conversion).await;
            }
        }
    }).await.expect("original observer joined");
}

async fn qualified_native_result_after_selected_detach(durable: bool, late_refusal: bool) {
    let mut actor = Actor::new(durable, false);
    let mut wire = IngressWire::new().await;
    let mut receiver = wire.receiver.take().unwrap();
    let (message, format) = messages(false);
    wire.send(0, message, format).await;
    let delivery = receiver.recv().await.unwrap();
    let mut replacement = if late_refusal {
        wire.detach().await;
        assert!(receiver.on_detach_owned().now_or_never().is_some());
        Some(replacement_receiver(&mut wire).await)
    } else {
        None
    };
    let selected = Arc::new(Notify::new());
    let retirement = async {
        receiver.on_detach_owned().await;
        selected.notified().await;
        SendRetirement::Detached
    };
    tokio::pin!(retirement);
    let mut custody = SendCustody::default();
    let polled = Arc::new(Notify::new());
    // Qualified helper front: gate the real retired-link observer. Refusal
    // admits on an already retired old handle; success retires after actual
    // ACK completion. Neither claims normal whole-link watcher admission.
    let mut pump = Box::pin(
        NATIVE_POLLED.scope(
            Arc::clone(&polled),
            AssertUnwindSafe(tokio::task::unconstrained(send_delivery_pump(
                &receiver,
                &delivery,
                &actor.namespace,
                &actor.entity,
                actor.broker.as_ref().unwrap(),
                &mut custody,
                retirement.as_mut(),
            )))
            .catch_unwind(),
        ),
    );
    timeout(WAIT, async { tokio::select! {
        biased;
        () = polled.notified() => {},
        result = pump.as_mut() => { let _ = result; panic!("native poll precedes selected retirement") },
    }}).await.unwrap();
    // With an empty native command queue and unconstrained first poll, the
    // original request was enqueued. The later actual command/wire FIFO
    // completes that original reply while this borrowed pump stays unpolled.
    if !late_refusal {
        assert!(matches!(
            wire.disposition(0).await.state,
            Some(DeliveryState::Accepted(_))
        ));
    }
    wire.no_ack_barrier().await;
    if !late_refusal {
        wire.detach().await;
        assert!(receiver.on_detach_owned().now_or_never().is_some());
        replacement = Some(replacement_receiver(&mut wire).await);
    }
    selected.notify_one();
    let primary = timeout(WAIT, pump.as_mut()).await.unwrap();
    assert!(matches!(&primary, Ok(Ok(Some(SendRetirement::Detached)))));
    drop(pump);
    let diagnostics = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let mut original =
        Box::pin(AssertUnwindSafe(finish_send(&mut custody, primary)).catch_unwind());
    let result = timeout(
        WAIT,
        std::future::poll_fn(|cx| {
            tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(cx))
        }),
    )
    .await
    .unwrap();
    assert!(matches!(result, Ok(Ok(Some(SendRetirement::Detached)))));
    assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
    drop(original);
    let packet = custody.native_packet.as_ref().unwrap();
    assert!(packet.started && packet.retired && !packet.panicked);
    if late_refusal {
        assert!(
            matches!(packet.result.as_ref(), Some(Err(amqp::EngineError::InvalidState(detail)))
            if detail == "settlement on an unknown link")
        );
    } else {
        assert!(matches!(packet.result, Some(Ok(()))));
    }
    let identity = packet.result.as_ref().unwrap() as *const _ as usize;
    custody.finish().await;
    assert_eq!(
        custody
            .native_packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap() as *const _ as usize,
        identity
    );
    assert_one_submit(&actor, false);
    let (message, format) = messages(false);
    wire.send(0, message, format).await;
    let replacement = replacement.as_mut().unwrap();
    let current = replacement.recv().await.unwrap();
    replacement.accept(&current).await.unwrap();
    assert!(matches!(
        wire.disposition(0).await.state,
        Some(DeliveryState::Accepted(_))
    ));
    wire.no_ack_barrier().await;
    wire.detach().await;
    wire.stop().await;
    drop(custody);
    assert_stored(&actor, 1);
    let before = actor.store().snapshot().unwrap();
    actor.reopen();
    assert_eq!(actor.store().snapshot().unwrap(), before);
}

async fn qualified_unstarted_send_or_reject_after_selected_detach(durable: bool, conversion: bool) {
    let mut actor = Actor::new(durable, false);
    let before = actor.store().snapshot().unwrap();
    let mut wire = IngressWire::new().await;
    let mut receiver = wire.receiver.take().unwrap();
    let (message, format) = messages(false);
    wire.send(0, message, if conversion { 7 } else { format })
        .await;
    let delivery = receiver.recv().await.unwrap();
    wire.detach().await;
    assert!(receiver.on_detach_owned().now_or_never().is_some());
    let mut replacement = replacement_receiver(&mut wire).await;
    let retirement = async {
        receiver.on_detach_owned().await;
        SendRetirement::Detached
    };
    tokio::pin!(retirement);
    let mut custody = SendCustody::default();
    // Qualified consumed-delivery helper, not the ordinary recv-loop edge.
    let primary = AssertUnwindSafe(send_delivery_pump(
        &receiver,
        &delivery,
        &actor.namespace,
        &actor.entity,
        actor.broker.as_ref().unwrap(),
        &mut custody,
        retirement.as_mut(),
    ))
    .catch_unwind()
    .await;
    assert!(matches!(&primary, Ok(Ok(Some(SendRetirement::Detached)))));
    let diagnostics = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    let mut original =
        Box::pin(AssertUnwindSafe(finish_send(&mut custody, primary)).catch_unwind());
    assert!(matches!(
        timeout(
            WAIT,
            std::future::poll_fn(|cx| {
                tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(cx))
            })
        )
        .await
        .unwrap(),
        Ok(Ok(Some(SendRetirement::Detached)))
    ));
    assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
    drop(original);
    if conversion {
        let packet = custody.native_packet.as_ref().unwrap();
        assert!(!packet.started && packet.retired && !packet.panicked && packet.result.is_none());
        assert!(custody.packet.is_none());
    } else {
        let packet = custody.packet.as_ref().unwrap();
        assert!(!packet.started && packet.retired && !packet.panicked && packet.result.is_none());
        assert!(custody.native.is_none() && custody.native_packet.is_none());
    }
    assert!(actor.log.lock().unwrap().is_empty());
    assert_eq!(actor.store().snapshot().unwrap(), before);
    wire.no_ack_barrier().await;
    let (message, format) = messages(false);
    wire.send(0, message, format).await;
    let current = replacement.recv().await.unwrap();
    replacement.accept(&current).await.unwrap();
    assert!(matches!(
        wire.disposition(0).await.state,
        Some(DeliveryState::Accepted(_))
    ));
    wire.detach().await;
    wire.stop().await;
    drop(custody);
    actor.reopen();
    assert_eq!(actor.store().snapshot().unwrap(), before);
}
