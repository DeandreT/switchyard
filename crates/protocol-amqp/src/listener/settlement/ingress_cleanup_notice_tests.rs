//! Actual Send retirement and qualified opposite-slot cleanup are separate proofs.

use super::*;
use crate::listener::ingress::RawSendResult;
use crate::listener::{
    ConnectionRetirementRequest, SEND_INTAKE_RETIREMENT, finish_send_with_retirement,
    serve_sending_client_with_retirement,
};

struct PollPoison<'a, O> {
    actual: Pin<Box<dyn Future<Output = O> + Send + 'a>>,
    trigger: Pin<Box<dyn Future<Output = ()> + Send>>,
    ready: Arc<Notify>,
    result: Option<O>,
    payload: Arc<str>,
    polls: Arc<AtomicUsize>,
    after_result: bool,
}

impl<'a, O> PollPoison<'a, O> {
    fn new(
        actual: impl Future<Output = O> + Send + 'a,
        trigger: Arc<Notify>,
        ready: Arc<Notify>,
        payload: Arc<str>,
        polls: Arc<AtomicUsize>,
        after_result: bool,
    ) -> Self {
        Self {
            actual: Box::pin(actual),
            trigger: Box::pin(async move { trigger.notified().await }),
            ready,
            result: None,
            payload,
            polls,
            after_result,
        }
    }
}

impl<O: Unpin> Future for PollPoison<'_, O> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if !self.after_result && self.trigger.as_mut().poll(context).is_ready() {
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        if self.result.is_none() {
            match self.actual.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.result = Some(result);
                    self.ready.notify_one();
                }
            }
        }
        if self.trigger.as_mut().poll(context).is_ready() {
            std::panic::panic_any(Arc::clone(&self.payload));
        }
        Poll::Pending
    }
}

fn assert_payload<T>(result: std::thread::Result<T>, expected: &Arc<str>) {
    let payload = match result {
        Err(payload) => payload,
        Ok(_) => panic!("the original raw panic must remain a fault"),
    };
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected,
    ));
}

#[derive(Clone)]
struct PostResultPoisonBroker {
    actual: ActualBroker,
    ready: Arc<Notify>,
    trigger: Arc<Notify>,
    payload: Arc<str>,
    calls: Arc<AtomicUsize>,
}

impl Broker for PostResultPoisonBroker {
    async fn submit(
        &self,
        namespace: domain::NamespaceName,
        entity: domain::EntityPath,
        command: CommandKind,
    ) -> RawSendResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _original_result = self.actual.submit(namespace, entity, command).await;
        self.ready.notify_one();
        self.trigger.notified().await;
        std::panic::panic_any(Arc::clone(&self.payload));
    }

    fn deliverable(
        &self,
        _: &domain::NamespaceName,
        _: &domain::EntityPath,
    ) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

async fn retain_native_result(
    actual: impl Future<Output = Result<(), amqp::EngineError>> + Send,
    ready: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
) -> Result<(), amqp::EngineError> {
    calls.fetch_add(1, Ordering::SeqCst);
    let result = actual.await;
    ready.notify_one();
    release.notified().await;
    result
}

#[tokio::test(flavor = "current_thread")]
async fn actual_retired_send_or_batch_post_result_panic_notifies_without_recovery() {
    tokio::spawn(async {
        for durable in [false, true] {
            for batch in [false, true] {
                for after in [false, true] {
                    for auth in [false, true] {
                        let mut actor = Actor::new(durable, false);
                        let before = actor.store().snapshot().unwrap();
                        actor.clock.set(2_000);
                        actor.gate.arm_put(actor.key(SequenceNumber::new(1)));
                        let mut wire = IngressWire::new().await;
                        let notice = ConnectionRetirementRequest::capture(&wire.connection);
                        let receiver = wire.receiver.take().unwrap();
                        let ready = Arc::new(Notify::new());
                        let trigger = Arc::new(Notify::new());
                        let payload: Arc<str> = Arc::from("same original Send post-result panic");
                        let calls = Arc::new(AtomicUsize::new(0));
                        let broker = PostResultPoisonBroker {
                            actual: actor.broker.as_ref().unwrap().clone(),
                            ready: Arc::clone(&ready),
                            trigger: Arc::clone(&trigger),
                            payload: Arc::clone(&payload),
                            calls: Arc::clone(&calls),
                        };
                        let authorization = auth.then(authorization);
                        let selected = Arc::new(Notify::new());
                        let (message, format) = messages(batch);
                        wire.send(0, message, format).await;
                        wire.no_ack_barrier().await;
                        let mut original = Box::pin(SEND_INTAKE_RETIREMENT.scope(
                            Arc::clone(&selected),
                            AssertUnwindSafe(serve_sending_client_with_retirement(
                                receiver, actor.namespace.clone(), actor.entity.clone(),
                                broker.clone(), authorization.clone(), Some(notice.clone()),
                            )).catch_unwind(),
                        ));
                        timeout(WAIT, async { tokio::select! {
                            result = original.as_mut() => { let _ = result; panic!("original apply held") },
                            () = actor.gate.reached(false) => {},
                        }}).await.unwrap();
                        if after { actor.gate.release(false); actor.gate.reached(true).await; }
                        assert_eq!(actor.store().snapshot().unwrap() == before, !after);
                        if let Some(authorization) = authorization.as_ref() {
                            expire(authorization).await;
                        } else {
                            wire.detach().await;
                        }
                        timeout(WAIT, async { tokio::select! {
                            result = original.as_mut() => { let _ = result; panic!("selected retirement drains original") },
                            () = selected.notified() => {},
                        }}).await.unwrap();
                        assert!(!notice.is_requested());
                        assert_eq!(actor.log.lock().unwrap().len(), 1);
                        actor.gate.release_all();
                        timeout(WAIT, async { tokio::select! {
                            result = original.as_mut() => { let _ = result; panic!("same returned result held before injected panic") },
                            () = ready.notified() => {},
                        }}).await.unwrap();
                        assert_one_submit(&actor, batch);
                        assert!(!notice.is_requested());
                        trigger.notify_one();
                        let ((), result) = timeout(WAIT, async {
                            tokio::join!(notice.observer(), original.as_mut())
                        }).await.unwrap();
                        assert_payload(result, &payload);
                        drop(original);
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert!(notice.is_requested());
                        notice.request();
                        timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                        assert_one_submit(&actor, batch);
                        assert_stored(&actor, if batch { 2 } else { 1 });
                        assert_eq!(StateMachine::new(actor.store().clone()).last_applied_time().unwrap().as_millis(), 2_000);
                        let committed = actor.store().snapshot().unwrap();
                        drop(broker);
                        actor.reopen();
                        assert_eq!(actor.store().snapshot().unwrap(), committed);
                    }
                }
            }
        }
    }).await.expect("actual Send observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_send_poison_caches_before_notice_and_held_actual_native_drain() {
    // Normal Send never overlaps submission and ACK. These manually composed
    // slots use actual originals but do not claim ordinary concurrent admission.
    tokio::spawn(async {
        for durable in [false, true] {
            for case in 0..4 {
                let mut actor = Actor::new(durable, false);
                let (mut wire, writes) = gated_wire().await;
                let _release = NativeRelease(Arc::clone(&writes));
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let mut receiver = wire.receiver.take().unwrap();
                let (message, format) = messages(false);
                wire.send(0, message, format).await;
                wire.no_ack_barrier().await;
                let delivery = receiver.recv().await.unwrap();
                let ready = Arc::new(Notify::new());
                let trigger = Arc::new(Notify::new());
                let payload: Arc<str> = Arc::from("qualified Send original drain poison");
                let polls = Arc::new(AtomicUsize::new(0));
                let mut custody = SendCustody::default();
                custody.expected = Some(SendOutcome::Single);
                custody.original = Some(SendIntake::new(PollPoison::new(
                    submission(&actor, false), Arc::clone(&trigger), Arc::clone(&ready),
                    Arc::clone(&payload), Arc::clone(&polls), true,
                )));
                {
                    let mut observed = Box::pin(custody.original.as_mut().unwrap().observe());
                    timeout(WAIT, async { tokio::select! {
                        result = observed.as_mut() => { let _ = result; panic!("actual result retained in injected wrapper") },
                        () = ready.notified() => {},
                    }}).await.unwrap();
                }
                assert_one_submit(&actor, false);
                writes.hold();
                let native_ready = Arc::new(Notify::new());
                let native_release = Arc::new(Notify::new());
                let native_calls = Arc::new(AtomicUsize::new(0));
                let native = match case {
                    0 => NativeSend::new(retain_native_result(receiver.accept(&delivery),
                        Arc::clone(&native_ready), Arc::clone(&native_release), Arc::clone(&native_calls))),
                    1 | 2 => NativeSend::new(retain_native_result(receiver.reject(&delivery,
                        Some(crate::listener::error_for(if case == 1 { AmqpError::NotAllowed } else { AmqpError::InvalidField }, "qualified original rejection".to_owned()))),
                        Arc::clone(&native_ready), Arc::clone(&native_release), Arc::clone(&native_calls))),
                    _ => NativeSend::new(retain_native_result(receiver.close_with_error(
                        crate::listener::unauthorized_error("qualified original Close")),
                        Arc::clone(&native_ready), Arc::clone(&native_release), Arc::clone(&native_calls))),
                };
                if case == 3 { custody.close = Some(native); }
                else { custody.native = Some(native); }
                {
                    let mut observed = Box::pin(async {
                        if case == 3 { custody.close.as_mut().unwrap().observe().await; }
                        else { custody.native.as_mut().unwrap().observe().await; }
                    });
                    timeout(WAIT, async { tokio::select! {
                        () = observed.as_mut() => panic!("actual native original held"),
                        () = writes.reached() => {},
                    }}).await.unwrap();
                }
                assert!(!notice.is_requested());
                trigger.notify_one();
                {
                    let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
                    timeout(WAIT, async { tokio::select! {
                        biased;
                        () = notice.observer() => {},
                        () = finish.as_mut() => panic!("qualified post-native-result gate remains held"),
                    }}).await.unwrap();
                }
                let packet = custody.packet.as_ref().unwrap();
                assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
                assert!(custody.original.is_none());
                assert_eq!(Arc::strong_count(&payload), 2, "raw payload cached outside poisoned original");
                let terminal_polls = polls.load(Ordering::SeqCst);
                notice.request();
                for _ in 0..2 {
                    pending_once(Box::pin(custody.finish_with_retirement(Some(&notice))).as_mut()).await;
                }
                {
                    let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
                    timeout(WAIT, async { tokio::select! {
                        biased;
                        () = native_ready.notified() => {},
                        () = finish.as_mut() => panic!("same native result retention is still held"),
                    }}).await.unwrap();
                }
                assert!(writes.held.load(Ordering::SeqCst));
                timeout(WAIT, wire.connection.shutdown()).await.unwrap().unwrap();
                native_release.notify_one();
                timeout(WAIT, custody.finish_with_retirement(Some(&notice))).await.unwrap();
                let packet = if case == 3 { custody.close_packet.as_ref() } else { custody.native_packet.as_ref() }.unwrap();
                assert!(packet.started && packet.retired && !packet.panicked);
                assert!(matches!(packet.result, Some(Err(amqp::EngineError::Stopped))));
                let address = packet.result.as_ref().unwrap() as *const _ as usize;
                custody.finish_with_retirement(Some(&notice)).await;
                let packet = if case == 3 { custody.close_packet.as_ref() } else { custody.native_packet.as_ref() }.unwrap();
                assert_eq!(packet.result.as_ref().unwrap() as *const _ as usize, address);
                assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
                assert_eq!(native_calls.load(Ordering::SeqCst), 1);
                let result = AssertUnwindSafe(finish_send_with_retirement(
                    &mut custody, Ok(Ok(Some(SendRetirement::Detached))), Some(&notice),
                )).catch_unwind().await;
                assert_payload(result, &payload);
                drop(custody);
                assert_one_submit(&actor, false);
                let committed = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
        }
    }).await.expect("qualified original slots joined");
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_native_poison_notifies_before_begun_opposite_close_finishes() {
    // Injected wrapper poison, not a native engine panic. The ACK/Reject write
    // is real; the additional started Close is a manual opposite-slot original.
    for reject in [false, true] {
        let (mut wire, writes) = gated_wire().await;
        let _release = NativeRelease(Arc::clone(&writes));
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let mut receiver = wire.receiver.take().unwrap();
        let (message, format) = messages(false);
        wire.send(0, message, format).await;
        wire.no_ack_barrier().await;
        let delivery = receiver.recv().await.unwrap();
        writes.hold();
        let trigger = Arc::new(Notify::new());
        let payload: Arc<str> = Arc::from("new original native wrapper poison");
        let polls = Arc::new(AtomicUsize::new(0));
        let mut custody = SendCustody::default();
        custody.native = Some(if reject {
            NativeSend::new(PollPoison::new(
                receiver.reject(&delivery, None),
                Arc::clone(&trigger),
                Arc::new(Notify::new()),
                Arc::clone(&payload),
                Arc::clone(&polls),
                false,
            ))
        } else {
            NativeSend::new(PollPoison::new(
                receiver.accept(&delivery),
                Arc::clone(&trigger),
                Arc::new(Notify::new()),
                Arc::clone(&payload),
                Arc::clone(&polls),
                false,
            ))
        });
        {
            let mut observed = Box::pin(custody.native.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = observed.as_mut() => { let _ = result; panic!("original ACK/Reject write held") },
                () = writes.reached() => {},
            }}).await.unwrap();
        }
        let ready = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        custody.close = Some(NativeSend::new(retain_native_result(
            receiver.close_with_error(crate::listener::unauthorized_error("opposite original")),
            Arc::clone(&ready),
            Arc::clone(&release),
            Arc::clone(&calls),
        )));
        pending_once(
            Box::pin(tokio::task::unconstrained(
                custody.close.as_mut().unwrap().observe(),
            ))
            .as_mut(),
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!notice.is_requested());
        trigger.notify_one();
        {
            let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
            timeout(WAIT, async { tokio::select! {
                biased;
                () = notice.observer() => {},
                () = finish.as_mut() => panic!("opposite original result retention remains held"),
            }}).await.unwrap();
        }
        let packet = custody.native_packet.as_ref().unwrap();
        assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
        assert!(custody.native.is_none());
        assert_eq!(Arc::strong_count(&payload), 2);
        let terminal_polls = polls.load(Ordering::SeqCst);
        notice.request();
        for _ in 0..2 {
            pending_once(Box::pin(custody.finish_with_retirement(Some(&notice))).as_mut()).await;
        }
        {
            let mut finish = Box::pin(custody.finish_with_retirement(Some(&notice)));
            timeout(WAIT, async { tokio::select! {
                biased;
                () = ready.notified() => {},
                () = finish.as_mut() => panic!("same opposite Close result retention is still held"),
            }}).await.unwrap();
        }
        assert!(writes.held.load(Ordering::SeqCst));
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        release.notify_one();
        custody.finish_with_retirement(Some(&notice)).await;
        let packet = custody.close_packet.as_ref().unwrap();
        assert!(packet.started && packet.retired && !packet.panicked);
        assert!(matches!(
            packet.result,
            Some(Err(amqp::EngineError::Stopped))
        ));
        let address = packet.result.as_ref().unwrap() as *const _ as usize;
        custody.finish_with_retirement(Some(&notice)).await;
        assert_eq!(
            custody
                .close_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap() as *const _ as usize,
            address
        );
        assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_payload(
            AssertUnwindSafe(finish_send_with_retirement(
                &mut custody,
                Ok(Ok(Some(SendRetirement::Unauthorized))),
                Some(&notice),
            ))
            .catch_unwind()
            .await,
            &payload,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_original_close_poison_keeps_raw_fault_and_stops_held_write() {
    let (mut wire, writes) = gated_wire().await;
    let _release = NativeRelease(Arc::clone(&writes));
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let receiver = wire.receiver.take().unwrap();
    wire.no_ack_barrier().await;
    writes.hold();
    let trigger = Arc::new(Notify::new());
    let payload: Arc<str> = Arc::from("new original authorization Close wrapper poison");
    let polls = Arc::new(AtomicUsize::new(0));
    let mut custody = SendCustody::default();
    custody.close = Some(NativeSend::new(PollPoison::new(
        receiver.close_with_error(crate::listener::unauthorized_error("expired")),
        Arc::clone(&trigger),
        Arc::new(Notify::new()),
        Arc::clone(&payload),
        Arc::clone(&polls),
        false,
    )));
    {
        let mut observed = Box::pin(custody.close.as_mut().unwrap().observe());
        timeout(WAIT, async { tokio::select! {
            result = observed.as_mut() => { let _ = result; panic!("actual original Close IO held") },
            () = writes.reached() => {},
        }}).await.unwrap();
    }
    assert!(!notice.is_requested());
    let mut cancelled_notice = Box::pin(notice.observer());
    pending_once(cancelled_notice.as_mut()).await;
    drop(cancelled_notice);
    trigger.notify_one();
    custody.finish_with_retirement(Some(&notice)).await;
    assert!(notice.is_requested());
    let packet = custody.close_packet.as_ref().unwrap();
    assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
    assert!(custody.close.is_none());
    assert_eq!(Arc::strong_count(&payload), 2);
    let terminal_polls = polls.load(Ordering::SeqCst);
    let observer = notice.observer();
    let mut borrowed = Box::pin(observer);
    assert!(borrowed.as_mut().now_or_never().is_some());
    drop(borrowed);
    notice.request();
    notice.observer().await;
    assert!(writes.held.load(Ordering::SeqCst));
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        custody.finish_with_retirement(Some(&notice)).await;
    }
    assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
    assert_payload(
        AssertUnwindSafe(finish_send_with_retirement(
            &mut custody,
            Ok(Ok(Some(SendRetirement::Detached))),
            Some(&notice),
        ))
        .catch_unwind()
        .await,
        &payload,
    );
}

async fn late_actual_native_result_without_notice(durable: bool, unknown_link: bool) {
    // Qualified helper: gate the real detached observer, admitting the old
    // handle only in the unknown-link row. This is not normal link admission.
    let mut actor = Actor::new(durable, false);
    let mut wire = IngressWire::new().await;
    let notice = ConnectionRetirementRequest::capture(&wire.connection);
    let mut receiver = wire.receiver.take().unwrap();
    let (message, format) = messages(false);
    wire.send(0, message, format).await;
    let delivery = receiver.recv().await.unwrap();
    let mut replacement = if unknown_link {
        wire.detach().await;
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
    let polled = Arc::new(Notify::new());
    let mut custody = SendCustody::default();
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
        result = pump.as_mut() => { let _ = result; panic!("original native poll precedes selected retirement") },
    }}).await.unwrap();
    if !unknown_link {
        assert!(matches!(
            wire.disposition(0).await.state,
            Some(DeliveryState::Accepted(_))
        ));
    }
    wire.no_ack_barrier().await;
    if !unknown_link {
        wire.detach().await;
        replacement = Some(replacement_receiver(&mut wire).await);
    }
    selected.notify_one();
    let primary = timeout(WAIT, pump.as_mut()).await.unwrap();
    assert!(matches!(&primary, Ok(Ok(Some(SendRetirement::Detached)))));
    drop(pump);
    assert!(matches!(
        finish_send_with_retirement(&mut custody, primary, Some(&notice)).await,
        Ok(Some(SendRetirement::Detached))
    ));
    assert!(!notice.is_requested());
    let packet = custody.native_packet.as_ref().unwrap();
    assert!(packet.started && packet.retired && !packet.panicked);
    if unknown_link {
        assert!(
            matches!(packet.result.as_ref(), Some(Err(amqp::EngineError::InvalidState(detail)))
            if detail == "settlement on an unknown link")
        );
    } else {
        assert!(matches!(packet.result, Some(Ok(()))));
    }
    let address = packet.result.as_ref().unwrap() as *const _ as usize;
    custody.finish_with_retirement(Some(&notice)).await;
    assert_eq!(
        custody
            .native_packet
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap() as *const _ as usize,
        address
    );
    assert!(!notice.is_requested());
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
    assert_one_submit(&actor, false);
    assert_stored(&actor, 1);
    let committed = actor.store().snapshot().unwrap();
    actor.reopen();
    assert_eq!(actor.store().snapshot().unwrap(), committed);
}

#[tokio::test(flavor = "current_thread")]
async fn benign_cached_unstarted_refusal_and_reporting_results_never_notify() {
    tokio::spawn(async {
        let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for durable in [false, true] {
            for unknown_link in [false, true] {
                late_actual_native_result_without_notice(durable, unknown_link).await;
            }
            for close in [false, true] {
                let mut actor = Actor::new(durable, false);
                let (mut wire, writes) = gated_wire().await;
                let _release = NativeRelease(Arc::clone(&writes));
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let receiver = wire.receiver.take().unwrap();
                let authorization = if close {
                    let authorization = authorization();
                    expire(&authorization).await;
                    Some(authorization)
                } else {
                    let (message, format) = messages(false);
                    wire.send(0, message, format).await;
                    wire.no_ack_barrier().await;
                    None
                };
                writes.hold();
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
                let mut original = Box::pin(AssertUnwindSafe(serve_sending_client_with_retirement(
                    receiver, actor.namespace.clone(), actor.entity.clone(),
                    actor.broker.as_ref().unwrap().clone(), authorization, Some(notice.clone()),
                )).catch_unwind());
                let mut observed = Box::pin(std::future::poll_fn(|context| {
                    tracing::dispatcher::with_default(&dispatch, || original.as_mut().poll(context))
                }));
                timeout(WAIT, async { tokio::select! {
                    result = observed.as_mut() => { let _ = result; panic!("ordinary native original held") },
                    () = writes.reached() => {},
                }}).await.unwrap();
                assert!(!notice.is_requested());
                wire.stop().await;
                assert!(matches!(timeout(WAIT, observed.as_mut()).await.unwrap(), Ok(Ok(()))));
                drop(observed);
                drop(original);
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
                assert!(!notice.is_requested());
                assert!(writes.held.load(Ordering::SeqCst));
                if close { assert!(actor.log.lock().unwrap().is_empty()); }
                else { assert_one_submit(&actor, false); }
                assert_stored(&actor, u64::from(!close));
                let committed = actor.store().snapshot().unwrap();
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
            // Actual success/refusal followed by a qualified held result. The
            // Unavailable/unexpected shapes are explicit wrapper-only controls.
            for case in 0..4 {
                let mut actor = Actor::new(durable, matches!(case, 1 | 2));
                let mut wire = IngressWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let ready = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let original_ready = Arc::clone(&ready);
                let original_release = Arc::clone(&release);
                let mut custody = SendCustody::default();
                custody.expected = Some(SendOutcome::Single);
                custody.original = Some(SendIntake::new(async {
                    let actual = submission(&actor, false).await;
                    original_ready.notify_one();
                    original_release.notified().await;
                    match case {
                        2 => Err(BrokerRejection::Unavailable("qualified late unavailable".to_owned())),
                        3 => Ok(CommandOutcome::QueueCreated),
                        _ => actual,
                    }
                }));
                {
                    let mut observed = Box::pin(custody.original.as_mut().unwrap().observe());
                    timeout(WAIT, async { tokio::select! {
                        result = observed.as_mut() => { let _ = result; panic!("qualified original result held") },
                        () = ready.notified() => {},
                    }}).await.unwrap();
                }
                let committed = actor.store().snapshot().unwrap();
                assert_one_submit(&actor, false);
                release.notify_one();
                custody.finish_with_retirement(Some(&notice)).await;
                assert!(!notice.is_requested());
                let packet = custody.packet.as_ref().unwrap();
                assert!(packet.started && packet.retired && !packet.panicked);
                match case {
                    0 => assert!(matches!(packet.result, Some(Ok(CommandOutcome::Sent { .. })))),
                    1 => assert!(matches!(packet.result, Some(Err(BrokerRejection::Refused(BrokerError::SessionRequired))))),
                    2 => assert!(matches!(packet.result, Some(Err(BrokerRejection::Unavailable(_))))),
                    _ => assert!(matches!(packet.result, Some(Ok(CommandOutcome::QueueCreated)))),
                }
                custody.finish_with_retirement(Some(&notice)).await;
                assert!(!notice.is_requested());
                drop(custody);
                assert_eq!(actor.store().snapshot().unwrap(), committed);
                wire.no_ack_barrier().await;
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
            for selected in 0..3 {
                let mut actor = Actor::new(durable, false);
                let mut wire = IngressWire::new().await;
                let notice = ConnectionRetirementRequest::capture(&wire.connection);
                let mut custody = SendCustody::default();
                custody.original = Some(SendIntake::new(submission(&actor, false)));
                timeout(WAIT, custody.original.as_mut().unwrap().observe()).await.unwrap();
                custody.capture_send();
                let diagnostics = Arc::new(AtomicUsize::new(0));
                let dispatch = tracing::Dispatch::new(PanicDiagnostics(Arc::clone(&diagnostics)));
                std::thread::spawn(tracing::callsite::rebuild_interest_cache).join().unwrap();
                let primary = Ok(Ok(match selected {
                    0 => None,
                    1 => Some(SendRetirement::Detached),
                    _ => Some(SendRetirement::Unauthorized),
                }));
                let mut finish = Box::pin(AssertUnwindSafe(
                    finish_send_with_retirement(&mut custody, primary, Some(&notice)),
                ).catch_unwind());
                let result = timeout(WAIT, std::future::poll_fn(|context| {
                    tracing::dispatcher::with_default(&dispatch, || finish.as_mut().poll(context))
                })).await.unwrap();
                if selected == 0 { assert!(result.is_err()); }
                else { assert!(matches!(result, Ok(Ok(Some(_))))); }
                drop(finish);
                assert_eq!(diagnostics.load(Ordering::SeqCst), 1);
                assert!(!notice.is_requested());
                let committed = actor.store().snapshot().unwrap();
                drop(custody);
                wire.no_ack_barrier().await;
                wire.stop().await;
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), committed);
            }
        }
        // Pure terminal-state controls; no backend or ordinary native admission
        // is claimed for these already-completed/injected original states.
        let mut wire = IngressWire::new().await;
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        for captured in [false, true] {
            let mut custody = SendCustody::default();
            custody.native = Some(NativeSend::new(async {
                Err(amqp::EngineError::InvalidState("old completed error".to_owned()))
            }));
            custody.native.as_mut().unwrap().observe().await;
            if captured { custody.capture_native(); }
            custody.finish_with_retirement(Some(&notice)).await;
            assert!(!notice.is_requested());
            assert!(matches!(custody.native_packet.as_ref().unwrap().result,
                Some(Err(amqp::EngineError::InvalidState(_)))));
        }
        for phase in 0..3 {
            let payload: Arc<str> = Arc::from("already observed original poison");
            let raw_payload = Arc::clone(&payload);
            let polls = Arc::new(AtomicUsize::new(0));
            let mut custody = SendCustody::default();
            if phase == 0 {
                custody.original = Some(SendIntake::new(PollWitness {
                    actual: Box::pin(async move { std::panic::panic_any(raw_payload) }),
                    polls: Arc::clone(&polls),
                }));
            } else {
                let native = NativeSend::new(PollWitness {
                    actual: Box::pin(async move { std::panic::panic_any(raw_payload) }),
                    polls: Arc::clone(&polls),
                });
                if phase == 1 { custody.native = Some(native); }
                else { custody.close = Some(native); }
            }
            let primary = AssertUnwindSafe(async {
                if phase == 0 { custody.original.as_mut().unwrap().observe().await; }
                else if phase == 1 { custody.native.as_mut().unwrap().observe().await; }
                else { custody.close.as_mut().unwrap().observe().await; }
            }).catch_unwind().await;
            assert_payload(primary, &payload);
            for _ in 0..2 { custody.finish_with_retirement(Some(&notice)).await; }
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert!(!notice.is_requested());
            if phase == 0 { assert!(custody.packet.as_ref().unwrap().panicked); }
            else if phase == 1 { assert!(custody.native_packet.as_ref().unwrap().panicked); }
            else { assert!(custody.close_packet.as_ref().unwrap().panicked); }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut custody = SendCustody::default();
        let send_calls = Arc::clone(&calls);
        custody.original = Some(SendIntake::new(async move {
            send_calls.fetch_add(1, Ordering::SeqCst);
            panic!("unstarted Send must be discarded")
        }));
        let native_calls = Arc::clone(&calls);
        custody.native = Some(NativeSend::new(async move {
            native_calls.fetch_add(1, Ordering::SeqCst);
            panic!("unstarted native must be discarded")
        }));
        let close_calls = Arc::clone(&calls);
        custody.close = Some(NativeSend::new(async move {
            close_calls.fetch_add(1, Ordering::SeqCst);
            panic!("unstarted Close must be discarded")
        }));
        custody.finish_with_retirement(Some(&notice)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!notice.is_requested());
        assert!(!custody.packet.as_ref().unwrap().started && !custody.packet.as_ref().unwrap().panicked);
        assert!(!custody.native_packet.as_ref().unwrap().started && !custody.native_packet.as_ref().unwrap().panicked);
        assert!(!custody.close_packet.as_ref().unwrap().started && !custody.close_packet.as_ref().unwrap().panicked);
        wire.no_ack_barrier().await;
        wire.stop().await;
    }).await.expect("negative original observer joined");
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_multiple_raw_poisons_preserve_primary_priority_and_notice_identity() {
    // Terminal-state and resolver composition, not real broker/native originals.
    let mut independent = IngressWire::new().await;
    let independent_notice = ConnectionRetirementRequest::capture(&independent.connection);
    for primary_case in 0..4 {
        let mut wire = IngressWire::new().await;
        let notice = ConnectionRetirementRequest::capture(&wire.connection);
        let payloads: [Arc<str>; 3] = [
            Arc::from("first Send drain poison"),
            Arc::from("second native drain poison"),
            Arc::from("third Close drain poison"),
        ];
        let triggers: [Arc<Notify>; 3] = std::array::from_fn(|_| Arc::new(Notify::new()));
        let polls: [Arc<AtomicUsize>; 3] = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let mut custody = SendCustody::default();
        custody.original = Some(SendIntake::new(PollPoison::new(
            std::future::pending::<RawSendResult>(),
            Arc::clone(&triggers[0]),
            Arc::new(Notify::new()),
            Arc::clone(&payloads[0]),
            Arc::clone(&polls[0]),
            false,
        )));
        custody.native = Some(NativeSend::new(PollPoison::new(
            std::future::pending::<Result<(), amqp::EngineError>>(),
            Arc::clone(&triggers[1]),
            Arc::new(Notify::new()),
            Arc::clone(&payloads[1]),
            Arc::clone(&polls[1]),
            false,
        )));
        custody.close = Some(NativeSend::new(PollPoison::new(
            std::future::pending::<Result<(), amqp::EngineError>>(),
            Arc::clone(&triggers[2]),
            Arc::new(Notify::new()),
            Arc::clone(&payloads[2]),
            Arc::clone(&polls[2]),
            false,
        )));
        pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
        pending_once(Box::pin(custody.native.as_mut().unwrap().observe()).as_mut()).await;
        pending_once(Box::pin(custody.close.as_mut().unwrap().observe()).as_mut()).await;
        let mut cancelled_notice = Box::pin(notice.observer());
        pending_once(cancelled_notice.as_mut()).await;
        drop(cancelled_notice);
        assert!(!notice.is_requested());
        if primary_case == 2 {
            // An already-captured native error is higher priority than new
            // drain panic; this injected state is not active admission evidence.
            custody.native = Some(NativeSend::new(async { Err(amqp::EngineError::Stopped) }));
            custody.native.as_mut().unwrap().observe().await;
            custody.capture_native();
        }
        for trigger in &triggers {
            trigger.notify_one();
        }
        custody.finish_with_retirement(Some(&notice)).await;
        assert!(notice.is_requested());
        assert!(!independent_notice.is_requested());
        assert!(custody.packet.as_ref().unwrap().panicked);
        assert!(custody.close_packet.as_ref().unwrap().panicked);
        assert_eq!(Arc::strong_count(&payloads[0]), 2);
        assert_eq!(Arc::strong_count(&payloads[2]), 2);
        if primary_case != 2 {
            assert!(custody.native_packet.as_ref().unwrap().panicked);
            assert_eq!(Arc::strong_count(&payloads[1]), 2);
        }
        let terminal_counts: Vec<_> = polls
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .collect();
        assert_eq!(
            terminal_counts,
            if primary_case == 2 {
                vec![2, 1, 2]
            } else {
                vec![2, 2, 2]
            }
        );
        notice.request();
        for _ in 0..2 {
            custody.finish_with_retirement(Some(&notice)).await;
        }
        assert_eq!(
            polls
                .iter()
                .map(|count| count.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
            terminal_counts
        );
        notice.observer().await;
        timeout(WAIT, wire.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
        let primary_payload: Arc<str> = Arc::from("higher raw outer primary");
        let primary = match primary_case {
            0 => Err(Box::new(Arc::clone(&primary_payload)) as Box<dyn std::any::Any + Send>),
            1 => Ok(Err(amqp::EngineError::InvalidState(
                "higher active primary".to_owned(),
            ))),
            2 => Ok(Ok(None)),
            _ => Ok(Ok(Some(SendRetirement::Detached))),
        };
        let result = AssertUnwindSafe(finish_send_with_retirement(
            &mut custody,
            primary,
            Some(&notice),
        ))
        .catch_unwind()
        .await;
        match primary_case {
            0 => assert_payload(result, &primary_payload),
            1 => assert!(
                matches!(result, Ok(Err(amqp::EngineError::InvalidState(detail))) if detail == "higher active primary")
            ),
            2 => assert!(matches!(result, Ok(Err(amqp::EngineError::Stopped)))),
            _ => assert_payload(result, &payloads[0]),
        }
        // Lower phase payloads remain cached even when a primary/native error wins.
        assert_eq!(Arc::strong_count(&payloads[2]), 2);
        independent.no_ack_barrier().await;
    }
    assert!(!independent_notice.is_requested());
    independent.stop().await;
}
