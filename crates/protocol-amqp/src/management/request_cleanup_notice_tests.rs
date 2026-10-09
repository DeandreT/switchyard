//! Actual request fronts; the post-result poison is a controlled original fault.

use super::*;
use crate::listener::connection_custody::ConnectionRetirementRequest;
use amqp::{
    Attach, Begin, Frame, Open, Performative, ProtocolHeader, SenderSettleMode, ServerConnection,
    ServerSession, Target, Transfer, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use std::{
    io,
    sync::atomic::AtomicUsize,
    task::{Context, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex};

#[derive(Default)]
struct NativeWriteHold {
    held: AtomicBool,
    reached: Notify,
    waker: StdMutex<Option<Waker>>,
}

impl NativeWriteHold {
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct HeldIo {
    actual: DuplexStream,
    hold: Arc<NativeWriteHold>,
}

impl AsyncRead for HeldIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.actual).poll_read(context, bytes)
    }
}

impl AsyncWrite for HeldIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.hold.held.load(Ordering::SeqCst) {
            *self.hold.waker.lock().unwrap() = Some(context.waker().clone());
            self.hold.reached.notify_one();
            return Poll::Pending;
        }
        Pin::new(&mut self.actual).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.actual).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.actual).poll_shutdown(context)
    }
}

struct NativeRequest {
    connection: ServerConnection,
    _session: ServerSession,
    _peer: DuplexStream,
    hold: Arc<NativeWriteHold>,
}

fn native_frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn native_control(peer: &mut DuplexStream, expected: u16) -> Performative {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("actual native control")
    };
    assert_eq!(channel, expected);
    assert!(payload.is_empty());
    performative
}

impl NativeRequest {
    async fn new(message: &Message) -> (Self, Receiver, amqp::Delivery) {
        let (actual, mut peer) = duplex(128 * 1024);
        let hold = Arc::new(NativeWriteHold::default());
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(
                    HeldIo {
                        actual,
                        hold: Arc::clone(&hold)
                    },
                    "request-cleanup-server",
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
                        &native_frame(0, Performative::Open(Open::new("request-cleanup-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(
                        native_control(&mut peer, 0).await,
                        Performative::Open(_)
                    ));
                }
            )
        })
        .await
        .unwrap();
        let mut connection = connection.unwrap();
        write_frame(
            &mut peer,
            &native_frame(1, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let mut session = timeout(WAIT, connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            native_control(&mut peer, 1).await,
            Performative::Begin(_)
        ));
        write_frame(
            &mut peer,
            &native_frame(
                1,
                Performative::Attach(Box::new(Attach {
                    name: "request-cleanup-link".to_owned(),
                    handle: 1,
                    role: Role::Sender,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: None,
                    target: Some(Target::new("orders/$management")),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: Some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
            ),
        )
        .await
        .unwrap();
        let attach = timeout(WAIT, session.next_incoming_attach())
            .await
            .unwrap()
            .unwrap();
        let endpoint = timeout(WAIT, session.accept_attach(attach, 128 * 1024))
            .await
            .unwrap()
            .unwrap();
        let LinkEndpoint::Receiver(mut receiver) = endpoint else {
            panic!("actual native receiver")
        };
        assert!(matches!(
            native_control(&mut peer, 1).await,
            Performative::Attach(_)
        ));
        assert!(matches!(
            native_control(&mut peer, 1).await,
            Performative::Flow(_)
        ));
        write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Transfer(Transfer {
                    handle: 1,
                    delivery_id: Some(1),
                    delivery_tag: Some(Binary::from(vec![1])),
                    message_format: Some(0),
                    settled: Some(false),
                    more: false,
                    rcv_settle_mode: None,
                    state: None,
                    resume: false,
                    aborted: false,
                    batchable: false,
                })),
                payload: encode_message(message).unwrap(),
            },
        )
        .await
        .unwrap();
        let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
        assert_eq!(
            delivery.message().properties.as_ref().unwrap().message_id,
            Some(MessageId::Ulong(87))
        );
        (
            Self {
                connection,
                _session: session,
                _peer: peer,
                hold,
            },
            receiver,
            delivery,
        )
    }
}

impl Drop for NativeRequest {
    fn drop(&mut self) {
        self.hold.release();
        self.connection.stop();
    }
}

fn counted<F: Future>(actual: F, polls: Arc<AtomicUsize>) -> impl Future<Output = F::Output> {
    let mut original = Box::pin(actual);
    poll_fn(move |context| {
        polls.fetch_add(1, Ordering::SeqCst);
        original.as_mut().poll(context)
    })
}

struct PostResultFault {
    reached: Notify,
    trigger: Notify,
    polls: AtomicUsize,
    payload: Arc<str>,
}

impl PostResultFault {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            reached: Notify::new(),
            trigger: Notify::new(),
            polls: AtomicUsize::new(0),
            payload: Arc::from("controlled original management request fault"),
        })
    }
}

#[derive(Clone)]
struct PoisonAfterActualResult {
    actual: ActualBroker,
    fault: Arc<PostResultFault>,
}

impl Broker for PoisonAfterActualResult {
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = RawResult> + Send {
        let fault = Arc::clone(&self.fault);
        let polls = Arc::clone(&fault);
        let mut original: Pin<Box<dyn Future<Output = RawResult> + Send + '_>> =
            Box::pin(async move {
                let _raw = self.actual.submit(namespace, entity, kind).await;
                fault.reached.notify_one();
                fault.trigger.notified().await;
                std::panic::panic_any(Arc::clone(&fault.payload))
            });
        poll_fn(move |context| {
            polls.polls.fetch_add(1, Ordering::SeqCst);
            original.as_mut().poll(context)
        })
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

async fn actual_request_cleanup_fault(durable: bool) {
    for after in [false, true] {
        let mut actor = Actor::new(durable, true);
        let hold = actor.accept();
        let management = ConnectionManagement::new();
        install_hold(&management, &actor, hold.clone()).await;
        let (_route, mut responses) = management
            .register_reply_route("panic-reply".to_owned())
            .await;
        actor.gate.arm(
            domain::keys::session(&actor.namespace, &actor.entity, &hold.session_id),
            true,
        );
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual management receiver")
        };
        let notice = wire.retirement_request();
        let (mut independent, _) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        independent.barrier().await;
        let fault = PostResultFault::new();
        wire.request_message(&correlated(state_request())).await;
        wire.barrier().await;
        let mut original = Box::pin(
            AssertUnwindSafe(serve_management_requests_with_retirement(
                receiver,
                actor.namespace.clone(),
                actor.entity.clone(),
                PoisonAfterActualResult {
                    actual: actor.actual.as_ref().unwrap().clone(),
                    fault: Arc::clone(&fault),
                },
                Arc::clone(&management),
                None,
                Some(notice.clone()),
            ))
            .catch_unwind(),
        );
        timeout(WAIT, async {
            tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("actual apply is held") },
                () = actor.gate.reached(false) => {},
            }
        })
        .await
        .unwrap();
        if after {
            actor.gate.release(false);
            actor.gate.reached(true).await;
        }
        wire.detach().await;
        // Cancel observers of the same whole pump, not its retained original.
        for _ in 0..2 {
            pending_once(Box::pin(tokio::task::unconstrained(original.as_mut())).as_mut()).await;
            assert!(!notice.is_requested());
        }
        if !after {
            actor.gate.release(false);
            actor.gate.reached(true).await;
        }
        assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
        actor.gate.release(true);
        timeout(WAIT, async {
            tokio::select! {
                result = original.as_mut() => { let _ = result; panic!("raw result waits at original poison frontier") },
                () = fault.reached.notified() => {},
            }
        })
        .await
        .unwrap();
        let raw = actor.submissions.lock().unwrap()[0].result.clone().unwrap();
        assert_eq!(raw, Ok(CommandOutcome::SessionStateSet));
        install_hold(&management, &actor, hold.clone()).await;
        let replacement = management.session(LINK).await.unwrap();
        let final_polls = fault.polls.load(Ordering::SeqCst);
        assert!(!notice.is_requested());
        fault.trigger.notify_one();
        let ((), result) = timeout(WAIT, async {
            tokio::join!(notice.observer(), original.as_mut())
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            result.unwrap_err().downcast_ref::<Arc<str>>().unwrap(),
            &fault.payload,
        ));
        assert!(fault.polls.load(Ordering::SeqCst) > final_polls);
        let terminal_polls = fault.polls.load(Ordering::SeqCst);
        drop(original);
        notice.request();
        timeout(WAIT, notice.observer()).await.unwrap();
        assert_eq!(fault.polls.load(Ordering::SeqCst), terminal_polls);
        assert_eq!(management.session(LINK).await, Some(replacement));
        assert!(matches!(
            responses.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        actor.assert_one(
            CommandKind::SetSessionState {
                session: hold.clone(),
                state: STATE.to_vec(),
            },
            raw,
        );
        let committed = actor.snapshot();
        wire.stop().await;
        actor.reopen(&committed);
        assert_eq!(actor.session().state, STATE);
        assert_eq!(actor.session().lock.unwrap().token, hold.token);
        notice.request();
        independent.barrier().await;
        independent.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_cleanup_fault_after_held_apply_memory() {
    actual_request_cleanup_fault(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_cleanup_fault_after_held_apply_fjall() {
    actual_request_cleanup_fault(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_actual_request_poison_stops_begun_ack_or_reject_without_releasing_write() {
    // Ordinary request admission does not start these two slots concurrently.
    for durable in [false, true] {
        for reject in [false, true] {
            let mut actor = Actor::new(durable, true);
            let hold = actor.accept();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold.clone()).await;
            let message = correlated(state_request());
            let (mut native, receiver, delivery) = NativeRequest::new(&message).await;
            let notice = ConnectionRetirementRequest::capture(&native.connection);
            let fault = PostResultFault::new();
            let broker = RequestBroker::new(PoisonAfterActualResult {
                actual: actor.actual.as_ref().unwrap().clone(),
                fault: Arc::clone(&fault),
            });
            let native_control = OperationControl::new();
            let native_polls = Arc::new(AtomicUsize::new(0));
            let mut custody = RequestCustody::default();
            custody.request = Some(PendingOperation::new(
                process_request(
                    &message,
                    MessageId::Ulong(87),
                    &actor.namespace,
                    &actor.entity,
                    &broker,
                    &management,
                    None,
                ),
                broker.control(),
            ));
            let mut request = Box::pin(custody.request.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = request.as_mut() => { let _ = result; panic!("actual result waits at poison frontier") },
                () = fault.reached.notified() => {},
            }}).await.unwrap();
            drop(request);
            let raw = actor.submissions.lock().unwrap()[0].result.clone().unwrap();
            assert_eq!(raw, Ok(CommandOutcome::SessionStateSet));
            custody.native = Some(PendingOperation::new(
                counted(
                    async {
                        assert!(native_control.begin());
                        if reject {
                            receiver.reject(&delivery, None).await
                        } else {
                            receiver.accept(&delivery).await
                        }
                    },
                    Arc::clone(&native_polls),
                ),
                native_control.clone(),
            ));
            native.hold.held.store(true, Ordering::SeqCst);
            let mut acknowledgement = Box::pin(custody.native.as_mut().unwrap().observe());
            timeout(WAIT, async { tokio::select! {
                result = acknowledgement.as_mut() => { let _ = result; panic!("actual ACK/Reject write held") },
                () = native.hold.reached.notified() => {},
            }}).await.unwrap();
            drop(acknowledgement);
            assert!(native_control.started());
            assert!(!notice.is_requested());
            for _ in 0..2 {
                pending_once(
                    Box::pin(tokio::task::unconstrained(custody.finish_with_retirement(
                        receiver.on_detach_owned(),
                        Some(&notice),
                    )))
                    .as_mut(),
                )
                .await;
                assert!(custody.request_packet.is_none() && custody.native_packet.is_none());
                assert!(!notice.is_requested());
            }
            fault.trigger.notify_one();
            timeout(WAIT, async {
                tokio::join!(
                    notice.observer(),
                    custody.finish_with_retirement(receiver.on_detach_owned(), Some(&notice),)
                );
            })
            .await
            .unwrap();
            assert!(native.hold.held.load(Ordering::SeqCst));
            // No explicit Stop or gate release: the captured notice interrupts native IO.
            timeout(WAIT, native.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
            assert!(native.hold.held.load(Ordering::SeqCst));
            let packet = custody.request_packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && packet.panicked && packet.result.is_none());
            let packet = custody.native_packet.as_ref().unwrap();
            assert!(packet.started && packet.retired && !packet.panicked);
            let retained = packet.result.as_ref().unwrap().as_ref().unwrap_err();
            let pointer = retained as *const EngineError;
            let description = retained.to_string();
            let kind = std::mem::discriminant(retained);
            let terminal_request_polls = fault.polls.load(Ordering::SeqCst);
            let terminal_native_polls = native_polls.load(Ordering::SeqCst);
            for _ in 0..2 {
                timeout(
                    WAIT,
                    custody.finish_with_retirement(receiver.on_detach_owned(), Some(&notice)),
                )
                .await
                .unwrap();
                let retained = custody
                    .native_packet
                    .as_ref()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .unwrap_err();
                assert_eq!(retained as *const EngineError, pointer);
                assert_eq!(fault.polls.load(Ordering::SeqCst), terminal_request_polls);
                assert_eq!(native_polls.load(Ordering::SeqCst), terminal_native_polls);
                notice.request();
            }
            let cleanup = custody.take_cleanup_panic().unwrap();
            assert!(Arc::ptr_eq(
                cleanup.downcast_ref::<Arc<str>>().unwrap(),
                &fault.payload
            ));
            let native_error = custody.take_native_error().unwrap();
            assert_eq!(std::mem::discriminant(&native_error), kind);
            let selected = finish_pump(Ok(Ok(())), Some(cleanup), Some(native_error)).unwrap_err();
            assert_eq!(
                selected.downcast_ref::<EngineError>().unwrap().to_string(),
                description
            );
            actor.assert_one(
                CommandKind::SetSessionState {
                    session: hold.clone(),
                    state: STATE.to_vec(),
                },
                raw,
            );
            let committed = actor.snapshot();
            drop(custody);
            drop(broker);
            drop(native);
            actor.reopen(&committed);
            assert_eq!(actor.session().state, STATE);
            assert_eq!(actor.session().lock.unwrap().token, hold.token);
        }
    }
}

struct PanicReport {
    reached: Arc<AtomicUsize>,
    payload: Arc<str>,
}

impl tracing::Subscriber for PanicReport {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::management::custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

async fn request_status_and_report_are_non_faults(durable: bool) {
    let _other_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    for refused in [false, true] {
        let mut actor = Actor::new(durable, true);
        let hold = actor.accept();
        let management = ConnectionManagement::new();
        install_hold(&management, &actor, hold.clone()).await;
        if refused {
            actor
                .clock
                .set(actor.session().lock.unwrap().locked_until.as_millis());
        }
        let broker = actor.request_broker();
        let message = state_request();
        let mut custody = RequestCustody::default();
        custody.request = Some(PendingOperation::new(
            process_request(
                &message,
                MessageId::Ulong(87),
                &actor.namespace,
                &actor.entity,
                &broker,
                &management,
                None,
            ),
            broker.control(),
        ));
        let raw = paused_broker_result(custody.request.as_mut().unwrap(), &actor).await;
        assert_eq!(raw.is_err(), refused);
        let (mut wire, endpoint) = Wire::new(Role::Sender, 0, ReceiverSettleMode::First).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("actual receiver")
        };
        let notice: ConnectionRetirementRequest = wire.retirement_request();
        actor.result_gate.release();
        timeout(
            WAIT,
            custody.finish_with_retirement(receiver.on_detach_owned(), Some(&notice)),
        )
        .await
        .unwrap();
        let packet = custody.request_packet.as_ref().unwrap();
        assert!(packet.started && packet.retired && !packet.panicked);
        assert_eq!(packet.result.as_ref().unwrap().status_code == 200, !refused);
        let pointer = packet.result.as_ref().unwrap() as *const ManagementResponse;
        assert!(!notice.is_requested());
        let reached = Arc::new(AtomicUsize::new(0));
        let payload: Arc<str> = Arc::from("controlled report-only management fault");
        let dispatch = tracing::Dispatch::new(PanicReport {
            reached: Arc::clone(&reached),
            payload: Arc::clone(&payload),
        });
        std::thread::spawn(tracing::callsite::rebuild_interest_cache)
            .join()
            .unwrap();
        // This invokes the production finish guard, not whole-link admission.
        let mut finish = Box::pin(
            AssertUnwindSafe(finish_request_with_retirement(
                &mut custody,
                &receiver,
                Ok(Ok(None)),
                Some(&notice),
            ))
            .catch_unwind(),
        );
        let result = timeout(
            WAIT,
            poll_fn(|context| {
                tracing::dispatcher::with_default(&dispatch, || finish.as_mut().poll(context))
            }),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            result.unwrap_err().downcast_ref::<Arc<str>>().unwrap(),
            &payload
        ));
        drop(finish);
        assert_eq!(reached.load(Ordering::SeqCst), 1);
        assert!(!notice.is_requested());
        assert_eq!(
            custody
                .request_packet
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap() as *const ManagementResponse,
            pointer
        );
        wire.barrier().await;
        actor.assert_one(
            CommandKind::SetSessionState {
                session: hold,
                state: STATE.to_vec(),
            },
            raw,
        );
        let committed = actor.snapshot();
        drop(custody);
        drop(broker);
        wire.stop().await;
        actor.reopen(&committed);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_status_and_report_do_not_notice_memory() {
    request_status_and_report_are_non_faults(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_request_status_and_report_do_not_notice_fjall() {
    request_status_and_report_are_non_faults(true).await;
}
