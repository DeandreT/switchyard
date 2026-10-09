//! Actual native frontiers; no descendant-join or ancestor-abort claim.

#[path = "leaf_fault_tests.rs"]
mod leaf_fault_tests;

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use amqp::{
    Begin, Close, End, Frame, Open, Performative, ProtocolHeader, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::Notify,
    time::timeout,
};

use super::*;
use crate::{
    Broker, BrokerRejection, SharedAccessAuthentication, authorization::ConnectionAuthorization,
};

const LIMIT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct WriteState {
    held: bool,
    reached: bool,
    panic: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<WriteState>,
    changed: Notify,
}

impl WriteGate {
    fn hold(&self) {
        self.state.lock().unwrap().held = true;
    }
    fn panic_held_write(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.held && state.reached);
        state.panic = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
    async fn reached(&self) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.state.lock().unwrap().reached {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("actual native writer reached the held write");
    }
}

struct NativeIo {
    inner: DuplexStream,
    writes: Arc<WriteGate>,
}

impl AsyncRead for NativeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for NativeIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        {
            let mut state = self.writes.state.lock().unwrap();
            if state.held {
                state.reached = true;
                state.waker = Some(cx.waker().clone());
                self.writes.changed.notify_waiters();
                if state.panic {
                    drop(state);
                    panic!("controlled connection native driver panic");
                }
                return Poll::Pending;
            }
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

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn opened() -> (ServerConnection, DuplexStream, Arc<WriteGate>) {
    let (inner, mut peer) = duplex(64 * 1024);
    let writes = Arc::new(WriteGate::default());
    let (connection, ()) = timeout(LIMIT, async {
        tokio::join!(
            ServerConnection::accept(
                NativeIo {
                    inner,
                    writes: Arc::clone(&writes)
                },
                "connection-custody",
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
                write_frame(&mut peer, &frame(0, Performative::Open(Open::new("peer"))))
                    .await
                    .unwrap();
                assert!(matches!(
                    read_frame(&mut peer).await.unwrap(),
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
            }
        )
    })
    .await
    .unwrap();
    (connection.unwrap(), peer, writes)
}

async fn pending_once<T>(future: Pin<&mut impl Future<Output = T>>) {
    let mut future = future;
    assert!(
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

async fn begin(peer: &mut DuplexStream, channel: u16) {
    write_frame(peer, &frame(channel, Performative::Begin(Begin::default())))
        .await
        .unwrap();
}

async fn buffered_begin(peer: &mut DuplexStream) {
    begin(peer, 1).await;
    // The answering unrelated End proves the driver processed the prior Begin
    // into its real incoming queue, without accepting that session.
    write_frame(peer, &frame(900, Performative::End(End::default())))
        .await
        .unwrap();
    assert!(matches!(
        timeout(LIMIT, read_frame(peer)).await.unwrap().unwrap(),
        Frame::Amqp {
            channel: 900,
            performative: Some(Performative::End(_)),
            ..
        }
    ));
}

#[derive(Clone, Default)]
struct NoBroker(Arc<std::sync::atomic::AtomicUsize>);

impl Broker for NoBroker {
    fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            Err(BrokerRejection::Unavailable(
                "unexpected connection broker command".into(),
            ))
        }
    }
    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

fn authorization(deadline: Duration, initial: bool) -> Arc<ConnectionAuthorization> {
    let scope = ResourceScope::namespace("tenant.servicebus.windows.net").unwrap();
    let rule = SharedAccessRule::new(
        "rule",
        scope,
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap();
    let policy = SharedAccessPolicy::new([rule]).unwrap();
    let grant = initial.then(|| policy.authenticate_plain("rule", "secret").unwrap());
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")
            .unwrap()
            .with_authorization_timeout(deadline),
        grant,
    )
}

fn assert_primary(result: std::thread::Result<Result<(), ConnectionError>>, identity: &Arc<()>) {
    let actual = result
        .expect_err("raw primary panic resumes after native cleanup")
        .downcast::<Arc<()>>()
        .unwrap();
    assert!(Arc::ptr_eq(identity, &actual));
}

#[tokio::test(flavor = "current_thread")]
async fn outer_connection_panic_stops_held_writer_and_full_native_queue_before_cleanup() {
    let (connection, _peer, writes) = opened().await;
    writes.hold();
    let mut held_close = Box::pin(connection.close_owned());
    pending_once(held_close.as_mut()).await;
    writes.reached().await;
    // The driver cannot consume commands while its first actual write is held.
    let mut queued = Vec::new();
    for _ in 0..256 {
        // Every admission gets a fresh cooperative budget before its first poll.
        tokio::task::yield_now().await;
        let mut original = Box::pin(connection.close_owned());
        pending_once(original.as_mut()).await;
        queued.push(original);
    }
    tokio::task::yield_now().await;
    let mut overflow = Box::pin(connection.close_owned());
    pending_once(overflow.as_mut()).await;
    let mut custody = ConnectionCustody::new(connection);
    let request = custody.request_handle();
    let fault = PumpFault::new(PumpPoint::Intake);
    let broker = NoBroker::default();
    {
        let mut pump = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(super::super::serve_open_connection(
                    &mut custody,
                    NamespaceName::new("tenant").unwrap(),
                    broker.clone(),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        pending_once(pump.as_mut()).await;
        timeout(LIMIT, async {
            tokio::select! {
                result = pump.as_mut() => panic!("original pump finished before its fault checkpoint: {result:?}"),
                () = fault.reached() => {},
            }
        })
        .await
        .unwrap();
        fault.request();
        let result = timeout(LIMIT, pump.as_mut()).await.unwrap();
        drop(pump);
        custody.record_primary(result);
    }
    assert!(
        !*request.requested.borrow(),
        "outer fault does not lose cleanup before Stop"
    );
    pending_once(Box::pin(custody.finish()).as_mut()).await;
    assert!(
        *request.requested.borrow(),
        "Stop request precedes the pending native join"
    );
    assert!(
        writes.state.lock().unwrap().held,
        "the test never releases native IO"
    );
    assert!(!custody.finished && custody.primary.is_some());
    timeout(LIMIT, custody.finish()).await.unwrap();
    assert!(matches!(custody.shutdown.as_ref(), Some(Ok(()))));
    assert_primary(
        std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result())),
        &fault.identity,
    );
    assert!(held_close.await.is_err());
    for original in queued {
        assert!(original.await.is_err());
    }
    assert!(overflow.await.is_err());
    assert_eq!(broker.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn outer_connection_panic_drains_begun_acceptance_or_timeout_close_without_replacement() {
    for accept in [false, true] {
        let (connection, mut peer, writes) = opened().await;
        if accept {
            buffered_begin(&mut peer).await;
        }
        writes.hold();
        let mut custody = ConnectionCustody::new(connection);
        let fault = PumpFault::new(PumpPoint::Native);
        let auth = (!accept).then(|| authorization(Duration::ZERO, false));
        let mut pump = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(super::super::serve_open_connection(
                    &mut custody,
                    NamespaceName::new("tenant").unwrap(),
                    NoBroker::default(),
                    auth,
                ))
                .catch_unwind(),
            ),
        );
        pending_once(pump.as_mut()).await;
        tokio::select! {
            result = pump.as_mut() => panic!("the original pump finished before its held write: {result:?}"),
            () = writes.reached() => {},
        }
        fault.request();
        let result = timeout(LIMIT, pump.as_mut()).await.unwrap();
        drop(pump);
        custody.record_primary(result);
        assert!(custody.original.as_ref().unwrap().started);
        assert!(custody.original.as_ref().unwrap().packet.is_none());
        pending_once(Box::pin(custody.finish()).as_mut()).await;
        assert!(custody.original.as_ref().unwrap().retired);
        timeout(LIMIT, custody.finish()).await.unwrap();
        assert!(matches!(
            custody.native_error.as_ref(),
            Some(EngineError::Stopped | EngineError::RemoteClosed)
        ));
        assert!(writes.state.lock().unwrap().held);
        assert_primary(
            std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result())),
            &fault.identity,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn outer_connection_panic_retains_ready_session_or_terminal_close_packet_before_adoption() {
    for accept in [false, true] {
        let (connection, mut peer, _writes) = opened().await;
        if accept {
            buffered_begin(&mut peer).await;
        } else {
            write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
                .await
                .unwrap();
        }
        let mut custody = ConnectionCustody::new(connection);
        let fault = PumpFault::new(PumpPoint::Packet);
        let mut pump = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(super::super::serve_open_connection(
                    &mut custody,
                    NamespaceName::new("tenant").unwrap(),
                    NoBroker::default(),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        pending_once(pump.as_mut()).await;
        assert!(matches!(
            timeout(LIMIT, read_frame(&mut peer))
                .await
                .unwrap()
                .unwrap(),
            Frame::Amqp {
                performative: Some(Performative::Begin(_) | Performative::Close(_)),
                ..
            }
        ));
        // Poll the same pump until it installs the real raw packet and pauses.
        let mut advancing = Box::pin(poll_fn(|cx| {
            assert!(pump.as_mut().poll(cx).is_pending());
            if fault.reached.load(std::sync::atomic::Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }));
        timeout(LIMIT, advancing.as_mut()).await.unwrap();
        drop(advancing);
        fault.request();
        let result = timeout(LIMIT, pump.as_mut()).await.unwrap();
        drop(pump);
        custody.record_primary(result);
        assert!(custody.original.as_ref().unwrap().actual.is_none());
        if accept {
            assert!(matches!(
                custody.original.as_ref().unwrap().packet,
                Some(NativePacket::Accepted(Ok(_)))
            ));
        } else {
            assert!(matches!(
                custody.original.as_ref().unwrap().packet,
                Some(NativePacket::Closed(Err(
                    EngineError::Stopped | EngineError::RemoteClosed
                )))
            ));
        }
        timeout(LIMIT, custody.finish()).await.unwrap();
        if accept {
            assert!(matches!(
                custody.original.as_ref().unwrap().packet,
                Some(NativePacket::Accepted(Ok(_)))
            ));
        }
        assert_primary(
            std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result())),
            &fault.identity,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn connection_retirement_keeps_held_grants_and_buffered_begin_preparation_unstarted() {
    for authorization_present in [false, true] {
        for before_poll in [false, true] {
            let (connection, mut peer, _writes) = opened().await;
            let authorization = authorization(
                if before_poll {
                    Duration::ZERO
                } else {
                    Duration::from_secs(20)
                },
                before_poll,
            );
            let row = authorization.grant_write_lock().await;
            let broker = NoBroker::default();
            let mut custody = ConnectionCustody::new(connection);
            let request = custody.request_handle();
            let mut pump = Box::pin(
                AssertUnwindSafe(super::super::serve_open_connection(
                    &mut custody,
                    NamespaceName::new("tenant").unwrap(),
                    broker.clone(),
                    authorization_present.then(|| Arc::clone(&authorization)),
                ))
                .catch_unwind(),
            );
            if !before_poll {
                pending_once(pump.as_mut()).await;
            }
            buffered_begin(&mut peer).await;
            request.request();
            assert!(
                timeout(LIMIT, pump.as_mut())
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            drop(pump);
            assert!(
                custody.original.is_none(),
                "no accept or Close was even prepared after retirement"
            );
            assert_eq!(broker.0.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(
                row.len(),
                if before_poll { 1 } else { 0 },
                "the actual grants row remains held and unchanged"
            );
            custody.record_primary(Ok(Ok(())));
            timeout(LIMIT, custody.finish()).await.unwrap();
            custody.finish_result().unwrap();
            drop(row);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_peer_close_is_answered_before_original_native_shutdown() {
    let (connection, mut peer, _writes) = opened().await;
    let mut custody = ConnectionCustody::new(connection);
    let mut pump = Box::pin(
        AssertUnwindSafe(super::super::serve_open_connection(
            &mut custody,
            NamespaceName::new("tenant").unwrap(),
            NoBroker::default(),
            None,
        ))
        .catch_unwind(),
    );
    pending_once(pump.as_mut()).await;
    write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
        .await
        .unwrap();
    assert!(matches!(
        timeout(LIMIT, read_frame(&mut peer))
            .await
            .unwrap()
            .unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Close(_)),
            ..
        }
    ));
    let result = timeout(LIMIT, pump.as_mut()).await.unwrap();
    drop(pump);
    assert!(result.as_ref().unwrap().is_ok());
    custody.record_primary(result);
    timeout(LIMIT, custody.finish()).await.unwrap();
    assert!(matches!(custody.shutdown.as_ref(), Some(Ok(()))));
    custody.finish_result().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn connection_cleanup_retains_poison_and_primary_precedence_without_repoll() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (connection, _peer, _writes) = opened().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&polls);
    let mut custody = ConnectionCustody::new(connection);
    custody.original = Some(OriginalNative::new(NativeFuture::Close(Box::pin(poll_fn(
        move |_| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                return Poll::Pending;
            }
            panic!("secondary poisoned native primitive")
        },
    )))));
    pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
    let identity = Arc::new(());
    let primary =
        std::panic::catch_unwind(|| std::panic::panic_any(Arc::clone(&identity))).unwrap_err();
    custody.record_primary(Err(primary));
    pending_once(Box::pin(custody.finish()).as_mut()).await;
    assert!(custody.original.as_ref().unwrap().poisoned);
    assert!(custody.original.as_ref().unwrap().packet.is_none());
    timeout(LIMIT, custody.finish()).await.unwrap();
    timeout(LIMIT, custody.finish()).await.unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "a panicked original primitive is terminal, not recovered"
    );
    assert_primary(
        std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result())),
        &identity,
    );
}

#[tokio::test(flavor = "current_thread")]
async fn closed_connection_retirement_watch_is_terminal() {
    let (sender, receiver) = watch::channel(false);
    let mut observer = Box::pin(wait_for_retirement(receiver));
    pending_once(observer.as_mut()).await;
    drop(sender);
    timeout(LIMIT, observer).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn connection_cleanup_keeps_primary_and_original_native_errors_before_secondary_panic() {
    for primary_error in [false, true] {
        let (connection, _peer, writes) = opened().await;
        writes.hold();
        let mut custody = ConnectionCustody::new(connection);
        custody.begin_close(None);
        pending_once(Box::pin(custody.original.as_mut().unwrap().observe()).as_mut()).await;
        writes.reached().await;
        // Precedence primitive with a real original native error, not an
        // injected native result or a whole task-tree panic claim.
        custody.record_primary(Ok(if primary_error {
            Err(EngineError::InvalidState("original primary connection error".into()).into())
        } else {
            Ok(())
        }));
        custody.secondary_panic = Some(Box::new("secondary cleanup panic primitive"));
        pending_once(Box::pin(custody.finish()).as_mut()).await;
        timeout(LIMIT, custody.finish()).await.unwrap();
        assert!(matches!(
            custody.native_error.as_ref(),
            Some(EngineError::Stopped | EngineError::RemoteClosed)
        ));
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result()))
            .expect("retained original errors precede secondary panic")
            .unwrap_err();
        if primary_error {
            assert!(
                matches!(result.downcast_ref::<EngineError>(), Some(EngineError::InvalidState(value)) if value == "original primary connection error")
            );
        } else {
            assert!(matches!(
                result.downcast_ref::<EngineError>(),
                Some(EngineError::Stopped | EngineError::RemoteClosed)
            ));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn connection_primary_fault_precedes_actual_original_driver_panic_shutdown_result() {
    for primary_panic in [false, true] {
        let (connection, mut peer, writes) = opened().await;
        buffered_begin(&mut peer).await;
        writes.hold();
        let mut custody = ConnectionCustody::new(connection);
        let fault = PumpFault::new(PumpPoint::Native);
        let mut pump = Box::pin(
            PUMP_FAULT.scope(
                Arc::clone(&fault),
                AssertUnwindSafe(super::super::serve_open_connection(
                    &mut custody,
                    NamespaceName::new("tenant").unwrap(),
                    NoBroker::default(),
                    None,
                ))
                .catch_unwind(),
            ),
        );
        pending_once(pump.as_mut()).await;
        tokio::select! {
            result = pump.as_mut() => panic!("original native acceptance must reach held IO: {result:?}"),
            () = writes.reached() => {},
        }
        writes.panic_held_write();
        assert!(
            timeout(LIMIT, read_frame(&mut peer))
                .await
                .unwrap()
                .is_err(),
            "actual native IO is dropped after the driver panic"
        );
        if primary_panic {
            fault.request();
        }
        let result = timeout(LIMIT, pump.as_mut()).await.unwrap();
        drop(pump);
        custody.record_primary(result);
        timeout(LIMIT, custody.finish()).await.unwrap();
        assert!(
            matches!(custody.shutdown.as_ref(), Some(Err(ConnectionShutdownError::DriverFailed(value))) if value.contains("controlled connection native driver panic"))
        );
        let returned = std::panic::catch_unwind(AssertUnwindSafe(|| custody.finish_result()));
        if primary_panic {
            assert_primary(returned, &fault.identity);
        } else {
            let error = returned
                .expect("primary typed error is not replaced by native task panic")
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<EngineError>(),
                Some(EngineError::Stopped)
            ));
        }
    }
}
