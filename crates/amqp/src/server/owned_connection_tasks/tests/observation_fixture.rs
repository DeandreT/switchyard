use std::{
    any::Any,
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::Notify,
};

use super::{NegotiatedConnection, PairOwner, fixture, locked};
use crate::{
    Close, Frame, Performative, ServerConnection, ServerPeerCloseObservation,
    server::{ConnectionOptions, NativeIngressPolicy, connection_launch},
};

pub(super) const DEADLINE: Duration = Duration::from_secs(2);
pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) struct PrivateWriteCause(pub(super) Arc<()>);
impl std::fmt::Debug for PrivateWriteCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("private write cause rendered")
    }
}
impl std::fmt::Display for PrivateWriteCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("private write cause rendered")
    }
}
impl std::error::Error for PrivateWriteCause {}

pub(super) struct WritePanic(pub(super) Arc<()>);

pub(super) struct WriteControl {
    mode: AtomicU8,
    entered: AtomicBool,
    changed: Notify,
    waker: Mutex<Option<Waker>>,
    pub(super) marker: Arc<()>,
}

impl WriteControl {
    fn new() -> Self {
        Self {
            mode: AtomicU8::new(0),
            entered: AtomicBool::new(false),
            changed: Notify::new(),
            waker: Mutex::new(None),
            marker: Arc::new(()),
        }
    }

    pub(super) fn arm(&self, mode: u8) {
        self.mode.store(mode, Ordering::Release);
    }

    pub(super) async fn entered(&self) -> Result<(), tokio::time::error::Elapsed> {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        })
        .await
    }

    pub(super) fn release(&self) {
        self.mode.store(0, Ordering::Release);
        if let Some(waker) = locked(&self.waker).take() {
            waker.wake();
        }
    }

    fn poll(&self, cx: &Context<'_>) -> Option<Poll<io::Result<usize>>> {
        let mode = self.mode.load(Ordering::Acquire);
        if mode == 0 {
            return None;
        }
        self.entered.store(true, Ordering::Release);
        self.changed.notify_waiters();
        match mode {
            1 => {
                *locked(&self.waker) = Some(cx.waker().clone());
                if self.mode.load(Ordering::Acquire) == 0 {
                    cx.waker().wake_by_ref();
                }
                Some(Poll::Pending)
            }
            2 => Some(Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                PrivateWriteCause(self.marker.clone()),
            )))),
            3 => std::panic::panic_any(WritePanic(self.marker.clone())),
            _ => unreachable!("private transport mode"),
        }
    }
}

struct ControlledIo {
    inner: DuplexStream,
    control: Arc<WriteControl>,
}

impl AsyncRead for ControlledIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ControlledIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(result) = self.control.poll(cx) {
            return result;
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

pub(super) async fn open<A>(
    owner: &mut PairOwner<A>,
) -> TestResult<(ServerConnection, DuplexStream, Arc<WriteControl>)> {
    let opened: Result<TestResult<_>, _> = caught(async {
        let (inner, mut peer) = tokio::io::duplex(64 * 1024);
        let control = Arc::new(WriteControl::new());
        let stream = ControlledIo {
            inner,
            control: control.clone(),
        };
        let (negotiated, greeted) = tokio::time::timeout(DEADLINE, async {
            tokio::join!(
                connection_launch::negotiate(
                    stream,
                    "observed-server",
                    None,
                    ConnectionOptions::default(),
                    NativeIngressPolicy::Disabled
                ),
                fixture::hello(&mut peer)
            )
        })
        .await?;
        greeted?;
        let connection = owner
            .launch(negotiated?)
            .unwrap_or_else(|_| panic!("fresh original owner refused"));
        Ok((connection, peer, control))
    })
    .await;
    if opened.as_ref().is_ok_and(Result::is_ok) {
        return resume(opened);
    }
    let report = owner.finish().await;
    let _original_report = &report;
    resume(opened)
}

pub(super) async fn launch<A>(
    owner: &mut PairOwner<A>,
    negotiated: NegotiatedConnection<fixture::MarkedIo>,
) -> ServerConnection {
    match caught(async { fixture::launch(owner, negotiated) }).await {
        Ok(connection) => connection,
        Err(payload) => {
            if let Some(gate) = &owner.controls.before_reader_poll {
                gate.release();
            }
            let report = owner.finish().await;
            let _original_report = &report;
            std::panic::resume_unwind(payload)
        }
    }
}

pub(super) async fn send_close(
    peer: &mut DuplexStream,
    close: Close,
    channel: u16,
    payload: Vec<u8>,
) -> TestResult {
    tokio::time::timeout(
        DEADLINE,
        crate::write_frame(
            peer,
            &Frame::Amqp {
                channel,
                performative: Some(Performative::Close(close)),
                payload,
            },
        ),
    )
    .await??;
    Ok(())
}

pub(super) async fn read_close(peer: &mut DuplexStream) -> TestResult {
    match tokio::time::timeout(DEADLINE, crate::read_frame(peer)).await?? {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(_)),
            payload,
        } if payload.is_empty() => Ok(()),
        _ => Err(io::Error::other("expected original close response").into()),
    }
}

pub(super) async fn received<A>(owner: &PairOwner<A>) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(DEADLINE, async {
        while owner.peer_close.received().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
}

pub(super) fn original_reader_abort<A>(owner: &PairOwner<A>) -> tokio::task::AbortHandle {
    match &*locked(&owner.reader.state) {
        super::State::Pending(handle) => handle.abort_handle(),
        _ => panic!("original Reader not rooted"),
    }
}

pub(super) fn same_write_cause(receipt: &ServerPeerCloseObservation, marker: &Arc<()>) -> bool {
    receipt
        .reply_result()
        .and_then(|result| result.as_ref().err())
        .and_then(io::Error::get_ref)
        .and_then(|cause| cause.downcast_ref::<PrivateWriteCause>())
        .is_some_and(|cause| Arc::ptr_eq(&cause.0, marker))
}

pub(super) async fn caught<F: Future>(future: F) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut future = pin!(future);
    poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

pub(super) fn resume<T>(observed: Result<T, Box<dyn Any + Send>>) -> T {
    match observed {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
