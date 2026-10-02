use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use amqp::{Frame, Performative, read_frame};
use futures_util::FutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::Notify,
};

const MAX_CAPTURE_BYTES: usize = 64 * 1024;

#[derive(Default)]
struct GateState {
    channel: Option<u16>,
    entered: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
pub(super) struct BeginGate {
    state: Mutex<GateState>,
    changed: Notify,
}

impl BeginGate {
    pub(super) fn block(&self, channel: u16) {
        let mut state = self.state.lock().expect("Begin flush gate");
        assert!(state.channel.is_none());
        state.channel = Some(channel);
        state.entered = false;
    }

    pub(super) async fn entered(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().expect("Begin flush gate").entered {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn release(&self) {
        let waker = {
            let mut state = self.state.lock().expect("Begin flush gate");
            state.channel = None;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn armed(&self) -> bool {
        self.state
            .lock()
            .expect("Begin flush gate")
            .channel
            .is_some()
    }

    fn poll_flush(&self, cx: &mut Context<'_>, bytes: &[u8]) -> bool {
        let mut state = self.state.lock().expect("Begin flush gate");
        let Some(channel) = state.channel else {
            return false;
        };
        if !state.entered && !contains_begin(bytes, channel) {
            return false;
        }
        state.entered = true;
        state.waker = Some(cx.waker().clone());
        drop(state);
        self.changed.notify_waiters();
        true
    }
}

fn contains_begin(mut bytes: &[u8], channel: u16) -> bool {
    let mut matched = false;
    while !bytes.is_empty() {
        let frame = read_frame(&mut bytes)
            .now_or_never()
            .expect("a complete in-memory frame cannot wait")
            .expect("native output is a valid complete frame");
        matched |= matches!(frame, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Begin(_)),
            payload,
        } if actual == channel && payload.is_empty());
    }
    matched
}

pub(super) struct GatedIo {
    inner: DuplexStream,
    gate: Arc<BeginGate>,
    accepted: Vec<u8>,
}

impl GatedIo {
    pub(super) fn new(inner: DuplexStream, gate: Arc<BeginGate>) -> Self {
        Self {
            inner,
            gate,
            accepted: Vec::new(),
        }
    }
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, bytes)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result
            && self.gate.armed()
        {
            assert!(self.accepted.len() + count <= MAX_CAPTURE_BYTES);
            self.accepted.extend_from_slice(&bytes[..count]);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.gate.poll_flush(cx, &self.accepted) {
            return Poll::Pending;
        }
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.accepted.clear();
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
