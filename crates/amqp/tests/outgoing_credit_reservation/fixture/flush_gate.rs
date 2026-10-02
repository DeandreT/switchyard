use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    task::{Context, Poll, Waker},
};

use amqp::{Frame, Performative, read_frame};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::Notify;

const MAX_CAPTURED_BYTES: usize = 4_096;

pub(in super::super) struct FlushGate {
    armed: AtomicBool,
    reached: AtomicBool,
    released: AtomicBool,
    channel: AtomicU16,
    changed: Notify,
    waiter: Mutex<Option<Waker>>,
}

impl FlushGate {
    pub(super) fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            reached: AtomicBool::new(false),
            released: AtomicBool::new(false),
            channel: AtomicU16::new(0),
            changed: Notify::new(),
            waiter: Mutex::new(None),
        }
    }

    pub(in super::super) fn arm(&self, channel: u16) {
        assert!(!self.reached.load(Ordering::Acquire));
        self.channel.store(channel, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }

    pub(in super::super) async fn wait(&self) {
        super::bounded(async {
            while !self.reached.load(Ordering::Acquire) {
                self.changed.notified().await;
            }
        })
        .await;
    }

    pub(in super::super) fn release(&self) {
        self.released.store(true, Ordering::Release);
        let waiter = self.waiter.lock().expect("flush gate mutex").take();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn block(&self, cx: &Context<'_>) -> bool {
        let mut waiter = self.waiter.lock().expect("flush gate mutex");
        if self.released.load(Ordering::Acquire) {
            waiter.take();
            return false;
        }
        *waiter = Some(cx.waker().clone());
        true
    }
}

pub(super) struct GatedIo {
    io: DuplexStream,
    gate: Option<Arc<FlushGate>>,
    written: Vec<u8>,
}

impl GatedIo {
    pub(super) fn new(io: DuplexStream, gate: Option<Arc<FlushGate>>) -> Self {
        Self {
            io,
            gate,
            written: Vec::new(),
        }
    }
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result
            && self
                .gate
                .as_ref()
                .is_some_and(|gate| gate.armed.load(Ordering::Acquire))
        {
            if self.written.len().saturating_add(count) > MAX_CAPTURED_BYTES {
                return Poll::Ready(Err(io::Error::other(
                    "test flush capture exceeded its bound",
                )));
            }
            self.written.extend_from_slice(&bytes[..count]);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(gate) = self.gate.as_ref() {
            if gate.armed.load(Ordering::Acquire) {
                let frame = {
                    let mut bytes = self.written.as_slice();
                    let mut reading = std::pin::pin!(read_frame(&mut bytes));
                    match reading.as_mut().poll(cx) {
                        Poll::Ready(result) => result?,
                        Poll::Pending => {
                            return Poll::Ready(Err(io::Error::other(
                                "incomplete captured native frame",
                            )));
                        }
                    }
                };
                let Frame::Amqp {
                    channel,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                    ..
                } = frame
                else {
                    return Poll::Ready(Err(io::Error::other("flush gate expected a typed Flow")));
                };
                if channel != gate.channel.load(Ordering::Acquire)
                    || flow.handle.is_some()
                    || !payload.is_empty()
                {
                    return Poll::Ready(Err(io::Error::other(
                        "flush gate expected a session Flow",
                    )));
                }
                gate.armed.store(false, Ordering::Release);
                gate.reached.store(true, Ordering::Release);
                gate.changed.notify_one();
            }
            if gate.reached.load(Ordering::Acquire) && gate.block(cx) {
                return Poll::Pending;
            }
        }
        self.written.clear();
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
