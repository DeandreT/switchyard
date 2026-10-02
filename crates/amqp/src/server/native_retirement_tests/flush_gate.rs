use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use tokio::io::{DuplexStream, ReadBuf};

use super::*;

#[derive(Clone, Copy)]
enum TargetFlush {
    Retirement { channel: u16, id: u32 },
    FinalTransfer { channel: u16, handle: u32 },
}

impl TargetFlush {
    fn matches(self, frame: &Frame) -> bool {
        match (self, frame) {
            (
                Self::Retirement { channel, id },
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(disposition)),
                    payload,
                },
            ) => {
                channel == *actual
                    && payload.is_empty()
                    && disposition.role == Role::Sender
                    && disposition.first == id
                    && disposition.last.is_none()
                    && !disposition.settled
                    && matches!(disposition.state, Some(DeliveryState::Transactional(_)))
            }
            (
                Self::FinalTransfer { channel, handle },
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Transfer(transfer)),
                    ..
                },
            ) => channel == *actual && handle == transfer.handle && !transfer.more,
            _ => false,
        }
    }
}

#[derive(Default)]
pub(super) struct FlushGate {
    armed: Mutex<Option<TargetFlush>>,
    entered: AtomicBool,
    blocked: AtomicBool,
    failed: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    changed: Notify,
}

impl FlushGate {
    fn arm(&self, target: TargetFlush) {
        self.entered.store(false, Ordering::Release);
        self.failed.store(false, Ordering::Release);
        self.blocked.store(true, Ordering::Release);
        *self.armed.lock().expect("flush target") = Some(target);
    }

    pub(super) fn retirement(&self, channel: u16, id: u32) {
        self.arm(TargetFlush::Retirement { channel, id });
    }

    pub(super) fn final_transfer(&self, channel: u16, handle: u32) {
        self.arm(TargetFlush::FinalTransfer { channel, handle });
    }

    pub(super) async fn wait(&self) {
        bounded("targeted native flush entered", async {
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
        .await;
    }

    pub(super) fn release(&self) {
        self.blocked.store(false, Ordering::Release);
        if let Some(waiter) = self.waiter.lock().expect("flush waiter").take() {
            waiter.wake();
        }
    }

    pub(super) fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.release();
    }
}

pub(super) struct GatedIo {
    pub(super) inner: DuplexStream,
    pub(super) gate: Arc<FlushGate>,
    captured: Vec<u8>,
    matching: Option<bool>,
}

impl GatedIo {
    pub(super) fn new(inner: DuplexStream, gate: Arc<FlushGate>) -> Self {
        Self {
            inner,
            gate,
            captured: Vec::new(),
            matching: None,
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
        if let Poll::Ready(Ok(written)) = result
            && self.gate.armed.lock().expect("flush target").is_some()
        {
            assert!(
                self.captured.len() + written <= 262_144,
                "bounded fixture frame capture"
            );
            self.captured.extend_from_slice(&bytes[..written]);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.matching.is_none() {
            let target = *self.gate.armed.lock().expect("flush target");
            self.matching = Some(target.is_some_and(|target| {
                target.matches(
                    &crate::codec::decode_frame_for_test(&self.captured)
                        .expect("captured complete frame"),
                )
            }));
        }
        if self.matching == Some(true) {
            self.gate.entered.store(true, Ordering::Release);
            self.gate.changed.notify_waiters();
            if self.gate.failed.load(Ordering::Acquire) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected targeted native flush failure",
                )));
            }
            if self.gate.blocked.load(Ordering::Acquire) {
                *self.gate.waiter.lock().expect("flush waiter") = Some(cx.waker().clone());
                if self.gate.failed.load(Ordering::Acquire) {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "injected targeted native flush failure",
                    )));
                }
                if self.gate.blocked.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
            }
        }
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            if self.matching == Some(true) {
                self.gate.armed.lock().expect("flush target").take();
            }
            self.captured.clear();
            self.matching = None;
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
