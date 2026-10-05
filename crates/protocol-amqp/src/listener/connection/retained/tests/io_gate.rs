use super::*;
use futures_util::task::AtomicWaker;
use std::{fmt, sync::Condvar};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

#[derive(Default)]
pub(super) struct PayloadWitness {
    pub(super) drops: AtomicUsize,
    pub(super) panic_on_drop: AtomicBool,
}

pub(super) struct PayloadError {
    pub(super) witness: Arc<PayloadWitness>,
    pub(super) name: &'static str,
}

impl fmt::Debug for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}
impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}
impl Error for PayloadError {}
impl Drop for PayloadError {
    fn drop(&mut self) {
        self.witness.drops.fetch_add(1, Ordering::SeqCst);
        if self.witness.panic_on_drop.load(Ordering::SeqCst) {
            panic!("post-barrier payload disposal");
        }
    }
}

#[derive(Default)]
pub(super) struct BlockGate {
    blocked: AtomicBool,
    entered: AtomicBool,
    released: std::sync::Mutex<bool>,
    changed: Condvar,
}

impl BlockGate {
    pub(super) fn arm(&self) {
        self.blocked.store(true, Ordering::SeqCst);
    }
    pub(super) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
    fn pause(&self) {
        if !self.blocked.load(Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*released {
            released = self
                .changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
    pub(super) fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
pub(super) struct ShutdownGate {
    pub(super) blocked: AtomicBool,
    pub(super) entered: AtomicBool,
    released: AtomicBool,
    waker: AtomicWaker,
}

impl ShutdownGate {
    pub(super) fn pulse(&self) {
        self.waker.wake();
    }
    pub(super) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.waker.wake();
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.blocked.load(Ordering::SeqCst) || self.released.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        self.waker.register(cx.waker());
        self.entered.store(true, Ordering::SeqCst);
        if self.released.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[derive(Default)]
pub(super) struct IoPlan {
    pub(super) drop_gate: BlockGate,
    pub(super) shutdown_gate: ShutdownGate,
    pub(super) dropped: AtomicUsize,
    pub(super) shutdown_error: std::sync::Mutex<Option<Arc<PayloadWitness>>>,
    pub(super) read_error: std::sync::Mutex<Option<Arc<PayloadWitness>>>,
    pub(super) drop_panic: std::sync::Mutex<Option<Arc<PayloadWitness>>>,
    pub(super) refused_seen_before_drop: AtomicBool,
    pub(super) controls: std::sync::Mutex<Option<Arc<Controls>>>,
}

impl IoPlan {
    pub(super) fn release(&self) {
        self.drop_gate.release();
        self.shutdown_gate.release();
    }
}

pub(super) struct GatedIo {
    pub(super) inner: DuplexStream,
    pub(super) plan: Arc<IoPlan>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let error = crate::listener::retained_connection::locked(&self.plan.read_error).take();
        if let Some(witness) = error {
            return Poll::Ready(Err(io::Error::other(PayloadError {
                witness,
                name: "private-read-cause",
            })));
        }
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.plan.shutdown_gate.poll(cx).is_pending() {
            return Poll::Pending;
        }
        let error = crate::listener::retained_connection::locked(&self.plan.shutdown_error).take();
        if let Some(witness) = error {
            return Poll::Ready(Err(io::Error::other(PayloadError {
                witness,
                name: "private-close-cause",
            })));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Drop for GatedIo {
    fn drop(&mut self) {
        self.plan.dropped.fetch_add(1, Ordering::SeqCst);
        let controls = crate::listener::retained_connection::locked(&self.plan.controls).clone();
        if let Some(controls) = controls {
            // Metadata only; no staged capability, root or actual token is exposed.
            self.plan
                .refused_seen_before_drop
                .store(controls.snapshot().launch_refused, Ordering::SeqCst);
        }
        self.plan.drop_gate.pause();
        let panic = crate::listener::retained_connection::locked(&self.plan.drop_panic).take();
        if let Some(witness) = panic {
            std::panic::panic_any(PayloadError {
                witness,
                name: "private-io-drop-panic",
            });
        }
    }
}
