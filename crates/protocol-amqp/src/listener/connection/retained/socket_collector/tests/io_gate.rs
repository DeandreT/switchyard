use futures_util::task::AtomicWaker;
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
pub(super) struct Plan {
    pub(super) blocked: AtomicBool,
    pub(super) entered: AtomicBool,
    waker: AtomicWaker,
}
impl Plan {
    pub(super) fn block(&self) {
        self.blocked.store(true, Ordering::SeqCst);
    }
    pub(super) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
    pub(super) fn release(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        self.waker.wake();
    }
}
pub(super) struct Gated<I> {
    pub(super) io: I,
    pub(super) plan: Arc<Plan>,
}
impl<I: AsyncRead + Unpin> AsyncRead for Gated<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl<I: AsyncWrite + Unpin> AsyncWrite for Gated<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.plan.blocked.load(Ordering::SeqCst) {
            self.plan.waker.register(cx.waker());
            if self.plan.blocked.load(Ordering::SeqCst) {
                self.plan.entered.store(true, Ordering::SeqCst);
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
