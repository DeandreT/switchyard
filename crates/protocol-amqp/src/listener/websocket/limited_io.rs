use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(super) struct LimitedIo<Io> {
    inner: Io,
    handshake_remaining: Option<usize>,
    reads_remaining: usize,
    bytes_remaining: usize,
}

impl<Io> LimitedIo<Io> {
    pub(super) fn new(inner: Io) -> Self {
        Self {
            inner,
            handshake_remaining: Some(super::HTTP_REQUEST_BYTES),
            reads_remaining: 32,
            bytes_remaining: 64 * 1024,
        }
    }

    pub(super) fn finish_handshake(&mut self) {
        self.handshake_remaining = None;
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for LimitedIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.handshake_remaining == Some(0) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket HTTP request exceeds its byte limit",
            )));
        }
        if self.reads_remaining == 0 || self.bytes_remaining == 0 {
            self.reads_remaining = 32;
            self.bytes_remaining = 64 * 1024;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let maximum = buf
            .remaining()
            .min(self.bytes_remaining)
            .min(self.handshake_remaining.unwrap_or(usize::MAX));
        let mut limited = ReadBuf::new(buf.initialize_unfilled_to(maximum));
        let result = Pin::new(&mut self.inner).poll_read(cx, &mut limited);
        self.reads_remaining -= 1;
        let count = limited.filled().len();
        self.bytes_remaining -= count;
        if let Some(remaining) = &mut self.handshake_remaining {
            *remaining -= count;
        }
        buf.advance(count);
        result
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for LimitedIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
