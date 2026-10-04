//! Bounded in-memory transport data, not a validated or durable state image.

use std::{
    fmt, io,
    io::SeekFrom,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

/// Maximum logical length and seek position of one snapshot buffer.
///
/// This is independent of future domain-image validation or admission budgets.
/// It does not bound allocator overhead, aggregate buffers, or process memory.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

/// In-memory snapshot data with checked writes and seek positions.
///
/// The buffer carries no validation, authorization, storage, or installation
/// authority. Flush and shutdown do not persist anything. Like an in-memory
/// cursor, it remains readable after shutdown.
///
/// ```compile_fail
/// fn unbounded_write(data: &mut cluster::BoundedSnapshotData) {
///     data.get_mut().extend_from_slice(b"unchecked");
/// }
/// ```
#[derive(Default)]
pub struct BoundedSnapshotData {
    buffer: Buffer<MAX_SNAPSHOT_BYTES>,
}

impl BoundedSnapshotData {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copies bounded bytes and starts reading at position zero.
    ///
    /// This does not adopt a caller's potentially oversized allocation.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Ok(Self {
            buffer: Buffer::from_bytes(bytes)?,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer.bytes
    }

    pub fn len(&self) -> usize {
        self.buffer.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.bytes.is_empty()
    }

    pub fn position(&self) -> u64 {
        self.buffer.position
    }
}

impl fmt::Debug for BoundedSnapshotData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedSnapshotData")
            .field("buffered_bytes", &self.len())
            .field("position", &self.position())
            .finish_non_exhaustive()
    }
}

impl AsyncRead for BoundedSnapshotData {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().buffer).poll_read(cx, buffer)
    }
}

impl AsyncWrite for BoundedSnapshotData {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().buffer).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().buffer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().buffer).poll_shutdown(cx)
    }
}

impl AsyncSeek for BoundedSnapshotData {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.get_mut().buffer).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.get_mut().buffer).poll_complete(cx)
    }
}

// The public associated type fixes its bound. Tiny internal bounds exercise the
// same I/O implementation without allocating production-size test payloads.
#[derive(Default)]
struct Buffer<const LIMIT: usize> {
    bytes: Vec<u8>,
    position: u64,
}

impl<const LIMIT: usize> Buffer<LIMIT> {
    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() > LIMIT {
            return Err(bound_error());
        }
        let mut buffer = Self::default();
        buffer.write_bounded(bytes)?;
        buffer.position = 0;
        Ok(buffer)
    }

    fn write_bounded(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let start = usize::try_from(self.position).map_err(|_| bound_error())?;
        let end = start
            .checked_add(bytes.len())
            .filter(|end| *end <= LIMIT)
            .ok_or_else(bound_error)?;
        let position = u64::try_from(end).map_err(|_| bound_error())?;

        // Nothing observable changes before fallible capacity reservation.
        self.bytes
            .try_reserve_exact(end.saturating_sub(self.bytes.len()))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "snapshot buffer allocation failed",
                )
            })?;
        if end > self.bytes.len() {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(bytes);
        self.position = position;
        Ok(bytes.len())
    }

    fn seek_bounded(&mut self, from: SeekFrom) -> io::Result<()> {
        let position = match from {
            SeekFrom::Start(position) => position,
            SeekFrom::End(offset) => u64::try_from(self.bytes.len())
                .map_err(|_| bound_error())?
                .checked_add_signed(offset)
                .ok_or_else(bound_error)?,
            SeekFrom::Current(offset) => self
                .position
                .checked_add_signed(offset)
                .ok_or_else(bound_error)?,
        };
        let limit = u64::try_from(LIMIT).map_err(|_| bound_error())?;
        if position > limit {
            return Err(bound_error());
        }
        self.position = position;
        Ok(())
    }
}

impl<const LIMIT: usize> AsyncRead for Buffer<LIMIT> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Ok(start) = usize::try_from(this.position) else {
            return Poll::Ready(Err(bound_error()));
        };
        let available = this.bytes.get(start..).unwrap_or_default();
        let count = available.len().min(buffer.remaining());
        let Ok(count_u64) = u64::try_from(count) else {
            return Poll::Ready(Err(bound_error()));
        };
        let Some(position) = this.position.checked_add(count_u64) else {
            return Poll::Ready(Err(bound_error()));
        };
        buffer.put_slice(&available[..count]);
        // The copied range ends at or before the already-bounded data length.
        this.position = position;
        Poll::Ready(Ok(()))
    }
}

impl<const LIMIT: usize> AsyncWrite for Buffer<LIMIT> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.get_mut().write_bounded(bytes))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<const LIMIT: usize> AsyncSeek for Buffer<LIMIT> {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        self.get_mut().seek_bounded(position)
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().position))
    }
}

fn bound_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "snapshot buffer bound exceeded",
    )
}

#[cfg(test)]
mod tests;
