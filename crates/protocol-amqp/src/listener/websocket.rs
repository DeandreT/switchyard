use std::{error::Error, fmt, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, protocol::WebSocketConfig};

mod adapter;
mod handshake;
mod headers;
mod limited_io;

use adapter::{TransportState, WebSocketIo};
use limited_io::LimitedIo;

const HTTP_REQUEST_BYTES: usize = 16 * 1024;
const READ_BUFFER_BYTES: usize = 16 * 1024;
const WRITE_CHUNK_BYTES: usize = 16 * 1024;
const WRITE_BUFFER_BYTES: usize = 64 * 1024;
const MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
type TransportError = Box<dyn Error + Send + Sync>;

struct WebSocketCloseFailure {
    boundary: &'static str,
    original: WebSocketError,
}

impl fmt::Debug for WebSocketCloseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketCloseFailure")
            .field("boundary", &self.boundary)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for WebSocketCloseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.boundary)
    }
}

impl Error for WebSocketCloseFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.original)
    }
}

fn close_failure(boundary: &'static str, original: WebSocketError) -> TransportError {
    std::io::Error::other(WebSocketCloseFailure { boundary, original }).into()
}

pub(in crate::listener) fn original_close_io_kind(
    error: &(dyn Error + Send + Sync + 'static),
) -> Option<std::io::ErrorKind> {
    let envelope = error.downcast_ref::<std::io::Error>()?;
    if envelope.kind() != std::io::ErrorKind::Other {
        return None;
    }
    let failure = envelope
        .get_ref()?
        .downcast_ref::<WebSocketCloseFailure>()?;
    match &failure.original {
        WebSocketError::Io(original) => Some(original.kind()),
        _ => None,
    }
}

pub(super) async fn upgrade<Io>(
    stream: Io,
    sasl: bool,
) -> Result<(WebSocketIo<Io>, CloseHandle<Io>), TransportError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let config = WebSocketConfig::default()
        .read_buffer_size(READ_BUFFER_BYTES)
        .write_buffer_size(0)
        .max_write_buffer_size(WRITE_BUFFER_BYTES)
        .max_message_size(Some(MESSAGE_BYTES))
        .max_frame_size(Some(MESSAGE_BYTES))
        .accept_unmasked_frames(false);
    let mut websocket = accept_hdr_async_with_config(
        LimitedIo::new(stream),
        handshake::validate_upgrade,
        Some(config),
    )
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WebSocket HTTP upgrade failed",
        )
    })?;
    websocket.get_mut().finish_handshake();
    let (returned, receiver) = oneshot::channel();
    Ok((
        WebSocketIo::new(websocket, sasl, returned),
        CloseHandle { receiver },
    ))
}

pub(super) struct CloseHandle<Io> {
    receiver: oneshot::Receiver<TransportState<Io>>,
}

impl<Io> CloseHandle<Io>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    pub(super) async fn finish(self) -> Result<(), TransportError> {
        tokio::time::timeout(CLOSE_TIMEOUT, async {
            let state = self.receiver.await.map_err(|_| {
                std::io::Error::other("WebSocket transport was not returned after AMQP shutdown")
            })?;
            finish_transport(state).await
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "WebSocket close deadline exceeded",
            )
        })?
    }
}

async fn finish_transport<Io>(mut state: TransportState<Io>) -> Result<(), TransportError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let close = state.websocket.close(state.close_frame).await;
    match close {
        Ok(()) => {
            while let Some(message) = state.websocket.next().await {
                match message {
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => break,
                    Err(error) => {
                        return Err(close_failure("WebSocket close exchange failed", error));
                    }
                }
            }
            match state.websocket.flush().await {
                Ok(()) | Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {}
                Err(error) => return Err(close_failure("WebSocket close flush failed", error)),
            }
        }
        Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {}
        Err(error) => return Err(close_failure("WebSocket close write failed", error)),
    }
    state.websocket.get_mut().shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
