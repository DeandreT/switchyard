use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebSocketError, Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

use super::{headers::HeaderTracker, limited_io::LimitedIo};

pub(super) struct TransportState<Io> {
    pub(super) websocket: WebSocketStream<LimitedIo<Io>>,
    pub(super) close_frame: Option<CloseFrame>,
}

pub(in crate::listener) struct WebSocketIo<Io> {
    state: Option<TransportState<Io>>,
    returned: Option<oneshot::Sender<TransportState<Io>>>,
    headers: HeaderTracker,
    incoming: Option<(tokio_tungstenite::tungstenite::Bytes, usize)>,
    eof: bool,
    close_pending: bool,
}

impl<Io> WebSocketIo<Io> {
    pub(super) fn new(
        websocket: WebSocketStream<LimitedIo<Io>>,
        sasl: bool,
        returned: oneshot::Sender<TransportState<Io>>,
    ) -> Self {
        Self {
            state: Some(TransportState {
                websocket,
                close_frame: None,
            }),
            returned: Some(returned),
            headers: HeaderTracker::new(sasl),
            incoming: None,
            eof: false,
            close_pending: false,
        }
    }

    fn fail(&mut self, code: CloseCode, reason: &'static str) -> io::Error {
        self.eof = true;
        if let Some(state) = &mut self.state {
            state.close_frame = Some(CloseFrame {
                code,
                reason: reason.into(),
            });
        }
        io::Error::new(io::ErrorKind::InvalidData, reason)
    }

    fn map_error(&mut self, error: WebSocketError) -> io::Error {
        match error {
            WebSocketError::Capacity(_) => {
                self.fail(CloseCode::Size, "WebSocket message exceeds transport limit")
            }
            WebSocketError::Protocol(_) | WebSocketError::Utf8(_) => {
                self.fail(CloseCode::Protocol, "Invalid WebSocket framing")
            }
            WebSocketError::Io(error) => io::Error::new(error.kind(), "WebSocket IO failed"),
            _ => io::Error::other("WebSocket transport failed"),
        }
    }
}

impl<Io> Drop for WebSocketIo<Io> {
    fn drop(&mut self) {
        // The engine owns the IO until both split halves have stopped.
        if let (Some(returned), Some(state)) = (self.returned.take(), self.state.take()) {
            let _ = returned.send(state);
        }
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        for _ in 0..32 {
            if let Some((message, offset)) = &mut self.incoming {
                let count = buf.remaining().min(message.len() - *offset);
                buf.put_slice(&message[*offset..*offset + count]);
                *offset += count;
                if *offset == message.len() {
                    self.incoming = None;
                }
                return Poll::Ready(Ok(()));
            }
            if self.close_pending {
                let Some(state) = &mut self.state else {
                    return Poll::Ready(Ok(()));
                };
                match Pin::new(&mut state.websocket).poll_flush(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(()))
                    | Poll::Ready(Err(
                        WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed,
                    )) => {
                        self.close_pending = false;
                        self.eof = true;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(self.map_error(error))),
                }
            }
            if self.eof {
                return Poll::Ready(Ok(()));
            }
            let Some(state) = &mut self.state else {
                return Poll::Ready(Ok(()));
            };
            match Pin::new(&mut state.websocket).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(self.map_error(error))),
                Poll::Ready(Some(Ok(Message::Binary(message)))) => {
                    if message.is_empty() {
                        continue;
                    }
                    if self.headers.message(&message).is_err() {
                        return Poll::Ready(Err(self.fail(
                            CloseCode::Protocol,
                            "Invalid AMQP WebSocket protocol header boundary",
                        )));
                    }
                    self.incoming = Some((message, 0));
                }
                Poll::Ready(Some(Ok(Message::Text(_)))) => {
                    return Poll::Ready(Err(self.fail(
                        CloseCode::Unsupported,
                        "AMQP requires binary WebSocket messages",
                    )));
                }
                Poll::Ready(Some(Ok(Message::Close(_)))) => self.close_pending = true,
                Poll::Ready(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
                Poll::Ready(Some(Ok(Message::Frame(_)))) => {
                    return Poll::Ready(Err(
                        self.fail(CloseCode::Protocol, "Unexpected raw WebSocket frame")
                    ));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.eof || self.close_pending {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket is closing",
            )));
        }
        let Some(state) = &mut self.state else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WebSocket transport is closed",
            )));
        };
        match Pin::new(&mut state.websocket).poll_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(self.map_error(error))),
            Poll::Ready(Ok(())) => {}
        }
        let count = buf.len().min(super::WRITE_CHUNK_BYTES);
        let message = Message::Binary(buf[..count].to_vec().into());
        match Pin::new(&mut state.websocket).start_send(message) {
            Ok(()) => Poll::Ready(Ok(count)),
            Err(error) => Poll::Ready(Err(self.map_error(error))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(state) = &mut self.state else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(&mut state.websocket).poll_flush(cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(self.map_error(error))),
            result => result.map_err(|_| io::Error::other("WebSocket flush failed")),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Start/flush Close here; the listener's retained owner drains its reply.
        let Some(state) = &mut self.state else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(&mut state.websocket).poll_close(cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(self.map_error(error))),
            result => result.map_err(|_| io::Error::other("WebSocket close failed")),
        }
    }
}
