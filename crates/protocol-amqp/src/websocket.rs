//! AMQP 1.0 WebSocket binding.
//!
//! WebSocket messages are only a tunnel for one ordered AMQP byte stream. The
//! boundaries of binary WebSocket messages have no AMQP meaning: a protocol
//! header or frame may span several messages, and one message may contain
//! several frames.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message, Result as WebSocketResult,
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderValue, StatusCode, header::SEC_WEBSOCKET_PROTOCOL},
        protocol::WebSocketConfig,
    },
};

/// Original AMQP 1.0 WebSocket binding spelling supported by Service Bus.
pub const AMQP_WEBSOCKET_SUBPROTOCOL: &str = "AMQPWSB10";

/// Standardized spelling used by recent Microsoft AMQP transports.
pub const AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL: &str = "amqp";

/// Service Bus path used for AMQP-over-WebSocket connections.
pub const SERVICE_BUS_WEBSOCKET_PATH: &str = "/$servicebus/websocket";

const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 512 * 1024;

/// Performs the HTTP upgrade and verifies the AMQP WebSocket binding.
pub async fn accept_amqp_websocket<S>(stream: S) -> WebSocketResult<WebSocketIo<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(MAX_WEBSOCKET_MESSAGE_BYTES);
    config.max_frame_size = Some(MAX_WEBSOCKET_MESSAGE_BYTES);
    let socket = accept_hdr_async_with_config(stream, validate_handshake, Some(config)).await?;
    Ok(WebSocketIo::new(socket))
}

#[allow(clippy::result_large_err)] // Tungstenite's handshake callback fixes this response type.
fn validate_handshake(
    request: &Request,
    mut response: Response,
) -> Result<Response, ErrorResponse> {
    let path = request.uri().path();
    let protocol_header = request
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect::<Vec<_>>();
    tracing::debug!(path, protocols = ?protocol_header, "validating AMQP WebSocket upgrade");
    if path != SERVICE_BUS_WEBSOCKET_PATH
        && path.strip_suffix('/') != Some(SERVICE_BUS_WEBSOCKET_PATH)
    {
        return Err(rejection(
            StatusCode::NOT_FOUND,
            "the request does not name the Service Bus WebSocket endpoint",
        ));
    }
    let offered = protocol_header
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .collect::<Vec<_>>();
    let selected = if offered.contains(&AMQP_WEBSOCKET_SUBPROTOCOL) {
        AMQP_WEBSOCKET_SUBPROTOCOL
    } else if offered.contains(&AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL) {
        AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL
    } else {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "the AMQPWSB10 or amqp WebSocket subprotocol is required",
        ));
    };
    response
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(selected));
    Ok(response)
}

fn rejection(status: StatusCode, description: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(description.to_owned()));
    *response.status_mut() = status;
    response
}

/// Presents a binary WebSocket as the byte stream expected by the AMQP engine.
///
/// Each write becomes one binary WebSocket message. Reads concatenate binary
/// messages and ignore their boundaries. Tungstenite queues protocol-required
/// pong and close replies while reading; this adapter flushes them before it
/// waits for more application bytes.
pub struct WebSocketIo<S> {
    socket: WebSocketStream<S>,
    read_buffer: Vec<u8>,
    read_offset: usize,
    control_pending: bool,
    write_pending: bool,
    read_waker: Option<Waker>,
    eof: bool,
    shutdown_started: bool,
}

impl<S> WebSocketIo<S> {
    pub fn new(socket: WebSocketStream<S>) -> Self {
        Self {
            socket,
            read_buffer: Vec::new(),
            read_offset: 0,
            control_pending: false,
            write_pending: false,
            read_waker: None,
            eof: false,
            shutdown_started: false,
        }
    }

    pub fn into_inner(self) -> WebSocketStream<S> {
        self.socket
    }
}

impl<S> AsyncRead for WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.read_waker = None;
        loop {
            if self.control_pending || self.write_pending {
                match Pin::new(&mut self.socket).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        self.control_pending = false;
                        self.write_pending = false;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(io_error(error))),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if self.read_offset < self.read_buffer.len() {
                let available = &self.read_buffer[self.read_offset..];
                let copied = available.len().min(output.remaining());
                output.put_slice(&available[..copied]);
                self.read_offset += copied;
                if self.read_offset == self.read_buffer.len() {
                    self.read_buffer.clear();
                    self.read_offset = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if self.eof || output.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let message = match Pin::new(&mut self.socket).poll_next(cx) {
                Poll::Ready(Some(Ok(message))) => message,
                Poll::Ready(Some(Err(WebSocketError::ConnectionClosed))) | Poll::Ready(None) => {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(io_error(error))),
                Poll::Pending => {
                    self.read_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            };
            match message {
                Message::Binary(bytes) => {
                    self.read_buffer = bytes.to_vec();
                    self.read_offset = 0;
                }
                Message::Ping(_) | Message::Pong(_) => {
                    // Reading a ping makes Tungstenite queue the matching pong.
                    // Flushing here keeps control traffic independent of AMQP
                    // application writes.
                    self.control_pending = true;
                }
                Message::Close(_) => {
                    // Tungstenite queues the answering close frame while
                    // reading. Flush it before exposing end-of-stream.
                    self.control_pending = true;
                    self.eof = true;
                }
                Message::Text(_) | Message::Frame(_) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "AMQP WebSockets accepts binary messages only",
                    )));
                }
            }
        }
    }
}

impl<S> AsyncWrite for WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.control_pending || self.write_pending {
            match Pin::new(&mut self.socket).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    self.control_pending = false;
                    self.write_pending = false;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(io_error(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match Pin::new(&mut self.socket).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(io_error(error))),
            Poll::Pending => return Poll::Pending,
        }
        if let Err(error) =
            Pin::new(&mut self.socket).start_send(Message::Binary(bytes.to_vec().into()))
        {
            return Poll::Ready(Err(io_error(error)));
        }
        // `Poll::Pending` means that none of the caller's bytes were accepted,
        // so acknowledge the bytes as soon as Tungstenite buffers them. AMQP
        // does not flush every protocol header or frame; wake a blocked reader
        // to drive the sink flush even when there is no subsequent write.
        self.write_pending = true;
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.socket).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.control_pending = false;
                self.write_pending = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(io_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown_started {
            match Pin::new(&mut self.socket).poll_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(
                    WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed,
                )) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(io_error(error))),
                Poll::Pending => return Poll::Pending,
            }
            match Pin::new(&mut self.socket).start_send(Message::Close(None)) {
                Ok(()) => self.shutdown_started = true,
                Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {
                    return Poll::Ready(Ok(()));
                }
                Err(error) => return Poll::Ready(Err(io_error(error))),
            }
        }
        match Pin::new(&mut self.socket).poll_close(cx) {
            Poll::Ready(Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed)) => {
                Poll::Ready(Ok(()))
            }
            other => other.map_err(io_error),
        }
    }
}

fn io_error(error: WebSocketError) -> io::Error {
    match error {
        WebSocketError::Io(error) => error,
        other => io::Error::other(other),
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{FutureExt, SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio_tungstenite::{
        WebSocketStream, client_async,
        tungstenite::{
            Error as WebSocketError, Message,
            client::IntoClientRequest,
            http::{HeaderValue, StatusCode, header::SEC_WEBSOCKET_PROTOCOL},
        },
    };

    use super::*;

    async fn connected_pair_with_capacity_and_protocol(
        capacity: usize,
        protocol: &'static str,
    ) -> (WebSocketIo<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (server_stream, client_stream) = tokio::io::duplex(capacity);
        let mut request = format!("ws://localhost{SERVICE_BUS_WEBSOCKET_PATH}")
            .into_client_request()
            .expect("a valid request");
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(protocol));
        let (server, client) = tokio::join!(
            accept_amqp_websocket(server_stream),
            client_async(request, client_stream)
        );
        let (client, response) = client.expect("the client handshake succeeds");
        assert_eq!(
            response.headers().get(SEC_WEBSOCKET_PROTOCOL),
            Some(&HeaderValue::from_static(protocol))
        );
        (server.expect("the server handshake succeeds"), client)
    }

    async fn connected_pair() -> (WebSocketIo<DuplexStream>, WebSocketStream<DuplexStream>) {
        connected_pair_with_capacity_and_protocol(1024 * 1024, AMQP_WEBSOCKET_SUBPROTOCOL).await
    }

    async fn rejected_handshake(path: &str, protocol: Option<&str>) -> StatusCode {
        let (server_stream, client_stream) = tokio::io::duplex(4096);
        let mut request = format!("ws://localhost{path}")
            .into_client_request()
            .expect("a valid request");
        if let Some(protocol) = protocol {
            request.headers_mut().insert(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_str(protocol).expect("a valid protocol header"),
            );
        }
        let (server, client) = tokio::join!(
            accept_amqp_websocket(server_stream),
            client_async(request, client_stream)
        );
        assert!(server.is_err(), "the server should reject the upgrade");
        match client.expect_err("the client should see an HTTP rejection") {
            WebSocketError::Http(response) => response.status(),
            other => panic!("unexpected handshake error: {other}"),
        }
    }

    #[tokio::test]
    async fn negotiates_the_service_bus_subprotocol_and_tunnels_bytes() {
        let (mut server, mut client) = connected_pair().await;
        client
            .send(Message::Binary(Vec::from(&b"AM"[..]).into()))
            .await
            .expect("the first fragment is sent");
        client
            .send(Message::Binary(Vec::from(&b"QP"[..]).into()))
            .await
            .expect("the second fragment is sent");

        let mut received = [0; 4];
        server
            .read_exact(&mut received)
            .await
            .expect("binary message boundaries disappear");
        assert_eq!(&received, b"AMQP");

        server
            .write_all(b"reply")
            .await
            .expect("the reply is accepted");
        server.flush().await.expect("the reply is flushed");
        assert_eq!(
            client
                .next()
                .await
                .expect("a reply")
                .expect("a valid reply"),
            Message::Binary(Vec::from(&b"reply"[..]).into())
        );
    }

    #[tokio::test]
    async fn rejects_the_wrong_path_or_subprotocol() {
        assert_eq!(
            rejected_handshake("/wrong", Some(AMQP_WEBSOCKET_SUBPROTOCOL)).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            rejected_handshake(SERVICE_BUS_WEBSOCKET_PATH, None).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            rejected_handshake(SERVICE_BUS_WEBSOCKET_PATH, Some("chat")).await,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn accepts_the_standardized_subprotocol_used_by_the_current_dotnet_client() {
        let (mut server, mut client) =
            connected_pair_with_capacity_and_protocol(4096, AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL)
                .await;
        client
            .send(Message::Binary(vec![1].into()))
            .await
            .expect("the official protocol spelling is accepted");
        let mut byte = [0];
        server.read_exact(&mut byte).await.expect("one byte");
        assert_eq!(byte, [1]);
    }

    #[tokio::test]
    async fn text_messages_are_not_part_of_the_amqp_tunnel() {
        let (mut server, mut client) = connected_pair().await;
        client
            .send(Message::Text("not AMQP".into()))
            .await
            .expect("the text frame reaches the peer");
        let mut byte = [0];
        let error = server
            .read_exact(&mut byte)
            .await
            .expect_err("text must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn ping_is_answered_and_close_becomes_end_of_stream() {
        let (mut server, mut client) = connected_pair().await;
        client
            .send(Message::Ping(Vec::from(&b"probe"[..]).into()))
            .await
            .expect("the ping is sent");

        let reader = tokio::spawn(async move {
            let mut byte = [0];
            let read = server.read(&mut byte).await;
            (server, read)
        });
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
                .await
                .expect("the pong is prompt")
                .expect("the peer remains open")
                .expect("the pong is valid"),
            Message::Pong(Vec::from(&b"probe"[..]).into())
        );
        client
            .send(Message::Binary(vec![7].into()))
            .await
            .expect("application data follows the ping");
        let (mut server, read) = reader.await.expect("the reader task completes");
        assert_eq!(read.expect("the binary byte is read"), 1);

        client.close(None).await.expect("the client sends close");
        let mut byte = [0];
        assert_eq!(server.read(&mut byte).await.expect("close is clean"), 0);
        server
            .shutdown()
            .await
            .expect("shutdown after peer close is clean");
    }

    #[tokio::test]
    async fn a_backpressured_write_is_delivered_without_truncation() {
        let (mut server, mut client) =
            connected_pair_with_capacity_and_protocol(4096, AMQP_WEBSOCKET_SUBPROTOCOL).await;
        let payload = vec![0x5a; 256 * 1024];
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            let written = server
                .write(&payload)
                .now_or_never()
                .expect("buffered bytes are acknowledged without waiting for a sink flush")?;
            assert_eq!(written, payload.len());
            server.flush().await
        });
        let message = client
            .next()
            .await
            .expect("the message arrives")
            .expect("the message is valid");
        assert_eq!(message, Message::Binary(expected.into()));
        writer
            .await
            .expect("the writer task completes")
            .expect("the write succeeds");
    }
}
