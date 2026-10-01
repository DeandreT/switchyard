use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use amqp::AMQP_HEADER;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        protocol::{Role, frame::coding::CloseCode},
    },
};

use super::{CloseHandle, adapter::WebSocketIo, limited_io::LimitedIo, upgrade};

async fn pair() -> (
    WebSocketIo<DuplexStream>,
    CloseHandle<DuplexStream>,
    WebSocketStream<DuplexStream>,
) {
    pair_with_capacity(128 * 1024).await
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .unwrap()
}

async fn pair_with_capacity(
    capacity: usize,
) -> (
    WebSocketIo<DuplexStream>,
    CloseHandle<DuplexStream>,
    WebSocketStream<DuplexStream>,
) {
    let (server, client) = tokio::io::duplex(capacity);
    let upgrade = tokio::spawn(upgrade(server, false));
    let mut request = "ws://localhost/$servicebus/websocket/"
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "amqp".parse().unwrap());
    let (client, response) = bounded(client_async(request, client)).await.unwrap();
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "amqp");
    let (server, close) = bounded(upgrade).await.unwrap().unwrap();
    (server, close, client)
}

async fn next(client: &mut WebSocketStream<DuplexStream>) -> Message {
    tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn close_pair(
    server: WebSocketIo<DuplexStream>,
    close: CloseHandle<DuplexStream>,
    mut client: WebSocketStream<DuplexStream>,
) {
    drop(server);
    let closing = tokio::spawn(close.finish());
    assert!(matches!(next(&mut client).await, Message::Close(_)));
    client.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn binary_stream_splits_and_coalesces_after_the_header() {
    let (mut server, close, mut client) = pair().await;
    client
        .send(Message::Binary(AMQP_HEADER.to_vec().into()))
        .await
        .unwrap();
    let mut header = [0; 8];
    bounded(server.read_exact(&mut header)).await.unwrap();
    assert_eq!(header, AMQP_HEADER);
    client
        .send(Message::Binary(vec![1, 2, 3].into()))
        .await
        .unwrap();
    client
        .send(Message::Binary(vec![4, 5, 6, 7, 8].into()))
        .await
        .unwrap();
    let mut content = [0; 8];
    bounded(server.read_exact(&mut content)).await.unwrap();
    assert_eq!(content, [1, 2, 3, 4, 5, 6, 7, 8]);
    close_pair(server, close, client).await;
}

#[tokio::test]
async fn outbound_headers_stay_one_message_and_writes_are_chunked() {
    let (mut server, close, mut client) = pair().await;
    server.write_all(&AMQP_HEADER).await.unwrap();
    server.flush().await.unwrap();
    assert_eq!(
        next(&mut client).await,
        Message::Binary(AMQP_HEADER.to_vec().into())
    );
    let bytes = vec![42; super::WRITE_CHUNK_BYTES + 17];
    server.write_all(&bytes).await.unwrap();
    server.flush().await.unwrap();
    let Message::Binary(first) = next(&mut client).await else {
        panic!("expected binary chunk");
    };
    let Message::Binary(second) = next(&mut client).await else {
        panic!("expected binary chunk");
    };
    assert_eq!(first.len(), super::WRITE_CHUNK_BYTES);
    assert_eq!(second.len(), 17);
    assert!(first.iter().chain(second.iter()).all(|byte| *byte == 42));
    close_pair(server, close, client).await;
}

#[tokio::test]
async fn invalid_headers_and_text_have_static_close_codes() {
    for (message, code) in [
        (
            Message::Binary(AMQP_HEADER[..4].to_vec().into()),
            CloseCode::Protocol,
        ),
        (Message::Text("private text".into()), CloseCode::Unsupported),
    ] {
        let (mut server, close, mut client) = pair().await;
        client.send(message).await.unwrap();
        let error = bounded(server.read_u8()).await.unwrap_err();
        assert!(!error.to_string().contains("private"));
        drop(server);
        let closing = tokio::spawn(close.finish());
        let Message::Close(Some(frame)) = next(&mut client).await else {
            panic!("expected error close");
        };
        assert_eq!(frame.code, code);
        client.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), closing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn ping_is_not_amqp_data_and_remote_close_is_flushed_before_eof() {
    let (mut server, close, mut client) = pair().await;
    client.send(Message::Ping(vec![9].into())).await.unwrap();
    client
        .send(Message::Binary(AMQP_HEADER.to_vec().into()))
        .await
        .unwrap();
    let mut header = [0; 8];
    bounded(server.read_exact(&mut header)).await.unwrap();
    assert_eq!(header, AMQP_HEADER);
    assert_eq!(next(&mut client).await, Message::Pong(vec![9].into()));
    client.close(None).await.unwrap();
    let mut byte = [0; 1];
    assert_eq!(bounded(server.read(&mut byte)).await.unwrap(), 0);
    assert!(matches!(next(&mut client).await, Message::Close(_)));
    drop(server);
    bounded(close.finish()).await.unwrap();
}

#[tokio::test]
async fn http_budget_stops_reads_without_consuming_the_next_byte() {
    let (server, mut client) = tokio::io::duplex(super::HTTP_REQUEST_BYTES + 1);
    client
        .write_all(&vec![7; super::HTTP_REQUEST_BYTES + 1])
        .await
        .unwrap();
    let mut limited = LimitedIo::new(server);
    bounded(limited.read_exact(&mut vec![0; super::HTTP_REQUEST_BYTES]))
        .await
        .unwrap();
    assert_eq!(
        limited.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    limited.finish_handshake();
    assert_eq!(limited.read_u8().await.unwrap(), 7);
}

#[tokio::test]
async fn empty_message_flood_yields_before_processing_all_events() {
    let (server, mut client) = tokio::io::duplex(4096);
    let mut bytes = Vec::new();
    for _ in 0..40 {
        bytes.extend_from_slice(&[0x82, 0x80, 0, 0, 0, 0]);
    }
    bytes.extend_from_slice(&[0x82, 0x88, 0, 0, 0, 0]);
    bytes.extend_from_slice(&AMQP_HEADER);
    client.write_all(&bytes).await.unwrap();
    let mut limited = LimitedIo::new(server);
    limited.finish_handshake();
    let websocket = WebSocketStream::from_raw_socket(limited, Role::Server, None).await;
    let (returned, _receiver) = tokio::sync::oneshot::channel();
    let mut server = WebSocketIo::new(websocket, false, returned);
    let mut header = [0; 8];
    let mut buffer = ReadBuf::new(&mut header);
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        Pin::new(&mut server).poll_read(&mut context, &mut buffer),
        Poll::Pending
    ));
    assert!(buffer.filled().is_empty());
    bounded(server.read_exact(&mut header)).await.unwrap();
    assert_eq!(header, AMQP_HEADER);
}

#[tokio::test]
async fn pending_writes_accept_no_extra_bytes_and_resume_exactly_once() {
    let (mut server, close, mut client) = pair_with_capacity(64).await;
    let bytes: Vec<_> = (0..super::WRITE_CHUNK_BYTES * 2 + 17)
        .map(|index| (index % 251) as u8)
        .collect();
    let accepted = bounded(server.write(&bytes)).await.unwrap();
    assert_eq!(accepted, super::WRITE_CHUNK_BYTES);
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(
        Pin::new(&mut server)
            .poll_write(&mut context, &bytes[accepted..])
            .is_pending()
    );
    assert!(Pin::new(&mut server).poll_flush(&mut context).is_pending());
    let remaining = bytes[accepted..].to_vec();
    let writer = tokio::spawn(async move {
        server.write_all(&remaining).await.unwrap();
        server.flush().await.unwrap();
        server
    });
    let mut received = Vec::new();
    while received.len() < bytes.len() {
        let Message::Binary(chunk) = next(&mut client).await else {
            panic!("expected binary chunk");
        };
        assert!(chunk.len() <= super::WRITE_CHUNK_BYTES);
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received, bytes);
    let server = bounded(writer).await.unwrap();
    close_pair(server, close, client).await;
}

struct BlockedIo;

impl AsyncRead for BlockedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for BlockedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn rejected_buffer_capacity_does_not_report_accepted_bytes() {
    let mut limited = LimitedIo::new(BlockedIo);
    limited.finish_handshake();
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .write_buffer_size(0)
        .max_write_buffer_size(super::WRITE_CHUNK_BYTES / 2);
    let websocket = WebSocketStream::from_raw_socket(limited, Role::Server, Some(config)).await;
    let (returned, _receiver) = tokio::sync::oneshot::channel();
    let mut server = WebSocketIo::new(websocket, false, returned);
    let mut context = Context::from_waker(std::task::Waker::noop());
    let bytes = vec![42; super::WRITE_CHUNK_BYTES];
    assert!(matches!(
        Pin::new(&mut server).poll_write(&mut context, &bytes),
        Poll::Ready(Err(_))
    ));
}

#[tokio::test]
async fn cancelling_close_without_a_peer_reply_drops_the_socket() {
    let (server, close, mut client) = pair().await;
    drop(server);
    let closing = tokio::spawn(close.finish());
    assert!(matches!(next(&mut client).await, Message::Close(_)));
    // Do not flush the client's automatically queued acknowledgement.
    assert!(!closing.is_finished());
    closing.abort();
    assert!(bounded(closing).await.unwrap_err().is_cancelled());
    let mut raw = client.into_inner();
    let mut byte = [0; 1];
    assert_eq!(bounded(raw.read(&mut byte)).await.unwrap(), 0);
}
