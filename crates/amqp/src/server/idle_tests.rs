use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncReadExt, DuplexStream, ReadBuf},
    time::{Instant, advance},
};

use super::*;

enum TestConnection {
    Server(ServerConnection),
    #[cfg(feature = "test-client")]
    Client(ClientConnection),
}

impl TestConnection {
    async fn shutdown(&self) {
        match self {
            Self::Server(connection) => connection.shutdown().await,
            #[cfg(feature = "test-client")]
            Self::Client(connection) => connection.shutdown().await,
        }
    }

    async fn terminated(&self) {
        match self {
            Self::Server(connection) => connection.lifecycle.wait_terminated().await,
            #[cfg(feature = "test-client")]
            Self::Client(connection) => connection.wait_terminated().await,
        }
    }
}

async fn ready() {
    // Keep virtual time still while the independent driver and reader poll.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    read_frame(peer).await.expect("engine frame")
}

async fn assert_no_bytes(peer: &mut DuplexStream) {
    std::future::poll_fn(|cx| {
        let mut bytes = [0];
        let mut buffer = ReadBuf::new(&mut bytes);
        match Pin::new(&mut *peer).poll_read(cx, &mut buffer) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => panic!("unexpected bytes or EOF: {result:?}"),
        }
    })
    .await;
}

async fn peer_open(peer: &mut DuplexStream, idle: Option<u32>) {
    write_amqp(
        peer,
        0,
        Performative::Open(Open {
            idle_time_out: idle,
            channel_max: 9,
            ..Open::new("raw-idle-peer")
        }),
        Vec::new(),
    )
    .await
    .expect("peer Open");
}

async fn server_pair(
    options: ConnectionOptions,
    peer_idle: Option<u32>,
    capacity: usize,
) -> (TestConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(capacity);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        peer_open(&mut peer, peer_idle).await;
        let Frame::Amqp {
            performative: Some(Performative::Open(open)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("server Open");
        };
        assert_eq!(open.idle_time_out, Some(options.advertised_idle_timeout()));
        peer
    };
    let (connection, peer) = tokio::join!(
        ServerConnection::accept_with_options(wire, "idle-server", None, options),
        opening,
    );
    ready().await;
    (
        TestConnection::Server(connection.expect("server opens")),
        peer,
    )
}

#[cfg(feature = "test-client")]
async fn client_pair(
    options: ConnectionOptions,
    peer_idle: Option<u32>,
    capacity: usize,
) -> (TestConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(capacity);
    let opening = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        let Frame::Amqp {
            performative: Some(Performative::Open(open)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("client Open");
        };
        assert_eq!(open.idle_time_out, Some(options.advertised_idle_timeout()));
        peer_open(&mut peer, peer_idle).await;
        peer
    };
    let (connection, peer) = tokio::join!(
        ClientConnection::builder()
            .connection_options(options)
            .open_with_stream(wire),
        opening,
    );
    ready().await;
    (
        TestConnection::Client(connection.expect("client opens")),
        peer,
    )
}

async fn pair(
    client: bool,
    options: ConnectionOptions,
    peer_idle: Option<u32>,
) -> (TestConnection, DuplexStream) {
    #[cfg(feature = "test-client")]
    if client {
        return client_pair(options, peer_idle, 4096).await;
    }
    assert!(!client, "client feature required");
    server_pair(options, peer_idle, 4096).await
}

fn sides() -> &'static [bool] {
    #[cfg(feature = "test-client")]
    {
        &[false, true]
    }
    #[cfg(not(feature = "test-client"))]
    {
        &[false]
    }
}

fn assert_heartbeat(frame: Frame) {
    assert_eq!(
        frame,
        Frame::Amqp {
            channel: 0,
            performative: None,
            payload: Vec::new(),
        }
    );
}

fn assert_idle_close(frame: Frame) {
    let Frame::Amqp {
        performative: Some(Performative::Close(close)),
        ..
    } = frame
    else {
        panic!("idle Close");
    };
    assert_eq!(
        close.error.expect("idle reason").condition,
        crate::ErrorCondition::Custom(Symbol::from("amqp:connection:forced"))
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeat_uses_the_peer_interval_even_when_our_receive_direction_is_disabled() {
    for &client in sides() {
        let (connection, mut peer) = pair(
            client,
            ConnectionOptions::default().idle_timeout_millis(0),
            Some(1001),
        )
        .await;
        advance(Duration::from_micros(500_499)).await;
        ready().await;
        assert_no_bytes(&mut peer).await;
        advance(Duration::from_micros(1)).await;
        assert_heartbeat(next_frame(&mut peer).await);
        advance(Duration::from_micros(500_500)).await;
        assert_heartbeat(next_frame(&mut peer).await);
        connection.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn omitted_and_zero_peer_idle_disable_heartbeats_not_our_receive_timeout() {
    for &client in sides() {
        for peer_idle in [None, Some(0)] {
            let (connection, mut peer) = pair(
                client,
                ConnectionOptions::default().idle_timeout_millis(1000),
                peer_idle,
            )
            .await;
            advance(Duration::from_millis(1999)).await;
            ready().await;
            assert_no_bytes(&mut peer).await;
            advance(Duration::from_millis(1)).await;
            assert_idle_close(next_frame(&mut peer).await);
            write_amqp(
                &mut peer,
                0,
                Performative::Close(Close::default()),
                Vec::new(),
            )
            .await
            .expect("ack Close");
            connection.terminated().await;
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).await.expect("EOF");
            assert!(rest.is_empty(), "no second Close or heartbeat");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn valid_non_session_heartbeat_resets_only_the_receive_direction() {
    for &client in sides() {
        let (connection, mut peer) = pair(
            client,
            ConnectionOptions::default().idle_timeout_millis(1000),
            Some(1000),
        )
        .await;
        advance(Duration::from_millis(500)).await;
        assert_heartbeat(next_frame(&mut peer).await);
        advance(Duration::from_millis(400)).await;
        write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 7,
                performative: None,
                payload: Vec::new(),
            },
        )
        .await
        .expect("heartbeat on unbound valid channel");
        ready().await;
        advance(Duration::from_millis(100)).await;
        assert_heartbeat(next_frame(&mut peer).await);
        // Incoming traffic did not postpone the peer's outgoing keepalive.
        advance(Duration::from_millis(500)).await;
        assert_heartbeat(next_frame(&mut peer).await);
        connection.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn partial_prefix_and_body_trickles_do_not_refresh_receive_activity() {
    for &client in sides() {
        for prefix in [true, false] {
            let (connection, mut peer) = pair(
                client,
                ConnectionOptions::default().idle_timeout_millis(1000),
                None,
            )
            .await;
            let bytes = crate::encode_frame(&Frame::Amqp {
                channel: 0,
                performative: None,
                payload: Vec::new(),
            })
            .expect("heartbeat bytes");
            let split = if prefix { 1 } else { 5 };
            advance(Duration::from_millis(1000)).await;
            peer.write_all(&bytes[..split])
                .await
                .expect("partial frame");
            ready().await;
            advance(Duration::from_millis(999)).await;
            ready().await;
            assert_no_bytes(&mut peer).await;
            advance(Duration::from_millis(1)).await;
            assert_idle_close(next_frame(&mut peer).await);
            // Never finish the frame or acknowledge Close: reader cancellation
            // and the independent Close deadline must still terminate.
            advance(DEFAULT_CLOSE_TIMEOUT).await;
            connection.terminated().await;
            let mut remaining = Vec::new();
            peer.read_to_end(&mut remaining)
                .await
                .expect("reader joined");
            assert!(remaining.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn complete_frames_refresh_receive_deadline_without_extending_outbound_deadline() {
    for &client in sides() {
        let (connection, mut peer) = pair(
            client,
            ConnectionOptions::default().idle_timeout_millis(1000),
            None,
        )
        .await;
        advance(Duration::from_millis(1900)).await;
        write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 1,
                performative: None,
                payload: Vec::new(),
            },
        )
        .await
        .expect("complete heartbeat");
        ready().await;
        advance(Duration::from_millis(1900)).await;
        ready().await;
        assert_no_bytes(&mut peer).await;
        advance(Duration::from_millis(100)).await;
        assert_idle_close(next_frame(&mut peer).await);
        connection.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn both_zero_directions_stay_quiet_without_a_busy_timer() {
    for &client in sides() {
        let (connection, mut peer) = pair(
            client,
            ConnectionOptions::default().idle_timeout_millis(0),
            Some(0),
        )
        .await;
        advance(Duration::from_secs(3600)).await;
        ready().await;
        assert_no_bytes(&mut peer).await;
        connection.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn maximum_peer_interval_is_not_truncated_or_doubled_in_u32() {
    let activity = Activity::new();
    let start = Instant::now();
    advance(Duration::from_micros(u64::from(u32::MAX) * 500 - 1)).await;
    assert!(!activity.heartbeat_is_due(u32::MAX));
    advance(Duration::from_micros(1)).await;
    assert!(activity.heartbeat_is_due(u32::MAX));
    assert_eq!(
        Instant::now() - start,
        Duration::from_micros(u64::from(u32::MAX) * 500)
    );
    let options = ConnectionOptions::default().idle_timeout_millis(u32::MAX);
    options.validate().expect("maximum idle interval");
    let activity = Activity::new();
    advance(Duration::from_millis(u64::from(u32::MAX) * 2)).await;
    assert_eq!(activity.timeout(options, 0).await, ActivityTimeout::Receive);
}

#[test]
fn local_invalid_options_are_refused_before_negotiation() {
    for idle in [1, 999] {
        assert!(matches!(
            ConnectionOptions::default()
                .idle_timeout_millis(idle)
                .validate(),
            Err(EngineError::InvalidState(_))
        ));
    }
    assert!(matches!(
        ConnectionOptions::default()
            .write_timeout(Duration::ZERO)
            .validate(),
        Err(EngineError::Timeout("write"))
    ));
    assert!(
        ConnectionOptions::default()
            .write_timeout(Duration::MAX)
            .validate()
            .is_err()
    );
    ConnectionOptions::default()
        .idle_timeout_millis(0)
        .validate()
        .expect("zero disables only receive idle");
}

#[derive(Default)]
struct BlockedWriteState {
    partial: AtomicBool,
    bytes: Mutex<Vec<u8>>,
}

struct BlockedWriter {
    state: Arc<BlockedWriteState>,
    block_flush: bool,
}

impl AsyncWrite for BlockedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.block_flush {
            self.state
                .bytes
                .lock()
                .expect("bytes")
                .extend_from_slice(bytes);
            return Poll::Ready(Ok(bytes.len()));
        }
        if !self.state.partial.swap(true, Ordering::Relaxed) {
            self.state.bytes.lock().expect("bytes").push(bytes[0]);
            return Poll::Ready(Ok(1));
        }
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn bounded_partial_writes_and_flushes_taint_the_transport_without_retry_or_close() {
    for block_flush in [false, true] {
        let state = Arc::new(BlockedWriteState::default());
        let activity = Activity::new();
        let mut writer = FrameWriter::new(
            BlockedWriter {
                state: state.clone(),
                block_flush,
            },
            512,
        )
        .expect("writer");
        writer.configure_activity(
            ConnectionOptions::default().write_timeout(Duration::from_millis(25)),
            0,
            activity.clone(),
        );
        let start = Instant::now();
        let frame = Frame::Amqp {
            channel: 0,
            performative: None,
            payload: Vec::new(),
        };
        assert_eq!(
            writer
                .write_frame(&frame)
                .await
                .expect_err("write/flush deadline")
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(Instant::now() - start, Duration::from_millis(25));
        assert!(activity.is_tainted());
        let before = state.bytes.lock().expect("bytes").clone();
        assert!(
            writer
                .write_amqp(0, Performative::Close(Close::default()), Vec::new())
                .await
                .is_err()
        );
        assert_eq!(
            *state.bytes.lock().expect("bytes"),
            before,
            "never append a Close to a truncated or unflushed frame"
        );
        assert_eq!(
            activity.timeout(ConnectionOptions::default(), 0).await,
            ActivityTimeout::WriteFailed
        );
    }
}

#[tokio::test(start_paused = true)]
async fn peer_remaining_activity_time_can_be_shorter_than_the_local_write_policy() {
    let activity = Activity::new();
    advance(Duration::from_millis(900)).await;
    let state = Arc::new(BlockedWriteState::default());
    let mut writer = FrameWriter::new(
        BlockedWriter {
            state,
            block_flush: false,
        },
        512,
    )
    .expect("writer");
    writer.configure_activity(ConnectionOptions::default(), 1000, activity.clone());
    let start = Instant::now();
    assert!(
        writer
            .write_frame(&Frame::Amqp {
                channel: 0,
                performative: None,
                payload: Vec::new()
            })
            .await
            .is_err()
    );
    assert_eq!(Instant::now() - start, Duration::from_millis(100));
    assert!(activity.is_tainted());
}

#[tokio::test(start_paused = true)]
async fn tiny_positive_peer_idle_is_refused_explicitly_after_open() {
    for &client in sides() {
        for idle in [1, 999] {
            let (wire, mut peer) = tokio::io::duplex(4096);
            let opening = async {
                if client {
                    expect_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .expect("client header");
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .expect("peer header");
                    assert!(matches!(
                        next_frame(&mut peer).await,
                        Frame::Amqp {
                            performative: Some(Performative::Open(_)),
                            ..
                        }
                    ));
                    peer_open(&mut peer, Some(idle)).await;
                } else {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .expect("peer header");
                    expect_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .expect("server header");
                    peer_open(&mut peer, Some(idle)).await;
                    assert!(matches!(
                        next_frame(&mut peer).await,
                        Frame::Amqp {
                            performative: Some(Performative::Open(_)),
                            ..
                        }
                    ));
                }
                let Frame::Amqp {
                    performative: Some(Performative::Close(close)),
                    ..
                } = next_frame(&mut peer).await
                else {
                    panic!("explanatory Close");
                };
                let error = close.error.expect("unsupported idle error");
                assert_eq!(error.condition, crate::AmqpError::InvalidField.into());
                assert!(error.description.expect("description").contains("1000"));
                let mut remaining = Vec::new();
                peer.read_to_end(&mut remaining)
                    .await
                    .expect("negotiation rejected");
                assert!(remaining.is_empty());
            };
            let accepting = async {
                #[cfg(feature = "test-client")]
                if client {
                    return ClientConnection::builder()
                        .open_with_stream(wire)
                        .await
                        .map(|_| ());
                }
                ServerConnection::accept(wire, "idle-server", None)
                    .await
                    .map(|_| ())
            };
            let (result, ()) = tokio::join!(accepting, opening);
            assert!(matches!(result, Err(EngineError::InvalidState(_))));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn local_invalid_options_emit_no_handshake_bytes() {
    for &client in sides() {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let options = ConnectionOptions::default().idle_timeout_millis(999);
        let result = async {
            #[cfg(feature = "test-client")]
            if client {
                return ClientConnection::builder()
                    .connection_options(options)
                    .open_with_stream(wire)
                    .await
                    .map(|_| ());
            }
            ServerConnection::accept_with_options(wire, "idle-server", None, options)
                .await
                .map(|_| ())
        }
        .await;
        assert!(matches!(result, Err(EngineError::InvalidState(_))));
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining)
            .await
            .expect("invalid config drops socket");
        assert!(remaining.is_empty());
    }
}

#[derive(Default)]
struct WriteGate {
    blocked: AtomicBool,
    partial: AtomicBool,
}

struct GatedIo {
    inner: DuplexStream,
    gate: Arc<WriteGate>,
    block_flush: bool,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.gate.blocked.load(Ordering::Acquire) && !self.block_flush {
            if self.gate.partial.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            let result = Pin::new(&mut self.inner).poll_write(cx, &bytes[..1]);
            if matches!(result, Poll::Ready(Ok(1))) {
                self.gate.partial.store(true, Ordering::Release);
            }
            return result;
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.gate.blocked.load(Ordering::Acquire) && self.block_flush {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn server_receive_watchdog_interrupts_blocked_write_and_flush_before_local_write_timeout() {
    for block_flush in [false, true] {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let gate = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner: wire,
            gate: gate.clone(),
            block_flush,
        };
        let opening = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("server header");
            peer_open(&mut peer, None).await;
            next_frame(&mut peer).await;
        };
        let (connection, ()) = tokio::join!(
            ServerConnection::accept_with_options(
                stream,
                "gated-server",
                None,
                ConnectionOptions::default().idle_timeout_millis(1000)
            ),
            opening
        );
        let mut connection = connection.expect("server opens");
        write_amqp(
            &mut peer,
            0,
            Performative::Begin(Begin::default()),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
        let incoming = connection
            .next_incoming_session()
            .await
            .expect("incoming session");
        gate.blocked.store(true, Ordering::Release);
        let start = Instant::now();
        let accepting = connection.accept_session(incoming);
        tokio::pin!(accepting);
        std::future::poll_fn(|cx| {
            assert!(accepting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        ready().await;
        advance(Duration::from_secs(2)).await;
        assert!(accepting.await.is_err(), "application waiter released");
        connection.lifecycle.wait_terminated().await;
        assert_eq!(Instant::now() - start, Duration::from_secs(2));
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining)
            .await
            .expect("reader joined after interrupted IO");
        if block_flush {
            let mut remaining = remaining.as_slice();
            assert!(matches!(
                read_frame(&mut remaining)
                    .await
                    .expect("one complete but unflushed frame"),
                Frame::Amqp {
                    performative: Some(Performative::Begin(_)),
                    ..
                }
            ));
            assert!(
                remaining.is_empty(),
                "no Close appended after stalled flush"
            );
        } else {
            assert_eq!(remaining.len(), 1, "no Close appended to a partial frame");
        }
    }
}

include!("idle_followup_tests.rs");

#[cfg(feature = "test-client")]
#[tokio::test(start_paused = true)]
async fn client_receive_watchdog_interrupts_blocked_write_and_flush_before_local_write_timeout() {
    for block_flush in [false, true] {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let gate = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner: wire,
            gate: gate.clone(),
            block_flush,
        };
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            next_frame(&mut peer).await;
            peer_open(&mut peer, None).await;
        };
        let (connection, ()) = tokio::join!(
            ClientConnection::builder()
                .idle_timeout_millis(1000)
                .open_with_stream(stream),
            opening
        );
        let mut connection = connection.expect("client opens");
        ready().await;
        gate.blocked.store(true, Ordering::Release);
        let start = Instant::now();
        {
            let beginning = connection.begin();
            tokio::pin!(beginning);
            std::future::poll_fn(|cx| {
                assert!(beginning.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            ready().await;
            advance(Duration::from_secs(2)).await;
            assert!(beginning.await.is_err(), "application waiter released");
        }
        connection.wait_terminated().await;
        assert_eq!(Instant::now() - start, Duration::from_secs(2));
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining)
            .await
            .expect("reader joined after interrupted IO");
        if block_flush {
            let mut remaining = remaining.as_slice();
            assert!(matches!(
                read_frame(&mut remaining)
                    .await
                    .expect("one complete but unflushed frame"),
                Frame::Amqp {
                    performative: Some(Performative::Begin(_)),
                    ..
                }
            ));
            assert!(
                remaining.is_empty(),
                "no Close appended after stalled flush"
            );
        } else {
            assert_eq!(remaining.len(), 1, "no Close appended to a partial frame");
        }
    }
}
