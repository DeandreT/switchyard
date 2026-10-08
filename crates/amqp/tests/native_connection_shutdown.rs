//! Native transport custody after a real AMQP header/Open handshake.

use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use amqp::{
    Attach, Begin, Binary, Close, ConnectionShutdownError, EngineError, Frame, LinkEndpoint,
    Message, Open, Performative, ProtocolHeader, ReceiverSettleMode, Role, Sender,
    SenderSettleMode, ServerConnection, Source, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex},
    sync::Notify,
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(3);

#[derive(Default)]
struct Probe {
    changed: Notify,
    read_pending: AtomicBool,
    read_bytes: AtomicUsize,
    block_writes: AtomicBool,
    write_pending: AtomicBool,
    buffer_writes: AtomicBool,
    block_flush: AtomicBool,
    flush_pending: AtomicBool,
    panic_read: AtomicBool,
    panic_write: AtomicBool,
    dropped: AtomicBool,
}

impl Probe {
    async fn wait(&self, ready: impl Fn() -> bool) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if ready() {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("witness must arrive before deadline");
    }

    async fn wait_drop(&self) {
        self.wait(|| self.dropped.load(Ordering::SeqCst)).await;
    }
}

struct WitnessIo {
    inner: DuplexStream,
    probe: Arc<Probe>,
    buffered: Vec<u8>,
}

impl Drop for WitnessIo {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
        self.probe.changed.notify_waiters();
    }
}

impl AsyncRead for WitnessIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert!(
            !self.probe.panic_read.load(Ordering::SeqCst),
            "reader custody witness panic"
        );
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if result.is_pending() {
            self.probe.read_pending.store(true, Ordering::SeqCst);
        } else if matches!(result, Poll::Ready(Ok(()))) {
            self.probe
                .read_bytes
                .fetch_add(buf.filled().len() - before, Ordering::SeqCst);
        }
        self.probe.changed.notify_waiters();
        result
    }
}

impl AsyncWrite for WitnessIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        assert!(
            !self.probe.panic_write.load(Ordering::SeqCst),
            "driver custody witness panic"
        );
        if self.probe.block_writes.load(Ordering::SeqCst) {
            self.probe.write_pending.store(true, Ordering::SeqCst);
            self.probe.changed.notify_waiters();
            return Poll::Pending;
        }
        if self.probe.buffer_writes.load(Ordering::SeqCst) {
            self.buffered.extend_from_slice(bytes);
            return Poll::Ready(Ok(bytes.len()));
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.probe.block_flush.load(Ordering::SeqCst) {
            this.probe.flush_pending.store(true, Ordering::SeqCst);
            this.probe.changed.notify_waiters();
            return Poll::Pending;
        }
        while !this.buffered.is_empty() {
            let written = match Pin::new(&mut this.inner).poll_write(cx, &this.buffered) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(written)) => written,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            };
            this.buffered.drain(..written);
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn handshake(peer: &mut DuplexStream) -> io::Result<()> {
    write_protocol_header(peer, ProtocolHeader::AMQP).await?;
    assert_eq!(read_protocol_header(peer).await?, ProtocolHeader::AMQP);
    write_frame(
        peer,
        &frame(0, Performative::Open(Open::new("custody-peer"))),
    )
    .await?;
    assert!(matches!(
        read_frame(peer).await?,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    Ok(())
}

async fn opened() -> (ServerConnection, DuplexStream, Arc<Probe>) {
    let (inner, mut peer) = duplex(64 * 1024);
    let probe = Arc::new(Probe::default());
    let stream = WitnessIo {
        inner,
        probe: Arc::clone(&probe),
        buffered: Vec::new(),
    };
    let (connection, peer_result) = timeout(LIMIT, async {
        tokio::join!(
            ServerConnection::accept(stream, "custody-server", None),
            handshake(&mut peer)
        )
    })
    .await
    .expect("native handshake must finish");
    peer_result.expect("peer handshake");
    let before = probe.read_bytes.load(Ordering::SeqCst);
    probe.read_pending.store(false, Ordering::SeqCst);
    peer.write_all(&[0, 0, 0, 8, 2, 0, 0, 0])
        .await
        .expect("post-Open heartbeat");
    probe
        .wait(|| {
            probe.read_bytes.load(Ordering::SeqCst) >= before + 8
                && probe.read_pending.load(Ordering::SeqCst)
        })
        .await;
    (connection.expect("server handshake"), peer, probe)
}

async fn joined(connection: &mut ServerConnection, probe: &Probe) {
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("shutdown must finish")
        .expect("tasks must join");
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "owned IO must be dropped before joined shutdown returns"
    );
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat shutdown must finish")
        .expect("cached successful joins");
}

async fn read_close(peer: &mut DuplexStream) {
    assert!(matches!(
        timeout(LIMIT, read_frame(peer))
            .await
            .expect("Close must arrive")
            .expect("valid Close"),
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(_)),
            ..
        }
    ));
}

async fn attached_sender(connection: &mut ServerConnection, peer: &mut DuplexStream) -> Sender {
    write_frame(peer, &frame(1, Performative::Begin(Begin::default())))
        .await
        .expect("peer Begin");
    let incoming = timeout(LIMIT, connection.next_incoming_session())
        .await
        .expect("incoming session")
        .expect("Begin offered");
    let mut session = timeout(LIMIT, connection.accept_session(incoming))
        .await
        .expect("accept session")
        .expect("session accepted");
    assert!(matches!(
        read_frame(peer).await.expect("server Begin"),
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    let attach = Attach {
        name: String::from("custody-sender"),
        handle: 1,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("orders")),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    write_frame(peer, &frame(1, Performative::Attach(Box::new(attach))))
        .await
        .expect("peer Attach");
    let incoming = timeout(LIMIT, session.next_incoming_attach())
        .await
        .expect("incoming attach")
        .expect("Attach offered");
    let endpoint = timeout(LIMIT, session.accept_attach(incoming, 1024 * 1024))
        .await
        .expect("accept link")
        .expect("link accepted");
    assert!(matches!(
        read_frame(peer).await.expect("server Attach"),
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    let LinkEndpoint::Sender(sender) = endpoint else {
        panic!("peer receiver creates server sender")
    };
    sender
}

#[tokio::test]
async fn stop_joins_an_idle_reader_and_drops_owned_io() {
    let (mut connection, _peer, probe) = opened().await;
    probe
        .wait(|| probe.read_pending.load(Ordering::SeqCst))
        .await;
    connection.stop();
    connection.stop();
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn stop_joins_a_reader_suspended_inside_a_partial_frame() {
    let (mut connection, mut peer, probe) = opened().await;
    probe
        .wait(|| probe.read_pending.load(Ordering::SeqCst))
        .await;
    let before = probe.read_bytes.load(Ordering::SeqCst);
    probe.read_pending.store(false, Ordering::SeqCst);
    peer.write_all(&[0, 0, 0, 12])
        .await
        .expect("partial frame prefix");
    probe
        .wait(|| {
            probe.read_bytes.load(Ordering::SeqCst) >= before + 4
                && probe.read_pending.load(Ordering::SeqCst)
        })
        .await;
    connection.stop();
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn stop_cancels_the_whole_driver_operation_when_close_write_is_blocked() {
    let (mut connection, mut peer, probe) = opened().await;
    let sender = timeout(LIMIT, attached_sender(&mut connection, &mut peer))
        .await
        .expect("native link handshake");
    let mut queued_send =
        Box::pin(sender.send_pending(Message::data(b"queued".to_vec()), Binary::from(vec![1])));
    assert!(
        poll_fn(|cx| Poll::Ready(queued_send.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    probe.block_writes.store(true, Ordering::SeqCst);
    {
        let mut close = Box::pin(connection.close());
        assert!(
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        probe
            .wait(|| probe.write_pending.load(Ordering::SeqCst))
            .await;
    }
    connection.stop();
    joined(&mut connection, &probe).await;
    assert!(matches!(
        timeout(LIMIT, queued_send)
            .await
            .expect("queued send cleanup"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(LIMIT, sender.on_credit())
            .await
            .expect("credit waiter cleanup"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(LIMIT, sender.on_drain())
            .await
            .expect("drain waiter cleanup"),
        Err(EngineError::RemoteDetached)
    ));
}

#[tokio::test]
async fn stop_keeps_link_cleanup_when_a_local_detach_write_is_blocked() {
    let (mut connection, mut peer, probe) = opened().await;
    let sender = timeout(LIMIT, attached_sender(&mut connection, &mut peer))
        .await
        .expect("native link handshake");
    let mut queued_send =
        Box::pin(sender.send_pending(Message::data(b"queued".to_vec()), Binary::from(vec![1])));
    assert!(
        poll_fn(|cx| Poll::Ready(queued_send.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    probe.block_writes.store(true, Ordering::SeqCst);
    {
        let mut detach = Box::pin(sender.close());
        assert!(
            poll_fn(|cx| Poll::Ready(detach.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        probe
            .wait(|| probe.write_pending.load(Ordering::SeqCst))
            .await;
    }
    connection.stop();
    joined(&mut connection, &probe).await;
    assert!(matches!(
        timeout(LIMIT, queued_send)
            .await
            .expect("queued send cleanup"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(LIMIT, sender.on_credit())
            .await
            .expect("credit cleanup"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(LIMIT, sender.on_drain())
            .await
            .expect("drain cleanup"),
        Err(EngineError::RemoteDetached)
    ));
}

#[tokio::test]
async fn a_canceled_borrowed_shutdown_can_be_retried_and_repeated() {
    let (mut connection, _peer, probe) = opened().await;
    probe
        .wait(|| probe.read_pending.load(Ordering::SeqCst))
        .await;
    {
        let mut shutdown = Box::pin(connection.shutdown());
        assert!(
            poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx)))
                .await
                .is_pending(),
            "current-thread tasks cannot finish their first join without yielding"
        );
    }
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn dropping_connection_requests_stop_without_peer_activity() {
    let (connection, _peer, probe) = opened().await;
    probe
        .wait(|| probe.read_pending.load(Ordering::SeqCst))
        .await;
    drop(connection);
    probe.wait_drop().await;
}

#[tokio::test]
async fn natural_eof_is_joinable_but_does_not_report_graceful_close_success() {
    let (mut connection, mut peer, probe) = opened().await;
    peer.shutdown().await.expect("peer write EOF");
    probe.wait_drop().await;
    assert!(matches!(
        timeout(LIMIT, connection.close())
            .await
            .expect("closed command queue"),
        Err(EngineError::Stopped)
    ));
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn graceful_close_keeps_its_peer_acknowledgement_contract() {
    let (mut connection, mut peer, probe) = opened().await;
    let (result, ()) = timeout(LIMIT, async {
        tokio::join!(connection.close(), async {
            read_close(&mut peer).await;
            write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
                .await
                .expect("peer Close reply");
        })
    })
    .await
    .expect("graceful Close handshake");
    result.expect("acknowledged Close succeeds");
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn peer_close_reply_is_flushed_before_native_io_is_dropped() {
    let (mut connection, mut peer, probe) = opened().await;
    probe.buffer_writes.store(true, Ordering::SeqCst);
    write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
        .await
        .expect("peer Close");
    read_close(&mut peer).await;
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn local_close_request_is_flushed_before_waiting_for_peer_ack() {
    let (mut connection, mut peer, probe) = opened().await;
    probe.buffer_writes.store(true, Ordering::SeqCst);
    let (result, ()) = timeout(LIMIT, async {
        tokio::join!(connection.close(), async {
            read_close(&mut peer).await;
            write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
                .await
                .expect("peer Close reply");
        })
    })
    .await
    .expect("buffered Close handshake");
    result.expect("peer acknowledgement is observed after explicit flush");
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn stop_cancels_a_terminal_close_flush_that_is_blocked() {
    let (mut connection, _peer, probe) = opened().await;
    probe.block_flush.store(true, Ordering::SeqCst);
    {
        let mut close = Box::pin(connection.close());
        assert!(
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        probe
            .wait(|| probe.flush_pending.load(Ordering::SeqCst))
            .await;
    }
    connection.stop();
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn eof_before_close_acknowledgement_is_not_fake_success() {
    let (mut connection, mut peer, probe) = opened().await;
    let (result, ()) = timeout(LIMIT, async {
        tokio::join!(connection.close(), async {
            read_close(&mut peer).await;
            peer.shutdown().await.expect("EOF instead of Close reply");
        })
    })
    .await
    .expect("Close refusal after EOF");
    assert!(matches!(result, Err(EngineError::RemoteClosed)));
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn queued_begin_and_close_are_processed_before_reader_eof() {
    let (mut connection, mut peer, probe) = opened().await;
    write_frame(&mut peer, &frame(7, Performative::Begin(Begin::default())))
        .await
        .expect("peer Begin");
    write_frame(&mut peer, &frame(0, Performative::Close(Close::default())))
        .await
        .expect("peer Close");
    peer.shutdown().await.expect("EOF behind queued frames");
    read_close(&mut peer).await;
    let incoming = timeout(LIMIT, connection.next_incoming_session())
        .await
        .expect("queued Begin must be preserved");
    assert!(incoming.is_some());
    joined(&mut connection, &probe).await;
}

#[tokio::test]
async fn reader_panic_requests_stop_and_is_cached_after_both_tasks_join() {
    let (mut connection, mut peer, probe) = opened().await;
    probe
        .wait(|| probe.read_pending.load(Ordering::SeqCst))
        .await;
    probe.panic_read.store(true, Ordering::SeqCst);
    peer.write_all(&[0, 0, 0, 8, 2, 0, 0, 0])
        .await
        .expect("wake the actual reader");
    probe.wait_drop().await;
    let first = timeout(LIMIT, connection.shutdown())
        .await
        .expect("join after reader panic")
        .expect_err("reader panic is not success");
    assert!(
        matches!(&first, ConnectionShutdownError::ReaderFailed(detail) if detail.contains("reader custody witness panic"))
    );
    let second = timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat failed join")
        .expect_err("cached reader failure");
    assert_eq!(first, second);
    assert!(probe.dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn driver_panic_is_cached_and_shutdown_still_joins_the_reader() {
    let (mut connection, _peer, probe) = opened().await;
    probe.panic_write.store(true, Ordering::SeqCst);
    assert!(matches!(
        timeout(LIMIT, connection.close())
            .await
            .expect("driver command must resolve"),
        Err(EngineError::Stopped)
    ));
    let first = timeout(LIMIT, connection.shutdown())
        .await
        .expect("join after driver panic")
        .expect_err("driver panic is not success");
    assert!(
        matches!(&first, ConnectionShutdownError::DriverFailed(detail) if detail.contains("driver custody witness panic"))
    );
    let second = timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat failed join")
        .expect_err("cached driver failure");
    assert_eq!(first, second);
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "failure must not skip the reader join"
    );
}
