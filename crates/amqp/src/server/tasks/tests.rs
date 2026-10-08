//! Original task identity and bounded-command pressure on accepted native IO.
//! Aborting both handles below is fault injection, not the shutdown policy.

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

use crate::server::engine::Command;
use crate::{
    ConnectionShutdownError, Frame, Open, Performative, ProtocolHeader, ServerConnection,
    read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::{Notify, mpsc::error::TrySendError, oneshot},
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(3);

#[derive(Default)]
struct Probe {
    block_write: AtomicBool,
    write_pending: AtomicBool,
    read_bytes: AtomicUsize,
    dropped: AtomicBool,
    changed: Notify,
}

impl Probe {
    async fn wait_write(&self) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.write_pending.load(Ordering::SeqCst) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("real driver write must block");
    }
}

struct WitnessIo {
    inner: DuplexStream,
    probe: Arc<Probe>,
}

impl Drop for WitnessIo {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
    }
}

impl AsyncRead for WitnessIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.probe
                .read_bytes
                .fetch_add(buf.filled().len() - before, Ordering::SeqCst);
            self.probe.changed.notify_waiters();
        }
        result
    }
}

impl AsyncWrite for WitnessIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.probe.block_write.load(Ordering::SeqCst) {
            self.probe.write_pending.store(true, Ordering::SeqCst);
            self.probe.changed.notify_waiters();
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn opened() -> (ServerConnection, DuplexStream, Arc<Probe>) {
    let (inner, mut peer) = duplex(64 * 1024);
    let probe = Arc::new(Probe::default());
    let stream = WitnessIo {
        inner,
        probe: Arc::clone(&probe),
    };
    let (connection, ()) = timeout(LIMIT, async {
        tokio::join!(
            ServerConnection::accept(stream, "custody-server", None),
            async {
                write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                    .await
                    .expect("peer header");
                assert_eq!(
                    read_protocol_header(&mut peer)
                        .await
                        .expect("server header"),
                    ProtocolHeader::AMQP
                );
                write_frame(
                    &mut peer,
                    &Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Open(Open::new("custody-peer"))),
                        payload: Vec::new(),
                    },
                )
                .await
                .expect("peer Open");
                assert!(matches!(
                    read_frame(&mut peer).await.expect("server Open"),
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
            }
        )
    })
    .await
    .expect("real native handshake");
    (connection.expect("accepted native connection"), peer, probe)
}

fn task_ids(connection: &ServerConnection) -> (tokio::task::Id, tokio::task::Id) {
    (
        connection
            .tasks
            .driver
            .as_ref()
            .expect("original driver")
            .id(),
        connection
            .tasks
            .reader
            .as_ref()
            .expect("original reader")
            .id(),
    )
}

#[tokio::test]
async fn canceled_shutdown_keeps_original_handles_and_stop_bypasses_full_commands() {
    let (mut connection, _peer, probe) = opened().await;
    let original = task_ids(&connection);
    probe.block_write.store(true, Ordering::SeqCst);
    {
        let mut close = Box::pin(connection.close());
        assert!(
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        probe.wait_write().await;
    }

    let mut queued = 0;
    loop {
        let (reply, _response) = oneshot::channel();
        match connection
            .commands
            .try_send(Command::Close { error: None, reply })
        {
            Ok(()) => queued += 1,
            Err(TrySendError::Full(_)) => break,
            Err(TrySendError::Closed(_)) => {
                panic!("driver must still be blocked on the actual write")
            }
        }
    }
    assert_eq!(queued, 256, "fill the actual connection command channel");
    assert_eq!(connection.commands.capacity(), 0);
    connection.stop();
    {
        let mut shutdown = Box::pin(connection.shutdown());
        assert!(
            poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert_eq!(
        task_ids(&connection),
        original,
        "cancellation must not detach or replace handles"
    );
    assert!(connection.tasks.driver_result.is_none());
    assert!(connection.tasks.reader_result.is_none());
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("stop independent of full commands")
        .expect("both native joins");
    assert_eq!(task_ids(&connection), original);
    assert!(matches!(
        connection.tasks.driver_result.as_ref(),
        Some(Ok(()))
    ));
    assert!(matches!(
        connection.tasks.reader_result.as_ref(),
        Some(Ok(()))
    ));
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "both split halves must release owned IO"
    );
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat shutdown")
        .expect("cached joins");
    assert_eq!(task_ids(&connection), original);
}

#[tokio::test]
async fn failure_of_both_original_tasks_is_joined_and_cached_without_replacement() {
    let (mut connection, _peer, probe) = opened().await;
    let original = task_ids(&connection);
    connection
        .tasks
        .driver
        .as_ref()
        .expect("original driver")
        .abort();
    connection
        .tasks
        .reader
        .as_ref()
        .expect("original reader")
        .abort();
    let first = timeout(LIMIT, connection.shutdown())
        .await
        .expect("both failed tasks must join")
        .expect_err("both failures are visible");
    assert!(
        matches!(&first, ConnectionShutdownError::BothFailed { driver, reader }
        if !driver.is_empty() && !reader.is_empty())
    );
    assert!(
        connection
            .tasks
            .driver_result
            .as_ref()
            .expect("driver result retained")
            .as_ref()
            .expect_err("driver canceled")
            .is_cancelled()
    );
    assert!(
        connection
            .tasks
            .reader_result
            .as_ref()
            .expect("reader result retained")
            .as_ref()
            .expect_err("reader canceled")
            .is_cancelled()
    );
    assert_eq!(task_ids(&connection), original);
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "error summary must not skip either join"
    );
    let second = timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat failed shutdown")
        .expect_err("cached both failures");
    assert_eq!(first, second);
    assert_eq!(task_ids(&connection), original);
}

async fn wait_read_bytes(probe: &Probe, expected: usize) {
    timeout(LIMIT, async {
        loop {
            let changed = probe.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if probe.read_bytes.load(Ordering::SeqCst) >= expected {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("actual reader must consume the complete wire prefix");
}

fn pressure_frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn write_pressure_frames(peer: &mut DuplexStream, frames: Vec<Frame>) -> usize {
    use tokio::io::AsyncWriteExt;

    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend_from_slice(&crate::encode_frame(&frame).expect("valid pressure frame"));
    }
    timeout(LIMIT, peer.write_all(&bytes))
        .await
        .expect("bounded wire write")
        .expect("pressure frames written");
    bytes.len()
}

async fn write_full_driver_channel_pressure(
    peer: &mut DuplexStream,
    probe: &Probe,
    mut items: Vec<Frame>,
) -> usize {
    let before = probe.read_bytes.load(Ordering::SeqCst);
    let item_bytes: usize = items
        .iter()
        .map(|frame| {
            crate::encode_frame(frame)
                .expect("valid pressure item")
                .len()
        })
        .sum();
    items.extend((0..257).map(|_| Frame::Amqp {
        channel: 0,
        performative: None,
        payload: Vec::new(),
    }));
    assert_eq!(
        write_pressure_frames(peer, items).await,
        item_bytes + 257 * 8
    );
    let blocked_prefix = before + item_bytes + 256 * 8;
    wait_read_bytes(probe, blocked_prefix).await;
    assert_eq!(
        probe.read_bytes.load(Ordering::SeqCst),
        blocked_prefix,
        "33 driver items plus 256 buffered frames and one pending frame fill both channels"
    );
    blocked_prefix + 8
}

async fn pressure_session(
    connection: &mut ServerConnection,
    peer: &mut DuplexStream,
) -> super::super::ServerSession {
    timeout(LIMIT, async {
        write_frame(
            peer,
            &pressure_frame(1, Performative::Begin(crate::Begin::default())),
        )
        .await
        .expect("peer Begin");
        let incoming = connection
            .next_incoming_session()
            .await
            .expect("incoming Begin");
        let session = connection
            .accept_session(incoming)
            .await
            .expect("accepted session");
        assert!(matches!(
            read_frame(peer).await.expect("server Begin"),
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        session
    })
    .await
    .expect("actual session handshake")
}

fn pressure_attach(handle: u32, role: crate::Role) -> crate::Attach {
    crate::Attach {
        name: format!("pressure-{handle}"),
        handle,
        role: role.clone(),
        snd_settle_mode: crate::SenderSettleMode::Unsettled,
        rcv_settle_mode: crate::ReceiverSettleMode::First,
        source: Some(crate::Source::new("orders")),
        target: Some(crate::Target::new("orders")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == crate::Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn pressure_receiver(
    session: &mut super::super::ServerSession,
    peer: &mut DuplexStream,
) -> super::super::Receiver {
    timeout(LIMIT, async {
        write_frame(
            peer,
            &pressure_frame(
                1,
                Performative::Attach(Box::new(pressure_attach(1, crate::Role::Sender))),
            ),
        )
        .await
        .expect("peer sender Attach");
        let attach = session
            .next_incoming_attach()
            .await
            .expect("incoming Attach");
        let endpoint = session
            .accept_attach(attach, 1024 * 1024)
            .await
            .expect("accepted link");
        assert!(matches!(
            read_frame(peer).await.expect("server Attach"),
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Attach(_)),
                ..
            }
        ));
        assert!(matches!(
            read_frame(peer).await.expect("server Flow"),
            Frame::Amqp {
                channel: 1,
                performative: Some(Performative::Flow(_)),
                ..
            }
        ));
        let super::super::LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("peer sender must create an actual receiving link");
        };
        receiver
    })
    .await
    .expect("actual link handshake")
}

async fn pressure_joined(
    connection: &mut ServerConnection,
    probe: &Probe,
    original: (tokio::task::Id, tokio::task::Id),
) {
    connection.stop();
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("stop must cancel full-channel waits")
        .expect("both original native tasks join");
    assert_eq!(task_ids(connection), original);
    assert!(matches!(
        connection.tasks.driver_result.as_ref(),
        Some(Ok(()))
    ));
    assert!(matches!(
        connection.tasks.reader_result.as_ref(),
        Some(Ok(()))
    ));
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "owned IO is released after both joins"
    );
    timeout(LIMIT, connection.shutdown())
        .await
        .expect("repeat shutdown")
        .expect("cached original joins");
    assert_eq!(task_ids(connection), original);
}

#[tokio::test]
async fn stop_cancels_full_actual_incoming_session_channel() {
    let (mut connection, mut peer, probe) = opened().await;
    let original = task_ids(&connection);
    let frames = (1..=34)
        .map(|channel| pressure_frame(channel, Performative::Begin(crate::Begin::default())))
        .collect();
    let last_heartbeat = write_full_driver_channel_pressure(&mut peer, &probe, frames).await;
    assert_eq!(connection.incoming_sessions.len(), 32);
    let first = connection
        .incoming_sessions
        .try_recv()
        .expect("first of 32 offered sessions");
    assert_eq!(first.channel, 1);
    // Reading the last heartbeat proves the driver consumed item 34 after refilling the offer queue.
    wait_read_bytes(&probe, last_heartbeat).await;
    assert_eq!(probe.read_bytes.load(Ordering::SeqCst), last_heartbeat);
    assert_eq!(connection.incoming_sessions.len(), 32);
    pressure_joined(&mut connection, &probe, original).await;
    for channel in 2..=33 {
        let incoming = connection
            .incoming_sessions
            .try_recv()
            .expect("retained offered session");
        assert_eq!(incoming.channel, channel);
    }
    assert!(matches!(
        connection.incoming_sessions.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn stop_cancels_full_actual_incoming_attach_channel() {
    let (mut connection, mut peer, probe) = opened().await;
    let original = task_ids(&connection);
    let mut session = pressure_session(&mut connection, &mut peer).await;
    let frames = (0..34)
        .map(|handle| {
            pressure_frame(
                1,
                Performative::Attach(Box::new(pressure_attach(handle, crate::Role::Receiver))),
            )
        })
        .collect();
    let last_heartbeat = write_full_driver_channel_pressure(&mut peer, &probe, frames).await;
    assert_eq!(session.incoming_attaches.len(), 32);
    let first = session
        .incoming_attaches
        .try_recv()
        .expect("first of 32 offered attaches");
    assert_eq!(first.attach.handle, 0);
    wait_read_bytes(&probe, last_heartbeat).await;
    assert_eq!(probe.read_bytes.load(Ordering::SeqCst), last_heartbeat);
    assert_eq!(session.incoming_attaches.len(), 32);
    pressure_joined(&mut connection, &probe, original).await;
    for handle in 1..=32 {
        let incoming = session
            .incoming_attaches
            .try_recv()
            .expect("retained offered attach");
        assert_eq!(incoming.attach.handle, handle);
    }
    assert!(matches!(
        session.incoming_attaches.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn stop_cancels_full_actual_incoming_delivery_channel() {
    let (mut connection, mut peer, probe) = opened().await;
    let original = task_ids(&connection);
    let mut session = pressure_session(&mut connection, &mut peer).await;
    let mut receiver = pressure_receiver(&mut session, &mut peer).await;
    let frames = (0..34)
        .map(|id| Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Transfer(crate::Transfer {
                handle: 1,
                delivery_id: Some(id),
                delivery_tag: Some(crate::Binary::from(vec![id as u8])),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload: crate::encode_message(&crate::Message::data(vec![id as u8]))
                .expect("valid delivery"),
        })
        .collect();
    let last_heartbeat = write_full_driver_channel_pressure(&mut peer, &probe, frames).await;
    assert_eq!(receiver.deliveries.len(), 32);
    let first = receiver
        .deliveries
        .try_recv()
        .expect("first of 32 offered deliveries");
    assert_eq!(first.id, 0);
    wait_read_bytes(&probe, last_heartbeat).await;
    assert_eq!(probe.read_bytes.load(Ordering::SeqCst), last_heartbeat);
    assert_eq!(receiver.deliveries.len(), 32);
    pressure_joined(&mut connection, &probe, original).await;
    assert!(
        *receiver.detached.borrow(),
        "full-channel receiving link is notified during cleanup"
    );
    for id in 1..=32 {
        let delivery = receiver
            .deliveries
            .try_recv()
            .expect("retained offered delivery");
        assert_eq!(delivery.id, id);
        assert_eq!(
            delivery.encoded_message,
            crate::encode_message(&crate::Message::data(vec![id as u8]))
                .expect("expected typed body")
        );
    }
    assert!(matches!(
        receiver.deliveries.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        timeout(LIMIT, receiver.recv())
            .await
            .expect("closed receiving link"),
        Err(crate::EngineError::RemoteDetached)
    ));
}

#[tokio::test]
async fn stop_cancels_actual_reader_send_into_full_frame_channel() {
    let (mut connection, mut peer, probe) = opened().await;
    let original = task_ids(&connection);
    probe.block_write.store(true, Ordering::SeqCst);
    {
        let mut close = Box::pin(connection.close());
        assert!(
            poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        probe.wait_write().await;
    }
    let before = probe.read_bytes.load(Ordering::SeqCst);
    let bytes = write_pressure_frames(
        &mut peer,
        (0..258)
            .map(|_| Frame::Amqp {
                channel: 0,
                performative: None,
                payload: Vec::new(),
            })
            .collect(),
    )
    .await;
    assert_eq!(bytes, 258 * 8);
    let expected = before + 257 * 8;
    wait_read_bytes(&probe, expected).await;
    assert_eq!(
        probe.read_bytes.load(Ordering::SeqCst),
        expected,
        "blocked driver cannot drain the 256 frames; the 257th is read but its send waits"
    );
    pressure_joined(&mut connection, &probe, original).await;
    assert_eq!(
        probe.read_bytes.load(Ordering::SeqCst),
        expected,
        "stop cancels the full-frame send before reading the 258th heartbeat"
    );
}

#[tokio::test]
async fn canceled_shutdown_primitive_preserves_first_cached_join_and_second_original_handle() {
    // This controlled original pair proves join custody only, not native transport scheduling.
    let (stop, _receiver) = tokio::sync::watch::channel(false);
    let (driver_started, driver_observed) = oneshot::channel();
    let driver: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let _ = driver_started.send(());
        panic!("cached join primitive driver panic");
    });
    let (reader_release, reader_wait) = oneshot::channel();
    let reader = tokio::spawn(async move {
        reader_wait
            .await
            .expect("release original primitive reader");
    });
    let original = (driver.id(), reader.id());
    let mut tasks = super::ConnectionTasks {
        stop,
        driver: Some(driver),
        reader: Some(reader),
        driver_result: None,
        reader_result: None,
    };
    timeout(LIMIT, driver_observed)
        .await
        .expect("original driver runs")
        .expect("driver witness");
    assert!(
        tasks
            .driver
            .as_ref()
            .expect("original driver")
            .is_finished()
    );
    assert!(
        !tasks
            .reader
            .as_ref()
            .expect("original reader")
            .is_finished()
    );
    {
        let mut shutdown = Box::pin(tasks.shutdown());
        assert!(
            poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx)))
                .await
                .is_pending(),
            "cached first join must not require the held reader to finish"
        );
    }
    let first_raw = tasks
        .driver_result
        .as_ref()
        .expect("first join cached before second await")
        .as_ref()
        .expect_err("original driver panicked")
        .to_string();
    assert!(first_raw.contains("cached join primitive driver panic"));
    assert!(tasks.reader_result.is_none());
    assert_eq!(
        (
            tasks.driver.as_ref().expect("driver handle retained").id(),
            tasks.reader.as_ref().expect("reader handle retained").id()
        ),
        original
    );
    reader_release
        .send(())
        .expect("release the same original reader");
    let first = timeout(LIMIT, tasks.shutdown())
        .await
        .expect("retry joins original reader")
        .expect_err("cached original driver failure");
    assert!(
        matches!(&first, ConnectionShutdownError::DriverFailed(detail)
        if detail.contains("cached join primitive driver panic"))
    );
    assert!(matches!(tasks.reader_result.as_ref(), Some(Ok(()))));
    assert_eq!(
        tasks
            .driver_result
            .as_ref()
            .expect("driver result remains cached")
            .as_ref()
            .expect_err("same driver failure")
            .to_string(),
        first_raw
    );
    let repeated = timeout(LIMIT, tasks.shutdown())
        .await
        .expect("repeat completed primitive joins")
        .expect_err("same cached original failure");
    assert_eq!(first, repeated);
    assert_eq!(
        (
            tasks.driver.as_ref().expect("driver handle retained").id(),
            tasks.reader.as_ref().expect("reader handle retained").id()
        ),
        original
    );
}
