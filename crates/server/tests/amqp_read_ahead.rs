//! A stalled actor leaves only a small, ordered frame backlog in its reader.

use std::{
    error::Error,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use amqp::{
    Attach, Begin, Body, ClientConnection, ClientReceiver, ClientSession, ConnectionOptions,
    DeliveryState, Flow, Frame, Message, Open, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SenderSettleMode, ServerConnection, ServerSession, Source, Target, Transfer,
    encode_frame, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::Notify,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const CASE_TIMEOUT: Duration = Duration::from_secs(30);
const FRAME_COUNT: usize = 20;
const READ_AHEAD: usize = 17;

#[derive(Default)]
struct IoProbe {
    bytes_read: AtomicUsize,
    dropped: AtomicBool,
    read_progress: Notify,
    flush_blocked: AtomicBool,
    flush_reached: AtomicBool,
    flush_progress: Notify,
    flush_waker: Mutex<Option<Waker>>,
}

impl IoProbe {
    fn arm_flush(&self) {
        self.flush_reached.store(false, Ordering::Release);
        self.flush_blocked.store(true, Ordering::Release);
    }

    fn release_flush(&self) {
        self.flush_blocked.store(false, Ordering::Release);
        if let Some(waker) = self.flush_waker.lock().expect("flush waker").take() {
            waker.wake();
        }
    }

    async fn wait_for_flush(&self) {
        while !self.flush_reached.load(Ordering::Acquire) {
            self.flush_progress.notified().await;
        }
    }

    async fn assert_read_ahead(&self, expected: usize) {
        while self.bytes_read.load(Ordering::Acquire) < expected {
            self.read_progress.notified().await;
        }
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            self.bytes_read.load(Ordering::Acquire),
            expected,
            "only sixteen queued frames and one reader-pending frame are consumed"
        );
    }
}

struct ProbedIo {
    inner: DuplexStream,
    probe: Arc<IoProbe>,
}

impl AsyncRead for ProbedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        let read = buffer.filled().len() - before;
        if read != 0 {
            self.probe.bytes_read.fetch_add(read, Ordering::AcqRel);
            self.probe.read_progress.notify_one();
        }
        result
    }
}

impl AsyncWrite for ProbedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut waker = self.probe.flush_waker.lock().expect("flush waker");
            if self.probe.flush_blocked.load(Ordering::Acquire) {
                *waker = Some(cx.waker().clone());
                self.probe.flush_reached.store(true, Ordering::Release);
                self.probe.flush_progress.notify_one();
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Drop for ProbedIo {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::Release);
    }
}

struct Peer(DuplexStream);

impl Peer {
    async fn send(&mut self, performative: Performative) -> TestResult {
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.0,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload: Vec::new(),
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.0)).await??)
    }

    async fn block_actor(&mut self, probe: &IoProbe) -> TestResult<usize> {
        probe.arm_flush();
        self.send(Performative::Flow(Flow {
            next_incoming_id: Some(0),
            incoming_window: 2_048,
            next_outgoing_id: 0,
            outgoing_window: 2_048,
            echo: true,
            ..Flow::default()
        }))
        .await?;
        timeout(IO_TIMEOUT, probe.wait_for_flush()).await?;
        assert!(matches!(self.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Flow(flow)),
            ..
        } if flow.handle.is_none()));
        Ok(probe.bytes_read.load(Ordering::Acquire))
    }

    async fn preload(
        &mut self,
        frames: Vec<Frame>,
        probe: &IoProbe,
        baseline: usize,
    ) -> TestResult<usize> {
        assert_eq!(frames.len(), FRAME_COUNT);
        let mut bytes = Vec::new();
        let mut prefix = 0;
        for (index, frame) in frames.iter().enumerate() {
            let encoded = encode_frame(frame)?;
            if index < READ_AHEAD {
                prefix += encoded.len();
            }
            bytes.extend_from_slice(&encoded);
        }
        timeout(IO_TIMEOUT, self.0.write_all(&bytes)).await??;
        let expected = baseline + prefix;
        timeout(IO_TIMEOUT, probe.assert_read_ahead(expected)).await?;
        assert!(!probe.dropped.load(Ordering::Acquire));
        Ok(expected)
    }

    async fn assert_eof_without_more_reads(
        &mut self,
        probe: &IoProbe,
        expected: usize,
    ) -> TestResult {
        assert!(
            probe.dropped.load(Ordering::Acquire),
            "both IO halves were dropped"
        );
        let mut tail = Vec::new();
        timeout(IO_TIMEOUT, self.0.read_to_end(&mut tail)).await??;
        assert!(tail.is_empty(), "cancellation did not append another frame");
        assert_eq!(probe.bytes_read.load(Ordering::Acquire), expected);
        Ok(())
    }
}

fn transport() -> (ProbedIo, Peer, Arc<IoProbe>) {
    let (wire, peer) = tokio::io::duplex(64 * 1024);
    let probe = Arc::new(IoProbe::default());
    (
        ProbedIo {
            inner: wire,
            probe: probe.clone(),
        },
        Peer(peer),
        probe,
    )
}

struct ServerNode {
    connection: ServerConnection,
    session: ServerSession,
    peer: Peer,
    probe: Arc<IoProbe>,
}

impl ServerNode {
    async fn new() -> TestResult<Self> {
        let (stream, mut peer, probe) = transport();
        let opening = async {
            write_protocol_header(&mut peer.0, ProtocolHeader::AMQP).await?;
            assert_eq!(
                read_protocol_header(&mut peer.0).await?,
                ProtocolHeader::AMQP
            );
            peer.send(Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: None,
                ..Open::new("read-ahead-peer")
            }))
            .await?;
            assert!(matches!(
                peer.read().await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            Ok::<_, Box<dyn Error>>(())
        };
        let (connection, opening) = tokio::join!(
            ServerConnection::accept_with_options(
                stream,
                "read-ahead-server",
                None,
                ConnectionOptions::default().idle_timeout_millis(0),
            ),
            opening,
        );
        opening?;
        let mut connection = connection?;
        peer.send(Performative::Begin(Begin::default())).await?;
        let incoming = connection
            .next_incoming_session()
            .await
            .expect("incoming session");
        let session = connection.accept_session(incoming).await?;
        assert!(matches!(peer.read().await?, Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Begin(begin)), ..
        } if begin.remote_channel == Some(0)));
        Ok(Self {
            connection,
            session,
            peer,
            probe,
        })
    }

    fn frames() -> Vec<Frame> {
        (0..FRAME_COUNT)
            .map(|index| Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Attach(Box::new(Attach {
                    name: format!("read-ahead-{index}"),
                    handle: index as u32,
                    role: Role::Sender,
                    snd_settle_mode: SenderSettleMode::Unsettled,
                    rcv_settle_mode: ReceiverSettleMode::First,
                    source: Some(Source::new("queue")),
                    target: Some(Target::new("queue").into()),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: Some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }))),
                payload: Vec::new(),
            })
            .collect()
    }
}

struct ClientNode {
    connection: ClientConnection,
    _session: ClientSession,
    receiver: ClientReceiver,
    peer: Peer,
    probe: Arc<IoProbe>,
}

impl ClientNode {
    async fn new() -> TestResult<Self> {
        let (stream, mut peer, probe) = transport();
        let opening = async {
            assert_eq!(
                read_protocol_header(&mut peer.0).await?,
                ProtocolHeader::AMQP
            );
            write_protocol_header(&mut peer.0, ProtocolHeader::AMQP).await?;
            assert!(matches!(
                peer.read().await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            peer.send(Performative::Open(Open {
                max_frame_size: 512,
                idle_time_out: None,
                ..Open::new("read-ahead-raw-server")
            }))
            .await
        };
        let (connection, opening) = tokio::join!(
            ClientConnection::builder()
                .container_id("read-ahead-client")
                .max_frame_size(512)
                .idle_timeout_millis(0)
                .open_with_stream(stream),
            opening,
        );
        opening?;
        let mut connection = connection?;
        let (session, ()) = tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
            async {
                assert!(matches!(
                    peer.read().await?,
                    Frame::Amqp {
                        channel: 0,
                        performative: Some(Performative::Begin(_)),
                        ..
                    }
                ));
                peer.send(Performative::Begin(Begin {
                    remote_channel: Some(0),
                    ..Begin::default()
                }))
                .await
            },
        )?;
        let mut session = session;
        let (receiver, ()) = tokio::try_join!(
            async {
                Ok::<_, Box<dyn Error>>(session.attach_receiver("read-ahead", "queue").await?)
            },
            async {
                let Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(request)),
                    ..
                } = peer.read().await?
                else {
                    panic!("client Attach")
                };
                assert_eq!(request.role, Role::Receiver);
                assert_eq!(request.handle, 0);
                let response = request.response(request.source.clone(), request.target.clone());
                peer.send(Performative::Attach(Box::new(response))).await
            },
        )?;
        assert!(matches!(peer.read().await?, Frame::Amqp {
            channel: 0, performative: Some(Performative::Flow(flow)), ..
        } if flow.handle == Some(0) && flow.link_credit == Some(32)));
        Ok(Self {
            connection,
            _session: session,
            receiver,
            peer,
            probe,
        })
    }

    fn frames() -> TestResult<Vec<Frame>> {
        (0..FRAME_COUNT)
            .map(|index| {
                Ok(Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Transfer(Transfer {
                        handle: 0,
                        delivery_id: Some(index as u32),
                        delivery_tag: Some(vec![index as u8].into()),
                        message_format: Some(0),
                        settled: Some(false),
                        more: false,
                        rcv_settle_mode: None,
                        state: None,
                        resume: false,
                        aborted: false,
                        batchable: false,
                    })),
                    payload: encode_message(&Message::data(vec![index as u8; index + 1]))?,
                })
            })
            .collect()
    }
}

async fn server_fifo() -> TestResult {
    let mut node = ServerNode::new().await?;
    let baseline = node.peer.block_actor(&node.probe).await?;
    node.peer
        .preload(ServerNode::frames(), &node.probe, baseline)
        .await?;
    node.probe.release_flush();
    let mut approvals = Vec::new();
    for index in 0..FRAME_COUNT {
        let incoming = node
            .session
            .next_incoming_attach()
            .await
            .expect("ordered approval");
        assert_eq!(incoming.attach().handle, index as u32);
        assert_eq!(incoming.attach().name, format!("read-ahead-{index}"));
        approvals.push(incoming);
    }
    node.connection.shutdown().await;
    assert!(node.probe.dropped.load(Ordering::Acquire));
    Ok(())
}

async fn client_fifo() -> TestResult {
    let mut node = ClientNode::new().await?;
    let baseline = node.peer.block_actor(&node.probe).await?;
    node.peer
        .preload(ClientNode::frames()?, &node.probe, baseline)
        .await?;
    node.probe.release_flush();
    for index in 0..FRAME_COUNT {
        let delivery = node.receiver.recv().await?;
        let Body::Data(sections) = &delivery.message().body else {
            panic!("data body")
        };
        assert_eq!(sections.len(), 1);
        assert_eq!(
            sections[0].as_ref(),
            vec![index as u8; index + 1].as_slice()
        );
        node.receiver.accept(&delivery).await?;
        loop {
            match node.peer.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, index as u32);
                    assert_eq!(disposition.last.unwrap_or(disposition.first), index as u32);
                    assert!(disposition.settled);
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                frame => panic!("expected settlement or credit Flow, got {frame:?}"),
            }
        }
    }
    node.connection.shutdown().await;
    assert!(node.probe.dropped.load(Ordering::Acquire));
    Ok(())
}

async fn server_cancellation() -> TestResult {
    let mut node = ServerNode::new().await?;
    let baseline = node.peer.block_actor(&node.probe).await?;
    let expected = node
        .peer
        .preload(ServerNode::frames(), &node.probe, baseline)
        .await?;
    node.connection.shutdown().await;
    node.peer
        .assert_eof_without_more_reads(&node.probe, expected)
        .await
}

async fn client_cancellation() -> TestResult {
    let mut node = ClientNode::new().await?;
    let baseline = node.peer.block_actor(&node.probe).await?;
    let expected = node
        .peer
        .preload(ClientNode::frames()?, &node.probe, baseline)
        .await?;
    node.connection.shutdown().await;
    node.peer
        .assert_eof_without_more_reads(&node.probe, expected)
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn server_frame_read_ahead_is_bounded_and_approvals_remain_fifo() -> TestResult {
    timeout(CASE_TIMEOUT, server_fifo()).await?
}

#[tokio::test(flavor = "current_thread")]
async fn client_frame_read_ahead_is_bounded_and_unsettled_deliveries_remain_fifo() -> TestResult {
    timeout(CASE_TIMEOUT, client_fifo()).await?
}

#[tokio::test(flavor = "current_thread")]
async fn server_shutdown_joins_the_reader_while_its_queue_and_actor_are_blocked() -> TestResult {
    timeout(CASE_TIMEOUT, server_cancellation()).await?
}

#[tokio::test(flavor = "current_thread")]
async fn client_shutdown_joins_the_reader_while_its_queue_and_actor_are_blocked() -> TestResult {
    timeout(CASE_TIMEOUT, client_cancellation()).await?
}
