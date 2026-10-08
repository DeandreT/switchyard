//! Session closure remains observable with buffered attach offers.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use amqp::{
    Attach, Begin, End, EngineError, Frame, LinkEndpoint, Open, Performative, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, ServerSession, Source,
    read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::Notify,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct WriteState {
    blocked: bool,
    entered: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<WriteState>,
    changed: Notify,
}

impl WriteGate {
    fn block(&self) {
        *self.state.lock().unwrap() = WriteState {
            blocked: true,
            ..WriteState::default()
        };
    }

    async fn reached(&self) {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.state.lock().unwrap().entered {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("original native writer reached the answering-End gate");
    }

    fn release(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.blocked = false;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct GatedIo {
    inner: DuplexStream,
    gate: Arc<WriteGate>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        {
            let mut state = self.gate.state.lock().unwrap();
            if state.blocked {
                state.entered = true;
                state.waker = Some(cx.waker().clone());
                self.gate.changed.notify_waiters();
                return Poll::Pending;
            }
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

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn control(peer: &mut DuplexStream, expected_channel: u16) -> Performative {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
    else {
        panic!("actual native control frame");
    };
    assert_eq!(channel, expected_channel);
    assert!(payload.is_empty());
    performative
}

struct Wire {
    connection: ServerConnection,
    peer: DuplexStream,
    writes: Arc<WriteGate>,
}

impl Wire {
    async fn new() -> Self {
        let (inner, mut peer) = duplex(64 * 1024);
        let writes = Arc::new(WriteGate::default());
        let stream = GatedIo {
            inner,
            gate: Arc::clone(&writes),
        };
        let (connection, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept(stream, "session-lifetime", None),
                async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut peer).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                    write_frame(
                        &mut peer,
                        &frame(0, Performative::Open(Open::new("lifetime-peer"))),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(control(&mut peer, 0).await, Performative::Open(_)));
                }
            )
        })
        .await
        .unwrap();
        Self {
            connection: connection.unwrap(),
            peer,
            writes,
        }
    }

    async fn begin(&mut self, channel: u16) -> ServerSession {
        write_frame(
            &mut self.peer,
            &frame(channel, Performative::Begin(Begin::default())),
        )
        .await
        .unwrap();
        let incoming = timeout(WAIT, self.connection.next_incoming_session())
            .await
            .unwrap()
            .unwrap();
        let session = timeout(WAIT, self.connection.accept_session(incoming))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            control(&mut self.peer, channel).await,
            Performative::Begin(_)
        ));
        session
    }

    async fn offer(&mut self, channel: u16, handle: u32, name: &str) {
        write_frame(
            &mut self.peer,
            &frame(
                channel,
                Performative::Attach(Box::new(Attach {
                    name: name.to_owned(),
                    handle,
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
                })),
            ),
        )
        .await
        .unwrap();
    }

    async fn stop(&mut self) {
        self.writes.release();
        self.connection.stop();
        timeout(WAIT, self.connection.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn buffered_offers_do_not_hide_processed_end_or_revive_an_old_session() {
    let mut wire = Wire::new().await;
    let mut original = wire.begin(1).await;
    assert!(!original.is_ended());
    wire.offer(1, 1, "buffered-one").await;
    wire.offer(1, 2, "buffered-two").await;
    // The second Begin is behind both offered frames on the actual native
    // reader/driver path, proving they reached the original attach receiver.
    let independent = wire.begin(2).await;
    wire.writes.block();
    write_frame(&mut wire.peer, &frame(1, Performative::End(End::default())))
        .await
        .unwrap();
    wire.writes.reached().await;
    assert!(
        original.is_ended(),
        "End removed the producer before its blocked reply"
    );
    assert!(!independent.is_ended());
    wire.writes.release();
    assert!(matches!(
        control(&mut wire.peer, 1).await,
        Performative::End(_)
    ));
    let old_offer = original.next_incoming_attach().await.unwrap();
    assert_eq!(old_offer.name, "buffered-one");
    assert_eq!(
        original.next_incoming_attach().await.unwrap().name,
        "buffered-two"
    );
    assert!(original.next_incoming_attach().await.is_none());
    assert!(original.is_ended());
    let mut replacement = wire.begin(1).await;
    assert!(!replacement.is_ended());
    assert!(original.is_ended());
    wire.offer(1, 1, "fresh-one").await;
    let offer = timeout(WAIT, replacement.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        original.accept_attach(old_offer, 1024 * 1024).await,
        Err(EngineError::RemoteDetached)
    ));
    let endpoint = replacement.accept_attach(offer, 1024 * 1024).await.unwrap();
    assert!(matches!(endpoint, LinkEndpoint::Sender(_)));
    let Performative::Attach(reply) = control(&mut wire.peer, 1).await else {
        panic!("fresh Attach reply");
    };
    assert_eq!(reply.name, "fresh-one");
    assert!(!replacement.is_ended());
    wire.stop().await;
    assert!(replacement.is_ended());
    assert!(independent.is_ended());
}

#[tokio::test(flavor = "current_thread")]
async fn joined_original_stop_marks_sessions_ended_with_buffered_offers() {
    let mut wire = Wire::new().await;
    let mut original = wire.begin(1).await;
    wire.offer(1, 1, "buffered-stop").await;
    let independent = wire.begin(2).await;
    assert!(!original.is_ended());
    wire.stop().await;
    assert!(original.is_ended());
    assert!(independent.is_ended());
    assert_eq!(
        original.next_incoming_attach().await.unwrap().name,
        "buffered-stop"
    );
    assert!(original.next_incoming_attach().await.is_none());
}
