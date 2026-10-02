use std::{
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

use tokio::{
    io::{DuplexStream, ReadBuf},
    time::timeout,
};

use super::*;

const DEADLINE: Duration = Duration::from_secs(3);
const PEER_CHANNEL: u16 = 19;

#[derive(Default)]
struct GateState {
    armed: bool,
    blocked: bool,
    fail: bool,
    entered: bool,
    bytes: Vec<u8>,
    waker: Option<Waker>,
}

#[derive(Default)]
struct BeginGate {
    state: Mutex<GateState>,
    entered: Notify,
}

impl BeginGate {
    fn arm(&self, fail: bool) {
        let mut state = self.state.lock().expect("Begin gate");
        assert!(!state.armed);
        *state = GateState {
            armed: true,
            blocked: true,
            fail,
            ..GateState::default()
        };
    }

    async fn wait_entered(&self) {
        timeout(DEADLINE, async {
            loop {
                let entered = self.entered.notified();
                tokio::pin!(entered);
                entered.as_mut().enable();
                if self.state.lock().expect("Begin gate").entered {
                    return;
                }
                entered.await;
            }
        })
        .await
        .expect("actual Begin reached its flush");
    }

    fn release(&self) {
        let waker = {
            let mut state = self.state.lock().expect("Begin gate");
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
    gate: Arc<BeginGate>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buffer)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(written)) = result {
            let mut state = this.gate.state.lock().expect("Begin gate");
            if state.armed {
                state.bytes.extend_from_slice(&bytes[..written]);
            }
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let (entered, blocked, failed) = {
            let mut state = this.gate.state.lock().expect("Begin gate");
            let begin = state.armed
                && matches!(
                    crate::codec::decode_frame_for_test(&state.bytes),
                    Ok(Frame::Amqp {
                        performative: Some(Performative::Begin(_)),
                        ..
                    })
                );
            if begin {
                let entered = !state.entered;
                state.entered = true;
                if state.blocked {
                    state.waker = Some(cx.waker().clone());
                }
                (entered, state.blocked, state.fail)
            } else {
                (false, false, false)
            }
        };
        if entered {
            this.gate.entered.notify_waiters();
        }
        if blocked {
            return Poll::Pending;
        }
        if failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected Begin flush failure",
            )));
        }
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        if let Poll::Ready(Ok(())) = result {
            let mut state = this.gate.state.lock().expect("Begin gate");
            state.bytes.clear();
            if state.entered {
                state.armed = false;
            }
        }
        result
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

async fn frame(peer: &mut DuplexStream) -> Frame {
    timeout(DEADLINE, read_frame(peer))
        .await
        .expect("bounded peer response")
        .expect("valid frame")
}

async fn pair() -> (ServerConnection, DuplexStream, Arc<BeginGate>) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let gate = Arc::new(BeginGate::default());
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert_eq!(
            read_protocol_header(&mut peer)
                .await
                .expect("native header"),
            ProtocolHeader::AMQP
        );
        let mut open = Open::new("owned-admission-peer");
        open.channel_max = 3;
        write_amqp(&mut peer, 0, Performative::Open(open), Vec::new())
            .await
            .expect("peer Open");
        assert!(matches!(
            frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer
    };
    let (connection, peer) = timeout(DEADLINE, async {
        tokio::join!(
            ServerConnection::accept(
                GatedIo {
                    inner: wire,
                    gate: gate.clone()
                },
                "owned-server",
                None
            ),
            opening
        )
    })
    .await
    .expect("bounded native negotiation");
    (connection.expect("native connection"), peer, gate)
}

async fn incoming(connection: &mut ServerConnection, peer: &mut DuplexStream) -> IncomingSession {
    write_amqp(
        peer,
        PEER_CHANNEL,
        Performative::Begin(Begin::default()),
        Vec::new(),
    )
    .await
    .expect("peer Begin");
    timeout(DEADLINE, connection.next_incoming_session())
        .await
        .expect("bounded native incoming session")
        .expect("actual session approval")
}

fn duplicate(incoming: &IncomingSession) -> IncomingSession {
    IncomingSession {
        channel: incoming.channel,
        identity: incoming.identity.clone(),
        begin: incoming.begin.clone(),
    }
}

fn capture(
    connection: &ServerConnection,
    incoming: IncomingSession,
) -> impl Future<Output = Result<ServerSession, EngineError>> + Send + 'static + use<> {
    connection.accept_session(incoming)
}

fn assert_begin(frame: Frame, peer_channel: u16) -> u16 {
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Begin(begin)),
        payload,
    } = frame
    else {
        panic!("native Begin response");
    };
    assert!(payload.is_empty());
    assert_eq!(begin.remote_channel, Some(peer_channel));
    channel
}

async fn shutdown(connection: &ServerConnection) {
    timeout(DEADLINE, connection.shutdown())
        .await
        .expect("bounded native shutdown");
}

#[tokio::test]
async fn owned_admission_outlives_the_borrow_and_waits_for_actual_begin_flush() {
    let (mut connection, mut peer, gate) = pair().await;
    let incoming = incoming(&mut connection, &mut peer).await;
    let identity = incoming.identity.clone();
    let admission = {
        let borrowed = &connection;
        capture(borrowed, incoming)
    };
    write_amqp(
        &mut peer,
        23,
        Performative::Begin(Begin::default()),
        Vec::new(),
    )
    .await
    .expect("second peer Begin while owned future remains unpolled");
    let second = timeout(DEADLINE, connection.next_incoming_session())
        .await
        .expect("the connection can still be borrowed mutably")
        .expect("second approval");
    assert!(!second.identity.same_session(&identity));
    gate.arm(false);
    let accepting = tokio::spawn(admission);
    gate.wait_entered().await;
    let channel = assert_begin(frame(&mut peer).await, PEER_CHANNEL);
    assert!(
        !accepting.is_finished(),
        "admission reply must follow the flush"
    );
    assert!(connection.connection_identity().is_active());
    gate.release();
    let session = timeout(DEADLINE, accepting)
        .await
        .expect("admission finishes")
        .expect("admission task")
        .expect("flushed admission succeeds");
    assert_eq!(session.channel, channel);
    assert!(session.identity.same_session(&identity));
    shutdown(&connection).await;
}

#[tokio::test]
async fn dropping_an_unpolled_owned_admission_does_not_approve_the_session() {
    let (mut connection, mut peer, _) = pair().await;
    let incoming = incoming(&mut connection, &mut peer).await;
    let later = duplicate(&incoming);
    drop(capture(&connection, incoming));
    let session = timeout(DEADLINE, capture(&connection, later))
        .await
        .expect("later admission finishes")
        .expect("the unpolled admission did not install a session");
    assert_eq!(
        session.channel,
        assert_begin(frame(&mut peer).await, PEER_CHANNEL)
    );
    shutdown(&connection).await;
}

#[tokio::test]
async fn dropping_a_queued_owned_admission_preserves_late_native_approval() {
    let (mut connection, mut peer, gate) = pair().await;
    let incoming = incoming(&mut connection, &mut peer).await;
    let later = duplicate(&incoming);
    gate.arm(false);
    let mut accepting = Box::pin(capture(&connection, incoming));
    tokio::select! {
        () = gate.wait_entered() => {}
        _ = &mut accepting => panic!("admission cannot finish before Begin flush"),
    }
    drop(accepting);
    gate.release();
    assert_begin(frame(&mut peer).await, PEER_CHANNEL);
    let result = timeout(DEADLINE, capture(&connection, later))
        .await
        .expect("duplicate approval responds");
    assert!(
        matches!(result, Err(EngineError::InvalidState(_))),
        "dropping the reply waiter does not undo actor admission"
    );
    assert!(connection.connection_identity().is_active());
    shutdown(&connection).await;
}

#[tokio::test]
async fn failed_actual_begin_flush_never_returns_a_session() {
    let (mut connection, mut peer, gate) = pair().await;
    let incoming = incoming(&mut connection, &mut peer).await;
    let identity = incoming.identity.clone();
    gate.arm(true);
    let accepting = tokio::spawn(capture(&connection, incoming));
    gate.wait_entered().await;
    assert_begin(frame(&mut peer).await, PEER_CHANNEL);
    assert!(!accepting.is_finished());
    gate.release();
    let result = timeout(DEADLINE, accepting)
        .await
        .expect("failed admission finishes")
        .expect("admission task");
    assert!(matches!(
        result,
        Err(EngineError::Stopped | EngineError::Io(_))
    ));
    shutdown(&connection).await;
    assert!(identity.is_retired());
    assert!(!connection.connection_identity().is_active());
}

#[tokio::test]
async fn delayed_owned_admission_cannot_accept_a_reused_session_generation() {
    let (mut connection, mut peer, _) = pair().await;
    let old = incoming(&mut connection, &mut peer).await;
    let old_identity = old.identity.clone();
    let delayed = capture(&connection, old);
    write_amqp(
        &mut peer,
        PEER_CHANNEL,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("end original session");
    let old_channel = assert_begin(frame(&mut peer).await, PEER_CHANNEL);
    assert!(matches!(frame(&mut peer).await, Frame::Amqp {
        channel, performative: Some(Performative::End(_)), ..
    } if channel == old_channel));
    let fresh = incoming(&mut connection, &mut peer).await;
    assert!(!fresh.identity.same_session(&old_identity));
    assert!(matches!(
        timeout(DEADLINE, delayed)
            .await
            .expect("retired approval refuses"),
        Err(EngineError::RemoteDetached)
    ));
    let session = timeout(DEADLINE, capture(&connection, fresh))
        .await
        .expect("fresh approval responds")
        .expect("fresh generation succeeds");
    assert_eq!(
        session.channel,
        assert_begin(frame(&mut peer).await, PEER_CHANNEL)
    );
    assert!(!session.identity.same_session(&old_identity));
    shutdown(&connection).await;
}
