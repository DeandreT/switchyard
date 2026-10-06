use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::{io::DuplexStream, task::JoinHandle, time::timeout};

pub(super) const LOCAL: u16 = 3;
pub(super) const PEER: u16 = 9;
pub(super) const HANDLE: u32 = 4;
pub(super) const PEER_HANDLE: u32 = 17;

pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(3), future)
        .await
        .expect("bounded original operation")
}

pub(super) async fn caught<T>(
    future: impl Future<Output = T>,
) -> Result<T, Box<dyn std::any::Any + Send>> {
    let mut future = Box::pin(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

pub(super) fn attach(role: Role) -> Attach {
    Attach {
        name: String::from("pending-refusal"),
        handle: PEER_HANDLE,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(crate::Source::new("entity")),
        target: Some(crate::Target::new("entity").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: Some(4096),
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

pub(super) fn denied() -> Error {
    Error::new(crate::AmqpError::UnauthorizedAccess, "not permitted", None)
}

#[derive(Default)]
pub(super) struct Capture(Mutex<Vec<u8>>);
pub(super) struct CaptureWriter(pub Arc<Capture>);
impl AsyncWrite for CaptureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.0.lock().expect("capture").extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// State tests create no actor or reader and make no socket-custody claim.
pub(super) struct State {
    pub sessions: HashMap<u16, SessionState>,
    pub receipt: IncomingAttach,
    pub writer: FrameWriter<CaptureWriter>,
    capture: Arc<Capture>,
}
impl State {
    pub fn new(role: Role, maximum: u32) -> Self {
        Self::with_attach(attach(role), maximum)
    }
    pub fn with_attach(request: Attach, maximum: u32) -> Self {
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(PEER);
        session.local_begin_sent = true;
        let receipt = IncomingAttach::new(request, session.identity.clone(), HANDLE);
        session.handle_aliases.insert(
            HANDLE,
            HandleAlias {
                identity: receipt.approval().link_identity().clone(),
                name: Arc::clone(receipt.approval().name()),
                role: receipt.approval().local_role(),
                peer_handle: Some(receipt.handle),
                own_attach_sent: false,
                error_detached: false,
            },
        );
        session
            .pending_attaches
            .insert(HANDLE, PendingLinkFlow::incoming(&receipt));
        let capture = Arc::new(Capture::default());
        Self {
            sessions: HashMap::from([(LOCAL, session)]),
            receipt,
            writer: FrameWriter::new(CaptureWriter(capture.clone()), maximum).expect("writer"),
            capture,
        }
    }
    pub async fn reject(&mut self, receipt: IncomingAttach) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        let identity = self.sessions[&LOCAL].identity.clone();
        let io = handle_rejection(
            LOCAL,
            identity,
            receipt,
            denied(),
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await;
        io?;
        bounded(response).await.expect("original reply")
    }
    pub async fn input(&mut self, performative: Performative) -> Result<FrameAction, EngineError> {
        let (incoming, _events) = mpsc::channel(1);
        bounded(handle_frame(
            Frame::Amqp {
                channel: PEER,
                performative: Some(performative),
                payload: Vec::new(),
            },
            &mut self.writer,
            &incoming,
            &mut self.sessions,
            512,
            u16::MAX,
            false,
        ))
        .await
    }
    pub async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.capture.0.lock().expect("capture"));
        let mut source = bytes.as_slice();
        let mut frames = Vec::new();
        while !source.is_empty() {
            frames.push(
                bounded(read_frame(&mut source))
                    .await
                    .expect("actual encoded frame"),
            );
        }
        frames
    }
    pub fn original_owned(&self) -> bool {
        let session = &self.sessions[&LOCAL];
        session.links.is_empty()
            && session
                .pending_attaches
                .get(&HANDLE)
                .and_then(|pending| pending.approval.as_ref())
                .is_some_and(|approval| Arc::ptr_eq(approval, self.receipt.approval()))
            && !self.receipt.approval().link_identity().is_retired()
    }
}

#[derive(Default)]
pub(super) struct FlushGate {
    held: AtomicBool,
    skip: AtomicUsize,
    entered: Notify,
    waker: Mutex<Option<Waker>>,
}
impl FlushGate {
    pub fn arm_after(&self, flushes: usize) {
        self.skip.store(flushes, Ordering::Release);
        self.held.store(true, Ordering::Release);
    }
    pub async fn entered(&self) {
        bounded(self.entered.notified()).await;
    }
    pub fn release(&self) {
        self.held.store(false, Ordering::Release);
        if let Some(waker) = self.waker.lock().expect("gate").take() {
            waker.wake();
        }
    }
}
pub(super) struct GatedIo {
    pub inner: DuplexStream,
    pub gate: Arc<FlushGate>,
}
impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
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
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.gate.held.load(Ordering::Acquire) {
            if self.gate.skip.load(Ordering::Acquire) > 0 {
                self.gate.skip.fetch_sub(1, Ordering::AcqRel);
                return Pin::new(&mut self.inner).poll_flush(cx);
            }
            *self.gate.waker.lock().expect("gate") = Some(cx.waker().clone());
            if self.gate.held.load(Ordering::Acquire) {
                self.gate.entered.notify_one();
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) struct Socket {
    pub owner: ServerConnectionOwner<Arc<()>>,
    pub connection: ServerConnection,
    pub peer: DuplexStream,
    pub gate: Arc<FlushGate>,
    pub acceptance: Result<Result<(), EngineError>, tokio::task::JoinError>,
    pub anchor: Arc<()>,
    pub callers: Vec<JoinHandle<Result<(), EngineError>>>,
    pub caller_results: Vec<Result<Result<(), EngineError>, tokio::task::JoinError>>,
}
impl Socket {
    pub async fn new() -> Self {
        let (io, mut peer) = tokio::io::duplex(65536);
        let gate = Arc::new(FlushGate::default());
        let anchor = Arc::new(());
        let (owner, acceptor) =
            ServerConnectionOwner::new(tokio::runtime::Handle::current(), anchor.clone());
        let (accepted, connection_rx) = oneshot::channel();
        let transport_gate = gate.clone();
        let mut acceptance: JoinHandle<_> = tokio::spawn(async move {
            let result = acceptor
                .accept_with_options(
                    GatedIo {
                        inner: io,
                        gate: transport_gate,
                    },
                    "refusal-owner",
                    None,
                    ConnectionOptions::default(),
                )
                .await?;
            match result {
                ScopedConnectionAcceptance::Accepted(connection) => {
                    accepted
                        .send(connection)
                        .map_err(|_| EngineError::Stopped)?;
                    Ok(())
                }
                ScopedConnectionAcceptance::Refused(_) => Err(EngineError::Stopped),
            }
        });
        bounded(crate::write_protocol_header(
            &mut peer,
            ProtocolHeader::AMQP,
        ))
        .await
        .expect("peer header");
        bounded(crate::read_protocol_header(&mut peer))
            .await
            .expect("server header");
        bounded(write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-peer"))),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("peer open");
        bounded(read_frame(&mut peer)).await.expect("server open");
        let connection = bounded(connection_rx)
            .await
            .expect("original acceptance connection");
        let acceptance = bounded(&mut acceptance).await;
        Self {
            owner,
            connection,
            peer,
            gate,
            acceptance,
            anchor,
            callers: Vec::new(),
            caller_results: Vec::new(),
        }
    }
    pub async fn session(&mut self) -> ServerSession {
        bounded(write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: PEER,
                performative: Some(Performative::Begin(Begin::default())),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("peer begin");
        let incoming = bounded(self.connection.next_incoming_session())
            .await
            .expect("original session");
        let session = bounded(self.connection.accept_session(incoming))
            .await
            .expect("accept session");
        bounded(read_frame(&mut self.peer))
            .await
            .expect("local begin");
        session
    }
    pub async fn receipt(&mut self, session: &mut ServerSession, role: Role) -> IncomingAttach {
        bounded(write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: PEER,
                performative: Some(Performative::Attach(Box::new(attach(role)))),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("peer attach");
        bounded(session.next_incoming_attach())
            .await
            .expect("original attach")
    }
    pub async fn finish(&mut self) -> ServerConnectionJoinReport<Arc<()>> {
        self.gate.release();
        self.owner.stop();
        for caller in &mut self.callers {
            self.caller_results.push(bounded(caller).await);
        }
        self.callers.clear();
        bounded(self.owner.finish())
            .await
            .expect("original socket report")
    }
}
