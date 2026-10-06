use super::*;
use crate::{Described, Descriptor, Value};
use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    task::{JoinError, JoinHandle},
    time::timeout,
};

pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(3), future)
        .await
        .expect("bounded original operation")
}
pub(super) async fn caught<T>(future: impl Future<Output = T>) -> Result<T, Box<dyn Any + Send>> {
    let mut future = Box::pin(future);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        },
    )
    .await
}

#[derive(Default)]
pub(super) struct FlushGate {
    held: AtomicBool,
    entered: Notify,
    waker: Mutex<Option<Waker>>,
}
impl FlushGate {
    pub fn arm(&self) {
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
struct RecordedIo {
    inner: DuplexStream,
    bytes: Arc<Mutex<Vec<u8>>>,
    gate: Arc<FlushGate>,
}
impl AsyncRead for RecordedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for RecordedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, bytes) {
            Poll::Ready(Ok(count)) => {
                self.bytes
                    .lock()
                    .expect("actual write capture")
                    .extend_from_slice(&bytes[..count]);
                Poll::Ready(Ok(count))
            }
            other => other,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.gate.held.load(Ordering::Acquire) {
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

pub(super) enum Endpoint {
    Sender(ClientSender),
    Receiver(Box<ClientReceiver>),
}
struct Caller {
    task: Option<JoinHandle<Result<Endpoint, EngineError>>>,
    result: Option<Result<Result<Endpoint, EngineError>, JoinError>>,
}

// This bypasses only negotiation. It launches production run_client and owns its
// ORIGINAL actor token; cooperative return includes its internal Reader shutdown.
pub(super) struct Fixture {
    pub connection: ClientConnection,
    pub session: Option<ClientSession>,
    pub peer: DuplexStream,
    pub peer_channel: u16,
    pub gate: Arc<FlushGate>,
    next_peer_channel: u16,
    actor: Option<JoinHandle<Result<(), EngineError>>>,
    pub actor_result: Option<Result<Result<(), EngineError>, JoinError>>,
    callers: Vec<Caller>,
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl Fixture {
    pub async fn new() -> Self {
        let (stream, peer) = tokio::io::duplex(65536);
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(FlushGate::default());
        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        let consumed = Arc::new(Notify::new());
        let (lifecycle, cancellation, exit) = ConnectionLifecycle::new();
        let wake = consumed.clone();
        let recorded = RecordedIo {
            inner: stream,
            bytes: bytes.clone(),
            gate: gate.clone(),
        };
        let actor = tokio::spawn(async move {
            let result = run_client(
                recorded,
                ConnectionSettings {
                    remote_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
                    local_max_frame_size: DEFAULT_MAX_FRAME_SIZE,
                    channel_max: u16::MAX,
                    remote_channel_max: u16::MAX,
                    options: ConnectionOptions::default(),
                    peer_idle_millis: 0,
                },
                exit.identity(),
                command_rx,
                closed_tx,
                wake,
                cancellation,
            )
            .await;
            drop(exit);
            result
        });
        let connection = ClientConnection {
            commands,
            closed,
            lifecycle,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            consumed,
        };
        let mut fixture = Self {
            connection,
            session: None,
            peer,
            peer_channel: 9,
            next_peer_channel: 10,
            actor: Some(actor),
            actor_result: None,
            callers: Vec::new(),
            bytes,
            gate,
        };
        fixture.session = Some(fixture.begin_on(9).await);
        fixture
    }
    async fn begin_on(&mut self, peer_channel: u16) -> ClientSession {
        let (reply, response) = oneshot::channel();
        bounded(
            self.connection
                .commands
                .send(ClientCommand::Begin { reply }),
        )
        .await
        .expect("begin enqueue");
        let channel = match self.read().await {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Begin(_)),
                ..
            } => channel,
            other => panic!("expected original Begin, got {other:?}"),
        };
        self.write_on(
            peer_channel,
            Performative::Begin(Begin {
                remote_channel: Some(channel),
                ..Begin::default()
            }),
        )
        .await;
        let (channel, identity) = bounded(response)
            .await
            .expect("begin reply")
            .expect("begin accepted");
        ClientSession {
            channel,
            identity,
            commands: self.connection.commands.clone(),
            consumed: self.connection.consumed.clone(),
        }
    }
    pub async fn barrier(&mut self) {
        let channel = self.next_peer_channel;
        self.next_peer_channel += 1;
        let session = self.begin_on(channel).await;
        // Remove the barrier session through the actual peer End path.
        self.write_on(channel, Performative::End(End::default()))
            .await;
        loop {
            if matches!(self.read().await, Frame::Amqp {
                channel: actual, performative: Some(Performative::End(_)), ..
            } if actual == session.channel)
            {
                break;
            }
        }
    }
    pub fn start(&mut self, role: Role, name: impl Into<String>) -> usize {
        let original = self.session.as_ref().expect("original session");
        let session = ClientSession {
            channel: original.channel,
            identity: original.identity.clone(),
            commands: original.commands.clone(),
            consumed: original.consumed.clone(),
        };
        self.start_owned(session, role, name.into())
    }
    pub async fn start_other(&mut self, role: Role, name: &str) -> usize {
        let channel = self.next_peer_channel;
        self.next_peer_channel += 1;
        let session = self.begin_on(channel).await;
        self.start_owned(session, role, name.to_owned())
    }
    pub async fn begin_other(&mut self) -> u16 {
        let channel = self.next_peer_channel;
        self.next_peer_channel += 1;
        let _original_session = self.begin_on(channel).await;
        channel
    }
    fn start_owned(&mut self, mut session: ClientSession, role: Role, name: String) -> usize {
        let task = tokio::spawn(async move {
            match role {
                Role::Sender => session
                    .attach_sender(name, "entity")
                    .await
                    .map(Endpoint::Sender),
                Role::Receiver => session
                    .attach_receiver(name, "entity")
                    .await
                    .map(Box::new)
                    .map(Endpoint::Receiver),
            }
        });
        let index = self.callers.len();
        self.callers.push(Caller {
            task: Some(task),
            result: None,
        });
        index
    }
    pub async fn request(&mut self) -> Attach {
        loop {
            match self.read().await {
                Frame::Amqp {
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => return *attach,
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() => {}
                other => panic!("expected original attach request, got {other:?}"),
            }
        }
    }
    pub async fn null_response(&mut self, request: &Attach, peer_handle: u32) {
        let mut response = request.response(None, None);
        response.handle = peer_handle;
        self.write(Performative::Attach(Box::new(response))).await;
    }
    pub async fn detach(&mut self, request: &Attach, peer_handle: u32) {
        self.write(Performative::Detach(Detach {
            handle: peer_handle,
            closed: true,
            error: Some(Error::new(
                crate::AmqpError::UnauthorizedAccess,
                "denied",
                None,
            )),
        }))
        .await;
        let response = self.read().await;
        assert!(
            matches!(response, Frame::Amqp { performative: Some(Performative::Detach(
            Detach { handle, closed: true, error: None })), .. } if handle == request.handle)
        );
    }
    pub async fn complete(&mut self, index: usize) {
        let caller = &mut self.callers[index];
        let result = bounded(caller.task.as_mut().expect("original caller")).await;
        caller.task.take();
        caller.result = Some(result);
    }
    pub async fn cancel_caller(&mut self, index: usize) {
        self.callers[index]
            .task
            .as_ref()
            .expect("original caller")
            .abort();
        self.complete(index).await;
    }
    pub fn pending(&self, index: usize) -> bool {
        self.callers[index]
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }
    pub fn endpoint(&mut self, index: usize) -> &mut Endpoint {
        match self.callers[index].result.as_mut().expect("joined caller") {
            Ok(Ok(endpoint)) => endpoint,
            _ => panic!("attach did not yield a terminal endpoint"),
        }
    }
    pub fn failed(&self, index: usize) -> bool {
        matches!(&self.callers[index].result, Some(Ok(Err(_))))
    }
    pub fn cancelled(&self, index: usize) -> bool {
        matches!(&self.callers[index].result, Some(Err(error)) if error.is_cancelled())
    }
    pub async fn read(&mut self) -> Frame {
        bounded(read_frame(&mut self.peer))
            .await
            .expect("actual peer frame")
    }
    pub async fn write(&mut self, performative: Performative) {
        self.write_on(self.peer_channel, performative).await;
    }
    pub async fn write_nonterminal_source_default(&mut self, mut response: Attach) {
        // The ordinary encoder rejects this malformed outcome before writing.
        response.source.as_mut().expect("source").default_outcome =
            Some(crate::DeliveryState::Released(crate::Released));
        let seed = crate::encode_frame(&Frame::Amqp {
            channel: self.peer_channel,
            performative: Some(Performative::Attach(Box::new(response))),
            payload: Vec::new(),
        })
        .expect("valid Attach seed");
        let (mut value, consumed) =
            crate::value_codec::decode_value(&seed[8..]).expect("structured Attach seed");
        assert_eq!(consumed, seed.len() - 8);
        let Value::Described(attach) = &mut value else {
            panic!("Attach descriptor")
        };
        assert_eq!(attach.descriptor, Descriptor::Code(0x12));
        let Value::List(fields) = &mut attach.value else {
            panic!("Attach fields")
        };
        let Value::Described(source) = &mut fields[5] else {
            panic!("Source descriptor")
        };
        assert_eq!(source.descriptor, Descriptor::Code(0x28));
        let Value::List(fields) = &mut source.value else {
            panic!("Source fields")
        };
        fields[8] = Value::Described(Box::new(Described {
            descriptor: Descriptor::Code(0x23),
            value: Value::List(vec![Value::Uint(0), Value::Ulong(0)]),
        }));
        let bytes = crate::encode_frame(&Frame::Amqp {
            channel: self.peer_channel,
            performative: None,
            payload: serde_amqp::to_vec(&value).expect("malformed source wire value"),
        })
        .expect("original frame envelope");
        let error = crate::codec::decode_frame_for_test(&bytes)
            .expect_err("nonterminal source outcome is rejected before admission");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "source default outcome must be terminal");
        bounded(self.peer.write_all(&bytes))
            .await
            .expect("actual malformed peer write");
    }
    pub async fn write_on(&mut self, channel: u16, performative: Performative) {
        bounded(write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel,
                performative: Some(performative),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("actual peer write");
    }
    pub async fn captured(&self) -> Vec<Frame> {
        let bytes = self.bytes.lock().expect("actual write capture").clone();
        let mut slice = bytes.as_slice();
        let mut frames = Vec::new();
        while !slice.is_empty() {
            frames.push(
                bounded(read_frame(&mut slice))
                    .await
                    .expect("captured frame"),
            );
        }
        frames
    }
    pub async fn no_activity(&self, handle: u32) {
        assert!(!self.captured().await.iter().any(|frame| matches!(frame,
            Frame::Amqp { performative: Some(Performative::Flow(Flow { handle: Some(actual), .. })), .. }
            | Frame::Amqp { performative: Some(Performative::Transfer(Transfer { handle: actual, .. })), .. }
            if *actual == handle)));
    }
    pub async fn finish(&mut self) {
        self.gate.release();
        let _ = self.connection.lifecycle.cancellation.send(true);
        if let Some(actor) = self.actor.as_mut() {
            self.actor_result = Some(bounded(actor).await);
            self.actor.take();
        }
        for caller in &mut self.callers {
            if let Some(task) = caller.task.as_mut() {
                caller.result = Some(bounded(task).await);
                caller.task.take();
            }
        }
    }
    pub fn joined(&self) {
        assert!(matches!(&self.actor_result, Some(Ok(Ok(())))));
        assert!(
            self.callers
                .iter()
                .all(|caller| caller.task.is_none() && caller.result.is_some())
        );
    }
}

pub(super) fn rethrow(result: Result<(), Box<dyn Any + Send>>) {
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

// This count-consistency unit case creates no actor/reader or wire delivery.
pub(super) fn mismatched_buffered_count_is_rejected_before_parking() {
    let mut session = SessionState::new(&Begin::default());
    let owner = session.identity.new_link();
    let (deliveries, _original_inbox) = mpsc::channel(1);
    let (detached, _original_watch) = watch::channel(false);
    let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
    let link = LinkState::Receiving(Box::new(ReceivingLink {
        max_message_size: 4096,
        deliveries: deliveries.into(),
        partial: None,
        detached,
        credit: ReceiveCredit::new(0, LINK_CREDIT, consumption.clone()),
        decoders: MessageFormatDecoders::default(),
        identity: owner.clone(),
        sender_settle_mode: SenderSettleMode::Unsettled,
        receiver_settle_mode: ReceiverSettleMode::First,
    }));
    session.handle_aliases.insert(
        4,
        HandleAlias {
            identity: owner.clone(),
            name: "count".into(),
            role: Role::Receiver,
            peer_handle: None,
            own_attach_sent: true,
            error_detached: false,
        },
    );
    let mut flow = PendingLinkFlow::new(Role::Sender, None);
    flow.update(Flow {
        delivery_count: Some(7),
        ..Flow::default()
    })
    .expect("existing pending count");
    session.pending_attaches.insert(4, flow);
    let (reply, _original_response) = oneshot::channel();
    let mut pending = PendingAttaches::default();
    pending.insert(
        "count".into(),
        PendingAttach {
            channel: 3,
            session: session.identity.clone(),
            handle: 4,
            reply,
            link,
            consumption,
            refused_response: None,
        },
    );
    let mut sessions = HashMap::from([(3, session)]);
    let response = Attach {
        name: "count".into(),
        handle: 17,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    assert!(matches!(
        park(response, 3, &mut pending, &mut sessions),
        Err(EngineError::InvalidState(_))
    ));
    assert!(sessions[&3].links.is_empty());
    assert!(sessions[&3].handle_aliases[&4].peer_handle.is_none());
    assert!(
        pending
            .get("count", &Role::Receiver)
            .expect("original pending")
            .refused_response
            .is_none()
    );
    assert!(!owner.is_retired());
    let valid = Attach {
        name: "count".into(),
        handle: 17,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(7),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    park(valid, 3, &mut pending, &mut sessions).expect("matching original count");
    assert!(!owner.is_retired());
    assert!(sessions[&3].links.is_empty() && sessions[&3].closing_handles.is_empty());
    assert!(sessions[&3].pending_attaches.contains_key(&4));
    assert_eq!(sessions[&3].handle_aliases[&4].peer_handle, Some(17));
    assert!(
        pending
            .get("count", &Role::Receiver)
            .expect("live original pending")
            .refused_response
            .is_some()
    );
}
