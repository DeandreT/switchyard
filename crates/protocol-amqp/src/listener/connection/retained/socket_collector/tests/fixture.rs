use super::{super::*, recorder::Recorder};
use amqp::{
    Attach, Begin, Frame, Open, Performative, ProtocolHeader, ReceiverSettleMode, Role,
    SenderSettleMode, Source, Target,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    future::{Future, poll_fn},
    io,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CHANNELS: [u16; 2] = [7, 9];

pub(super) enum Peer {
    Tcp(TcpStream),
    Ws(Box<WebSocketStream<TcpStream>>),
}
impl Peer {
    pub(super) async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        let frame = Frame::Amqp {
            channel,
            performative: Some(performative),
            payload: Vec::new(),
        };
        match self {
            Self::Tcp(peer) => amqp::write_frame(peer, &frame).await?,
            Self::Ws(peer) => {
                peer.send(Message::Binary(amqp::encode_frame(&frame)?.into()))
                    .await?
            }
        }
        Ok(())
    }
    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        match self {
            Self::Tcp(peer) => Ok(amqp::read_frame(peer).await?),
            Self::Ws(peer) => {
                let bytes = match peer.next().await {
                    Some(Ok(Message::Binary(bytes))) => bytes,
                    Some(Err(error)) => return Err(error.into()),
                    _ => return Err(io::Error::other("missing binary AMQP frame").into()),
                };
                Ok(amqp::read_frame(&mut bytes.as_ref()).await?)
            }
        }
    }
    async fn hello(&mut self) -> TestResult {
        match self {
            Self::Tcp(peer) => {
                amqp::write_protocol_header(peer, ProtocolHeader::AMQP).await?;
                if amqp::read_protocol_header(peer).await? != ProtocolHeader::AMQP {
                    return Err("unexpected protocol header".into());
                }
            }
            Self::Ws(peer) => {
                peer.send(Message::Binary(amqp::AMQP_HEADER.to_vec().into()))
                    .await?;
                if !matches!(peer.next().await, Some(Ok(Message::Binary(bytes))) if bytes.as_ref() == amqp::AMQP_HEADER)
                {
                    return Err("unexpected WS protocol header".into());
                }
            }
        }
        self.send(
            0,
            Performative::Open(Open::new("retained-socket-collector-peer")),
        )
        .await?;
        if !matches!(
            self.frame().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ) {
            return Err("missing Open echo".into());
        }
        Ok(())
    }
}

type Upgrade = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (
                        WebSocketStream<TcpStream>,
                        tokio_tungstenite::tungstenite::handshake::client::Response,
                    ),
                    tokio_tungstenite::tungstenite::Error,
                >,
            > + Send,
    >,
>;
pub(super) struct Anchor {
    pub(super) drops: Arc<AtomicUsize>,
    _local: Rc<()>,
}
impl Anchor {
    pub(super) fn new() -> Self {
        Self {
            drops: Arc::new(AtomicUsize::new(0)),
            _local: Rc::new(()),
        }
    }
}
impl Drop for Anchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
pub(super) struct Fixture<A = ()> {
    pub(super) root: Root<A, Recorder>,
    pub(super) hooks: Arc<hooks::Hooks>,
    pub(super) peer: Option<Peer>,
    pub(super) report: Option<Report<A, Recorder>>,
    start: Option<Result<(), RetainedConnectionStartError<Recorder>>>,
    upgrade: Option<Upgrade>,
    upgrade_error: Option<tokio_tungstenite::tungstenite::Error>,
    upgrade_response: Option<tokio_tungstenite::tungstenite::handshake::client::Response>,
    pub(super) recorder: Recorder,
    pub(super) io: Option<Arc<super::io_gate::Plan>>,
}
pub(super) struct Cleanup<A> {
    pub(super) report: Option<Report<A, Recorder>>,
    pub(super) start: Option<Result<(), RetainedConnectionStartError<Recorder>>>,
    pub(super) faults: Vec<hooks::Fault>,
    pub(super) child_faults: Vec<atomic::ScopedFault>,
    upgrade: Option<Upgrade>,
    upgrade_error: Option<tokio_tungstenite::tungstenite::Error>,
    upgrade_response: Option<tokio_tungstenite::tungstenite::handshake::client::Response>,
}
impl<A> Cleanup<A> {
    pub(super) fn report_only(report: Report<A, Recorder>) -> Self {
        Self {
            report: Some(report),
            start: None,
            faults: Vec::new(),
            child_faults: Vec::new(),
            upgrade: None,
            upgrade_error: None,
            upgrade_response: None,
        }
    }
}
impl Fixture {
    pub(super) async fn tcp<const MESSAGING: bool>(limit: usize) -> TestResult<Self> {
        Self::with_anchor::<MESSAGING>(limit, (), false).await
    }
}
impl<A> Fixture<A> {
    pub(super) async fn with_anchor<const MESSAGING: bool>(
        limit: usize,
        anchor: A,
        websocket: bool,
    ) -> TestResult<Self> {
        let namespace = domain::NamespaceName::new("tenant")?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        // No covered role exists during this OS acceptance/setup.
        let (connected, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let peer = connected?;
        let (server, _) = accepted?;
        let (root, launch) = match Root::new::<MESSAGING>(Handle::current(), limit, anchor) {
            Ok(pair) => pair,
            Err(_) => return Err("fixture constructor needs a valid limit".into()),
        };
        let hooks = root.control.hooks.clone();
        let recorder = Recorder::default();
        let config =
            AmqpListener::new(recorder.clone(), namespace).with_handshake_timeout(DEADLINE);
        let config = if websocket {
            config.with_websocket()
        } else {
            config
        };
        let start = Some(launch.start(config, server));
        Ok(Self {
            root,
            hooks,
            peer: Some(Peer::Tcp(peer)),
            report: None,
            start,
            upgrade: None,
            upgrade_error: None,
            upgrade_response: None,
            recorder,
            io: None,
        })
    }
    pub(super) async fn drive<T>(
        &mut self,
        future: impl Future<Output = TestResult<T>>,
    ) -> TestResult<T> {
        drive_parts(&mut self.root, future).await
    }
    pub(super) async fn hello(&mut self) -> TestResult {
        let Self { root, peer, .. } = self;
        drive_parts(root, peer.as_mut().expect("retained original peer").hello()).await
    }
    pub(super) async fn websocket(&mut self) -> TestResult {
        let mut request = "ws://localhost/$servicebus/websocket/".into_client_request()?;
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", "amqp".parse()?);
        if !matches!(self.peer, Some(Peer::Tcp(_))) {
            return Err("unupgraded original peer required".into());
        }
        let Some(Peer::Tcp(peer)) = self.peer.take() else {
            unreachable!("checked original TCP peer");
        };
        self.upgrade = Some(Box::pin(tokio_tungstenite::client_async(request, peer)));
        let Self {
            root,
            upgrade,
            peer,
            upgrade_error,
            upgrade_response,
            ..
        } = self;
        drive_parts(
            root,
            poll_fn(|cx| {
                match upgrade
                    .as_mut()
                    .expect("rooted upgrade future")
                    .as_mut()
                    .poll(cx)
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok((stream, response))) => {
                        *peer = Some(Peer::Ws(Box::new(stream)));
                        *upgrade_response = Some(response);
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(error)) => {
                        *upgrade_error = Some(error);
                        Poll::Ready(Err("upgrade boundary failed".into()))
                    }
                }
            }),
        )
        .await
    }
    pub(super) async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        let Self { root, peer, .. } = self;
        drive_parts(
            root,
            peer.as_mut()
                .expect("retained peer")
                .send(channel, performative),
        )
        .await
    }
    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        let Self { root, peer, .. } = self;
        drive_parts(root, peer.as_mut().expect("retained peer").frame()).await
    }
    pub(super) async fn begin(&mut self, index: usize) -> TestResult {
        self.begin_echo(index).await?;
        let controls = self
            .root
            .collector
            .as_ref()
            .expect("bound collector")
            .scoped_controls();
        self.drive(async move {
            while controls.session_commits() < index + 1 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn begin_echo(&mut self, index: usize) -> TestResult {
        self.send(CHANNELS[index], Performative::Begin(Begin::default()))
            .await?;
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { channel, performative: Some(Performative::Begin(_)), .. } if channel == CHANNELS[index])
            {
                return Ok(());
            }
        }
        Err("missing bounded Begin echo".into())
    }
    pub(super) async fn attached(&mut self, index: usize, attach: Attach) -> TestResult {
        let controls = self
            .root
            .collector
            .as_ref()
            .expect("bound collector")
            .scoped_controls();
        let expected = controls.worker_commits() + 1;
        self.send(CHANNELS[index], Performative::Attach(Box::new(attach)))
            .await?;
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { channel, performative: Some(Performative::Attach(_)), .. } if channel == CHANNELS[index])
            {
                return self
                    .drive(async move {
                        while controls.worker_commits() < expected {
                            tokio::task::yield_now().await;
                        }
                        Ok(())
                    })
                    .await;
            }
        }
        Err("missing bounded Attach echo".into())
    }
    pub(super) async fn bound(&mut self) -> TestResult<atomic::ScopedControls> {
        let control = self.root.control.clone();
        self.drive(async move {
            if control.binding().await == Binding::Bound {
                Ok(())
            } else {
                Err("closed before binding".into())
            }
        })
        .await?;
        Ok(self
            .root
            .collector
            .as_ref()
            .expect("bound collector")
            .scoped_controls())
    }
    pub(super) async fn gate(&mut self, gate: &hooks::Gate) -> TestResult {
        self.drive(async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn finish_at(&mut self, gate: &hooks::Gate) -> TestResult {
        self.finish_when(|| gate.entered()).await
    }
    pub(super) async fn finish_when(&mut self, predicate: impl Fn() -> bool) -> TestResult {
        let Self { root, report, .. } = self;
        let mut original = Box::pin(root.finish());
        let observed = timeout(
            DEADLINE,
            poll_fn(|cx| {
                if let Poll::Ready(value) = original.as_mut().poll(cx) {
                    *report = Some(value);
                    return Poll::Ready(Err("finish preceded requested gate".into()));
                }
                if predicate() {
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
        )
        .await;
        drop(original);
        observed?
    }
    pub(super) async fn complete(mut self) -> Cleanup<A> {
        self.root.stop();
        self.hooks.release();
        if let Some(io) = &self.io {
            io.release();
        }
        let view = self
            .root
            .collector
            .as_ref()
            .map(atomic::Root::scoped_controls);
        if let Some(view) = &view {
            view.release_observers();
        }
        drop(self.peer.take());
        if self.report.is_none() {
            self.report = Some(self.root.finish().await);
        }
        if let Some(view) = &view {
            view.release_all();
        }
        let child_faults = view.map_or_else(Vec::new, |view| view.take_unused());
        Cleanup {
            report: self.report,
            start: self.start,
            faults: self.hooks.unused(),
            child_faults,
            upgrade: self.upgrade,
            upgrade_error: self.upgrade_error,
            upgrade_response: self.upgrade_response,
        }
    }
}
impl Fixture {
    pub(super) async fn gated() -> TestResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (peer, server) = tokio::join!(TcpStream::connect(address), listener.accept());
        let peer = peer?;
        let (server, _) = server?;
        let (root, Launch { starter, bridge }) = match Root::new::<false>(Handle::current(), 2, ())
        {
            Ok(pair) => pair,
            Err(_) => return Err("valid gated factory refused".into()),
        };
        let hooks = root.control.hooks.clone();
        let recorder = Recorder::default();
        let io = Arc::new(super::io_gate::Plan::default());
        let started = crate::listener::connection::retained::start_test(
            super::io_gate::Gated {
                io: server,
                plan: io.clone(),
            },
            false,
            crate::listener::connection::ConnectionSettings {
                container_id: "gated-socket-collector".into(),
                namespace: domain::NamespaceName::new("tenant")?,
                broker: recorder.clone(),
                shared_access_authentication: None,
                connection_options: amqp::ConnectionOptions::default(),
                deadline: tokio::time::Instant::now() + DEADLINE,
            },
            bridge,
            starter,
        );
        let fixture = Self {
            root,
            hooks,
            peer: Some(Peer::Tcp(peer)),
            report: None,
            start: None,
            upgrade: None,
            upgrade_error: None,
            upgrade_response: None,
            recorder,
            io: Some(io),
        };
        if !started {
            let cleanup = fixture.complete().await;
            let _ = dispose(cleanup);
            return Err("gated start refused".into());
        }
        Ok(fixture)
    }
}
async fn drive_parts<A, T>(
    root: &mut Root<A, Recorder>,
    future: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let mut future = std::pin::pin!(future);
    timeout(DEADLINE, async {
        tokio::select! { original = future.as_mut() => original, () = root.drive() => unreachable!("borrowed aggregate drive") }
    }).await?
}
pub(super) fn producer(handle: u32) -> Attach {
    Attach {
        name: format!("socket-collector-producer-{handle}"),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::default()),
        target: Some(Target::new("orders").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}
pub(super) fn controller(handle: u32) -> Attach {
    let mut attach = producer(handle);
    attach.name = format!("socket-collector-controller-{handle}");
    attach.target = Some(amqp::Coordinator::default().into());
    attach
}
pub(super) fn consumer(handle: u32) -> Attach {
    let mut attach = producer(handle);
    attach.name = format!("socket-collector-consumer-{handle}");
    attach.role = Role::Receiver;
    attach.snd_settle_mode = SenderSettleMode::Mixed;
    attach.rcv_settle_mode = ReceiverSettleMode::Second;
    attach.source = Some(Source::new("orders"));
    attach.target = Some(Target::default().into());
    attach.initial_delivery_count = None;
    attach
}
pub(super) async fn opened<const MESSAGING: bool>(
    limit: usize,
) -> TestResult<(Fixture, atomic::ScopedControls)> {
    let mut fixture = Fixture::tcp::<MESSAGING>(limit).await?;
    let opening = fixture.hello().await;
    let binding = if opening.is_ok() {
        fixture.bound().await
    } else {
        Err("opening did not complete".into())
    };
    if opening.is_ok() && binding.is_ok() {
        let controls = match binding {
            Ok(controls) => controls,
            Err(_) => unreachable!("checked bound setup"),
        };
        return Ok((fixture, controls));
    }
    let cleanup = fixture.complete().await;
    let _disposal = dispose(cleanup);
    opening?;
    match binding {
        Err(error) => Err(error),
        Ok(_) => Err("incomplete fixture setup".into()),
    }
}
pub(super) struct Payload {
    pub(super) drops: Arc<AtomicUsize>,
    pub(super) panic: bool,
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Payload(..)")
    }
}
impl std::fmt::Display for Payload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private injected payload")
    }
}
impl std::error::Error for Payload {}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic {
            panic!("controlled postbarrier payload disposal");
        }
    }
}
pub(super) fn payload() -> (Payload, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Payload {
            drops: drops.clone(),
            panic: true,
        },
        drops,
    )
}
pub(super) fn evidence<A>(cleanup: &Cleanup<A>) -> (usize, usize, usize, bool) {
    let Some(report) = &cleanup.report else {
        return (0, 0, 0, false);
    };
    let socket = report.socket.as_ref().is_some_and(|socket| {
        socket.wrapper().is_some() && socket.actor().is_some() && socket.reader().is_some()
    });
    let counts = report
        .collector
        .as_ref()
        .map_or((0, 0, 0), atomic::Report::scope_counts);
    (
        counts.0,
        counts.1,
        counts.2,
        socket && report.context.is_some() && report.binding_refused.is_none(),
    )
}
pub(super) fn dispose<A>(cleanup: Cleanup<A>) -> bool {
    let Cleanup {
        report,
        start,
        faults,
        child_faults,
        upgrade,
        upgrade_error,
        upgrade_response,
    } = cleanup;
    fn caught<T>(value: T) -> bool {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))).is_err()
    }
    let mut panicked = false;
    if let Some(Report {
        socket,
        collector,
        histories,
        stopped,
        context,
        binding_refused,
        anchor,
    }) = report
    {
        if let Some(socket) = socket {
            let (joins, outcomes, ()) = socket.into_parts();
            panicked |= caught(joins.wrapper);
            panicked |= caught(joins.actor);
            panicked |= caught(joins.reader);
            panicked |= caught(outcomes.primary);
            panicked |= caught(outcomes.websocket_close);
        }
        if let Some(collector) = collector {
            panicked |= collector.dispose_scoped();
        }
        for original in histories {
            panicked |= caught(original);
        }
        for original in stopped {
            panicked |= caught(original);
        }
        panicked |= caught(context);
        panicked |= caught(binding_refused);
        panicked |= caught(anchor);
    }
    panicked |= std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(start))).is_err();
    for fault in faults {
        panicked |= std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match fault {
            hooks::Fault::Error(error) => drop(error),
            hooks::Fault::Panic(payload) => drop(payload),
        }))
        .is_err();
    }
    for fault in child_faults {
        panicked |=
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fault.dispose())).is_err();
    }
    panicked |= std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop((upgrade, upgrade_error, upgrade_response))
    }))
    .is_err();
    panicked
}
