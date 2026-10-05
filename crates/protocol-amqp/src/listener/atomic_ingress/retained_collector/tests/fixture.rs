use std::{
    future::Future,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    Attach, Begin, ConnectionOptions, Coordinator, End, Frame, IncomingSession, Open, Performative,
    ProtocolHeader, ReceiverSettleMode, Role, ScopedConnectionAcceptance, SenderSettleMode,
    ServerConnection, ServerConnectionJoinReport, ServerConnectionOwner, Source, Target,
    read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{io::DuplexStream, runtime::Handle, time::timeout};

use super::super::super::{
    IngressMode,
    retained_session::controls::{Fault, Gate},
};
use super::super::{Refused, Report, Root, Settings, admissions::Original, controls::Controls};
use super::recorder::Recorder;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CHANNELS: [u16; 2] = [7, 9];

pub(super) struct Anchor {
    pub(super) drops: Arc<AtomicUsize>,
    _not_send: Rc<()>,
}
impl Anchor {
    fn new() -> Self {
        Self {
            drops: Arc::new(AtomicUsize::new(0)),
            _not_send: Rc::new(()),
        }
    }
}
impl Drop for Anchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
pub(super) struct Cleanup<A = Anchor> {
    pub(super) report: Option<Report<A>>,
    pub(super) refused: Option<Box<Refused<A, Recorder>>>,
    pub(super) socket: Option<ServerConnectionJoinReport<()>>,
    pub(super) unused: Vec<Fault>,
    pub(super) returned_offers: Vec<super::super::OfferRefused>,
}
pub(super) struct Fixture<A = Anchor> {
    pub(super) root: Option<Root<A, Recorder>>,
    pub(super) controls: Arc<Controls>,
    pub(super) recorder: Recorder,
    pub(super) peer: Option<DuplexStream>,
    pub(super) connection: Option<ServerConnection>,
    socket: ServerConnectionOwner<()>,
    pub(super) report: Option<Report<A>>,
    refused: Option<Box<Refused<A, Recorder>>>,
    returned_offers: Vec<super::super::OfferRefused>,
}
impl Fixture {
    pub(super) async fn new(
        mode: IngressMode,
        limit: usize,
        controls: Arc<Controls>,
    ) -> TestResult<Self> {
        Self::with_anchor(mode, limit, controls, Anchor::new()).await
    }
}
impl<A> Fixture<A> {
    pub(super) async fn with_anchor(
        mode: IngressMode,
        limit: usize,
        controls: Arc<Controls>,
        anchor: A,
    ) -> TestResult<Self> {
        let namespace = domain::NamespaceName::new("tenant")?;
        let (server, mut peer) = tokio::io::duplex(16_384);
        let (mut socket, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
        let acceptance = async {
            match mode {
                IngressMode::Posting => {
                    acceptor
                        .accept_with_transactional_ingress(
                            server,
                            "retained-collector-server",
                            None,
                            ConnectionOptions::default(),
                        )
                        .await
                }
                IngressMode::Messaging => {
                    acceptor
                        .accept_with_transactional_work_defaults(
                            server,
                            "retained-collector-server",
                            None,
                            ConnectionOptions::default(),
                        )
                        .await
                }
            }
        };
        let negotiation = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
            if read_protocol_header(&mut peer).await? != ProtocolHeader::AMQP {
                return Err("unexpected protocol header".into());
            }
            send(
                &mut peer,
                0,
                Performative::Open(Open::new("collector-peer")),
            )
            .await?;
            if !matches!(
                read_frame(&mut peer).await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ) {
                return Err("missing Open echo".into());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let mut paired = Box::pin(async { tokio::join!(acceptance, negotiation) });
        let observed = timeout(DEADLINE, paired.as_mut()).await;
        let (accepted, negotiated) = match observed {
            Ok(results) => results,
            Err(error) => {
                socket.stop();
                let joined = socket.finish().await;
                drop(paired);
                drop(peer);
                drop(joined);
                return Err(error.into());
            }
        };
        drop(paired);
        let connection = match (accepted, negotiated) {
            (Ok(ScopedConnectionAcceptance::Accepted(connection)), Ok(())) => connection,
            (accepted, negotiated) => {
                socket.stop();
                drop(peer);
                let joined = socket.finish().await;
                drop(joined);
                negotiated?;
                match accepted {
                    Err(error) => return Err(error.into()),
                    Ok(_) => return Err("unexpected launch refusal".into()),
                }
            }
        };
        let recorder = Recorder::default();
        let settings = Settings {
            identity: connection.connection_identity().clone(),
            namespace,
            broker: recorder.clone(),
            authorization: None,
            mode,
            runtime: Handle::current(),
            controls: controls.clone(),
        };
        let (root, refused) = match Root::new(settings, limit, anchor) {
            Ok(root) => (Some(root), None),
            Err(refused) => (None, Some(refused)),
        };
        Ok(Self {
            root,
            controls,
            recorder,
            peer: Some(peer),
            connection: Some(connection),
            socket,
            report: None,
            refused,
            returned_offers: Vec::new(),
        })
    }
    pub(super) fn counts(&self) -> (usize, (usize, usize, bool), (usize, usize, bool)) {
        self.root.as_ref().expect("created collector").counts()
    }
    pub(super) async fn drive<T>(
        &mut self,
        future: impl Future<Output = TestResult<T>>,
    ) -> TestResult<T> {
        drive_parts(&mut self.root, future).await
    }
    pub(super) async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        let Self { root, peer, .. } = self;
        drive_parts(
            root,
            send(peer.as_mut().expect("retained peer"), channel, performative),
        )
        .await
    }
    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        let Self { root, peer, .. } = self;
        drive_parts(root, async {
            Ok(read_frame(peer.as_mut().expect("retained peer")).await?)
        })
        .await
    }
    pub(super) async fn incoming(&mut self, channel: u16) -> TestResult<IncomingSession> {
        self.send(channel, Performative::Begin(Begin::default()))
            .await?;
        let Self {
            root, connection, ..
        } = self;
        drive_parts(root, async {
            connection
                .as_mut()
                .expect("retained connection")
                .next_incoming_session()
                .await
                .ok_or_else(|| "missing incoming Session".into())
        })
        .await
    }
    pub(super) fn offer(
        &mut self,
        incoming: IncomingSession,
    ) -> Result<usize, super::super::OfferRefused> {
        self.root.as_mut().expect("created collector").offer(
            self.connection.as_ref().expect("retained connection"),
            incoming,
        )
    }
    pub(super) fn returned_offer(&mut self, refused: super::super::OfferRefused) {
        self.returned_offers.push(refused);
    }
    pub(super) fn offer_with(
        &mut self,
        incoming: IncomingSession,
        wrap: impl FnOnce(Original) -> Original,
    ) -> Result<usize, super::super::OfferRefused> {
        self.root.as_mut().expect("created collector").offer_with(
            self.connection.as_ref().expect("retained connection"),
            incoming,
            wrap,
        )
    }
    pub(super) async fn accept(&mut self, channel: u16) -> TestResult<usize> {
        let incoming = self.incoming(channel).await?;
        let expected_launches = self.counts().1.1 + 1;
        let index = match self.offer(incoming) {
            Ok(index) => index,
            Err(refused) => {
                self.returned_offers.push(refused);
                return Err("unexpected private offer refusal".into());
            }
        };
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { channel: ch, performative: Some(Performative::Begin(_)), .. } if ch == channel)
            {
                self.wait_sessions(expected_launches).await?;
                return Ok(index);
            }
        }
        Err("missing bounded Begin echo".into())
    }
    pub(super) async fn sessions(&mut self) -> TestResult {
        for channel in CHANNELS {
            self.accept(channel).await?;
        }
        Ok(())
    }
    pub(super) async fn attached(&mut self, channel: u16, attach: Attach) -> TestResult {
        let handle = attach.handle;
        self.send(channel, Performative::Attach(Box::new(attach)))
            .await?;
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { channel: ch, performative: Some(Performative::Attach(attach)), .. }
                if ch == channel && attach.handle == handle)
            {
                return Ok(());
            }
        }
        Err("missing bounded Attach echo".into())
    }
    pub(super) async fn wait_gate(&mut self, gate: &Gate) -> TestResult {
        self.drive(async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn wait_sessions(&mut self, expected: usize) -> TestResult {
        let root = self.root.as_mut().expect("created collector");
        let budget = root.session_budget.clone();
        self.drive(async {
            while budget.counts().1 < expected {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn wait_workers(&mut self, expected: usize) -> TestResult {
        let budget = self
            .root
            .as_ref()
            .expect("created collector")
            .worker_budget
            .clone();
        self.drive(async {
            while budget.counts().1 < expected {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn end(&mut self, index: usize) -> TestResult {
        self.send(CHANNELS[index], Performative::End(End::default()))
            .await
    }
    pub(super) async fn cancel_finish_at(&mut self, gate: &Gate) -> TestResult {
        let Self { root, report, .. } = self;
        let mut finish = Box::pin(root.as_mut().expect("created collector").finish());
        let observed = timeout(DEADLINE, async {
            tokio::select! {
                original = finish.as_mut() => {
                    *report = Some(original);
                    Err("finish completed before cancellation checkpoint".into())
                },
                () = async { while !gate.entered() { tokio::task::yield_now().await; } } => Ok(()),
            }
        })
        .await;
        // Cancel the borrowed observation, not any retained root/task/result.
        drop(finish);
        observed?
    }
    pub(super) async fn unwind_finish(&mut self) -> std::thread::Result<()> {
        use futures_util::FutureExt;
        let Self { root, report, .. } = self;
        let mut finish = Box::pin(root.as_mut().expect("created collector").finish());
        let observed = std::panic::AssertUnwindSafe(finish.as_mut())
            .catch_unwind()
            .await;
        let result = match observed {
            Ok(original) => {
                *report = Some(original);
                Ok(())
            }
            Err(payload) => Err(payload),
        };
        drop(finish);
        result
    }
    pub(super) async fn complete(mut self) -> Cleanup<A> {
        if self.report.is_none()
            && let Some(root) = &mut self.root
        {
            root.stop();
            self.controls.release_observers();
            self.report = Some(root.finish().await);
        }
        self.controls.release_all();
        self.socket.stop();
        drop(self.peer.take());
        drop(self.connection.take());
        let socket = self.socket.finish().await;
        let mut unused = Vec::new();
        for controls in &self.controls.sessions {
            if let Some(fault) = controls.take_fault() {
                unused.push(fault);
            }
            if let Some(fault) = controls.take_session_fault() {
                unused.push(fault);
            }
        }
        Cleanup {
            report: self.report,
            refused: self.refused,
            socket,
            unused,
            returned_offers: self.returned_offers,
        }
    }
}

async fn drive_parts<A, T>(
    root: &mut Option<Root<A, Recorder>>,
    future: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let mut future = std::pin::pin!(future);
    let root = root.as_mut().expect("created collector");
    timeout(DEADLINE, async {
        tokio::select! {
            result = future.as_mut() => result,
            () = root.drive() => unreachable!("borrowed active drive"),
        }
    })
    .await?
}

pub(super) async fn send(
    peer: &mut DuplexStream,
    channel: u16,
    performative: Performative,
) -> TestResult {
    Ok(write_frame(
        peer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload: Vec::new(),
        },
    )
    .await?)
}
pub(super) fn producer(handle: u32) -> Attach {
    Attach {
        name: format!("collector-producer-{handle}"),
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
    attach.target = Some(Coordinator::default().into());
    attach
}
pub(super) fn consumer(handle: u32) -> Attach {
    let mut attach = producer(handle);
    attach.name = format!("collector-consumer-{handle}");
    attach.role = Role::Receiver;
    attach.source = Some(Source::new("orders"));
    attach.target = Some(Target::default().into());
    attach.rcv_settle_mode = ReceiverSettleMode::Second;
    attach.initial_delivery_count = None;
    attach
}

pub(super) fn evidence<A>(cleanup: &Cleanup<A>) -> (usize, usize, usize, bool) {
    let Some(report) = &cleanup.report else {
        return (0, 0, 0, false);
    };
    let mut ids = Vec::new();
    let mut ordinals = Vec::new();
    let mut unique = true;
    for packet in &report.packets {
        for launch in &packet.launches {
            unique &= !ids.contains(&launch.id) && !ordinals.contains(&launch.ordinal);
            unique &= packet.rows.iter().filter(|row| row.id == launch.id).count() == 1;
            ids.push(launch.id);
            ordinals.push(launch.ordinal);
        }
    }
    let sessions = report.sessions.iter().flatten().count();
    if let [Some(first), Some(second)] = &report.sessions {
        unique &= first.id != second.id;
    }
    unique &= report
        .sessions
        .iter()
        .flatten()
        .all(|session| !ids.contains(&session.id));
    let socket = cleanup.socket.as_ref().is_some_and(|report| {
        report.actor().is_some_and(Result::is_ok)
            && report.reader().is_some_and(|result| {
                result.is_ok() || result.as_ref().is_err_and(|e| e.is_cancelled())
            })
    });
    for (index, record) in report.admissions.iter().enumerate() {
        if let Some(record) = record
            && let super::super::admissions::Outcome::Launched { id, ordinal } = &record.outcome
        {
            unique &= *ordinal < 2
                && report.sessions[index]
                    .as_ref()
                    .is_some_and(|session| session.id == *id);
        }
    }
    unique &= (0..report.worker_counts.1).all(|ordinal| ordinals.contains(&ordinal));
    (
        report.attempts,
        sessions,
        ids.len(),
        unique && socket && cleanup.unused.is_empty() && cleanup.returned_offers.is_empty(),
    )
}
