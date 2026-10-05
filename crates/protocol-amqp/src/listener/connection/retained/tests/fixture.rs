use super::io_gate::GatedIo;
use super::*;
use crate::{
    Attachment, BrokerRejection, EntityAdmission, EntityMetadata, NativeAtomicBrokerCompletion,
    NativeAtomicResponseUnavailable, OwnedNativeAtomicMessagingSubmission,
};
use amqp::{Frame, Open, Performative, ProtocolHeader};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, RuleDefinition,
    SubscriptionName,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
pub(super) const OBSERVATION: Duration = Duration::from_secs(3);

pub(super) async fn bounded<F: Future>(
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(OBSERVATION, future).await
}

#[derive(Default)]
pub(super) struct DriverPlan {
    pub(super) calls: AtomicUsize,
    pub(super) wait_begin: AtomicBool,
    pub(super) result: std::sync::Mutex<Option<RetainedConnectionResult>>,
    pub(super) future_drop_panic: std::sync::Mutex<Option<Arc<PayloadWitness>>>,
}

#[derive(Clone, Default)]
pub(super) struct NoBroker {
    pub(super) plan: Arc<DriverPlan>,
}

impl NoBroker {
    fn unavailable(&self) -> BrokerRejection {
        self.plan.calls.fetch_add(1, Ordering::SeqCst);
        BrokerRejection::Unavailable("unused scoped-socket fixture broker".into())
    }
}

impl Broker for NoBroker {
    async fn bind(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn submit_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        Err(self.unavailable())
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        self.plan.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeAtomicBroker for NoBroker {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
    + Send
    + 'static {
        let plan = self.plan.clone();
        async move {
            plan.calls.fetch_add(1, Ordering::SeqCst);
            drop(submission);
            Err(NativeAtomicResponseUnavailable)
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct TestDriver;

struct DriverFuture<'a> {
    future: Pin<Box<dyn Future<Output = RetainedConnectionResult> + Send + 'a>>,
    panic: Option<Arc<PayloadWitness>>,
}

impl Future for DriverFuture<'_> {
    type Output = RetainedConnectionResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl Drop for DriverFuture<'_> {
    fn drop(&mut self) {
        if let Some(witness) = self.panic.take() {
            std::panic::panic_any(PayloadError {
                witness,
                name: "private-driver-future-drop",
            });
        }
    }
}

impl ConnectionDriver<NoBroker> for TestDriver {
    const ADMISSION: AdmissionMode = AdmissionMode::Ordinary;
    fn serve_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        _: NamespaceName,
        broker: NoBroker,
        _: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = RetainedConnectionResult> + Send + 'a {
        let panic =
            crate::listener::retained_connection::locked(&broker.plan.future_drop_panic).take();
        DriverFuture {
            future: Box::pin(async move {
                if broker.plan.wait_begin.load(Ordering::SeqCst) {
                    let incoming = connection
                        .next_incoming_session()
                        .await
                        .ok_or_else(|| io::Error::other("fixture Begin was not received"))?;
                    drop(incoming);
                }
                let result =
                    crate::listener::retained_connection::locked(&broker.plan.result).take();
                result.unwrap_or(Ok(()))
            }),
            panic,
        }
    }
}

pub(super) struct Harness<A = ()> {
    pub(super) owner: RetainedConnectionOwner<A>,
    pub(super) controls: Arc<Controls>,
    pub(super) peer: Option<DuplexStream>,
    pub(super) io: Arc<IoPlan>,
    pub(super) broker: NoBroker,
    pub(super) started: bool,
    pub(super) deadline: tokio::time::Instant,
}

impl<A> Harness<A> {
    pub(super) fn new<D: ConnectionDriver<NoBroker>>(
        anchor: A,
        driver: D,
        websocket: bool,
    ) -> Self {
        Self::configured(anchor, driver, websocket, None, Duration::from_secs(2))
    }

    pub(super) fn configured<D: ConnectionDriver<NoBroker>>(
        anchor: A,
        driver: D,
        websocket: bool,
        authentication: Option<SharedAccessAuthentication>,
        timeout: Duration,
    ) -> Self {
        let (owner, starter) =
            RetainedConnectionOwner::new(tokio::runtime::Handle::current(), anchor);
        let controls = owner.controls();
        let io = Arc::new(IoPlan::default());
        *crate::listener::retained_connection::locked(&io.controls) = Some(controls.clone());
        let (inner, peer) = tokio::io::duplex(128 * 1024);
        let broker = NoBroker::default();
        let deadline = tokio::time::Instant::now() + timeout;
        let started = start_test(
            GatedIo {
                inner,
                plan: io.clone(),
            },
            websocket,
            ConnectionSettings {
                container_id: "retained-fixture".into(),
                namespace: NamespaceName::new("tenant").expect("fixture namespace"),
                broker: broker.clone(),
                shared_access_authentication: authentication,
                connection_options: amqp::ConnectionOptions::default(),
                deadline,
            },
            driver,
            starter,
        );
        Self {
            owner,
            controls,
            peer: Some(peer),
            io,
            broker,
            started,
            deadline,
        }
    }

    pub(super) fn error(&self, witness: Arc<PayloadWitness>) {
        *crate::listener::retained_connection::locked(&self.broker.plan.result) =
            Some(Err(Box::new(PayloadError {
                witness,
                name: "private-primary-cause",
            })));
    }

    pub(super) fn wait_begin(&self) {
        self.broker.plan.wait_begin.store(true, Ordering::SeqCst);
    }

    pub(super) fn release(&self) {
        self.io.release();
        self.controls.release();
    }

    pub(super) async fn finish(&mut self) -> Option<RetainedConnectionJoinReport<A>> {
        self.release();
        self.owner.stop();
        drop(self.peer.take());
        self.owner.finish().await
    }
}

pub(super) async fn hello<Io: AsyncRead + AsyncWrite + Unpin>(peer: &mut Io) -> io::Result<()> {
    amqp::write_protocol_header(peer, ProtocolHeader::AMQP).await?;
    if amqp::read_protocol_header(peer).await? != ProtocolHeader::AMQP {
        return Err(io::Error::other("unexpected server protocol header"));
    }
    amqp::write_frame(
        peer,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open::new("peer"))),
            payload: Vec::new(),
        },
    )
    .await?;
    match amqp::read_frame(peer).await? {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        } => Ok(()),
        _ => Err(io::Error::other("unexpected server Open")),
    }
}

pub(super) async fn send_begin<Io: AsyncWrite + Unpin>(peer: &mut Io) -> io::Result<()> {
    amqp::write_frame(
        peer,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Begin(amqp::Begin {
                remote_channel: None,
                next_outgoing_id: 0,
                incoming_window: 16,
                outgoing_window: 16,
                handle_max: u32::MAX,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            payload: Vec::new(),
        },
    )
    .await
}

pub(super) async fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (connected, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let connected = connected?;
    let (accepted, _) = accepted?;
    Ok((accepted, connected))
}

pub(super) async fn sasl_hello<Io: AsyncRead + AsyncWrite + Unpin>(
    peer: &mut Io,
    mechanism: &str,
) -> io::Result<amqp::SaslCode> {
    amqp::write_protocol_header(peer, ProtocolHeader::SASL).await?;
    if amqp::read_protocol_header(peer).await? != ProtocolHeader::SASL {
        return Err(io::Error::other("unexpected SASL protocol header"));
    }
    match amqp::read_frame(peer).await? {
        Frame::Sasl(amqp::SaslPerformative::Mechanisms(_)) => {}
        _ => return Err(io::Error::other("missing SASL mechanisms")),
    }
    amqp::write_frame(
        peer,
        &Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
            mechanism: mechanism.into(),
            initial_response: None,
            hostname: None,
        })),
    )
    .await?;
    match amqp::read_frame(peer).await? {
        Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) => Ok(outcome.code),
        _ => Err(io::Error::other("missing SASL outcome")),
    }
}

pub(super) fn authentication() -> TestResult<SharedAccessAuthentication> {
    Ok(SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([])?,
        "tenant.servicebus.windows.net",
    )?)
}
