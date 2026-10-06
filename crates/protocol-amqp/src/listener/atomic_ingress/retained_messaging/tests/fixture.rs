use std::{
    any::Any,
    future::{Future, pending, poll_fn},
    panic::AssertUnwindSafe,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use amqp::{
    Attach, Begin, Body, DeliveryState, Disposition, Flow, Frame, Message, Open, Performative,
    Properties, ProtocolHeader, ReceiverSettleMode, Role, SaslCode, SaslInit, SaslPerformative,
    SenderSettleMode, Source, Target, Transfer,
};
use futures_util::FutureExt;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::OwnedSemaphorePermit,
    time::timeout,
};

use super::super::{
    RetainedAtomicMessagingLimits as Limits, RetainedAtomicMessagingOwner as Owner,
    RetainedAtomicMessagingReport as Report,
    outcomes::{Gate, Hooks, locked},
};
use crate::{
    Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata, NativeAtomicBroker,
    NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable,
    OwnedNativeAtomicMessagingSubmission, RetainedConnectionStartError, SharedAccessAuthentication,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CHANNELS: [u16; 3] = [7, 9, 13];

#[derive(Default)]
pub(super) struct Recorder {
    pub(super) binds: Arc<AtomicUsize>,
    pub(super) submitted: Arc<AtomicUsize>,
    pub(super) clone_fault: Arc<std::sync::Mutex<Option<Box<dyn Any + Send>>>>,
}
impl Clone for Recorder {
    fn clone(&self) -> Self {
        if let Some(original) = locked(&self.clone_fault).take() {
            std::panic::resume_unwind(original);
        }
        Self {
            binds: Arc::clone(&self.binds),
            submitted: Arc::clone(&self.submitted),
            clone_fault: Arc::clone(&self.clone_fault),
        }
    }
}
impl Broker for Recorder {
    async fn bind(
        &self,
        namespace: domain::NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.binds.fetch_add(1, Ordering::SeqCst);
        Ok(Some(crate::broker::test_admission(
            namespace,
            target,
            EntityMetadata::Queue(domain::QueueConfig::default()),
        )))
    }
    async fn submit_fenced(
        &self,
        _: domain::EntityBinding,
        _: domain::EntityPath,
        _: domain::CommandKind,
    ) -> Result<domain::CommandOutcome, BrokerRejection> {
        Err(BrokerRejection::Refused(domain::BrokerError::QueueNotFound))
    }
    async fn submit(
        &self,
        _: domain::NamespaceName,
        _: domain::EntityPath,
        _: domain::CommandKind,
    ) -> Result<domain::CommandOutcome, BrokerRejection> {
        Err(BrokerRejection::Refused(domain::BrokerError::QueueNotFound))
    }
    async fn entity_metadata(
        &self,
        _: domain::NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok(None)
    }
    async fn rules(
        &self,
        _: domain::NamespaceName,
        _: domain::EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        Ok(Vec::new())
    }
    async fn rules_fenced(
        &self,
        _: domain::EntityBinding,
        _: domain::EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        Ok(Vec::new())
    }
    async fn deliverable(&self, _: &domain::NamespaceName, _: &domain::EntityPath) {
        pending::<()>().await;
    }
}
impl NativeAtomicBroker for Recorder {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
    + Send
    + 'static {
        let abort = submission.permit().abort_on_drop();
        self.submitted.fetch_add(1, Ordering::SeqCst);
        async move {
            let _abort = abort;
            let _submission = submission;
            Err(NativeAtomicResponseUnavailable)
        }
    }
}

pub(super) struct Anchor {
    pub(super) drops: Arc<AtomicUsize>,
    pub(super) permit: Option<OwnedSemaphorePermit>,
    _local: Rc<()>,
}
impl Anchor {
    pub(super) fn new(permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            drops: Arc::new(AtomicUsize::new(0)),
            permit,
            _local: Rc::new(()),
        }
    }
}
impl Drop for Anchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct Payload {
    pub(super) identity: Arc<()>,
    pub(super) drops: Arc<AtomicUsize>,
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalTestPayload(..)")
    }
}
impl std::fmt::Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original test routing failure")
    }
}
impl std::error::Error for Payload {}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
pub(super) fn payload() -> (Payload, Arc<()>, Arc<AtomicUsize>) {
    let identity = Arc::new(());
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Payload {
            identity: Arc::clone(&identity),
            drops: Arc::clone(&drops),
        },
        identity,
        drops,
    )
}

pub(super) struct Fixture<A = Anchor> {
    pub(super) owner: Owner<A, Recorder>,
    pub(super) peer: Option<TcpStream>,
    pub(super) hooks: Arc<Hooks>,
    pub(super) recorder: Recorder,
    pub(super) report: Option<Report<A>>,
    pub(super) start: Option<Result<(), RetainedConnectionStartError<Recorder>>>,
    authenticated: bool,
    outgoing_id: u32,
    incoming_id: u32,
}

pub(super) fn authentication(timeout: Duration) -> TestResult<SharedAccessAuthentication> {
    let policy = auth::SharedAccessPolicy::new([auth::SharedAccessRule::new(
        "test-rule",
        auth::ResourceScope::namespace("tenant.servicebus.windows.net")?,
        auth::SharedAccessKey::new("test-secret")?,
        None,
        auth::PermissionSet::SEND | auth::PermissionSet::LISTEN,
    )?])?;
    Ok(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")?
            .with_authorization_timeout(timeout),
    )
}

impl Fixture {
    pub(super) async fn new(sessions: usize, workers: usize) -> TestResult<Self> {
        Self::with_anchor(sessions, workers, Anchor::new(None), None, false).await
    }
}

impl<A> Fixture<A> {
    pub(super) async fn with_anchor(
        sessions: usize,
        workers: usize,
        anchor: A,
        authentication: Option<SharedAccessAuthentication>,
        ordinary: bool,
    ) -> TestResult<Self> {
        let limits = Limits::new(sessions, workers)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (connected, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let peer = connected?;
        let (server, _) = accepted?;
        let (owner, starter) = match Owner::new(Handle::current(), limits, anchor) {
            Ok(pair) => pair,
            Err(error) => {
                drop(error);
                return Err("bounded test root allocation refused".into());
            }
        };
        let hooks = owner.hooks();
        let recorder = Recorder::default();
        let mut configured =
            crate::AmqpListener::new(recorder.clone(), domain::NamespaceName::new("tenant")?)
                .with_handshake_timeout(DEADLINE);
        let authenticated = authentication.is_some();
        if let Some(authentication) = authentication {
            configured = configured.with_shared_access_authentication(authentication);
        }
        let start = if ordinary {
            // Negative default-policy probe only: this report does not certify legacy descendants.
            let super::super::root::RetainedAtomicMessagingStarter { socket, bridge } = starter;
            drop(bridge);
            configured.start_retained_connection(server, socket)
        } else {
            configured.start_retained_collected_atomic_messaging(server, starter)
        };
        Ok(Self {
            owner,
            peer: Some(peer),
            hooks,
            recorder,
            report: None,
            start: Some(start),
            authenticated,
            outgoing_id: 0,
            incoming_id: 0,
        })
    }

    pub(super) async fn drive<T>(
        &mut self,
        original: impl Future<Output = TestResult<T>>,
    ) -> TestResult<T> {
        drive_parts(&mut self.owner, original).await
    }

    pub(super) async fn hello(&mut self) -> TestResult {
        let authenticated = self.authenticated;
        let Self { owner, peer, .. } = self;
        drive_parts(owner, async {
            let peer = peer.as_mut().expect("original peer socket");
            if authenticated {
                amqp::write_protocol_header(peer, ProtocolHeader::SASL).await?;
                if amqp::read_protocol_header(peer).await? != ProtocolHeader::SASL { return Err("missing SASL header".into()); }
                match amqp::read_frame(peer).await? {
                    Frame::Sasl(SaslPerformative::Mechanisms(mechanisms)) => {
                        assert!(mechanisms.mechanisms.iter().any(|mechanism| mechanism.as_str() == "MSSBCBS"));
                    }
                    _ => return Err("missing SASL mechanisms".into()),
                }
                amqp::write_frame(peer, &Frame::Sasl(SaslPerformative::Init(SaslInit {
                    mechanism: "MSSBCBS".into(), initial_response: None,
                    hostname: Some("tenant.servicebus.windows.net".into()),
                }))).await?;
                if !matches!(amqp::read_frame(peer).await?, Frame::Sasl(SaslPerformative::Outcome(outcome)) if outcome.code == SaslCode::Ok) {
                    return Err("CBS SASL negotiation refused".into());
                }
            }
            amqp::write_protocol_header(peer, ProtocolHeader::AMQP).await?;
            if amqp::read_protocol_header(peer).await? != ProtocolHeader::AMQP { return Err("missing AMQP header".into()); }
            send_raw(peer, 0, Performative::Open(Open::new("retained-messaging-peer")), Vec::new()).await?;
            if !matches!(amqp::read_frame(peer).await?, Frame::Amqp { performative: Some(Performative::Open(_)), .. }) {
                return Err("missing original Open echo".into());
            }
            Ok(())
        }).await
    }

    pub(super) async fn bound(&mut self) -> TestResult {
        let control = self.owner.control();
        self.drive(async move {
            while !control.progress().bound() {
                if control.progress().authority_closed() {
                    return Err("closed before logical binding".into());
                }
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        self.send_payload(channel, performative, Vec::new()).await
    }
    pub(super) async fn send_payload(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        let Self { owner, peer, .. } = self;
        drive_parts(
            owner,
            send_raw(
                peer.as_mut().expect("original peer"),
                channel,
                performative,
                payload,
            ),
        )
        .await
    }
    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        let Self { owner, peer, .. } = self;
        drive_parts(owner, async {
            Ok(amqp::read_frame(peer.as_mut().expect("original peer")).await?)
        })
        .await
    }

    pub(super) async fn begin(&mut self, index: usize) -> TestResult {
        self.send(CHANNELS[index], Performative::Begin(Begin::default()))
            .await?;
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { channel, performative: Some(Performative::Begin(_)), .. } if channel == CHANNELS[index])
            {
                return Ok(());
            }
        }
        Err("missing bounded Begin response".into())
    }

    pub(super) async fn wait_sessions(&mut self, expected: usize) -> TestResult {
        // Returned original handles, not attempt count, establish actual installation.
        timeout(DEADLINE, async {
            loop {
                if self.owner.original_session_ids().len() >= expected {
                    return Ok(());
                }
                self.owner.drive_step().await;
            }
        })
        .await?
    }

    pub(super) async fn attach(&mut self, index: usize, attach: Attach) -> TestResult<Attach> {
        let name = attach.name.clone();
        self.send(CHANNELS[index], Performative::Attach(Box::new(attach)))
            .await?;
        for _ in 0..64 {
            match self.frame().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::Attach(response)),
                    ..
                } if channel == CHANNELS[index] && response.name == name => return Ok(*response),
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                _ => return Err("unexpected bounded Attach response".into()),
            }
        }
        Err("missing bounded Attach response".into())
    }

    pub(super) async fn wait_workers(&mut self, expected: usize) -> TestResult {
        let control = self.owner.control();
        self.drive(async move {
            while control.progress().worker_launches() < expected {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn end(&mut self, index: usize) -> TestResult {
        self.send(CHANNELS[index], Performative::End(amqp::End::default()))
            .await
    }

    pub(super) async fn gate(&mut self, gate: &Gate) -> TestResult {
        self.drive(async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn cancel_finish_at(&mut self, gate: &Gate) -> TestResult {
        let Self { owner, report, .. } = self;
        let mut original = std::pin::pin!(owner.finish());
        timeout(
            DEADLINE,
            poll_fn(|cx| {
                if let Poll::Ready(value) = original.as_mut().poll(cx) {
                    *report = value;
                    return Poll::Ready(Err(
                        "finish preceded original cancellation checkpoint".into()
                    ));
                }
                if gate.entered() {
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
        )
        .await?
    }

    pub(super) async fn complete(mut self) -> TestResult<Report<A>> {
        self.owner.stop();
        self.hooks.release();
        drop(self.peer.take());
        let report = if let Some(report) = self.report.take() {
            report
        } else {
            timeout(DEADLINE, self.owner.finish())
                .await?
                .ok_or("missing first original report")?
        };
        if self.start.as_ref().is_some_and(Result::is_err) {
            return Err("retained setup refused".into());
        }
        drop(self.start.take());
        Ok(report)
    }

    pub(super) async fn cbs_links(&mut self) -> TestResult<(u32, u32)> {
        let requests = self
            .attach(
                0,
                request("retained-cbs-requests", 11, Role::Sender, crate::CBS_NODE),
            )
            .await?;
        let mut reply = request("retained-cbs-replies", 12, Role::Receiver, crate::CBS_NODE);
        reply.target = Some(Target::new("retained-cbs-route").into());
        let replies = self.attach(0, reply).await?;
        self.send(
            CHANNELS[0],
            Performative::Flow(Flow {
                next_incoming_id: Some(self.incoming_id),
                incoming_window: 100,
                next_outgoing_id: self.outgoing_id,
                outgoing_window: 100,
                handle: Some(12),
                delivery_count: Some(0),
                link_credit: Some(4),
                ..Flow::default()
            }),
        )
        .await?;
        Ok((requests.handle, replies.handle))
    }

    pub(super) async fn cbs_token(
        &mut self,
        local: (u32, u32),
        token: &str,
        expected_status: i32,
    ) -> TestResult {
        let id = self.outgoing_id;
        let correlation = format!("retained-grant-{id}");
        let mut properties = amqp::ApplicationProperties::default();
        properties.insert("operation", "put-token".to_owned());
        properties.insert("type", "servicebus.windows.net:sastoken".to_owned());
        properties.insert(
            "name",
            "amqps://tenant.servicebus.windows.net/orders".to_owned(),
        );
        let message = Message {
            properties: Some(Properties {
                message_id: Some(correlation.clone().into()),
                reply_to: Some("retained-cbs-route".into()),
                ..Properties::default()
            }),
            application_properties: Some(properties),
            body: Body::Value(serde_amqp::Value::String(token.into())),
            ..Message::default()
        };
        self.send_payload(
            CHANNELS[0],
            Performative::Transfer(Transfer {
                handle: 11,
                delivery_id: Some(id),
                delivery_tag: Some(vec![id as u8].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            amqp::encode_message(&message)?,
        )
        .await?;
        self.outgoing_id += 1;
        let mut accepted = false;
        let mut refilled = false;
        let mut replied = false;
        for _ in 0..64 {
            match self.frame().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } if disposition.role == Role::Receiver && disposition.first == id => {
                    assert!(!accepted);
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    accepted = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                    ..
                } => {
                    assert!(!replied);
                    assert_eq!(transfer.handle, local.1);
                    let reply = amqp::decode_message(&payload)?;
                    assert_eq!(
                        reply
                            .properties
                            .as_ref()
                            .and_then(|properties| properties.correlation_id.as_ref()),
                        Some(&correlation.clone().into())
                    );
                    assert_eq!(
                        reply
                            .application_properties
                            .as_ref()
                            .and_then(|properties| properties.get("status-code")),
                        Some(&serde_amqp::Value::Int(expected_status))
                    );
                    self.incoming_id += 1;
                    self.send(
                        CHANNELS[0],
                        Performative::Disposition(Disposition {
                            role: Role::Receiver,
                            first: transfer
                                .delivery_id
                                .ok_or("missing original CBS delivery id")?,
                            last: None,
                            settled: true,
                            state: Some(DeliveryState::Accepted(amqp::Accepted)),
                            batchable: false,
                        }),
                    )
                    .await?;
                    replied = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() => {}
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(local.0) => {
                    if flow.delivery_count == Some(id + 1) {
                        assert!(!refilled);
                        assert_eq!(flow.link_credit, Some(32));
                        refilled = true;
                    } else {
                        assert_eq!(flow.delivery_count, Some(0));
                    }
                }
                _ => return Err("unexpected bounded CBS reply".into()),
            }
            if accepted && refilled && replied {
                return Ok(());
            }
        }
        Err("missing bounded CBS result/refill".into())
    }
}

pub(super) async fn drive_parts<A, T>(
    owner: &mut Owner<A, Recorder>,
    original: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let mut original = std::pin::pin!(original);
    timeout(DEADLINE, async {
        loop {
            tokio::select! { result = original.as_mut() => return result, () = owner.drive_step() => {} }
        }
    }).await?
}
pub(super) async fn send_raw(
    peer: &mut TcpStream,
    channel: u16,
    performative: Performative,
    payload: Vec<u8>,
) -> TestResult {
    Ok(amqp::write_frame(
        peer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload,
        },
    )
    .await?)
}
pub(super) async fn caught<T>(
    original: impl Future<Output = TestResult<T>>,
) -> Result<TestResult<T>, Box<dyn Any + Send>> {
    AssertUnwindSafe(original).catch_unwind().await
}
pub(super) fn rethrow<T>(original: Result<TestResult<T>, Box<dyn Any + Send>>) -> TestResult<T> {
    match original {
        Ok(original) => original,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
pub(super) fn request(name: &str, handle: u32, role: Role, address: &str) -> Attach {
    Attach {
        name: name.into(),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: (role == Role::Receiver).then(|| Source::new(address)),
        target: (role == Role::Sender).then(|| Target::new(address).into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}
pub(super) fn producer(handle: u32) -> Attach {
    request(
        &format!("retained-producer-{handle}"),
        handle,
        Role::Sender,
        "orders",
    )
}
pub(super) fn controller(handle: u32) -> Attach {
    let mut attach = request(
        &format!("retained-controller-{handle}"),
        handle,
        Role::Sender,
        "orders",
    );
    attach.target = Some(amqp::Coordinator::default().into());
    attach
}
pub(super) fn original_pointer(original: &super::super::admission::Original) -> usize {
    original.as_ref().get_ref() as *const _ as *const () as usize
}
pub(super) fn facts<A>(report: &Report<A>) -> (usize, usize, usize, bool) {
    let sessions: Vec<_> = report.sessions().iter().map(|row| row.id()).collect();
    let workers: Vec<_> = report.workers().map(|row| row.id()).collect();
    let unique = sessions
        .iter()
        .enumerate()
        .all(|(index, id)| !sessions[..index].contains(id) && !workers.contains(id))
        && workers
            .iter()
            .enumerate()
            .all(|(index, id)| !workers[..index].contains(id));
    let raw = report.socket().wrapper().is_some()
        && report.socket().actor().is_some()
        && report.socket().reader().is_some();
    (
        report.session_attempts(),
        report.session_joins(),
        report.worker_joins(),
        unique && raw,
    )
}
pub(super) fn worker_ids(hooks: &Hooks) -> Vec<tokio::task::Id> {
    locked(&hooks.worker_ids).clone()
}
