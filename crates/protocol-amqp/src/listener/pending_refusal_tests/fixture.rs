use super::*;
use amqp::{
    Begin, Body, ConnectionOptions, DeliveryState, Detach, Disposition, Flow, Frame, Message, Open,
    Performative, Properties, ProtocolHeader, ReceiverSettleMode, ScopedConnectionAcceptance,
    ServerConnectionJoinReport, ServerConnectionOwner, Source, Target, Transfer, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{EntityBinding, SequenceNumber};
use futures_util::FutureExt;
use std::{
    any::Any,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream},
    sync::{Notify, oneshot},
    task::{JoinError, JoinHandle},
    time::timeout,
};

pub(super) type WorkerResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(3), future)
        .await
        .expect("bounded original operation")
}
pub(super) async fn caught<T>(future: impl Future<Output = T>) -> Result<T, Box<dyn Any + Send>> {
    AssertUnwindSafe(future).catch_unwind().await
}
pub(super) fn rethrow(result: Result<(), Box<dyn Any + Send>>) {
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
pub(super) fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}
pub(super) fn authorization(
    permissions: PermissionSet,
    initial: bool,
) -> Arc<ConnectionAuthorization> {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "test-rule",
        ResourceScope::namespace("tenant.servicebus.windows.net").expect("scope"),
        SharedAccessKey::new("test-secret").expect("key"),
        None,
        permissions,
    )
    .expect("rule")])
    .expect("policy");
    let grant = initial.then(|| {
        policy
            .authenticate_plain("test-rule", "test-secret")
            .expect("initial grant")
    });
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")
            .expect("authentication"),
        grant,
    )
}

#[derive(Clone, Default)]
pub(super) struct CountedBroker {
    pub binds: Arc<AtomicUsize>,
    pub commands: Arc<Mutex<Vec<(EntityBinding, CommandKind)>>>,
    pub missing: Arc<AtomicBool>,
    pub panic_submit: Arc<AtomicBool>,
    pub submitted: Arc<Notify>,
}
impl Broker for CountedBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<crate::EntityAdmission>, BrokerRejection> {
        self.binds.fetch_add(1, Ordering::SeqCst);
        if self.missing.load(Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(Some(crate::broker::test_admission(
            namespace,
            target,
            crate::EntityMetadata::Queue(domain::QueueConfig::default()),
        )))
    }
    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(binding.namespace(), &namespace());
        assert_eq!(binding.target(), &entity);
        assert!(matches!(kind, CommandKind::SendEnvelope { .. }));
        let mut records = self.commands.lock().expect("bounded commands");
        assert!(records.len() < 8);
        records.push((binding, kind));
        drop(records);
        self.submitted.notify_one();
        if self.panic_submit.load(Ordering::SeqCst) {
            std::panic::panic_any(String::from("original counted broker panic"));
        }
        Ok(CommandOutcome::Sent {
            sequence: SequenceNumber::new(7),
        })
    }
    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("refusal/workflow must not read rules")
    }
    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<crate::EntityMetadata>, BrokerRejection> {
        panic!("admission is one fenced read")
    }
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("unfenced submit")
    }
    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("unfenced rules")
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("no receive worker is claimed")
    }
}

pub(super) fn request(name: &str, peer_handle: u32, role: Role, address: &str) -> Attach {
    Attach {
        name: name.into(),
        handle: peer_handle,
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
struct GatedIo {
    inner: DuplexStream,
    gate: Arc<FlushGate>,
}
impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.gate.held.load(Ordering::Acquire) {
            *self.gate.waker.lock().expect("gate") = Some(cx.waker().clone());
            if self.gate.held.load(Ordering::Acquire) {
                self.gate.entered.notify_one();
                return Poll::Pending;
            }
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// No public native client spawn or unowned whole-listener worker is used.
pub(super) struct Fixture {
    owner: ServerConnectionOwner<Arc<()>>,
    pub connection: ServerConnection,
    pub session: Option<ServerSession>,
    pub peer: DuplexStream,
    pub gate: Arc<FlushGate>,
    pub local_channel: u16,
    pub broker: CountedBroker,
    pub authorization: Arc<ConnectionAuthorization>,
    acceptance: Result<WorkerResult, JoinError>,
    session_task: Option<JoinHandle<WorkerResult>>,
    session_result: Option<Result<WorkerResult, JoinError>>,
    workers: Vec<JoinHandle<WorkerResult>>,
    pub worker_results: Vec<Result<WorkerResult, JoinError>>,
    pub report: Option<ServerConnectionJoinReport<Arc<()>>>,
    anchor: Arc<()>,
    peer_outgoing_id: u32,
    peer_incoming_id: u32,
}
impl Fixture {
    pub async fn new(authorization: Arc<ConnectionAuthorization>) -> Self {
        let (io, mut peer) = tokio::io::duplex(65536);
        let gate = Arc::new(FlushGate::default());
        let anchor = Arc::new(());
        let (owner, acceptor) =
            ServerConnectionOwner::new(tokio::runtime::Handle::current(), anchor.clone());
        let transport_gate = gate.clone();
        let (accepted, receiving) = oneshot::channel();
        let mut acceptance = tokio::spawn(async move {
            match acceptor
                .accept_with_options(
                    GatedIo {
                        inner: io,
                        gate: transport_gate,
                    },
                    "protocol-refusal",
                    None,
                    ConnectionOptions::default(),
                )
                .await?
            {
                ScopedConnectionAcceptance::Accepted(connection) => {
                    accepted
                        .send(connection)
                        .map_err(|_| EngineError::Stopped)?;
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                }
                ScopedConnectionAcceptance::Refused(_) => Err(EngineError::Stopped.into()),
            }
        });
        bounded(write_protocol_header(&mut peer, ProtocolHeader::AMQP))
            .await
            .expect("peer header");
        bounded(read_protocol_header(&mut peer))
            .await
            .expect("server header");
        bounded(write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-protocol-peer"))),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("peer open");
        bounded(read_frame(&mut peer)).await.expect("server open");
        let mut connection = bounded(receiving).await.expect("original connection");
        let acceptance = bounded(&mut acceptance).await;
        bounded(write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 9,
                performative: Some(Performative::Begin(Begin::default())),
                payload: Vec::new(),
            },
        ))
        .await
        .expect("peer begin");
        let incoming = bounded(connection.next_incoming_session())
            .await
            .expect("original incoming session");
        let session = bounded(connection.accept_session(incoming))
            .await
            .expect("native session");
        let local_channel = match bounded(read_frame(&mut peer)).await.expect("local Begin") {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Begin(begin)),
                ..
            } => {
                assert_eq!(begin.remote_channel, Some(9));
                channel
            }
            other => panic!("expected local Begin: {other:?}"),
        };
        Self {
            owner,
            connection,
            session: Some(session),
            peer,
            gate,
            local_channel,
            broker: CountedBroker::default(),
            authorization,
            acceptance,
            session_task: None,
            session_result: None,
            workers: Vec::new(),
            worker_results: Vec::new(),
            report: None,
            anchor,
            peer_outgoing_id: 0,
            peer_incoming_id: 0,
        }
    }
    pub fn start_loop(&mut self) {
        let session = self.session.take().expect("original session parent");
        let broker = self.broker.clone();
        let authorization = self.authorization.clone();
        self.session_task = Some(tokio::spawn(serve_session(
            session,
            namespace(),
            broker,
            Some(authorization),
            ConnectionManagement::new(),
        )));
    }
    pub async fn write(&mut self, performative: Performative) {
        self.write_payload(performative, Vec::new()).await;
    }
    async fn write_payload(&mut self, performative: Performative, payload: Vec<u8>) {
        bounded(write_frame(
            &mut self.peer,
            &Frame::Amqp {
                channel: 9,
                performative: Some(performative),
                payload,
            },
        ))
        .await
        .expect("peer frame");
    }
    pub async fn read(&mut self) -> Frame {
        bounded(read_frame(&mut self.peer))
            .await
            .expect("actual server frame")
    }
    pub async fn send_attach(&mut self, request: &Attach) {
        self.write(Performative::Attach(Box::new(request.clone())))
            .await;
    }
    pub async fn receipt(&mut self, request: &Attach) -> amqp::IncomingAttach {
        self.send_attach(request).await;
        bounded(
            self.session
                .as_mut()
                .expect("direct session")
                .next_incoming_attach(),
        )
        .await
        .expect("original wire receipt")
    }
    pub async fn pair(&mut self, request: &Attach, expected: &str) -> u32 {
        let response = match self.read().await {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(response)),
                payload,
            } => {
                assert_eq!(channel, self.local_channel);
                assert!(payload.is_empty());
                assert_eq!(response.name, request.name);
                assert_eq!(response.role, request.role.opposite());
                assert!(response.source.is_none() && response.target.is_none());
                assert_eq!(
                    response.initial_delivery_count,
                    (request.role == Role::Receiver).then_some(0)
                );
                *response
            }
            other => panic!("expected pending refusal Attach, got {other:?}"),
        };
        match self.read().await {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Detach(detach)),
                payload,
            } => {
                assert_eq!(channel, self.local_channel);
                assert!(payload.is_empty());
                assert_eq!(detach.handle, response.handle);
                assert!(detach.closed);
                assert_eq!(
                    detach
                        .error
                        .expect("typed refusal")
                        .condition
                        .as_symbol()
                        .as_str(),
                    expected
                );
            }
            other => panic!("no Flow/Transfer is admitted before refusal Detach: {other:?}"),
        }
        self.write(Performative::Detach(Detach {
            handle: request.handle,
            closed: true,
            error: None,
        }))
        .await;
        response.handle
    }
    pub async fn deny_direct(&mut self, request: &Attach) {
        let receipt = self.receipt(request).await;
        let result = plan_link(
            &self.broker,
            &namespace(),
            if request.role == Role::Sender {
                request
                    .target
                    .as_ref()
                    .and_then(amqp::TargetTerminus::as_target)
                    .and_then(|target| target.address.as_deref())
                    .unwrap_or("")
            } else {
                request
                    .source
                    .as_ref()
                    .and_then(|source| source.address.as_deref())
                    .unwrap_or("")
            },
            &receipt,
            Some(&self.authorization),
            self.session.as_ref().map(|session| (session, &receipt)),
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("must deny before admission"),
        };
        bounded(
            self.session
                .as_ref()
                .expect("session")
                .reject_attach(receipt, error.primary.into_error()),
        )
        .await
        .expect("native pending denial");
        self.pair(request, "amqp:unauthorized-access").await;
    }
    async fn accept_direct(&mut self, receipt: amqp::IncomingAttach) -> (LinkEndpoint, Attach) {
        let endpoint = bounded(self.session.as_ref().expect("session").accept_attach(
            receipt,
            crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
        ))
        .await
        .expect("ordinary authorized acceptance");
        let response = match self.read().await {
            Frame::Amqp {
                performative: Some(Performative::Attach(attach)),
                ..
            } => *attach,
            other => panic!("expected positive Attach, got {other:?}"),
        };
        if response.role == Role::Receiver {
            assert!(matches!(self.read().await, Frame::Amqp {
                performative: Some(Performative::Flow(Flow { handle: Some(handle), link_credit: Some(credit), .. })), ..
            } if handle == response.handle && credit > 0));
        }
        (endpoint, response)
    }
    pub async fn cbs_grant(&mut self) {
        let requests = request("cbs-request", 11, Role::Sender, crate::CBS_NODE);
        let receipt = self.receipt(&requests).await;
        let (endpoint, request_response) = self.accept_direct(receipt).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("CBS request receiver");
        };
        self.workers.push(tokio::spawn(serve_cbs_requests(
            receiver,
            self.authorization.clone(),
        )));
        let mut replies = request("cbs-response", 12, Role::Receiver, crate::CBS_NODE);
        replies.target = Some(Target::new("refusal-replies").into());
        let receipt = self.receipt(&replies).await;
        let (endpoint, response) = self.accept_direct(receipt).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("CBS reply sender");
        };
        let (route, incoming) = self
            .authorization
            .register_reply_route("refusal-replies".into())
            .await;
        self.workers.push(tokio::spawn(serve_cbs_replies(
            sender,
            "refusal-replies".into(),
            route,
            incoming,
            self.authorization.clone(),
        )));
        self.credit(replies.handle, 4).await;
        let mut properties = amqp::ApplicationProperties::default();
        properties.insert("operation", "put-token".to_owned());
        properties.insert("type", "servicebus.windows.net:sastoken".to_owned());
        properties.insert(
            "name",
            "amqps://tenant.servicebus.windows.net/orders".to_owned(),
        );
        // HMAC-SHA256 with test-secret, URL-encoded audience + LF + 4102444800.
        // Static test data avoids adding a signing dependency to this crate.
        let token = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=t%2BJqGjYXAhFvGHj0Ah2YRKCttTeRfTm31ZGniQQrdoA%3D&se=4102444800&skn=test-rule";
        let message = Message {
            properties: Some(Properties {
                message_id: Some("grant-1".into()),
                reply_to: Some("refusal-replies".into()),
                ..Properties::default()
            }),
            application_properties: Some(properties),
            body: Body::Value(Value::String(token.into())),
            ..Message::default()
        };
        self.transfer(requests.handle, 0, message).await;
        let mut request_accepted = false;
        let mut request_refilled = false;
        let mut reply = None;
        while !request_accepted || !request_refilled || reply.is_none() {
            match self.read().await {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } if disposition.role == Role::Receiver && disposition.first == 0 => {
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    request_accepted = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                    ..
                } => {
                    assert_eq!(transfer.handle, response.handle);
                    let message = amqp::decode_message(&payload).expect("original CBS reply");
                    assert_eq!(
                        message
                            .properties
                            .as_ref()
                            .and_then(|p| p.correlation_id.as_ref()),
                        Some(&"grant-1".into())
                    );
                    assert_eq!(
                        message
                            .application_properties
                            .as_ref()
                            .and_then(|p| p.get("status-code")),
                        Some(&Value::Int(202))
                    );
                    let id = transfer.delivery_id.expect("CBS delivery id");
                    self.peer_incoming_id += 1;
                    self.write(Performative::Disposition(Disposition {
                        role: Role::Receiver,
                        first: id,
                        last: None,
                        settled: true,
                        state: Some(DeliveryState::Accepted(amqp::Accepted)),
                        batchable: false,
                    }))
                    .await;
                    reply = Some(message);
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() => {}
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(request_response.handle) => {
                    assert!(!request_refilled);
                    assert_eq!(flow.delivery_count, Some(1));
                    assert_eq!(flow.link_credit, Some(32));
                    request_refilled = true;
                }
                other => panic!("unexpected CBS wire operation: {other:?}"),
            }
        }
    }
    pub async fn credit(&mut self, peer_handle: u32, credit: u32) {
        self.write(Performative::Flow(Flow {
            next_incoming_id: Some(self.peer_incoming_id),
            incoming_window: 100,
            next_outgoing_id: self.peer_outgoing_id,
            outgoing_window: 100,
            handle: Some(peer_handle),
            delivery_count: Some(0),
            link_credit: Some(credit),
            ..Flow::default()
        }))
        .await;
    }
    pub async fn transfer(&mut self, peer_handle: u32, id: u32, message: Message) {
        self.write_payload(
            Performative::Transfer(Transfer {
                handle: peer_handle,
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
            amqp::encode_message(&message).expect("actual peer message"),
        )
        .await;
        self.peer_outgoing_id += 1;
    }
    pub async fn authorized_sender(&mut self, peer_handle: u32) -> u32 {
        let request = request("authorized-send", peer_handle, Role::Sender, "orders");
        let receipt = self.receipt(&request).await;
        let (entity, accepted, authorization, broker) = bounded(plan_link(
            &self.broker,
            &namespace(),
            "orders",
            &receipt,
            Some(&self.authorization),
            self.session.as_ref().map(|session| (session, &receipt)),
        ))
        .await
        .expect("authorized plan");
        assert!(accepted.is_none());
        let (endpoint, response) = self.accept_direct(receipt).await;
        let LinkEndpoint::Receiver(receiver) = endpoint else {
            panic!("data receiver");
        };
        self.workers.push(tokio::spawn(serve_sending_client(
            receiver,
            namespace(),
            entity,
            broker,
            authorization,
        )));
        response.handle
    }
    pub async fn authorized_listen(&mut self, peer_handle: u32) -> Sender {
        let request = request("authorized-listen", peer_handle, Role::Receiver, "orders");
        let receipt = self.receipt(&request).await;
        let (_, accepted, _, _) = bounded(plan_link(
            &self.broker,
            &namespace(),
            "orders",
            &receipt,
            Some(&self.authorization),
            self.session.as_ref().map(|session| (session, &receipt)),
        ))
        .await
        .expect("authorized listen plan");
        assert!(accepted.is_none());
        let (endpoint, _) = self.accept_direct(receipt).await;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("data sender");
        };
        self.credit(peer_handle, 2).await;
        sender
    }
    pub async fn accepted_send(&mut self, peer_handle: u32, local_handle: u32) {
        self.transfer(peer_handle, 1, Message::data("counted"))
            .await;
        let mut accepted = false;
        let mut refilled = false;
        while !accepted || !refilled {
            match self.read().await {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } if disposition.role == Role::Receiver && disposition.first == 1 => {
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                    assert!(!accepted);
                    accepted = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() => {}
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(local_handle) => {
                    assert!(!refilled);
                    assert_eq!(flow.delivery_count, Some(1));
                    assert_eq!(flow.link_credit, Some(32));
                    refilled = true;
                }
                other => panic!("unexpected data settlement: {other:?}"),
            }
        }
    }
    pub fn stop(&self) {
        self.owner.stop();
    }
    pub async fn finish(&mut self) {
        self.gate.release();
        self.owner.stop();
        if let Some(task) = self.session_task.as_mut() {
            self.session_result = Some(bounded(task).await);
            self.session_task.take();
        }
        for task in &mut self.workers {
            self.worker_results.push(bounded(task).await);
        }
        self.workers.clear();
        self.report = Some(
            bounded(self.owner.finish())
                .await
                .expect("original socket report"),
        );
    }
    pub fn joined(&self) {
        assert!(matches!(&self.acceptance, Ok(Ok(()))));
        assert!(self.session_task.is_none());
        if self.session.is_none() {
            assert!(self.session_result.is_some());
        }
        assert!(self.workers.is_empty());
        let report = self.report.as_ref().expect("retained report");
        assert!(report.actor().is_some_and(Result::is_ok));
        assert!(report.reader().is_some());
        assert!(Arc::ptr_eq(report.anchor(), &self.anchor));
        let _original_connection_identity = self.connection.connection_identity();
    }
    pub fn no_broker_effects(&self) {
        assert_eq!(self.broker.binds.load(Ordering::SeqCst), 0);
        assert!(self.broker.commands.lock().expect("commands").is_empty());
        assert!(self.workers.is_empty() && self.worker_results.is_empty());
    }
}
