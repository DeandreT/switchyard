use super::*;

type Pause = (flume::Sender<AtomicCommitPermit>, flume::Receiver<()>);

#[derive(Debug, Default)]
pub(super) struct Controls {
    pub(super) reads: AtomicUsize,
    pub(super) writes: AtomicUsize,
    pub(super) clocks: AtomicUsize,
    pub(super) binds: AtomicUsize,
    pub(super) handoffs: AtomicUsize,
    pub(super) completed: AtomicUsize,
    pub(super) now: AtomicU64,
    pub(super) fail_before: AtomicBool,
    pub(super) fail_after: AtomicBool,
    permits: Mutex<Vec<AtomicCommitPermit>>,
    handoff_pause: Mutex<Option<Pause>>,
}

impl Controls {
    pub(super) fn reset(&self) {
        for counter in [
            &self.reads,
            &self.writes,
            &self.clocks,
            &self.binds,
            &self.handoffs,
            &self.completed,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
        self.permits.lock().expect("permit observers").clear();
    }

    pub(super) fn pause_handoff(&self) -> (flume::Receiver<AtomicCommitPermit>, Release) {
        let (entered, observed) = flume::bounded(1);
        let (resume, resumed) = flume::bounded(1);
        assert!(
            self.handoff_pause
                .lock()
                .expect("handoff gate")
                .replace((entered, resumed))
                .is_none()
        );
        (observed, Release(Some(resume)))
    }

    pub(super) fn states(&self) -> Vec<AtomicCommitState> {
        self.permits
            .lock()
            .expect("permit observers")
            .iter()
            .map(|p| p.state())
            .collect()
    }
}

pub(super) struct Release(Option<flume::Sender<()>>);
impl Release {
    pub(super) fn release(mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    controls: Arc<Controls>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.writes.fetch_add(1, Ordering::SeqCst);
        if self.controls.fail_before.swap(false, Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.apply(batch)?;
        if self.controls.fail_after.swap(false, Ordering::SeqCst) {
            return Err(injected());
        }
        Ok(())
    }
}

fn injected() -> StorageError {
    StorageError::Backend {
        operation: "atomic socket commit",
        detail: "DO-NOT-EXPOSE-storage-detail".into(),
    }
}

#[derive(Clone)]
struct CountingClock(Arc<Controls>);
impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.0.clocks.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(self.0.now.load(Ordering::SeqCst))
    }
}

#[derive(Clone)]
struct DelegatingBroker {
    inner: BrokerHandle,
    controls: Arc<Controls>,
}

impl protocol_amqp::Broker for DelegatingBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.controls.binds.fetch_add(1, Ordering::SeqCst);
        protocol_amqp::Broker::bind(&self.inner, namespace, target).await
    }
    fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind)
    }
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind)
    }
    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }
    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription)
    }
    fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription)
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

impl NativeAtomicBroker for DelegatingBroker {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
    + Send
    + 'static {
        let permit = submission.permit().clone();
        self.controls.handoffs.fetch_add(1, Ordering::SeqCst);
        self.controls
            .permits
            .lock()
            .expect("permit observers")
            .push(permit.clone());
        let pause = self
            .controls
            .handoff_pause
            .lock()
            .expect("handoff gate")
            .take();
        let pending =
            NativeAtomicBroker::submit_native_atomic_messaging_owned(&self.inner, submission);
        let controls = self.controls.clone();
        async move {
            if let Some((entered, resume)) = pause {
                let _ = entered.send(permit);
                let _ = resume.recv_async().await;
            }
            let result = pending.await;
            controls.completed.fetch_add(1, Ordering::SeqCst);
            result
        }
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) broker: Broker,
    pub(super) store: P::Store,
    pub(super) namespace: NamespaceName,
    pub(super) controls: Arc<Controls>,
    pub(super) address: std::net::SocketAddr,
    listener: JoinHandle<()>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P, atomic: bool) -> TestResult<Self> {
        Self::start_with_security(provider, atomic, None).await
    }

    pub(super) async fn start_with_security(
        provider: P,
        atomic: bool,
        security: Option<(
            rustls::ServerConfig,
            protocol_amqp::SharedAccessAuthentication,
        )>,
    ) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let machine = StateMachine::new(store.clone());
        for entity in ["orders", "healthy", "other", "session"] {
            machine.apply(&Command::new(
                namespace.clone(),
                EntityPath::new(entity)?,
                Timestamp::from_millis(1_000),
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session: entity == "session",
                        ..QueueConfig::default()
                    },
                },
            ))?;
        }
        machine.apply(&Command::new(
            namespace.clone(),
            EntityPath::new("topic")?,
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?;
        machine.apply(&Command::new(
            namespace.clone(),
            EntityPath::new("topic")?,
            Timestamp::from_millis(1_000),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Alpha")?,
                config: SubscriptionConfig::default(),
            },
        ))?;
        let controls = Arc::new(Controls::default());
        controls.now.store(2_000, Ordering::SeqCst);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(ObservedStore {
                inner: store.clone(),
                controls: controls.clone(),
            }),
            CountingClock(controls.clone()),
        ));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let delegate = DelegatingBroker {
            inner: broker.handle(),
            controls: controls.clone(),
        };
        let scope = namespace.clone();
        let listener = tokio::spawn(async move {
            let mut service =
                protocol_amqp::AmqpListener::new(delegate, scope).with_idle_timeout_millis(0);
            if let Some((tls, authentication)) = security {
                service = service
                    .with_tls(tls)
                    .with_shared_access_authentication(authentication);
            }
            if atomic {
                let _ = service.serve_atomic_posting_ingress(socket).await;
            } else {
                let _ = service.serve(socket).await;
            }
        });
        Ok(Self {
            broker,
            store,
            namespace,
            controls,
            address,
            listener,
            provider,
        })
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    pub(super) fn messages(&self, entity: &str) -> TestResult<Vec<domain::Delivery>> {
        peek(&self.store, &self.namespace, entity)
    }

    pub(super) fn unchanged(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(self.snapshot()?, *before);
        assert_eq!(self.controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.clocks.load(Ordering::SeqCst), 0);
        Ok(())
    }

    pub(super) async fn reopen(self) -> TestResult<(P, P::Store)> {
        let Self {
            broker,
            store,
            listener,
            provider,
            ..
        } = self;
        listener.abort();
        let _ = listener.await;
        drop(broker);
        drop(store);
        let reopened = provider.open()?;
        Ok((provider, reopened))
    }

    pub(super) async fn stop(self) {
        let _ = self.reopen().await.expect("node shutdown/reopen");
    }
}

pub(super) fn peek<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &str,
) -> TestResult<Vec<domain::Delivery>> {
    let outcome = StateMachine::new(store.clone()).apply(&Command::new(
        namespace.clone(),
        EntityPath::new(entity)?,
        Timestamp::from_millis(2_000),
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 100,
            session_id: None,
        },
    ))?;
    let CommandOutcome::Peeked(messages) = outcome else {
        return Err("expected Peek".into());
    };
    Ok(messages)
}

pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

pub(super) struct Peer {
    stream: Box<dyn Stream>,
    channels: HashMap<u16, u16>,
    handles: HashMap<u16, HashSet<u32>>,
    sent: HashMap<u16, u32>,
    deliveries: HashMap<u16, u32>,
}

impl Peer {
    pub(super) async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
        stream.set_nodelay(true)?;
        Self::open(Box::new(stream)).await
    }

    pub(super) async fn open(mut stream: Box<dyn Stream>) -> TestResult<Self> {
        timeout(
            DEADLINE,
            write_protocol_header(&mut stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        let mut peer = Self {
            stream,
            channels: HashMap::new(),
            handles: HashMap::new(),
            sent: HashMap::new(),
            deliveries: HashMap::new(),
        };
        let mut open = Open::new("native-atomic-raw-peer");
        open.channel_max = 3;
        open.hostname = Some("tenant.servicebus.windows.net".into());
        peer.send(0, Performative::Open(open), vec![]).await?;
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(peer)
    }

    pub(super) async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(&performative, Performative::Transfer(_)) {
            *self.sent.entry(channel).or_default() += 1;
        }
        timeout(
            DEADLINE,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    pub(super) async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(DEADLINE, read_frame(&mut self.stream)).await??)
    }

    pub(super) fn local(&self, channel: u16) -> u16 {
        self.channels[&channel]
    }

    fn flow(&self, frame: &Frame) -> bool {
        matches!(frame, Frame::Amqp { channel, performative: Some(Performative::Flow(flow)), payload }
            if payload.is_empty() && self.channels.values().any(|known| known == channel)
            && flow.handle.is_none_or(|handle| self.handles.get(channel).is_some_and(|known| known.contains(&handle))))
    }

    pub(super) async fn begin(&mut self, channel: u16) -> TestResult {
        let expected_local = (0_u16..=3)
            .find(|candidate| !self.channels.values().any(|known| known == candidate))
            .expect("a local test session channel is available");
        self.sent.insert(channel, 0);
        self.deliveries.insert(channel, 0);
        self.send(
            channel,
            Performative::Begin(Begin {
                handle_max: 0,
                ..Begin::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: local,
                performative: Some(Performative::Begin(begin)),
                payload,
            } = &frame
            {
                assert_eq!(begin.remote_channel, Some(channel));
                assert_eq!(*local, expected_local);
                assert!(payload.is_empty());
                self.channels.insert(channel, *local);
                self.handles.insert(*local, HashSet::new());
                return Ok(());
            }
            assert!(self.flow(&frame), "unexpected Begin: {frame:?}");
        }
        Err("bounded Begin missing".into())
    }

    pub(super) async fn attach(
        &mut self,
        channel: u16,
        handle: u32,
        target: &str,
        coordinator: bool,
        mode: ReceiverSettleMode,
    ) -> TestResult {
        self.send(
            channel,
            Performative::Attach(Box::new(Attach {
                name: format!("native-{channel}-{handle}"),
                handle,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: mode,
                source: coordinator.then(|| Source {
                    outcomes: Some(
                        vec![
                            Symbol::from("amqp:declared:list"),
                            Symbol::from("amqp:accepted:list"),
                            Symbol::from("amqp:rejected:list"),
                        ]
                        .into(),
                    ),
                    ..Source::default()
                }),
                target: Some(if coordinator {
                    Coordinator::default().into()
                } else {
                    Target::new(target).into()
                }),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            vec![],
        )
        .await
    }

    pub(super) async fn admitted(&mut self, channel: u16, coordinator: bool) -> TestResult {
        let mut attached = false;
        for _ in 0..32 {
            let frame = self.read().await?;
            match &frame {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Attach(attach)),
                    payload,
                } if *actual == self.local(channel) => {
                    assert_eq!(attach.handle, 0);
                    assert_eq!(attach.role, Role::Receiver);
                    assert!(payload.is_empty());
                    assert_eq!(
                        attach
                            .target
                            .as_ref()
                            .is_some_and(|t| t.as_coordinator().is_some()),
                        coordinator
                    );
                    self.handles
                        .entry(*actual)
                        .or_default()
                        .insert(attach.handle);
                    attached = true;
                }
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if *actual == self.local(channel) && flow.handle == Some(0) => {
                    assert!(attached);
                    assert!(payload.is_empty());
                    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
                    return Ok(());
                }
                _ => assert!(self.flow(&frame), "unexpected link admission: {frame:?}"),
            }
        }
        Err("bounded link admission missing".into())
    }

    pub(super) async fn setup(&mut self, mode: ReceiverSettleMode) -> TestResult {
        self.begin(CONTROL).await?;
        self.attach(CONTROL, CONTROL_HANDLE, "", true, mode.clone())
            .await?;
        self.admitted(CONTROL, true).await?;
        self.producer(POST, "orders", mode).await
    }

    pub(super) async fn producer(
        &mut self,
        channel: u16,
        target: &str,
        mode: ReceiverSettleMode,
    ) -> TestResult {
        self.begin(channel).await?;
        self.attach(channel, POST_HANDLE, target, false, mode)
            .await?;
        self.admitted(channel, false).await
    }

    pub(super) async fn transfer(
        &mut self,
        channel: u16,
        handle: u32,
        transaction: Option<&TransactionId>,
        format: u32,
        message: &Message,
    ) -> TestResult<u32> {
        let id = self.deliveries[&channel];
        self.deliveries.insert(channel, id + 1);
        self.send(
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id),
                delivery_tag: Some(vec![id as u8].into()),
                message_format: Some(format),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: transaction.map(|transaction| {
                    DeliveryState::Transactional(TransactionalState {
                        txn_id: transaction.clone(),
                        outcome: None,
                    })
                }),
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(message)?,
        )
        .await?;
        Ok(id)
    }

    pub(super) async fn disposition(&mut self, channel: u16, id: u32) -> TestResult<Disposition> {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Disposition(d)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, self.local(channel));
                assert!(payload.is_empty());
                assert_eq!(d.role, Role::Receiver);
                assert_eq!(d.first, id);
                assert!(d.last.is_none());
                return Ok(d.clone());
            }
            assert!(self.flow(&frame), "unexpected disposition: {frame:?}");
        }
        Err("bounded disposition missing".into())
    }

    pub(super) async fn sender_ack(&mut self, channel: u16, id: u32) -> TestResult {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: id,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            vec![],
        )
        .await?;
        self.barrier(channel).await
    }

    pub(super) async fn declare(&mut self, mode: ReceiverSettleMode) -> TestResult<TransactionId> {
        let message = control(TransactionCommand::Declare(Declare::default()));
        let id = self
            .transfer(CONTROL, CONTROL_HANDLE, None, 0, &message)
            .await?;
        let disposition = self.disposition(CONTROL, id).await?;
        assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
        let Some(DeliveryState::Declared(declared)) = disposition.state else {
            return Err("Declared missing".into());
        };
        if mode == ReceiverSettleMode::Second {
            self.sender_ack(CONTROL, id).await?;
        }
        Ok(declared.txn_id)
    }

    pub(super) async fn provisional(
        &mut self,
        channel: u16,
        id: u32,
        transaction: &TransactionId,
    ) -> TestResult {
        let disposition = self.disposition(channel, id).await?;
        assert!(!disposition.settled);
        assert!(
            matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState {
            txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == *transaction)
        );
        Ok(())
    }

    pub(super) async fn discharge(
        &mut self,
        transaction: &TransactionId,
        fail: bool,
    ) -> TestResult<u32> {
        self.transfer(
            CONTROL,
            CONTROL_HANDLE,
            None,
            0,
            &control(TransactionCommand::Discharge(Discharge {
                txn_id: transaction.clone(),
                fail: Some(fail),
            })),
        )
        .await
    }

    pub(super) async fn final_outcome(
        &mut self,
        channel: u16,
        id: u32,
        mode: ReceiverSettleMode,
        accepted: bool,
    ) -> TestResult {
        let disposition = self.disposition(channel, id).await?;
        assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
        if accepted {
            assert!(matches!(
                disposition.state,
                Some(DeliveryState::Accepted(_))
            ));
        } else {
            assert!(
                disposition.state.is_none(),
                "rollback post cleanup has no applied outcome"
            );
        }
        Ok(())
    }

    pub(super) async fn detached(
        &mut self,
        channel: u16,
        handle: u32,
        condition: &str,
    ) -> TestResult {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Attach(attach)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, self.local(channel));
                assert_eq!(attach.handle, 0);
                assert!(payload.is_empty());
                self.handles.entry(*actual).or_default().insert(0);
                continue;
            }
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Detach(detach)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, self.local(channel));
                assert_eq!(detach.handle, 0);
                assert!(detach.closed);
                assert!(payload.is_empty());
                let error = detach.error.as_ref().expect("scoped explicit error");
                assert_eq!(error.condition.as_symbol().as_str(), condition);
                assert!(
                    !error
                        .description
                        .as_deref()
                        .unwrap_or("")
                        .contains("DO-NOT-EXPOSE")
                );
                self.send(
                    channel,
                    Performative::Detach(amqp::Detach {
                        handle,
                        closed: true,
                        error: None,
                    }),
                    vec![],
                )
                .await?;
                return Ok(());
            }
            assert!(
                self.flow(&frame),
                "no accepted posting/control while refusing: {frame:?}"
            );
        }
        Err("bounded error Detach missing".into())
    }

    pub(super) async fn ended(&mut self, channel: u16, condition: &str) -> TestResult {
        for _ in 0..32 {
            let frame = self.read().await?;
            if let Frame::Amqp {
                channel: actual,
                performative: Some(Performative::End(end)),
                payload,
            } = &frame
            {
                assert_eq!(*actual, self.local(channel));
                assert!(payload.is_empty());
                assert_eq!(
                    end.error
                        .as_ref()
                        .expect("End error")
                        .condition
                        .as_symbol()
                        .as_str(),
                    condition
                );
                self.send(channel, Performative::End(End::default()), vec![])
                    .await?;
                let local = self
                    .channels
                    .remove(&channel)
                    .expect("ended session mapping");
                self.handles.remove(&local);
                return Ok(());
            }
            assert!(self.flow(&frame), "unexpected session refusal: {frame:?}");
        }
        Err("bounded End missing".into())
    }

    pub(super) async fn barrier(&mut self, channel: u16) -> TestResult {
        let sent = self.sent[&channel];
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: sent,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { channel: actual, performative: Some(Performative::Flow(flow)), payload }
                if *actual == self.local(channel) && flow.handle.is_none()
                    && flow.next_incoming_id == Some(sent) && payload.is_empty())
            {
                return Ok(());
            }
            assert!(
                self.flow(&frame),
                "barrier found forbidden early outcome: {frame:?}"
            );
        }
        Err("bounded Flow barrier missing".into())
    }

    pub(super) async fn healthy(&mut self) -> TestResult {
        self.producer(HEALTHY, "healthy", ReceiverSettleMode::First)
            .await?;
        let id = self
            .transfer(HEALTHY, POST_HANDLE, None, 0, &message("healthy", b"alive"))
            .await?;
        self.final_outcome(HEALTHY, id, ReceiverSettleMode::First, true)
            .await
    }

    pub(super) async fn close(mut self) -> TestResult {
        self.send(0, Performative::Close(Close { error: None }), vec![])
            .await?;
        for _ in 0..32 {
            let frame = self.read().await?;
            if matches!(&frame, Frame::Amqp { performative: Some(Performative::Close(close)), payload, .. }
                if close.error.is_none() && payload.is_empty())
            {
                return Ok(());
            }
            assert!(self.flow(&frame), "unexpected close response: {frame:?}");
        }
        Err("bounded Close missing".into())
    }
}

pub(super) fn control(command: TransactionCommand) -> Message {
    Message {
        body: Body::Value(Value::from(command)),
        ..Message::default()
    }
}

pub(super) fn message(id: &str, body: &[u8]) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some(id.into()),
            subject: Some("rich subject".into()),
            ..Properties::default()
        })
        .body(Body::Data(vec![body.to_vec().into()]))
        .build()
}

pub(super) fn batch(messages: &[Message]) -> TestResult<Message> {
    let sections = messages
        .iter()
        .map(encode_message)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message {
        body: Body::Data(sections.into_iter().map(Into::into).collect()),
        ..Message::default()
    })
}
