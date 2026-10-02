use super::*;

#[derive(Default)]
pub(super) struct Controls {
    now: AtomicU64,
    pub(super) receives: AtomicUsize,
    pub(super) completed: AtomicUsize,
    pub(super) writes: AtomicUsize,
    pub(super) clocks: AtomicUsize,
    deliveries: Mutex<Vec<domain::Delivery>>,
    response_gate: Mutex<Option<Arc<ReceiveResponseGateState>>>,
    changed: Notify,
}

impl Controls {
    pub(super) fn pause_receive_response(&self, ordinal: usize) -> ReceiveResponseGate {
        let state = Arc::new(ReceiveResponseGateState {
            ordinal,
            reached: AtomicBool::new(false),
            released: AtomicBool::new(false),
            changed: Notify::new(),
            release: Notify::new(),
        });
        let mut gate = self.response_gate.lock().expect("receive response gate");
        assert!(gate.is_none(), "the receive response gate is one-shot");
        *gate = Some(Arc::clone(&state));
        ReceiveResponseGate(state)
    }

    pub(super) fn advance_to(&self, millis: u64) {
        let previous = self.now.swap(millis, Ordering::SeqCst);
        assert!(millis >= previous, "the test clock only advances");
    }

    pub(super) fn reset(&self) {
        self.receives.store(0, Ordering::SeqCst);
        self.completed.store(0, Ordering::SeqCst);
        self.writes.store(0, Ordering::SeqCst);
        self.clocks.store(0, Ordering::SeqCst);
        self.deliveries.lock().expect("observed deliveries").clear();
    }

    pub(super) async fn wait_completed(&self, expected: usize) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.completed.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await?;
        assert_eq!(self.receives.load(Ordering::SeqCst), expected);
        assert_eq!(self.completed.load(Ordering::SeqCst), expected);
        Ok(())
    }

    pub(super) fn delivery(&self, index: usize) -> domain::Delivery {
        self.deliveries.lock().expect("observed deliveries")[index].clone()
    }
}

struct ReceiveResponseGateState {
    ordinal: usize,
    reached: AtomicBool,
    released: AtomicBool,
    changed: Notify,
    release: Notify,
}

pub(super) struct ReceiveResponseGate(Arc<ReceiveResponseGateState>);

impl ReceiveResponseGate {
    pub(super) async fn wait(&self) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.reached.load(Ordering::SeqCst) {
                    return;
                }
                changed.await;
            }
        })
        .await?;
        Ok(())
    }

    pub(super) fn release(&self) {
        self.0.released.store(true, Ordering::SeqCst);
        self.0.release.notify_waiters();
    }
}

impl Drop for ReceiveResponseGate {
    fn drop(&mut self) {
        self.release();
    }
}

impl ReceiveResponseGateState {
    async fn park(&self) {
        assert!(!self.reached.swap(true, Ordering::SeqCst));
        self.changed.notify_waiters();
        loop {
            let release = self.release.notified();
            tokio::pin!(release);
            release.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            release.await;
        }
    }
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    controls: Arc<Controls>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
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
struct ObservedBroker {
    inner: BrokerHandle,
    controls: Arc<Controls>,
}

impl protocol_amqp::Broker for ObservedBroker {
    fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityAdmission>, BrokerRejection>> + Send {
        protocol_amqp::Broker::bind(&self.inner, namespace, target)
    }

    fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        let inner = self.inner.clone();
        let controls = self.controls.clone();
        let receive = matches!(kind, CommandKind::Receive { .. });
        async move {
            let ordinal = receive.then(|| controls.receives.fetch_add(1, Ordering::SeqCst) + 1);
            let result = protocol_amqp::Broker::submit_fenced(&inner, binding, entity, kind).await;
            if receive {
                if let Ok(CommandOutcome::Received(Some(delivery))) = &result {
                    controls
                        .deliveries
                        .lock()
                        .expect("observed deliveries")
                        .push(delivery.clone());
                }
                let gate = controls
                    .response_gate
                    .lock()
                    .expect("receive response gate")
                    .as_ref()
                    .filter(|gate| Some(gate.ordinal) == ordinal)
                    .cloned();
                if let Some(gate) = gate {
                    // The real broker reply and lock exist before this test-only
                    // pause; no owner/store mutex is held across the wait.
                    assert!(matches!(&result, Ok(CommandOutcome::Received(Some(_)))));
                    gate.park().await;
                }
                controls.completed.fetch_add(1, Ordering::SeqCst);
                controls.changed.notify_waiters();
            }
            result
        }
    }

    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind)
    }
    fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription)
    }
    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription)
    }
    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) broker: Broker,
    pub(super) store: P::Store,
    pub(super) namespace: NamespaceName,
    pub(super) controls: Arc<Controls>,
    pub(super) address: std::net::SocketAddr,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        Self::start_secure(provider, None).await
    }

    pub(super) async fn start_secure(
        provider: P,
        security: Option<(
            rustls::ServerConfig,
            protocol_amqp::SharedAccessAuthentication,
        )>,
    ) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let controls = Arc::new(Controls::default());
        controls.advance_to(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(ObservedStore {
                inner: store.clone(),
                controls: controls.clone(),
            }),
            CountingClock(controls.clone()),
        ));
        for entity in ["orders", "empty"] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(entity)?,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("topic")?,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("topic")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Alpha")?,
                config: SubscriptionConfig::default(),
            },
        )?;
        let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
        let address = socket.local_addr()?;
        let delegate = ObservedBroker {
            inner: broker.handle(),
            controls: controls.clone(),
        };
        let scope = namespace.clone();
        let listener = tokio::spawn(async move {
            let mut listener =
                protocol_amqp::AmqpListener::new(delegate, scope).with_idle_timeout_millis(0);
            if let Some((tls, authentication)) = security {
                listener = listener
                    .with_tls(tls)
                    .with_shared_access_authentication(authentication);
            }
            let _ = listener.serve(socket).await;
        });
        controls.reset();
        Ok(Self {
            broker,
            store,
            namespace,
            controls,
            address,
            listener,
            _provider: provider,
        })
    }

    pub(super) async fn seed(&self, entity: &str, id: &str) -> TestResult {
        timeout(
            DEADLINE,
            self.broker.handle().submit(
                self.namespace.clone(),
                EntityPath::new(entity)?,
                CommandKind::Send {
                    message_id: id.into(),
                    body: id.as_bytes().to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            ),
        )
        .await??;
        Ok(())
    }

    pub(super) async fn fence(&self) -> TestResult {
        let config = timeout(
            DEADLINE,
            self.broker
                .handle()
                .queue_config(self.namespace.clone(), EntityPath::new("orders")?),
        )
        .await??;
        assert!(config.is_some());
        Ok(())
    }

    pub(super) async fn expire_locks(&self, returned: u32) -> TestResult {
        let deadline = (0..returned as usize)
            .map(|index| {
                self.controls
                    .delivery(index)
                    .lock
                    .expect("held lock")
                    .locked_until
                    .as_millis()
            })
            .max()
            .ok_or("at least one held lock is required")?;
        self.controls
            .advance_to(deadline.checked_add(1).ok_or("lock deadline overflow")?);
        let outcome = timeout(
            DEADLINE,
            self.broker.handle().submit(
                self.namespace.clone(),
                EntityPath::new("orders")?,
                CommandKind::ExpireLocks,
            ),
        )
        .await??;
        assert_eq!(
            outcome,
            CommandOutcome::LocksExpired {
                returned_to_ready: returned,
                dead_lettered: 0,
                dropped: 0,
            }
        );
        Ok(())
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    pub(super) fn record(
        &self,
        entity: &str,
        sequence: u64,
    ) -> TestResult<Option<domain::MessageRecord>> {
        Ok(StateMachine::new(self.store.clone()).message(
            &self.namespace,
            &EntityPath::new(entity)?,
            SequenceNumber::new(sequence),
        )?)
    }

    pub(super) fn ready(&self, entity: &str, sequence: u64, id: &str) -> TestResult {
        let record = self
            .record(entity, sequence)?
            .ok_or("ready original is missing")?;
        assert_eq!(record.message_id, id);
        assert_eq!(record.body, id.as_bytes());
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.delivery_count, 0);
        Ok(())
    }

    pub(super) fn inert(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(self.controls.receives.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.completed.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.clocks.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(&self.snapshot()?, before);
        Ok(())
    }

    pub(super) async fn stop(self) {
        self.listener.abort();
        let _ = self.listener.await;
        drop(self.broker);
    }
}
