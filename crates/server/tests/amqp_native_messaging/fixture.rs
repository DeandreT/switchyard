use super::*;

type Pause = (flume::Sender<AtomicCommitPermit>, flume::Receiver<()>);

#[derive(Default)]
pub(super) struct Controls {
    pub(super) reads: AtomicUsize,
    pub(super) writes: AtomicUsize,
    pub(super) clocks: AtomicUsize,
    pub(super) binds: AtomicUsize,
    pub(super) handoffs: AtomicUsize,
    pub(super) completed: AtomicUsize,
    pub(super) cancelled: AtomicUsize,
    pub(super) receive_starts: AtomicUsize,
    pub(super) receive_applied: AtomicUsize,
    pub(super) now: AtomicU64,
    pub(super) fail_before: AtomicBool,
    pub(super) fail_after: AtomicBool,
    held: Mutex<Option<domain::Delivery>>,
    permits: Mutex<Vec<AtomicCommitPermit>>,
    pause: Mutex<Option<Pause>>,
    changed: Notify,
}

impl Controls {
    pub(super) fn reset_io(&self) {
        for counter in [
            &self.reads,
            &self.writes,
            &self.clocks,
            &self.binds,
            &self.handoffs,
            &self.completed,
            &self.cancelled,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
        self.permits.lock().expect("permit observers").clear();
    }

    pub(super) fn reset_receives(&self) {
        self.receive_starts.store(0, Ordering::SeqCst);
        self.receive_applied.store(0, Ordering::SeqCst);
        self.held.lock().expect("canonical held delivery").take();
    }

    pub(super) fn held(&self) -> domain::Delivery {
        self.held
            .lock()
            .expect("canonical held delivery")
            .clone()
            .expect("actual broker Receive completed")
    }

    pub(super) fn states(&self) -> Vec<AtomicCommitState> {
        self.permits
            .lock()
            .expect("permit observers")
            .iter()
            .map(AtomicCommitPermit::state)
            .collect()
    }

    pub(super) fn pause_handoff(&self) -> (flume::Receiver<AtomicCommitPermit>, Release) {
        let (entered, observed) = flume::bounded(1);
        let (resume, resumed) = flume::bounded(1);
        assert!(
            self.pause
                .lock()
                .expect("handoff pause")
                .replace((entered, resumed))
                .is_none()
        );
        (observed, Release(Some(resume)))
    }

    pub(super) async fn wait_receive_starts(&self, expected: usize) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.receive_starts.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await?;
        assert_eq!(self.receive_starts.load(Ordering::SeqCst), expected);
        Ok(())
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
        assert_eq!(self.completed.load(Ordering::SeqCst), expected);
        Ok(())
    }

    pub(super) async fn wait_cancelled(&self) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.cancelled.load(Ordering::SeqCst) != 0 {
                    return;
                }
                changed.await;
            }
        })
        .await?;
        assert_eq!(self.cancelled.load(Ordering::SeqCst), 1);
        Ok(())
    }
}

struct CallGuard {
    controls: Arc<Controls>,
    armed: bool,
}

impl CallGuard {
    fn complete(&mut self) {
        self.armed = false;
    }
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.armed {
            self.controls.cancelled.fetch_add(1, Ordering::SeqCst);
            self.controls.changed.notify_waiters();
        }
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

#[derive(Clone)]
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
        operation: "mixed native commit",
        detail: "DO-NOT-EXPOSE-mixed-store-detail".into(),
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
        let inner = self.inner.clone();
        let controls = self.controls.clone();
        let receive = entity.as_str() == "orders" && matches!(kind, CommandKind::Receive { .. });
        async move {
            if receive {
                let ordinal = controls.receive_starts.fetch_add(1, Ordering::SeqCst);
                controls.changed.notify_waiters();
                if ordinal != 0 {
                    // Observe fetch-next without allowing a second acquisition to obscure the commit.
                    std::future::pending::<()>().await;
                }
            }
            let result = protocol_amqp::Broker::submit_fenced(&inner, binding, entity, kind).await;
            if receive {
                controls.receive_applied.fetch_add(1, Ordering::SeqCst);
                if let Ok(CommandOutcome::Received(Some(delivery))) = &result {
                    assert!(
                        controls
                            .held
                            .lock()
                            .expect("canonical held delivery")
                            .replace(delivery.clone())
                            .is_none()
                    );
                }
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
        let pause = self.controls.pause.lock().expect("handoff pause").take();
        let pending =
            NativeAtomicBroker::submit_native_atomic_messaging_owned(&self.inner, submission);
        let controls = self.controls.clone();
        let mut guard = CallGuard {
            controls: controls.clone(),
            armed: true,
        };
        async move {
            if let Some((entered, resumed)) = pause {
                let _ = entered.send(permit);
                let _ = resumed.recv_async().await;
            }
            let result = pending.await;
            guard.complete();
            controls.completed.fetch_add(1, Ordering::SeqCst);
            controls.changed.notify_waiters();
            result
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ListenerMode {
    Messaging,
    Posting,
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
    pub(super) async fn start(provider: P, mode: ListenerMode) -> TestResult<Self> {
        Self::start_with_security(provider, mode, None).await
    }

    pub(super) async fn start_with_security(
        provider: P,
        mode: ListenerMode,
        security: Option<(
            rustls::ServerConfig,
            protocol_amqp::SharedAccessAuthentication,
        )>,
    ) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let machine = StateMachine::new(store.clone());
        for entity in ["orders", "other", "healthy", "session"] {
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
        let orders = EntityPath::new("orders")?;
        machine.apply(&Command::new(
            namespace.clone(),
            orders.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::Send {
                message_id: "consumed-prefix".into(),
                body: vec![],
                time_to_live_millis: None,
                session_id: None,
            },
        ))?;
        machine.apply(&Command::new(
            namespace.clone(),
            orders.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        ))?;
        machine.apply(&Command::new(
            namespace.clone(),
            orders,
            Timestamp::from_millis(1_000),
            CommandKind::Send {
                message_id: "held-original".into(),
                body: b"held-body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        ))?;
        let topic = EntityPath::new("topic")?;
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?;
        machine.apply(&Command::new(
            namespace.clone(),
            topic,
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
            match mode {
                ListenerMode::Messaging => {
                    let _ = service.serve_atomic_messaging_ingress(socket).await;
                }
                ListenerMode::Posting => {
                    let _ = service.serve_atomic_posting_ingress(socket).await;
                }
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

    pub(super) async fn expire_held(&self) -> TestResult {
        let deadline = self
            .controls
            .held()
            .lock
            .expect("old canonical lock")
            .locked_until;
        self.controls
            .now
            .store(deadline.as_millis() + 1, Ordering::SeqCst);
        let outcome = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                EntityPath::new("orders")?,
                CommandKind::ExpireLocks,
            )
            .await?;
        assert_eq!(
            outcome,
            CommandOutcome::LocksExpired {
                returned_to_ready: 1,
                dead_lettered: 0,
                dropped: 0,
            }
        );
        self.controls.reset_receives();
        Ok(())
    }

    pub(super) async fn additional_listener(
        &self,
        mode: ListenerMode,
    ) -> TestResult<(std::net::SocketAddr, JoinHandle<()>)> {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let delegate = DelegatingBroker {
            inner: self.broker.handle(),
            controls: self.controls.clone(),
        };
        let namespace = self.namespace.clone();
        let task = tokio::spawn(async move {
            let service =
                protocol_amqp::AmqpListener::new(delegate, namespace).with_idle_timeout_millis(0);
            match mode {
                ListenerMode::Messaging => {
                    let _ = service.serve_atomic_messaging_ingress(socket).await;
                }
                ListenerMode::Posting => {
                    let _ = service.serve_atomic_posting_ingress(socket).await;
                }
            }
        });
        Ok((address, task))
    }

    pub(super) fn record(&self) -> TestResult<Option<domain::MessageRecord>> {
        Ok(StateMachine::new(self.store.clone()).message(
            &self.namespace,
            &EntityPath::new("orders")?,
            self.controls.held().sequence,
        )?)
    }

    pub(super) fn messages(&self) -> TestResult<Vec<domain::Delivery>> {
        peek(&self.store, &self.namespace)
    }

    pub(super) fn unchanged(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(self.snapshot()?, *before);
        assert_eq!(self.controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.clocks.load(Ordering::SeqCst), 0);
        Ok(())
    }

    pub(super) async fn reopen(self) -> TestResult<(P, P::Store, NamespaceName)> {
        let Self {
            broker,
            store,
            listener,
            provider,
            namespace,
            ..
        } = self;
        listener.abort();
        let _ = listener.await;
        drop(broker);
        drop(store);
        let reopened = provider.open()?;
        Ok((provider, reopened, namespace))
    }

    pub(super) async fn stop(self) {
        self.reopen().await.expect("node shutdown/reopen");
    }
}

pub(super) fn peek<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
) -> TestResult<Vec<domain::Delivery>> {
    let machine = StateMachine::new(store.clone());
    let outcome = machine.apply(&Command::new(
        namespace.clone(),
        EntityPath::new("orders")?,
        machine.last_applied_time()?,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 100,
            session_id: None,
        },
    ))?;
    let CommandOutcome::Peeked(deliveries) = outcome else {
        return Err("expected Peek".into());
    };
    Ok(deliveries)
}
