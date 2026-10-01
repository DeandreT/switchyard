use super::*;

type CommitPause = (flume::Sender<()>, flume::Receiver<()>);

#[derive(Clone)]
struct ControlledStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    error_after_commit: Arc<AtomicBool>,
    pause_next: Arc<Mutex<Option<CommitPause>>>,
    commits: Arc<AtomicUsize>,
}

impl<S: StateStore> StateStore for ControlledStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::Relaxed);
        let pause = self
            .pause_next
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .take();
        if let Some((started, resume)) = pause {
            started.send(()).map_err(|_| StorageError::Backend {
                operation: "pause",
                detail: "test observer closed".to_owned(),
            })?;
            resume.recv().map_err(|_| StorageError::Backend {
                operation: "resume",
                detail: "test controller closed".to_owned(),
            })?;
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected failure".to_owned(),
            });
        }
        self.inner.apply(batch)?;
        if self.error_after_commit.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "report commit",
                detail: "injected unknown commit decision".to_owned(),
            });
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub broker: Broker,
    pub store: P::Store,
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub binding: EntityBinding,
    pub clock: ManualClock,
    pub fail_next: Arc<AtomicBool>,
    pub error_after_commit: Arc<AtomicBool>,
    pub commits: Arc<AtomicUsize>,
    pause_next: Arc<Mutex<Option<CommitPause>>>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub async fn new(provider: P) -> TestResult<Self> {
        Self::with_config(provider, QueueConfig::default()).await
    }

    pub async fn with_config(provider: P, config: QueueConfig) -> TestResult<Self> {
        let store = provider.open()?;
        let fail_next = Arc::new(AtomicBool::new(false));
        let error_after_commit = Arc::new(AtomicBool::new(false));
        let pause_next = Arc::new(Mutex::new(None));
        let commits = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(ControlledStore {
                inner: store.clone(),
                fail_next: fail_next.clone(),
                error_after_commit: error_after_commit.clone(),
                pause_next: pause_next.clone(),
                commits: commits.clone(),
            }),
            clock.clone(),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        for path in [entity.clone(), EntityPath::new("neighbor")?] {
            broker
                .handle()
                .submit(namespace.clone(), path, CommandKind::CreateQueue { config })
                .await?;
        }
        let binding = broker
            .handle()
            .bind(namespace.clone(), Attachment::Queue(entity.clone()))
            .await?
            .ok_or("queue admission missing")?
            .binding;
        Ok(Self {
            broker,
            store,
            namespace,
            entity,
            binding,
            clock,
            fail_next,
            error_after_commit,
            pause_next,
            commits,
            _provider: provider,
        })
    }

    pub async fn apply(
        &self,
        kinds: Vec<CommandKind>,
    ) -> Result<AtomicMessagingApplication, SubmitError> {
        self.broker
            .handle()
            .submit_atomic_messaging(self.binding.clone(), kinds)
            .await
    }

    pub async fn seed_held(&self) -> TestResult<Delivery> {
        self.broker
            .handle()
            .submit(self.namespace.clone(), self.entity.clone(), send("held"))
            .await?;
        let CommandOutcome::Received(Some(delivery)) = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                self.entity.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .await?
        else {
            return Err("held delivery missing".into());
        };
        Ok(delivery)
    }

    pub async fn peek(&self, entity: EntityPath) -> TestResult<Vec<Delivery>> {
        let CommandOutcome::Peeked(messages) = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                entity,
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(0),
                    max_messages: 100,
                    session_id: None,
                },
            )
            .await?
        else {
            return Err("peek outcome missing".into());
        };
        Ok(messages)
    }

    pub fn pause_commit(&self) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (started, observation) = flume::bounded(1);
        let (resume, permission) = flume::bounded(1);
        *self.pause_next.lock().unwrap() = Some((started, permission));
        (observation, resume)
    }
}

pub(super) fn complete(delivery: &Delivery) -> CommandKind {
    CommandKind::Complete {
        sequence: delivery.sequence,
        lock_token: delivery.lock.unwrap().token,
    }
}
