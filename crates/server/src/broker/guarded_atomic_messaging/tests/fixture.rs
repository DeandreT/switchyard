use super::*;

type PauseChannels = (flume::Sender<()>, flume::Receiver<()>);
type GuardedResponse =
    flume::Receiver<Result<AtomicMessagingApplication, GuardedAtomicSubmitError>>;
type ParkedResponse = flume::Receiver<Result<Timestamp, ProposeError>>;

#[derive(Default)]
pub(super) struct Controls {
    pub(super) reads: AtomicUsize,
    pub(super) writes: AtomicUsize,
    pub(super) clock_calls: AtomicUsize,
    pub(super) clock_millis: AtomicU64,
    pub(super) fail_read: AtomicBool,
    pub(super) panic_read: AtomicBool,
    pub(super) fail_before_commit: AtomicBool,
    pub(super) fail_after_commit: AtomicBool,
    pause_read: Mutex<Option<PauseChannels>>,
    pause_commit: Mutex<Option<PauseChannels>>,
}

impl Controls {
    pub(super) fn reset_counts(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.writes.store(0, Ordering::SeqCst);
        self.clock_calls.store(0, Ordering::SeqCst);
    }

    fn pause(slot: &Mutex<Option<PauseChannels>>) -> (flume::Receiver<()>, ReleasePause) {
        let (entered, observed) = flume::bounded(1);
        let (resume, resumed) = flume::bounded(1);
        assert!(slot.lock().unwrap().replace((entered, resumed)).is_none());
        (observed, ReleasePause(Some(resume)))
    }

    pub(super) fn pause_commit(&self) -> (flume::Receiver<()>, ReleasePause) {
        Self::pause(&self.pause_commit)
    }
}

pub(super) struct ReleasePause(Option<flume::Sender<()>>);

impl ReleasePause {
    pub(super) fn release(mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

impl Drop for ReleasePause {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

fn await_pause(slot: &Mutex<Option<PauseChannels>>) -> Result<(), StorageError> {
    let pause = slot.lock().map_err(|_| StorageError::LockPoisoned)?.take();
    if let Some((entered, resumed)) = pause {
        entered.send(()).map_err(|_| injected("pause observer"))?;
        resumed.recv().map_err(|_| injected("pause release"))?;
    }
    Ok(())
}

fn injected(operation: &'static str) -> StorageError {
    StorageError::Backend {
        operation,
        detail: "controlled test failure".to_owned(),
    }
}

#[derive(Clone)]
struct ControlledStore<S> {
    inner: S,
    controls: Arc<Controls>,
}

impl<S: StateStore> StateStore for ControlledStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
        await_pause(&self.controls.pause_read)?;
        assert!(
            !self.controls.panic_read.swap(false, Ordering::SeqCst),
            "controlled owner read panic"
        );
        if self.controls.fail_read.swap(false, Ordering::SeqCst) {
            return Err(injected("read"));
        }
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.writes.fetch_add(1, Ordering::SeqCst);
        await_pause(&self.controls.pause_commit)?;
        if self
            .controls
            .fail_before_commit
            .swap(false, Ordering::SeqCst)
        {
            return Err(injected("before commit"));
        }
        self.inner.apply(batch)?;
        if self
            .controls
            .fail_after_commit
            .swap(false, Ordering::SeqCst)
        {
            return Err(injected("after commit"));
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
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}

#[derive(Clone)]
struct CountingClock(Arc<Controls>);

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.0.clock_calls.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(self.0.clock_millis.load(Ordering::SeqCst))
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) broker: Broker,
    pub(super) store: P::Store,
    pub(super) binding: EntityBinding,
    pub(super) controls: Arc<Controls>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) fn new(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let machine = StateMachine::new(store.clone());
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))?;
        let binding = machine
            .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue)?
            .ok_or("queue binding missing")?;
        let controls = Arc::new(Controls::default());
        controls.clock_millis.store(2_000, Ordering::SeqCst);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(ControlledStore {
                inner: store.clone(),
                controls: Arc::clone(&controls),
            }),
            CountingClock(Arc::clone(&controls)),
        ));
        Ok(Self {
            broker,
            store,
            binding,
            controls,
            provider,
        })
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    pub(super) fn enqueue(
        &self,
        ticket: AtomicCommitTicket,
        kinds: Vec<CommandKind>,
    ) -> TestResult<GuardedResponse> {
        let (reply, result) = flume::bounded(1);
        self.broker
            .handle()
            .requests
            .send(Request::ApplyAtomicMessagingGuarded {
                binding: self.binding.clone(),
                kinds,
                ticket,
                reply,
            })?;
        Ok(result)
    }

    pub(super) async fn park_owner(&self) -> TestResult<(ReleasePause, ParkedResponse)> {
        let (entered, release) = Controls::pause(&self.controls.pause_read);
        let (reply, response) = flume::bounded(1);
        self.broker
            .handle()
            .requests
            .send(Request::LastApplied { reply })?;
        timeout(DEADLINE, entered.recv_async()).await??;
        Ok((release, response))
    }

    pub(super) async fn fence(&self) -> TestResult {
        let (reply, response) = flume::bounded(1);
        self.broker
            .handle()
            .requests
            .send_async(Request::LastApplied { reply })
            .await?;
        timeout(DEADLINE, response.recv_async()).await???;
        Ok(())
    }

    pub(super) fn assert_no_work(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(self.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.clock_calls.load(Ordering::SeqCst), 0);
        assert_eq!(&self.snapshot()?, before);
        Ok(())
    }

    pub(super) fn messages(&self) -> TestResult<Vec<domain::Delivery>> {
        let machine = StateMachine::new(self.store.clone());
        let CommandOutcome::Peeked(messages) = machine.apply(&Command::new(
            self.binding.namespace().clone(),
            self.binding.target().clone(),
            Timestamp::from_millis(2_000),
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 10,
                session_id: None,
            },
        ))?
        else {
            return Err("peek response missing".into());
        };
        Ok(messages)
    }

    pub(super) fn reopen(self) -> TestResult<(P, P::Store)> {
        let Self {
            broker,
            store,
            provider,
            ..
        } = self;
        drop(broker);
        drop(store);
        let reopened = provider.open()?;
        Ok((provider, reopened))
    }
}

pub(super) fn permit() -> (AtomicCommitPermit, AtomicCommitTicket) {
    AtomicCommitPermit::new(Instant::now() + Duration::from_secs(60))
}

pub(super) fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}
