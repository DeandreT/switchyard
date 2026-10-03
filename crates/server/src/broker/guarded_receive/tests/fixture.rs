use super::*;

type Pause = (flume::Sender<()>, flume::Receiver<()>);

#[derive(Default)]
pub(super) struct Controls {
    pub(super) reads: AtomicUsize,
    pub(super) writes: AtomicUsize,
    pub(super) clocks: AtomicUsize,
    pub(super) now: AtomicU64,
    pub(super) fail_before: AtomicBool,
    pub(super) fail_after: AtomicBool,
    get_pause: Mutex<Option<Pause>>,
    apply_pause: Mutex<Option<Pause>>,
}

impl Controls {
    pub(super) fn reset(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.writes.store(0, Ordering::SeqCst);
        self.clocks.store(0, Ordering::SeqCst);
    }

    fn pause(slot: &Mutex<Option<Pause>>) -> (flume::Receiver<()>, Release) {
        let (entered, observed) = flume::bounded(1);
        let (resume, resumed) = flume::bounded(1);
        assert!(slot.lock().unwrap().replace((entered, resumed)).is_none());
        (observed, Release(Some(resume)))
    }

    pub(super) fn pause_apply(&self) -> (flume::Receiver<()>, Release) {
        Self::pause(&self.apply_pause)
    }
}

pub(super) struct Release(Option<flume::Sender<()>>);

impl Release {
    pub(super) fn release(mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

fn await_pause(slot: &Mutex<Option<Pause>>) -> Result<(), StorageError> {
    let pause = slot.lock().map_err(|_| StorageError::LockPoisoned)?.take();
    if let Some((entered, resumed)) = pause {
        entered.send(()).map_err(|_| injected())?;
        resumed.recv().map_err(|_| injected())?;
    }
    Ok(())
}

fn injected() -> StorageError {
    StorageError::Backend {
        operation: "private receive operation",
        detail: "private message body and backend path".to_owned(),
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
        await_pause(&self.controls.get_pause)?;
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.writes.fetch_add(1, Ordering::SeqCst);
        await_pause(&self.controls.apply_pause)?;
        if self.controls.fail_before.swap(false, Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.apply(batch)?;
        if self.controls.fail_after.swap(false, Ordering::SeqCst) {
            return Err(injected());
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
        self.0.clocks.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(self.0.now.load(Ordering::SeqCst))
    }
}

pub(super) struct Node<P: StoreProvider> {
    broker: Option<Broker>,
    pub(super) store: P::Store,
    pub(super) binding: EntityBinding,
    pub(super) controls: Arc<Controls>,
    _provider: P,
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
                config: QueueConfig {
                    default_time_to_live_millis: None,
                    dead_lettering_on_message_expiration: true,
                    ..QueueConfig::default()
                },
            },
        ))?;
        let binding = machine
            .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue)?
            .ok_or("queue binding missing")?;
        let controls = Arc::new(Controls::default());
        controls.now.store(2_000, Ordering::SeqCst);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(ControlledStore {
                inner: store.clone(),
                controls: controls.clone(),
            }),
            CountingClock(controls.clone()),
        ));
        Ok(Self {
            broker: Some(broker),
            store,
            binding,
            controls,
            _provider: provider,
        })
    }

    pub(super) fn handle(&self) -> BrokerHandle {
        self.broker.as_ref().expect("live test broker").handle()
    }

    pub(super) fn stop(&mut self) {
        drop(self.broker.take());
    }

    pub(super) fn submission(
        &self,
        mode: ReceiveMode,
        expiry: u64,
    ) -> (ReceiveClaimPermit, OwnedReceiveSubmission) {
        let (permit, ticket) = ReceiveClaimPermit::new(expiry);
        (
            permit,
            OwnedReceiveSubmission::new(
                self.binding.clone(),
                self.binding.target().clone(),
                mode,
                None,
                ticket,
            ),
        )
    }

    pub(super) fn seed(&self, id: &str, ttl: Option<u64>) -> TestResult<SequenceNumber> {
        let outcome = self.handle().submit_blocking(
            self.binding.namespace().clone(),
            self.binding.target().clone(),
            CommandKind::Send {
                message_id: id.to_owned(),
                body: id.as_bytes().to_vec(),
                time_to_live_millis: ttl,
                session_id: None,
            },
        )?;
        let CommandOutcome::Sent { sequence } = outcome else {
            return Err("seed send returned the wrong outcome".into());
        };
        Ok(sequence)
    }

    pub(super) fn record(&self, number: u64) -> TestResult<Option<MessageRecord>> {
        self.store
            .get(&keys::message(
                self.binding.namespace(),
                self.binding.target(),
                SequenceNumber::new(number),
            ))?
            .map(|bytes| MessageRecord::decode(&bytes).map_err(Into::into))
            .transpose()
    }

    pub(super) fn counters(&self) -> TestResult<QueueCounters> {
        Ok(codec::decode(
            &self
                .store
                .get(&keys::queue_counters(
                    self.binding.namespace(),
                    self.binding.target(),
                ))?
                .ok_or("queue counter missing")?,
        )?)
    }

    pub(super) async fn park_owner(
        &self,
    ) -> TestResult<(Release, flume::Receiver<Result<Timestamp, ProposeError>>)> {
        let (observed, release) = Controls::pause(&self.controls.get_pause);
        let (reply, response) = flume::bounded(1);
        self.handle()
            .requests
            .send(Request::LastApplied { reply })?;
        timeout(DEADLINE, observed.recv_async()).await??;
        Ok((release, response))
    }

    pub(super) async fn fence(&self) -> TestResult {
        let (reply, response) = flume::bounded(1);
        self.handle()
            .requests
            .send_async(Request::LastApplied { reply })
            .await?;
        timeout(DEADLINE, response.recv_async()).await???;
        Ok(())
    }

    pub(super) fn assert_no_work(&self, before: &StoreSnapshot) -> TestResult {
        assert_eq!(self.controls.reads.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.controls.clocks.load(Ordering::SeqCst), 0);
        assert_eq!(&self.store.snapshot()?, before);
        Ok(())
    }
}
