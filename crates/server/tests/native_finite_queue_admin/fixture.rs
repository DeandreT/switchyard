use std::{
    any::Any,
    fmt,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures_util::FutureExt;
use storage::{Key, StorageError, Value};

use super::*;

pub(super) type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn Any + Send>>;

#[derive(Default)]
pub(super) struct Observations {
    pub reads: AtomicUsize,
    pub scans: AtomicUsize,
    pub attempts: AtomicUsize,
    pub commits: AtomicUsize,
    pub fail_next: AtomicBool,
    pub batches: Mutex<Vec<WriteBatch>>,
    guarded: AtomicBool,
    applied: AtomicBool,
    gate: Mutex<Option<(Key, Arc<ReadGate>)>>,
}

#[derive(Clone)]
pub(super) struct ObservedStore<S> {
    pub inner: Arc<S>,
    pub observations: Arc<Observations>,
}

impl<S: StateStore> ObservedStore<S> {
    fn check_read(&self) -> Result<(), StorageError> {
        if self.observations.guarded.load(Ordering::SeqCst)
            && self.observations.applied.load(Ordering::SeqCst)
        {
            return Err(StorageError::Backend {
                operation: "read after commit",
                detail: "finite response performed a postcommit read".into(),
            });
        }
        Ok(())
    }

    pub fn guard(&self) -> NoPostRead {
        self.observations.applied.store(false, Ordering::SeqCst);
        assert!(!self.observations.guarded.swap(true, Ordering::SeqCst));
        NoPostRead(self.observations.clone())
    }
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.check_read()?;
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        let value = self.inner.get(key);
        let gate = {
            let mut gate = self.observations.gate.lock().expect("read gate");
            if gate.as_ref().is_some_and(|(selected, _)| selected == key) {
                gate.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::SeqCst);
            gate.notification.notify_waiters();
            let mut released = gate.released.lock().expect("released gate");
            while !*released {
                released = gate.changed.wait(released).expect("released gate");
            }
        }
        value
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.check_read()?;
        self.observations.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.check_read()?;
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.attempts.fetch_add(1, Ordering::SeqCst);
        if self.observations.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "private-finite-store-detail".into(),
            });
        }
        self.inner.apply(batch.clone())?;
        self.observations.commits.fetch_add(1, Ordering::SeqCst);
        self.observations
            .batches
            .lock()
            .expect("batches")
            .push(batch);
        self.observations.applied.store(true, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) struct NoPostRead(Arc<Observations>);

impl Drop for NoPostRead {
    fn drop(&mut self) {
        self.0.guarded.store(false, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub(super) struct CountingClock {
    pub manual: ManualClock,
    calls: Arc<AtomicUsize>,
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.manual.now()
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub broker: Option<Broker>,
    pub service: NativeAdminService,
    pub store: ObservedStore<P::Store>,
    pub clock: CountingClock,
    provider: P,
}

pub(super) struct Checkpoint {
    pub image: StoreSnapshot,
    pub reads: usize,
    pub scans: usize,
    pub attempts: usize,
    pub commits: usize,
    pub clocks: usize,
}

impl<P: StoreProvider> Node<P> {
    pub fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: Arc::new(provider.open()?),
            observations: Arc::default(),
        };
        let clock = CountingClock {
            manual: ManualClock::at(1_000),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), namespace()?);
        Ok(Self {
            broker: Some(broker),
            service,
            store,
            clock,
            provider,
        })
    }

    pub fn checkpoint(&self) -> TestResult<Checkpoint> {
        let observations = &self.store.observations;
        Ok(Checkpoint {
            image: self.store.inner.snapshot()?,
            reads: observations.reads.load(Ordering::SeqCst),
            scans: observations.scans.load(Ordering::SeqCst),
            attempts: observations.attempts.load(Ordering::SeqCst),
            commits: observations.commits.load(Ordering::SeqCst),
            clocks: self.clock.calls.load(Ordering::SeqCst),
        })
    }

    pub fn unchanged(&self, before: &Checkpoint, clock_delta: usize) -> TestResult {
        let after = self.checkpoint()?;
        assert_eq!(after.image, before.image);
        assert_eq!(after.attempts, before.attempts);
        assert_eq!(after.commits, before.commits);
        assert_eq!(after.clocks, before.clocks + clock_delta);
        Ok(())
    }

    pub fn untouched(&self, before: &Checkpoint) -> TestResult {
        self.unchanged(before, 0)?;
        let after = self.checkpoint()?;
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.scans, before.scans);
        Ok(())
    }

    pub async fn create(
        &self,
        input: CreateFiniteQueueRequest,
    ) -> Result<FiniteQueue, tonic::Status> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.service.create_finite_queue(Request::new(input)),
        )
        .await
        .expect("bounded finite creation")?
        .into_inner())
    }

    pub async fn get(&self, input: GetFiniteQueueRequest) -> Result<FiniteQueue, tonic::Status> {
        Ok(
            tokio::time::timeout(DEADLINE, self.service.get_finite_queue(Request::new(input)))
                .await
                .expect("bounded finite description")?
                .into_inner(),
        )
    }

    pub async fn update(
        &self,
        input: SetFiniteQueueDefinitionRequest,
    ) -> Result<FiniteQueue, tonic::Status> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.service
                .set_finite_queue_definition(Request::new(input)),
        )
        .await
        .expect("bounded finite definition")?
        .into_inner())
    }

    pub async fn submit(&self, path: &str, command: CommandKind) -> TestResult<CommandOutcome> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.broker
                .as_ref()
                .expect("original owner")
                .handle()
                .submit(namespace()?, EntityPath::new(path)?, command),
        )
        .await??)
    }

    pub fn gate(&self, key: Key) -> GateRelease {
        let gate = Arc::new(ReadGate::default());
        assert!(
            self.store
                .observations
                .gate
                .lock()
                .expect("read gate")
                .replace((key, gate.clone()))
                .is_none()
        );
        GateRelease(gate)
    }

    fn release(self) -> TestResult<P> {
        let Self {
            broker,
            service,
            store,
            clock: _,
            provider,
        } = self;
        drop(service);
        // This synchronous original-owner join has no exposed result or deadline.
        drop(broker);
        let ObservedStore {
            inner,
            observations: _,
        } = store;
        let backend =
            Arc::try_unwrap(inner).map_err(|_| "original counted backend is still held")?;
        drop(backend);
        Ok(provider)
    }

    pub fn finish(self, observed: Observed) -> TestResult {
        self.finish_with_cleanup(observed, Ok(()))
    }

    pub fn finish_with_cleanup(self, observed: Observed, listener: TestResult) -> TestResult {
        let owner = self.release().map(drop);
        let cleanup = match (listener, owner) {
            (Ok(()), Ok(())) => Ok(()),
            (listener, owner) => Err(Box::new(FixtureFailure {
                primary: listener.err(),
                cleanup: owner.err(),
            }) as Box<dyn Error>),
        };
        settle(observed, cleanup)
    }

    pub fn reopen(self, observed: Observed) -> TestResult<Self> {
        let released = self.release();
        match released {
            Ok(provider) => {
                settle(observed, Ok(()))?;
                Self::start(provider)
            }
            Err(error) => {
                settle(observed, Err(error))?;
                unreachable!("failed cleanup cannot become a reopened owner")
            }
        }
    }
}

#[derive(Default)]
struct ReadGate {
    entered: AtomicBool,
    notification: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
}

pub(super) struct GateRelease(Arc<ReadGate>);

impl GateRelease {
    pub async fn entered(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.0.notification.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.entered.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await?;
        Ok(())
    }

    pub fn release(&self) {
        *self.0.released.lock().expect("released gate") = true;
        self.0.changed.notify_all();
    }
}

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) struct FixtureFailure {
    primary: Option<Box<dyn Error>>,
    cleanup: Option<Box<dyn Error>>,
}

impl fmt::Debug for FixtureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("FixtureFailure")
            .field("primary", &self.primary.is_some())
            .field("cleanup", &self.cleanup.is_some())
            .finish()
    }
}

impl fmt::Display for FixtureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("native finite probe or original cleanup failed")
    }
}

impl Error for FixtureFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.primary.as_deref().or(self.cleanup.as_deref())
    }
}

pub(super) fn settle(observed: Observed, cleanup: TestResult) -> TestResult {
    let primary = match observed {
        Err(payload) => {
            if cleanup.is_err() {
                eprintln!("native finite original cleanup also failed during panic");
            }
            std::mem::drop(cleanup);
            resume_unwind(payload)
        }
        Ok(Err(error)) => Err(error.into()),
        Ok(Ok(result)) => result,
    };
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (primary, cleanup) => Err(Box::new(FixtureFailure {
            primary: primary.err(),
            cleanup: cleanup.err(),
        })),
    }
}

pub(super) async fn observe(future: impl Future<Output = TestResult>) -> Observed {
    AssertUnwindSafe(tokio::time::timeout(TEST_DEADLINE, future))
        .catch_unwind()
        .await
}

pub(super) async fn run<P, F>(provider: P, case: F) -> TestResult
where
    P: StoreProvider,
    F: for<'a> FnOnce(&'a Node<P>) -> Pin<Box<dyn Future<Output = TestResult> + 'a>>,
{
    let node = Node::start(provider)?;
    let observed = observe(case(&node)).await;
    node.finish(observed)
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}

pub(super) fn full_config() -> QueueConfiguration {
    let defaults = QueueConfig::default();
    QueueConfiguration {
        lock_duration_millis: Some(defaults.lock_duration_millis),
        max_delivery_count: Some(defaults.max_delivery_count),
        default_time_to_live: Some(DefaultTimeToLive::DefaultTtlUnlimited(
            UnlimitedTimeToLive {},
        )),
        max_message_bytes: Some(defaults.max_message_bytes as u64),
        requires_session: Some(false),
        requires_duplicate_detection: Some(false),
        duplicate_detection_history_time_window_millis: Some(
            defaults.duplicate_detection_history_time_window_millis,
        ),
        dead_lettering_on_message_expiration: Some(false),
    }
}

pub(super) fn create(path: &str) -> CreateFiniteQueueRequest {
    CreateFiniteQueueRequest {
        namespace: "tenant".into(),
        path: path.into(),
        config: Some(full_config()),
        reservation_limit_bytes: Some(1_048_576),
    }
}

pub(super) fn get(path: &str) -> GetFiniteQueueRequest {
    GetFiniteQueueRequest {
        namespace: "tenant".into(),
        path: path.into(),
    }
}

pub(super) fn definition(current: &FiniteQueue) -> SetFiniteQueueDefinitionRequest {
    SetFiniteQueueDefinitionRequest {
        namespace: current.namespace.clone(),
        path: current.path.clone(),
        expected_generation: Some(current.generation),
        config: current.config,
        reservation_limit_bytes: Some(current.reservation_limit_bytes),
    }
}

pub(super) fn code<T: fmt::Debug>(
    result: Result<T, tonic::Status>,
    expected: Code,
) -> tonic::Status {
    let error = result.expect_err("expected finite RPC refusal");
    assert_eq!(error.code(), expected, "{error}");
    error
}

pub(super) fn legacy_create(path: &str) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Queue as i32,
        ..Default::default()
    }
}

pub(super) fn unchanged_except(before: &StoreSnapshot, after: &StoreSnapshot, changed: &[Vec<u8>]) {
    let retain = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !changed.contains(key))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(retain(before), retain(after));
}
