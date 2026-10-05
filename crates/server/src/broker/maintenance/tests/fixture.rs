use super::*;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::thread::{self, ThreadId};

type Entries = Vec<(Vec<u8>, Vec<u8>)>;
type ApplyOutcome = Result<domain::CommandOutcome, crate::ProposeError>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub gets: Vec<(Vec<u8>, ThreadId)>,
    pub scans: Vec<(Vec<u8>, Vec<u8>, usize)>,
    pub snapshots: usize,
    pub applies: usize,
}

#[derive(Clone, Default)]
pub(super) struct ObservedStore {
    pub memory: MemoryStore,
    counts: Arc<Mutex<Counts>>,
    failures: Arc<AtomicUsize>,
}

impl ObservedStore {
    pub fn counts(&self) -> Counts {
        self.counts.lock().expect("test count lock").clone()
    }
    pub fn reset(&self) {
        *self.counts.lock().expect("test count lock") = Counts::default();
    }
    pub fn fail_floor(&self, count: usize) {
        self.failures.store(count, Ordering::SeqCst);
    }
    pub fn bytes(&self) -> TestResult<Entries> {
        Ok(self.memory.snapshot()?.entries().to_vec())
    }
}

impl StateStore for ObservedStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.counts
            .lock()
            .expect("test count lock")
            .gets
            .push((key.to_vec(), thread::current().id()));
        if key == [0]
            && self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        {
            return Err(StorageError::Backend {
                operation: "read test floor",
                detail: "private clock backend secret".into(),
            });
        }
        self.memory.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.lock().expect("test count lock").applies += 1;
        self.memory.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.counts.lock().expect("test count lock").snapshots += 1;
        self.memory.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.counts.lock().expect("test count lock").scans.push((
            prefix.to_vec(),
            start.to_vec(),
            limit,
        ));
        self.memory.scan_from(prefix, start, limit)
    }
}

#[derive(Default)]
struct Gate {
    next: bool,
    released: bool,
}

#[derive(Clone)]
pub(super) struct ProbeClock {
    millis: Arc<AtomicU64>,
    calls: Arc<Mutex<Vec<ThreadId>>>,
    gate: Arc<(Mutex<Gate>, Condvar)>,
    entered: flume::Sender<()>,
    observation: flume::Receiver<()>,
    panic_next: Arc<AtomicBool>,
}

impl ProbeClock {
    pub fn new(millis: u64) -> Self {
        let (entered, observation) = flume::bounded(1);
        Self {
            millis: Arc::new(AtomicU64::new(millis)),
            calls: Arc::new(Mutex::new(Vec::new())),
            gate: Arc::new((Mutex::new(Gate::default()), Condvar::new())),
            entered,
            observation,
            panic_next: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn set(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }
    pub fn calls(&self) -> Vec<ThreadId> {
        self.calls.lock().expect("test calls lock").clone()
    }
    pub fn arm(&self) {
        *self.gate.0.lock().expect("test gate") = Gate {
            next: true,
            released: false,
        };
    }
    pub fn entered(&self) -> TestResult {
        self.observation.recv_timeout(WAIT)?;
        Ok(())
    }
    pub fn release(&self) {
        self.gate.0.lock().expect("test gate").released = true;
        self.gate.1.notify_all();
    }
    pub fn panic_once(&self) {
        self.panic_next.store(true, Ordering::SeqCst);
    }
}

impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        self.calls
            .lock()
            .expect("test calls lock")
            .push(thread::current().id());
        let mut gate = self.gate.0.lock().expect("test gate");
        if gate.next {
            gate.next = false;
            let _ = self.entered.try_send(());
            let (returned, _) = self
                .gate
                .1
                .wait_timeout_while(gate, WAIT * 2, |state| !state.released)
                .expect("test gate wait");
            gate = returned;
        }
        drop(gate);
        if self.panic_next.swap(false, Ordering::SeqCst) {
            panic!("test owner clock unwind");
        }
        Timestamp::from_millis(self.millis.load(Ordering::SeqCst))
    }
}

pub(super) struct Fixture {
    pub store: ObservedStore,
    pub clock: ProbeClock,
    pub handle: BrokerHandle,
    broker: Option<Broker>,
}

pub(super) struct Cleanup {
    pub owner: thread::Result<()>,
}

impl Fixture {
    pub fn new(floor: Option<u64>, now: u64, allowance: u64) -> TestResult<Self> {
        let store = ObservedStore::default();
        if let Some(floor) = floor {
            let (namespace, entity) = names();
            StateMachine::new(store.clone()).apply(&Command::new(
                namespace,
                entity,
                Timestamp::from_millis(floor),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ))?;
        }
        store.reset();
        let clock = ProbeClock::new(now);
        let broker = Broker::spawn(
            crate::LocalProposer::new(StateMachine::new(store.clone()), clock.clone())
                .with_max_clock_regression_millis(allowance),
        );
        let handle = broker.handle();
        Ok(Self {
            store,
            clock,
            handle,
            broker: Some(broker),
        })
    }
    pub fn finish(mut self) -> Cleanup {
        self.clock.release();
        let mut broker = self.broker.take().expect("fixture owns broker");
        let _ = broker.handle.requests.send(Request::Stop);
        let owner = broker.owner.take().expect("original owner token").join();
        drop(broker);
        Cleanup { owner }
    }
    pub fn queued_apply(&self, kind: CommandKind) -> TestResult<flume::Receiver<ApplyOutcome>> {
        let (reply, outcome) = flume::bounded(1);
        let (namespace, entity) = names();
        self.handle
            .requests
            .send(Request::Apply {
                namespace,
                entity,
                binding: None,
                kind: Box::new(kind),
                reply,
            })
            .map_err(|_| "test queue closed")?;
        Ok(outcome)
    }
    pub fn queued_query(&self) -> TestResult<flume::Receiver<Assessment>> {
        let (reply, outcome) = flume::bounded(1);
        self.handle
            .requests
            .send(Request::MaintenanceClockAssessment { reply })
            .map_err(|_| "test queue closed")?;
        Ok(outcome)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.clock.release();
        drop(self.broker.take());
    }
}
