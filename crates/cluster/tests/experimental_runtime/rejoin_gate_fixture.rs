use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use cluster::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogProfile,
};
use domain::{MessageRecord, StateMachine};
use storage::{CommittedStore, StateStore, StorageError, StoreSnapshot, WriteBatch};
use tokio::sync::Notify;

use super::{TestResult, fixture};

// These controls own readers and observations, never a writer or its mutex.
// Consequently the physical writer Drop below runs on its actual native owner.
pub(super) struct Pair<R: StateStore> {
    pub(super) log: Control<R>,
    pub(super) state: Control<R>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Counts {
    pub(super) reads: usize,
    pub(super) commits: usize,
}

pub(super) struct Control<R: StateStore> {
    reader: R,
    observation: Arc<Observation>,
}

struct Observation {
    reads: AtomicUsize,
    commits: AtomicUsize,
    read_gate: Mutex<Option<Arc<NativeGate>>>,
    drop_gate: Mutex<Option<Arc<NativeGate>>>,
    writer_dropped: AtomicBool,
    changed: Notify,
}

impl Observation {
    fn before_read(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .read_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        // The gate runs before acquiring any physical backend lock.
        if let Some(gate) = gate {
            gate.block();
        }
    }
}

struct Writer<W: CommittedStore> {
    inner: Option<W>,
    observation: Arc<Observation>,
}

#[derive(Clone)]
struct Reader<R: StateStore> {
    inner: R,
    observation: Arc<Observation>,
}

fn wrap<W: CommittedStore>(writer: W) -> (Writer<W>, Control<W::Reader>) {
    let reader = writer.reader();
    let observation = Arc::new(Observation {
        reads: AtomicUsize::new(0),
        commits: AtomicUsize::new(0),
        read_gate: Mutex::new(None),
        drop_gate: Mutex::new(None),
        writer_dropped: AtomicBool::new(false),
        changed: Notify::new(),
    });
    (
        Writer {
            inner: Some(writer),
            observation: observation.clone(),
        },
        Control {
            reader,
            observation,
        },
    )
}

impl<W: CommittedStore> CommittedStore for Writer<W> {
    type Reader = Reader<W::Reader>;

    fn reader(&self) -> Self::Reader {
        Reader {
            inner: self.inner.as_ref().expect("live gated writer").reader(),
            observation: self.observation.clone(),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.observation.reads.fetch_add(1, Ordering::SeqCst);
        self.inner
            .as_ref()
            .expect("live gated writer")
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observation.commits.fetch_add(1, Ordering::SeqCst);
        let result = self
            .inner
            .as_mut()
            .expect("live gated writer")
            .commit(batch);
        self.observation.changed.notify_waiters();
        result
    }
}

impl<W: CommittedStore> Drop for Writer<W> {
    fn drop(&mut self) {
        let gate = self
            .observation
            .drop_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(gate) = gate {
            gate.block();
        }
        drop(self.inner.take());
        self.observation
            .writer_dropped
            .store(true, Ordering::SeqCst);
        self.observation.changed.notify_waiters();
    }
}

impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.observation.before_read();
        self.inner.get(key)
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::ReplicaWriteRequired)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observation.before_read();
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.observation.before_read();
        self.inner.scan_from(prefix, start, limit)
    }
}

impl<R: StateStore> Control<R> {
    pub(super) fn counts(&self) -> Counts {
        Counts {
            reads: self.observation.reads.load(Ordering::SeqCst),
            commits: self.observation.commits.load(Ordering::SeqCst),
        }
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        // Test observation does not consume a candidate owner's armed gate.
        Ok(self.reader.snapshot()?)
    }

    pub(super) fn writer_dropped(&self) -> bool {
        self.observation.writer_dropped.load(Ordering::SeqCst)
    }

    pub(super) async fn dropped(&self) {
        loop {
            let changed = self.observation.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.writer_dropped() {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn gate_next_read(&self) -> Gate {
        self.arm(&self.observation.read_gate)
    }

    pub(super) fn gate_writer_drop(&self) -> Gate {
        self.arm(&self.observation.drop_gate)
    }

    fn arm(&self, slot: &Mutex<Option<Arc<NativeGate>>>) -> Gate {
        let gate = Arc::new(NativeGate {
            entered: AtomicBool::new(false),
            changed: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        let previous = slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .replace(gate.clone());
        assert!(previous.is_none(), "only one native gate at a time");
        Gate(gate)
    }

    pub(super) fn message(&self, sequence: u64) -> TestResult<Option<MessageRecord>> {
        Ok(StateMachine::new(self.reader.clone()).message(
            &fixture::namespace()?,
            &fixture::entity()?,
            domain::SequenceNumber::new(sequence),
        )?)
    }

    pub(super) async fn wait_message(&self, sequence: u64) -> TestResult<MessageRecord> {
        loop {
            let changed = self.observation.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(message) = self.message(sequence)? {
                return Ok(message);
            }
            changed.await;
        }
    }
}

struct NativeGate {
    entered: AtomicBool,
    changed: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl NativeGate {
    fn block(&self) {
        self.entered.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while !*released {
            // Native I/O is never failed or released by a timeout.
            released = self
                .wake
                .wait(released)
                .unwrap_or_else(|poison| poison.into_inner());
        }
    }
}

pub(super) struct Gate(Arc<NativeGate>);

impl Gate {
    pub(super) fn has_entered(&self) -> bool {
        self.0.entered.load(Ordering::SeqCst)
    }

    pub(super) async fn entered(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.has_entered() {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn release(&self) {
        *self
            .0
            .released
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = true;
        self.0.wake.notify_all();
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) async fn prepare<W: CommittedStore>(
    node_id: u64,
    log: W,
    state: W,
    create: bool,
) -> TestResult<(ExperimentalReplicaStores, Pair<W::Reader>)> {
    let (log, log_control) = wrap(log);
    let (state, state_control) = wrap(state);
    let profile = LogProfile::new(node_id, fixture::stream()?)?;
    let log = if create {
        ExperimentalLogStore::create(log, profile)?
    } else {
        ExperimentalLogStore::open(log, profile)?
    };
    let state = if create {
        ExperimentalStateMachine::create(state, fixture::stream()?)
    } else {
        ExperimentalStateMachine::open(state, fixture::stream()?)
    };
    let state = match state {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error.into());
        }
    };
    let prepared = ExperimentalReplicaStores::prepare(node_id, log, state).await?;
    Ok((
        prepared,
        Pair {
            log: log_control,
            state: state_control,
        },
    ))
}
