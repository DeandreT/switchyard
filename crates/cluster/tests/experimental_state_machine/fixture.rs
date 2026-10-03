use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use cluster::{
    AppliedState, ExperimentalStateMachine, LogEntry, LogId, QueueLogCommand, StateMachineWorkload,
};
use domain::{
    CommittedEntryId, CommittedSend, CommittedStreamId, EntityPath, MessageRecord, NamespaceName,
    QueueConfig, QueueCounters, SequenceNumber, StateMachine, Timestamp,
};
use openraft::{BasicNode, CommittedLeaderId, EntryPayload, Membership, storage::RaftStateMachine};
use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use tokio::sync::Notify;

use super::{DEADLINE, TestResult};

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([9; 16])?)
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}
pub(super) fn entity() -> TestResult<EntityPath> {
    Ok(EntityPath::new("orders")?)
}

// Frozen SWYC checkpoint key, kept test-only rather than exposing a raw writer key.
pub(super) fn checkpoint_key() -> Key {
    vec![0x12]
}

pub(super) fn id(term: u64, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, 7), index)
}

pub(super) fn entry_id(index: u64) -> CommittedEntryId {
    CommittedEntryId {
        term: 1,
        node_id: 7,
        index,
    }
}

pub(super) fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Blank,
    }
}

pub(super) fn create(index: u64, time: u64, config: QueueConfig) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            namespace()?,
            entity()?,
            Timestamp::from_millis(time),
            config,
        )),
    })
}

pub(super) fn send(index: u64, time: u64, body: Vec<u8>) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(time),
            CommittedSend {
                message_id: format!("message-{index}"),
                body,
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    })
}

pub(super) fn membership(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("three")),
            ]),
        )),
    }
}

pub(super) fn message<R: StateStore>(
    reader: &R,
    sequence: u64,
) -> TestResult<Option<MessageRecord>> {
    Ok(StateMachine::new(reader.clone()).message(
        &namespace()?,
        &entity()?,
        SequenceNumber::new(sequence),
    )?)
}

pub(super) fn counters<R: StateStore>(reader: &R) -> TestResult<Option<QueueCounters>> {
    reader
        .get(&domain::keys::queue_counters(&namespace()?, &entity()?))?
        .map(|bytes| Ok(domain::codec::decode(&bytes)?))
        .transpose()
}

pub(super) async fn model(entries: &[LogEntry]) -> TestResult<(StoreSnapshot, AppliedState)> {
    let writer = storage::MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    machine.apply(entries.to_vec()).await?;
    let state = machine.applied_state().await?;
    let snapshot = reader.snapshot()?;
    machine.shutdown().await?;
    Ok((snapshot, state))
}

pub(super) fn restore<W: CommittedStore>(
    control: &Control<W>,
    baseline: &StoreSnapshot,
) -> TestResult {
    let mut batch = WriteBatch::default();
    for (key, _) in control.reader().snapshot()?.entries() {
        batch.push_delete(key.clone());
    }
    for (key, value) in baseline.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    control.inject(batch)?;
    Ok(())
}

pub(super) async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(
            "state-machine operation completed before its controlled boundary".into(),
        )),
    })
    .await
}

pub(super) async fn workload(
    machine: &ExperimentalStateMachine,
    jobs: usize,
) -> TestResult<StateMachineWorkload> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let observed = machine.workload()?;
        if observed.accepted_jobs == jobs {
            return Ok(observed);
        }
        if Instant::now() >= deadline {
            return Err("state-machine workload did not reach the expected bounded state".into());
        }
        tokio::task::yield_now().await;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub reads: Vec<Key>,
    pub scans: usize,
    pub commits: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Before,
    After,
    ExitBefore,
    ExitAfter,
    PanicBefore,
    PanicAfter,
}

struct SharedWriter<W> {
    writer: W,
    fault: Option<(usize, Fault)>,
    gate: Option<(usize, Arc<CommitGate>)>,
    last_batch: Option<WriteBatch>,
}

pub(super) struct ObservedWriter<W: CommittedStore> {
    writer: Arc<Mutex<SharedWriter<W>>>,
    reader: ObservedReader<W::Reader>,
    commits: Arc<AtomicUsize>,
}

pub(super) struct Control<W: CommittedStore> {
    writer: Arc<Mutex<SharedWriter<W>>>,
    reader: ObservedReader<W::Reader>,
    commits: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub(super) struct ObservedReader<R> {
    inner: R,
    observations: Arc<Mutex<Counts>>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (ObservedWriter<W>, Control<W>) {
    let reader = ObservedReader {
        inner: writer.reader(),
        observations: Arc::new(Mutex::new(Counts::default())),
    };
    let writer = Arc::new(Mutex::new(SharedWriter {
        writer,
        fault: None,
        gate: None,
        last_batch: None,
    }));
    let commits = Arc::new(AtomicUsize::new(0));
    (
        ObservedWriter {
            writer: writer.clone(),
            reader: reader.clone(),
            commits: commits.clone(),
        },
        Control {
            writer,
            reader,
            commits,
        },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        let mut counts = self
            .reader
            .observations
            .lock()
            .expect("test observations lock")
            .clone();
        counts.commits = self.commits.load(Ordering::SeqCst);
        counts
    }

    pub(super) fn reset_reads(&self) {
        let mut counts = self
            .reader
            .observations
            .lock()
            .expect("test observations lock");
        counts.reads.clear();
        counts.scans = 0;
    }

    pub(super) fn reader(&self) -> ObservedReader<W::Reader> {
        self.reader.clone()
    }

    pub(super) fn fault_after(&self, delta: usize, fault: Fault) {
        assert!(delta > 0);
        self.writer.lock().expect("test writer lock").fault =
            Some((self.commits.load(Ordering::SeqCst) + delta, fault));
    }

    pub(super) fn gate_after(&self, delta: usize) -> GateGuard {
        assert!(delta > 0);
        let gate = Arc::new(CommitGate {
            entered: AtomicBool::new(false),
            notify: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        self.writer.lock().expect("test writer lock").gate =
            Some((self.commits.load(Ordering::SeqCst) + delta, gate.clone()));
        GateGuard(gate)
    }

    pub(super) fn last_batch(&self) -> WriteBatch {
        self.writer
            .lock()
            .expect("test writer lock")
            .last_batch
            .clone()
            .expect("observed commit batch")
    }

    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }

    pub(super) fn recover_writer(&self) -> ObservedWriter<W> {
        ObservedWriter {
            writer: self.writer.clone(),
            reader: self.reader.clone(),
            commits: self.commits.clone(),
        }
    }
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = ObservedReader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let attempt = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        let (fault, gate) = {
            let mut writer = self.writer.lock().expect("test writer lock");
            writer.last_batch = Some(batch.clone());
            let fault = if writer.fault.is_some_and(|(target, _)| target == attempt) {
                writer.fault.take().map(|(_, fault)| fault)
            } else {
                None
            };
            let gate = if writer
                .gate
                .as_ref()
                .is_some_and(|(target, _)| *target == attempt)
            {
                writer.gate.take().map(|(_, gate)| gate)
            } else {
                None
            };
            (fault, gate)
        };
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::SeqCst);
            gate.notify.notify_waiters();
            let released = gate.released.lock().expect("test commit gate lock");
            let (released, _) = gate
                .wake
                .wait_timeout_while(released, DEADLINE, |released| !*released)
                .expect("test commit gate lock");
            if !*released {
                return Err(injected_error());
            }
        }
        match fault {
            Some(Fault::Before) => return Err(injected_error()),
            Some(Fault::ExitBefore) => std::process::exit(79),
            Some(Fault::PanicBefore) => panic!("injected state-machine worker panic"),
            _ => {}
        }
        self.writer
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)?;
        match fault {
            Some(Fault::After) => Err(injected_error()),
            Some(Fault::ExitAfter) => std::process::exit(80),
            Some(Fault::PanicAfter) => panic!("injected state-machine worker panic after commit"),
            _ => Ok(()),
        }
    }
}

impl<R: StateStore> StateStore for ObservedReader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations
            .lock()
            .expect("test observations lock")
            .reads
            .push(key.to_vec());
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observations
            .lock()
            .expect("test observations lock")
            .scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
}

fn injected_error() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "injected state-machine commit failure".into(),
    }
}

struct CommitGate {
    entered: AtomicBool,
    notify: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

pub(super) struct GateGuard(Arc<CommitGate>);
impl GateGuard {
    pub(super) async fn entered(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.0.notify.notified();
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
    pub(super) fn release(&self) {
        *self.0.released.lock().expect("test commit gate lock") = true;
        self.0.wake.notify_all();
    }
}
impl Drop for GateGuard {
    fn drop(&mut self) {
        self.release();
    }
}
