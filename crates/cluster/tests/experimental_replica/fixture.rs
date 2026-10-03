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
    ExperimentalLogStore, ExperimentalStateMachine, LogEntry, LogId, LogProfile, QueueLogCommand,
};
use domain::{CommittedSend, CommittedStreamId, EntityPath, NamespaceName, QueueConfig, Timestamp};
use openraft::{
    BasicNode, CommittedLeaderId, EntryPayload, Membership,
    storage::{RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use tokio::sync::Notify;

use super::{DEADLINE, TestResult};

pub(super) const NODE: u64 = 7;
pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([11; 16])?)
}
pub(super) fn other_stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([12; 16])?)
}
pub(super) fn profile() -> TestResult<LogProfile> {
    Ok(LogProfile::new(NODE, stream()?)?)
}
pub(super) fn id(index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(1, NODE), index)
}

pub(super) fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(members()),
    }
}

pub(super) fn members() -> Membership<u64, BasicNode> {
    Membership::new(
        vec![BTreeSet::from([7, 8, 9])],
        BTreeMap::from([
            (7, BasicNode::new("seven")),
            (8, BasicNode::new("eight")),
            (9, BasicNode::new("nine")),
        ]),
    )
}

pub(super) fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}

pub(super) fn create(index: u64, time: u64) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(index),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(time),
            QueueConfig::default(),
        )),
    })
}

pub(super) fn send(index: u64, time: u64, body: Vec<u8>) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(index),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
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

pub(super) async fn append_all(log: &mut ExperimentalLogStore, entries: &[LogEntry]) -> TestResult {
    for chunk in entries.chunks(15) {
        log.blocking_append(chunk.to_vec()).await?;
    }
    Ok(())
}

pub(super) async fn seed<W: CommittedStore>(
    log: W,
    state: W,
    entries: &[LogEntry],
    applied: usize,
) -> TestResult<(
    ExperimentalLogStore,
    ExperimentalStateMachine,
    Control<W>,
    Control<W>,
)> {
    seed_profiled(log, state, profile()?, stream()?, entries, applied).await
}

pub(super) async fn seed_profiled<W: CommittedStore>(
    log: W,
    state: W,
    log_profile: LogProfile,
    state_stream: CommittedStreamId,
    entries: &[LogEntry],
    applied: usize,
) -> TestResult<(
    ExperimentalLogStore,
    ExperimentalStateMachine,
    Control<W>,
    Control<W>,
)> {
    let (log_writer, log_control) = observed(log);
    let (state_writer, state_control) = observed(state);
    let mut log = ExperimentalLogStore::create(log_writer, log_profile)?;
    let mut state = ExperimentalStateMachine::create(state_writer, state_stream)?;
    append_all(&mut log, entries).await?;
    if applied > 0 {
        state.apply(entries[..applied].to_vec()).await?;
    }
    Ok((log, state, log_control, state_control))
}

pub(super) async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(
            "replica preparation completed before its controlled read boundary".into(),
        )),
    })
    .await
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

struct Shared<W> {
    writer: W,
}

pub(super) struct ObservedWriter<W: CommittedStore> {
    shared: Arc<Mutex<Shared<W>>>,
    reader: ObservedReader<W::Reader>,
    commits: Arc<AtomicUsize>,
    retired: Arc<AtomicBool>,
}

pub(super) struct Control<W: CommittedStore> {
    shared: Arc<Mutex<Shared<W>>>,
    reader: ObservedReader<W::Reader>,
    commits: Arc<AtomicUsize>,
    retired: Arc<AtomicBool>,
}

struct ReadControls {
    next: Mutex<Option<ReadFault>>,
    reads: AtomicUsize,
}
enum ReadFault {
    Error,
    Panic,
    Gate(Arc<ReadGate>),
}

#[derive(Clone)]
pub(super) struct ObservedReader<R> {
    inner: R,
    controls: Arc<ReadControls>,
}

fn observed<W: CommittedStore>(writer: W) -> (ObservedWriter<W>, Control<W>) {
    let reader = ObservedReader {
        inner: writer.reader(),
        controls: Arc::new(ReadControls {
            next: Mutex::new(None),
            reads: AtomicUsize::new(0),
        }),
    };
    let shared = Arc::new(Mutex::new(Shared { writer }));
    let commits = Arc::new(AtomicUsize::new(0));
    let retired = Arc::new(AtomicBool::new(false));
    (
        ObservedWriter {
            shared: shared.clone(),
            reader: reader.clone(),
            commits: commits.clone(),
            retired: retired.clone(),
        },
        Control {
            shared,
            reader,
            commits,
            retired,
        },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn reader(&self) -> ObservedReader<W::Reader> {
        self.reader.clone()
    }
    pub(super) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }
    pub(super) fn reads(&self) -> usize {
        self.reader.controls.reads.load(Ordering::SeqCst)
    }
    pub(super) fn retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }
    pub(super) fn error_next_read(&self) {
        *self
            .reader
            .controls
            .next
            .lock()
            .expect("test read control lock") = Some(ReadFault::Error);
    }
    pub(super) fn panic_next_read(&self) {
        *self
            .reader
            .controls
            .next
            .lock()
            .expect("test read control lock") = Some(ReadFault::Panic);
    }
    pub(super) fn gate_next_read(&self) -> ReadGateGuard {
        let gate = Arc::new(ReadGate {
            entered: AtomicBool::new(false),
            notify: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        *self
            .reader
            .controls
            .next
            .lock()
            .expect("test read control lock") = Some(ReadFault::Gate(gate.clone()));
        ReadGateGuard(gate)
    }
    pub(super) fn recover_writer(&self) -> ObservedWriter<W> {
        assert!(self.retired());
        self.retired.store(false, Ordering::SeqCst);
        ObservedWriter {
            shared: self.shared.clone(),
            reader: self.reader.clone(),
            commits: self.commits.clone(),
            retired: self.retired.clone(),
        }
    }
    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }
}

impl<W: CommittedStore> Drop for ObservedWriter<W> {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::SeqCst);
    }
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = ObservedReader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.shared
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }
}

impl<R: StateStore> StateStore for ObservedReader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
        let fault = self
            .controls
            .next
            .lock()
            .expect("test read control lock")
            .take();
        match fault {
            Some(ReadFault::Error) => return Err(injected_error()),
            Some(ReadFault::Panic) => panic!("private replica test owner read panic"),
            Some(ReadFault::Gate(gate)) => {
                gate.entered.store(true, Ordering::SeqCst);
                gate.notify.notify_waiters();
                let released = gate.released.lock().expect("test read gate lock");
                let (released, _) = gate
                    .wake
                    .wait_timeout_while(released, DEADLINE, |released| !*released)
                    .expect("test read gate lock");
                if !*released {
                    return Err(injected_error());
                }
            }
            None => {}
        }
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.controls.reads.fetch_add(1, Ordering::SeqCst);
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
        detail: "private replica read failure".into(),
    }
}

struct ReadGate {
    entered: AtomicBool,
    notify: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

pub(super) struct ReadGateGuard(Arc<ReadGate>);
impl ReadGateGuard {
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
        *self.0.released.lock().expect("test read gate lock") = true;
        self.0.wake.notify_all();
    }
}
impl Drop for ReadGateGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) async fn owners_retired<W: CommittedStore>(
    log: &Control<W>,
    state: &Control<W>,
) -> TestResult {
    let deadline = Instant::now() + DEADLINE;
    while !log.retired() || !state.retired() {
        if Instant::now() >= deadline {
            return Err("owned replica preparation did not retire both backing adapters".into());
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}
