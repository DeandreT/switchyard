use std::{
    future::{Future, poll_fn},
    path::Path,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use cluster::{
    ExperimentalLogStore, ExperimentalRaftCluster, ExperimentalRaftHandle,
    ExperimentalReplicaStores, ExperimentalStateMachine, LogProfile, QueueIntent, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection,
};
use domain::{
    CommittedSend, CommittedStreamId, EntityPath, MessageRecord, NamespaceName, QueueConfig,
    SequenceNumber, StateMachine,
};
use storage::{CommittedStore, FjallReplicaStore, Mutation, StorageError, WriteBatch};
use tokio::sync::Notify;

use super::{DEADLINE, TestResult};

pub(super) const IDS: [u64; 3] = [7, 8, 9];

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}
pub(super) fn entity() -> TestResult<EntityPath> {
    Ok(EntityPath::new("orders")?)
}
pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([41; 16])?)
}

pub(super) fn send(body: Vec<u8>, name: &str) -> TestResult<QueueIntent> {
    Ok(QueueIntent::send(
        namespace()?,
        entity()?,
        CommittedSend {
            message_id: name.to_owned(),
            body,
            time_to_live_millis: None,
            session_id: None,
        },
    )?)
}

pub(super) fn durable_stores(
    path: &Path,
) -> TestResult<[(FjallReplicaStore, FjallReplicaStore); 3]> {
    let mut stores = Vec::new();
    for id in IDS {
        stores.push((
            FjallReplicaStore::open(path.join(format!("log-{id}")))?,
            FjallReplicaStore::open(path.join(format!("state-{id}")))?,
        ));
    }
    stores
        .try_into()
        .map_err(|_| "expected three durable pairs".into())
}

pub(super) struct ReplicaControl<W: CommittedStore> {
    pub(super) log: Control<W>,
    pub(super) state: Control<W>,
}

pub(super) async fn create<W: CommittedStore>(
    backends: [(W, W); 3],
) -> TestResult<(ExperimentalRaftCluster, [ReplicaControl<W>; 3])> {
    let mut prepared = Vec::new();
    let mut controls = Vec::new();
    for (id, (log, state)) in IDS.into_iter().zip(backends) {
        let (log, log_control) = observed(log);
        let (state, state_control) = observed(state);
        let log = ExperimentalLogStore::create(log, LogProfile::new(id, stream()?)?)?;
        let state = ExperimentalStateMachine::create(state, stream()?)?;
        prepared.push(ExperimentalReplicaStores::prepare(id, log, state).await?);
        controls.push(ReplicaControl {
            log: log_control,
            state: state_control,
        });
    }
    let prepared = prepared
        .try_into()
        .map_err(|_| "expected three prepared pairs")?;
    let cluster = ExperimentalRaftCluster::create(prepared).await?;
    let controls = controls.try_into().map_err(|_| "expected three controls")?;
    Ok((cluster, controls))
}

pub(super) async fn create_queue(
    cluster: &ExperimentalRaftCluster,
) -> TestResult<(u64, ExperimentalRaftHandle)> {
    loop {
        if let Some(id) = cluster.leader_hint() {
            let handle = cluster
                .handle(id)
                .ok_or("leader hint references closed node")?;
            let intent =
                QueueIntent::create_queue(namespace()?, entity()?, QueueConfig::default())?;
            match handle.submit(intent).await {
                Ok(result) if result.outcome == QueueWriteOutcome::QueueCreated => {
                    return Ok((id, handle));
                }
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {}
                result => return Err(format!("unexpected initial queue result: {result:?}").into()),
            }
        }
        tokio::task::yield_now().await;
    }
}

pub(super) async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
        std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(
            "operation completed before controlled persistence".into(),
        )),
    })
    .await
}

struct Shared<W> {
    writer: W,
    gate: Option<(Match, Arc<CommitGate>)>,
}
enum Match {
    Prefix(u8),
    Exact(Vec<u8>),
}
impl Match {
    fn matches(&self, batch: &WriteBatch) -> bool {
        batch.mutations().iter().any(|mutation| match mutation {
            Mutation::Put { key, .. } => match self {
                Self::Prefix(prefix) => key.first() == Some(prefix),
                Self::Exact(expected) => key == expected,
            },
            Mutation::Delete { .. } => false,
        })
    }
}

struct ObservedWriter<W: CommittedStore> {
    shared: Arc<Mutex<Shared<W>>>,
    reader: W::Reader,
    changed: Arc<Notify>,
}
pub(super) struct Control<W: CommittedStore> {
    shared: Arc<Mutex<Shared<W>>>,
    reader: W::Reader,
    changed: Arc<Notify>,
}

fn observed<W: CommittedStore>(writer: W) -> (ObservedWriter<W>, Control<W>) {
    let reader = writer.reader();
    let shared = Arc::new(Mutex::new(Shared { writer, gate: None }));
    let changed = Arc::new(Notify::new());
    (
        ObservedWriter {
            shared: shared.clone(),
            reader: reader.clone(),
            changed: changed.clone(),
        },
        Control {
            shared,
            reader,
            changed,
        },
    )
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = W::Reader;
    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .writer
            .is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let gate = {
            let mut shared = self.shared.lock().map_err(|_| StorageError::LockPoisoned)?;
            if shared
                .gate
                .as_ref()
                .is_some_and(|(target, _)| target.matches(&batch))
            {
                shared.gate.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = &gate {
            gate.block()?;
        }
        self.shared
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .writer
            .commit(batch)?;
        if let Some(gate) = gate {
            gate.persisted.store(true, Ordering::SeqCst);
            gate.changed.notify_waiters();
        }
        self.changed.notify_waiters();
        Ok(())
    }
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn reader(&self) -> W::Reader {
        self.reader.clone()
    }
    pub(super) fn gate_log_append(&self) -> Gate {
        self.gate(Match::Prefix(0x10))
    }
    pub(super) fn gate_message(&self, sequence: u64) -> TestResult<Gate> {
        Ok(self.gate(Match::Exact(domain::keys::message(
            &namespace()?,
            &entity()?,
            SequenceNumber::new(sequence),
        ))))
    }
    fn gate(&self, target: Match) -> Gate {
        let gate = Arc::new(CommitGate {
            entered: AtomicBool::new(false),
            persisted: AtomicBool::new(false),
            changed: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        let mut shared = self.shared.lock().expect("test writer lock");
        assert!(shared.gate.is_none());
        shared.gate = Some((target, gate.clone()));
        Gate(gate)
    }
    pub(super) fn message(&self, sequence: u64) -> TestResult<Option<MessageRecord>> {
        Ok(StateMachine::new(self.reader()).message(
            &namespace()?,
            &entity()?,
            SequenceNumber::new(sequence),
        )?)
    }
    pub(super) async fn wait_queue(&self) -> TestResult {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if StateMachine::new(self.reader())
                .queue_config(&namespace()?, &entity()?)?
                .is_some()
            {
                return Ok(());
            }
            changed.await;
        }
    }
    pub(super) async fn wait_message(&self, sequence: u64) -> TestResult<MessageRecord> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(message) = self.message(sequence)? {
                return Ok(message);
            }
            changed.await;
        }
    }
}

struct CommitGate {
    entered: AtomicBool,
    persisted: AtomicBool,
    changed: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}
impl CommitGate {
    fn block(&self) -> Result<(), StorageError> {
        self.entered.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        let deadline = Instant::now() + DEADLINE;
        let mut released = self
            .released
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?;
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(StorageError::CorruptMetadata {
                    detail: "test commit gate timed out".into(),
                });
            }
            released = self
                .wake
                .wait_timeout(released, remaining)
                .map_err(|_| StorageError::LockPoisoned)?
                .0;
        }
        Ok(())
    }
}
pub(super) struct Gate(Arc<CommitGate>);
impl Gate {
    pub(super) fn has_entered(&self) -> bool {
        self.0.entered.load(Ordering::SeqCst)
    }
    pub(super) async fn entered(&self) {
        self.wait(&self.0.entered).await;
    }
    pub(super) async fn persisted(&self) {
        self.wait(&self.0.persisted).await;
    }
    async fn wait(&self, flag: &AtomicBool) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if flag.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }
    pub(super) fn release(&self) {
        *self.0.released.lock().expect("test gate lock") = true;
        self.0.wake.notify_all();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}
