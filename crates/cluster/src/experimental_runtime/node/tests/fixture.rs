use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use domain::{
    CommittedSend, CommittedStreamId, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine, Timestamp,
};
use openraft::{
    BasicNode, CommittedLeaderId, EntryPayload, Membership,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use tokio::sync::Notify;

use crate::experimental_runtime::{
    network::{Routes, stable_label},
    node::Node,
};
use crate::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogEntry, LogId,
    LogProfile, LogVote, QueueLogCommand,
};

use super::{DEADLINE, TestResult};

pub(super) const FOLLOWER: u64 = 8;
pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([31; 16])?)
}
pub(super) fn members() -> BTreeMap<u64, BasicNode> {
    [7, 8, 9]
        .into_iter()
        .map(|id| (id, BasicNode::new(stable_label(id))))
        .collect()
}
pub(super) fn id(index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(1, 7), index)
}
pub(super) fn vote() -> LogVote {
    LogVote::new_committed(1, 7)
}

pub(super) fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(Membership::new(
            vec![[7, 8, 9].into_iter().collect()],
            members(),
        )),
    }
}
pub(super) fn create() -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(1),
            QueueConfig::default(),
        )),
    })
}
pub(super) fn send(index: u64) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(index),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(index),
            CommittedSend {
                message_id: format!("message-{index}"),
                body: format!("body-{index}").into_bytes(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    })
}

struct Flags {
    retired: AtomicBool,
    retired_changed: Notify,
    read_fault: AtomicU8,
    commit_panic: AtomicBool,
    commit_gate: Mutex<Option<Arc<GateState>>>,
}

pub(super) struct Control<R: StateStore> {
    pub(super) reader: R,
    flags: Arc<Flags>,
}

impl<R: StateStore> Control<R> {
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.reader.snapshot()?)
    }
    pub(super) fn fail_read(&self) {
        self.flags.read_fault.store(1, Ordering::Release);
    }
    pub(super) fn panic_read(&self) {
        self.flags.read_fault.store(2, Ordering::Release);
    }
    pub(super) fn read_fault_pending(&self) -> bool {
        self.flags.read_fault.load(Ordering::Acquire) != 0
    }
    pub(super) fn panic_commit(&self) {
        self.flags.commit_panic.store(true, Ordering::Release);
    }
    pub(super) fn retired(&self) -> bool {
        self.flags.retired.load(Ordering::Acquire)
    }
    pub(super) async fn wait_retired(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let changed = self.flags.retired_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.retired() {
                    break;
                }
                changed.await;
            }
        })
        .await?;
        Ok(())
    }
    pub(super) fn gate_commit(&self) -> CommitGate {
        let state = Arc::new(GateState {
            entered: AtomicBool::new(false),
            changed: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        *self.flags.commit_gate.lock().expect("test commit gate") = Some(state.clone());
        CommitGate(state)
    }
}

struct GateState {
    entered: AtomicBool,
    changed: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

pub(super) struct CommitGate(Arc<GateState>);

impl CommitGate {
    pub(super) async fn entered(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.entered.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
        })
        .await?;
        Ok(())
    }
    pub(super) fn release(&self) {
        *self.0.released.lock().expect("test commit release") = true;
        self.0.wake.notify_all();
    }
}
impl Drop for CommitGate {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) struct Observed<W: CommittedStore> {
    inner: W,
    flags: Arc<Flags>,
}
#[derive(Clone)]
pub(super) struct Reader<R: StateStore> {
    inner: R,
    flags: Arc<Flags>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (Observed<W>, Control<W::Reader>) {
    let flags = Arc::new(Flags {
        retired: AtomicBool::new(false),
        retired_changed: Notify::new(),
        read_fault: AtomicU8::new(0),
        commit_panic: AtomicBool::new(false),
        commit_gate: Mutex::new(None),
    });
    let reader = writer.reader();
    (
        Observed {
            inner: writer,
            flags: flags.clone(),
        },
        Control { reader, flags },
    )
}

impl<R: StateStore> StateStore for Reader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.before_read()?;
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.before_read()?;
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
}

impl<R: StateStore> Reader<R> {
    fn before_read(&self) -> Result<(), StorageError> {
        match self.flags.read_fault.swap(0, Ordering::AcqRel) {
            1 => return Err(StorageError::LockPoisoned),
            2 => panic!("controlled runtime storage read panic"),
            _ => {}
        }
        Ok(())
    }
}
impl<W: CommittedStore> CommittedStore for Observed<W> {
    type Reader = Reader<W::Reader>;
    fn reader(&self) -> Self::Reader {
        Reader {
            inner: self.inner.reader(),
            flags: self.flags.clone(),
        }
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let gate = self
            .flags
            .commit_gate
            .lock()
            .expect("test commit slot")
            .take();
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::Release);
            gate.changed.notify_waiters();
            let mut released = gate.released.lock().expect("test owner commit gate");
            while !*released {
                released = gate.wake.wait(released).expect("test owner commit release");
            }
        }
        assert!(
            !self.flags.commit_panic.swap(false, Ordering::AcqRel),
            "controlled runtime commit panic"
        );
        self.inner.commit(batch)
    }
}
impl<W: CommittedStore> Drop for Observed<W> {
    fn drop(&mut self) {
        self.flags.retired.store(true, Ordering::Release);
        self.flags.retired_changed.notify_waiters();
    }
}

pub(super) async fn prepared<W: CommittedStore>(
    log_writer: W,
    state_writer: W,
) -> TestResult<(
    ExperimentalReplicaStores,
    Control<W::Reader>,
    Control<W::Reader>,
)> {
    let (log_writer, log_control) = observed(log_writer);
    let (state_writer, state_control) = observed(state_writer);
    let mut log = ExperimentalLogStore::create(log_writer, LogProfile::new(FOLLOWER, stream()?)?)?;
    let mut state = ExperimentalStateMachine::create(state_writer, stream()?)?;
    let entries = vec![initial(), create()?];
    log.blocking_append(entries.clone()).await?;
    log.save_vote(&vote()).await?;
    state.apply(entries).await?;
    let mut prepared = ExperimentalReplicaStores::prepare(FOLLOWER, log, state).await?;
    prepared.pause_runtime_ticks();
    Ok((prepared, log_control, state_control))
}

pub(super) async fn node<W: CommittedStore>(
    log: W,
    state: W,
) -> TestResult<(Node, Arc<Routes>, Control<W::Reader>, Control<W::Reader>)> {
    let (prepared, log_control, state_control) = prepared(log, state).await?;
    let routes = Routes::new(stream()?, [7, 8, 9])?;
    let pending = routes.begin_node(FOLLOWER)?;
    let node = tokio::time::timeout(DEADLINE, Node::start(prepared, pending, members())).await??;
    Ok((node, routes, log_control, state_control))
}

pub(super) fn assert_bodies<R: StateStore>(reader: R) -> TestResult {
    let machine = StateMachine::new(reader);
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    assert_eq!(
        machine.ready_sequences(&namespace, &entity, 10)?,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    for (sequence, index) in [(1, 2), (2, 3)] {
        let message = machine
            .message(&namespace, &entity, SequenceNumber::new(sequence))?
            .ok_or("missing committed original")?;
        assert_eq!(message.body, format!("body-{index}").into_bytes());
        assert_eq!(message.message_id, format!("message-{index}"));
    }
    assert!(
        machine
            .message(&namespace, &entity, SequenceNumber::new(3))?
            .is_none()
    );
    Ok(())
}
