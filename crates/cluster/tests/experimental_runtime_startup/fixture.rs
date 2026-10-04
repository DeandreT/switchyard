use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use cluster::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogEntry, LogId,
    LogProfile, LogVote,
};
use domain::CommittedStreamId;
use openraft::{
    BasicNode, EntryPayload, Membership,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use super::{DEADLINE, TestResult};

pub(super) const IDS: [u64; 3] = [7, 8, 9];

pub(super) fn stream() -> CommittedStreamId {
    CommittedStreamId::new([71; 16]).unwrap()
}
pub(super) fn other_stream() -> CommittedStreamId {
    CommittedStreamId::new([72; 16]).unwrap()
}
pub(super) fn label(id: u64) -> String {
    format!("switchyard-in-process-{id}")
}
pub(super) fn members() -> Membership<u64, BasicNode> {
    Membership::new(
        vec![BTreeSet::from(IDS)],
        IDS.into_iter()
            .map(|id| (id, BasicNode::new(label(id))))
            .collect::<BTreeMap<_, _>>(),
    )
}
pub(super) fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(members()),
    }
}
pub(super) fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 7), index),
        payload: EntryPayload::Blank,
    }
}

pub(super) trait Backend {
    type Writer: CommittedStore;
    fn writers(&self, name: &str) -> TestResult<[(Self::Writer, Self::Writer); 3]>;
}

pub(super) struct Memory;
impl Backend for Memory {
    type Writer = storage::MemoryReplicaStore;
    fn writers(&self, _name: &str) -> TestResult<[(Self::Writer, Self::Writer); 3]> {
        Ok(std::array::from_fn(|_| {
            (
                storage::MemoryReplicaStore::new(),
                storage::MemoryReplicaStore::new(),
            )
        }))
    }
}

pub(super) struct Durable(pub(super) PathBuf);
impl Backend for Durable {
    type Writer = storage::FjallReplicaStore;
    fn writers(&self, name: &str) -> TestResult<[(Self::Writer, Self::Writer); 3]> {
        let mut writers = Vec::new();
        for index in 0..3 {
            let root = self.0.join(name).join(index.to_string());
            writers.push((
                storage::FjallReplicaStore::open(root.join("log"))?,
                storage::FjallReplicaStore::open(root.join("state"))?,
            ));
        }
        writers
            .try_into()
            .map_err(|_| "three writer pairs required".into())
    }
}

#[derive(Clone)]
pub(super) struct Seed {
    pub(super) ids: [u64; 3],
    pub(super) streams: [CommittedStreamId; 3],
    pub(super) histories: [Vec<LogEntry>; 3],
    pub(super) applied: [usize; 3],
    pub(super) votes: [Option<LogVote>; 3],
}

impl Default for Seed {
    fn default() -> Self {
        Self {
            ids: IDS,
            streams: [stream(); 3],
            histories: std::array::from_fn(|_| Vec::new()),
            applied: [0; 3],
            votes: [None; 3],
        }
    }
}

impl Seed {
    pub(super) fn initialized() -> Self {
        Self {
            histories: std::array::from_fn(|_| vec![initial(), blank(1)]),
            applied: [1; 3],
            votes: [Some(LogVote::new_committed(1, 7)); 3],
            ..Self::default()
        }
    }
}

struct Shared<W: CommittedStore> {
    writer: Mutex<W>,
    commits: AtomicUsize,
    retired: AtomicBool,
    full_scans: AtomicUsize,
    fail_full_scan: AtomicBool,
    fault_fired: AtomicBool,
    reads: AtomicUsize,
}

pub(super) struct Control<W: CommittedStore> {
    shared: Arc<Shared<W>>,
    reader: W::Reader,
}

impl<W: CommittedStore> Clone for Control<W> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            reader: self.reader.clone(),
        }
    }
}

struct ObservedWriter<W: CommittedStore> {
    control: Control<W>,
}
struct ObservedReader<W: CommittedStore> {
    control: Control<W>,
}

impl<W: CommittedStore> Clone for ObservedReader<W> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}

impl<W: CommittedStore> Control<W> {
    fn new(writer: W) -> Self {
        let reader = writer.reader();
        Self {
            reader,
            shared: Arc::new(Shared {
                writer: Mutex::new(writer),
                commits: AtomicUsize::new(0),
                retired: AtomicBool::new(false),
                full_scans: AtomicUsize::new(0),
                fail_full_scan: AtomicBool::new(false),
                fault_fired: AtomicBool::new(false),
                reads: AtomicUsize::new(0),
            }),
        }
    }
    fn writer(&self) -> ObservedWriter<W> {
        self.shared.retired.store(false, Ordering::Release);
        ObservedWriter {
            control: self.clone(),
        }
    }
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.reader.snapshot()?)
    }
    pub(super) fn queue_config(&self) -> TestResult<Option<domain::QueueConfig>> {
        Ok(domain::StateMachine::new(self.reader.clone()).queue_config(
            &domain::NamespaceName::new("tenant")?,
            &domain::EntityPath::new("orders")?,
        )?)
    }
    pub(super) fn commits(&self) -> usize {
        self.shared.commits.load(Ordering::Acquire)
    }
    pub(super) fn reads(&self) -> usize {
        self.shared.reads.load(Ordering::Acquire)
    }
    pub(super) fn retired(&self) -> bool {
        self.shared.retired.load(Ordering::Acquire)
    }
    pub(super) fn full_scans(&self) -> usize {
        self.shared.full_scans.load(Ordering::Acquire)
    }
    pub(super) fn fault_fired(&self) -> bool {
        self.shared.fault_fired.load(Ordering::Acquire)
    }
    pub(super) fn fail_next_full_scan(&self) {
        self.shared.fail_full_scan.store(true, Ordering::Release);
    }
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = ObservedReader<W>;
    fn reader(&self) -> Self::Reader {
        ObservedReader {
            control: self.control.clone(),
        }
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control.shared.commits.fetch_add(1, Ordering::AcqRel);
        self.control
            .shared
            .writer
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .commit(batch)
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.control
            .shared
            .writer
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .is_initialized()
    }
}

impl<W: CommittedStore> Drop for ObservedWriter<W> {
    fn drop(&mut self) {
        self.control.shared.retired.store(true, Ordering::Release);
    }
}

impl<W: CommittedStore> StateStore for ObservedReader<W> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.shared.reads.fetch_add(1, Ordering::AcqRel);
        self.control.reader.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control.reader.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.shared.reads.fetch_add(1, Ordering::AcqRel);
        self.control.reader.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.control.shared.reads.fetch_add(1, Ordering::AcqRel);
        // Limited preflight reads use limit one. This is the actual full-log
        // adapter read used during engine startup, not a guessed call count.
        if prefix == [0x10] && limit == cluster::MAX_RETAINED_ENTRIES as usize + 1 {
            self.control
                .shared
                .full_scans
                .fetch_add(1, Ordering::AcqRel);
            if self
                .control
                .shared
                .fail_full_scan
                .swap(false, Ordering::AcqRel)
            {
                self.control
                    .shared
                    .fault_fired
                    .store(true, Ordering::Release);
                return Err(StorageError::Backend {
                    operation: "test startup read",
                    detail: "controlled read failure".to_owned(),
                });
            }
        }
        self.control.reader.scan_from(prefix, start, limit)
    }
}

pub(super) struct Evidence<W: CommittedStore> {
    pub(super) log: Control<W>,
    pub(super) state: Control<W>,
    baseline: [StoreSnapshot; 2],
    commits: [usize; 2],
    reads: [usize; 2],
}

impl<W: CommittedStore> Evidence<W> {
    pub(super) fn unchanged(&self) -> TestResult {
        assert_eq!(self.log.snapshot()?, self.baseline[0]);
        assert_eq!(self.state.snapshot()?, self.baseline[1]);
        assert_eq!([self.log.commits(), self.state.commits()], self.commits);
        Ok(())
    }
    pub(super) fn unread(&self) {
        assert_eq!([self.log.reads(), self.state.reads()], self.reads);
    }
}

pub(super) async fn prepare<B: Backend>(
    backend: &B,
    name: &str,
    seed: Seed,
) -> TestResult<([ExperimentalReplicaStores; 3], Vec<Evidence<B::Writer>>)> {
    let mut stores = Vec::new();
    let mut evidence = Vec::new();
    for (index, (log, state)) in backend.writers(name)?.into_iter().enumerate() {
        let log = Control::new(log);
        let state = Control::new(state);
        let mut log_store = ExperimentalLogStore::create(
            log.writer(),
            LogProfile::new(seed.ids[index], seed.streams[index])?,
        )?;
        let mut state_store =
            ExperimentalStateMachine::create(state.writer(), seed.streams[index])?;
        for chunk in seed.histories[index].chunks(15) {
            log_store.blocking_append(chunk.to_vec()).await?;
        }
        if let Some(vote) = seed.votes[index] {
            log_store.save_vote(&vote).await?;
        }
        if seed.applied[index] > 0 {
            state_store
                .apply(seed.histories[index][..seed.applied[index]].to_vec())
                .await?;
        }
        stores.push(
            ExperimentalReplicaStores::prepare(seed.ids[index], log_store, state_store).await?,
        );
        evidence.push(Evidence {
            baseline: [log.snapshot()?, state.snapshot()?],
            commits: [log.commits(), state.commits()],
            reads: [log.reads(), state.reads()],
            log,
            state,
        });
    }
    Ok((
        stores
            .try_into()
            .map_err(|_| "three prepared stores required")?,
        evidence,
    ))
}

pub(super) async fn retired<W: CommittedStore>(evidence: &[Evidence<W>]) -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        while !evidence
            .iter()
            .all(|row| row.log.retired() && row.state.retired())
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

pub(super) async fn reopen<W: CommittedStore>(
    evidence: &[Evidence<W>],
    ids: [u64; 3],
    streams: [CommittedStreamId; 3],
) -> TestResult<[ExperimentalReplicaStores; 3]> {
    assert!(
        evidence
            .iter()
            .all(|row| row.log.retired() && row.state.retired())
    );
    let mut stores = Vec::new();
    for (index, row) in evidence.iter().enumerate() {
        let log = ExperimentalLogStore::open(
            row.log.writer(),
            LogProfile::new(ids[index], streams[index])?,
        )?;
        let state = ExperimentalStateMachine::open(row.state.writer(), streams[index])?;
        stores.push(ExperimentalReplicaStores::prepare(ids[index], log, state).await?);
    }
    Ok(stores
        .try_into()
        .map_err(|_| "three reopened pairs required")?)
}

pub(super) async fn stop_prepared(stores: [ExperimentalReplicaStores; 3]) -> TestResult {
    for store in stores {
        store.shutdown().await?;
    }
    Ok(())
}
