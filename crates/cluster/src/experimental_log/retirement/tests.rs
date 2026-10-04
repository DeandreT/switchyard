use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedSend, CommittedStateMachine,
    CommittedStreamId, EntityPath, NamespaceName, QueueConfig,
};
use openraft::{
    BasicNode, Membership,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{
    CommittedStore, MemoryReplicaStore, StateStore, StorageError, StoreSnapshot, WriteBatch,
};

use super::super::{ExperimentalLogStore, LogEntry, QueueLogCommand, types::EncodedAppend};
use super::*;

mod lifecycle;

const DEADLINE: Duration = Duration::from_secs(5);
const READ_ERROR: u8 = 1;
const READ_PANIC: u8 = 2;
const COMMIT_ERROR: u8 = 3;
const DROP_PANIC: u8 = 4;
const DROP_BLOCK: u8 = 5;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([72; 16]).unwrap()
}
fn profile() -> LogProfile {
    LogProfile::new(7, stream()).unwrap()
}
fn id(index: u64) -> LogId {
    if index == 0 {
        LogId::default()
    } else {
        LogId::new(openraft::CommittedLeaderId::new(1, 7), index)
    }
}
fn membership(address: &str) -> Membership<u64, BasicNode> {
    Membership::new(
        vec![BTreeSet::from([7, 8, 9])],
        [7, 8, 9]
            .into_iter()
            .map(|node| (node, BasicNode::new(format!("{address}-{node}"))))
            .collect::<BTreeMap<_, _>>(),
    )
}
fn entries() -> Vec<LogEntry> {
    let namespace = NamespaceName::new("private-namespace").unwrap();
    let entity = EntityPath::new("private-path").unwrap();
    vec![
        LogEntry {
            log_id: id(0),
            payload: EntryPayload::Membership(membership("initial")),
        },
        LogEntry {
            log_id: id(1),
            payload: EntryPayload::Normal(QueueLogCommand::create_queue(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(20),
                QueueConfig::default(),
            )),
        },
        LogEntry {
            log_id: id(2),
            payload: EntryPayload::Blank,
        },
        LogEntry {
            log_id: id(3),
            payload: EntryPayload::Normal(QueueLogCommand::send(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(30),
                CommittedSend {
                    message_id: "private-message".into(),
                    body: b"private body not retained".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        },
        LogEntry {
            log_id: id(4),
            payload: EntryPayload::Membership(membership("later")),
        },
        LogEntry {
            log_id: id(5),
            payload: EntryPayload::Normal(QueueLogCommand::send(
                namespace,
                entity,
                Timestamp::from_millis(40),
                CommittedSend {
                    message_id: "unapplied-tail".into(),
                    body: b"later body".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        },
    ]
}
fn state(entries: &[LogEntry]) -> StoreState<MemoryReplicaStore> {
    let mut state = StoreState::create(MemoryReplicaStore::new(), profile()).unwrap();
    if !entries.is_empty() {
        state
            .append(&EncodedAppend::from_entries(entries.to_vec()).unwrap())
            .unwrap();
    }
    state
}
fn checkpoints(entries: &[LogEntry]) -> Vec<CommittedCheckpoint> {
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()).unwrap();
    let mut checkpoints = vec![machine.checkpoint().unwrap()];
    for entry in entries {
        let work = match entry.payload.clone() {
            EntryPayload::Blank => CommittedQueueWork::Blank,
            EntryPayload::Membership(value) => CommittedQueueWork::Membership {
                schema_version: MEMBERSHIP_SCHEMA_VERSION,
                payload: encode_membership(&value).unwrap(),
            },
            EntryPayload::Normal(value) => value.into_committed_work(),
        };
        machine
            .apply_committed(
                &CommittedCheckpointUpdate {
                    stream: stream(),
                    expected_previous: checkpoints.last().unwrap().last(),
                    entry: domain_id(entry.log_id),
                },
                &work,
            )
            .unwrap();
        checkpoints.push(machine.checkpoint().unwrap());
    }
    checkpoints
}

#[test]
fn full_retained_report_matches_every_actual_applied_prefix_and_unapplied_tail() {
    let entries = entries();
    let state = state(&entries);
    let report = state.retirement_report().unwrap();
    assert_eq!(report.profile(), &profile());
    assert_eq!(report.retention().last_present, Some(id(5)));
    assert_eq!(report.prefixes.len(), 6);
    assert_eq!(report.membership_events.len(), 2);
    for checkpoint in checkpoints(&entries) {
        assert!(report.matches_checkpoint(&checkpoint));
    }
    let debug = format!("{report:?}");
    for private in [
        "private body",
        "private-message",
        "private-path",
        "private-namespace",
        "later body",
    ] {
        assert!(!debug.contains(private));
    }
}

#[test]
fn full_marks_previous_chain_time_membership_and_stream_are_not_interchangeable() {
    let entries = entries();
    let checkpoints = checkpoints(&entries);
    let report = state(&entries).retirement_report().unwrap();
    let checkpoint = &checkpoints[4];
    let mut wrong = report.clone();
    wrong.prefixes[3].mark.id.node_id += 1;
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.prefixes[2].mark.fingerprint[0] ^= 1;
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.prefixes[3].highest_timestamp = Timestamp::from_millis(31);
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.membership_events[0].payload = encode_membership(&membership("wrong")).unwrap();
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.membership_events[0].schema_version += 1;
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.membership_events[0].source.node_id += 1;
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.profile = LogProfile::new(7, CommittedStreamId::new([73; 16]).unwrap()).unwrap();
    assert!(!wrong.matches_checkpoint(&checkpoints[0]));
    assert!(!wrong.matches_checkpoint(checkpoint));
    let mut wrong = report.clone();
    wrong.prefixes[5].mark.fingerprint[0] ^= 1;
    assert!(wrong.matches_checkpoint(checkpoint));
    assert!(!wrong.matches_checkpoint(&checkpoints[6]));
}

#[test]
fn same_full_id_with_different_body_changes_the_tail_content_chain() {
    let original = entries();
    let mut changed = original.clone();
    changed[5].payload = EntryPayload::Normal(QueueLogCommand::send(
        NamespaceName::new("private-namespace").unwrap(),
        EntityPath::new("private-path").unwrap(),
        Timestamp::from_millis(40),
        CommittedSend {
            message_id: "unapplied-tail".into(),
            body: b"different body".to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    ));
    let report = state(&changed).retirement_report().unwrap();
    let checkpoints = checkpoints(&original);
    assert!(report.matches_checkpoint(&checkpoints[5]));
    assert!(!report.matches_checkpoint(&checkpoints[6]));
    assert_eq!(
        report.prefixes[5].mark.id,
        checkpoints[6].last().unwrap().id
    );
}

#[test]
fn complete_persisted_vote_is_retained_without_normalizing_committed_or_leader_bits() {
    let mut state = state(&entries());
    let before = state.retirement_report().unwrap();
    assert_eq!(before.vote, None);
    let uncommitted = LogVote::new(2, 7);
    state.save_vote(uncommitted).unwrap();
    let uncommitted_report = state.retirement_report().unwrap();
    assert_eq!(uncommitted_report.vote, Some(uncommitted));
    state.save_vote(LogVote::new_committed(2, 7)).unwrap();
    let committed = state.retirement_report().unwrap();
    assert_eq!(committed.vote, Some(LogVote::new_committed(2, 7)));
    assert_ne!(uncommitted_report, committed);
    state.save_vote(LogVote::new_committed(3, 9)).unwrap();
    assert_eq!(
        state.retirement_report().unwrap().vote,
        Some(LogVote::new_committed(3, 9))
    );
}

#[test]
fn report_bounds_all_retained_marks_and_membership_events_and_refuses_purged_prefixes() {
    let entries = (0..MAX_RETAINED_ENTRIES)
        .map(|index| LogEntry {
            log_id: id(index),
            payload: EntryPayload::Membership(membership("bounded")),
        })
        .collect::<Vec<_>>();
    let mut state = state(&[]);
    for chunk in entries.chunks(super::super::MAX_APPEND_ENTRIES) {
        state
            .append(&EncodedAppend::from_entries(chunk.to_vec()).unwrap())
            .unwrap();
    }
    let report = state.retirement_report().unwrap();
    assert_eq!(report.prefixes.len(), MAX_RETAINED_ENTRIES as usize);
    assert_eq!(
        report.membership_events.len(),
        MAX_RETAINED_ENTRIES as usize
    );
    state.purge(id(0)).unwrap();
    assert_eq!(state.retirement_report(), Err(LogStorageError::Corrupt));
    assert_eq!(state.retirement_report(), Err(LogStorageError::Poisoned));
}

#[test]
fn a_poisoned_sink_never_generates_or_publishes_healthy_evidence() {
    let sink = Arc::new(ReportSink::default());
    let mut receiver = sink.enable().unwrap();
    let poisoned = sink.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned.0.lock().unwrap();
            panic!("test-only retirement sink poison");
        })
        .join()
        .is_err()
    );
    let generated = AtomicBool::new(false);
    sink.finish(|| {
        generated.store(true, Ordering::SeqCst);
        state(&[]).retirement_report()
    });
    assert!(!generated.load(Ordering::SeqCst));
    assert_eq!(receiver.try_recv().unwrap(), Err(LogStorageError::Panicked));
}

type MemoryReader = <MemoryReplicaStore as CommittedStore>::Reader;
struct Signals {
    mode: AtomicU8,
    reads: AtomicUsize,
    drop_entered: AtomicBool,
    changed: tokio::sync::Notify,
    release: Mutex<bool>,
    wake: Condvar,
}
struct Writer {
    inner: MemoryReplicaStore,
    signals: Arc<Signals>,
}
#[derive(Clone)]
struct Reader {
    inner: MemoryReader,
    signals: Arc<Signals>,
}

fn controlled() -> (Writer, Arc<Signals>) {
    let signals = Arc::new(Signals {
        mode: AtomicU8::new(0),
        reads: AtomicUsize::new(0),
        drop_entered: AtomicBool::new(false),
        changed: tokio::sync::Notify::new(),
        release: Mutex::new(false),
        wake: Condvar::new(),
    });
    (
        Writer {
            inner: MemoryReplicaStore::new(),
            signals: signals.clone(),
        },
        signals,
    )
}

impl CommittedStore for Writer {
    type Reader = Reader;
    fn reader(&self) -> Self::Reader {
        Reader {
            inner: self.inner.reader(),
            signals: self.signals.clone(),
        }
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.signals.mode.load(Ordering::SeqCst) == COMMIT_ERROR {
            return Err(StorageError::Backend {
                operation: "test log commit",
                detail: "private failure".into(),
            });
        }
        self.inner.commit(batch)
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }
}
impl Drop for Writer {
    fn drop(&mut self) {
        match self.signals.mode.load(Ordering::SeqCst) {
            DROP_PANIC => panic!("test-only actual writer Drop panic"),
            DROP_BLOCK => {
                self.signals.drop_entered.store(true, Ordering::SeqCst);
                self.signals.changed.notify_waiters();
                let mut release = self.signals.release.lock().unwrap();
                while !*release {
                    let (next, timeout) =
                        self.signals.wake.wait_timeout(release, DEADLINE).unwrap();
                    release = next;
                    assert!(
                        !timeout.timed_out() || *release,
                        "test writer Drop gate timed out"
                    );
                }
            }
            _ => {}
        }
    }
}
impl StateStore for Reader {
    fn get(&self, key: &[u8]) -> Result<Option<storage::Value>, StorageError> {
        self.signals.reads.fetch_add(1, Ordering::SeqCst);
        if key == super::super::types::PROFILE_KEY {
            match self.signals.mode.load(Ordering::SeqCst) {
                READ_ERROR => {
                    return Err(StorageError::Backend {
                        operation: "test report read",
                        detail: "private failure".into(),
                    });
                }
                READ_PANIC => panic!("test-only report read panic"),
                _ => {}
            }
        }
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(storage::Key, storage::Value)>, StorageError> {
        self.signals.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}
