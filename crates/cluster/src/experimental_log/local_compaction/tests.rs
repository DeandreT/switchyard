use super::*;
use crate::{LogEntry, LogId, LogProfile, LogVote, QueueLogCommand};
use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueWork,
    CommittedStateMachine, CommittedStreamId, Timestamp,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use storage::{
    CommittedStore, MemoryReplicaStore, MemoryStore, StateStore, StorageError, WriteBatch,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
mod codec_cases;
mod proof_cases;

fn profile() -> TestResult<LogProfile> {
    Ok(LogProfile::new(7, CommittedStreamId::new([93; 16])?)?)
}
fn id(index: u64) -> LogId {
    if index == 0 {
        LogId::default()
    } else {
        LogId::new(openraft::CommittedLeaderId::new(1, 7), index)
    }
}
fn entries() -> TestResult<Vec<LogEntry>> {
    Ok(vec![
        LogEntry {
            log_id: id(0),
            payload: openraft::EntryPayload::Membership(openraft::Membership::new(
                vec![BTreeSet::from([7])],
                BTreeMap::from([(7, openraft::BasicNode::new("node-7"))]),
            )),
        },
        LogEntry {
            log_id: id(1),
            payload: openraft::EntryPayload::Normal(QueueLogCommand::create_queue(
                domain::NamespaceName::new("tenant")?,
                domain::EntityPath::new("orders")?,
                Timestamp::from_millis(10),
                domain::QueueConfig::default(),
            )),
        },
        LogEntry {
            log_id: id(2),
            payload: openraft::EntryPayload::Normal(QueueLogCommand::send(
                domain::NamespaceName::new("tenant")?,
                domain::EntityPath::new("orders")?,
                Timestamp::from_millis(12),
                domain::CommittedSend {
                    message_id: "PRIVATE-2".into(),
                    body: vec![2; 32],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        },
    ])
}
fn source(
    entries: &[LogEntry],
) -> TestResult<(
    CommittedCheckpoint,
    crate::EncodedNativeSnapshotMetadata,
    openraft::SnapshotMeta<u64, openraft::BasicNode>,
)> {
    let profile = profile()?;
    let mut state = CommittedStateMachine::create(MemoryReplicaStore::new(), profile.stream())?;
    for entry in entries {
        let previous = state.checkpoint()?.last();
        let entry_id = CommittedEntryId {
            term: entry.log_id.leader_id.term,
            node_id: entry.log_id.leader_id.node_id,
            index: entry.log_id.index,
        };
        let work = match entry.payload.clone() {
            openraft::EntryPayload::Blank => CommittedQueueWork::Blank,
            openraft::EntryPayload::Normal(command) => command.into_committed_work(),
            openraft::EntryPayload::Membership(membership) => CommittedQueueWork::Membership {
                schema_version: crate::experimental_log::MEMBERSHIP_SCHEMA_VERSION,
                payload: crate::experimental_log::encode_membership(&membership)?,
            },
        };
        state.apply_committed(
            &CommittedCheckpointUpdate {
                stream: profile.stream(),
                expected_previous: previous,
                entry: entry_id,
            },
            &work,
        )?;
    }
    let checkpoint = state.checkpoint()?;
    let image = state.export_create_send_image()?;
    let metadata = crate::EncodedNativeSnapshotMetadata::encode(image.as_bytes())?;
    let projection =
        crate::DecodedNativeSnapshotPair::decode(metadata.as_bytes(), image.as_bytes())?
            .snapshot_meta()?;
    Ok((checkpoint, metadata, projection))
}

// An explicit white-box backend: reader mutation models corruption between two
// owner operations, not a supported concurrent writer or compare-and-swap.
#[derive(Clone)]
struct Control {
    records: MemoryStore,
    initialized: Arc<AtomicBool>,
    commits: Arc<AtomicUsize>,
}
struct Writer(Control);
impl Writer {
    fn new() -> (Self, Control) {
        let control = Control {
            records: MemoryStore::default(),
            initialized: Arc::new(AtomicBool::new(false)),
            commits: Arc::new(AtomicUsize::new(0)),
        };
        (Self(control.clone()), control)
    }
}
impl CommittedStore for Writer {
    type Reader = MemoryStore;
    fn reader(&self) -> Self::Reader {
        self.0.records.clone()
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        Ok(self.0.initialized.load(Ordering::SeqCst))
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.commits.fetch_add(1, Ordering::SeqCst);
        self.0.records.apply(batch)?;
        self.0.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }
}
fn put(control: &Control, key: &[u8], value: &[u8]) -> TestResult {
    let mut batch = WriteBatch::default();
    batch.push_put(key, value);
    control.records.apply(batch)?;
    Ok(())
}
