use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    CommittedEntryMark, CommittedSend, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine, Timestamp,
};
use openraft::{BasicNode, Membership};
use serde::{Deserialize, Serialize};
use storage::{
    Key, MemoryReplicaStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch,
};

use crate::{
    experimental_log::QueueLogCommand, experimental_state_machine::response::LogQueueRefusal,
};

use super::*;

type Reader = <MemoryReplicaStore as CommittedStore>::Reader;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([7; 16]).expect("stream")
}

fn id(node: u64, index: u64) -> LogId {
    LogId::new(openraft::CommittedLeaderId::new(1, node), index)
}

fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(7, index),
        payload: EntryPayload::Blank,
    }
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}
fn entity() -> EntityPath {
    EntityPath::new("orders").expect("entity")
}

fn create(index: u64, issued_at: u64) -> LogEntry {
    LogEntry {
        log_id: id(7, index),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            namespace(),
            entity(),
            Timestamp::from_millis(issued_at),
            QueueConfig::default(),
        )),
    }
}

fn send(index: u64, body: &[u8], issued_at: u64) -> LogEntry {
    LogEntry {
        log_id: id(7, index),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            namespace(),
            entity(),
            Timestamp::from_millis(issued_at),
            CommittedSend {
                message_id: format!("message-{index}"),
                body: body.to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    }
}

fn membership() -> Membership<u64, BasicNode> {
    Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
        BTreeMap::from([
            (1, BasicNode::new("one")),
            (2, BasicNode::new("two")),
            (3, BasicNode::new("three")),
            (4, BasicNode::new("four")),
        ]),
    )
}

fn member(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(7, index),
        payload: EntryPayload::Membership(membership()),
    }
}

fn packet(entries: impl IntoIterator<Item = LogEntry>) -> PreparedApply {
    PreparedApply::from_entries(entries).expect("bounded valid apply")
}

#[derive(Default)]
struct Controls {
    commits: AtomicUsize,
    fail_before: AtomicUsize,
    fail_after: AtomicUsize,
    fail_read: AtomicBool,
}

struct Writer {
    inner: Arc<Mutex<MemoryReplicaStore>>,
    reader: ObservedReader,
    controls: Arc<Controls>,
}

struct Control {
    inner: Arc<Mutex<MemoryReplicaStore>>,
    reader: ObservedReader,
    controls: Arc<Controls>,
}

#[derive(Clone)]
struct ObservedReader {
    inner: Reader,
    controls: Arc<Controls>,
}

fn injected() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "SECRET path/body injected failure".into(),
    }
}

impl ObservedReader {
    fn check(&self) -> Result<(), StorageError> {
        if self.controls.fail_read.swap(false, Ordering::SeqCst) {
            Err(injected())
        } else {
            Ok(())
        }
    }
}

impl StateStore for ObservedReader {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.check()?;
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
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.check()?;
        self.inner.scan_from(prefix, start, limit)
    }
}

impl CommittedStore for Writer {
    type Reader = ObservedReader;
    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }
    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.lock().expect("writer lock").is_initialized()
    }
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let index = self.controls.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if self
            .controls
            .fail_before
            .compare_exchange(index, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(injected());
        }
        self.inner.lock().expect("writer lock").commit(batch)?;
        if self
            .controls
            .fail_after
            .compare_exchange(index, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(injected());
        }
        Ok(())
    }
}

impl Control {
    fn writer(&self) -> Writer {
        Writer {
            inner: self.inner.clone(),
            reader: self.reader.clone(),
            controls: self.controls.clone(),
        }
    }
    fn commits(&self) -> usize {
        self.controls.commits.load(Ordering::SeqCst)
    }
    fn snapshot(&self) -> StoreSnapshot {
        self.reader.snapshot().expect("snapshot")
    }
    fn inject(&self, batch: WriteBatch) {
        self.inner
            .lock()
            .expect("writer lock")
            .commit(batch)
            .expect("privileged test injection");
    }
}

fn controlled() -> Control {
    let writer = MemoryReplicaStore::new();
    let controls = Arc::new(Controls::default());
    let reader = ObservedReader {
        inner: writer.reader(),
        controls: controls.clone(),
    };
    Control {
        inner: Arc::new(Mutex::new(writer)),
        reader,
        controls,
    }
}

fn fresh() -> (StoreState<Writer>, Control) {
    let control = controlled();
    let state = StoreState::create(control.writer(), stream()).expect("fresh state");
    (state, control)
}

fn message(control: &Control, sequence: u64) -> Option<domain::MessageRecord> {
    StateMachine::new(control.reader.clone())
        .message(&namespace(), &entity(), SequenceNumber::new(sequence))
        .expect("message read")
}

#[test]
fn initial_state_is_durable_empty_and_cannot_be_recreated() {
    let (mut state, control) = fresh();
    assert_eq!(control.commits(), 1);
    assert_eq!(control.snapshot().entries().len(), 1);
    let (last, membership) = state.applied_state().expect("initial state");
    assert_eq!(last, None);
    assert_eq!(membership, StoredMembership::default());
    assert!(state.apply(packet([])).expect("empty apply").is_empty());
    assert_eq!(control.commits(), 1);
    drop(state);
    assert_eq!(
        StoreState::create(control.writer(), stream()).err(),
        Some(StateMachineError::InvalidState)
    );
    let mut reopened = StoreState::open(control.writer(), stream()).expect("reopen");
    assert_eq!(
        reopened.applied_state().expect("recovered initial state").0,
        None
    );
}

#[test]
fn membership_and_all_entry_progress_survive_recovery() {
    let (mut state, control) = fresh();
    assert_eq!(
        state
            .apply(packet([
                member(0),
                blank(1),
                create(2, 10),
                send(3, b"original", 20),
                blank(4)
            ]))
            .expect("mixed application"),
        vec![
            LogApplication::CheckpointOnly,
            LogApplication::CheckpointOnly,
            LogApplication::QueueCreated,
            LogApplication::Sent { sequence: 1 },
            LogApplication::CheckpointOnly
        ]
    );
    let expected = (
        Some(id(7, 4)),
        StoredMembership::new(Some(id(7, 0)), membership()),
    );
    assert_eq!(state.applied_state().expect("state"), expected);
    assert_eq!(
        message(&control, 1).expect("canonical message").body,
        b"original"
    );
    let before = control.snapshot();
    let commits = control.commits();
    drop(state);
    let mut reopened = StoreState::open(control.writer(), stream()).expect("open");
    assert_eq!(reopened.applied_state().expect("recovered state"), expected);
    assert_eq!(control.snapshot(), before);
    assert_eq!(control.commits(), commits);
}

#[test]
fn known_refusal_advances_checkpoint_and_watermark_without_business_changes() {
    let (mut state, control) = fresh();
    state
        .apply(packet([member(0), create(1, 10)]))
        .expect("setup");
    let prior_clock = control
        .reader
        .get(&domain::keys::clock())
        .expect("clock read");
    assert_eq!(
        state.apply(packet([create(2, 30)])).expect("known refusal"),
        vec![LogApplication::Refused(LogQueueRefusal::QueueAlreadyExists)]
    );
    assert_eq!(
        state.applied_state().expect("progress"),
        (
            Some(id(7, 2)),
            StoredMembership::new(Some(id(7, 0)), membership())
        )
    );
    assert_eq!(
        state
            .machine
            .checkpoint()
            .expect("checkpoint")
            .highest_timestamp(),
        Timestamp::from_millis(30)
    );
    assert_eq!(
        control
            .reader
            .get(&domain::keys::clock())
            .expect("clock read"),
        prior_clock
    );
    assert_eq!(
        state
            .apply(packet([send(3, b"too-old", 20)]))
            .expect("clock refusal"),
        vec![LogApplication::Refused(LogQueueRefusal::ClockRegression {
            last_applied_millis: 30,
            proposed_millis: 20
        })]
    );
    assert_eq!(message(&control, 1), None);
    state
        .apply(packet([send(4, b"accepted", 40)]))
        .expect("later accepted entry");
    assert_eq!(
        message(&control, 1).expect("canonical message").body,
        b"accepted"
    );
}

#[test]
fn latest_replay_reports_provenance_without_reconstructing_sent_outcome() {
    let (mut state, control) = fresh();
    state
        .apply(packet([create(0, 10), send(1, b"original", 20)]))
        .expect("setup");
    let before = control.snapshot();
    let commits = control.commits();
    assert_eq!(
        state
            .apply(packet([send(1, b"original", 20)]))
            .expect("exact latest replay"),
        vec![LogApplication::AlreadyApplied {
            entry: domain_id(id(7, 1))
        }]
    );
    assert_eq!(control.snapshot(), before);
    assert_eq!(control.commits(), commits);
    assert_eq!(message(&control, 2), None);
    assert_eq!(
        state
            .apply(packet([send(1, b"original", 20), send(2, b"next", 30)]))
            .expect("latest replay and new suffix"),
        vec![
            LogApplication::AlreadyApplied {
                entry: domain_id(id(7, 1))
            },
            LogApplication::Sent { sequence: 2 }
        ]
    );
}

#[test]
fn changed_latest_work_and_old_history_fail_closed_without_mutation() {
    for entry in [send(1, b"changed", 20), create(0, 10)] {
        let (mut state, control) = fresh();
        state
            .apply(packet([create(0, 10), send(1, b"original", 20)]))
            .expect("setup");
        let before = control.snapshot();
        let commits = control.commits();
        assert!(state.apply(packet([entry])).is_err());
        assert_eq!(
            state.applied_state().err(),
            Some(StateMachineError::Poisoned)
        );
        assert_eq!(
            state.apply(packet([blank(2)])).err(),
            Some(StateMachineError::Poisoned)
        );
        assert_eq!(control.snapshot(), before);
        assert_eq!(control.commits(), commits);
    }
}

#[test]
fn first_entry_requires_exact_full_successor_not_only_index_or_term() {
    for entry in [
        LogEntry {
            log_id: id(6, 1),
            payload: EntryPayload::Blank,
        },
        blank(2),
    ] {
        let (mut state, control) = fresh();
        state.apply(packet([create(0, 10)])).expect("setup");
        let before = control.snapshot();
        let commits = control.commits();
        assert_eq!(
            state.apply(packet([entry])).err(),
            Some(StateMachineError::InvalidApply)
        );
        assert_eq!(control.snapshot(), before);
        assert_eq!(control.commits(), commits);
        assert_eq!(
            state.applied_state().err(),
            Some(StateMachineError::Poisoned)
        );
    }
}

#[test]
fn full_apply_accepts_more_than_append_count_and_byte_caps() {
    let (mut state, control) = fresh();
    let body = vec![0x61; 128 * 1024];
    let entries = std::iter::once(create(0, 10))
        .chain((1..=33).map(|index| send(index, &body, 20 + index)))
        .collect::<Vec<_>>();
    let input = packet(entries);
    assert!(input.encoded_bytes() > crate::experimental_log::MAX_APPEND_BYTES);
    let responses = state.apply(input).expect("whole committed range");
    assert_eq!(responses.len(), 34);
    assert_eq!(responses[0], LogApplication::QueueCreated);
    for (index, response) in responses.iter().enumerate().skip(1) {
        assert_eq!(
            *response,
            LogApplication::Sent {
                sequence: index as u64
            }
        );
    }
    assert_eq!(state.applied_state().expect("progress").0, Some(id(7, 33)));
    assert_eq!(
        message(&control, 33).expect("last canonical message").body,
        body
    );
}

#[test]
fn all_256_entries_return_exactly_one_response_each() {
    let (mut state, control) = fresh();
    let responses = state
        .apply(packet((0..256).map(blank)))
        .expect("full entry cap");
    assert_eq!(responses, vec![LogApplication::CheckpointOnly; 256]);
    assert_eq!(state.applied_state().expect("progress").0, Some(id(7, 255)));
    assert_eq!(control.snapshot().entries().len(), 1);
    assert_eq!(control.commits(), 257);
}

#[test]
fn physical_middle_failure_keeps_only_the_durable_prefix_and_blocks_live_reads() {
    for after in [false, true] {
        let (mut state, control) = fresh();
        let fault = control.commits() + 3;
        if after {
            control.controls.fail_after.store(fault, Ordering::SeqCst);
        } else {
            control.controls.fail_before.store(fault, Ordering::SeqCst);
        }
        let error = state
            .apply(packet([
                create(0, 10),
                blank(1),
                send(2, b"original", 20),
                blank(3),
            ]))
            .expect_err("indeterminate middle failure");
        assert_eq!(error, StateMachineError::Storage);
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        assert_eq!(
            state.applied_state().err(),
            Some(StateMachineError::Poisoned)
        );
        assert_eq!(
            state.apply(packet([blank(3)])).err(),
            Some(StateMachineError::Poisoned)
        );
        let checkpoint = state
            .machine
            .checkpoint()
            .expect("private recovery diagnostic");
        assert_eq!(
            checkpoint.last().map(|mark| mark.id),
            Some(domain_id(id(7, if after { 2 } else { 1 })))
        );
        assert_eq!(control.commits(), fault);
        assert_eq!(message(&control, 1).is_some(), after);
        drop(state);
        let mut reopened =
            StoreState::open(control.writer(), stream()).expect("authoritative recovery");
        if after {
            assert_eq!(
                reopened
                    .apply(packet([send(2, b"original", 20)]))
                    .expect("latest replay"),
                vec![LogApplication::AlreadyApplied {
                    entry: domain_id(id(7, 2))
                }]
            );
        } else {
            reopened
                .apply(packet([send(2, b"original", 20)]))
                .expect("remaining original entry");
        }
        reopened
            .apply(packet([blank(3), send(4, b"next", 30)]))
            .expect("remaining suffix");
        assert_eq!(
            message(&control, 1).expect("original sequence").body,
            b"original"
        );
        assert_eq!(
            message(&control, 2).expect("continued sequence").body,
            b"next"
        );
        assert_eq!(message(&control, 3), None);
    }
}

#[test]
fn physical_checkpoint_read_error_poisoning_is_not_empty_progress() {
    let (mut state, control) = fresh();
    control.controls.fail_read.store(true, Ordering::SeqCst);
    assert_eq!(
        state.applied_state().err(),
        Some(StateMachineError::Storage)
    );
    assert_eq!(
        state.apply(packet([blank(0)])).err(),
        Some(StateMachineError::Poisoned)
    );
    assert_eq!(control.commits(), 1);
}

#[test]
fn foreign_stream_and_uninitialized_data_cannot_be_adopted() {
    let control = controlled();
    assert_eq!(
        StoreState::open(control.writer(), stream()).err(),
        Some(StateMachineError::InvalidState)
    );
    let state = StoreState::create(control.writer(), stream()).expect("initialize");
    let before = control.snapshot();
    drop(state);
    let foreign = CommittedStreamId::new([8; 16]).expect("foreign stream");
    assert_eq!(
        StoreState::open(control.writer(), foreign).err(),
        Some(StateMachineError::InvalidState)
    );
    assert_eq!(control.snapshot(), before);
}

#[test]
fn opaque_domain_membership_must_match_the_pinned_canonical_schema() {
    for (schema, payload) in [
        (2, encode_membership(&membership()).expect("membership")),
        (
            MEMBERSHIP_SCHEMA_VERSION,
            b"SECRET malformed membership".to_vec(),
        ),
    ] {
        let control = controlled();
        let mut domain = CommittedStateMachine::create(control.writer(), stream())
            .expect("domain initialization");
        domain
            .apply_committed(
                &CommittedCheckpointUpdate {
                    stream: stream(),
                    expected_previous: None,
                    entry: domain_id(id(7, 0)),
                },
                &CommittedQueueWork::Membership {
                    schema_version: schema,
                    payload,
                },
            )
            .expect("the domain stores bounded opaque membership");
        let before = control.snapshot();
        drop(domain);
        let error = StoreState::open(control.writer(), stream())
            .err()
            .expect("incompatible membership");
        assert_eq!(error, StateMachineError::InvalidState);
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        assert_eq!(control.snapshot(), before);
    }
}

#[derive(Serialize, Deserialize)]
struct CheckpointMirror {
    stream: CommittedStreamId,
    last: Option<CommittedEntryMark>,
    previous: Option<CommittedEntryMark>,
    highest_timestamp: u64,
    membership: Option<MembershipMirror>,
}

#[derive(Serialize, Deserialize)]
struct MembershipMirror {
    source: CommittedEntryId,
    schema_version: u16,
    payload: Vec<u8>,
}

#[test]
fn recovery_rejects_same_term_node_regression_in_previous_or_membership() {
    for corrupt_member in [false, true] {
        let (mut state, control) = fresh();
        state
            .apply(packet([create(0, 10), blank(1), blank(2)]))
            .expect("setup");
        drop(state);
        let key = vec![0x12];
        let bytes = control
            .reader
            .get(&key)
            .expect("checkpoint read")
            .expect("checkpoint");
        let mut checkpoint: CheckpointMirror =
            postcard::from_bytes(&bytes[5..]).expect("frozen checkpoint schema");
        if corrupt_member {
            checkpoint.membership = Some(MembershipMirror {
                source: domain_id(id(8, 0)),
                schema_version: MEMBERSHIP_SCHEMA_VERSION,
                payload: encode_membership(&membership()).expect("membership"),
            });
        } else {
            checkpoint.previous.as_mut().expect("previous").id.node_id = 8;
        }
        let mut bytes = b"SWYC\x01".to_vec();
        bytes.extend(postcard::to_stdvec(&checkpoint).expect("canonical test checkpoint"));
        control.inject(WriteBatch::default().put(key, bytes));
        let before = control.snapshot();
        let commits = control.commits();
        assert_eq!(
            StoreState::open(control.writer(), stream()).err(),
            Some(StateMachineError::InvalidState)
        );
        assert_eq!(control.snapshot(), before);
        assert_eq!(control.commits(), commits);
    }
}
