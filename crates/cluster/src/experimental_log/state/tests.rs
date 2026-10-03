use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
};

use domain::{CommittedSend, CommittedStreamId, EntityPath, NamespaceName, Timestamp};
use storage::{Key, MemoryReplicaStore, StoreSnapshot, Value};

use super::super::types::QueueLogCommand;
use super::*;

type Reader = <MemoryReplicaStore as CommittedStore>::Reader;

fn profile() -> LogProfile {
    LogProfile::new(7, CommittedStreamId::new([9; 16]).expect("valid stream"))
        .expect("valid profile")
}

fn id(term: u64, node: u64, index: u64) -> LogId {
    LogId::new(openraft::CommittedLeaderId::new(term, node), index)
}

fn blank(term: u64, node: u64, index: u64) -> LogEntry {
    LogEntry {
        log_id: id(term, node, index),
        payload: openraft::EntryPayload::Blank,
    }
}

fn blanks(start: u64, end: u64) -> EncodedAppend {
    EncodedAppend::from_entries((start..end).map(|index| blank(1, 7, index)))
        .expect("bounded blank append")
}

fn send(index: u64, body: Vec<u8>) -> LogEntry {
    LogEntry {
        log_id: id(1, 7, index),
        payload: openraft::EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant").expect("namespace"),
            EntityPath::new("orders").expect("entity"),
            Timestamp::from_millis(1000),
            CommittedSend {
                message_id: format!("message-{index}"),
                body,
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    }
}

fn snapshot(reader: &impl StateStore) -> StoreSnapshot {
    reader.snapshot().expect("snapshot")
}

fn initialized_records(progress: &LogProgress) -> WriteBatch {
    WriteBatch::default()
        .put(
            PROFILE_KEY,
            codec::encode_profile(&profile()).expect("profile encoding"),
        )
        .put(
            PROGRESS_KEY,
            codec::encode_progress(progress).expect("progress encoding"),
        )
}

#[derive(Default)]
struct Controls {
    commits: AtomicUsize,
    fail_before: AtomicBool,
    fail_after: AtomicBool,
    fail_read: AtomicBool,
}

struct ObservedWriter {
    inner: MemoryReplicaStore,
    controls: Arc<Controls>,
}

#[derive(Clone)]
struct ObservedReader {
    inner: Reader,
    controls: Arc<Controls>,
}

fn injected() -> StorageError {
    StorageError::Backend {
        operation: "controlled operation",
        detail: "SECRET backend path/body".into(),
    }
}

impl ObservedReader {
    fn check(&self) -> Result<(), StorageError> {
        if self.controls.fail_read.swap(false, AtomicOrdering::SeqCst) {
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

impl CommittedStore for ObservedWriter {
    type Reader = ObservedReader;

    fn reader(&self) -> Self::Reader {
        ObservedReader {
            inner: self.inner.reader(),
            controls: Arc::clone(&self.controls),
        }
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.commits.fetch_add(1, AtomicOrdering::SeqCst);
        if self
            .controls
            .fail_before
            .swap(false, AtomicOrdering::SeqCst)
        {
            return Err(injected());
        }
        self.inner.commit(batch)?;
        if self.controls.fail_after.swap(false, AtomicOrdering::SeqCst) {
            return Err(injected());
        }
        Ok(())
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }
}

fn observed() -> (StoreState<ObservedWriter>, ObservedReader, Arc<Controls>) {
    let controls = Arc::new(Controls::default());
    let writer = ObservedWriter {
        inner: MemoryReplicaStore::new(),
        controls: Arc::clone(&controls),
    };
    let reader = writer.reader();
    let state = StoreState::create(writer, profile()).expect("fresh log");
    controls.commits.store(0, AtomicOrdering::SeqCst);
    (state, reader, controls)
}

#[test]
fn initialization_is_one_batch_and_cannot_be_recreated() {
    let controls = Arc::new(Controls::default());
    let writer = ObservedWriter {
        inner: MemoryReplicaStore::new(),
        controls: Arc::clone(&controls),
    };
    let reader = writer.reader();
    let state = StoreState::create(writer, profile()).expect("create log");
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(snapshot(&reader).entries().len(), 2);
    assert!(
        reader
            .inner
            .get(PROFILE_KEY)
            .expect("profile read")
            .is_some()
    );
    assert!(
        reader
            .inner
            .get(PROGRESS_KEY)
            .expect("progress read")
            .is_some()
    );
    assert_eq!(state.read_vote().expect("vote"), None);
    assert_eq!(state.log_state().expect("log state").last_log_id, None);
    assert_eq!(
        StoreState::create(state.writer, profile()).err(),
        Some(LogStateError::InvalidProfile)
    );
}

#[test]
fn failed_initialization_does_not_turn_missing_progress_into_pristine() {
    for after in [false, true] {
        let controls = Arc::new(Controls::default());
        controls.fail_before.store(!after, AtomicOrdering::SeqCst);
        controls.fail_after.store(after, AtomicOrdering::SeqCst);
        let writer = ObservedWriter {
            inner: MemoryReplicaStore::new(),
            controls,
        };
        let reader = writer.reader();
        assert_eq!(
            StoreState::create(writer, profile()).err(),
            Some(LogStateError::Storage)
        );
        let entries = snapshot(&reader).entries().to_vec();
        assert_eq!(entries.len(), if after { 2 } else { 0 });
        assert!(
            entries
                .iter()
                .all(|(key, _)| key == PROFILE_KEY || key == PROGRESS_KEY)
        );
    }
}

#[test]
fn startup_rejects_unknown_keys_missing_progress_and_wrong_profile() {
    for batch in [
        initialized_records(&LogProgress::default()).put(vec![3], vec![0]),
        WriteBatch::default().put(
            PROFILE_KEY,
            codec::encode_profile(&profile()).expect("profile"),
        ),
        initialized_records(&LogProgress::default()).put(vec![ENTRY_PREFIX], vec![0]),
    ] {
        let mut writer = MemoryReplicaStore::new();
        writer.commit(batch).expect("inject records");
        assert_eq!(
            StoreState::open(writer, profile()).err(),
            Some(LogStateError::Corrupt)
        );
    }
    let writer = MemoryReplicaStore::new();
    let state = StoreState::create(writer, profile()).expect("initial log");
    let foreign = LogProfile::new(8, profile().stream()).expect("foreign profile");
    assert_eq!(
        StoreState::open(state.writer, foreign).err(),
        Some(LogStateError::InvalidProfile)
    );
}

#[test]
fn startup_checks_canonical_key_identity_and_complete_bookkeeping() {
    let entry = codec::encode_entry(&blank(1, 7, 0)).expect("entry");
    let progress = LogProgress {
        last_present: Some(entry.id()),
        retained_entries: 1,
        retained_bytes: entry.encoded_len() as u64,
        ..LogProgress::default()
    };
    for (key, bytes, byte_delta) in [
        (entry_key(1), entry.bytes().to_vec(), 0),
        (entry_key(0), [entry.bytes(), &[0]].concat(), 0),
        (entry_key(0), entry.bytes().to_vec(), 1),
    ] {
        let mut progress = progress.clone();
        progress.retained_bytes += byte_delta;
        let mut writer = MemoryReplicaStore::new();
        writer
            .commit(initialized_records(&progress).put(key, bytes))
            .expect("inject row");
        assert_eq!(
            StoreState::open(writer, profile()).err(),
            Some(LogStateError::Corrupt)
        );
    }
}

#[test]
fn whole_append_validation_prevents_partial_gap_or_conflict_writes() {
    let (mut state, reader, controls) = observed();
    state.append(&blanks(0, 2)).expect("original rows");
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    let gap = EncodedAppend::from_entries([blank(1, 7, 2), blank(1, 7, 4)]).expect("encoded gap");
    assert_eq!(state.append(&gap), Err(LogStateError::InvalidAppend));
    let conflict =
        EncodedAppend::from_entries([blank(1, 8, 1), blank(1, 8, 2)]).expect("encoded conflict");
    assert_eq!(state.append(&conflict), Err(LogStateError::Conflict));
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
}

#[test]
fn identical_overlap_does_not_recount_and_can_extend_the_tail() {
    let (mut state, reader, controls) = observed();
    state.append(&blanks(0, 4)).expect("initial append");
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    state.append(&blanks(1, 3)).expect("matching overlap");
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
    state.append(&blanks(2, 6)).expect("overlap and new suffix");
    assert_eq!(state.progress.retained_entries, 6);
    assert_eq!(
        state.log_state().expect("state").last_log_id,
        Some(id(1, 7, 5))
    );
    let reopened = StoreState::open(state.writer, profile()).expect("reopen");
    assert_eq!(
        reopened
            .read_full(OwnedLogRange::from_range(..))
            .expect("read")
            .len(),
        6
    );
}

#[test]
fn same_log_id_with_changed_body_is_not_idempotent() {
    let (mut state, reader, controls) = observed();
    state
        .append(
            &EncodedAppend::from_entries([send(0, b"original".to_vec())]).expect("first packet"),
        )
        .expect("first append");
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    assert_eq!(
        state.append(
            &EncodedAppend::from_entries([send(0, b"changed".to_vec())]).expect("second packet")
        ),
        Err(LogStateError::Conflict)
    );
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
}

#[test]
fn votes_preserve_default_term_node_and_committed_ordering() {
    let (mut state, reader, controls) = observed();
    for vote in [
        LogVote::new(1, 7),
        LogVote::new(1, 8),
        LogVote::new_committed(1, 8),
        LogVote::new(2, 1),
    ] {
        state.save_vote(vote).expect("ascending vote");
        assert_eq!(state.read_vote().expect("vote"), Some(vote));
    }
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    state.save_vote(LogVote::new(2, 1)).expect("same vote");
    assert_eq!(
        state.save_vote(LogVote::new_committed(1, 8)),
        Err(LogStateError::VoteRegression)
    );
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
    let state = StoreState::open(state.writer, profile()).expect("reopen vote");
    assert_eq!(
        state.read_vote().expect("reopened vote"),
        Some(LogVote::new(2, 1))
    );
}

#[test]
fn truncation_preserves_vote_and_purged_baseline() {
    let (mut state, reader, controls) = observed();
    state.append(&blanks(0, 6)).expect("append");
    state.save_vote(LogVote::new_committed(2, 7)).expect("vote");
    state.purge(id(1, 7, 1)).expect("purge prefix");
    state
        .truncate(id(9, 9, 4))
        .expect("index-scoped suffix truncation");
    assert_eq!(
        state.log_state().expect("state").last_log_id,
        Some(id(1, 7, 3))
    );
    state.truncate(id(9, 9, 2)).expect("remove remaining rows");
    assert_eq!(state.progress.retained_entries, 0);
    assert_eq!(state.progress.retained_bytes, 0);
    assert_eq!(
        state.log_state().expect("state").last_log_id,
        Some(id(1, 7, 1))
    );
    assert_eq!(
        state.read_vote().expect("vote"),
        Some(LogVote::new_committed(2, 7))
    );
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    assert_eq!(
        state.truncate(id(1, 7, 1)),
        Err(LogStateError::InvalidTruncate)
    );
    state.truncate(id(9, 9, 100)).expect("absent suffix");
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
    state.append(&blanks(2, 4)).expect("replacement suffix");
    StoreState::open(state.writer, profile()).expect("valid reopen");
}

#[test]
fn purge_beyond_tail_advances_full_boundary_without_resurrection() {
    let (mut state, reader, _) = observed();
    state.append(&blanks(0, 3)).expect("append");
    state
        .purge(id(2, 8, 9))
        .expect("snapshot boundary beyond tail");
    assert_eq!(state.progress.retained_entries, 0);
    assert_eq!(
        state.log_state().expect("state").last_purged_log_id,
        Some(id(2, 8, 9))
    );
    assert_eq!(
        state.log_state().expect("state").last_log_id,
        Some(id(2, 8, 9))
    );
    let before = snapshot(&reader);
    assert_eq!(state.append(&blanks(0, 1)), Err(LogStateError::Conflict));
    assert_eq!(state.purge(id(2, 9, 9)), Err(LogStateError::InvalidPurge));
    assert_eq!(snapshot(&reader), before);
    state
        .append(&EncodedAppend::from_entries([blank(3, 1, 10)]).expect("next packet"))
        .expect("next row");
    let state = StoreState::open(state.writer, profile()).expect("reopen");
    assert_eq!(
        state
            .read_full(OwnedLogRange::from_range(..))
            .expect("read")
            .len(),
        1
    );
}

#[test]
fn maximum_index_reads_and_boundaries_do_not_wrap() {
    let (mut state, _, _) = observed();
    state.purge(id(1, 7, u64::MAX - 1)).expect("high baseline");
    state
        .append(&EncodedAppend::from_entries([blank(1, 7, u64::MAX)]).expect("maximum row"))
        .expect("maximum append");
    let rows = state
        .read_full(OwnedLogRange::from_range(u64::MAX..=u64::MAX))
        .expect("inclusive maximum");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].log_id, id(1, 7, u64::MAX));
    assert!(
        state
            .read_full(OwnedLogRange::from_range((
                Bound::Excluded(u64::MAX),
                Bound::Unbounded
            )))
            .expect("excluded maximum")
            .is_empty()
    );
    state.purge(id(1, 7, u64::MAX)).expect("maximum purge");
    assert!(
        state
            .read_full(OwnedLogRange::from_range(..))
            .expect("empty after purge")
            .is_empty()
    );
    assert_eq!(
        state.append(&EncodedAppend::from_entries([blank(2, 7, u64::MAX)]).expect("encoded")),
        Err(LogStateError::Conflict)
    );
    StoreState::open(state.writer, profile()).expect("reopen maximum boundary");
}

#[test]
fn full_reads_are_not_limited_but_replication_reads_are_count_bounded() {
    let (mut state, _, _) = observed();
    state.append(&blanks(0, 32)).expect("first append");
    state.append(&blanks(32, 64)).expect("second append");
    assert_eq!(
        state
            .read_full(OwnedLogRange::from_range(..))
            .expect("all rows")
            .len(),
        64
    );
    let rows = state.read_limited(0, 64).expect("limited rows");
    assert_eq!(rows.len(), MAX_LIMITED_ENTRIES);
    assert_eq!(rows[0].log_id.index, 0);
    assert_eq!(rows.last().expect("last row").log_id.index, 31);
    assert_eq!(
        state
            .read_full(OwnedLogRange::from_range(4..=7))
            .expect("closed range")
            .len(),
        4
    );
    assert!(
        state
            .read_full(OwnedLogRange::from_range(64..))
            .expect("missing range")
            .is_empty()
    );
    assert!(state.read_limited(10, 10).expect("empty input").is_empty());
    assert_eq!(
        state.read_limited(64, 65).err(),
        Some(LogStateError::InvalidRange)
    );
}

#[test]
fn limited_reads_stop_before_the_encoded_byte_cap_without_empty_prefixes() {
    let (mut state, _, _) = observed();
    for start in [0, 12] {
        let packet = EncodedAppend::from_entries(
            (start..start + 12).map(|index| send(index, vec![0x5a; 200 * 1024])),
        )
        .expect("bounded large packet");
        state.append(&packet).expect("append large packet");
    }
    let rows = state.read_limited(0, 24).expect("limited large read");
    assert!(!rows.is_empty());
    assert!(rows.len() < 24);
    let bytes: usize = rows
        .iter()
        .map(|row| {
            codec::encode_entry(row)
                .expect("canonical row")
                .encoded_len()
        })
        .sum();
    assert!(bytes <= MAX_LIMITED_BYTES);
    let next =
        codec::encode_entry(&send(rows.len() as u64, vec![0x5a; 200 * 1024])).expect("next row");
    assert!(bytes + next.encoded_len() > MAX_LIMITED_BYTES);
    assert_eq!(
        state
            .read_full(OwnedLogRange::from_range(..))
            .expect("full large read")
            .len(),
        24
    );
}

#[test]
fn retained_count_exhaustion_cannot_write_a_partial_append() {
    let (mut state, reader, controls) = observed();
    for start in (0..MAX_RETAINED_ENTRIES).step_by(MAX_APPEND_ENTRIES) {
        state
            .append(&blanks(start, start + MAX_APPEND_ENTRIES as u64))
            .expect("fill retained cap");
    }
    let before = snapshot(&reader);
    let commits = controls.commits.load(AtomicOrdering::SeqCst);
    assert_eq!(
        state.append(&blanks(MAX_RETAINED_ENTRIES, MAX_RETAINED_ENTRIES + 1)),
        Err(LogStateError::Capacity)
    );
    assert_eq!(snapshot(&reader), before);
    assert_eq!(controls.commits.load(AtomicOrdering::SeqCst), commits);
    assert_eq!(state.progress.retained_entries, MAX_RETAINED_ENTRIES);
    StoreState::open(state.writer, profile()).expect("cap is valid on reopen");
}

#[test]
fn physical_error_before_or_after_commit_poison_reads_and_writes() {
    for after in [false, true] {
        let (mut state, reader, controls) = observed();
        controls.fail_before.store(!after, AtomicOrdering::SeqCst);
        controls.fail_after.store(after, AtomicOrdering::SeqCst);
        let error = state
            .append(&blanks(0, 2))
            .expect_err("controlled physical error");
        assert_eq!(error, LogStateError::Storage);
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        assert_eq!(
            state.progress.retained_entries, 0,
            "cache changes only after success"
        );
        assert_eq!(state.read_vote(), Err(LogStateError::Poisoned));
        assert_eq!(state.log_state().err(), Some(LogStateError::Poisoned));
        assert_eq!(
            state.read_full(OwnedLogRange::from_range(..)).err(),
            Some(LogStateError::Poisoned)
        );
        assert_eq!(
            state.save_vote(LogVote::new(2, 7)),
            Err(LogStateError::Poisoned)
        );
        assert_eq!(state.truncate(id(1, 7, 0)), Err(LogStateError::Poisoned));
        assert_eq!(state.purge(id(1, 7, 0)), Err(LogStateError::Poisoned));
        assert_eq!(snapshot(&reader).entries().len(), if after { 4 } else { 2 });
        let state = StoreState::open(state.writer, profile()).expect("authoritative reopen");
        assert_eq!(state.progress.retained_entries, if after { 2 } else { 0 });
    }
}

#[test]
fn physical_read_error_poisoning_is_not_a_missing_log_result() {
    let (mut state, _, controls) = observed();
    state.append(&blanks(0, 2)).expect("append");
    controls.fail_read.store(true, AtomicOrdering::SeqCst);
    assert_eq!(
        state.read_full(OwnedLogRange::from_range(..)).err(),
        Some(LogStateError::Storage)
    );
    assert_eq!(
        state.read_limited(0, 1).err(),
        Some(LogStateError::Poisoned)
    );
    assert_eq!(state.append(&blanks(2, 3)), Err(LogStateError::Poisoned));
}

#[test]
fn a_missing_stored_predecessor_poisoning_is_not_an_empty_range() {
    let (mut state, _, _) = observed();
    state.append(&blanks(0, 3)).expect("append");
    // A privileged test injection breaks the single-writer storage contract.
    state
        .writer
        .inner
        .commit(WriteBatch::default().delete(entry_key(0)))
        .expect("inject missing predecessor");
    assert_eq!(
        state.read_full(OwnedLogRange::from_range(1..)).err(),
        Some(LogStateError::Corrupt)
    );
    assert_eq!(state.read_vote(), Err(LogStateError::Poisoned));
    assert_eq!(state.append(&blanks(3, 4)), Err(LogStateError::Poisoned));
}

#[test]
fn malformed_stored_entries_poison_later_operations() {
    let (mut state, _, _) = observed();
    state.append(&blanks(0, 2)).expect("append");
    state
        .writer
        .inner
        .commit(WriteBatch::default().put(entry_key(0), b"SECRET invalid record".to_vec()))
        .expect("inject corrupt row");
    let error = state.read_limited(0, 2).expect_err("corrupt read");
    assert_eq!(error, LogStateError::Corrupt);
    assert!(!format!("{error:?} {error}").contains("SECRET"));
    assert_eq!(state.log_state().err(), Some(LogStateError::Poisoned));
    assert_eq!(state.purge(id(1, 7, 1)), Err(LogStateError::Poisoned));
}
