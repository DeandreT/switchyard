//! Real backends with explicit logical reopen and simulated apply errors.
//! These controls do not simulate a power cut or a failed fsync syscall.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use cluster::{
    JOURNAL_FORMAT_VERSION, JOURNAL_VALIDATION_PAGE_ENTRIES, Journal, JournalError,
    MAX_JOURNAL_PAYLOAD_BYTES, MAX_JOURNAL_READ_ENTRIES,
};
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, Key, MemoryStore, Mutation, StateStore, StorageError, StoreSnapshot, Value,
    WriteBatch,
};
use tempfile::TempDir;

const PREFIX: &[u8] = b"\xF0switchyard/journal\0";

#[derive(Clone)]
enum Backend {
    Memory(MemoryStore),
    Fjall(FjallStore),
}

impl StateStore for Backend {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        match self {
            Self::Memory(store) => store.get(key),
            Self::Fjall(store) => store.get(key),
        }
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        match self {
            Self::Memory(store) => store.apply(batch),
            Self::Fjall(store) => store.apply(batch),
        }
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match self {
            Self::Memory(store) => store.snapshot(),
            Self::Fjall(store) => store.snapshot(),
        }
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        match self {
            Self::Memory(store) => store.scan_from(prefix, start, limit),
            Self::Fjall(store) => store.scan_from(prefix, start, limit),
        }
    }
}

struct Fixture {
    store: Option<Backend>,
    directory: Option<TempDir>,
}

impl Fixture {
    fn new(durable: bool) -> Self {
        let directory = durable.then(|| TempDir::new().unwrap());
        let store = match &directory {
            Some(directory) => Backend::Fjall(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(MemoryStore::default()),
        };
        Self {
            store: Some(store),
            directory,
        }
    }
    fn store(&self) -> &Backend {
        self.store.as_ref().unwrap()
    }
    fn snapshot(&self) -> Vec<(Key, Value)> {
        self.store().snapshot().unwrap().entries().to_vec()
    }
    fn reopen_store(&mut self) {
        let original = self.store.take().unwrap();
        self.store = Some(match &self.directory {
            Some(directory) => {
                drop(original);
                Backend::Fjall(FjallStore::open(directory.path()).unwrap())
            }
            // Memory reopen means a fresh journal over retained shared state,
            // not persistence after losing the in-memory backend.
            None => original,
        });
    }
    fn reopen(&mut self) -> Journal<Backend> {
        self.reopen_store();
        Journal::open(self.store().clone()).unwrap()
    }
}

#[derive(Default)]
struct Trace {
    gets: Vec<Vec<u8>>,
    scans: Vec<(Vec<u8>, Vec<u8>, usize)>,
    batches: Vec<Vec<Mutation>>,
}

#[derive(Clone)]
struct Observed {
    actual: Backend,
    trace: Arc<Mutex<Trace>>,
    fault: Arc<AtomicUsize>,
}

impl Observed {
    fn new(actual: Backend) -> Self {
        Self {
            actual,
            trace: Arc::new(Mutex::new(Trace::default())),
            fault: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn arm(&self, after_apply: bool) {
        self.fault
            .store(if after_apply { 2 } else { 1 }, Ordering::SeqCst);
    }
}

fn simulated_error() -> StorageError {
    StorageError::Backend {
        operation: "simulate journal apply failure",
        detail: "controlled logical boundary".to_owned(),
    }
}

impl StateStore for Observed {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.trace.lock().unwrap().gets.push(key.to_vec());
        self.actual.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.trace
            .lock()
            .unwrap()
            .batches
            .push(batch.mutations().to_vec());
        let fault = self.fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(simulated_error());
        }
        self.actual.apply(batch)?;
        if fault == 2 {
            return Err(simulated_error());
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.actual.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.trace
            .lock()
            .unwrap()
            .scans
            .push((prefix.to_vec(), start.to_vec(), limit));
        self.actual.scan_from(prefix, start, limit)
    }
}

fn key(tag: u8) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(tag);
    key
}
fn entry_key(index: u64) -> Vec<u8> {
    let mut key = key(2);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn seed_other_records(fixture: &Fixture) -> Vec<(Key, Value)> {
    // Broker-shaped raw records and a different future cluster namespace.
    // Their preservation is byte-level evidence, not domain profile health.
    fixture
        .store()
        .apply(
            WriteBatch::default()
                .put(vec![0], vec![1])
                .put(
                    b"\x03tenant\0orders\0\0\0\0\0\0\0\0\x01".to_vec(),
                    vec![0, 255, 1],
                )
                .put(b"\xF1separate-owner".to_vec(), b"retained".to_vec()),
        )
        .unwrap();
    fixture.snapshot()
}

fn other_records(rows: &[(Key, Value)]) -> Vec<(Key, Value)> {
    rows.iter()
        .filter(|(key, _)| !key.starts_with(PREFIX))
        .cloned()
        .collect()
}

#[test]
fn memory_opaque_append_commit_and_reopen() {
    opaque_roundtrip(false);
}
#[test]
fn fjall_opaque_append_commit_and_reopen() {
    opaque_roundtrip(true);
}
#[test]
fn memory_invalid_inputs_and_noops_write_nothing() {
    refusals(false);
}
#[test]
fn fjall_invalid_inputs_and_noops_write_nothing() {
    refusals(true);
}
#[test]
fn memory_committed_pages_and_full_prefix_validation() {
    pages(false);
}
#[test]
fn fjall_committed_pages_and_full_prefix_validation() {
    pages(true);
}
#[test]
fn memory_valid_tail_is_retained_without_promotion() {
    tail(false);
}
#[test]
fn fjall_valid_tail_is_retained_without_promotion() {
    tail(true);
}
#[test]
fn memory_corrupt_prefix_and_tail_refuse_without_repair() {
    corruption(false);
}
#[test]
fn fjall_corrupt_prefix_and_tail_refuse_without_repair() {
    corruption(true);
}
#[test]
fn memory_ambiguous_apply_failures_require_reopen() {
    apply_failures(false);
}
#[test]
fn fjall_ambiguous_apply_failures_require_reopen() {
    apply_failures(true);
}
#[test]
fn memory_journal_footprint_preserves_other_records() {
    footprint(false);
}
#[test]
fn fjall_journal_footprint_preserves_other_records() {
    footprint(true);
}
#[test]
fn memory_journal_uses_the_existing_store_owner() {
    database_owner(false);
}
#[test]
fn fjall_journal_uses_the_existing_store_owner() {
    database_owner(true);
}

fn opaque_roundtrip(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let before = fixture.snapshot();
    let mut journal = Journal::open(fixture.store().clone()).unwrap();
    assert_eq!(fixture.snapshot(), before, "empty open is read-only");
    assert_eq!(journal.committed_index(), Ok(0));
    assert_eq!(journal.last_appended_index(), Ok(0));
    journal.commit(0).unwrap();
    assert_eq!(fixture.snapshot(), before);
    journal.append(1, &[]).unwrap();
    assert!(journal.read_committed(1, 1).unwrap().is_empty());
    let payload = b"\0opaque non-command bytes\xFF";
    journal.append(2, payload).unwrap();
    journal.commit(2).unwrap();
    assert_eq!(
        fixture.store().get(&key(0)).unwrap(),
        Some(JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec())
    );
    let rows = journal.read_committed(1, 2).unwrap();
    assert_eq!(
        rows.iter()
            .map(|entry| (entry.index(), entry.payload()))
            .collect::<Vec<_>>(),
        vec![(1, &[][..]), (2, &payload[..])]
    );
    drop(journal);
    let reopened = fixture.reopen();
    assert_eq!(reopened.committed_index(), Ok(2));
    assert_eq!(reopened.last_appended_index(), Ok(2));
    assert_eq!(reopened.read_committed(1, 2).unwrap(), rows);
}

fn refusals(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let observed = Observed::new(fixture.store().clone());
    let mut journal = Journal::open(observed.clone()).unwrap();
    journal.append(1, b"first").unwrap();
    journal.commit(1).unwrap();
    let before = fixture.snapshot();
    let writes = observed.trace.lock().unwrap().batches.len();
    for index in [0, 1, 3, u64::MAX] {
        assert_eq!(
            journal.append(index, b"replacement"),
            Err(JournalError::InvalidAppendIndex {
                found: index,
                expected: 2
            })
        );
    }
    let oversized = vec![0; MAX_JOURNAL_PAYLOAD_BYTES + 1];
    assert_eq!(
        journal.append(2, &oversized),
        Err(JournalError::PayloadTooLarge {
            bytes: oversized.len(),
            maximum: MAX_JOURNAL_PAYLOAD_BYTES
        })
    );
    assert!(matches!(
        journal.commit(0),
        Err(JournalError::InvalidCommit { .. })
    ));
    assert!(matches!(
        journal.commit(2),
        Err(JournalError::InvalidCommit { .. })
    ));
    assert_eq!(
        journal.read_committed(0, 1),
        Err(JournalError::InvalidReadStart)
    );
    for limit in [0, MAX_JOURNAL_READ_ENTRIES + 1, usize::MAX] {
        assert_eq!(
            journal.read_committed(1, limit),
            Err(JournalError::InvalidReadLimit {
                requested: limit,
                maximum: MAX_JOURNAL_READ_ENTRIES
            })
        );
    }
    journal.commit(1).unwrap();
    assert_eq!(fixture.snapshot(), before);
    assert_eq!(observed.trace.lock().unwrap().batches.len(), writes);
    let maximum = vec![0xA5; MAX_JOURNAL_PAYLOAD_BYTES];
    journal.append(2, &maximum).unwrap();
    journal.commit(2).unwrap();
    assert_eq!(journal.read_committed(2, 1).unwrap()[0].payload(), maximum);
    let after_maximum = fixture.snapshot();
    drop(journal);
    drop(observed);
    let reopened = fixture.reopen();
    assert_eq!(reopened.committed_index(), Ok(2));
    assert_eq!(reopened.read_committed(2, 1).unwrap()[0].payload(), maximum);
    assert_eq!(fixture.snapshot(), after_maximum);
}

fn pages(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let mut journal = Journal::open(fixture.store().clone()).unwrap();
    let total = (JOURNAL_VALIDATION_PAGE_ENTRIES * 2 + 7) as u64;
    for index in 1..=total {
        journal.append(index, &index.to_be_bytes()).unwrap();
    }
    let committed = MAX_JOURNAL_READ_ENTRIES as u64 + 2;
    journal.commit(committed).unwrap();
    drop(journal);
    fixture.reopen_store();
    let observed = Observed::new(fixture.store().clone());
    let reopened = Journal::open(observed.clone()).unwrap();
    {
        let trace = observed.trace.lock().unwrap();
        assert!(
            trace.scans.len() >= 3,
            "open validates the whole journal, not just its last row"
        );
        assert!(trace.scans.iter().all(
            |(prefix, _, limit)| prefix == PREFIX && *limit == JOURNAL_VALIDATION_PAGE_ENTRIES
        ));
        assert!(trace.scans.windows(2).all(|pages| pages[0].1 < pages[1].1));
        assert!(trace.gets.is_empty());
    }
    let first = reopened
        .read_committed(1, MAX_JOURNAL_READ_ENTRIES)
        .unwrap();
    assert_eq!(
        first.iter().map(|entry| entry.index()).collect::<Vec<_>>(),
        (1..=MAX_JOURNAL_READ_ENTRIES as u64).collect::<Vec<_>>()
    );
    let last = reopened
        .read_committed(committed - 1, MAX_JOURNAL_READ_ENTRIES)
        .unwrap();
    assert_eq!(
        last.iter().map(|entry| entry.index()).collect::<Vec<_>>(),
        vec![committed - 1, committed]
    );
    assert!(
        reopened
            .read_committed(committed + 1, 1)
            .unwrap()
            .is_empty()
    );
    assert!(reopened.read_committed(u64::MAX, 1).unwrap().is_empty());
    assert_eq!(reopened.last_appended_index(), Ok(total));
    drop(reopened);
    drop(observed);
    fixture
        .store()
        .apply(WriteBatch::default().delete(entry_key(JOURNAL_VALIDATION_PAGE_ENTRIES as u64 + 2)))
        .unwrap();
    let corrupted = fixture.snapshot();
    fixture.reopen_store();
    assert!(
        matches!(
            Journal::open(fixture.store().clone()),
            Err(JournalError::Corrupt { .. })
        ),
        "a gap beyond the first validation page is refused"
    );
    assert_eq!(fixture.snapshot(), corrupted);
}

fn tail(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let mut journal = Journal::open(fixture.store().clone()).unwrap();
    journal.append(1, b"committed").unwrap();
    journal.commit(1).unwrap();
    journal.append(2, b"tail two").unwrap();
    journal.append(3, b"tail three").unwrap();
    let before = fixture.snapshot();
    drop(journal);
    let mut reopened = fixture.reopen();
    assert_eq!(fixture.snapshot(), before);
    assert_eq!(reopened.committed_index(), Ok(1));
    assert_eq!(reopened.last_appended_index(), Ok(3));
    assert_eq!(reopened.read_committed(1, 3).unwrap().len(), 1);
    assert!(reopened.read_committed(2, 3).unwrap().is_empty());
    reopened.commit(3).unwrap();
    assert_eq!(reopened.read_committed(1, 3).unwrap().len(), 3);
    drop(reopened);
    let final_owner = fixture.reopen();
    assert_eq!(final_owner.committed_index(), Ok(3));
    assert_eq!(final_owner.last_appended_index(), Ok(3));
}

fn frontier_value(index: u64) -> Vec<u8> {
    let mut header = JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec();
    header.extend_from_slice(&index.to_be_bytes());
    let mut hash = Sha256::new();
    hash.update(b"switchyard journal frontier v1\0");
    hash.update(&header);
    header.extend_from_slice(&hash.finalize());
    header
}

fn corruption(durable: bool) {
    for variant in 0..22 {
        let mut fixture = Fixture::new(durable);
        let others = seed_other_records(&fixture);
        let mut journal = Journal::open(fixture.store().clone()).unwrap();
        for index in 1..=3 {
            journal.append(index, b"opaque").unwrap();
        }
        journal.commit(if variant == 8 { 2 } else { 1 }).unwrap();
        drop(journal);
        let mut value = fixture.store().get(&entry_key(1)).unwrap().unwrap();
        let mut frontier = fixture.store().get(&key(1)).unwrap().unwrap();
        let batch = match variant {
            0 => WriteBatch::default().delete(key(0)),
            1 => WriteBatch::default().put(key(0), vec![1]),
            2 => WriteBatch::default().put(key(0), 2_u32.to_be_bytes().to_vec()),
            3 => WriteBatch::default().delete(key(1)),
            4 => WriteBatch::default().put(key(1), vec![0]),
            5 => {
                frontier[12] ^= 1;
                WriteBatch::default().put(key(1), frontier)
            }
            6 => {
                frontier[..4].copy_from_slice(&2_u32.to_be_bytes());
                WriteBatch::default().put(key(1), frontier)
            }
            7 => WriteBatch::default().put(key(1), frontier_value(4)),
            8 => WriteBatch::default().delete(entry_key(1)),
            9 => WriteBatch::default().delete(entry_key(2)),
            10 => WriteBatch::default().put(key(3), Vec::new()),
            11 => {
                let mut malformed = entry_key(1);
                malformed.push(0);
                WriteBatch::default().put(malformed, value)
            }
            12 => {
                let mut malformed = entry_key(1);
                malformed.pop();
                WriteBatch::default().put(malformed, value)
            }
            13 => {
                value[4..12].copy_from_slice(&2_u64.to_be_bytes());
                WriteBatch::default().put(entry_key(1), value)
            }
            14 => WriteBatch::default().put(entry_key(1), vec![0; 47]),
            15 => {
                value[12..16].copy_from_slice(&1_u32.to_be_bytes());
                WriteBatch::default().put(entry_key(1), value)
            }
            16 => {
                value.push(0);
                WriteBatch::default().put(entry_key(1), value)
            }
            17 => {
                value[48] ^= 1;
                WriteBatch::default().put(entry_key(1), value)
            }
            18 => {
                value[..4].copy_from_slice(&2_u32.to_be_bytes());
                WriteBatch::default().put(entry_key(3), value)
            }
            19 => WriteBatch::default().put(entry_key(3), vec![0; 49 + MAX_JOURNAL_PAYLOAD_BYTES]),
            20 => WriteBatch::default()
                .delete(entry_key(1))
                .put(entry_key(0), value),
            21 => {
                let mut malformed = key(0);
                malformed.push(0);
                WriteBatch::default().put(malformed, vec![0])
            }
            _ => unreachable!(),
        };
        fixture.store().apply(batch).unwrap();
        let before = fixture.snapshot();
        let opened = Journal::open(fixture.store().clone());
        if matches!(variant, 2 | 6 | 18) {
            assert!(
                matches!(
                    opened,
                    Err(JournalError::UnsupportedVersion {
                        found: 2,
                        expected: JOURNAL_FORMAT_VERSION
                    })
                ),
                "variant {variant}"
            );
        } else {
            assert!(
                matches!(opened, Err(JournalError::Corrupt { .. })),
                "variant {variant}"
            );
        }
        assert_eq!(
            fixture.snapshot(),
            before,
            "refusal never repairs variant {variant}"
        );
        assert_eq!(other_records(&fixture.snapshot()), others);
        fixture.reopen_store();
        assert!(Journal::open(fixture.store().clone()).is_err());
        assert_eq!(fixture.snapshot(), before);
    }
    // Metadata-only partial journals must not be adopted either.
    for batch in [
        WriteBatch::default().put(key(0), JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec()),
        WriteBatch::default().put(key(1), frontier_value(0)),
    ] {
        let fixture = Fixture::new(durable);
        fixture.store().apply(batch).unwrap();
        let before = fixture.snapshot();
        assert!(matches!(
            Journal::open(fixture.store().clone()),
            Err(JournalError::Corrupt { .. })
        ));
        assert_eq!(fixture.snapshot(), before);
    }
}

fn apply_failures(durable: bool) {
    for after_apply in [false, true] {
        for stage in 0..3 {
            let mut fixture = Fixture::new(durable);
            let others = seed_other_records(&fixture);
            let mut setup = Journal::open(fixture.store().clone()).unwrap();
            if stage > 0 {
                setup.append(1, b"original").unwrap();
                setup.commit(1).unwrap();
            }
            if stage == 2 {
                setup.append(2, b"pending commit").unwrap();
            }
            let last_before = setup.last_appended_index().unwrap();
            let committed_before = setup.committed_index().unwrap();
            drop(setup);
            let before = fixture.snapshot();
            let observed = Observed::new(fixture.store().clone());
            let mut journal = Journal::open(observed.clone()).unwrap();
            observed.arm(after_apply);
            let result = if stage == 2 {
                journal.commit(2)
            } else {
                journal.append(last_before + 1, b"ambiguous append")
            };
            assert_eq!(result, Err(JournalError::Storage(simulated_error())));
            assert_eq!(journal.committed_index(), Err(JournalError::Unusable));
            assert_eq!(journal.last_appended_index(), Err(JournalError::Unusable));
            assert_eq!(journal.read_committed(1, 1), Err(JournalError::Unusable));
            assert_eq!(
                journal.append(last_before + 1, b"must not retry"),
                Err(JournalError::Unusable)
            );
            assert_eq!(
                journal.commit(committed_before),
                Err(JournalError::Unusable)
            );
            assert_eq!(observed.trace.lock().unwrap().batches.len(), 1);
            if !after_apply {
                assert_eq!(fixture.snapshot(), before);
            }
            assert_eq!(other_records(&fixture.snapshot()), others);
            drop(journal);
            drop(observed);
            let mut reopened = fixture.reopen();
            let last = last_before + u64::from(after_apply && stage != 2);
            let committed = if after_apply && stage == 2 {
                2
            } else {
                committed_before
            };
            assert_eq!(reopened.last_appended_index(), Ok(last));
            assert_eq!(reopened.committed_index(), Ok(committed));
            assert_eq!(
                reopened
                    .read_committed(1, MAX_JOURNAL_READ_ENTRIES)
                    .unwrap()
                    .len(),
                committed as usize
            );
            reopened.append(last + 1, b"new owner append").unwrap();
            reopened.commit(last + 1).unwrap();
            drop(reopened);
            let final_owner = fixture.reopen();
            assert_eq!(final_owner.committed_index(), Ok(last + 1));
            assert_eq!(other_records(&fixture.snapshot()), others);
        }
    }
}

fn footprint(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let others = seed_other_records(&fixture);
    let observed = Observed::new(fixture.store().clone());
    let mut journal = Journal::open(observed.clone()).unwrap();
    journal.append(1, b"first").unwrap();
    journal.append(2, b"second").unwrap();
    journal.commit(2).unwrap();
    journal.commit(2).unwrap();
    let entries = journal.read_committed(1, MAX_JOURNAL_READ_ENTRIES).unwrap();
    assert_eq!(entries.len(), 2);
    {
        let trace = observed.trace.lock().unwrap();
        assert!(trace.gets.is_empty());
        assert_eq!(
            trace.scans,
            vec![
                (
                    PREFIX.to_vec(),
                    PREFIX.to_vec(),
                    JOURNAL_VALIDATION_PAGE_ENTRIES
                ),
                (key(2), entry_key(1), 2)
            ]
        );
        assert_eq!(trace.batches.len(), 3);
        let keys = trace
            .batches
            .iter()
            .map(|batch| {
                batch
                    .iter()
                    .map(|mutation| match mutation {
                        Mutation::Put { key, .. } => {
                            assert!(key.starts_with(PREFIX));
                            key.clone()
                        }
                        Mutation::Delete { .. } => {
                            panic!("this journal never deletes or repairs records")
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                vec![key(0), key(1), entry_key(1)],
                vec![entry_key(2)],
                vec![key(1)]
            ]
        );
    }
    assert_eq!(other_records(&fixture.snapshot()), others);
    let before = fixture.snapshot();
    drop(journal);
    drop(observed);
    let reopened = fixture.reopen();
    assert_eq!(reopened.read_committed(1, 2).unwrap(), entries);
    assert_eq!(fixture.snapshot(), before);
    assert_eq!(other_records(&fixture.snapshot()), others);
}

fn database_owner(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let other_handle = fixture.store().clone();
    let mut journal = Journal::open(fixture.store().clone()).unwrap();
    journal.append(1, b"same database").unwrap();
    journal.commit(1).unwrap();
    assert!(other_handle.get(&entry_key(1)).unwrap().is_some());
    other_handle
        .apply(WriteBatch::default().put(b"outside-journal".to_vec(), b"same owner".to_vec()))
        .unwrap();
    assert_eq!(
        fixture.store().get(b"outside-journal").unwrap(),
        Some(b"same owner".to_vec())
    );
    let before = fixture.snapshot();
    if let Some(directory) = &fixture.directory {
        assert!(matches!(
            FjallStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        assert_eq!(fixture.snapshot(), before);
    }
    drop(journal);
    drop(other_handle);
    let reopened = fixture.reopen();
    assert_eq!(reopened.committed_index(), Ok(1));
    assert_eq!(
        reopened.read_committed(1, 1).unwrap()[0].payload(),
        b"same database"
    );
    assert_eq!(fixture.snapshot(), before);
}
