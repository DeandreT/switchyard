use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use storage::MemoryStore;

#[derive(Clone, Default)]
struct CountingStore {
    inner: MemoryStore,
    gets: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
    snapshots: Arc<AtomicUsize>,
}

impl StateStore for CountingStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
}

fn reader(budget: ReadBudget) -> PageStore<CountingStore> {
    PageStore {
        inner: CountingStore::default(),
        budget: Arc::new(Mutex::new(budget)),
    }
}

#[test]
fn point_read_and_input_key_limits_are_reserved_before_backend_reads() {
    let store = reader(ReadBudget {
        gets: MAX_GETS - 1,
        key_bytes: MAX_KEY_BYTES - 1,
        ..ReadBudget::default()
    });
    assert_eq!(store.get(b"k"), Ok(None));
    assert_eq!(store.inner.gets.load(Ordering::SeqCst), 1);
    assert_eq!(store.get(b"k"), Err(StorageError::ReadLimitExceeded));
    assert_eq!(store.inner.gets.load(Ordering::SeqCst), 1);
    let budget = store.budget.lock().unwrap();
    assert_eq!(budget.gets, MAX_GETS);
    assert_eq!(budget.key_bytes, MAX_KEY_BYTES);
    drop(budget);
    let store = reader(ReadBudget {
        key_bytes: MAX_KEY_BYTES,
        ..ReadBudget::default()
    });
    assert_eq!(store.get(b"k"), Err(StorageError::ReadLimitExceeded));
    assert_eq!(store.inner.gets.load(Ordering::SeqCst), 0);
    assert_eq!(store.budget.lock().unwrap().gets, 0);
}

#[test]
fn returned_point_values_count_every_read_without_copy_or_rss_claim() {
    let store = reader(ReadBudget {
        value_bytes: MAX_VALUE_BYTES - 3,
        ..ReadBudget::default()
    });
    store
        .inner
        .inner
        .apply(WriteBatch::default().put(b"k".to_vec(), vec![1, 2, 3]))
        .unwrap();
    assert_eq!(store.get(b"k"), Ok(Some(vec![1, 2, 3])));
    assert_eq!(store.budget.lock().unwrap().value_bytes, MAX_VALUE_BYTES);
    assert_eq!(store.get(b"k"), Err(StorageError::ReadLimitExceeded));
    assert_eq!(store.inner.gets.load(Ordering::SeqCst), 2);
    assert_eq!(store.budget.lock().unwrap().value_bytes, MAX_VALUE_BYTES);
}

#[test]
fn scan_slots_and_input_key_bytes_are_reserved_before_backend_scans() {
    let store = reader(ReadBudget {
        scans: MAX_SCANS - 1,
        key_bytes: MAX_KEY_BYTES - 2,
        ..ReadBudget::default()
    });
    assert_eq!(store.scan_from(b"p", b"p", 1), Ok(Vec::new()));
    assert_eq!(
        store.scan_from(b"p", b"p", 1),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(store.inner.scans.load(Ordering::SeqCst), 1);
    assert_eq!(store.budget.lock().unwrap().scans, MAX_SCANS);
    assert_eq!(store.budget.lock().unwrap().key_bytes, MAX_KEY_BYTES);
    let store = reader(ReadBudget {
        key_bytes: MAX_KEY_BYTES - 1,
        ..ReadBudget::default()
    });
    assert_eq!(
        store.scan_from(b"p", b"p", 1),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(store.inner.scans.load(Ordering::SeqCst), 0);
    assert_eq!(store.budget.lock().unwrap().scans, 0);
}

#[test]
fn all_returned_rows_keys_values_and_repeated_lookahead_reads_are_charged() {
    let store = reader(ReadBudget::default());
    store
        .inner
        .inner
        .apply(
            WriteBatch::default()
                .put(b"p1".to_vec(), vec![1; 3])
                .put(b"p2".to_vec(), vec![2; 5]),
        )
        .unwrap();
    let first = store.scan_from(b"p", b"p", 2).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(store.scan_from(b"p", b"p", 2).unwrap(), first);
    let budget = store.budget.lock().unwrap();
    assert_eq!(budget.rows, 4);
    assert_eq!(budget.scans, 2);
    assert_eq!(budget.key_bytes, 12); // Two input bytes and four returned key bytes each scan.
    assert_eq!(budget.value_bytes, 16);
}

#[test]
fn raw_row_headroom_never_produces_a_zero_limit_or_clamped_exhaustion() {
    for rows in [MAX_ROWS - 1, MAX_ROWS] {
        let store = reader(ReadBudget {
            rows,
            ..ReadBudget::default()
        });
        assert_eq!(store.raw_page_limit(), Err(StorageError::ReadLimitExceeded));
        assert_eq!(store.inner.scans.load(Ordering::SeqCst), 0);
    }
    let store = reader(ReadBudget {
        rows: MAX_ROWS - 2,
        ..ReadBudget::default()
    });
    assert_eq!(store.raw_page_limit(), Ok(1));
    assert_eq!(
        store.scan_from(b"p", b"p", 3),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(store.inner.scans.load(Ordering::SeqCst), 0);
    store
        .inner
        .inner
        .apply(
            WriteBatch::default()
                .put(b"p1".to_vec(), vec![1])
                .put(b"p2".to_vec(), vec![2]),
        )
        .unwrap();
    assert_eq!(store.scan_from(b"p", b"p", 2).unwrap().len(), 2);
    assert_eq!(store.budget.lock().unwrap().rows, MAX_ROWS);
    assert_eq!(store.raw_page_limit(), Err(StorageError::ReadLimitExceeded));
}

#[test]
fn returned_scan_byte_limits_are_checked_before_rows_reach_the_domain() {
    let store = reader(ReadBudget {
        value_bytes: MAX_VALUE_BYTES - 2,
        ..ReadBudget::default()
    });
    store
        .inner
        .inner
        .apply(WriteBatch::default().put(b"p1".to_vec(), vec![1; 3]))
        .unwrap();
    assert_eq!(
        store.scan_from(b"p", b"p", 1),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(store.inner.scans.load(Ordering::SeqCst), 1);
    assert_eq!(store.budget.lock().unwrap().rows, 0);
    assert_eq!(
        store.budget.lock().unwrap().value_bytes,
        MAX_VALUE_BYTES - 2
    );
    let store = reader(ReadBudget {
        key_bytes: MAX_KEY_BYTES - 3,
        ..ReadBudget::default()
    });
    store
        .inner
        .inner
        .apply(WriteBatch::default().put(b"p1".to_vec(), vec![1]))
        .unwrap();
    assert_eq!(
        store.scan_from(b"p", b"p", 1),
        Err(StorageError::ReadLimitExceeded)
    );
    assert_eq!(store.inner.scans.load(Ordering::SeqCst), 1);
    assert_eq!(store.budget.lock().unwrap().rows, 0);
    assert_eq!(store.budget.lock().unwrap().key_bytes, MAX_KEY_BYTES - 1);
}

#[test]
fn overflow_and_forbidden_storage_operations_fail_without_backend_forwarding() {
    assert_eq!(
        add(usize::MAX, 1, usize::MAX),
        Err(StorageError::ReadLimitExceeded)
    );
    let store = reader(ReadBudget::default());
    assert_eq!(
        store.apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_eq!(store.snapshot(), Err(StorageError::ReadLimitExceeded));
    assert_eq!(store.inner.applies.load(Ordering::SeqCst), 0);
    assert_eq!(store.inner.snapshots.load(Ordering::SeqCst), 0);
}
