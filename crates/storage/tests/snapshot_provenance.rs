//! Logical capture and the opt-in existing-handle provenance API.
//! These controls do not certify domain health, authentication or power loss.

use std::sync::{Arc, Mutex};

use storage::{
    ACTIVE_STORE_FORMAT, FjallStore, Key, MemoryStore, SnapshotProvenance, StateStore,
    StorageError, StoreSnapshot, Value, WriteBatch, capture_logical_snapshot,
};
use tempfile::TempDir;

fn rows() -> Vec<(Key, Value)> {
    vec![
        (vec![0, 0xff], vec![0, 0xff, 0x80]),
        (b"format_version".to_vec(), vec![0xde, 0xad]),
        (b"zero".to_vec(), vec![]),
        (vec![0xff, 0, 1], vec![0x80, 0x7f]),
    ]
}

fn seeded_batch() -> WriteBatch {
    let mut batch = WriteBatch::default();
    for (key, value) in rows() {
        batch.push_put(key, value);
    }
    batch
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Trace {
    clones: usize,
    gets: usize,
    applies: usize,
    snapshots: usize,
    scans: usize,
    prefixes: usize,
}

struct Observed {
    actual: MemoryStore,
    trace: Arc<Mutex<Trace>>,
    fault: Arc<Mutex<Option<StorageError>>>,
}

impl Observed {
    fn new(populated: bool) -> Self {
        let actual = MemoryStore::default();
        if populated {
            actual.apply(seeded_batch()).unwrap();
        }
        Self {
            actual,
            trace: Arc::new(Mutex::new(Trace::default())),
            fault: Arc::new(Mutex::new(None)),
        }
    }

    fn trace(&self) -> Trace {
        self.trace.lock().unwrap().clone()
    }
}

impl Clone for Observed {
    fn clone(&self) -> Self {
        self.trace.lock().unwrap().clones += 1;
        Self {
            actual: self.actual.clone(),
            trace: Arc::clone(&self.trace),
            fault: Arc::clone(&self.fault),
        }
    }
}

impl StateStore for Observed {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.trace.lock().unwrap().gets += 1;
        self.actual.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.trace.lock().unwrap().applies += 1;
        self.actual.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.trace.lock().unwrap().snapshots += 1;
        if let Some(error) = self.fault.lock().unwrap().clone() {
            return Err(error);
        }
        self.actual.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.trace.lock().unwrap().scans += 1;
        self.actual.scan_from(prefix, start, limit)
    }

    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.trace.lock().unwrap().prefixes += 1;
        self.actual.scan_prefix(prefix, limit)
    }
}

fn one_snapshot_only() -> Trace {
    Trace {
        snapshots: 1,
        ..Trace::default()
    }
}

#[test]
fn logical_memory_capture_preserves_exact_rows_and_immutable_result() {
    let store = MemoryStore::default();
    let empty = capture_logical_snapshot(&store).unwrap();
    assert_eq!(empty.provenance(), &SnapshotProvenance::LogicalOnly);
    assert!(empty.snapshot().entries().is_empty());
    store.apply(seeded_batch()).unwrap();
    let before = store.snapshot().unwrap();
    let captured = capture_logical_snapshot(&store).unwrap();
    let retained = captured.clone();
    assert_eq!(captured.snapshot(), &before);
    assert_eq!(captured.snapshot().entries(), rows());
    assert_eq!(captured.provenance(), &SnapshotProvenance::LogicalOnly);
    store
        .apply(
            WriteBatch::default()
                .delete(b"zero".to_vec())
                .put(b"later".to_vec(), vec![9]),
        )
        .unwrap();
    assert_eq!(captured, retained);
    assert_eq!(captured.snapshot(), &before);
    assert!(empty.snapshot().entries().is_empty());
    assert_ne!(store.snapshot().unwrap(), before);
    // This clone retains shared memory, not a persistence/restart certificate.
    let clone = store.clone();
    drop(store);
    assert_eq!(
        capture_logical_snapshot(&clone).unwrap().provenance(),
        &SnapshotProvenance::LogicalOnly
    );
}

#[test]
fn logical_custom_capture_calls_only_one_original_snapshot() {
    for populated in [false, true] {
        let store = Observed::new(populated);
        let before = store.actual.snapshot().unwrap();
        let captured = capture_logical_snapshot(&store).unwrap();
        assert_eq!(captured.provenance(), &SnapshotProvenance::LogicalOnly);
        assert_eq!(captured.snapshot(), &before);
        assert_eq!(store.trace(), one_snapshot_only());
        assert_eq!(store.actual.snapshot().unwrap(), before);
    }
}

#[test]
fn logical_custom_snapshot_fault_passes_through_without_other_operations() {
    for error in [
        StorageError::LockPoisoned,
        StorageError::Backend {
            operation: "controlled custom snapshot",
            detail: "original returned fault".to_owned(),
        },
    ] {
        let store = Observed::new(true);
        let before = store.actual.snapshot().unwrap();
        *store.fault.lock().unwrap() = Some(error.clone());
        assert_eq!(capture_logical_snapshot(&store).unwrap_err(), error);
        assert_eq!(store.trace(), one_snapshot_only());
        assert_eq!(store.actual.snapshot().unwrap(), before);
    }
}

#[test]
fn logical_fjall_capture_never_infers_durable_provenance() {
    let directory = TempDir::new().unwrap();
    let store = FjallStore::open(directory.path()).unwrap();
    let empty = capture_logical_snapshot(&store).unwrap();
    assert_eq!(empty.provenance(), &SnapshotProvenance::LogicalOnly);
    assert!(empty.snapshot().entries().is_empty());
    store.apply(seeded_batch()).unwrap();
    let clone = store.clone();
    let before = store.snapshot().unwrap();
    let captured = capture_logical_snapshot(&clone).unwrap();
    assert_eq!(captured.provenance(), &SnapshotProvenance::LogicalOnly);
    assert_eq!(captured.snapshot(), &before);
    let checked = clone.snapshot_with_provenance().unwrap();
    assert_eq!(checked.snapshot(), &before);
    assert!(matches!(checked.provenance(), SnapshotProvenance::Fjall(_)));
    assert_ne!(captured.provenance(), checked.provenance());
    drop(clone);
    drop(store);
    // Captured results contain owned logical bytes, not live database handles.
    let reopened = FjallStore::open(directory.path()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), before);
    assert_eq!(capture_logical_snapshot(&reopened).unwrap(), captured);
    assert_eq!(captured.snapshot().entries(), rows());
    assert!(empty.snapshot().entries().is_empty());
}

#[test]
fn fjall_checked_metadata_and_image_remain_immutable_after_later_writes() {
    let directory = TempDir::new().unwrap();
    let store = FjallStore::open(directory.path()).unwrap();
    store.apply(seeded_batch()).unwrap();
    let clone = store.clone();
    drop(store);
    let captured = clone.snapshot_with_provenance().unwrap();
    let retained = captured.clone();
    let SnapshotProvenance::Fjall(metadata) = *captured.provenance() else {
        panic!("the explicit checked Fjall API returned logical-only provenance");
    };
    assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
    assert_eq!(metadata.format_version(), ACTIVE_STORE_FORMAT);
    assert_eq!(captured.snapshot().entries(), rows());
    // A logical record with this name is opaque and is not the meta marker.
    assert_eq!(
        clone.get(b"format_version").unwrap(),
        Some(vec![0xde, 0xad])
    );
    clone
        .apply(
            WriteBatch::default()
                .put(b"format_version".to_vec(), vec![])
                .put(b"later".to_vec(), vec![1, 2]),
        )
        .unwrap();
    assert_eq!(captured, retained);
    assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
    let later = clone.snapshot_with_provenance().unwrap();
    assert_ne!(later.snapshot(), captured.snapshot());
    assert_eq!(later.provenance(), captured.provenance());
    let later_image = later.snapshot().clone();
    drop(clone);
    let reopened = FjallStore::open(directory.path()).unwrap();
    assert_eq!(
        reopened.snapshot_with_provenance().unwrap().snapshot(),
        &later_image
    );
    assert_eq!(captured, retained);
    assert_eq!(metadata.format_version(), ACTIVE_STORE_FORMAT);
}
