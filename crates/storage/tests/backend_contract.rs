//! The [`StateStore`] contract, asserted identically against every backend.
//!
//! Anything the state machine relies on belongs here rather than in one
//! backend's own tests, so that a new backend inherits the whole suite and a
//! difference between backends shows up as a failure rather than as a bug in the
//! broker.

use storage::{FjallStore, MemoryStore, StateStore, StorageError, WriteBatch};
use tempfile::TempDir;

/// Supplies empty stores to the suite. The durable variant owns the directory
/// its stores live in, so it has to outlive them.
trait Backend {
    type Store: StateStore;

    fn create() -> Self;

    /// Opens the store this backend stands for. Calling it again reaches the
    /// same state, which is how the suite exercises a restart; the durable
    /// backend requires the previous handle to be dropped first, because a store
    /// directory has a single owner.
    fn open(&self) -> Result<Self::Store, StorageError>;
}

struct Memory {
    store: MemoryStore,
}

impl Backend for Memory {
    type Store = MemoryStore;

    fn create() -> Self {
        Self {
            store: MemoryStore::default(),
        }
    }

    fn open(&self) -> Result<MemoryStore, StorageError> {
        Ok(self.store.clone())
    }
}

struct Durable {
    directory: TempDir,
}

impl Backend for Durable {
    type Store = FjallStore;

    fn create() -> Self {
        Self {
            directory: TempDir::new().expect("a temporary directory"),
        }
    }

    fn open(&self) -> Result<FjallStore, StorageError> {
        FjallStore::open(self.directory.path())
    }
}

fn keys(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    entries.iter().map(|(key, _)| key.clone()).collect()
}

// ---- the suite -------------------------------------------------------------

fn applies_every_mutation_in_a_batch_as_one_unit<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"message:1".to_vec(), b"first".to_vec())
            .put(b"message:2".to_vec(), b"second".to_vec())
            .delete(b"message:1".to_vec()),
    )?;

    assert_eq!(store.get(b"message:1")?, None);
    assert_eq!(store.get(b"message:2")?, Some(b"second".to_vec()));
    assert_eq!(store.snapshot()?.entries().len(), 1);
    Ok(())
}

fn the_last_mutation_naming_a_key_decides_its_fate<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"locked".to_vec(), b"stale".to_vec())
            .delete(b"locked".to_vec())
            .put(b"locked".to_vec(), b"current".to_vec())
            .put(b"gone".to_vec(), b"stale".to_vec())
            .delete(b"gone".to_vec()),
    )?;

    // The state machine relies on this: a command that rewrites a record it also
    // deleted earlier in the same batch must end with the rewrite.
    assert_eq!(store.get(b"locked")?, Some(b"current".to_vec()));
    assert_eq!(store.get(b"gone")?, None);
    Ok(())
}

fn distinguishes_an_empty_value_from_a_missing_key<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(WriteBatch::default().put(b"ready:1".to_vec(), Vec::new()))?;

    // Every index entry the state machine writes carries an empty value, so an
    // empty value has to remain a present key.
    assert_eq!(store.get(b"ready:1")?, Some(Vec::new()));
    assert_eq!(store.get(b"ready:2")?, None);
    assert_eq!(
        store.scan_prefix(b"ready:", 16)?,
        vec![(b"ready:1".to_vec(), Vec::new())]
    );
    Ok(())
}

fn scans_a_prefix_in_key_order_within_its_limit<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"ready:\x00\x02".to_vec(), Vec::new())
            .put(b"ready:\x00\x01".to_vec(), Vec::new())
            .put(b"ready:\x00\x03".to_vec(), Vec::new())
            .put(b"locks:\x00\x01".to_vec(), Vec::new()),
    )?;

    assert_eq!(
        keys(&store.scan_prefix(b"ready:", 2)?),
        vec![b"ready:\x00\x01".to_vec(), b"ready:\x00\x02".to_vec()]
    );
    assert_eq!(store.scan_prefix(b"ready:", 16)?.len(), 3);
    assert_eq!(store.scan_prefix(b"absent:", 16)?.len(), 0);
    Ok(())
}

fn scans_nothing_for_a_zero_limit<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(WriteBatch::default().put(b"ready:1".to_vec(), Vec::new()))?;

    assert_eq!(store.scan_prefix(b"ready:", 0)?, Vec::new());
    Ok(())
}

fn a_prefix_scan_stops_at_the_end_of_its_prefix<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"tenant\x00orders\x00".to_vec(), Vec::new())
            .put(b"tenant\x00orders-archive\x00".to_vec(), Vec::new())
            .put(b"tenant\x00orders\x00\xff".to_vec(), Vec::new()),
    )?;

    // Entity scopes are separated by a zero byte, so a longer sibling name must
    // stay outside a shorter one's scan.
    assert_eq!(
        keys(&store.scan_prefix(b"tenant\x00orders\x00", 16)?),
        vec![
            b"tenant\x00orders\x00".to_vec(),
            b"tenant\x00orders\x00\xff".to_vec()
        ]
    );
    Ok(())
}

fn a_snapshot_holds_every_entry_in_key_order<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(vec![0x02], b"counters".to_vec())
            .put(vec![0x00], b"clock".to_vec())
            .put(vec![0x01], b"config".to_vec()),
    )?;

    assert_eq!(
        store.snapshot()?.entries(),
        &[
            (vec![0x00], b"clock".to_vec()),
            (vec![0x01], b"config".to_vec()),
            (vec![0x02], b"counters".to_vec()),
        ]
    );
    Ok(())
}

fn a_deleted_key_leaves_no_trace_behind<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"ready:1".to_vec(), Vec::new())
            .put(b"ready:2".to_vec(), Vec::new()),
    )?;
    store.apply(WriteBatch::default().delete(b"ready:1".to_vec()))?;

    assert_eq!(
        keys(&store.scan_prefix(b"ready:", 16)?),
        vec![b"ready:2".to_vec()]
    );
    assert_eq!(keys(store.snapshot()?.entries()), vec![b"ready:2".to_vec()]);
    Ok(())
}

fn state_is_visible_after_reopening_the_store<B: Backend>() -> Result<(), StorageError> {
    let backend = B::create();
    let store = backend.open()?;
    store.apply(
        WriteBatch::default()
            .put(b"message:1".to_vec(), b"first".to_vec())
            .put(b"ready:1".to_vec(), Vec::new()),
    )?;
    // The durable backend will not open a directory twice, and dropping the
    // handle is what makes the reopen below a restart rather than a second
    // owner. For the memory backend nothing is read back off a disk, so this
    // only asserts that a fresh handle shares the same keyspace.
    drop(store);

    let reopened = backend.open()?;
    assert_eq!(reopened.get(b"message:1")?, Some(b"first".to_vec()));
    assert_eq!(reopened.get(b"ready:1")?, Some(Vec::new()));
    assert_eq!(
        keys(&reopened.scan_prefix(b"ready:", 16)?),
        vec![b"ready:1".to_vec()]
    );
    Ok(())
}

fn a_scan_resumes_from_its_start_key<B: Backend>() -> Result<(), StorageError> {
    let store = B::create().open()?;
    store.apply(
        WriteBatch::default()
            .put(b"ready:a\x00\x01".to_vec(), Vec::new())
            .put(b"ready:a\x00\x02".to_vec(), Vec::new())
            .put(b"ready:b\x00\x01".to_vec(), Vec::new())
            .put(b"locks:c\x00\x01".to_vec(), Vec::new()),
    )?;

    // Skipping every entry of `a` without reading them is how a receive walks
    // past a session it cannot use.
    assert_eq!(
        keys(&store.scan_from(b"ready:", b"ready:a\x01", 16)?),
        vec![b"ready:b\x00\x01".to_vec()]
    );
    // A start below the prefix is the same as starting at the prefix, and a
    // start past it ends the walk rather than escaping into the next prefix.
    assert_eq!(
        keys(&store.scan_from(b"ready:", b"aaaa", 16)?),
        keys(&store.scan_prefix(b"ready:", 16)?)
    );
    assert_eq!(store.scan_from(b"ready:", b"ready;", 16)?, Vec::new());
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

/// Runs every named case against both backends, so the two suites cannot drift.
macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), super::StorageError> {
                    super::$case::<super::Memory>()
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), super::StorageError> {
                    super::$case::<super::Durable>()
                }
            )+
        }
    };
}

for_each_backend! {
    applies_every_mutation_in_a_batch_as_one_unit,
    the_last_mutation_naming_a_key_decides_its_fate,
    distinguishes_an_empty_value_from_a_missing_key,
    scans_a_prefix_in_key_order_within_its_limit,
    scans_nothing_for_a_zero_limit,
    a_prefix_scan_stops_at_the_end_of_its_prefix,
    a_snapshot_holds_every_entry_in_key_order,
    a_deleted_key_leaves_no_trace_behind,
    a_scan_resumes_from_its_start_key,
    state_is_visible_after_reopening_the_store,
}
