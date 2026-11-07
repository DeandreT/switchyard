//! Offline, exact-image journal prefix compaction at an attached anchor. No live
//! or background compaction, retention policy, replay or runtime activation.
//!
//! All writers, raw clones, keyspaces and prior journal/indexed/replay owners
//! must be excluded by the caller. Neither these checks nor checksums
//! authenticate history.

use std::sync::Arc;

use storage::{
    ImageInstallBackend, Key, SnapshotProvenance, SnapshotWithProvenance, StateStore, StorageError,
    Value, WriteBatch,
};
use thiserror::Error;

use super::anchor::SnapshotAnchor;
use super::format::RecordShape;
use super::recovery::validated_anchor;
use super::{
    SnapshotCaptureError, SnapshotFormat, SnapshotFormatError, SnapshotLimits, SnapshotManifest,
};
use crate::journal::{JournalError, anchor_key, entry_key};

/// Post-read bounds for the current and resulting images and the one batch.
/// No backend allocation, physical space or latency bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCompactionLimits {
    pub snapshot: SnapshotLimits,
    pub max_deleted_entries: usize,
    /// Deleted entry keys plus the anchor key and its rewritten value.
    pub max_mutation_bytes: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotCompactionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Target(#[from] SnapshotCaptureError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Format(#[from] SnapshotFormatError),
    #[error("compaction requires an attached snapshot anchor")]
    Unanchored,
    #[error("the journal is already compacted through the anchor")]
    AlreadyCompacted,
    #[error("compaction deletes {requested} entries, maximum {maximum}")]
    DeletionLimit { requested: u64, maximum: usize },
    #[error("compaction needs {requested} mutation bytes, maximum {maximum}")]
    MutationLimit { requested: usize, maximum: usize },
    #[error("compaction size is not representable")]
    SizeOverflow,
    #[error("offline compaction owner is retired; close all handles and reopen")]
    Retired,
}

/// Prebuilt operational result, not authentication or a recovery certificate.
/// The manifest's P/C/L and applied identity are unchanged by compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCompactionReceipt {
    pub target_provenance: SnapshotProvenance,
    pub manifest: SnapshotManifest,
    pub applied_index: u64,
    pub removed_through_before: u64,
    pub removed_through_after: u64,
    pub deleted_entries: usize,
    pub mutations: usize,
    pub logical_mutation_bytes: usize,
}

/// Only this checked plan can stage the rewritten anchor and its batch. The
/// deleted range still borrows the validated image.
#[derive(Debug)]
struct CheckedCompaction<'a> {
    anchor: &'a SnapshotAnchor,
    deleted: &'a [(Key, Value)],
    anchor_key: Vec<u8>,
    mutation_bytes: usize,
}

/// Native Fjall commits one batch with a u32 combined mutation count.
fn deletion_maximum(limit: usize, native: bool) -> usize {
    if native {
        limit.min(u32::MAX as usize - 1)
    } else {
        limit
    }
}

fn preflight<'a>(
    records: &'a [(Key, Value)],
    anchor: &'a SnapshotAnchor,
    native: bool,
    limits: SnapshotCompactionLimits,
) -> Result<CheckedCompaction<'a>, SnapshotCompactionError> {
    let (floor, applied) = (anchor.removed_through(), anchor.applied_index());
    let requested = applied - floor;
    let maximum = deletion_maximum(limits.max_deleted_entries, native);
    let count = usize::try_from(requested)
        .ok()
        .filter(|count| *count <= maximum)
        .ok_or(SnapshotCompactionError::DeletionLimit { requested, maximum })?;
    // Validation already proved B+1..=L contiguous with A equal to the original;
    // the endpoints re-establish that this exact borrowed range is B+1..=A.
    let first = entry_key(floor + 1);
    let start = records.partition_point(|(key, _)| *key < first);
    let deleted = start
        .checked_add(count)
        .and_then(|end| records.get(start..end))
        .filter(|deleted| {
            deleted.first().is_some_and(|(key, _)| *key == first)
                && deleted.last().is_some_and(|(key, value)| {
                    *key == entry_key(applied) && value == anchor.original_entry()
                })
        })
        .ok_or(JournalError::Corrupt {
            detail: "compacted prefix is not the retained B+1..=A range",
        })?;
    let anchor_bytes = SnapshotAnchor::encoded_size(anchor.original_entry())
        .map_err(|_| SnapshotCompactionError::SizeOverflow)?;
    let anchor_key = anchor_key();
    let mutation_bytes = deleted
        .iter()
        .try_fold(0usize, |bytes, (key, _)| bytes.checked_add(key.len()))
        .and_then(|bytes| bytes.checked_add(anchor_key.len()))
        .and_then(|bytes| bytes.checked_add(anchor_bytes))
        .ok_or(SnapshotCompactionError::SizeOverflow)?;
    if mutation_bytes > limits.max_mutation_bytes {
        return Err(SnapshotCompactionError::MutationLimit {
            requested: mutation_bytes,
            maximum: limits.max_mutation_bytes,
        });
    }
    // The exact virtual result skips the deleted range and resizes only the
    // anchor, without copying records or allocating an image-sized array.
    let resulting = records[..start]
        .iter()
        .chain(&records[start + count..])
        .map(|(key, value)| RecordShape {
            key,
            value_bytes: if *key == anchor_key {
                anchor_bytes
            } else {
                value.len()
            },
        });
    SnapshotFormat::preflight_shapes(true, resulting, limits.snapshot)?;
    Ok(CheckedCompaction {
        anchor,
        deleted,
        anchor_key,
        mutation_bytes,
    })
}

impl CheckedCompaction<'_> {
    fn stage(self) -> Result<WriteBatch, SnapshotCompactionError> {
        let anchor = self
            .anchor
            .compacted()
            .encode()
            .map_err(|_| SnapshotCompactionError::SizeOverflow)?;
        let mut batch = WriteBatch::default();
        for (key, _) in self.deleted {
            batch.push_delete(key.clone());
        }
        batch.push_put(self.anchor_key, anchor);
        Ok(batch)
    }
}

/// Non-Clone offline owner; ALL other writers and previous owners must be gone.
/// A returned apply error or escaping original panic retires all I/O and release
/// APIs until reopen, without retries or compensating writes.
///
/// ```compile_fail
/// use cluster::SnapshotCompactionOwner;
/// use storage::MemoryStore;
/// let owner = SnapshotCompactionOwner::new(MemoryStore::default());
/// let duplicate = owner.clone();
/// ```
#[derive(Debug)]
pub struct SnapshotCompactionOwner<S: ImageInstallBackend> {
    store: S,
    retired: bool,
}

impl<S: ImageInstallBackend> SnapshotCompactionOwner<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            retired: false,
        }
    }
    pub fn is_retired(&self) -> bool {
        self.retired
    }
    fn require_usable(&self) -> Result<(), SnapshotCompactionError> {
        if self.retired {
            Err(SnapshotCompactionError::Retired)
        } else {
            Ok(())
        }
    }
    pub fn snapshot(&self) -> Result<SnapshotWithProvenance, SnapshotCompactionError> {
        self.require_usable()?;
        Ok(self.store.inspect_image()?)
    }
    pub fn into_store(self) -> Result<S, SnapshotCompactionError> {
        self.require_usable()?;
        Ok(self.store)
    }

    /// Inspects the actual backend once and validates that complete immutable
    /// image. The deletion range and anchor come only from those records; an
    /// unanchored or already compacted image refuses without writes.
    pub fn prepare<'a>(
        &'a mut self,
        limits: SnapshotCompactionLimits,
    ) -> Result<PreparedSnapshotCompaction<'a, S>, SnapshotCompactionError> {
        self.require_usable()?;
        let image = Arc::new(self.store.inspect_image()?);
        let (manifest, anchor) = validated_anchor(Arc::clone(&image), limits.snapshot)?;
        let anchor = anchor.ok_or(SnapshotCompactionError::Unanchored)?;
        if anchor.removed_through() == anchor.applied_index() {
            return Err(SnapshotCompactionError::AlreadyCompacted);
        }
        // All caps precede the rewritten anchor, its encoding and the batch.
        let checked = preflight(image.snapshot().entries(), &anchor, S::NATIVE, limits)?;
        let deleted_entries = checked.deleted.len();
        let receipt = SnapshotCompactionReceipt {
            target_provenance: *image.provenance(),
            manifest,
            applied_index: anchor.applied_index(),
            removed_through_before: anchor.removed_through(),
            removed_through_after: anchor.applied_index(),
            deleted_entries,
            mutations: deleted_entries
                .checked_add(1)
                .ok_or(SnapshotCompactionError::SizeOverflow)?,
            logical_mutation_bytes: checked.mutation_bytes,
        };
        let batch = checked.stage()?;
        Ok(PreparedSnapshotCompaction {
            owner: self,
            batch,
            receipt,
        })
    }

    pub fn compact(
        &mut self,
        limits: SnapshotCompactionLimits,
    ) -> Result<SnapshotCompactionReceipt, SnapshotCompactionError> {
        self.prepare(limits)?.apply()
    }
}

/// Single-use, non-Clone preparation borrowing the exact target owner. Dropping
/// it is mutation-free; no caller can substitute another target or batch.
///
/// ```compile_fail
/// use cluster::{PreparedSnapshotCompaction, SnapshotCompactionLimits, SnapshotCompactionOwner};
/// use storage::MemoryStore;
/// fn detach(owner: &mut SnapshotCompactionOwner<MemoryStore>, limits: SnapshotCompactionLimits)
///     -> PreparedSnapshotCompaction<'static, MemoryStore> {
///     owner.prepare(limits).unwrap()
/// }
/// ```
pub struct PreparedSnapshotCompaction<'a, S: ImageInstallBackend> {
    owner: &'a mut SnapshotCompactionOwner<S>,
    batch: WriteBatch,
    receipt: SnapshotCompactionReceipt,
}

impl<S: ImageInstallBackend> PreparedSnapshotCompaction<'_, S> {
    pub fn receipt(&self) -> SnapshotCompactionReceipt {
        self.receipt
    }

    pub fn apply(self) -> Result<SnapshotCompactionReceipt, SnapshotCompactionError> {
        apply_compaction(
            &self.owner.store,
            &mut self.owner.retired,
            self.batch,
            self.receipt,
        )
    }
}

fn apply_compaction<S: StateStore>(
    store: &S,
    retired: &mut bool,
    batch: WriteBatch,
    receipt: SnapshotCompactionReceipt,
) -> Result<SnapshotCompactionReceipt, SnapshotCompactionError> {
    *retired = true;
    store.apply(batch)?;
    *retired = false;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{SnapshotAnchorLimits, SnapshotAnchorOwner, SnapshotCapture};
    use domain::{DurableProposal, IndexedWriter};
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use storage::{FjallStore, MemoryStore, Mutation, StoreSnapshot};
    use tempfile::TempDir;

    fn limits() -> SnapshotCompactionLimits {
        SnapshotCompactionLimits {
            snapshot: SnapshotLimits {
                max_encoded_bytes: 1 << 20,
                max_records: 100,
                max_key_bytes: 1024,
                max_value_bytes: 1 << 19,
            },
            max_deleted_entries: 100,
            max_mutation_bytes: 1 << 20,
        }
    }
    fn proposal(at: u64) -> DurableProposal {
        DurableProposal::unbound(domain::Command::new(
            domain::NamespaceName::new("n").unwrap(),
            domain::EntityPath::new("q").unwrap(),
            domain::Timestamp::from_millis(at),
            domain::CommandKind::ExpireSessionLocks,
        ))
    }
    /// A=P=2, C=3 and an opaque L=4, attached at B=0.
    fn seed<S: ImageInstallBackend>(store: &S) {
        let mut journal = crate::Journal::open(store.clone()).unwrap();
        let mut writer = IndexedWriter::open(store.clone()).unwrap();
        for index in 1..=3 {
            journal
                .append(index, &proposal(index).encode().unwrap())
                .unwrap();
        }
        journal.commit(3).unwrap();
        journal.append(4, &[0xff]).unwrap();
        for index in 1..=2 {
            writer.apply(index, &proposal(index)).unwrap();
        }
        drop((journal, writer));
        let capture = SnapshotCapture::logical(store, limits().snapshot).unwrap();
        SnapshotAnchorOwner::new(store.clone())
            .attach(
                &capture,
                SnapshotAnchorLimits {
                    snapshot: limits().snapshot,
                    max_mutation_bytes: 1 << 20,
                },
            )
            .unwrap();
    }

    #[test]
    fn native_deletions_reserve_the_anchor_put_within_u32_mutations() {
        assert_eq!(deletion_maximum(usize::MAX, true), u32::MAX as usize - 1);
        assert_eq!(deletion_maximum(7, true), 7);
        assert_eq!(deletion_maximum(usize::MAX, false), usize::MAX);
    }

    #[test]
    fn borrowed_plan_checks_all_output_bounds_before_owned_staging() {
        let store = MemoryStore::default();
        seed(&store);
        let image = Arc::new(store.inspect_image().unwrap());
        let (_, anchor) = validated_anchor(Arc::clone(&image), limits().snapshot).unwrap();
        let anchor = anchor.unwrap();
        let records = image.snapshot().entries();
        let checked = preflight(records, &anchor, true, limits()).unwrap();
        let start = records.partition_point(|(key, _)| *key < entry_key(1));
        assert!(std::ptr::eq(checked.deleted, &records[start..start + 2]));
        let mutation_bytes = checked.mutation_bytes;
        let batch = checked.stage().unwrap();
        let mut resulting = records
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>();
        for mutation in batch.mutations() {
            match mutation {
                Mutation::Put { key, value } => resulting.insert(key.clone(), value.clone()),
                Mutation::Delete { key } => resulting.remove(key),
            };
        }
        let resulting = resulting.into_iter().collect::<Vec<_>>();
        assert_eq!(resulting.len(), records.len() - 2);
        let manifest = SnapshotManifest::new(
            super::super::SNAPSHOT_LAYOUT_VERSION,
            2,
            3,
            4,
            anchor.source_manifest().latest_applied_digest(),
        )
        .unwrap();
        let encoded = SnapshotFormat::encode(&manifest, &resulting, limits().snapshot).unwrap();
        let exact = SnapshotCompactionLimits {
            snapshot: SnapshotLimits {
                max_encoded_bytes: encoded.len(),
                max_records: resulting.len(),
                max_key_bytes: resulting.iter().map(|(key, _)| key.len()).max().unwrap(),
                max_value_bytes: resulting
                    .iter()
                    .map(|(_, value)| value.len())
                    .max()
                    .unwrap(),
            },
            max_deleted_entries: 2,
            max_mutation_bytes: mutation_bytes,
        };
        assert_eq!(
            preflight(records, &anchor, false, exact)
                .unwrap()
                .stage()
                .unwrap()
                .mutations(),
            batch.mutations()
        );
        for boundary in 0..6 {
            for zero in [false, true] {
                let mut smaller = exact;
                let cap = match boundary {
                    0 => &mut smaller.max_deleted_entries,
                    1 => &mut smaller.max_mutation_bytes,
                    2 => &mut smaller.snapshot.max_encoded_bytes,
                    3 => &mut smaller.snapshot.max_records,
                    4 => &mut smaller.snapshot.max_key_bytes,
                    _ => &mut smaller.snapshot.max_value_bytes,
                };
                *cap = if zero { 0 } else { *cap - 1 };
                // A failed borrowed preflight cannot produce the type whose
                // stage method encodes the anchor or creates its batch.
                let error = preflight(records, &anchor, false, smaller).unwrap_err();
                match boundary {
                    0 => assert_eq!(
                        error,
                        SnapshotCompactionError::DeletionLimit {
                            requested: 2,
                            maximum: smaller.max_deleted_entries,
                        }
                    ),
                    1 => assert_eq!(
                        error,
                        SnapshotCompactionError::MutationLimit {
                            requested: mutation_bytes,
                            maximum: smaller.max_mutation_bytes,
                        }
                    ),
                    _ => {
                        let expected = match boundary {
                            2 => super::super::SnapshotLimit::EncodedBytes,
                            3 => super::super::SnapshotLimit::Records,
                            4 => super::super::SnapshotLimit::KeyBytes,
                            _ => super::super::SnapshotLimit::ValueBytes,
                        };
                        assert!(matches!(error, SnapshotCompactionError::Format(
                            SnapshotFormatError::LimitExceeded { kind, .. }
                        ) if kind == expected));
                    }
                }
            }
        }
        assert_eq!(store.snapshot().unwrap(), *image.snapshot());
    }

    #[derive(Clone)]
    struct Faulty<S> {
        inner: S,
        mode: u8,
        calls: Arc<AtomicUsize>,
        payload: Arc<String>,
        observed: Arc<Mutex<Vec<Mutation>>>,
    }
    impl<S: StateStore> StateStore for Faulty<S> {
        fn get(&self, _: &[u8]) -> Result<Option<Value>, StorageError> {
            panic!("apply must not read")
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            panic!("apply must not recapture")
        }
        fn scan_from(
            &self,
            _: &[u8],
            _: &[u8],
            _: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            panic!("apply must not scan")
        }
        fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed.lock().unwrap() = batch.mutations().to_vec();
            if self.mode == 1 {
                return Err(StorageError::LockPoisoned);
            }
            if self.mode == 2 {
                panic_any(Arc::clone(&self.payload));
            }
            self.inner.apply(batch)?;
            if self.mode == 3 {
                return Err(StorageError::LockPoisoned);
            }
            if self.mode == 4 {
                panic_any(Arc::clone(&self.payload));
            }
            Ok(())
        }
    }
    fn faults<S: ImageInstallBackend>(store: S, mode: u8) -> StoreSnapshot {
        seed(&store);
        let before = store.snapshot().unwrap();
        let mut owner = SnapshotCompactionOwner::new(store);
        let PreparedSnapshotCompaction {
            owner: borrowed,
            batch,
            receipt,
        } = owner.prepare(limits()).unwrap();
        let expected = batch.mutations().to_vec();
        assert_eq!(
            expected[..2],
            [
                Mutation::Delete { key: entry_key(1) },
                Mutation::Delete { key: entry_key(2) }
            ]
        );
        assert!(matches!(&expected[2..], [Mutation::Put { key, .. }] if *key == anchor_key()));
        // Private test seam at the *same* production apply function. The public
        // backend remains sealed; no production provenance/fault escape hatch.
        let calls = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new("original compaction panic".to_owned());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let fault = Faulty {
            inner: borrowed.store.clone(),
            mode,
            calls: Arc::clone(&calls),
            payload: Arc::clone(&payload),
            observed: Arc::clone(&observed),
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            apply_compaction(&fault, &mut borrowed.retired, batch, receipt)
        }));
        match mode {
            0 => assert_eq!(result.unwrap().unwrap(), receipt),
            1 | 3 => assert_eq!(
                result.unwrap(),
                Err(SnapshotCompactionError::Storage(StorageError::LockPoisoned))
            ),
            2 | 4 => assert!(Arc::ptr_eq(
                &result.unwrap_err().downcast::<Arc<String>>().unwrap(),
                &payload
            )),
            _ => unreachable!(),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*observed.lock().unwrap(), expected);
        let actual = fault.inner.snapshot().unwrap();
        if mode == 1 || mode == 2 {
            assert_eq!(actual, before);
        } else {
            let mut expected_rows = before
                .entries()
                .iter()
                .cloned()
                .collect::<std::collections::BTreeMap<_, _>>();
            for mutation in &expected {
                match mutation {
                    Mutation::Put { key, value } => {
                        expected_rows.insert(key.clone(), value.clone())
                    }
                    Mutation::Delete { key } => expected_rows.remove(key),
                };
            }
            let expected_rows = expected_rows.into_iter().collect::<Vec<_>>();
            assert_eq!(actual.entries(), expected_rows.as_slice());
            let replay = crate::CommittedReplay::open(fault.inner.clone()).unwrap();
            assert_eq!(replay.applied_index().unwrap(), 2);
        }
        drop(fault);
        if mode != 0 {
            assert!(owner.is_retired());
            assert_eq!(owner.snapshot(), Err(SnapshotCompactionError::Retired));
            assert!(matches!(
                owner.prepare(limits()),
                Err(SnapshotCompactionError::Retired)
            ));
            assert!(matches!(
                owner.into_store(),
                Err(SnapshotCompactionError::Retired)
            ));
        } else {
            assert!(!owner.is_retired());
            drop(owner.into_store().unwrap());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        actual
    }
    #[test]
    fn memory_exact_prefix_batch_errors_and_original_panics_retire() {
        for mode in 0..5 {
            faults(MemoryStore::default(), mode);
        }
    }
    #[test]
    fn fjall_exact_prefix_batch_errors_and_original_panics_full_reopen() {
        for mode in 0..5 {
            let dir = TempDir::new().unwrap();
            let expected = faults(FjallStore::open(dir.path()).unwrap(), mode);
            let reopened = FjallStore::open(dir.path()).unwrap();
            assert_eq!(reopened.snapshot().unwrap(), expected);
            crate::CommittedReplay::open(reopened).unwrap();
        }
    }
}
