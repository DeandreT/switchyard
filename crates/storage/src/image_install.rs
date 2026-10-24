//! Bounded offline replacement of one logical image, not semantic certification.
//!
//! The caller must exclude ALL writers, raw clones, keyspace/topology mutators,
//! and preexisting domain, journal, indexed and replay owners until completion.
//! Consuming a handle cannot enforce that exclusion. Never reuse stale owners.

use std::sync::Arc;

use thiserror::Error;

use crate::{
    FjallStore, MemoryStore, SnapshotProvenance, SnapshotWithProvenance, StateStore, StorageError,
    StoreSnapshot, WriteBatch, capture_logical_snapshot,
};

mod sealed {
    pub trait Sealed {}
}

/// Supported concrete backends. Sealed so an adapter cannot substitute invented
/// provenance or silently downgrade a Fjall target to logical inspection.
pub trait ImageInstallBackend: StateStore + sealed::Sealed {
    #[doc(hidden)]
    const NATIVE: bool;
    #[doc(hidden)]
    fn inspect_image(&self) -> Result<SnapshotWithProvenance, StorageError>;
}

impl sealed::Sealed for MemoryStore {}
impl ImageInstallBackend for MemoryStore {
    const NATIVE: bool = false;
    fn inspect_image(&self) -> Result<SnapshotWithProvenance, StorageError> {
        capture_logical_snapshot(self)
    }
}

impl sealed::Sealed for FjallStore {}
impl ImageInstallBackend for FjallStore {
    const NATIVE: bool = true;
    fn inspect_image(&self) -> Result<SnapshotWithProvenance, StorageError> {
        self.snapshot_with_provenance()
    }
}

/// Post-read acceptance and batch-construction bounds. These do not bound a
/// backend's snapshot allocation, allocator overhead or physical disk usage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageInstallLimits {
    /// Applied independently to incoming and old records.
    pub max_records: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Deletes plus puts; overlapping keys count twice.
    pub max_mutations: usize,
    /// Old deletion keys plus incoming keys and values (not old values).
    pub max_mutation_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageInstallLimit {
    Records,
    KeyBytes,
    ValueBytes,
    Mutations,
    MutationBytes,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ImageInstallError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("offline image owner is retired; close all handles and reopen")]
    Retired,
    #[error("image install {kind:?} requires {requested}, maximum {maximum}")]
    LimitExceeded {
        kind: ImageInstallLimit,
        requested: usize,
        maximum: usize,
    },
    #[error("image size is not representable")]
    SizeOverflow,
    #[error("image records must be strictly ordered and unique")]
    NonCanonicalRecords,
    #[error("native replacement requires nonempty keys")]
    EmptyNativeKey,
    #[error("replacement batch allocation failed")]
    AllocationFailed,
}

/// Operational result only, not a durability, authentication or recovery
/// certificate. Target evidence describes the inspected pre-replacement store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImageInstallReceipt {
    pub target_provenance: SnapshotProvenance,
    pub deleted_records: usize,
    pub inserted_records: usize,
    pub mutations: usize,
    pub logical_mutation_bytes: usize,
}

/// Non-Clone owner. No mutable backend or raw clone is exposed. On any returned
/// apply error or escaping panic, every I/O/release API refuses until reopen.
///
/// ```compile_fail
/// use storage::{MemoryStore, OfflineImageOwner};
/// let owner = OfflineImageOwner::new(MemoryStore::default());
/// let duplicate = owner.clone();
/// ```
#[derive(Debug)]
pub struct OfflineImageOwner<S: ImageInstallBackend> {
    store: S,
    retired: bool,
}

impl<S: ImageInstallBackend> OfflineImageOwner<S> {
    /// Caller-enforced exclusive offline ownership is a precondition, including
    /// exclusion of every earlier higher-level owner, not just active writes.
    pub fn new(store: S) -> Self {
        Self {
            store,
            retired: false,
        }
    }

    pub fn is_retired(&self) -> bool {
        self.retired
    }

    fn require_usable(&self) -> Result<(), ImageInstallError> {
        if self.retired {
            Err(ImageInstallError::Retired)
        } else {
            Ok(())
        }
    }

    pub fn snapshot(&self) -> Result<SnapshotWithProvenance, ImageInstallError> {
        self.require_usable()?;
        Ok(self.store.inspect_image()?)
    }

    pub fn into_store(self) -> Result<S, ImageInstallError> {
        self.require_usable()?;
        Ok(self.store)
    }

    /// Takes exactly one target image and checks every bound before constructing
    /// any batch. A higher layer may validate this same image before preparation.
    pub fn inspect<'a>(
        &'a mut self,
        incoming: &'a StoreSnapshot,
        limits: ImageInstallLimits,
    ) -> Result<ImageInstallInspection<'a, S>, ImageInstallError> {
        self.require_usable()?;
        let image = self.store.inspect_image()?;
        let receipt = preflight(&image, incoming, limits, S::NATIVE)?;
        Ok(ImageInstallInspection {
            owner: self,
            image: Arc::new(image),
            incoming,
            receipt,
        })
    }
}

/// Opaque target-bound inspection. Dropping it without preparation does not
/// mutate or retire the owner. No live read is needed to validate its image.
pub struct ImageInstallInspection<'a, S: ImageInstallBackend> {
    owner: &'a mut OfflineImageOwner<S>,
    image: Arc<SnapshotWithProvenance>,
    incoming: &'a StoreSnapshot,
    receipt: ImageInstallReceipt,
}

impl<'a, S: ImageInstallBackend> ImageInstallInspection<'a, S> {
    pub fn image(&self) -> &Arc<SnapshotWithProvenance> {
        &self.image
    }

    pub fn receipt(&self) -> ImageInstallReceipt {
        self.receipt
    }

    pub fn prepare(self) -> Result<PreparedImageInstall<'a, S>, ImageInstallError> {
        let mut batch = WriteBatch::default();
        batch
            .mutations
            .try_reserve_exact(self.receipt.mutations)
            .map_err(|_| ImageInstallError::AllocationFailed)?;
        for (key, _) in self.image.snapshot().entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in self.incoming.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        Ok(PreparedImageInstall {
            owner: self.owner,
            batch,
            receipt: self.receipt,
        })
    }
}

/// Single-use preparation cannot be applied to another owner or reused. All
/// validation and result allocation precede this value's only external apply.
///
/// ```compile_fail
/// use storage::{ImageInstallLimits, MemoryStore, OfflineImageOwner, StateStore};
/// let incoming = MemoryStore::default().snapshot().unwrap();
/// let mut owner = OfflineImageOwner::new(MemoryStore::default());
/// let limits = ImageInstallLimits { max_records: 0, max_key_bytes: 0,
///     max_value_bytes: 0, max_mutations: 0, max_mutation_bytes: 0 };
/// let prepared = owner.inspect(&incoming, limits).unwrap().prepare().unwrap();
/// drop(owner); // The exact target remains mutably borrowed until apply/drop.
/// prepared.apply().unwrap();
/// ```
pub struct PreparedImageInstall<'a, S: ImageInstallBackend> {
    owner: &'a mut OfflineImageOwner<S>,
    batch: WriteBatch,
    receipt: ImageInstallReceipt,
}

impl<S: ImageInstallBackend> PreparedImageInstall<'_, S> {
    pub fn apply(self) -> Result<ImageInstallReceipt, ImageInstallError> {
        // Deliberately do not catch, replace, retry or compensate a backend panic.
        self.owner.retired = true;
        self.owner.store.apply(self.batch)?;
        // Receipt is already built; no post-apply allocation or fallible work.
        self.owner.retired = false;
        Ok(self.receipt)
    }
}

fn bound(
    kind: ImageInstallLimit,
    requested: usize,
    maximum: usize,
) -> Result<(), ImageInstallError> {
    if requested > maximum {
        Err(ImageInstallError::LimitExceeded {
            kind,
            requested,
            maximum,
        })
    } else {
        Ok(())
    }
}

fn record_bounds(
    image: &StoreSnapshot,
    limits: ImageInstallLimits,
    native: bool,
) -> Result<(), ImageInstallError> {
    bound(
        ImageInstallLimit::Records,
        image.entries().len(),
        limits.max_records,
    )?;
    let mut previous: Option<&[u8]> = None;
    for (key, value) in image.entries() {
        if previous.is_some_and(|previous| previous >= key.as_slice()) {
            return Err(ImageInstallError::NonCanonicalRecords);
        }
        previous = Some(key);
        record_length_bounds(key.len(), value.len(), limits, native)?;
    }
    Ok(())
}

fn record_length_bounds(
    key_bytes: usize,
    value_bytes: usize,
    limits: ImageInstallLimits,
    native: bool,
) -> Result<(), ImageInstallError> {
    bound(ImageInstallLimit::KeyBytes, key_bytes, limits.max_key_bytes)?;
    bound(
        ImageInstallLimit::ValueBytes,
        value_bytes,
        limits.max_value_bytes,
    )?;
    if native {
        if key_bytes == 0 {
            return Err(ImageInstallError::EmptyNativeKey);
        }
        bound(
            ImageInstallLimit::KeyBytes,
            key_bytes,
            usize::from(u16::MAX),
        )?;
        bound(
            ImageInstallLimit::ValueBytes,
            value_bytes,
            u32::MAX as usize,
        )?;
    }
    Ok(())
}

fn preflight(
    old: &SnapshotWithProvenance,
    incoming: &StoreSnapshot,
    limits: ImageInstallLimits,
    native: bool,
) -> Result<ImageInstallReceipt, ImageInstallError> {
    record_bounds(incoming, limits, native)?;
    record_bounds(old.snapshot(), limits, native)?;
    let mutations = mutation_count(
        old.snapshot().entries().len(),
        incoming.entries().len(),
        limits.max_mutations,
        native,
    )?;
    let mut bytes = 0_usize;
    for (key, _) in old.snapshot().entries() {
        bytes = bytes
            .checked_add(key.len())
            .ok_or(ImageInstallError::SizeOverflow)?;
    }
    for (key, value) in incoming.entries() {
        bytes = bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or(ImageInstallError::SizeOverflow)?;
    }
    bound(
        ImageInstallLimit::MutationBytes,
        bytes,
        limits.max_mutation_bytes,
    )?;
    Ok(ImageInstallReceipt {
        target_provenance: *old.provenance(),
        deleted_records: old.snapshot().entries().len(),
        inserted_records: incoming.entries().len(),
        mutations,
        logical_mutation_bytes: bytes,
    })
}

fn mutation_count(
    old: usize,
    incoming: usize,
    maximum: usize,
    native: bool,
) -> Result<usize, ImageInstallError> {
    let count = old
        .checked_add(incoming)
        .ok_or(ImageInstallError::SizeOverflow)?;
    bound(ImageInstallLimit::Mutations, count, maximum)?;
    if native {
        bound(ImageInstallLimit::Mutations, count, u32::MAX as usize)?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Key, Mutation, Value};
    use std::{
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tempfile::TempDir;

    fn caps() -> ImageInstallLimits {
        ImageInstallLimits {
            max_records: usize::MAX,
            max_key_bytes: usize::MAX,
            max_value_bytes: usize::MAX,
            max_mutations: usize::MAX,
            max_mutation_bytes: usize::MAX,
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Fault {
        None,
        ErrorBefore,
        ErrorAfter,
        PanicBefore,
        PanicAfter,
        ReadError,
    }

    #[derive(Default)]
    struct Calls {
        reads: AtomicUsize,
        applies: AtomicUsize,
        batches: Mutex<Vec<WriteBatch>>,
    }

    struct Instrumented<S> {
        inner: S,
        fault: Fault,
        calls: Arc<Calls>,
        payload: Arc<String>,
    }

    impl<S> Clone for Instrumented<S> {
        fn clone(&self) -> Self {
            panic!("the live install target must never be cloned")
        }
    }

    impl<S: ImageInstallBackend> StateStore for Instrumented<S> {
        fn get(&self, _: &[u8]) -> Result<Option<Value>, StorageError> {
            panic!("live get")
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            panic!("unprovenanced snapshot fallback")
        }
        fn scan_from(
            &self,
            _: &[u8],
            _: &[u8],
            _: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            panic!("live scan")
        }
        fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
            self.calls.applies.fetch_add(1, Ordering::SeqCst);
            self.calls.batches.lock().unwrap().push(batch.clone());
            if self.fault == Fault::ErrorBefore {
                return Err(StorageError::LockPoisoned);
            }
            if self.fault == Fault::PanicBefore {
                panic_any(Arc::clone(&self.payload));
            }
            self.inner.apply(batch)?;
            if self.fault == Fault::ErrorAfter {
                return Err(StorageError::LockPoisoned);
            }
            if self.fault == Fault::PanicAfter {
                panic_any(Arc::clone(&self.payload));
            }
            Ok(())
        }
    }

    impl<S> sealed::Sealed for Instrumented<S> {}
    impl<S: ImageInstallBackend> ImageInstallBackend for Instrumented<S> {
        const NATIVE: bool = S::NATIVE;
        fn inspect_image(&self) -> Result<SnapshotWithProvenance, StorageError> {
            self.calls.reads.fetch_add(1, Ordering::SeqCst);
            if self.fault == Fault::ReadError {
                return Err(StorageError::LockPoisoned);
            }
            self.inner.inspect_image()
        }
    }

    fn rows(first: bool) -> StoreSnapshot {
        StoreSnapshot {
            entries: if first {
                vec![
                    (b"a".to_vec(), b"old".to_vec()),
                    (b"shared".to_vec(), vec![]),
                ]
            } else {
                vec![
                    (b"b".to_vec(), vec![0, 0xff]),
                    (b"shared".to_vec(), b"new".to_vec()),
                ]
            },
        }
    }

    fn fault_case<S: ImageInstallBackend>(store: S, fault: Fault) -> StoreSnapshot {
        let old = rows(true);
        let new = rows(false);
        let mut batch = WriteBatch::default();
        for (key, value) in old.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        store.apply(batch).unwrap();
        let retained = store.snapshot().unwrap();
        let calls = Arc::new(Calls::default());
        let payload = Arc::new("original backend panic, not an install replacement".to_owned());
        let target = Instrumented {
            inner: store,
            fault,
            calls: Arc::clone(&calls),
            payload: Arc::clone(&payload),
        };
        let mut owner = OfflineImageOwner::new(target);
        if fault == Fault::ReadError {
            assert_eq!(
                owner.inspect(&new, caps()).err(),
                Some(ImageInstallError::Storage(StorageError::LockPoisoned))
            );
            assert!(!owner.is_retired());
            assert_eq!(calls.reads.load(Ordering::SeqCst), 1);
            assert_eq!(calls.applies.load(Ordering::SeqCst), 0);
            let actual = owner.store.inner.snapshot().unwrap();
            assert_eq!(actual, old);
            assert_eq!(retained, old);
            drop(owner);
            return actual;
        }
        let prepared = owner.inspect(&new, caps()).unwrap().prepare().unwrap();
        assert_eq!(calls.reads.load(Ordering::SeqCst), 1);
        let result = catch_unwind(AssertUnwindSafe(|| prepared.apply()));
        match fault {
            Fault::None => {
                assert!(result.unwrap().is_ok());
                assert!(!owner.is_retired());
            }
            Fault::ErrorBefore | Fault::ErrorAfter => {
                assert_eq!(
                    result.unwrap(),
                    Err(ImageInstallError::Storage(StorageError::LockPoisoned))
                );
                assert!(owner.is_retired());
            }
            Fault::PanicBefore | Fault::PanicAfter => {
                let caught = result.unwrap_err().downcast::<Arc<String>>().unwrap();
                assert!(
                    Arc::ptr_eq(&caught, &payload),
                    "original panic payload must escape unchanged"
                );
                assert!(owner.is_retired());
            }
            Fault::ReadError => unreachable!(),
        }
        assert_eq!(calls.applies.load(Ordering::SeqCst), 1);
        let expected = old
            .entries()
            .iter()
            .map(|(key, _)| Mutation::Delete { key: key.clone() })
            .chain(new.entries().iter().map(|(key, value)| Mutation::Put {
                key: key.clone(),
                value: value.clone(),
            }))
            .collect::<Vec<_>>();
        assert_eq!(calls.batches.lock().unwrap()[0].mutations(), expected);
        if owner.is_retired() {
            assert_eq!(owner.snapshot(), Err(ImageInstallError::Retired));
            assert_eq!(
                owner.inspect(&new, caps()).err(),
                Some(ImageInstallError::Retired)
            );
            assert_eq!(calls.reads.load(Ordering::SeqCst), 1);
            assert_eq!(calls.applies.load(Ordering::SeqCst), 1);
        }
        // Private fixture observation only: this bypass is deliberately NOT an
        // owner API. The Fjall test below then drops every handle and reopens.
        let actual = owner.store.inner.snapshot().unwrap();
        let expected_new = matches!(fault, Fault::None | Fault::ErrorAfter | Fault::PanicAfter);
        assert_eq!(actual, if expected_new { new } else { old.clone() });
        assert_eq!(retained, old);
        if owner.is_retired() {
            assert!(matches!(
                owner.into_store(),
                Err(ImageInstallError::Retired)
            ));
            assert_eq!(calls.reads.load(Ordering::SeqCst), 1);
            assert_eq!(calls.applies.load(Ordering::SeqCst), 1);
        } else {
            drop(owner.into_store().unwrap());
        }
        actual
    }

    #[test]
    fn memory_one_image_one_batch_and_original_faults() {
        for fault in [
            Fault::None,
            Fault::ErrorBefore,
            Fault::ErrorAfter,
            Fault::PanicBefore,
            Fault::PanicAfter,
            Fault::ReadError,
        ] {
            fault_case(MemoryStore::default(), fault);
        }
    }

    #[test]
    fn fjall_one_image_one_batch_and_original_faults_full_reopen() {
        for fault in [
            Fault::None,
            Fault::ErrorBefore,
            Fault::ErrorAfter,
            Fault::PanicBefore,
            Fault::PanicAfter,
            Fault::ReadError,
        ] {
            let dir = TempDir::new().unwrap();
            let expected = fault_case(FjallStore::open(dir.path()).unwrap(), fault);
            let reopened = FjallStore::open(dir.path()).unwrap();
            assert_eq!(reopened.snapshot().unwrap(), expected);
        }
    }

    // The public backend is sealed; observe apply calls here rather than adding
    // an integration-test wrapper that could supply unrelated provenance.
    fn limits_and_abandonment_never_apply<S: ImageInstallBackend>(store: S) -> StoreSnapshot {
        let old = StoreSnapshot {
            entries: vec![
                (b"old".to_vec(), b"12345".to_vec()),
                (b"same".to_vec(), vec![]),
            ],
        };
        let new = StoreSnapshot {
            entries: vec![
                (b"new".to_vec(), b"123456".to_vec()),
                (b"same".to_vec(), b"next".to_vec()),
            ],
        };
        let mut batch = WriteBatch::default();
        for (key, value) in old.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        store.apply(batch).unwrap();
        let calls = Arc::new(Calls::default());
        let mut owner = OfflineImageOwner::new(Instrumented {
            inner: store,
            fault: Fault::None,
            calls: Arc::clone(&calls),
            payload: Arc::new(String::new()),
        });
        let exact = ImageInstallLimits {
            max_records: 2,
            max_key_bytes: 4,
            max_value_bytes: 6,
            max_mutations: 4,
            max_mutation_bytes: 24,
        };
        for kind in [
            ImageInstallLimit::Records,
            ImageInstallLimit::KeyBytes,
            ImageInstallLimit::ValueBytes,
            ImageInstallLimit::Mutations,
            ImageInstallLimit::MutationBytes,
        ] {
            let mut smaller = exact;
            let cap = match kind {
                ImageInstallLimit::Records => &mut smaller.max_records,
                ImageInstallLimit::KeyBytes => &mut smaller.max_key_bytes,
                ImageInstallLimit::ValueBytes => &mut smaller.max_value_bytes,
                ImageInstallLimit::Mutations => &mut smaller.max_mutations,
                ImageInstallLimit::MutationBytes => &mut smaller.max_mutation_bytes,
            };
            let requested = *cap;
            *cap -= 1;
            assert_eq!(
                owner.inspect(&new, smaller).err(),
                Some(ImageInstallError::LimitExceeded {
                    kind,
                    requested,
                    maximum: requested - 1,
                })
            );
            assert_eq!(calls.applies.load(Ordering::SeqCst), 0, "{kind:?}");
            assert!(calls.batches.lock().unwrap().is_empty());
            assert_eq!(owner.snapshot().unwrap().snapshot(), &old);
            assert!(!owner.is_retired());
        }
        let tiny = StoreSnapshot {
            entries: vec![(b"x".to_vec(), b"y".to_vec())],
        };
        for (kind, maximum, requested) in [
            (ImageInstallLimit::Records, 1, 2),
            (ImageInstallLimit::KeyBytes, 3, 4),
            (ImageInstallLimit::ValueBytes, 4, 5),
        ] {
            let mut smaller = caps();
            match kind {
                ImageInstallLimit::Records => smaller.max_records = maximum,
                ImageInstallLimit::KeyBytes => smaller.max_key_bytes = maximum,
                ImageInstallLimit::ValueBytes => smaller.max_value_bytes = maximum,
                _ => unreachable!(),
            }
            assert_eq!(
                owner.inspect(&tiny, smaller).err(),
                Some(ImageInstallError::LimitExceeded {
                    kind,
                    requested,
                    maximum,
                })
            );
            assert_eq!(calls.applies.load(Ordering::SeqCst), 0, "old {kind:?}");
            assert!(calls.batches.lock().unwrap().is_empty());
            assert!(!owner.is_retired());
        }
        if S::NATIVE {
            for key in [vec![], vec![1; usize::from(u16::MAX) + 1]] {
                let expected = if key.is_empty() {
                    ImageInstallError::EmptyNativeKey
                } else {
                    ImageInstallError::LimitExceeded {
                        kind: ImageInstallLimit::KeyBytes,
                        requested: key.len(),
                        maximum: usize::from(u16::MAX),
                    }
                };
                let incoming = StoreSnapshot {
                    entries: vec![(key, vec![])],
                };
                assert_eq!(owner.inspect(&incoming, caps()).err(), Some(expected));
                assert_eq!(calls.applies.load(Ordering::SeqCst), 0);
                assert!(calls.batches.lock().unwrap().is_empty());
                assert!(!owner.is_retired());
            }
        }
        drop(owner.inspect(&new, exact).unwrap().prepare().unwrap());
        assert_eq!(calls.applies.load(Ordering::SeqCst), 0);
        assert!(calls.batches.lock().unwrap().is_empty());
        assert_eq!(owner.snapshot().unwrap().snapshot(), &old);
        assert!(!owner.is_retired());
        owner
            .inspect(&new, exact)
            .unwrap()
            .prepare()
            .unwrap()
            .apply()
            .unwrap();
        assert_eq!(calls.applies.load(Ordering::SeqCst), 1);
        assert_eq!(calls.batches.lock().unwrap().len(), 1);
        assert_eq!(owner.snapshot().unwrap().snapshot(), &new);
        drop(owner.into_store().unwrap());
        new
    }

    #[test]
    fn memory_all_limit_refusals_and_abandoned_preparation_make_zero_apply_calls() {
        limits_and_abandonment_never_apply(MemoryStore::default());
    }

    #[test]
    fn fjall_all_limit_refusals_and_abandoned_preparation_make_zero_apply_calls() {
        let dir = TempDir::new().unwrap();
        let new = limits_and_abandonment_never_apply(FjallStore::open(dir.path()).unwrap());
        assert_eq!(FjallStore::open(dir.path()).unwrap().snapshot().unwrap(), new);
    }

    #[test]
    fn malformed_logical_images_refuse_before_preparation() {
        for entries in [
            vec![(vec![2], vec![]), (vec![1], vec![])],
            vec![(vec![1], vec![]), (vec![1], vec![3])],
        ] {
            let malformed = StoreSnapshot { entries };
            let mut owner = OfflineImageOwner::new(MemoryStore::default());
            assert_eq!(
                owner.inspect(&malformed, caps()).err(),
                Some(ImageInstallError::NonCanonicalRecords)
            );
            assert!(owner.snapshot().unwrap().snapshot().entries().is_empty());
        }
    }

    #[test]
    fn native_representability_boundaries_without_giant_allocations() {
        assert_eq!(
            record_length_bounds(1, u32::MAX as usize, caps(), true),
            Ok(())
        );
        assert_eq!(
            mutation_count(1, u32::MAX as usize - 1, usize::MAX, true),
            Ok(u32::MAX as usize)
        );
        assert_eq!(
            mutation_count(usize::MAX, 1, usize::MAX, false),
            Err(ImageInstallError::SizeOverflow)
        );
        if let Some(over) = (u32::MAX as usize).checked_add(1) {
            assert_eq!(
                record_length_bounds(1, over, caps(), true),
                Err(ImageInstallError::LimitExceeded {
                    kind: ImageInstallLimit::ValueBytes,
                    requested: over,
                    maximum: u32::MAX as usize,
                })
            );
            assert_eq!(record_length_bounds(1, over, caps(), false), Ok(()));
            assert!(mutation_count(1, u32::MAX as usize, usize::MAX, true).is_err());
        }
        let max_key = StoreSnapshot {
            entries: vec![(vec![1; usize::from(u16::MAX)], vec![])],
        };
        assert!(record_bounds(&max_key, caps(), true).is_ok());
        let too_long = StoreSnapshot {
            entries: vec![(vec![1; usize::from(u16::MAX) + 1], vec![])],
        };
        assert!(record_bounds(&too_long, caps(), true).is_err());
    }
}
