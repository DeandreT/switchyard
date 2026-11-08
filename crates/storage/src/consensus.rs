//! Isolated custody for fresh consensus stores. No StateStore, Clone, image
//! installation, raw handle or conversion into an unindexed writer is exposed.

use std::path::Path;

use crate::{FjallStore, Key, MemoryStore, StateStore, StorageError, Value, WriteBatch};

/// Trusted atomic backend contract, deliberately separate from StateStore.
///
/// Reads/scans are ordered and coherent with successful atomic batches. A durable
/// implementation must synchronously persist a whole batch before returning Ok.
/// Errors/panics may occur AFTER persistence; the higher owner must retire until
/// reopen. A custom implementation must exclude all external writers, raw clones
/// and topology mutators for the owner's entire lifetime. This trait cannot prove
/// that exclusion or durability. Memory is explicitly volatile.
pub trait ConsensusBackend: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError>;
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError>;
    fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError>;
}

/// Volatile, uncloneable backend; moving it into a consensus owner leaves no
/// ordinary alias capable of unindexed domain writes.
///
/// ```compile_fail
/// let store = storage::ConsensusMemoryStore::default();
/// let alias = store.clone();
/// ```
/// ```compile_fail
/// fn ordinary<S: storage::StateStore>(_: S) {}
/// ordinary(storage::ConsensusMemoryStore::default());
/// ```
#[derive(Debug, Default)]
pub struct ConsensusMemoryStore {
    inner: MemoryStore,
}

/// Non-Clone owner of a format-4 directory, using Fjall's existing atomic SyncAll
/// batches and directory lock. No ordinary format-3 handle can be extracted.
///
/// ```compile_fail
/// let store = storage::ConsensusFjallStore::open("consensus").unwrap();
/// let alias = store.clone();
/// ```
#[derive(Debug)]
pub struct ConsensusFjallStore {
    inner: FjallStore,
}

impl ConsensusFjallStore {
    /// Create only in an absent/empty directory; existing databases require an
    /// exact meta/records inventory and sole format-4 marker. No auto-adoption or
    /// metadata repair. Native recovery IO is not a byte-identical-file promise.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            inner: FjallStore::open_consensus(directory.as_ref())?,
        })
    }
}

macro_rules! backend {
    ($ty:ty) => {
        impl ConsensusBackend for $ty {
            fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
                self.inner.get(key)
            }
            fn scan_from(
                &self,
                prefix: &[u8],
                start: &[u8],
                limit: usize,
            ) -> Result<Vec<(Key, Value)>, StorageError> {
                if limit == 0 || limit > 64 {
                    return Err(StorageError::CorruptMetadata {
                        detail: "consensus scans require 1..=64 records".into(),
                    });
                }
                self.inner.scan_from(prefix, start, limit)
            }
            fn apply(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
                self.inner.apply(batch)
            }
        }
    };
}
backend!(ConsensusMemoryStore);
backend!(ConsensusFjallStore);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_legacy_image_and_snapshot_paths_explicitly_refuse_four() {
        let directory = tempfile::tempdir().unwrap();
        let store = ConsensusFjallStore::open(directory.path()).unwrap();
        // Only this private test can name the hidden ordinary inner handle.
        assert!(matches!(
            store.inner.snapshot_with_provenance(),
            Err(StorageError::UnsupportedStoreFormat {
                found: 4,
                expected: 3
            })
        ));
        let mut image = crate::OfflineImageOwner::new(store.inner);
        assert!(matches!(
            image.snapshot(),
            Err(crate::ImageInstallError::Storage(
                StorageError::UnsupportedStoreFormat {
                    found: 4,
                    expected: 3
                }
            ))
        ));
        let incoming = crate::StoreSnapshot::default();
        assert!(
            image
                .inspect(
                    &incoming,
                    crate::ImageInstallLimits {
                        max_records: 8,
                        max_key_bytes: 128,
                        max_value_bytes: 128,
                        max_mutations: 16,
                        max_mutation_bytes: 4096
                    }
                )
                .is_err()
        );
    }
}
