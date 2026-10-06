//! The atomic storage contract and its backends.
//!
//! Ordinary state uses [`StateStore`]: read one key, walk an ordered prefix,
//! commit a batch. Isolated replica state uses a unique [`CommittedStore`]
//! writer and read-only views. Both backends have matching conformance suites,
//! so a queue behaves identically whether its state lives in memory or on disk.
//! [`BoundedStateStore`] optionally adds complete, stable reads with explicit
//! row and logical key/value byte limits; ordinary reads remain unchanged.

#![forbid(unsafe_code)]

#[cfg(test)]
mod batch_reservation_tests;
mod bounded;
mod catalog;
mod complete_catalog_state;
mod durable;
mod memory;
mod protected_state;
mod replica;

use thiserror::Error;

pub(crate) use bounded::ReadBudget;
pub use bounded::{BoundedStateStore, ReadLimits};
pub use catalog::{
    CatalogBoundsError, CatalogCommittedStore, CatalogReadError, MAX_CATALOG_ARTIFACT_BYTES,
    MAX_CATALOG_METADATA_BYTES, SnapshotCatalogReader, SnapshotCatalogRecord,
    StoredSnapshotCatalog,
};

pub use complete_catalog_state::{
    COMPLETE_CATALOG_STATE_RECORD_LIMITS, CompleteCatalogReplicaStateReader,
    MAX_COMPLETE_CATALOG_STATE_BYTES, StoredCatalogReplicaState,
};

pub use durable::{
    ACTIVE_PROTECTED_STATE_STORE_FORMAT, FjallProtectedStateOpenError, FjallProtectedStateReader,
    FjallProtectedStateStore,
};

pub use memory::{MemoryProtectedStateReader, MemoryProtectedStateStore};
pub use protected_state::{
    MAX_PROTECTED_STATE_BYTES, MAX_PROTECTED_STATE_FENCE_BYTES, PROTECTED_STATE_RECORD_LIMITS,
    ProtectedStateError, ProtectedStatePublication, ProtectedStateReader, StoredProtectedState,
};

pub use crate::{
    durable::{
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT, ACTIVE_REPLICA_STORE_FORMAT, ACTIVE_STORE_FORMAT,
        FjallCatalogReader, FjallCatalogReplicaStore, FjallReplicaStore, FjallStore,
        STORE_FORMAT_V1, STORE_FORMAT_V2, STORE_FORMAT_V3, STORE_FORMAT_V4, STORE_FORMAT_V5,
        STORE_FORMAT_V6, STORE_FORMAT_V7, STORE_FORMAT_V8, STORE_FORMAT_V9, STORE_FORMAT_V10,
        STORE_FORMAT_V11, STORE_FORMAT_V12, STORE_FORMAT_V13, STORE_FORMAT_V14,
    },
    memory::{MemoryCatalogReader, MemoryCatalogReplicaStore, MemoryReplicaStore, MemoryStore},
};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put { key: Key, value: Value },
    Delete { key: Key },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    mutations: Vec<Mutation>,
}

impl WriteBatch {
    /// Fallibly reserves mutation capacity without changing batch contents.
    ///
    /// This bounds neither mutation data nor allocator capacity/RSS and grants
    /// no write authority. Callers still own their separate storage capability.
    pub fn try_reserve_mutations(
        &mut self,
        additional: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        self.mutations.try_reserve_exact(additional)
    }

    pub fn put(mut self, key: impl Into<Key>, value: impl Into<Value>) -> Self {
        self.push_put(key, value);
        self
    }

    pub fn delete(mut self, key: impl Into<Key>) -> Self {
        self.push_delete(key);
        self
    }

    pub fn push_put(&mut self, key: impl Into<Key>, value: impl Into<Value>) {
        self.mutations.push(Mutation::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn push_delete(&mut self, key: impl Into<Key>) {
        self.mutations.push(Mutation::Delete { key: key.into() });
    }

    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    /// Takes the mutations in the order they were recorded. A backend applies
    /// them in that order, so the last mutation naming a key decides its fate.
    pub fn into_mutations(self) -> Vec<Mutation> {
        self.mutations
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreSnapshot {
    entries: Vec<(Key, Value)>,
}

impl StoreSnapshot {
    pub fn entries(&self) -> &[(Key, Value)] {
        &self.entries
    }
}

pub trait StateStore: Clone + Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError>;

    /// Commits every mutation in `batch` as one unit. A later reader observes
    /// all of the batch or none of it, including a reader that opens the store
    /// again after the process died mid-commit. A durable backend has persisted
    /// the batch before this returns, so nothing is acknowledged that a power
    /// failure could take back.
    ///
    /// An error does not prove the batch is absent. A complete journal entry
    /// can reach storage before a persistence error is reported, and reopening
    /// can recover the entire batch. Such a commit decision is unknown to the
    /// caller: publish no success effects and do not assume retry idempotence.
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError>;

    /// Returns every entry in the store, in ascending key order, read at a
    /// single point in time.
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError>;

    /// Returns up to `limit` entries whose key starts with `prefix` and sorts at
    /// or after `start`, in ascending key order.
    ///
    /// `start` lets a caller resume a walk past entries it has already decided
    /// about — skipping every entry of one session, say — without paying to read
    /// them again. A `start` below `prefix` yields the same entries as starting
    /// at `prefix`, since nothing outside the prefix is returned either way.
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError>;

    /// Returns up to `limit` entries whose key starts with `prefix`, in ascending
    /// key order. Callers encode index keys so that lexicographic order is the
    /// order they need to walk, and rely on `limit` to bound the work a single
    /// state-machine command performs.
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.scan_from(prefix, prefix, limit)
    }
}

/// A trusted, unique writer for replica state.
///
/// The reader must observe exactly this writer's records and must refuse every
/// [`StateStore::apply`] call. Implementations commit the supplied mutations
/// and their initialized flag atomically, with the same durability and unknown
/// error-decision contract as [`StateStore::apply`]. An empty privileged commit
/// still initializes the store.
///
/// This is a low-level storage capability, not evidence that a batch came from
/// consensus or passed authorization. Its owner must serialize preparation and
/// commit; the interface supplies no compare-and-swap or transaction isolation
/// across earlier reads. Domain adapters should expose typed operations rather
/// than this raw batch capability or the writer itself.
pub trait CommittedStore: Send + 'static {
    type Reader: StateStore;

    fn reader(&self) -> Self::Reader;
    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError>;
    fn is_initialized(&self) -> Result<bool, StorageError>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StorageError {
    #[error("storage lock was poisoned")]
    LockPoisoned,
    #[error("storage read limit exceeded")]
    ReadLimitExceeded,
    #[error("replica state requires a committed-store writer")]
    ReplicaWriteRequired,
    #[error("replica metadata cannot be opened as a standalone store")]
    ReplicaMetadataInStandalone,
    /// A durable backend refused an operation.
    ///
    /// The backend's own error is rendered to a string rather than carried,
    /// because `StorageError` has to stay comparable: the domain crate's error
    /// enum derives `PartialEq` so that a test can assert the exact rejection a
    /// command produced.
    #[error("durable storage failed to {operation}: {detail}")]
    Backend {
        operation: &'static str,
        detail: String,
    },
    #[error(
        "store directory holds format version {found}, but this build reads and writes version {expected}"
    )]
    UnsupportedStoreFormat { found: u32, expected: u32 },
    #[error("store metadata is unreadable: {detail}")]
    CorruptMetadata { detail: String },
}

/// Where a prefix scan actually begins.
///
/// A scan never leaves its prefix, so a `start` that sorts below the prefix
/// begins at the prefix rather than at unrelated keys in between — otherwise the
/// walk would end on the first of those instead of returning the prefix.
pub(crate) fn scan_start<'a>(prefix: &'a [u8], start: &'a [u8]) -> &'a [u8] {
    if start < prefix { prefix } else { start }
}

impl StorageError {
    pub(crate) fn backend(operation: &'static str, error: &dyn std::error::Error) -> Self {
        Self::Backend {
            operation,
            detail: error.to_string(),
        }
    }
}
