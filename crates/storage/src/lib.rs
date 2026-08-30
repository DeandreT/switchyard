//! The atomic storage contract and its backends.
//!
//! Everything above this crate sees only [`StateStore`]: read one key, walk an
//! ordered prefix, commit a batch. Both backends implement that contract and
//! the same conformance suite runs against both, so a queue behaves identically
//! whether its state lives in memory or on disk.

#![forbid(unsafe_code)]

mod durable;
mod memory;

use thiserror::Error;

pub use crate::{
    durable::{FjallStore, STORE_FORMAT_V1},
    memory::MemoryStore,
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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StorageError {
    #[error("storage lock was poisoned")]
    LockPoisoned,
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
