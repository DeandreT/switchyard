//! The in-memory backend.
//!
//! Holds the keyspace in a `BTreeMap`, so key ordering matches the durable
//! backend exactly and a prefix scan walks entries in the same sequence. State
//! lives only as long as the handles that share it, which is why this backend is
//! reserved for tests, deterministic simulations, and local development.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::{
    BoundedStateStore, CommittedStore, Key, Mutation, ReadBudget, ReadLimits, StateStore,
    StorageError, StoreSnapshot, Value, WriteBatch, replica::ReplicaReader,
};

/// Cloning shares one keyspace, so every clone reads what any other wrote.
#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    entries: Arc<RwLock<BTreeMap<Key, Value>>>,
}

/// A unique committed writer for a fresh in-memory replica keyspace.
///
/// The read-only handles share the records, but cannot recover or clone this
/// writer. This backend is not durable across process exit.
///
/// ```compile_fail
/// let writer = storage::MemoryReplicaStore::new();
/// let another_writer = writer.clone();
/// ```
///
/// ```compile_fail
/// use storage::CommittedStore;
/// let reader = storage::MemoryReplicaStore::new().reader();
/// let writable_store = reader.inner;
/// ```
#[derive(Default)]
pub struct MemoryReplicaStore {
    store: MemoryStore,
    initialized: bool,
}

impl std::fmt::Debug for MemoryReplicaStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryReplicaStore")
            .field("initialized", &self.initialized)
            .finish_non_exhaustive()
    }
}

impl MemoryReplicaStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CommittedStore for MemoryReplicaStore {
    type Reader = ReplicaReader<MemoryStore>;

    fn reader(&self) -> Self::Reader {
        ReplicaReader::new(self.store.clone())
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut entries = self
            .store
            .entries
            .write()
            .map_err(|_| StorageError::LockPoisoned)?;
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => {
                    entries.insert(key, value);
                }
                Mutation::Delete { key } => {
                    entries.remove(&key);
                }
            }
        }
        self.initialized = true;
        Ok(())
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        let _entries = self
            .store
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(self.initialized)
    }
}

impl StateStore for MemoryStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(entries.get(key).cloned())
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        // Holding the write lock across the whole batch is what makes it atomic
        // here: no reader can observe a partially applied batch.
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StorageError::LockPoisoned)?;
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => {
                    entries.insert(key, value);
                }
                Mutation::Delete { key } => {
                    entries.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(StoreSnapshot {
            entries: entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(entries
            .range(crate::scan_start(prefix, start).to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl BoundedStateStore for MemoryStore {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        let mut budget = ReadBudget::new(limits);
        let mut result = Vec::new();
        for (key, value) in entries.iter() {
            budget.consume(key.len(), value.len())?;
            result.push((key.clone(), value.clone()));
        }
        Ok(StoreSnapshot { entries: result })
    }
}

#[cfg(test)]
mod bounded_tests;
