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

use crate::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

/// Cloning shares one keyspace, so every clone reads what any other wrote.
#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    entries: Arc<RwLock<BTreeMap<Key, Value>>>,
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

    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(entries
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
