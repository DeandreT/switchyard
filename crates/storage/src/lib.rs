#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use thiserror::Error;

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
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError>;
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError>;

    /// Returns up to `limit` entries whose key starts with `prefix`, in ascending
    /// key order. Callers encode index keys so that lexicographic order is the
    /// order they need to walk, and rely on `limit` to bound the work a single
    /// state-machine command performs.
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError>;
}

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
        let mut entries = self
            .entries
            .write()
            .map_err(|_| StorageError::LockPoisoned)?;
        for mutation in batch.mutations {
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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StorageError {
    #[error("storage lock was poisoned")]
    LockPoisoned,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_a_batch_atomically_to_the_memory_view() -> Result<(), StorageError> {
        let store = MemoryStore::default();
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

    #[test]
    fn scans_a_prefix_in_key_order_within_its_limit() -> Result<(), StorageError> {
        let store = MemoryStore::default();
        store.apply(
            WriteBatch::default()
                .put(b"ready:\x00\x02".to_vec(), Vec::new())
                .put(b"ready:\x00\x01".to_vec(), Vec::new())
                .put(b"ready:\x00\x03".to_vec(), Vec::new())
                .put(b"locks:\x00\x01".to_vec(), Vec::new()),
        )?;

        let scanned = store.scan_prefix(b"ready:", 2)?;
        assert_eq!(
            scanned
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            vec![b"ready:\x00\x01".to_vec(), b"ready:\x00\x02".to_vec()]
        );
        assert_eq!(store.scan_prefix(b"ready:", 16)?.len(), 3);
        assert_eq!(store.scan_prefix(b"absent:", 16)?.len(), 0);
        Ok(())
    }
}
