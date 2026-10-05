use std::fmt;

use crate::{
    CatalogCommittedStore, CatalogReadError, SnapshotCatalogReader, SnapshotCatalogRecord,
    StoredSnapshotCatalog,
};

use super::*;

#[derive(Default)]
struct CatalogState {
    entries: BTreeMap<Key, Value>,
    initialized: bool,
    catalog: Option<(Vec<u8>, Vec<u8>)>,
}

/// A unique writer for fresh in-memory business records and an opaque catalog.
///
/// All state is shared under one lock. This backend is not durable across
/// process exit; ordinary replica constructors and their profiles are unchanged.
///
/// ```compile_fail
/// let writer = storage::MemoryCatalogReplicaStore::new();
/// let another = writer.clone();
/// ```
#[derive(Default)]
pub struct MemoryCatalogReplicaStore {
    state: Arc<RwLock<CatalogState>>,
}

impl MemoryCatalogReplicaStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn commit_inner(
        &mut self,
        batch: WriteBatch,
        catalog: Option<SnapshotCatalogRecord<'_>>,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().map_err(|_| StorageError::LockPoisoned)?;
        validate_state(&state)?;
        let catalog =
            catalog.map(|record| (record.metadata().to_vec(), record.artifact().to_vec()));
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => {
                    state.entries.insert(key, value);
                }
                Mutation::Delete { key } => {
                    state.entries.remove(&key);
                }
            }
        }
        if let Some(catalog) = catalog {
            state.catalog = Some(catalog);
        }
        state.initialized = true;
        Ok(())
    }
}

impl fmt::Debug for MemoryCatalogReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryCatalogReplicaStore")
            .finish_non_exhaustive()
    }
}

impl CommittedStore for MemoryCatalogReplicaStore {
    type Reader = ReplicaReader<MemoryCatalogRecords>;

    fn reader(&self) -> Self::Reader {
        ReplicaReader::new(MemoryCatalogRecords {
            state: Arc::clone(&self.state),
        })
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commit_inner(batch, None)
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        validate_state(&state)?;
        Ok(state.initialized)
    }
}

impl CatalogCommittedStore for MemoryCatalogReplicaStore {
    type CatalogReader = MemoryCatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader {
        MemoryCatalogReader {
            state: Arc::clone(&self.state),
        }
    }

    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.commit_inner(batch, Some(catalog))
    }
}

/// An opaque business-record view; ordinary mutation is always refused.
#[derive(Clone)]
pub struct MemoryCatalogRecords {
    state: Arc<RwLock<CatalogState>>,
}

impl StateStore for MemoryCatalogRecords {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        Ok(state.entries.get(key).cloned())
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::ReplicaWriteRequired)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        Ok(StoreSnapshot {
            entries: state
                .entries
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
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        Ok(state
            .entries
            .range(crate::scan_start(prefix, start).to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl BoundedStateStore for MemoryCatalogRecords {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        let mut budget = ReadBudget::new(limits);
        let mut entries = Vec::new();
        for (key, value) in &state.entries {
            budget.consume(key.len(), value.len())?;
            entries.push((key.clone(), value.clone()));
        }
        Ok(StoreSnapshot { entries })
    }
}

/// A separate read-only view of the originating writer's opaque catalog slot.
///
/// ```compile_fail
/// use storage::{CatalogCommittedStore, StateStore};
/// let reader = storage::MemoryCatalogReplicaStore::new().catalog_reader();
/// reader.apply(storage::WriteBatch::default());
/// ```
#[derive(Clone)]
pub struct MemoryCatalogReader {
    state: Arc<RwLock<CatalogState>>,
}

impl SnapshotCatalogReader for MemoryCatalogReader {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        let state = self.state.read().map_err(|_| StorageError::LockPoisoned)?;
        validate_state(&state)?;
        state
            .catalog
            .as_ref()
            .map(|(metadata, artifact)| StoredSnapshotCatalog::copy_from_parts(metadata, artifact))
            .transpose()
    }
}

impl fmt::Debug for MemoryCatalogReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryCatalogReader")
            .finish_non_exhaustive()
    }
}

fn validate_state(state: &CatalogState) -> Result<(), StorageError> {
    if !state.initialized && (!state.entries.is_empty() || state.catalog.is_some()) {
        return Err(StorageError::CorruptMetadata {
            detail: "uninitialized catalog replica contains state".into(),
        });
    }
    if let Some((metadata, artifact)) = &state.catalog {
        crate::catalog::check_bounds(metadata.len(), artifact.len())
            .map_err(|_| StorageError::ReadLimitExceeded)?;
    }
    Ok(())
}

mod complete;

#[cfg(test)]
mod tests;
