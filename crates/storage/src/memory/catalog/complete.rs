use crate::complete_catalog_state::{
    RecordShape, copy_bytes, inconsistent, payload_bytes, reserve_rows,
};
use crate::{CatalogReadError, CompleteCatalogReplicaStateReader, StoredCatalogReplicaState};

use super::{CatalogState, MemoryCatalogReader, validate_state};

impl CompleteCatalogReplicaStateReader for MemoryCatalogReader {
    fn capture_complete_state(&self) -> Result<StoredCatalogReplicaState, CatalogReadError> {
        let state = self
            .state
            .read()
            .map_err(|_| crate::StorageError::LockPoisoned)?;
        capture_state(&state)
    }
}

fn capture_state(state: &CatalogState) -> Result<StoredCatalogReplicaState, CatalogReadError> {
    validate_state(state)?;
    let mut shape = RecordShape::new();
    let mut previous: Option<&[u8]> = None;
    for (key, value) in &state.entries {
        if previous.is_some_and(|old| old >= key.as_slice()) {
            return Err(inconsistent());
        }
        shape.consume(key.len(), value.len())?;
        previous = Some(key);
    }
    let lengths = state
        .catalog
        .as_ref()
        .map(|(meta, image)| (meta.len(), image.len()));
    let total = payload_bytes(shape.bytes, lengths)?;

    // No caller-result reservation or copy precedes the complete preflight.
    let mut entries = reserve_rows(shape.rows)?;
    let mut copied = RecordShape::new();
    let mut previous: Option<&[u8]> = None;
    for (key, value) in &state.entries {
        if previous.is_some_and(|old| old >= key.as_slice()) {
            return Err(inconsistent());
        }
        copied.consume(key.len(), value.len())?;
        entries.push((copy_bytes(key)?, copy_bytes(value)?));
        previous = Some(key);
    }
    if copied.rows != shape.rows || copied.bytes != shape.bytes {
        return Err(inconsistent());
    }
    let catalog = state
        .catalog
        .as_ref()
        .map(|(meta, image)| crate::StoredSnapshotCatalog::copy_from_parts(meta, image))
        .transpose()?;
    Ok(StoredCatalogReplicaState::from_parts(
        state.initialized,
        entries,
        catalog,
        total,
    ))
}

#[cfg(test)]
mod tests;
