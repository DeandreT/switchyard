use fjall::Readable;

use crate::complete_catalog_state::{
    RecordShape, copy_bytes, inconsistent, payload_bytes, reserve_rows,
};
use crate::{
    CatalogReadError, CompleteCatalogReplicaStateReader, StorageError, StoredCatalogReplicaState,
};

use super::{
    CATALOG_ARTIFACT_KEY, CATALOG_METADATA_KEY, FjallCatalogReader, required_value, validate_view,
};

impl CompleteCatalogReplicaStateReader for FjallCatalogReader {
    fn capture_complete_state(&self) -> Result<StoredCatalogReplicaState, CatalogReadError> {
        let snapshot = self.database.snapshot();
        capture_view(self, &snapshot)
    }
}

fn capture_view(
    reader: &FjallCatalogReader,
    snapshot: &fjall::Snapshot,
) -> Result<StoredCatalogReplicaState, CatalogReadError> {
    let view = validate_view(snapshot, &reader.records, &reader.meta)?;
    let shape = record_shape(reader, snapshot)?;
    let total = payload_bytes(shape.bytes, view.lengths)?;

    let mut entries = reserve_rows(shape.rows)?;
    let mut copied = RecordShape::new();
    let mut previous: Option<fjall::UserKey> = None;
    for guard in snapshot.iter(&reader.records) {
        copied.check_next_row()?;
        let key = guard
            .key()
            .map_err(|error| StorageError::backend("read a complete state key", &error))?;
        if previous
            .as_ref()
            .is_some_and(|old| old.as_ref() >= key.as_ref())
        {
            return Err(inconsistent());
        }
        let size = record_size(reader, snapshot, &key)?;
        copied.consume(key.len(), size)?;
        let value = snapshot
            .get(&reader.records, &key)
            .map_err(|error| StorageError::backend("read a complete state value", &error))?
            .ok_or_else(inconsistent)?;
        if value.len() != size || entries.len() == shape.rows {
            return Err(inconsistent());
        }
        entries.push((copy_bytes(&key)?, copy_bytes(&value)?));
        previous = Some(key);
    }
    if copied.rows != shape.rows || copied.bytes != shape.bytes {
        return Err(inconsistent());
    }
    let catalog = if let Some((metadata_bytes, artifact_bytes)) = view.lengths {
        let metadata =
            required_value(snapshot, &reader.meta, CATALOG_METADATA_KEY, metadata_bytes)?;
        let artifact =
            required_value(snapshot, &reader.meta, CATALOG_ARTIFACT_KEY, artifact_bytes)?;
        Some(crate::StoredSnapshotCatalog::copy_from_parts(
            &metadata, &artifact,
        )?)
    } else {
        None
    };
    Ok(StoredCatalogReplicaState::from_parts(
        view.initialized,
        entries,
        catalog,
        total,
    ))
}

fn record_shape(
    reader: &FjallCatalogReader,
    snapshot: &fjall::Snapshot,
) -> Result<RecordShape, CatalogReadError> {
    let mut shape = RecordShape::new();
    let mut previous: Option<fjall::UserKey> = None;
    for guard in snapshot.iter(&reader.records) {
        shape.check_next_row()?;
        let key = guard
            .key()
            .map_err(|error| StorageError::backend("measure a complete state key", &error))?;
        if previous
            .as_ref()
            .is_some_and(|old| old.as_ref() >= key.as_ref())
        {
            return Err(inconsistent());
        }
        shape.consume(key.len(), record_size(reader, snapshot, &key)?)?;
        previous = Some(key);
    }
    Ok(shape)
}

fn record_size(
    reader: &FjallCatalogReader,
    snapshot: &fjall::Snapshot,
    key: &[u8],
) -> Result<usize, CatalogReadError> {
    let size = snapshot
        .size_of(&reader.records, key)
        .map_err(|error| StorageError::backend("measure a complete state value", &error))?
        .ok_or_else(inconsistent)?;
    usize::try_from(size).map_err(|_| CatalogReadError::LimitExceeded)
}

#[cfg(test)]
mod tests;
