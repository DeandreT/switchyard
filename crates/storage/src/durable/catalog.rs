use std::fmt;

use crate::{
    CatalogCommittedStore, CatalogReadError, CommittedStore, MAX_CATALOG_ARTIFACT_BYTES,
    MAX_CATALOG_METADATA_BYTES, SnapshotCatalogReader, SnapshotCatalogRecord,
    StoredSnapshotCatalog, replica::ReplicaReader,
};

use super::*;

const PROFILE_KEY: &[u8] = b"replica_profile";
const INITIALIZED_KEY: &[u8] = b"replica_initialized";
const PROFILE: &[u8] = b"committed-state-catalog-v1";
const CATALOG_METADATA_KEY: &[u8] = b"snapshot_meta";
const CATALOG_ARTIFACT_KEY: &[u8] = b"snapshot_image";
const META_KEYS: [&[u8]; 5] = [
    FORMAT_VERSION_KEY,
    PROFILE_KEY,
    INITIALIZED_KEY,
    CATALOG_METADATA_KEY,
    CATALOG_ARTIFACT_KEY,
];

/// A unique committed writer with an explicitly opted-in opaque catalog.
///
/// The profile is disjoint from standalone and ordinary replica layouts.
/// Existing directories are never migrated or adopted, even when their records
/// are empty. Catalog values live in metadata, outside every business scan and
/// snapshot. Old/default constructors refuse this profile.
///
/// ```compile_fail
/// fn duplicate(writer: storage::FjallCatalogReplicaStore) {
///     let another = writer.clone();
/// }
/// ```
pub struct FjallCatalogReplicaStore {
    store: FjallStore,
    meta: Keyspace,
}

impl FjallCatalogReplicaStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        let directory = directory.as_ref().to_path_buf();
        let database = Database::builder(&directory)
            .open()
            .map_err(|error| StorageError::backend("open the catalog replica directory", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open catalog replica metadata", &error))?;
        let snapshot = database.snapshot();
        reject_paired_metadata_markers(&snapshot, &meta)?;
        drop(snapshot);
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open catalog replica records", &error))?;

        let snapshot = database.snapshot();
        match value_size(&snapshot, &meta, FORMAT_VERSION_KEY)? {
            Some(_) => {
                validate_view(&snapshot, &records, &meta)?;
            }
            None => {
                if !is_empty(&snapshot, &meta)? || !is_empty(&snapshot, &records)? {
                    return Err(corrupt(
                        "unversioned catalog replica directory is not empty",
                    ));
                }
                let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
                batch.insert(
                    &meta,
                    FORMAT_VERSION_KEY,
                    ACTIVE_CATALOG_REPLICA_STORE_FORMAT.to_be_bytes().to_vec(),
                );
                batch.insert(&meta, PROFILE_KEY, PROFILE);
                batch.insert(&meta, INITIALIZED_KEY, vec![0]);
                batch.commit().map_err(|error| {
                    StorageError::backend("stamp the catalog replica profile", &error)
                })?;
            }
        }
        drop(snapshot);
        Ok(Self {
            store: FjallStore {
                database,
                records,
                directory,
            },
            meta,
        })
    }

    pub fn directory(&self) -> &Path {
        self.store.directory()
    }

    fn commit_inner(
        &mut self,
        batch: WriteBatch,
        catalog: Option<SnapshotCatalogRecord<'_>>,
    ) -> Result<(), StorageError> {
        // The writer is unique; this is a profile check, not a read/commit CAS.
        let snapshot = self.store.database.snapshot();
        validate_view(&snapshot, &self.store.records, &self.meta)?;
        drop(snapshot);
        let mut durable = self
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => durable.insert(&self.store.records, key, value),
                Mutation::Delete { key } => durable.remove(&self.store.records, key),
            }
        }
        if let Some(catalog) = catalog {
            durable.insert(&self.meta, CATALOG_METADATA_KEY, catalog.metadata());
            durable.insert(&self.meta, CATALOG_ARTIFACT_KEY, catalog.artifact());
        }
        durable.insert(&self.meta, INITIALIZED_KEY, vec![1]);
        durable
            .commit()
            .map_err(|error| StorageError::backend("commit a catalog replica batch", &error))
    }
}

impl fmt::Debug for FjallCatalogReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FjallCatalogReplicaStore")
            .field("directory", &self.directory())
            .finish_non_exhaustive()
    }
}

impl CommittedStore for FjallCatalogReplicaStore {
    type Reader = ReplicaReader<FjallStore>;

    fn reader(&self) -> Self::Reader {
        ReplicaReader::new(self.store.clone())
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commit_inner(batch, None)
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        let snapshot = self.store.database.snapshot();
        Ok(validate_view(&snapshot, &self.store.records, &self.meta)?.initialized)
    }
}

impl CatalogCommittedStore for FjallCatalogReplicaStore {
    type CatalogReader = FjallCatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader {
        FjallCatalogReader {
            database: self.store.database.clone(),
            records: self.store.records.clone(),
            meta: self.meta.clone(),
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

/// A separate read-only capability retaining one database ownership handle.
///
/// The profile, initialization, and both components come from one pinned view.
/// Component lengths are checked before caller-owned copies, then actual value
/// lengths are checked against that same view. Backend iterator/get/size_of
/// materialization and allocations are not bounded by the caller-copy limits.
///
/// ```compile_fail
/// use storage::{CatalogCommittedStore, StateStore};
/// fn no_catalog_mutation(writer: &storage::FjallCatalogReplicaStore) {
///     writer.catalog_reader().apply(storage::WriteBatch::default());
/// }
/// ```
#[derive(Clone)]
pub struct FjallCatalogReader {
    database: Database,
    records: Keyspace,
    meta: Keyspace,
}

impl SnapshotCatalogReader for FjallCatalogReader {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
        let snapshot = self.database.snapshot();
        read_catalog_view(&snapshot, &self.records, &self.meta)
    }
}

impl fmt::Debug for FjallCatalogReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FjallCatalogReader")
            .finish_non_exhaustive()
    }
}

struct CatalogView {
    initialized: bool,
    lengths: Option<(usize, usize)>,
}

fn validate_view(
    snapshot: &fjall::Snapshot,
    records: &Keyspace,
    meta: &Keyspace,
) -> Result<CatalogView, StorageError> {
    for (index, guard) in snapshot.iter(meta).enumerate() {
        if index == META_KEYS.len() {
            return Err(corrupt("catalog replica has too many metadata keys"));
        }
        let key = guard
            .key()
            .map_err(|error| StorageError::backend("read a catalog metadata key", &error))?;
        if !META_KEYS.contains(&key.as_ref()) {
            return Err(corrupt("catalog replica has an unknown metadata key"));
        }
    }
    let format = required_value(snapshot, meta, FORMAT_VERSION_KEY, 4)?;
    require_format_version(&format, ACTIVE_CATALOG_REPLICA_STORE_FORMAT)?;
    if required_value(snapshot, meta, PROFILE_KEY, PROFILE.len())?.as_ref() != PROFILE {
        return Err(corrupt("catalog replica profile is unsupported"));
    }
    let initialized = match required_value(snapshot, meta, INITIALIZED_KEY, 1)?.as_ref() {
        [0] => false,
        [1] => true,
        _ => return Err(corrupt("catalog replica initialized flag is malformed")),
    };
    let lengths = match (
        value_size(snapshot, meta, CATALOG_METADATA_KEY)?,
        value_size(snapshot, meta, CATALOG_ARTIFACT_KEY)?,
    ) {
        (None, None) => None,
        (Some(metadata), Some(artifact)) => Some((metadata, artifact)),
        _ => {
            return Err(corrupt(
                "catalog replica contains only one catalog component",
            ));
        }
    };
    if !initialized && (lengths.is_some() || !is_empty(snapshot, records)?) {
        return Err(corrupt("uninitialized catalog replica contains state"));
    }
    if let Some((metadata, artifact)) = lengths
        && (metadata > MAX_CATALOG_METADATA_BYTES || artifact > MAX_CATALOG_ARTIFACT_BYTES)
    {
        return Err(StorageError::ReadLimitExceeded);
    }
    Ok(CatalogView {
        initialized,
        lengths,
    })
}

fn read_catalog_view(
    snapshot: &fjall::Snapshot,
    records: &Keyspace,
    meta: &Keyspace,
) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError> {
    let view = validate_view(snapshot, records, meta)?;
    let Some((metadata_bytes, artifact_bytes)) = view.lengths else {
        return Ok(None);
    };
    let metadata = required_value(snapshot, meta, CATALOG_METADATA_KEY, metadata_bytes)?;
    let artifact = required_value(snapshot, meta, CATALOG_ARTIFACT_KEY, artifact_bytes)?;
    StoredSnapshotCatalog::copy_from_parts(&metadata, &artifact).map(Some)
}

fn required_value(
    snapshot: &fjall::Snapshot,
    meta: &Keyspace,
    key: &[u8],
    expected_bytes: usize,
) -> Result<fjall::UserValue, StorageError> {
    if value_size(snapshot, meta, key)? != Some(expected_bytes) {
        return Err(corrupt(
            "catalog metadata record is missing or has the wrong length",
        ));
    }
    let value = snapshot
        .get(meta, key)
        .map_err(|error| StorageError::backend("read a catalog metadata value", &error))?
        .ok_or_else(|| corrupt("catalog metadata value is missing from its stable view"))?;
    if value.len() != expected_bytes {
        return Err(corrupt(
            "catalog metadata length differs within its stable view",
        ));
    }
    Ok(value)
}

fn value_size(
    snapshot: &fjall::Snapshot,
    meta: &Keyspace,
    key: &[u8],
) -> Result<Option<usize>, StorageError> {
    snapshot
        .size_of(meta, key)
        .map_err(|error| StorageError::backend("read a catalog metadata size", &error))?
        .map(|size| usize::try_from(size).map_err(|_| StorageError::ReadLimitExceeded))
        .transpose()
}

fn is_empty(snapshot: &fjall::Snapshot, keyspace: &Keyspace) -> Result<bool, StorageError> {
    snapshot
        .iter(keyspace)
        .next()
        .map(|guard| {
            guard
                .key()
                .map(|_| ())
                .map_err(|error| StorageError::backend("inspect catalog replica emptiness", &error))
        })
        .transpose()
        .map(|entry| entry.is_none())
}

fn corrupt(detail: &'static str) -> StorageError {
    StorageError::CorruptMetadata {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests;
