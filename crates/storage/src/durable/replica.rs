use crate::{CommittedStore, replica::ReplicaReader};

use super::*;

const REPLICA_PROFILE_KEY: &[u8] = b"replica_profile";
const REPLICA_INITIALIZED_KEY: &[u8] = b"replica_initialized";
const REPLICA_PROFILE: &[u8] = b"committed-state-v1";

/// A unique committed writer for a durable replica directory.
///
/// Only a pristine directory can be initialized. Standalone directories are
/// not adopted, even if their record keyspace is empty. Every privileged batch
/// changes the initialized flag in the same durable transaction as its records.
/// Readers cannot mutate the store, and no writable inner handle is exposed.
///
/// ```compile_fail
/// fn duplicate(writer: storage::FjallReplicaStore) {
///     let another_writer = writer.clone();
/// }
/// ```
pub struct FjallReplicaStore {
    store: FjallStore,
    meta: Keyspace,
}

impl FjallReplicaStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        let directory = directory.as_ref().to_path_buf();
        let database = Database::builder(&directory)
            .open()
            .map_err(|error| StorageError::backend("open the store directory", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the metadata keyspace", &error))?;
        let snapshot = database.snapshot();
        reject_paired_metadata_markers(&snapshot, &meta)?;
        drop(snapshot);
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the record keyspace", &error))?;

        match meta
            .get(FORMAT_VERSION_KEY)
            .map_err(|error| StorageError::backend("read the store format version", &error))?
        {
            Some(recorded) => {
                require_format_version(&recorded, ACTIVE_REPLICA_STORE_FORMAT)?;
                require_profile(&meta)?;
                if !read_initialized(&meta)? && !keyspace_is_empty(&records)? {
                    return Err(corrupt("uninitialized replica contains records"));
                }
            }
            None => {
                if !keyspace_is_empty(&meta)? || !keyspace_is_empty(&records)? {
                    return Err(corrupt("unversioned replica directory is not empty"));
                }
                let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
                batch.insert(
                    &meta,
                    FORMAT_VERSION_KEY,
                    ACTIVE_REPLICA_STORE_FORMAT.to_be_bytes().to_vec(),
                );
                batch.insert(&meta, REPLICA_PROFILE_KEY, REPLICA_PROFILE);
                batch.insert(&meta, REPLICA_INITIALIZED_KEY, vec![0]);
                batch.commit().map_err(|error| {
                    StorageError::backend("stamp the replica store profile", &error)
                })?;
            }
        }

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
}

impl std::fmt::Debug for FjallReplicaStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FjallReplicaStore")
            .field("directory", &self.directory())
            .finish_non_exhaustive()
    }
}

impl CommittedStore for FjallReplicaStore {
    type Reader = ReplicaReader<FjallStore>;

    fn reader(&self) -> Self::Reader {
        ReplicaReader::new(self.store.clone())
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        require_profile(&self.meta)?;
        read_initialized(&self.meta)?;
        let mut durable = self
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => {
                    durable.insert(&self.store.records, key, value);
                }
                Mutation::Delete { key } => {
                    durable.remove(&self.store.records, key);
                }
            }
        }
        durable.insert(&self.meta, REPLICA_INITIALIZED_KEY, vec![1]);
        durable
            .commit()
            .map_err(|error| StorageError::backend("commit a replica batch", &error))
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        require_profile(&self.meta)?;
        read_initialized(&self.meta)
    }
}

pub(super) fn has_replica_metadata(meta: &Keyspace) -> Result<bool, StorageError> {
    for key in [REPLICA_PROFILE_KEY, REPLICA_INITIALIZED_KEY] {
        if meta
            .get(key)
            .map_err(|error| StorageError::backend("read the replica store profile", &error))?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn require_profile(meta: &Keyspace) -> Result<(), StorageError> {
    let profile = meta
        .get(REPLICA_PROFILE_KEY)
        .map_err(|error| StorageError::backend("read the replica store profile", &error))?
        .ok_or_else(|| corrupt("replica profile record is missing"))?;
    if profile.as_ref() != REPLICA_PROFILE {
        return Err(corrupt("replica profile record is unsupported"));
    }
    Ok(())
}

fn read_initialized(meta: &Keyspace) -> Result<bool, StorageError> {
    let initialized = meta
        .get(REPLICA_INITIALIZED_KEY)
        .map_err(|error| StorageError::backend("read the replica initialized flag", &error))?
        .ok_or_else(|| corrupt("replica initialized flag is missing"))?;
    match initialized.as_ref() {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(corrupt("replica initialized flag is malformed")),
    }
}

fn keyspace_is_empty(keyspace: &Keyspace) -> Result<bool, StorageError> {
    Ok(keyspace
        .iter()
        .next()
        .map(read_entry)
        .transpose()?
        .is_none())
}

fn corrupt(detail: &'static str) -> StorageError {
    StorageError::CorruptMetadata {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests;
