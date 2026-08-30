//! The durable backend: a local LSM-tree, through Fjall.
//!
//! A batch is written to the journal and fsynced before [`StateStore::apply`]
//! returns. Recovery replays the journal, and a batch is one journal record, so
//! a process killed mid-commit comes back holding either all of that command's
//! effects or none of them.
//!
//! Two keyspaces are used. `records` holds exactly the keys the caller writes,
//! so a scan or a snapshot never surfaces anything this module added of its own.
//! `meta` holds the V1 on-disk format marker, which is checked on every open.

use std::path::{Path, PathBuf};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};

use crate::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

/// The durable layout: caller keys verbatim in `records`, and a big-endian V1
/// format marker in `meta`.
pub const STORE_FORMAT_V1: u32 = 1;

const RECORDS_KEYSPACE: &str = "records";
const META_KEYSPACE: &str = "meta";
const FORMAT_VERSION_KEY: &[u8] = b"format_version";

pub struct FjallStore {
    database: Database,
    records: Keyspace,
    directory: PathBuf,
}

impl FjallStore {
    /// Opens the store in `directory`, creating it if it does not exist.
    ///
    /// A directory has a single owner: opening one that another live handle
    /// already holds is refused rather than shared.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        let directory = directory.as_ref().to_path_buf();
        let database = Database::builder(&directory)
            .open()
            .map_err(|error| StorageError::backend("open the store directory", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the metadata keyspace", &error))?;
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the record keyspace", &error))?;

        match meta
            .get(FORMAT_VERSION_KEY)
            .map_err(|error| StorageError::backend("read the store format version", &error))?
        {
            Some(recorded) => require_readable_format(&recorded)?,
            // No version record means nothing has ever been written here, so
            // stamp the directory durably before it can hold a single record.
            None => {
                let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
                batch.insert(
                    &meta,
                    FORMAT_VERSION_KEY,
                    STORE_FORMAT_V1.to_be_bytes().to_vec(),
                );
                batch.commit().map_err(|error| {
                    StorageError::backend("stamp the store format version", &error)
                })?;
            }
        }

        Ok(Self {
            database,
            records,
            directory,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

/// Cloning shares one open database, so every clone reads what any other wrote.
/// It does not open the directory again, which a second owner is not allowed to
/// do anyway.
impl Clone for FjallStore {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            records: self.records.clone(),
            directory: self.directory.clone(),
        }
    }
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FjallStore")
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

impl StateStore for FjallStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.records
            .get(key)
            .map(|found| found.map(|value| value.to_vec()))
            .map_err(|error| StorageError::backend("read a record", &error))
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        // SyncAll rather than the default buffered persist: the caller treats a
        // returned Ok as "this survives the machine losing power".
        let mut durable = self.database.batch().durability(Some(PersistMode::SyncAll));
        for mutation in batch.into_mutations() {
            match mutation {
                Mutation::Put { key, value } => durable.insert(&self.records, key, value),
                Mutation::Delete { key } => durable.remove(&self.records, key),
            }
        }
        durable
            .commit()
            .map_err(|error| StorageError::backend("commit a batch", &error))
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        // A database snapshot, so a batch committed while this is being read
        // cannot show up half applied.
        let snapshot = self.database.snapshot();
        let mut entries = Vec::new();
        for guard in snapshot.iter(&self.records) {
            entries.push(read_entry(guard)?);
        }
        Ok(StoreSnapshot { entries })
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let mut entries = Vec::new();
        let start = crate::scan_start(prefix, start).to_vec();
        for guard in self.records.range(start..).take(limit) {
            let (key, value) = read_entry(guard)?;
            // The range is open-ended, so the walk ends at the first key that
            // has left the prefix.
            if !key.starts_with(prefix) {
                break;
            }
            entries.push((key, value));
        }
        Ok(entries)
    }
}

/// Rejects a store this build cannot read, rather than misreading it.
fn require_readable_format(recorded: &[u8]) -> Result<(), StorageError> {
    let bytes = <[u8; 4]>::try_from(recorded).map_err(|_| StorageError::CorruptMetadata {
        detail: format!(
            "format version record is {} bytes, expected 4",
            recorded.len()
        ),
    })?;
    let found = u32::from_be_bytes(bytes);
    if found != STORE_FORMAT_V1 {
        return Err(StorageError::UnsupportedStoreFormat {
            found,
            expected: STORE_FORMAT_V1,
        });
    }
    Ok(())
}

fn read_entry(guard: fjall::Guard) -> Result<(Key, Value), StorageError> {
    let (key, value) = guard
        .into_inner()
        .map_err(|error| StorageError::backend("read a scanned record", &error))?;
    Ok((key.to_vec(), value.to_vec()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// Writes `version` into a store directory's metadata, standing in for a
    /// build whose active format differs from this one.
    fn stamp_format(directory: &Path, version: &[u8]) -> Result<(), StorageError> {
        let database = Database::builder(directory)
            .open()
            .map_err(|error| StorageError::backend("open the store directory", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the metadata keyspace", &error))?;
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&meta, FORMAT_VERSION_KEY, version.to_vec());
        batch
            .commit()
            .map_err(|error| StorageError::backend("stamp a format version", &error))
    }

    #[test]
    fn stamps_version_one_when_it_creates_a_store() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let store = FjallStore::open(directory.path())?;
        assert_eq!(store.directory(), directory.path());

        // Reopening accepts the version it wrote, and the stamp is not a record.
        drop(store);
        let reopened = FjallStore::open(directory.path())?;
        assert_eq!(reopened.snapshot()?.entries(), &[]);
        assert_eq!(reopened.get(FORMAT_VERSION_KEY)?, None);
        Ok(())
    }

    #[test]
    fn refuses_a_store_with_any_other_format() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &(STORE_FORMAT_V1 + 1).to_be_bytes())?;

        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V1 + 1,
                expected: STORE_FORMAT_V1,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_whose_format_record_is_unreadable() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), b"1")?;

        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::CorruptMetadata {
                detail: String::from("format version record is 1 bytes, expected 4"),
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_second_owner_of_a_live_store() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let held = FjallStore::open(directory.path())?;

        let second = FjallStore::open(directory.path());
        assert!(
            matches!(second, Err(StorageError::Backend { .. })),
            "a live store directory cannot be opened twice, got {second:?}"
        );

        // The refusal left the first owner working.
        held.apply(WriteBatch::default().put(b"key".to_vec(), b"value".to_vec()))?;
        assert_eq!(held.get(b"key")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn a_committed_batch_is_readable_after_reopening() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let store = FjallStore::open(directory.path())?;
        store.apply(
            WriteBatch::default()
                .put(b"message:1".to_vec(), b"first".to_vec())
                .put(b"ready:1".to_vec(), Vec::new()),
        )?;
        drop(store);

        let reopened = FjallStore::open(directory.path())?;
        assert_eq!(reopened.get(b"message:1")?, Some(b"first".to_vec()));
        // Index entries carry an empty value, which must survive as a value
        // rather than come back as a missing key.
        assert_eq!(reopened.get(b"ready:1")?, Some(Vec::new()));
        assert_eq!(reopened.scan_prefix(b"ready:", 16)?.len(), 1);
        Ok(())
    }
}
