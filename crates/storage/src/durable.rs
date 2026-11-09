//! The durable backend: a local LSM-tree, through Fjall.
//!
//! A batch is written to the journal and fsynced before [`StateStore::apply`]
//! returns. Recovery replays the journal, and a batch is one journal record, so
//! a process killed mid-commit comes back holding either all of that command's
//! effects or none of them.
//!
//! Two keyspaces are used. `records` holds exactly the keys the caller writes,
//! so a scan or a snapshot never surfaces anything this module added of its own.
//! `meta` holds the active on-disk format marker, which is checked on every open.

use std::path::{Path, PathBuf};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};

use crate::{
    FjallSnapshotMetadata, Key, Mutation, SnapshotProvenance, SnapshotWithProvenance, StateStore,
    StorageError, StoreSnapshot, Value, WriteBatch,
};

/// Historical layout before domain entities required persisted owner identities.
pub const STORE_FORMAT_V1: u32 = 1;
/// Layout requiring live owner identities for domain entity records.
pub const STORE_FORMAT_V2: u32 = 2;
/// Version 2 record encodings, fenced for the fixed bounded AMQP content
/// profile. The profile is checked by the explicit offline upgrade and by
/// snapshot capture, not yet by ordinary writes: write-path enforcement arrives
/// with #16's typed-content admission, so a format 3 store can hold content
/// written after upgrade that capture then refuses.
pub const STORE_FORMAT_V3: u32 = 3;
/// The ordinary domain layout; consensus uses a separate, isolated opener.
pub const ACTIVE_STORE_FORMAT: u32 = STORE_FORMAT_V3;
/// Fresh-only consensus backend profile. Not a domain value/queue format.
pub const CONSENSUS_STORE_FORMAT: u32 = 4;

const RECORDS_KEYSPACE: &str = "records";
const META_KEYSPACE: &str = "meta";
pub(crate) const FORMAT_VERSION_KEY: &[u8] = b"format_version";

pub struct FjallStore {
    database: Database,
    records: Keyspace,
    meta: Keyspace,
    directory: PathBuf,
}

impl FjallStore {
    /// Opens the store in `directory`, creating it if it does not exist.
    ///
    /// A directory has a single owner: opening one that another live handle
    /// already holds is refused rather than shared.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_profile(directory.as_ref(), ACTIVE_STORE_FORMAT)
    }

    fn open_profile(directory: &Path, expected: u32) -> Result<Self, StorageError> {
        let directory = directory.to_path_buf();
        let database = Database::builder(&directory)
            .open()
            .map_err(|error| StorageError::backend("open the store directory", &error))?;
        if expected == CONSENSUS_STORE_FORMAT && !database.list_keyspace_names().is_empty() {
            require_known_inventory(&database)?;
        }
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
            Some(recorded) => require_format(&recorded, expected)?,
            None => {
                // A missing marker does not prove this is a fresh store.
                let metadata_entry = meta.iter().next().map(read_entry).transpose()?;
                let record_entry = records.iter().next().map(read_entry).transpose()?;
                if metadata_entry.is_some() || record_entry.is_some() {
                    return Err(StorageError::CorruptMetadata {
                        detail: String::from(
                            "unversioned store contains existing records or metadata",
                        ),
                    });
                }

                let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
                batch.insert(&meta, FORMAT_VERSION_KEY, expected.to_be_bytes().to_vec());
                batch.commit().map_err(|error| {
                    StorageError::backend("stamp the store format version", &error)
                })?;
            }
        }

        if expected == CONSENSUS_STORE_FORMAT {
            inspect_native(&database, &meta, &records, |recorded| {
                require_format(recorded, expected)?;
                Ok(false)
            })?;
        }
        Ok(Self {
            database,
            records,
            meta,
            directory,
        })
    }

    /// Only the non-Clone consensus wrapper can call this. Existing databases
    /// are recovered without creating keyspaces or changing logical metadata.
    pub(crate) fn open_consensus(directory: &Path) -> Result<Self, StorageError> {
        let nonempty = match std::fs::read_dir(directory) {
            Ok(mut entries) => entries
                .next()
                .transpose()
                .map_err(|error| StorageError::backend("inspect the consensus directory", &error))?
                .is_some(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(StorageError::backend(
                    "inspect the consensus directory",
                    &error,
                ));
            }
        };
        if !nonempty {
            return Self::open_profile(directory, CONSENSUS_STORE_FORMAT);
        }
        let (database, meta, records) = recover_existing(directory)?;
        inspect_native(&database, &meta, &records, |recorded| {
            require_format(recorded, CONSENSUS_STORE_FORMAT)?;
            Ok(false)
        })?;
        Ok(Self {
            database,
            meta,
            records,
            directory: directory.to_path_buf(),
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Captures logical records with exact selected-backend format metadata.
    ///
    /// The caller must exclude every writer through all store clones and raw
    /// database/keyspace handles, including keyspace creation, deletion and
    /// configuration changes, throughout this operation. The keyspace inventory
    /// is a live read; metadata and records then share one cross-keyspace snapshot.
    /// Neither this borrow nor the directory's open lock enforces that exclusivity.
    /// Exact inventory plus exclusivity makes the current known-handle lookups
    /// noncreating; their IDs must match the retained handles. If exclusivity is
    /// violated, those native create-or-open lookups could create a missing name,
    /// so the no-write and coherent-capture guarantees do not apply.
    ///
    /// This inspects an already-open store and makes no logical writes. Ordinary
    /// [`Self::open`] may already have created keyspaces or stamped empty metadata;
    /// this is not a read-only directory opener or a zero-I/O/byte-identical-directory
    /// guarantee. Capturing the full image copies O(total image bytes) and does not
    /// bound allocations from hostile stored values. Provenance is neither
    /// authentication nor semantic validation of the logical records.
    pub fn snapshot_with_provenance(&self) -> Result<SnapshotWithProvenance, StorageError> {
        inspect_native(&self.database, &self.meta, &self.records, |recorded| {
            require_readable_format(recorded).map(|()| true)
        })
        .map(|image| image.expect("an accepted marker reads every record"))
    }
}

/// Opens an existing store directory without creating a database or keyspace.
///
/// Native recovery still performs its own journal and lock-file I/O. The
/// keyspace inventory is read before any create-or-open lookup, so a missing or
/// extra keyspace refuses rather than being created. No marker is checked here.
pub(crate) fn recover_existing(
    directory: &Path,
) -> Result<(Database, Keyspace, Keyspace), StorageError> {
    // `Database::recover` is doc-hidden in fjall: unlike the builder's `open`,
    // it never creates a database. The workspace pins fjall exactly so an
    // upgrade cannot silently change this noncreating behavior.
    let database = Database::recover(Database::builder(directory).into_config())
        .map_err(|error| StorageError::backend("recover an existing store directory", &error))?;
    require_known_inventory(&database)?;
    // The exact inventory makes both lookups noncreating under exclusion.
    let meta = database
        .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("open the metadata keyspace", &error))?;
    let records = database
        .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("open the record keyspace", &error))?;
    Ok((database, meta, records))
}

fn require_known_inventory(database: &Database) -> Result<(), StorageError> {
    let keyspaces = database.list_keyspace_names();
    if keyspaces.len() != 2
        || !keyspaces
            .iter()
            .any(|name| name.as_bytes() == META_KEYSPACE.as_bytes())
        || !keyspaces
            .iter()
            .any(|name| name.as_bytes() == RECORDS_KEYSPACE.as_bytes())
    {
        return Err(StorageError::CorruptMetadata {
            detail: String::from(
                "snapshot provenance requires exactly the meta and records keyspaces",
            ),
        });
    }
    Ok(())
}

/// Exact inventory, original handle identity and one marker row, then the
/// marker and all records through one cross-keyspace snapshot. `accept` sees
/// the checked four-byte marker before any record is copied; returning false
/// stops without materializing records.
pub(crate) fn inspect_native(
    database: &Database,
    meta: &Keyspace,
    records: &Keyspace,
    accept: impl FnOnce(&[u8]) -> Result<bool, StorageError>,
) -> Result<Option<SnapshotWithProvenance>, StorageError> {
    require_known_inventory(database)?;
    // Exclusive topology and the exact inventory keep both lookups noncreating.
    let current_meta = database
        .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("inspect the metadata keyspace", &error))?;
    let current_records = database
        .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("inspect the record keyspace", &error))?;
    if current_meta.id() != meta.id() || current_records.id() != records.id() {
        return Err(StorageError::CorruptMetadata {
            detail: String::from(
                "snapshot provenance requires the original meta and records keyspace handles",
            ),
        });
    }

    let snapshot = database.snapshot();
    let mut metadata = snapshot.iter(meta);
    let (key, recorded) = metadata
        .next()
        .map(read_entry)
        .transpose()?
        .ok_or_else(|| StorageError::CorruptMetadata {
            detail: String::from("snapshot provenance requires the format version marker"),
        })?;
    if key.as_slice() != FORMAT_VERSION_KEY
        || metadata.next().map(read_entry).transpose()?.is_some()
    {
        return Err(StorageError::CorruptMetadata {
            detail: String::from("snapshot provenance requires only the format version marker"),
        });
    }
    let format_marker =
        <[u8; 4]>::try_from(recorded.as_slice()).map_err(|_| StorageError::CorruptMetadata {
            detail: format!(
                "format version record is {} bytes, expected 4",
                recorded.len()
            ),
        })?;
    if !accept(&recorded)? {
        return Ok(None);
    }

    let mut entries = Vec::new();
    for guard in snapshot.iter(records) {
        entries.push(read_entry(guard)?);
    }
    Ok(Some(SnapshotWithProvenance {
        snapshot: StoreSnapshot { entries },
        provenance: SnapshotProvenance::Fjall(FjallSnapshotMetadata { format_marker }),
    }))
}

/// Cloning shares one open database, so every clone reads what any other wrote.
/// It does not open the directory again, which a second owner is not allowed to
/// do anyway.
impl Clone for FjallStore {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            records: self.records.clone(),
            meta: self.meta.clone(),
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
    require_format(recorded, ACTIVE_STORE_FORMAT)
}

fn require_format(recorded: &[u8], expected: u32) -> Result<(), StorageError> {
    let bytes = <[u8; 4]>::try_from(recorded).map_err(|_| StorageError::CorruptMetadata {
        detail: format!(
            "format version record is {} bytes, expected 4",
            recorded.len()
        ),
    })?;
    let found = u32::from_be_bytes(bytes);
    if found != expected {
        return Err(StorageError::UnsupportedStoreFormat { found, expected });
    }
    Ok(())
}

pub(crate) fn read_entry(guard: fjall::Guard) -> Result<(Key, Value), StorageError> {
    let (key, value) = guard
        .into_inner()
        .map_err(|error| StorageError::backend("read a scanned record", &error))?;
    Ok((key.to_vec(), value.to_vec()))
}

#[cfg(test)]
mod provenance_tests;

#[cfg(test)]
mod image_install_tests;

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
    fn stamps_version_three_when_it_creates_a_store() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let store = FjallStore::open(directory.path())?;
        assert_eq!(store.directory(), directory.path());

        // Reopening accepts the version it wrote, and the stamp is not a record.
        drop(store);
        let reopened = FjallStore::open(directory.path())?;
        assert_eq!(reopened.snapshot()?.entries(), &[]);
        assert_eq!(reopened.get(FORMAT_VERSION_KEY)?, None);
        drop(reopened);
        assert_eq!(
            read_known_rows(directory.path())?,
            KnownRows {
                meta: vec![(
                    FORMAT_VERSION_KEY.to_vec(),
                    STORE_FORMAT_V3.to_be_bytes().to_vec(),
                )],
                records: Vec::new(),
            }
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_with_any_other_format() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &(ACTIVE_STORE_FORMAT + 1).to_be_bytes())?;

        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: ACTIVE_STORE_FORMAT + 1,
                expected: ACTIVE_STORE_FORMAT,
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

    #[derive(Debug, Default, Eq, PartialEq)]
    struct KnownRows {
        meta: Vec<(Key, Value)>,
        records: Vec<(Key, Value)>,
    }

    fn seed_known_rows(directory: &Path, rows: &KnownRows) -> Result<(), StorageError> {
        let database = Database::builder(directory)
            .open()
            .map_err(|error| StorageError::backend("open raw known keyspaces", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open raw metadata", &error))?;
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open raw records", &error))?;
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        for (key, value) in &rows.meta {
            batch.insert(&meta, key.as_slice(), value.as_slice());
        }
        for (key, value) in &rows.records {
            batch.insert(&records, key.as_slice(), value.as_slice());
        }
        batch
            .commit()
            .map_err(|error| StorageError::backend("seed raw known rows", &error))
    }

    fn read_known_rows(directory: &Path) -> Result<KnownRows, StorageError> {
        let database = Database::builder(directory)
            .open()
            .map_err(|error| StorageError::backend("open raw known keyspaces", &error))?;
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open raw metadata", &error))?;
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open raw records", &error))?;
        let snapshot = database.snapshot();
        Ok(KnownRows {
            meta: snapshot
                .iter(&meta)
                .map(read_entry)
                .collect::<Result<_, _>>()?,
            records: snapshot
                .iter(&records)
                .map(read_entry)
                .collect::<Result<_, _>>()?,
        })
    }

    fn assert_unversioned_refusal(
        directory: &Path,
        expected: &KnownRows,
    ) -> Result<(), StorageError> {
        assert_eq!(&read_known_rows(directory)?, expected);
        assert!(
            expected
                .meta
                .iter()
                .all(|(key, _)| key.as_slice() != FORMAT_VERSION_KEY)
        );
        for _ in 0..2 {
            assert_eq!(
                FjallStore::open(directory).err(),
                Some(StorageError::CorruptMetadata {
                    detail: String::from("unversioned store contains existing records or metadata"),
                }),
            );
            assert_eq!(
                &read_known_rows(directory)?,
                expected,
                "known raw rows and missing marker stay exact"
            );
        }
        Ok(())
    }

    fn unversioned_meta_rows() -> Vec<(Key, Value)> {
        vec![
            (b"a-unknown".to_vec(), Vec::new()),
            (b"z-unknown".to_vec(), b"retained metadata".to_vec()),
        ]
    }

    fn unversioned_record_rows() -> Vec<(Key, Value)> {
        vec![
            (b"a-record".to_vec(), Vec::new()),
            (b"z-record".to_vec(), b"retained record".to_vec()),
        ]
    }

    #[test]
    fn initializes_existing_empty_unversioned_known_keyspaces() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        seed_known_rows(directory.path(), &KnownRows::default())?;
        assert_eq!(read_known_rows(directory.path())?, KnownRows::default());
        let store = FjallStore::open(directory.path())?;
        assert!(store.snapshot()?.entries().is_empty());
        assert_eq!(store.get(FORMAT_VERSION_KEY)?, None);
        drop(store);

        let expected = KnownRows {
            meta: vec![(
                FORMAT_VERSION_KEY.to_vec(),
                ACTIVE_STORE_FORMAT.to_be_bytes().to_vec(),
            )],
            records: Vec::new(),
        };
        assert_eq!(read_known_rows(directory.path())?, expected);
        let reopened = FjallStore::open(directory.path())?;
        assert!(reopened.snapshot()?.entries().is_empty());
        drop(reopened);
        assert_eq!(read_known_rows(directory.path())?, expected);
        Ok(())
    }

    #[test]
    fn refuses_unversioned_metadata_without_changing_known_rows() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let rows = KnownRows {
            meta: unversioned_meta_rows(),
            records: Vec::new(),
        };
        seed_known_rows(directory.path(), &rows)?;
        assert_unversioned_refusal(directory.path(), &rows)
    }

    #[test]
    fn refuses_unversioned_records_without_changing_known_rows() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let rows = KnownRows {
            meta: Vec::new(),
            records: unversioned_record_rows(),
        };
        seed_known_rows(directory.path(), &rows)?;
        assert_unversioned_refusal(directory.path(), &rows)
    }

    #[test]
    fn refuses_unversioned_metadata_and_records_without_changing_known_rows()
    -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let rows = KnownRows {
            meta: unversioned_meta_rows(),
            records: unversioned_record_rows(),
        };
        seed_known_rows(directory.path(), &rows)?;
        assert_unversioned_refusal(directory.path(), &rows)
    }

    #[test]
    fn marked_version_three_accepts_populated_known_keyspaces() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let mut metadata = unversioned_meta_rows();
        metadata.insert(
            1,
            (
                FORMAT_VERSION_KEY.to_vec(),
                STORE_FORMAT_V3.to_be_bytes().to_vec(),
            ),
        );
        let rows = KnownRows {
            meta: metadata,
            records: unversioned_record_rows(),
        };
        seed_known_rows(directory.path(), &rows)?;
        let store = FjallStore::open(directory.path())?;
        assert_eq!(store.snapshot()?.entries(), rows.records.as_slice());
        for (key, value) in &rows.records {
            assert_eq!(store.get(key)?, Some(value.clone()));
        }
        drop(store);
        assert_eq!(read_known_rows(directory.path())?, rows);
        let reopened = FjallStore::open(directory.path())?;
        assert_eq!(reopened.snapshot()?.entries(), rows.records.as_slice());
        drop(reopened);
        assert_eq!(read_known_rows(directory.path())?, rows);
        Ok(())
    }

    /// Ordinary open never upgrades version 2; only the explicit offline
    /// upgrade moves its marker.
    #[test]
    fn marked_version_two_refuses_empty_and_populated_known_keyspaces_without_changing_rows()
    -> Result<(), StorageError> {
        for records in [Vec::new(), unversioned_record_rows()] {
            let directory = TempDir::new().expect("a temporary directory");
            let rows = KnownRows {
                meta: vec![(
                    FORMAT_VERSION_KEY.to_vec(),
                    STORE_FORMAT_V2.to_be_bytes().to_vec(),
                )],
                records,
            };
            seed_known_rows(directory.path(), &rows)?;
            for _ in 0..2 {
                assert_eq!(
                    FjallStore::open(directory.path()).err(),
                    Some(StorageError::UnsupportedStoreFormat {
                        found: STORE_FORMAT_V2,
                        expected: ACTIVE_STORE_FORMAT,
                    })
                );
                assert_eq!(read_known_rows(directory.path())?, rows);
            }
        }
        Ok(())
    }

    fn assert_marked_version_one_refusal(
        directory: &Path,
        expected: &KnownRows,
    ) -> Result<(), StorageError> {
        assert_eq!(&read_known_rows(directory)?, expected);
        assert_eq!(
            expected
                .meta
                .iter()
                .find(|(key, _)| key.as_slice() == FORMAT_VERSION_KEY)
                .map(|(_, value)| value.clone()),
            Some(STORE_FORMAT_V1.to_be_bytes().to_vec())
        );
        for _ in 0..2 {
            assert_eq!(
                FjallStore::open(directory).err(),
                Some(StorageError::UnsupportedStoreFormat {
                    found: STORE_FORMAT_V1,
                    expected: ACTIVE_STORE_FORMAT,
                })
            );
            assert_eq!(
                &read_known_rows(directory)?,
                expected,
                "known raw rows and historical marker stay exact"
            );
        }
        Ok(())
    }

    #[test]
    fn marked_version_one_refuses_empty_known_keyspaces_without_changing_rows()
    -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let rows = KnownRows {
            meta: vec![(
                FORMAT_VERSION_KEY.to_vec(),
                STORE_FORMAT_V1.to_be_bytes().to_vec(),
            )],
            records: Vec::new(),
        };
        seed_known_rows(directory.path(), &rows)?;
        assert_marked_version_one_refusal(directory.path(), &rows)
    }

    #[test]
    fn marked_version_one_refuses_populated_known_keyspaces_without_changing_rows()
    -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        let mut metadata = unversioned_meta_rows();
        metadata.insert(
            1,
            (
                FORMAT_VERSION_KEY.to_vec(),
                STORE_FORMAT_V1.to_be_bytes().to_vec(),
            ),
        );
        let rows = KnownRows {
            meta: metadata,
            records: unversioned_record_rows(),
        };
        seed_known_rows(directory.path(), &rows)?;
        assert_marked_version_one_refusal(directory.path(), &rows)
    }
}
