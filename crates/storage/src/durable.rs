//! The durable backend: a local LSM-tree, through Fjall.
//!
//! A batch is written to the journal and fsynced before [`StateStore::apply`]
//! returns. Recovery replays the journal, and a batch is one journal record, so
//! a process killed mid-commit comes back holding either all of that command's
//! effects or none of them.
//!
//! Two keyspaces are used. `records` holds exactly the keys the caller writes,
//! so a scan or a snapshot never surfaces anything this module added of its own.
//! `meta` holds the on-disk format version, which is checked on every open.

use std::path::{Path, PathBuf};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};

use crate::{
    BoundedStateStore, Key, Mutation, ReadBudget, ReadLimits, StateStore, StorageError,
    StoreSnapshot, Value, WriteBatch,
};

mod catalog;
pub use catalog::{FjallCatalogReader, FjallCatalogReplicaStore};

mod replica;
pub use replica::FjallReplicaStore;

mod protected_state;
pub use protected_state::{
    ACTIVE_PROTECTED_STATE_STORE_FORMAT, FjallProtectedStateOpenError, FjallProtectedStateReader,
    FjallProtectedStateStore,
};

/// Version 1 of the durable layout: caller keys verbatim in `records`, and a
/// big-endian format version in `meta`.
pub const STORE_FORMAT_V1: u32 = 1;

/// Version 2: the broker's keyspace moved dead-lettered messages out of their
/// own tag and into shadow dead-letter queues. Same physical layout as
/// version 1; the caller's keys mean different things.
pub const STORE_FORMAT_V2: u32 = 2;

/// Version 3: scheduled messages have a deadline index and become active under
/// a new sequence number. Earlier builds cannot activate or cancel them.
pub const STORE_FORMAT_V3: u32 = 3;

/// Version 4: duplicate-detection history and its expiry index are part of the
/// enqueue contract. Earlier builds cannot enforce or prune that history.
pub const STORE_FORMAT_V4: u32 = 4;

/// Version 5: typed message sections are retained in message records. Earlier
/// builds cannot read those records without losing producer content.
pub const STORE_FORMAT_V5: u32 = 5;

/// Version 6: only ready messages have TTL expiry entries. Locked messages
/// are protected until release, and deferred messages expire on retrieval.
pub const STORE_FORMAT_V6: u32 = 6;

/// Version 7: queue configurations select drop or dead-letter on expiration.
/// Earlier builds cannot read the appended policy or enforce it.
pub const STORE_FORMAT_V7: u32 = 7;

/// Version 8: topic metadata and bounded subscription membership reserve
/// receive-only queue paths. Earlier builds could overwrite their topology.
pub const STORE_FORMAT_V8: u32 = 8;

/// Version 9: topic copies retain session identifiers independently of their
/// ready-index policy, and missing-session copies enter subscription shadows.
/// Earlier builds would select the wrong ready index when releasing a copy.
pub const STORE_FORMAT_V9: u32 = 9;

/// Version 10: scheduled topic publications are retained on their parent and
/// fan out only when activated. Earlier builds cannot activate those records.
pub const STORE_FORMAT_V10: u32 = 10;

/// Version 11: subscription rules explicitly select publication targets,
/// including subscriptions whose last rule was removed. Earlier builds would
/// ignore those rules and deliver excluded publications.
pub const STORE_FORMAT_V11: u32 = 11;

/// Version 12: SQL rule selection and subscription-local filter-error policy.
/// Earlier builds cannot interpret those filters or their failure destinations.
pub const STORE_FORMAT_V12: u32 = 12;

/// Version 13: retained entity incarnations fence admitted live endpoints.
/// Earlier builds ignore these records and can address a replacement by name.
pub const STORE_FORMAT_V13: u32 = 13;

/// Version 14: SQL actions produce independently transformed subscription
/// copies. Earlier builds cannot interpret action-bearing rules or that fanout.
pub const STORE_FORMAT_V14: u32 = 14;

/// Version 15: version-2 SQL actions interpret bounded literal SET as well as
/// REMOVE. All derived profiles advance with this record-meaning boundary.
pub const STORE_FORMAT_V15: u32 = 15;

/// Version 16: session-required message locks carry scoped ownership sidecars
/// and block takeover until their actual lock exits commit.
pub const STORE_FORMAT_V16: u32 = 16;

/// The layout version this build reads and writes.
///
/// Bump it when the bytes in `records` change meaning — a different key
/// encoding, or a keyspace split. An open refuses any other version in both
/// directions, because reading a newer store as if it were this one would
/// silently corrupt queue state rather than fail.
pub const ACTIVE_STORE_FORMAT: u32 = STORE_FORMAT_V16;

/// Replica layouts use a disjoint version namespace so standalone and older
/// binaries cannot mistake their records for an ordinary store. Record-layout
/// changes advance both formats; replica header changes also require an
/// explicit profile-version change.
pub const ACTIVE_REPLICA_STORE_FORMAT: u32 = 0x8000_0000 | ACTIVE_STORE_FORMAT;

/// An explicitly opted-in replica catalog namespace, disjoint from standalone
/// and ordinary replicas. Record-layout changes advance this format too; catalog
/// profile changes additionally require an explicit profile-version change.
/// Existing constructors and directories are not upgraded or adopted.
pub const ACTIVE_CATALOG_REPLICA_STORE_FORMAT: u32 = 0xc000_0000 | ACTIVE_STORE_FORMAT;

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
        let snapshot = database.snapshot();
        reject_paired_metadata_markers(&snapshot, &meta)?;
        drop(snapshot);
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|error| StorageError::backend("open the record keyspace", &error))?;

        if replica::has_replica_metadata(&meta)? {
            return Err(StorageError::ReplicaMetadataInStandalone);
        }

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
                    ACTIVE_STORE_FORMAT.to_be_bytes().to_vec(),
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

impl BoundedStateStore for FjallStore {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        let snapshot = self.database.snapshot();
        read_bounded_snapshot(&snapshot, &self.records, limits)
    }
}

fn read_bounded_snapshot(
    snapshot: &fjall::Snapshot,
    records: &Keyspace,
    limits: ReadLimits,
) -> Result<StoreSnapshot, StorageError> {
    let mut budget = ReadBudget::new(limits);
    let mut entries = Vec::new();
    for guard in snapshot.iter(records) {
        budget.check_next_row()?;
        let key = guard
            .key()
            .map_err(|error| StorageError::backend("read a bounded record key", &error))?;
        if key.len() > limits.max_key_bytes {
            return Err(StorageError::ReadLimitExceeded);
        }
        // Every lookup uses this same pinned view; live size/get reads would race.
        let value_bytes = snapshot
            .size_of(records, &key)
            .map_err(|error| StorageError::backend("read a bounded record size", &error))?
            .ok_or_else(|| bounded_snapshot_error("record size is missing from its stable view"))?;
        let value_bytes =
            usize::try_from(value_bytes).map_err(|_| StorageError::ReadLimitExceeded)?;
        budget.consume(key.len(), value_bytes)?;
        let value = snapshot
            .get(records, &key)
            .map_err(|error| StorageError::backend("read a bounded record value", &error))?
            .ok_or_else(|| bounded_snapshot_error("record is missing from its stable view"))?;
        if value.len() != value_bytes {
            return Err(bounded_snapshot_error(
                "record size differs within its stable view",
            ));
        }
        entries.push((key.to_vec(), value.to_vec()));
    }
    Ok(StoreSnapshot { entries })
}

fn bounded_snapshot_error(detail: &'static str) -> StorageError {
    StorageError::Backend {
        operation: "read a bounded snapshot",
        detail: detail.into(),
    }
}

fn reject_paired_metadata_markers(
    snapshot: &fjall::Snapshot,
    meta: &Keyspace,
) -> Result<(), StorageError> {
    // Presence fences the paired role even when the marker itself is malformed.
    for key in [&[0x22, 0x01][..], &[0x22, 0x02][..], &[0x22, 0x03][..]] {
        if snapshot
            .size_of(meta, key)
            .map_err(|error| StorageError::backend("check a paired replica marker", &error))?
            .is_some()
        {
            return Err(StorageError::CorruptMetadata {
                detail: "paired replica metadata cannot be opened by a generic store".into(),
            });
        }
    }
    Ok(())
}

/// Rejects a store this build cannot read, rather than misreading it.
fn require_readable_format(recorded: &[u8]) -> Result<(), StorageError> {
    require_format_version(recorded, ACTIVE_STORE_FORMAT)
}

fn require_format_version(recorded: &[u8], expected: u32) -> Result<(), StorageError> {
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

fn read_entry(guard: fjall::Guard) -> Result<(Key, Value), StorageError> {
    let (key, value) = guard
        .into_inner()
        .map_err(|error| StorageError::backend("read a scanned record", &error))?;
    Ok((key.to_vec(), value.to_vec()))
}

#[cfg(test)]
mod bounded_tests;

#[cfg(test)]
mod paired_marker_tests;

#[cfg(test)]
mod paired_prototype;

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
    fn stamps_the_active_format_when_it_creates_a_store() -> Result<(), StorageError> {
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
    fn refuses_a_store_written_by_a_newer_format() -> Result<(), StorageError> {
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
    fn refuses_a_store_from_before_scheduled_messages() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V2.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V2,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_duplicate_detection() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V3.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V3,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_typed_message_content() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V4.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V4,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_with_the_previous_expiry_index_contract() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V5.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V5,
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
    fn refuses_a_store_without_an_expiration_policy() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V6.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V6,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_typed_topic_topology() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V7.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V7,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_with_the_prior_topic_session_index_rules() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V8.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V8,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_topic_scheduling() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V9.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V9,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_subscription_rules() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V10.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V10,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_sql_filter_policy() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V11.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V11,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn refuses_a_store_from_before_entity_incarnations() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V12.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V12,
                expected: ACTIVE_STORE_FORMAT,
            })
        );
        Ok(())
    }

    #[test]
    fn incarnation_layout_is_not_readable_as_the_previous_layout() {
        let recorded = STORE_FORMAT_V13.to_be_bytes();
        assert_eq!(require_format_version(&recorded, STORE_FORMAT_V13), Ok(()));
        assert_eq!(
            require_format_version(&recorded, STORE_FORMAT_V12),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V13,
                expected: STORE_FORMAT_V12,
            })
        );
    }

    #[test]
    fn refuses_a_store_from_before_sql_actions() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("a temporary directory");
        stamp_format(directory.path(), &STORE_FORMAT_V13.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V13,
                expected: STORE_FORMAT_V16,
            })
        );
        assert_eq!(ACTIVE_STORE_FORMAT, STORE_FORMAT_V16);
        Ok(())
    }

    #[test]
    fn action_layout_is_not_readable_as_the_previous_layout() {
        let recorded = STORE_FORMAT_V14.to_be_bytes();
        assert_eq!(require_format_version(&recorded, STORE_FORMAT_V14), Ok(()));
        assert_eq!(
            require_format_version(&recorded, STORE_FORMAT_V13),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V14,
                expected: STORE_FORMAT_V13,
            })
        );
    }

    #[test]
    fn literal_set_layout_refuses_version_fourteen_directories() -> Result<(), StorageError> {
        let directory = TempDir::new().expect("temporary old layout");
        stamp_format(directory.path(), &STORE_FORMAT_V14.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V14,
                expected: STORE_FORMAT_V16
            })
        );
        assert_eq!(ACTIVE_REPLICA_STORE_FORMAT, 0x8000_0000 | STORE_FORMAT_V16);
        assert_eq!(
            ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
            0xc000_0000 | STORE_FORMAT_V16
        );
        assert_eq!(
            ACTIVE_PROTECTED_STATE_STORE_FORMAT,
            0xd000_0000 | STORE_FORMAT_V16
        );
        Ok(())
    }

    #[test]
    fn literal_set_layout_is_readable_only_as_version_fifteen() {
        let recorded = STORE_FORMAT_V15.to_be_bytes();
        assert_eq!(
            require_readable_format(&recorded),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V15,
                expected: STORE_FORMAT_V16
            })
        );
        assert_eq!(require_format_version(&recorded, STORE_FORMAT_V15), Ok(()));
        assert_eq!(
            require_format_version(&recorded, STORE_FORMAT_V14),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V15,
                expected: STORE_FORMAT_V14
            })
        );
        assert_eq!(
            require_readable_format(&STORE_FORMAT_V14.to_be_bytes()),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V14,
                expected: STORE_FORMAT_V16
            })
        );
    }

    #[test]
    fn session_message_lock_layout_refuses_version_fifteen_directories() -> Result<(), StorageError>
    {
        let directory = TempDir::new().expect("temporary previous layout");
        stamp_format(directory.path(), &STORE_FORMAT_V15.to_be_bytes())?;
        assert_eq!(
            FjallStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V15,
                expected: STORE_FORMAT_V16
            })
        );
        Ok(())
    }

    #[test]
    fn session_message_lock_layout_is_readable_only_as_version_sixteen() {
        let recorded = STORE_FORMAT_V16.to_be_bytes();
        assert_eq!(require_readable_format(&recorded), Ok(()));
        assert_eq!(require_format_version(&recorded, STORE_FORMAT_V16), Ok(()));
        assert_eq!(
            require_format_version(&recorded, STORE_FORMAT_V15),
            Err(StorageError::UnsupportedStoreFormat {
                found: STORE_FORMAT_V16,
                expected: STORE_FORMAT_V15
            })
        );
        assert_eq!(
            require_readable_format(&17_u32.to_be_bytes()),
            Err(StorageError::UnsupportedStoreFormat {
                found: 17,
                expected: STORE_FORMAT_V16
            })
        );
    }

    #[test]
    fn derived_store_layouts_follow_session_message_lock_version_sixteen() {
        assert_eq!(ACTIVE_STORE_FORMAT, STORE_FORMAT_V16);
        assert_eq!(ACTIVE_REPLICA_STORE_FORMAT, 0x8000_0010);
        assert_eq!(ACTIVE_CATALOG_REPLICA_STORE_FORMAT, 0xc000_0010);
        assert_eq!(ACTIVE_PROTECTED_STATE_STORE_FORMAT, 0xd000_0010);
        for format in [
            ACTIVE_REPLICA_STORE_FORMAT,
            ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
            ACTIVE_PROTECTED_STATE_STORE_FORMAT,
        ] {
            assert_eq!(
                require_format_version(&format.to_be_bytes(), format),
                Ok(())
            );
            for other in [format - 1, format + 1] {
                assert_eq!(
                    require_format_version(&other.to_be_bytes(), format),
                    Err(StorageError::UnsupportedStoreFormat {
                        found: other,
                        expected: format
                    })
                );
                assert_eq!(
                    require_format_version(&format.to_be_bytes(), other),
                    Err(StorageError::UnsupportedStoreFormat {
                        found: format,
                        expected: other
                    })
                );
            }
            assert_eq!(
                require_readable_format(&format.to_be_bytes()),
                Err(StorageError::UnsupportedStoreFormat {
                    found: format,
                    expected: STORE_FORMAT_V16
                })
            );
        }
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
