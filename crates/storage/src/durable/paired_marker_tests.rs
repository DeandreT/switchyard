//! Controlled valid native fixtures only. Logical dictionaries may be damaged;
//! normal backend recovery effects are allowed, not arbitrary physical repair.

use tempfile::TempDir;

use crate::{CatalogCommittedStore, CommittedStore, SnapshotCatalogReader};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Rows = Vec<(Key, Value)>;

const MARKERS: [&[u8]; 3] = [&[0x22, 0x01], &[0x22, 0x02], &[0x22, 0x03]];
const PROFILE: &[u8] = b"replica_profile";
const INITIALIZED: &[u8] = b"replica_initialized";
const CATALOG_META: &[u8] = b"snapshot_meta";
const CATALOG_IMAGE: &[u8] = b"snapshot_image";
const OPENERS: [Opener; 3] = [Opener::Standalone, Opener::Replica, Opener::Catalog];

#[derive(Clone, Copy, Debug)]
enum Opener {
    Standalone,
    Replica,
    Catalog,
}

impl Opener {
    fn attempt(self, directory: &Path) -> Result<(), StorageError> {
        match self {
            Self::Standalone => FjallStore::open(directory).map(drop),
            Self::Replica => FjallReplicaStore::open(directory).map(drop),
            Self::Catalog => FjallCatalogReplicaStore::open(directory).map(drop),
        }
    }

    fn format(self) -> u32 {
        match self {
            Self::Standalone => ACTIVE_STORE_FORMAT,
            Self::Replica => ACTIVE_REPLICA_STORE_FORMAT,
            Self::Catalog => ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
        }
    }

    fn headers(self, initialized: bool) -> Rows {
        let mut rows = vec![(
            FORMAT_VERSION_KEY.to_vec(),
            self.format().to_be_bytes().to_vec(),
        )];
        let profile = match self {
            Self::Standalone => return rows,
            Self::Replica => b"committed-state-v1".as_slice(),
            Self::Catalog => b"committed-state-catalog-v1".as_slice(),
        };
        rows.push((PROFILE.to_vec(), profile.to_vec()));
        rows.push((INITIALIZED.to_vec(), vec![u8::from(initialized)]));
        rows
    }

    fn read_records(self, directory: &Path) -> Result<StoreSnapshot, StorageError> {
        match self {
            Self::Standalone => FjallStore::open(directory)?.snapshot(),
            Self::Replica => FjallReplicaStore::open(directory)?.reader().snapshot(),
            Self::Catalog => FjallCatalogReplicaStore::open(directory)?
                .reader()
                .snapshot(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct LogicalView {
    metadata: Rows,
    records: Option<Rows>,
}

fn marker_refusal() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "paired replica metadata cannot be opened by a generic store".into(),
    }
}

fn stamp(
    directory: &Path,
    metadata: &[(Key, Value)],
    records: Option<&[(Key, Value)]>,
) -> TestResult {
    let database = Database::builder(directory).worker_threads(1).open()?;
    let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
    let record_space = records
        .map(|_| database.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default))
        .transpose()?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    for (key, value) in metadata {
        batch.insert(&meta, key, value);
    }
    if let (Some(space), Some(rows)) = (&record_space, records) {
        for (key, value) in rows {
            batch.insert(space, key, value);
        }
    }
    batch.commit()?;
    drop(record_space);
    drop(meta);
    drop(database);
    Ok(())
}

fn capture(directory: &Path) -> TestResult<LogicalView> {
    let database = Database::builder(directory).worker_threads(1).open()?;
    // These fixtures were created here; this is not an arbitrary existing-only API.
    assert!(database.keyspace_exists(META_KEYSPACE));
    let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
    let record_space = if database.keyspace_exists(RECORDS_KEYSPACE) {
        Some(database.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)?)
    } else {
        None
    };
    let snapshot = database.snapshot();
    let metadata = snapshot
        .iter(&meta)
        .map(read_entry)
        .collect::<Result<_, _>>()?;
    let records = record_space
        .as_ref()
        .map(|space| {
            snapshot
                .iter(space)
                .map(read_entry)
                .collect::<Result<Rows, _>>()
        })
        .transpose()?;
    let value = LogicalView { metadata, records };
    drop(snapshot);
    drop(record_space);
    drop(meta);
    drop(database);
    Ok(value)
}

fn assert_refused(directory: &Path, opener: Opener, before: &LogicalView) -> TestResult {
    assert_eq!(
        opener.attempt(directory).err(),
        Some(marker_refusal()),
        "{opener:?}"
    );
    // Owned values survive two independent reopens; each helper drops ALL handles.
    let first = capture(directory)?;
    assert_eq!(&first, before);
    let second = capture(directory)?;
    assert_eq!(&second, before);
    assert_eq!(first, second);
    Ok(())
}

fn marker_values(marker: &[u8]) -> Vec<Vec<u8>> {
    let mut fixed = vec![0; 112];
    fixed[..4].copy_from_slice(if marker[1] == 2 { b"SWCR" } else { b"SWAP" });
    fixed[4..6].copy_from_slice(&1u16.to_be_bytes());
    fixed[6] = 1;
    fixed[7] = u8::from(marker[1] == 2);
    fixed[8..24].fill(1);
    fixed[24..40].fill(2);
    fixed[40..56].fill(3);
    fixed[56..64].copy_from_slice(&7u64.to_be_bytes());
    fixed[64..80].fill(7);
    fixed[80..112].fill(9);
    let mut bad_magic = fixed.clone();
    bad_magic[0] ^= 1;
    let mut bad_schema = fixed.clone();
    bad_schema[5] = 2;
    let mut bad_role = fixed.clone();
    bad_role[6] = 99;
    vec![
        Vec::new(),
        vec![0xff],
        fixed,
        bad_magic,
        bad_schema,
        bad_role,
        vec![0; 128 * 1024 + 1],
    ]
}

#[test]
fn every_marker_value_shape_refuses_every_generic_opener_without_logical_writes() -> TestResult {
    let records = vec![(b"business".to_vec(), b"unchanged".to_vec())];
    for marker in MARKERS {
        for value in marker_values(marker) {
            for opener in OPENERS {
                let directory = TempDir::new()?;
                stamp(
                    directory.path(),
                    &[(marker.to_vec(), value.clone())],
                    Some(&records),
                )?;
                let before = capture(directory.path())?;
                assert_refused(directory.path(), opener, &before)?;
                assert!(
                    !before
                        .metadata
                        .iter()
                        .any(|(key, _)| key == FORMAT_VERSION_KEY)
                );
            }
        }
    }
    Ok(())
}

#[test]
fn marker_presence_precedes_all_common_header_subsets_and_matching_profiles() -> TestResult {
    let records = vec![(b"business".to_vec(), b"preserved".to_vec())];
    for opener in OPENERS {
        for marker in MARKERS {
            for subset in 0..8u8 {
                let mut metadata = opener.headers(true);
                if matches!(opener, Opener::Standalone) {
                    metadata.push((PROFILE.to_vec(), b"opaque-profile".to_vec()));
                    metadata.push((INITIALIZED.to_vec(), vec![1]));
                }
                metadata.retain(|(key, _)| {
                    (key != FORMAT_VERSION_KEY || subset & 1 != 0)
                        && (key != PROFILE || subset & 2 != 0)
                        && (key != INITIALIZED || subset & 4 != 0)
                });
                if matches!(opener, Opener::Catalog) {
                    metadata.push((CATALOG_META.to_vec(), b"old-meta".to_vec()));
                    metadata.push((CATALOG_IMAGE.to_vec(), b"old-image".to_vec()));
                }
                metadata.push((marker.to_vec(), Vec::new()));
                let directory = TempDir::new()?;
                stamp(directory.path(), &metadata, Some(&records))?;
                let before = capture(directory.path())?;
                assert_refused(directory.path(), opener, &before)?;
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_headers_and_several_markers_never_fall_through_to_stamp_or_profile_checks()
-> TestResult {
    let records = vec![(b"business".to_vec(), b"preserved".to_vec())];
    for opener in OPENERS {
        for common in [FORMAT_VERSION_KEY, PROFILE, INITIALIZED] {
            let directory = TempDir::new()?;
            let mut metadata = opener.headers(true);
            metadata.retain(|(key, _)| key != common);
            metadata.push((common.to_vec(), vec![0xff]));
            for (index, marker) in MARKERS.into_iter().enumerate() {
                metadata.push((marker.to_vec(), vec![index as u8]));
            }
            stamp(directory.path(), &metadata, Some(&records))?;
            let before = capture(directory.path())?;
            assert_refused(directory.path(), opener, &before)?;
        }
    }
    Ok(())
}

#[test]
fn marker_only_metadata_never_acquires_or_creates_the_absent_records_keyspace() -> TestResult {
    for marker in MARKERS {
        for opener in OPENERS {
            let directory = TempDir::new()?;
            stamp(directory.path(), &[(marker.to_vec(), Vec::new())], None)?;
            let before = capture(directory.path())?;
            assert!(before.records.is_none());
            assert_refused(directory.path(), opener, &before)?;
        }
    }
    Ok(())
}

#[test]
fn marker_presence_comes_from_the_supplied_pinned_view_even_after_live_changes() -> TestResult {
    for marker in MARKERS {
        let directory = TempDir::new()?;
        let database = Database::builder(directory.path())
            .worker_threads(1)
            .open()?;
        let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
        let old = database.snapshot();
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&meta, marker, b"PRIVATE-marker-value".as_slice());
        batch.commit()?;
        let with_marker = database.snapshot();
        assert_eq!(reject_paired_metadata_markers(&old, &meta), Ok(()));
        assert_eq!(
            reject_paired_metadata_markers(&with_marker, &meta),
            Err(marker_refusal())
        );
        let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
        batch.remove(&meta, marker);
        batch.commit()?;
        let after = database.snapshot();
        assert_eq!(reject_paired_metadata_markers(&after, &meta), Ok(()));
        assert_eq!(
            reject_paired_metadata_markers(&with_marker, &meta),
            Err(marker_refusal())
        );
    }
    Ok(())
}

#[test]
fn no_marker_fresh_and_initialized_profiles_keep_exact_logical_behavior_and_business_keys()
-> TestResult {
    for opener in OPENERS {
        let fresh = TempDir::new()?;
        opener.attempt(fresh.path())?;
        let first = capture(fresh.path())?;
        let mut expected = opener.headers(false);
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(first.metadata, expected);
        assert!(first.records.as_ref().is_none_or(Vec::is_empty));
        opener.attempt(fresh.path())?;
        assert_eq!(capture(fresh.path())?.metadata, expected);
        let initialized = TempDir::new()?;
        let mut records = vec![(b"business".to_vec(), b"unchanged".to_vec())];
        for marker in MARKERS {
            records.push((marker.to_vec(), b"ordinary record".to_vec()));
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        let mut metadata = opener.headers(true);
        if matches!(opener, Opener::Catalog) {
            metadata.push((CATALOG_META.to_vec(), b"old-meta".to_vec()));
            metadata.push((CATALOG_IMAGE.to_vec(), b"old-image".to_vec()));
        }
        stamp(initialized.path(), &metadata, Some(&records))?;
        let before = capture(initialized.path())?;
        assert_eq!(
            opener.read_records(initialized.path())?.entries(),
            records.as_slice()
        );
        assert_eq!(capture(initialized.path())?, before);
        if matches!(opener, Opener::Catalog) {
            let writer = FjallCatalogReplicaStore::open(initialized.path())?;
            let reader = writer.catalog_reader();
            let retained = reader.read_catalog()?.ok_or("catalog missing")?;
            assert_eq!(retained.metadata(), b"old-meta");
            assert_eq!(retained.artifact(), b"old-image");
            drop(reader);
            drop(writer);
            assert_eq!(capture(initialized.path())?, before);
        }
    }
    Ok(())
}

#[test]
fn no_marker_existing_refusal_order_and_unversioned_standalone_behavior_are_preserved() -> TestResult
{
    for opener in OPENERS {
        let directory = TempDir::new()?;
        let found = opener.format() + 1;
        let metadata = vec![(FORMAT_VERSION_KEY.to_vec(), found.to_be_bytes().to_vec())];
        stamp(directory.path(), &metadata, Some(&[]))?;
        let before = capture(directory.path())?;
        assert_eq!(
            opener.attempt(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found,
                expected: opener.format()
            })
        );
        assert_eq!(capture(directory.path())?, before);
    }
    let replica = TempDir::new()?;
    let records = vec![(b"business".to_vec(), b"unchanged".to_vec())];
    let metadata = vec![
        (PROFILE.to_vec(), b"committed-state-v1".to_vec()),
        (
            FORMAT_VERSION_KEY.to_vec(),
            (ACTIVE_STORE_FORMAT + 1).to_be_bytes().to_vec(),
        ),
    ];
    stamp(replica.path(), &metadata, Some(&records))?;
    let before = capture(replica.path())?;
    assert_eq!(
        Opener::Standalone.attempt(replica.path()).err(),
        Some(StorageError::ReplicaMetadataInStandalone)
    );
    assert_eq!(capture(replica.path())?, before);
    let catalog = TempDir::new()?;
    let metadata = vec![
        (
            FORMAT_VERSION_KEY.to_vec(),
            (ACTIVE_CATALOG_REPLICA_STORE_FORMAT + 1)
                .to_be_bytes()
                .to_vec(),
        ),
        (b"unknown".to_vec(), vec![0]),
    ];
    stamp(catalog.path(), &metadata, Some(&records))?;
    let before = capture(catalog.path())?;
    assert_eq!(
        Opener::Catalog.attempt(catalog.path()).err(),
        Some(StorageError::CorruptMetadata {
            detail: "catalog replica has an unknown metadata key".into(),
        })
    );
    assert_eq!(capture(catalog.path())?, before);
    let standalone = TempDir::new()?;
    stamp(
        standalone.path(),
        &[(b"unknown".to_vec(), vec![0])],
        Some(&records),
    )?;
    assert_eq!(
        Opener::Standalone
            .read_records(standalone.path())?
            .entries(),
        records.as_slice()
    );
    let after = capture(standalone.path())?;
    assert!(after.metadata.contains(&(
        FORMAT_VERSION_KEY.to_vec(),
        ACTIVE_STORE_FORMAT.to_be_bytes().to_vec()
    )));
    assert!(after.metadata.contains(&(b"unknown".to_vec(), vec![0])));
    Ok(())
}

#[test]
fn marker_refusal_diagnostics_contain_no_marker_value_or_directory() -> TestResult {
    for opener in OPENERS {
        let directory = TempDir::new()?;
        stamp(
            directory.path(),
            &[(
                MARKERS[0].to_vec(),
                b"PRIVATE-marker-id-hash-payload".to_vec(),
            )],
            None,
        )?;
        let error = opener
            .attempt(directory.path())
            .err()
            .ok_or("marker admitted")?;
        assert_eq!(error, marker_refusal());
        let text = format!("{error:?}: {error}");
        assert!(!text.contains("PRIVATE"));
        let directory_text = directory.path().to_string_lossy();
        assert!(!text.contains(directory_text.as_ref()));
    }
    Ok(())
}
