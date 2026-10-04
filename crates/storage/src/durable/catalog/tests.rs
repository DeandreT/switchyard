use tempfile::TempDir;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn headers<'a>(version: &'a [u8], initialized: &'a [u8]) -> [(&'a [u8], &'a [u8]); 3] {
    [
        (FORMAT_VERSION_KEY, version),
        (PROFILE_KEY, PROFILE),
        (INITIALIZED_KEY, initialized),
    ]
}

fn stamp(directory: &Path, metadata: &[(&[u8], &[u8])], records: &[(&[u8], &[u8])]) -> TestResult {
    let database = Database::builder(directory).open()?;
    let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
    let record_space = database.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    for &(key, value) in metadata {
        batch.insert(&meta, key, value);
    }
    for &(key, value) in records {
        batch.insert(&record_space, key, value);
    }
    batch.commit()?;
    Ok(())
}

fn inspect(directory: &Path) -> Result<(StoreSnapshot, StoreSnapshot), StorageError> {
    let database = Database::builder(directory)
        .open()
        .map_err(|error| StorageError::backend("inspect catalog test directory", &error))?;
    let meta = database
        .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("inspect catalog test metadata", &error))?;
    let records = database
        .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("inspect catalog test records", &error))?;
    let snapshot = database.snapshot();
    Ok((
        StoreSnapshot {
            entries: snapshot
                .iter(&meta)
                .map(read_entry)
                .collect::<Result<_, _>>()?,
        },
        StoreSnapshot {
            entries: snapshot
                .iter(&records)
                .map(read_entry)
                .collect::<Result<_, _>>()?,
        },
    ))
}

#[test]
fn fresh_profile_has_exactly_three_headers_and_no_business_or_catalog() -> TestResult {
    let directory = TempDir::new()?;
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let snapshot = writer.store.database.snapshot();
    assert_eq!(snapshot.iter(&writer.meta).count(), 3);
    assert_eq!(
        snapshot.get(&writer.meta, FORMAT_VERSION_KEY)?.as_deref(),
        Some(ACTIVE_CATALOG_REPLICA_STORE_FORMAT.to_be_bytes().as_slice())
    );
    assert_eq!(
        snapshot.get(&writer.meta, PROFILE_KEY)?.as_deref(),
        Some(PROFILE)
    );
    assert_eq!(
        snapshot.get(&writer.meta, INITIALIZED_KEY)?.as_deref(),
        Some([0].as_slice())
    );
    assert!(!writer.is_initialized()?);
    assert!(writer.reader().snapshot()?.entries().is_empty());
    assert!(writer.catalog_reader().read_catalog()?.is_none());
    Ok(())
}

#[test]
fn disjoint_format_couples_record_layout_without_changing_existing_constants() {
    assert_eq!(
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
        0xc000_0000 | ACTIVE_STORE_FORMAT
    );
    assert_eq!(
        ACTIVE_REPLICA_STORE_FORMAT,
        0x8000_0000 | ACTIVE_STORE_FORMAT
    );
    assert_ne!(
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
        ACTIVE_REPLICA_STORE_FORMAT
    );
    assert_ne!(ACTIVE_CATALOG_REPLICA_STORE_FORMAT, ACTIVE_STORE_FORMAT);
}

#[test]
fn one_pinned_old_view_and_one_new_view_never_mix_slot_records_and_initialization() -> TestResult {
    let directory = TempDir::new()?;
    let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
    let pristine = writer.store.database.snapshot();
    writer.commit_with_catalog(
        WriteBatch::default().put(b"business", b"first"),
        SnapshotCatalogRecord::new(b"first-meta", b"first-artifact")?,
    )?;
    let first = writer.store.database.snapshot();
    writer.commit_with_catalog(
        WriteBatch::default().put(b"business", b"second"),
        SnapshotCatalogRecord::new(b"second-meta", b"second-artifact")?,
    )?;
    let second = writer.store.database.snapshot();
    assert!(!validate_view(&pristine, &writer.store.records, &writer.meta)?.initialized);
    assert!(read_catalog_view(&pristine, &writer.store.records, &writer.meta)?.is_none());
    assert_eq!(pristine.get(&writer.store.records, b"business")?, None);
    for (view, business, metadata, artifact) in [
        (
            &first,
            b"first".as_slice(),
            b"first-meta".as_slice(),
            b"first-artifact".as_slice(),
        ),
        (
            &second,
            b"second".as_slice(),
            b"second-meta".as_slice(),
            b"second-artifact".as_slice(),
        ),
    ] {
        assert!(validate_view(view, &writer.store.records, &writer.meta)?.initialized);
        let slot = read_catalog_view(view, &writer.store.records, &writer.meta)?
            .ok_or("missing captured slot")?;
        assert_eq!(
            view.get(&writer.store.records, b"business")?.as_deref(),
            Some(business)
        );
        assert_eq!(slot.metadata(), metadata);
        assert_eq!(slot.artifact(), artifact);
    }
    Ok(())
}

#[test]
fn partial_malformed_and_unknown_headers_are_refused_without_changing_bytes() -> TestResult {
    let version = ACTIVE_CATALOG_REPLICA_STORE_FORMAT.to_be_bytes();
    let valid = headers(&version, &[0]);
    let cases = vec![
        vec![valid[0]],
        vec![valid[0], valid[1]],
        vec![valid[0], valid[2]],
        vec![valid[1], valid[2]],
        vec![valid[0], (PROFILE_KEY, b""), valid[2]],
        vec![
            valid[0],
            (PROFILE_KEY, b"committed-state-catalog-v2"),
            valid[2],
        ],
        vec![valid[0], valid[1], (INITIALIZED_KEY, b"")],
        vec![valid[0], valid[1], (INITIALIZED_KEY, &[2])],
        vec![valid[0], valid[1], (INITIALIZED_KEY, &[0, 1])],
        vec![(FORMAT_VERSION_KEY, &[0]), valid[1], valid[2]],
        vec![
            valid[0],
            valid[1],
            valid[2],
            (b"future-profile-key", b"opaque"),
        ],
    ];
    for metadata in cases {
        let directory = TempDir::new()?;
        stamp(directory.path(), &metadata, &[])?;
        let before = inspect(directory.path())?;
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(inspect(directory.path())?, before);
    }
    Ok(())
}

#[test]
fn unversioned_nonempty_directories_are_not_stamped_or_adopted() -> TestResult {
    for (metadata, records) in [
        (vec![(b"unknown".as_slice(), b"value".as_slice())], vec![]),
        (vec![], vec![(b"business".as_slice(), b"value".as_slice())]),
        (vec![(CATALOG_METADATA_KEY, b"meta".as_slice())], vec![]),
    ] {
        let directory = TempDir::new()?;
        stamp(directory.path(), &metadata, &records)?;
        let before = inspect(directory.path())?;
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(inspect(directory.path())?, before);
    }
    Ok(())
}

#[test]
fn an_uninitialized_profile_cannot_cover_records_or_even_an_empty_catalog_pair() -> TestResult {
    let version = ACTIVE_CATALOG_REPLICA_STORE_FORMAT.to_be_bytes();
    for catalog in [false, true] {
        let directory = TempDir::new()?;
        let mut metadata = headers(&version, &[0]).to_vec();
        let records = if catalog {
            metadata.extend([
                (CATALOG_METADATA_KEY, b"".as_slice()),
                (CATALOG_ARTIFACT_KEY, b"".as_slice()),
            ]);
            Vec::new()
        } else {
            vec![(b"business".as_slice(), b"unexpected".as_slice())]
        };
        stamp(directory.path(), &metadata, &records)?;
        let before = inspect(directory.path())?;
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(inspect(directory.path())?, before);
    }
    Ok(())
}

#[test]
fn either_half_present_slot_is_corrupt_on_open_read_and_both_write_paths() -> TestResult {
    for key in [CATALOG_METADATA_KEY, CATALOG_ARTIFACT_KEY] {
        let directory = TempDir::new()?;
        let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
        writer.commit(WriteBatch::default().put(b"business", b"original"))?;
        let mut damage = writer
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        damage.insert(&writer.meta, key, b"only-half".as_slice());
        damage.commit()?;
        assert!(matches!(
            writer.catalog_reader().read_catalog(),
            Err(CatalogReadError::Storage(
                StorageError::CorruptMetadata { .. }
            ))
        ));
        assert!(matches!(
            writer.commit(WriteBatch::default().put(b"business", b"forbidden")),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert!(matches!(
            writer.commit_with_catalog(
                WriteBatch::default().delete(b"business"),
                SnapshotCatalogRecord::new(b"replacement", b"replacement")?,
            ),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(
            writer.reader().get(b"business")?,
            Some(b"original".to_vec())
        );
        drop(writer);
        let before = inspect(directory.path())?;
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(inspect(directory.path())?, before);
    }
    Ok(())
}

#[test]
fn unknown_metadata_or_bad_live_header_blocks_reads_and_preserving_commits() -> TestResult {
    for (key, value) in [
        (b"unknown".as_slice(), b"opaque".as_slice()),
        (PROFILE_KEY, b"committed-state-catalog-v2".as_slice()),
        (INITIALIZED_KEY, b"bad".as_slice()),
    ] {
        let directory = TempDir::new()?;
        let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
        writer.commit_with_catalog(
            WriteBatch::default().put(b"business", b"original"),
            SnapshotCatalogRecord::new(b"meta", b"artifact")?,
        )?;
        let mut damage = writer
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        damage.insert(&writer.meta, key, value);
        damage.commit()?;
        assert!(matches!(
            writer.is_initialized(),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert!(matches!(
            writer.catalog_reader().read_catalog(),
            Err(CatalogReadError::Storage(
                StorageError::CorruptMetadata { .. }
            ))
        ));
        assert!(matches!(
            writer.commit(WriteBatch::default().delete(b"business")),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(
            writer.reader().get(b"business")?,
            Some(b"original".to_vec())
        );
    }
    Ok(())
}

#[test]
fn oversized_physical_components_are_refused_on_read_and_reopen_before_result_copy() -> TestResult {
    for (key, limit) in [
        (CATALOG_METADATA_KEY, MAX_CATALOG_METADATA_BYTES),
        (CATALOG_ARTIFACT_KEY, MAX_CATALOG_ARTIFACT_BYTES),
    ] {
        let directory = TempDir::new()?;
        let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
        writer.commit_with_catalog(
            WriteBatch::default().put(b"business", b"original"),
            SnapshotCatalogRecord::new(b"meta", b"artifact")?,
        )?;
        let oversized = vec![0; limit + 1];
        let mut damage = writer
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        damage.insert(&writer.meta, key, oversized);
        damage.commit()?;
        assert_eq!(
            writer.catalog_reader().read_catalog().unwrap_err(),
            CatalogReadError::LimitExceeded
        );
        assert_eq!(
            writer.is_initialized(),
            Err(StorageError::ReadLimitExceeded)
        );
        assert_eq!(
            writer.commit(WriteBatch::default().delete(b"business")),
            Err(StorageError::ReadLimitExceeded)
        );
        assert_eq!(
            writer.reader().get(b"business")?,
            Some(b"original".to_vec())
        );
        drop(writer);
        assert_eq!(
            FjallCatalogReplicaStore::open(directory.path()).err(),
            Some(StorageError::ReadLimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn exact_value_lengths_are_checked_against_the_same_pinned_view() -> TestResult {
    let directory = TempDir::new()?;
    let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
    writer.commit_with_catalog(
        WriteBatch::default(),
        SnapshotCatalogRecord::new(b"old", b"artifact")?,
    )?;
    let old = writer.store.database.snapshot();
    writer.commit_with_catalog(
        WriteBatch::default(),
        SnapshotCatalogRecord::new(b"new-metadata", b"new-artifact")?,
    )?;
    assert_eq!(
        required_value(&old, &writer.meta, CATALOG_METADATA_KEY, 3)?.as_ref(),
        b"old"
    );
    assert!(matches!(
        required_value(&old, &writer.meta, CATALOG_METADATA_KEY, 12),
        Err(StorageError::CorruptMetadata { .. })
    ));
    let current = writer.store.database.snapshot();
    assert_eq!(
        required_value(&current, &writer.meta, CATALOG_METADATA_KEY, 12)?.as_ref(),
        b"new-metadata"
    );
    Ok(())
}

#[test]
fn captured_profile_and_catalog_are_not_revalidated_against_a_different_live_view() -> TestResult {
    let directory = TempDir::new()?;
    let mut writer = FjallCatalogReplicaStore::open(directory.path())?;
    writer.commit_with_catalog(
        WriteBatch::default(),
        SnapshotCatalogRecord::new(b"old meta", b"old artifact")?,
    )?;
    let captured = writer.store.database.snapshot();
    let mut damage = writer
        .store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    damage.insert(
        &writer.meta,
        PROFILE_KEY,
        b"committed-state-catalog-v2".as_slice(),
    );
    damage.commit()?;
    let old = read_catalog_view(&captured, &writer.store.records, &writer.meta)?
        .ok_or("missing old captured catalog")?;
    assert_eq!(old.metadata(), b"old meta");
    assert_eq!(old.artifact(), b"old artifact");
    assert!(matches!(
        writer.catalog_reader().read_catalog(),
        Err(CatalogReadError::Storage(
            StorageError::CorruptMetadata { .. }
        ))
    ));
    Ok(())
}

#[test]
fn other_catalog_layout_versions_are_refused_without_rewriting_their_headers() -> TestResult {
    for found in [
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT - 1,
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT + 1,
        u32::MAX,
    ] {
        let directory = TempDir::new()?;
        let version = found.to_be_bytes();
        stamp(directory.path(), &headers(&version, &[0]), &[])?;
        let before = inspect(directory.path())?;
        assert_eq!(
            FjallCatalogReplicaStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found,
                expected: ACTIVE_CATALOG_REPLICA_STORE_FORMAT
            })
        );
        assert_eq!(inspect(directory.path())?, before);
    }
    Ok(())
}
