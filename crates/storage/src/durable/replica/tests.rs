use tempfile::TempDir;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn stamp(
    directory: &Path,
    metadata: &[(&[u8], &[u8])],
    records: &[(&[u8], &[u8])],
) -> Result<(), StorageError> {
    let database = Database::builder(directory)
        .open()
        .map_err(|error| StorageError::backend("open the test directory", &error))?;
    let meta = database
        .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("open the test metadata", &error))?;
    let records_keyspace = database
        .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
        .map_err(|error| StorageError::backend("open the test records", &error))?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    for &(key, value) in metadata {
        batch.insert(&meta, key, value);
    }
    for &(key, value) in records {
        batch.insert(&records_keyspace, key, value);
    }
    batch
        .commit()
        .map_err(|error| StorageError::backend("stamp the test directory", &error))
}

fn valid_headers<'a>(version: &'a [u8], initialized: &'a [u8]) -> [(&'a [u8], &'a [u8]); 3] {
    [
        (FORMAT_VERSION_KEY, version),
        (REPLICA_PROFILE_KEY, REPLICA_PROFILE),
        (REPLICA_INITIALIZED_KEY, initialized),
    ]
}

#[test]
fn fresh_creation_has_one_exact_replica_header() -> TestResult {
    let directory = TempDir::new().expect("temporary directory");
    let writer = FjallReplicaStore::open(directory.path())?;
    let snapshot = writer.store.database.snapshot();
    assert_eq!(
        snapshot.get(&writer.meta, FORMAT_VERSION_KEY)?.as_deref(),
        Some(ACTIVE_REPLICA_STORE_FORMAT.to_be_bytes().as_slice())
    );
    assert_eq!(
        snapshot.get(&writer.meta, REPLICA_PROFILE_KEY)?.as_deref(),
        Some(REPLICA_PROFILE)
    );
    assert_eq!(
        snapshot
            .get(&writer.meta, REPLICA_INITIALIZED_KEY)?
            .as_deref(),
        Some([0].as_slice())
    );
    assert_eq!(snapshot.iter(&writer.meta).count(), 3);
    assert!(writer.reader().snapshot()?.entries().is_empty());
    assert!(!writer.is_initialized()?);
    Ok(())
}

#[test]
fn replica_format_is_outside_the_standalone_version_namespace() {
    assert_eq!(
        ACTIVE_REPLICA_STORE_FORMAT,
        0x8000_0000 | ACTIVE_STORE_FORMAT
    );
    assert_eq!(
        require_format_version(
            &ACTIVE_REPLICA_STORE_FORMAT.to_be_bytes(),
            ACTIVE_STORE_FORMAT
        ),
        Err(StorageError::UnsupportedStoreFormat {
            found: ACTIVE_REPLICA_STORE_FORMAT,
            expected: ACTIVE_STORE_FORMAT,
        })
    );
}

#[test]
fn standalone_refuses_each_replica_marker_before_stamping() -> TestResult {
    let version = ACTIVE_STORE_FORMAT.to_be_bytes();
    for format in [None, Some(version.as_slice())] {
        for marker in [
            (REPLICA_PROFILE_KEY, REPLICA_PROFILE),
            (REPLICA_INITIALIZED_KEY, [0].as_slice()),
        ] {
            let directory = TempDir::new().expect("temporary directory");
            let mut metadata = vec![marker];
            if let Some(format) = format {
                metadata.push((FORMAT_VERSION_KEY, format));
            }
            stamp(directory.path(), &metadata, &[])?;
            assert_eq!(
                FjallStore::open(directory.path()).err(),
                Some(StorageError::ReplicaMetadataInStandalone)
            );
            let database = Database::builder(directory.path())
                .open()
                .map_err(|error| StorageError::backend("inspect the test directory", &error))?;
            let meta = database
                .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(|error| StorageError::backend("inspect the test metadata", &error))?;
            assert_eq!(meta.get(FORMAT_VERSION_KEY)?.as_deref(), format);
            assert_eq!(meta.get(marker.0)?.as_deref(), Some(marker.1));
        }
    }
    Ok(())
}

#[test]
fn replica_refuses_partial_and_malformed_headers() -> TestResult {
    let version = ACTIVE_REPLICA_STORE_FORMAT.to_be_bytes();
    let headers = valid_headers(&version, &[0]);
    let cases = vec![
        vec![headers[0]],
        vec![headers[0], headers[1]],
        vec![headers[0], headers[2]],
        vec![headers[1], headers[2]],
        vec![headers[0], (REPLICA_PROFILE_KEY, b""), headers[2]],
        vec![
            headers[0],
            (REPLICA_PROFILE_KEY, b"committed-state-v2"),
            headers[2],
        ],
        vec![headers[0], headers[1], (REPLICA_INITIALIZED_KEY, b"")],
        vec![headers[0], headers[1], (REPLICA_INITIALIZED_KEY, &[2])],
        vec![headers[0], headers[1], (REPLICA_INITIALIZED_KEY, &[0, 1])],
        vec![(FORMAT_VERSION_KEY, &[0]), headers[1], headers[2]],
    ];
    for metadata in cases {
        let directory = TempDir::new().expect("temporary directory");
        stamp(directory.path(), &metadata, &[])?;
        assert!(matches!(
            FjallReplicaStore::open(directory.path()),
            Err(StorageError::CorruptMetadata { .. })
        ));
    }
    Ok(())
}

#[test]
fn replica_refuses_other_layouts_without_adoption() -> TestResult {
    for found in [
        ACTIVE_STORE_FORMAT,
        ACTIVE_REPLICA_STORE_FORMAT - 1,
        ACTIVE_REPLICA_STORE_FORMAT + 1,
    ] {
        let directory = TempDir::new().expect("temporary directory");
        let version = found.to_be_bytes();
        stamp(directory.path(), &valid_headers(&version, &[0]), &[])?;
        assert_eq!(
            FjallReplicaStore::open(directory.path()).err(),
            Some(StorageError::UnsupportedStoreFormat {
                found,
                expected: ACTIVE_REPLICA_STORE_FORMAT,
            })
        );
    }
    Ok(())
}

#[test]
fn unversioned_records_or_metadata_are_never_adopted() -> TestResult {
    for (metadata, records) in [
        (vec![], vec![(b"record".as_slice(), b"value".as_slice())]),
        (vec![(b"unknown".as_slice(), b"value".as_slice())], vec![]),
        (
            vec![(b"unknown".as_slice(), b"value".as_slice())],
            vec![(b"record".as_slice(), b"value".as_slice())],
        ),
    ] {
        let directory = TempDir::new().expect("temporary directory");
        stamp(directory.path(), &metadata, &records)?;
        assert_eq!(
            FjallReplicaStore::open(directory.path()).err(),
            Some(corrupt("unversioned replica directory is not empty"))
        );
    }
    Ok(())
}

#[test]
fn false_initialized_flag_cannot_cover_existing_records() -> TestResult {
    let directory = TempDir::new().expect("temporary directory");
    let version = ACTIVE_REPLICA_STORE_FORMAT.to_be_bytes();
    stamp(
        directory.path(),
        &valid_headers(&version, &[0]),
        &[(b"record", b"value")],
    )?;
    assert_eq!(
        FjallReplicaStore::open(directory.path()).err(),
        Some(corrupt("uninitialized replica contains records"))
    );
    Ok(())
}

#[test]
fn initialized_reads_authoritative_metadata_and_bad_flags_block_commit() -> TestResult {
    let directory = TempDir::new().expect("temporary directory");
    let mut writer = FjallReplicaStore::open(directory.path())?;
    assert!(!writer.is_initialized()?);
    let mut change = writer
        .store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    change.insert(&writer.meta, REPLICA_INITIALIZED_KEY, vec![1]);
    change
        .commit()
        .map_err(|error| StorageError::backend("change test flag", &error))?;
    assert!(writer.is_initialized()?);

    let before = writer.reader().snapshot()?;
    let mut change = writer
        .store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    change.insert(&writer.meta, REPLICA_INITIALIZED_KEY, vec![2]);
    change
        .commit()
        .map_err(|error| StorageError::backend("corrupt test flag", &error))?;
    assert_eq!(
        writer.is_initialized(),
        Err(corrupt("replica initialized flag is malformed"))
    );
    assert_eq!(
        writer.commit(WriteBatch::default().put(b"record", b"value")),
        Err(corrupt("replica initialized flag is malformed"))
    );
    assert_eq!(writer.reader().snapshot()?, before);
    Ok(())
}

#[test]
fn initialized_and_records_share_one_snapshot_boundary() -> TestResult {
    let directory = TempDir::new().expect("temporary directory");
    let mut writer = FjallReplicaStore::open(directory.path())?;
    let before = writer.store.database.snapshot();
    writer.commit(WriteBatch::default().put(b"record", b"value"))?;
    let after = writer.store.database.snapshot();
    assert_eq!(
        before
            .get(&writer.meta, REPLICA_INITIALIZED_KEY)?
            .as_deref(),
        Some([0].as_slice())
    );
    assert_eq!(before.get(&writer.store.records, b"record")?, None);
    assert_eq!(
        after.get(&writer.meta, REPLICA_INITIALIZED_KEY)?.as_deref(),
        Some([1].as_slice())
    );
    assert_eq!(
        after.get(&writer.store.records, b"record")?.as_deref(),
        Some(b"value".as_slice())
    );
    Ok(())
}
