use std::path::PathBuf;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use tempfile::TempDir;

use super::super::{
    ACTIVE_CATALOG_REPLICA_STORE_FORMAT, CATALOG_ARTIFACT_KEY, CATALOG_METADATA_KEY,
    FORMAT_VERSION_KEY, FjallCatalogReplicaStore, FjallStore, INITIALIZED_KEY, META_KEYSPACE,
    PROFILE, PROFILE_KEY, RECORDS_KEYSPACE,
};
use super::*;
use crate::{CatalogCommittedStore, CommittedStore, SnapshotCatalogRecord, WriteBatch};

type TestResult = Result<(), Box<dyn std::error::Error>>;
type Fixture = (TempDir, FjallCatalogReplicaStore);

fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let path: PathBuf = directory.path().to_path_buf();
    let database = Database::builder(&path).worker_threads(1).open()?;
    let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
    let records = database.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    batch.insert(
        &meta,
        FORMAT_VERSION_KEY,
        ACTIVE_CATALOG_REPLICA_STORE_FORMAT.to_be_bytes().to_vec(),
    );
    batch.insert(&meta, PROFILE_KEY, PROFILE);
    batch.insert(&meta, INITIALIZED_KEY, vec![0]);
    batch.commit()?;
    Ok((
        directory,
        FjallCatalogReplicaStore {
            store: FjallStore {
                database,
                records,
                directory: path,
            },
            meta,
        },
    ))
}

fn edit(writer: &FjallCatalogReplicaStore, changes: &[(&[u8], Option<&[u8]>)]) -> TestResult {
    let mut batch = writer
        .store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    for &(key, value) in changes {
        if let Some(value) = value {
            batch.insert(&writer.meta, key, value);
        } else {
            batch.remove(&writer.meta, key);
        }
    }
    batch.commit()?;
    Ok(())
}

fn catalog(
    writer: &mut FjallCatalogReplicaStore,
    batch: WriteBatch,
    meta: &[u8],
    image: &[u8],
) -> TestResult {
    writer.commit_with_catalog(batch, SnapshotCatalogRecord::new(meta, image)?)?;
    Ok(())
}

#[test]
fn fjall_pristine_complete_state() -> TestResult {
    let (_directory, writer) = fixture()?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert!(!state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(state.catalog().is_none());
    assert_eq!(state.logical_payload_bytes(), 0);
    Ok(())
}

#[test]
fn fjall_initialized_empty_without_catalog() -> TestResult {
    let (_directory, mut writer) = fixture()?;
    writer.commit(WriteBatch::default())?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert!(state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(state.catalog().is_none());
    assert_eq!(state.logical_payload_bytes(), 0);
    Ok(())
}

#[test]
fn fjall_present_empty_catalog_is_not_absence() -> TestResult {
    let (_directory, mut writer) = fixture()?;
    catalog(&mut writer, WriteBatch::default(), b"", b"")?;
    let state = writer.catalog_reader().capture_complete_state()?;
    let pair = state.catalog().ok_or("missing present empty pair")?;
    assert!(state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(pair.metadata().is_empty());
    assert!(pair.artifact().is_empty());
    assert_eq!(state.logical_payload_bytes(), 0);
    Ok(())
}

#[test]
fn fjall_complete_opaque_records_and_catalog() -> TestResult {
    let (_directory, mut writer) = fixture()?;
    catalog(
        &mut writer,
        WriteBatch::default()
            .put(b"z-secret-key", b"secret-value")
            .put(b"a", b""),
        b"secret-meta",
        b"secret-image",
    )?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert_eq!(
        state.records().entries(),
        &[
            (b"a".to_vec(), Vec::new()),
            (b"z-secret-key".to_vec(), b"secret-value".to_vec()),
        ]
    );
    let pair = state.catalog().ok_or("missing pair")?;
    assert_eq!(
        (pair.metadata(), pair.artifact()),
        (b"secret-meta".as_slice(), b"secret-image".as_slice())
    );
    assert_eq!(state.logical_payload_bytes(), 1 + 12 + 12 + 11 + 12);
    let debug = format!("{state:?}");
    for secret in ["secret-key", "secret-value", "secret-meta", "secret-image"] {
        assert!(!debug.contains(secret));
    }
    catalog(&mut writer, WriteBatch::default(), b"", b"")?;
    let empty = writer.catalog_reader().capture_complete_state()?;
    assert!(
        empty
            .catalog()
            .ok_or("missing empty pair")?
            .artifact()
            .is_empty()
    );
    assert_eq!(empty.records().entries(), state.records().entries());
    Ok(())
}

#[test]
fn fjall_older_catalog_coexists_with_new_business() -> TestResult {
    let (_directory, mut writer) = fixture()?;
    catalog(
        &mut writer,
        WriteBatch::default().put(b"business", b"old"),
        b"older-meta",
        b"older-image",
    )?;
    writer.commit(WriteBatch::default().put(b"business", b"new"))?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert_eq!(
        state.records().entries(),
        &[(b"business".to_vec(), b"new".to_vec())]
    );
    let pair = state.catalog().ok_or("missing older pair")?;
    assert_eq!(
        (pair.metadata(), pair.artifact()),
        (b"older-meta".as_slice(), b"older-image".as_slice())
    );
    writer.commit(WriteBatch::default().delete(b"business"))?;
    let empty = writer.catalog_reader().capture_complete_state()?;
    assert!(empty.records().entries().is_empty());
    assert_eq!(
        empty.catalog().ok_or("missing preserved pair")?.artifact(),
        b"older-image"
    );
    Ok(())
}

#[test]
fn fjall_owned_capture_outlives_originating_handles() -> TestResult {
    let (directory, mut writer) = fixture()?;
    catalog(
        &mut writer,
        WriteBatch::default().put(b"key", b"value"),
        b"meta",
        b"image",
    )?;
    let reader = writer.catalog_reader();
    let clone = reader.clone();
    let state = clone.capture_complete_state()?;
    drop(writer);
    drop(reader);
    drop(clone);
    assert_eq!(
        state.records().entries(),
        &[(b"key".to_vec(), b"value".to_vec())]
    );
    assert_eq!(
        state.catalog().ok_or("missing owned pair")?.artifact(),
        b"image"
    );
    assert_eq!(state.logical_payload_bytes(), 3 + 5 + 4 + 5);
    drop(directory);
    Ok(())
}

#[test]
fn fjall_uninitialized_orphans_are_refused() -> TestResult {
    for shape in 0..3 {
        let (_directory, writer) = fixture()?;
        let mut batch = writer
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        match shape {
            0 => {
                batch.insert(&writer.store.records, b"orphan", b"");
            }
            1 => {
                batch.insert(&writer.meta, CATALOG_METADATA_KEY, b"");
                batch.insert(&writer.meta, CATALOG_ARTIFACT_KEY, b"");
            }
            _ => {
                batch.insert(&writer.meta, CATALOG_METADATA_KEY, b"opaque");
                batch.insert(&writer.meta, CATALOG_ARTIFACT_KEY, b"bytes");
            }
        }
        batch.commit()?;
        assert!(matches!(
            writer.catalog_reader().capture_complete_state(),
            Err(CatalogReadError::Storage(
                StorageError::CorruptMetadata { .. }
            ))
        ));
    }
    Ok(())
}

#[test]
fn fjall_closed_headers_and_markers_are_refused() -> TestResult {
    for shape in 0..15 {
        let (_directory, writer) = fixture()?;
        match shape {
            0 => edit(&writer, &[(FORMAT_VERSION_KEY, None)])?,
            1 => edit(&writer, &[(FORMAT_VERSION_KEY, Some(&[0]))])?,
            2 => edit(
                &writer,
                &[(
                    FORMAT_VERSION_KEY,
                    Some(&crate::ACTIVE_STORE_FORMAT.to_be_bytes()),
                )],
            )?,
            3 => edit(&writer, &[(PROFILE_KEY, None)])?,
            4 => edit(&writer, &[(PROFILE_KEY, Some(b"x"))])?,
            5 => edit(&writer, &[(PROFILE_KEY, Some(&vec![b'x'; PROFILE.len()]))])?,
            6 => edit(&writer, &[(INITIALIZED_KEY, None)])?,
            7 => edit(&writer, &[(INITIALIZED_KEY, Some(b""))])?,
            8 => edit(&writer, &[(INITIALIZED_KEY, Some(&[0, 0]))])?,
            9 => edit(&writer, &[(INITIALIZED_KEY, Some(&[2]))])?,
            10 => edit(&writer, &[(b"unknown", Some(b"opaque"))])?,
            11 => edit(
                &writer,
                &[
                    (INITIALIZED_KEY, Some(&[1])),
                    (CATALOG_METADATA_KEY, Some(b"")),
                    (CATALOG_ARTIFACT_KEY, Some(b"")),
                    (b"zz-sixth", Some(b"")),
                ],
            )?,
            12 => edit(&writer, &[(&[0x22, 0x01], Some(b""))])?,
            13 => edit(&writer, &[(&[0x22, 0x02], Some(b"malformed"))])?,
            _ => edit(&writer, &[(&[0x22, 0x03], Some(&[1]))])?,
        }
        let before = writer.store.database.snapshot();
        let result = writer.catalog_reader().capture_complete_state();
        let after = writer.store.database.snapshot();
        let measure = |snapshot: &fjall::Snapshot| -> Result<Vec<_>, fjall::Error> {
            snapshot
                .iter(&writer.meta)
                .map(fjall::Guard::into_inner)
                .collect()
        };
        let old = measure(&before)?;
        let new = measure(&after)?;
        assert!(matches!(result, Err(CatalogReadError::Storage(_))));
        assert_eq!(old, new);
    }
    Ok(())
}

#[test]
fn fjall_each_half_catalog_is_refused() -> TestResult {
    for key in [CATALOG_METADATA_KEY, CATALOG_ARTIFACT_KEY] {
        for value in [b"".as_slice(), b"opaque".as_slice()] {
            let (_directory, writer) = fixture()?;
            edit(
                &writer,
                &[(INITIALIZED_KEY, Some(&[1])), (key, Some(value))],
            )?;
            assert!(matches!(
                writer.catalog_reader().capture_complete_state(),
                Err(CatalogReadError::Storage(
                    StorageError::CorruptMetadata { .. }
                ))
            ));
        }
    }
    Ok(())
}

#[test]
fn fjall_record_shape_limits_before_result_copy() -> TestResult {
    for shape in 0..3 {
        let (_directory, writer) = fixture()?;
        let mut batch = writer
            .store
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        batch.insert(&writer.meta, INITIALIZED_KEY, vec![1]);
        match shape {
            0 => {
                batch.insert(&writer.store.records, vec![b'k'; 1025], b"");
            }
            1 => {
                batch.insert(&writer.store.records, b"key", vec![0; 266_241]);
            }
            _ => {
                for key in 0..=65_536u32 {
                    batch.insert(&writer.store.records, key.to_be_bytes().to_vec(), b"");
                }
            }
        }
        batch.commit()?;
        assert!(matches!(
            writer.catalog_reader().capture_complete_state(),
            Err(CatalogReadError::LimitExceeded)
        ));
    }
    Ok(())
}

#[test]
fn fjall_catalog_caps_before_any_result_copy() -> TestResult {
    let (_directory, writer) = fixture()?;
    let metadata = vec![0; 8193];
    edit(
        &writer,
        &[
            (INITIALIZED_KEY, Some(&[1])),
            (CATALOG_METADATA_KEY, Some(&metadata)),
            (CATALOG_ARTIFACT_KEY, Some(b"")),
        ],
    )?;
    let mut batch = writer
        .store
        .database
        .batch()
        .durability(Some(PersistMode::SyncAll));
    batch.insert(&writer.store.records, b"also-overlong", vec![0; 266_241]);
    batch.commit()?;
    assert!(matches!(
        writer.catalog_reader().capture_complete_state(),
        Err(CatalogReadError::LimitExceeded)
    ));
    let oversized = vec![0; 67_108_865];
    edit(
        &writer,
        &[
            (CATALOG_METADATA_KEY, Some(b"")),
            (CATALOG_ARTIFACT_KEY, Some(&oversized)),
        ],
    )?;
    assert!(matches!(
        writer.catalog_reader().capture_complete_state(),
        Err(CatalogReadError::LimitExceeded)
    ));
    Ok(())
}

#[test]
fn fjall_pinned_complete_old_view_never_mixes_live_controls() -> TestResult {
    let (_directory, mut writer) = fixture()?;
    catalog(
        &mut writer,
        WriteBatch::default().put(b"business", b"old"),
        b"old-meta",
        b"old-image",
    )?;
    let reader = writer.catalog_reader();
    let pinned = writer.store.database.snapshot();
    catalog(
        &mut writer,
        WriteBatch::default().put(b"business", b"new"),
        b"new-meta",
        b"new-image",
    )?;
    edit(&writer, &[(INITIALIZED_KEY, Some(&[0]))])?;
    let old = capture_view(&reader, &pinned)?;
    let current = reader.capture_complete_state();
    assert!(old.is_initialized());
    assert_eq!(
        old.records().entries(),
        &[(b"business".to_vec(), b"old".to_vec())]
    );
    let pair = old.catalog().ok_or("missing pinned pair")?;
    assert_eq!(
        (pair.metadata(), pair.artifact()),
        (b"old-meta".as_slice(), b"old-image".as_slice())
    );
    assert_eq!(old.logical_payload_bytes(), 8 + 3 + 8 + 9);
    assert!(matches!(
        current,
        Err(CatalogReadError::Storage(
            StorageError::CorruptMetadata { .. }
        ))
    ));
    Ok(())
}
