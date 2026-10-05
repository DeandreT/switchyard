use std::time::Duration;

use super::*;
use crate::{
    CatalogCommittedStore, CommittedStore, MemoryCatalogReplicaStore, SnapshotCatalogRecord,
    StorageError, WriteBatch,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn catalog(
    writer: &mut MemoryCatalogReplicaStore,
    batch: WriteBatch,
    meta: &[u8],
    image: &[u8],
) -> TestResult {
    writer.commit_with_catalog(batch, SnapshotCatalogRecord::new(meta, image)?)?;
    Ok(())
}

#[test]
fn memory_pristine_complete_state() -> TestResult {
    let writer = MemoryCatalogReplicaStore::new();
    let state = writer.catalog_reader().capture_complete_state()?;
    assert!(!state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(state.catalog().is_none());
    assert_eq!(state.logical_payload_bytes(), 0);
    Ok(())
}

#[test]
fn memory_initialized_empty_without_catalog() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
    writer.commit(WriteBatch::default())?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert!(state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(state.catalog().is_none());
    assert_eq!(state.logical_payload_bytes(), 0);
    Ok(())
}

#[test]
fn memory_present_empty_catalog_is_not_absence() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
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
fn memory_complete_opaque_records_and_catalog() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
    catalog(
        &mut writer,
        WriteBatch::default()
            .put(b"z-secret-key", b"secret-value")
            .put(b"", b"")
            .put(b"a", b""),
        b"secret-meta",
        b"secret-image",
    )?;
    let state = writer.catalog_reader().capture_complete_state()?;
    assert_eq!(
        state.records().entries(),
        &[
            (Vec::new(), Vec::new()),
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
    assert!(debug.contains("record_count: 3"));
    Ok(())
}

#[test]
fn memory_older_catalog_coexists_with_new_business() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
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
fn memory_owned_capture_outlives_originating_handles() -> TestResult {
    let state = {
        let mut writer = MemoryCatalogReplicaStore::new();
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
        state
    };
    assert_eq!(
        state.records().entries(),
        &[(b"key".to_vec(), b"value".to_vec())]
    );
    assert_eq!(
        state.catalog().ok_or("missing owned pair")?.artifact(),
        b"image"
    );
    assert_eq!(state.logical_payload_bytes(), 3 + 5 + 4 + 5);
    Ok(())
}

#[test]
fn memory_uninitialized_orphans_are_refused() -> TestResult {
    for shape in 0..3 {
        let writer = MemoryCatalogReplicaStore::new();
        {
            let mut state = writer
                .state
                .write()
                .map_err(|_| StorageError::LockPoisoned)?;
            match shape {
                0 => {
                    state.entries.insert(b"orphan".to_vec(), Vec::new());
                }
                1 => {
                    state.catalog = Some((Vec::new(), Vec::new()));
                }
                _ => {
                    state.catalog = Some((b"opaque".to_vec(), b"bytes".to_vec()));
                }
            }
        }
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
fn memory_record_shape_limits_before_result_copy() -> TestResult {
    for shape in 0..3 {
        let writer = MemoryCatalogReplicaStore::new();
        {
            let mut state = writer
                .state
                .write()
                .map_err(|_| StorageError::LockPoisoned)?;
            state.initialized = true;
            match shape {
                0 => {
                    state.entries.insert(vec![b'k'; 1025], Vec::new());
                }
                1 => {
                    state.entries.insert(b"key".to_vec(), vec![0; 266_241]);
                }
                _ => {
                    for key in 0..=65_536u32 {
                        state.entries.insert(key.to_be_bytes().to_vec(), Vec::new());
                    }
                }
            }
        }
        assert!(matches!(
            writer.catalog_reader().capture_complete_state(),
            Err(CatalogReadError::LimitExceeded)
        ));
    }
    Ok(())
}

#[test]
fn memory_catalog_caps_before_any_result_copy() -> TestResult {
    let writer = MemoryCatalogReplicaStore::new();
    {
        let mut state = writer
            .state
            .write()
            .map_err(|_| StorageError::LockPoisoned)?;
        state.initialized = true;
        state
            .entries
            .insert(b"also-overlong".to_vec(), vec![0; 266_241]);
        state.catalog = Some((vec![0; 8193], Vec::new()));
    }
    assert!(matches!(
        writer.catalog_reader().capture_complete_state(),
        Err(CatalogReadError::LimitExceeded)
    ));
    {
        let mut state = writer
            .state
            .write()
            .map_err(|_| StorageError::LockPoisoned)?;
        state.catalog = Some((Vec::new(), vec![0; 67_108_865]));
    }
    assert!(matches!(
        writer.catalog_reader().capture_complete_state(),
        Err(CatalogReadError::LimitExceeded)
    ));
    Ok(())
}

#[test]
fn memory_one_locked_complete_view_blocks_publication() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
    let reader = writer.catalog_reader();
    let shared = reader.state.clone();
    std::thread::scope(|scope| -> TestResult {
        let guard = shared.read().map_err(|_| StorageError::LockPoisoned)?;
        let old = capture_state(&guard);
        let (started, receiving) = std::sync::mpsc::channel();
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_completed = completed.clone();
        let thread = scope.spawn(move || -> Result<_, StorageError> {
            let send = started.send(());
            let result = writer.commit_with_catalog(
                WriteBatch::default().put(b"key", b"new"),
                SnapshotCatalogRecord::new(b"meta", b"image").expect("small fixed pair"),
            );
            send.map_err(|_| StorageError::LockPoisoned)?;
            result?;
            worker_completed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(writer)
        });
        let observed = receiving.recv_timeout(Duration::from_secs(5));
        let completed_while_held = completed.load(std::sync::atomic::Ordering::SeqCst);
        drop(guard);
        let joined = thread.join();
        let _writer = joined.map_err(|_| "complete memory writer thread panicked")??;
        observed?;
        let old = old?;
        let new = reader.capture_complete_state()?;
        assert!(!completed_while_held);
        assert!(!old.is_initialized());
        assert!(old.records().entries().is_empty());
        assert!(old.catalog().is_none());
        assert!(new.is_initialized());
        assert_eq!(
            new.records().entries(),
            &[(b"key".to_vec(), b"new".to_vec())]
        );
        assert_eq!(
            new.catalog().ok_or("missing published pair")?.artifact(),
            b"image"
        );
        Ok(())
    })
}
