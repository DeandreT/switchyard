use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct CapturedView {
    initialized: bool,
    entries: BTreeMap<Key, Value>,
    catalog: Option<(Vec<u8>, Vec<u8>)>,
}

fn capture(writer: &MemoryCatalogReplicaStore) -> Result<CapturedView, StorageError> {
    let state = writer
        .state
        .read()
        .map_err(|_| StorageError::LockPoisoned)?;
    Ok(CapturedView {
        initialized: state.initialized,
        entries: state.entries.clone(),
        catalog: state.catalog.clone(),
    })
}

#[test]
fn captured_old_and_new_views_pair_business_init_and_catalog_under_one_lock() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
    let before = capture(&writer)?;
    writer.commit_with_catalog(
        WriteBatch::default().put(b"business", b"first"),
        SnapshotCatalogRecord::new(b"first-meta", b"first-artifact")?,
    )?;
    let first = capture(&writer)?;
    writer.commit_with_catalog(
        WriteBatch::default().put(b"business", b"second"),
        SnapshotCatalogRecord::new(b"second-meta", b"second-artifact")?,
    )?;
    let second = capture(&writer)?;
    assert!(!before.initialized);
    assert!(before.entries.is_empty());
    assert!(before.catalog.is_none());
    assert!(first.initialized);
    assert_eq!(
        first.entries.get(b"business".as_slice()),
        Some(&b"first".to_vec())
    );
    assert_eq!(
        first.catalog,
        Some((b"first-meta".to_vec(), b"first-artifact".to_vec()))
    );
    assert!(second.initialized);
    assert_eq!(
        second.entries.get(b"business".as_slice()),
        Some(&b"second".to_vec())
    );
    assert_eq!(
        second.catalog,
        Some((b"second-meta".to_vec(), b"second-artifact".to_vec()))
    );
    Ok(())
}

#[test]
fn a_held_catalog_read_view_prevents_any_business_or_catalog_publication() -> TestResult {
    let mut writer = MemoryCatalogReplicaStore::new();
    let reader = writer.catalog_reader();
    let shared = Arc::clone(&reader.state);
    std::thread::scope(|scope| -> TestResult {
        let guard = shared.read().map_err(|_| StorageError::LockPoisoned)?;
        let (started, receiving) = std::sync::mpsc::channel();
        let thread = scope.spawn(move || -> Result<_, StorageError> {
            started.send(()).map_err(|_| StorageError::LockPoisoned)?;
            writer.commit_with_catalog(
                WriteBatch::default().put(b"business", b"committed"),
                SnapshotCatalogRecord::new(b"meta", b"artifact").expect("small bounded inputs"),
            )?;
            Ok(writer)
        });
        receiving.recv()?;
        assert!(!guard.initialized);
        assert!(guard.entries.is_empty());
        assert!(guard.catalog.is_none());
        drop(guard);
        let writer = thread
            .join()
            .map_err(|_| "memory catalog commit thread panicked")??;
        assert!(writer.is_initialized()?);
        assert_eq!(
            writer.reader().get(b"business")?,
            Some(b"committed".to_vec())
        );
        let catalog = reader.read_catalog()?.ok_or("missing committed catalog")?;
        assert_eq!(catalog.metadata(), b"meta");
        assert_eq!(catalog.artifact(), b"artifact");
        Ok(())
    })
}

#[test]
fn uninitialized_state_cannot_hide_business_records_or_a_catalog() -> TestResult {
    for catalog in [false, true] {
        let mut writer = MemoryCatalogReplicaStore::new();
        {
            let mut state = writer
                .state
                .write()
                .map_err(|_| StorageError::LockPoisoned)?;
            if catalog {
                state.catalog = Some((Vec::new(), Vec::new()));
            } else {
                state
                    .entries
                    .insert(b"business".to_vec(), b"unexpected".to_vec());
            }
        }
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
        let before = capture(&writer)?;
        assert!(matches!(
            writer.commit(WriteBatch::default().put(b"business", b"forbidden")),
            Err(StorageError::CorruptMetadata { .. })
        ));
        assert_eq!(capture(&writer)?.entries, before.entries);
    }
    Ok(())
}

#[test]
fn oversized_stored_components_are_refused_before_result_copy_and_block_writes() -> TestResult {
    for artifact in [false, true] {
        let mut writer = MemoryCatalogReplicaStore::new();
        writer.commit(WriteBatch::default().put(b"business", b"original"))?;
        {
            let mut state = writer
                .state
                .write()
                .map_err(|_| StorageError::LockPoisoned)?;
            state.catalog = Some(if artifact {
                (Vec::new(), vec![0; crate::MAX_CATALOG_ARTIFACT_BYTES + 1])
            } else {
                (vec![0; crate::MAX_CATALOG_METADATA_BYTES + 1], Vec::new())
            });
        }
        assert_eq!(
            writer.catalog_reader().read_catalog().unwrap_err(),
            CatalogReadError::LimitExceeded
        );
        assert_eq!(
            writer.is_initialized(),
            Err(StorageError::ReadLimitExceeded)
        );
        assert_eq!(
            writer.commit(WriteBatch::default().put(b"business", b"forbidden")),
            Err(StorageError::ReadLimitExceeded)
        );
        assert_eq!(
            writer.reader().get(b"business")?,
            Some(b"original".to_vec())
        );
    }
    Ok(())
}
