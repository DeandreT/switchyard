use super::*;

#[test]
fn fixed_bounds_are_independent_inclusive_and_allow_empty_values() {
    assert_eq!(MAX_CATALOG_METADATA_BYTES, 8 * 1024);
    assert_eq!(MAX_CATALOG_ARTIFACT_BYTES, 64 * 1024 * 1024);
    assert!(check_bounds(0, 0).is_ok());
    assert!(check_bounds(MAX_CATALOG_METADATA_BYTES, MAX_CATALOG_ARTIFACT_BYTES).is_ok());
    assert_eq!(
        check_bounds(MAX_CATALOG_METADATA_BYTES + 1, 0),
        Err(CatalogBoundsError)
    );
    assert_eq!(
        check_bounds(0, MAX_CATALOG_ARTIFACT_BYTES + 1),
        Err(CatalogBoundsError)
    );
}

#[test]
fn borrowed_constructor_checks_actual_component_lengths_without_copying() {
    let metadata = vec![7; MAX_CATALOG_METADATA_BYTES + 1];
    assert_eq!(
        SnapshotCatalogRecord::new(&metadata, &[]).unwrap_err(),
        CatalogBoundsError
    );
    let artifact = vec![9; MAX_CATALOG_ARTIFACT_BYTES + 1];
    assert_eq!(
        SnapshotCatalogRecord::new(&[], &artifact).unwrap_err(),
        CatalogBoundsError
    );
    let record = SnapshotCatalogRecord::new(
        &metadata[..MAX_CATALOG_METADATA_BYTES],
        &artifact[..MAX_CATALOG_ARTIFACT_BYTES],
    )
    .unwrap();
    assert_eq!(record.metadata().as_ptr(), metadata.as_ptr());
    assert_eq!(record.artifact().as_ptr(), artifact.as_ptr());
}

#[test]
fn explicit_owned_copies_are_independent_and_preserve_empty_values() {
    let mut metadata = b"opaque metadata".to_vec();
    let mut artifact = b"opaque artifact".to_vec();
    let stored = StoredSnapshotCatalog::copy_from_parts(&metadata, &artifact).unwrap();
    metadata.fill(0);
    artifact.fill(0);
    assert_eq!(stored.metadata(), b"opaque metadata");
    assert_eq!(stored.artifact(), b"opaque artifact");
    let empty = StoredSnapshotCatalog::copy_from_parts(&[], &[]).unwrap();
    assert!(empty.metadata().is_empty());
    assert!(empty.artifact().is_empty());
    assert_eq!(
        StoredSnapshotCatalog::copy_from_parts(&vec![0; MAX_CATALOG_METADATA_BYTES + 1], &[])
            .unwrap_err(),
        CatalogReadError::LimitExceeded
    );
}

#[test]
fn borrowed_and_owned_debug_show_lengths_but_never_component_content() {
    let metadata = b"private-metadata-content";
    let artifact = b"private-artifact-content";
    let record = SnapshotCatalogRecord::new(metadata, artifact).unwrap();
    let stored = StoredSnapshotCatalog::copy_from_parts(metadata, artifact).unwrap();
    for rendered in [format!("{record:?}"), format!("{stored:?}")] {
        assert!(rendered.contains("metadata_bytes"));
        assert!(rendered.contains("artifact_bytes"));
        assert!(!rendered.contains("private-metadata-content"));
        assert!(!rendered.contains("private-artifact-content"));
        assert!(!rendered.contains(&format!("{metadata:?}")));
        assert!(!rendered.contains(&format!("{artifact:?}")));
    }
}

#[test]
fn low_level_errors_preserve_storage_causes_without_a_privacy_promise() {
    let backend = StorageError::Backend {
        operation: "read a test component",
        detail: "backend-specific-detail".into(),
    };
    let error = CatalogReadError::from(backend.clone());
    assert_eq!(error, CatalogReadError::Storage(backend));
    assert!(error.to_string().contains("backend-specific-detail"));
    assert_eq!(
        CatalogReadError::from(StorageError::ReadLimitExceeded),
        CatalogReadError::LimitExceeded
    );
    assert_eq!(
        CatalogReadError::from(StorageError::LockPoisoned),
        CatalogReadError::Storage(StorageError::LockPoisoned)
    );
}
