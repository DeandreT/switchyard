use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use storage::{CommittedStore, MAX_CATALOG_METADATA_BYTES, MemoryCatalogReplicaStore};

use super::*;

#[test]
fn export_errors_keep_their_static_boundary() {
    for (input, expected) in [
        (
            CommittedImageExportError::Poisoned,
            CommittedCatalogError::Poisoned,
        ),
        (
            CommittedImageExportError::ReadFailed,
            CommittedCatalogError::ReadFailed,
        ),
        (
            CommittedImageExportError::LimitExceeded,
            CommittedCatalogError::LimitExceeded,
        ),
        (
            CommittedImageExportError::Allocation,
            CommittedCatalogError::Allocation,
        ),
        (
            CommittedImageExportError::UnsupportedProfile,
            CommittedCatalogError::UnsupportedProfile,
        ),
        (
            CommittedImageExportError::InvalidImage,
            CommittedCatalogError::InvalidImage,
        ),
    ] {
        assert_eq!(export_error(input), expected);
    }
}

#[test]
fn low_level_catalog_errors_are_mapped_without_private_causes() {
    assert_eq!(
        catalog_read_error(CatalogReadError::LimitExceeded),
        CommittedCatalogError::LimitExceeded
    );
    assert_eq!(
        catalog_read_error(CatalogReadError::Storage(StorageError::ReadLimitExceeded)),
        CommittedCatalogError::LimitExceeded
    );
    assert_eq!(
        catalog_read_error(CatalogReadError::Allocation),
        CommittedCatalogError::Allocation
    );
    for error in [
        StorageError::LockPoisoned,
        StorageError::ReplicaWriteRequired,
        StorageError::ReplicaMetadataInStandalone,
        StorageError::Backend {
            operation: "private-address",
            detail: "private-body-token".into(),
        },
        StorageError::UnsupportedStoreFormat {
            found: u32::MAX,
            expected: 42,
        },
        StorageError::CorruptMetadata {
            detail: "private-header-token".into(),
        },
    ] {
        let mapped = catalog_read_error(CatalogReadError::Storage(error));
        assert_eq!(mapped, CommittedCatalogError::ReadFailed);
        assert!(mapped.source().is_none());
        let diagnostic = format!("{mapped:?}: {mapped}");
        assert!(!diagnostic.contains("private-"));
        assert!(!diagnostic.contains("4294967295"));
    }
}

#[test]
fn container_and_business_refusals_are_static_not_source_certificates() {
    for (input, expected) in [
        (
            CommittedImageError::UnsupportedFormat,
            CommittedCatalogError::UnsupportedProfile,
        ),
        (
            CommittedImageError::Malformed,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::LimitExceeded,
            CommittedCatalogError::LimitExceeded,
        ),
        (
            CommittedImageError::InvalidStream,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::InvalidRows,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::InvalidCheckpoint,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::CheckpointStreamMismatch,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::ChecksumMismatch,
            CommittedCatalogError::InvalidImage,
        ),
        (
            CommittedImageError::Allocation,
            CommittedCatalogError::Allocation,
        ),
    ] {
        assert_eq!(container_error(input), expected);
    }
    for error in [
        CommittedImageValidationError::InvalidKey,
        CommittedImageValidationError::InvalidRecord,
        CommittedImageValidationError::InconsistentMetadata,
        CommittedImageValidationError::InconsistentMessage,
        CommittedImageValidationError::InconsistentIndex,
        CommittedImageValidationError::InconsistentHistory,
        CommittedImageValidationError::InvalidClock,
    ] {
        assert_eq!(validation_error(error), CommittedCatalogError::InvalidImage);
    }
    assert_eq!(
        validation_error(CommittedImageValidationError::UnsupportedProfile),
        CommittedCatalogError::UnsupportedProfile
    );
}

#[test]
fn public_diagnostics_are_static_and_have_no_source_chain() {
    for error in [
        CommittedCatalogError::Poisoned,
        CommittedCatalogError::ReadFailed,
        CommittedCatalogError::LimitExceeded,
        CommittedCatalogError::Allocation,
        CommittedCatalogError::UnsupportedProfile,
        CommittedCatalogError::InvalidImage,
        CommittedCatalogError::WrongStream,
        CommittedCatalogError::CommitUnknown,
    ] {
        assert!(error.source().is_none());
        let diagnostic = format!("{error:?}: {error}");
        assert!(!diagnostic.contains("private-body-token"));
        assert!(!diagnostic.contains("private-address"));
    }
}

struct CountedWriter {
    inner: MemoryCatalogReplicaStore,
    catalog_commits: Arc<AtomicUsize>,
}

impl CommittedStore for CountedWriter {
    type Reader = <MemoryCatalogReplicaStore as CommittedStore>::Reader;

    fn reader(&self) -> Self::Reader {
        self.inner.reader()
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.inner.is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.commit(batch)
    }
}

impl CatalogCommittedStore for CountedWriter {
    type CatalogReader = <MemoryCatalogReplicaStore as CatalogCommittedStore>::CatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader {
        self.inner.catalog_reader()
    }

    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError> {
        self.catalog_commits.fetch_add(1, Ordering::SeqCst);
        self.inner.commit_with_catalog(batch, catalog)
    }
}

#[test]
fn consuming_a_privately_poisoned_token_refuses_before_commit_even_if_metadata_is_overlong() {
    for metadata in [Vec::new(), vec![0; MAX_CATALOG_METADATA_BYTES + 1]] {
        let catalog_commits = Arc::new(AtomicUsize::new(0));
        let writer = CountedWriter {
            inner: MemoryCatalogReplicaStore::new(),
            catalog_commits: Arc::clone(&catalog_commits),
        };
        let catalog_reader = writer.catalog_reader();
        let mut machine =
            CommittedStateMachine::create(writer, crate::CommittedStreamId::new([7; 16]).unwrap())
                .unwrap();
        let token = machine.prepare_create_send_catalog().unwrap();
        // Public code cannot poison or apply through this exclusive token borrow.
        token.machine.poisoned = true;
        assert_eq!(
            token.retain(&metadata).unwrap_err(),
            CommittedCatalogError::Poisoned
        );
        assert_eq!(catalog_commits.load(Ordering::SeqCst), 0);
        assert!(catalog_reader.read_catalog().unwrap().is_none());
    }
}
