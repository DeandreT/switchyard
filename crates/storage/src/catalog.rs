//! Opt-in, opaque snapshot catalog storage, not snapshot validation or adoption.

use std::fmt;

use crate::{CommittedStore, StorageError, WriteBatch};

pub const MAX_CATALOG_METADATA_BYTES: usize = 8 * 1024;
pub const MAX_CATALOG_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

/// A static refusal before any catalog input is copied or written.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("snapshot catalog component exceeds its byte limit")]
pub struct CatalogBoundsError;

/// Borrowed, immutable, individually bounded catalog components.
///
/// Empty components are legal opaque values; an absent slot is different from
/// a present slot containing empty values. These bytes are not validated as an
/// image, native metadata, committed state, or evidence of source provenance.
pub struct SnapshotCatalogRecord<'a> {
    metadata: &'a [u8],
    artifact: &'a [u8],
}

impl<'a> SnapshotCatalogRecord<'a> {
    pub fn new(metadata: &'a [u8], artifact: &'a [u8]) -> Result<Self, CatalogBoundsError> {
        check_bounds(metadata.len(), artifact.len())?;
        Ok(Self { metadata, artifact })
    }

    pub fn metadata(&self) -> &[u8] {
        self.metadata
    }

    pub fn artifact(&self) -> &[u8] {
        self.artifact
    }
}

impl fmt::Debug for SnapshotCatalogRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotCatalogRecord")
            .field("metadata_bytes", &self.metadata.len())
            .field("artifact_bytes", &self.artifact.len())
            .finish_non_exhaustive()
    }
}

/// An immutable owned copy of one atomically captured catalog slot.
///
/// This value holds no backend handle and grants no mutation, installation,
/// authenticity, ancestry, or log-purge authority. Its two logical byte bounds
/// exclude spare capacity, backend materialization/staging, and process memory.
///
/// ```compile_fail
/// fn duplicate(value: storage::StoredSnapshotCatalog) {
///     let another = value.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn mutate(value: &mut storage::StoredSnapshotCatalog) {
///     value.artifact().push(0);
/// }
/// ```
pub struct StoredSnapshotCatalog {
    metadata: Vec<u8>,
    artifact: Vec<u8>,
}

impl StoredSnapshotCatalog {
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    pub fn artifact(&self) -> &[u8] {
        &self.artifact
    }

    pub(crate) fn copy_from_parts(
        metadata: &[u8],
        artifact: &[u8],
    ) -> Result<Self, CatalogReadError> {
        // Check both components before the first caller-owned copy.
        check_bounds(metadata.len(), artifact.len())
            .map_err(|_| CatalogReadError::LimitExceeded)?;
        Ok(Self {
            metadata: copy_bytes(metadata)?,
            artifact: copy_bytes(artifact)?,
        })
    }
}

impl fmt::Debug for StoredSnapshotCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredSnapshotCatalog")
            .field("metadata_bytes", &self.metadata.len())
            .field("artifact_bytes", &self.artifact.len())
            .finish_non_exhaustive()
    }
}

/// Low-level catalog read errors, not sanitized external diagnostics.
///
/// The storage cause may contain backend details. Adapters must map these
/// causes before publishing external diagnostics. Explicit result copies use
/// fallible reservations; this does not make every backend allocation fallible.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CatalogReadError {
    #[error("snapshot catalog storage failed: {0}")]
    Storage(#[source] StorageError),
    #[error("snapshot catalog component exceeds its byte limit")]
    LimitExceeded,
    #[error("snapshot catalog result could not be allocated")]
    Allocation,
}

impl From<StorageError> for CatalogReadError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::ReadLimitExceeded => Self::LimitExceeded,
            error => Self::Storage(error),
        }
    }
}

/// A matching read-only capability for one complete catalog slot.
///
/// Each read validates its backend profile and initialization and captures both
/// components in one stable view. No partial slot or unbounded fallback is
/// returned. Cloning never creates a writer, but may keep a database open.
pub trait SnapshotCatalogReader: Clone + Send + Sync + 'static {
    fn read_catalog(&self) -> Result<Option<StoredSnapshotCatalog>, CatalogReadError>;
}

/// A trusted unique writer for an explicitly opted-in replica catalog profile.
///
/// The catalog reader observes exactly this writer's catalog. Business readers
/// and their snapshots contain only caller records, never catalog metadata.
/// This opted-in contract also requires [`CommittedStore::is_initialized`] to
/// validate the profile and initialization invariants: an uninitialized store
/// contains neither business records nor a catalog. Malformed profile/init or
/// uninitialized state containing either must return an error, not Ok(false).
/// This requirement is specific to catalog writers and does not broaden the
/// ordinary CommittedStore contract or provide a read/commit compare-and-swap.
/// The privileged batch, initialized flag, and both supplied catalog components
/// must be committed atomically. Ordinary [`CommittedStore::commit`] preserves
/// the catalog slot unchanged. A retained catalog may legitimately be older
/// than the latest business records; this contract does not interpret or compare
/// their checkpoints, and has no compare-and-swap across preparation reads.
///
/// A physical commit error does not prove the batch is absent, including an
/// error after the complete durable commit. Do not publish success effects,
/// assume rollback, or automatically retry. Backend writes may use ordinary
/// allocations; this contract is not an all-writes-fallible or OOM-safety claim.
/// This capability supplies no consensus, validation, or installation authority.
pub trait CatalogCommittedStore: CommittedStore {
    type CatalogReader: SnapshotCatalogReader;

    fn catalog_reader(&self) -> Self::CatalogReader;
    fn commit_with_catalog(
        &mut self,
        batch: WriteBatch,
        catalog: SnapshotCatalogRecord<'_>,
    ) -> Result<(), StorageError>;
}

pub(crate) fn check_bounds(metadata: usize, artifact: usize) -> Result<(), CatalogBoundsError> {
    if metadata > MAX_CATALOG_METADATA_BYTES || artifact > MAX_CATALOG_ARTIFACT_BYTES {
        Err(CatalogBoundsError)
    } else {
        Ok(())
    }
}

fn copy_bytes(source: &[u8]) -> Result<Vec<u8>, CatalogReadError> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(source.len())
        .map_err(|_| CatalogReadError::Allocation)?;
    value.extend_from_slice(source);
    Ok(value)
}

#[cfg(test)]
mod tests;
