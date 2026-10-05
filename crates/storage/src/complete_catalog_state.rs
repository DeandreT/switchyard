//! Complete opaque legacy catalog-replica reads, not publication authority.

use std::fmt;

use crate::{
    CatalogReadError, ReadBudget, ReadLimits, StorageError, StoreSnapshot, StoredSnapshotCatalog,
};

/// Fixed logical limits for all returned business key/value bytes.
///
/// Zero-length keys and values are allowed by this capture policy. This does
/// not promise that every backend can represent an empty key. Collection and
/// backend allocations, spare capacity, retained history and RSS are excluded.
pub const COMPLETE_CATALOG_STATE_RECORD_LIMITS: ReadLimits = ReadLimits {
    max_rows: 65_536,
    max_key_bytes: 1024,
    max_value_bytes: 266_240,
    max_total_bytes: 64 * 1024 * 1024,
};

/// Business payload plus both opaque catalog components, not encoded image size.
pub const MAX_COMPLETE_CATALOG_STATE_BYTES: usize = COMPLETE_CATALOG_STATE_RECORD_LIMITS
    .max_total_bytes
    + crate::MAX_CATALOG_METADATA_BYTES
    + crate::MAX_CATALOG_ARTIFACT_BYTES;

/// An owned complete view of one legacy catalog-replica state.
///
/// No backend handle, source borrow, mutable buffer, selection fence, writer or
/// commitment/provenance receipt is retained. The originating handles may go
/// away while this value remains usable. Explicit records are readable; only
/// this wrapper's Debug is opaque. Existing low-level error causes are not
/// sanitized external diagnostics.
///
/// ```compile_fail
/// fn duplicate(state: storage::StoredCatalogReplicaState) {
///     let _ = state.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct(records: storage::StoreSnapshot) {
///     let _ = storage::StoredCatalogReplicaState {
///         initialized: false, records, catalog: None, logical_payload_bytes: 0,
///     };
/// }
/// ```
///
/// ```compile_fail
/// fn mutate(state: &mut storage::StoredCatalogReplicaState) {
///     state.records().entries().push((Vec::new(), Vec::new()));
/// }
/// ```
///
/// ```compile_fail
/// use storage::{CatalogCommittedStore, StateStore};
/// fn mutate_reader(writer: &storage::MemoryCatalogReplicaStore) {
///     writer.catalog_reader().apply(storage::WriteBatch::default());
/// }
/// ```
///
/// ```compile_fail
/// fn batch(state: storage::StoredCatalogReplicaState) -> storage::WriteBatch {
///     state.into()
/// }
/// ```
///
/// ```compile_fail
/// fn commit(state: storage::StoredCatalogReplicaState) {
///     let _ = state.commit();
/// }
/// ```
pub struct StoredCatalogReplicaState {
    initialized: bool,
    records: StoreSnapshot,
    catalog: Option<StoredSnapshotCatalog>,
    logical_payload_bytes: usize,
}

impl StoredCatalogReplicaState {
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn records(&self) -> &StoreSnapshot {
        &self.records
    }

    pub fn catalog(&self) -> Option<&StoredSnapshotCatalog> {
        self.catalog.as_ref()
    }

    pub fn logical_payload_bytes(&self) -> usize {
        self.logical_payload_bytes
    }

    pub(crate) fn from_parts(
        initialized: bool,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        catalog: Option<StoredSnapshotCatalog>,
        logical_payload_bytes: usize,
    ) -> Self {
        Self {
            initialized,
            records: StoreSnapshot { entries },
            catalog,
            logical_payload_bytes,
        }
    }
}

impl fmt::Debug for StoredCatalogReplicaState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredCatalogReplicaState")
            .field("initialized", &self.initialized)
            .field("record_count", &self.records.entries().len())
            .field("catalog_present", &self.catalog.is_some())
            .field("logical_payload_bytes", &self.logical_payload_bytes)
            .finish_non_exhaustive()
    }
}

/// One complete bounded legacy state from one originating backend view.
///
/// Implementations validate all controls, catalog presence and limits, record
/// shape/order and the combined logical limit before any caller-result copy.
/// There is no default implementation composing public catalog/business reads
/// or allocating an ordinary snapshot. This trusted storage contract supplies
/// no source authenticity, read/commit CAS, paired role or adoption authority.
/// Catalogs may be older than business rows. Absence of a legacy fence is not
/// a selection identity. Capture opens no database or keyspace and mutates none.
/// Reader clones can keep an already-open database alive; output bytes cannot.
///
/// ```no_run
/// use storage::{CatalogCommittedStore, CommittedStore, CompleteCatalogReplicaStateReader};
/// fn capture() -> Result<storage::StoredCatalogReplicaState, storage::CatalogReadError> {
///     let state = {
///         let mut writer = storage::MemoryCatalogReplicaStore::new();
///         writer.commit(storage::WriteBatch::default())?;
///         let reader = writer.catalog_reader();
///         let another = reader.clone();
///         another.capture_complete_state()?
///     };
///     let _: bool = state.is_initialized();
///     let _: &storage::StoreSnapshot = state.records();
///     let _: Option<&storage::StoredSnapshotCatalog> = state.catalog();
///     let _: usize = state.logical_payload_bytes();
///     let _: storage::ReadLimits = storage::COMPLETE_CATALOG_STATE_RECORD_LIMITS;
///     let _: usize = storage::MAX_COMPLETE_CATALOG_STATE_BYTES;
///     Ok(state)
/// }
/// ```
pub trait CompleteCatalogReplicaStateReader: Clone + Send + Sync + 'static {
    fn capture_complete_state(&self) -> Result<StoredCatalogReplicaState, CatalogReadError>;
}

pub(crate) struct RecordShape {
    budget: ReadBudget,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
}

impl RecordShape {
    pub(crate) fn new() -> Self {
        Self {
            budget: ReadBudget::new(COMPLETE_CATALOG_STATE_RECORD_LIMITS),
            rows: 0,
            bytes: 0,
        }
    }

    pub(crate) fn check_next_row(&self) -> Result<(), CatalogReadError> {
        self.budget.check_next_row().map_err(Into::into)
    }

    pub(crate) fn consume(&mut self, key: usize, value: usize) -> Result<(), CatalogReadError> {
        self.budget.consume(key, value)?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or(CatalogReadError::LimitExceeded)?;
        self.bytes = key
            .checked_add(value)
            .and_then(|bytes| self.bytes.checked_add(bytes))
            .ok_or(CatalogReadError::LimitExceeded)?;
        Ok(())
    }
}

pub(crate) fn payload_bytes(
    records: usize,
    catalog: Option<(usize, usize)>,
) -> Result<usize, CatalogReadError> {
    if records > COMPLETE_CATALOG_STATE_RECORD_LIMITS.max_total_bytes {
        return Err(CatalogReadError::LimitExceeded);
    }
    let (metadata, artifact) = catalog.unwrap_or((0, 0));
    crate::catalog::check_bounds(metadata, artifact)
        .map_err(|_| CatalogReadError::LimitExceeded)?;
    records
        .checked_add(metadata)
        .and_then(|bytes| bytes.checked_add(artifact))
        .filter(|bytes| *bytes <= MAX_COMPLETE_CATALOG_STATE_BYTES)
        .ok_or(CatalogReadError::LimitExceeded)
}

type RecordEntries = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) fn reserve_rows(rows: usize) -> Result<RecordEntries, CatalogReadError> {
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(rows)
        .map_err(|_| CatalogReadError::Allocation)?;
    Ok(entries)
}

pub(crate) fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>, CatalogReadError> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(bytes.len())
        .map_err(|_| CatalogReadError::Allocation)?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

pub(crate) fn inconsistent() -> CatalogReadError {
    StorageError::CorruptMetadata {
        detail: "complete catalog state differs within its stable view".into(),
    }
    .into()
}

#[cfg(test)]
mod tests;
