//! Separate Memory publication data, not domain validation or adoption authority.

use std::fmt;

use crate::{
    CatalogReadError, MAX_CATALOG_ARTIFACT_BYTES, MAX_CATALOG_METADATA_BYTES, ReadBudget,
    ReadLimits, SnapshotCatalogRecord, StoreSnapshot, StoredSnapshotCatalog,
};

pub const PROTECTED_STATE_RECORD_LIMITS: ReadLimits = ReadLimits {
    max_rows: 65_536,
    max_key_bytes: 1_024,
    max_value_bytes: 266_240,
    max_total_bytes: 64 * 1024 * 1024,
};
pub const MAX_PROTECTED_STATE_FENCE_BYTES: usize = 256;
pub const MAX_PROTECTED_STATE_BYTES: usize = 134_226_176;

/// Static low-level refusals. Unknown publication is terminal, not rollback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtectedStateError {
    #[error("protected state limit exceeded")]
    LimitExceeded,
    #[error("protected state copy could not be allocated")]
    Allocation,
    #[error("protected state input shape is invalid")]
    InvalidInput,
    #[error("protected state current shape is invalid")]
    InvalidState,
    #[error("protected state fence is unchanged")]
    UnchangedFence,
    #[error("protected state is poisoned")]
    Poisoned,
    #[error("protected state publication outcome is unknown")]
    PublishUnknown,
}

/// Borrowed complete offered data. Shape checks confer no source trust.
///
/// Rows must have strictly ascending nonempty keys. Catalog and fence remain
/// opaque; no image parser, hash, checkpoint or expected-old value is involved.
/// Construction makes no owned copy. Logical limits exclude allocator capacity,
/// collection metadata, backend internals and aggregate process memory.
///
/// ```compile_fail
/// let rows: &[(&[u8], &[u8])] = &[];
/// let pair = storage::SnapshotCatalogRecord::new(b"", b"").unwrap();
/// let checked = storage::ProtectedStatePublication::new(rows, pair, b"f").unwrap();
/// let forged = storage::ProtectedStatePublication { rows, ..checked };
/// ```
pub struct ProtectedStatePublication<'a> {
    pub(crate) rows: &'a [(&'a [u8], &'a [u8])],
    pub(crate) live: SnapshotCatalogRecord<'a>,
    pub(crate) fence: &'a [u8],
    pub(crate) shape: Shape,
}

impl<'a> ProtectedStatePublication<'a> {
    pub fn new(
        rows: &'a [(&'a [u8], &'a [u8])],
        live: SnapshotCatalogRecord<'a>,
        fence: &'a [u8],
    ) -> Result<Self, ProtectedStateError> {
        let shape = check_rows(
            rows.iter().copied(),
            rows.len(),
            live.metadata().len(),
            live.artifact().len(),
            fence.len(),
            ProtectedStateError::InvalidInput,
        )?;
        Ok(Self {
            rows,
            live,
            fence,
            shape,
        })
    }
}

impl fmt::Debug for ProtectedStatePublication<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedStatePublication")
            .field("rows", &self.shape.rows)
            .field("logical_payload_bytes", &self.shape.total)
            .finish_non_exhaustive()
    }
}

/// An owned complete view, without any backend handle or mutation authority.
///
/// Explicit accessors expose ordinary bytes; Debug is numeric only. A view can
/// outlive every originating handle and become stale immediately. It is not a
/// receipt, canonical fence, provenance proof or adoption permission.
///
/// ```compile_fail
/// fn commit(view: storage::StoredProtectedState) {
///     view.commit(storage::WriteBatch::default());
/// }
/// ```
pub struct StoredProtectedState {
    pub(crate) initialized: bool,
    pub(crate) records: StoreSnapshot,
    pub(crate) live: Option<StoredSnapshotCatalog>,
    pub(crate) fence: Option<Vec<u8>>,
    pub(crate) logical_bytes: usize,
}

impl StoredProtectedState {
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
    pub fn records(&self) -> &StoreSnapshot {
        &self.records
    }
    pub fn live_catalog(&self) -> Option<&StoredSnapshotCatalog> {
        self.live.as_ref()
    }
    pub fn fence(&self) -> Option<&[u8]> {
        self.fence.as_deref()
    }
    pub fn logical_payload_bytes(&self) -> usize {
        self.logical_bytes
    }

    pub(crate) fn pristine() -> Self {
        Self {
            initialized: false,
            records: StoreSnapshot::default(),
            live: None,
            fence: None,
            logical_bytes: 0,
        }
    }

    pub(crate) fn check(&self) -> Result<Shape, ProtectedStateError> {
        if !self.initialized {
            if !self.records.entries().is_empty()
                || self.live.is_some()
                || self.fence.is_some()
                || self.logical_bytes != 0
            {
                return Err(ProtectedStateError::InvalidState);
            }
            return Ok(Shape {
                rows: 0,
                business: 0,
                total: 0,
            });
        }
        let (Some(live), Some(fence)) = (&self.live, &self.fence) else {
            return Err(ProtectedStateError::InvalidState);
        };
        let shape = check_rows(
            self.records
                .entries()
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice())),
            self.records.entries().len(),
            live.metadata().len(),
            live.artifact().len(),
            fence.len(),
            ProtectedStateError::InvalidState,
        )?;
        if shape.total != self.logical_bytes {
            return Err(ProtectedStateError::InvalidState);
        }
        Ok(shape)
    }
}

impl fmt::Debug for StoredProtectedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredProtectedState")
            .field("initialized", &self.initialized)
            .field("rows", &self.records.entries().len())
            .field("live_present", &self.live.is_some())
            .field("fence_present", &self.fence.is_some())
            .field("logical_payload_bytes", &self.logical_bytes)
            .finish_non_exhaustive()
    }
}

/// Trusted read-only implementation contract: one complete stable view, no
/// composition of unrelated business/catalog reads or allocating fallback.
/// Implementing this trait is not compiler-certified source provenance.
pub trait ProtectedStateReader: Clone + Send + Sync + 'static {
    fn capture_protected_state(&self) -> Result<StoredProtectedState, ProtectedStateError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Shape {
    pub(crate) rows: usize,
    pub(crate) business: usize,
    pub(crate) total: usize,
}

pub(crate) fn check_components(
    rows: usize,
    metadata: usize,
    artifact: usize,
    fence: usize,
    invalid: ProtectedStateError,
) -> Result<(), ProtectedStateError> {
    if rows > PROTECTED_STATE_RECORD_LIMITS.max_rows
        || metadata > MAX_CATALOG_METADATA_BYTES
        || artifact > MAX_CATALOG_ARTIFACT_BYTES
        || fence > MAX_PROTECTED_STATE_FENCE_BYTES
    {
        return Err(ProtectedStateError::LimitExceeded);
    }
    if fence == 0 {
        return Err(invalid);
    }
    Ok(())
}

pub(crate) fn combined_bytes(
    business: usize,
    metadata: usize,
    artifact: usize,
    fence: usize,
) -> Result<usize, ProtectedStateError> {
    business
        .checked_add(metadata)
        .and_then(|n| n.checked_add(artifact))
        .and_then(|n| n.checked_add(fence))
        .filter(|n| *n <= MAX_PROTECTED_STATE_BYTES)
        .ok_or(ProtectedStateError::LimitExceeded)
}

pub(crate) fn check_rows<'a>(
    rows: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    count: usize,
    metadata: usize,
    artifact: usize,
    fence: usize,
    invalid: ProtectedStateError,
) -> Result<Shape, ProtectedStateError> {
    check_components(count, metadata, artifact, fence, invalid)?;
    let mut budget = ReadBudget::new(PROTECTED_STATE_RECORD_LIMITS);
    let mut previous: Option<&[u8]> = None;
    let mut observed = 0usize;
    let mut business = 0usize;
    for (key, value) in rows {
        budget
            .consume(key.len(), value.len())
            .map_err(|_| ProtectedStateError::LimitExceeded)?;
        if key.is_empty() || previous.is_some_and(|old| old >= key) {
            return Err(invalid);
        }
        observed = observed
            .checked_add(1)
            .ok_or(ProtectedStateError::LimitExceeded)?;
        business = business
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(ProtectedStateError::LimitExceeded)?;
        previous = Some(key);
    }
    if observed != count {
        return Err(invalid);
    }
    Ok(Shape {
        rows: observed,
        business,
        total: combined_bytes(business, metadata, artifact, fence)?,
    })
}

pub(crate) fn reserve<T>(value: &mut Vec<T>, count: usize) -> Result<(), ProtectedStateError> {
    value
        .try_reserve_exact(count)
        .map_err(|_| ProtectedStateError::Allocation)
}

fn copy_bytes(value: &[u8]) -> Result<Vec<u8>, ProtectedStateError> {
    let mut copied = Vec::new();
    reserve(&mut copied, value.len())?;
    copied.extend_from_slice(value);
    Ok(copied)
}

pub(crate) fn copy_parts<'a>(
    rows: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    live: &SnapshotCatalogRecord<'_>,
    fence: &[u8],
    expected: Shape,
) -> Result<StoredProtectedState, ProtectedStateError> {
    let mut entries = Vec::new();
    reserve(&mut entries, expected.rows)?;
    let mut previous: Option<&[u8]> = None;
    let mut budget = ReadBudget::new(PROTECTED_STATE_RECORD_LIMITS);
    let mut business = 0usize;
    for (key, value) in rows {
        budget
            .consume(key.len(), value.len())
            .map_err(|_| ProtectedStateError::LimitExceeded)?;
        if entries.len() >= expected.rows
            || key.is_empty()
            || previous.is_some_and(|old| old >= key)
        {
            return Err(ProtectedStateError::InvalidState);
        }
        business = business
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(ProtectedStateError::LimitExceeded)?;
        entries.push((copy_bytes(key)?, copy_bytes(value)?));
        previous = Some(key);
    }
    let total = combined_bytes(
        business,
        live.metadata().len(),
        live.artifact().len(),
        fence.len(),
    )?;
    if entries.len() != expected.rows || business != expected.business || total != expected.total {
        return Err(ProtectedStateError::InvalidState);
    }
    let live = StoredSnapshotCatalog::copy_from_parts(live.metadata(), live.artifact()).map_err(
        |error| match error {
            CatalogReadError::Allocation => ProtectedStateError::Allocation,
            CatalogReadError::LimitExceeded => ProtectedStateError::LimitExceeded,
            CatalogReadError::Storage(_) => ProtectedStateError::InvalidState,
        },
    )?;
    Ok(StoredProtectedState {
        initialized: true,
        records: StoreSnapshot { entries },
        live: Some(live),
        fence: Some(copy_bytes(fence)?),
        logical_bytes: total,
    })
}

pub(crate) fn copy_state(
    value: &StoredProtectedState,
    shape: Shape,
) -> Result<StoredProtectedState, ProtectedStateError> {
    if !value.initialized {
        return Ok(StoredProtectedState::pristine());
    }
    let (Some(live), Some(fence)) = (&value.live, &value.fence) else {
        return Err(ProtectedStateError::InvalidState);
    };
    let pair = SnapshotCatalogRecord::new(live.metadata(), live.artifact())
        .map_err(|_| ProtectedStateError::LimitExceeded)?;
    copy_parts(
        value
            .records
            .entries()
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice())),
        &pair,
        fence,
        shape,
    )
}
