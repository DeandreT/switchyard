use crate::{StateStore, StorageError, StoreSnapshot};

/// Limits on the complete caller-owned record data returned by a bounded read.
///
/// Total bytes count key and value lengths, not collection metadata, allocator
/// overhead, backend-internal allocations or process memory. Zero limits are
/// valid: an empty view always fits; a zero-byte record still needs one row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadLimits {
    pub max_rows: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_total_bytes: usize,
}

/// Optional complete bounded reads with no allocating fallback implementation.
///
/// A successful snapshot contains every record in ascending key order from one
/// stable view. If any limit would be exceeded, this returns an error instead
/// of a truncated or partial snapshot. Implementations check each record's
/// lengths before copying its key or value into the result.
///
/// Existing stores must opt in; an ordinary snapshot is not a bounded fallback.
///
/// ```compile_fail
/// use storage::{BoundedStateStore, ReadLimits, StateStore};
/// fn no_allocating_fallback<S: StateStore>(store: S) {
///     let limits = ReadLimits {
///         max_rows: 1,
///         max_key_bytes: 1,
///         max_value_bytes: 1,
///         max_total_bytes: 2,
///     };
///     let _ = store.snapshot_bounded(limits);
/// }
/// ```
pub trait BoundedStateStore: StateStore {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError>;
}

pub(crate) struct ReadBudget {
    limits: ReadLimits,
    rows: usize,
    bytes: usize,
}

impl ReadBudget {
    pub(crate) fn new(limits: ReadLimits) -> Self {
        Self {
            limits,
            rows: 0,
            bytes: 0,
        }
    }

    /// Permits a backend to refuse another row before retrieving its value.
    pub(crate) fn check_next_row(&self) -> Result<(), StorageError> {
        self.next_rows().map(drop)
    }

    pub(crate) fn consume(
        &mut self,
        key_bytes: usize,
        value_bytes: usize,
    ) -> Result<(), StorageError> {
        let rows = self.next_rows()?;
        if key_bytes > self.limits.max_key_bytes || value_bytes > self.limits.max_value_bytes {
            return Err(StorageError::ReadLimitExceeded);
        }
        let bytes = key_bytes
            .checked_add(value_bytes)
            .and_then(|record_bytes| self.bytes.checked_add(record_bytes))
            .filter(|bytes| *bytes <= self.limits.max_total_bytes)
            .ok_or(StorageError::ReadLimitExceeded)?;
        self.rows = rows;
        self.bytes = bytes;
        Ok(())
    }

    fn next_rows(&self) -> Result<usize, StorageError> {
        self.rows
            .checked_add(1)
            .filter(|rows| *rows <= self.limits.max_rows)
            .ok_or(StorageError::ReadLimitExceeded)
    }
}

#[cfg(test)]
mod tests;
