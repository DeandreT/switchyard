use crate::{
    BoundedStateStore, Key, ReadLimits, StateStore, StorageError, StoreSnapshot, Value, WriteBatch,
};

/// The read-only view associated with a committed-store writer.
///
/// Its constructor and underlying store are private. Cloning this view never
/// supplies a committed writer or enables ordinary mutation.
#[derive(Clone)]
pub struct ReplicaReader<S> {
    inner: S,
}

impl<S> std::fmt::Debug for ReplicaReader<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicaReader")
            .finish_non_exhaustive()
    }
}

impl<S> ReplicaReader<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: StateStore> StateStore for ReplicaReader<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }

    fn apply(&self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::ReplicaWriteRequired)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

impl<S: BoundedStateStore> BoundedStateStore for ReplicaReader<S> {
    fn snapshot_bounded(&self, limits: ReadLimits) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot_bounded(limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_require_a_debug_backend() {
        struct NoDebug;

        let reader = ReplicaReader::new(NoDebug);
        assert_eq!(format!("{reader:?}"), "ReplicaReader { .. }");
    }
}
