use openraft::{ErrorVerb, RaftSnapshotBuilder, Snapshot, StorageError};

use crate::LogTypes;

use super::snapshot_error;

/// An explicit refusal, not an empty or metadata-only snapshot of queue state.
pub struct UnsupportedSnapshotBuilder {
    pub(super) _private: (),
}

impl RaftSnapshotBuilder<LogTypes> for UnsupportedSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LogTypes>, StorageError<u64>> {
        Err(snapshot_error(ErrorVerb::Read))
    }
}
