//! Explicit standalone builder; the engine-facing adapter remains unsupported.

use std::fmt;

use openraft::{ErrorSubject, ErrorVerb, RaftSnapshotBuilder, Snapshot, StorageError};

use crate::LogTypes;

use super::{
    ExperimentalStateMachine,
    owner::{Handle, Operation, Reply},
};

const _: () = {
    assert!(domain::MAX_COMMITTED_IMAGE_BYTES <= crate::MAX_SNAPSHOT_BYTES);
};

/// An owning standalone builder using the native catalog owner's existing path.
///
/// The factory does not perform I/O or borrow the facade. The private request
/// handle has no public operation, writer, close, or join authority. Old owner
/// constructors still refuse its first build without source I/O; explicit catalog
/// constructors enable the existing catalog capability, not any engine trait.
/// Accepted work reserves the full existing 64 MiB budget, with the catalog's
/// documented bounded metadata overhead. Losing a build waiter does not cancel
/// capture, retention, publication/refund, or the owner's actual retirement.
///
/// Successful retention precedes the consuming transport handoff. The original
/// encoded image and owned projection move without a second artifact copy.
/// Normal small Box/channel/library allocations are not an all-allocations-fallible
/// or RSS guarantee. A defensive handoff refusal is static and does not imply
/// rollback or authorize retry of the already-attempted catalog retention.
///
/// This builder is deliberately NOT returned by the existing RaftStateMachine.
/// It enables no engine snapshot adoption, get-current/receiving/installation,
/// network transport, runtime policy, compaction, or history-purge authority.
///
/// ```compile_fail
/// fn duplicate(builder: cluster::CreateSendSnapshotBuilder) {
///     let another = builder.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn escape(builder: cluster::CreateSendSnapshotBuilder) {
///     let writer = builder.handle;
/// }
/// ```
pub struct CreateSendSnapshotBuilder {
    handle: Handle,
}

impl ExperimentalStateMachine {
    /// Create an inert, owning standalone builder with no facade lifetime.
    ///
    /// The existing catalog capability is checked by the owner when a build is
    /// first polled. Keeping this value, or an unpolled build future, cannot keep
    /// the physical owner alive after explicit facade shutdown and join.
    pub fn create_send_snapshot_builder(&mut self) -> CreateSendSnapshotBuilder {
        CreateSendSnapshotBuilder {
            handle: self.handle.clone(),
        }
    }
}

impl RaftSnapshotBuilder<LogTypes> for CreateSendSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LogTypes>, StorageError<u64>> {
        match self.handle.request(Operation::BuildCatalog).await {
            Ok(Reply::CatalogBuilt(Ok(built))) => {
                built.into_standalone_snapshot().map_err(|_| build_error())
            }
            _ => Err(build_error()),
        }
    }
}

impl fmt::Debug for CreateSendSnapshotBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateSendSnapshotBuilder")
            .finish_non_exhaustive()
    }
}

fn build_error() -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Snapshot(None),
        ErrorVerb::Read,
        std::io::Error::other("standalone committed snapshot build did not complete"),
    )
}

#[cfg(test)]
mod tests;
