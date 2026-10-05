use domain::{CommittedImageBootstrapError, CommittedStateMachine, TrustedCreateSendBootstrap};
use storage::{BoundedStateStore, CommittedStore};

use super::{
    ExperimentalStateMachine, StateMachineError,
    state::{StoreState, recover},
};

/// Static failures for an explicitly selected pristine native-state bootstrap.
///
/// Selection is trusted intent, not authenticity, quorum, ancestry, or snapshot
/// installation authority. No backend path, artifact bytes, or membership bytes
/// are carried by these errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineImageBootstrapError {
    #[error("the selected image has incompatible native metadata")]
    IncompatibleMetadata,
    #[error("the selected image bootstrap did not complete: {0}")]
    Domain(CommittedImageBootstrapError),
    #[error("the selected image was committed but its native owner could not be started")]
    OwnerStartAfterCommit,
}

impl ExperimentalStateMachine {
    /// Bootstrap one selected CreateSendV1 image into a pristine native owner.
    ///
    /// Pure native recovery validates the selected full checkpoint identities
    /// and membership before domain source validation or target operations in
    /// this function. The domain then checks the actual artifact against that
    /// exact checkpoint, stream, and complete digest before accessing the target.
    /// The caller has already opened the writer; opening it is outside this
    /// function's source-before-target-operation guarantee.
    ///
    /// Success follows one atomic domain commit and native thread startup, with
    /// no postcommit validation read. Export is deliberately disabled. This is
    /// not populated-state replacement, a snapshot catalog, engine adoption,
    /// installation, or purge. The writer is consumed on every result.
    ///
    /// A domain CommitUnknown error does not prove rollback or permit blind
    /// retry. OwnerStartAfterCommit instead means the domain commit succeeded
    /// but no owner facade was returned. Reopen the selected durable target after
    /// all physical writer and reader handles have been released.
    pub fn bootstrap_create_send_image<W: CommittedStore>(
        writer: W,
        request: TrustedCreateSendBootstrap<'_>,
    ) -> Result<Self, StateMachineImageBootstrapError> {
        bootstrap_with_starter(writer, request, Self::start)
    }

    /// Bootstrap with the same explicit bounded export capability as an opted-in
    /// create/open constructor. All source and commit rules above still apply.
    ///
    /// Existing constructors and the plain bootstrap leave export disabled.
    /// No allocating fallback is added for readers lacking bounded capture.
    ///
    /// ```compile_fail
    /// fn no_unbounded_fallback<W: storage::CommittedStore>(
    ///     writer: W,
    ///     request: domain::TrustedCreateSendBootstrap<'_>,
    /// ) {
    ///     let _ = cluster::ExperimentalStateMachine::bootstrap_create_send_image_with_export(
    ///         writer, request,
    ///     );
    /// }
    /// ```
    pub fn bootstrap_create_send_image_with_export<W>(
        writer: W,
        request: TrustedCreateSendBootstrap<'_>,
    ) -> Result<Self, StateMachineImageBootstrapError>
    where
        W: CommittedStore,
        W::Reader: BoundedStateStore,
    {
        bootstrap_with_starter(writer, request, |mut state| {
            state.image_export = Some(CommittedStateMachine::<W>::export_create_send_image);
            Self::start(state)
        })
    }
}

// The private starter is only an ownership-preserving boundary around the real
// startup call. Tests can refuse that stage without exhausting OS threads.
fn bootstrap_with_starter<W, S>(
    writer: W,
    request: TrustedCreateSendBootstrap<'_>,
    start: S,
) -> Result<ExperimentalStateMachine, StateMachineImageBootstrapError>
where
    W: CommittedStore,
    S: FnOnce(StoreState<W>) -> Result<ExperimentalStateMachine, StateMachineError>,
{
    recover(request.expected_checkpoint())
        .map_err(|_| StateMachineImageBootstrapError::IncompatibleMetadata)?;
    let machine = CommittedStateMachine::bootstrap_create_send_image(writer, request)
        .map_err(StateMachineImageBootstrapError::Domain)?;
    // Domain success already validated the exact artifact and committed it. Do
    // not call validated/open/checkpoint here or turn a successful commit into a
    // new source-read decision.
    start(StoreState {
        machine,
        poisoned: false,
        image_export: None,
        image_catalog: None,
        image_replacement: None,
    })
    .map_err(|_| StateMachineImageBootstrapError::OwnerStartAfterCommit)
}

#[cfg(test)]
mod tests;
