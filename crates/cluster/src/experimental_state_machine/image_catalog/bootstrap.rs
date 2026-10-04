use domain::{CommittedImageBootstrapError, CommittedStateMachine, TrustedCreateSendBootstrap};
use storage::{BoundedStateStore, CatalogCommittedStore};

use super::super::{
    DecodedNativeSnapshotPair, ExperimentalStateMachine, NativeSnapshotMetadataError,
    StateMachineError, state::StoreState,
};

/// Static before-target, unknown-commit, and known-commit startup causes.
///
/// A matching pair and trusted selection do not establish log ancestry,
/// authenticity, quorum, populated-target replacement, or engine adoption.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineCatalogBootstrapError {
    #[error("the selected native catalog pair was refused: {0}")]
    Metadata(NativeSnapshotMetadataError),
    #[error("the selected catalog bootstrap was refused: {0}")]
    Domain(CommittedImageBootstrapError),
    #[error("the selected catalog was committed but its native owner could not be started")]
    OwnerStartAfterCommit,
}

impl ExperimentalStateMachine {
    /// Bootstrap exact selected business rows and their native catalog atomically.
    ///
    /// The actual borrowed metadata/artifact pair is fully validated and its owned
    /// native projection derived before any target API in this function, including
    /// a reader factory. The domain then independently checks the trusted stream,
    /// full checkpoint, and complete artifact digest before proving a pristine
    /// target and issuing its sole combined business/init/catalog commit.
    ///
    /// Opening the consumed writer earlier is outside this operation's guarantee.
    /// No postcommit validation read, second retain, normalization, or fallback is
    /// performed. The native state is constructed directly from domain success.
    /// Existing image-export and catalog-operation capabilities remain disabled.
    /// No bounded target reader is required: no source capture occurs here.
    ///
    /// Domain CommitUnknown returns no usable machine and does not prove rollback.
    /// OwnerStartAfterCommit instead follows known domain success. Release all
    /// physical handles and recover the actual target; do not blindly retry.
    ///
    /// This does not activate engine snapshot traits, runtime adoption, current
    /// snapshot results, installation into populated state, transport, or purge.
    ///
    /// Metadata is borrowed, bounded, and immutable for this synchronous call.
    /// Business batch rows and caller artifact coexist; source/backend staging,
    /// normal collection allocation, and RSS are outside logical artifact bounds.
    ///
    /// ```compile_fail
    /// fn no_ordinary_target<W: storage::CommittedStore>(
    ///     writer: W, selection: domain::TrustedCreateSendBootstrap<'_>, metadata: &[u8],
    /// ) {
    ///     let _ = cluster::ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
    ///         writer, selection, metadata,
    ///     );
    /// }
    /// ```
    pub fn bootstrap_create_send_image_with_catalog<W: CatalogCommittedStore>(
        writer: W,
        request: TrustedCreateSendBootstrap<'_>,
        metadata: &[u8],
    ) -> Result<Self, StateMachineCatalogBootstrapError> {
        bootstrap_with_starter(writer, request, metadata, Self::start)
    }

    /// Bootstrap with the separately explicit catalog build/read capability.
    ///
    /// This adds the same private monomorphized capability as opted-in catalog
    /// create/open, but keeps the independent image-export operation disabled.
    /// Bounds apply only to this variant; plain bootstrap has no bounded-reader
    /// requirement. No extra validation or capture is performed after commit.
    ///
    /// ```compile_fail
    /// fn no_unbounded_capture<W: storage::CatalogCommittedStore>(
    ///     writer: W, selection: domain::TrustedCreateSendBootstrap<'_>, metadata: &[u8],
    /// ) {
    ///     let _ = cluster::ExperimentalStateMachine::bootstrap_create_send_image_with_catalog_operations(
    ///         writer, selection, metadata,
    ///     );
    /// }
    /// ```
    pub fn bootstrap_create_send_image_with_catalog_operations<W>(
        writer: W,
        request: TrustedCreateSendBootstrap<'_>,
        metadata: &[u8],
    ) -> Result<Self, StateMachineCatalogBootstrapError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        bootstrap_with_starter(writer, request, metadata, |mut state| {
            state.image_catalog = Some(super::capabilities::<W>());
            Self::start(state)
        })
    }
}

// The consuming private starter is a test boundary, not public injection.
fn bootstrap_with_starter<W, S>(
    writer: W,
    request: TrustedCreateSendBootstrap<'_>,
    metadata: &[u8],
    start: S,
) -> Result<ExperimentalStateMachine, StateMachineCatalogBootstrapError>
where
    W: CatalogCommittedStore,
    S: FnOnce(StoreState<W>) -> Result<ExperimentalStateMachine, StateMachineError>,
{
    {
        let pair = DecodedNativeSnapshotPair::decode(metadata, request.artifact_bytes())
            .map_err(StateMachineCatalogBootstrapError::Metadata)?;
        // Complete the bounded owned projection before any target operation.
        // Nothing from this preflight is treated as target progress or authority.
        let _projection = pair
            .snapshot_meta()
            .map_err(StateMachineCatalogBootstrapError::Metadata)?;
    }
    let machine =
        CommittedStateMachine::bootstrap_create_send_image_with_catalog(writer, request, metadata)
            .map_err(StateMachineCatalogBootstrapError::Domain)?;
    start(StoreState {
        machine,
        poisoned: false,
        image_export: None,
        image_catalog: None,
    })
    .map_err(|_| StateMachineCatalogBootstrapError::OwnerStartAfterCommit)
}

#[cfg(test)]
mod tests;
