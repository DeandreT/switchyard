use std::future::Future;

use domain::{CommittedImageReplacementError, CommittedStateMachine, TrustedCreateSendReplacement};
use storage::{BoundedStateStore, CatalogCommittedStore, CommittedStore};

use super::{
    DecodedNativeSnapshotPair, EncodedNativeSnapshotMetadata, ExperimentalStateMachine,
    NativeSnapshotMetadataError, StateMachineError,
    owner::{Operation, Reply},
    state::StoreState,
};

mod input;
pub use input::OwnedTrustedNativeReplacement;

/// Static source/target refusals and unknown owner or commit outcomes.
///
/// No nested error exposes backend detail or caller metadata. CommitUnknown and
/// an accepted operation losing its owner response are not rollback evidence.
/// No returned refusal authorizes retry, engine adoption, or log purge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineImageReplacementError {
    #[error("the native replacement owner did not complete: {0}")]
    Owner(StateMachineError),
    #[error("the selected native replacement was refused: {0}")]
    Domain(CommittedImageReplacementError),
    #[error("the selected native replacement metadata was refused: {0}")]
    Metadata(NativeSnapshotMetadataError),
    #[error("native image replacement was not enabled for this owner")]
    Disabled,
}

type Result<T> = std::result::Result<T, StateMachineImageReplacementError>;

/// The sole combined domain commit returned success, not a historical receipt.
///
/// This static non-Clone result carries no image, IDs, source-health statement,
/// engine-adoption, ancestry, anti-rollback, quorum, or purge authorization. An
/// explicitly trusted replacement may select earlier progress. Its private
/// construction prevents a caller from manufacturing this owner result.
///
/// ```compile_fail
/// fn duplicate(value: cluster::CommittedNativeReplacement) { let _ = value.clone(); }
/// ```
///
/// ```compile_fail
/// let _ = cluster::CommittedNativeReplacement { _private: () };
/// ```
#[derive(Debug)]
pub struct CommittedNativeReplacement {
    _private: (),
}

pub(super) type ReplacementCapability<M> =
    fn(&mut M, &OwnedTrustedNativeReplacement) -> Result<CommittedNativeReplacement>;

impl ExperimentalStateMachine {
    /// Create a pristine owner with separately enabled trusted replacement.
    ///
    /// This does not enable export, catalog build/read, engine snapshot traits,
    /// runtime, or purge. Ordinary and existing opt-in constructors are unchanged.
    ///
    /// ```compile_fail
    /// fn ordinary<W: storage::CommittedStore>(writer: W, stream: domain::CommittedStreamId)
    /// where W::Reader: storage::BoundedStateStore {
    ///     let _ = cluster::ExperimentalStateMachine::create_with_snapshot_replacement(writer, stream);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn unbounded<W: storage::CatalogCommittedStore>(writer: W, stream: domain::CommittedStreamId) {
    ///     let _ = cluster::ExperimentalStateMachine::create_with_snapshot_replacement(writer, stream);
    /// }
    /// ```
    pub fn create_with_snapshot_replacement<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> std::result::Result<Self, StateMachineError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        let mut state = StoreState::create(writer, stream)?;
        state.image_replacement = Some(replace::<W>);
        Self::start(state)
    }

    /// Open the same independent replacement capability with native recovery.
    pub fn open_with_snapshot_replacement<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> std::result::Result<Self, StateMachineError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        let mut state = StoreState::open(writer, stream)?;
        state.image_replacement = Some(replace::<W>);
        Self::start(state)
    }

    /// Return an inert owning future for one exact trusted replacement.
    ///
    /// Request packaging checks finite source/meta/checkpoint bounds before this
    /// operation, so packaging may refuse before any owner poison check. Once
    /// admitted, healthy/disabled checks precede pure native/source preflight.
    /// The actual complete immutable body and every supplied SnapshotMeta field
    /// must agree before any target I/O. Trusted stream, both full checkpoints,
    /// and complete-artifact SHA remain independent caller expectations.
    ///
    /// The owner borrows the privately owned carrier only during this operation;
    /// its cursor is ignored and no body clone/extraction, source query, raw
    /// writer, second writer, or postcommit validation read is needed. Canonical
    /// bounded metadata is derived before the sole combined domain commit.
    /// The static receipt is prepared before commit and only moved afterward.
    ///
    /// The full existing 64 MiB charge excludes other accepted owner work until
    /// publication/refund; it is admission accounting, not an aggregate memory
    /// limit. Caller-owned source, raw/encoded target capture, up to 128 MiB
    /// mutation payload, bounded metadata/checkpoints/projections, spare capacity,
    /// semantic/backend staging and RSS may coexist. Unpolled caller futures and
    /// normal allocator/channel internals remain outside this accounting.
    ///
    /// Accepted caller loss cannot cancel durable work or lease cleanup. Every
    /// domain commit error returns CommitUnknown and poisons this owner; physical
    /// target capture errors poison too. Lost response/panic after admission
    /// cannot establish rollback. No automatic retry or usable success is returned
    /// on these paths. This is explicitly not OpenRaft install_snapshot, engine
    /// admission, ancestry/anti-rollback policy, or cross-owner purge authorization.
    pub fn replace_create_send_image_with_catalog(
        &mut self,
        request: OwnedTrustedNativeReplacement,
    ) -> impl Future<Output = Result<CommittedNativeReplacement>> + Send + 'static + use<> {
        let handle = self.handle.clone();
        async move {
            match handle
                .request(Operation::ReplaceCatalog(Box::new(request)))
                .await
            {
                Ok(Reply::CatalogReplaced(result)) => result,
                Ok(_) => Err(StateMachineImageReplacementError::Owner(
                    StateMachineError::InvalidState,
                )),
                Err(error) => Err(StateMachineImageReplacementError::Owner(error)),
            }
        }
    }
}

fn replace<W>(
    machine: &mut CommittedStateMachine<W>,
    request: &OwnedTrustedNativeReplacement,
) -> Result<CommittedNativeReplacement>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let artifact = request.source.snapshot.as_bytes();
    let metadata = EncodedNativeSnapshotMetadata::encode(artifact)
        .map_err(StateMachineImageReplacementError::Metadata)?;
    {
        let pair = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), artifact)
            .map_err(StateMachineImageReplacementError::Metadata)?;
        let projection = pair
            .snapshot_meta()
            .map_err(StateMachineImageReplacementError::Metadata)?;
        if projection != request.source.meta {
            return Err(StateMachineImageReplacementError::Metadata(
                NativeSnapshotMetadataError::ImageMismatch,
            ));
        }
    }
    let selection = TrustedCreateSendReplacement::new(
        request.stream,
        &request.target,
        &request.selected,
        request.digest,
        artifact,
    );
    let receipt = CommittedNativeReplacement { _private: () };
    machine
        .replace_create_send_image_with_catalog(selection, metadata.as_bytes())
        .map_err(StateMachineImageReplacementError::Domain)?;
    Ok(receipt)
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn replace_create_send_image_with_catalog(
        &mut self,
        request: &OwnedTrustedNativeReplacement,
    ) -> Result<CommittedNativeReplacement> {
        self.ensure_healthy()
            .map_err(StateMachineImageReplacementError::Owner)?;
        let replace = self
            .image_replacement
            .ok_or(StateMachineImageReplacementError::Disabled)?;
        let result = replace(&mut self.machine, request);
        if matches!(
            &result,
            Err(StateMachineImageReplacementError::Domain(
                CommittedImageReplacementError::Poisoned
                    | CommittedImageReplacementError::TargetReadFailed
                    | CommittedImageReplacementError::CommitUnknown
            ))
        ) {
            self.poisoned = true;
        }
        result
    }
}

#[cfg(test)]
mod tests;
