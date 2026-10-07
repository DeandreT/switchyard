use std::future::Future;

use domain::{CommittedImageExportError, DecodedCommittedImage, EncodedCommittedImage};
use storage::{BoundedStateStore, CommittedStore};

use super::{
    ExperimentalStateMachine, StateMachineError,
    owner::{Operation, Reply},
    state::{StoreState, recover},
};

/// Static failures for an optional owner-serialized image export.
///
/// These are not OpenRaft snapshot results, installation authority, or proof
/// of rollback. Nested causes contain only static enums, never source bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineImageExportError {
    #[error("the image export owner did not complete: {0}")]
    Owner(StateMachineError),
    #[error("the image export source was refused: {0}")]
    Domain(CommittedImageExportError),
    #[error("image export was not enabled for this state owner")]
    Disabled,
    #[error("the captured image has incompatible native metadata")]
    IncompatibleMetadata,
}

impl ExperimentalStateMachine {
    /// Initializes a pristine owner with bounded CreateSendLayout17V1 export enabled.
    ///
    /// This adds no OpenRaft snapshot construction, installation, transport, or
    /// purge support. Existing constructors deliberately leave export disabled.
    ///
    /// ```compile_fail
    /// fn no_unbounded_fallback<W: storage::CommittedStore>(
    ///     writer: W,
    ///     stream: domain::CommittedStreamId,
    /// ) {
    ///     let _ = cluster::ExperimentalStateMachine::create_with_image_export(writer, stream);
    /// }
    /// ```
    pub fn create_with_image_export<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> Result<Self, StateMachineError>
    where
        W: CommittedStore,
        W::Reader: BoundedStateStore,
    {
        Self::start(StoreState::create_with_image_export(writer, stream)?)
    }

    /// Opens exact progress with the same explicit bounded export capability.
    pub fn open_with_image_export<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> Result<Self, StateMachineError>
    where
        W: CommittedStore,
        W::Reader: BoundedStateStore,
    {
        Self::start(StoreState::open_with_image_export(writer, stream)?)
    }

    /// Returns an inert owned export future from the unique facade.
    ///
    /// First poll attempts one bounded owner admission, charged the complete
    /// 64 MiB artifact limit. Accepted work stays owned and charged until actual
    /// completion even if its waiter is lost. Returning crosses the existing
    /// publication/refund barrier. Caller-owned returned bytes are not charged.
    ///
    /// Poison is checked before capture. Exactly one complete bounded snapshot
    /// supplies both the business rows and checkpoint. Native metadata recovery
    /// uses only that captured checkpoint, never a diagnostic progress query.
    /// Source and encoded artifact may coexist during encoding; the admission
    /// charge is not a combined allocation, backend-cache, or RSS bound.
    ///
    /// The future owns no facade lifetime and cannot prevent facade shutdown.
    /// Export adds no authenticity, ancestry, anti-rollback, quorum, historical
    /// operation result, or installation authority.
    pub fn export_create_send_image(
        &mut self,
    ) -> impl Future<Output = Result<EncodedCommittedImage, StateMachineImageExportError>>
    + Send
    + 'static
    + use<> {
        let handle = self.handle.clone();
        async move {
            match handle.request(Operation::ExportImage).await {
                Ok(Reply::ImageExport(result)) => result,
                Ok(_) => Err(StateMachineImageExportError::Owner(
                    StateMachineError::InvalidState,
                )),
                Err(error) => Err(StateMachineImageExportError::Owner(error)),
            }
        }
    }
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn export_create_send_image(
        &mut self,
    ) -> Result<EncodedCommittedImage, StateMachineImageExportError> {
        self.ensure_healthy()
            .map_err(StateMachineImageExportError::Owner)?;
        let export = self
            .image_export
            .ok_or(StateMachineImageExportError::Disabled)?;
        let image = match export(&mut self.machine) {
            Ok(image) => image,
            Err(error) => {
                if matches!(
                    error,
                    CommittedImageExportError::ReadFailed | CommittedImageExportError::Poisoned
                ) {
                    self.poisoned = true;
                }
                return Err(StateMachineImageExportError::Domain(error));
            }
        };
        let decoded = DecodedCommittedImage::decode(image.as_bytes())
            .map_err(|_| StateMachineImageExportError::IncompatibleMetadata)?;
        recover(decoded.checkpoint())
            .map_err(|_| StateMachineImageExportError::IncompatibleMetadata)?;
        drop(decoded);
        Ok(image)
    }
}
