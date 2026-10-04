use storage::{BoundedStateStore, CommittedStore, ReadLimits, StorageError};

use crate::{
    CommittedImageError, CommittedImageRole, CommittedImageValidationError, DecodedCommittedImage,
    EncodedCommittedImage, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_IMAGE_KEY_BYTES,
    MAX_COMMITTED_IMAGE_ROWS, MAX_COMMITTED_IMAGE_VALUE_BYTES, ValidatedCreateSendImage,
};

use super::CommittedStateMachine;

/// Static export failures, with no backend detail or source content.
///
/// Refusing an arbitrary reopened source's declared profile is not proof of
/// corruption. Only a physical bounded-read failure poisons this machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedImageExportError {
    #[error("the committed image source is poisoned")]
    Poisoned,
    #[error("the committed image source could not be read")]
    ReadFailed,
    #[error("the committed image exceeds its export limits")]
    LimitExceeded,
    #[error("the committed image output could not be allocated")]
    Allocation,
    #[error("the committed image source profile is unsupported")]
    UnsupportedProfile,
    #[error("the committed image source is inconsistent with the requested profile")]
    InvalidImage,
}

const LIMITS: ReadLimits = ReadLimits {
    max_rows: MAX_COMMITTED_IMAGE_ROWS,
    max_key_bytes: MAX_COMMITTED_IMAGE_KEY_BYTES,
    max_value_bytes: MAX_COMMITTED_IMAGE_VALUE_BYTES,
    max_total_bytes: MAX_COMMITTED_IMAGE_BYTES,
};

impl<W> CommittedStateMachine<W>
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    /// Captures and checks one complete bounded CreateSendV1 image.
    ///
    /// The checkpoint comes exclusively from the same captured rows as the
    /// business state. Constructor validation and the exclusive writer already
    /// establish initialization; no separate checkpoint or initialization read
    /// is mixed into this captured view. This does not re-attest a backend that
    /// violates its contract or is modified through trusted out-of-band access.
    ///
    /// The source key/value data and the complete encoded artifact each have a
    /// 64 MiB logical bound and coexist during encoding. Collection overhead,
    /// spare capacity, backend materialization, metadata, and RSS are excluded.
    /// The captured source is dropped before borrowed semantic validation.
    ///
    /// This grants no installation, history purge, membership interpretation,
    /// authenticity, ancestry, anti-rollback, or quorum authority. It changes
    /// neither constructor behavior nor the ordinary allocating snapshot API.
    ///
    /// There is deliberately no allocating fallback for a StateStore reader.
    ///
    /// ```compile_fail
    /// fn no_fallback<W: storage::CommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    /// ) {
    ///     let _ = machine.export_create_send_image();
    /// }
    /// ```
    pub fn export_create_send_image(
        &mut self,
    ) -> Result<EncodedCommittedImage, CommittedImageExportError> {
        if self.poisoned {
            return Err(CommittedImageExportError::Poisoned);
        }
        let snapshot = match self.machine.store().snapshot_bounded(LIMITS) {
            Ok(snapshot) => snapshot,
            Err(StorageError::ReadLimitExceeded) => {
                return Err(CommittedImageExportError::LimitExceeded);
            }
            Err(_) => {
                self.poisoned = true;
                return Err(CommittedImageExportError::ReadFailed);
            }
        };
        let image =
            EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, self.stream, &snapshot)
                .map_err(container_error)?;
        drop(snapshot);
        {
            let decoded =
                DecodedCommittedImage::decode(image.as_bytes()).map_err(container_error)?;
            ValidatedCreateSendImage::validate(decoded).map_err(validation_error)?;
        }
        Ok(image)
    }
}

fn container_error(error: CommittedImageError) -> CommittedImageExportError {
    match error {
        CommittedImageError::LimitExceeded => CommittedImageExportError::LimitExceeded,
        CommittedImageError::Allocation => CommittedImageExportError::Allocation,
        CommittedImageError::UnsupportedFormat => CommittedImageExportError::UnsupportedProfile,
        _ => CommittedImageExportError::InvalidImage,
    }
}

fn validation_error(error: CommittedImageValidationError) -> CommittedImageExportError {
    match error {
        CommittedImageValidationError::UnsupportedProfile => {
            CommittedImageExportError::UnsupportedProfile
        }
        _ => CommittedImageExportError::InvalidImage,
    }
}

#[cfg(test)]
mod tests;
