use sha2::{Digest, Sha256};

use crate::{
    CommittedCheckpoint, CommittedImageError, CommittedImageValidationError, CommittedStreamId,
    DecodedCommittedImage, ValidatedCreateSendImage,
};

#[derive(Clone, Copy)]
pub(super) enum SelectionError {
    InvalidSelection,
    SelectionMismatch,
    LimitExceeded,
    Allocation,
    UnsupportedProfile,
    InvalidImage,
}

// Pure shared preflight; target-specific mutation authority stays with its caller.
pub(super) fn validate_selection<'a>(
    stream: CommittedStreamId,
    checkpoint: &CommittedCheckpoint,
    digest: [u8; 32],
    artifact: &'a [u8],
) -> Result<ValidatedCreateSendImage<'a>, SelectionError> {
    stream
        .validate()
        .map_err(|_| SelectionError::InvalidSelection)?;
    if checkpoint.stream() != stream {
        return Err(SelectionError::InvalidSelection);
    }
    let image = DecodedCommittedImage::decode(artifact).map_err(container_error)?;
    if image.stream() != stream
        || image.checkpoint() != checkpoint
        || <[u8; 32]>::from(Sha256::digest(artifact)) != digest
    {
        return Err(SelectionError::SelectionMismatch);
    }
    ValidatedCreateSendImage::validate(image).map_err(|error| match error {
        CommittedImageValidationError::UnsupportedProfile => SelectionError::UnsupportedProfile,
        _ => SelectionError::InvalidImage,
    })
}

fn container_error(error: CommittedImageError) -> SelectionError {
    match error {
        CommittedImageError::LimitExceeded => SelectionError::LimitExceeded,
        CommittedImageError::Allocation => SelectionError::Allocation,
        CommittedImageError::UnsupportedFormat => SelectionError::UnsupportedProfile,
        _ => SelectionError::InvalidImage,
    }
}
