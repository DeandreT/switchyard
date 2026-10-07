//! Pure agreement over an owned protected capture, not publication authority.

use std::fmt;

use sha2::{Digest, Sha256};
use storage::{MAX_PROTECTED_STATE_FENCE_BYTES, StoredProtectedState};

use crate::{
    CommittedCheckpoint, CommittedImageRows, CreateSendImageExpectation,
    MAX_COMMITTED_MEMBERSHIP_BYTES,
};

use super::{
    CommittedImageError, CommittedImageValidationError, DecodedCommittedImage,
    MAX_COMMITTED_IMAGE_BYTES, ValidatedCreateSendImage,
};

#[cfg(test)]
mod tests;

mod layout17;
pub use layout17::{
    CheckedProtectedCreateSendLayout17Image, check_protected_create_send_layout17_image,
};

/// Static data refusals, not source-health, poison or mutation diagnostics.
///
/// A legitimately older catalog can disagree with business rows. Neither
/// `BusinessMismatch` nor `UnsupportedProfile` makes that owner corrupt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtectedCreateSendImageError {
    #[error("protected image agreement exceeds its logical limits")]
    LimitExceeded,
    #[error("protected image validation could not be allocated")]
    Allocation,
    #[error("protected image expectations are invalid")]
    InvalidExpectation,
    #[error("protected capture is not initialized")]
    NotInitialized,
    #[error("protected capture is incomplete")]
    IncompleteState,
    #[error("protected image profile is unsupported")]
    UnsupportedProfile,
    #[error("protected image is invalid")]
    InvalidImage,
    #[error("protected image does not match its expected identity")]
    IdentityMismatch,
    #[error("protected capture does not match its expected opaque fence")]
    FenceMismatch,
    #[error("protected business rows do not match the validated image")]
    BusinessMismatch,
}

type Result<T> = std::result::Result<T, ProtectedCreateSendImageError>;

/// Non-Clone immutable agreement view borrowing only the caller-owned capture.
///
/// The checked function is its sole constructor. Business rows and artifact
/// bytes stay borrowed; the existing decoder owns bounded checkpoint metadata.
/// Explicit accessors reveal ordinary descriptive bytes. Debug is numeric only.
/// This view can be stale immediately and is not source authenticity, canonical
/// selection, an expected-old CAS, a receipt, or permission to publish/adopt.
///
/// ```compile_fail
/// fn duplicate(view: domain::CheckedProtectedCreateSendImage<'_>) {
///     let _ = view.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct<'a>(
///     state: &'a storage::StoredProtectedState,
///     image: domain::ValidatedCreateSendImage<'a>,
/// ) -> domain::CheckedProtectedCreateSendImage<'a> {
///     domain::CheckedProtectedCreateSendImage { state, image }
/// }
/// ```
///
/// ```compile_fail
/// fn batch(view: domain::CheckedProtectedCreateSendImage<'_>) -> storage::WriteBatch {
///     view.into()
/// }
/// ```
///
/// ```compile_fail
/// fn publish(view: domain::CheckedProtectedCreateSendImage<'_>) {
///     let _ = view.publish();
/// }
/// ```
///
/// ```compile_fail
/// fn escape(
///     expected: &domain::CreateSendImageExpectation<'_>,
/// ) -> domain::CheckedProtectedCreateSendImage<'static> {
///     use storage::ProtectedStateReader;
///     let state = storage::MemoryProtectedStateStore::new()
///         .reader().capture_protected_state().unwrap();
///     domain::check_protected_create_send_image(&state, expected, b"fence").unwrap()
/// }
/// ```
///
/// ```compile_fail
/// fn drop_capture(
///     state: storage::StoredProtectedState,
///     expected: &domain::CreateSendImageExpectation<'_>,
/// ) {
///     let view = domain::check_protected_create_send_image(&state, expected, b"fence").unwrap();
///     drop(state);
///     let _ = view.image().row_count();
/// }
/// ```
pub struct CheckedProtectedCreateSendImage<'a> {
    state: &'a StoredProtectedState,
    image: ValidatedCreateSendImage<'a>,
}

impl<'a> CheckedProtectedCreateSendImage<'a> {
    pub fn protected_state(&self) -> &'a StoredProtectedState {
        self.state
    }

    pub fn image(&self) -> &ValidatedCreateSendImage<'a> {
        &self.image
    }
}

impl fmt::Debug for CheckedProtectedCreateSendImage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let live = self.state.live_catalog();
        formatter
            .debug_struct("CheckedProtectedCreateSendImage")
            .field("rows", &self.image.row_count())
            .field(
                "artifact_bytes",
                &live.map_or(0, |live| live.artifact().len()),
            )
            .field(
                "metadata_bytes",
                &live.map_or(0, |live| live.metadata().len()),
            )
            .field("fence_bytes", &self.state.fence().map_or(0, <[u8]>::len))
            .finish_non_exhaustive()
    }
}

/// Checks complete initialized capture/image/expectation/fence/row agreement.
///
/// Expected scalar bounds and nonzero stream precede image recovery. Existing
/// complete container and business validation precede lazy full-checkpoint,
/// complete-length and whole-artifact SHA-256 comparison, then opaque fence and
/// exhaustive row equality. The digest includes the artifact's checksum bytes.
/// Earlier errors may mask later mismatches; no partial view is returned.
///
/// Capture shape/limits rely on the existing private storage DTO contract.
/// `IncompleteState` is defensive: public storage construction/capture cannot
/// yield an initialized half-state. Catalog metadata is never interpreted as
/// native metadata. Valid initial and memberless images remain descriptive data.
/// Expectations are not authenticated; matching untrusted inputs prove only
/// agreement. Existing validator/checkpoint allocations remain ordinary; no
/// universal fallible-allocation, allocator-capacity or RSS guarantee is added.
/// No originating store is read, changed, poisoned, retried or adopted.
///
/// ```no_run
/// fn inspect(
///     state: &storage::StoredProtectedState,
///     expected_checkpoint: domain::CommittedCheckpoint,
///     expected_bytes: usize,
///     expected_sha256: [u8; 32],
///     expected_fence: Vec<u8>,
/// ) -> Result<usize, domain::ProtectedCreateSendImageError> {
///     let view = {
///         let checkpoint = expected_checkpoint;
///         let fence = expected_fence;
///         let expected = domain::CreateSendImageExpectation {
///             checkpoint: &checkpoint,
///             artifact_bytes: expected_bytes,
///             artifact_sha256: expected_sha256,
///         };
///         domain::check_protected_create_send_image(state, &expected, &fence)?
///     };
///     let _: &storage::StoredProtectedState = view.protected_state();
///     let _: &domain::CommittedCheckpoint = view.image().checkpoint();
///     for row in view.image().rows() {
///         let _: (&[u8], &[u8]) = (row.key(), row.value());
///     }
///     Ok(view.image().row_count())
/// }
/// ```
pub fn check_protected_create_send_image<'a>(
    state: &'a StoredProtectedState,
    expected: &CreateSendImageExpectation<'_>,
    expected_fence: &[u8],
) -> Result<CheckedProtectedCreateSendImage<'a>> {
    let artifact = captured_artifact(state, expected, expected_fence)?;
    let decoded = DecodedCommittedImage::decode(artifact).map_err(container_error)?;
    let image = ValidatedCreateSendImage::validate(decoded).map_err(business_error)?;
    check_agreement(
        state,
        expected,
        expected_fence,
        image.checkpoint(),
        image.rows(),
        image.row_count(),
    )?;
    Ok(CheckedProtectedCreateSendImage { state, image })
}

fn captured_artifact<'a>(
    state: &'a StoredProtectedState,
    expected: &CreateSendImageExpectation<'_>,
    expected_fence: &[u8],
) -> Result<&'a [u8]> {
    check_expectation_shape(expected, expected_fence)?;
    expected
        .checkpoint
        .stream()
        .validate()
        .map_err(|_| ProtectedCreateSendImageError::InvalidExpectation)?;
    if !state.is_initialized() {
        return Err(ProtectedCreateSendImageError::NotInitialized);
    }
    let live = state
        .live_catalog()
        .ok_or(ProtectedCreateSendImageError::IncompleteState)?;
    state
        .fence()
        .filter(|fence| !fence.is_empty())
        .ok_or(ProtectedCreateSendImageError::IncompleteState)?;
    Ok(live.artifact())
}

fn check_agreement(
    state: &StoredProtectedState,
    expected: &CreateSendImageExpectation<'_>,
    expected_fence: &[u8],
    checkpoint: &CommittedCheckpoint,
    rows: CommittedImageRows<'_>,
    row_count: usize,
) -> Result<()> {
    let artifact = state
        .live_catalog()
        .ok_or(ProtectedCreateSendImageError::IncompleteState)?
        .artifact();
    if checkpoint != expected.checkpoint
        || artifact.len() != expected.artifact_bytes
        || <[u8; 32]>::from(Sha256::digest(artifact)) != expected.artifact_sha256
    {
        return Err(ProtectedCreateSendImageError::IdentityMismatch);
    }
    if state.fence() != Some(expected_fence) {
        return Err(ProtectedCreateSendImageError::FenceMismatch);
    }
    if state.records().entries().len() != row_count {
        return Err(ProtectedCreateSendImageError::BusinessMismatch);
    }
    let mut business = state.records().entries().iter();
    let mut rows = rows;
    loop {
        match (business.next(), rows.next()) {
            (Some((key, value)), Some(row))
                if key.as_slice() == row.key() && value.as_slice() == row.value() => {}
            (None, None) => break,
            _ => return Err(ProtectedCreateSendImageError::BusinessMismatch),
        }
    }
    Ok(())
}

fn check_expectation_shape(expected: &CreateSendImageExpectation<'_>, fence: &[u8]) -> Result<()> {
    if expected.artifact_bytes > MAX_COMMITTED_IMAGE_BYTES
        || expected
            .checkpoint
            .membership()
            .is_some_and(|membership| membership.payload.len() > MAX_COMMITTED_MEMBERSHIP_BYTES)
        || fence.len() > MAX_PROTECTED_STATE_FENCE_BYTES
    {
        return Err(ProtectedCreateSendImageError::LimitExceeded);
    }
    if fence.is_empty() {
        return Err(ProtectedCreateSendImageError::InvalidExpectation);
    }
    Ok(())
}

fn container_error(error: CommittedImageError) -> ProtectedCreateSendImageError {
    match error {
        CommittedImageError::LimitExceeded => ProtectedCreateSendImageError::LimitExceeded,
        CommittedImageError::Allocation => ProtectedCreateSendImageError::Allocation,
        CommittedImageError::UnsupportedFormat => ProtectedCreateSendImageError::UnsupportedProfile,
        _ => ProtectedCreateSendImageError::InvalidImage,
    }
}

fn business_error(error: CommittedImageValidationError) -> ProtectedCreateSendImageError {
    match error {
        CommittedImageValidationError::UnsupportedProfile => {
            ProtectedCreateSendImageError::UnsupportedProfile
        }
        _ => ProtectedCreateSendImageError::InvalidImage,
    }
}
