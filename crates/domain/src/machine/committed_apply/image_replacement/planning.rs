//! Pure checked data and counts; no target capture or mutation authority.

use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    CommittedCheckpoint, CommittedImageError, CommittedImageRows, CommittedImageValidationError,
    DecodedCommittedImage, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_MEMBERSHIP_BYTES,
    ValidatedCreateSendImage,
};

use super::{CommittedImageReplacementError, batch};

#[cfg(test)]
mod tests;

mod layout17;
pub use layout17::{PlannedCreateSendLayout17Replacement, plan_create_send_layout17_replacement};

/// Independently offered identity data, not trust or a replacement permit.
pub struct CreateSendImageExpectation<'a> {
    pub checkpoint: &'a CommittedCheckpoint,
    pub artifact_bytes: usize,
    pub artifact_sha256: [u8; 32],
}

impl fmt::Debug for CreateSendImageExpectation<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateSendImageExpectation")
            .field("artifact_bytes", &self.artifact_bytes)
            .finish_non_exhaustive()
    }
}

/// Conservative logical Delete/Put counts, not allocated or committed bytes.
///
/// Every SELECTED row is a Put, even in an exact body no-op. Only OLD-only keys
/// are Deletes. Framing, catalog, selection-fence and log writes are excluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateSendReplacementCounts {
    pub delete_rows: usize,
    pub put_rows: usize,
    pub total_mutations: usize,
    pub logical_payload_bytes: usize,
}

/// Checked immutable image views borrowing only the two offered artifacts.
///
/// Both full business images and independent full checkpoint/length/whole SHA
/// identities are checked. Neither expectations nor their checkpoint storage
/// are retained. The result is not proof of a current target, provenance,
/// history, ancestry, native membership, physical completeness or authority.
///
/// ```compile_fail
/// fn duplicate(plan: domain::PlannedCreateSendReplacement<'_, '_>) {
///     let _ = plan.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct<'a, 'b>(
///     old: &'a [u8], selected: &'b [u8],
///     old_image: domain::ValidatedCreateSendImage<'a>,
///     selected_image: domain::ValidatedCreateSendImage<'b>,
///     counts: domain::CreateSendReplacementCounts,
/// ) -> domain::PlannedCreateSendReplacement<'a, 'b> {
///     domain::PlannedCreateSendReplacement {
///         old_artifact: old, selected_artifact: selected,
///         old_image, selected_image, counts,
///     }
/// }
/// ```
///
/// ```compile_fail
/// fn mutate(
///     old: &mut Vec<u8>, selected: &[u8],
///     expected: &domain::CreateSendImageExpectation<'_>,
/// ) {
///     let plan = domain::plan_create_send_replacement(old, selected, expected, expected).unwrap();
///     old[0] = 0;
///     let _ = plan.counts();
/// }
/// ```
///
/// ```compile_fail
/// fn batch(plan: domain::PlannedCreateSendReplacement<'_, '_>) -> storage::WriteBatch {
///     plan.into()
/// }
/// ```
///
/// ```compile_fail
/// fn commit(plan: domain::PlannedCreateSendReplacement<'_, '_>) {
///     let _ = plan.commit();
/// }
/// ```
///
/// ```compile_fail
/// fn escape(expected: &domain::CreateSendImageExpectation<'_>)
///     -> domain::PlannedCreateSendReplacement<'static, 'static>
/// {
///     let old = Vec::new();
///     domain::plan_create_send_replacement(&old, &[], expected, expected).unwrap()
/// }
/// ```
pub struct PlannedCreateSendReplacement<'old, 'selected> {
    old_artifact: &'old [u8],
    selected_artifact: &'selected [u8],
    old_image: ValidatedCreateSendImage<'old>,
    selected_image: ValidatedCreateSendImage<'selected>,
    counts: CreateSendReplacementCounts,
}

impl<'old, 'selected> PlannedCreateSendReplacement<'old, 'selected> {
    pub fn old_artifact_bytes(&self) -> &'old [u8] {
        self.old_artifact
    }

    pub fn selected_artifact_bytes(&self) -> &'selected [u8] {
        self.selected_artifact
    }

    pub fn old_image(&self) -> &ValidatedCreateSendImage<'old> {
        &self.old_image
    }

    pub fn selected_image(&self) -> &ValidatedCreateSendImage<'selected> {
        &self.selected_image
    }

    pub fn counts(&self) -> CreateSendReplacementCounts {
        self.counts
    }
}

impl fmt::Debug for PlannedCreateSendReplacement<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannedCreateSendReplacement")
            .field("old_artifact_bytes", &self.old_artifact.len())
            .field("selected_artifact_bytes", &self.selected_artifact.len())
            .field("counts", &self.counts)
            .finish_non_exhaustive()
    }
}

/// Static data refusals, never physical read, poison or commit diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CreateSendReplacementPlanError {
    #[error("the image count plan exceeds its logical limits")]
    LimitExceeded,
    #[error("the image count plan could not be allocated")]
    Allocation,
    #[error("the image count plan expectations are invalid")]
    InvalidExpectation,
    #[error("the image count plan profile is unsupported")]
    UnsupportedProfile,
    #[error("the image count plan image is invalid")]
    InvalidImage,
    #[error("the image does not match its independent expected identity")]
    IdentityMismatch,
    #[error("the image pair is outside the count plan policy")]
    UnsupportedPairPolicy,
}

type Result<T> = std::result::Result<T, CreateSendReplacementPlanError>;

/// Validates two offered images and reports a bounded read-only count plan.
///
/// Borrowed length checks precede recovery and hashes. Expected streams are
/// checked next; OLD then SELECTED complete container/business validation
/// precedes external identity checks. Identity comparison is lazy full CP,
/// complete length, whole SHA for OLD, then SELECTED. Noninitial, member-bearing
/// pairs are required; member bytes remain opaque. Earlier SELECTED progress is
/// only data, never permission to replace or discard history. No store is read.
///
/// ```no_run
/// use sha2::{Digest, Sha256};
///
/// fn inspect_counts(
///     old: &[u8], selected: &[u8],
///     old_checkpoint: domain::CommittedCheckpoint,
///     selected_checkpoint: domain::CommittedCheckpoint,
/// ) -> Result<domain::CreateSendReplacementCounts, domain::CreateSendReplacementPlanError> {
///     let plan: domain::PlannedCreateSendReplacement<'_, '_> = {
///         let old_checkpoint = old_checkpoint;
///         let selected_checkpoint = selected_checkpoint;
///         let old_expected = domain::CreateSendImageExpectation {
///             checkpoint: &old_checkpoint, artifact_bytes: old.len(),
///             artifact_sha256: Sha256::digest(old).into(),
///         };
///         let selected_expected = domain::CreateSendImageExpectation {
///             checkpoint: &selected_checkpoint, artifact_bytes: selected.len(),
///             artifact_sha256: Sha256::digest(selected).into(),
///         };
///         domain::plan_create_send_replacement(old, selected, &old_expected, &selected_expected)?
///     };
///     let _: &[u8] = plan.old_artifact_bytes();
///     let _: &[u8] = plan.selected_artifact_bytes();
///     let rows = [plan.old_image().rows().next(), plan.selected_image().rows().next()];
///     for row in rows.into_iter().flatten() {
///         let _: (&[u8], &[u8]) = (row.key(), row.value());
///     }
///     let _: (&domain::CommittedCheckpoint, &domain::CommittedCheckpoint) =
///         (plan.old_image().checkpoint(), plan.selected_image().checkpoint());
///     let counts = plan.counts();
///     let _ = (counts.delete_rows, counts.put_rows, counts.total_mutations,
///         counts.logical_payload_bytes);
///     Ok(counts)
/// }
/// ```
pub fn plan_create_send_replacement<'old, 'selected>(
    old_artifact: &'old [u8],
    selected_artifact: &'selected [u8],
    old_expected: &CreateSendImageExpectation<'_>,
    selected_expected: &CreateSendImageExpectation<'_>,
) -> Result<PlannedCreateSendReplacement<'old, 'selected>> {
    check_expectations(
        old_artifact.len(),
        selected_artifact.len(),
        old_expected,
        selected_expected,
    )?;
    let old_image = validate(old_artifact)?;
    let selected_image = validate(selected_artifact)?;
    check_identity(old_image.checkpoint(), old_artifact, old_expected)?;
    check_identity(
        selected_image.checkpoint(),
        selected_artifact,
        selected_expected,
    )?;
    check_pair(old_image.checkpoint(), selected_image.checkpoint())?;
    let counts = count_pair(
        old_image.rows(),
        selected_image.rows(),
        old_image.row_count(),
        selected_image.row_count(),
    )?;
    Ok(PlannedCreateSendReplacement {
        old_artifact,
        selected_artifact,
        old_image,
        selected_image,
        counts,
    })
}

fn check_expectations(
    old_bytes: usize,
    selected_bytes: usize,
    old_expected: &CreateSendImageExpectation<'_>,
    selected_expected: &CreateSendImageExpectation<'_>,
) -> Result<()> {
    check_shape(old_bytes, selected_bytes, old_expected, selected_expected)?;
    old_expected
        .checkpoint
        .stream()
        .validate()
        .map_err(|_| CreateSendReplacementPlanError::InvalidExpectation)?;
    selected_expected
        .checkpoint
        .stream()
        .validate()
        .map_err(|_| CreateSendReplacementPlanError::InvalidExpectation)?;
    if old_expected.checkpoint.stream() != selected_expected.checkpoint.stream() {
        return Err(CreateSendReplacementPlanError::InvalidExpectation);
    }
    Ok(())
}

fn check_pair(old: &CommittedCheckpoint, selected: &CommittedCheckpoint) -> Result<()> {
    for checkpoint in [old, selected] {
        if checkpoint.last().is_none() || checkpoint.membership().is_none() {
            return Err(CreateSendReplacementPlanError::UnsupportedPairPolicy);
        }
    }
    Ok(())
}

fn count_pair(
    old: CommittedImageRows<'_>,
    selected: CommittedImageRows<'_>,
    old_count: usize,
    selected_count: usize,
) -> Result<CreateSendReplacementCounts> {
    let counted =
        batch::count_rows(old, selected, old_count, selected_count).map_err(count_error)?;
    let (delete_rows, put_rows, total_mutations, logical_payload_bytes) = counted.counts();
    Ok(CreateSendReplacementCounts {
        delete_rows,
        put_rows,
        total_mutations,
        logical_payload_bytes,
    })
}

fn check_shape(
    old_bytes: usize,
    selected_bytes: usize,
    old_expected: &CreateSendImageExpectation<'_>,
    selected_expected: &CreateSendImageExpectation<'_>,
) -> Result<()> {
    if [
        old_bytes,
        selected_bytes,
        old_expected.artifact_bytes,
        selected_expected.artifact_bytes,
    ]
    .into_iter()
    .any(|bytes| bytes > MAX_COMMITTED_IMAGE_BYTES)
        || [old_expected, selected_expected]
            .into_iter()
            .any(|expected| {
                expected.checkpoint.membership().is_some_and(|membership| {
                    membership.payload.len() > MAX_COMMITTED_MEMBERSHIP_BYTES
                })
            })
    {
        return Err(CreateSendReplacementPlanError::LimitExceeded);
    }
    Ok(())
}

fn validate(artifact: &[u8]) -> Result<ValidatedCreateSendImage<'_>> {
    let decoded = DecodedCommittedImage::decode(artifact).map_err(container_error)?;
    ValidatedCreateSendImage::validate(decoded).map_err(business_error)
}

fn check_identity(
    checkpoint: &CommittedCheckpoint,
    artifact: &[u8],
    expected: &CreateSendImageExpectation<'_>,
) -> Result<()> {
    if checkpoint != expected.checkpoint
        || artifact.len() != expected.artifact_bytes
        || <[u8; 32]>::from(Sha256::digest(artifact)) != expected.artifact_sha256
    {
        return Err(CreateSendReplacementPlanError::IdentityMismatch);
    }
    Ok(())
}

fn container_error(error: CommittedImageError) -> CreateSendReplacementPlanError {
    match error {
        CommittedImageError::LimitExceeded => CreateSendReplacementPlanError::LimitExceeded,
        CommittedImageError::Allocation => CreateSendReplacementPlanError::Allocation,
        CommittedImageError::UnsupportedFormat => {
            CreateSendReplacementPlanError::UnsupportedProfile
        }
        _ => CreateSendReplacementPlanError::InvalidImage,
    }
}

fn business_error(error: CommittedImageValidationError) -> CreateSendReplacementPlanError {
    match error {
        CommittedImageValidationError::UnsupportedProfile => {
            CreateSendReplacementPlanError::UnsupportedProfile
        }
        _ => CreateSendReplacementPlanError::InvalidImage,
    }
}

fn count_error(error: CommittedImageReplacementError) -> CreateSendReplacementPlanError {
    match error {
        CommittedImageReplacementError::LimitExceeded => {
            CreateSendReplacementPlanError::LimitExceeded
        }
        _ => CreateSendReplacementPlanError::InvalidImage,
    }
}
