//! Pure paired role2 validation and counts, not target or mutation authority.

use crate::ValidatedCreateSendLayout17Image;

use super::*;

#[cfg(test)]
mod tests;

/// Immutable role2 images and bounded logical replacement counts.
///
/// Both offered images must satisfy the closed generation-one NonFinite
/// business profile. Inputs remain borrowed; independent expectation storage
/// is not retained. This is not current-target evidence, selection authority,
/// provenance, history, ancestry, physical completeness, native membership
/// interpretation, installed finite-capacity admission, or a WriteBatch.
/// Counts include actual mode rows and are logical Delete/Put costs, not RSS.
/// The historical role1 planner remains a separate pure API.
///
/// ```compile_fail
/// fn duplicate(plan: domain::PlannedCreateSendLayout17Replacement<'_, '_>) {
///     let _ = plan.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct<'a, 'b>(old_artifact: &'a [u8], selected_artifact: &'b [u8],
///     old_image: domain::ValidatedCreateSendLayout17Image<'a>,
///     selected_image: domain::ValidatedCreateSendLayout17Image<'b>,
///     counts: domain::CreateSendReplacementCounts)
///     -> domain::PlannedCreateSendLayout17Replacement<'a, 'b> {
///     domain::PlannedCreateSendLayout17Replacement {
///         old_artifact, selected_artifact, old_image, selected_image, counts,
///     }
/// }
/// ```
///
/// ```compile_fail
/// fn batch(plan: domain::PlannedCreateSendLayout17Replacement<'_, '_>) -> storage::WriteBatch {
///     plan.into()
/// }
/// ```
pub struct PlannedCreateSendLayout17Replacement<'old, 'selected> {
    old_artifact: &'old [u8],
    selected_artifact: &'selected [u8],
    old_image: ValidatedCreateSendLayout17Image<'old>,
    selected_image: ValidatedCreateSendLayout17Image<'selected>,
    counts: CreateSendReplacementCounts,
}

impl<'old, 'selected> PlannedCreateSendLayout17Replacement<'old, 'selected> {
    pub fn old_artifact_bytes(&self) -> &'old [u8] {
        self.old_artifact
    }
    pub fn selected_artifact_bytes(&self) -> &'selected [u8] {
        self.selected_artifact
    }
    pub fn old_image(&self) -> &ValidatedCreateSendLayout17Image<'old> {
        &self.old_image
    }
    pub fn selected_image(&self) -> &ValidatedCreateSendLayout17Image<'selected> {
        &self.selected_image
    }
    pub fn counts(&self) -> CreateSendReplacementCounts {
        self.counts
    }
}

impl fmt::Debug for PlannedCreateSendLayout17Replacement<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannedCreateSendLayout17Replacement")
            .field("old_artifact_bytes", &self.old_artifact.len())
            .field("selected_artifact_bytes", &self.selected_artifact.len())
            .field("counts", &self.counts)
            .finish_non_exhaustive()
    }
}

/// Validates full role2 images and independent identities before counting.
///
/// Borrowed scalar limits and expected streams precede OLD then SELECTED
/// container/business validation. Lazy full CP/length/whole-SHA checks run OLD
/// then SELECTED, followed by the same noninitial/member-bearing pair policy
/// and ordered count walk as role1. Member bytes stay opaque; earlier selected
/// progress is data, never permission to discard history. No store is read.
pub fn plan_create_send_layout17_replacement<'old, 'selected>(
    old_artifact: &'old [u8],
    selected_artifact: &'selected [u8],
    old_expected: &CreateSendImageExpectation<'_>,
    selected_expected: &CreateSendImageExpectation<'_>,
) -> Result<PlannedCreateSendLayout17Replacement<'old, 'selected>> {
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
    Ok(PlannedCreateSendLayout17Replacement {
        old_artifact,
        selected_artifact,
        old_image,
        selected_image,
        counts,
    })
}

fn validate(artifact: &[u8]) -> Result<ValidatedCreateSendLayout17Image<'_>> {
    let decoded = DecodedCommittedImage::decode(artifact).map_err(container_error)?;
    ValidatedCreateSendLayout17Image::validate(decoded).map_err(business_error)
}
