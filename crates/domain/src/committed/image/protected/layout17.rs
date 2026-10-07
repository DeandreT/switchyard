//! Current layout17 protected agreement; no store or mutation capability.

use crate::ValidatedCreateSendLayout17Image;

use super::*;

#[cfg(test)]
mod tests;

/// Immutable agreement over a caller-owned capture and current role2 image.
///
/// Only the closed generation-one NonFinite image profile is supported. This
/// does not certify owner health, history, selection, source authenticity,
/// ancestry, compare-and-swap, publication, installation, or finite-capacity
/// admission. The capture and image stay borrowed and can immediately be stale.
/// Expected scalars are checked before recovery; complete business validation
/// precedes full checkpoint/length/whole-SHA identity, opaque fence, and exact
/// row equality. The historical role1 check remains a separate pure API.
///
/// ```compile_fail
/// fn duplicate(view: domain::CheckedProtectedCreateSendLayout17Image<'_>) {
///     let _ = view.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct<'a>(state: &'a storage::StoredProtectedState,
///     image: domain::ValidatedCreateSendLayout17Image<'a>)
///     -> domain::CheckedProtectedCreateSendLayout17Image<'a> {
///     domain::CheckedProtectedCreateSendLayout17Image { state, image }
/// }
/// ```
///
/// ```compile_fail
/// fn batch(view: domain::CheckedProtectedCreateSendLayout17Image<'_>) -> storage::WriteBatch {
///     view.into()
/// }
/// ```
pub struct CheckedProtectedCreateSendLayout17Image<'a> {
    state: &'a StoredProtectedState,
    image: ValidatedCreateSendLayout17Image<'a>,
}

impl<'a> CheckedProtectedCreateSendLayout17Image<'a> {
    pub fn protected_state(&self) -> &'a StoredProtectedState {
        self.state
    }
    pub fn image(&self) -> &ValidatedCreateSendLayout17Image<'a> {
        &self.image
    }
}

impl fmt::Debug for CheckedProtectedCreateSendLayout17Image<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let live = self.state.live_catalog();
        formatter
            .debug_struct("CheckedProtectedCreateSendLayout17Image")
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

/// Checks the current role2 image and full protected-capture agreement.
///
/// This is a pure agreement API, not originating-store or writer
/// authority. It inherits the capture DTO's bounded shape, decoder/checkpoint
/// allocation behavior, lazy comparison ordering, and ordinary allocation
/// exclusions of the historical agreement function.
pub fn check_protected_create_send_layout17_image<'a>(
    state: &'a StoredProtectedState,
    expected: &CreateSendImageExpectation<'_>,
    expected_fence: &[u8],
) -> Result<CheckedProtectedCreateSendLayout17Image<'a>> {
    let artifact = captured_artifact(state, expected, expected_fence)?;
    let decoded = DecodedCommittedImage::decode(artifact).map_err(container_error)?;
    let image = ValidatedCreateSendLayout17Image::validate(decoded).map_err(business_error)?;
    check_agreement(
        state,
        expected,
        expected_fence,
        image.checkpoint(),
        image.rows(),
        image.row_count(),
    )?;
    Ok(CheckedProtectedCreateSendLayout17Image { state, image })
}
