//! Read-only classification of one offline image. No repair, replay, promotion,
//! trust decision or runtime activation; selection is not a recovery certificate.

use std::sync::Arc;

use storage::{ImageInstallBackend, SnapshotProvenance, SnapshotWithProvenance};

use super::anchor::SnapshotAnchor;
use super::capture::validate_image;
use super::{SnapshotCaptureError, SnapshotLimits, SnapshotManifest};
use crate::journal::anchor_key;

/// Which prefix the validated image retains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRecoveryClass {
    /// No anchor: every entry from 1 is retained.
    LegacyUnanchored,
    /// Anchored with B < A: entries B+1..=L, including the original at A.
    Anchored,
    /// Anchored with B = A: entries A+1..=L; the anchor holds the original.
    Compacted,
}

/// Facts about one immutable image, not authentication, historical authority
/// or proof that a later reopen observes the same records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRecoverySelection {
    pub class: SnapshotRecoveryClass,
    pub provenance: SnapshotProvenance,
    /// Current P, C and L with the indexed identity at P.
    pub manifest: SnapshotManifest,
    /// A; absent for legacy images.
    pub anchor_applied_index: Option<u64>,
    /// B; zero for legacy images.
    pub removed_through: u64,
}

/// Inspects the sealed backend once and validates that complete image exactly as
/// capture does. Every invalid, gapped, divergent or corrupt image refuses.
pub fn select_recovery<S: ImageInstallBackend>(
    store: &S,
    limits: SnapshotLimits,
) -> Result<SnapshotRecoverySelection, SnapshotCaptureError> {
    let image = Arc::new(store.inspect_image()?);
    let (manifest, anchor) = validated_anchor(Arc::clone(&image), limits)?;
    let removed_through = anchor.as_ref().map_or(0, SnapshotAnchor::removed_through);
    let anchor_applied_index = anchor.as_ref().map(SnapshotAnchor::applied_index);
    let class = match anchor_applied_index {
        None => SnapshotRecoveryClass::LegacyUnanchored,
        Some(applied) if removed_through == applied => SnapshotRecoveryClass::Compacted,
        Some(_) => SnapshotRecoveryClass::Anchored,
    };
    Ok(SnapshotRecoverySelection {
        class,
        provenance: *image.provenance(),
        manifest,
        anchor_applied_index,
        removed_through,
    })
}

/// The anchor row of precisely this validated image, never another read.
pub(super) fn validated_anchor(
    image: Arc<SnapshotWithProvenance>,
    limits: SnapshotLimits,
) -> Result<(SnapshotManifest, Option<SnapshotAnchor>), SnapshotCaptureError> {
    let manifest = validate_image(Arc::clone(&image), limits)?;
    let records = image.snapshot().entries();
    let anchor_key = anchor_key();
    let anchor = records
        .binary_search_by(|(key, _)| key.cmp(&anchor_key))
        .ok()
        .map(|index| SnapshotAnchor::decode(&records[index].1))
        .transpose()?;
    Ok((manifest, anchor))
}
