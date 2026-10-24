//! Typed offline capture installation. No transport parsing, trust decision,
//! historical anti-rollback authority, replay, runtime activation or repair.

use std::sync::Arc;

use storage::{
    ImageInstallBackend, ImageInstallError, ImageInstallLimits, ImageInstallReceipt,
    OfflineImageOwner, SnapshotProvenance, SnapshotWithProvenance,
};
use thiserror::Error;

use super::{
    SnapshotCapture, SnapshotCaptureError, SnapshotFormat, SnapshotLimits, SnapshotManifest,
    capture::validate_image,
};

/// Cross-backend acceptance is a deliberate caller policy, never a fallback
/// after failed native target inspection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SnapshotInstallPolicy {
    #[default]
    MatchingProvenance,
    AllowCrossBackend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotInstallLimits {
    pub snapshot: SnapshotLimits,
    pub replacement: ImageInstallLimits,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotInstallError {
    #[error(transparent)]
    Storage(#[from] ImageInstallError),
    #[error("old target validation failed: {0}")]
    Target(#[from] SnapshotCaptureError),
    #[error("incoming capture exceeds install frame limits: {0}")]
    Incoming(super::SnapshotFormatError),
    #[error("source and target provenance classes differ; explicit opt-in required")]
    ProvenanceMismatch,
}

/// Operational receipt, not a recovery/authentication/physical durability
/// certificate. Source evidence never becomes native source evidence merely
/// because a checked native target accepted its logical records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotInstallReceipt {
    pub source_provenance: SnapshotProvenance,
    pub source_manifest: SnapshotManifest,
    pub old_target_manifest: SnapshotManifest,
    pub replacement: ImageInstallReceipt,
}

/// Owns an offline target without exposing a clone or stale higher-level owner.
/// The caller must exclude ALL other writers/raw clones/topology mutators and
/// preexisting domain/journal/indexed/replay owners throughout the operation.
#[derive(Debug)]
pub struct SnapshotInstallOwner<S: ImageInstallBackend> {
    target: OfflineImageOwner<S>,
}

impl<S: ImageInstallBackend> SnapshotInstallOwner<S> {
    pub fn new(store: S) -> Self {
        Self {
            target: OfflineImageOwner::new(store),
        }
    }

    pub fn is_retired(&self) -> bool {
        self.target.is_retired()
    }

    pub fn snapshot(&self) -> Result<SnapshotWithProvenance, SnapshotInstallError> {
        Ok(self.target.snapshot()?)
    }

    pub fn into_store(self) -> Result<S, SnapshotInstallError> {
        Ok(self.target.into_store()?)
    }

    /// Only a typed capture is accepted. Trust/authentication and historical
    /// authority are still the caller's responsibility. Both old and incoming
    /// images are bounded before semantic validation and batch construction.
    pub fn install(
        &mut self,
        capture: &SnapshotCapture,
        limits: SnapshotInstallLimits,
        policy: SnapshotInstallPolicy,
    ) -> Result<SnapshotInstallReceipt, SnapshotInstallError> {
        let inspection = self
            .target
            .inspect(capture.image().snapshot(), limits.replacement)?;
        let source = *capture.provenance();
        let target = *inspection.image().provenance();
        let same_class = matches!(
            (source, target),
            (
                SnapshotProvenance::LogicalOnly,
                SnapshotProvenance::LogicalOnly
            ) | (SnapshotProvenance::Fjall(_), SnapshotProvenance::Fjall(_))
        );
        if !same_class && policy == SnapshotInstallPolicy::MatchingProvenance {
            return Err(SnapshotInstallError::ProvenanceMismatch);
        }
        SnapshotFormat::preflight(
            capture.manifest().latest_applied_digest().is_some(),
            capture.image().snapshot().entries(),
            limits.snapshot,
        )
        .map_err(SnapshotInstallError::Incoming)?;
        let old_target_manifest = validate_image(Arc::clone(inspection.image()), limits.snapshot)?;
        let receipt = SnapshotInstallReceipt {
            source_provenance: source,
            source_manifest: capture.manifest(),
            old_target_manifest,
            replacement: inspection.receipt(),
        };
        inspection.prepare()?.apply()?;
        Ok(receipt)
    }
}
