use std::fmt;

use storage::{BoundedStateStore, CatalogCommittedStore, SnapshotCatalogRecord};

use crate::{
    CommittedCheckpoint, CommittedImageError, CommittedImageValidationError, CommittedStreamId,
    DecodedCommittedImage, ValidatedCreateSendLayout17Image,
};

use super::{
    CommittedImageExportError, CommittedStateMachine,
    image_selection::{SelectionError, validate_selection},
};

mod batch;
mod planning;
pub use planning::{
    CreateSendImageExpectation, CreateSendReplacementCounts, CreateSendReplacementPlanError,
    PlannedCreateSendLayout17Replacement, PlannedCreateSendReplacement,
    plan_create_send_layout17_replacement, plan_create_send_replacement,
};

#[cfg(test)]
mod tests;

/// Explicitly trusted selection and exact target expectation for replacement.
///
/// Both full checkpoints and the complete-artifact SHA-256 digest come from the
/// caller's trusted mutation-selection policy, not inferred authorization from
/// supplied bytes. Stream identity names both source and target. This request
/// does not relabel an image or interpret its opaque membership or metadata.
///
/// The caller may explicitly select earlier progress. This domain capability
/// implements no ancestry, anti-rollback, authenticity, quorum, engine adoption,
/// or log-purge policy. Future native installation requires separate owner
/// admission and a durable state/catalog-before-purge barrier across owners.
///
/// ```compile_fail
/// fn duplicate(request: domain::TrustedCreateSendReplacement<'_>) {
///     let another = request.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn change_expectation(request: &mut domain::TrustedCreateSendReplacement<'_>) {
///     request.expected_target_checkpoint().membership().unwrap().payload.push(0);
/// }
/// ```
///
/// ```compile_fail
/// fn escape_bytes(request: domain::TrustedCreateSendReplacement<'_>) {
///     let bytes = request.artifact;
/// }
/// ```
pub struct TrustedCreateSendReplacement<'a> {
    stream: CommittedStreamId,
    target_checkpoint: &'a CommittedCheckpoint,
    selected_checkpoint: &'a CommittedCheckpoint,
    digest: [u8; 32],
    artifact: &'a [u8],
}

impl<'a> TrustedCreateSendReplacement<'a> {
    pub fn new(
        expected_stream: CommittedStreamId,
        expected_target_checkpoint: &'a CommittedCheckpoint,
        selected_checkpoint: &'a CommittedCheckpoint,
        complete_artifact_digest: [u8; 32],
        artifact: &'a [u8],
    ) -> Self {
        Self {
            stream: expected_stream,
            target_checkpoint: expected_target_checkpoint,
            selected_checkpoint,
            digest: complete_artifact_digest,
            artifact,
        }
    }

    pub fn expected_target_checkpoint(&self) -> &CommittedCheckpoint {
        self.target_checkpoint
    }

    pub fn selected_checkpoint(&self) -> &CommittedCheckpoint {
        self.selected_checkpoint
    }
}

impl fmt::Debug for TrustedCreateSendReplacement<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedCreateSendReplacement")
            .finish_non_exhaustive()
    }
}

/// Static replacement refusals, without source or backend diagnostics.
///
/// Semantic/profile failures on arbitrary opened targets are conservative
/// refusals, not certified corruption. Only physical target capture failure and
/// every returned commit error poison the machine. A commit error cannot prove
/// absence, even if its low-level cause describes a limit or invalid metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedImageReplacementError {
    #[error("the committed image replacement machine is poisoned")]
    Poisoned,
    #[error("the committed image replacement selection is invalid")]
    InvalidSelection,
    #[error("the committed image does not match the trusted replacement selection")]
    SelectionMismatch,
    #[error("the committed image replacement exceeds its logical limits")]
    LimitExceeded,
    #[error("the committed image replacement could not be allocated")]
    Allocation,
    #[error("the committed image replacement source profile is unsupported")]
    UnsupportedProfile,
    #[error("the committed image replacement source is invalid")]
    InvalidImage,
    #[error("the committed image replacement target checkpoint does not match")]
    TargetMismatch,
    #[error("the committed image replacement target profile is unsupported")]
    UnsupportedTargetProfile,
    #[error("the committed image replacement target is inconsistent with the requested profile")]
    InvalidTarget,
    #[error("the committed image replacement target could not be read")]
    TargetReadFailed,
    #[error("the committed image replacement commit decision is unknown")]
    CommitUnknown,
}

type Result<T> = std::result::Result<T, CommittedImageReplacementError>;

const _: () = {
    assert!(crate::MAX_COMMITTED_IMAGE_BYTES <= storage::MAX_CATALOG_ARTIFACT_BYTES);
};

impl<W> CommittedStateMachine<W>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    /// Atomically replaces initialized CreateSend state and its opaque catalog.
    ///
    /// Poison refusal precedes all input validation and target I/O. Catalog
    /// bounds and full trusted role2 source selection/semantics are checked before
    /// the sole complete bounded target capture, using the existing exporter.
    /// The expected old full checkpoint is compared only with that capture.
    /// There is no live checkpoint/init query, catalog read, ordinary snapshot,
    /// allocating fallback, pristine bootstrap, or second writer.
    ///
    /// Two linear ordered merge walks plan and copy stale-only Delete keys;
    /// every source row is then Put, including overlapping keys and original
    /// checkpoint/membership bytes. Mutation count is bounded by twice the
    /// existing row limit, and logical mutation payload by twice the artifact
    /// byte limit. Mutation/key/value reservations are fallible. The old encoded
    /// view and artifact are dropped before source Put copies are made.
    ///
    /// Exactly one commit_with_catalog uses the existing private writer and
    /// original selected artifact slice. Success leaves the live reader,
    /// private machine and stream unchanged: that reader observes the committed
    /// rows. No postcommit read, factory, reopen, constructor validation, or
    /// refresh is needed. Every commit error poisons, returns CommitUnknown,
    /// publishes no success, and forbids retry through this machine. Existing
    /// diagnostic checkpoint behavior remains unchanged.
    ///
    /// Source/raw-target/encoded-target can coexist during bounded export;
    /// source/Delete/Put/backend copies can coexist during commit. These are
    /// separate logical limits, not aggregate allocation or RSS bounds. Metadata,
    /// collection overhead, spare capacity, semantic/backend allocations and
    /// staging remain excluded. An encoded-target framing limit can refuse an
    /// otherwise bounded raw target conservatively. This is not all-path OOM
    /// safety, target provenance certification, or a compare-and-swap against
    /// trusted out-of-band mutation or a backend breaking its unique-writer
    /// contract. Constructor initialization is relied on, not re-attested.
    ///
    /// The initialized target may have only its initial or refusal checkpoint;
    /// a selected no-clock image is preserved exactly. Explicit trusted choice
    /// can replace with earlier progress; no anti-rollback or native adoption,
    /// runtime snapshot trait, transport, log, or purge authority is implied.
    ///
    /// ```compile_fail
    /// fn no_catalog_capability<W: storage::CommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    ///     request: domain::TrustedCreateSendReplacement<'_>,
    /// ) where W::Reader: storage::BoundedStateStore {
    ///     let _ = machine.replace_create_send_image_with_catalog(request, &[]);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_allocating_capture_fallback<W: storage::CatalogCommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    ///     request: domain::TrustedCreateSendReplacement<'_>,
    /// ) {
    ///     let _ = machine.replace_create_send_image_with_catalog(request, &[]);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_reader_target(
    ///     reader: storage::MemoryStore,
    ///     request: domain::TrustedCreateSendReplacement<'_>,
    /// ) {
    ///     let _ = domain::CommittedStateMachine::replace_create_send_image_with_catalog(
    ///         &mut reader, request, &[],
    ///     );
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_simultaneous_machine_owner<W>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    ///     request: domain::TrustedCreateSendReplacement<'_>,
    ///     update: &domain::CommittedCheckpointUpdate,
    ///     work: &domain::CommittedQueueWork,
    /// ) where W: storage::CatalogCommittedStore, W::Reader: storage::BoundedStateStore {
    ///     let held = &mut *machine;
    ///     let _ = machine.apply_committed(update, work);
    ///     let _ = held.replace_create_send_image_with_catalog(request, &[]);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_raw_writer<W: storage::CatalogCommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    /// ) {
    ///     let writer = &mut machine.writer;
    /// }
    /// ```
    pub fn replace_create_send_image_with_catalog(
        &mut self,
        request: TrustedCreateSendReplacement<'_>,
        metadata: &[u8],
    ) -> Result<()> {
        if self.poisoned {
            return Err(CommittedImageReplacementError::Poisoned);
        }
        let catalog = SnapshotCatalogRecord::new(metadata, request.artifact)
            .map_err(|_| CommittedImageReplacementError::LimitExceeded)?;
        if request.stream != self.stream || request.target_checkpoint.stream() != self.stream {
            return Err(CommittedImageReplacementError::InvalidSelection);
        }
        let selected = validate_selection(
            request.stream,
            request.selected_checkpoint,
            request.digest,
            request.artifact,
        )
        .map_err(selection_error)?;
        let old_artifact = self
            .export_create_send_image()
            .map_err(target_export_error)?;
        let old = DecodedCommittedImage::decode(old_artifact.as_bytes())
            .map_err(target_container_error)?;
        if old.checkpoint() != request.target_checkpoint {
            return Err(CommittedImageReplacementError::TargetMismatch);
        }
        let old =
            ValidatedCreateSendLayout17Image::validate(old).map_err(target_validation_error)?;
        let plan = batch::plan_rows(
            old.rows(),
            selected.rows(),
            old.row_count(),
            selected.row_count(),
        )?;
        let mut batch = batch::copy_delete_rows(old.rows(), selected.rows(), &plan)?;
        drop(old);
        drop(old_artifact);
        batch::copy_put_rows(&mut batch, selected.rows(), &plan)?;
        if self.writer.commit_with_catalog(batch, catalog).is_err() {
            self.poisoned = true;
            return Err(CommittedImageReplacementError::CommitUnknown);
        }
        Ok(())
    }
}

fn selection_error(error: SelectionError) -> CommittedImageReplacementError {
    match error {
        SelectionError::InvalidSelection => CommittedImageReplacementError::InvalidSelection,
        SelectionError::SelectionMismatch => CommittedImageReplacementError::SelectionMismatch,
        SelectionError::LimitExceeded => CommittedImageReplacementError::LimitExceeded,
        SelectionError::Allocation => CommittedImageReplacementError::Allocation,
        SelectionError::UnsupportedProfile => CommittedImageReplacementError::UnsupportedProfile,
        SelectionError::InvalidImage => CommittedImageReplacementError::InvalidImage,
    }
}

fn target_container_error(error: CommittedImageError) -> CommittedImageReplacementError {
    match error {
        CommittedImageError::LimitExceeded => CommittedImageReplacementError::LimitExceeded,
        CommittedImageError::Allocation => CommittedImageReplacementError::Allocation,
        CommittedImageError::UnsupportedFormat => {
            CommittedImageReplacementError::UnsupportedTargetProfile
        }
        _ => CommittedImageReplacementError::InvalidTarget,
    }
}

fn target_export_error(error: CommittedImageExportError) -> CommittedImageReplacementError {
    match error {
        CommittedImageExportError::Poisoned => CommittedImageReplacementError::Poisoned,
        CommittedImageExportError::ReadFailed => CommittedImageReplacementError::TargetReadFailed,
        CommittedImageExportError::LimitExceeded => CommittedImageReplacementError::LimitExceeded,
        CommittedImageExportError::Allocation => CommittedImageReplacementError::Allocation,
        CommittedImageExportError::UnsupportedProfile => {
            CommittedImageReplacementError::UnsupportedTargetProfile
        }
        CommittedImageExportError::InvalidImage => CommittedImageReplacementError::InvalidTarget,
    }
}

fn target_validation_error(error: CommittedImageValidationError) -> CommittedImageReplacementError {
    match error {
        CommittedImageValidationError::UnsupportedProfile => {
            CommittedImageReplacementError::UnsupportedTargetProfile
        }
        _ => CommittedImageReplacementError::InvalidTarget,
    }
}
