use std::fmt;

use storage::{CommittedStore, StateStore, WriteBatch};

use crate::{
    CommittedCheckpoint, CommittedStreamId, StateMachine, ValidatedCreateSendLayout17Image,
};

use super::{
    CommittedStateMachine,
    image_selection::{self, SelectionError},
};

/// Deliberately trusted, immutable selection for a pristine-target bootstrap.
///
/// The expected checkpoint and digest must come from the caller's trusted
/// selection policy, not be inferred as authorization from untrusted bytes.
/// The digest is SHA-256 of the complete artifact, including its checksum.
/// Stream identity, checkpoint equality, and digest equality pin this request;
/// they do not establish source provenance, ancestry, authenticity, or quorum.
/// The separately consumed unique writer supplies mutation authority.
///
/// The selected stream names both source and target. This API does not relabel
/// an image, rewrite its checkpoint, or install into an initialized target.
pub struct TrustedCreateSendBootstrap<'a> {
    stream: CommittedStreamId,
    checkpoint: &'a CommittedCheckpoint,
    digest: [u8; 32],
    artifact: &'a [u8],
}

impl<'a> TrustedCreateSendBootstrap<'a> {
    pub fn new(
        expected_stream: CommittedStreamId,
        expected_checkpoint: &'a CommittedCheckpoint,
        expected_digest: [u8; 32],
        artifact: &'a [u8],
    ) -> Self {
        Self {
            stream: expected_stream,
            checkpoint: expected_checkpoint,
            digest: expected_digest,
            artifact,
        }
    }

    /// Borrow the immutable selected expectation, not artifact validity or
    /// evidence of commitment, provenance, or installation authority.
    pub fn expected_checkpoint(&self) -> &CommittedCheckpoint {
        self.checkpoint
    }

    /// Borrow the immutable selected artifact bytes, still unvalidated here.
    ///
    /// This view is not proof of the trusted expectation, source validity,
    /// commitment, provenance, ancestry, or installation authority. It grants
    /// no mutation capability and makes no copy of the selected artifact.
    pub fn artifact_bytes(&self) -> &'a [u8] {
        self.artifact
    }
}

impl fmt::Debug for TrustedCreateSendBootstrap<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedCreateSendBootstrap")
            .finish_non_exhaustive()
    }
}

/// Static bootstrap failures, without source or backend diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedImageBootstrapError {
    #[error("the committed image bootstrap selection is invalid")]
    InvalidSelection,
    #[error("the committed image does not match the trusted bootstrap selection")]
    SelectionMismatch,
    #[error("the committed image exceeds bootstrap limits")]
    LimitExceeded,
    #[error("the committed image bootstrap batch could not be allocated")]
    Allocation,
    #[error("the committed image bootstrap profile is unsupported")]
    UnsupportedProfile,
    #[error("the committed image bootstrap source is invalid")]
    InvalidImage,
    #[error("the committed image bootstrap target is not pristine")]
    TargetNotPristine,
    #[error("the committed image bootstrap target could not be read")]
    TargetReadFailed,
    #[error("the committed image bootstrap commit decision is unknown")]
    CommitUnknown,
}

type Result<T> = std::result::Result<T, CommittedImageBootstrapError>;

mod catalog;

impl<W: CommittedStore> CommittedStateMachine<W> {
    /// Atomically seeds only a pristine replica with the exact selected image.
    ///
    /// Full current role2 source validation precedes target I/O. Historical
    /// role1 selections are refused before any target probe. The unique writer then
    /// proves the target uninitialized and empty. Every selected row, including
    /// the original checkpoint bytes, is copied into one Put-only batch before
    /// its sole commit. That commit also initializes the target atomically.
    /// There is no postcommit read, apply, normalization, delete, or fallback.
    /// The ordinary limit-one target probe bounds row count, not value bytes or
    /// backend allocation; it preserves the existing constructor contract.
    /// The caller has already opened the writer; source-before-target-I/O refers
    /// to this function, not backend opening or earlier caller operations.
    ///
    /// A physical commit error may follow the entire durable commit. The writer
    /// is consumed and no usable machine is returned; reopen and inspect actual
    /// state rather than automatically retrying or assuming rollback.
    ///
    /// Artifact bytes and copied batch rows can coexist, each with a 64 MiB
    /// logical bound. Metadata, spare capacity, backend staging, and RSS are
    /// excluded. Row and mutation copies reserve fallibly; this is not a claim
    /// that every allocation inside semantic validation or a backend is fallible.
    ///
    /// This is a domain bootstrap, not OpenRaft snapshot installation. It adds
    /// no persisted snapshot catalog, populated-target replacement, membership
    /// interpretation, runtime adoption, log purge, or rollback authorization.
    ///
    /// ```compile_fail
    /// fn a_reader_is_not_a_target_writer(
    ///     reader: storage::MemoryStore,
    ///     selection: domain::TrustedCreateSendBootstrap<'_>,
    /// ) {
    ///     let _ = domain::CommittedStateMachine::bootstrap_create_send_image(reader, selection);
    /// }
    /// ```
    pub fn bootstrap_create_send_image(
        mut writer: W,
        request: TrustedCreateSendBootstrap<'_>,
    ) -> Result<Self> {
        let image = validate_selection(&request)?;
        let reader = pristine_reader(&writer)?;
        let batch = copy_rows(&image)?;
        writer
            .commit(batch)
            .map_err(|_| CommittedImageBootstrapError::CommitUnknown)?;
        Ok(Self {
            writer,
            machine: StateMachine::new(reader),
            stream: request.stream,
            poisoned: false,
        })
    }
}

fn validate_selection<'a>(
    request: &TrustedCreateSendBootstrap<'a>,
) -> Result<ValidatedCreateSendLayout17Image<'a>> {
    image_selection::validate_selection(
        request.stream,
        request.checkpoint,
        request.digest,
        request.artifact,
    )
    .map_err(selection_error)
}

fn pristine_reader<W: CommittedStore>(writer: &W) -> Result<W::Reader> {
    let reader = writer.reader();
    if writer
        .is_initialized()
        .map_err(|_| CommittedImageBootstrapError::TargetReadFailed)?
        || !reader
            .scan_prefix(&[], 1)
            .map_err(|_| CommittedImageBootstrapError::TargetReadFailed)?
            .is_empty()
    {
        return Err(CommittedImageBootstrapError::TargetNotPristine);
    }
    Ok(reader)
}

fn copy_rows(image: &ValidatedCreateSendLayout17Image<'_>) -> Result<WriteBatch> {
    let mut batch = WriteBatch::default();
    batch
        .try_reserve_mutations(image.row_count())
        .map_err(|_| CommittedImageBootstrapError::Allocation)?;
    for row in image.rows() {
        batch.push_put(copy_bytes(row.key())?, copy_bytes(row.value())?);
    }
    if batch.mutations().len() != image.row_count() {
        return Err(CommittedImageBootstrapError::InvalidImage);
    }
    Ok(batch)
}

fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(bytes.len())
        .map_err(|_| CommittedImageBootstrapError::Allocation)?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

fn selection_error(error: SelectionError) -> CommittedImageBootstrapError {
    match error {
        SelectionError::InvalidSelection => CommittedImageBootstrapError::InvalidSelection,
        SelectionError::SelectionMismatch => CommittedImageBootstrapError::SelectionMismatch,
        SelectionError::LimitExceeded => CommittedImageBootstrapError::LimitExceeded,
        SelectionError::Allocation => CommittedImageBootstrapError::Allocation,
        SelectionError::UnsupportedProfile => CommittedImageBootstrapError::UnsupportedProfile,
        SelectionError::InvalidImage => CommittedImageBootstrapError::InvalidImage,
    }
}

#[cfg(test)]
mod tests;
