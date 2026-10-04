use std::fmt;

use storage::{
    BoundedStateStore, CatalogCommittedStore, CatalogReadError, SnapshotCatalogReader,
    SnapshotCatalogRecord, StorageError, StoredSnapshotCatalog, WriteBatch,
};

use crate::{
    CommittedCheckpoint, CommittedImageError, CommittedImageValidationError, DecodedCommittedImage,
    EncodedCommittedImage, ValidatedCreateSendImage,
};

use super::{CommittedImageExportError, CommittedStateMachine};

const _: () = {
    assert!(crate::MAX_COMMITTED_IMAGE_BYTES <= storage::MAX_CATALOG_ARTIFACT_BYTES);
};

/// Static catalog failures, without source bytes or backend diagnostic details.
///
/// A declared profile, checksum, or arbitrary opaque source does not certify
/// provenance or corruption. Source semantic refusals are nonfatal. Failure to
/// obtain a trusted complete storage view poisons further work, as does every
/// catalog commit error whose decision may be unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedCatalogError {
    #[error("the committed catalog machine is poisoned")]
    Poisoned,
    #[error("the committed catalog source could not be read")]
    ReadFailed,
    #[error("the committed catalog request exceeds its byte limits")]
    LimitExceeded,
    #[error("the committed catalog result could not be allocated")]
    Allocation,
    #[error("the committed catalog image profile is unsupported")]
    UnsupportedProfile,
    #[error("the committed catalog image is inconsistent with the requested profile")]
    InvalidImage,
    #[error("the committed catalog image belongs to another stream")]
    WrongStream,
    #[error("the committed catalog write decision is unknown")]
    CommitUnknown,
}

/// One validated capture holding the machine's exclusive preparation borrow.
///
/// There is no public constructor, clone, mutable image, or writer escape. The
/// image owns its bytes; the checkpoint is a separate small owned value, not a
/// self-referential decoded view. Dropping this token writes nothing. Caller
/// metadata remains opaque and should be fully encoded into owned bytes before
/// consuming the token; a slice borrowed from this token cannot accompany its
/// move into retain.
///
/// ```compile_fail
/// fn duplicate<W: storage::CatalogCommittedStore>(
///     token: domain::PreparedCreateSendCatalog<'_, W>,
/// ) {
///     let another = token.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn mutate<W: storage::CatalogCommittedStore>(
///     token: &mut domain::PreparedCreateSendCatalog<'_, W>,
/// ) {
///     token.image_bytes().push(0);
/// }
/// ```
///
/// ```compile_fail
/// fn recover_writer<W: storage::CatalogCommittedStore>(
///     token: domain::PreparedCreateSendCatalog<'_, W>,
/// ) {
///     let writer = token.machine.writer;
/// }
/// ```
///
/// ```compile_fail
/// fn mutate_checkpoint<W: storage::CatalogCommittedStore>(
///     token: &mut domain::PreparedCreateSendCatalog<'_, W>,
/// ) {
///     token.checkpoint().membership().unwrap().payload.push(0);
/// }
/// ```
///
/// ```compile_fail
/// fn metadata_cannot_borrow_the_consumed_token<W: storage::CatalogCommittedStore>(
///     token: domain::PreparedCreateSendCatalog<'_, W>,
/// ) {
///     let metadata = token.image_bytes();
///     let _ = token.retain(metadata);
/// }
/// ```
pub struct PreparedCreateSendCatalog<'a, W: CatalogCommittedStore> {
    machine: &'a mut CommittedStateMachine<W>,
    image: EncodedCommittedImage,
    checkpoint: CommittedCheckpoint,
}

impl<W: CatalogCommittedStore> PreparedCreateSendCatalog<'_, W> {
    pub fn image_bytes(&self) -> &[u8] {
        self.image.as_bytes()
    }

    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        &self.checkpoint
    }

    /// Atomically retains this exact image with bounded opaque metadata.
    ///
    /// Exactly one empty-business catalog commit uses the same private writer.
    /// There is no intervening apply, checkpoint/init query, ordinary commit,
    /// artifact copy, or postcommit validation read. Successful return moves
    /// the original owned image. Every commit error poisons the machine and
    /// yields CommitUnknown: the full catalog may already be durable, so this
    /// returns no success image and performs no retry or rollback assumption.
    /// A metadata limit refusal consumes the token but does not write or poison.
    ///
    /// This changes only the catalog, not business rows or applied progress.
    /// It is not pristine-target bootstrap, populated-state replacement, native
    /// metadata validation, snapshot installation, or history-purge authority.
    pub fn retain(self, metadata: &[u8]) -> Result<EncodedCommittedImage, CommittedCatalogError> {
        if self.machine.poisoned {
            return Err(CommittedCatalogError::Poisoned);
        }
        let record = SnapshotCatalogRecord::new(metadata, self.image.as_bytes())
            .map_err(|_| CommittedCatalogError::LimitExceeded)?;
        if self
            .machine
            .writer
            .commit_with_catalog(WriteBatch::default(), record)
            .is_err()
        {
            self.machine.poisoned = true;
            return Err(CommittedCatalogError::CommitUnknown);
        }
        Ok(self.image)
    }
}

impl<W: CatalogCommittedStore> fmt::Debug for PreparedCreateSendCatalog<'_, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedCreateSendCatalog")
            .field("image_bytes", &self.image.len())
            .finish_non_exhaustive()
    }
}

/// An owned retained pair whose artifact passed CreateSendV1 validation.
///
/// Metadata is bounded and opaque, not interpreted or validated by this type.
/// The checkpoint belongs to this retained artifact, which may be older than
/// current applied business state. The storage result is wrapped without an
/// artifact clone or self-referential view. No backend/machine handle, mutation,
/// native snapshot result, authenticity, ancestry, or adoption authority exists.
///
/// ```compile_fail
/// fn duplicate(value: domain::RetainedCreateSendCatalog) {
///     let another = value.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn mutate_image(value: &mut domain::RetainedCreateSendCatalog) {
///     value.image_bytes().push(0);
/// }
/// ```
///
/// ```compile_fail
/// fn mutate_metadata(value: &mut domain::RetainedCreateSendCatalog) {
///     value.metadata().push(0);
/// }
/// ```
pub struct RetainedCreateSendCatalog {
    stored: StoredSnapshotCatalog,
    checkpoint: CommittedCheckpoint,
}

impl RetainedCreateSendCatalog {
    pub fn metadata(&self) -> &[u8] {
        self.stored.metadata()
    }

    pub fn image_bytes(&self) -> &[u8] {
        self.stored.artifact()
    }

    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        &self.checkpoint
    }
}

impl fmt::Debug for RetainedCreateSendCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedCreateSendCatalog")
            .field("metadata_bytes", &self.stored.metadata().len())
            .field("image_bytes", &self.stored.artifact().len())
            .finish_non_exhaustive()
    }
}

impl<W> CommittedStateMachine<W>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    /// Captures one complete bounded image and exclusively borrows its writer.
    ///
    /// Existing export performs the single bounded source capture and complete
    /// image validation. A further pure decode of those same immutable bytes
    /// owns the small captured checkpoint; no extra storage read is added.
    /// Constructor validation and the unique writer establish initialization,
    /// not a new live checkpoint or initialized query. This cannot re-attest a
    /// backend modified through trusted out-of-band access or breaking its
    /// storage contract. Source/image coexistence and allocation exclusions are
    /// unchanged from export; small checkpoint cloning is not an OOM guarantee.
    ///
    /// The mutable borrow remains held until retain or drop. Pristine bootstrap
    /// of image rows plus catalog would need a separate single business/init/
    /// catalog commit, not bootstrap followed by this second write.
    ///
    /// ```compile_fail
    /// fn no_catalog_capability<W: storage::CommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    /// ) where W::Reader: storage::BoundedStateStore {
    ///     let _ = machine.prepare_create_send_catalog();
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_unbounded_capture<W: storage::CatalogCommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    /// ) {
    ///     let _ = machine.prepare_create_send_catalog();
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_intervening_apply<W>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    ///     update: &domain::CommittedCheckpointUpdate,
    ///     work: &domain::CommittedQueueWork,
    /// ) where W: storage::CatalogCommittedStore, W::Reader: storage::BoundedStateStore {
    ///     let token = machine.prepare_create_send_catalog().unwrap();
    ///     let _ = machine.apply_committed(update, work);
    ///     let _ = token.retain(&[]);
    /// }
    /// ```
    pub fn prepare_create_send_catalog(
        &mut self,
    ) -> Result<PreparedCreateSendCatalog<'_, W>, CommittedCatalogError> {
        let image = self.export_create_send_image().map_err(export_error)?;
        let checkpoint = DecodedCommittedImage::decode(image.as_bytes())
            .map_err(container_error)?
            .checkpoint()
            .clone();
        Ok(PreparedCreateSendCatalog {
            machine: self,
            image,
            checkpoint,
        })
    }
}

impl<W: CatalogCommittedStore> CommittedStateMachine<W> {
    /// Reads and validates one retained artifact without consulting current state.
    ///
    /// One originating catalog-reader factory and one read supply the exact
    /// stored pair. The returned artifact's container, stream, and complete
    /// CreateSendV1 semantics are checked; its metadata remains opaque. No
    /// business-reader bound, checkpoint/init query, ordinary or bounded business
    /// snapshot, clock, writer, live-progress comparison, or fallback is used.
    /// An absent slot is Ok(None). An older valid slot is not made invalid by
    /// subsequent application. Arbitrary opaque source semantic/profile/stream
    /// refusals do not certify corruption and do not poison the machine.
    ///
    /// ```compile_fail
    /// fn no_catalog_read_capability<W: storage::CommittedStore>(
    ///     machine: &mut domain::CommittedStateMachine<W>,
    /// ) {
    ///     let _ = machine.read_create_send_catalog();
    /// }
    /// ```
    pub fn read_create_send_catalog(
        &mut self,
    ) -> Result<Option<RetainedCreateSendCatalog>, CommittedCatalogError> {
        if self.poisoned {
            return Err(CommittedCatalogError::Poisoned);
        }
        let stored = match self.writer.catalog_reader().read_catalog() {
            Ok(Some(stored)) => stored,
            Ok(None) => return Ok(None),
            Err(error) => {
                let error = catalog_read_error(error);
                if error == CommittedCatalogError::ReadFailed {
                    self.poisoned = true;
                }
                return Err(error);
            }
        };
        let checkpoint = {
            let decoded =
                DecodedCommittedImage::decode(stored.artifact()).map_err(container_error)?;
            if decoded.stream() != self.stream {
                return Err(CommittedCatalogError::WrongStream);
            }
            let checked = ValidatedCreateSendImage::validate(decoded).map_err(validation_error)?;
            checked.checkpoint().clone()
        };
        Ok(Some(RetainedCreateSendCatalog { stored, checkpoint }))
    }
}

fn export_error(error: CommittedImageExportError) -> CommittedCatalogError {
    match error {
        CommittedImageExportError::Poisoned => CommittedCatalogError::Poisoned,
        CommittedImageExportError::ReadFailed => CommittedCatalogError::ReadFailed,
        CommittedImageExportError::LimitExceeded => CommittedCatalogError::LimitExceeded,
        CommittedImageExportError::Allocation => CommittedCatalogError::Allocation,
        CommittedImageExportError::UnsupportedProfile => CommittedCatalogError::UnsupportedProfile,
        CommittedImageExportError::InvalidImage => CommittedCatalogError::InvalidImage,
    }
}

fn catalog_read_error(error: CatalogReadError) -> CommittedCatalogError {
    match error {
        CatalogReadError::LimitExceeded
        | CatalogReadError::Storage(StorageError::ReadLimitExceeded) => {
            CommittedCatalogError::LimitExceeded
        }
        CatalogReadError::Allocation => CommittedCatalogError::Allocation,
        CatalogReadError::Storage(_) => CommittedCatalogError::ReadFailed,
    }
}

fn container_error(error: CommittedImageError) -> CommittedCatalogError {
    match error {
        CommittedImageError::LimitExceeded => CommittedCatalogError::LimitExceeded,
        CommittedImageError::Allocation => CommittedCatalogError::Allocation,
        CommittedImageError::UnsupportedFormat => CommittedCatalogError::UnsupportedProfile,
        _ => CommittedCatalogError::InvalidImage,
    }
}

fn validation_error(error: CommittedImageValidationError) -> CommittedCatalogError {
    match error {
        CommittedImageValidationError::UnsupportedProfile => {
            CommittedCatalogError::UnsupportedProfile
        }
        _ => CommittedCatalogError::InvalidImage,
    }
}

#[cfg(test)]
mod tests;
