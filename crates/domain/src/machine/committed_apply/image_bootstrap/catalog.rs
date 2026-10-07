use storage::{CatalogCommittedStore, SnapshotCatalogRecord};

use super::{
    CommittedImageBootstrapError, CommittedStateMachine, Result, StateMachine,
    TrustedCreateSendBootstrap, copy_rows, pristine_reader, validate_selection,
};

const _: () = {
    assert!(crate::MAX_COMMITTED_IMAGE_BYTES <= storage::MAX_CATALOG_ARTIFACT_BYTES);
};

impl<W: CatalogCommittedStore> CommittedStateMachine<W> {
    /// Publishes a selected image and bounded opaque catalog only into a pristine target.
    ///
    /// Catalog bounds are checked first, then the unchanged trusted selection
    /// validator checks the exact stream, full checkpoint, complete-artifact
    /// digest, container and CreateSendLayout17V1 semantics. Every source or metadata
    /// refusal precedes this function's first target operation. An overlong
    /// component takes precedence over other source-selection errors.
    ///
    /// The same uninitialized/empty target probe and fallible Put-only row copy
    /// used by ordinary bootstrap precede one commit_with_catalog through the
    /// consumed unique writer. That commit atomically publishes the original
    /// business/checkpoint bytes, initialization and both catalog components.
    /// The catalog artifact is exactly the immutable selected slice, not a
    /// re-encoding or another domain-owned artifact copy. Metadata is opaque;
    /// empty metadata is legal. No native metadata interpretation occurs.
    ///
    /// The matching target reader is reused directly in the returned machine.
    /// There is no ordinary bootstrap/commit followed by retention, catalog
    /// read, bounded/ordinary target snapshot, postcommit read, constructor
    /// revalidation, retry, normalization or rollback assumption. Every commit
    /// error becomes static CommitUnknown; the writer is consumed and no usable
    /// machine is returned, even when the complete commit is already durable.
    ///
    /// Target pristine/catalog consistency relies on the trusted opted-in store
    /// profile contract and unique writer, not a read/commit compare-and-swap.
    /// Target probing and allocation exclusions are unchanged from ordinary
    /// bootstrap: source/batch/backend copies may coexist and this is not a
    /// bounded-RSS or all-allocations-fallible guarantee. This adds no populated
    /// replacement, source authenticity, native installation, runtime adoption,
    /// ancestry, log purge or rollback authority.
    ///
    /// ```compile_fail
    /// fn no_catalog_writer<W: storage::CommittedStore>(
    ///     writer: W,
    ///     selection: domain::TrustedCreateSendBootstrap<'_>,
    /// ) {
    ///     let _ = domain::CommittedStateMachine::bootstrap_create_send_image_with_catalog(
    ///         writer, selection, &[],
    ///     );
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn a_reader_is_not_a_catalog_target(
    ///     reader: storage::MemoryStore,
    ///     selection: domain::TrustedCreateSendBootstrap<'_>,
    /// ) {
    ///     let _ = domain::CommittedStateMachine::bootstrap_create_send_image_with_catalog(
    ///         reader, selection, &[],
    ///     );
    /// }
    /// ```
    pub fn bootstrap_create_send_image_with_catalog(
        mut writer: W,
        request: TrustedCreateSendBootstrap<'_>,
        metadata: &[u8],
    ) -> Result<Self> {
        let catalog = SnapshotCatalogRecord::new(metadata, request.artifact)
            .map_err(|_| CommittedImageBootstrapError::LimitExceeded)?;
        let image = validate_selection(&request)?;
        let reader = pristine_reader(&writer)?;
        let batch = copy_rows(&image)?;
        writer
            .commit_with_catalog(batch, catalog)
            .map_err(|_| CommittedImageBootstrapError::CommitUnknown)?;
        Ok(Self {
            writer,
            machine: StateMachine::new(reader),
            stream: request.stream,
            poisoned: false,
        })
    }
}

#[cfg(test)]
mod tests;
