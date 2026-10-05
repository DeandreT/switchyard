use std::{fmt, future::Future};

mod bootstrap;
pub use bootstrap::StateMachineCatalogBootstrapError;

use domain::{
    CommittedCatalogError, CommittedCheckpoint, CommittedStateMachine, EncodedCommittedImage,
    RetainedCreateSendCatalog,
};
use openraft::{BasicNode, SnapshotMeta};
use storage::{BoundedStateStore, CatalogCommittedStore, CommittedStore};

use super::{
    DecodedNativeSnapshotPair, EncodedNativeSnapshotMetadata, ExperimentalStateMachine,
    MAX_NATIVE_SNAPSHOT_METADATA_BYTES, NativeSnapshotMetadataError, StateMachineError,
    owner::{Operation, Reply},
    state::StoreState,
};

/// Explicit encoded-metadata overhead outside the artifact admission charge.
///
/// Catalog operations reserve the entire existing 64 MiB owner budget, excluding
/// every other packet until publication/refund. Their metadata may additionally
/// occupy at most 8 KiB. Small captured checkpoints/native membership projections,
/// collection capacity, source snapshots, backend staging, and RSS remain outside
/// that admission accounting, as in existing image export and scalar queries.
pub const MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES: usize = MAX_NATIVE_SNAPSHOT_METADATA_BYTES;

/// Static owner, source, and pure-metadata refusals, not rollback evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateMachineCatalogError {
    #[error("the native catalog owner did not complete: {0}")]
    Owner(StateMachineError),
    #[error("the native catalog source or retention was refused: {0}")]
    Domain(CommittedCatalogError),
    #[error("the native catalog metadata was refused: {0}")]
    Metadata(NativeSnapshotMetadataError),
    #[error("native catalog operations were not enabled for this owner")]
    Disabled,
}

type Result<T> = std::result::Result<T, StateMachineCatalogError>;

struct BuiltDetails {
    metadata: EncodedNativeSnapshotMetadata,
    projection: SnapshotMeta<u64, BasicNode>,
}

/// Original captured image, encoded metadata, and their owned native projection.
///
/// This holds no owner/backend handle and grants no installation, source-health,
/// ancestry, quorum, rollback, or purge authority. Its diagnostics are redacted;
/// the explicit projection getter intentionally exposes IDs and membership.
///
/// ```compile_fail
/// fn duplicate(value: cluster::BuiltNativeSnapshotCatalog) { let _ = value.clone(); }
/// ```
pub struct BuiltNativeSnapshotCatalog {
    image: EncodedCommittedImage,
    details: Box<BuiltDetails>,
}

impl BuiltNativeSnapshotCatalog {
    // The catalog is already retained. This private handoff moves both owned
    // parts; the standalone builder maps defensive transport errors statically.
    pub(super) fn into_standalone_snapshot(
        self,
    ) -> std::io::Result<openraft::Snapshot<crate::LogTypes>> {
        let Self { image, details } = self;
        let BuiltDetails { projection, .. } = *details;
        let data = crate::BoundedSnapshotData::from_image(image)?;
        Ok(openraft::Snapshot {
            meta: projection,
            snapshot: Box::new(data),
        })
    }

    pub fn image_bytes(&self) -> &[u8] {
        self.image.as_bytes()
    }
    pub fn metadata_bytes(&self) -> &[u8] {
        self.details.metadata.as_bytes()
    }
    pub fn snapshot_meta(&self) -> &SnapshotMeta<u64, BasicNode> {
        &self.details.projection
    }
}

impl fmt::Debug for BuiltNativeSnapshotCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BuiltNativeSnapshotCatalog")
            .field("image_bytes", &self.image_bytes().len())
            .field("metadata_bytes", &self.metadata_bytes().len())
            .finish_non_exhaustive()
    }
}

struct RetainedDetails {
    catalog: RetainedCreateSendCatalog,
    projection: SnapshotMeta<u64, BasicNode>,
}

/// A fully checked retained pair with an owned projection, not a live frontier.
///
/// The original domain storage result is moved, not cloned or made self-referential.
/// A pair older than current progress is legitimate. Even an otherwise valid pair
/// ahead of current progress supplies no engine-adoption authority: this DTO does
/// not compare live progress or establish retained-log ancestry.
///
/// ```compile_fail
/// fn mutate(value: &mut cluster::RetainedNativeSnapshotCatalog) {
///     value.image_bytes().push(0);
/// }
/// ```
pub struct RetainedNativeSnapshotCatalog {
    details: Box<RetainedDetails>,
}

impl RetainedNativeSnapshotCatalog {
    pub fn image_bytes(&self) -> &[u8] {
        self.details.catalog.image_bytes()
    }
    pub fn metadata_bytes(&self) -> &[u8] {
        self.details.catalog.metadata()
    }
    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        self.details.catalog.checkpoint()
    }
    pub fn snapshot_meta(&self) -> &SnapshotMeta<u64, BasicNode> {
        &self.details.projection
    }
}

impl fmt::Debug for RetainedNativeSnapshotCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedNativeSnapshotCatalog")
            .field("image_bytes", &self.image_bytes().len())
            .field("metadata_bytes", &self.metadata_bytes().len())
            .finish_non_exhaustive()
    }
}

pub(super) struct CatalogCapabilities<M> {
    build_local: fn(
        &mut M,
        crate::experimental_local_compaction::frontier::PairIdentity,
        u64,
        u64,
        &CommittedCheckpoint,
    ) -> Result<crate::experimental_local_compaction::frontier::Receipt>,
    build: fn(&mut M) -> Result<BuiltNativeSnapshotCatalog>,
    read: fn(&mut M) -> Result<Option<RetainedNativeSnapshotCatalog>>,
}

impl ExperimentalStateMachine {
    #[cfg(test)]
    pub(crate) fn create_catalog_and_export_for_test<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> std::result::Result<Self, StateMachineError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        let mut state = StoreState::create_with_image_export(writer, stream)?;
        state.image_catalog = Some(capabilities::<W>());
        Self::start(state)
    }

    /// Create a pristine owner with explicit bounded native catalog operations.
    ///
    /// Existing constructors leave this capability disabled. This constructor
    /// does not enable the separate image-export operation or an engine-facing
    /// snapshot builder/current-snapshot/install method, engine adoption, or log purge.
    ///
    /// ```compile_fail
    /// fn no_catalog<W: storage::CommittedStore>(writer: W, stream: domain::CommittedStreamId)
    /// where W::Reader: storage::BoundedStateStore {
    ///     let _ = cluster::ExperimentalStateMachine::create_with_snapshot_catalog(writer, stream);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn no_unbounded_capture<W: storage::CatalogCommittedStore>(
    ///     writer: W, stream: domain::CommittedStreamId,
    /// ) {
    ///     let _ = cluster::ExperimentalStateMachine::create_with_snapshot_catalog(writer, stream);
    /// }
    /// ```
    pub fn create_with_snapshot_catalog<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> std::result::Result<Self, StateMachineError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        let mut state = StoreState::create(writer, stream)?;
        state.image_catalog = Some(capabilities::<W>());
        Self::start(state)
    }

    /// Open exact native progress with the same independent opt-in capability.
    pub fn open_with_snapshot_catalog<W>(
        writer: W,
        stream: domain::CommittedStreamId,
    ) -> std::result::Result<Self, StateMachineError>
    where
        W: CatalogCommittedStore,
        W::Reader: BoundedStateStore,
    {
        let mut state = StoreState::open(writer, stream)?;
        state.image_catalog = Some(capabilities::<W>());
        Self::start(state)
    }

    /// Return an inert owned build future, charged only on first admission.
    ///
    /// Accepted caller loss cannot cancel capture, retention, or result cleanup.
    /// The complete 64 MiB reservation persists through publication/refund; bounded
    /// encoded metadata is explicitly additional overhead. All fallible native
    /// derivation and small DTO allocation occur before the sole retention commit.
    /// After success this function only moves its prepared carrier; no postcommit
    /// source read is made. This is not a global allocation guarantee for owner
    /// publication channels, library internals, or the allocator.
    pub fn build_create_send_catalog(
        &mut self,
    ) -> impl Future<Output = Result<BuiltNativeSnapshotCatalog>> + Send + 'static + use<> {
        let handle = self.handle.clone();
        async move {
            match handle.request(Operation::BuildCatalog).await {
                Ok(Reply::CatalogBuilt(result)) => result,
                Ok(_) => Err(StateMachineCatalogError::Owner(
                    StateMachineError::InvalidState,
                )),
                Err(error) => Err(StateMachineCatalogError::Owner(error)),
            }
        }
    }

    /// Read one retained pair without consulting current applied progress.
    ///
    /// This returns no installability/history/authorization capability. Metadata
    /// agreement and business consistency alone cannot authorize engine adoption.
    /// The owned future retains no facade lifetime, raw writer, or public handle.
    pub fn read_create_send_catalog(
        &mut self,
    ) -> impl Future<Output = Result<Option<RetainedNativeSnapshotCatalog>>> + Send + 'static + use<>
    {
        let handle = self.handle.clone();
        async move {
            match handle.request(Operation::ReadCatalog).await {
                Ok(Reply::CatalogRead(result)) => result,
                Ok(_) => Err(StateMachineCatalogError::Owner(
                    StateMachineError::InvalidState,
                )),
                Err(error) => Err(StateMachineCatalogError::Owner(error)),
            }
        }
    }
}

fn capabilities<W>() -> CatalogCapabilities<CommittedStateMachine<W>>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    CatalogCapabilities {
        build_local: build_local::<W>,
        build: build::<W>,
        read: read::<W>,
    }
}

fn build<W>(machine: &mut CommittedStateMachine<W>) -> Result<BuiltNativeSnapshotCatalog>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let token = machine
        .prepare_create_send_catalog()
        .map_err(StateMachineCatalogError::Domain)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(token.image_bytes())
        .map_err(StateMachineCatalogError::Metadata)?;
    let projection = {
        let pair = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), token.image_bytes())
            .map_err(StateMachineCatalogError::Metadata)?;
        pair.snapshot_meta()
            .map_err(StateMachineCatalogError::Metadata)?
    };
    // Allocate this small carrier before retention; after commit only moves remain.
    let details = Box::new(BuiltDetails {
        metadata,
        projection,
    });
    let image = token
        .retain(details.metadata.as_bytes())
        .map_err(StateMachineCatalogError::Domain)?;
    Ok(BuiltNativeSnapshotCatalog { image, details })
}

fn build_local<W>(
    machine: &mut CommittedStateMachine<W>,
    identity: crate::experimental_local_compaction::frontier::PairIdentity,
    node_id: u64,
    attempt: u64,
    expected: &CommittedCheckpoint,
) -> Result<crate::experimental_local_compaction::frontier::Receipt>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    use crate::experimental_local_compaction::frontier::ReceiptData;
    let token = machine
        .prepare_create_send_catalog()
        .map_err(StateMachineCatalogError::Domain)?;
    if token.checkpoint() != expected {
        return Err(StateMachineCatalogError::Metadata(
            NativeSnapshotMetadataError::IncompatibleCheckpoint,
        ));
    }
    let metadata = EncodedNativeSnapshotMetadata::encode(token.image_bytes())
        .map_err(StateMachineCatalogError::Metadata)?;
    let projection = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), token.image_bytes())
        .and_then(|pair| pair.snapshot_meta())
        .map_err(StateMachineCatalogError::Metadata)?;
    let receipt = std::sync::Arc::new(ReceiptData {
        identity,
        attempt,
        node_id,
        checkpoint: token.checkpoint().clone(),
        metadata,
        projection,
    });
    let image = token
        .retain(receipt.metadata.as_bytes())
        .map_err(StateMachineCatalogError::Domain)?;
    drop(image);
    Ok(receipt)
}

fn read<W>(machine: &mut CommittedStateMachine<W>) -> Result<Option<RetainedNativeSnapshotCatalog>>
where
    W: CatalogCommittedStore,
{
    let Some(catalog) = machine
        .read_create_send_catalog()
        .map_err(StateMachineCatalogError::Domain)?
    else {
        return Ok(None);
    };
    let projection = {
        let pair = DecodedNativeSnapshotPair::decode(catalog.metadata(), catalog.image_bytes())
            .map_err(StateMachineCatalogError::Metadata)?;
        pair.snapshot_meta()
            .map_err(StateMachineCatalogError::Metadata)?
    };
    Ok(Some(RetainedNativeSnapshotCatalog {
        details: Box::new(RetainedDetails {
            catalog,
            projection,
        }),
    }))
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn local_catalog_enabled(&self) -> bool {
        self.image_catalog.is_some()
    }

    pub(super) fn build_for_local_compaction(
        &mut self,
        identity: crate::experimental_local_compaction::frontier::PairIdentity,
        node_id: u64,
        attempt: u64,
        expected: &CommittedCheckpoint,
    ) -> Result<crate::experimental_local_compaction::frontier::Receipt> {
        self.ensure_healthy()
            .map_err(StateMachineCatalogError::Owner)?;
        let build = self
            .image_catalog
            .as_ref()
            .map(|cap| cap.build_local)
            .ok_or(StateMachineCatalogError::Disabled)?;
        let result = build(&mut self.machine, identity, node_id, attempt, expected);
        self.catalog_result(result)
    }

    pub(super) fn build_create_send_catalog(&mut self) -> Result<BuiltNativeSnapshotCatalog> {
        self.ensure_healthy()
            .map_err(StateMachineCatalogError::Owner)?;
        let build = self
            .image_catalog
            .as_ref()
            .map(|capability| capability.build)
            .ok_or(StateMachineCatalogError::Disabled)?;
        let result = build(&mut self.machine);
        self.catalog_result(result)
    }

    pub(super) fn read_create_send_catalog(
        &mut self,
    ) -> Result<Option<RetainedNativeSnapshotCatalog>> {
        self.ensure_healthy()
            .map_err(StateMachineCatalogError::Owner)?;
        let read = self
            .image_catalog
            .as_ref()
            .map(|capability| capability.read)
            .ok_or(StateMachineCatalogError::Disabled)?;
        let result = read(&mut self.machine);
        self.catalog_result(result)
    }

    fn catalog_result<T>(&mut self, result: Result<T>) -> Result<T> {
        if matches!(
            &result,
            Err(StateMachineCatalogError::Domain(
                CommittedCatalogError::Poisoned
                    | CommittedCatalogError::ReadFailed
                    | CommittedCatalogError::CommitUnknown
            ))
        ) {
            self.poisoned = true;
        }
        result
    }
}

#[cfg(test)]
pub(super) mod tests;
