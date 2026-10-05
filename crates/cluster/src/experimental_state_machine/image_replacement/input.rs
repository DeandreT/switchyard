use std::fmt;

use domain::{CommittedCheckpoint, CommittedStreamId, MAX_COMMITTED_MEMBERSHIP_BYTES};
use openraft::Snapshot;

use crate::{LogTypes, MAX_SNAPSHOT_BYTES, experimental_log::bounded_membership_len};

use super::super::NativeSnapshotMetadataError;

const MAX_SNAPSHOT_ID_BYTES: usize = b"swyi-v1-sha256:".len() + 64;

/// Consumed whole transport carrier plus independently trusted mutation choice.
///
/// No selection field is derived as authority from the incoming body or native
/// metadata. This moves the original Box/body, ignores its cursor, and grants no
/// authenticity, quorum, ancestry, anti-rollback, engine-install or purge policy.
/// Private ownership makes even an originally mutable transport body immutable
/// while queued/in flight; no sealed-backing inspection or extraction is needed.
///
/// ```compile_fail
/// fn duplicate(request: cluster::OwnedTrustedNativeReplacement) { let _ = request.clone(); }
/// ```
///
/// ```compile_fail
/// fn mutate(request: &mut cluster::OwnedTrustedNativeReplacement) {
///     request.source.snapshot.write_all(b"changed");
/// }
/// ```
///
/// ```compile_fail
/// fn extract(request: cluster::OwnedTrustedNativeReplacement) { let _ = request.source; }
/// ```
///
/// ```compile_fail
/// fn borrow_then_move(
///     stream: domain::CommittedStreamId, old: domain::CommittedCheckpoint,
///     selected: domain::CommittedCheckpoint, source: openraft::Snapshot<cluster::LogTypes>,
/// ) {
///     let bytes = source.snapshot.as_bytes();
///     let _ = cluster::OwnedTrustedNativeReplacement::new(stream, old, selected, [0; 32], source);
///     std::hint::black_box(bytes);
/// }
/// ```
pub struct OwnedTrustedNativeReplacement {
    pub(super) stream: CommittedStreamId,
    pub(super) target: CommittedCheckpoint,
    pub(super) selected: CommittedCheckpoint,
    pub(super) digest: [u8; 32],
    pub(super) source: Snapshot<LogTypes>,
}

impl OwnedTrustedNativeReplacement {
    /// Allocation-free bounded packaging, not full source/selection validation.
    ///
    /// Arbitrary public SnapshotMeta ID/config/node/address payloads and both
    /// checkpoint payloads are checked before any replacement Box/channel is
    /// allocated. Already caller-owned allocations and spare capacity are not
    /// reclaimed or counted as RSS. This synchronous packaging may refuse before
    /// an owner checks poison. Full source and trusted selection are checked by
    /// the admitted operation before target I/O. A refusal consumes these inputs.
    pub fn new(
        expected_stream: CommittedStreamId,
        expected_target_checkpoint: CommittedCheckpoint,
        selected_checkpoint: CommittedCheckpoint,
        complete_artifact_digest: [u8; 32],
        source: Snapshot<LogTypes>,
    ) -> Result<Self, NativeSnapshotMetadataError> {
        if source.snapshot.as_bytes().len() > MAX_SNAPSHOT_BYTES
            || source.snapshot.as_bytes().len() > domain::MAX_COMMITTED_IMAGE_BYTES
            || source.meta.snapshot_id.len() > MAX_SNAPSHOT_ID_BYTES
            || [&expected_target_checkpoint, &selected_checkpoint]
                .iter()
                .any(|checkpoint| {
                    checkpoint.membership().is_some_and(|membership| {
                        membership.payload.len() > MAX_COMMITTED_MEMBERSHIP_BYTES
                    })
                })
            || bounded_membership_len(source.meta.last_membership.membership()).is_err()
        {
            return Err(NativeSnapshotMetadataError::LimitExceeded);
        }
        Ok(Self {
            stream: expected_stream,
            target: expected_target_checkpoint,
            selected: selected_checkpoint,
            digest: complete_artifact_digest,
            source,
        })
    }
}

impl fmt::Debug for OwnedTrustedNativeReplacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedTrustedNativeReplacement")
            .field("artifact_bytes", &self.source.snapshot.as_bytes().len())
            .finish_non_exhaustive()
    }
}
