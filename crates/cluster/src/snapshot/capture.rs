//! Opt-in offline capture. The caller must exclude every writer, including raw
//! clones and keyspace/topology handles, throughout the complete capture call.
//! These APIs do not enforce exclusion or enable online/runtime capture.

use std::sync::Arc;

use domain::snapshot_validation::{SnapshotCompleteStateError, validate_complete_state};
use storage::{
    FjallStore, Key, SnapshotProvenance, SnapshotWithProvenance, StateStore, StorageError,
    StoreSnapshot, Value, WriteBatch, capture_logical_snapshot,
};
use thiserror::Error;

use super::{
    SNAPSHOT_LAYOUT_VERSION, SnapshotFormat, SnapshotFormatError, SnapshotLimits, SnapshotManifest,
};
use crate::{CommittedReplay, JournalError, ReplayError, journal::validate_snapshot_namespace};

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotCaptureError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    State(#[from] SnapshotCompleteStateError),
    #[error(transparent)]
    JournalNamespace(#[from] JournalError),
    #[error(transparent)]
    Replay(#[from] ReplayError),
    #[error(transparent)]
    Format(#[from] SnapshotFormatError),
}

/// One immutable image validated and encoded without repair or application.
///
/// The frame preserves every record byte, but does not encode storage provenance.
/// Keep this typed result when Fjall evidence matters: decoding `encoded()` alone
/// cannot establish backend provenance. This is not authentication, historical
/// authority, installability, an acknowledgement or a recovery certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotCapture {
    image: Arc<SnapshotWithProvenance>,
    manifest: SnapshotManifest,
    encoded: Vec<u8>,
}

impl SnapshotCapture {
    /// Captures exactly once with the unchanged logical StateStore contract.
    /// Even a Fjall source here has only `LogicalOnly` provenance.
    ///
    /// Caller-enforced offline exclusion must cover all writers and raw clones.
    /// The full read and temporary validation facts are O(image bytes). Limits
    /// are checked after the backend read, so do not bound its allocations, but
    /// precede domain/owner decoding. Capture and encode share the exact frame
    /// preflight; encode rechecks the validated manifest before output allocation.
    pub fn logical<S: StateStore>(
        store: &S,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotCaptureError> {
        Self::from_image(capture_logical_snapshot(store)?, limits)
    }

    /// Captures through the existing metadata/provenance bridge exactly once.
    /// The caller must quiesce every writer, raw clone and keyspace/topology
    /// handle for the whole call. The API neither detects nor enforces that
    /// precondition. Failed capture/metadata checks propagate without retry.
    pub fn fjall(store: &FjallStore, limits: SnapshotLimits) -> Result<Self, SnapshotCaptureError> {
        Self::from_image(store.snapshot_with_provenance()?, limits)
    }

    pub fn image(&self) -> &SnapshotWithProvenance {
        &self.image
    }

    pub fn provenance(&self) -> &SnapshotProvenance {
        self.image.provenance()
    }

    pub fn manifest(&self) -> SnapshotManifest {
        self.manifest
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    fn from_image(
        image: SnapshotWithProvenance,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotCaptureError> {
        let image = Arc::new(image);
        let records = image.snapshot().entries();
        // Every valid image with F1 rows has a nonzero checkpoint and digest.
        // Raw tag presence is only an untrusted header-width hint: it neither
        // decodes private ownership bytes nor establishes an applied identity.
        let has_applied_identity = records.iter().any(|(key, _)| key.first() == Some(&0xf1));
        // Check caps/order before allocating semantic facts or using binary lookup.
        // The validated manifest will determine the encoder's final header width.
        SnapshotFormat::preflight(has_applied_identity, records, limits)?;
        // The report cannot be supplied by a caller or borrowed from another image.
        validate_records(records)?;
        let replay = CommittedReplay::open(FrozenImage(Arc::clone(&image)))?;
        let manifest = SnapshotManifest::new(
            SNAPSHOT_LAYOUT_VERSION,
            replay.applied_index()?,
            replay.committed_index()?,
            replay.last_appended_index()?,
            replay.latest_applied_digest()?,
        )?;
        let encoded = SnapshotFormat::encode(&manifest, records, limits)?;
        Ok(Self {
            image,
            manifest,
            encoded,
        })
    }
}

fn validate_records(records: &[(Key, Value)]) -> Result<(), SnapshotCaptureError> {
    validate_complete_state(records)?;
    for (key, _) in records {
        validate_snapshot_namespace(key)?;
    }
    Ok(())
}

/// Only this already-canonical immutable image is visible to the existing owners.
/// No access to the live source survives into validation. Apply is prohibited,
/// and capture never invokes replay_batch or an indexed apply operation.
#[derive(Clone, Debug)]
struct FrozenImage(Arc<SnapshotWithProvenance>);

impl StateStore for FrozenImage {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let records = self.0.snapshot().entries();
        Ok(records
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| records[index].1.clone()))
    }

    fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::Backend {
            operation: "apply to a frozen snapshot",
            detail: "snapshot validation is read-only".into(),
        })
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        Ok(self.0.snapshot().clone())
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let records = self.0.snapshot().entries();
        let start = start.max(prefix);
        let offset = records.partition_point(|(key, _)| key.as_slice() < start);
        Ok(records[offset..]
            .iter()
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::snapshot_validation::{SnapshotCatalogError, SnapshotStateError};

    #[test]
    fn noncanonical_images_are_rejected_before_adapter_lookup() {
        let root = (b"\xF0switchyard/journal\0\0".to_vec(), vec![0, 0, 0, 1]);
        let other = (b"\xF1other".to_vec(), vec![]);
        for records in [vec![root.clone(), root.clone()], vec![other, root]] {
            assert!(matches!(
                validate_records(&records),
                Err(SnapshotCaptureError::State(
                    SnapshotCompleteStateError::State(SnapshotStateError::Catalog(
                        SnapshotCatalogError::InputOrder { row: 1 }
                    ))
                ))
            ));
        }
    }

    #[test]
    fn frozen_image_rejects_apply_and_implements_bounded_prefix_reads() {
        let store = storage::MemoryStore::default();
        store
            .apply(
                WriteBatch::default()
                    .put(vec![1], vec![2])
                    .put(vec![2], vec![3]),
            )
            .unwrap();
        let image = FrozenImage(Arc::new(capture_logical_snapshot(&store).unwrap()));
        assert_eq!(image.get(&[1]).unwrap(), Some(vec![2]));
        assert_eq!(image.get(&[3]).unwrap(), None);
        assert_eq!(
            image.scan_from(&[1], &[], 1).unwrap(),
            vec![(vec![1], vec![2])]
        );
        assert!(image.scan_from(&[1], &[2], 1).unwrap().is_empty());
        assert!(image.scan_from(&[], &[], 0).unwrap().is_empty());
        assert!(image.apply(WriteBatch::default()).is_err());
        assert_eq!(image.snapshot().unwrap(), store.snapshot().unwrap());
    }
}
