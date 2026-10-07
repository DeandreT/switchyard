//! Pure captured-image/native-metadata agreement, not snapshot authority.

use std::fmt;

use domain::{
    CommittedCheckpoint, CommittedImageError, DecodedCommittedImage,
    ValidatedCreateSendLayout17Image,
};
use openraft::{BasicNode, SnapshotMeta};

use super::{AppliedState, state::recover};

mod codec;
mod local_summary;
pub(crate) use local_summary::NativeCheckpointSummary;

#[cfg(test)]
mod tests;

/// Complete metadata framing, canonical fields, and checksum, not allocator RSS.
pub const MAX_NATIVE_SNAPSHOT_METADATA_BYTES: usize = 8 * 1024;

/// Static pure-codec refusals. None grants authority to poison a source owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeSnapshotMetadataError {
    #[error("snapshot metadata or image exceeds its finite limit")]
    LimitExceeded,
    #[error("snapshot metadata output allocation failed")]
    Allocation,
    #[error("snapshot metadata or image format is unsupported")]
    UnsupportedFormat,
    #[error("snapshot metadata is malformed or noncanonical")]
    InvalidMetadata,
    #[error("the captured snapshot image is invalid or out of profile")]
    InvalidImage,
    #[error("the captured checkpoint has incompatible native metadata")]
    IncompatibleCheckpoint,
    #[error("snapshot metadata does not name the exact captured image")]
    ImageMismatch,
}

type Result<T> = std::result::Result<T, NativeSnapshotMetadataError>;

/// Immutable metadata derived from one fully checked borrowed image.
///
/// This owns only bounded metadata, not the source image or any source-health,
/// catalog, installation, quorum, ancestry, or authorization capability.
///
/// ```compile_fail
/// fn duplicate(metadata: cluster::EncodedNativeSnapshotMetadata) {
///     let _ = metadata.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn rewrite(metadata: &mut cluster::EncodedNativeSnapshotMetadata) {
///     metadata.get_mut().clear();
/// }
/// ```
pub struct EncodedNativeSnapshotMetadata {
    bytes: Vec<u8>,
}

impl EncodedNativeSnapshotMetadata {
    /// Fully validates container, CreateSendLayout17V1 business consistency, and native
    /// recovery before deriving canonical metadata from that same captured CP.
    /// SHA-256 names the complete immutable artifact, including its footer.
    /// No store, clock, counter, writer, or current applied checkpoint is read.
    pub fn encode(artifact: &[u8]) -> Result<Self> {
        let (image, _) = checked_image(artifact)?;
        let wire = codec::MetadataV1::from_image(image.checkpoint(), artifact)?;
        Ok(Self {
            bytes: codec::encode(&wire)?,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl fmt::Debug for EncodedNativeSnapshotMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedNativeSnapshotMetadata")
            .field("encoded_bytes", &self.len())
            .finish_non_exhaustive()
    }
}

/// One fully checked metadata/image pair borrowing both complete inputs.
///
/// Decoding checks business semantics, not just a declared role. The view
/// retains the checked image and its captured checkpoint. It is not installable
/// authority or proof of a source's health, authenticity, history, or quorum.
/// A retained catalog may be older than current applied progress: this API has
/// no current-checkpoint input or store query.
pub struct DecodedNativeSnapshotPair<'a> {
    metadata: &'a [u8],
    artifact: &'a [u8],
    image: ValidatedCreateSendLayout17Image<'a>,
    recovered: AppliedState,
    digest: [u8; 32],
}

impl<'a> DecodedNativeSnapshotPair<'a> {
    /// Refuses malformed framing before processing the complete image, then
    /// compares every frozen field to the exact fully checked captured image.
    /// Membership deserialization and canonical comparison borrow the metadata
    /// input; no second full image buffer is allocated.
    pub fn decode(metadata: &'a [u8], artifact: &'a [u8]) -> Result<Self> {
        let wire = codec::decode(metadata)?;
        let (image, recovered) = checked_image(artifact)?;
        let expected = codec::MetadataV1::from_image(image.checkpoint(), artifact)?;
        if wire != expected {
            return Err(NativeSnapshotMetadataError::ImageMismatch);
        }
        let digest = expected.digest;
        Ok(Self {
            metadata,
            artifact,
            image,
            recovered,
            digest,
        })
    }

    pub fn metadata_bytes(&self) -> &'a [u8] {
        self.metadata
    }
    pub fn artifact_bytes(&self) -> &'a [u8] {
        self.artifact
    }
    pub fn image(&self) -> &ValidatedCreateSendLayout17Image<'a> {
        &self.image
    }
    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        self.image.checkpoint()
    }

    /// Intentionally exposes the library's IDs and membership, unlike redacted
    /// wrapper Debug/errors. The owned native membership clone is bounded by
    /// its canonical 4 KiB payload, not a heap/RSS claim. Its collection copies
    /// use the library's normal allocator; this is not universal OOM recovery.
    pub fn snapshot_meta(&self) -> Result<SnapshotMeta<u64, BasicNode>> {
        Ok(SnapshotMeta {
            last_log_id: self.recovered.0,
            last_membership: self.recovered.1.clone(),
            snapshot_id: codec::snapshot_id(&self.digest)?,
        })
    }
}

impl fmt::Debug for DecodedNativeSnapshotPair<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedNativeSnapshotPair")
            .field("metadata_bytes", &self.metadata.len())
            .field("artifact_bytes", &self.artifact.len())
            .finish_non_exhaustive()
    }
}

fn checked_image(artifact: &[u8]) -> Result<(ValidatedCreateSendLayout17Image<'_>, AppliedState)> {
    let decoded = DecodedCommittedImage::decode(artifact).map_err(|error| match error {
        CommittedImageError::LimitExceeded => NativeSnapshotMetadataError::LimitExceeded,
        CommittedImageError::Allocation => NativeSnapshotMetadataError::Allocation,
        CommittedImageError::UnsupportedFormat => NativeSnapshotMetadataError::UnsupportedFormat,
        _ => NativeSnapshotMetadataError::InvalidImage,
    })?;
    let image = ValidatedCreateSendLayout17Image::validate(decoded)
        .map_err(|_| NativeSnapshotMetadataError::InvalidImage)?;
    let recovered = recover(image.checkpoint())
        .map_err(|_| NativeSnapshotMetadataError::IncompatibleCheckpoint)?;
    Ok((image, recovered))
}
