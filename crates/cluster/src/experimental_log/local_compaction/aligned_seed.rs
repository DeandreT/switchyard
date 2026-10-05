//! Pure inspection of offered canonical bytes, not paired storage or admission.

use std::{cmp::Ordering, fmt};

use domain::{CommittedCheckpoint, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_MEMBERSHIP_BYTES};
use openraft::{BasicNode, SnapshotMeta};
use sha2::{Digest, Sha256};

use crate::{DecodedNativeSnapshotPair, LogProfile, LogVote, NativeSnapshotMetadataError};

use super::codec::{Baseline, MAX_BASELINE_BYTES, ObservedProfileError, decode_observed_profile};

mod prepared;
pub use prepared::{EncodedAlignedSeedCandidate, prepare_aligned_seed_candidate};

/// Offered immutable data only, not a complete physical capture or trusted source.
pub struct BorrowedAlignedSeed<'a> {
    pub metadata: &'a [u8],
    pub artifact: &'a [u8],
    pub log_rows: &'a [(&'a [u8], &'a [u8])],
}

/// Independent caller expectation, not authentication, quorum or ancestry proof.
///
/// Deriving these fields from the offered bytes cannot create independent trust.
pub struct AlignedSeedExpectation<'a> {
    pub profile: &'a LogProfile,
    pub checkpoint: &'a CommittedCheckpoint,
    pub artifact_sha256: [u8; 32],
    pub artifact_bytes: usize,
    pub native: &'a SnapshotMeta<u64, BasicNode>,
    pub vote: LogVote,
    pub baseline_ordinal: u64,
}

/// Static inspection/preparation refusals; no source is poisoned or write outcome diagnosed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AlignedSeedInspectionError {
    #[error("aligned seed exceeds a finite logical limit")]
    LimitExceeded,
    #[error("aligned seed inspection allocation failed")]
    Allocation,
    #[error("aligned seed expectation is inconsistent")]
    InvalidExpectation,
    #[error("aligned seed profile is unsupported")]
    UnsupportedProfile,
    #[error("aligned seed business image is invalid")]
    InvalidImage,
    #[error("aligned seed native pair is invalid")]
    InvalidNativePair,
    #[error("aligned seed logical log is invalid")]
    InvalidLog,
    #[error("aligned seed does not match the independent expectation")]
    IdentityMismatch,
    #[error("aligned seed policy is unsupported")]
    UnsupportedSeedPolicy,
    #[error("aligned seed retained tail is unsupported")]
    UnsupportedTail,
}

type Result<T> = std::result::Result<T, AlignedSeedInspectionError>;
type Error = AlignedSeedInspectionError;

/// Checked borrowed data, never a writer, seal, install permit or join receipt.
///
/// The supplied dictionary is checked, not proven to include every on-disk row.
/// Existing semantic codecs allocate bounded small metadata, not OOM-safe RSS.
/// No physical role, protected fence, committed history or custody is certified.
///
/// ```compile_fail
/// fn duplicate(value: cluster::InspectedAlignedSeed<'_>) { let _ = value.clone(); }
/// ```
///
/// ```compile_fail
/// fn construct() {
///     let _ = cluster::InspectedAlignedSeed {
///         pair: todo!(), ordinal: 1, vote: Default::default(),
///     };
/// }
/// ```
///
/// ```compile_fail
/// fn escape(expected: &cluster::AlignedSeedExpectation<'_>) -> cluster::InspectedAlignedSeed<'static> {
///     let bytes = Vec::<u8>::new();
///     let rows = [(&[1][..], &[][..]), (&[2][..], &[][..]), (&[3][..], &[][..])];
///     cluster::inspect_aligned_seed(cluster::BorrowedAlignedSeed {
///         metadata: &bytes, artifact: &bytes, log_rows: &rows,
///     }, expected).unwrap()
/// }
/// ```
///
/// ```compile_fail
/// fn no_install(value: cluster::InspectedAlignedSeed<'_>) { value.into_writer(); }
/// ```
pub struct InspectedAlignedSeed<'a> {
    pair: DecodedNativeSnapshotPair<'a>,
    ordinal: u64,
    vote: LogVote,
}

impl<'a> InspectedAlignedSeed<'a> {
    pub fn image_pair(&self) -> &DecodedNativeSnapshotPair<'a> {
        &self.pair
    }
    pub fn baseline_ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn vote(&self) -> LogVote {
        self.vote
    }
}

impl fmt::Debug for BorrowedAlignedSeed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedAlignedSeed")
            .field("metadata_bytes", &self.metadata.len())
            .field("artifact_bytes", &self.artifact.len())
            .field("log_rows", &self.log_rows.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for AlignedSeedExpectation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedSeedExpectation")
            .field("artifact_bytes", &self.artifact_bytes)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for InspectedAlignedSeed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InspectedAlignedSeed")
            .field("artifact_bytes", &self.pair.artifact_bytes().len())
            .finish_non_exhaustive()
    }
}

/// Inspect exactly offered image/catalog and three SWLQ/SWLS/SWLF rows.
///
/// This is synchronous and read-only: no storage, owner, clock, callback or cache
/// is accessed. Noninitial membership and an empty retained tail are required.
/// All caller-owned shape bounds precede allocating existing semantic codecs;
/// observed controls are validated before external identity/policy shortcuts.
///
/// ```no_run
/// pub fn inspect<'a>(
///     offered: cluster::BorrowedAlignedSeed<'a>,
///     expected: &cluster::AlignedSeedExpectation<'_>,
/// ) -> Result<cluster::InspectedAlignedSeed<'a>, cluster::AlignedSeedInspectionError> {
///     cluster::inspect_aligned_seed(offered, expected)
/// }
/// ```
pub fn inspect_aligned_seed<'a>(
    observed: BorrowedAlignedSeed<'a>,
    expected: &AlignedSeedExpectation<'_>,
) -> Result<InspectedAlignedSeed<'a>> {
    shape(&observed, expected)?;
    let pair = DecodedNativeSnapshotPair::decode(observed.metadata, observed.artifact)
        .map_err(metadata_error)?;
    let profile = decode_observed_profile(observed.log_rows[0].1).map_err(profile_error)?;
    let progress = super::super::codec::decode_progress(observed.log_rows[1].1)
        .map_err(|_| Error::InvalidLog)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(observed.log_rows[2].1.len())
        .map_err(|_| Error::Allocation)?;
    bytes.extend_from_slice(observed.log_rows[2].1);
    let baseline = Baseline::decode(&profile, bytes).map_err(local_error)?;
    if profile.stream() != pair.checkpoint().stream() {
        return Err(Error::InvalidLog);
    }
    if pair.checkpoint().last().is_none() || pair.checkpoint().membership().is_none() {
        return Err(Error::UnsupportedSeedPolicy);
    }
    if &profile != expected.profile || !pair_matches(&pair, expected)? {
        return Err(Error::IdentityMismatch);
    }
    if expected.baseline_ordinal == 0 || !expected.vote.committed {
        return Err(Error::InvalidExpectation);
    }
    let summary = baseline
        .summary
        .as_ref()
        .ok_or(Error::UnsupportedSeedPolicy)?;
    if baseline.ordinal != expected.baseline_ordinal
        || !summary.matches(pair.checkpoint())
        || summary.artifact_bytes != expected.artifact_bytes as u64
        || summary.digest != expected.artifact_sha256
    {
        return Err(Error::IdentityMismatch);
    }
    let last = pair
        .checkpoint()
        .last()
        .ok_or(Error::UnsupportedSeedPolicy)?;
    let last = super::codec::raft_id(last.id);
    if progress.last_purged != Some(last) || baseline.through() != Some(last) {
        return Err(Error::InvalidLog);
    }
    if progress.last_present.is_some()
        || progress.retained_entries != 0
        || progress.retained_bytes != 0
    {
        return Err(Error::UnsupportedTail);
    }
    let vote = progress.vote.ok_or(Error::UnsupportedSeedPolicy)?;
    if !vote.committed {
        return Err(Error::UnsupportedSeedPolicy);
    }
    if vote != expected.vote {
        return Err(Error::IdentityMismatch);
    }
    if !matches!(
        vote.leader_id.partial_cmp(&last.leader_id),
        Some(Ordering::Equal | Ordering::Greater)
    ) {
        return Err(Error::UnsupportedSeedPolicy);
    }
    Ok(InspectedAlignedSeed {
        pair,
        ordinal: baseline.ordinal,
        vote,
    })
}

fn shape(observed: &BorrowedAlignedSeed<'_>, expected: &AlignedSeedExpectation<'_>) -> Result<()> {
    if artifact_limits_exceeded(observed.artifact.len(), expected)
        || observed.metadata.len() > crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES
        || expectation_limits_exceeded(expected)
    {
        return Err(Error::LimitExceeded);
    }
    if observed.log_rows.len() > 3 {
        return Err(Error::UnsupportedTail);
    }
    if observed.log_rows.len() != 3 {
        return Err(Error::InvalidLog);
    }
    for ((key, value), (required, cap)) in observed.log_rows.iter().zip([
        (&[1][..], crate::MAX_LOG_METADATA_BYTES),
        (&[2][..], crate::MAX_LOG_METADATA_BYTES),
        (&[3][..], MAX_BASELINE_BYTES),
    ]) {
        if *key != required {
            return Err(Error::InvalidLog);
        }
        if value.len() > cap {
            return Err(Error::LimitExceeded);
        }
    }
    expectation_shape(expected)
}

fn artifact_limits_exceeded(bytes: usize, expected: &AlignedSeedExpectation<'_>) -> bool {
    bytes > MAX_COMMITTED_IMAGE_BYTES || expected.artifact_bytes > MAX_COMMITTED_IMAGE_BYTES
}

fn expectation_limits_exceeded(expected: &AlignedSeedExpectation<'_>) -> bool {
    expected.native.snapshot_id.len() > crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES
        || expected
            .checkpoint
            .membership()
            .is_some_and(|member| member.payload.len() > MAX_COMMITTED_MEMBERSHIP_BYTES)
}

fn expectation_shape(expected: &AlignedSeedExpectation<'_>) -> Result<()> {
    if expected.profile.stream() != expected.checkpoint.stream() {
        return Err(Error::InvalidExpectation);
    }
    crate::experimental_log::bounded_membership_len(expected.native.last_membership.membership())
        .map_err(|_| Error::LimitExceeded)?;
    Ok(())
}

fn pair_matches(
    pair: &DecodedNativeSnapshotPair<'_>,
    expected: &AlignedSeedExpectation<'_>,
) -> Result<bool> {
    Ok(pair.checkpoint() == expected.checkpoint
        && pair.artifact_bytes().len() == expected.artifact_bytes
        && <[u8; 32]>::from(Sha256::digest(pair.artifact_bytes())) == expected.artifact_sha256
        && pair.snapshot_meta().map_err(metadata_error)? == *expected.native)
}

fn metadata_error(error: NativeSnapshotMetadataError) -> Error {
    match error {
        NativeSnapshotMetadataError::LimitExceeded => Error::LimitExceeded,
        NativeSnapshotMetadataError::Allocation => Error::Allocation,
        NativeSnapshotMetadataError::UnsupportedFormat => Error::UnsupportedProfile,
        NativeSnapshotMetadataError::InvalidImage => Error::InvalidImage,
        _ => Error::InvalidNativePair,
    }
}
fn profile_error(error: ObservedProfileError) -> Error {
    match error {
        ObservedProfileError::Limit => Error::LimitExceeded,
        ObservedProfileError::Allocation => Error::Allocation,
        ObservedProfileError::Unsupported => Error::UnsupportedProfile,
        ObservedProfileError::Invalid => Error::InvalidLog,
    }
}
fn local_error(error: crate::LocalCompactionError) -> Error {
    match error {
        crate::LocalCompactionError::Allocation => Error::Allocation,
        crate::LocalCompactionError::LimitExceeded => Error::LimitExceeded,
        _ => Error::InvalidLog,
    }
}

#[cfg(test)]
mod tests;
