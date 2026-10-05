//! Canonical candidate bytes only, not publication or trusted seed authority.

use crate::EncodedNativeSnapshotMetadata;
use crate::experimental_log::types::{LogProgress, PROFILE_KEY, PROGRESS_KEY};

use super::super::codec::{BASELINE_KEY, encode_profile};
use super::*;

/// Immutable small candidate data borrowing the complete caller-owned artifact.
///
/// Requested profile, vote and ordinal are desired data, not evidence of any
/// historical durable vote, source health, ancestry, quorum or authorization.
/// This value supplies no physical completeness, protected fence, writer, owner,
/// publication permit or native-resource retirement capability.
///
/// ```compile_fail
/// fn duplicate(candidate: cluster::EncodedAlignedSeedCandidate<'_>) {
///     let _copy = candidate.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn construct() {
///     let artifact: &[u8] = &[];
///     let metadata = cluster::EncodedNativeSnapshotMetadata::encode(artifact).unwrap();
///     let _candidate = cluster::EncodedAlignedSeedCandidate {
///         artifact, metadata, controls: [Vec::new(), Vec::new(), Vec::new()],
///     };
/// }
/// ```
///
/// ```compile_fail
/// fn mutate(candidate: &mut cluster::EncodedAlignedSeedCandidate<'_>) {
///     candidate.metadata_bytes()[0] = 0;
/// }
/// ```
///
/// ```compile_fail
/// fn publish(candidate: cluster::EncodedAlignedSeedCandidate<'_>) {
///     candidate.into_writer();
/// }
/// ```
///
/// ```compile_fail
/// fn escape(expected: &cluster::AlignedSeedExpectation<'_>)
///     -> cluster::EncodedAlignedSeedCandidate<'static>
/// {
///     let artifact = Vec::<u8>::new();
///     cluster::prepare_aligned_seed_candidate(&artifact, expected).unwrap()
/// }
/// ```
pub struct EncodedAlignedSeedCandidate<'a> {
    artifact: &'a [u8],
    metadata: EncodedNativeSnapshotMetadata,
    controls: [Vec<u8>; 3],
}

impl<'a> EncodedAlignedSeedCandidate<'a> {
    pub fn artifact_bytes(&self) -> &'a [u8] {
        self.artifact
    }

    pub fn metadata_bytes(&self) -> &[u8] {
        self.metadata.as_bytes()
    }

    /// Bind the returned row array before calling the existing borrowed inspector.
    pub fn log_rows(&self) -> [(&[u8], &[u8]); 3] {
        [
            (PROFILE_KEY, &self.controls[0]),
            (PROGRESS_KEY, &self.controls[1]),
            (BASELINE_KEY, &self.controls[2]),
        ]
    }

    /// Recheck against caller expectations without retaining a self-borrowing view.
    pub fn reinspect(&self, expected: &AlignedSeedExpectation<'_>) -> Result<()> {
        let rows = self.log_rows();
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: self.metadata_bytes(),
                artifact: self.artifact,
                log_rows: &rows,
            },
            expected,
        )
        .map(|_| ())
    }
}

impl fmt::Debug for EncodedAlignedSeedCandidate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedAlignedSeedCandidate")
            .field("artifact_bytes", &self.artifact.len())
            .field("metadata_bytes", &self.metadata.len())
            .field("profile_bytes", &self.controls[0].len())
            .field("progress_bytes", &self.controls[1].len())
            .field("baseline_bytes", &self.controls[2].len())
            .finish_non_exhaustive()
    }
}

/// Prepare the existing canonical metadata and three empty-tail log controls.
///
/// All caller shape bounds precede generating output or allocating semantic
/// recovery. The complete artifact stays borrowed. Noninitial membership, exact
/// independently supplied full image/native identity, a positive ordinal and a
/// sufficient committed requested vote are required. The expectation need not
/// be trustworthy: generating matching bytes establishes no durable provenance.
///
/// Output uses only the existing frozen codecs. This synchronous function has
/// no storage, callback, clock, owner, task, cache or publication operation.
/// Bounded semantic allocations are not universal OOM recovery or an RSS quota.
///
/// ```no_run
/// pub fn prepare<'a>(
///     artifact: &'a [u8],
///     expected: &cluster::AlignedSeedExpectation<'_>,
/// ) -> Result<cluster::EncodedAlignedSeedCandidate<'a>, cluster::AlignedSeedInspectionError> {
///     let candidate = cluster::prepare_aligned_seed_candidate(artifact, expected)?;
///     let borrowed_artifact: &'a [u8] = candidate.artifact_bytes();
///     {
///         let metadata: &[u8] = candidate.metadata_bytes();
///         let rows: [(&[u8], &[u8]); 3] = candidate.log_rows();
///         let _checked: cluster::InspectedAlignedSeed<'_> = cluster::inspect_aligned_seed(
///             cluster::BorrowedAlignedSeed {
///                 metadata, artifact: borrowed_artifact, log_rows: &rows,
///             }, expected,
///         )?;
///     }
///     candidate.reinspect(expected)?;
///     Ok(candidate)
/// }
/// ```
pub fn prepare_aligned_seed_candidate<'a>(
    artifact: &'a [u8],
    expected: &AlignedSeedExpectation<'_>,
) -> Result<EncodedAlignedSeedCandidate<'a>> {
    if artifact_limits_exceeded(artifact.len(), expected) || expectation_limits_exceeded(expected) {
        return Err(Error::LimitExceeded);
    }
    expectation_shape(expected)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(artifact).map_err(metadata_error)?;
    let last = {
        let pair = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), artifact)
            .map_err(metadata_error)?;
        if pair.checkpoint().last().is_none() || pair.checkpoint().membership().is_none() {
            return Err(Error::UnsupportedSeedPolicy);
        }
        if !pair_matches(&pair, expected)? {
            return Err(Error::IdentityMismatch);
        }
        if expected.baseline_ordinal == 0 || !expected.vote.committed {
            return Err(Error::InvalidExpectation);
        }
        let last = pair
            .checkpoint()
            .last()
            .ok_or(Error::UnsupportedSeedPolicy)?;
        let last = super::super::codec::raft_id(last.id);
        if !matches!(
            expected.vote.leader_id.partial_cmp(&last.leader_id),
            Some(Ordering::Equal | Ordering::Greater)
        ) {
            return Err(Error::UnsupportedSeedPolicy);
        }
        last
    };
    let baseline = Baseline::make(
        expected.profile,
        expected.baseline_ordinal,
        metadata.as_bytes(),
    )
    .map_err(local_error)?;
    let progress = LogProgress {
        vote: Some(expected.vote),
        last_purged: Some(last),
        ..Default::default()
    };
    let controls = [
        encode_profile(expected.profile).map_err(local_error)?,
        crate::experimental_log::codec::encode_progress(&progress)
            .map_err(|_| Error::InvalidLog)?,
        baseline.bytes,
    ];
    let rows = [
        (PROFILE_KEY, controls[0].as_slice()),
        (PROGRESS_KEY, controls[1].as_slice()),
        (BASELINE_KEY, controls[2].as_slice()),
    ];
    // A temporary view validates actual generated buffers, never fabricated rows.
    inspect_aligned_seed(
        BorrowedAlignedSeed {
            metadata: metadata.as_bytes(),
            artifact,
            log_rows: &rows,
        },
        expected,
    )?;
    Ok(EncodedAlignedSeedCandidate {
        artifact,
        metadata,
        controls,
    })
}
