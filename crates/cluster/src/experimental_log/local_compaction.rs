//! Disjoint standalone logical role; never an OpenRaft adapter or runtime store.
mod aligned_seed;
mod codec;
mod owner;
mod state;
mod store;
pub use aligned_seed::{
    AlignedSeedExpectation, AlignedSeedInspectionError, BorrowedAlignedSeed, InspectedAlignedSeed,
    inspect_aligned_seed,
};
pub use store::ExperimentalCompactionLogStore;

#[cfg(test)]
pub(crate) fn encode_expected_controls_for_test(
    profile: &crate::LogProfile,
    ordinal: u64,
    metadata: &[u8],
    vote: Option<crate::LogVote>,
    last_present: Option<crate::LogId>,
    retained_entries: u64,
    retained_bytes: u64,
) -> Result<(Vec<u8>, Vec<u8>), crate::experimental_local_compaction::LocalCompactionError> {
    let baseline = codec::Baseline::make(profile, ordinal, metadata)?;
    let progress = super::types::LogProgress {
        vote,
        last_purged: baseline.through(),
        last_present,
        retained_entries,
        retained_bytes,
    };
    let progress = super::codec::encode_progress(&progress)
        .map_err(|_| crate::experimental_local_compaction::LocalCompactionError::InvalidHistory)?;
    Ok((baseline.bytes, progress))
}

#[cfg(test)]
mod tests;
