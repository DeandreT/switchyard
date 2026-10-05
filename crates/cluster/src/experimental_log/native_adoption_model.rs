//! Test-only paired-record model. No storage, task, receipt or mutation authority.

use serde::{Deserialize, Serialize};

mod classify;
mod entries;
mod frame;
mod identity;
mod records;
mod tests;
mod wire;

const MAX_CONTROL: usize = 256;
const MAX_BASELINE: usize = 16 * 1024;
const MAX_INTENT: usize = 128 * 1024;
const MAX_CHECKPOINT: usize = domain::MAX_COMMITTED_CHECKPOINT_BYTES;
const MAX_MEMBER: usize = domain::MAX_COMMITTED_MEMBERSHIP_BYTES;
const MAX_NATIVE: usize = crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES;
const MAX_IMAGE: usize = domain::MAX_COMMITTED_IMAGE_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum ModelCodecError {
    #[error("model input exceeds a fixed limit")]
    Limit,
    #[error("model output allocation failed")]
    Allocation,
    #[error("model record is unsupported or noncanonical")]
    Format,
    #[error("model record fields are inconsistent")]
    Fields,
    #[error("model observation is malformed")]
    Observation,
}

type Result<T> = std::result::Result<T, ModelCodecError>;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Role {
    State = 1,
    Log = 2,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
    Header = 1,
    Progress = 2,
    Baseline = 3,
    Intent = 4,
    StateFence = 5,
    LogFence = 6,
    StageBinding = 7,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
struct Binding {
    pair: [u8; 16],
    state: [u8; 16],
    log: [u8; 16],
    node: u64,
    stream: [u8; 16],
}

impl Binding {
    fn check(self) -> Result<()> {
        if [self.pair, self.state, self.log].contains(&[0; 16])
            || self.pair == self.state
            || self.pair == self.log
            || self.state == self.log
            || domain::CommittedStreamId::new(self.stream).is_err()
        {
            return Err(ModelCodecError::Fields);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Row<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).into()
}
