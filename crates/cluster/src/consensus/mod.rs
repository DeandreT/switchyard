//! Opt-in consensus persistence and a separate persisted RawNode driver. No
//! application projection or production activation. The storage API alone
//! accepts trusted local metadata; only the driver runs elections/replication.

mod codec;
pub mod driver;
mod identity;
mod store;

pub use identity::{ConsensusIdentity, Voter};
pub use raft;
pub use store::{ConsensusStore, EntryKind, RetainedLogLimits, entry_kind};

use storage::StorageError;
use thiserror::Error;

pub const CONSENSUS_FORMAT_VERSION: u32 = 1;
pub const MAX_CONSENSUS_PAGE_ENTRIES: usize = 64;
pub const MAX_CONSENSUS_PROPOSAL_BYTES: usize = domain::MAX_DURABLE_PROPOSAL_BYTES;
// Proposal data must fit the existing journal unchanged. Context is separate
// Raft metadata; reserve a bounded context budget without reducing data capacity.
pub const MAX_CONSENSUS_CONTEXT_BYTES: usize = crate::MAX_JOURNAL_PAYLOAD_BYTES;
pub const MAX_CONSENSUS_ENTRY_BYTES: usize =
    MAX_CONSENSUS_PROPOSAL_BYTES + MAX_CONSENSUS_CONTEXT_BYTES + 64;

const _: () = assert!(MAX_CONSENSUS_PROPOSAL_BYTES <= crate::MAX_JOURNAL_PAYLOAD_BYTES);
const PREFIX: &[u8] = b"\xF2switchyard/consensus\0";
const ROOT: u8 = 0;
const HARD_STATE: u8 = 1;
const ENTRY: u8 = 2;

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("consensus owner is retired; reopen before further use")]
    Retired,
    #[error("invalid consensus identity: {0}")]
    Identity(&'static str),
    #[error("persisted consensus identity differs from the requested replica/configuration")]
    ForeignIdentity,
    #[error("consensus bootstrap requires an empty backend; local history is not quorum history")]
    NotFresh,
    #[error("consensus metadata is missing")]
    MissingMetadata,
    #[error("unsupported consensus persistence version {0}")]
    Version(u32),
    #[error("corrupt consensus storage: {0}")]
    Corrupt(&'static str),
    #[error("invalid consensus update: {0}")]
    InvalidUpdate(&'static str),
    #[error("consensus bound exceeded: {0}")]
    Limit(&'static str),
    #[error("configuration entries are unsupported by fixed-voter bootstrap")]
    MembershipChange,
    #[error("consensus proposal is invalid: {0}")]
    Proposal(#[from] domain::DurableProposalError),
    #[error("consensus protobuf is invalid: {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
}

fn key(tag: u8) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(tag);
    key
}

fn entry_key(index: u64) -> Vec<u8> {
    let mut key = key(ENTRY);
    key.extend_from_slice(&index.to_be_bytes());
    key
}
