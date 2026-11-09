use protobuf::Message;
use raft::eraftpb::{Entry, HardState};
use sha2::{Digest, Sha256};

use super::{
    CONSENSUS_FORMAT_VERSION, ConsensusError, ENTRY, MAX_CONSENSUS_ENTRY_BYTES, entry_key,
};

const HEADER: usize = 8;
const DIGEST: usize = 32;
pub(super) const FRAME_OVERHEAD: usize = HEADER + DIGEST;

pub(super) fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = CONSENSUS_FORMAT_VERSION.to_be_bytes().to_vec();
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    let digest = digest(tag, &bytes);
    bytes.extend_from_slice(&digest);
    bytes
}

fn digest(tag: u8, bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"switchyard consensus record v1\0");
    hash.update([tag]);
    hash.update(bytes);
    hash.finalize().into()
}

pub(super) fn unframe(tag: u8, bytes: &[u8], maximum: usize) -> Result<&[u8], ConsensusError> {
    if bytes.len() < HEADER + DIGEST || bytes.len() > maximum + HEADER + DIGEST {
        return Err(ConsensusError::Corrupt("record size outside bound"));
    }
    let version = u32::from_be_bytes(bytes[..4].try_into().expect("checked header"));
    if version != CONSENSUS_FORMAT_VERSION {
        return Err(ConsensusError::Version(version));
    }
    let length = u32::from_be_bytes(bytes[4..8].try_into().expect("checked header")) as usize;
    if length != bytes.len() - HEADER - DIGEST {
        return Err(ConsensusError::Corrupt("record framing length"));
    }
    let end = bytes.len() - DIGEST;
    if bytes[end..] != digest(tag, &bytes[..end]) {
        return Err(ConsensusError::Corrupt("record digest"));
    }
    Ok(&bytes[HEADER..end])
}

pub(super) fn encode_entry(entry: &Entry) -> Result<Vec<u8>, ConsensusError> {
    Ok(frame(ENTRY, &entry.write_to_bytes()?))
}

pub(super) fn decode_entry(key: &[u8], bytes: &[u8]) -> Result<Entry, ConsensusError> {
    let payload = unframe(ENTRY, bytes, MAX_CONSENSUS_ENTRY_BYTES)?;
    let entry = Entry::parse_from_bytes(payload)?;
    if key != entry_key(entry.index)
        || entry.write_to_bytes()? != payload
        || entry.unknown_fields.iter().next().is_some()
    {
        return Err(ConsensusError::Corrupt("noncanonical entry/key"));
    }
    Ok(entry)
}

pub(super) fn decode_hard_state(bytes: &[u8]) -> Result<HardState, ConsensusError> {
    let payload = unframe(super::HARD_STATE, bytes, 64)?;
    let hs = HardState::parse_from_bytes(payload)?;
    if hs.write_to_bytes()? != payload || hs.unknown_fields.iter().next().is_some() {
        return Err(ConsensusError::Corrupt("noncanonical hard state"));
    }
    Ok(hs)
}
