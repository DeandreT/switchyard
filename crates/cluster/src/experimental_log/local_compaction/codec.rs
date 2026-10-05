use domain::CommittedStreamId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    LogId, LogProfile, experimental_local_compaction::LocalCompactionError as Error,
    experimental_state_machine::NativeCheckpointSummary,
};

pub(super) const BASELINE_KEY: &[u8] = &[3];
pub(super) const MAX_BASELINE_BYTES: usize = 16 * 1024;
const PROFILE_HEADER: &[u8] = b"SWLQ\x01queue-log-local-compaction-v1\0";

pub(super) fn encode_profile(profile: &LogProfile) -> Result<Vec<u8>, Error> {
    let inner = super::super::codec::encode_profile(profile).map_err(|_| Error::InvalidPair)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(PROFILE_HEADER.len() + 4 + inner.len() + 4)
        .map_err(|_| Error::Allocation)?;
    bytes.extend_from_slice(PROFILE_HEADER);
    bytes.extend_from_slice(&(inner.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&inner);
    bytes.extend_from_slice(&(MAX_BASELINE_BYTES as u32).to_be_bytes());
    Ok(bytes)
}

pub(super) fn check_profile(bytes: &[u8], profile: &LogProfile) -> Result<(), Error> {
    if bytes.len() > crate::MAX_LOG_METADATA_BYTES || bytes != encode_profile(profile)? {
        return Err(Error::InvalidPair);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Id {
    term: u64,
    node_id: u64,
    index: u64,
}
impl Id {
    fn from_log(id: LogId) -> Self {
        Self {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
            index: id.index,
        }
    }
    fn to_log(&self) -> LogId {
        LogId::new(
            openraft::CommittedLeaderId::new(self.term, self.node_id),
            self.index,
        )
    }
}

#[derive(Serialize, Deserialize)]
struct Wire<'a> {
    node_id: u64,
    stream: [u8; 16],
    ordinal: u64,
    through: Option<Id>,
    #[serde(borrow)]
    metadata: &'a [u8],
}

#[derive(Clone)]
pub(super) struct Baseline {
    pub(super) ordinal: u64,
    pub(super) bytes: Vec<u8>,
    pub(super) summary: Option<NativeCheckpointSummary>,
}

impl Baseline {
    pub(super) fn empty(profile: &LogProfile) -> Result<Self, Error> {
        Self::make(profile, 0, &[])
    }
    pub(super) fn make(profile: &LogProfile, ordinal: u64, metadata: &[u8]) -> Result<Self, Error> {
        let summary = if ordinal == 0 {
            if !metadata.is_empty() {
                return Err(Error::InvalidHistory);
            }
            None
        } else {
            let summary =
                NativeCheckpointSummary::decode(metadata).map_err(|_| Error::InvalidHistory)?;
            if summary.stream != profile.stream()
                || summary.last.is_none()
                || summary.membership.is_none()
            {
                return Err(Error::InvalidHistory);
            }
            if summary.artifact_bytes < 60
                || summary.last.is_some_and(|last| {
                    last.id.index == 0
                        && (raft_id(last.id) != LogId::default()
                            || summary.highest_timestamp != domain::Timestamp::UNIX_EPOCH)
                })
                || summary.membership.as_ref().is_some_and(|member| {
                    member.source.index == 0 && raft_id(member.source) != LogId::default()
                })
            {
                return Err(Error::InvalidHistory);
            }
            Some(summary)
        };
        let wire = Wire {
            node_id: profile.node_id(),
            stream: *profile.stream().as_bytes(),
            ordinal,
            through: summary
                .as_ref()
                .and_then(|s| s.last)
                .map(|m| Id::from_log(raft_id(m.id))),
            metadata,
        };
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(MAX_BASELINE_BYTES - 44)
            .map_err(|_| Error::Allocation)?;
        payload.resize(MAX_BASELINE_BYTES - 44, 0);
        let len = postcard::to_slice(&wire, &mut payload)
            .map_err(|_| Error::LimitExceeded)?
            .len();
        payload.truncate(len);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(44 + len)
            .map_err(|_| Error::Allocation)?;
        bytes.extend_from_slice(b"SWLF\0\x01\0\x01");
        bytes.extend_from_slice(&(len as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        Ok(Self {
            ordinal,
            bytes,
            summary,
        })
    }

    pub(super) fn decode(profile: &LogProfile, bytes: Vec<u8>) -> Result<Self, Error> {
        if bytes.len() > MAX_BASELINE_BYTES
            || bytes.len() < 44
            || &bytes[..8] != b"SWLF\0\x01\0\x01"
        {
            return Err(Error::InvalidHistory);
        }
        let len = u32::from_be_bytes(bytes[8..12].try_into().map_err(|_| Error::InvalidHistory)?)
            as usize;
        let end = 12_usize
            .checked_add(len)
            .filter(|end| end.checked_add(32) == Some(bytes.len()))
            .ok_or(Error::InvalidHistory)?;
        if Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..] {
            return Err(Error::InvalidHistory);
        }
        let (wire, rest): (Wire<'_>, _) =
            postcard::take_from_bytes(&bytes[12..end]).map_err(|_| Error::InvalidHistory)?;
        if !rest.is_empty()
            || wire.node_id != profile.node_id()
            || CommittedStreamId::new(wire.stream).ok() != Some(profile.stream())
            || wire.metadata.len() > crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES
        {
            return Err(Error::InvalidHistory);
        }
        let decoded = Self::make(profile, wire.ordinal, wire.metadata)?;
        if decoded.bytes != bytes || wire.through.as_ref().map(Id::to_log) != decoded.through() {
            return Err(Error::InvalidHistory);
        }
        Ok(Self { bytes, ..decoded })
    }
    pub(super) fn through(&self) -> Option<LogId> {
        self.summary
            .as_ref()
            .and_then(|s| s.last)
            .map(|m| raft_id(m.id))
    }
    pub(super) fn checksum(&self) -> [u8; 32] {
        Sha256::digest(&self.bytes).into()
    }
}

pub(crate) fn raft_id(id: domain::CommittedEntryId) -> LogId {
    LogId::new(
        openraft::CommittedLeaderId::new(id.term, id.node_id),
        id.index,
    )
}
