use std::io::{self, Write};

use domain::{CommittedCheckpoint, CommittedEntryId, CommittedEntryMark};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{MAX_NATIVE_SNAPSHOT_METADATA_BYTES, NativeSnapshotMetadataError as Error, Result};

pub(super) const HEADER_BYTES: usize = 12;
pub(super) const CHECKSUM_BYTES: usize = 32;
pub(super) const MAX_MEMBERSHIP_BYTES: usize = 4096;
// 12 frame + 16 stream + 32 digest + 10 artifact length + 2*(1+30+32)
// marks + 10 watermark + (1+30+3+2+4096) membership + 32 checksum.
pub(super) const MAX_FROZEN_METADATA_BYTES: usize = 4370;
const MAGIC: &[u8; 4] = b"SWYM";
const SCHEMA: u16 = 1;
const CREATE_SEND_ROLE: u16 = 1;
const SNAPSHOT_ID_PREFIX: &str = "swyi-v1-sha256:";
pub(super) const SNAPSHOT_ID_BYTES: usize = SNAPSHOT_ID_PREFIX.len() + 64;

// Explicit versioned fields, never OpenRaft's serde layout or Command ordinals.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct MetadataV1<'a> {
    pub stream: [u8; 16],
    pub artifact_bytes: u64,
    pub digest: [u8; 32],
    pub last: Option<MarkV1>,
    pub previous: Option<MarkV1>,
    pub highest_timestamp: u64,
    #[serde(borrow)]
    pub membership: Option<MembershipV1<'a>>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct IdV1 {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct MarkV1 {
    pub id: IdV1,
    pub fingerprint: [u8; 32],
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct MembershipV1<'a> {
    pub source: IdV1,
    pub schema_version: u16,
    #[serde(borrow)]
    pub payload: &'a [u8],
}

impl<'a> MetadataV1<'a> {
    pub fn from_image(checkpoint: &'a CommittedCheckpoint, artifact: &[u8]) -> Result<Self> {
        if artifact.len() > domain::MAX_COMMITTED_IMAGE_BYTES {
            return Err(Error::LimitExceeded);
        }
        let wire = Self {
            stream: *checkpoint.stream().as_bytes(),
            artifact_bytes: u64::try_from(artifact.len()).map_err(|_| Error::LimitExceeded)?,
            digest: Sha256::digest(artifact).into(),
            last: checkpoint.last().map(MarkV1::from_mark),
            previous: checkpoint.previous().map(MarkV1::from_mark),
            highest_timestamp: checkpoint.highest_timestamp().as_millis(),
            membership: checkpoint.membership().map(|membership| MembershipV1 {
                source: IdV1::from_id(membership.source),
                schema_version: membership.schema_version,
                payload: &membership.payload,
            }),
        };
        wire.check_bounds()?;
        Ok(wire)
    }

    fn check_bounds(&self) -> Result<()> {
        if self.artifact_bytes > domain::MAX_COMMITTED_IMAGE_BYTES as u64
            || self
                .membership
                .as_ref()
                .is_some_and(|value| value.payload.len() > MAX_MEMBERSHIP_BYTES)
        {
            return Err(Error::LimitExceeded);
        }
        Ok(())
    }
}

impl IdV1 {
    fn from_id(id: CommittedEntryId) -> Self {
        Self {
            term: id.term,
            node_id: id.node_id,
            index: id.index,
        }
    }
}

impl MarkV1 {
    fn from_mark(mark: CommittedEntryMark) -> Self {
        Self {
            id: IdV1::from_id(mark.id),
            fingerprint: mark.fingerprint,
        }
    }
}

pub(super) fn encode(wire: &MetadataV1<'_>) -> Result<Vec<u8>> {
    wire.check_bounds()?;
    let payload_len = encoded_size(wire)?;
    let length = total_length(payload_len)?;
    if length > MAX_FROZEN_METADATA_BYTES {
        return Err(Error::LimitExceeded);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::Allocation)?;
    bytes.resize(length, 0);
    bytes[..4].copy_from_slice(MAGIC);
    bytes[4..6].copy_from_slice(&SCHEMA.to_be_bytes());
    bytes[6..8].copy_from_slice(&CREATE_SEND_ROLE.to_be_bytes());
    bytes[8..12].copy_from_slice(
        &u32::try_from(payload_len)
            .map_err(|_| Error::LimitExceeded)?
            .to_be_bytes(),
    );
    let end = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or(Error::LimitExceeded)?;
    let written = postcard::to_slice(wire, &mut bytes[HEADER_BYTES..end])
        .map_err(|_| Error::InvalidMetadata)?;
    if written.len() != payload_len {
        return Err(Error::InvalidMetadata);
    }
    let checksum = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8]) -> Result<MetadataV1<'_>> {
    if bytes.len() > MAX_NATIVE_SNAPSHOT_METADATA_BYTES {
        return Err(Error::LimitExceeded);
    }
    if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
        return Err(Error::InvalidMetadata);
    }
    if &bytes[..4] != MAGIC {
        return Err(Error::InvalidMetadata);
    }
    let schema = u16::from_be_bytes([bytes[4], bytes[5]]);
    let role = u16::from_be_bytes([bytes[6], bytes[7]]);
    if schema != SCHEMA || role != CREATE_SEND_ROLE {
        return Err(Error::UnsupportedFormat);
    }
    let payload_len = usize::try_from(u32::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11],
    ]))
    .map_err(|_| Error::LimitExceeded)?;
    if total_length(payload_len)? != bytes.len() {
        return Err(Error::InvalidMetadata);
    }
    let end = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or(Error::LimitExceeded)?;
    if Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..] {
        return Err(Error::InvalidMetadata);
    }
    let payload = &bytes[HEADER_BYTES..end];
    let (wire, remaining): (MetadataV1<'_>, _) =
        postcard::take_from_bytes(payload).map_err(|_| Error::InvalidMetadata)?;
    if !remaining.is_empty() {
        return Err(Error::InvalidMetadata);
    }
    wire.check_bounds()?;
    let mut comparison = Comparison {
        expected: payload,
        position: 0,
    };
    if postcard::to_io(&wire, &mut comparison).is_err() || comparison.position != payload.len() {
        return Err(Error::InvalidMetadata);
    }
    Ok(wire)
}

fn total_length(payload_len: usize) -> Result<usize> {
    HEADER_BYTES
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(CHECKSUM_BYTES))
        .filter(|length| *length <= MAX_NATIVE_SNAPSHOT_METADATA_BYTES)
        .ok_or(Error::LimitExceeded)
}

fn encoded_size(wire: &MetadataV1<'_>) -> Result<usize> {
    let mut counter = Counter {
        length: 0,
        exceeded: false,
    };
    if postcard::to_io(wire, &mut counter).is_err() {
        return Err(if counter.exceeded {
            Error::LimitExceeded
        } else {
            Error::InvalidMetadata
        });
    }
    Ok(counter.length)
}

struct Counter {
    length: usize,
    exceeded: bool,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(length) = self.length.checked_add(bytes.len()).filter(|length| {
            *length <= MAX_NATIVE_SNAPSHOT_METADATA_BYTES - HEADER_BYTES - CHECKSUM_BYTES
        }) else {
            self.exceeded = true;
            return Err(io::Error::other("snapshot metadata bound exceeded"));
        };
        self.length = length;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Comparison<'a> {
    expected: &'a [u8],
    position: usize,
}
impl Write for Comparison<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("snapshot metadata comparison overflow"))?;
        if self.expected.get(self.position..end) != Some(bytes) {
            return Err(io::Error::other("noncanonical snapshot metadata"));
        }
        self.position = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn snapshot_id(digest: &[u8; 32]) -> Result<String> {
    let mut id = String::new();
    id.try_reserve_exact(SNAPSHOT_ID_BYTES)
        .map_err(|_| Error::Allocation)?;
    id.push_str(SNAPSHOT_ID_PREFIX);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        id.push(HEX[usize::from(byte >> 4)] as char);
        id.push(HEX[usize::from(byte & 15)] as char);
    }
    Ok(id)
}
