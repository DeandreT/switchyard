//! Canonical, bounded containers for declared committed-state image rows.
//!
//! A declared role is not validation that the business rows implement that
//! role. This module has no store, writer, install, export, or purge authority.

use std::fmt;

use sha2::{Digest, Sha256};
use storage::StoreSnapshot;

use super::{CommittedCheckpoint, CommittedStreamId, MAX_COMMITTED_ENTRY_BYTES, decode_checkpoint};

mod protected;
pub use protected::{
    CheckedProtectedCreateSendImage, CheckedProtectedCreateSendLayout17Image,
    ProtectedCreateSendImageError, check_protected_create_send_image,
    check_protected_create_send_layout17_image,
};

mod validated;
pub use validated::{
    CommittedImageValidationError, ValidatedCreateSendImage, ValidatedCreateSendLayout17Image,
};

#[cfg(test)]
mod tests;

/// Maximum complete serialized container, including header, row lengths, and checksum.
/// This is a logical artifact bound, not an allocator capacity or RSS bound.
pub const MAX_COMMITTED_IMAGE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum rows in this container version, not a business entity or message limit.
pub const MAX_COMMITTED_IMAGE_ROWS: usize = 65_536;
/// Explicit key bound for this container version; future roles may need a new version.
pub const MAX_COMMITTED_IMAGE_KEY_BYTES: usize = 1024;
/// Explicit value bound for this container version, including a value's own encoding.
pub const MAX_COMMITTED_IMAGE_VALUE_BYTES: usize = MAX_COMMITTED_ENTRY_BYTES;

const MAGIC: &[u8; 4] = b"SWYI";
const SCHEMA_VERSION: u16 = 1;
const CREATE_SEND_ROLE: u16 = 1;
const CREATE_SEND_LAYOUT17_ROLE: u16 = 2;
const HEADER_BYTES: usize = 28;
const ROW_HEADER_BYTES: usize = 8;
const CHECKSUM_BYTES: usize = 32;
const CHECKPOINT_KEY: &[u8] = &[0x12];

/// The declared role carried by an image container.
///
/// This declaration does not certify the included business rows. Unknown tags
/// and malformed business records can be structurally packaged. Separate
/// semantic validation must refuse them before any installation is possible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommittedImageRole {
    CreateSendV1,
    /// Closed Create/Send business layout 17 with mandatory NonFinite queue modes.
    CreateSendLayout17V1,
}

/// Static, source-private container failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedImageError {
    #[error("committed image format is unsupported")]
    UnsupportedFormat,
    #[error("committed image encoding is malformed")]
    Malformed,
    #[error("committed image exceeds its container limits")]
    LimitExceeded,
    #[error("committed image stream identity is invalid")]
    InvalidStream,
    #[error("committed image rows are invalid")]
    InvalidRows,
    #[error("committed image checkpoint is invalid")]
    InvalidCheckpoint,
    #[error("committed image checkpoint stream does not match")]
    CheckpointStreamMismatch,
    #[error("committed image checksum does not match")]
    ChecksumMismatch,
    #[error("committed image output allocation failed")]
    Allocation,
}

/// Immutable owned canonical bytes, with no mutable buffer or writer escape.
///
/// This is not an installable image, source-health certificate, commitment
/// proof, authentication result, or evidence of durable provenance. Its
/// SHA-256 checksum detects accidental byte changes; it is not a signature.
///
/// ```compile_fail
/// fn duplicate(image: domain::EncodedCommittedImage) {
///     let _ = image.clone();
/// }
/// ```
pub struct EncodedCommittedImage {
    bytes: Vec<u8>,
}

impl EncodedCommittedImage {
    /// Packages one complete caller-owned snapshot after structural validation.
    ///
    /// No store is read or mutated. All limits, ordering, checkpoint encoding,
    /// and stream agreement are checked before allocating the large output.
    /// Only the existing bounded checkpoint codec may copy small metadata.
    pub fn encode(
        role: CommittedImageRole,
        stream: CommittedStreamId,
        snapshot: &StoreSnapshot,
    ) -> Result<Self, CommittedImageError> {
        encode_entries(role, stream, snapshot.entries(), Limits::CURRENT)
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

impl fmt::Debug for EncodedCommittedImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedCommittedImage")
            .field("encoded_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// A structurally checked container borrowing its complete encoded bytes.
///
/// Large keys and values are never copied by decoding. The checksum is checked
/// before the existing bounded checkpoint decoder allocates small metadata.
/// Business-record consistency, role certification, source health, ancestry,
/// installation, and commitment remain outside this type's guarantees.
pub struct DecodedCommittedImage<'a> {
    bytes: &'a [u8],
    row_bytes: &'a [u8],
    row_count: usize,
    stream: CommittedStreamId,
    checkpoint: CommittedCheckpoint,
    role: CommittedImageRole,
}

impl<'a> DecodedCommittedImage<'a> {
    pub fn decode(bytes: &'a [u8]) -> Result<Self, CommittedImageError> {
        decode_image(bytes, Limits::CURRENT)
    }

    pub fn role(&self) -> CommittedImageRole {
        self.role
    }

    pub fn stream(&self) -> CommittedStreamId {
        self.stream
    }

    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        &self.checkpoint
    }

    pub fn encoded_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn rows(&self) -> CommittedImageRows<'a> {
        CommittedImageRows {
            remaining: self.row_bytes,
            remaining_rows: self.row_count,
        }
    }
}

impl fmt::Debug for DecodedCommittedImage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedCommittedImage")
            .field("row_count", &self.row_count)
            .field("encoded_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// One immutable row borrowing the validated container bytes.
#[derive(Clone, Copy)]
pub struct CommittedImageRow<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> CommittedImageRow<'a> {
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn value(&self) -> &'a [u8] {
        self.value
    }
}

impl fmt::Debug for CommittedImageRow<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedImageRow")
            .field("key_bytes", &self.key.len())
            .field("value_bytes", &self.value.len())
            .finish_non_exhaustive()
    }
}

/// Ascending, unique rows from an immutable validated container.
pub struct CommittedImageRows<'a> {
    remaining: &'a [u8],
    remaining_rows: usize,
}

impl<'a> Iterator for CommittedImageRows<'a> {
    type Item = CommittedImageRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            return None;
        }
        // Recheck slices rather than exposing offsets or trusting raw indexing.
        let row = match take_row(&mut self.remaining, Limits::CURRENT) {
            Ok(row) => row,
            Err(_) => {
                self.remaining_rows = 0;
                self.remaining = &[];
                return None;
            }
        };
        self.remaining_rows -= 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining_rows))
    }
}

impl std::iter::FusedIterator for CommittedImageRows<'_> {}

impl fmt::Debug for CommittedImageRows<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedImageRows")
            .field("remaining_rows", &self.remaining_rows)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
struct Limits {
    bytes: usize,
    rows: usize,
    key: usize,
    value: usize,
}

impl Limits {
    const CURRENT: Self = Self {
        bytes: MAX_COMMITTED_IMAGE_BYTES,
        rows: MAX_COMMITTED_IMAGE_ROWS,
        key: MAX_COMMITTED_IMAGE_KEY_BYTES,
        value: MAX_COMMITTED_IMAGE_VALUE_BYTES,
    };

    fn row(self, key: &[u8], value: &[u8]) -> Result<(), CommittedImageError> {
        if key.is_empty() {
            return Err(CommittedImageError::InvalidRows);
        }
        if key.len() > self.key || value.len() > self.value {
            return Err(CommittedImageError::LimitExceeded);
        }
        Ok(())
    }
}

fn encode_entries(
    role: CommittedImageRole,
    stream: CommittedStreamId,
    entries: &[(Vec<u8>, Vec<u8>)],
    limits: Limits,
) -> Result<EncodedCommittedImage, CommittedImageError> {
    stream
        .validate()
        .map_err(|_| CommittedImageError::InvalidStream)?;
    if entries.len() > limits.rows {
        return Err(CommittedImageError::LimitExceeded);
    }
    let row_count = u32::try_from(entries.len()).map_err(|_| CommittedImageError::LimitExceeded)?;
    let mut previous: Option<&[u8]> = None;
    let mut checkpoint = None;
    let mut encoded_bytes = HEADER_BYTES + CHECKSUM_BYTES;
    for (key, value) in entries {
        limits.row(key, value)?;
        check_order(&mut previous, key)?;
        let _ = u32::try_from(key.len()).map_err(|_| CommittedImageError::LimitExceeded)?;
        let _ = u32::try_from(value.len()).map_err(|_| CommittedImageError::LimitExceeded)?;
        encoded_bytes = checked_row_size(encoded_bytes, key.len(), value.len(), limits.bytes)?;
        if key.as_slice() == CHECKPOINT_KEY {
            checkpoint = Some(value.as_slice());
        }
    }
    if encoded_bytes > limits.bytes {
        return Err(CommittedImageError::LimitExceeded);
    }
    let checkpoint = checkpoint.ok_or(CommittedImageError::InvalidRows)?;
    validate_checkpoint(checkpoint, stream)?;

    let mut bytes = allocate_output(encoded_bytes)?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&SCHEMA_VERSION.to_be_bytes());
    bytes.extend_from_slice(&role_number(role).to_be_bytes());
    bytes.extend_from_slice(stream.as_bytes());
    bytes.extend_from_slice(&row_count.to_be_bytes());
    for (key, value) in entries {
        bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
    }
    let checksum: [u8; CHECKSUM_BYTES] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    Ok(EncodedCommittedImage { bytes })
}

fn decode_image(
    bytes: &[u8],
    limits: Limits,
) -> Result<DecodedCommittedImage<'_>, CommittedImageError> {
    if bytes.len() > limits.bytes {
        return Err(CommittedImageError::LimitExceeded);
    }
    if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
        return Err(CommittedImageError::Malformed);
    }
    if bytes.get(..4) != Some(MAGIC.as_slice())
        || read_u16(bytes.get(4..6).ok_or(CommittedImageError::Malformed)?)? != SCHEMA_VERSION
    {
        return Err(CommittedImageError::UnsupportedFormat);
    }
    let role = decode_role(read_u16(
        bytes.get(6..8).ok_or(CommittedImageError::Malformed)?,
    )?)?;
    let stream_bytes: [u8; 16] = bytes
        .get(8..24)
        .ok_or(CommittedImageError::Malformed)?
        .try_into()
        .map_err(|_| CommittedImageError::Malformed)?;
    let stream =
        CommittedStreamId::new(stream_bytes).map_err(|_| CommittedImageError::InvalidStream)?;
    let row_count = usize::try_from(read_u32(
        bytes.get(24..28).ok_or(CommittedImageError::Malformed)?,
    )?)
    .map_err(|_| CommittedImageError::LimitExceeded)?;
    if row_count > limits.rows {
        return Err(CommittedImageError::LimitExceeded);
    }
    let payload_end = bytes.len() - CHECKSUM_BYTES;
    let row_bytes = bytes
        .get(HEADER_BYTES..payload_end)
        .ok_or(CommittedImageError::Malformed)?;
    let mut remaining = row_bytes;
    let mut previous = None;
    let mut checkpoint = None;
    for _ in 0..row_count {
        let row = take_row(&mut remaining, limits)?;
        check_order(&mut previous, row.key)?;
        if row.key == CHECKPOINT_KEY {
            checkpoint = Some(row.value);
        }
    }
    if !remaining.is_empty() {
        return Err(CommittedImageError::Malformed);
    }
    let checkpoint = checkpoint.ok_or(CommittedImageError::InvalidRows)?;
    let checksum: [u8; CHECKSUM_BYTES] = Sha256::digest(&bytes[..payload_end]).into();
    if bytes.get(payload_end..) != Some(checksum.as_slice()) {
        return Err(CommittedImageError::ChecksumMismatch);
    }
    let checkpoint = validate_checkpoint(checkpoint, stream)?;
    Ok(DecodedCommittedImage {
        bytes,
        row_bytes,
        row_count,
        stream,
        checkpoint,
        role,
    })
}

fn take_row<'a>(
    remaining: &mut &'a [u8],
    limits: Limits,
) -> Result<CommittedImageRow<'a>, CommittedImageError> {
    let header = remaining
        .get(..ROW_HEADER_BYTES)
        .ok_or(CommittedImageError::Malformed)?;
    let key_len =
        usize::try_from(read_u32(&header[..4])?).map_err(|_| CommittedImageError::LimitExceeded)?;
    let value_len =
        usize::try_from(read_u32(&header[4..])?).map_err(|_| CommittedImageError::LimitExceeded)?;
    if key_len == 0 {
        return Err(CommittedImageError::InvalidRows);
    }
    if key_len > limits.key || value_len > limits.value {
        return Err(CommittedImageError::LimitExceeded);
    }
    let key_end = ROW_HEADER_BYTES
        .checked_add(key_len)
        .ok_or(CommittedImageError::LimitExceeded)?;
    let value_end = key_end
        .checked_add(value_len)
        .ok_or(CommittedImageError::LimitExceeded)?;
    let key = remaining
        .get(ROW_HEADER_BYTES..key_end)
        .ok_or(CommittedImageError::Malformed)?;
    let value = remaining
        .get(key_end..value_end)
        .ok_or(CommittedImageError::Malformed)?;
    let next = remaining
        .get(value_end..)
        .ok_or(CommittedImageError::Malformed)?;
    *remaining = next;
    Ok(CommittedImageRow { key, value })
}

fn check_order<'a>(
    previous: &mut Option<&'a [u8]>,
    key: &'a [u8],
) -> Result<(), CommittedImageError> {
    if previous.is_some_and(|old| old >= key) {
        return Err(CommittedImageError::InvalidRows);
    }
    *previous = Some(key);
    Ok(())
}

fn checked_row_size(
    current: usize,
    key_bytes: usize,
    value_bytes: usize,
    maximum: usize,
) -> Result<usize, CommittedImageError> {
    let size = current
        .checked_add(ROW_HEADER_BYTES)
        .and_then(|size| size.checked_add(key_bytes))
        .and_then(|size| size.checked_add(value_bytes))
        .ok_or(CommittedImageError::LimitExceeded)?;
    if size > maximum {
        return Err(CommittedImageError::LimitExceeded);
    }
    Ok(size)
}

fn validate_checkpoint(
    bytes: &[u8],
    stream: CommittedStreamId,
) -> Result<CommittedCheckpoint, CommittedImageError> {
    let checkpoint =
        decode_checkpoint(bytes).map_err(|_| CommittedImageError::InvalidCheckpoint)?;
    if checkpoint.stream() != stream {
        return Err(CommittedImageError::CheckpointStreamMismatch);
    }
    Ok(checkpoint)
}

fn allocate_output(bytes: usize) -> Result<Vec<u8>, CommittedImageError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes)
        .map_err(|_| CommittedImageError::Allocation)?;
    Ok(output)
}

fn role_number(role: CommittedImageRole) -> u16 {
    match role {
        CommittedImageRole::CreateSendV1 => CREATE_SEND_ROLE,
        CommittedImageRole::CreateSendLayout17V1 => CREATE_SEND_LAYOUT17_ROLE,
    }
}

fn decode_role(role: u16) -> Result<CommittedImageRole, CommittedImageError> {
    match role {
        CREATE_SEND_ROLE => Ok(CommittedImageRole::CreateSendV1),
        CREATE_SEND_LAYOUT17_ROLE => Ok(CommittedImageRole::CreateSendLayout17V1),
        _ => Err(CommittedImageError::UnsupportedFormat),
    }
}

fn read_u16(bytes: &[u8]) -> Result<u16, CommittedImageError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| CommittedImageError::Malformed)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(bytes: &[u8]) -> Result<u32, CommittedImageError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| CommittedImageError::Malformed)?;
    Ok(u32::from_be_bytes(bytes))
}
