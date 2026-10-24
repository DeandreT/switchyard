use sha2::{Digest, Sha256};
use storage::{Key, Value};
use thiserror::Error;

pub const SNAPSHOT_MAGIC: [u8; 4] = *b"SWSS";
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;
pub const SNAPSHOT_LAYOUT_VERSION: u32 = 2;
pub const SNAPSHOT_FRAME_HASH_SCOPE: &[u8] = b"switchyard snapshot frame v1\0";
pub const SNAPSHOT_FRAME_DIGEST_BYTES: usize = 32;

const HEADER_WITHOUT_IDENTITY_BYTES: usize = 53;
const RECORD_LENGTH_BYTES: usize = 16;

/// Trusted caller policy, independent of proposal or backend limits.
///
/// These caps bound accepted input and copied output, not already materialized
/// inputs, allocator overhead, semantic health or scan latency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotLimits {
    pub max_encoded_bytes: usize,
    pub max_records: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotLimit {
    EncodedBytes,
    Records,
    KeyBytes,
    ValueBytes,
}

/// Fixed-data errors preserve allocation-free decoding, including refusals.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SnapshotFormatError {
    #[error("snapshot magic is invalid")]
    InvalidMagic,
    #[error("snapshot format {found} is unsupported; expected {expected}")]
    UnsupportedFormatVersion { found: u32, expected: u32 },
    #[error("snapshot layout {found} is unsupported; expected {expected}")]
    UnsupportedLayoutVersion { found: u32, expected: u32 },
    #[error("snapshot frontiers must satisfy {applied} <= {committed} <= {last_appended}")]
    InvalidFrontiers {
        applied: u64,
        committed: u64,
        last_appended: u64,
    },
    #[error("snapshot applied index {applied} has invalid digest presence {has_digest}")]
    InvalidLatestIdentity { applied: u64, has_digest: bool },
    #[error("snapshot {kind:?} limit {maximum} is exceeded by {requested}")]
    LimitExceeded {
        kind: SnapshotLimit,
        requested: u64,
        maximum: usize,
    },
    #[error("snapshot frame is malformed: {detail}")]
    Malformed { detail: &'static str },
    #[error("snapshot records are not strictly ordered and unique")]
    NonCanonicalRecords,
    #[error("snapshot frame digest differs")]
    DigestMismatch,
    #[error("snapshot size is not representable")]
    SizeOverflow,
    #[error("snapshot output allocation failed")]
    AllocationFailed,
}

/// Checked declarations only; no actual F0/F1 or domain state is certified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    layout_version: u32,
    applied: u64,
    committed: u64,
    last_appended: u64,
    latest_applied_digest: Option<[u8; 32]>,
}

impl SnapshotManifest {
    pub fn new(
        layout_version: u32,
        applied: u64,
        committed: u64,
        last_appended: u64,
        latest_applied_digest: Option<[u8; 32]>,
    ) -> Result<Self, SnapshotFormatError> {
        let manifest = Self {
            layout_version,
            applied,
            committed,
            last_appended,
            latest_applied_digest,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn layout_version(&self) -> u32 {
        self.layout_version
    }

    pub fn applied_index(&self) -> u64 {
        self.applied
    }

    pub fn committed_index(&self) -> u64 {
        self.committed
    }

    pub fn last_appended_index(&self) -> u64 {
        self.last_appended
    }

    pub fn latest_applied_digest(&self) -> Option<[u8; 32]> {
        self.latest_applied_digest
    }

    fn validate(&self) -> Result<(), SnapshotFormatError> {
        if self.layout_version != SNAPSHOT_LAYOUT_VERSION {
            return Err(SnapshotFormatError::UnsupportedLayoutVersion {
                found: self.layout_version,
                expected: SNAPSHOT_LAYOUT_VERSION,
            });
        }
        if self.applied > self.committed || self.committed > self.last_appended {
            return Err(SnapshotFormatError::InvalidFrontiers {
                applied: self.applied,
                committed: self.committed,
                last_appended: self.last_appended,
            });
        }
        if (self.applied != 0) != self.latest_applied_digest.is_some() {
            return Err(SnapshotFormatError::InvalidLatestIdentity {
                applied: self.applied,
                has_digest: self.latest_applied_digest.is_some(),
            });
        }
        Ok(())
    }
}

/// Fully checked frame whose record slices borrow the original input.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotView<'a> {
    manifest: SnapshotManifest,
    records: &'a [u8],
    record_count: usize,
}

impl SnapshotView<'_> {
    pub fn manifest(&self) -> SnapshotManifest {
        self.manifest
    }

    pub fn records(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        Records {
            cursor: Cursor::new(self.records),
            remaining: self.record_count,
        }
    }
}

/// Stateless structural codec; it performs no store access or normalization.
#[derive(Debug)]
pub struct SnapshotFormat;

/// Internal preflight facts; these do not certify the records' domain meaning.
#[derive(Debug)]
pub(crate) struct FrameSize {
    count: u64,
    section_length: u64,
    encoded_bytes: usize,
}

impl SnapshotFormat {
    /// Preflights the whole frame before one fallible output reservation.
    pub fn encode(
        manifest: &SnapshotManifest,
        records: &[(Key, Value)],
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, SnapshotFormatError> {
        manifest.validate()?;
        let FrameSize {
            count,
            section_length,
            encoded_bytes,
        } = Self::preflight(manifest.latest_applied_digest.is_some(), records, limits)?;

        let mut output = Vec::new();
        output
            .try_reserve_exact(encoded_bytes)
            .map_err(|_| SnapshotFormatError::AllocationFailed)?;
        output.extend_from_slice(&SNAPSHOT_MAGIC);
        output.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_be_bytes());
        output.extend_from_slice(&manifest.layout_version.to_be_bytes());
        output.extend_from_slice(&manifest.applied.to_be_bytes());
        output.extend_from_slice(&manifest.committed.to_be_bytes());
        output.extend_from_slice(&manifest.last_appended.to_be_bytes());
        match manifest.latest_applied_digest {
            None => output.push(0),
            Some(digest) => {
                output.push(1);
                output.extend_from_slice(&digest);
            }
        }
        output.extend_from_slice(&count.to_be_bytes());
        output.extend_from_slice(&section_length.to_be_bytes());
        for (key, value) in records {
            output.extend_from_slice(&wire_length(key.len())?.to_be_bytes());
            output.extend_from_slice(&wire_length(value.len())?.to_be_bytes());
            output.extend_from_slice(key);
            output.extend_from_slice(value);
        }
        let digest = frame_digest(&output);
        output.extend_from_slice(&digest);
        Ok(output)
    }

    /// The encoder's exact record checks and size arithmetic, without allocation.
    /// Identity presence selects only the header width, not verified ownership.
    /// Capture may supply an untrusted structural hint before semantic validation;
    /// encode validates the manifest and repeats these checks with its identity.
    pub(crate) fn preflight(
        has_applied_identity: bool,
        records: &[(Key, Value)],
        limits: SnapshotLimits,
    ) -> Result<FrameSize, SnapshotFormatError> {
        let count = wire_length(records.len())?;
        check_limit(SnapshotLimit::Records, count, limits.max_records)?;
        let mut section_bytes = 0;
        let mut previous: Option<&[u8]> = None;
        for (key, value) in records {
            check_limit(
                SnapshotLimit::KeyBytes,
                wire_length(key.len())?,
                limits.max_key_bytes,
            )?;
            check_limit(
                SnapshotLimit::ValueBytes,
                wire_length(value.len())?,
                limits.max_value_bytes,
            )?;
            if previous.is_some_and(|previous| previous >= key.as_slice()) {
                return Err(SnapshotFormatError::NonCanonicalRecords);
            }
            section_bytes = checked_add(section_bytes, RECORD_LENGTH_BYTES)?;
            section_bytes = checked_add(section_bytes, key.len())?;
            section_bytes = checked_add(section_bytes, value.len())?;
            previous = Some(key);
        }
        let section_length = wire_length(section_bytes)?;
        let identity_bytes = if has_applied_identity { 32 } else { 0 };
        let header_bytes = checked_add(HEADER_WITHOUT_IDENTITY_BYTES, identity_bytes)?;
        let encoded_bytes = checked_add(
            checked_add(header_bytes, section_bytes)?,
            SNAPSHOT_FRAME_DIGEST_BYTES,
        )?;
        check_limit(
            SnapshotLimit::EncodedBytes,
            wire_length(encoded_bytes)?,
            limits.max_encoded_bytes,
        )?;

        Ok(FrameSize {
            count,
            section_length,
            encoded_bytes,
        })
    }

    /// Validates the complete capped input without allocating owned records.
    pub fn decode(
        input: &[u8],
        limits: SnapshotLimits,
    ) -> Result<SnapshotView<'_>, SnapshotFormatError> {
        check_limit(
            SnapshotLimit::EncodedBytes,
            wire_length(input.len())?,
            limits.max_encoded_bytes,
        )?;
        let mut cursor = Cursor::new(input);
        if cursor.take(SNAPSHOT_MAGIC.len())? != SNAPSHOT_MAGIC.as_slice() {
            return Err(SnapshotFormatError::InvalidMagic);
        }
        let version = cursor.u32()?;
        if version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotFormatError::UnsupportedFormatVersion {
                found: version,
                expected: SNAPSHOT_FORMAT_VERSION,
            });
        }
        let layout = cursor.u32()?;
        let applied = cursor.u64()?;
        let committed = cursor.u64()?;
        let last_appended = cursor.u64()?;
        let latest_applied_digest = match cursor.take(1)? {
            [0] => None,
            [1] => Some(cursor.array::<32>()?),
            _ => return Err(malformed("latest identity tag is unsupported")),
        };
        let manifest = SnapshotManifest::new(
            layout,
            applied,
            committed,
            last_appended,
            latest_applied_digest,
        )?;
        let count = cursor.u64()?;
        check_limit(SnapshotLimit::Records, count, limits.max_records)?;
        let section_length = cursor.u64()?;
        let minimum = count
            .checked_mul(RECORD_LENGTH_BYTES as u64)
            .ok_or(SnapshotFormatError::SizeOverflow)?;
        if minimum > section_length {
            return Err(malformed("record count exceeds the section's minimum size"));
        }
        let count = usize::try_from(count).map_err(|_| SnapshotFormatError::SizeOverflow)?;
        let section_length =
            usize::try_from(section_length).map_err(|_| SnapshotFormatError::SizeOverflow)?;
        let records = cursor.take(section_length)?;
        let digest = cursor.array::<32>()?;
        if !cursor.is_empty() {
            return Err(malformed("frame has trailing bytes"));
        }

        let mut section = Cursor::new(records);
        let mut previous: Option<&[u8]> = None;
        for _ in 0..count {
            let key_length = section.u64()?;
            let value_length = section.u64()?;
            check_limit(SnapshotLimit::KeyBytes, key_length, limits.max_key_bytes)?;
            check_limit(
                SnapshotLimit::ValueBytes,
                value_length,
                limits.max_value_bytes,
            )?;
            let key_length =
                usize::try_from(key_length).map_err(|_| SnapshotFormatError::SizeOverflow)?;
            let value_length =
                usize::try_from(value_length).map_err(|_| SnapshotFormatError::SizeOverflow)?;
            let key = section.take(key_length)?;
            section.take(value_length)?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err(SnapshotFormatError::NonCanonicalRecords);
            }
            previous = Some(key);
        }
        if !section.is_empty() {
            return Err(malformed("record count does not exhaust its section"));
        }
        let hashed_length = input
            .len()
            .checked_sub(SNAPSHOT_FRAME_DIGEST_BYTES)
            .ok_or(SnapshotFormatError::SizeOverflow)?;
        if frame_digest(&input[..hashed_length]) != digest {
            return Err(SnapshotFormatError::DigestMismatch);
        }
        Ok(SnapshotView {
            manifest,
            records,
            record_count: count,
        })
    }
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], SnapshotFormatError> {
        let (bytes, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or_else(|| malformed("frame is truncated"))?;
        self.remaining = remaining;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SnapshotFormatError> {
        self.take(N)?
            .try_into()
            .map_err(|_| malformed("fixed field length differs"))
    }

    fn u32(&mut self) -> Result<u32, SnapshotFormatError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, SnapshotFormatError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

struct Records<'a> {
    cursor: Cursor<'a>,
    remaining: usize,
}

impl<'a> Iterator for Records<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // Only a fully validated immutable section can construct this iterator.
        let key_length = usize::try_from(self.cursor.u64().ok()?).ok()?;
        let value_length = usize::try_from(self.cursor.u64().ok()?).ok()?;
        let key = self.cursor.take(key_length).ok()?;
        let value = self.cursor.take(value_length).ok()?;
        self.remaining -= 1;
        Some((key, value))
    }
}

fn wire_length(length: usize) -> Result<u64, SnapshotFormatError> {
    u64::try_from(length).map_err(|_| SnapshotFormatError::SizeOverflow)
}

fn checked_add(left: usize, right: usize) -> Result<usize, SnapshotFormatError> {
    left.checked_add(right)
        .ok_or(SnapshotFormatError::SizeOverflow)
}

fn check_limit(
    kind: SnapshotLimit,
    requested: u64,
    maximum: usize,
) -> Result<(), SnapshotFormatError> {
    if u128::from(requested) > maximum as u128 {
        Err(SnapshotFormatError::LimitExceeded {
            kind,
            requested,
            maximum,
        })
    } else {
        Ok(())
    }
}

fn frame_digest(bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(SNAPSHOT_FRAME_HASH_SCOPE);
    digest.update(bytes);
    digest.finalize().into()
}

fn malformed(detail: &'static str) -> SnapshotFormatError {
    SnapshotFormatError::Malformed { detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNBOUNDED: SnapshotLimits = SnapshotLimits {
        max_encoded_bytes: usize::MAX,
        max_records: usize::MAX,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
    };

    #[test]
    fn shared_preflight_preserves_exact_identity_and_record_boundaries() {
        let records = vec![(vec![1], vec![0, 255]), (vec![2, 3], vec![])];
        for identity in [None, Some([0x42; 32])] {
            let applied = u64::from(identity.is_some());
            let manifest =
                SnapshotManifest::new(SNAPSHOT_LAYOUT_VERSION, applied, 1, 1, identity).unwrap();
            let size = SnapshotFormat::preflight(identity.is_some(), &records, UNBOUNDED).unwrap();
            assert_eq!(size.count, 2);
            assert_eq!(size.section_length, 37);
            assert_eq!(
                size.encoded_bytes,
                if identity.is_some() { 154 } else { 122 }
            );
            let exact = SnapshotLimits {
                max_encoded_bytes: size.encoded_bytes,
                max_records: 2,
                max_key_bytes: 2,
                max_value_bytes: 2,
            };
            assert_eq!(
                SnapshotFormat::preflight(identity.is_some(), &records, exact)
                    .unwrap()
                    .encoded_bytes,
                size.encoded_bytes
            );
            let encoded = SnapshotFormat::encode(&manifest, &records, exact).unwrap();
            assert_eq!(encoded.len(), size.encoded_bytes);
            assert_eq!(
                SnapshotFormat::encode(&manifest, &records, UNBOUNDED).unwrap(),
                encoded
            );
            for kind in [
                SnapshotLimit::Records,
                SnapshotLimit::KeyBytes,
                SnapshotLimit::ValueBytes,
                SnapshotLimit::EncodedBytes,
            ] {
                let mut smaller = exact;
                let cap = match kind {
                    SnapshotLimit::Records => &mut smaller.max_records,
                    SnapshotLimit::KeyBytes => &mut smaller.max_key_bytes,
                    SnapshotLimit::ValueBytes => &mut smaller.max_value_bytes,
                    SnapshotLimit::EncodedBytes => &mut smaller.max_encoded_bytes,
                };
                let requested = *cap;
                *cap -= 1;
                let expected = SnapshotFormatError::LimitExceeded {
                    kind,
                    requested: u64::try_from(requested).unwrap(),
                    maximum: requested - 1,
                };
                assert_eq!(
                    SnapshotFormat::preflight(identity.is_some(), &records, smaller).unwrap_err(),
                    expected
                );
                assert_eq!(
                    SnapshotFormat::encode(&manifest, &records, smaller).unwrap_err(),
                    expected
                );
            }
        }
    }

    #[test]
    fn shared_preflight_keeps_empty_frame_and_validation_priority() {
        let manifest = SnapshotManifest::new(SNAPSHOT_LAYOUT_VERSION, 0, 0, 0, None).unwrap();
        let exact = SnapshotLimits {
            max_encoded_bytes: 85,
            max_records: 0,
            max_key_bytes: 0,
            max_value_bytes: 0,
        };
        assert_eq!(
            SnapshotFormat::preflight(false, &[], exact)
                .unwrap()
                .encoded_bytes,
            85
        );
        assert_eq!(
            SnapshotFormat::encode(&manifest, &[], exact).unwrap().len(),
            85
        );
        let smaller = SnapshotLimits {
            max_encoded_bytes: 84,
            ..exact
        };
        let expected = SnapshotFormatError::LimitExceeded {
            kind: SnapshotLimit::EncodedBytes,
            requested: 85,
            maximum: 84,
        };
        assert_eq!(
            SnapshotFormat::preflight(false, &[], smaller).unwrap_err(),
            expected
        );
        assert_eq!(
            SnapshotFormat::encode(&manifest, &[], smaller).unwrap_err(),
            expected
        );

        let duplicates = vec![(vec![1], vec![]), (vec![1], vec![])];
        assert_eq!(
            SnapshotFormat::preflight(false, &duplicates, UNBOUNDED).unwrap_err(),
            SnapshotFormatError::NonCanonicalRecords
        );
        assert_eq!(
            SnapshotFormat::encode(&manifest, &duplicates, UNBOUNDED).unwrap_err(),
            SnapshotFormatError::NonCanonicalRecords
        );
        let capped = SnapshotLimits {
            max_records: 1,
            ..UNBOUNDED
        };
        let expected = SnapshotFormatError::LimitExceeded {
            kind: SnapshotLimit::Records,
            requested: 2,
            maximum: 1,
        };
        assert_eq!(
            SnapshotFormat::preflight(false, &duplicates, capped).unwrap_err(),
            expected
        );
        assert_eq!(
            SnapshotFormat::encode(&manifest, &duplicates, capped).unwrap_err(),
            expected
        );
    }

    #[test]
    fn encode_validates_manifest_before_shared_record_preflight() {
        let valid = SnapshotManifest::new(SNAPSHOT_LAYOUT_VERSION, 0, 0, 0, None).unwrap();
        let records = vec![(vec![1], vec![])];
        let capped = SnapshotLimits {
            max_records: 0,
            ..UNBOUNDED
        };
        for (manifest, expected) in [
            (
                SnapshotManifest {
                    layout_version: u32::MAX,
                    ..valid
                },
                SnapshotFormatError::UnsupportedLayoutVersion {
                    found: u32::MAX,
                    expected: SNAPSHOT_LAYOUT_VERSION,
                },
            ),
            (
                SnapshotManifest {
                    committed: 1,
                    ..valid
                },
                SnapshotFormatError::InvalidFrontiers {
                    applied: 0,
                    committed: 1,
                    last_appended: 0,
                },
            ),
            (
                SnapshotManifest {
                    latest_applied_digest: Some([0; 32]),
                    ..valid
                },
                SnapshotFormatError::InvalidLatestIdentity {
                    applied: 0,
                    has_digest: true,
                },
            ),
        ] {
            assert_eq!(
                SnapshotFormat::encode(&manifest, &records, capped).unwrap_err(),
                expected
            );
        }
    }
}
