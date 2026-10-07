use super::*;

use storage::{MemoryStore, StateStore, WriteBatch};

use super::super::encode_checkpoint;
use crate::{CommittedEntryId, CommittedEntryMark, CommittedMembership, Timestamp};

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([7; 16]).unwrap()
}

fn checkpoint() -> Vec<u8> {
    encode_checkpoint(&CommittedCheckpoint::initial(stream())).unwrap()
}

fn entries() -> Vec<(Vec<u8>, Vec<u8>)> {
    vec![
        (CHECKPOINT_KEY.to_vec(), checkpoint()),
        (b"\x7fprivate-key".to_vec(), b"private-body".to_vec()),
    ]
}

fn snapshot(rows: &[(Vec<u8>, Vec<u8>)]) -> StoreSnapshot {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in rows {
        batch.push_put(key.clone(), value.clone());
    }
    store.apply(batch).unwrap();
    store.snapshot().unwrap()
}

fn encode(rows: &[(Vec<u8>, Vec<u8>)]) -> EncodedCommittedImage {
    EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, stream(), &snapshot(rows))
        .unwrap()
}

fn raw_container(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&SCHEMA_VERSION.to_be_bytes());
    bytes.extend_from_slice(&CREATE_SEND_ROLE.to_be_bytes());
    bytes.extend_from_slice(stream().as_bytes());
    bytes.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for (key, value) in rows {
        bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
    }
    let checksum: [u8; CHECKSUM_BYTES] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    bytes
}

fn resign(bytes: &mut [u8]) {
    let end = bytes.len() - CHECKSUM_BYTES;
    let checksum: [u8; CHECKSUM_BYTES] = Sha256::digest(&bytes[..end]).into();
    bytes[end..].copy_from_slice(&checksum);
}

fn tiny_limits(bytes: usize) -> Limits {
    Limits {
        bytes,
        rows: 8,
        key: 32,
        value: 128,
    }
}

#[test]
fn snapshot_round_trip_keeps_exact_rows_and_stream() {
    let rows = entries();
    let encoded = encode(&rows);
    let decoded = DecodedCommittedImage::decode(encoded.as_bytes()).unwrap();
    assert_eq!(decoded.role(), CommittedImageRole::CreateSendV1);
    assert_eq!(decoded.stream(), stream());
    assert_eq!(
        decoded.checkpoint(),
        &CommittedCheckpoint::initial(stream())
    );
    assert_eq!(decoded.row_count(), rows.len());
    assert_eq!(decoded.encoded_bytes(), encoded.as_bytes());
    assert_eq!(
        decoded
            .rows()
            .map(|row| (row.key(), row.value()))
            .collect::<Vec<_>>(),
        rows.iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>()
    );
    assert!(!encoded.is_empty());
}

#[test]
fn fixed_width_header_and_checksum_cover_the_complete_container() {
    let rows = vec![(CHECKPOINT_KEY.to_vec(), checkpoint())];
    let encoded = encode(&rows);
    assert_eq!(&encoded.as_bytes()[..8], b"SWYI\0\x01\0\x01");
    assert_eq!(&encoded.as_bytes()[8..24], stream().as_bytes());
    assert_eq!(&encoded.as_bytes()[24..28], &1_u32.to_be_bytes());
    assert_eq!(
        encoded.len(),
        HEADER_BYTES + ROW_HEADER_BYTES + 1 + rows[0].1.len() + CHECKSUM_BYTES
    );
    assert_eq!(encoded.as_bytes(), raw_container(&rows));
    assert_eq!(MAX_COMMITTED_IMAGE_BYTES, 64 * 1024 * 1024);
}

#[test]
fn row_keys_and_values_borrow_the_encoded_buffer() {
    let encoded = encode(&entries());
    let decoded = DecodedCommittedImage::decode(encoded.as_bytes()).unwrap();
    let mut rows = decoded.rows();
    let first = rows.next().unwrap();
    let first_key = HEADER_BYTES + ROW_HEADER_BYTES;
    assert!(std::ptr::eq(
        first.key().as_ptr(),
        encoded.as_bytes()[first_key..].as_ptr()
    ));
    assert!(std::ptr::eq(
        first.value().as_ptr(),
        encoded.as_bytes()[first_key + first.key().len()..].as_ptr()
    ));
    let second = rows.next().unwrap();
    let second_key = first_key + first.key().len() + first.value().len() + ROW_HEADER_BYTES;
    assert!(std::ptr::eq(
        second.value().as_ptr(),
        encoded.as_bytes()[second_key + second.key().len()..].as_ptr()
    ));
    assert!(rows.next().is_none());
    assert!(rows.next().is_none());
}

#[test]
fn structural_packaging_does_not_certify_business_records() {
    let rows = vec![
        (b"\x03not-a-message-key".to_vec(), b"not-a-message".to_vec()),
        (CHECKPOINT_KEY.to_vec(), checkpoint()),
        (b"\xffunknown-tag".to_vec(), Vec::new()),
    ];
    let encoded = encode(&rows);
    let decoded = DecodedCommittedImage::decode(encoded.as_bytes()).unwrap();
    assert_eq!(decoded.row_count(), 3);
    assert_eq!(decoded.rows().last().unwrap().value(), &[]);
}

#[test]
fn complete_source_ordering_is_required_without_sorting_or_deduplication() {
    let rows = entries();
    for invalid in [
        vec![rows[1].clone(), rows[0].clone()],
        vec![rows[0].clone(), rows[0].clone()],
    ] {
        assert_eq!(
            encode_entries(
                CommittedImageRole::CreateSendV1,
                stream(),
                &invalid,
                Limits::CURRENT
            )
            .unwrap_err(),
            CommittedImageError::InvalidRows
        );
        assert_eq!(
            DecodedCommittedImage::decode(&raw_container(&invalid)).unwrap_err(),
            CommittedImageError::InvalidRows
        );
    }
}

#[test]
fn empty_keys_and_missing_or_duplicate_checkpoints_are_refused() {
    for rows in [
        Vec::new(),
        vec![(b"other".to_vec(), Vec::new())],
        vec![
            (Vec::new(), Vec::new()),
            (CHECKPOINT_KEY.to_vec(), checkpoint()),
        ],
        vec![
            (CHECKPOINT_KEY.to_vec(), checkpoint()),
            (CHECKPOINT_KEY.to_vec(), checkpoint()),
        ],
    ] {
        assert_eq!(
            encode_entries(
                CommittedImageRole::CreateSendV1,
                stream(),
                &rows,
                Limits::CURRENT
            )
            .unwrap_err(),
            CommittedImageError::InvalidRows
        );
        assert_eq!(
            DecodedCommittedImage::decode(&raw_container(&rows)).unwrap_err(),
            CommittedImageError::InvalidRows
        );
    }
}

#[test]
fn exact_tiny_serialized_limit_is_inclusive_and_counts_framing() {
    let rows = entries();
    let size = raw_container(&rows).len();
    let encoded = encode_entries(
        CommittedImageRole::CreateSendV1,
        stream(),
        &rows,
        tiny_limits(size),
    )
    .unwrap();
    assert_eq!(encoded.len(), size);
    assert!(decode_image(encoded.as_bytes(), tiny_limits(size)).is_ok());
    assert_eq!(
        encode_entries(
            CommittedImageRole::CreateSendV1,
            stream(),
            &rows,
            tiny_limits(size - 1),
        )
        .unwrap_err(),
        CommittedImageError::LimitExceeded
    );
    assert_eq!(
        decode_image(encoded.as_bytes(), tiny_limits(size - 1)).unwrap_err(),
        CommittedImageError::LimitExceeded
    );
}

#[test]
fn count_key_and_value_limits_apply_to_encode_and_decode() {
    let rows = entries();
    let encoded = raw_container(&rows);
    let base = tiny_limits(encoded.len());
    for limits in [
        Limits { rows: 1, ..base },
        Limits {
            key: rows[1].0.len() - 1,
            ..base
        },
        Limits {
            value: rows[0].1.len() - 1,
            ..base
        },
    ] {
        assert_eq!(
            encode_entries(CommittedImageRole::CreateSendV1, stream(), &rows, limits).unwrap_err(),
            CommittedImageError::LimitExceeded
        );
        assert_eq!(
            decode_image(&encoded, limits).unwrap_err(),
            CommittedImageError::LimitExceeded
        );
    }
}

#[test]
fn checked_size_and_capacity_failures_are_static_without_large_allocations() {
    assert_eq!(
        checked_row_size(usize::MAX, 1, 1, usize::MAX),
        Err(CommittedImageError::LimitExceeded)
    );
    assert_eq!(
        checked_row_size(0, usize::MAX, 1, usize::MAX),
        Err(CommittedImageError::LimitExceeded)
    );
    assert_eq!(
        allocate_output(usize::MAX),
        Err(CommittedImageError::Allocation)
    );
}

#[test]
fn huge_declared_lengths_and_row_counts_fail_before_row_extraction() {
    let encoded = encode(&entries());
    for offset in [24, HEADER_BYTES, HEADER_BYTES + 4] {
        let mut bytes = encoded.as_bytes().to_vec();
        bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            DecodedCommittedImage::decode(&bytes).unwrap_err(),
            CommittedImageError::LimitExceeded
        );
    }
}

#[test]
fn exact_row_count_and_payload_exhaustion_are_required() {
    let encoded = encode(&entries());
    let mut too_few = encoded.as_bytes().to_vec();
    too_few[24..28].copy_from_slice(&1_u32.to_be_bytes());
    resign(&mut too_few);
    assert_eq!(
        DecodedCommittedImage::decode(&too_few).unwrap_err(),
        CommittedImageError::Malformed
    );
    let mut too_many = encoded.as_bytes().to_vec();
    too_many[24..28].copy_from_slice(&3_u32.to_be_bytes());
    resign(&mut too_many);
    assert_eq!(
        DecodedCommittedImage::decode(&too_many).unwrap_err(),
        CommittedImageError::Malformed
    );
    let mut extra = encoded.as_bytes().to_vec();
    extra.insert(extra.len() - CHECKSUM_BYTES, 0);
    resign(&mut extra);
    assert_eq!(
        DecodedCommittedImage::decode(&extra).unwrap_err(),
        CommittedImageError::Malformed
    );
}

#[test]
fn all_truncated_prefixes_are_refused() {
    let encoded = encode(&entries());
    for end in 0..encoded.len() {
        assert!(DecodedCommittedImage::decode(&encoded.as_bytes()[..end]).is_err());
    }
}

#[test]
fn unknown_magic_schema_and_role_are_refused() {
    let encoded = encode(&entries());
    for (offset, value) in [(0, b'X'), (4, 1), (5, 2), (6, 1), (7, 3)] {
        let mut bytes = encoded.as_bytes().to_vec();
        bytes[offset] = value;
        resign(&mut bytes);
        assert_eq!(
            DecodedCommittedImage::decode(&bytes).unwrap_err(),
            CommittedImageError::UnsupportedFormat
        );
    }
}

#[test]
fn stream_is_nonzero_and_matches_the_canonical_checkpoint() {
    let rows = entries();
    let invalid_stream: CommittedStreamId = postcard::from_bytes(&[0; 16]).unwrap();
    assert_eq!(
        encode_entries(
            CommittedImageRole::CreateSendV1,
            invalid_stream,
            &rows,
            Limits::CURRENT
        )
        .unwrap_err(),
        CommittedImageError::InvalidStream
    );
    let foreign = CommittedStreamId::new([8; 16]).unwrap();
    assert_eq!(
        encode_entries(
            CommittedImageRole::CreateSendV1,
            foreign,
            &rows,
            Limits::CURRENT
        )
        .unwrap_err(),
        CommittedImageError::CheckpointStreamMismatch
    );
    let encoded = encode(&rows);
    let mut zero = encoded.as_bytes().to_vec();
    zero[8..24].fill(0);
    resign(&mut zero);
    assert_eq!(
        DecodedCommittedImage::decode(&zero).unwrap_err(),
        CommittedImageError::InvalidStream
    );
    let mut mismatch = encoded.as_bytes().to_vec();
    mismatch[8..24].copy_from_slice(foreign.as_bytes());
    resign(&mut mismatch);
    assert_eq!(
        DecodedCommittedImage::decode(&mismatch).unwrap_err(),
        CommittedImageError::CheckpointStreamMismatch
    );
}

#[test]
fn checksum_covers_valid_header_and_row_content_changes() {
    let encoded = encode(&entries());
    let mut header = encoded.as_bytes().to_vec();
    header[8] ^= 1;
    assert_eq!(
        DecodedCommittedImage::decode(&header).unwrap_err(),
        CommittedImageError::ChecksumMismatch
    );
    let mut body = encoded.as_bytes().to_vec();
    let last_value = body.len() - CHECKSUM_BYTES - 1;
    body[last_value] ^= 1;
    assert_eq!(
        DecodedCommittedImage::decode(&body).unwrap_err(),
        CommittedImageError::ChecksumMismatch
    );
    let mut checksum = encoded.as_bytes().to_vec();
    *checksum.last_mut().unwrap() ^= 1;
    assert_eq!(
        DecodedCommittedImage::decode(&checksum).unwrap_err(),
        CommittedImageError::ChecksumMismatch
    );
}

#[test]
fn checksum_is_verified_before_bounded_checkpoint_decoding() {
    let rows = vec![(
        CHECKPOINT_KEY.to_vec(),
        b"private-invalid-checkpoint".to_vec(),
    )];
    let mut bytes = raw_container(&rows);
    *bytes.last_mut().unwrap() ^= 1;
    assert_eq!(
        DecodedCommittedImage::decode(&bytes).unwrap_err(),
        CommittedImageError::ChecksumMismatch
    );
    resign(&mut bytes);
    assert_eq!(
        DecodedCommittedImage::decode(&bytes).unwrap_err(),
        CommittedImageError::InvalidCheckpoint
    );
    assert_eq!(
        encode_entries(
            CommittedImageRole::CreateSendV1,
            stream(),
            &rows,
            Limits::CURRENT
        )
        .unwrap_err(),
        CommittedImageError::InvalidCheckpoint
    );
}

#[test]
fn noncanonical_and_trailing_checkpoint_bytes_are_refused() {
    let canonical = checkpoint();
    let mut overlong = canonical.clone();
    // Initial checkpoint: header5 + stream16 + lastNone + previousNone.
    overlong.splice(23..24, [0x80, 0]);
    let mut trailing = canonical;
    trailing.push(0);
    for bytes in [overlong, trailing] {
        let rows = vec![(CHECKPOINT_KEY.to_vec(), bytes)];
        assert_eq!(
            encode_entries(
                CommittedImageRole::CreateSendV1,
                stream(),
                &rows,
                Limits::CURRENT
            )
            .unwrap_err(),
            CommittedImageError::InvalidCheckpoint
        );
        assert_eq!(
            DecodedCommittedImage::decode(&raw_container(&rows)).unwrap_err(),
            CommittedImageError::InvalidCheckpoint
        );
    }
}

#[test]
fn full_checkpoint_metadata_is_preserved_without_interpreting_membership() {
    let entry = CommittedEntryId {
        term: 2,
        node_id: 7,
        index: 0,
    };
    let expected = CommittedCheckpoint {
        stream: stream(),
        last: Some(CommittedEntryMark {
            id: entry,
            fingerprint: [3; 32],
        }),
        previous: None,
        highest_timestamp: Timestamp::from_millis(123),
        membership: Some(CommittedMembership {
            source: entry,
            schema_version: 3,
            payload: b"opaque-membership".to_vec(),
        }),
    };
    let rows = vec![(
        CHECKPOINT_KEY.to_vec(),
        encode_checkpoint(&expected).unwrap(),
    )];
    let encoded = encode(&rows);
    let decoded = DecodedCommittedImage::decode(encoded.as_bytes()).unwrap();
    assert_eq!(decoded.checkpoint(), &expected);
}

#[test]
fn debug_and_errors_never_include_source_keys_values_or_checkpoint_payloads() {
    let encoded = encode(&entries());
    let decoded = DecodedCommittedImage::decode(encoded.as_bytes()).unwrap();
    let text = format!(
        "{encoded:?} {decoded:?} {:?} {:?}",
        decoded.rows(),
        decoded.rows().last()
    );
    assert!(!text.contains("private-key"));
    assert!(!text.contains("private-body"));
    let checksum = format!(
        "{:?}",
        &encoded.as_bytes()[encoded.len() - CHECKSUM_BYTES..]
    );
    assert!(!text.contains(&checksum));
    for error in [
        CommittedImageError::UnsupportedFormat,
        CommittedImageError::Malformed,
        CommittedImageError::LimitExceeded,
        CommittedImageError::InvalidStream,
        CommittedImageError::InvalidRows,
        CommittedImageError::InvalidCheckpoint,
        CommittedImageError::CheckpointStreamMismatch,
        CommittedImageError::ChecksumMismatch,
        CommittedImageError::Allocation,
    ] {
        assert!(!format!("{error:?} {error}").contains("private"));
        assert!(std::error::Error::source(&error).is_none());
    }
}
