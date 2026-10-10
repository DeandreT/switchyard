//! Pure byte-format controls, not semantic image or backend certification.
//! Loop cases are scenarios within the annotated tests, not extra identities.

use cluster::{
    SnapshotFormat, SnapshotFormatError, SnapshotLimit, SnapshotLimits, SnapshotManifest,
};
use sha2::{Digest, Sha256};
use storage::{Key, Value};

const SCOPE: &[u8] = b"switchyard snapshot frame v1\0";
const DIGEST_BYTES: usize = 32;
const IDENTITY: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];

// Literal frame fixtures were independently hashed.
const EMPTY_HEX: &str = concat!(
    "53575353",
    "00000001",
    "00000002",
    "0000000000000000",
    "0000000000000000",
    "0000000000000000",
    "00",
    "0000000000000000",
    "0000000000000000",
    "fdccefc15fb8a84b95e2774f93d74697aef1265927e3abf4d87ced937d388efa",
);
const BINARY_HEX: &str = concat!(
    "53575353",
    "00000001",
    "00000002",
    "0000000000000002",
    "0000000000000003",
    "0000000000000004",
    "01",
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    "0000000000000003",
    "0000000000000038",
    "0000000000000000",
    "0000000000000002",
    "00ff",
    "0000000000000001",
    "0000000000000000",
    "00",
    "0000000000000002",
    "0000000000000003",
    "80ff",
    "7f0001",
    "92cc73e0ad086498015a57540cf81e924ac1211bc4b47bb0b1d5cb10b02199e7",
);
const MAX_HEX: &str = concat!(
    "53575353",
    "00000001",
    "00000002",
    "ffffffffffffffff",
    "ffffffffffffffff",
    "ffffffffffffffff",
    "01",
    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    "0000000000000000",
    "0000000000000000",
    "7380f7cccdd93ebd9a63f54268a49238999a4033ce37cf8f88ae2f96c0450381",
);

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("invalid literal hexadecimal byte"),
    }
}

fn hex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2));
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| nibble(pair[0]) * 16 + nibble(pair[1]))
        .collect()
}

fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_encoded_bytes: 4096,
        max_records: 64,
        max_key_bytes: 1024,
        max_value_bytes: 2048,
    }
}

fn exact_binary_limits() -> SnapshotLimits {
    SnapshotLimits {
        max_encoded_bytes: 173,
        max_records: 3,
        max_key_bytes: 2,
        max_value_bytes: 3,
    }
}

fn manifest(a: u64, c: u64, l: u64, identity: Option<[u8; 32]>) -> SnapshotManifest {
    SnapshotManifest::new(2, a, c, l, identity).unwrap()
}

fn binary_rows() -> Vec<(Key, Value)> {
    vec![
        (vec![], vec![0x00, 0xff]),
        (vec![0x00], vec![]),
        (vec![0x80, 0xff], vec![0x7f, 0x00, 0x01]),
    ]
}

fn decoded_rows(bytes: &[u8]) -> Vec<(Key, Value)> {
    SnapshotFormat::decode(bytes, limits())
        .unwrap()
        .records()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

fn decode_error(bytes: &[u8], caps: SnapshotLimits) -> SnapshotFormatError {
    match SnapshotFormat::decode(bytes, caps) {
        Ok(_) => panic!("malformed or over-limit frame was accepted"),
        Err(error) => error,
    }
}

fn digest(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    let mut hash = Sha256::new();
    hash.update(SCOPE);
    hash.update(bytes);
    hash.finalize().into()
}

fn resign(frame: &mut [u8]) {
    let end = frame.len() - DIGEST_BYTES;
    let checksum = digest(&frame[..end]);
    frame[end..].copy_from_slice(&checksum);
}

fn assert_valid_digest(frame: &[u8]) {
    let end = frame.len() - DIGEST_BYTES;
    assert_eq!(&frame[end..], &digest(&frame[..end]));
}

fn put_u32(frame: &mut [u8], offset: usize, value: u32) {
    frame[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(frame: &mut [u8], offset: usize, value: u64) {
    frame[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

// Independent fixture assembly permits checksum-valid noncanonical rows. It
// never calls SnapshotFormat::encode and is not an acceptance implementation.
fn raw_frame(header: SnapshotManifest, rows: &[(Key, Value)]) -> Vec<u8> {
    let mut frame = b"SWSS".to_vec();
    frame.extend_from_slice(&1_u32.to_be_bytes());
    frame.extend_from_slice(&header.layout_version().to_be_bytes());
    frame.extend_from_slice(&header.applied_index().to_be_bytes());
    frame.extend_from_slice(&header.committed_index().to_be_bytes());
    frame.extend_from_slice(&header.last_appended_index().to_be_bytes());
    match header.latest_applied_digest() {
        Some(identity) => {
            frame.push(1);
            frame.extend_from_slice(&identity);
        }
        None => frame.push(0),
    }
    let section_bytes: usize = rows
        .iter()
        .map(|(key, value)| 16 + key.len() + value.len())
        .sum();
    frame.extend_from_slice(&(rows.len() as u64).to_be_bytes());
    frame.extend_from_slice(&(section_bytes as u64).to_be_bytes());
    for (key, value) in rows {
        frame.extend_from_slice(&(key.len() as u64).to_be_bytes());
        frame.extend_from_slice(&(value.len() as u64).to_be_bytes());
        frame.extend_from_slice(key);
        frame.extend_from_slice(value);
    }
    let checksum = digest(&frame);
    frame.extend_from_slice(&checksum);
    frame
}

#[test]
fn snapshot_format_empty_full_bytes_and_digest_are_frozen() {
    let expected = hex(EMPTY_HEX);
    assert_eq!(expected.len(), 85);
    assert_valid_digest(&expected);
    let header = manifest(0, 0, 0, None);
    let rows: Vec<(Key, Value)> = vec![];
    assert_eq!(
        SnapshotFormat::encode(&header, &rows, limits()).unwrap(),
        expected
    );
    let view = SnapshotFormat::decode(&expected, limits()).unwrap();
    assert_eq!(view.manifest(), header);
    assert_eq!(view.records().count(), 0);
    assert_eq!(raw_frame(header, &rows), expected);
}

#[test]
fn snapshot_format_binary_full_bytes_borrow_original_input() {
    let expected = hex(BINARY_HEX);
    assert_eq!(expected.len(), 173);
    assert_valid_digest(&expected);
    let header = manifest(2, 3, 4, Some(IDENTITY));
    let rows = binary_rows();
    let before = rows.clone();
    assert_eq!(
        SnapshotFormat::encode(&header, &rows, exact_binary_limits()).unwrap(),
        expected
    );
    assert_eq!(rows, before);
    let view = SnapshotFormat::decode(&expected, exact_binary_limits()).unwrap();
    assert_eq!(view.manifest(), header);
    let observed: Vec<_> = view.records().collect();
    assert_eq!(observed.len(), 3);
    for ((key, value), (key_offset, value_offset)) in
        observed.iter().zip([(101, 101), (119, 120), (136, 138)])
    {
        assert_eq!(key.as_ptr(), expected.as_ptr().wrapping_add(key_offset));
        assert_eq!(value.as_ptr(), expected.as_ptr().wrapping_add(value_offset));
    }
    assert_eq!(decoded_rows(&expected), rows);
    assert_eq!(raw_frame(header, &rows), expected);
    assert_eq!(
        SnapshotFormat::encode(&view.manifest(), &decoded_rows(&expected), limits()).unwrap(),
        expected
    );
}

#[test]
fn snapshot_format_maximum_frontiers_full_bytes_are_frozen() {
    let expected = hex(MAX_HEX);
    assert_eq!(expected.len(), 117);
    assert_valid_digest(&expected);
    let header = manifest(u64::MAX, u64::MAX, u64::MAX, Some([0xff; 32]));
    assert_eq!(
        SnapshotFormat::encode(&header, &[], limits()).unwrap(),
        expected
    );
    let view = SnapshotFormat::decode(&expected, limits()).unwrap();
    assert_eq!(view.manifest(), header);
    assert_eq!(view.records().count(), 0);
    assert_eq!(raw_frame(header, &[]), expected);
}

#[test]
fn snapshot_format_checked_manifest_has_only_structural_frontier_rules() {
    for (a, c, l) in [
        (0, 0, 0),
        (0, 1, 2),
        (0, u64::MAX, u64::MAX),
        (1, 1, 1),
        (1, 2, 3),
        (u64::MAX, u64::MAX, u64::MAX),
    ] {
        let identity = if a == 0 { None } else { Some([0; 32]) };
        let header = manifest(a, c, l, identity);
        assert_eq!(header.layout_version(), 2);
        assert_eq!(header.applied_index(), a);
        assert_eq!(header.committed_index(), c);
        assert_eq!(header.last_appended_index(), l);
        assert_eq!(header.latest_applied_digest(), identity);
        // Empty records cannot certify these declared frontiers or identity.
        let bytes = SnapshotFormat::encode(&header, &[], limits()).unwrap();
        assert_eq!(
            SnapshotFormat::decode(&bytes, limits()).unwrap().manifest(),
            header
        );
    }
    for (a, c, l) in [
        (1, 0, 1),
        (1, 1, 0),
        (2, 1, 0),
        (u64::MAX, 0, u64::MAX),
        (0, u64::MAX, 0),
        (u64::MAX, u64::MAX, u64::MAX - 1),
    ] {
        let identity = if a == 0 { None } else { Some([0; 32]) };
        assert_eq!(
            SnapshotManifest::new(2, a, c, l, identity).unwrap_err(),
            SnapshotFormatError::InvalidFrontiers {
                applied: a,
                committed: c,
                last_appended: l
            },
        );
    }
    for (a, identity) in [
        (0, Some([0; 32])),
        (0, Some([0xff; 32])),
        (1, None),
        (u64::MAX, None),
    ] {
        assert_eq!(
            SnapshotManifest::new(2, a, a, a, identity).unwrap_err(),
            SnapshotFormatError::InvalidLatestIdentity {
                applied: a,
                has_digest: identity.is_some()
            },
        );
    }
    for layout in [0, 1, 3, u32::MAX] {
        assert_eq!(
            SnapshotManifest::new(layout, 0, 0, 0, None).unwrap_err(),
            SnapshotFormatError::UnsupportedLayoutVersion {
                found: layout,
                expected: 2
            },
        );
    }
}

#[test]
fn snapshot_format_exact_caller_limits_and_one_over_refuse() {
    let frame = hex(BINARY_HEX);
    let rows = binary_rows();
    let before = rows.clone();
    let header = manifest(2, 3, 4, Some(IDENTITY));
    assert_eq!(
        SnapshotFormat::encode(&header, &rows, exact_binary_limits()).unwrap(),
        frame
    );
    SnapshotFormat::decode(&frame, exact_binary_limits()).unwrap();
    for (kind, requested, maximum) in [
        (SnapshotLimit::EncodedBytes, 173, 172),
        (SnapshotLimit::Records, 3, 2),
        (SnapshotLimit::KeyBytes, 2, 1),
        (SnapshotLimit::ValueBytes, 3, 2),
    ] {
        let reduced = || {
            let mut caps = exact_binary_limits();
            match kind {
                SnapshotLimit::EncodedBytes => caps.max_encoded_bytes = maximum,
                SnapshotLimit::Records => caps.max_records = maximum,
                SnapshotLimit::KeyBytes => caps.max_key_bytes = maximum,
                SnapshotLimit::ValueBytes => caps.max_value_bytes = maximum,
            }
            caps
        };
        let error = SnapshotFormatError::LimitExceeded {
            kind,
            requested,
            maximum,
        };
        assert_eq!(
            SnapshotFormat::encode(&header, &rows, reduced()).unwrap_err(),
            error
        );
        assert_eq!(decode_error(&frame, reduced()), error);
    }
    assert_eq!(rows, before);
    let empty_caps = || SnapshotLimits {
        max_encoded_bytes: 85,
        max_records: 0,
        max_key_bytes: 0,
        max_value_bytes: 0,
    };
    let empty_header = manifest(0, 0, 0, None);
    assert_eq!(
        SnapshotFormat::encode(&empty_header, &[], empty_caps()).unwrap(),
        hex(EMPTY_HEX)
    );
    SnapshotFormat::decode(&hex(EMPTY_HEX), empty_caps()).unwrap();
    let rows = vec![(vec![], vec![])];
    let caps = || SnapshotLimits {
        max_encoded_bytes: 101,
        max_records: 1,
        max_key_bytes: 0,
        max_value_bytes: 0,
    };
    let frame = SnapshotFormat::encode(&empty_header, &rows, caps()).unwrap();
    assert_eq!(frame.len(), 101);
    assert_eq!(
        SnapshotFormat::decode(&frame, caps())
            .unwrap()
            .records()
            .next(),
        Some((&[][..], &[][..]))
    );
}

#[test]
fn snapshot_format_sorted_unique_raw_keys_are_not_normalized() {
    let header = manifest(0, 0, 0, None);
    let invalid_rows = [
        vec![(vec![], vec![1]), (vec![], vec![2])],
        vec![(vec![0], vec![1]), (vec![0], vec![2])],
        vec![(vec![2], vec![]), (vec![1], vec![])],
        vec![(vec![1, 0], vec![]), (vec![1], vec![])],
    ];
    for rows in invalid_rows {
        let before = rows.clone();
        assert_eq!(
            SnapshotFormat::encode(&header, &rows, limits()).unwrap_err(),
            SnapshotFormatError::NonCanonicalRecords
        );
        assert_eq!(rows, before);
        let frame = raw_frame(header, &rows);
        let before_frame = frame.clone();
        assert_valid_digest(&frame);
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::NonCanonicalRecords
        );
        assert_eq!(frame, before_frame);
    }
    let mut rows = vec![(vec![], vec![])];
    rows.extend((0_u8..=255).map(|byte| (vec![byte], vec![byte, 0, 0xff])));
    let caps = || SnapshotLimits {
        max_encoded_bytes: 8192,
        max_records: 257,
        max_key_bytes: 1,
        max_value_bytes: 3,
    };
    let before = rows.clone();
    let frame = SnapshotFormat::encode(&header, &rows, caps()).unwrap();
    let view = SnapshotFormat::decode(&frame, caps()).unwrap();
    assert_eq!(view.records().count(), 257);
    for ((actual_key, actual_value), (key, value)) in view.records().zip(&rows) {
        assert_eq!(actual_key, key.as_slice());
        assert_eq!(actual_value, value.as_slice());
    }
    assert_eq!(rows, before);
}

#[test]
fn snapshot_format_checksum_valid_header_and_manifest_violations_refuse() {
    let mut frame = hex(BINARY_HEX);
    frame[0] = b'X';
    resign(&mut frame);
    assert_valid_digest(&frame);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::InvalidMagic
    );
    for version in [0, 2, u32::MAX] {
        let mut frame = hex(BINARY_HEX);
        put_u32(&mut frame, 4, version);
        resign(&mut frame);
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::UnsupportedFormatVersion {
                found: version,
                expected: 1
            }
        );
    }
    for layout in [0, 1, 3, u32::MAX] {
        let mut frame = hex(BINARY_HEX);
        put_u32(&mut frame, 8, layout);
        resign(&mut frame);
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::UnsupportedLayoutVersion {
                found: layout,
                expected: 2
            }
        );
    }
    for (a, c, l) in [(4, 3, 4), (2, 3, 2), (u64::MAX, 3, 4)] {
        let mut frame = hex(BINARY_HEX);
        put_u64(&mut frame, 12, a);
        put_u64(&mut frame, 20, c);
        put_u64(&mut frame, 28, l);
        resign(&mut frame);
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::InvalidFrontiers {
                applied: a,
                committed: c,
                last_appended: l
            }
        );
    }
    let mut frame = hex(BINARY_HEX);
    put_u64(&mut frame, 12, 0);
    resign(&mut frame);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::InvalidLatestIdentity {
            applied: 0,
            has_digest: true
        }
    );
    let mut frame = hex(EMPTY_HEX);
    for offset in [12, 20, 28] {
        put_u64(&mut frame, offset, 1);
    }
    resign(&mut frame);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::InvalidLatestIdentity {
            applied: 1,
            has_digest: false
        }
    );
    for tag in [2, 0xff] {
        let mut frame = hex(BINARY_HEX);
        frame[36] = tag;
        resign(&mut frame);
        assert!(matches!(
            decode_error(&frame, limits()),
            SnapshotFormatError::Malformed { .. }
        ));
    }
}

#[test]
fn snapshot_format_checksum_valid_counts_sections_and_lengths_refuse() {
    for count in [0, 1, 2, 4] {
        let mut frame = hex(BINARY_HEX);
        put_u64(&mut frame, 69, count);
        resign(&mut frame);
        assert_valid_digest(&frame);
        assert!(matches!(
            decode_error(&frame, limits()),
            SnapshotFormatError::Malformed { .. }
        ));
    }
    for section in [0, 47, 55, 57, u64::MAX] {
        let mut frame = hex(BINARY_HEX);
        put_u64(&mut frame, 77, section);
        resign(&mut frame);
        assert_valid_digest(&frame);
        assert!(matches!(
            decode_error(&frame, limits()),
            SnapshotFormatError::Malformed { .. } | SnapshotFormatError::SizeOverflow
        ));
    }
    let mut frame = hex(BINARY_HEX);
    put_u64(&mut frame, 69, u64::MAX);
    resign(&mut frame);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::LimitExceeded {
            kind: SnapshotLimit::Records,
            requested: u64::MAX,
            maximum: 64,
        }
    );
    for (offset, kind, maximum) in [
        (85, SnapshotLimit::KeyBytes, 1024),
        (93, SnapshotLimit::ValueBytes, 2048),
    ] {
        let mut frame = hex(BINARY_HEX);
        put_u64(&mut frame, offset, u64::MAX);
        resign(&mut frame);
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::LimitExceeded {
                kind,
                requested: u64::MAX,
                maximum
            }
        );
        let caps = SnapshotLimits {
            max_encoded_bytes: 4096,
            max_records: 64,
            max_key_bytes: usize::MAX,
            max_value_bytes: usize::MAX,
        };
        assert!(matches!(
            decode_error(&frame, caps),
            SnapshotFormatError::SizeOverflow
                | SnapshotFormatError::Malformed { .. }
                | SnapshotFormatError::LimitExceeded { .. }
        ));
    }
    // This count fits a caller count cap on 64-bit, but cannot fit even its
    // minimum length pairs. Smaller usize targets must still refuse safely.
    let mut frame = hex(BINARY_HEX);
    put_u64(&mut frame, 69, u64::MAX);
    resign(&mut frame);
    let caps = SnapshotLimits {
        max_encoded_bytes: 4096,
        max_records: usize::MAX,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
    };
    assert!(matches!(
        decode_error(&frame, caps),
        SnapshotFormatError::SizeOverflow
            | SnapshotFormatError::Malformed { .. }
            | SnapshotFormatError::LimitExceeded { .. }
    ));
}

#[test]
fn snapshot_format_every_truncated_prefix_and_trailing_byte_refuse() {
    for text in [EMPTY_HEX, BINARY_HEX, MAX_HEX] {
        let frame = hex(text);
        for cut in 0..frame.len() {
            assert!(
                SnapshotFormat::decode(&frame[..cut], limits()).is_err(),
                "accepted cut {cut}"
            );
        }
        let mut appended = frame.clone();
        appended.push(0);
        assert!(SnapshotFormat::decode(&appended, limits()).is_err());
        let mut checksum_valid_extra = frame.clone();
        checksum_valid_extra.insert(frame.len() - DIGEST_BYTES, 0);
        resign(&mut checksum_valid_extra);
        assert_valid_digest(&checksum_valid_extra);
        assert!(matches!(
            decode_error(&checksum_valid_extra, limits()),
            SnapshotFormatError::Malformed { .. }
        ));
        assert_eq!(
            SnapshotFormat::decode(&frame, limits())
                .unwrap()
                .records()
                .count(),
            if text == BINARY_HEX { 3 } else { 0 }
        );
    }
}

#[test]
fn snapshot_format_digest_covers_manifest_identity_and_exact_raw_bytes() {
    let original = hex(BINARY_HEX);
    for offset in [37, 52, 68, 101, 102, 119, 136, 138, 139, 140] {
        let mut frame = original.clone();
        frame[offset] ^= 1;
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::DigestMismatch
        );
    }
    let mut frame = original.clone();
    put_u64(&mut frame, 12, 1);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::DigestMismatch
    );
    for offset in original.len() - DIGEST_BYTES..original.len() {
        let mut frame = original.clone();
        frame[offset] ^= 1;
        assert_eq!(
            decode_error(&frame, limits()),
            SnapshotFormatError::DigestMismatch
        );
    }
    let mut frame = original.clone();
    let checksum_without_domain: [u8; 32] =
        Sha256::digest(&frame[..frame.len() - DIGEST_BYTES]).into();
    let end = frame.len() - DIGEST_BYTES;
    frame[end..].copy_from_slice(&checksum_without_domain);
    assert_eq!(
        decode_error(&frame, limits()),
        SnapshotFormatError::DigestMismatch
    );
    assert_eq!(original, hex(BINARY_HEX));
}

#[test]
fn snapshot_format_opaque_values_and_limits_do_not_inherit_proposal_policy() {
    let rows = vec![
        (vec![0x00], vec![0xff, 0, 1]),
        (vec![0x11, 0, 0xff], vec![b'S', b'W', b'D', b'P', 0xff]),
        (b"\xF0switchyard/journal\0\x02".to_vec(), vec![0]),
        (b"\xF1switchyard/replay\0\x01".to_vec(), vec![0xff]),
        (vec![0xff], vec![]),
    ];
    let before = rows.clone();
    let header = manifest(1, 2, 3, Some([0; 32]));
    let frame = SnapshotFormat::encode(&header, &rows, limits()).unwrap();
    assert_eq!(decoded_rows(&frame), rows);
    assert_eq!(rows, before);
    // These deliberately opaque values are not valid domain/F0/F1 envelopes.
    // Acceptance of their bytes is not semantic health or backend provenance.
    let rows = vec![(vec![0], vec![0x80; 1024 * 1024 + 1])];
    let before = rows.clone();
    let caps = || SnapshotLimits {
        max_encoded_bytes: 2 * 1024 * 1024,
        max_records: 1,
        max_key_bytes: 1,
        max_value_bytes: 1024 * 1024 + 1,
    };
    let frame = SnapshotFormat::encode(&header, &rows, caps()).unwrap();
    let view = SnapshotFormat::decode(&frame, caps()).unwrap();
    let (key, value) = view.records().next().unwrap();
    assert_eq!(key, rows[0].0.as_slice());
    assert_eq!(value, rows[0].1.as_slice());
    assert_eq!(rows, before);
}
