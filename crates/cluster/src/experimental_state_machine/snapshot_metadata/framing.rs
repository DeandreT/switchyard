use sha2::{Digest, Sha256};

use super::{bootstrap_fixture as captured, *};

#[test]
fn frozen_frame_and_field_order_have_an_explicit_small_golden_encoding() -> TestResult {
    let wire = super::super::codec::MetadataV1 {
        stream: [7; 16],
        artifact_bytes: 1,
        digest: [0xa5; 32],
        last: None,
        previous: None,
        highest_timestamp: 0,
        membership: None,
    };
    let encoded = super::super::codec::encode(&wire)?;
    let mut payload = vec![7; 16];
    payload.push(1);
    payload.extend_from_slice(&[0xa5; 32]);
    payload.extend_from_slice(&[0; 4]);
    assert_eq!(payload.len(), 53);
    assert_eq!(&encoded[..12], b"SWYM\x00\x01\x00\x01\x00\x00\x00\x35");
    assert_eq!(&encoded[12..65], &payload);
    assert_eq!(&encoded[65..], Sha256::digest(&encoded[..65]).as_slice());
    assert_eq!(encoded.len(), 97);
    assert!(super::super::codec::decode(&encoded)? == wire);
    Ok(())
}

#[test]
fn every_truncated_prefix_is_refused_without_a_partial_pair() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    for end in 0..metadata.len() {
        assert!(
            DecodedNativeSnapshotPair::decode(&metadata.as_bytes()[..end], source.image.as_bytes())
                .is_err()
        );
    }
    assert!(
        DecodedNativeSnapshotPair::decode(metadata.as_bytes(), source.image.as_bytes()).is_ok()
    );
    Ok(())
}

#[test]
fn wrong_schema_role_and_magic_are_static_refusals() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    for offset in [5, 7] {
        let mut changed = metadata.as_bytes().to_vec();
        changed[offset] = 2;
        fixture::reseal(&mut changed);
        assert_eq!(
            DecodedNativeSnapshotPair::decode(&changed, source.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::UnsupportedFormat)
        );
    }
    let mut changed = metadata.as_bytes().to_vec();
    changed[0] ^= 1;
    fixture::reseal(&mut changed);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&changed, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    Ok(())
}

#[test]
fn body_and_checksum_damage_are_refused() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    for offset in [12, metadata.len() - 33, metadata.len() - 1] {
        let mut changed = metadata.as_bytes().to_vec();
        changed[offset] ^= 1;
        assert_eq!(
            DecodedNativeSnapshotPair::decode(&changed, source.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::InvalidMetadata)
        );
    }
    Ok(())
}

#[test]
fn malformed_lengths_overflow_caps_and_never_return_a_truncated_pair() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let oversized = vec![0; MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1];
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&oversized, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    let mut declared = metadata.as_bytes().to_vec();
    declared[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&declared, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    declared[8..12].copy_from_slice(&0u32.to_be_bytes());
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&declared, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    let mut trailing = metadata.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&trailing, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    Ok(())
}

#[test]
fn canonical_comparison_rejects_nonminimal_varint_and_trailing_payload() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let end = metadata.len() - 32;
    let mut payload = metadata.as_bytes()[12..end].to_vec();
    let final_byte = (16..payload.len())
        .find(|index| payload[*index] & 0x80 == 0)
        .ok_or("missing artifact length varint")?;
    payload[final_byte] |= 0x80;
    payload.insert(final_byte + 1, 0);
    let noncanonical = fixture::frame(&payload);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&noncanonical, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    let mut trailing = metadata.as_bytes()[12..end].to_vec();
    trailing.push(0);
    let trailing = fixture::frame(&trailing);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&trailing, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    Ok(())
}

#[test]
fn invalid_option_tag_and_borrowed_membership_bound_are_refused() -> TestResult {
    let source = captured::initial()?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let mut payload = metadata.as_bytes()[12..metadata.len() - 32].to_vec();
    let end = (16..payload.len())
        .find(|index| payload[*index] & 0x80 == 0)
        .ok_or("missing artifact length varint")?;
    payload[end + 1 + 32] = 2;
    let invalid = fixture::frame(&payload);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&invalid, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidMetadata)
    );
    let too_large = vec![0; 4097];
    let mut wire = super::super::codec::decode(metadata.as_bytes())?;
    wire.membership = Some(super::super::codec::MembershipV1 {
        source: super::super::codec::IdV1 {
            term: 1,
            node_id: 7,
            index: 0,
        },
        schema_version: 1,
        payload: &too_large,
    });
    assert_eq!(
        super::super::codec::encode(&wire).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    let oversized = fixture::frame(&postcard::to_stdvec(&wire)?);
    assert!(oversized.len() < MAX_NATIVE_SNAPSHOT_METADATA_BYTES);
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&oversized, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn maximum_frozen_scalar_fields_establish_the_4370_byte_upper_bound() -> TestResult {
    let membership = [0xff; 4096];
    let id = super::super::codec::IdV1 {
        term: u64::MAX,
        node_id: u64::MAX,
        index: u64::MAX,
    };
    let mark = super::super::codec::MarkV1 {
        id,
        fingerprint: [0xff; 32],
    };
    let mut wire = super::super::codec::MetadataV1 {
        stream: [0xff; 16],
        artifact_bytes: u64::MAX,
        digest: [0xff; 32],
        last: Some(mark),
        previous: Some(mark),
        highest_timestamp: u64::MAX,
        membership: Some(super::super::codec::MembershipV1 {
            source: id,
            schema_version: u16::MAX,
            payload: &membership,
        }),
    };
    // This intentionally is not a valid image/checkpoint. It exercises the
    // maximum representation of every frozen field, independent of business.
    let serialized = postcard::to_stdvec(&wire)?;
    assert_eq!(
        12 + serialized.len() + 32,
        super::super::codec::MAX_FROZEN_METADATA_BYTES
    );
    assert_eq!(super::super::codec::MAX_FROZEN_METADATA_BYTES, 4370);
    const {
        assert!(
            super::super::codec::MAX_FROZEN_METADATA_BYTES <= MAX_NATIVE_SNAPSHOT_METADATA_BYTES
        );
    }
    assert_eq!(
        super::super::codec::encode(&wire).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    wire.artifact_bytes = domain::MAX_COMMITTED_IMAGE_BYTES as u64;
    let encoded = super::super::codec::encode(&wire)?;
    assert!(encoded.len() <= 4370);
    assert!(super::super::codec::decode(&encoded)? == wire);
    Ok(())
}

#[test]
fn artifact_limit_and_corrupt_image_are_checked_by_both_public_entrypoints() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let too_large = vec![0; domain::MAX_COMMITTED_IMAGE_BYTES + 1];
    assert_eq!(
        EncodedNativeSnapshotMetadata::encode(&too_large).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    assert_eq!(
        DecodedNativeSnapshotPair::decode(metadata.as_bytes(), &too_large).err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    drop(too_large);
    let mut damaged = source.image.as_bytes().to_vec();
    *damaged.last_mut().ok_or("empty image")? ^= 1;
    assert_eq!(
        EncodedNativeSnapshotMetadata::encode(&damaged).err(),
        Some(NativeSnapshotMetadataError::InvalidImage)
    );
    assert_eq!(
        DecodedNativeSnapshotPair::decode(metadata.as_bytes(), &damaged).err(),
        Some(NativeSnapshotMetadataError::InvalidImage)
    );
    Ok(())
}

#[test]
fn snapshot_id_is_fixed_prefix_full_lowercase_digest_without_a_counter() -> TestResult {
    let digest = std::array::from_fn(|index| index as u8);
    let id = super::super::codec::snapshot_id(&digest)?;
    assert_eq!(
        id,
        "swyi-v1-sha256:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    );
    assert_eq!(id.len(), super::super::codec::SNAPSHOT_ID_BYTES);
    assert_eq!(id, super::super::codec::snapshot_id(&digest)?);
    Ok(())
}
