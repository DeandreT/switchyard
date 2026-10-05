use std::collections::{BTreeMap, BTreeSet};

use super::*;

#[test]
fn aligned_seed_any_extra_row_refused() -> TestResult {
    let case = Case::new()?;
    for key in [&[0x10, 0, 0, 0, 0, 0, 0, 0, 3][..], &[9][..]] {
        let mut rows = case.rows().to_vec();
        rows.push((key, &[]));
        assert_eq!(
            inspect_aligned_seed(
                BorrowedAlignedSeed {
                    metadata: &case.image.metadata,
                    artifact: &case.image.artifact,
                    log_rows: &rows,
                },
                &case.expectation()
            )
            .map(|_| ()),
            Err(Error::UnsupportedTail)
        );
    }
    Ok(())
}

#[test]
fn aligned_seed_missing_duplicate_unsorted_keys() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let duplicate = [rows[0], rows[0], rows[2]];
    let unsorted = [rows[1], rows[0], rows[2]];
    let unknown = [rows[0], rows[1], (&[9][..], rows[2].1)];
    for offered in [&rows[..2], &duplicate[..], &unsorted[..], &unknown[..]] {
        assert_eq!(
            inspect_aligned_seed(
                BorrowedAlignedSeed {
                    metadata: &case.image.metadata,
                    artifact: &case.image.artifact,
                    log_rows: offered,
                },
                &case.expectation()
            )
            .map(|_| ()),
            Err(Error::InvalidLog)
        );
    }
    Ok(())
}

#[test]
fn aligned_seed_all_top_level_bounds() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let large_artifact = vec![0; MAX_COMMITTED_IMAGE_BYTES + 1];
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &large_artifact,
                log_rows: &rows,
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    let large_metadata = vec![0; crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1];
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &large_metadata,
                artifact: &case.image.artifact,
                log_rows: &rows,
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    for (index, cap) in [
        (0, crate::MAX_LOG_METADATA_BYTES),
        (1, crate::MAX_LOG_METADATA_BYTES),
        (2, MAX_BASELINE_BYTES),
    ] {
        let oversized = vec![0; cap + 1];
        let mut controls = rows;
        controls[index].1 = &oversized;
        assert_eq!(
            inspect_aligned_seed(
                BorrowedAlignedSeed {
                    metadata: &[],
                    artifact: &[],
                    log_rows: &controls,
                },
                &case.expectation()
            )
            .map(|_| ()),
            Err(Error::LimitExceeded)
        );
    }
    let mut expected = case.expectation();
    expected.artifact_bytes = MAX_COMMITTED_IMAGE_BYTES + 1;
    assert_eq!(case.run(&expected), Err(Error::LimitExceeded));
    let mut native = case.image.native.clone();
    native.snapshot_id = "x".repeat(crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1);
    expected = case.expectation();
    expected.native = &native;
    assert_eq!(case.run(&expected), Err(Error::LimitExceeded));
    Ok(())
}

#[test]
fn aligned_seed_native_shape_before_semantics() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let configs = openraft::Membership::new(
        vec![BTreeSet::from([7]); 3],
        BTreeMap::from([(7, BasicNode::new("node"))]),
    );
    let members = openraft::Membership::new(
        vec![(0..33).collect::<BTreeSet<_>>()],
        BTreeMap::<u64, BasicNode>::new(),
    );
    let nodes = openraft::Membership::new(
        vec![BTreeSet::from([7])],
        (0..33)
            .map(|id| (id, BasicNode::new("node")))
            .collect::<BTreeMap<_, _>>(),
    );
    let address = openraft::Membership::new(
        vec![BTreeSet::from([7])],
        BTreeMap::from([(7, BasicNode::new("x".repeat(513)))]),
    );
    let bytes = openraft::Membership::new(
        vec![(0..16).collect::<BTreeSet<_>>()],
        (0..16)
            .map(|id| (id, BasicNode::new("x".repeat(512))))
            .collect::<BTreeMap<_, _>>(),
    );
    for membership in [configs, members, nodes, address, bytes] {
        let mut native = case.image.native.clone();
        native.last_membership =
            openraft::StoredMembership::new(*native.last_membership.log_id(), membership);
        let mut expected = case.expectation();
        expected.native = &native;
        assert_eq!(
            inspect_aligned_seed(
                BorrowedAlignedSeed {
                    metadata: &[],
                    artifact: &[],
                    log_rows: &rows,
                },
                &expected
            )
            .map(|_| ()),
            Err(Error::LimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn aligned_seed_malformed_noncanonical_controls() -> TestResult {
    for index in 0..3 {
        let mut case = Case::new()?;
        case.controls[index].push(0);
        assert_eq!(case.run(&case.expectation()), Err(Error::InvalidLog));
    }
    let mut case = Case::new()?;
    case.controls[0][4] ^= 1;
    assert_eq!(
        case.run(&case.expectation()),
        Err(Error::UnsupportedProfile)
    );
    let mut case = Case::new()?;
    let length = case.controls[0].len();
    case.controls[0][length - 1] ^= 1;
    assert_eq!(case.run(&case.expectation()), Err(Error::InvalidLog));
    let profile = decode_observed_profile(&Case::new()?.controls[0]).map_err(profile_error)?;
    assert_eq!(profile, case.profile);
    Ok(())
}

#[test]
fn aligned_seed_malformed_under_mismatch() -> TestResult {
    let mut case = Case::new()?;
    case.controls[1] = vec![0];
    let mut expected = case.expectation();
    expected.artifact_sha256[0] ^= 1;
    assert_eq!(case.run(&expected), Err(Error::InvalidLog));
    Ok(())
}

fn damage_nested_baseline(bytes: &mut [u8]) -> TestResult {
    let offset = bytes
        .windows(4)
        .position(|window| window == b"SWYM")
        .ok_or("metadata missing")?;
    bytes[offset + 8..offset + 12].copy_from_slice(&u32::MAX.to_be_bytes());
    // Negative fixture damage, not a second canonical SWLF/SWYM encoder.
    let end = bytes.len().checked_sub(32).ok_or("checksum missing")?;
    let checksum: [u8; 32] = Sha256::digest(&bytes[..end]).into();
    bytes[end..].copy_from_slice(&checksum);
    Ok(())
}

#[test]
fn aligned_seed_malformed_under_foreign_expected_profile() -> TestResult {
    let foreign = LogProfile::new(8, domain::CommittedStreamId::new([7; 16])?)?;
    for nested in [false, true] {
        let mut case = Case::new()?;
        if nested {
            damage_nested_baseline(&mut case.controls[2])?;
        } else {
            case.controls[1] = vec![0];
        }
        let mut expected = case.expectation();
        expected.profile = &foreign;
        assert_eq!(case.run(&expected), Err(Error::InvalidLog));
    }
    let mut case = Case::new()?;
    case.controls[2] = Baseline::make(&foreign, 1, &case.image.metadata)?.bytes;
    assert_eq!(case.run(&case.expectation()), Err(Error::InvalidLog));
    Ok(())
}

#[test]
fn aligned_seed_valid_foreign_expected_identity() -> TestResult {
    let case = Case::new()?;
    let foreign = LogProfile::new(8, case.profile.stream())?;
    let mut expected = case.expectation();
    expected.profile = &foreign;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    assert_eq!(case.run(&case.expectation()), Ok(()));
    Ok(())
}

#[test]
fn aligned_seed_image_native_nested_lengths() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let mut artifact = case.image.artifact.clone();
    artifact[32..36].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &case.image.metadata,
                artifact: &artifact,
                log_rows: &rows,
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    let mut metadata = case.image.metadata.clone();
    metadata[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &metadata,
                artifact: &case.image.artifact,
                log_rows: &rows,
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}
