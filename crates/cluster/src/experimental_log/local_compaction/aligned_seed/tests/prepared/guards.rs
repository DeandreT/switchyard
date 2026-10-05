use super::*;

#[test]
fn prepared_seed_artifact_and_expected_length_limits() -> TestResult {
    let case = Case::new()?;
    // Heavy boundary: root executes this allocation case serially.
    let oversized = vec![0; MAX_COMMITTED_IMAGE_BYTES + 1];
    assert_eq!(
        prepare_aligned_seed_candidate(&oversized, &case.expectation()).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    let mut expected = case.expectation();
    expected.artifact_bytes = MAX_COMMITTED_IMAGE_BYTES + 1;
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_expected_native_id_limit() -> TestResult {
    let case = Case::new()?;
    let mut native = case.image.native.clone();
    native.snapshot_id = "x".repeat(crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1);
    let mut expected = case.expectation();
    expected.native = &native;
    assert_eq!(prepare(&case, &expected), Err(Error::LimitExceeded));
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_expected_membership_shape_limits() -> TestResult {
    let case = Case::new()?;
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
    for membership in [configs, members, nodes, address] {
        let mut native = case.image.native.clone();
        native.last_membership =
            openraft::StoredMembership::new(*native.last_membership.log_id(), membership);
        let mut expected = case.expectation();
        expected.native = &native;
        assert_eq!(prepare(&case, &expected), Err(Error::LimitExceeded));
        assert_eq!(
            prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
            Err(Error::LimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn prepared_seed_expected_membership_wire_limit() -> TestResult {
    let case = Case::new()?;
    let membership = openraft::Membership::new(
        vec![(0..16).collect::<BTreeSet<_>>()],
        (0..16)
            .map(|id| (id, BasicNode::new("x".repeat(512))))
            .collect::<BTreeMap<_, _>>(),
    );
    assert_eq!(membership.get_joint_config().len(), 1);
    assert_eq!(membership.get_joint_config()[0].len(), 16);
    assert_eq!(membership.nodes().count(), 16);
    assert!(membership.nodes().all(|(_, node)| node.addr.len() == 512));
    assert!(crate::experimental_log::bounded_membership_len(&membership).is_err());
    let mut native = case.image.native.clone();
    native.last_membership =
        openraft::StoredMembership::new(*native.last_membership.log_id(), membership);
    let mut expected = case.expectation();
    expected.native = &native;
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_shape_precedes_image_processing() -> TestResult {
    let case = Case::new()?;
    let foreign = LogProfile::new(7, domain::CommittedStreamId::new([8; 16])?)?;
    let mut native = case.image.native.clone();
    native.last_membership = openraft::StoredMembership::new(
        *native.last_membership.log_id(),
        openraft::Membership::new(
            vec![BTreeSet::from([7]); 3],
            BTreeMap::from([(7, BasicNode::new("node"))]),
        ),
    );
    let mut expected = case.expectation();
    expected.profile = &foreign;
    expected.native = &native;
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::InvalidExpectation)
    );
    expected.profile = &case.profile;
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    expected.artifact_bytes = MAX_COMMITTED_IMAGE_BYTES + 1;
    expected.profile = &foreign;
    assert_eq!(
        prepare_aligned_seed_candidate(&[], &expected).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_mutated_exported_copy_does_not_mutate_candidate() -> TestResult {
    let case = Case::new()?;
    let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    let mut metadata = candidate.metadata_bytes().to_vec();
    let mut artifact = candidate.artifact_bytes().to_vec();
    let mut controls = candidate.log_rows().map(|(_, value)| value.to_vec());
    *metadata.first_mut().ok_or("metadata empty")? ^= 1;
    *artifact.first_mut().ok_or("artifact empty")? ^= 1;
    for value in &mut controls {
        *value.first_mut().ok_or("control empty")? ^= 1;
    }
    assert_ne!(metadata, candidate.metadata_bytes());
    assert_ne!(artifact, candidate.artifact_bytes());
    for (changed, (_, original)) in controls.iter().zip(candidate.log_rows()) {
        assert_ne!(changed, original);
    }
    assert_eq!(candidate.metadata_bytes(), case.image.metadata);
    assert_eq!(candidate.artifact_bytes(), case.image.artifact);
    assert_eq!(candidate.log_rows(), case.rows());
    assert_eq!(candidate.reinspect(&case.expectation()), Ok(()));
    Ok(())
}

#[test]
fn prepared_seed_debug_and_errors_private() -> TestResult {
    let case = Case::new()?;
    let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    let rendered = format!("{candidate:?}");
    for hidden in [
        "PRIVATE",
        "snapshot_id",
        "swyi-v1-sha256:",
        "leader_id",
        "node_id",
        "fingerprint",
        "artifact_sha256",
        "vote",
    ] {
        assert!(!rendered.contains(hidden));
    }
    assert!(rendered.contains("artifact_bytes"));
    assert!(rendered.contains("metadata_bytes"));
    for error in [
        Error::LimitExceeded,
        Error::Allocation,
        Error::InvalidExpectation,
        Error::UnsupportedProfile,
        Error::InvalidImage,
        Error::InvalidNativePair,
        Error::InvalidLog,
        Error::IdentityMismatch,
        Error::UnsupportedSeedPolicy,
        Error::UnsupportedTail,
    ] {
        let rendered = format!("{error:?}: {error}");
        for hidden in [
            "PRIVATE",
            "swyi-v1-sha256:",
            "leader_id",
            "node_id",
            "fingerprint",
        ] {
            assert!(!rendered.contains(hidden));
        }
    }
    Ok(())
}

#[test]
fn prepared_seed_b3_top_limits_precede_row_shape() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.artifact_bytes = MAX_COMMITTED_IMAGE_BYTES + 1;
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &[],
                log_rows: &[],
            },
            &expected
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    let large_metadata = vec![0; crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1];
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &large_metadata,
                artifact: &[],
                log_rows: &[],
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    let mut native = case.image.native.clone();
    native.snapshot_id = "x".repeat(crate::MAX_NATIVE_SNAPSHOT_METADATA_BYTES + 1);
    expected = case.expectation();
    expected.native = &native;
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &[],
                log_rows: &[],
            },
            &expected
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_b3_row_shape_precedes_expectation_consistency() -> TestResult {
    let case = Case::new()?;
    let foreign = LogProfile::new(7, domain::CommittedStreamId::new([8; 16])?)?;
    let mut expected = case.expectation();
    expected.profile = &foreign;
    for rows in [&[][..], &case.rows()[..2]] {
        assert_eq!(
            inspect_aligned_seed(
                BorrowedAlignedSeed {
                    metadata: &[],
                    artifact: &[],
                    log_rows: rows,
                },
                &expected
            )
            .map(|_| ()),
            Err(Error::InvalidLog)
        );
    }
    let four = [
        case.rows()[0],
        case.rows()[1],
        case.rows()[2],
        (&[9][..], &[][..]),
    ];
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &[],
                log_rows: &four,
            },
            &expected
        )
        .map(|_| ()),
        Err(Error::UnsupportedTail)
    );
    let oversized = vec![0; crate::MAX_LOG_METADATA_BYTES + 1];
    let mut unknown = case.rows();
    unknown[0] = (&[9], &oversized);
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &[],
                log_rows: &unknown,
            },
            &expected
        )
        .map(|_| ()),
        Err(Error::InvalidLog)
    );
    assert_eq!(case.run(&expected), Err(Error::InvalidExpectation));
    Ok(())
}

#[test]
fn prepared_seed_b3_component_limits_precede_native_shape() -> TestResult {
    let case = Case::new()?;
    let foreign = LogProfile::new(7, domain::CommittedStreamId::new([8; 16])?)?;
    let mut native = case.image.native.clone();
    native.last_membership = openraft::StoredMembership::new(
        *native.last_membership.log_id(),
        openraft::Membership::new(
            vec![BTreeSet::from([7]); 3],
            BTreeMap::from([(7, BasicNode::new("node"))]),
        ),
    );
    let mut expected = case.expectation();
    expected.profile = &foreign;
    expected.native = &native;
    for (index, cap) in [
        (0, crate::MAX_LOG_METADATA_BYTES),
        (1, crate::MAX_LOG_METADATA_BYTES),
        (2, MAX_BASELINE_BYTES),
    ] {
        let oversized = vec![0; cap + 1];
        let mut rows = case.rows();
        rows[index].1 = &oversized;
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
    assert_eq!(case.run(&expected), Err(Error::InvalidExpectation));
    expected.profile = &case.profile;
    assert_eq!(case.run(&expected), Err(Error::LimitExceeded));
    let oversized = vec![0; crate::MAX_LOG_METADATA_BYTES + 1];
    let mut rows = case.rows();
    rows[0].1 = &oversized;
    rows[1].0 = &[9];
    assert_eq!(
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &[],
                artifact: &[],
                log_rows: &rows,
            },
            &case.expectation()
        )
        .map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn prepared_seed_b3_observed_semantics_precede_external_identity() -> TestResult {
    let foreign = LogProfile::new(8, domain::CommittedStreamId::new([7; 16])?)?;
    for index in [0, 1] {
        let mut case = Case::new()?;
        case.controls[index].push(0);
        let mut expected = case.expectation();
        expected.profile = &foreign;
        expected.artifact_sha256[0] ^= 1;
        assert_eq!(case.run(&expected), Err(Error::InvalidLog));
    }
    let mut case = Case::new()?;
    // Malformed nested metadata with a recomputed outer checksum is negative
    // fixture damage, not a second semantic parser or canonical encoder.
    let offset = case.controls[2]
        .windows(4)
        .position(|bytes| bytes == b"SWYM")
        .ok_or("metadata missing")?;
    case.controls[2][offset + 8..offset + 12].copy_from_slice(&u32::MAX.to_be_bytes());
    let end = case.controls[2]
        .len()
        .checked_sub(32)
        .ok_or("checksum missing")?;
    let checksum: [u8; 32] = Sha256::digest(&case.controls[2][..end]).into();
    case.controls[2][end..].copy_from_slice(&checksum);
    let mut expected = case.expectation();
    expected.profile = &foreign;
    assert_eq!(case.run(&expected), Err(Error::InvalidLog));
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.profile = &foreign;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    assert_eq!(case.run(&case.expectation()), Ok(()));
    Ok(())
}
