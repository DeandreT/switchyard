use std::collections::{BTreeMap, BTreeSet};

use super::*;

mod guards;

fn prepare(case: &Case, expected: &AlignedSeedExpectation<'_>) -> Result<()> {
    prepare_aligned_seed_candidate(&case.image.artifact, expected).map(|_| ())
}

#[test]
fn prepared_seed_exact_reference_bytes() -> TestResult {
    let case = Case::new()?;
    let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    assert_eq!(candidate.metadata_bytes(), case.image.metadata);
    assert_eq!(candidate.log_rows(), case.rows());
    assert_eq!(candidate.artifact_bytes(), case.image.artifact);
    assert_eq!(case.image.native.snapshot_id.len(), 79);
    let rows = candidate.log_rows();
    let checked = inspect_aligned_seed(
        BorrowedAlignedSeed {
            metadata: candidate.metadata_bytes(),
            artifact: candidate.artifact_bytes(),
            log_rows: &rows,
        },
        &case.expectation(),
    )?;
    assert_eq!(checked.image_pair().snapshot_meta()?, case.image.native);
    assert_eq!(checked.baseline_ordinal(), 1);
    assert_eq!(checked.vote(), case.vote);
    Ok(())
}

#[test]
fn prepared_seed_repeated_bytes_and_borrowed_artifact() -> TestResult {
    let case = Case::new()?;
    let first = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    let second = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    assert_eq!(first.metadata_bytes(), second.metadata_bytes());
    assert_eq!(first.log_rows(), second.log_rows());
    assert_eq!(
        first.artifact_bytes().as_ptr(),
        case.image.artifact.as_ptr()
    );
    assert_eq!(
        second.artifact_bytes().as_ptr(),
        case.image.artifact.as_ptr()
    );
    let artifact: &[u8] = {
        let temporary = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
        temporary.artifact_bytes()
    };
    assert_eq!(artifact.as_ptr(), case.image.artifact.as_ptr());
    assert_eq!(artifact, case.image.artifact);
    Ok(())
}

#[test]
fn prepared_seed_temporary_reinspection() -> TestResult {
    let case = Case::new()?;
    let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    assert_eq!(candidate.reinspect(&case.expectation()), Ok(()));
    let mut foreign = case.expectation();
    foreign.artifact_sha256[0] ^= 1;
    assert_eq!(candidate.reinspect(&foreign), Err(Error::IdentityMismatch));
    assert_eq!(candidate.reinspect(&case.expectation()), Ok(()));
    Ok(())
}

#[test]
fn prepared_seed_same_checkpoint_different_body() -> TestResult {
    let case = Case::new()?;
    let changed = case.image.changed_body()?;
    assert_eq!(changed.checkpoint, case.image.checkpoint);
    assert_ne!(changed.artifact, case.image.artifact);
    assert_eq!(
        prepare_aligned_seed_candidate(&changed.artifact, &case.expectation()).map(|_| ()),
        Err(Error::IdentityMismatch)
    );
    let changed_case = Case::from_image(changed)?;
    let original = prepare_aligned_seed_candidate(&case.image.artifact, &case.expectation())?;
    let altered =
        prepare_aligned_seed_candidate(&changed_case.image.artifact, &changed_case.expectation())?;
    assert_ne!(original.metadata_bytes(), altered.metadata_bytes());
    assert_ne!(original.log_rows()[2].1, altered.log_rows()[2].1);
    Ok(())
}

#[test]
fn prepared_seed_full_checkpoint_fields() -> TestResult {
    let case = Case::new()?;
    let clock = Image::variant([7; 16], 20, 21, 7)?;
    let member = Image::variant([7; 16], 10, 11, 17)?;
    let next = case.image.after_blank()?;
    assert_ne!(
        clock.checkpoint.highest_timestamp(),
        case.image.checkpoint.highest_timestamp()
    );
    assert_ne!(
        clock.checkpoint.previous(),
        case.image.checkpoint.previous()
    );
    assert_ne!(clock.checkpoint.last(), case.image.checkpoint.last());
    assert_ne!(
        member.checkpoint.membership(),
        case.image.checkpoint.membership()
    );
    assert_ne!(next.checkpoint.last(), case.image.checkpoint.last());
    for checkpoint in [&clock.checkpoint, &member.checkpoint, &next.checkpoint] {
        let mut expected = case.expectation();
        expected.checkpoint = checkpoint;
        assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    }
    Ok(())
}

#[test]
fn prepared_seed_native_projection_fields() -> TestResult {
    let case = Case::new()?;
    let original = &case.image.native;
    let stored = &original.last_membership;
    let membership = stored.membership();
    let nodes = membership
        .nodes()
        .map(|(id, node)| (*id, node.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut variants = Vec::new();
    let mut last = original.clone();
    last.last_log_id = None;
    variants.push(last);
    let mut source = original.clone();
    source.last_membership = openraft::StoredMembership::new(None, membership.clone());
    variants.push(source);
    let mut configs = membership.get_joint_config().clone();
    assert!(configs.first_mut().ok_or("config missing")?.remove(&9));
    let config_only = openraft::Membership::new(configs, nodes.clone());
    assert_eq!(
        config_only.nodes().collect::<Vec<_>>(),
        membership.nodes().collect::<Vec<_>>()
    );
    let mut config = original.clone();
    config.last_membership = openraft::StoredMembership::new(*stored.log_id(), config_only);
    variants.push(config);
    let mut addresses = nodes.clone();
    addresses
        .get_mut(&7)
        .ok_or("node missing")?
        .addr
        .push_str("-changed");
    let address_only = openraft::Membership::new(membership.get_joint_config().clone(), addresses);
    assert_eq!(
        address_only.get_joint_config(),
        membership.get_joint_config()
    );
    let mut address = original.clone();
    address.last_membership = openraft::StoredMembership::new(*stored.log_id(), address_only);
    variants.push(address);
    let mut ids = nodes;
    let node = ids.get(&9).ok_or("node missing")?.clone();
    ids.insert(19, node);
    let id_only = openraft::Membership::new(membership.get_joint_config().clone(), ids);
    assert_eq!(id_only.get_joint_config(), membership.get_joint_config());
    let mut node_id = original.clone();
    node_id.last_membership = openraft::StoredMembership::new(*stored.log_id(), id_only);
    variants.push(node_id);
    let mut snapshot_id = original.clone();
    snapshot_id.snapshot_id.push('x');
    variants.push(snapshot_id);
    for native in &variants {
        let mut expected = case.expectation();
        expected.native = native;
        assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    }
    Ok(())
}

#[test]
fn prepared_seed_profile_is_requested_data() -> TestResult {
    let case = Case::new()?;
    let requested = LogProfile::new(99, case.profile.stream())?;
    let mut expected = case.expectation();
    expected.profile = &requested;
    let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &expected)?;
    assert_eq!(candidate.metadata_bytes(), case.image.metadata);
    assert_ne!(candidate.log_rows()[0].1, case.controls[0]);
    assert_ne!(candidate.log_rows()[2].1, case.controls[2]);
    assert_eq!(candidate.reinspect(&expected), Ok(()));
    assert_eq!(
        candidate.reinspect(&case.expectation()),
        Err(Error::IdentityMismatch)
    );
    let other = Image::variant([8; 16], 10, 11, 7)?;
    let foreign = LogProfile::new(99, other.checkpoint.stream())?;
    expected.profile = &foreign;
    assert_eq!(prepare(&case, &expected), Err(Error::InvalidExpectation));
    expected.checkpoint = &other.checkpoint;
    assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn prepared_seed_positive_ordinal_is_requested_data() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.baseline_ordinal = 0;
    assert_eq!(prepare(&case, &expected), Err(Error::InvalidExpectation));
    for ordinal in [2, u64::MAX] {
        expected.baseline_ordinal = ordinal;
        let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &expected)?;
        assert_ne!(candidate.log_rows()[2].1, case.controls[2]);
        assert_eq!(candidate.reinspect(&expected), Ok(()));
        assert_eq!(
            candidate.reinspect(&case.expectation()),
            Err(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn prepared_seed_vote_is_requested_data() -> TestResult {
    let case = Case::new()?;
    for vote in [LogVote::new_committed(3, 7), LogVote::new_committed(3, 99)] {
        let mut expected = case.expectation();
        expected.vote = vote;
        let candidate = prepare_aligned_seed_candidate(&case.image.artifact, &expected)?;
        assert_ne!(candidate.log_rows()[1].1, case.controls[1]);
        assert_eq!(candidate.log_rows()[0].1, case.controls[0]);
        assert_eq!(candidate.log_rows()[2].1, case.controls[2]);
        assert_eq!(candidate.reinspect(&expected), Ok(()));
        assert_eq!(
            candidate.reinspect(&case.expectation()),
            Err(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn prepared_seed_initial_and_noninitial_no_member_policy() -> TestResult {
    for noninitial in [false, true] {
        let case = Case::from_image(Image::initial(noninitial)?)?;
        assert_eq!(case.image.checkpoint.last().is_some(), noninitial);
        assert!(case.image.checkpoint.membership().is_none());
        assert_eq!(
            prepare(&case, &case.expectation()),
            Err(Error::UnsupportedSeedPolicy)
        );
        let mut mismatched = case.expectation();
        mismatched.artifact_sha256[0] ^= 1;
        assert_eq!(
            prepare(&case, &mismatched),
            Err(Error::UnsupportedSeedPolicy)
        );
    }
    Ok(())
}

#[test]
fn prepared_seed_uncommitted_requested_vote() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.vote = LogVote::new(2, 7);
    assert_eq!(prepare(&case, &expected), Err(Error::InvalidExpectation));
    Ok(())
}

#[test]
fn prepared_seed_insufficient_requested_leader() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.vote = LogVote::new_committed(0, 99);
    assert_eq!(prepare(&case, &expected), Err(Error::UnsupportedSeedPolicy));
    Ok(())
}

#[test]
fn prepared_seed_equal_term_node_order() -> TestResult {
    let case = Case::new()?;
    for (node, accepted) in [(6, false), (7, true), (8, true)] {
        let mut expected = case.expectation();
        expected.vote = LogVote::new_committed(1, node);
        assert_eq!(
            prepare(&case, &expected),
            if accepted {
                Ok(())
            } else {
                Err(Error::UnsupportedSeedPolicy)
            }
        );
    }
    Ok(())
}

#[test]
fn prepared_seed_artifact_identity_mismatch() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.artifact_bytes += 1;
    assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    expected = case.expectation();
    expected.artifact_sha256[0] ^= 1;
    assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    // Exact pair identity is checked before requested policy.
    expected.baseline_ordinal = 0;
    assert_eq!(prepare(&case, &expected), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn prepared_seed_bad_image_has_no_candidate() -> TestResult {
    let case = Case::new()?;
    for artifact in [&[][..], &case.image.artifact[..32]] {
        assert!(prepare_aligned_seed_candidate(artifact, &case.expectation()).is_err());
    }
    let mut damaged = case.image.artifact.clone();
    *damaged.last_mut().ok_or("footer missing")? ^= 1;
    assert_eq!(
        prepare_aligned_seed_candidate(&damaged, &case.expectation()).map(|_| ()),
        Err(Error::InvalidImage)
    );
    let mut nested = case.image.artifact.clone();
    nested[32..36].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        prepare_aligned_seed_candidate(&nested, &case.expectation()).map(|_| ()),
        Err(Error::LimitExceeded)
    );
    Ok(())
}
