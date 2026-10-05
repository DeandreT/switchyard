use super::*;

mod controls;
mod fixture;
mod policy;

use fixture::{Case, Image, TestResult};

#[test]
fn aligned_seed_exact_empty_tail() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let result = inspect_aligned_seed(
        BorrowedAlignedSeed {
            metadata: &case.image.metadata,
            artifact: &case.image.artifact,
            log_rows: &rows,
        },
        &case.expectation(),
    )?;
    assert_eq!(result.baseline_ordinal(), 1);
    assert_eq!(result.vote(), case.vote);
    assert_eq!(result.image_pair().checkpoint(), &case.image.checkpoint);
    assert_eq!(result.image_pair().snapshot_meta()?, case.image.native);
    assert_eq!(case.image.native.snapshot_id.len(), 79);
    Ok(())
}

#[test]
fn aligned_seed_same_cp_different_body() -> TestResult {
    let case = Case::new()?;
    let changed = case.image.changed_body()?;
    assert_eq!(changed.checkpoint, case.image.checkpoint);
    assert_ne!(changed.artifact, case.image.artifact);
    let mut expected = case.expectation();
    expected.artifact_sha256 = Sha256::digest(&changed.artifact).into();
    expected.artifact_bytes = changed.artifact.len();
    expected.native = &changed.native;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn aligned_seed_full_checkpoint_fields() -> TestResult {
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
    assert_ne!(
        next.checkpoint.last().map(|mark| mark.id),
        case.image.checkpoint.last().map(|mark| mark.id)
    );
    for checkpoint in [&clock.checkpoint, &member.checkpoint, &next.checkpoint] {
        let mut expected = case.expectation();
        expected.checkpoint = checkpoint;
        assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    }
    Ok(())
}

#[test]
fn aligned_seed_native_projection_fields() -> TestResult {
    let case = Case::new()?;
    let member = Image::variant([7; 16], 10, 11, 17)?;
    let mut variants = Vec::new();
    let mut last = case.image.native.clone();
    last.last_log_id = None;
    variants.push(last);
    let mut source = case.image.native.clone();
    source.last_membership =
        openraft::StoredMembership::new(None, source.last_membership.membership().clone());
    variants.push(source);
    let mut nodes = case.image.native.clone();
    nodes.last_membership = member.native.last_membership.clone();
    variants.push(nodes);
    let stored = &case.image.native.last_membership;
    let original = stored.membership();
    let node_map = original
        .nodes()
        .map(|(id, node)| (*id, node.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut addresses = node_map.clone();
    addresses
        .get_mut(&7)
        .ok_or("node missing")?
        .addr
        .push_str("-changed");
    let address_only = openraft::Membership::new(original.get_joint_config().clone(), addresses);
    assert_eq!(address_only.get_joint_config(), original.get_joint_config());
    let mut address = case.image.native.clone();
    address.last_membership = openraft::StoredMembership::new(*stored.log_id(), address_only);
    assert_eq!(address.last_membership.log_id(), stored.log_id());
    assert_eq!(address.last_log_id, case.image.native.last_log_id);
    assert_eq!(address.snapshot_id, case.image.native.snapshot_id);
    variants.push(address);
    let mut configs = original.get_joint_config().clone();
    assert!(configs.first_mut().ok_or("config missing")?.remove(&9));
    let config_only = openraft::Membership::new(configs, node_map);
    assert_eq!(
        config_only.nodes().collect::<Vec<_>>(),
        original.nodes().collect::<Vec<_>>()
    );
    let mut config = case.image.native.clone();
    config.last_membership = openraft::StoredMembership::new(*stored.log_id(), config_only);
    assert_eq!(config.last_membership.log_id(), stored.log_id());
    assert_eq!(config.last_log_id, case.image.native.last_log_id);
    assert_eq!(config.snapshot_id, case.image.native.snapshot_id);
    variants.push(config);
    let mut id = case.image.native.clone();
    id.snapshot_id.push('x');
    variants.push(id);
    for native in &variants {
        let mut expected = case.expectation();
        expected.native = native;
        assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    }
    Ok(())
}

#[test]
fn aligned_seed_independent_profile_node_and_stream() -> TestResult {
    let case = Case::new()?;
    let other = Image::variant([8; 16], 10, 11, 7)?;
    for profile in [
        LogProfile::new(8, case.profile.stream())?,
        LogProfile::new(7, other.checkpoint.stream())?,
    ] {
        let mut expected = case.expectation();
        expected.profile = &profile;
        if profile.stream() != case.image.checkpoint.stream() {
            expected.checkpoint = &other.checkpoint;
        }
        assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    }
    let profile = LogProfile::new(7, other.checkpoint.stream())?;
    let mut inconsistent = case.expectation();
    inconsistent.profile = &profile;
    assert_eq!(case.run(&inconsistent), Err(Error::InvalidExpectation));
    Ok(())
}

#[test]
fn aligned_seed_artifact_identity() -> TestResult {
    let case = Case::new()?;
    let mut expected = case.expectation();
    expected.artifact_bytes += 1;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    expected = case.expectation();
    expected.artifact_sha256[0] ^= 1;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn aligned_seed_debug_and_errors_private() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let observed = BorrowedAlignedSeed {
        metadata: &case.image.metadata,
        artifact: &case.image.artifact,
        log_rows: &rows,
    };
    let offered_debug = format!("{observed:?}");
    let expected = case.expectation();
    let result = inspect_aligned_seed(observed, &expected)?;
    for value in [
        offered_debug,
        format!("{expected:?}"),
        format!("{result:?}"),
    ] {
        for hidden in [
            "PRIVATE",
            "snapshot_id",
            "leader_id",
            "node_id",
            "fingerprint",
            "artifact_sha256",
        ] {
            assert!(!value.contains(hidden));
        }
    }
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
        assert!(!format!("{error:?}: {error}").contains("PRIVATE"));
    }
    Ok(())
}

#[test]
fn aligned_seed_borrowed_artifact_address_unchanged() -> TestResult {
    let case = Case::new()?;
    let rows = case.rows();
    let result = inspect_aligned_seed(
        BorrowedAlignedSeed {
            metadata: &case.image.metadata,
            artifact: &case.image.artifact,
            log_rows: &rows,
        },
        &case.expectation(),
    )?;
    assert_eq!(
        result.image_pair().artifact_bytes().as_ptr(),
        case.image.artifact.as_ptr()
    );
    assert_eq!(
        result.image_pair().metadata_bytes().as_ptr(),
        case.image.metadata.as_ptr()
    );
    assert_eq!(result.image_pair().artifact_bytes(), case.image.artifact);
    Ok(())
}
