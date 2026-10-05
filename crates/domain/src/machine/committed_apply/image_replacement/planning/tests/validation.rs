use crate::{MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_MEMBERSHIP_BYTES, codec, keys};

use super::fixture::{digest, from_rows, malformed_business, malformed_containers, unchecked_rows};
use super::{
    CreateSendImageExpectation, CreateSendReplacementPlanError as Error, Image, TestResult, pair,
    plan_create_send_replacement,
};

#[test]
fn malformed_old_containers_never_produce_a_plan() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for (artifact, error) in malformed_containers(&old)? {
        assert_eq!(
            plan_create_send_replacement(
                &artifact,
                &selected.artifact,
                &old.expectation(),
                &selected.expectation()
            )
            .err(),
            Some(error)
        );
    }
    Ok(())
}

#[test]
fn malformed_selected_containers_never_produce_a_plan() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for (artifact, error) in malformed_containers(&selected)? {
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &artifact,
                &old.expectation(),
                &selected.expectation()
            )
            .err(),
            Some(error)
        );
    }
    Ok(())
}

fn wrong_expectations(image: &Image) -> Vec<CreateSendImageExpectation<'_>> {
    let mut length = image.expectation();
    length.artifact_bytes += 1;
    let mut hash = image.expectation();
    hash.artifact_sha256[0] ^= 1;
    vec![length, hash]
}

#[test]
fn old_business_validation_precedes_external_mismatch() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for malformed in malformed_business(&old)? {
        for expected in wrong_expectations(&malformed) {
            assert_eq!(
                plan_create_send_replacement(
                    &malformed.artifact,
                    &selected.artifact,
                    &expected,
                    &selected.expectation()
                )
                .err(),
                Some(Error::InvalidImage)
            );
        }
        let mut foreign = malformed.checkpoint.clone();
        foreign
            .last
            .as_mut()
            .ok_or("missing last mark")?
            .fingerprint[0] ^= 1;
        let expected = CreateSendImageExpectation {
            checkpoint: &foreign,
            ..malformed.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &malformed.artifact,
                &selected.artifact,
                &expected,
                &selected.expectation()
            )
            .err(),
            Some(Error::InvalidImage)
        );
    }
    Ok(())
}

#[test]
fn selected_business_validation_precedes_external_mismatch() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for malformed in malformed_business(&selected)? {
        for expected in wrong_expectations(&malformed) {
            assert_eq!(
                plan_create_send_replacement(
                    &old.artifact,
                    &malformed.artifact,
                    &old.expectation(),
                    &expected
                )
                .err(),
                Some(Error::InvalidImage)
            );
        }
        let mut foreign = malformed.checkpoint.clone();
        foreign
            .last
            .as_mut()
            .ok_or("missing last mark")?
            .fingerprint[0] ^= 1;
        let expected = CreateSendImageExpectation {
            checkpoint: &foreign,
            ..malformed.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &malformed.artifact,
                &old.expectation(),
                &expected
            )
            .err(),
            Some(Error::InvalidImage)
        );
    }
    Ok(())
}

#[test]
fn recognized_broader_profiles_are_not_called_corrupt() -> TestResult {
    let image = Image::one()?;
    let mut cases = Vec::new();
    let mut rows = image.rows()?;
    rows.iter_mut()
        .find(|(key, _)| *key == keys::clock())
        .ok_or("missing clock")?
        .1 = vec![12, 0];
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut rows = image.rows()?;
    rows.push((vec![0xff], Vec::new()));
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut rows = image.rows()?;
    let message = rows
        .iter_mut()
        .find(|(key, _)| key.first() == Some(&3))
        .ok_or("missing message")?;
    let mut record: crate::MessageRecord = codec::decode(&message.1)?;
    record.state = crate::MessageState::Deferred;
    message.1 = codec::encode(&record)?;
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut artifact = unchecked_rows(&image.rows()?, image.checkpoint.stream())?;
    artifact[6..8].copy_from_slice(&2u16.to_be_bytes());
    let end = artifact.len() - 32;
    let checksum = digest(&artifact[..end]);
    artifact[end..].copy_from_slice(&checksum);
    cases.push(artifact);
    for artifact in cases {
        assert_eq!(
            plan_create_send_replacement(
                &artifact,
                &image.artifact,
                &image.expectation(),
                &image.expectation()
            )
            .err(),
            Some(Error::UnsupportedProfile)
        );
        assert_eq!(
            plan_create_send_replacement(
                &image.artifact,
                &artifact,
                &image.expectation(),
                &image.expectation()
            )
            .err(),
            Some(Error::UnsupportedProfile)
        );
    }
    Ok(())
}

#[test]
fn initial_selected_is_report_only_refusal() -> TestResult {
    assert_eq!(
        pair(&Image::one()?, &Image::initial()?).err(),
        Some(Error::UnsupportedPairPolicy)
    );
    Ok(())
}

#[test]
fn noninitial_old_without_member_is_refused() -> TestResult {
    let old = Image::one()?.no_member()?;
    assert!(
        super::super::validate(&old.artifact)?
            .checkpoint()
            .last()
            .is_some()
    );
    assert_eq!(
        pair(&old, &Image::one()?).err(),
        Some(Error::UnsupportedPairPolicy)
    );
    Ok(())
}

#[test]
fn noninitial_selected_without_member_is_refused() -> TestResult {
    let selected = Image::one()?.no_member()?;
    assert!(
        super::super::validate(&selected.artifact)?
            .checkpoint()
            .last()
            .is_some()
    );
    assert_eq!(
        pair(&Image::one()?, &selected).err(),
        Some(Error::UnsupportedPairPolicy)
    );
    Ok(())
}

#[test]
fn artifact_limits_precede_all_recovery_and_expectation_consistency() -> TestResult {
    // One explicit large allocation, reused for both sides and then released.
    let oversized = vec![0; MAX_COMMITTED_IMAGE_BYTES + 1];
    let image = Image::one()?;
    let mut foreign = image.checkpoint.clone();
    foreign.stream = crate::CommittedStreamId::new([40; 16])?;
    let foreign_expected = CreateSendImageExpectation {
        checkpoint: &foreign,
        ..image.expectation()
    };
    for peer in [&image.artifact[..], &[][..]] {
        assert_eq!(
            plan_create_send_replacement(&oversized, peer, &image.expectation(), &foreign_expected)
                .err(),
            Some(Error::LimitExceeded)
        );
        assert_eq!(
            plan_create_send_replacement(peer, &oversized, &image.expectation(), &foreign_expected)
                .err(),
            Some(Error::LimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn expected_lengths_and_member_caps_precede_container_errors() -> TestResult {
    let image = Image::one()?;
    for selected in [false, true] {
        let mut over_length = image.expectation();
        over_length.artifact_bytes = MAX_COMMITTED_IMAGE_BYTES + 1;
        let bounded = image.expectation();
        let (old, new) = if selected {
            (&bounded, &over_length)
        } else {
            (&over_length, &bounded)
        };
        assert_eq!(
            plan_create_send_replacement(&[], &[], old, new).err(),
            Some(Error::LimitExceeded)
        );
        let mut oversized_member = image.checkpoint.clone();
        oversized_member
            .membership
            .as_mut()
            .ok_or("missing membership")?
            .payload = vec![0; MAX_COMMITTED_MEMBERSHIP_BYTES + 1];
        let over_member = CreateSendImageExpectation {
            checkpoint: &oversized_member,
            ..image.expectation()
        };
        let (old, new) = if selected {
            (&bounded, &over_member)
        } else {
            (&over_member, &bounded)
        };
        assert_eq!(
            plan_create_send_replacement(&[], &[], old, new).err(),
            Some(Error::LimitExceeded)
        );
    }
    let mut at_cap = image.checkpoint.clone();
    at_cap
        .membership
        .as_mut()
        .ok_or("missing membership")?
        .payload = vec![0; MAX_COMMITTED_MEMBERSHIP_BYTES];
    let expected = CreateSendImageExpectation {
        checkpoint: &at_cap,
        artifact_bytes: MAX_COMMITTED_IMAGE_BYTES,
        artifact_sha256: [0; 32],
    };
    assert_eq!(
        super::super::check_shape(
            MAX_COMMITTED_IMAGE_BYTES,
            MAX_COMMITTED_IMAGE_BYTES,
            &expected,
            &expected
        ),
        Ok(())
    );
    assert_eq!(
        super::super::check_shape(usize::MAX, 0, &expected, &expected),
        Err(Error::LimitExceeded)
    );
    assert_eq!(
        super::super::check_shape(0, usize::MAX, &expected, &expected),
        Err(Error::LimitExceeded)
    );
    let expected = CreateSendImageExpectation {
        artifact_bytes: usize::MAX,
        ..expected
    };
    assert_eq!(
        super::super::check_shape(0, 0, &expected, &image.expectation()),
        Err(Error::LimitExceeded)
    );
    assert_eq!(
        super::super::check_shape(0, 0, &image.expectation(), &expected),
        Err(Error::LimitExceeded)
    );
    Ok(())
}

#[test]
fn expected_stream_consistency_precedes_container_decode() -> TestResult {
    let image = Image::one()?;
    let mut foreign = image.checkpoint.clone();
    foreign.stream = crate::CommittedStreamId::new([40; 16])?;
    let expected = CreateSendImageExpectation {
        checkpoint: &foreign,
        ..image.expectation()
    };
    assert_eq!(
        plan_create_send_replacement(&[], &[], &image.expectation(), &expected).err(),
        Some(Error::InvalidExpectation)
    );
    let mut invalid = image.checkpoint.clone();
    invalid.stream = postcard::from_bytes(&[0; 16])?;
    let expected = CreateSendImageExpectation {
        checkpoint: &invalid,
        ..image.expectation()
    };
    for selected in [false, true] {
        let bounded = image.expectation();
        let (old, new) = if selected {
            (&bounded, &expected)
        } else {
            (&expected, &bounded)
        };
        assert_eq!(
            plan_create_send_replacement(&[], &[], old, new).err(),
            Some(Error::InvalidExpectation)
        );
    }
    Ok(())
}

#[test]
fn both_business_checks_precede_identity_and_initial_policy() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let malformed = malformed_business(&selected)?.remove(0);
    let initial = Image::initial()?;
    assert_eq!(
        plan_create_send_replacement(
            &initial.artifact,
            &malformed.artifact,
            &initial.expectation(),
            &malformed.expectation()
        )
        .err(),
        Some(Error::InvalidImage)
    );
    for expected in wrong_expectations(&old) {
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &malformed.artifact,
                &expected,
                &malformed.expectation()
            )
            .err(),
            Some(Error::InvalidImage)
        );
    }
    let mut foreign = old.checkpoint.clone();
    foreign
        .last
        .as_mut()
        .ok_or("missing last mark")?
        .fingerprint[0] ^= 1;
    let expected = CreateSendImageExpectation {
        checkpoint: &foreign,
        ..old.expectation()
    };
    assert_eq!(
        plan_create_send_replacement(
            &old.artifact,
            &malformed.artifact,
            &expected,
            &malformed.expectation()
        )
        .err(),
        Some(Error::InvalidImage)
    );
    let bad_old = malformed_business(&old)?.remove(0);
    let mut broader = selected.artifact.clone();
    broader[0] ^= 1;
    assert_eq!(
        plan_create_send_replacement(
            &bad_old.artifact,
            &broader,
            &bad_old.expectation(),
            &selected.expectation()
        )
        .err(),
        Some(Error::InvalidImage)
    );
    Ok(())
}
