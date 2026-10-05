use std::collections::BTreeSet;

use crate::{CommittedImageError, CommittedImageValidationError};

use super::super::{
    CreateSendImageExpectation, CreateSendReplacementCounts,
    CreateSendReplacementPlanError as Error, business_error, container_error,
    plan_create_send_replacement,
};
use super::fixture::{checkpoint_variants, digest};
use super::{Image, TestResult, pair};

#[test]
fn valid_plan_borrows_both_artifacts_and_existing_image_views() -> TestResult {
    let old = Image::one()?;
    let selected = Image::populated(&["new-queue-sentinel"], b"new-body-sentinel")?;
    let plan = pair(&old, &selected)?;
    assert_eq!(plan.old_artifact_bytes().as_ptr(), old.artifact.as_ptr());
    assert_eq!(
        plan.selected_artifact_bytes().as_ptr(),
        selected.artifact.as_ptr()
    );
    assert_eq!(plan.old_artifact_bytes().len(), old.artifact.len());
    assert_eq!(
        plan.selected_artifact_bytes().len(),
        selected.artifact.len()
    );
    assert_eq!(plan.old_image().checkpoint(), &old.checkpoint);
    assert_eq!(plan.selected_image().checkpoint(), &selected.checkpoint);
    for (image, bytes) in [
        (plan.old_image(), old.artifact.as_slice()),
        (plan.selected_image(), selected.artifact.as_slice()),
    ] {
        for row in image.rows() {
            for part in [row.key(), row.value()] {
                let offset = part.as_ptr() as usize - bytes.as_ptr() as usize;
                assert!(offset <= bytes.len() && part.len() <= bytes.len() - offset);
            }
        }
    }
    assert_eq!(plan.counts().put_rows, plan.selected_image().row_count());
    assert_eq!(
        plan.counts().total_mutations,
        plan.counts().delete_rows + plan.counts().put_rows
    );
    Ok(())
}

#[test]
fn expectation_values_and_checkpoints_can_drop_before_plan() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let plan = {
        let old_checkpoint = old.checkpoint.clone();
        let selected_checkpoint = selected.checkpoint.clone();
        let old_expected = CreateSendImageExpectation {
            checkpoint: &old_checkpoint,
            artifact_bytes: old.artifact.len(),
            artifact_sha256: digest(&old.artifact),
        };
        let selected_expected = CreateSendImageExpectation {
            checkpoint: &selected_checkpoint,
            artifact_bytes: selected.artifact.len(),
            artifact_sha256: digest(&selected.artifact),
        };
        plan_create_send_replacement(
            &old.artifact,
            &selected.artifact,
            &old_expected,
            &selected_expected,
        )?
    };
    assert_eq!(plan.old_image().checkpoint(), &old.checkpoint);
    assert_eq!(plan.selected_image().checkpoint(), &selected.checkpoint);
    assert_eq!(plan.counts().delete_rows, 0);
    Ok(())
}

#[test]
fn every_old_full_checkpoint_component_is_compared() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for changed in checkpoint_variants(&old.checkpoint)? {
        let expected = CreateSendImageExpectation {
            checkpoint: &changed,
            ..old.expectation()
        };
        let error = if changed.stream() != selected.checkpoint.stream() {
            Error::InvalidExpectation
        } else {
            Error::IdentityMismatch
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &expected,
                &selected.expectation()
            )
            .err(),
            Some(error)
        );
    }
    Ok(())
}

#[test]
fn every_selected_full_checkpoint_component_is_compared() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for changed in checkpoint_variants(&selected.checkpoint)? {
        let expected = CreateSendImageExpectation {
            checkpoint: &changed,
            ..selected.expectation()
        };
        let error = if changed.stream() != old.checkpoint.stream() {
            Error::InvalidExpectation
        } else {
            Error::IdentityMismatch
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &old.expectation(),
                &expected
            )
            .err(),
            Some(error)
        );
    }
    Ok(())
}

#[test]
fn old_complete_artifact_length_is_exact() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for length in [old.artifact.len() - 1, old.artifact.len() + 1] {
        let expected = CreateSendImageExpectation {
            artifact_bytes: length,
            ..old.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &expected,
                &selected.expectation()
            )
            .err(),
            Some(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn selected_complete_artifact_length_is_exact() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    for length in [selected.artifact.len() - 1, selected.artifact.len() + 1] {
        let expected = CreateSendImageExpectation {
            artifact_bytes: length,
            ..selected.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &old.expectation(),
                &expected
            )
            .err(),
            Some(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn old_whole_artifact_digest_is_exact() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let mut wrong = digest(&old.artifact);
    wrong[0] ^= 1;
    for artifact_sha256 in [wrong, digest(&old.artifact[..old.artifact.len() - 32])] {
        let expected = CreateSendImageExpectation {
            artifact_sha256,
            ..old.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &expected,
                &selected.expectation()
            )
            .err(),
            Some(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn selected_whole_artifact_digest_is_exact() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let mut wrong = digest(&selected.artifact);
    wrong[0] ^= 1;
    for artifact_sha256 in [
        wrong,
        digest(&selected.artifact[..selected.artifact.len() - 32]),
    ] {
        let expected = CreateSendImageExpectation {
            artifact_sha256,
            ..selected.expectation()
        };
        assert_eq!(
            plan_create_send_replacement(
                &old.artifact,
                &selected.artifact,
                &old.expectation(),
                &expected
            )
            .err(),
            Some(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn same_cp_different_old_body_is_not_interchangeable() -> TestResult {
    let old = Image::one()?;
    let different = old.different_body()?;
    let selected = Image::one()?;
    assert_eq!(old.checkpoint, different.checkpoint);
    assert_eq!(old.artifact.len(), different.artifact.len());
    assert_ne!(digest(&old.artifact), digest(&different.artifact));
    assert!(pair(&different, &selected).is_ok());
    assert_eq!(
        plan_create_send_replacement(
            &different.artifact,
            &selected.artifact,
            &old.expectation(),
            &selected.expectation()
        )
        .err(),
        Some(Error::IdentityMismatch)
    );
    Ok(())
}

#[test]
fn same_cp_different_selected_body_is_not_interchangeable() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let different = selected.different_body()?;
    assert_eq!(selected.checkpoint, different.checkpoint);
    assert_eq!(selected.artifact.len(), different.artifact.len());
    assert_ne!(digest(&selected.artifact), digest(&different.artifact));
    assert!(pair(&old, &different).is_ok());
    assert_eq!(
        plan_create_send_replacement(
            &old.artifact,
            &different.artifact,
            &old.expectation(),
            &selected.expectation()
        )
        .err(),
        Some(Error::IdentityMismatch)
    );
    Ok(())
}

#[test]
fn opaque_non_native_membership_is_not_reinterpreted() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let plan = pair(&old, &selected)?;
    for image in [plan.old_image(), plan.selected_image()] {
        let member = image.checkpoint().membership().ok_or("missing member")?;
        assert_eq!(member.schema_version, 999);
        assert_eq!(member.payload, b"opaque-domain-member-sentinel");
    }
    Ok(())
}

#[test]
fn interleaved_overlap_stale_and_new_rows_have_exact_counts() -> TestResult {
    let old = Image::populated(&["alpha", "middle", "zulu"], b"old-content")?;
    let selected = Image::populated(&["bravo", "middle", "yankee"], b"selected-content")?;
    let old_rows = old.rows()?;
    let selected_rows = selected.rows()?;
    let keys: BTreeSet<_> = selected_rows
        .iter()
        .map(|(key, _)| key.as_slice())
        .collect();
    let deletes: Vec<_> = old_rows
        .iter()
        .filter(|(key, _)| !keys.contains(key.as_slice()))
        .collect();
    assert!(!deletes.is_empty());
    assert!(
        old_rows
            .iter()
            .any(|(key, _)| keys.contains(key.as_slice()))
    );
    let bytes = deletes.iter().map(|(key, _)| key.len()).sum::<usize>()
        + selected_rows
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>();
    assert_eq!(
        pair(&old, &selected)?.counts(),
        CreateSendReplacementCounts {
            delete_rows: deletes.len(),
            put_rows: selected_rows.len(),
            total_mutations: deletes.len() + selected_rows.len(),
            logical_payload_bytes: bytes,
        }
    );
    Ok(())
}

#[test]
fn exact_body_noop_counts_every_selected_put() -> TestResult {
    let image = Image::one()?;
    let rows = image.rows()?;
    assert_eq!(
        pair(&image, &image)?.counts(),
        CreateSendReplacementCounts {
            delete_rows: 0,
            put_rows: rows.len(),
            total_mutations: rows.len(),
            logical_payload_bytes: rows
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
        }
    );
    Ok(())
}

#[test]
fn earlier_selected_progress_is_only_a_count_report() -> TestResult {
    let old = Image::one()?;
    let selected = old.earlier()?;
    let plan = pair(&old, &selected)?;
    assert!(
        plan.selected_image()
            .checkpoint()
            .last()
            .ok_or("selected last")?
            .id
            .index
            < plan
                .old_image()
                .checkpoint()
                .last()
                .ok_or("old last")?
                .id
                .index
    );
    assert_eq!(plan.counts().delete_rows, 0);
    assert_eq!(plan.counts().put_rows, plan.selected_image().row_count());
    Ok(())
}

#[test]
fn initial_old_is_report_only_refusal() -> TestResult {
    let old = Image::initial()?;
    let selected = Image::one()?;
    assert_eq!(
        pair(&old, &selected).err(),
        Some(Error::UnsupportedPairPolicy)
    );
    Ok(())
}

#[test]
fn wrappers_and_errors_redact_content_and_identifiers() -> TestResult {
    let old = Image::one()?;
    let selected = Image::one()?;
    let plan = pair(&old, &selected)?;
    let text = format!(
        "{plan:?} {:?} {:?}",
        old.expectation(),
        selected.expectation()
    );
    for forbidden in [
        "domain-plan-sentinel",
        "queue-sentinel",
        "body-sentinel",
        "message-sentinel",
        "opaque-domain-member-sentinel",
        "CommittedEntryId",
        "CommittedStreamId",
        "fingerprint",
    ] {
        assert!(!text.contains(forbidden));
    }
    for error in [
        Error::LimitExceeded,
        Error::Allocation,
        Error::InvalidExpectation,
        Error::UnsupportedProfile,
        Error::InvalidImage,
        Error::IdentityMismatch,
        Error::UnsupportedPairPolicy,
    ] {
        let text = format!("{error:?} {error}");
        assert!(!text.contains("sentinel"));
        assert!(std::error::Error::source(&error).is_none());
    }
    assert_eq!(
        container_error(CommittedImageError::LimitExceeded),
        Error::LimitExceeded
    );
    assert_eq!(
        container_error(CommittedImageError::Allocation),
        Error::Allocation
    );
    assert_eq!(
        container_error(CommittedImageError::UnsupportedFormat),
        Error::UnsupportedProfile
    );
    assert_eq!(
        container_error(CommittedImageError::Malformed),
        Error::InvalidImage
    );
    assert_eq!(
        business_error(CommittedImageValidationError::UnsupportedProfile),
        Error::UnsupportedProfile
    );
    assert_eq!(
        business_error(CommittedImageValidationError::InvalidClock),
        Error::InvalidImage
    );
    Ok(())
}
