use std::collections::BTreeSet;

use crate::{
    CommittedImageRole, ValidatedCreateSendImage,
    committed::layout17_test_fixture::{self as fixture, Image, TestResult},
};

use super::*;

fn plan<'a, 'b>(
    old: &'a Image,
    selected: &'b Image,
) -> Result<PlannedCreateSendLayout17Replacement<'a, 'b>> {
    plan_create_send_layout17_replacement(
        &old.artifact,
        &selected.artifact,
        &old.expectation(),
        &selected.expectation(),
    )
}

#[test]
fn current_pair_counts_actual_modes_and_borrows_inputs_not_expectations() -> TestResult {
    let old = fixture::current(&["old-private", "shared-private"], b"old-private-body")?;
    let selected = fixture::current(&["shared-private", "new-private"], b"selected-private-body")?;
    let planned = {
        let old_checkpoint = old.checkpoint.clone();
        let selected_checkpoint = selected.checkpoint.clone();
        let old_expected = CreateSendImageExpectation {
            checkpoint: &old_checkpoint,
            ..old.expectation()
        };
        let selected_expected = CreateSendImageExpectation {
            checkpoint: &selected_checkpoint,
            ..selected.expectation()
        };
        plan_create_send_layout17_replacement(
            &old.artifact,
            &selected.artifact,
            &old_expected,
            &selected_expected,
        )?
    };
    assert!(std::ptr::eq(
        planned.old_artifact_bytes(),
        old.artifact.as_slice()
    ));
    assert!(std::ptr::eq(
        planned.selected_artifact_bytes(),
        selected.artifact.as_slice()
    ));
    assert_eq!(planned.old_image().checkpoint(), &old.checkpoint);
    assert_eq!(planned.selected_image().checkpoint(), &selected.checkpoint);
    for image in [planned.old_image(), planned.selected_image()] {
        assert_eq!(image.role(), CommittedImageRole::CreateSendLayout17V1);
        assert_eq!(image.queue_count(), 2);
        assert_eq!(
            image
                .rows()
                .filter(|row| row.key().first() == Some(&0x16))
                .count(),
            2
        );
    }
    let old_rows = old.rows()?;
    let selected_rows = selected.rows()?;
    let selected_keys: BTreeSet<_> = selected_rows
        .iter()
        .map(|(key, _)| key.as_slice())
        .collect();
    let deleted: Vec<_> = old_rows
        .iter()
        .filter(|(key, _)| !selected_keys.contains(key.as_slice()))
        .collect();
    assert_eq!(
        planned.counts(),
        CreateSendReplacementCounts {
            delete_rows: deleted.len(),
            put_rows: selected_rows.len(),
            total_mutations: deleted.len() + selected_rows.len(),
            logical_payload_bytes: deleted.iter().map(|(key, _)| key.len()).sum::<usize>()
                + selected_rows
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>(),
        }
    );
    let debug = format!("{planned:?}");
    for secret in ["private", "layout17", "tenant", "opaque-member"] {
        assert!(!debug.contains(secret));
    }
    Ok(())
}

#[test]
fn pure_planners_remain_role_specific_for_either_offered_image() -> TestResult {
    let current = fixture::current(&["queue"], b"body")?;
    let historical = fixture::initial(CommittedImageRole::CreateSendV1)?;
    assert!(
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(&historical.artifact)?)
            .is_ok()
    );
    for (old, selected) in [(&historical, &current), (&current, &historical)] {
        assert_eq!(
            plan(old, selected).err(),
            Some(CreateSendReplacementPlanError::UnsupportedProfile)
        );
    }
    assert_eq!(
        super::super::plan_create_send_replacement(
            &current.artifact,
            &current.artifact,
            &current.expectation(),
            &current.expectation()
        )
        .err(),
        Some(CreateSendReplacementPlanError::UnsupportedProfile)
    );
    Ok(())
}

#[test]
fn complete_selected_business_validation_precedes_old_identity_mismatch() -> TestResult {
    let old = fixture::current(&["queue"], b"body")?;
    let mut old_expected = old.expectation();
    old_expected.artifact_sha256 = [0; 32];
    for (value, expected) in [
        (None, CreateSendReplacementPlanError::InvalidImage),
        (
            Some(vec![11, 1, 1, 0, 0]),
            CreateSendReplacementPlanError::InvalidImage,
        ),
        (
            Some(vec![11, 1, 1, 1, 1]),
            CreateSendReplacementPlanError::UnsupportedProfile,
        ),
    ] {
        let mut rows = old.rows()?;
        let mode = rows
            .iter_mut()
            .find(|(key, _)| key.first() == Some(&0x16))
            .ok_or("missing mode")?;
        if let Some(value) = value {
            mode.1 = value;
        } else {
            rows.retain(|(key, _)| key.first() != Some(&0x16));
        }
        let selected = fixture::from_rows(
            CommittedImageRole::CreateSendLayout17V1,
            rows,
            old.checkpoint.clone(),
        )?;
        assert_eq!(
            plan_create_send_layout17_replacement(
                &old.artifact,
                &selected.artifact,
                &old_expected,
                &selected.expectation()
            )
            .err(),
            Some(expected)
        );
    }
    Ok(())
}

#[test]
fn shape_identity_and_noninitial_member_policy_keep_static_priorities() -> TestResult {
    let image = fixture::current(&["queue"], b"body")?;
    let oversized = CreateSendImageExpectation {
        artifact_bytes: MAX_COMMITTED_IMAGE_BYTES + 1,
        ..image.expectation()
    };
    assert_eq!(
        plan_create_send_layout17_replacement(
            b"malformed",
            b"malformed",
            &oversized,
            &image.expectation()
        )
        .err(),
        Some(CreateSendReplacementPlanError::LimitExceeded)
    );
    let wrong = CreateSendImageExpectation {
        artifact_sha256: [0; 32],
        ..image.expectation()
    };
    assert_eq!(
        plan_create_send_layout17_replacement(
            &image.artifact,
            &image.artifact,
            &wrong,
            &image.expectation()
        )
        .err(),
        Some(CreateSendReplacementPlanError::IdentityMismatch)
    );
    let initial = fixture::initial(CommittedImageRole::CreateSendLayout17V1)?;
    assert_eq!(
        plan(&initial, &initial).err(),
        Some(CreateSendReplacementPlanError::UnsupportedPairPolicy)
    );
    let same = plan(&image, &image)?;
    assert_eq!(same.counts().delete_rows, 0);
    assert_eq!(same.counts().put_rows, image.rows()?.len());
    assert_eq!(same.counts().total_mutations, same.counts().put_rows);
    Ok(())
}
