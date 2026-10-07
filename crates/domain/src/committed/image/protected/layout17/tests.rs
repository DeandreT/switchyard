use crate::{
    CommittedImageRole,
    committed::layout17_test_fixture::{self as fixture, TestResult},
};
use storage::ProtectedStateReader;

use super::*;

#[test]
fn current_agreement_borrows_complete_capture_not_expectation_or_fence_storage() -> TestResult {
    let image = fixture::current(&["private-queue"], b"private-body")?;
    let state = image.capture(b"private-metadata", b"private-fence")?;
    let view = {
        let checkpoint = image.checkpoint.clone();
        let fence = b"private-fence".to_vec();
        let expected = CreateSendImageExpectation {
            checkpoint: &checkpoint,
            ..image.expectation()
        };
        check_protected_create_send_layout17_image(&state, &expected, &fence)?
    };
    assert!(std::ptr::eq(view.protected_state(), &state));
    assert_eq!(view.image().checkpoint(), &image.checkpoint);
    assert_eq!(
        view.image().role(),
        CommittedImageRole::CreateSendLayout17V1
    );
    assert_eq!(view.image().queue_count(), 1);
    assert_eq!(
        view.image()
            .rows()
            .filter(|row| row.key().first() == Some(&0x16))
            .count(),
        1
    );
    assert_eq!(
        view.image()
            .rows()
            .map(|row| (row.key(), row.value()))
            .collect::<Vec<_>>(),
        state
            .records()
            .entries()
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>()
    );
    let artifact = state.live_catalog().ok_or("missing catalog")?.artifact();
    for row in view.image().rows() {
        let start = row.value().as_ptr() as usize;
        let backing = artifact.as_ptr() as usize;
        assert!(start >= backing && start + row.value().len() <= backing + artifact.len());
    }
    let debug = format!("{view:?}");
    for secret in ["private", "tenant", "opaque-member"] {
        assert!(!debug.contains(secret));
    }
    Ok(())
}

#[test]
fn protected_checks_remain_role_specific_and_refuse_bad_mode_before_identity_or_fence() -> TestResult
{
    let historical = fixture::initial(CommittedImageRole::CreateSendV1)?;
    let old_state = historical.capture(b"opaque", b"fence")?;
    assert!(
        super::super::check_protected_create_send_image(
            &old_state,
            &historical.expectation(),
            b"fence"
        )
        .is_ok()
    );
    assert_eq!(
        check_protected_create_send_layout17_image(&old_state, &historical.expectation(), b"wrong")
            .err(),
        Some(ProtectedCreateSendImageError::UnsupportedProfile)
    );
    let current = fixture::current(&["queue"], b"body")?;
    let state = current.capture(b"opaque", b"fence")?;
    assert_eq!(
        super::super::check_protected_create_send_image(&state, &current.expectation(), b"fence")
            .err(),
        Some(ProtectedCreateSendImageError::UnsupportedProfile)
    );
    for (value, expected) in [
        (None, ProtectedCreateSendImageError::InvalidImage),
        (
            Some(vec![11, 1, 1, 0, 0]),
            ProtectedCreateSendImageError::InvalidImage,
        ),
        (
            Some(vec![11, 1, 1, 1, 1]),
            ProtectedCreateSendImageError::UnsupportedProfile,
        ),
    ] {
        let mut rows = current.rows()?;
        if let Some(value) = value {
            rows.iter_mut()
                .find(|(key, _)| key.first() == Some(&0x16))
                .ok_or("missing mode")?
                .1 = value;
        } else {
            rows.retain(|(key, _)| key.first() != Some(&0x16));
        }
        let bad = fixture::from_rows(
            CommittedImageRole::CreateSendLayout17V1,
            rows,
            current.checkpoint.clone(),
        )?;
        let state = bad.capture(b"opaque", b"fence")?;
        let wrong = CreateSendImageExpectation {
            artifact_sha256: [0; 32],
            ..bad.expectation()
        };
        assert_eq!(
            check_protected_create_send_layout17_image(&state, &wrong, b"wrong").err(),
            Some(expected)
        );
    }
    Ok(())
}

#[test]
fn full_identity_precedes_fence_then_complete_business_mode_row_equality() -> TestResult {
    let image = fixture::current(&["queue"], b"body")?;
    let mut rows = image.rows()?;
    rows.iter_mut()
        .find(|(key, _)| key.first() == Some(&0x16))
        .ok_or("missing mode")?
        .1
        .push(0);
    let state = fixture::capture(&rows, &image.artifact, b"opaque", b"fence")?;
    let wrong = CreateSendImageExpectation {
        artifact_sha256: [0; 32],
        ..image.expectation()
    };
    assert_eq!(
        check_protected_create_send_layout17_image(&state, &wrong, b"wrong").err(),
        Some(ProtectedCreateSendImageError::IdentityMismatch)
    );
    assert_eq!(
        check_protected_create_send_layout17_image(&state, &image.expectation(), b"wrong").err(),
        Some(ProtectedCreateSendImageError::FenceMismatch)
    );
    assert_eq!(
        check_protected_create_send_layout17_image(&state, &image.expectation(), b"fence").err(),
        Some(ProtectedCreateSendImageError::BusinessMismatch)
    );
    Ok(())
}

#[test]
fn initial_role2_agreement_is_pure_data_and_scalar_limits_precede_capture_shape() -> TestResult {
    let image = fixture::initial(CommittedImageRole::CreateSendLayout17V1)?;
    let state = image.capture(b"opaque", b"fence")?;
    let view = check_protected_create_send_layout17_image(&state, &image.expectation(), b"fence")?;
    assert_eq!(view.image().row_count(), 1);
    assert_eq!(view.image().queue_count(), 0);
    assert_eq!(view.image().message_count(), 0);
    let empty = storage::MemoryProtectedStateStore::new()
        .reader()
        .capture_protected_state()?;
    let oversized = CreateSendImageExpectation {
        artifact_bytes: MAX_COMMITTED_IMAGE_BYTES + 1,
        ..image.expectation()
    };
    assert_eq!(
        check_protected_create_send_layout17_image(&empty, &oversized, b"").err(),
        Some(ProtectedCreateSendImageError::LimitExceeded)
    );
    assert_eq!(
        check_protected_create_send_layout17_image(&empty, &image.expectation(), b"").err(),
        Some(ProtectedCreateSendImageError::InvalidExpectation)
    );
    assert_eq!(
        check_protected_create_send_layout17_image(&empty, &image.expectation(), b"fence").err(),
        Some(ProtectedCreateSendImageError::NotInitialized)
    );
    Ok(())
}
