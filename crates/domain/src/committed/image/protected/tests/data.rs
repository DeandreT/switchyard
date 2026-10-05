use storage::{MemoryProtectedStateStore, ProtectedStateReader};

use crate::{CheckedProtectedCreateSendImage, check_protected_create_send_image};

use super::super::ProtectedCreateSendImageError as Error;
use super::fixture::{Image, TestResult, capture, publish};

#[test]
fn protected_rows_and_live_artifact_agree() -> TestResult {
    let image = Image::populated(&["first", "second"], b"protected-body-sentinel")?;
    let state = image.capture(b"opaque-metadata", b"fence-one")?;
    let view = check_protected_create_send_image(&state, &image.expectation(), b"fence-one")?;
    assert_eq!(view.image().checkpoint(), &image.checkpoint);
    assert_eq!(view.image().queue_count(), 2);
    assert_eq!(view.image().message_count(), 2);
    assert_eq!(view.image().row_count(), state.records().entries().len());
    for ((key, value), row) in state.records().entries().iter().zip(view.image().rows()) {
        assert_eq!(key.as_slice(), row.key());
        assert_eq!(value.as_slice(), row.value());
    }
    Ok(())
}

#[test]
fn initial_checkpoint_image_is_valid_descriptive_data() -> TestResult {
    let image = Image::initial()?;
    let state = image.capture(b"initial-opaque-metadata", b"initial-fence")?;
    let view = check_protected_create_send_image(&state, &image.expectation(), b"initial-fence")?;
    assert!(view.image().checkpoint().last().is_none());
    assert!(view.image().checkpoint().membership().is_none());
    assert_eq!(view.image().queue_count(), 0);
    assert_eq!(view.image().message_count(), 0);
    assert_eq!(view.image().row_count(), 1);
    Ok(())
}

#[test]
fn noninitial_image_without_membership_remains_in_profile() -> TestResult {
    let image = Image::one()?.no_member()?;
    let state = image.capture(b"opaque", b"fence")?;
    let view = check_protected_create_send_image(&state, &image.expectation(), b"fence")?;
    assert!(view.image().checkpoint().last().is_some());
    assert!(view.image().checkpoint().membership().is_none());
    assert_eq!(view.image().message_count(), 1);
    Ok(())
}

#[test]
fn checked_rows_and_artifact_stay_borrowed() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"fence")?;
    let view = check_protected_create_send_image(&state, &image.expectation(), b"fence")?;
    assert!(std::ptr::eq(view.protected_state(), &state));
    let artifact = state
        .live_catalog()
        .ok_or("missing live catalog")?
        .artifact();
    let first = artifact.as_ptr() as usize;
    let end = first + artifact.len();
    for row in view.image().rows() {
        for bytes in [row.key(), row.value()] {
            let pointer = bytes.as_ptr() as usize;
            assert!(pointer >= first && pointer + bytes.len() <= end);
        }
    }
    assert!(std::ptr::eq(
        view.protected_state().records().entries().as_ptr(),
        state.records().entries().as_ptr()
    ));
    Ok(())
}

#[test]
fn checked_view_survives_originating_handle_release() -> TestResult {
    let image = Image::one()?;
    let state = {
        let mut writer = MemoryProtectedStateStore::new();
        publish(
            &mut writer,
            &image.rows()?,
            &image.artifact,
            b"metadata",
            b"fence",
        )?;
        let reader = writer.reader();
        let another = reader.clone();
        another.capture_protected_state()?
    };
    let view = check_protected_create_send_image(&state, &image.expectation(), b"fence")?;
    assert_eq!(view.image().message_count(), 1);
    assert_eq!(view.protected_state().fence(), Some(b"fence".as_slice()));
    Ok(())
}

#[test]
fn expectation_and_fence_inputs_are_not_retained() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"fence")?;
    let view: CheckedProtectedCreateSendImage<'_> = {
        let checkpoint = image.checkpoint.clone();
        let fence = b"fence".to_vec();
        let expected = crate::CreateSendImageExpectation {
            checkpoint: &checkpoint,
            ..image.expectation()
        };
        check_protected_create_send_image(&state, &expected, &fence)?
    };
    assert_eq!(view.image().checkpoint(), &image.checkpoint);
    assert_eq!(view.image().message_count(), 1);
    Ok(())
}

#[test]
fn opaque_catalog_metadata_is_not_decoded() -> TestResult {
    let image = Image::one()?;
    for metadata in [
        Vec::new(),
        b"not-a-native-record".to_vec(),
        b"SWYN\xff".to_vec(),
    ] {
        let state = image.capture(&metadata, b"fence")?;
        let view = check_protected_create_send_image(&state, &image.expectation(), b"fence")?;
        assert_eq!(
            view.protected_state()
                .live_catalog()
                .ok_or("missing catalog")?
                .metadata(),
            metadata.as_slice()
        );
    }
    Ok(())
}

#[test]
fn stale_capture_remains_only_descriptive() -> TestResult {
    let old = Image::one()?;
    let new = old.different_body()?;
    let mut writer = MemoryProtectedStateStore::new();
    publish(
        &mut writer,
        &old.rows()?,
        &old.artifact,
        b"old",
        b"fence-one",
    )?;
    let reader = writer.reader();
    let old_state = reader.capture_protected_state()?;
    publish(
        &mut writer,
        &new.rows()?,
        &new.artifact,
        b"new",
        b"fence-two",
    )?;
    let view = check_protected_create_send_image(&old_state, &old.expectation(), b"fence-one")?;
    assert_eq!(
        view.protected_state().fence(),
        Some(b"fence-one".as_slice())
    );
    let current = reader.capture_protected_state()?;
    assert_eq!(current.fence(), Some(b"fence-two".as_slice()));
    check_protected_create_send_image(&current, &new.expectation(), b"fence-two")?;
    assert_ne!(old_state.records().entries(), current.records().entries());
    Ok(())
}

#[test]
fn missing_business_row_refuses() -> TestResult {
    let image = Image::one()?;
    let mut rows = image.rows()?;
    rows.remove(0);
    let state = capture(&rows, &image.artifact, b"metadata", b"fence")?;
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::BusinessMismatch)
    );
    Ok(())
}

#[test]
fn extra_business_row_refuses() -> TestResult {
    let image = Image::one()?;
    let mut rows = image.rows()?;
    rows.push((vec![0xff], b"extra".to_vec()));
    let state = capture(&rows, &image.artifact, b"metadata", b"fence")?;
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::BusinessMismatch)
    );
    Ok(())
}

#[test]
fn different_business_key_refuses() -> TestResult {
    let image = Image::one()?;
    let mut rows = image.rows()?;
    rows.last_mut().ok_or("missing last row")?.0.push(0);
    let state = capture(&rows, &image.artifact, b"metadata", b"fence")?;
    assert_eq!(rows.len(), image.rows()?.len());
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::BusinessMismatch)
    );
    Ok(())
}

#[test]
fn different_business_value_refuses() -> TestResult {
    let image = Image::one()?;
    let mut rows = image.rows()?;
    rows.last_mut().ok_or("missing last row")?.1.push(0);
    let state = capture(&rows, &image.artifact, b"metadata", b"fence")?;
    assert_eq!(rows.len(), image.rows()?.len());
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::BusinessMismatch)
    );
    Ok(())
}
