use storage::{MAX_PROTECTED_STATE_FENCE_BYTES, MemoryProtectedStateStore, ProtectedStateReader};

use crate::{
    CreateSendImageExpectation, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_MEMBERSHIP_BYTES,
    MessageState, check_protected_create_send_image, codec, keys,
};

use super::super::{ProtectedCreateSendImageError as Error, check_expectation_shape};
use super::fixture::{
    Image, TestResult, capture, digest, from_rows, malformed_business, malformed_containers,
};

#[test]
fn pristine_capture_is_not_initialized() -> TestResult {
    let image = Image::one()?;
    let state = MemoryProtectedStateStore::new()
        .reader()
        .capture_protected_state()?;
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::NotInitialized)
    );
    assert!(!state.is_initialized());
    assert!(state.records().entries().is_empty());
    assert!(state.live_catalog().is_none() && state.fence().is_none());
    Ok(())
}

#[test]
fn present_empty_catalog_is_not_a_create_send_image() -> TestResult {
    let image = Image::one()?;
    let state = capture(&image.rows()?, b"", b"metadata", b"fence")?;
    assert!(state.is_initialized());
    assert!(state.live_catalog().ok_or("catalog")?.artifact().is_empty());
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"fence").err(),
        Some(Error::InvalidImage)
    );
    Ok(())
}

#[test]
fn malformed_containers_are_refused() -> TestResult {
    let image = Image::one()?;
    let rows = image.rows()?;
    for (artifact, error) in malformed_containers(&image)? {
        let state = capture(&rows, &artifact, b"metadata", b"fence")?;
        let mut expected = image.expectation();
        expected.artifact_bytes += 1;
        expected.artifact_sha256[0] ^= 1;
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"wrong-fence").err(),
            Some(error)
        );
    }
    Ok(())
}

#[test]
fn invalid_business_images_are_refused() -> TestResult {
    let image = Image::one()?;
    let rows = image.rows()?;
    for malformed in malformed_business(&image)? {
        let state = capture(&rows, &malformed.artifact, b"metadata", b"fence")?;
        let mut expected = malformed.expectation();
        expected.artifact_bytes += 1;
        expected.artifact_sha256[0] ^= 1;
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"wrong-fence").err(),
            Some(Error::InvalidImage)
        );
        let mut checkpoint = malformed.checkpoint.clone();
        checkpoint.last.as_mut().ok_or("last")?.fingerprint[0] ^= 1;
        let expected = CreateSendImageExpectation {
            checkpoint: &checkpoint,
            ..malformed.expectation()
        };
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"wrong-fence").err(),
            Some(Error::InvalidImage)
        );
    }
    Ok(())
}

#[test]
fn broader_profiles_are_unsupported_not_corrupt() -> TestResult {
    let image = Image::one()?;
    let original = image.rows()?;
    let mut cases = Vec::new();
    let mut rows = image.rows()?;
    rows.iter_mut()
        .find(|(key, _)| *key == keys::clock())
        .ok_or("clock")?
        .1 = vec![12, 0];
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut rows = image.rows()?;
    rows.push((vec![0xff], Vec::new()));
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut rows = image.rows()?;
    let row = rows
        .iter_mut()
        .find(|(key, _)| key.first() == Some(&3))
        .ok_or("message")?;
    let mut message: crate::MessageRecord = codec::decode(&row.1)?;
    message.state = MessageState::Deferred;
    row.1 = codec::encode(&message)?;
    cases.push(from_rows(rows, image.checkpoint.clone())?.artifact);
    let mut artifact = image.artifact.clone();
    artifact[6..8].copy_from_slice(&2u16.to_be_bytes());
    let end = artifact.len() - 32;
    let checksum = digest(&artifact[..end]);
    artifact[end..].copy_from_slice(&checksum);
    cases.push(artifact);
    for artifact in cases {
        let state = capture(&original, &artifact, b"metadata", b"fence")?;
        assert_eq!(
            check_protected_create_send_image(&state, &image.expectation(), b"wrong-fence").err(),
            Some(Error::UnsupportedProfile)
        );
    }
    Ok(())
}

#[test]
fn expectation_limits_precede_image_recovery() -> TestResult {
    let image = Image::one()?;
    let state = capture(&image.rows()?, b"", b"metadata", b"fence")?;
    let pristine = MemoryProtectedStateStore::new()
        .reader()
        .capture_protected_state()?;
    for bytes in [MAX_COMMITTED_IMAGE_BYTES + 1, usize::MAX] {
        let expected = CreateSendImageExpectation {
            artifact_bytes: bytes,
            ..image.expectation()
        };
        for capture in [&state, &pristine] {
            assert_eq!(
                check_protected_create_send_image(capture, &expected, b"").err(),
                Some(Error::LimitExceeded)
            );
        }
    }
    let mut checkpoint = image.checkpoint.clone();
    checkpoint.membership.as_mut().ok_or("member")?.payload =
        vec![0; MAX_COMMITTED_MEMBERSHIP_BYTES + 1];
    let expected = CreateSendImageExpectation {
        checkpoint: &checkpoint,
        ..image.expectation()
    };
    assert_eq!(
        check_protected_create_send_image(&state, &expected, b"fence").err(),
        Some(Error::LimitExceeded)
    );
    assert_eq!(
        check_protected_create_send_image(
            &state,
            &image.expectation(),
            &[0; MAX_PROTECTED_STATE_FENCE_BYTES + 1]
        )
        .err(),
        Some(Error::LimitExceeded)
    );
    checkpoint.membership.as_mut().ok_or("member")?.payload =
        vec![0; MAX_COMMITTED_MEMBERSHIP_BYTES];
    let at_cap = CreateSendImageExpectation {
        checkpoint: &checkpoint,
        artifact_bytes: MAX_COMMITTED_IMAGE_BYTES,
        artifact_sha256: [0; 32],
    };
    let fence = [0; MAX_PROTECTED_STATE_FENCE_BYTES];
    assert_eq!(check_expectation_shape(&at_cap, &fence), Ok(()));
    let valid = image.capture(b"metadata", &fence)?;
    assert_eq!(
        check_protected_create_send_image(&valid, &at_cap, &fence).err(),
        Some(Error::IdentityMismatch)
    );
    check_protected_create_send_image(&valid, &image.expectation(), &fence)?;
    Ok(())
}

#[test]
fn invalid_expected_stream_or_empty_fence_refuses() -> TestResult {
    let image = Image::one()?;
    let state = capture(&image.rows()?, b"", b"metadata", b"fence")?;
    let pristine = MemoryProtectedStateStore::new()
        .reader()
        .capture_protected_state()?;
    let mut checkpoint = image.checkpoint.clone();
    checkpoint.stream = postcard::from_bytes(&[0; 16])?;
    let expected = CreateSendImageExpectation {
        checkpoint: &checkpoint,
        ..image.expectation()
    };
    for capture in [&state, &pristine] {
        assert_eq!(
            check_protected_create_send_image(capture, &expected, b"fence").err(),
            Some(Error::InvalidExpectation)
        );
        assert_eq!(
            check_protected_create_send_image(capture, &image.expectation(), b"").err(),
            Some(Error::InvalidExpectation)
        );
    }
    Ok(())
}

#[test]
fn debug_and_static_errors_do_not_recurse_into_data() -> TestResult {
    let image = Image::one()?;
    let metadata = b"protected-metadata-sentinel";
    let fence = b"protected-fence-sentinel";
    let state = image.capture(metadata, fence)?;
    let view = check_protected_create_send_image(&state, &image.expectation(), fence)?;
    assert_eq!(
        format!("{view:?}"),
        format!(
            "CheckedProtectedCreateSendImage {{ rows: {}, artifact_bytes: {}, metadata_bytes: {}, fence_bytes: {}, .. }}",
            view.image().row_count(),
            image.artifact.len(),
            metadata.len(),
            fence.len()
        )
    );
    for (error, display) in [
        (
            Error::LimitExceeded,
            "protected image agreement exceeds its logical limits",
        ),
        (
            Error::Allocation,
            "protected image validation could not be allocated",
        ),
        (
            Error::InvalidExpectation,
            "protected image expectations are invalid",
        ),
        (
            Error::NotInitialized,
            "protected capture is not initialized",
        ),
        (Error::IncompleteState, "protected capture is incomplete"),
        (
            Error::UnsupportedProfile,
            "protected image profile is unsupported",
        ),
        (Error::InvalidImage, "protected image is invalid"),
        (
            Error::IdentityMismatch,
            "protected image does not match its expected identity",
        ),
        (
            Error::FenceMismatch,
            "protected capture does not match its expected opaque fence",
        ),
        (
            Error::BusinessMismatch,
            "protected business rows do not match the validated image",
        ),
    ] {
        let copied = error;
        assert_eq!(copied, error);
        assert_eq!(error.to_string(), display);
        assert!(std::error::Error::source(&error).is_none());
    }
    // Static defensive diagnostics do not imply a publicly constructible half-capture.
    Ok(())
}
