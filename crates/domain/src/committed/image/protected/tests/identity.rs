use storage::{MemoryProtectedStateStore, ProtectedStateReader};

use crate::{
    CommittedStreamId, CreateSendImageExpectation, Timestamp, check_protected_create_send_image,
};

use super::super::ProtectedCreateSendImageError as Error;
use super::fixture::{Image, TestResult, digest, publish};

#[test]
fn every_full_checkpoint_component_is_compared() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"fence")?;
    for part in 0..19 {
        let mut checkpoint = image.checkpoint.clone();
        match part {
            0 => checkpoint.last.as_mut().ok_or("last")?.id.term += 1,
            1 => checkpoint.last.as_mut().ok_or("last")?.id.node_id += 1,
            2 => checkpoint.last.as_mut().ok_or("last")?.id.index += 1,
            3 => checkpoint.last.as_mut().ok_or("last")?.fingerprint[0] ^= 1,
            4 => checkpoint.previous.as_mut().ok_or("previous")?.id.term += 1,
            5 => checkpoint.previous.as_mut().ok_or("previous")?.id.node_id += 1,
            6 => checkpoint.previous.as_mut().ok_or("previous")?.id.index += 1,
            7 => checkpoint.previous.as_mut().ok_or("previous")?.fingerprint[0] ^= 1,
            8 => checkpoint.highest_timestamp = Timestamp::from_millis(100),
            9 => checkpoint.membership.as_mut().ok_or("member")?.source.term += 1,
            10 => {
                checkpoint
                    .membership
                    .as_mut()
                    .ok_or("member")?
                    .source
                    .node_id += 1
            }
            11 => checkpoint.membership.as_mut().ok_or("member")?.source.index += 1,
            12 => {
                checkpoint
                    .membership
                    .as_mut()
                    .ok_or("member")?
                    .schema_version += 1
            }
            13 => checkpoint
                .membership
                .as_mut()
                .ok_or("member")?
                .payload
                .push(0),
            14 => checkpoint.last = None,
            15 => checkpoint.previous = None,
            16 => checkpoint.membership = None,
            17 => {
                checkpoint
                    .membership
                    .as_mut()
                    .ok_or("member")?
                    .schema_version = 0
            }
            _ => checkpoint.stream = CommittedStreamId::new([42; 16])?,
        }
        let expected = CreateSendImageExpectation {
            checkpoint: &checkpoint,
            ..image.expectation()
        };
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"different-fence").err(),
            Some(Error::IdentityMismatch),
            "checkpoint component {part}"
        );
    }
    Ok(())
}

#[test]
fn complete_artifact_length_is_compared() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"fence")?;
    for bytes in [0, image.artifact.len() - 1, image.artifact.len() + 1] {
        let expected = CreateSendImageExpectation {
            artifact_bytes: bytes,
            ..image.expectation()
        };
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"different-fence").err(),
            Some(Error::IdentityMismatch)
        );
    }
    Ok(())
}

#[test]
fn whole_artifact_digest_includes_checksum() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"fence")?;
    let prefix = digest(&image.artifact[..image.artifact.len() - 32]);
    assert_ne!(prefix, digest(&image.artifact));
    for hash in [prefix, {
        let mut changed = digest(&image.artifact);
        changed[0] ^= 1;
        changed
    }] {
        let expected = CreateSendImageExpectation {
            artifact_sha256: hash,
            ..image.expectation()
        };
        assert_eq!(
            check_protected_create_send_image(&state, &expected, b"different-fence").err(),
            Some(Error::IdentityMismatch)
        );
    }
    check_protected_create_send_image(&state, &image.expectation(), b"fence")?;
    Ok(())
}

#[test]
fn same_checkpoint_different_artifact_refuses_foreign_expectation() -> TestResult {
    let selected = Image::one()?;
    let other = selected.different_body()?;
    assert_eq!(selected.checkpoint, other.checkpoint);
    assert_eq!(selected.artifact.len(), other.artifact.len());
    assert_ne!(selected.artifact, other.artifact);
    let state = other.capture(b"metadata", b"fence")?;
    assert_eq!(
        check_protected_create_send_image(&state, &selected.expectation(), b"fence").err(),
        Some(Error::IdentityMismatch)
    );
    check_protected_create_send_image(&state, &other.expectation(), b"fence")?;
    Ok(())
}

#[test]
fn different_opaque_fence_refuses() -> TestResult {
    let image = Image::one()?;
    let state = image.capture(b"metadata", b"phase-is-opaque\0one")?;
    assert_eq!(
        check_protected_create_send_image(&state, &image.expectation(), b"phase-is-opaque\0two")
            .err(),
        Some(Error::FenceMismatch)
    );
    check_protected_create_send_image(&state, &image.expectation(), b"phase-is-opaque\0one")?;
    Ok(())
}

#[test]
fn older_catalog_and_newer_business_is_known_mismatch() -> TestResult {
    let old = Image::one()?;
    let new = Image::populated(&["first", "second"], b"new-business")?;
    let mut writer = MemoryProtectedStateStore::new();
    publish(
        &mut writer,
        &new.rows()?,
        &old.artifact,
        b"older-opaque-catalog",
        b"fence-one",
    )?;
    let reader = writer.reader();
    let state = reader.capture_protected_state()?;
    assert_eq!(
        check_protected_create_send_image(&state, &old.expectation(), b"fence-one").err(),
        Some(Error::BusinessMismatch)
    );
    let unchanged = reader.capture_protected_state()?;
    assert_eq!(state.records().entries(), unchanged.records().entries());
    assert_eq!(state.fence(), unchanged.fence());
    assert_eq!(
        state.live_catalog().ok_or("catalog")?.artifact(),
        unchanged.live_catalog().ok_or("catalog")?.artifact()
    );
    assert_eq!(
        state.live_catalog().ok_or("catalog")?.metadata(),
        unchanged.live_catalog().ok_or("catalog")?.metadata()
    );
    publish(
        &mut writer,
        &new.rows()?,
        &new.artifact,
        b"new-opaque-catalog",
        b"fence-two",
    )?;
    let current = reader.capture_protected_state()?;
    check_protected_create_send_image(&current, &new.expectation(), b"fence-two")?;
    Ok(())
}
