use storage::{
    FjallProtectedStateStore, ProtectedStatePublication, ProtectedStateReader,
    SnapshotCatalogRecord,
};

use crate::check_protected_create_send_image;

use super::super::ProtectedCreateSendImageError as Error;
use super::fixture::{Image, Rows, TestResult};

fn publish(
    writer: &mut FjallProtectedStateStore,
    rows: &Rows,
    artifact: &[u8],
    metadata: &[u8],
    fence: &[u8],
) -> TestResult {
    let borrowed: Vec<_> = rows
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    writer.publish(ProtectedStatePublication::new(
        &borrowed,
        SnapshotCatalogRecord::new(metadata, artifact)?,
        fence,
    )?)?;
    Ok(())
}

#[test]
fn fjall_capture_agrees_with_existing_borrowed_create_send_checker() -> TestResult {
    let parent = testkit::DurableProvider::temporary()?;
    let path = parent.path().join("protected");
    let image = Image::populated(&["first", "second"], b"durable-body")?;
    let mut writer = FjallProtectedStateStore::create_new(&path)?;
    publish(
        &mut writer,
        &image.rows()?,
        &image.artifact,
        b"not-domain-metadata",
        b"durable-fence",
    )?;
    let reader = writer.reader();
    let state = reader.capture_protected_state()?;
    let checked =
        check_protected_create_send_image(&state, &image.expectation(), b"durable-fence")?;
    assert!(std::ptr::eq(checked.protected_state(), &state));
    assert_eq!(checked.image().checkpoint(), &image.checkpoint);
    assert_eq!(checked.image().queue_count(), 2);
    assert_eq!(checked.image().message_count(), 2);
    assert_eq!(
        state.live_catalog().ok_or("missing live pair")?.metadata(),
        b"not-domain-metadata"
    );
    for ((key, value), row) in state.records().entries().iter().zip(checked.image().rows()) {
        assert_eq!(key.as_slice(), row.key());
        assert_eq!(value.as_slice(), row.value());
    }
    drop(reader);
    drop(writer);
    assert_eq!(checked.image().message_count(), 2);
    drop(parent);
    Ok(())
}

#[test]
fn fjall_reopened_capture_and_stale_owned_view_remain_descriptive() -> TestResult {
    let parent = testkit::DurableProvider::temporary()?;
    let path = parent.path().join("protected");
    let old_image = Image::one()?;
    let new_image = old_image.different_body()?;
    let mut writer = FjallProtectedStateStore::create_new(&path)?;
    publish(
        &mut writer,
        &old_image.rows()?,
        &old_image.artifact,
        b"old-meta",
        b"old-fence",
    )?;
    let reader = writer.reader();
    let old_state = reader.capture_protected_state()?;
    let old_checked =
        check_protected_create_send_image(&old_state, &old_image.expectation(), b"old-fence")?;
    publish(
        &mut writer,
        &new_image.rows()?,
        &new_image.artifact,
        b"new-meta",
        b"new-fence",
    )?;
    let current = reader.capture_protected_state()?;
    let current_checked =
        check_protected_create_send_image(&current, &new_image.expectation(), b"new-fence")?;
    drop(reader);
    drop(writer);
    let reopened_state = {
        let reopened = FjallProtectedStateStore::open_existing(&path)?;
        let reader = reopened.reader();
        let state = reader.capture_protected_state()?;
        drop(reader);
        drop(reopened);
        state
    };
    let reopened_checked =
        check_protected_create_send_image(&reopened_state, &new_image.expectation(), b"new-fence")?;
    assert_eq!(current.records(), reopened_state.records());
    assert_eq!(
        current_checked.image().checkpoint(),
        reopened_checked.image().checkpoint()
    );
    assert_eq!(
        old_checked.protected_state().fence(),
        Some(&b"old-fence"[..])
    );
    assert_ne!(
        old_state.live_catalog().ok_or("old live")?.artifact(),
        reopened_state.live_catalog().ok_or("new live")?.artifact()
    );
    assert_eq!(old_checked.image().message_count(), 1);
    drop(parent);
    Ok(())
}

#[test]
fn fjall_old_catalog_business_mismatch_does_not_poison_publication() -> TestResult {
    let parent = testkit::DurableProvider::temporary()?;
    let path = parent.path().join("protected");
    let old_image = Image::one()?;
    let new_image = old_image.different_body()?;
    let mut writer = FjallProtectedStateStore::create_new(&path)?;
    publish(
        &mut writer,
        &new_image.rows()?,
        &old_image.artifact,
        b"opaque-old",
        b"mixed-fence",
    )?;
    let reader = writer.reader();
    let mixed = reader.capture_protected_state()?;
    assert_eq!(
        check_protected_create_send_image(&mixed, &old_image.expectation(), b"mixed-fence").err(),
        Some(Error::BusinessMismatch)
    );
    publish(
        &mut writer,
        &new_image.rows()?,
        &new_image.artifact,
        b"opaque-new",
        b"matching-fence",
    )?;
    let matching = reader.capture_protected_state()?;
    let checked =
        check_protected_create_send_image(&matching, &new_image.expectation(), b"matching-fence")?;
    assert_eq!(checked.image().message_count(), 1);
    drop(reader);
    drop(writer);
    let reopened = FjallProtectedStateStore::open_existing(&path)?;
    let persisted = reopened.reader().capture_protected_state()?;
    check_protected_create_send_image(&persisted, &new_image.expectation(), b"matching-fence")?;
    drop(reopened);
    drop(parent);
    Ok(())
}
