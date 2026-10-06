use super::*;

#[test]
fn protected_fjall_pristine_profile_captures_closed_empty_state() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let path = parent.path().join("selected");
    let (result, events) = acquisition::observed_create(&path, false, false);
    let writer = result?;
    assert_eq!(events, (0..18).collect::<Vec<_>>());
    let raw = dictionary(&writer.inner.database)?;
    assert_eq!(raw.names, ["meta", "records"]);
    assert_eq!(raw.records, Some(Vec::new()));
    let mut expected = vec![
        (
            KEYS[0].to_vec(),
            ACTIVE_PROTECTED_STATE_STORE_FORMAT.to_be_bytes().to_vec(),
        ),
        (PROFILE_KEY.to_vec(), PROFILE.to_vec()),
        (INITIALIZED_KEY.to_vec(), vec![0]),
    ];
    expected.sort();
    assert_eq!(raw.meta, Some(expected));
    for id in ["0", "1", "2"] {
        assert!(path.join("keyspaces").join(id).join("tables").is_dir());
    }
    assert_eq!(std::fs::metadata(path.join("lock"))?.len(), 0);
    let view = writer.reader().capture_protected_state()?;
    assert_eq!(
        logical(&view),
        Logical {
            initialized: false,
            records: Vec::new(),
            live: None,
            fence: None,
            bytes: 0
        }
    );
    drop(writer);
    let (one, two) = twice(&path)?;
    assert_eq!(logical(&one), logical(&view));
    assert_eq!(logical(&two), logical(&view));
    drop(parent);
    Ok(())
}

#[test]
fn empty_publication_initializes_present_empty_catalog_and_fence() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    publish(&mut writer, &[], b"", b"", b"f")?;
    let view = writer.reader().capture_protected_state()?;
    assert_eq!(
        logical(&view),
        Logical {
            initialized: true,
            records: Vec::new(),
            live: Some((Vec::new(), Vec::new())),
            fence: Some(b"f".to_vec()),
            bytes: 1
        }
    );
    assert_eq!(counts(&writer).native, 1);
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn complete_replacement_removes_old_rows_and_replaces_all_components() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    publish(
        &mut writer,
        &[(b"a", b"old-only"), (b"b", b"old")],
        b"old-meta",
        b"old-artifact",
        b"old-fence",
    )?;
    publish(
        &mut writer,
        &[(b"b", b"new"), (b"c", b"added")],
        b"new-meta",
        b"new-artifact",
        b"new-fence",
    )?;
    let view = writer.reader().capture_protected_state()?;
    assert_eq!(
        logical(&view),
        Logical {
            initialized: true,
            records: vec![
                (b"b".to_vec(), b"new".to_vec()),
                (b"c".to_vec(), b"added".to_vec())
            ],
            live: Some((b"new-meta".to_vec(), b"new-artifact".to_vec())),
            fence: Some(b"new-fence".to_vec()),
            bytes: 39
        }
    );
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn noop_publication_requires_distinct_fence_and_allows_later_old_bytes() -> TestResult {
    let (parent, _, mut writer) = fresh()?;
    publish(&mut writer, &[(b"k", b"v")], b"m", b"a", b"one")?;
    let first = writer.reader().capture_protected_state()?;
    publish(&mut writer, &[(b"k", b"v")], b"m", b"a", b"two")?;
    publish(&mut writer, &[(b"k", b"v")], b"m", b"a", b"one")?;
    let final_view = writer.reader().capture_protected_state()?;
    assert_eq!(logical(&first), logical(&final_view));
    assert_eq!(counts(&writer).entries, 3);
    assert_eq!(counts(&writer).native, 3);
    drop(writer);
    drop(parent);
    Ok(())
}

#[test]
fn cloned_readers_share_origin_and_keep_directory_locked() -> TestResult {
    let (parent, path, writer) = fresh()?;
    let reader = writer.reader();
    let clone = reader.clone();
    assert!(Arc::ptr_eq(&writer.inner, &reader.inner));
    assert!(Arc::ptr_eq(&reader.inner, &clone.inner));
    drop(writer);
    drop(reader);
    assert!(FjallProtectedStateStore::open_existing(&path).is_err());
    assert!(!clone.capture_protected_state()?.is_initialized());
    drop(clone);
    let reopened = FjallProtectedStateStore::open_existing(&path)?;
    drop(reopened);
    drop(parent);
    Ok(())
}

#[test]
fn captures_outlive_every_native_handle_without_retaining_lock() -> TestResult {
    let (parent, path, mut writer) = fresh()?;
    token(&mut writer, 7)?;
    let reader = writer.reader();
    let clone = reader.clone();
    let old = clone.capture_protected_state()?;
    drop(clone);
    drop(reader);
    drop(writer);
    let (one, two) = twice(&path)?;
    assert_token(&old, 7);
    assert_eq!(logical(&one), logical(&old));
    assert_eq!(logical(&two), logical(&old));
    drop(parent);
    Ok(())
}

#[test]
fn finite_concurrent_captures_are_wholly_old_or_new() -> TestResult {
    let (parent, path, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let reader = writer.reader();
    let old_snapshot = reader.inner.database.snapshot();
    let pinned = reader.inner.gate.read().map_err(|_| "test read gate")?;
    let old = view::measure(&reader.inner, &old_snapshot)?;
    let mut observations = Vec::with_capacity(24);
    let original = std::thread::spawn(move || -> Result<(), ProtectedStateError> {
        for value in 2..=9 {
            token(&mut writer, value)?;
        }
        Ok(())
    });
    let observation_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Release helper-held resources before capture; unwinding also releases them.
        drop(old);
        drop(old_snapshot);
        drop(pinned);
        for _ in 0..24 {
            observations.push(reader.capture_protected_state());
        }
    }));
    let outcome = original.join();
    // Original task outcome and every observation remain owned at this barrier.
    if let Err(panic) = observation_outcome {
        std::panic::resume_unwind(panic);
    }
    let write_result = match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    write_result?;
    for observation in observations {
        let value = observation?;
        let token = value
            .fence()
            .and_then(|f| f.first())
            .copied()
            .ok_or("missing token")?;
        assert!((1..=9).contains(&token));
        assert_token(&value, token);
    }
    let final_view = reader.capture_protected_state()?;
    assert_token(&final_view, 9);
    drop(reader);
    let (one, two) = twice(&path)?;
    assert_token(&one, 9);
    assert_token(&two, 9);
    drop(parent);
    Ok(())
}
