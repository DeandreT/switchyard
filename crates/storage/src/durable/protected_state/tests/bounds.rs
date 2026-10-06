use super::*;

#[test]
fn publication_mutation_and_logical_byte_caps_use_checked_arithmetic() {
    assert_eq!(
        publication::mutation_budget(65_536, 67_108_864, 65_536, 134_226_176),
        Ok((131_076, 201_335_108))
    );
    assert_eq!(publication::mutation_budget(0, 0, 0, 0), Ok((4, 68)));
    for (rows, keys, offered_rows, bytes) in [
        (65_537, 67_108_864, 65_536, 134_226_176),
        (65_536, 67_108_865, 65_536, 134_226_176),
        (usize::MAX, 0, 1, 0),
        (0, usize::MAX, 0, 1),
        (1, 0, usize::MAX, 0),
        (0, 1, 0, usize::MAX),
    ] {
        assert_eq!(
            publication::mutation_budget(rows, keys, offered_rows, bytes),
            Err(ProtectedStateError::LimitExceeded)
        );
    }
}

#[test]
fn wrapper_open_errors_and_owned_dto_debug_are_static_numeric() -> TestResult {
    use std::error::Error;
    let (parent, path, mut writer) = fresh()?;
    publish(
        &mut writer,
        &[(b"secret-key", b"secret-value")],
        b"secret-meta",
        b"secret-artifact",
        b"secret-fence",
    )?;
    let reader = writer.reader();
    let view = reader.capture_protected_state()?;
    for debug in [
        format!("{writer:?}"),
        format!("{reader:?}"),
        format!("{view:?}"),
    ] {
        assert!(!debug.contains("secret"));
        assert!(!debug.contains(&path.to_string_lossy().to_string()));
    }
    for error in [
        FjallProtectedStateOpenError::UnsupportedPlatform,
        FjallProtectedStateOpenError::DirectoryUnavailable,
        FjallProtectedStateOpenError::Backend,
        FjallProtectedStateOpenError::InvalidLayout,
        FjallProtectedStateOpenError::LimitExceeded,
        FjallProtectedStateOpenError::StampUnknown,
    ] {
        assert!(error.source().is_none());
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(!format!("{error:?} {error}").contains(&path.to_string_lossy().to_string()));
    }
    assert!(ProtectedStateError::PublishUnknown.source().is_none());
    assert_eq!(view.records().entries()[0].0, b"secret-key");
    assert_eq!(
        view.live_catalog().ok_or("missing catalog")?.artifact(),
        b"secret-artifact"
    );
    assert_eq!(view.fence(), Some(&b"secret-fence"[..]));
    drop(reader);
    drop(writer);
    drop(parent);
    Ok(())
}
