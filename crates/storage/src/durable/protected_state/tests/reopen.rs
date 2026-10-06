use super::*;
use crate::{FjallCatalogReplicaStore, FjallReplicaStore, FjallStore};

#[test]
fn fresh_creation_refuses_existing_paths_without_application_writes() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let file = parent.path().join("file");
    std::fs::write(&file, b"caller-file")?;
    assert_eq!(
        FjallProtectedStateStore::create_new(&file).err(),
        Some(FjallProtectedStateOpenError::DirectoryUnavailable)
    );
    assert_eq!(std::fs::read(&file)?, b"caller-file");
    let empty = parent.path().join("empty");
    std::fs::create_dir(&empty)?;
    assert_eq!(
        FjallProtectedStateStore::create_new(&empty).err(),
        Some(FjallProtectedStateOpenError::DirectoryUnavailable)
    );
    assert_eq!(std::fs::read_dir(&empty)?.count(), 0);
    for kind in 0..4 {
        let path = parent.path().join(format!("existing-{kind}"));
        match kind {
            0 => drop(FjallStore::open(&path)?),
            1 => drop(FjallReplicaStore::open(&path)?),
            2 => drop(FjallCatalogReplicaStore::open(&path)?),
            _ => drop(FjallProtectedStateStore::create_new(&path)?),
        }
        let before = observe(&path)?;
        assert_eq!(
            FjallProtectedStateStore::create_new(&path).err(),
            Some(FjallProtectedStateOpenError::DirectoryUnavailable)
        );
        let after = observe(&path)?;
        assert_eq!(before, after);
    }
    for fixed_file in [true, false] {
        let path = parent.path().join(if fixed_file {
            "fixed-file-refusal"
        } else {
            "child-directory-refusal"
        });
        let (result, events) = acquisition::observed_create(&path, fixed_file, !fixed_file);
        assert_eq!(
            result.err(),
            Some(FjallProtectedStateOpenError::DirectoryUnavailable)
        );
        assert!(path.is_dir());
        assert!(!events.contains(&16));
        assert!(!events.contains(&17));
        if fixed_file {
            assert_eq!(events, [0, 1]);
            assert_eq!(std::fs::metadata(path.join("lock"))?.len(), 0);
            assert!(!path.join("keyspaces").exists());
        } else {
            assert_eq!(events, [0, 1, 2, 3, 4, 5, 6]);
            let raw = observe(&path)?;
            assert_eq!(raw.meta, Some(Vec::new()));
            assert_eq!(raw.records, Some(Vec::new()));
        }
    }
    drop(parent);
    Ok(())
}

#[test]
fn active_reader_prevents_second_selected_and_generic_owner() -> TestResult {
    let (parent, path, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    let reader = writer.reader();
    let clone = reader.clone();
    assert!(FjallProtectedStateStore::open_existing(&path).is_err());
    assert!(FjallStore::open(&path).is_err());
    assert!(FjallReplicaStore::open(&path).is_err());
    assert!(FjallCatalogReplicaStore::open(&path).is_err());
    token(&mut writer, 2)?;
    assert_token(&clone.capture_protected_state()?, 2);
    drop(writer);
    drop(reader);
    assert!(FjallProtectedStateStore::open_existing(&path).is_err());
    assert_token(&clone.capture_protected_state()?, 2);
    drop(clone);
    let (one, two) = twice(&path)?;
    assert_token(&one, 2);
    assert_token(&two, 2);
    drop(parent);
    Ok(())
}

#[test]
fn every_generic_opener_refuses_pristine_and_initialized_profile() -> TestResult {
    for initialized in [false, true] {
        let (parent, path, mut writer) = fresh()?;
        if initialized {
            token(&mut writer, 1)?;
        }
        drop(writer);
        let before = observe(&path)?;
        assert!(FjallStore::open(&path).is_err());
        assert_eq!(observe(&path)?, before);
        assert!(FjallReplicaStore::open(&path).is_err());
        assert_eq!(observe(&path)?, before);
        assert!(FjallCatalogReplicaStore::open(&path).is_err());
        assert_eq!(observe(&path)?, before);
        drop(parent);
    }
    Ok(())
}

#[test]
fn selected_reopen_refuses_other_versions_profiles_and_closed_shapes_without_stamp() -> TestResult {
    for selected in [
        Damage::WrongFormat,
        Damage::WrongProfile,
        Damage::BadInit,
        Damage::Unknown,
        Damage::PartialMetadata,
        Damage::PartialArtifact,
        Damage::PartialFence,
        Damage::Orphan,
    ] {
        let (parent, path, mut writer) = fresh()?;
        token(&mut writer, 1)?;
        damage(&writer, selected)?;
        drop(writer);
        let before = observe(&path)?;
        assert_eq!(
            FjallProtectedStateStore::open_existing(&path).err(),
            Some(FjallProtectedStateOpenError::InvalidLayout)
        );
        assert_eq!(observe(&path)?, before);
        drop(parent);
    }
    for version in [
        0,
        ACTIVE_PROTECTED_STATE_STORE_FORMAT + 1,
        super::super::super::ACTIVE_REPLICA_STORE_FORMAT,
        super::super::super::ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
    ] {
        let (parent, path, writer) = fresh()?;
        put_meta(&writer, KEYS[0], &version.to_be_bytes())?;
        drop(writer);
        let before = observe(&path)?;
        assert_eq!(
            FjallProtectedStateStore::open_existing(&path).err(),
            Some(FjallProtectedStateOpenError::InvalidLayout)
        );
        assert_eq!(observe(&path)?, before);
        drop(parent);
    }
    for (key, value) in [
        (KEYS[0], &b"bad"[..]),
        (INITIALIZED_KEY, &b""[..]),
        (INITIALIZED_KEY, &b"00"[..]),
        (PROFILE_KEY, &b"committed-state-v1"[..]),
    ] {
        let (parent, path, writer) = fresh()?;
        put_meta(&writer, key, value)?;
        drop(writer);
        let before = observe(&path)?;
        assert_eq!(
            FjallProtectedStateStore::open_existing(&path).err(),
            Some(FjallProtectedStateOpenError::InvalidLayout)
        );
        assert_eq!(observe(&path)?, before);
        drop(parent);
    }
    for key in [
        &b"foreign-key"[..],
        &[0x22, 0x01][..],
        &[0x22, 0x02][..],
        &[0x22, 0x03][..],
    ] {
        let (parent, path, writer) = fresh()?;
        put_meta(&writer, key, b"foreign-marker")?;
        drop(writer);
        let before = observe(&path)?;
        assert_eq!(
            FjallProtectedStateStore::open_existing(&path).err(),
            Some(FjallProtectedStateOpenError::InvalidLayout)
        );
        assert_eq!(observe(&path)?, before);
        drop(parent);
    }
    Ok(())
}

#[test]
fn missing_application_keyspace_not_recreated() -> TestResult {
    for records in [false, true] {
        let (parent, path, writer) = fresh()?;
        let removed = if records {
            writer.inner.records.clone()
        } else {
            writer.inner.meta.clone()
        };
        writer.inner.database.delete_keyspace(removed)?;
        drop(writer);
        let before = observe(&path)?;
        assert_eq!(before.names.len(), 1);
        assert_eq!(
            FjallProtectedStateStore::open_existing(&path).err(),
            Some(FjallProtectedStateOpenError::InvalidLayout)
        );
        let after = observe(&path)?;
        assert_eq!(after, before);
        assert_eq!(after.records.is_none(), records);
        assert_eq!(after.meta.is_none(), !records);
        drop(parent);
    }
    Ok(())
}

#[test]
fn successful_complete_publication_reopens_twice_exactly() -> TestResult {
    let (parent, path, mut writer) = fresh()?;
    token(&mut writer, 1)?;
    publish(
        &mut writer,
        &[(b"a", b"new"), (b"b", b"replacement")],
        b"meta-two",
        b"artifact-two",
        b"fence-two",
    )?;
    let reader = writer.reader();
    let expected = reader.capture_protected_state()?;
    drop(reader);
    drop(writer);
    let (one, two) = twice(&path)?;
    assert_eq!(logical(&one), logical(&expected));
    assert_eq!(logical(&two), logical(&expected));
    drop(parent);
    Ok(())
}
