use super::*;
use ::fjall::{PersistMode, Readable};

pub(super) const OLD_ROWS: [BorrowedRow<'static>; 2] =
    [(b"business", b"old"), (b"obsolete", b"old")];
pub(super) const NEW_ROWS: [BorrowedRow<'static>; 2] =
    [(b"business", b"new"), (b"second", b"complete")];

pub(super) fn binding() -> PhysicalPairBinding {
    PhysicalPairBinding::new([[1; 16], [2; 16], [3; 16]], 7, [7; 16], [9; 32])
        .expect("fixed valid fixture binding")
}
pub(super) fn state_parts() -> SeedStateParts<'static> {
    SeedStateParts {
        header: b"opaque-state-header",
        fence: b"fixture-ready",
        business: &OLD_ROWS,
        live: Some((b"old-meta", b"old-image")),
    }
}
pub(super) fn log_parts() -> SeedLogParts<'static> {
    SeedLogParts {
        header: b"opaque-log-header",
        progress: b"opaque-progress",
        baseline: b"opaque-baseline",
        manifest: b"opaque-seed-not-a-digest-proof",
        entries: &[],
    }
}
pub(super) fn composite_parts() -> CompositeFixtureParts<'static> {
    CompositeFixtureParts {
        business: &NEW_ROWS,
        live: (b"new-meta", b"new-image"),
        fence: b"fixture-selected",
    }
}

pub(super) fn native(
    parent: &std::path::Path,
) -> TestResult<(ControlledLocations, PreparedFixturePair)> {
    let locations = ControlledLocations::reserve(parent)?;
    let pair = PreparedFixturePair::fjall(&locations, binding(), &state_parts(), &log_parts())?;
    Ok((locations, pair))
}

pub(super) fn pair_case(
    native_backend: bool,
) -> TestResult<(
    tempfile::TempDir,
    Option<ControlledLocations>,
    PreparedFixturePair,
)> {
    let parent = tempfile::TempDir::new()?;
    if native_backend {
        let (locations, pair) = native(parent.path())?;
        Ok((parent, Some(locations), pair))
    } else {
        let pair = PreparedFixturePair::memory(binding(), &state_parts(), &log_parts())?;
        Ok((parent, None, pair))
    }
}

pub(super) fn twice(locations: &ControlledLocations) -> TestResult<PairInventory> {
    // Each call closes BOTH physical roles; the first DTO survives the second open.
    let first = reopen_controlled_fixture(locations, binding())?;
    let second = reopen_controlled_fixture(locations, binding())?;
    assert_eq!(first, second);
    Ok(first)
}

pub(super) fn assert_prefix(value: &PairInventory, committed: usize) {
    let expected_state = if committed >= 2 {
        super::super::inventory::RoleData::seed_state(binding(), &state_parts())
            .expect("bounded expected state")
    } else {
        super::super::inventory::RoleData::default()
    };
    let expected_log = if committed >= 1 {
        super::super::inventory::RoleData::seed_log(
            binding(),
            &log_parts(),
            if committed >= 3 {
                CreationPhase::Ready
            } else {
                CreationPhase::Prepared
            },
        )
        .expect("bounded expected log")
    } else {
        super::super::inventory::RoleData::default()
    };
    assert_eq!(
        value.state,
        expected_state
            .capture(Role::State, binding())
            .expect("expected state dictionary")
    );
    assert_eq!(
        value.log,
        expected_log
            .capture(Role::Log, binding())
            .expect("expected log dictionary")
    );
    assert_eq!(
        value.state.status,
        if committed >= 2 {
            RoleStatus::Paired(CreationPhase::Ready)
        } else {
            RoleStatus::Empty
        }
    );
    assert_eq!(
        value.log.status,
        if committed >= 3 {
            RoleStatus::Paired(CreationPhase::Ready)
        } else if committed >= 1 {
            RoleStatus::Paired(CreationPhase::Prepared)
        } else {
            RoleStatus::Empty
        }
    );
    if committed < 2 {
        assert!(value.state.metadata.is_empty());
        assert!(value.state.records.is_empty());
    }
    if committed == 0 {
        assert!(value.log.metadata.is_empty());
        assert!(value.log.records.is_empty());
    }
    if committed >= 2 {
        assert_eq!(
            value.state.records,
            OLD_ROWS
                .iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect::<Rows>()
        );
        assert_eq!(
            find(&value.state.metadata, LOGICAL[1]),
            Some(b"fixture-ready".as_slice())
        );
        assert_eq!(
            find(&value.state.metadata, LOGICAL[5]),
            Some(b"old-meta".as_slice())
        );
        assert_eq!(
            find(&value.state.metadata, LOGICAL[6]),
            Some(b"old-image".as_slice())
        );
    }
}

pub(super) fn find<'a>(rows: &'a [(Vec<u8>, Vec<u8>)], key: &[u8]) -> Option<&'a [u8]> {
    rows.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_slice())
}

pub(super) fn assert_selected(value: &PairInventory) {
    let mut expected = super::super::inventory::RoleData::seed_state(binding(), &state_parts())
        .expect("bounded expected state");
    expected.records.clear();
    for &(key, value) in &NEW_ROWS {
        expected.records.insert(key.to_vec(), value.to_vec());
    }
    for (key, value) in [
        (LOGICAL[1], b"fixture-selected".as_slice()),
        (LOGICAL[5], b"new-meta".as_slice()),
        (LOGICAL[6], b"new-image".as_slice()),
    ] {
        expected.metadata.insert(key.to_vec(), value.to_vec());
    }
    assert_eq!(
        value.state,
        expected
            .capture(Role::State, binding())
            .expect("expected selected dictionary")
    );
    let log =
        super::super::inventory::RoleData::seed_log(binding(), &log_parts(), CreationPhase::Ready)
            .expect("bounded expected log");
    assert_eq!(
        value.log,
        log.capture(Role::Log, binding())
            .expect("expected unchanged log dictionary")
    );
    assert_eq!(
        value.state.records,
        NEW_ROWS
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Rows>()
    );
    assert_eq!(
        find(&value.state.metadata, LOGICAL[1]),
        Some(b"fixture-selected".as_slice())
    );
    assert_eq!(
        find(&value.state.metadata, LOGICAL[5]),
        Some(b"new-meta".as_slice())
    );
    assert_eq!(
        find(&value.state.metadata, LOGICAL[6]),
        Some(b"new-image".as_slice())
    );
}

pub(super) fn damage_state_meta(
    pair: &mut PreparedFixturePair,
    key: &[u8],
    value: Option<&[u8]>,
) -> TestResult {
    // Logical dictionary damage through normal backend operations, never native files.
    match &mut pair.backend {
        BackendPair::Memory { state, .. } => {
            let mut data = state
                .capsule
                .data
                .write()
                .map_err(|_| "fixture memory lock poisoned")?;
            if let Some(value) = value {
                data.metadata.insert(key.to_vec(), value.to_vec());
            } else {
                data.metadata.remove(key);
            }
        }
        BackendPair::Fjall { state, .. } => {
            let mut batch = state
                .capsule
                .database
                .batch()
                .durability(Some(PersistMode::SyncAll));
            if let Some(value) = value {
                batch.insert(&state.capsule.meta, key, value);
            } else {
                batch.remove(&state.capsule.meta, key);
            }
            batch.commit()?;
        }
    }
    Ok(())
}

pub(super) fn physical_capture_without_admission(
    path: &std::path::Path,
) -> TestResult<(Rows, Rows)> {
    let database = ::fjall::Database::recover(
        ::fjall::Database::builder(path)
            .worker_threads(1)
            .into_config(),
    )?;
    assert!(database.keyspace_exists("meta") && database.keyspace_exists("records"));
    let meta = database.keyspace("meta", ::fjall::KeyspaceCreateOptions::default)?;
    let records = database.keyspace("records", ::fjall::KeyspaceCreateOptions::default)?;
    let snapshot = database.snapshot();
    let read = |space: &::fjall::Keyspace| -> TestResult<Rows> {
        snapshot
            .iter(space)
            .map(|guard| {
                let (key, value) = guard.into_inner()?;
                Ok((key.to_vec(), value.to_vec()))
            })
            .collect()
    };
    let result = (read(&meta)?, read(&records)?);
    drop(snapshot);
    drop(records);
    drop(meta);
    drop(database);
    Ok(result)
}
