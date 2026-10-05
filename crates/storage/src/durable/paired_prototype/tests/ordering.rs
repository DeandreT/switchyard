use super::*;

const STEPS: [CreationStep; 3] = [
    CreationStep::LogPrepared,
    CreationStep::StateReady,
    CreationStep::LogReady,
];

#[test]
fn every_creation_unknown_step_poisons_both_without_manufacturing_result_via_reads() -> TestResult {
    for native_backend in [false, true] {
        for (index, step) in STEPS.into_iter().enumerate() {
            for fault in [CommitFault::BeforeBackendErr, CommitFault::AfterSyncErr] {
                let (_parent, locations, mut pair) = pair_case(native_backend)?;
                assert_eq!(pair.create(Some((step, fault))), Err(Error::CommitUnknown));
                let entered = pair.counts.entered;
                let backend = pair.counts.backend;
                let captures = pair.counts.captures;
                assert_eq!(entered[index], 1);
                assert!(entered[index + 1..].iter().all(|n| *n == 0));
                assert_eq!(
                    backend[index],
                    usize::from(fault == CommitFault::AfterSyncErr)
                );
                assert_eq!(pair.capture().err(), Some(Error::Poisoned));
                assert_eq!(pair.create(None), Err(Error::Poisoned));
                assert_eq!(
                    pair.composite(&composite_parts(), CommitFault::None),
                    Err(Error::Poisoned)
                );
                assert_eq!(pair.counts.entered, entered);
                assert_eq!(pair.counts.backend, backend);
                assert_eq!(pair.counts.captures, captures);
                match &mut pair.backend {
                    BackendPair::Memory { state, log } => {
                        assert_eq!(state.capsule.capture().err(), Some(Error::Poisoned));
                        assert_eq!(log.capsule.capture().err(), Some(Error::Poisoned));
                        assert_eq!(
                            state
                                .capsule
                                .replace(super::super::inventory::RoleData::default()),
                            Err(Error::Poisoned)
                        );
                        assert_eq!(
                            log.capsule
                                .replace(super::super::inventory::RoleData::default()),
                            Err(Error::Poisoned)
                        );
                    }
                    BackendPair::Fjall { state, log } => {
                        assert_eq!(state.capsule.capture().err(), Some(Error::Poisoned));
                        assert_eq!(log.capsule.capture().err(), Some(Error::Poisoned));
                        let candidate = super::super::inventory::RoleData::default();
                        for capsule in [&mut state.capsule, &mut log.capsule] {
                            assert_eq!(capsule.seed(&candidate), Err(Error::Poisoned));
                            assert_eq!(capsule.ready(&candidate), Err(Error::Poisoned));
                            assert_eq!(capsule.composite(&candidate, &[]), Err(Error::Poisoned));
                        }
                    }
                }
                drop(pair);
                if let Some(locations) = locations {
                    assert_prefix(
                        &twice(&locations)?,
                        index + usize::from(fault == CommitFault::AfterSyncErr),
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn composite_unknown_before_and_after_sync_poison_both_without_postquery_or_retry() -> TestResult {
    for native_backend in [false, true] {
        for fault in [CommitFault::BeforeBackendErr, CommitFault::AfterSyncErr] {
            let (_parent, locations, mut pair) = pair_case(native_backend)?;
            pair.create(None)?;
            let old = pair.capture()?;
            assert_eq!(
                pair.composite(&composite_parts(), fault),
                Err(Error::CommitUnknown)
            );
            let counts = (
                pair.counts.entered,
                pair.counts.backend,
                pair.counts.captures,
            );
            assert_eq!(counts.0, [1, 1, 1, 1]);
            assert_eq!(counts.1[3], usize::from(fault == CommitFault::AfterSyncErr));
            assert_eq!(pair.capture().err(), Some(Error::Poisoned));
            assert_eq!(
                pair.composite(&composite_parts(), CommitFault::None),
                Err(Error::Poisoned)
            );
            assert_eq!(
                (
                    pair.counts.entered,
                    pair.counts.backend,
                    pair.counts.captures
                ),
                counts
            );
            drop(pair);
            if let Some(locations) = locations {
                let value = twice(&locations)?;
                assert_eq!(value.log, old.log);
                if fault == CommitFault::AfterSyncErr {
                    assert_selected(&value);
                } else {
                    assert_eq!(value, old);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn preflight_refusal_has_no_native_application_commit_and_operations_are_one_shot() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let locations = ControlledLocations::reserve(parent.path())?;
    let oversized = [0; 257];
    let parts = SeedStateParts {
        fence: &oversized,
        ..state_parts()
    };
    assert!(matches!(
        PreparedFixturePair::fjall(&locations, binding(), &parts, &log_parts()),
        Err(PrepareFailure::Input(Error::Limit))
    ));
    assert!(std::fs::read_dir(&locations.paths[0])?.next().is_none());
    assert!(std::fs::read_dir(&locations.paths[1])?.next().is_none());
    let mut pair = PreparedFixturePair::fjall(&locations, binding(), &state_parts(), &log_parts())?;
    pair.create(None)?;
    assert_eq!(pair.create(None), Err(Error::Used));
    let counts = pair.counts.entered;
    assert_eq!(
        pair.composite(
            &CompositeFixtureParts {
                fence: b"fixture-ready",
                ..composite_parts()
            },
            CommitFault::None
        ),
        Err(Error::InvalidLogical)
    );
    assert_eq!(pair.counts.entered, counts);
    assert_eq!(
        pair.composite(&composite_parts(), CommitFault::None),
        Err(Error::Used)
    );
    drop(pair);
    assert_prefix(&twice(&locations)?, 3);
    Ok(())
}

#[test]
fn exclusive_locations_refuse_existing_directory_and_retain_raw_redacted_cause() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    std::fs::create_dir(parent.path().join("log"))?;
    let error = ControlledLocations::reserve(parent.path())
        .err()
        .ok_or("existing directory admitted")?;
    match &error.cause {
        AcquisitionCause::Io(cause) => assert_eq!(cause.kind(), std::io::ErrorKind::AlreadyExists),
        AcquisitionCause::Native(_) => return Err("wrong retained acquisition cause".into()),
    }
    assert!(error.paths.iter().all(|path| path.is_dir()));
    let text = format!("{error:?}: {error}");
    assert!(!text.contains(parent.path().to_string_lossy().as_ref()));
    Ok(())
}

#[test]
fn partial_native_acquisition_is_not_emptynative_or_a_repair_fixture() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let locations = ControlledLocations::reserve(parent.path())?;
    // A valid live native owner prevents the second acquisition. No native files are damaged.
    let held = ::fjall::Database::create_new(
        ::fjall::Database::builder(&locations.paths[1])
            .worker_threads(1)
            .into_config(),
    )?;
    let error = PreparedFixturePair::fjall(&locations, binding(), &state_parts(), &log_parts())
        .err()
        .ok_or("busy peer admitted")?;
    let PrepareFailure::Acquisition(error) = error else {
        return Err("wrong failure class".into());
    };
    if let AcquisitionCause::Native(cause) = &error.cause {
        assert!(!cause.to_string().is_empty());
    } else {
        return Err("missing raw native cause".into());
    }
    assert_eq!(error.paths, locations.paths);
    assert!(!format!("{error:?}: {error}").contains(parent.path().to_string_lossy().as_ref()));
    drop(held);
    // Deliberately no guaranteed paired-reopen call for this partial acquisition.
    Ok(())
}

#[test]
fn every_marked_creation_prefix_refuses_generic_openers_without_logical_writes() -> TestResult {
    for (committed, stop_before) in [
        (1, Some(CreationStep::StateReady)),
        (2, Some(CreationStep::LogReady)),
        (3, None),
    ] {
        for opener in 0..3 {
            let parent = tempfile::TempDir::new()?;
            let (locations, mut pair) = native(parent.path())?;
            if let Some(step) = stop_before {
                assert_eq!(
                    pair.create(Some((step, CommitFault::BeforeBackendErr))),
                    Err(Error::CommitUnknown)
                );
            } else {
                pair.create(None)?;
            }
            drop(pair);
            let before = twice(&locations)?;
            assert_prefix(&before, committed);
            for role in 0..2 {
                if role == 0 && committed < 2 {
                    continue;
                }
                let path = &locations.paths[role];
                let error = match opener {
                    0 => super::super::super::FjallStore::open(path).err(),
                    1 => super::super::super::FjallReplicaStore::open(path).err(),
                    _ => super::super::super::FjallCatalogReplicaStore::open(path).err(),
                }
                .ok_or("generic paired opener admitted")?;
                assert_eq!(
                    error,
                    crate::StorageError::CorruptMetadata {
                        detail: "paired replica metadata cannot be opened by a generic store"
                            .into()
                    }
                );
                assert_eq!(twice(&locations)?, before);
            }
        }
    }
    Ok(())
}

#[test]
fn swapped_expected_binding_is_inspection_error_without_logical_relabeling() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let (locations, mut pair) = native(parent.path())?;
    pair.create(None)?;
    let before = pair.capture()?;
    drop(pair);
    let foreign = PhysicalPairBinding::new([[4; 16], [5; 16], [6; 16]], 7, [7; 16], [9; 32])?;
    assert_eq!(
        reopen_controlled_fixture(&locations, foreign).err(),
        Some(Error::InvalidLogical)
    );
    let swapped = ControlledLocations {
        paths: [locations.paths[1].clone(), locations.paths[0].clone()],
    };
    assert_eq!(
        reopen_controlled_fixture(&swapped, binding()).err(),
        Some(Error::InvalidLogical)
    );
    assert_eq!(twice(&locations)?, before);
    Ok(())
}

#[test]
fn independent_empty_native_prefixes_allow_only_ordinary_generic_stamps_without_seed_business()
-> TestResult {
    for opener in 0..3 {
        for target in 0..2 {
            let parent = tempfile::TempDir::new()?;
            let (locations, mut pair) = native(parent.path())?;
            // BOTH databases and EACH meta/records acquisition succeeded, but no seed commit entered.
            let empty = pair.capture()?;
            assert_prefix(&empty, 0);
            assert_eq!(pair.counts.entered, [0; 4]);
            drop(pair);
            let path = &locations.paths[target];
            let (format, profile) = match opener {
                0 => {
                    drop(super::super::super::FjallStore::open(path)?);
                    (super::super::super::ACTIVE_STORE_FORMAT, None)
                }
                1 => {
                    drop(super::super::super::FjallReplicaStore::open(path)?);
                    (
                        super::super::super::ACTIVE_REPLICA_STORE_FORMAT,
                        Some(b"committed-state-v1".as_slice()),
                    )
                }
                _ => {
                    drop(super::super::super::FjallCatalogReplicaStore::open(path)?);
                    (
                        super::super::super::ACTIVE_CATALOG_REPLICA_STORE_FORMAT,
                        Some(b"committed-state-catalog-v1".as_slice()),
                    )
                }
            };
            let mut expected = vec![(
                super::super::super::FORMAT_VERSION_KEY.to_vec(),
                format.to_be_bytes().to_vec(),
            )];
            if let Some(profile) = profile {
                expected.push((
                    super::super::control::PROFILE_KEY.to_vec(),
                    profile.to_vec(),
                ));
                expected.push((super::super::control::INIT_KEY.to_vec(), vec![0]));
            }
            expected.sort_by(|left, right| left.0.cmp(&right.0));
            // Raw ordinary-profile inspection only: never paired reopen, resume or mutation authority.
            let ordinary = physical_capture_without_admission(path)?;
            assert_eq!(ordinary.0, expected);
            assert!(ordinary.1.is_empty());
            assert_eq!(physical_capture_without_admission(path)?, ordinary);
            let peer = physical_capture_without_admission(&locations.paths[1 - target])?;
            assert!(peer.0.is_empty() && peer.1.is_empty());
            assert_eq!(
                physical_capture_without_admission(&locations.paths[1 - target])?,
                peer
            );
            assert_prefix(&empty, 0);
        }
    }
    Ok(())
}
