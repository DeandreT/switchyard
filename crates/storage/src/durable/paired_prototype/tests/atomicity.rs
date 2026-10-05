use super::*;

#[test]
fn both_backends_seed_then_replace_whole_business_live_and_fence_atomically() -> TestResult {
    for native_backend in [false, true] {
        let (_parent, locations, mut pair) = pair_case(native_backend)?;
        let pristine = pair.capture()?;
        assert_prefix(&pristine, 0);
        pair.create(None)?;
        let old = pair.capture()?;
        assert_prefix(&old, 3);
        pair.composite(&composite_parts(), CommitFault::None)?;
        let selected = pair.capture()?;
        assert_selected(&selected);
        assert_eq!(old.log, selected.log);
        for (key, value) in &old.state.metadata {
            if ![LOGICAL[1], LOGICAL[5], LOGICAL[6]].contains(&key.as_slice()) {
                assert_eq!(find(&selected.state.metadata, key), Some(value.as_slice()));
            }
        }
        drop(pair);
        if let Some(locations) = locations {
            assert_eq!(twice(&locations)?, selected);
        }
    }
    Ok(())
}

#[test]
fn native_pinned_empty_old_and_new_views_never_mix_cross_keyspace_parts() -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let (locations, mut pair) = native(parent.path())?;
    let empty = match &pair.backend {
        BackendPair::Fjall { state, .. } => state.capsule.database.snapshot(),
        _ => unreachable!(),
    };
    pair.create(None)?;
    let old = match &pair.backend {
        BackendPair::Fjall { state, .. } => state.capsule.database.snapshot(),
        _ => unreachable!(),
    };
    pair.composite(&composite_parts(), CommitFault::None)?;
    if let BackendPair::Fjall { state, .. } = &pair.backend {
        let pristine = state.capsule.capture_view(&empty)?;
        assert_eq!(pristine.status, RoleStatus::Empty);
        assert!(pristine.records.is_empty());
        assert!(pristine.metadata.is_empty());
        let retained = state.capsule.capture_view(&old)?;
        assert_eq!(
            find(&retained.records, b"business"),
            Some(b"old".as_slice())
        );
        assert_eq!(
            find(&retained.metadata, LOGICAL[5]),
            Some(b"old-meta".as_slice())
        );
        assert_eq!(
            find(&retained.metadata, LOGICAL[6]),
            Some(b"old-image".as_slice())
        );
        assert_eq!(
            find(&retained.metadata, LOGICAL[1]),
            Some(b"fixture-ready".as_slice())
        );
        assert_eq!(
            find(&retained.metadata, super::super::control::INIT_KEY),
            Some([1].as_slice())
        );
    }
    let selected = pair.capture()?;
    assert_selected(&selected);
    drop(old);
    drop(empty);
    drop(pair);
    assert_eq!(twice(&locations)?, selected);
    Ok(())
}

#[test]
fn partial_staging_is_preserved_as_bounded_fact_not_live_pair_or_repair() -> TestResult {
    for native_backend in [false, true] {
        for staged in [LOGICAL[2], LOGICAL[3], LOGICAL[4]] {
            let (_parent, locations, mut pair) = pair_case(native_backend)?;
            pair.create(None)?;
            damage_state_meta(&mut pair, staged, Some(b"partial-stage"))?;
            let old = pair.capture()?;
            pair.composite(&composite_parts(), CommitFault::None)?;
            let selected = pair.capture()?;
            assert_eq!(
                find(&selected.state.metadata, staged),
                Some(b"partial-stage".as_slice())
            );
            assert_eq!(old.log, selected.log);
            drop(pair);
            if let Some(locations) = locations {
                assert_eq!(twice(&locations)?, selected);
            }
        }
    }
    Ok(())
}

#[test]
fn both_absent_live_seed_and_present_empty_components_remain_distinct() -> TestResult {
    for live in [None, Some((b"".as_slice(), b"".as_slice()))] {
        for native_backend in [false, true] {
            let parent = tempfile::TempDir::new()?;
            let locations = if native_backend {
                Some(ControlledLocations::reserve(parent.path())?)
            } else {
                None
            };
            let parts = SeedStateParts {
                live,
                ..state_parts()
            };
            let mut pair = if let Some(locations) = &locations {
                PreparedFixturePair::fjall(locations, binding(), &parts, &log_parts())?
            } else {
                PreparedFixturePair::memory(binding(), &parts, &log_parts())?
            };
            pair.create(None)?;
            let old = pair.capture()?;
            assert_eq!(find(&old.state.metadata, LOGICAL[5]), live.map(|v| v.0));
            assert_eq!(find(&old.state.metadata, LOGICAL[6]), live.map(|v| v.1));
            drop(pair);
            if let Some(locations) = locations {
                assert_eq!(twice(&locations)?, old);
            }
        }
    }
    Ok(())
}

#[test]
fn exact_body_and_catalog_noop_still_commits_distinct_opaque_fence() -> TestResult {
    for native_backend in [false, true] {
        let (_parent, locations, mut pair) = pair_case(native_backend)?;
        pair.create(None)?;
        let old = pair.capture()?;
        pair.composite(
            &CompositeFixtureParts {
                business: &OLD_ROWS,
                live: (b"old-meta", b"old-image"),
                fence: b"different-opaque-phase",
            },
            CommitFault::None,
        )?;
        let changed = pair.capture()?;
        assert_eq!(old.state.records, changed.state.records);
        assert_eq!(old.log, changed.log);
        assert_ne!(
            find(&old.state.metadata, LOGICAL[1]),
            find(&changed.state.metadata, LOGICAL[1])
        );
        assert_eq!(pair.counts.entered, [1, 1, 1, 1]);
        drop(pair);
        if let Some(locations) = locations {
            assert_eq!(twice(&locations)?, changed);
        }
    }
    Ok(())
}

#[test]
fn partial_live_unknown_or_malformed_fixed_records_poison_without_write_or_repair() -> TestResult {
    for native_backend in [false, true] {
        for (key, value) in [
            (LOGICAL[5], None),
            (b"unknown".as_slice(), Some(b"x".as_slice())),
            (
                super::super::control::PROFILE_KEY,
                Some(b"foreign".as_slice()),
            ),
            (super::super::control::INIT_KEY, Some([0].as_slice())),
        ] {
            let (_parent, locations, mut pair) = pair_case(native_backend)?;
            pair.create(None)?;
            damage_state_meta(&mut pair, key, value)?;
            let counts = pair.counts.backend;
            assert_eq!(pair.capture().err(), Some(Error::InvalidLogical));
            assert_eq!(
                pair.composite(&composite_parts(), CommitFault::None),
                Err(Error::Poisoned)
            );
            assert_eq!(pair.counts.backend, counts);
            drop(pair);
            if let Some(locations) = locations {
                let before_state = physical_capture_without_admission(&locations.paths[0])?;
                let before_log = physical_capture_without_admission(&locations.paths[1])?;
                assert_eq!(
                    reopen_controlled_fixture(&locations, binding()).err(),
                    Some(Error::InvalidLogical)
                );
                assert_eq!(
                    physical_capture_without_admission(&locations.paths[0])?,
                    before_state
                );
                assert_eq!(
                    physical_capture_without_admission(&locations.paths[1])?,
                    before_log
                );
            }
        }
    }
    Ok(())
}

#[test]
fn oversized_live_and_staged_values_refuse_before_inventory_body_copy() -> TestResult {
    // This focused heavy case is intended to run serially after root resource checks.
    for native_backend in [false, true] {
        for (key, limit) in [
            (LOGICAL[3], 8192),
            (LOGICAL[4], 64 * 1024 * 1024),
            (LOGICAL[5], 8192),
            (LOGICAL[6], 64 * 1024 * 1024),
        ] {
            let (_parent, locations, mut pair) = pair_case(native_backend)?;
            pair.create(None)?;
            let retained_old = pair.capture()?;
            let oversized = vec![0; limit + 1];
            damage_state_meta(&mut pair, key, Some(&oversized))?;
            let writes = pair.counts.backend;
            assert_eq!(pair.capture().err(), Some(Error::Limit));
            assert_eq!(
                pair.composite(&composite_parts(), CommitFault::None),
                Err(Error::Poisoned)
            );
            assert_eq!(pair.counts.backend, writes);
            drop(oversized);
            drop(pair);
            if let Some(locations) = locations {
                assert_eq!(
                    reopen_controlled_fixture(&locations, binding()).err(),
                    Some(Error::Limit)
                );
                assert_eq!(
                    reopen_controlled_fixture(&locations, binding()).err(),
                    Some(Error::Limit)
                );
            }
            assert_prefix(&retained_old, 3);
        }
    }
    Ok(())
}
