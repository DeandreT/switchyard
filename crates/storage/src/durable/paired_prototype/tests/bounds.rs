use super::super::control::{BINDING_KEY, STAMP_KEY};
use super::super::inventory::{
    Budget, LOG_META_BYTES, LOG_RECORD_BYTES, MAX_IMAGE, STATE_META_BYTES, Shape,
};
use super::*;

#[test]
fn physical_controls_roundtrip_exact_closed_roles_and_phases() -> TestResult {
    for role in [Role::State, Role::Log] {
        for phase in [
            None,
            Some(CreationPhase::Prepared),
            Some(CreationPhase::Ready),
        ] {
            let bytes = binding().encode(role, phase);
            let (actual, actual_phase) =
                PhysicalPairBinding::decode(&bytes, role, phase.is_some())?;
            assert_eq!(actual, binding());
            assert_eq!(actual_phase, phase);
            assert_eq!(bytes.len(), 112);
        }
    }
    assert_eq!(Role::State.profile().len(), 34);
    assert_eq!(Role::Log.profile().len(), 32);
    assert_eq!(
        Role::State.format(),
        0xa000_0000 | super::super::super::ACTIVE_STORE_FORMAT
    );
    assert_eq!(
        Role::Log.format(),
        0xb000_0000 | super::super::super::ACTIVE_STORE_FORMAT
    );
    Ok(())
}

#[test]
fn physical_controls_reject_unknown_schema_role_reserved_length_and_aliases() {
    for stamp in [false, true] {
        let original = binding().encode(Role::State, stamp.then_some(CreationPhase::Ready));
        for index in [0, 4, 5, 6, 7] {
            let mut changed = original;
            changed[index] = 99;
            assert!(PhysicalPairBinding::decode(&changed, Role::State, stamp).is_err());
        }
        assert!(PhysicalPairBinding::decode(&original[..111], Role::State, stamp).is_err());
        assert!(PhysicalPairBinding::decode(&[0; 113], Role::State, stamp).is_err());
        assert!(PhysicalPairBinding::decode(&original, Role::Log, stamp).is_err());
    }
    assert_eq!(
        PhysicalPairBinding::new([[1; 16], [1; 16], [3; 16]], 7, [7; 16], [9; 32]),
        Err(Error::InvalidLogical)
    );
    assert_eq!(
        PhysicalPairBinding::new([[0; 16], [2; 16], [3; 16]], 7, [7; 16], [9; 32]),
        Err(Error::InvalidLogical)
    );
    assert_eq!(
        PhysicalPairBinding::new([[1; 16], [2; 16], [3; 16]], 7, [0; 16], [9; 32]),
        Err(Error::InvalidLogical)
    );
}

#[test]
fn borrowed_seed_and_composite_limits_precede_all_owned_preparation() {
    let too_long = [0; 257];
    let mut state = state_parts();
    state.fence = &too_long;
    assert!(matches!(
        PreparedFixturePair::memory(binding(), &state, &log_parts()),
        Err(Error::Limit)
    ));
    let baseline = vec![0; 16385];
    let mut log = log_parts();
    log.baseline = &baseline;
    assert!(matches!(
        PreparedFixturePair::memory(binding(), &state_parts(), &log),
        Err(Error::Limit)
    ));
    let input = CompositeFixtureParts {
        business: &[],
        live: (&[0; 8193], b"opaque"),
        fence: b"changed",
    };
    assert_eq!(input.validate(), Err(Error::Limit));
}

#[test]
fn business_and_entry_shapes_refuse_empty_duplicate_unordered_and_wrong_keys() {
    for rows in [
        vec![(b"".as_slice(), b"x".as_slice())],
        vec![(b"a".as_slice(), b"x".as_slice()), (b"a", b"y")],
        vec![(b"z".as_slice(), b"x".as_slice()), (b"a", b"y")],
    ] {
        let input = SeedStateParts {
            business: &rows,
            ..state_parts()
        };
        assert_eq!(input.validate(), Err(Error::InvalidLogical));
    }
    let rows = [(b"wrong".as_slice(), b"value".as_slice())];
    assert_eq!(
        SeedLogParts {
            entries: &rows,
            ..log_parts()
        }
        .validate(),
        Err(Error::InvalidLogical)
    );
    let huge_key = [0; 1025];
    let rows = [(huge_key.as_slice(), b"value".as_slice())];
    assert_eq!(
        SeedStateParts {
            business: &rows,
            ..state_parts()
        }
        .validate(),
        Err(Error::Limit)
    );
}

#[test]
fn exact_logical_budget_counts_and_checked_overflow_are_not_rss_claims() -> TestResult {
    for (rows, bytes) in [
        (65_545, 134_226_944),
        (265, 67_272_704),
        (131_076, 201_335_808),
    ] {
        let mut budget = Budget::new(rows, bytes);
        budget.consume(bytes, 0)?;
        assert_eq!(budget.consume(1, 0), Err(Error::Limit));
    }
    let mut one = Budget::new(1, usize::MAX);
    one.consume(usize::MAX, 0)?;
    assert_eq!(one.consume(0, 0), Err(Error::Limit));
    let mut overflow = Budget::new(usize::MAX, usize::MAX);
    assert_eq!(overflow.consume(usize::MAX, 1), Err(Error::Limit));
    let mut count = Budget::new(2, 0);
    count.consume(0, 0)?;
    count.consume(0, 0)?;
    assert_eq!(count.consume(0, 0), Err(Error::Limit));
    assert_eq!(STATE_META_BYTES + MAX_IMAGE, 201_344_103);
    assert_eq!(LOG_META_BYTES + LOG_RECORD_BYTES, 67_390_821);
    Ok(())
}

#[test]
fn finite_closed_control_bounds_accept_maxima_and_refuse_each_extra_byte() -> TestResult {
    for role in [Role::State, Role::Log] {
        let controls = if role == Role::State {
            &LOGICAL[..]
        } else {
            &LOGICAL[..5]
        };
        for (index, key) in controls.iter().enumerate() {
            let cap = if role == Role::State {
                match index {
                    0..=2 => 256,
                    3 | 5 => 8192,
                    _ => MAX_IMAGE,
                }
            } else {
                match index {
                    0 | 1 | 4 => 256,
                    2 => 16384,
                    _ => 131072,
                }
            };
            let mut shape = Shape::new(role);
            if role == Role::State {
                shape.metadata(key, cap)?;
                assert_eq!(Shape::new(role).metadata(key, cap + 1), Err(Error::Limit));
            } else {
                shape.record(key, cap)?;
                assert_eq!(Shape::new(role).record(key, cap + 1), Err(Error::Limit));
            }
        }
    }
    assert_eq!(
        Shape::new(Role::State).metadata(b"unknown", 0),
        Err(Error::InvalidLogical)
    );
    assert_eq!(
        Shape::new(Role::Log).record(b"unknown", 0),
        Err(Error::InvalidLogical)
    );
    let mut shape = Shape::new(Role::State);
    shape.metadata(BINDING_KEY, 112)?;
    assert_eq!(shape.metadata(BINDING_KEY, 112), Err(Error::InvalidLogical));
    assert_eq!(
        Shape::new(Role::State).metadata(STAMP_KEY, 113),
        Err(Error::Limit)
    );
    Ok(())
}

#[test]
fn opaque_components_do_not_assert_canonical_image_or_seed_digest_authority() -> TestResult {
    let mut pair = PreparedFixturePair::memory(binding(), &state_parts(), &log_parts())?;
    pair.create(None)?;
    let value = pair.capture()?;
    assert_eq!(
        find(&value.state.metadata, LOGICAL[6]),
        Some(b"old-image".as_slice())
    );
    assert!(find(&value.log.records, LOGICAL[2]).is_some());
    assert!(!format!("{value:?} {:?}", binding()).contains("opaque"));
    Ok(())
}

#[test]
fn full_closed_metadata_and_log_record_shape_maxima_match_exact_cap_arithmetic() -> TestResult {
    for role in [Role::State, Role::Log] {
        let mut shape = Shape::new(role);
        for (key, size) in [
            (super::super::super::FORMAT_VERSION_KEY, 4),
            (super::super::control::PROFILE_KEY, 64),
            (super::super::control::INIT_KEY, 1),
            (BINDING_KEY, 112),
            (STAMP_KEY, 112),
        ] {
            shape.metadata(key, size)?;
        }
        if role == Role::State {
            for (index, key) in LOGICAL.iter().enumerate() {
                shape.metadata(
                    key,
                    match index {
                        0..=2 => 256,
                        3 | 5 => 8192,
                        _ => MAX_IMAGE,
                    },
                )?;
            }
        } else {
            shape.metadata(super::super::control::MANIFEST_KEY, 131072)?;
            for (index, key) in LOGICAL[..5].iter().enumerate() {
                shape.record(
                    key,
                    match index {
                        0 | 1 | 4 => 256,
                        2 => 16384,
                        _ => 131072,
                    },
                )?;
            }
            for index in 0..256u64 {
                let mut key = [0x21; 9];
                key[1..].copy_from_slice(&index.to_be_bytes());
                shape.record(&key, MAX_IMAGE / 256)?;
            }
        }
        assert!(!shape.finish()?);
        assert_eq!(
            shape.counts(),
            if role == Role::State {
                (12, 0)
            } else {
                (6, 261)
            }
        );
    }
    Ok(())
}

#[test]
fn business_and_entry_aggregate_bytes_and_extra_rows_refuse_before_result_copy() -> TestResult {
    let mut business = Shape::new(Role::State);
    for index in 0..65_536u64 {
        business.record(&index.to_be_bytes(), 0)?;
    }
    assert_eq!(
        business.record(&65_536u64.to_be_bytes(), 0),
        Err(Error::Limit)
    );
    let mut entries = Shape::new(Role::Log);
    for index in 0..256u64 {
        let mut key = [0x21; 9];
        key[1..].copy_from_slice(&index.to_be_bytes());
        entries.record(&key, 0)?;
    }
    let extra = [0x21, 0, 0, 0, 0, 0, 0, 1, 0];
    assert_eq!(entries.record(&extra, 0), Err(Error::Limit));
    let mut aggregate = Shape::new(Role::Log);
    aggregate.record(&[0x21; 9], MAX_IMAGE)?;
    assert_eq!(aggregate.record(&extra, 1), Err(Error::Limit));
    let mut business = Shape::new(Role::State);
    let mut left = MAX_IMAGE;
    let mut index = 0u64;
    while left > 0 {
        let value = (left - 8).min(266240);
        business.record(&index.to_be_bytes(), value)?;
        left -= 8 + value;
        index += 1;
    }
    assert_eq!(business.record(b"extra", 0), Err(Error::Limit));
    Ok(())
}

#[test]
fn paired_topic_layout_eighteen_refuses_each_previous_role_without_logical_rewrite() -> TestResult {
    for role in [Role::State, Role::Log] {
        let parent = tempfile::TempDir::new()?;
        let (locations, mut pair) = native(parent.path())?;
        pair.create(None)?;
        let expected = pair.capture()?;
        let old = match role {
            Role::State => 0xa000_0000 | super::super::super::STORE_FORMAT_V17,
            Role::Log => 0xb000_0000 | super::super::super::STORE_FORMAT_V17,
        };
        let index = if role == Role::State { 0 } else { 1 };
        let BackendPair::Fjall { state, log } = &mut pair.backend else {
            panic!("native fixture")
        };
        let capsule = if role == Role::State {
            &state.capsule
        } else {
            &log.capsule
        };
        let mut batch = capsule
            .database
            .batch()
            .durability(Some(::fjall::PersistMode::SyncAll));
        batch.insert(
            &capsule.meta,
            super::super::super::FORMAT_VERSION_KEY,
            old.to_be_bytes().to_vec(),
        );
        batch.commit()?;
        drop(pair);
        let before = [
            physical_capture_without_admission(&locations.paths[0])?,
            physical_capture_without_admission(&locations.paths[1])?,
        ];
        assert_eq!(
            reopen_controlled_fixture(&locations, binding()),
            Err(Error::InvalidLogical)
        );
        assert_eq!(
            physical_capture_without_admission(&locations.paths[0])?,
            before[0]
        );
        assert_eq!(
            physical_capture_without_admission(&locations.paths[1])?,
            before[1]
        );
        let database = ::fjall::Database::recover(
            ::fjall::Database::builder(&locations.paths[index])
                .worker_threads(1)
                .into_config(),
        )?;
        let meta = database.keyspace(
            super::super::super::META_KEYSPACE,
            ::fjall::KeyspaceCreateOptions::default,
        )?;
        let mut batch = database
            .batch()
            .durability(Some(::fjall::PersistMode::SyncAll));
        batch.insert(
            &meta,
            super::super::super::FORMAT_VERSION_KEY,
            role.format().to_be_bytes().to_vec(),
        );
        batch.commit()?;
        drop(meta);
        drop(database);
        assert_eq!(twice(&locations)?, expected);
    }
    Ok(())
}
