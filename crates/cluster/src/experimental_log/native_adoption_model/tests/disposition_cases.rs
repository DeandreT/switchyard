use super::*;

#[test]
fn complete_role_and_stage_matrix_has_no_write_or_commit_receipt() -> TestResult {
    let case = fixture::Case::new()?;
    for (state, log, stage, disposition) in [
        (false, false, false, Disposition::PendingOldPair),
        (false, false, true, Disposition::PendingOldPair),
        (true, false, true, Disposition::PendingFinalization),
        (true, true, true, Disposition::RetainedFinalized),
        (false, true, true, Disposition::OrderingInconsistent),
        (true, false, false, Disposition::OrderingInconsistent),
        (true, true, false, Disposition::OrderingInconsistent),
        (false, true, false, Disposition::OrderingInconsistent),
    ] {
        let result = case.classify(state, log, stage)?;
        assert_eq!(
            result.state,
            if state {
                RoleIdentity::Selected
            } else {
                RoleIdentity::Old
            }
        );
        assert_eq!(
            result.log,
            if log {
                RoleIdentity::Selected
            } else {
                RoleIdentity::Old
            }
        );
        assert_eq!(
            result.stage,
            if stage {
                StageFact::ExactSelected
            } else {
                StageFact::Absent
            }
        );
        assert_eq!(result.disposition, disposition);
        assert_eq!(result.refusal, None);
    }
    Ok(())
}

#[test]
fn same_full_checkpoint_different_valid_body_is_not_old_or_inferred_selected() -> TestResult {
    let case = fixture::Case::new()?;
    assert_eq!(case.old.domain_checkpoint, case.selected.domain_checkpoint);
    assert_ne!(digest(&case.old.artifact), digest(&case.selected.artifact));
    let result = case.inspect(
        &case.selected.artifact,
        &case.state(false, true)?,
        &case.log(false)?,
    )?;
    assert_eq!(result.state, RoleIdentity::Neither);
    assert_eq!(result.log, RoleIdentity::Old);
    assert_eq!(result.disposition, Disposition::Halt);
    Ok(())
}

#[test]
fn whole_data_noop_is_distinguished_by_both_phase_fences() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.selected = case.old.clone();
    case.rebuild()?;
    let old = case.classify(false, false, true)?;
    let selected = case.classify(true, true, true)?;
    assert_eq!(old.state, RoleIdentity::Old);
    assert_eq!(old.log, RoleIdentity::Old);
    assert_eq!(selected.state, RoleIdentity::Selected);
    assert_eq!(selected.log, RoleIdentity::Selected);
    assert_ne!(case.state(false, true)?, case.state(true, true)?);
    assert_ne!(case.log(false)?, case.log(true)?);
    Ok(())
}

#[test]
fn partial_or_changed_stage_halts_without_erasing_exact_role_facts() -> TestResult {
    let case = fixture::Case::new()?;
    for tag in [3, 4, 5] {
        let mut state = case.state(true, true)?;
        state.retain(|(key, _)| key != &[0x20, tag]);
        let result = case.inspect(&case.selected.artifact, &state, &case.log(true)?)?;
        assert_eq!(result.stage, StageFact::Invalid);
        assert_eq!(result.state, RoleIdentity::Selected);
        assert_eq!(result.log, RoleIdentity::Selected);
        assert_eq!(result.disposition, Disposition::Halt);
    }
    let mut state = case.state(true, true)?;
    let artifact = &mut state
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 5])
        .ok_or("stage missing")?
        .1;
    *artifact.last_mut().ok_or("empty image")? ^= 1;
    let result = case.inspect(&case.selected.artifact, &state, &case.log(true)?)?;
    assert_eq!(result.stage, StageFact::Invalid);
    assert_eq!(result.disposition, Disposition::Halt);
    Ok(())
}

#[test]
fn selected_fence_is_bound_to_the_complete_current_intent() -> TestResult {
    let case = fixture::Case::new()?;
    let mut state = case.state(true, true)?;
    let bytes = &mut state
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 2])
        .ok_or("fence missing")?
        .1;
    let mut fence = state_fence(bytes)?;
    fence.intent = Some([0; 32]);
    *bytes = frame::encode(Kind::StateFence, Role::State, &fence, MAX_CONTROL)?;
    let result = case.inspect(&case.selected.artifact, &state, &case.log(true)?)?;
    assert_eq!(result.state, RoleIdentity::Neither);
    assert_eq!(result.log, RoleIdentity::Selected);
    assert_eq!(result.disposition, Disposition::Halt);
    Ok(())
}

#[test]
fn wrong_peer_header_is_not_laundered_by_valid_images_or_log_fences() -> TestResult {
    let case = fixture::Case::new()?;
    let mut state = case.state(true, true)?;
    let bytes = &mut state[0].1;
    let mut value = header(Role::State, bytes)?;
    value.binding.state = [4; 16];
    *bytes = frame::encode(Kind::Header, Role::State, &value, MAX_CONTROL)?;
    let result = case.inspect(&case.selected.artifact, &state, &case.log(true)?)?;
    assert_eq!(result.state, RoleIdentity::Neither);
    assert_eq!(result.log, RoleIdentity::Neither);
    assert_eq!(result.disposition, Disposition::Halt);
    Ok(())
}

#[test]
fn missing_or_mismatched_fences_do_not_skip_observed_business_or_catalog_validation() -> TestResult
{
    let case = fixture::Case::new()?;
    let mut state = case.state(false, false)?;
    state.retain(|(key, _)| key != &[0x20, 2]);
    let valid = case.inspect(&case.old.artifact, &state, &case.log(false)?)?;
    assert_eq!(valid.state, RoleIdentity::Neither);
    assert_eq!(valid.disposition, Disposition::Halt);
    let mut body = case.old.artifact.clone();
    *body.last_mut().ok_or("body missing")? ^= 1;
    assert!(case.inspect(&body, &state, &case.log(false)?).is_err());
    let mut missing_catalog_fence = state.clone();
    let catalog = &mut missing_catalog_fence
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 7])
        .ok_or("catalog missing")?
        .1;
    *catalog.last_mut().ok_or("empty catalog")? ^= 1;
    assert!(
        case.inspect(
            &case.old.artifact,
            &missing_catalog_fence,
            &case.log(false)?
        )
        .is_err()
    );
    state = case.state(true, true)?;
    let fence_bytes = &mut state
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 2])
        .ok_or("fence missing")?
        .1;
    let mut fence = state_fence(fence_bytes)?;
    fence.intent = Some([0; 32]);
    *fence_bytes = frame::encode(Kind::StateFence, Role::State, &fence, MAX_CONTROL)?;
    let catalog = &mut state
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 7])
        .ok_or("catalog missing")?
        .1;
    *catalog.last_mut().ok_or("empty catalog")? ^= 1;
    assert!(
        case.inspect(&case.selected.artifact, &state, &case.log(true)?)
            .is_err()
    );
    Ok(())
}

#[test]
fn foreign_pair_binding_does_not_skip_canonical_observed_log_rows() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.seed_rows = vec![fixture::blank(3)?];
    case.rebuild()?;
    let mut state = case.state(false, false)?;
    let mut value = header(Role::State, &state[0].1)?;
    value.binding.state = [4; 16];
    state[0].1 = frame::encode(Kind::Header, Role::State, &value, MAX_CONTROL)?;
    let mut log = case.log(false)?;
    let valid = case.inspect(&case.old.artifact, &state, &log)?;
    assert_eq!(valid.state, RoleIdentity::Neither);
    assert_eq!(valid.log, RoleIdentity::Neither);
    assert_eq!(valid.disposition, Disposition::Halt);
    let mut corrupt_body = case.old.artifact.clone();
    *corrupt_body.last_mut().ok_or("empty body")? ^= 1;
    assert!(case.inspect(&corrupt_body, &state, &log).is_err());
    let mut corrupt_catalog = state.clone();
    let catalog = &mut corrupt_catalog
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 7])
        .ok_or("catalog missing")?
        .1;
    *catalog.last_mut().ok_or("empty catalog")? ^= 1;
    assert!(
        case.inspect(&case.old.artifact, &corrupt_catalog, &log)
            .is_err()
    );
    let mut corrupt_baseline = log.clone();
    let bytes = &mut corrupt_baseline
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 3])
        .ok_or("baseline missing")?
        .1;
    let mut value = baseline(bytes)?;
    let mut metadata = value.metadata.0.to_vec();
    *metadata.last_mut().ok_or("empty metadata")? ^= 1;
    value.metadata = Blob(&metadata);
    *bytes = frame::encode(Kind::Baseline, Role::Log, &value, MAX_BASELINE)?;
    assert!(
        case.inspect(&case.old.artifact, &state, &corrupt_baseline)
            .is_err()
    );
    let entry = &mut log.last_mut().ok_or("entry missing")?.1;
    *entry.last_mut().ok_or("empty entry")? ^= 1;
    assert!(case.inspect(&case.old.artifact, &state, &log).is_err());
    Ok(())
}

#[test]
fn unknown_duplicate_old_prefix_and_wrong_owner_controls_are_refused() -> TestResult {
    let case = fixture::Case::new()?;
    let state = case.state(false, false)?;
    let log = case.log(false)?;
    for changed in [0, 1, 2, 3] {
        let mut bad = log.clone();
        match changed {
            0 => bad.push((vec![0x20, 6], vec![])),
            1 => bad.push(bad[0].clone()),
            2 => bad.push((vec![0x10, 0, 0, 0, 0, 0, 0, 0, 3], vec![])),
            _ => bad[0].1 = state[0].1.clone(),
        }
        assert!(case.inspect(&case.old.artifact, &state, &bad).is_err());
    }
    Ok(())
}

#[test]
fn prior_or_missing_journal_cannot_be_reused_or_inferred_from_selected_bytes() -> TestResult {
    let case = fixture::Case::new()?;
    let mut log = case.log(true)?;
    log.retain(|(key, _)| key != &[0x20, 4]);
    assert!(
        case.inspect(&case.selected.artifact, &case.state(true, true)?, &log)
            .is_err()
    );
    log = case.log(true)?;
    let bytes = &mut log
        .iter_mut()
        .find(|(key, _)| key == &[0x20, 5])
        .ok_or("fence missing")?
        .1;
    let mut fence = log_fence(bytes)?;
    fence.serial = 2;
    *bytes = frame::encode(Kind::LogFence, Role::Log, &fence, MAX_CONTROL)?;
    assert!(
        case.inspect(&case.selected.artifact, &case.state(true, true)?, &log)
            .is_err()
    );
    Ok(())
}

#[test]
fn persisted_old_fence_absence_is_report_only_not_reconstructed_from_body() -> TestResult {
    let mut case = fixture::Case::new()?;
    let mut intent = case.view()?;
    intent.old_fence = None;
    let encoded = encode_intent(&intent)?;
    case.intent = encoded;
    let mut state = case.state(false, false)?;
    state.retain(|(key, _)| key != &[0x20, 2]);
    let result = case.inspect(&case.old.artifact, &state, &case.log(false)?)?;
    assert_eq!(result.state, RoleIdentity::Neither);
    assert_eq!(result.refusal, Some(PolicyRefusal::Fence));
    assert_eq!(result.disposition, Disposition::Halt);
    Ok(())
}

#[test]
fn exact_log_identity_checks_complete_entry_values_not_only_progress() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.seed_rows = vec![fixture::blank(3)?];
    case.rebuild()?;
    let mut log = case.log(false)?;
    let value = &mut log.last_mut().ok_or("entry missing")?.1;
    *value.last_mut().ok_or("empty entry")? ^= 1;
    assert!(
        case.inspect(&case.old.artifact, &case.state(false, false)?, &log)
            .is_err()
    );
    Ok(())
}
