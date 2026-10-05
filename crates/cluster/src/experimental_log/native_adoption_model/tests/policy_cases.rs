use super::*;

#[test]
fn explicitly_bound_absent_catalog_requires_exact_seed_and_noninitial_baseline() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.catalog = None;
    case.rebuild()?;
    let result = case.classify(false, false, false)?;
    assert_eq!(result.state, RoleIdentity::Old);
    assert_eq!(result.refusal, None);
    assert_eq!(result.disposition, Disposition::PendingOldPair);
    Ok(())
}

#[test]
fn valid_stale_catalog_requires_exact_baseline_and_checkpoint_recurrence() -> TestResult {
    let mut case = fixture::Case::new()?;
    let catalog = case.old.clone();
    case.old = catalog.after_blank()?;
    case.selected = case.old.changed_body()?;
    case.catalog = Some(catalog);
    case.seed_rows = vec![fixture::blank(3)?];
    case.rebuild()?;
    let result = case.classify(false, false, true)?;
    assert_eq!(result.refusal, None);
    assert_eq!(result.state, RoleIdentity::Old);
    Ok(())
}

#[test]
fn same_checkpoint_different_catalog_body_and_ahead_catalog_are_refused() -> TestResult {
    for ahead in [false, true] {
        let mut case = fixture::Case::new()?;
        case.catalog = Some(if ahead {
            case.old.after_blank()?
        } else {
            case.old.changed_body()?
        });
        case.rebuild()?;
        let result = case.classify(false, false, false)?;
        assert_eq!(result.refusal, Some(PolicyRefusal::Catalog));
        assert_eq!(result.disposition, Disposition::Halt);
    }
    Ok(())
}

#[test]
fn selected_initial_and_initial_old_seed_are_report_only() -> TestResult {
    for noninitial in [false, true] {
        let report_only = if noninitial {
            fixture::Image::populated()?.without_membership()?
        } else {
            fixture::Image::initial()?
        };
        assert_eq!(report_only.domain_checkpoint.last().is_some(), noninitial);
        assert!(report_only.domain_checkpoint.membership().is_none());
        let native = native_fields(&report_only.native)?;
        assert_eq!(native.last.is_some(), noninitial);
        assert!(native.member_source.is_none() && native.membership.0.is_empty());
        assert_eq!(native.member_schema, 0);
        assert!(identity::native_matches(
            &report_only.metadata,
            report_only.identity(),
            &report_only.native
        )?);
        let mut case = fixture::Case::new()?;
        case.selected = report_only.clone();
        case.rebuild()?;
        let selected = case.classify(false, false, false)?;
        assert_eq!(selected.refusal, Some(PolicyRefusal::InitialSelected));
        assert_eq!(selected.disposition, Disposition::Halt);
        case = fixture::Case::new()?;
        case.old = report_only;
        case.catalog = Some(case.old.clone());
        case.rebuild()?;
        let old = case.classify(false, false, false)?;
        assert_eq!(old.refusal, Some(PolicyRefusal::InitialOld));
        assert_eq!(old.disposition, Disposition::Halt);
    }
    Ok(())
}

#[test]
fn absent_uncommitted_insufficient_and_changed_votes_are_refused() -> TestResult {
    for vote in [
        None,
        Some(Vote {
            term: 0,
            node: 0,
            committed: true,
        }),
        Some(Vote {
            term: 1,
            node: 8,
            committed: false,
        }),
    ] {
        let mut case = fixture::Case::new()?;
        case.vote = vote;
        case.final_vote = vote;
        case.rebuild()?;
        assert_eq!(
            case.classify(false, false, false)?.refusal,
            Some(PolicyRefusal::Vote)
        );
    }
    let mut case = fixture::Case::new()?;
    case.final_vote = Some(Vote {
        term: 3,
        node: 7,
        committed: true,
    });
    case.rebuild()?;
    assert_eq!(
        case.classify(false, false, false)?.refusal,
        Some(PolicyRefusal::Vote)
    );
    Ok(())
}

#[test]
fn committed_vote_requirement_delegates_to_native_partial_ordering() -> TestResult {
    let selected = domain::CommittedEntryId {
        term: 1,
        node_id: 7,
        index: 2,
    };
    for vote in [
        Vote {
            term: 0,
            node: 0,
            committed: true,
        },
        Vote {
            term: 1,
            node: 7,
            committed: true,
        },
        Vote {
            term: 1,
            node: 8,
            committed: true,
        },
        Vote {
            term: 2,
            node: 7,
            committed: true,
        },
    ] {
        let expected = matches!(
            vote.native().partial_cmp(&crate::LogVote::new_committed(
                selected.term,
                selected.node_id
            )),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        );
        assert_eq!(
            entries::vote_sufficient(Some(vote), selected, &[])?,
            expected
        );
    }
    Ok(())
}

#[test]
fn reset_to_earlier_leader_with_empty_seed_rows_still_covers_old_baseline_vote() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.old = case.old.with_leader_term(3)?;
    case.catalog = Some(case.old.clone());
    case.vote = Some(Vote {
        term: 1,
        node: 7,
        committed: true,
    });
    case.final_vote = case.vote;
    case.rebuild()?;
    assert!(case.seed_rows.is_empty());
    let result = case.classify(false, false, false)?;
    assert_eq!(result.refusal, Some(PolicyRefusal::Vote));
    assert_eq!(result.disposition, Disposition::Halt);
    case.vote = Some(Vote {
        term: 3,
        node: 7,
        committed: true,
    });
    case.final_vote = case.vote;
    case.rebuild()?;
    assert_eq!(case.classify(false, false, false)?.refusal, None);
    Ok(())
}

#[test]
fn independently_trusted_exact_continuation_keeps_every_suffix_byte() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.tail = TailChoice::ExactContinuation;
    case.seed_rows = vec![fixture::blank(3)?, fixture::blank(4)?];
    case.final_rows = case.seed_rows.clone();
    case.rebuild()?;
    assert_eq!(case.classify(false, false, true)?.refusal, None);
    assert_eq!(
        case.classify(true, true, true)?.disposition,
        Disposition::RetainedFinalized
    );
    Ok(())
}

#[test]
fn unrelated_suffix_is_deleted_only_by_explicit_exact_inventory_reset_choice() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.seed_rows = vec![fixture::blank(3)?, fixture::blank(4)?];
    case.tail = TailChoice::ExactContinuation;
    case.rebuild()?;
    assert_eq!(
        case.classify(false, false, false)?.refusal,
        Some(PolicyRefusal::Tail)
    );
    case.tail = TailChoice::ExactResetEmpty;
    case.rebuild()?;
    assert_eq!(case.classify(false, false, false)?.refusal, None);
    Ok(())
}

#[test]
fn ahead_or_behind_continuation_requires_reset_not_gap_inference() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.selected = case.old.after_blank()?;
    case.tail = TailChoice::ExactContinuation;
    case.rebuild()?;
    assert_eq!(
        case.classify(false, false, false)?.refusal,
        Some(PolicyRefusal::Tail)
    );
    case.tail = TailChoice::ExactResetEmpty;
    case.rebuild()?;
    assert_eq!(case.classify(false, false, false)?.refusal, None);
    case.old = case.old.after_blank()?;
    case.selected = fixture::Image::populated()?;
    case.catalog = Some(case.old.clone());
    case.tail = TailChoice::ExactContinuation;
    case.rebuild()?;
    assert_eq!(
        case.classify(false, false, false)?.refusal,
        Some(PolicyRefusal::Tail)
    );
    Ok(())
}

#[test]
fn duplicate_gapped_and_wrong_native_identity_entries_refuse() -> TestResult {
    let mut case = fixture::Case::new()?;
    case.seed_rows = vec![fixture::blank(4)?];
    assert!(case.rebuild().is_err());
    case.seed_rows = vec![fixture::blank(3)?, fixture::blank(3)?];
    assert!(case.rebuild().is_err());
    let (mut key, value) = fixture::blank(3)?;
    key[8] = 4;
    let rows = [Row {
        key: &key,
        value: &value,
    }];
    let progress = Progress {
        vote: case.vote,
        purged: case.old.domain_checkpoint.last().map(|mark| mark.id),
        present: Some(domain::CommittedEntryId {
            term: 1,
            node_id: 7,
            index: 4,
        }),
        entries: 1,
        bytes: value.len() as u64,
    };
    let summary =
        crate::experimental_state_machine::NativeCheckpointSummary::decode(&case.old.metadata)?;
    assert!(entries::validate(&rows, progress, &summary).is_err());
    Ok(())
}

#[test]
fn independent_seed_selection_native_and_tail_expectations_are_not_derived_authority() -> TestResult
{
    let case = fixture::Case::new()?;
    let state = case.state(false, false)?;
    let log = case.log(false)?;
    let state_rows = fixture::rows(&state);
    let log_rows = fixture::rows(&log);
    for changed in [0, 1, 2, 3] {
        let mut trust = TrustedModelInputs {
            seed: case.seed,
            selected: case.selected.identity(),
            native: Blob(&case.selected.native),
            tail: case.tail,
            final_entries: case.view()?.final_entries,
            seed_rows: &[],
        };
        match changed {
            0 => trust.seed[0] ^= 1,
            1 => trust.selected.digest[0] ^= 1,
            2 => trust.native = Blob(&case.old.native),
            _ => trust.tail = TailChoice::ExactContinuation,
        }
        let result = classify::classify(
            &case.intent,
            BorrowedPairObservation {
                business: &case.old.artifact,
                state: &state_rows,
                log: &log_rows,
            },
            trust,
        )?;
        assert_eq!(result.refusal, Some(PolicyRefusal::Trust));
        assert_eq!(result.disposition, Disposition::Halt);
    }
    Ok(())
}

#[test]
fn exact_entry_count_value_caps_and_hash_framing_are_checked() -> TestResult {
    let (key, value) = fixture::blank(3)?;
    let same = entries::manifest(&[Row {
        key: &key,
        value: &value,
    }])?;
    let mut changed = value.clone();
    changed.push(0);
    assert_ne!(
        same.digest,
        entries::manifest(&[Row {
            key: &key,
            value: &changed
        }])?
        .digest
    );
    let values: Vec<_> = (0..=crate::MAX_RETAINED_ENTRIES)
        .map(|index| (entries::key(index).to_vec(), vec![0]))
        .collect();
    assert_eq!(
        entries::manifest(&fixture::rows(&values)).err(),
        Some(ModelCodecError::Limit)
    );
    let huge = vec![0; crate::MAX_LOG_ENTRY_BYTES + 1];
    assert_eq!(
        entries::manifest(&[Row {
            key: &key,
            value: &huge
        }])
        .err(),
        Some(ModelCodecError::Limit)
    );
    Ok(())
}
