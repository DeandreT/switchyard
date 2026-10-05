use super::*;

#[test]
fn aligned_seed_initial_policy() -> TestResult {
    let case = Case::from_image(Image::initial(false)?)?;
    assert!(case.image.checkpoint.last().is_none());
    assert_eq!(
        case.run(&case.expectation()),
        Err(Error::UnsupportedSeedPolicy)
    );
    Ok(())
}

#[test]
fn aligned_seed_noninitial_no_member_policy() -> TestResult {
    let case = Case::from_image(Image::initial(true)?)?;
    assert!(case.image.checkpoint.last().is_some());
    assert!(case.image.checkpoint.membership().is_none());
    assert_eq!(
        case.run(&case.expectation()),
        Err(Error::UnsupportedSeedPolicy)
    );
    Ok(())
}

#[test]
fn aligned_seed_positive_exact_ordinal() -> TestResult {
    let mut case = Case::new()?;
    let mut expected = case.expectation();
    expected.baseline_ordinal = 0;
    assert_eq!(case.run(&expected), Err(Error::InvalidExpectation));
    expected.baseline_ordinal = 2;
    assert_eq!(case.run(&expected), Err(Error::IdentityMismatch));
    case.controls[2] = Baseline::empty(&case.profile)?.bytes;
    assert_eq!(
        case.run(&case.expectation()),
        Err(Error::UnsupportedSeedPolicy)
    );
    Ok(())
}

#[test]
fn aligned_seed_baseline_stale_ahead_conflict() -> TestResult {
    let mut case = Case::new()?;
    let stale = Image::before_send()?;
    let ahead = case.image.after_blank()?;
    assert!(
        stale.checkpoint.last().map(|mark| mark.id.index)
            < case.image.checkpoint.last().map(|mark| mark.id.index)
    );
    assert!(
        ahead.checkpoint.last().map(|mark| mark.id.index)
            > case.image.checkpoint.last().map(|mark| mark.id.index)
    );
    let candidates = [
        stale.metadata,
        ahead.metadata,
        Image::variant([7; 16], 10, 11, 17)?.metadata,
    ];
    for metadata in &candidates {
        case.controls[2] = Baseline::make(&case.profile, 1, metadata)?.bytes;
        assert_eq!(case.run(&case.expectation()), Err(Error::IdentityMismatch));
    }
    Ok(())
}

#[test]
fn aligned_seed_baseline_body_identity() -> TestResult {
    let mut case = Case::new()?;
    let changed = case.image.changed_body()?;
    assert_eq!(changed.checkpoint, case.image.checkpoint);
    case.controls[2] = Baseline::make(&case.profile, 1, &changed.metadata)?.bytes;
    assert_eq!(case.run(&case.expectation()), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn aligned_seed_vote_absent_uncommitted() -> TestResult {
    let mut case = Case::new()?;
    for vote in [None, Some(LogVote::new(2, 7))] {
        let mut progress = case.progress()?;
        progress.vote = vote;
        case.set_progress(&progress)?;
        assert_eq!(
            case.run(&case.expectation()),
            Err(Error::UnsupportedSeedPolicy)
        );
    }
    let mut expected = case.expectation();
    expected.vote = LogVote::new(2, 7);
    assert_eq!(case.run(&expected), Err(Error::InvalidExpectation));
    Ok(())
}

#[test]
fn aligned_seed_vote_below_required_leader() -> TestResult {
    let mut case = Case::new()?;
    case.vote = LogVote::new_committed(0, 99);
    let mut progress = case.progress()?;
    progress.vote = Some(case.vote);
    case.set_progress(&progress)?;
    assert_eq!(
        case.run(&case.expectation()),
        Err(Error::UnsupportedSeedPolicy)
    );
    Ok(())
}

#[test]
fn aligned_seed_equal_term_node_order() -> TestResult {
    let mut case = Case::new()?;
    for (node, accepted) in [(6, false), (7, true), (8, true)] {
        case.vote = LogVote::new_committed(1, node);
        let mut progress = case.progress()?;
        progress.vote = Some(case.vote);
        case.set_progress(&progress)?;
        assert_eq!(
            case.run(&case.expectation()),
            if accepted {
                Ok(())
            } else {
                Err(Error::UnsupportedSeedPolicy)
            }
        );
    }
    Ok(())
}

#[test]
fn aligned_seed_changed_sufficient_vote() -> TestResult {
    let mut case = Case::new()?;
    let mut progress = case.progress()?;
    progress.vote = Some(LogVote::new_committed(3, 7));
    case.set_progress(&progress)?;
    assert_eq!(case.run(&case.expectation()), Err(Error::IdentityMismatch));
    Ok(())
}

#[test]
fn aligned_seed_progress_empty_tail_facts() -> TestResult {
    let mut case = Case::new()?;
    let original = case.progress()?;
    let last = original.last_purged.ok_or("last missing")?;
    let mut progress = original.clone();
    progress.last_purged = None;
    case.set_progress(&progress)?;
    assert_eq!(case.run(&case.expectation()), Err(Error::InvalidLog));
    progress = original;
    progress.last_present = Some(crate::LogId::new(last.leader_id, last.index + 1));
    progress.retained_entries = 1;
    progress.retained_bytes = 1;
    case.set_progress(&progress)?;
    assert_eq!(case.run(&case.expectation()), Err(Error::UnsupportedTail));
    Ok(())
}
