use super::{fixture::Fixture, *};

#[test]
fn assessment_reads_clock_and_floor_on_actual_owner() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    let caller = std::thread::current().id();
    let state = fixture.handle.maintenance_clock_assessment_blocking();
    let calls = fixture.clock.calls();
    let counts = fixture.store.counts();
    let cleanup = fixture.finish();
    assert!(cleanup.owner.is_ok());
    assert_eq!(state, Assessment::Ready);
    assert_eq!(calls.len(), 1);
    assert_ne!(calls[0], caller);
    assert_eq!(counts.gets, vec![(vec![0], calls[0])]);
    assert!(counts.scans.is_empty());
    assert_eq!((counts.snapshots, counts.applies), (0, 0));
    Ok(())
}

#[test]
fn empty_store_is_genuinely_assessed_without_mutation() -> TestResult {
    for now in [0, u64::MAX] {
        let fixture = Fixture::new(None, now, 500)?;
        let observed = (|| -> TestResult<_> {
            let before = fixture.store.bytes()?;
            let state = fixture.handle.maintenance_clock_assessment_blocking();
            Ok((
                before,
                state,
                fixture.store.bytes()?,
                fixture.clock.calls(),
                fixture.store.counts(),
            ))
        })();
        let cleanup = fixture.finish();
        let (before, state, after, calls, counts) = observed?;
        assert!(cleanup.owner.is_ok());
        assert_eq!(state, Assessment::Ready);
        assert!(before.is_empty());
        assert_eq!(after, before);
        assert_eq!(calls.len(), 1);
        assert_eq!(counts.gets.len(), 1);
        assert_eq!((counts.applies, counts.snapshots), (0, 0));
    }
    Ok(())
}

#[test]
fn empty_sweep_success_is_not_clock_evidence() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    let observed = (|| -> TestResult<_> {
        let (namespace, entity) = names();
        fixture.handle.submit_blocking(
            namespace,
            entity,
            CommandKind::DeleteEntity {
                target: domain::DeleteEntityTarget::Queue,
            },
        )?;
        fixture.clock.set(0);
        let before = fixture.clock.calls().len();
        let report = TimerWorker::new(&fixture.handle).sweep_once()?;
        let after = fixture.clock.calls().len();
        let state = fixture.handle.maintenance_clock_assessment_blocking();
        Ok((before, after, report, state, fixture.clock.calls().len()))
    })();
    let cleanup = fixture.finish();
    let (before, after, report, state, final_calls) = observed?;
    assert!(cleanup.owner.is_ok());
    assert!(report.is_idle());
    assert_eq!((report.queues_swept, report.topics_swept), (0, 0));
    assert_eq!(before, after);
    assert_eq!(state, Assessment::Unsafe);
    assert_eq!(final_calls, after + 1);
    Ok(())
}

#[test]
fn threshold_and_configured_override_are_exact() -> TestResult {
    for allowance in [500, 17] {
        for regression in [allowance - 1, allowance, allowance + 1] {
            let fixture = Fixture::new(Some(1_000), 1_000 - regression, allowance)?;
            let assessed = fixture.handle.maintenance_clock_assessment_blocking();
            let counts = fixture.store.counts();
            let cleanup = fixture.finish();
            assert!(cleanup.owner.is_ok());
            assert_eq!(
                assessed,
                if regression <= allowance {
                    Assessment::Ready
                } else {
                    Assessment::Unsafe
                }
            );
            assert_eq!(counts.applies, 0);
        }
    }
    Ok(())
}

#[test]
fn small_regression_reports_ready_without_advancing_floor() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 500, 500)?;
    let observed = (|| -> TestResult<_> {
        let before = fixture.store.bytes()?;
        let state = fixture.handle.maintenance_clock_assessment_blocking();
        let after = fixture.store.bytes()?;
        let query_applies = fixture.store.counts().applies;
        let (namespace, entity) = names();
        let sent = fixture
            .handle
            .submit_blocking(namespace, entity, send_kind())?;
        let floor = fixture.handle.last_applied_blocking()?;
        Ok((before, state, after, query_applies, sent, floor))
    })();
    let cleanup = fixture.finish();
    let (before, state, after, applies, sent, floor) = observed?;
    assert!(cleanup.owner.is_ok());
    assert_eq!(state, Assessment::Ready);
    assert_eq!(before, after);
    assert_eq!(applies, 0);
    assert!(matches!(sent, domain::CommandOutcome::Sent { .. }));
    assert_eq!(floor, Timestamp::from_millis(1_000));
    Ok(())
}

#[test]
fn unsafe_clock_catches_up_without_latch_or_reset() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 499, 500)?;
    let unsafe_state = fixture.handle.maintenance_clock_assessment_blocking();
    fixture.clock.set(1_001);
    let ready_state = fixture.handle.maintenance_clock_assessment_blocking();
    let (namespace, entity) = names();
    let sent = fixture
        .handle
        .submit_blocking(namespace, entity, send_kind());
    let floor = fixture.handle.last_applied_blocking();
    let cleanup = fixture.finish();
    assert!(cleanup.owner.is_ok());
    assert_eq!(unsafe_state, Assessment::Unsafe);
    assert_eq!(ready_state, Assessment::Ready);
    assert!(matches!(sent?, domain::CommandOutcome::Sent { .. }));
    assert_eq!(floor?, Timestamp::from_millis(1_001));
    Ok(())
}

#[test]
fn ready_query_cannot_bypass_later_command_stamp() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    let observed = (|| -> TestResult<_> {
        let before = fixture.store.bytes()?;
        let state = fixture.handle.maintenance_clock_assessment_blocking();
        fixture.clock.set(499);
        let (namespace, entity) = names();
        let refused = fixture
            .handle
            .submit_blocking(namespace, entity, send_kind());
        Ok((
            state,
            refused,
            before,
            fixture.store.bytes()?,
            fixture.store.counts().applies,
        ))
    })();
    let cleanup = fixture.finish();
    let (state, refused, before, after, applies) = observed?;
    assert!(cleanup.owner.is_ok());
    assert_eq!(state, Assessment::Ready);
    assert_eq!(
        refused,
        Err(crate::SubmitError::Propose(
            crate::ProposeError::ClockWentBackward {
                last_applied: Timestamp::from_millis(1_000),
                now: Timestamp::from_millis(499),
                allowed_millis: 500
            }
        ))
    );
    assert_eq!(before, after);
    assert_eq!(applies, 0);
    Ok(())
}

#[test]
fn floor_read_failure_is_unavailable_and_redacted() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    fixture.store.fail_floor(1);
    let state = fixture.handle.maintenance_clock_assessment_blocking();
    let counts = fixture.store.counts();
    let cleanup = fixture.finish();
    assert!(cleanup.owner.is_ok());
    assert_eq!(state, Assessment::Unavailable);
    assert_eq!(format!("{state:?}"), "Unavailable");
    assert_eq!(state.to_string(), "Unavailable");
    assert!(!format!("{state:?}").contains("private"));
    assert_eq!(counts.gets.len(), 1);
    assert_eq!(counts.applies, 0);
    Ok(())
}

#[test]
fn probe_does_not_scan_snapshot_discover_or_commit() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 0, 500)?;
    let observed = (|| -> TestResult<_> {
        let before = fixture.store.bytes()?;
        let state = fixture.handle.maintenance_clock_assessment_blocking();
        Ok((
            before,
            fixture.store.bytes()?,
            state,
            fixture.store.counts(),
            fixture.clock.calls(),
        ))
    })();
    let cleanup = fixture.finish();
    let (before, after, state, counts, calls) = observed?;
    assert!(cleanup.owner.is_ok());
    assert_eq!(state, Assessment::Unsafe);
    assert_eq!(before, after);
    assert_eq!(counts.gets, vec![(vec![0], calls[0])]);
    assert_eq!(calls.len(), 1);
    assert!(counts.scans.is_empty());
    assert_eq!((counts.snapshots, counts.applies), (0, 0));
    Ok(())
}
