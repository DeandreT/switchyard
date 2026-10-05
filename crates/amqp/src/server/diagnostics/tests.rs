use std::{sync::Barrier, thread};

use super::*;

mod capture;
mod schema;
mod writer;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const EVENT: DiagnosticEvent = DiagnosticEvent::Fixture(DiagnosticFixtureBoundary::Started);

fn fixture() -> Result<(ServerDiagnosticRecorder, DiagnosticScope), DiagnosticRefusal> {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let scope = recorder.scope(DiagnosticScopeKind::Fixture, None)?;
    Ok((recorder, scope))
}

#[test]
fn empty_capture_has_no_coverage_claim_or_rows() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let capture = recorder.capture()?;
    assert!(capture.records().is_empty());
    assert_eq!(capture.losses(), DiagnosticLossSummary::default());
    assert!(
        capture
            .format_bounded()?
            .starts_with("rows=0 buffer_full=0")
    );
    Ok(())
}

#[test]
fn scopes_are_fresh_and_parented_only_by_local_correlation() -> TestResult {
    let (recorder, root) = fixture()?;
    let child = recorder.scope(DiagnosticScopeKind::Connection, Some(&root))?;
    recorder.record(&root, EVENT, Duration::ZERO)?;
    recorder.record(&child, EVENT, Duration::from_millis(9))?;
    let capture = recorder.capture()?;
    assert_eq!(capture.records()[0].scope_ordinal(), 1);
    assert_eq!(capture.records()[0].parent_ordinal(), None);
    assert_eq!(capture.records()[1].scope_ordinal(), 2);
    assert_eq!(capture.records()[1].parent_ordinal(), Some(1));
    assert_eq!(
        capture.records()[1].scope_kind(),
        DiagnosticScopeKind::Connection
    );
    assert_eq!(capture.records()[1].elapsed_millis(), 9);
    assert_eq!(capture.records()[1].event(), EVENT);
    Ok(())
}

#[test]
fn dropping_scopes_does_not_reuse_ordinals() -> TestResult {
    let (recorder, first) = fixture()?;
    drop(first);
    let second = recorder.scope(DiagnosticScopeKind::Write, None)?;
    recorder.record(&second, EVENT, Duration::ZERO)?;
    assert_eq!(recorder.capture()?.records()[0].scope_ordinal(), 2);
    Ok(())
}

#[test]
fn recorder_and_scope_clones_keep_the_same_provenance() -> TestResult {
    let (recorder, scope) = fixture()?;
    recorder
        .clone()
        .record(&scope.clone(), EVENT, Duration::ZERO)?;
    assert_eq!(
        recorder.capture()?.records()[0].scope_ordinal(),
        scope.ordinal
    );
    Ok(())
}

#[test]
fn foreign_parent_refusal_does_not_consume_a_local_ordinal() -> TestResult {
    let (_, foreign) = fixture()?;
    let recorder = ServerDiagnosticRecorder::try_new()?;
    assert_eq!(
        recorder
            .scope(DiagnosticScopeKind::Connection, Some(&foreign))
            .unwrap_err(),
        DiagnosticRefusal::ForeignScope,
    );
    let local = recorder.scope(DiagnosticScopeKind::Connection, None)?;
    assert_eq!(local.ordinal, 1);
    assert_eq!(recorder.loss_summary().foreign_scope, 1);
    assert!(recorder.capture()?.records().is_empty());
    Ok(())
}

#[test]
fn foreign_record_refusal_preserves_rows_and_sequence() -> TestResult {
    let (_, foreign) = fixture()?;
    let (recorder, scope) = fixture()?;
    assert_eq!(
        recorder.record(&foreign, EVENT, Duration::ZERO),
        Err(DiagnosticRefusal::ForeignScope)
    );
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    assert_eq!(recorder.capture()?.records()[0].sequence(), 1);
    assert_eq!(recorder.loss_summary().foreign_scope, 1);
    Ok(())
}

#[test]
fn last_valid_ordinal_is_maximum_and_never_wraps_or_resumes() -> TestResult {
    let (recorder, _) = fixture()?;
    recorder.0.last_scope.store(u64::MAX - 1, Ordering::Relaxed);
    let last = recorder.scope(DiagnosticScopeKind::Write, None)?;
    assert_eq!(last.ordinal, u64::MAX);
    drop(last);
    for _ in 0..2 {
        assert_eq!(
            recorder
                .scope(DiagnosticScopeKind::Write, None)
                .unwrap_err(),
            DiagnosticRefusal::OrdinalExhausted
        );
    }
    assert_eq!(recorder.0.last_scope.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(recorder.loss_summary().ordinal_exhausted, 2);
    Ok(())
}

#[test]
fn event_limit_is_exact_and_refusal_never_grows_or_overwrites() -> TestResult {
    let (recorder, scope) = fixture()?;
    let initial_capacity = recorder.0.records.lock().unwrap().capacity();
    assert!(initial_capacity >= MAX_SERVER_DIAGNOSTIC_EVENTS);
    for _ in 0..MAX_SERVER_DIAGNOSTIC_EVENTS {
        recorder.record(&scope, EVENT, Duration::ZERO)?;
    }
    for _ in 0..2 {
        assert_eq!(
            recorder.record(&scope, EVENT, Duration::ZERO),
            Err(DiagnosticRefusal::Full)
        );
    }
    let capture = recorder.capture()?;
    assert_eq!(capture.records().len(), 4096);
    assert_eq!(capture.losses().buffer_full, 2);
    assert!(
        capture
            .records()
            .iter()
            .enumerate()
            .all(|(index, row)| row.sequence() == index as u64 + 1)
    );
    assert_eq!(
        recorder.0.records.lock().unwrap().capacity(),
        initial_capacity
    );
    Ok(())
}

#[test]
fn capture_is_immutable_independent_of_later_publication_and_losses() -> TestResult {
    let (recorder, scope) = fixture()?;
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    let captured = recorder.capture()?;
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    let held = recorder.0.records.lock().unwrap();
    assert_eq!(
        recorder.record(&scope, EVENT, Duration::ZERO),
        Err(DiagnosticRefusal::Contended)
    );
    drop(held);
    assert_eq!(captured.records().len(), 1);
    assert_eq!(captured.losses().contention, 0);
    assert_eq!(recorder.capture()?.records().len(), 2);
    assert_eq!(recorder.loss_summary().contention, 1);
    Ok(())
}

#[test]
fn supplied_elapsed_duration_saturates_without_a_clock() -> TestResult {
    let (recorder, scope) = fixture()?;
    recorder.record(&scope, EVENT, Duration::MAX)?;
    assert_eq!(recorder.capture()?.records()[0].elapsed_millis(), u64::MAX);
    Ok(())
}

#[test]
fn occupied_publication_is_refused_without_waiting_or_adding_a_row() -> TestResult {
    let (recorder, scope) = fixture()?;
    let held = recorder.0.records.lock().unwrap();
    assert_eq!(
        recorder.record(&scope, EVENT, Duration::ZERO),
        Err(DiagnosticRefusal::Contended)
    );
    assert_eq!(recorder.loss_summary().contention, 1);
    assert!(held.is_empty());
    drop(held);
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    assert_eq!(recorder.capture()?.records()[0].sequence(), 1);
    Ok(())
}

#[test]
fn occupied_capture_is_a_distinct_static_refusal() -> TestResult {
    let (recorder, _) = fixture()?;
    let held = recorder.0.records.lock().unwrap();
    assert_eq!(
        recorder.capture().unwrap_err(),
        DiagnosticRefusal::Contended
    );
    assert_eq!(recorder.loss_summary().contention, 1);
    drop(held);
    assert!(recorder.capture()?.records().is_empty());
    Ok(())
}

#[test]
fn poisoned_storage_is_contained_for_record_and_capture() -> TestResult {
    let (recorder, scope) = fixture()?;
    let poison = recorder.clone();
    let joined = thread::spawn(move || {
        let _held = poison.0.records.lock().unwrap();
        panic!("synthetic mutex poisoning");
    })
    .join();
    assert!(joined.is_err());
    assert_eq!(
        recorder.record(&scope, EVENT, Duration::ZERO),
        Err(DiagnosticRefusal::Poisoned)
    );
    assert_eq!(recorder.capture().unwrap_err(), DiagnosticRefusal::Poisoned);
    assert_eq!(recorder.loss_summary().poisoned, 2);
    Ok(())
}

#[test]
fn every_loss_counter_saturates_without_wrapping() -> TestResult {
    let losses = Losses::default();
    let counters = [
        &losses.buffer_full,
        &losses.contention,
        &losses.poisoned,
        &losses.ordinal_exhausted,
        &losses.foreign_scope,
        &losses.capture_allocation,
    ];
    for counter in counters {
        counter.store(u64::MAX - 1, Ordering::Relaxed);
        lose(counter);
        lose(counter);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }
    let expected = DiagnosticLossSummary {
        buffer_full: u64::MAX,
        contention: u64::MAX,
        poisoned: u64::MAX,
        ordinal_exhausted: u64::MAX,
        foreign_scope: u64::MAX,
        capture_allocation: u64::MAX,
    };
    assert_eq!(losses.read(), expected);
    Ok(())
}

#[test]
fn private_capacity_overflow_returns_allocation_refusal_not_global_oom_evidence() {
    let mut output = Vec::<DiagnosticRecord>::new();
    assert_eq!(
        reserve(&mut output, usize::MAX),
        Err(DiagnosticRefusal::Allocation)
    );
    assert!(output.is_empty());
    assert_eq!(output.capacity(), 0);
}

#[test]
fn diagnostics_are_owned_send_sync_and_schema_is_copy() {
    fn owned<T: Send + Sync + 'static>() {}
    fn copy<T: Copy>() {}
    owned::<ServerDiagnosticRecorder>();
    owned::<DiagnosticScope>();
    owned::<DiagnosticCapture>();
    copy::<DiagnosticRecord>();
    copy::<DiagnosticEvent>();
    copy::<DiagnosticLossSummary>();
    copy::<DiagnosticRefusal>();
}

#[test]
fn two_concurrent_publishers_account_for_every_attempt() -> TestResult {
    let (recorder, scope) = fixture()?;
    let barrier = Arc::new(Barrier::new(2));
    let joins: Vec<_> = (0..2)
        .map(|_| {
            let recorder = recorder.clone();
            let scope = scope.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut accepted = 0;
                for _ in 0..512 {
                    match recorder.record(&scope, EVENT, Duration::ZERO) {
                        Ok(()) => accepted += 1,
                        Err(DiagnosticRefusal::Contended) => {}
                        other => panic!("unexpected typed recording result: {other:?}"),
                    }
                }
                accepted
            })
        })
        .collect();
    let results: Vec<_> = joins.into_iter().map(|join| join.join()).collect();
    let accepted: usize = results.into_iter().map(|result| result.unwrap()).sum();
    let capture = recorder.capture()?;
    assert_eq!(capture.records().len(), accepted);
    assert_eq!(accepted as u64 + capture.losses().contention, 1024);
    assert_eq!(capture.losses().buffer_full, 0);
    assert!(
        capture
            .records()
            .iter()
            .enumerate()
            .all(|(index, row)| row.sequence() == index as u64 + 1)
    );
    Ok(())
}

#[test]
fn two_concurrent_scope_allocators_never_duplicate_correlation() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let joins: Vec<_> = (0..2)
        .map(|_| {
            let recorder = recorder.clone();
            thread::spawn(move || {
                (0..128)
                    .map(|_| {
                        recorder
                            .scope(DiagnosticScopeKind::Connection, None)
                            .unwrap()
                            .ordinal
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let results: Vec<_> = joins.into_iter().map(|join| join.join()).collect();
    let mut ordinals: Vec<_> = results
        .into_iter()
        .flat_map(|result| result.unwrap())
        .collect();
    ordinals.sort_unstable();
    assert_eq!(ordinals, (1..=256).collect::<Vec<u64>>());
    Ok(())
}
