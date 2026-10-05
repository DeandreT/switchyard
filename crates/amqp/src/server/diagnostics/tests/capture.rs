use super::super::capture::{FixedLine, MAX_RECORD_BYTES, MAX_SUMMARY_BYTES};
use super::*;

#[test]
fn capture_keeps_only_owned_rows_after_recorder_and_scope_drop() -> TestResult {
    let (recorder, scope) = fixture()?;
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    let weak = Arc::downgrade(&recorder.0);
    let capture = recorder.capture()?;
    drop(scope);
    drop(recorder);
    assert!(weak.upgrade().is_none());
    assert_eq!(capture.records()[0].event(), EVENT);
    assert!(capture.format_bounded()?.contains("event=fixture-started"));
    Ok(())
}

#[test]
fn maximal_full_capture_stays_within_private_output_bound() -> TestResult {
    let row = super::schema::widest_row(
        DiagnosticEvent::ActorDispatch(DiagnosticFrameClass::TransactionalDisposition),
        DiagnosticScopeKind::Retirement,
    );
    let losses = DiagnosticLossSummary {
        buffer_full: u64::MAX,
        contention: u64::MAX,
        poisoned: u64::MAX,
        ordinal_exhausted: u64::MAX,
        foreign_scope: u64::MAX,
        capture_allocation: u64::MAX,
    };
    let capture = DiagnosticCapture::new(vec![row; MAX_SERVER_DIAGNOSTIC_EVENTS], losses);
    let formatted = capture.format_bounded()?;
    let maximum = MAX_SERVER_DIAGNOSTIC_FORMAT_BYTES;
    assert_eq!(maximum, 918_016);
    assert!(maximum < 1024 * 1024);
    assert!(formatted.len() <= maximum);
    assert_eq!(formatted.lines().count(), 4097);
    assert!(
        formatted
            .lines()
            .take(4096)
            .all(|row| row.len() < MAX_RECORD_BYTES)
    );
    assert!(formatted.lines().last().unwrap().len() < MAX_SUMMARY_BYTES);
    Ok(())
}

#[test]
fn formatter_rejects_excess_rows_and_insufficient_ceiling() -> TestResult {
    let row = super::schema::widest_row(EVENT, DiagnosticScopeKind::Fixture);
    let too_many = DiagnosticCapture::new(
        vec![row; MAX_SERVER_DIAGNOSTIC_EVENTS + 1],
        DiagnosticLossSummary::default(),
    );
    assert_eq!(
        too_many.format_bounded(),
        Err(DiagnosticRefusal::FormatLimit)
    );
    let one = DiagnosticCapture::new(vec![row], DiagnosticLossSummary::default());
    assert_eq!(
        one.format_with_limit(MAX_RECORD_BYTES + MAX_SUMMARY_BYTES - 1),
        Err(DiagnosticRefusal::FormatLimit)
    );
    assert!(
        one.format_with_limit(MAX_RECORD_BYTES + MAX_SUMMARY_BYTES)
            .is_ok()
    );
    Ok(())
}

#[test]
fn fixed_line_refuses_growth_before_mutating_existing_output() -> TestResult {
    use std::fmt::Write;

    let mut line = FixedLine::<4>::new();
    line.write_str("1234")?;
    assert!(line.write_str("5").is_err());
    assert_eq!(line.as_str()?, "1234");
    let mut empty = FixedLine::<0>::new();
    assert!(empty.write_str("1").is_err());
    assert_eq!(empty.as_str()?, "");
    Ok(())
}

#[test]
fn absent_parent_and_zero_elapsed_have_fixed_numeric_rendering() -> TestResult {
    let (recorder, scope) = fixture()?;
    recorder.record(&scope, EVENT, Duration::ZERO)?;
    let capture = recorder.capture()?;
    assert_eq!(
        capture.format_bounded()?.lines().next().unwrap(),
        "sequence=1 scope=1 parent=0 kind=fixture elapsed_ms=0 event=fixture-started",
    );
    Ok(())
}
