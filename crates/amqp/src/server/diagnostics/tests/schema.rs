use super::*;

fn all_events() -> Vec<DiagnosticEvent> {
    let mut output = Vec::new();
    for boundary in [
        DiagnosticFixtureBoundary::Started,
        DiagnosticFixtureBoundary::ClientEnded,
        DiagnosticFixtureBoundary::CleanupStarted,
        DiagnosticFixtureBoundary::CleanupEnded,
    ] {
        output.push(DiagnosticEvent::Fixture(boundary));
    }
    for phase in [
        DiagnosticConnectionPhase::Accepted,
        DiagnosticConnectionPhase::AdmissionRefused,
        DiagnosticConnectionPhase::HandshakeStarted,
        DiagnosticConnectionPhase::HandshakeSucceeded,
        DiagnosticConnectionPhase::HandshakeFailed,
    ] {
        output.push(DiagnosticEvent::Connection(phase));
    }
    for frame in [
        DiagnosticFrameClass::Heartbeat,
        DiagnosticFrameClass::Open,
        DiagnosticFrameClass::Begin,
        DiagnosticFrameClass::Attach,
        DiagnosticFrameClass::Flow,
        DiagnosticFrameClass::Transfer,
        DiagnosticFrameClass::Disposition,
        DiagnosticFrameClass::TransactionalDisposition,
        DiagnosticFrameClass::Detach,
        DiagnosticFrameClass::End,
        DiagnosticFrameClass::Close,
        DiagnosticFrameClass::Other,
    ] {
        output.push(DiagnosticEvent::DecodedFrame(frame));
        output.push(DiagnosticEvent::ActorDispatch(frame));
    }
    for reason in [
        DiagnosticEndClass::Returned,
        DiagnosticEndClass::Cancelled,
        DiagnosticEndClass::Eof,
        DiagnosticEndClass::ReadError,
        DiagnosticEndClass::DecodeError,
        DiagnosticEndClass::FrameError,
        DiagnosticEndClass::CommandError,
        DiagnosticEndClass::ReceiveIdle,
        DiagnosticEndClass::PeerIdle,
        DiagnosticEndClass::WriteFailed,
        DiagnosticEndClass::CloseTimeout,
        DiagnosticEndClass::Panicked,
        DiagnosticEndClass::Other,
    ] {
        output.push(DiagnosticEvent::Ended(reason));
    }
    for phase in [
        DiagnosticRetirementPhase::Installed,
        DiagnosticRetirementPhase::CollectorReceived,
        DiagnosticRetirementPhase::EventPublished,
        DiagnosticRetirementPhase::Admitted,
        DiagnosticRetirementPhase::Refused,
        DiagnosticRetirementPhase::StagingAccepted,
        DiagnosticRetirementPhase::StagingRefused,
        DiagnosticRetirementPhase::ProvisionalRequested,
        DiagnosticRetirementPhase::Prepared,
        DiagnosticRetirementPhase::PreparationRefused,
        DiagnosticRetirementPhase::ReplyPublished,
        DiagnosticRetirementPhase::ReplyLost,
        DiagnosticRetirementPhase::OwnerClosed,
        DiagnosticRetirementPhase::Obsolete,
    ] {
        output.push(DiagnosticEvent::Retirement(phase));
    }
    for phase in [
        DiagnosticWriterPhase::PreflightStarted,
        DiagnosticWriterPhase::PreflightRefused,
        DiagnosticWriterPhase::WriteStarted,
        DiagnosticWriterPhase::WriteAllDone,
        DiagnosticWriterPhase::FlushDone,
        DiagnosticWriterPhase::WriteAccepted,
        DiagnosticWriterPhase::TimeoutWrite,
        DiagnosticWriterPhase::TimeoutFlush,
        DiagnosticWriterPhase::TimeoutAfterFlush,
        DiagnosticWriterPhase::ErrorWrite,
        DiagnosticWriterPhase::ErrorFlush,
        DiagnosticWriterPhase::DroppedWrite,
        DiagnosticWriterPhase::DroppedFlush,
    ] {
        output.push(DiagnosticEvent::Writer(phase));
    }
    for phase in [
        DiagnosticTaskPhase::Reserved,
        DiagnosticTaskPhase::Spawned,
        DiagnosticTaskPhase::AbortRequested,
        DiagnosticTaskPhase::BodyEnded,
        DiagnosticTaskPhase::ActuallyJoined,
        DiagnosticTaskPhase::SupervisorInterrupted,
        DiagnosticTaskPhase::CapacityRefused,
    ] {
        output.push(DiagnosticEvent::Task(phase));
    }
    output
}

pub(super) fn widest_row(event: DiagnosticEvent, kind: DiagnosticScopeKind) -> DiagnosticRecord {
    DiagnosticRecord {
        sequence: u64::MAX,
        scope: u64::MAX,
        parent: Some(u64::MAX),
        kind,
        elapsed_millis: u64::MAX,
        event,
    }
}

#[test]
fn every_schema_family_and_label_fits_the_maximal_numeric_row() -> TestResult {
    let events = all_events();
    assert_eq!(events.len(), 80);
    let mut labels = std::collections::HashSet::new();
    for event in events {
        assert!(labels.insert(event.label()));
        for kind in [
            DiagnosticScopeKind::Fixture,
            DiagnosticScopeKind::Listener,
            DiagnosticScopeKind::Connection,
            DiagnosticScopeKind::Actor,
            DiagnosticScopeKind::Reader,
            DiagnosticScopeKind::Session,
            DiagnosticScopeKind::Collector,
            DiagnosticScopeKind::Retirement,
            DiagnosticScopeKind::Write,
        ] {
            let capture = DiagnosticCapture::new(
                vec![widest_row(event, kind)],
                DiagnosticLossSummary::default(),
            );
            let formatted = capture.format_bounded()?;
            let row = formatted.lines().next().unwrap();
            assert!(row.len() < super::super::capture::MAX_RECORD_BYTES);
            assert!(formatted.is_ascii());
            assert!(row.ends_with(event.label()));
        }
    }
    Ok(())
}

#[test]
fn fixed_refusals_have_no_source_or_unbounded_private_rendering() {
    use std::error::Error;

    for refusal in [
        DiagnosticRefusal::Allocation,
        DiagnosticRefusal::Contended,
        DiagnosticRefusal::Poisoned,
        DiagnosticRefusal::Full,
        DiagnosticRefusal::OrdinalExhausted,
        DiagnosticRefusal::ForeignScope,
        DiagnosticRefusal::FormatLimit,
    ] {
        assert!(refusal.source().is_none());
        assert!(refusal.to_string().is_ascii());
        assert!(refusal.to_string().len() < 80);
        assert!(format!("{refusal:?}").len() < 32);
    }
}

#[test]
fn typed_capture_and_debug_never_render_external_sentinel_content() -> TestResult {
    let private = [
        "SECRET-name",
        "SECRET-address",
        "SECRET-body",
        "SECRET-tag",
        "SECRET-token",
        "SECRET-TxID",
        "SECRET-description",
        "SECRET-password",
        "SECRET-error",
        "SECRET-frame",
    ];
    let (recorder, scope) = fixture()?;
    for event in all_events() {
        recorder.record(&scope, event, Duration::ZERO)?;
    }
    let capture = recorder.capture()?;
    let rendering = format!(
        "{}\n{recorder:?}\n{scope:?}\n{capture:?}",
        capture.format_bounded()?
    );
    assert!(rendering.is_ascii());
    for sentinel in private {
        assert!(!rendering.contains(sentinel));
    }
    assert!(!rendering.contains("0x"));
    assert!(format!("{recorder:?}").len() < 80);
    assert!(format!("{scope:?}").len() < 120);
    assert!(format!("{capture:?}").len() < 300);
    Ok(())
}
