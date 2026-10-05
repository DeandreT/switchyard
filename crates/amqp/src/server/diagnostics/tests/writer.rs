use super::*;
use crate::server::{
    ConnectionOptions,
    frame_writer::{FrameWriteError, FrameWriter},
    idle::{Activity, ActivityTimeout},
};
use crate::{Frame, Performative, codec};
use DiagnosticWriterPhase as Phase;
use std::{future::Future, io, pin::Pin, task::Poll};
use tokio::time::{Instant, advance};

mod cancellation;
mod fixture;
mod losses;
mod outcomes;
mod preflight;
mod privacy;
use fixture::*;

const SUCCESS: [Phase; 5] = [
    Phase::PreflightStarted,
    Phase::WriteStarted,
    Phase::WriteAllDone,
    Phase::FlushDone,
    Phase::WriteAccepted,
];

fn phases(recorder: &ServerDiagnosticRecorder) -> Result<Vec<Phase>, DiagnosticRefusal> {
    Ok(recorder
        .capture()?
        .records()
        .iter()
        .filter_map(|record| match record.event() {
            DiagnosticEvent::Writer(phase) => Some(phase),
            _ => None,
        })
        .collect())
}

#[tokio::test(start_paused = true)]
async fn default_and_bound_unpolled_writes_are_inert() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let frame = heartbeat();
    let (mut default, default_control, default_activity) = writer(Mode::Healthy, None, None)?;
    drop(default.write_frame(&frame));
    let (mut bound, control, activity) = writer(Mode::Healthy, Some(&recorder), None)?;
    let before_scope = recorder.0.last_scope.load(Ordering::Relaxed);
    drop(bound.write_frame(&frame));
    advance(Duration::from_millis(7)).await;
    assert_eq!(default_control.snapshot(), control.snapshot());
    assert_eq!(control.snapshot(), WireSnapshot::default());
    assert!(!default_activity.is_tainted());
    assert!(!activity.is_tainted());
    assert!(!default_activity.is_closing());
    assert!(!activity.is_closing());
    assert_eq!(recorder.0.last_scope.load(Ordering::Relaxed), before_scope);
    assert!(recorder.capture()?.records().is_empty());
    assert_eq!(recorder.loss_summary(), DiagnosticLossSummary::default());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn healthy_frames_match_default_bytes_with_fresh_local_scopes() -> TestResult {
    let (recorder, parent) = super::fixture()?;
    let (mut bound, control, activity) = writer(Mode::Healthy, Some(&recorder), Some(&parent))?;
    let (mut default, plain, plain_activity) = writer(Mode::Healthy, None, None)?;
    let frames = [
        heartbeat(),
        control_frame(),
        transfer(b"PRIVATE_BODY".to_vec()),
    ];
    advance(Duration::from_millis(11)).await;
    for frame in &frames {
        default.write_frame(frame).await?;
        bound.write_frame(frame).await?;
    }
    assert_eq!(control.snapshot(), plain.snapshot());
    assert_eq!(control.snapshot().writes, 3);
    assert_eq!(control.snapshot().flushes, 3);
    assert_eq!(activity.is_tainted(), plain_activity.is_tainted());
    assert_eq!(activity.is_closing(), plain_activity.is_closing());
    let capture = recorder.capture()?;
    assert_eq!(capture.records().len(), 15);
    let mut previous = parent.ordinal;
    for chunk in capture.records().chunks_exact(5) {
        assert!(chunk[0].scope_ordinal() > previous);
        previous = chunk[0].scope_ordinal();
        for (record, phase) in chunk.iter().zip(SUCCESS) {
            assert_eq!(record.event(), DiagnosticEvent::Writer(phase));
            assert_eq!(record.scope_ordinal(), previous);
            assert_eq!(record.parent_ordinal(), Some(parent.ordinal));
            assert_eq!(record.scope_kind(), DiagnosticScopeKind::Write);
            assert_eq!(record.elapsed_millis(), 11);
        }
    }
    assert_eq!(recorder.loss_summary(), DiagnosticLossSummary::default());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn write_amqp_delegates_once_and_direct_encoding_remains_unobserved() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let (mut bound, control, _) = writer(Mode::Healthy, Some(&recorder), None)?;
    let frame = transfer(b"PRIVATE_BODY".to_vec());
    let expected = bound.encoded_frame(&frame)?;
    assert!(recorder.capture()?.records().is_empty());
    assert_eq!(recorder.0.last_scope.load(Ordering::Relaxed), 0);
    let Frame::Amqp {
        channel,
        performative,
        payload,
    } = frame
    else {
        return Err("AMQP fixture expected".into());
    };
    bound
        .write_amqp(channel, performative.ok_or("transfer expected")?, payload)
        .await?;
    assert_eq!(control.snapshot().bytes, expected);
    assert_eq!(control.snapshot().writes, 1);
    assert_eq!(control.snapshot().flushes, 1);
    assert_eq!(phases(&recorder)?, SUCCESS);
    assert_eq!(recorder.0.last_scope.load(Ordering::Relaxed), 1);
    Ok(())
}
