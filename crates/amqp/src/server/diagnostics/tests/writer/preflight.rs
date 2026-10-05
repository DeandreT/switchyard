use super::*;
use crate::{Close, SaslMechanisms, SaslPerformative};
use serde_amqp::primitives::Symbol;

fn overflow(error: &io::Error) -> Option<FrameWriteError> {
    error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<FrameWriteError>())
        .copied()
}

#[tokio::test(start_paused = true)]
async fn peer_and_codec_refusals_keep_typed_causes_without_socket_polls() -> TestResult {
    let invalid = Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
        mechanisms: vec![Symbol::from("non-ascii-\u{e9}")],
    }));
    for (frame, maximum, typed) in [
        (
            sized_transfer(513),
            512,
            Some(FrameWriteError {
                actual: 513,
                maximum: 512,
            }),
        ),
        (sized_transfer(codec::MAX_FRAME_SIZE + 1), u32::MAX, None),
        (invalid, 512, None),
    ] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let (mut observed, control, activity) =
            writer_with_maximum(Mode::Healthy, maximum, Some(&recorder), None)?;
        let (mut plain, plain_control, plain_activity) =
            writer_with_maximum(Mode::Healthy, maximum, None, None)?;
        let error = observed
            .write_frame(&frame)
            .await
            .expect_err("preflight refusal");
        let original = plain
            .write_frame(&frame)
            .await
            .expect_err("default refusal");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.kind(), original.kind());
        assert_eq!(overflow(&error), typed);
        assert_eq!(overflow(&original), typed);
        assert_eq!(control.snapshot(), WireSnapshot::default());
        assert_eq!(control.snapshot(), plain_control.snapshot());
        assert!(!activity.is_tainted());
        assert!(!plain_activity.is_tainted());
        assert_eq!(
            phases(&recorder)?,
            [Phase::PreflightStarted, Phase::PreflightRefused]
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn preflight_limit_refusal_keeps_later_valid_write_healthy() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let (mut observed, control, activity) = writer(Mode::Healthy, Some(&recorder), None)?;
    let error = observed
        .write_frame(&sized_transfer(513))
        .await
        .expect_err("one extra byte");
    assert_eq!(
        overflow(&error),
        Some(FrameWriteError {
            actual: 513,
            maximum: 512
        })
    );
    let frame = sized_transfer(512);
    observed.write_frame(&frame).await?;
    assert_eq!(control.snapshot().bytes, codec::encode_frame(&frame)?);
    assert_eq!(control.snapshot().writes, 1);
    assert_eq!(control.snapshot().flushes, 1);
    assert!(!activity.is_tainted());
    let mut expected = vec![Phase::PreflightStarted, Phase::PreflightRefused];
    expected.extend(SUCCESS);
    assert_eq!(phases(&recorder)?, expected);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn tainted_preflight_preserves_activity_and_never_appends_close() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let frame = Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Close(Close::default())),
        payload: Vec::new(),
    };
    for observed in [false, true] {
        let (mut output, control, activity) =
            writer(Mode::Healthy, observed.then_some(&recorder), None)?;
        activity.begin_write(false);
        let error = output
            .write_frame(&frame)
            .await
            .expect_err("tainted transport");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(activity.is_tainted());
        assert!(!activity.is_closing());
        assert_eq!(control.snapshot(), WireSnapshot::default());
        let mut timeout =
            Box::pin(activity.timeout(ConnectionOptions::default().idle_timeout_millis(0), 0));
        assert!(poll_once(timeout.as_mut()).is_pending());
    }
    assert_eq!(
        phases(&recorder)?,
        [Phase::PreflightStarted, Phase::PreflightRefused]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_and_unrepresentable_deadline_refusals_do_not_begin_writes() -> TestResult {
    for unrepresentable in [false, true] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let (mut observed, control, activity) = writer(Mode::Healthy, Some(&recorder), None)?;
        let (mut plain, plain_control, plain_activity) = writer(Mode::Healthy, None, None)?;
        let options = if unrepresentable {
            ConnectionOptions::default().write_timeout(Duration::MAX)
        } else {
            ConnectionOptions::default()
        };
        let peer_idle = if unrepresentable { 0 } else { 1000 };
        observed.configure_activity(options, peer_idle, activity.clone());
        plain.configure_activity(options, peer_idle, plain_activity.clone());
        if !unrepresentable {
            advance(Duration::from_millis(1000)).await;
        }
        let error = observed
            .write_frame(&heartbeat())
            .await
            .expect_err("deadline refusal");
        let original = plain
            .write_frame(&heartbeat())
            .await
            .expect_err("default deadline refusal");
        assert_eq!(error.kind(), original.kind());
        assert_eq!(
            error.kind(),
            if unrepresentable {
                io::ErrorKind::InvalidInput
            } else {
                io::ErrorKind::TimedOut
            }
        );
        assert!(!activity.is_tainted());
        assert!(!plain_activity.is_tainted());
        assert_eq!(control.snapshot(), WireSnapshot::default());
        assert_eq!(control.snapshot(), plain_control.snapshot());
        assert_eq!(
            phases(&recorder)?,
            [Phase::PreflightStarted, Phase::PreflightRefused]
        );
    }
    Ok(())
}
