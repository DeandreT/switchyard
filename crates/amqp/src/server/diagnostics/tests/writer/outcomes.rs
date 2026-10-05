use super::*;
use crate::Close;

#[tokio::test(start_paused = true)]
async fn write_and_flush_timeouts_keep_default_deadline_activity_and_bytes() -> TestResult {
    for (mode, terminal) in [
        (Mode::PendingWrite, Phase::TimeoutWrite),
        (Mode::PendingFlush, Phase::TimeoutFlush),
    ] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let mut snapshots = Vec::new();
        for observed in [false, true] {
            let (mut output, control, activity) =
                writer(mode, observed.then_some(&recorder), None)?;
            let options = ConnectionOptions::default().write_timeout(Duration::from_millis(25));
            output.configure_activity(options, 0, activity.clone());
            let started = Instant::now();
            assert_eq!(
                output
                    .write_frame(&heartbeat())
                    .await
                    .expect_err("bounded write")
                    .kind(),
                io::ErrorKind::TimedOut
            );
            assert_eq!(Instant::now() - started, Duration::from_millis(25));
            assert!(activity.is_tainted());
            assert_eq!(
                activity.timeout(options, 0).await,
                ActivityTimeout::WriteFailed
            );
            let before = control.snapshot();
            assert!(
                output
                    .write_amqp(0, Performative::Close(Close::default()), Vec::new())
                    .await
                    .is_err()
            );
            assert_eq!(control.snapshot(), before);
            snapshots.push(before);
        }
        assert_eq!(snapshots[0], snapshots[1]);
        let mut expected = vec![Phase::PreflightStarted, Phase::WriteStarted];
        if terminal == Phase::TimeoutFlush {
            expected.push(Phase::WriteAllDone);
        }
        expected.extend([terminal, Phase::PreflightStarted, Phase::PreflightRefused]);
        assert_eq!(phases(&recorder)?, expected);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn underlying_errors_including_timed_out_keep_original_typed_identity() -> TestResult {
    for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::TimedOut] {
        for (mode, terminal) in [
            (Mode::ErrorWrite(kind), Phase::ErrorWrite),
            (Mode::ErrorFlush(kind), Phase::ErrorFlush),
        ] {
            let recorder = ServerDiagnosticRecorder::try_new()?;
            let mut snapshots = Vec::new();
            for observed in [false, true] {
                let (mut output, control, activity) =
                    writer(mode, observed.then_some(&recorder), None)?;
                let error = output
                    .write_frame(&heartbeat())
                    .await
                    .expect_err("underlying error");
                assert_native_error(&error, kind, &control);
                assert!(activity.is_tainted());
                assert_eq!(
                    activity.timeout(ConnectionOptions::default(), 0).await,
                    ActivityTimeout::WriteFailed
                );
                snapshots.push(control.snapshot());
            }
            assert_eq!(snapshots[0], snapshots[1]);
            let mut expected = vec![Phase::PreflightStarted, Phase::WriteStarted];
            if terminal == Phase::ErrorFlush {
                expected.push(Phase::WriteAllDone);
            }
            expected.push(terminal);
            assert_eq!(phases(&recorder)?, expected);
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn ready_late_flush_records_completion_but_never_acceptance() -> TestResult {
    for elapsed in [Duration::from_millis(25), Duration::from_millis(30)] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let mut snapshots = Vec::new();
        for observed in [false, true] {
            let control = Arc::new(Control::default());
            let io = LateWriter {
                control: control.clone(),
                flush: Box::pin(tokio::time::sleep(elapsed)),
            };
            let mut output = if observed {
                FrameWriter::new_with_diagnostics(io, 512, recorder.clone(), None)?
            } else {
                FrameWriter::new(io, 512)?
            };
            let activity = Activity::new();
            let options = ConnectionOptions::default().write_timeout(Duration::from_millis(25));
            output.configure_activity(options, 0, activity.clone());
            let frame = heartbeat();
            let mut writing = Box::pin(output.write_frame(&frame));
            assert!(poll_once(writing.as_mut()).is_pending());
            advance(elapsed).await;
            assert_eq!(
                writing.await.expect_err("strict late flush").kind(),
                io::ErrorKind::TimedOut
            );
            assert!(activity.is_tainted());
            assert_eq!(
                activity.timeout(options, 0).await,
                ActivityTimeout::WriteFailed
            );
            snapshots.push(control.snapshot());
        }
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(
            phases(&recorder)?,
            [
                Phase::PreflightStarted,
                Phase::WriteStarted,
                Phase::WriteAllDone,
                Phase::FlushDone,
                Phase::TimeoutAfterFlush
            ]
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn peer_remaining_time_and_close_exception_preserve_existing_policy() -> TestResult {
    for (close, local, expected) in [
        (false, Duration::from_secs(5), Duration::from_millis(100)),
        (true, Duration::from_millis(25), Duration::from_millis(25)),
        (
            true,
            Duration::from_secs(50),
            crate::server::DEFAULT_CLOSE_TIMEOUT,
        ),
    ] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let mut snapshots = Vec::new();
        for observed in [false, true] {
            let (mut output, control, activity) =
                writer(Mode::PendingFlush, observed.then_some(&recorder), None)?;
            let options = ConnectionOptions::default().write_timeout(local);
            output.configure_activity(options, 1000, activity.clone());
            advance(Duration::from_millis(if close { 1000 } else { 900 })).await;
            let frame = if close {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Close(Close::default())),
                    payload: Vec::new(),
                }
            } else {
                heartbeat()
            };
            let started = Instant::now();
            assert_eq!(
                output
                    .write_frame(&frame)
                    .await
                    .expect_err("remaining deadline")
                    .kind(),
                io::ErrorKind::TimedOut
            );
            assert_eq!(Instant::now() - started, expected);
            assert_eq!(activity.is_closing(), close);
            assert!(activity.is_tainted());
            assert_eq!(control.snapshot().bytes, codec::encode_frame(&frame)?);
            assert_eq!(
                activity.timeout(options, 1000).await,
                ActivityTimeout::WriteFailed
            );
            snapshots.push(control.snapshot());
        }
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(
            phases(&recorder)?,
            [
                Phase::PreflightStarted,
                Phase::WriteStarted,
                Phase::WriteAllDone,
                Phase::TimeoutFlush
            ]
        );
    }
    Ok(())
}
