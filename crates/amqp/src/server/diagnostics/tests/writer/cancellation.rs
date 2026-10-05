use super::*;

#[tokio::test(start_paused = true)]
async fn dropped_write_and_flush_record_once_without_mutating_activity_failure() -> TestResult {
    for (mode, terminal) in [
        (Mode::PendingWrite, Phase::DroppedWrite),
        (Mode::PendingFlush, Phase::DroppedFlush),
    ] {
        let recorder = ServerDiagnosticRecorder::try_new()?;
        let mut snapshots = Vec::new();
        for observed in [false, true] {
            let (mut output, control, activity) =
                writer(mode, observed.then_some(&recorder), None)?;
            let frame = heartbeat();
            let mut writing = Box::pin(output.write_frame(&frame));
            assert!(poll_once(writing.as_mut()).is_pending());
            let before = control.snapshot();
            drop(writing);
            assert_eq!(control.snapshot(), before);
            snapshots.push(before);
            assert!(activity.is_tainted());
            assert!(!activity.is_closing());
            let mut timeout =
                Box::pin(activity.timeout(ConnectionOptions::default().idle_timeout_millis(0), 0));
            assert!(poll_once(timeout.as_mut()).is_pending());
            drop(timeout);
            let error = output
                .write_frame(&frame)
                .await
                .expect_err("taint retained");
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(&control.snapshot(), snapshots.last().unwrap());
        }
        assert_eq!(snapshots[0], snapshots[1]);
        let mut expected = vec![Phase::PreflightStarted, Phase::WriteStarted];
        if terminal == Phase::DroppedFlush {
            expected.push(Phase::WriteAllDone);
        }
        expected.extend([terminal, Phase::PreflightStarted, Phase::PreflightRefused]);
        assert_eq!(phases(&recorder)?, expected);
        assert_eq!(
            phases(&recorder)?
                .iter()
                .filter(|phase| **phase == terminal)
                .count(),
            1
        );
    }
    Ok(())
}
