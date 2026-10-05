use super::{fixture::Fixture, *};

#[test]
fn fifo_query_observes_the_preceding_actual_apply() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 2_000, 500)?;
    let observed = (|| -> TestResult<_> {
        fixture.clock.arm();
        let preceding = fixture.queued_apply(send_kind())?;
        fixture.clock.entered()?;
        let query = fixture.queued_query()?;
        fixture.clock.set(2_000);
        fixture.clock.release();
        let applied = preceding.recv_timeout(WAIT)?;
        let state = query.recv_timeout(WAIT)?;
        fixture.clock.set(1_499);
        let regressed = fixture.handle.maintenance_clock_assessment_blocking();
        Ok((
            applied,
            state,
            regressed,
            fixture.handle.last_applied_blocking(),
        ))
    })();
    let cleanup = fixture.finish();
    let (applied, state, regressed, floor) = observed?;
    assert!(cleanup.owner.is_ok());
    assert!(matches!(applied?, domain::CommandOutcome::Sent { .. }));
    assert_eq!(state, Assessment::Ready);
    assert_eq!(regressed, Assessment::Unsafe);
    assert_eq!(floor?, Timestamp::from_millis(2_000));
    Ok(())
}

#[test]
fn cancellation_before_queue_admission_reads_no_clock() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    let observed = (|| -> TestResult<_> {
        let unpolled = fixture.handle.maintenance_clock_assessment();
        drop(unpolled);
        let before = fixture.clock.calls().len();
        fixture.clock.arm();
        let held = fixture.queued_query()?;
        fixture.clock.entered()?;
        for _ in 0..COMMAND_QUEUE_DEPTH {
            let (reply, outcome) = flume::bounded(1);
            fixture
                .handle
                .requests
                .send(Request::LastApplied { reply })
                .map_err(|_| "queue closed")?;
            drop(outcome);
        }
        let full = fixture.handle.requests.len();
        let mut cancelled = Box::pin(fixture.handle.maintenance_clock_assessment());
        let pending = poll_once(cancelled.as_mut()).is_pending();
        drop(cancelled);
        fixture.clock.release();
        let held_result = held.recv_timeout(WAIT)?;
        // This FIFO sentinel completes after every filled request has drained.
        fixture.handle.last_applied_blocking()?;
        Ok((
            before,
            full,
            pending,
            held_result,
            fixture.clock.calls().len(),
            fixture.store.counts().applies,
        ))
    })();
    let cleanup = fixture.finish();
    let (before, full, pending, held, after, applies) = observed?;
    assert!(cleanup.owner.is_ok());
    assert_eq!(before, 0);
    assert_eq!(full, COMMAND_QUEUE_DEPTH);
    assert!(pending);
    assert_eq!(held, Assessment::Ready);
    assert_eq!(after, 1);
    assert_eq!(applies, 0);
    Ok(())
}

#[test]
fn cancellation_after_admission_finishes_read_only() -> TestResult {
    let fixture = Fixture::new(Some(1_000), 1_000, 500)?;
    let observed = (|| -> TestResult<_> {
        let before = fixture.store.bytes()?;
        fixture.clock.arm();
        let mut cancelled = Box::pin(fixture.handle.maintenance_clock_assessment());
        let pending = poll_once(cancelled.as_mut()).is_pending();
        fixture.clock.entered()?;
        drop(cancelled);
        fixture.clock.release();
        let floor = fixture.handle.last_applied_blocking()?;
        Ok((
            pending,
            floor,
            before,
            fixture.store.bytes()?,
            fixture.clock.calls(),
            fixture.store.counts(),
        ))
    })();
    let cleanup = fixture.finish();
    let (pending, floor, before, after, calls, counts) = observed?;
    assert!(cleanup.owner.is_ok());
    assert!(pending);
    assert_eq!(floor, Timestamp::from_millis(1_000));
    assert_eq!(before, after);
    assert_eq!(calls.len(), 1);
    assert_eq!(counts.gets.len(), 2);
    assert!(counts.scans.is_empty());
    assert_eq!((counts.applies, counts.snapshots), (0, 0));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn closed_owner_returns_stopped_without_join_claim() -> TestResult {
    let fixture = Fixture::new(None, 0, 500)?;
    let handle = fixture.handle.clone();
    let cleanup = fixture.finish();
    let blocking = handle.maintenance_clock_assessment_blocking();
    let asynchronous = tokio::time::timeout(WAIT, handle.maintenance_clock_assessment()).await?;
    assert!(cleanup.owner.is_ok());
    assert_eq!(blocking, Assessment::Stopped);
    assert_eq!(asynchronous, Assessment::Stopped);
    Ok(())
}

#[test]
fn owner_unwind_wakes_pending_query_without_sample() -> TestResult {
    let fixture = Fixture::new(None, 0, 500)?;
    let observed = (|| -> TestResult<_> {
        fixture.clock.arm();
        fixture.clock.panic_once();
        let first = fixture.queued_query()?;
        fixture.clock.entered()?;
        let mut pending = Box::pin(fixture.handle.maintenance_clock_assessment());
        let was_pending = poll_once(pending.as_mut()).is_pending();
        fixture.clock.release();
        let disconnected = first.recv_timeout(WAIT).is_err();
        let deadline = std::time::Instant::now() + WAIT;
        let result = loop {
            if let Poll::Ready(value) = poll_once(pending.as_mut()) {
                break value;
            }
            if std::time::Instant::now() >= deadline {
                return Err("pending query did not observe owner loss".into());
            }
            std::thread::yield_now();
        };
        Ok((
            was_pending,
            disconnected,
            result,
            fixture.store.counts(),
            fixture.clock.calls(),
        ))
    })();
    let cleanup = fixture.finish();
    let (pending, disconnected, result, counts, calls) = observed?;
    // The original failed join result stays owned through the actual join above.
    assert!(cleanup.owner.is_err());
    assert!(pending && disconnected);
    assert_eq!(result, Assessment::Stopped);
    assert_eq!(calls.len(), 1);
    assert!(counts.gets.is_empty());
    assert_eq!(counts.applies, 0);
    Ok(())
}

#[test]
fn query_interleaving_leaves_timer_cursors_and_fairness_unchanged() -> TestResult {
    let baseline = Fixture::new(None, 1_000, 500)?;
    let interleaved = Fixture::new(None, 1_000, 500)?;
    let observed = (|| -> TestResult<_> {
        for fixture in [&baseline, &interleaved] {
            let namespace = names().0;
            for index in 0..=crate::MAX_QUEUES_PER_SWEEP {
                fixture.handle.submit_blocking(
                    namespace.clone(),
                    EntityPath::new(format!("queue-{index:04}"))?,
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                )?;
            }
            for index in 0..=crate::MAX_TOPICS_PER_SWEEP {
                fixture.handle.submit_blocking(
                    namespace.clone(),
                    EntityPath::new(format!("topic-{index:04}"))?,
                    CommandKind::CreateTopic {
                        config: TopicConfig::default(),
                    },
                )?;
            }
            fixture.store.reset();
            fixture.store.fail_floor(1);
        }
        let left = TimerWorker::new(&baseline.handle);
        let right = TimerWorker::new(&interleaved.handle);
        let mut reports = Vec::new();
        for _ in 0..4 {
            // Probe once after the single injected sweep failure, not before it.
            let l = left.sweep_once();
            let r = right.sweep_once();
            let assessed = interleaved.handle.maintenance_clock_assessment_blocking();
            reports.push((l, r, assessed));
        }
        Ok((
            reports,
            baseline.store.counts().scans,
            interleaved.store.counts().scans,
            baseline.store.bytes()?,
            interleaved.store.bytes()?,
        ))
    })();
    let left_cleanup = baseline.finish();
    let right_cleanup = interleaved.finish();
    let (reports, left_scans, right_scans, left_bytes, right_bytes) = observed?;
    assert!(left_cleanup.owner.is_ok() && right_cleanup.owner.is_ok());
    assert!(reports[0].0.is_err());
    for (left, right, state) in reports {
        assert_eq!(left, right);
        assert_eq!(state, Assessment::Ready);
    }
    assert_eq!(left_scans, right_scans);
    assert_eq!(left_bytes, right_bytes);
    Ok(())
}
