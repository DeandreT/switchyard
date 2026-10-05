use super::*;

#[tokio::test]
async fn original_before_lease_failure_joins_before_single_rescue() -> TestResult {
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(
        ready_tasks(2).await,
        anchor,
        interrupted_plan(Checkpoint::BeforeLease, Checkpoint::Never),
        &Releases(Vec::new()),
    )
    .await?;
    drop(starter);
    let report = finish(&mut root).await?;
    let progress = observer.snapshot();
    assert_eq!(report.summary().returned, 2);
    assert!(report.summary().original_interrupted);
    assert!(!report.summary().rescue_interrupted && !report.summary().inline_used);
    assert!(progress.original_joined && progress.rescue_joined && progress.report_ready);
    assert!(progress.rescue_submitted && progress.rescue_started);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn original_after_drain_failure_needs_actual_join_but_no_rescue() -> TestResult {
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(
        ready_tasks(2).await,
        anchor,
        interrupted_plan(Checkpoint::AfterDrain, Checkpoint::Never),
        &Releases(Vec::new()),
    )
    .await?;
    drop(starter);
    let report = finish(&mut root).await?;
    assert_eq!(report.summary().returned, 2);
    assert!(report.summary().original_interrupted);
    assert!(report.owners.rescue.is_none());
    assert!(observer.snapshot().original_joined);
    assert!(!observer.snapshot().rescue_submitted);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn borrowed_inline_waiter_drop_restores_the_same_job_after_both_owner_joins() -> TestResult {
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let (tasks, entry) = gated_task(47_usize, &gate).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        mut observer,
    } = admit(
        tasks,
        anchor,
        interrupted_plan(Checkpoint::BeforeLease, Checkpoint::AfterLease),
        &releases,
    )
    .await?;
    drop(starter);
    let mut joining = Box::pin(root.finish());
    let mut early = None;
    let observed = tokio::select! {
        progress = observe(&mut observer, |progress| progress.inline_started) => progress,
        report = joining.as_mut() => {
            early = report;
            Err(std::io::Error::other("inline cleanup reported before blocked child release").into())
        }
    };
    drop(joining);
    let before = observer.snapshot();
    let retained = drops.load(Ordering::Acquire);
    releases.release();
    let report = match early {
        Some(report) => report,
        None => finish(&mut root).await?,
    };
    entry?;
    observed?;
    assert!(before.original_joined && before.rescue_joined);
    assert!(!before.drained && !before.report_ready);
    assert_eq!(retained, 0);
    assert_eq!(report.outcomes[0].as_ref().ok(), Some(&47));
    assert!(report.summary().original_interrupted && report.summary().rescue_interrupted);
    assert!(report.summary().inline_used);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn inline_poll_unwind_returns_job_without_spawning_a_third_owner() -> TestResult {
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let (tasks, entry) = gated_task(53_usize, &gate).await;
    let (anchor, drops) = anchor();
    let plan = ControlPlan {
        checkpoints: [
            Checkpoint::BeforeLease,
            Checkpoint::BeforeLease,
            Checkpoint::AfterLease,
        ],
        ..ControlPlan::default()
    };
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, plan, &releases).await?;
    drop(starter);
    let controls = Arc::clone(&root.shared.job.controls);
    let mut joining = Box::pin(root.finish());
    let deadline = tokio::time::Instant::now() + OBSERVATION_LIMIT;
    let mut unwound = false;
    let mut early = None;
    loop {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll_once(joining.as_mut())))
        {
            Err(_) => {
                unwound = true;
                break;
            }
            Ok(Poll::Ready(report)) => {
                early = report;
                break;
            }
            Ok(Poll::Pending) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    drop(joining);
    let before = observer.snapshot();
    let retained = drops.load(Ordering::Acquire);
    // Failure cleanup must not re-trigger an unconsumed injected checkpoint.
    controls.disarm();
    releases.release();
    let report = match early {
        Some(report) => report,
        None => finish(&mut root).await?,
    };
    entry?;
    assert!(unwound);
    assert!(before.original_joined && before.rescue_joined && before.inline_started);
    assert!(!before.drained && !before.report_ready);
    assert_eq!(retained, 0);
    assert_eq!(report.outcomes[0].as_ref().ok(), Some(&53));
    assert!(report.summary().inline_used);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}
