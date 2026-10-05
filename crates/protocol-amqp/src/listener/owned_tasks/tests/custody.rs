use super::*;

#[tokio::test]
async fn accepted_unpolled_starter_future_drop_queues_the_same_job() -> TestResult {
    let tasks = ready_tasks(3).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, ControlPlan::default(), &Releases(Vec::new())).await?;
    drop(starter.observe());
    let queued = observer.snapshot();
    let report = finish(&mut root).await?;
    assert!(queued.original_submitted);
    assert!(!queued.original_started);
    assert_eq!(report.summary().returned, 3);
    assert!(observer.snapshot().original_joined);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn first_polled_starter_and_borrowed_join_waiter_loss_restore_custody() -> TestResult {
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let (tasks, entry) = gated_task(17_usize, &gate).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        mut observer,
    } = admit(tasks, anchor, ControlPlan::default(), &releases).await?;
    let mut starting = Box::pin(starter.observe());
    let starter_pending = poll_once(starting.as_mut()).is_pending();
    drop(starting);
    let started = observe(&mut observer, |progress| progress.original_started).await;
    let mut joining = Box::pin(root.finish());
    let waiter_pending = poll_once(joining.as_mut()).is_pending();
    drop(joining);
    let retained = drops.load(Ordering::Acquire);
    releases.release();
    let report = finish(&mut root).await?;
    entry?;
    started?;
    assert!(starter_pending && waiter_pending);
    assert_eq!(retained, 0);
    assert_eq!(report.outcomes[0].as_ref().ok(), Some(&17));
    assert!(observer.snapshot().original_joined);
    assert!(!observer.snapshot().rescue_submitted);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn loss_of_every_status_observer_still_leaves_external_actual_awaiter() -> TestResult {
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(
        ready_tasks(4).await,
        anchor,
        ControlPlan::default(),
        &Releases(Vec::new()),
    )
    .await?;
    drop(starter.observe());
    drop(observer);
    let report = finish(&mut root).await?;
    assert_eq!(report.summary().returned, 4);
    assert!(report.owners.original.is_ok());
    assert!(report.owners.rescue.is_none());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn poisoned_custody_locks_preserve_job_handles_and_join_results() -> TestResult {
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let (tasks, entry) = gated_task(31_usize, &gate).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, ControlPlan::default(), &releases).await?;
    let job_poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = root.shared.job.state.lock().unwrap();
        panic!("controlled job custody poison");
    }));
    let handle_poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = root.shared.owners.lock().unwrap();
        panic!("controlled fixed-handle custody poison");
    }));
    drop(starter);
    let mut joining = Box::pin(root.finish());
    let pending = poll_once(joining.as_mut()).is_pending();
    drop(joining);
    releases.release();
    let report = finish(&mut root).await?;
    entry?;
    assert!(job_poison.is_err() && handle_poison.is_err());
    assert!(pending);
    assert_eq!(report.outcomes[0].as_ref().ok(), Some(&31));
    assert!(observer.snapshot().original_joined);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_body_exit_marker_is_not_its_actual_join_barrier() -> TestResult {
    let exit = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&exit)]);
    let (anchor, drops) = anchor();
    let plan = ControlPlan {
        exit_gates: [Some(Arc::clone(&exit)), None],
        ..ControlPlan::default()
    };
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(JoinSet::<usize>::new(), anchor, plan, &releases).await?;
    drop(starter);
    let entry = tokio::time::timeout(OBSERVATION_LIMIT, exit.entered()).await;
    let before = observer.snapshot();
    let mut joining = Box::pin(root.finish());
    let pending = poll_once(joining.as_mut()).is_pending();
    drop(joining);
    let retained = drops.load(Ordering::Acquire);
    releases.release();
    let report = finish(&mut root).await?;
    entry?;
    assert!(before.drained && before.original_exited);
    assert!(!before.original_joined && !before.report_ready);
    assert!(pending);
    assert_eq!(retained, 0);
    assert!(observer.snapshot().original_joined && observer.snapshot().report_ready);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}
