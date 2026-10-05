use super::*;

async fn destructor_case(panic_payload: bool) -> TestResult {
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let dangerous = Arc::new(AtomicUsize::new(0));
    let ordinary = Arc::new(AtomicUsize::new(0));
    let (tasks, entry) = probe_tasks(panic_payload, &gate, &dangerous, &ordinary).await;
    let (anchor, anchor_drops) = anchor();
    let Accepted {
        mut root,
        starter,
        mut observer,
    } = admit(
        tasks,
        anchor,
        interrupted_plan(Checkpoint::AfterOneJoin, Checkpoint::AfterLease),
        &releases,
    )
    .await?;
    drop(starter.observe());
    let mut joining = Box::pin(root.finish());
    let mut early = None;
    let observed = tokio::select! {
        progress = observe(&mut observer, |progress| progress.inline_started) => progress,
        report = joining.as_mut() => {
            early = report;
            Err(std::io::Error::other("payload case finished before blocked child release").into())
        }
    };
    drop(joining);
    let before = observer.snapshot();
    let dangerous_before = dangerous.load(Ordering::Acquire);
    let ordinary_before = ordinary.load(Ordering::Acquire);
    let anchor_before = anchor_drops.load(Ordering::Acquire);
    drop(observer);
    releases.release();
    let report = match early {
        Some(report) => report,
        None => finish(&mut root).await?,
    };
    let summary = report.summary();
    let complete = *root.shared.job.progress.borrow();
    let report_drop = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(report)));
    // All error propagation/assertions happen after actual joins and disposal.
    entry?;
    observed?;
    assert!(before.original_joined && before.rescue_joined && before.inline_started);
    assert_eq!(before.children_joined, 1);
    assert!(!before.drained && !before.report_ready);
    assert_eq!(
        (dangerous_before, ordinary_before, anchor_before),
        (0, 0, 0)
    );
    assert_eq!(summary.admitted, 2);
    assert_eq!(summary.returned, if panic_payload { 1 } else { 2 });
    assert_eq!(summary.panicked, usize::from(panic_payload));
    assert!(summary.original_interrupted && summary.rescue_interrupted && summary.inline_used);
    assert!(complete.original_joined && complete.rescue_joined && complete.report_ready);
    assert!(report_drop.is_err());
    assert_eq!(dangerous.load(Ordering::Acquire), 1);
    assert_eq!(ordinary.load(Ordering::Acquire), 1);
    assert_eq!(anchor_drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn panicking_t_destructor_waits_for_siblings_and_both_owner_joins() -> TestResult {
    destructor_case(false).await
}

#[tokio::test]
async fn panicking_joinerror_payload_waits_for_siblings_and_both_owner_joins() -> TestResult {
    destructor_case(true).await
}

#[tokio::test]
async fn mixed_child_results_preserve_original_t_and_typed_panic_identity() -> TestResult {
    struct ChildPanic(Arc<()>);
    let identity = Arc::new(());
    let mut tasks = ready_tasks(1).await;
    let (send, receive) = tokio::sync::oneshot::channel();
    let panic_identity = Arc::clone(&identity);
    tasks.spawn(async move {
        let _ = send.send(());
        std::panic::panic_any(ChildPanic(panic_identity));
    });
    let _ = receive.await;
    tasks.spawn(async { std::future::pending::<usize>().await });
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, ControlPlan::default(), &Releases(Vec::new())).await?;
    drop(starter);
    let report = finish(&mut root).await?;
    let summary = report.summary();
    let Report {
        outcomes,
        owners,
        anchor,
        ..
    } = report;
    let mut same_identity = false;
    let mut value = None;
    for outcome in outcomes {
        match outcome {
            Ok(original) => value = Some(original),
            Err(error) if error.is_panic() => {
                let payload = error.into_panic();
                same_identity = payload
                    .downcast_ref::<ChildPanic>()
                    .is_some_and(|payload| Arc::ptr_eq(&payload.0, &identity));
            }
            Err(_) => {}
        }
    }
    drop(owners);
    drop(anchor);
    assert_eq!(summary.admitted, 3);
    assert_eq!(
        (summary.returned, summary.panicked, summary.cancelled),
        (1, 1, 1)
    );
    assert_eq!(value, Some(0));
    assert!(same_identity);
    assert!(observer.snapshot().original_joined && !observer.snapshot().rescue_submitted);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_destructor_marker_cannot_replace_its_actual_handle_join() -> TestResult {
    struct FinalDrop {
        marker: Arc<AtomicUsize>,
        gate: Arc<BlockingGate>,
    }
    impl Drop for FinalDrop {
        fn drop(&mut self) {
            self.marker.fetch_add(1, Ordering::AcqRel);
            self.gate.block();
        }
    }
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let marker = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    let final_drop = FinalDrop {
        marker: Arc::clone(&marker),
        gate: Arc::clone(&gate),
    };
    tasks.spawn(async move {
        let _final_drop = final_drop;
        61_usize
    });
    let entry = tokio::time::timeout(OBSERVATION_LIMIT, gate.entered()).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, ControlPlan::default(), &releases).await?;
    drop(starter);
    let before = observer.snapshot();
    let mut joining = Box::pin(root.finish());
    let pending = poll_once(joining.as_mut()).is_pending();
    drop(joining);
    let marked = marker.load(Ordering::Acquire);
    let retained = drops.load(Ordering::Acquire);
    releases.release();
    let report = finish(&mut root).await?;
    entry?;
    assert_eq!(marked, 1);
    assert!(pending && !before.drained && !before.report_ready);
    assert_eq!(retained, 0);
    assert_eq!(report.summary().admitted, 1);
    assert!(observer.snapshot().original_joined);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}
