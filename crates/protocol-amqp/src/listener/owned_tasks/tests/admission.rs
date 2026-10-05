use super::*;

#[tokio::test]
async fn accepted_empty_batch_is_inert_until_external_root_drives_it() -> TestResult {
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(
        JoinSet::<usize>::new(),
        anchor,
        ControlPlan::default(),
        &Releases(Vec::new()),
    )
    .await?;
    let before = observer.snapshot();
    let report = finish(&mut root).await?;
    let after = observer.snapshot();
    let summary = report.summary();
    let no_second_report = root.finish().await.is_none();
    // This late starter must not reopen the now-sealed fixed handle slots.
    drop(starter);
    tokio::task::yield_now().await;
    let after_stale_drop = observer.snapshot();
    assert_eq!(before, Progress::default());
    assert_eq!(summary.admitted, 0);
    assert!(after.original_joined && after.report_ready);
    assert!(!after.rescue_submitted && !after.inline_started);
    assert_eq!(after, after_stale_drop);
    assert!(no_second_report);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(report.anchor.drops.load(Ordering::Acquire), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn exact_capacity_retains_every_original_success_value() -> TestResult {
    let tasks = ready_tasks(TASK_LIMIT).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(tasks, anchor, ControlPlan::default(), &Releases(Vec::new())).await?;
    drop(starter);
    let report = finish(&mut root).await?;
    let mut values: Vec<_> = report
        .outcomes
        .iter()
        .filter_map(|result| result.as_ref().ok().copied())
        .collect();
    values.sort_unstable();
    assert_eq!(values, (0..TASK_LIMIT).collect::<Vec<_>>());
    assert_eq!(report.summary().returned, TASK_LIMIT);
    assert_eq!(observer.snapshot().children_joined, TASK_LIMIT);
    assert!(observer.snapshot().original_joined);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn over_capacity_refusal_returns_untouched_set_and_anchor() -> TestResult {
    let tasks = ready_tasks(TASK_LIMIT + 1).await;
    let (anchor, drops) = anchor();
    let refused = match retire(tasks, anchor, Some(Handle::current())) {
        Err(refused) => refused,
        Ok(Accepted {
            mut root, starter, ..
        }) => {
            drop(starter);
            drop(finish(&mut root).await?);
            return Err(std::io::Error::other("over-capacity input was accepted").into());
        }
    };
    let kind = refused.kind;
    let len = refused.tasks.len();
    let anchor_before = drops.load(Ordering::Acquire);
    drain_refused(refused).await;
    assert_eq!(kind, RefusalKind::TaskLimit);
    assert_eq!(len, TASK_LIMIT + 1);
    assert_eq!(anchor_before, 0);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn missing_fallback_refusal_preserves_all_caller_ownership() -> TestResult {
    let tasks = ready_tasks(2).await;
    let (anchor, drops) = anchor();
    let refused = match retire(tasks, anchor, None) {
        Err(refused) => refused,
        Ok(Accepted {
            mut root, starter, ..
        }) => {
            drop(starter);
            drop(finish(&mut root).await?);
            return Err(std::io::Error::other("missing fallback was accepted").into());
        }
    };
    let kind = refused.kind;
    let len = refused.tasks.len();
    drain_refused(refused).await;
    assert_eq!(kind, RefusalKind::NoFallback);
    assert_eq!(len, 2);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn external_anchor_needs_no_global_send_bound() -> TestResult {
    struct LocalAnchor(std::rc::Rc<std::cell::Cell<usize>>);
    impl Drop for LocalAnchor {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let drops = std::rc::Rc::new(std::cell::Cell::new(0));
    let Accepted {
        mut root, starter, ..
    } = admit(
        ready_tasks(1).await,
        LocalAnchor(std::rc::Rc::clone(&drops)),
        ControlPlan::default(),
        &Releases(Vec::new()),
    )
    .await?;
    drop(starter);
    let report = finish(&mut root).await?;
    assert_eq!(report.summary().returned, 1);
    assert_eq!(drops.get(), 0);
    drop(report);
    assert_eq!(drops.get(), 1);
    Ok(())
}
