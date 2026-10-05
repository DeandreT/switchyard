use super::*;

struct DeadHost {
    task: tokio::task::JoinHandle<Option<Progress>>,
    first_poll_pending: bool,
}

fn alternate_host<T: Send + 'static>(
    starter: Starter<T>,
    started: Option<std::sync::mpsc::Sender<bool>>,
    shutdown: Option<std::sync::mpsc::Receiver<()>>,
) -> std::io::Result<DeadHost> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = runtime.spawn(async move {
        let mut observing = Box::pin(starter.observe());
        let first_poll_pending = poll_once(observing.as_mut()).is_pending();
        let _ = send.send(first_poll_pending);
        observing.await
    });
    let first_poll_pending = runtime.block_on(receive).unwrap_or(false);
    if let Some(started) = started {
        let _ = started.send(first_poll_pending);
    }
    if let Some(shutdown) = shutdown {
        let _ = shutdown.recv_timeout(OBSERVATION_LIMIT);
    }
    // Keep B's actual handle after runtime destruction, then await it on A.
    drop(runtime);
    Ok(DeadHost {
        task,
        first_poll_pending,
    })
}

#[tokio::test]
async fn distinct_current_runtime_death_before_original_poll_keeps_fallback_join() -> TestResult {
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        observer,
    } = admit(
        ready_tasks(3).await,
        anchor,
        ControlPlan::default(),
        &Releases(Vec::new()),
    )
    .await?;
    // A is current-thread and does not yield until B and its OS host exit.
    let host_result = std::thread::spawn(move || alternate_host(starter, None, None)).join();
    let before = observer.snapshot();
    let report_result = finish(&mut root).await;
    let (host_ok, first_pending, actual_b_cancelled) = match host_result {
        Ok(Ok(host)) => {
            let joined = host.task.await;
            (
                true,
                host.first_poll_pending,
                joined.as_ref().is_err_and(JoinError::is_cancelled),
            )
        }
        _ => (false, false, false),
    };
    let report = report_result?;
    assert!(host_ok && first_pending && actual_b_cancelled);
    assert!(before.original_submitted && !before.original_started);
    assert_eq!(report.summary().returned, 3);
    assert!(observer.snapshot().original_joined);
    assert!(!observer.snapshot().rescue_submitted);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_current_runtime_death_during_cleanup_cannot_cancel_fallback_worker() -> TestResult
{
    let gate = Arc::new(BlockingGate::default());
    let releases = Releases(vec![Arc::clone(&gate)]);
    let (tasks, entry) = gated_task(71_usize, &gate).await;
    let (anchor, drops) = anchor();
    let Accepted {
        mut root,
        starter,
        mut observer,
    } = admit(tasks, anchor, ControlPlan::default(), &releases).await?;
    let (started_send, started_receive) = std::sync::mpsc::channel();
    let (shutdown_send, shutdown_receive) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        alternate_host(starter, Some(started_send), Some(shutdown_receive))
    });
    let host_started = started_receive.recv_timeout(OBSERVATION_LIMIT);
    let worker_started = observe(&mut observer, |progress| progress.original_started).await;
    let mut joining = Box::pin(root.finish());
    let pending = poll_once(joining.as_mut()).is_pending();
    drop(joining);
    let _ = shutdown_send.send(());
    let host_result = host.join();
    let after_b_death = observer.snapshot();
    let retained = drops.load(Ordering::Acquire);
    releases.release();
    let report_result = finish(&mut root).await;
    let (host_ok, first_pending, actual_b_cancelled) = match host_result {
        Ok(Ok(host)) => {
            let joined = host.task.await;
            (
                true,
                host.first_poll_pending,
                joined.as_ref().is_err_and(JoinError::is_cancelled),
            )
        }
        _ => (false, false, false),
    };
    let report = report_result?;
    entry?;
    worker_started?;
    assert_eq!(host_started.ok(), Some(true));
    assert!(host_ok && first_pending && actual_b_cancelled);
    assert!(pending && after_b_death.original_started);
    assert!(!after_b_death.original_joined && !after_b_death.drained);
    assert_eq!(retained, 0);
    assert_eq!(report.outcomes[0].as_ref().ok(), Some(&71));
    assert!(observer.snapshot().original_joined && !observer.snapshot().rescue_submitted);
    drop(report);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    Ok(())
}
