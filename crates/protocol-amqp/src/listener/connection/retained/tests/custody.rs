use super::*;

async fn pending_shutdown(fault: bool) -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let primary = Arc::new(PayloadWitness::default());
    h.error(primary.clone());
    let panic = Arc::new(PayloadWitness::default());
    if fault {
        h.controls.panic_at(
            Site::ShutdownPending,
            Box::new(PayloadError {
                witness: panic.clone(),
                name: "private-shutdown-poll-panic",
            }),
        );
    }
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.shutdown_pending)).await;
    let stalled = bounded(async {
        while !h.io.drop_gate.entered() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = (
        primary.drops.load(Ordering::SeqCst),
        panic.drops.load(Ordering::SeqCst),
    );
    let actual_io_destructor = h.io.drop_gate.entered();
    let published = h.owner.published();
    if !fault {
        h.owner.abort_wrapper();
    }
    let report = h.finish().await.expect("first full report");
    // All injected original objects remain alive through gate release/all joins.
    setup??;
    observed?;
    stalled?;
    assert!(actual_io_destructor && published.0 && !published.1);
    assert_eq!(before, (0, 0));
    assert!(matches!(report.actor(), Some(Ok(()))));
    assert!(normal_reader_join(report.reader()));
    assert!(report.wrapper().is_some_and(|r| r.as_ref().is_err_and(|e| {
        if fault {
            e.is_panic()
        } else {
            e.is_cancelled()
        }
    })));
    let error = primary_error(&report).and_then(|e| e.downcast_ref::<PayloadError>());
    assert!(error.is_some_and(|e| Arc::ptr_eq(&e.witness, &primary)));
    drop(report);
    assert_eq!(primary.drops.load(Ordering::SeqCst), 1);
    assert_eq!(panic.drops.load(Ordering::SeqCst), usize::from(fault));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_wrapper_abort_after_primary_ready_retains_error_during_actual_shutdown_pending()
-> TestResult {
    pending_shutdown(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_wrapper_unwind_after_primary_ready_retains_error_during_actual_shutdown_pending()
-> TestResult {
    pending_shutdown(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_ready_output_is_rooted_before_completed_future_capture_drop_panics() -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let primary = Arc::new(PayloadWitness::default());
    let capture = Arc::new(PayloadWitness::default());
    h.error(primary.clone());
    *crate::listener::retained_connection::locked(&h.broker.plan.future_drop_panic) =
        Some(capture.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.primary_ready)).await;
    let before = (
        primary.drops.load(Ordering::SeqCst),
        capture.drops.load(Ordering::SeqCst),
    );
    let published = h.owner.published();
    let report = h.finish().await.expect("first full report");
    setup??;
    observed?;
    assert!(published.0 && !published.1);
    assert_eq!(before, (0, 0));
    assert!(
        report
            .wrapper()
            .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_panic()))
    );
    assert!(matches!(report.actor(), Some(Ok(()))));
    assert!(normal_reader_join(report.reader()));
    assert!(
        primary_error(&report)
            .and_then(|e| e.downcast_ref::<PayloadError>())
            .is_some_and(|e| Arc::ptr_eq(&e.witness, &primary))
    );
    drop(report);
    assert_eq!(primary.drops.load(Ordering::SeqCst), 1);
    assert_eq!(capture.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_finish_timeout_restores_wrapper_token_and_retains_ready_primary() -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let payload = Arc::new(PayloadWitness::default());
    h.error(payload.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.shutdown_pending)).await;
    let observation = {
        let mut borrowed = Box::pin(h.owner.finish());
        let outcome = tokio::time::timeout(Duration::from_millis(20), &mut borrowed).await;
        drop(borrowed);
        outcome
    };
    let before = payload.drops.load(Ordering::SeqCst);
    h.release();
    let final_report = h.finish().await;
    setup??;
    observed?;
    assert!(observation.is_err());
    assert_eq!(before, 0);
    let report = final_report.expect("restored original wrapper token");
    successful_socket_joins(&report);
    assert!(report.outcomes().primary.is_some());
    drop(report);
    drop(observation);
    assert_eq!(payload.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_finish_unwind_restores_original_wrapper_before_later_actual_joins() -> TestResult
{
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.shutdown_pending)).await;
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let mut borrowed = Box::pin(h.owner.finish());
        let pending = borrowed
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()));
        if pending.is_pending() {
            panic!("controlled outside borrowed-finish loss");
        }
        pending
    }));
    let report = h.finish().await.expect("restored full report");
    setup??;
    observed?;
    assert!(unwound.is_err());
    successful_socket_joins(&report);
    drop(unwound);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_original_panic_payload_waits_for_actual_actor_reader_barriers() -> TestResult {
    let mut h = Harness::new((), TestDriver, false);
    h.wait_begin();
    h.io.drop_gate.arm();
    let payload = Arc::new(PayloadWitness::default());
    payload.panic_on_drop.store(true, Ordering::SeqCst);
    h.controls.panic_at(
        Site::PrimaryReady,
        Box::new(PayloadError {
            witness: payload.clone(),
            name: "private-wrapper-panic-payload",
        }),
    );
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await?;
        send_begin(peer).await
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.primary_ready)).await;
    let observation = {
        let mut borrowed = Box::pin(h.owner.finish());
        let outcome = tokio::time::timeout(Duration::from_millis(20), &mut borrowed).await;
        drop(borrowed);
        outcome
    };
    let before = payload.drops.load(Ordering::SeqCst);
    let final_report = h.finish().await;
    let was_pending = observation.is_err();
    let returned_report = final_report.is_some();
    let report = final_report
        .as_ref()
        .or_else(|| observation.as_ref().ok().and_then(Option::as_ref));
    let actor_joined = report.is_some_and(|r| matches!(r.actor(), Some(Ok(()))));
    let reader_joined = report.is_some_and(|r| normal_reader_join(r.reader()));
    let wrapper_panicked = report.is_some_and(|r| {
        r.wrapper()
            .is_some_and(|result| result.as_ref().is_err_and(|error| error.is_panic()))
    });
    let unused_fault = h.controls.take_unused_fault();
    let fault_was_unused = unused_fault.is_some();
    // Both possible report holders are disposed after joins, before assertions.
    let disposal = catch_unwind(AssertUnwindSafe(|| {
        drop(final_report.map(RetainedConnectionJoinReport::into_parts));
    }));
    let observation_disposal = catch_unwind(AssertUnwindSafe(|| drop(observation)));
    let unused_disposal = catch_unwind(AssertUnwindSafe(|| drop(unused_fault)));
    setup??;
    observed?;
    assert!(was_pending && returned_report);
    assert_eq!(before, 0);
    assert!(actor_joined && reader_joined && wrapper_panicked);
    assert!(disposal.is_err());
    assert!(observation_disposal.is_ok());
    assert!(!fault_was_unused && unused_disposal.is_ok());
    assert_eq!(payload.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn post_open_expiry(abort: bool) -> TestResult {
    let mut h = Harness::configured((), TestDriver, false, None, Duration::from_secs(1));
    h.io.drop_gate.arm();
    let gate = Gate::new();
    h.controls.open_gate(gate.clone());
    let setup = bounded(async {
        let peer = h
            .peer
            .as_mut()
            .ok_or_else(|| io::Error::other("missing peer"))?;
        hello(peer).await
    })
    .await;
    let entered = bounded(gate.entered()).await;
    // Reuse the original absolute deadline; no protocol policy or timer reset.
    tokio::time::sleep_until(h.deadline).await;
    gate.release();
    let observed = bounded(h.controls.wait_for(|m| m.shutdown_pending)).await;
    let before = h.owner.published();
    if abort {
        h.owner.abort_wrapper();
    }
    let report = h.finish().await.expect("actual report");
    setup??;
    entered?;
    observed?;
    assert_eq!(before, (false, false));
    if abort {
        assert!(report.outcomes().primary.is_none());
        assert!(
            report
                .wrapper()
                .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_cancelled()))
        );
    } else {
        assert!(
            primary_error(&report)
                .and_then(|e| e.downcast_ref::<io::Error>())
                .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
        );
    }
    assert!(report.actor().is_some());
    // Reader creation is optional if root seal won its first claim.
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_open_expired_abort_during_actual_shutdown_has_no_invented_primary() -> TestResult {
    post_open_expiry(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_open_expired_constructs_original_timeout_only_after_actual_shutdown_signal()
-> TestResult {
    post_open_expiry(false).await
}
