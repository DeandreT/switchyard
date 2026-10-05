use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use amqp::{Detach, Performative};
use futures_util::FutureExt;
use tokio::runtime::Builder;

use super::super::super::{IngressMode, routing::Branch};
use super::super::{SessionExit, controls::Controls, packet::Drain};
use super::fixture::{Cleanup, Fixture, TestResult, controller, producer};
use super::{Payload, dispose, error, panic_payload};

async fn blocked_siblings(siblings: u32, controls: &Arc<Controls>) -> TestResult<Fixture> {
    Fixture::new(
        IngressMode::Posting,
        siblings as usize + 1,
        None,
        Arc::clone(controls),
    )
    .await
}

async fn install(h: &mut Fixture, siblings: u32) -> TestResult {
    for handle in 1..=siblings {
        h.send_attach(controller(handle)).await?;
        h.attached(handle).await?;
    }
    h.send_attach(producer(siblings + 1)).await?;
    h.attached(siblings + 1).await?;
    h.wait_committed(siblings as usize + 1).await
}

fn worker_payload(cleanup: &Cleanup, id: usize, drain: Drain) -> (usize, bool, bool) {
    let Some(report) = &cleanup.report else {
        return (0, false, false);
    };
    let matching: Vec<_> = report
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row.drain == drain
                && row
                    .original
                    .as_ref()
                    .ok()
                    .and_then(|result| result.as_ref().err())
                    .and_then(|error| error.downcast_ref::<Payload>())
                    .is_some_and(|payload| payload.id == id)
        })
        .collect();
    let disposition = matches!(&report.session, Ok(SessionExit::WorkerFailed(index))
        if matching.first().is_some_and(|(row, _)| row == index));
    (
        matching.len(),
        disposition,
        cleanup.unused_fault.is_none() && cleanup.unused_session_fault.is_none(),
    )
}

fn settings() -> Arc<Controls> {
    let controls = Arc::new(Controls::default());
    *controls
        .worker_branch
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(Branch::Controller);
    controls.worker_start.arm();
    controls
}

#[tokio::test]
async fn original_worker_final_error_is_unique_with_payload_free_session_failure() -> TestResult {
    let controls = settings();
    controls.session_ready.arm();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = blocked_siblings(1, &controls).await?;
    *controls.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some(error(17, &drops));
    let observed = async {
        install(&mut h, 1).await?;
        h.send_performative(Performative::Detach(Detach {
            handle: 2,
            closed: true,
            error: None,
        }))
        .await?;
        h.wait_gate(&controls.session_ready).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let evidence = worker_payload(&cleanup, 17, Drain::Live);
    let rows = cleanup.report.as_ref().map(|report| report.rows.len());
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(before, 0);
    assert_eq!(rows, Some(2));
    assert_eq!(evidence, (1, true, true));
    assert!(disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn peer_end_drain_retains_original_worker_failure_before_session_disposition() -> TestResult {
    let controls = settings();
    controls.worker_final.arm();
    controls.session_ready.arm();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = blocked_siblings(1, &controls).await?;
    *controls.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some(error(18, &drops));
    let observed = async {
        install(&mut h, 1).await?;
        h.peer_end().await?;
        h.drive(async {
            while !controls.peer_end_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        controls.worker_final.release();
        h.wait_gate(&controls.session_ready).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let evidence = worker_payload(&cleanup, 18, Drain::PeerEnd);
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(before, 0);
    assert_eq!(evidence, (1, true, true));
    assert!(disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn worker_panic_payload_is_retained_while_actual_sibling_remains_blocked() -> TestResult {
    let controls = settings();
    controls.session_ready.arm();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = blocked_siblings(1, &controls).await?;
    *controls.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some(panic_payload(19, &drops));
    let observed = async {
        install(&mut h, 1).await?;
        h.wait_gate(&controls.worker_start).await?;
        h.send_performative(Performative::Detach(Detach {
            handle: 2,
            closed: true,
            error: None,
        }))
        .await?;
        h.wait_gate(&controls.session_ready).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let worker_drops_before_root_abort = controls.worker_drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let retained_panic = cleanup.report.as_ref().is_some_and(|report| {
        report
            .rows
            .iter()
            .filter(|row| row.original.as_ref().is_err_and(|error| error.is_panic()))
            .count()
            == 1
    });
    let all_rows = cleanup.report.as_ref().map(|report| report.rows.len());
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(before, 0);
    assert_eq!(worker_drops_before_root_abort, 1);
    assert_eq!(all_rows, Some(2));
    assert!(retained_panic);
    assert!(disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn original_session_join_result_is_rooted_before_owner_close_callback() -> TestResult {
    for panic in [false, true] {
        let controls = Arc::new(Controls::default());
        controls.session_ready.arm();
        controls.session_start.arm();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut h = Fixture::new(IngressMode::Posting, 1, None, Arc::clone(&controls)).await?;
        *controls
            .session_fault
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(if panic {
            panic_payload(20, &drops)
        } else {
            error(20, &drops)
        });
        controls.session_start.release();
        let observed = h.wait_gate(&controls.session_ready).await;
        let before = drops.load(Ordering::SeqCst);
        let closed_before_root_callback = controls.authority_closed.load(Ordering::SeqCst);
        let cleanup = h.complete().await;
        let identity = cleanup
            .report
            .as_ref()
            .is_some_and(|report| match &report.session {
                Err(error) => panic && error.is_panic(),
                Ok(SessionExit::Routing(error)) => {
                    !panic
                        && error
                            .downcast_ref::<Payload>()
                            .is_some_and(|payload| payload.id == 20)
                }
                _ => false,
            });
        let rows = cleanup.report.as_ref().map(|report| report.rows.len());
        let disposed = dispose(cleanup);
        observed?;
        assert_eq!(before, 0);
        assert!(!closed_before_root_callback);
        assert!(identity);
        assert_eq!(rows, Some(0));
        assert!(disposed.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn borrowed_finish_cancellation_restores_set_and_collected_raw_results() -> TestResult {
    let controls = settings();
    controls.finish_after_row.arm();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = blocked_siblings(2, &controls).await?;
    *controls.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some(error(21, &drops));
    let observed = async {
        install(&mut h, 2).await?;
        h.send_performative(Performative::Detach(Detach {
            handle: 3,
            closed: true,
            error: None,
        }))
        .await?;
        // wait_gate's losing borrowed finish is deliberately canceled after a rooted drain row.
        h.wait_gate(&controls.finish_after_row).await
    }
    .await;
    let packet_counts = if observed.is_ok() {
        let root = h.root.as_ref().expect("root");
        let packet = root.cell.loan(
            Arc::clone(&root.budget),
            root.runtime.clone(),
            Arc::clone(&root.controls),
        );
        Some((packet.packet().set.len(), packet.packet().rows.len()))
    } else {
        None
    };
    let before = drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let evidence = worker_payload(&cleanup, 21, Drain::Live);
    let rows = cleanup.report.as_ref().map(|report| report.rows.len());
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(packet_counts, Some((1, 2)));
    assert_eq!(before, 0);
    assert_eq!(rows, Some(3));
    assert_eq!(evidence, (1, true, true));
    assert!(disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn borrowed_finish_unwind_restores_set_without_payload_disposal() -> TestResult {
    let controls = settings();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = blocked_siblings(2, &controls).await?;
    *controls.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some(error(22, &drops));
    let started = async {
        install(&mut h, 2).await?;
        h.send_performative(Performative::Detach(Detach {
            handle: 3,
            closed: true,
            error: None,
        }))
        .await
    }
    .await;
    // Preserve an unexpectedly returned report too; never dispose it before socket joins.
    let observed = if started.is_ok() {
        controls.row_panic.store(true, Ordering::SeqCst);
        Some(
            AssertUnwindSafe(h.root.as_mut().expect("root").finish())
                .catch_unwind()
                .await,
        )
    } else {
        None
    };
    let mut original_panic = None;
    match observed {
        Some(Ok(report)) => h.report = Some(report),
        Some(Err(payload)) => original_panic = Some(payload),
        None => {}
    }
    let packet_counts = if original_panic.is_some() {
        let root = h.root.as_ref().expect("root");
        let packet = root.cell.loan(
            Arc::clone(&root.budget),
            root.runtime.clone(),
            Arc::clone(&root.controls),
        );
        Some((packet.packet().set.len(), packet.packet().rows.len()))
    } else {
        None
    };
    let before = drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let evidence = worker_payload(&cleanup, 22, Drain::Live);
    let disposed = dispose(cleanup);
    started?;
    assert!(original_panic.is_some());
    assert_eq!(packet_counts, Some((1, 2)));
    assert_eq!(before, 0);
    assert_eq!(evidence, (1, true, true));
    assert!(disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn observer_runtime_loss_does_not_own_or_detach_a_runtime_session_workers() -> TestResult {
    let runtime_a = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let controls = settings();
    let mut h = runtime_a.block_on(Fixture::new(
        IngressMode::Posting,
        1,
        None,
        Arc::clone(&controls),
    ))?;
    let runtime_b = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let cleanup = runtime_a.block_on(h.complete());
            drop(cleanup);
            return Err(error.into());
        }
    };
    let observed = runtime_b.block_on(async {
        h.send_attach(controller(1)).await?;
        h.wait_gate(&controls.worker_start).await
    });
    // The borrowed B observation is gone; root and the actual A tasks remain retained.
    drop(runtime_b);
    let before = controls.worker_drops.load(Ordering::SeqCst);
    let cleanup = runtime_a.block_on(h.complete());
    observed?;
    super::fixture::assert_barriers(&cleanup, 1);
    assert_eq!(before, 0);
    assert_eq!(controls.worker_drops.load(Ordering::SeqCst), 1);
    Ok(())
}
