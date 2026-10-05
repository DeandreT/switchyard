use std::sync::{Arc, atomic::Ordering};

use tokio::runtime::Builder;

use super::super::super::IngressMode;
use super::super::controls::Controls;
use super::dispose;
use super::fixture::{CHANNELS, Fixture, TestResult, evidence, producer};

#[tokio::test(flavor = "current_thread")]
async fn queued_unpolled_session_abort_restores_its_whole_worker_packet() -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.sessions[0].session_start.arm();
    controls.sessions[1].session_start.arm();
    controls.admission_ready[1].arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, controls.clone()).await?;
    let setup = async {
        h.accept(CHANNELS[0]).await?;
        let incoming = h.incoming(CHANNELS[1]).await?;
        match h.offer(incoming) {
            Ok(_) => {}
            Err(refused) => {
                h.returned_offer(refused);
                return Err("unexpected offer refusal".into());
            }
        }
        h.wait_gate(&controls.admission_ready[1]).await
    }
    .await;
    if setup.is_ok() {
        controls.admission_ready[1].release();
        // Immediate Ready checkpoint return -> spawn/install -> stop without yielding.
        let root = h.root.as_mut().expect("root");
        root.accepted(1).await;
        root.stop();
    }
    let entered = controls.sessions[1].session_start.entered();
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let cancelled = cleanup.report.as_ref().is_some_and(|r| {
        r.sessions[1]
            .as_ref()
            .is_some_and(|s| s.original.as_ref().is_err_and(|e| e.is_cancelled()))
    });
    let disposed = dispose(cleanup);
    setup?;
    assert!(!entered && !controls.sessions[1].session_start.entered());
    assert!(cancelled);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn one_session_unwind_after_worker_install_restores_without_losing_sibling_session()
-> TestResult {
    let controls = Arc::new(Controls::default());
    controls.sessions[0].worker_start.arm();
    controls.sessions[1].worker_start.arm();
    controls.session_ready[0].arm();
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let setup = async {
        h.sessions().await?;
        h.attached(CHANNELS[1], producer(2)).await?;
        h.wait_workers(1).await
    }
    .await;
    if setup.is_ok() {
        controls.sessions[0]
            .session_unwind_after_launch
            .store(true, Ordering::SeqCst);
    }
    let observed = async {
        if setup.is_err() {
            return Ok(());
        }
        h.attached(CHANNELS[0], producer(1)).await?;
        h.wait_gate(&controls.session_ready[0]).await
    }
    .await;
    let before = controls
        .sessions
        .iter()
        .map(|c| c.worker_drops.load(Ordering::SeqCst))
        .sum::<usize>();
    let restored = if observed.is_ok() {
        let root = h.root.as_ref().expect("root");
        let packet = root.cells[0].loan(
            root.worker_budget.clone(),
            root.settings.runtime.clone(),
            controls.sessions[0].clone(),
        );
        Some((
            packet.packet().launches.len(),
            packet.packet().set.len(),
            packet.packet().rows.len(),
        ))
    } else {
        None
    };
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let panic = cleanup.report.as_ref().is_some_and(|r| {
        r.sessions[0]
            .as_ref()
            .is_some_and(|s| s.original.as_ref().is_err_and(|e| e.is_panic()))
    });
    let disposed = dispose(cleanup);
    setup?;
    observed?;
    assert_eq!(before, 0);
    assert_eq!(restored, Some((1, 1, 0)));
    assert!(panic);
    assert_eq!(facts, (2, 2, 2, true));
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn original_worker_error_waits_for_blocked_other_session_workers() -> TestResult {
    super::workflow::failure(false, false, 61).await
}
#[tokio::test]
async fn original_worker_panic_payload_waits_for_both_session_and_all_worker_joins() -> TestResult {
    super::workflow::failure(false, true, 62).await
}

#[tokio::test]
async fn owner_close_precedes_root_abort_and_receiver_teardown_for_both_sessions() -> TestResult {
    let controls = Arc::new(Controls::default());
    for c in &controls.sessions {
        c.worker_start.arm();
    }
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let observed = async {
        h.sessions().await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.attached(CHANNELS[1], producer(2)).await?;
        h.wait_workers(2).await
    }
    .await;
    let before = controls
        .sessions
        .iter()
        .map(|c| c.worker_drops.load(Ordering::SeqCst))
        .sum::<usize>();
    h.root.as_mut().expect("root").stop();
    let closed = controls.authority_closed.load(Ordering::SeqCst);
    let torn_down = controls.receiver_torn_down.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let flags = controls.sessions.iter().all(|c| {
        c.authority_closed.load(Ordering::SeqCst) && !c.disposed_before_close.load(Ordering::SeqCst)
    });
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(before, 0);
    assert!(closed && !torn_down && flags);
    assert_eq!(facts, (2, 2, 2, true));
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn borrowed_finish_cancellation_after_first_session_join_keeps_second_original_token()
-> TestResult {
    let controls = Arc::new(Controls::default());
    controls.session_ready[0].arm();
    let mut h = Fixture::new(IngressMode::Posting, 1, controls.clone()).await?;
    let setup = h.sessions().await;
    let observed = h.cancel_finish_at(&controls.session_ready[0]).await;
    let rooted = h
        .root
        .as_ref()
        .expect("root")
        .sessions
        .iter()
        .enumerate()
        .map(|(i, s)| {
            (
                i,
                matches!(
                    s,
                    super::super::super::retained_session::SessionSlot::Joined { .. }
                ),
            )
        })
        .collect::<Vec<_>>();
    let count = controls.observed_sessions.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    setup?;
    observed?;
    assert_eq!(rooted, vec![(0, true), (1, false)]);
    assert_eq!(count, 1);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok());
    Ok(())
}

async fn row_observation(unwind: bool) -> TestResult {
    let controls = Arc::new(Controls::default());
    for c in &controls.sessions {
        c.worker_start.arm();
    }
    controls.row_ready.arm();
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let setup = async {
        h.sessions().await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.attached(CHANNELS[1], producer(2)).await?;
        h.wait_workers(2).await
    }
    .await;
    if unwind {
        controls.row_panic.store(true, Ordering::SeqCst);
    }
    let mut observed_unwind = None;
    let observed = if unwind {
        observed_unwind = Some(h.unwind_finish().await);
        Ok(())
    } else {
        h.cancel_finish_at(&controls.row_ready).await
    };
    let before = controls.observed_rows.load(Ordering::SeqCst);
    let teardown_before = controls.owner_torn_down.load(Ordering::SeqCst);
    let restored = if before == 1 {
        let root = h.root.as_ref().expect("root");
        Some(
            root.cells
                .iter()
                .enumerate()
                .map(|(index, cell)| {
                    let packet = cell.loan(
                        root.worker_budget.clone(),
                        root.settings.runtime.clone(),
                        controls.sessions[index].clone(),
                    );
                    (
                        packet.packet().launches.len(),
                        packet.packet().set.len(),
                        packet.packet().rows.len(),
                    )
                })
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let packets = cleanup.report.as_ref().map(|r| {
        r.packets
            .iter()
            .map(|p| (p.launches.len(), p.rows.len()))
            .collect::<Vec<_>>()
    });
    let disposed = dispose(cleanup);
    let caught = observed_unwind.as_ref().is_some_and(Result::is_err);
    let observer_disposed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(observed_unwind)));
    setup?;
    observed?;
    assert_eq!(before, 1);
    assert!(!teardown_before);
    assert!(
        restored
            .as_ref()
            .is_some_and(|r| r.iter().all(|(launches, _, _)| *launches == 1)
                && r.iter().map(|(_, set, _)| set).sum::<usize>() == 1
                && r.iter().map(|(_, _, rows)| rows).sum::<usize>() == 1)
    );
    assert_eq!(facts, (2, 2, 2, true));
    assert_eq!(packets, Some(vec![(1, 1), (1, 1)]));
    assert_eq!(caught, unwind);
    assert!(disposed.is_ok() && observer_disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn borrowed_finish_cancellation_after_worker_ready_restores_both_whole_packets() -> TestResult
{
    row_observation(false).await
}
#[tokio::test]
async fn borrowed_finish_unwind_after_worker_ready_restores_all_original_rows_and_sets()
-> TestResult {
    row_observation(true).await
}

#[tokio::test]
async fn non_send_non_static_anchor_never_enters_session_worker_or_admission_task() -> TestResult {
    let mut anchor = std::rc::Rc::new(());
    let mut h = Fixture::with_anchor(
        IngressMode::Posting,
        1,
        Arc::new(Controls::default()),
        &mut anchor,
    )
    .await?;
    let observed = h.sessions().await;
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let rooted = cleanup
        .report
        .as_ref()
        .is_some_and(|r| std::rc::Rc::strong_count(&*r.anchor) == 1);
    let disposed = dispose(cleanup);
    observed?;
    assert!(rooted);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok());
    assert_eq!(std::rc::Rc::strong_count(&anchor), 1);
    Ok(())
}

#[test]
fn lost_observer_runtime_b_preserves_collector_on_live_runtime_a() -> TestResult {
    let a = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let b = Builder::new_current_thread().enable_all().build()?;
    let controls = Arc::new(Controls::default());
    controls.session_ready[0].arm();
    let mut h = a.block_on(Fixture::new(IngressMode::Posting, 1, controls.clone()))?;
    let setup = a.block_on(h.sessions());
    // B only observes a borrowed finish. Original children target captured A.
    let observed = b.block_on(h.cancel_finish_at(&controls.session_ready[0]));
    drop(b);
    let before = controls.owner_torn_down.load(Ordering::SeqCst);
    let cleanup = a.block_on(h.complete());
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    let checkpoint = observed.is_ok();
    let original_observation =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(observed)));
    setup?;
    assert!(checkpoint && !before);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok() && original_observation.is_ok());
    Ok(())
}
