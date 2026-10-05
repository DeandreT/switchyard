use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use amqp::{Detach, Performative};

use super::super::super::{
    IngressMode,
    retained_session::{SessionExit, packet::Drain},
    routing::Branch,
};
use super::super::controls::Controls;
use super::fixture::{CHANNELS, Fixture, TestResult, consumer, controller, evidence, producer};
use super::{Payload, dispose, error};

async fn wait_joined(h: &mut Fixture, expected: usize) -> TestResult {
    let controls = h.controls.clone();
    h.drive(async {
        while controls.observed_sessions.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await
}

async fn workflow(mode: IngressMode, controller_first: bool, receiving: bool) -> TestResult {
    let controls = Arc::new(Controls::default());
    let mut h = Fixture::new(mode, 2, controls).await?;
    let observed = async {
        h.sessions().await?;
        h.attached(
            CHANNELS[0],
            if controller_first {
                controller(1)
            } else {
                producer(1)
            },
        )
        .await?;
        h.attached(
            CHANNELS[1],
            if receiving { consumer(2) } else { producer(2) },
        )
        .await?;
        h.wait_workers(2).await?;
        h.end(0).await?;
        h.end(1).await?;
        wait_joined(&mut h, 2).await
    }
    .await;
    let binds = h.recorder.binds.load(Ordering::SeqCst);
    let submitted = h.recorder.submitted.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let complete = cleanup.report.as_ref().is_some_and(|r| {
        r.sessions
            .iter()
            .flatten()
            .all(|s| matches!(s.original, Ok(SessionExit::Completed)))
    });
    let branches = cleanup.report.as_ref().map(|r| {
        r.packets
            .iter()
            .filter_map(|p| p.launches.first().map(|l| l.branch))
            .collect::<Vec<_>>()
    });
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(facts, (2, 2, 2, true));
    assert!(complete);
    assert_eq!(binds, if controller_first { 1 } else { 2 });
    assert_eq!(submitted, 0);
    assert_eq!(
        branches,
        Some(vec![
            if controller_first {
                Branch::Controller
            } else {
                Branch::Producer
            },
            if receiving {
                Branch::Consumer
            } else {
                Branch::Producer
            }
        ])
    );
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn two_actual_sessions_two_producers_use_one_owner() -> TestResult {
    workflow(IngressMode::Posting, false, false).await
}
#[tokio::test]
async fn posting_controller_and_producer_on_different_sessions_share_event_owner() -> TestResult {
    workflow(IngressMode::Posting, true, false).await
}
#[tokio::test]
async fn messaging_mode_two_sessions_reuse_the_same_actual_routing_body() -> TestResult {
    workflow(IngressMode::Messaging, false, true).await
}

pub(super) async fn failure(peer_end: bool, panic: bool, id: usize) -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.sessions[1].worker_start.arm();
    controls.session_ready[0].arm();
    if peer_end {
        controls.sessions[0].worker_final.arm();
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let setup = async {
        h.sessions().await?;
        h.attached(CHANNELS[1], controller(2)).await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.wait_workers(2).await
    }
    .await;
    // Deliberate payloads are armed only after fallible setup; cleanup owns unused ones.
    if setup.is_ok() {
        *controls.sessions[0]
            .fault
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(if panic {
            super::panic_payload(id, &drops)
        } else {
            error(id, &drops)
        });
    }
    let observed = async {
        if setup.is_err() {
            return Ok(());
        }
        if peer_end {
            h.end(0).await?;
            h.drive(async {
                while !controls.sessions[0].peer_end_entered.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
            controls.sessions[0].worker_final.release();
        } else {
            h.send(
                CHANNELS[0],
                Performative::Detach(Detach {
                    handle: 1,
                    closed: true,
                    error: None,
                }),
            )
            .await?;
        }
        h.wait_gate(&controls.session_ready[0]).await
    }
    .await;
    let before = drops.load(Ordering::SeqCst);
    let blocked_drops = controls.sessions[1].worker_drops.load(Ordering::SeqCst);
    let mut cleanup = h.complete().await;
    let mut matches = 0;
    let mut disposition = false;
    if let Some(report) = &cleanup.report {
        for (index, row) in report.packets[0].rows.iter().enumerate() {
            let original = if panic {
                row.original.as_ref().is_err_and(|e| e.is_panic())
            } else {
                row.original
                    .as_ref()
                    .ok()
                    .and_then(|r| r.as_ref().err())
                    .and_then(|e| e.downcast_ref::<Payload>())
                    .is_some_and(|p| p.id == id)
            };
            if original {
                matches += 1;
                disposition = matches!(&report.sessions[0], Some(s) if matches!(&s.original,
                    Ok(SessionExit::WorkerFailed(local)) if *local == index));
                disposition &= row.drain
                    == if peer_end {
                        Drain::PeerEnd
                    } else {
                        Drain::Live
                    };
            }
        }
    }
    let facts = evidence(&cleanup);
    // Inspect the ORIGINAL panic object only after every collector/socket barrier.
    let mut retained_panic = None;
    if panic && let Some(report) = &mut cleanup.report {
        for row in &mut report.packets[0].rows {
            if row.original.as_ref().is_err_and(|e| e.is_panic()) {
                let original = std::mem::replace(&mut row.original, Ok(Ok(())));
                if let Err(original) = original {
                    retained_panic = Some(original.try_into_panic());
                }
                break;
            }
        }
    }
    let typed_panic = retained_panic
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .and_then(|p| p.downcast_ref::<Payload>())
        .is_some_and(|p| p.id == id);
    let disposed = dispose(cleanup);
    let panic_disposed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(retained_panic)));
    setup?;
    observed?;
    assert_eq!(before, 0);
    assert_eq!(blocked_drops, 0);
    assert_eq!(facts, (2, 2, 2, true));
    assert_eq!(matches, 1);
    assert!(disposition);
    assert_eq!(typed_panic, panic);
    assert!(disposed.is_err() || panic_disposed.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn live_drain_error_has_unique_session_and_original_worker_row() -> TestResult {
    failure(false, false, 31).await
}
#[tokio::test]
async fn peer_end_drain_error_has_unique_session_and_original_worker_row() -> TestResult {
    failure(true, false, 32).await
}

#[tokio::test]
async fn worker_stopped_ack_pump_lives_through_the_last_sibling_join() -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.sessions[1].worker_final.arm();
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let observed = async {
        h.sessions().await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.attached(CHANNELS[1], producer(2)).await?;
        h.wait_workers(2).await?;
        h.end(0).await?;
        h.end(1).await?;
        h.wait_gate(&controls.sessions[1].worker_final).await?;
        wait_joined(&mut h, 1).await
    }
    .await;
    let stopped = controls.worker_stopped.load(Ordering::SeqCst);
    let joined = controls.observed_sessions.load(Ordering::SeqCst);
    let closed = controls.authority_closed.load(Ordering::SeqCst);
    controls.sessions[1].worker_final.release();
    let last = wait_joined(&mut h, 2).await;
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    observed?;
    last?;
    assert_eq!(stopped, 2);
    assert_eq!(joined, 1);
    assert!(!closed);
    assert_eq!(facts, (2, 2, 2, true));
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn postjoin_event_owner_teardown_waits_for_two_sessions_and_every_worker() -> TestResult {
    let controls = Arc::new(Controls::default());
    let mut h = Fixture::new(IngressMode::Posting, 2, controls.clone()).await?;
    let observed = async {
        h.sessions().await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.attached(CHANNELS[1], controller(2)).await?;
        h.wait_workers(2).await
    }
    .await;
    let before = (
        controls.receiver_torn_down.load(Ordering::SeqCst),
        controls.owner_torn_down.load(Ordering::SeqCst),
    );
    let submitted = h.recorder.submitted.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let after = (
        controls.receiver_torn_down.load(Ordering::SeqCst),
        controls.owner_torn_down.load(Ordering::SeqCst),
    );
    let counts = cleanup
        .report
        .as_ref()
        .map(|r| (r.session_counts, r.worker_counts));
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(before, (false, false));
    assert_eq!(after, (true, true));
    assert_eq!(facts, (2, 2, 2, true));
    assert_eq!(counts, Some(((0, 2, true), (0, 2, true))));
    assert_eq!(submitted, 0);
    assert!(disposed.is_ok());
    Ok(())
}
