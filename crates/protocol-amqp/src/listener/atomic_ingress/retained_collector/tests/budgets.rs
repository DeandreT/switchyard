use std::sync::{Arc, atomic::Ordering};

use amqp::{Detach, Performative};

use super::super::super::{
    IngressMode,
    retained_session::budget::{Budget, Closed},
};
use super::super::controls::Controls;
use super::dispose;
use super::fixture::{CHANNELS, Fixture, TestResult, evidence, producer};

#[tokio::test]
async fn global_worker_history_budget_is_not_per_session_or_live_permit() -> TestResult {
    let controls = Arc::new(Controls::default());
    let mut h = Fixture::new(IngressMode::Posting, 1, controls.clone()).await?;
    let observed = async {
        h.sessions().await?;
        h.attached(CHANNELS[0], producer(1)).await?;
        h.wait_workers(1).await?;
        h.send(
            CHANNELS[0],
            Performative::Detach(Detach {
                handle: 1,
                closed: true,
                error: None,
            }),
        )
        .await?;
        h.drive(async {
            while controls.sessions[0].joined_rows.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        h.send(CHANNELS[1], Performative::Attach(Box::new(producer(2))))
            .await?;
        h.drive(async {
            while !controls.authority_closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    .await;
    let completed = controls.sessions[0].worker_drops.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let counts = cleanup.report.as_ref().map(|r| r.worker_counts);
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(completed, 1);
    assert_eq!(facts, (2, 2, 1, true));
    assert_eq!(counts, Some((0, 1, true)));
    assert!(disposed.is_ok());
    Ok(())
}

#[tokio::test]
async fn completed_session_does_not_refund_committed_session_history() -> TestResult {
    let controls = Arc::new(Controls::default());
    let mut h = Fixture::new(IngressMode::Posting, 1, controls.clone()).await?;
    let observed = async {
        h.sessions().await?;
        h.end(0).await?;
        h.drive(async {
            while controls.observed_sessions.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    .await;
    let counts = h.counts();
    let closed = controls.authority_closed.load(Ordering::SeqCst);
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    observed?;
    assert_eq!(counts.1, (0, 2, false));
    assert!(!closed);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok());
    Ok(())
}

#[test]
fn two_outstanding_claims_reverse_commit_assign_unique_ordinals_and_sticky_quota() {
    let budget = Budget::new(2);
    let first = budget
        .reserve()
        .expect("first reservation")
        .claim()
        .expect("first claim");
    let second = budget
        .reserve()
        .expect("second reservation")
        .claim()
        .expect("second claim");
    assert_eq!(budget.counts(), (2, 0, false));
    assert_eq!(second.commit(), 0);
    assert_eq!(first.commit(), 1);
    assert_eq!(budget.counts(), (0, 2, false));
    assert!(matches!(budget.reserve(), Err(Closed::Budget)));
    budget.seal();
    assert!(matches!(budget.reserve(), Err(Closed::Sealed)));
}

#[tokio::test]
async fn accepted_claim_keeps_reserved_installation_obligation_across_seal() -> TestResult {
    let budget = Budget::new(2);
    let accepted = budget
        .reserve()
        .expect("reservation")
        .claim()
        .expect("accepted claim");
    let unused = budget.reserve().expect("unused ticket");
    budget.seal();
    assert!(matches!(unused.claim(), Err(Closed::Sealed)));
    assert_eq!(budget.counts(), (1, 0, true));
    assert_eq!(accepted.commit(), 0);
    assert_eq!(budget.counts(), (0, 1, true));
    for limit in [0, 129] {
        let h = Fixture::new(IngressMode::Posting, limit, Arc::new(Controls::default())).await?;
        let cleanup = h.complete().await;
        let original = cleanup.refused.as_ref().map(|r| {
            (
                r.limit,
                r.settings.mode == IngressMode::Posting,
                r.anchor.drops.load(Ordering::SeqCst),
            )
        });
        let socket = cleanup
            .socket
            .as_ref()
            .is_some_and(|r| r.actor().is_some() && r.reader().is_some());
        let report_absent = cleanup.report.is_none();
        let disposed = dispose(cleanup);
        assert_eq!(original, Some((limit, true, 0)));
        assert!(socket && report_absent && disposed.is_ok());
    }
    Ok(())
}
