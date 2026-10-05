use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use amqp::{End, EngineError, Frame, Performative, ServerSession};

use super::super::super::{IngressMode, retained_session::budget::Closed};
use super::super::{
    admissions::{Original, Outcome},
    controls::Controls,
};
use super::fixture::{CHANNELS, Fixture, TestResult, evidence};
use super::{Payload, dispose};

#[tokio::test]
async fn two_attempt_history_refusal_returns_original_incoming_session() -> TestResult {
    let mut h = Fixture::new(IngressMode::Posting, 1, Arc::new(Controls::default())).await?;
    let setup = h.sessions().await;
    let incoming = if setup.is_ok() {
        h.incoming(11).await.map(Some)
    } else {
        Ok(None)
    };
    let refused = incoming.as_ref().is_ok_and(Option::is_some);
    let result = incoming.map(|incoming| incoming.map(|incoming| h.offer(incoming)));
    let counts = h.counts();
    let cleanup = h.complete().await;
    let facts = evidence(&cleanup);
    let original = result
        .as_ref()
        .ok()
        .and_then(Option::as_ref)
        .is_some_and(|r| match r {
            Err(refused) => {
                let _exact_original = &refused.incoming;
                refused.cause == Closed::Budget
            }
            Ok(_) => false,
        });
    let disposed = dispose(cleanup);
    // Caller-owned original refused offer survives collector AND socket barriers.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result)));
    setup?;
    assert!(refused && original);
    assert_eq!(counts.0, 2);
    assert_eq!(facts, (2, 2, 0, true));
    assert!(disposed.is_ok() && result.is_ok());
    Ok(())
}

#[tokio::test]
async fn actual_remote_end_before_accept_refunds_session_ticket_not_attempt_history() -> TestResult
{
    let mut h = Fixture::new(IngressMode::Posting, 1, Arc::new(Controls::default())).await?;
    h.controls.admission_ready[0].arm();
    let mut incoming = None;
    let observed = async {
        incoming = Some(h.incoming(CHANNELS[0]).await?);
        h.send(CHANNELS[0], Performative::End(End::default()))
            .await?;
        let mut ended = false;
        for _ in 0..64 {
            if matches!(h.frame().await?, Frame::Amqp { channel, performative: Some(Performative::End(_)), .. }
                if channel == CHANNELS[0])
            {
                ended = true;
                break;
            }
        }
        if !ended {
            return Err("missing End acknowledgment".into());
        }
        match h.offer(incoming.take().expect("retained original incoming Session")) {
            Ok(_) => {}
            Err(refused) => {
                h.returned_offer(refused);
                return Err("unexpected detached offer refusal".into());
            }
        }
        let controls = h.controls.clone();
        h.drive(async {
            while !controls.admission_ready[0].entered() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        // Gate is armed to observe the exact raw Ready first, then allow classification.
        controls.admission_ready[0].release();
        let budget = h.root.as_ref().expect("root").session_budget.clone();
        h.drive(async {
            while budget.counts().0 != 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        h.accept(CHANNELS[1]).await?;
        Ok(())
    };
    // Installed before first drive; actual accept sees a retired IncomingSession.
    // The gate records observed Ready, not a pre-poll checkpoint.
    let observed: TestResult = observed.await;
    let counts = h.counts();
    let cleanup = h.complete().await;
    let detached = cleanup.report.as_ref().is_some_and(|r| matches!(&r.admissions[0],
        Some(record) if matches!(record.outcome, Outcome::Observed(Err(EngineError::RemoteDetached)))));
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    let unoffered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(incoming)));
    observed?;
    assert!(detached);
    assert_eq!(counts, (2, (0, 1, false), (0, 0, false)));
    assert_eq!(facts, (2, 1, 0, true));
    assert!(disposed.is_ok() && unoffered.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_actual_pending_admission_keeps_original_future_through_task_barriers() -> TestResult
{
    let mut h = Fixture::new(IngressMode::Posting, 1, Arc::new(Controls::default())).await?;
    let setup = h.incoming(CHANNELS[0]).await;
    let offered = setup.map(|incoming| h.offer(incoming));
    let mut cx = Context::from_waker(Waker::noop());
    let pending = h
        .root
        .as_mut()
        .expect("root")
        .poll_admissions(&mut cx)
        .is_pending();
    let observed_pending = h.root.as_ref().expect("root").admissions[0]
        .as_ref()
        .is_some_and(|r| r.observed_pending && matches!(r.outcome, Outcome::Pending));
    let cleanup = h.complete().await;
    let retained = cleanup.report.as_ref().is_some_and(|r| {
        matches!(&r.admissions[0],
        Some(record) if matches!(record.outcome, Outcome::Pending) && record.ticket.is_none())
    });
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    let offered_ok = offered.as_ref().is_ok_and(|r| matches!(r, Ok(0)));
    let disposed_offer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(offered)));
    assert!(offered_ok && pending && observed_pending && retained);
    assert_eq!(facts, (1, 0, 0, true));
    assert!(disposed.is_ok() && disposed_offer.is_ok());
    Ok(())
}

async fn accepted_unlaunched(capture: bool) -> TestResult {
    let controls = Arc::new(Controls::default());
    controls.admission_ready[0].arm();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut h = Fixture::new(IngressMode::Posting, 1, controls.clone()).await?;
    let setup = h.incoming(CHANNELS[0]).await;
    let offered = setup.map(|incoming| {
        if capture {
            h.offer_with(incoming, |inner| {
                Box::pin(HoldingAdmission {
                    inner,
                    witness: Payload {
                        id: 53,
                        drops: drops.clone(),
                        panic_on_drop: true,
                    },
                })
            })
        } else {
            h.offer(incoming)
        }
    });
    let observed = h.wait_gate(&controls.admission_ready[0]).await;
    let before = drops.load(Ordering::SeqCst);
    let ready = h.root.as_ref().expect("root").admissions[0]
        .as_ref()
        .is_some_and(|record| matches!(record.outcome, Outcome::Observed(Ok(_))));
    h.root.as_mut().expect("root").stop();
    let cleanup = h.complete().await;
    let retained = cleanup.report.as_ref().is_some_and(|r| matches!(&r.admissions[0],
        Some(record) if matches!(record.outcome, Outcome::Observed(Ok(_))) && record.ticket.is_none()));
    let facts = evidence(&cleanup);
    let disposed = dispose(cleanup);
    let offered_ok = offered.as_ref().is_ok_and(|r| matches!(r, Ok(0)));
    let disposed_offer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(offered)));
    observed?;
    assert!(offered_ok && ready && retained);
    assert_eq!(before, 0);
    assert_eq!(facts, (1, 0, 0, true));
    assert!(disposed_offer.is_ok());
    assert_eq!(disposed.is_err(), capture);
    assert_eq!(drops.load(Ordering::SeqCst), usize::from(capture));
    Ok(())
}

struct HoldingAdmission {
    inner: Original,
    witness: Payload,
}
impl Future for HoldingAdmission {
    type Output = Result<ServerSession, EngineError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _retained_capture = &self.witness;
        self.inner.as_mut().poll(cx)
    }
}

#[tokio::test]
async fn ready_accepted_session_sealed_before_claim_stays_rooted_unlaunched() -> TestResult {
    accepted_unlaunched(false).await
}
#[tokio::test]
async fn admission_ready_output_survives_completed_capture_drop_panic_after_barriers() -> TestResult
{
    accepted_unlaunched(true).await
}
