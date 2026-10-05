use super::*;
use crate::listener::retained_connection::ready::{self as observe, Attempt, Primary};

fn empty_capabilities() -> (RetainedConnectionOwner<()>, Publisher) {
    let (owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let (claim, acceptor, publisher) = starter.claim().expect("fresh private claim");
    drop(claim);
    drop(acceptor);
    (owner, publisher)
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

fn publish_success(primary: &Primary) {
    let Attempt::Available(loan) = primary.loan() else {
        panic!("restored unique capability");
    };
    loan.publish(Outcome::Finished(Ok(())));
}

#[tokio::test]
async fn pending_leaf_cancel_restores_unique_primary_before_next_observer() {
    let (mut owner, publisher) = empty_capabilities();
    let primary = Primary::new(publisher.primary);
    drop(publisher.close);
    let observed = {
        let mut future = Box::pin(observe::with_primary(
            std::future::pending::<()>(),
            &primary,
            || (),
            |(), _| (),
            || {},
        ));
        let observed = poll_once(future.as_mut());
        drop(future);
        observed
    };
    publish_success(&primary);
    drop(primary);
    let report = owner.finish().await.expect("empty role report");
    assert!(observed.is_pending());
    assert!(matches!(
        report.outcomes().primary,
        Some(Outcome::Finished(Ok(())))
    ));
    assert!(report.wrapper().is_none() && report.actor().is_none() && report.reader().is_none());
}

#[tokio::test]
async fn inner_poll_unwind_restores_capability_without_inventing_ready_output() {
    let (mut owner, publisher) = empty_capabilities();
    let primary = Primary::new(publisher.primary);
    drop(publisher.close);
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let future = std::future::poll_fn(|_| -> Poll<()> {
            panic!("unobserved leaf poll");
        });
        let mut observed = Box::pin(observe::with_primary(
            future,
            &primary,
            || (),
            |(), _| (),
            || {},
        ));
        poll_once(observed.as_mut())
    }));
    let before = owner.published();
    publish_success(&primary);
    drop(primary);
    let report = owner.finish().await.expect("empty role report");
    assert!(unwound.is_err());
    assert_eq!(before, (false, false));
    assert!(report.outcomes().primary.is_some());
    drop(unwound);
}

#[tokio::test]
async fn published_marker_never_polls_another_raw_outcome_future() {
    let (mut owner, publisher) = empty_capabilities();
    let primary = Primary::new(publisher.primary);
    drop(publisher.close);
    publish_success(&primary);
    let polls = AtomicUsize::new(0);
    let closed = observe::with_primary(
        std::future::poll_fn(|_| {
            polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(())
        }),
        &primary,
        || true,
        |(), _| false,
        || {},
    )
    .await;
    drop(primary);
    let report = owner.finish().await.expect("empty role report");
    assert!(closed);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(report.outcomes().primary.is_some());
}

#[tokio::test]
async fn overlapping_private_loan_refuses_before_poll_not_as_published() {
    let (mut owner, publisher) = empty_capabilities();
    let primary = Primary::new(publisher.primary);
    drop(publisher.close);
    let Attempt::Available(loan) = primary.loan() else {
        panic!("fresh loan");
    };
    let polls = AtomicUsize::new(0);
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let mut observed = Box::pin(observe::with_primary(
            std::future::poll_fn(|_| {
                polls.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(())
            }),
            &primary,
            || (),
            |(), _| (),
            || {},
        ));
        poll_once(observed.as_mut())
    }));
    drop(loan);
    publish_success(&primary);
    drop(primary);
    let report = owner.finish().await.expect("empty role report");
    assert!(unwound.is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(report.outcomes().primary.is_some());
    drop(unwound);
}

struct ReadyThenDropPanic {
    result: Option<RetainedConnectionResult>,
    capture: Arc<PayloadWitness>,
}
impl Future for ReadyThenDropPanic {
    type Output = RetainedConnectionResult;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.result.take().expect("single actual Ready"))
    }
}
impl Drop for ReadyThenDropPanic {
    fn drop(&mut self) {
        std::panic::panic_any(PayloadError {
            witness: self.capture.clone(),
            name: "private-completed-future-capture",
        });
    }
}

async fn future_drop(close: bool) {
    let (mut owner, publisher) = empty_capabilities();
    let original = Arc::new(PayloadWitness::default());
    let capture = Arc::new(PayloadWitness::default());
    let future = ReadyThenDropPanic {
        result: Some(Err(Box::new(PayloadError {
            witness: original.clone(),
            name: "private-ready-result",
        }))),
        capture: capture.clone(),
    };
    let unwound = if close {
        drop(publisher.primary);
        catch_unwind(AssertUnwindSafe(|| {
            let mut observed = Box::pin(observe::observe(
                future,
                |result| publisher.close.publish(result),
                || {},
            ));
            let ready = poll_once(observed.as_mut());
            drop(observed);
            ready
        }))
    } else {
        drop(publisher.close);
        let primary = Primary::new(publisher.primary);
        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let mut observed = Box::pin(observe::with_primary(
                future,
                &primary,
                || (),
                |result, loan| loan.publish(Outcome::Finished(result)),
                || {},
            ));
            let ready = poll_once(observed.as_mut());
            drop(observed);
            ready
        }));
        drop(primary);
        unwound
    };
    let before = (
        original.drops.load(Ordering::SeqCst),
        capture.drops.load(Ordering::SeqCst),
    );
    let report = owner.finish().await.expect("empty role report");
    assert!(unwound.is_err());
    assert_eq!(before, (0, 0));
    let error = if close {
        report
            .outcomes()
            .websocket_close
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .map(Box::as_ref)
    } else {
        primary_error(&report)
    };
    assert!(
        error
            .and_then(|e| e.downcast_ref::<PayloadError>())
            .is_some_and(|e| Arc::ptr_eq(&e.witness, &original))
    );
    drop(report);
    drop(unwound);
    assert_eq!(original.drops.load(Ordering::SeqCst), 1);
    assert_eq!(capture.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn primary_ready_arm_roots_output_before_completed_future_drop_panics() {
    future_drop(false).await;
}

#[tokio::test]
async fn close_ready_arm_roots_output_before_completed_future_drop_panics() {
    future_drop(true).await;
}
