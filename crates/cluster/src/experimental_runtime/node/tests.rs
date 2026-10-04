use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use super::{NodeStopCause, StopSignal};

mod fixture;
mod runtime;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(_) => Poll::Ready(Err(
            "owned lifecycle completed before its controlled boundary".into(),
        )),
    })
    .await
}

#[tokio::test]
async fn a_stop_published_before_the_wait_is_observed() -> TestResult {
    let stop = StopSignal::new();
    assert!(stop.request(NodeStopCause::Shutdown));
    assert!(!stop.request(NodeStopCause::Shutdown));
    tokio::time::timeout(DEADLINE, stop.requested()).await?;
    assert!(stop.is_requested());
    assert!(!stop.has_fatal());
    Ok(())
}

#[tokio::test]
async fn a_registered_waiter_observes_one_synchronous_publication() -> TestResult {
    let stop = StopSignal::new();
    let mut notified = Box::pin(stop.requested());
    pending(notified.as_mut()).await?;
    stop.request(NodeStopCause::NetworkWorkerLost);
    tokio::time::timeout(DEADLINE, notified).await?;
    assert!(stop.has_fatal());
    Ok(())
}

#[tokio::test]
async fn normal_shutdown_cannot_mask_a_later_owner_failure() -> TestResult {
    let stop = StopSignal::new();
    stop.request(NodeStopCause::Shutdown);
    assert!(!stop.has_fatal());
    assert!(stop.request(NodeStopCause::ClientOwnerLost));
    assert!(stop.has_fatal());
    stop.request(NodeStopCause::NetworkWorkerLost);
    tokio::time::timeout(DEADLINE, stop.requested()).await?;
    assert!(stop.has_fatal());
    Ok(())
}

#[test]
fn signal_clones_have_no_drop_or_debug_authority() {
    let stop = StopSignal::new();
    drop(stop.clone());
    assert!(!stop.is_requested());
    assert_eq!(format!("{stop:?}"), "StopSignal { .. }");
}
