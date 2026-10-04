use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use tokio::sync::watch;

use super::{Error, NodeRetirement};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(_) => Poll::Ready(Err(
            "retirement completed before its controlled publication".into(),
        )),
    })
    .await
}

#[tokio::test]
async fn a_completion_published_before_join_is_retained() -> TestResult {
    let (finished, completed) = watch::channel(None);
    let notice = NodeRetirement::new(completed);
    finished.send(Some(Ok(())))?;
    drop(finished);
    tokio::time::timeout(DEADLINE, notice.join()).await??;
    Ok(())
}

#[tokio::test]
async fn canceling_one_join_cannot_discard_another_retained_notice() -> TestResult {
    let (finished, completed) = watch::channel(None);
    let notice = NodeRetirement::new(completed);
    let mut canceled = Box::pin(notice.clone().join());
    pending(canceled.as_mut()).await?;
    drop(canceled);
    let mut retained = Box::pin(notice.clone().join());
    pending(retained.as_mut()).await?;
    finished.send(Some(Ok(())))?;
    tokio::time::timeout(DEADLINE, retained).await??;
    tokio::time::timeout(DEADLINE, notice.join()).await??;
    Ok(())
}

#[tokio::test]
async fn every_notice_retains_the_exact_joined_failure() -> TestResult {
    let (finished, completed) = watch::channel(None);
    let notice = NodeRetirement::new(completed);
    let second = notice.clone();
    finished.send(Some(Err(Error::OwnerFailure)))?;
    drop(finished);
    assert_eq!(
        tokio::time::timeout(DEADLINE, notice.join()).await?,
        Err(Error::OwnerFailure)
    );
    assert_eq!(
        tokio::time::timeout(DEADLINE, second.join()).await?,
        Err(Error::OwnerFailure)
    );
    Ok(())
}

#[tokio::test]
async fn a_lost_completion_publisher_is_not_reported_as_joined() -> TestResult {
    let (finished, completed) = watch::channel(None);
    let notice = NodeRetirement::new(completed);
    let mut joined = Box::pin(notice.join());
    pending(joined.as_mut()).await?;
    drop(finished);
    assert_eq!(
        tokio::time::timeout(DEADLINE, joined).await?,
        Err(Error::TaskFailed)
    );
    Ok(())
}

#[tokio::test]
async fn notice_clones_and_debug_cannot_publish_or_cancel_completion() -> TestResult {
    let (finished, completed) = watch::channel(None);
    let notice = NodeRetirement::new(completed);
    assert_eq!(format!("{notice:?}"), "NodeRetirement { .. }");
    drop(notice.clone());
    assert_eq!(*finished.borrow(), None);
    let mut joined = Box::pin(notice.join());
    pending(joined.as_mut()).await?;
    assert_eq!(*finished.borrow(), None);
    finished.send(Some(Ok(())))?;
    tokio::time::timeout(DEADLINE, joined).await??;
    Ok(())
}

async fn retained_cluster_shutdown(first: Result<(), Error>) -> TestResult {
    let (first_sender, first_notice) = watch::channel(Some(first));
    let (second_sender, second_notice) = watch::channel(Some(Ok(())));
    let (last_sender, last_notice) = watch::channel(None);
    let stream = domain::CommittedStreamId::new([99; 16])?;
    let cluster = crate::experimental_runtime::ExperimentalRaftCluster {
        nodes: BTreeMap::new(),
        retiring: BTreeMap::from([
            (7, NodeRetirement::new(first_notice)),
            (8, NodeRetirement::new(second_notice)),
            (9, NodeRetirement::new(last_notice)),
        ]),
        routes: crate::experimental_runtime::network::Routes::new(stream, [7, 8, 9])?,
        stream,
    };
    let mut shutdown = Box::pin(cluster.shutdown());
    pending(shutdown.as_mut()).await?;
    // Receiver closure fences actual traversal by the owned shutdown task,
    // rather than assuming a yield scheduled it before a Pending assertion.
    tokio::time::timeout(DEADLINE, first_sender.closed()).await?;
    tokio::time::timeout(DEADLINE, second_sender.closed()).await?;
    assert_eq!(last_sender.receiver_count(), 1);
    pending(shutdown.as_mut()).await?;
    last_sender.send(Some(Ok(())))?;
    assert_eq!(tokio::time::timeout(DEADLINE, shutdown).await?, first);
    Ok(())
}

#[tokio::test]
async fn cluster_shutdown_retains_a_prior_stop_after_joining_ready_notices() -> TestResult {
    retained_cluster_shutdown(Ok(())).await
}

#[tokio::test]
async fn cluster_shutdown_joins_every_prior_stop_even_after_one_failed() -> TestResult {
    retained_cluster_shutdown(Err(Error::OwnerFailure)).await
}
