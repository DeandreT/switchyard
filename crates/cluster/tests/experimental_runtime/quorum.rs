use cluster::{QueueWriteError, QueueWriteOutcome, QueueWriteRejection, QueueWriteUnknown};
use storage::CommittedStore;

use super::{
    TestResult,
    fixture::{self, IDS},
};

for_each_backend!(
    send_acknowledgment_requires_persisted_majority_and_local_application,
    minority_cannot_submit_a_queue_intent,
    shutdown_holds_unknown_reply_until_native_storage_drains,
    canceled_node_stop_remains_in_the_whole_cluster_join_barrier,
);

async fn send_acknowledgment_requires_persisted_majority_and_local_application<
    W: CommittedStore,
>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    for control in &controls {
        control.state.wait_queue().await?;
    }
    let leader_index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let followers = (0..3)
        .filter(|index| *index != leader_index)
        .collect::<Vec<_>>();
    let first = controls[followers[0]].log.gate_log_append();
    let second = controls[followers[1]].log.gate_log_append();
    let application = controls[leader_index].state.gate_message(1)?;
    let body = b"original quorum message".to_vec();
    let mut submission = Box::pin(handle.submit(fixture::send(body.clone(), "quorum-message")?));
    fixture::pending(submission.as_mut()).await?;
    first.entered().await;
    second.entered().await;
    fixture::pending(submission.as_mut()).await?;
    assert!(!application.has_entered());
    assert!(controls[leader_index].state.message(1)?.is_none());

    first.release();
    first.persisted().await;
    application.entered().await;
    fixture::pending(submission.as_mut()).await?;
    assert!(controls[leader_index].state.message(1)?.is_none());
    application.release();
    application.persisted().await;
    let result = submission.await?;
    assert_eq!(result.outcome, QueueWriteOutcome::Sent { sequence: 1 });
    assert_eq!(result.entry.node_id, leader);
    assert!(result.entry.term > 0 && result.entry.index > 0);
    assert_eq!(handle.workload().accepted_jobs, 0);
    assert_eq!(handle.workload().encoded_bytes, 0);
    assert_eq!(
        controls[leader_index]
            .state
            .message(1)?
            .ok_or("missing leader message")?
            .body,
        body
    );
    assert_eq!(
        controls[followers[0]].state.wait_message(1).await?.body,
        body
    );
    second.release();
    second.persisted().await;
    assert_eq!(
        controls[followers[1]].state.wait_message(1).await?.body,
        body
    );
    cluster.shutdown().await?;
    Ok(())
}

async fn canceled_node_stop_remains_in_the_whole_cluster_join_barrier<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (mut cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let application = controls[index].state.gate_message(1)?;
    let mut submission = Box::pin(handle.submit(fixture::send(
        b"drain after canceled stop".to_vec(),
        "canceled-stop",
    )?));
    fixture::pending(submission.as_mut()).await?;
    application.entered().await;
    let mut stop = Box::pin(cluster.stop_node(leader));
    fixture::pending(stop.as_mut()).await?;
    drop(stop);
    assert!(cluster.handle(leader).is_none());
    assert_ne!(cluster.leader_hint(), Some(leader));
    let mut repeated_stop = Box::pin(cluster.stop_node(leader));
    fixture::pending(repeated_stop.as_mut()).await?;
    drop(repeated_stop);
    let mut shutdown = Box::pin(cluster.shutdown());
    fixture::pending(shutdown.as_mut()).await?;
    assert_eq!(
        handle.submit(fixture::send(Vec::new(), "closed")?).await,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    fixture::pending(shutdown.as_mut()).await?;
    fixture::pending(submission.as_mut()).await?;
    assert_eq!(handle.workload().accepted_jobs, 1);
    application.release();
    application.persisted().await;
    shutdown.await?;
    assert_eq!(
        submission.await,
        Err(QueueWriteError::Unknown(QueueWriteUnknown::Stopped))
    );
    assert_eq!(handle.workload().accepted_jobs, 0);
    assert_eq!(
        controls[index]
            .state
            .message(1)?
            .ok_or("canceled stop lost queued application")?
            .body,
        b"drain after canceled stop"
    );
    Ok(())
}

async fn minority_cannot_submit_a_queue_intent<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (mut cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    for id in IDS.into_iter().filter(|id| *id != leader) {
        cluster.stop_node(id).await?;
    }
    let result = handle
        .submit(fixture::send(b"not submitted".to_vec(), "minority")?)
        .await;
    assert_eq!(
        result,
        Err(QueueWriteError::KnownRejected(
            QueueWriteRejection::QuorumUnavailable
        ))
    );
    for control in &controls {
        assert!(control.state.message(1)?.is_none());
    }
    assert_eq!(handle.workload().accepted_jobs, 0);
    cluster.shutdown().await?;
    Ok(())
}

async fn shutdown_holds_unknown_reply_until_native_storage_drains<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let application = controls[index].state.gate_message(1)?;
    let mut submission = Box::pin(handle.submit(fixture::send(
        b"committed before shutdown".to_vec(),
        "shutdown",
    )?));
    fixture::pending(submission.as_mut()).await?;
    application.entered().await;
    let mut shutdown = Box::pin(cluster.shutdown());
    fixture::pending(shutdown.as_mut()).await?;
    let rejected = handle
        .submit(fixture::send(Vec::new(), "after-close")?)
        .await;
    assert_eq!(
        rejected,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    fixture::pending(shutdown.as_mut()).await?;
    fixture::pending(submission.as_mut()).await?;
    assert_eq!(handle.workload().accepted_jobs, 1);
    application.release();
    application.persisted().await;
    shutdown.await?;
    assert_eq!(
        submission.await,
        Err(QueueWriteError::Unknown(QueueWriteUnknown::Stopped))
    );
    assert_eq!(handle.workload().accepted_jobs, 0);
    assert_eq!(handle.workload().encoded_bytes, 0);
    assert_eq!(
        controls[index]
            .state
            .message(1)?
            .ok_or("accepted message lost during native drain")?
            .body,
        b"committed before shutdown"
    );
    Ok(())
}
