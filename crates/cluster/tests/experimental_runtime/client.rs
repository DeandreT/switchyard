use std::future::Future;

use cluster::{
    ClientWorkload, LogQueueRefusal, MAX_LOG_BODY_BYTES, QueueWriteError, QueueWriteOutcome,
    QueueWriteRejection,
};
use storage::{CommittedStore, StateStore};

use super::{
    TestResult,
    fixture::{self, IDS},
};

for_each_backend!(
    unpolled_submission_is_inert_and_joined_shutdown_retires_old_handles,
    caller_loss_retains_active_admission_and_count_capacity_until_real_completion,
    encoded_byte_capacity_is_independent_of_the_sixteen_job_limit,
    business_refusal_is_committed_without_allocating_the_next_message,
);

fn owned<F: Future + Send + 'static>(future: F) -> F {
    future
}

async fn unpolled_submission_is_inert_and_joined_shutdown_retires_old_handles<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let before = controls[index].state.reader().snapshot()?;
    let unpolled = owned(handle.submit(fixture::send(b"never admitted".to_vec(), "unpolled")?));
    assert_eq!(handle.workload(), ClientWorkload::default());
    drop(unpolled);
    assert_eq!(handle.workload(), ClientWorkload::default());
    assert_eq!(controls[index].state.reader().snapshot()?, before);
    assert!(controls[index].state.message(1)?.is_none());

    cluster.shutdown().await?;
    let stopped = controls
        .iter()
        .map(|control| control.state.reader().snapshot())
        .collect::<Result<Vec<_>, _>>()?;
    let result =
        owned(handle.submit(fixture::send(b"retired generation".to_vec(), "stale")?)).await;
    assert_eq!(
        result,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    assert_eq!(handle.workload(), ClientWorkload::default());
    for (control, snapshot) in controls.iter().zip(stopped) {
        assert_eq!(control.state.reader().snapshot()?, snapshot);
        assert!(control.state.message(1)?.is_none());
    }
    Ok(())
}

async fn caller_loss_retains_active_admission_and_count_capacity_until_real_completion<
    W: CommittedStore,
>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let commit = controls[index].state.gate_message(1)?;
    let original = b"caller lost after native submission".to_vec();
    let mut first = Box::pin(owned(
        handle.submit(fixture::send(original.clone(), "lost-first")?),
    ));
    fixture::pending(first.as_mut()).await?;
    commit.entered().await;
    assert_eq!(handle.workload().accepted_jobs, 1);
    assert!(handle.workload().encoded_bytes > 0);
    assert!(controls[index].state.message(1)?.is_none());
    drop(first);
    assert_eq!(handle.workload().accepted_jobs, 1);

    let mut queued = Vec::new();
    for sequence in 2..=16_u64 {
        let mut submission = Box::pin(owned(handle.submit(fixture::send(
            vec![sequence as u8],
            &format!("count-{sequence}"),
        )?)));
        fixture::pending(submission.as_mut()).await?;
        queued.push((sequence, submission));
    }
    let full = handle.workload();
    assert_eq!(full.accepted_jobs, 16);
    assert!(full.encoded_bytes < 4 * 1024 * 1024);
    let result = handle
        .submit(fixture::send(b"not admitted".to_vec(), "seventeenth")?)
        .await;
    assert_eq!(
        result,
        Err(QueueWriteError::KnownRejected(
            QueueWriteRejection::Capacity
        ))
    );
    assert_eq!(handle.workload(), full);
    assert!(controls[index].state.message(1)?.is_none());

    commit.release();
    commit.persisted().await;
    for (sequence, submission) in queued {
        assert_eq!(
            submission.await?.outcome,
            QueueWriteOutcome::Sent { sequence }
        );
    }
    assert_eq!(handle.workload(), ClientWorkload::default());
    let first = controls[index]
        .state
        .message(1)?
        .ok_or("lost caller discarded the admitted message")?;
    assert_eq!(first.body, original);
    assert_eq!(first.message_id, "lost-first");
    for sequence in 2..=16_u64 {
        let message = controls[index]
            .state
            .message(sequence)?
            .ok_or("missing admitted queued message")?;
        assert_eq!(message.body, vec![sequence as u8]);
        assert_eq!(message.message_id, format!("count-{sequence}"));
    }
    assert!(controls[index].state.message(17)?.is_none());
    cluster.shutdown().await?;
    Ok(())
}

async fn encoded_byte_capacity_is_independent_of_the_sixteen_job_limit<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let commit = controls[index].state.gate_message(1)?;
    let mut submissions = Vec::new();
    for sequence in 1..=15_u64 {
        let mut submission = Box::pin(owned(handle.submit(fixture::send(
            vec![sequence as u8; MAX_LOG_BODY_BYTES],
            &format!("bytes-{sequence}"),
        )?)));
        fixture::pending(submission.as_mut()).await?;
        if sequence == 1 {
            commit.entered().await;
        }
        submissions.push((sequence, submission));
    }
    let full = handle.workload();
    assert_eq!(full.accepted_jobs, 15);
    assert!(full.accepted_jobs < 16);
    assert!(full.encoded_bytes <= 4 * 1024 * 1024);
    assert!(full.encoded_bytes + MAX_LOG_BODY_BYTES > 4 * 1024 * 1024);
    let result = handle
        .submit(fixture::send(vec![0; MAX_LOG_BODY_BYTES], "byte-overflow")?)
        .await;
    assert_eq!(
        result,
        Err(QueueWriteError::KnownRejected(
            QueueWriteRejection::Capacity
        ))
    );
    assert_eq!(handle.workload(), full);
    assert!(controls[index].state.message(1)?.is_none());

    commit.release();
    commit.persisted().await;
    for (sequence, submission) in submissions {
        assert_eq!(
            submission.await?.outcome,
            QueueWriteOutcome::Sent { sequence }
        );
    }
    assert_eq!(handle.workload(), ClientWorkload::default());
    for sequence in 1..=15_u64 {
        let message = controls[index]
            .state
            .message(sequence)?
            .ok_or("missing byte-budgeted message")?;
        assert_eq!(message.body, vec![sequence as u8; MAX_LOG_BODY_BYTES]);
        assert_eq!(message.message_id, format!("bytes-{sequence}"));
    }
    assert!(controls[index].state.message(16)?.is_none());
    cluster.shutdown().await?;
    Ok(())
}

async fn business_refusal_is_committed_without_allocating_the_next_message<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (cluster, controls) = fixture::create(stores).await?;
    let (leader, handle) = fixture::create_queue(&cluster).await?;
    let index = IDS
        .iter()
        .position(|id| *id == leader)
        .ok_or("unknown leader")?;
    let refusal = handle
        .submit(fixture::send(b"refused body".to_vec(), &"x".repeat(129))?)
        .await?;
    assert_eq!(
        refusal.outcome,
        QueueWriteOutcome::Refused(LogQueueRefusal::MessageIdTooLong {
            length: 129,
            maximum: 128,
        })
    );
    assert_eq!(refusal.entry.node_id, leader);
    assert!(controls[index].state.message(1)?.is_none());
    let body = b"original committed body".to_vec();
    let sent = handle
        .submit(fixture::send(body.clone(), "after-refusal")?)
        .await?;
    assert_eq!(sent.outcome, QueueWriteOutcome::Sent { sequence: 1 });
    assert!(sent.entry.index > refusal.entry.index);
    assert_eq!(handle.workload(), ClientWorkload::default());
    let message = controls[index]
        .state
        .message(1)?
        .ok_or("missing original body after refusal")?;
    assert_eq!(message.body, body);
    assert_eq!(message.message_id, "after-refusal");
    assert!(controls[index].state.message(2)?.is_none());
    cluster.shutdown().await?;
    Ok(())
}
