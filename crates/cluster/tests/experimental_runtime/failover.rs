use cluster::{
    ClientWorkload, ExperimentalRaftCluster, ExperimentalRaftHandle, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection, QueueWriteResult, TransportWorkload,
};
use domain::{CommittedEntryId, EntityIncarnation, MessageRecord, MessageState, StateMachine};
use storage::{CommittedStore, StateStore, StoreSnapshot};

use super::{
    TestResult,
    fixture::{self, IDS, ReplicaControl},
};

for_each_backend!(
    joined_leader_stop_elects_a_surviving_majority_without_resending,
    joined_follower_stop_preserves_the_original_leaders_majority,
);

struct Original {
    result: QueueWriteResult,
    message: MessageRecord,
    incarnation: EntityIncarnation,
}

async fn seed<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult<(ExperimentalRaftCluster, [ReplicaControl<W>; 3], Original)> {
    let (cluster, controls) = fixture::create(stores).await?;
    let original = async {
        let (leader, handle) = fixture::create_queue(&cluster).await?;
        let body = b"original confirmed before any node stops".to_vec();
        let result = handle
            .submit(fixture::send(body.clone(), "original")?)
            .await?;
        assert_eq!(result.outcome, QueueWriteOutcome::Sent { sequence: 1 });
        assert_eq!(result.entry.node_id, leader);
        assert!(result.entry.term > 0 && result.entry.index > 0);
        let mut original = None;
        let mut identity = None;
        for control in &controls {
            let message = control.state.wait_message(1).await?;
            assert_eq!(message.body, body);
            assert_eq!(message.message_id, "original");
            assert_eq!(message.sequence.as_u64(), 1);
            assert_eq!(message.state, MessageState::Ready);
            assert_eq!(message.delivery_count, 0);
            let machine = StateMachine::new(control.state.reader());
            assert_eq!(
                machine.queue_config(&fixture::namespace()?, &fixture::entity()?)?,
                Some(domain::QueueConfig::default())
            );
            let incarnation = machine
                .entity_incarnation(&fixture::namespace()?, &fixture::entity()?)?
                .ok_or("confirmed queue has no incarnation")?;
            assert_eq!(incarnation.generation(), 1);
            assert!(!incarnation.is_retired());
            if let Some(expected) = &original {
                assert_eq!(&message, expected);
            }
            if let Some(expected) = identity {
                assert_eq!(incarnation, expected);
            }
            original = Some(message);
            identity = Some(incarnation);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Original {
            result,
            message: original.ok_or("missing replicated original")?,
            incarnation: identity.ok_or("missing replicated identity")?,
        })
    }
    .await;
    match original {
        Ok(original) => Ok((cluster, controls, original)),
        Err(error) => {
            cluster.shutdown().await?;
            Err(error)
        }
    }
}

fn handles(cluster: &ExperimentalRaftCluster) -> TestResult<Vec<ExperimentalRaftHandle>> {
    IDS.into_iter()
        .map(|id| {
            cluster
                .handle(id)
                .ok_or_else(|| "missing original node handle".into())
        })
        .collect()
}

fn node_index(id: u64) -> TestResult<usize> {
    IDS.iter()
        .position(|candidate| *candidate == id)
        .ok_or_else(|| "unexpected replica ID".into())
}

fn snapshot<W: CommittedStore>(control: &ReplicaControl<W>) -> TestResult<[StoreSnapshot; 2]> {
    Ok([
        control.log.reader().snapshot()?,
        control.state.reader().snapshot()?,
    ])
}

fn unchanged<W: CommittedStore>(
    control: &ReplicaControl<W>,
    expected: &[StoreSnapshot; 2],
) -> TestResult {
    assert_eq!(snapshot(control)?, *expected);
    Ok(())
}

fn stopped_routes(cluster: &ExperimentalRaftCluster, stopped: u64) {
    assert!(cluster.handle(stopped).is_none());
    assert_eq!(
        cluster.node_ids().collect::<Vec<_>>(),
        IDS.into_iter()
            .filter(|id| *id != stopped)
            .collect::<Vec<_>>()
    );
    assert_ne!(cluster.leader_hint(), Some(stopped));
}

async fn closed(handle: &ExperimentalRaftHandle) -> TestResult {
    assert_eq!(
        handle
            .submit(fixture::send(Vec::new(), "never-admitted-after-stop")?)
            .await,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    assert_eq!(handle.workload(), ClientWorkload::default());
    Ok(())
}

async fn transport_idle(cluster: &ExperimentalRaftCluster) -> TestResult {
    loop {
        if cluster.transport_workload()?
            == (TransportWorkload {
                accepted_jobs: 0,
                encoded_bytes: 0,
            })
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn elected_majority<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[ReplicaControl<W>; 3],
    stopped: u64,
    after_term: u64,
) -> TestResult<CommittedEntryId> {
    loop {
        let mut applied = None;
        let mut agrees = true;
        for (index, id) in IDS.into_iter().enumerate() {
            if id == stopped {
                continue;
            }
            let position = controls[index].state.applied_position()?;
            if position
                .is_none_or(|position| position.term <= after_term || position.node_id == stopped)
            {
                agrees = false;
                break;
            }
            if applied.is_some_and(|first| Some(first) != position) {
                agrees = false;
                break;
            }
            applied = position;
        }
        if agrees
            && let Some(position) = applied
            && cluster.leader_hint() == Some(position.node_id)
        {
            // The higher-term native entry has actually reached both surviving
            // state writers. The routing hint alone is never the fence.
            return Ok(position);
        }
        tokio::task::yield_now().await;
    }
}

async fn submit_after_failover<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[ReplicaControl<W>; 3],
    original: &Original,
) -> TestResult<QueueWriteResult> {
    loop {
        let fence = elected_majority(
            cluster,
            controls,
            original.result.entry.node_id,
            original.result.entry.term,
        )
        .await?;
        let handle = cluster
            .handle(fence.node_id)
            .ok_or("elected survivor has no handle")?;
        match handle
            .submit(fixture::send(
                b"new intent after confirmed leader retirement".to_vec(),
                "after-failover",
            )?)
            .await
        {
            Ok(result) => {
                assert_eq!(result.entry.node_id, fence.node_id);
                assert!(result.entry.term >= fence.term);
                return Ok(result);
            }
            Err(QueueWriteError::KnownRejected(
                QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
            )) => {}
            // In particular, a submitted Unknown is returned once, never
            // retried or counted as an acknowledgement.
            Err(error) => return Err(error.into()),
        }
    }
}

async fn verify_survivors<W: CommittedStore>(
    controls: &[ReplicaControl<W>; 3],
    stopped: u64,
    original: &Original,
    result: QueueWriteResult,
    body: &[u8],
    name: &str,
) -> TestResult {
    assert_eq!(result.outcome, QueueWriteOutcome::Sent { sequence: 2 });
    assert!(result.entry.index > original.result.entry.index);
    assert_ne!(result.entry, original.result.entry);
    let mut expected = None;
    for (index, id) in IDS.into_iter().enumerate() {
        assert_eq!(
            controls[index].state.message(1)?,
            Some(original.message.clone())
        );
        assert!(controls[index].state.message(3)?.is_none());
        let machine = StateMachine::new(controls[index].state.reader());
        assert_eq!(
            machine.entity_incarnation(&fixture::namespace()?, &fixture::entity()?)?,
            Some(original.incarnation)
        );
        if id == stopped {
            assert!(controls[index].state.message(2)?.is_none());
            continue;
        }
        let message = controls[index].state.wait_message(2).await?;
        assert_eq!(message.sequence.as_u64(), 2);
        assert_eq!(message.body, body);
        assert_eq!(message.message_id, name);
        assert_eq!(message.state, MessageState::Ready);
        assert_eq!(message.delivery_count, 0);
        assert!(message.enqueued_at >= original.message.enqueued_at);
        if let Some(expected) = &expected {
            assert_eq!(&message, expected);
        }
        expected = Some(message);
    }
    Ok(())
}

async fn joined_leader_stop_elects_a_surviving_majority_without_resending<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (mut cluster, controls, original) = seed(stores).await?;
    let retained = handles(&cluster)?;
    let stopped = original.result.entry.node_id;
    let index = node_index(stopped)?;
    let mut stopped_snapshot = None;
    let result = async {
        cluster.stop_node(stopped).await?;
        stopped_routes(&cluster, stopped);
        closed(&retained[index]).await?;
        let baseline = snapshot(&controls[index])?;
        stopped_snapshot = Some(baseline.clone());
        let next = submit_after_failover(&cluster, &controls, &original).await?;
        assert_ne!(next.entry.node_id, stopped);
        assert!(next.entry.term > original.result.entry.term);
        verify_survivors(
            &controls,
            stopped,
            &original,
            next,
            b"new intent after confirmed leader retirement",
            "after-failover",
        )
        .await?;
        unchanged(&controls[index], &baseline)?;
        closed(&retained[index]).await?;
        transport_idle(&cluster).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    // Even a native Unknown failure must retire/join the real cluster before
    // returning; it never authorizes a retry or assumptions about the intent.
    let shutdown = cluster.shutdown().await;
    for handle in &retained {
        closed(handle).await?;
    }
    if let Some(baseline) = &stopped_snapshot {
        unchanged(&controls[index], baseline)?;
    }
    shutdown?;
    for control in &controls {
        assert!(control.state.message(3)?.is_none());
    }
    result
}

async fn joined_follower_stop_preserves_the_original_leaders_majority<W: CommittedStore>(
    stores: [(W, W); 3],
) -> TestResult {
    let (mut cluster, controls, original) = seed(stores).await?;
    let retained = handles(&cluster)?;
    let leader = original.result.entry.node_id;
    let stopped = IDS
        .into_iter()
        .find(|id| *id != leader)
        .ok_or("missing follower")?;
    let index = node_index(stopped)?;
    let leader_index = node_index(leader)?;
    let mut stopped_snapshot = None;
    let result = async {
        cluster.stop_node(stopped).await?;
        stopped_routes(&cluster, stopped);
        closed(&retained[index]).await?;
        let baseline = snapshot(&controls[index])?;
        stopped_snapshot = Some(baseline.clone());
        let next = loop {
            match retained[leader_index]
                .submit(fixture::send(
                    b"new intent on the original surviving leader".to_vec(),
                    "after-follower-stop",
                )?)
                .await
            {
                Ok(result) => break result,
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        assert_eq!(next.entry.node_id, leader);
        assert!(next.entry.term >= original.result.entry.term);
        verify_survivors(
            &controls,
            stopped,
            &original,
            next,
            b"new intent on the original surviving leader",
            "after-follower-stop",
        )
        .await?;
        unchanged(&controls[index], &baseline)?;
        transport_idle(&cluster).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let shutdown = cluster.shutdown().await;
    for handle in &retained {
        closed(handle).await?;
    }
    if let Some(baseline) = &stopped_snapshot {
        unchanged(&controls[index], baseline)?;
    }
    shutdown?;
    for control in &controls {
        assert!(control.state.message(3)?.is_none());
    }
    result
}
