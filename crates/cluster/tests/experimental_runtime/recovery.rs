use cluster::{
    ExperimentalLogStore, ExperimentalRaftCluster, ExperimentalReplicaStores,
    ExperimentalStateMachine, LogEntry, LogId, LogProfile, QueueLogCommand, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection, QueueWriteResult, QueueWriteUnknown,
};
use domain::{CommittedSend, StateMachine};
use openraft::{CommittedLeaderId, EntryPayload, RaftLogReader, storage::RaftStateMachine};
use storage::{CommittedStore, StateStore};

use super::{
    DEADLINE, TestResult,
    fixture::{self, IDS},
};

#[tokio::test]
async fn joined_cluster_reopens_all_six_directories_without_reinitializing_or_resending()
-> TestResult {
    tokio::time::timeout(DEADLINE, durable_recovery()).await??;
    Ok(())
}

async fn durable_recovery() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let (cluster, controls) = fixture::create(fixture::durable_stores(directory.path())?).await?;
    let (_, handle) = fixture::create_queue(&cluster).await?;
    let body = b"original persisted body".to_vec();
    let result = handle
        .submit(fixture::send(body.clone(), "original")?)
        .await?;
    assert_eq!(result.outcome, QueueWriteOutcome::Sent { sequence: 1 });
    let mut messages = Vec::new();
    for control in &controls {
        messages.push(control.state.wait_message(1).await?);
    }
    cluster.shutdown().await?;
    let mut snapshots = Vec::new();
    for control in &controls {
        snapshots.push((
            control.log.reader().snapshot()?,
            control.state.reader().snapshot()?,
        ));
    }
    drop(controls);

    let mut prepared = Vec::new();
    let mut readers = Vec::new();
    for (index, (log_writer, state_writer)) in fixture::durable_stores(directory.path())?
        .into_iter()
        .enumerate()
    {
        assert_eq!(log_writer.reader().snapshot()?, snapshots[index].0);
        assert_eq!(state_writer.reader().snapshot()?, snapshots[index].1);
        let reader = state_writer.reader();
        let mut log = ExperimentalLogStore::open(
            log_writer,
            LogProfile::new(IDS[index], fixture::stream()?)?,
        )?;
        let mut state = ExperimentalStateMachine::open(state_writer, fixture::stream()?)?;
        let log_id = LogId::new(
            CommittedLeaderId::new(result.entry.term, result.entry.node_id),
            result.entry.index,
        );
        let original = LogEntry {
            log_id,
            payload: EntryPayload::Normal(QueueLogCommand::send(
                fixture::namespace()?,
                fixture::entity()?,
                messages[index].enqueued_at,
                CommittedSend {
                    message_id: "original".into(),
                    body: body.clone(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        };
        assert_eq!(
            log.limited_get_log_entries(log_id.index, log_id.index + 1)
                .await?,
            vec![original]
        );
        let applied = state
            .applied_state()
            .await?
            .0
            .ok_or("missing recovered applied state")?;
        assert!(applied.index >= log_id.index);
        if applied.index == log_id.index {
            assert_eq!(applied, log_id);
        }
        assert_eq!(
            StateMachine::new(reader.clone()).message(
                &fixture::namespace()?,
                &fixture::entity()?,
                domain::SequenceNumber::new(1)
            )?,
            Some(messages[index].clone())
        );
        readers.push(reader);
        prepared.push(ExperimentalReplicaStores::prepare(IDS[index], log, state).await?);
    }
    let prepared = prepared
        .try_into()
        .map_err(|_| "expected three reopened pairs")?;
    let reopened = ExperimentalRaftCluster::open(prepared).await?;
    // Recovery does not promise stable leadership for the next submission.
    // A native leadership-change result is unknown and is never resubmitted.
    let next =
        submit_on_elected_leader(&reopened, b"second persisted body".to_vec(), "second").await?;
    let acknowledged_node = match next {
        Ok(next) => {
            assert_eq!(next.outcome, QueueWriteOutcome::Sent { sequence: 2 });
            assert!(next.entry.index > result.entry.index);
            Some(next.entry.node_id)
        }
        Err(QueueWriteError::Unknown(QueueWriteUnknown::LeadershipChanged)) => None,
        Err(error) => return Err(error.into()),
    };
    reopened.shutdown().await?;
    for (index, reader) in readers.into_iter().enumerate() {
        let machine = StateMachine::new(reader);
        assert_eq!(
            machine.message(
                &fixture::namespace()?,
                &fixture::entity()?,
                domain::SequenceNumber::new(1)
            )?,
            Some(messages[index].clone())
        );
        let second = machine.message(
            &fixture::namespace()?,
            &fixture::entity()?,
            domain::SequenceNumber::new(2),
        )?;
        if acknowledged_node == Some(IDS[index]) {
            assert!(second.is_some());
        }
        if let Some(second) = second {
            assert_eq!(second.body, b"second persisted body");
            assert_eq!(second.message_id, "second");
        }
        assert!(
            machine
                .message(
                    &fixture::namespace()?,
                    &fixture::entity()?,
                    domain::SequenceNumber::new(3)
                )?
                .is_none()
        );
    }
    assert_eq!(
        handle.submit(fixture::send(Vec::new(), "stale")?).await,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    Ok(())
}

async fn submit_on_elected_leader(
    cluster: &ExperimentalRaftCluster,
    body: Vec<u8>,
    name: &str,
) -> TestResult<Result<QueueWriteResult, QueueWriteError>> {
    loop {
        if let Some(id) = cluster.leader_hint() {
            let handle = cluster.handle(id).ok_or("closed leader hint")?;
            match handle.submit(fixture::send(body.clone(), name)?).await {
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {}
                result => return Ok(result),
            }
        }
        tokio::task::yield_now().await;
    }
}
