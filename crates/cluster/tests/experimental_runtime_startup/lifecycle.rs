use cluster::{
    ExperimentalLogStore, ExperimentalRaftCluster, ExperimentalReplicaStores,
    ExperimentalStateMachine, LogProfile, QueueIntent, QueueWriteError, QueueWriteOutcome,
    QueueWriteRejection, QueueWriteResult, QueueWriteUnknown, ReplicaRuntimeError,
};

use super::{
    DEADLINE, TestResult,
    fixture::{self, Backend, Seed},
};

async fn submit_create_queue(
    cluster: &ExperimentalRaftCluster,
) -> TestResult<Result<QueueWriteResult, QueueWriteError>> {
    // Only known pre-submission leadership refusals may be retried. A submitted
    // unknown decision is never treated as permission to send the intent again.
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let Some(id) = cluster.leader_hint() {
                let handle = cluster
                    .handle(id)
                    .ok_or("leader hint has no bounded handle")?;
                let intent = QueueIntent::create_queue(
                    domain::NamespaceName::new("tenant")?,
                    domain::EntityPath::new("orders")?,
                    domain::QueueConfig::default(),
                )?;
                match handle.submit(intent).await {
                    Err(QueueWriteError::KnownRejected(
                        QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                    )) => {}
                    result => {
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result);
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

async fn create_queue(cluster: &ExperimentalRaftCluster) -> TestResult {
    let result = submit_create_queue(cluster).await??;
    assert_eq!(result.outcome, QueueWriteOutcome::QueueCreated);
    assert!(result.entry.index > 0);
    Ok(())
}

pub(super) async fn unpolled_create_does_not_start_or_mutate<B: Backend>(
    backend: &B,
) -> TestResult {
    let (stores, evidence) = fixture::prepare(backend, "unpolled", Seed::default()).await?;
    drop(ExperimentalRaftCluster::create(stores));
    fixture::retired(&evidence).await?;
    for row in &evidence {
        row.unchanged()?;
        row.unread();
        assert_eq!(row.log.full_scans(), 0);
    }
    Ok(())
}

pub(super) async fn actual_create_uses_public_bounded_handle<B: Backend>(
    backend: &B,
) -> TestResult {
    let (stores, evidence) = fixture::prepare(backend, "positive-create", Seed::default()).await?;
    let cluster = ExperimentalRaftCluster::create(stores).await?;
    assert_eq!(cluster.node_ids().collect::<Vec<_>>(), fixture::IDS);
    assert_eq!(cluster.stream(), fixture::stream());
    assert!(cluster.handle(10).is_none());
    create_queue(&cluster).await?;
    let old = cluster.handle(7).ok_or("missing node handle")?;
    cluster.shutdown().await?;
    fixture::retired(&evidence).await?;
    let intent = QueueIntent::create_queue(
        domain::NamespaceName::new("tenant")?,
        domain::EntityPath::new("never")?,
        domain::QueueConfig::default(),
    )?;
    assert_eq!(
        old.submit(intent).await,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    Ok(())
}

pub(super) async fn actual_open_uses_existing_fixed_history<B: Backend>(backend: &B) -> TestResult {
    let (stores, evidence) =
        fixture::prepare(backend, "positive-open", Seed::initialized()).await?;
    let cluster = ExperimentalRaftCluster::open(stores).await?;
    // Opening preserves history, but does not promise stable leadership for a
    // new intent. Never resubmit a native post-attempt unknown result.
    let acknowledged_node = match submit_create_queue(&cluster).await? {
        Ok(result) => {
            assert_eq!(result.outcome, QueueWriteOutcome::QueueCreated);
            assert!(result.entry.index > 1);
            Some(result.entry.node_id)
        }
        Err(QueueWriteError::Unknown(QueueWriteUnknown::LeadershipChanged)) => None,
        Err(error) => return Err(error.into()),
    };
    cluster.shutdown().await?;
    fixture::retired(&evidence).await?;
    for (index, row) in evidence.iter().enumerate() {
        let config = row.state.queue_config()?;
        if acknowledged_node == Some(fixture::IDS[index]) {
            assert!(config.is_some());
        }
        if let Some(config) = config {
            assert_eq!(config, domain::QueueConfig::default());
        }
    }
    let stores = fixture::reopen(&evidence, fixture::IDS, [fixture::stream(); 3]).await?;
    for store in &stores {
        assert!(store.progress().applied().is_some());
        assert_eq!(
            store.progress().membership(),
            &openraft::StoredMembership::new(Some(cluster::LogId::default()), fixture::members())
        );
    }
    fixture::stop_prepared(stores).await?;
    Ok(())
}

pub(super) async fn second_node_start_failure_joins_every_storage_owner<B: Backend>(
    backend: &B,
) -> TestResult {
    let (stores, evidence) =
        fixture::prepare(backend, "second-failure", Seed::initialized()).await?;
    evidence[1].log.fail_next_full_scan();
    match ExperimentalRaftCluster::open(stores).await {
        Err(error) => assert_eq!(error, ReplicaRuntimeError::CoreFailure),
        Ok(cluster) => {
            cluster.shutdown().await?;
            return Err("second startup read failure was ignored".into());
        }
    }
    assert!(evidence[0].log.full_scans() > 0);
    assert!(evidence[1].log.fault_fired());
    assert_eq!(evidence[2].log.full_scans(), 0);
    fixture::retired(&evidence).await?;
    for row in &evidence {
        row.unchanged()?;
    }
    let stores = fixture::reopen(&evidence, fixture::IDS, [fixture::stream(); 3]).await?;
    for store in &stores {
        assert_eq!(store.progress().log_tail(), Some(fixture::blank(1).log_id));
        assert_eq!(store.progress().applied(), Some(cluster::LogId::default()));
    }
    fixture::stop_prepared(stores).await?;
    for row in &evidence {
        row.unchanged()?;
    }
    Ok(())
}

pub(super) async fn durable_direct_reopen() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let backend = fixture::Durable(directory.path().to_path_buf());
    let mut prepared = Vec::new();
    for (index, (log, state)) in backend.writers("direct")?.into_iter().enumerate() {
        let log = ExperimentalLogStore::create(
            log,
            LogProfile::new(fixture::IDS[index], fixture::stream())?,
        )?;
        let state = ExperimentalStateMachine::create(state, fixture::stream())?;
        prepared.push(ExperimentalReplicaStores::prepare(fixture::IDS[index], log, state).await?);
    }
    let cluster = ExperimentalRaftCluster::create(
        prepared
            .try_into()
            .map_err(|_| "three fresh pairs required")?,
    )
    .await?;
    create_queue(&cluster).await?;
    cluster.shutdown().await?;
    // No retained matching-writer control or old readers: these are new actual
    // durable store instances opened at the same paths after joined shutdown.
    let mut prepared = Vec::new();
    for (index, (log, state)) in backend.writers("direct")?.into_iter().enumerate() {
        let log = ExperimentalLogStore::open(
            log,
            LogProfile::new(fixture::IDS[index], fixture::stream())?,
        )?;
        let state = ExperimentalStateMachine::open(state, fixture::stream())?;
        prepared.push(ExperimentalReplicaStores::prepare(fixture::IDS[index], log, state).await?);
    }
    let cluster = ExperimentalRaftCluster::open(
        prepared
            .try_into()
            .map_err(|_| "three durable pairs required")?,
    )
    .await?;
    assert_eq!(cluster.node_ids().collect::<Vec<_>>(), fixture::IDS);
    cluster.shutdown().await?;
    Ok(())
}
