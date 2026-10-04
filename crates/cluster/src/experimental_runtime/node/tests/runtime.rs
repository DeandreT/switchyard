use openraft::{
    BasicNode,
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{AppendEntriesRequest, AppendEntriesResponse},
    storage::RaftLogStorage,
};
use storage::{CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StoreSnapshot};
use testkit::DurableProvider;

use crate::experimental_runtime::{
    Error,
    client::{QueueIntent, QueueWriteError, QueueWriteRejection},
    network::{Routes, stable_label},
    node::{Node, NodeRetirement, wait_completed},
};
use crate::{ExperimentalLogStore, ExperimentalStateMachine, LogProfile, LogTypes};

use super::{DEADLINE, TestResult, fixture::*, pending};

async fn worker_drain<W: CommittedStore>(
    log: W,
    state: W,
    cancel_waiter: bool,
) -> TestResult<(StoreSnapshot, StoreSnapshot)> {
    let (node, routes, log_control, state_control) = node(log, state).await?;
    let client_handle = node.client();
    let notice = NodeRetirement::with_evidence(node.completed.clone(), node.evidence.clone());
    let mut finished = node.completed.clone();
    let gate = state_control.gate_commit();
    let source = routes.begin_node(7)?;
    let mut factory = source.factory();
    let mut network = factory
        .new_client(FOLLOWER, &BasicNode::new(stable_label(FOLLOWER)))
        .await;
    for (index, previous) in [(2, 1), (3, 2)] {
        let response = tokio::time::timeout(
            DEADLINE,
            network.append_entries(
                AppendEntriesRequest::<LogTypes> {
                    vote: vote(),
                    prev_log_id: Some(id(previous)),
                    entries: vec![send(index)?],
                    leader_commit: Some(id(index)),
                },
                RPCOption::new(DEADLINE),
            ),
        )
        .await??;
        assert_eq!(response, AppendEntriesResponse::Success);
        if index == 2 {
            gate.entered().await?;
        }
    }
    // A later real core request fences the prior Commit command: the core
    // sends the second apply to its worker before handling this heartbeat.
    let response = tokio::time::timeout(
        DEADLINE,
        network.append_entries(
            AppendEntriesRequest::<LogTypes> {
                vote: vote(),
                prev_log_id: Some(id(3)),
                entries: vec![],
                leader_commit: Some(id(3)),
            },
            RPCOption::new(DEADLINE),
        ),
    )
    .await??;
    assert_eq!(response, AppendEntriesResponse::Success);
    assert!(!state_control.retired());
    let mut shutdown = Box::pin(node.shutdown());
    pending(shutdown.as_mut()).await?;
    log_control.wait_retired().await?;
    assert_eq!(notice.joined_evidence().err(), Some(Error::Closed));
    assert!(!state_control.retired());
    pending(shutdown.as_mut()).await?;
    if cancel_waiter {
        drop(shutdown);
        gate.release();
    } else {
        gate.release();
        tokio::time::timeout(DEADLINE, shutdown).await??;
    }
    tokio::time::timeout(DEADLINE, wait_completed(&mut finished)).await??;
    assert!(log_control.retired());
    assert!(state_control.retired());
    log_control.wait_retired().await?;
    state_control.wait_retired().await?;
    notice.clone().join().await?;
    let evidence = notice.joined_evidence()?;
    assert_eq!(evidence.log.profile().node_id(), FOLLOWER);
    assert_eq!(evidence.log.profile().stream(), stream()?);
    assert_eq!(evidence.log.retention().last_present, Some(id(3)));
    assert_eq!(
        evidence.checkpoint.last().map(|mark| mark.id),
        Some(domain::CommittedEntryId {
            term: 1,
            node_id: 7,
            index: 3,
        })
    );
    assert_eq!(
        evidence.checkpoint.highest_timestamp(),
        domain::Timestamp::from_millis(3)
    );
    assert!(std::sync::Arc::ptr_eq(
        &evidence,
        &notice.joined_evidence()?
    ));
    assert_eq!(client_handle.workload().accepted_jobs, 0);
    assert_eq!(client_handle.workload().encoded_bytes, 0);
    let intent = QueueIntent::create_queue(
        domain::NamespaceName::new("tenant")?,
        domain::EntityPath::new("closed")?,
        domain::QueueConfig::default(),
    )?;
    assert!(matches!(
        client_handle.submit(intent).await,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    ));
    assert_bodies(state_control.reader.clone())?;
    let result = (log_control.snapshot()?, state_control.snapshot()?);
    drop(client_handle);
    drop(network);
    drop(factory);
    drop(source);
    drop(routes);
    drop(finished);
    drop(log_control);
    drop(state_control);
    Ok(result)
}

async fn reopen_drain(
    log: &DurableProvider,
    state: &DurableProvider,
    snapshots: (StoreSnapshot, StoreSnapshot),
) -> TestResult {
    let log_writer = FjallReplicaStore::open(log.path())?;
    let state_writer = FjallReplicaStore::open(state.path())?;
    assert_eq!(log_writer.reader().snapshot()?, snapshots.0);
    assert_eq!(state_writer.reader().snapshot()?, snapshots.1);
    assert_bodies(state_writer.reader())?;
    let mut log = ExperimentalLogStore::open(log_writer, LogProfile::new(FOLLOWER, stream()?)?)?;
    let state = ExperimentalStateMachine::open(state_writer, stream()?)?;
    assert_eq!(log.get_log_state().await?.last_log_id, Some(id(3)));
    assert_eq!(
        state.checkpoint().await?.last().map(|mark| mark.id.index),
        Some(3)
    );
    let (log, state) = tokio::join!(log.shutdown(), state.shutdown());
    log?;
    state?;
    Ok(())
}

#[tokio::test]
async fn actual_worker_drains_its_queued_suffix_after_core_shutdown() -> TestResult {
    worker_drain(MemoryReplicaStore::new(), MemoryReplicaStore::new(), false).await?;
    Ok(())
}

#[tokio::test]
async fn actual_worker_drain_is_joined_before_fjall_recovery() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    let snapshots = worker_drain(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
        false,
    )
    .await?;
    reopen_drain(&log, &state, snapshots).await
}

#[tokio::test]
async fn lost_shutdown_waiter_does_not_cancel_the_actual_worker_suffix() -> TestResult {
    worker_drain(MemoryReplicaStore::new(), MemoryReplicaStore::new(), true).await?;
    Ok(())
}

#[tokio::test]
async fn lost_shutdown_waiter_still_joins_both_fjall_owners() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    let snapshots = worker_drain(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
        true,
    )
    .await?;
    reopen_drain(&log, &state, snapshots).await
}

async fn failed_start<W: CommittedStore>(
    log: W,
    state: W,
    panic: bool,
) -> TestResult<(StoreSnapshot, StoreSnapshot)> {
    let (prepared, log_control, state_control) = prepared(log, state).await?;
    let snapshots = (log_control.snapshot()?, state_control.snapshot()?);
    let routes = Routes::new(stream()?, [7, 8, 9])?;
    let pending = routes.begin_node(FOLLOWER)?;
    let generation = pending.generation();
    if panic {
        log_control.panic_read();
    } else {
        log_control.fail_read();
    }
    let error = tokio::time::timeout(DEADLINE, Node::start(prepared, pending, members()))
        .await?
        .err();
    assert_eq!(
        error,
        Some(if panic {
            Error::OwnerFailure
        } else {
            Error::CoreFailure
        })
    );
    assert!(!log_control.read_fault_pending());
    assert!(!generation.is_live());
    assert!(log_control.retired());
    assert!(state_control.retired());
    assert_eq!(log_control.snapshot()?, snapshots.0);
    assert_eq!(state_control.snapshot()?, snapshots.1);
    drop(log_control);
    drop(state_control);
    drop(generation);
    drop(routes);
    Ok(snapshots)
}

async fn reopen_failed_start(
    log: &DurableProvider,
    state: &DurableProvider,
    snapshots: (StoreSnapshot, StoreSnapshot),
) -> TestResult {
    let log_writer = FjallReplicaStore::open(log.path())?;
    let state_writer = FjallReplicaStore::open(state.path())?;
    assert_eq!(log_writer.reader().snapshot()?, snapshots.0);
    assert_eq!(state_writer.reader().snapshot()?, snapshots.1);
    let log = ExperimentalLogStore::open(log_writer, LogProfile::new(FOLLOWER, stream()?)?)?;
    let state = ExperimentalStateMachine::open(state_writer, stream()?)?;
    assert_eq!(
        state.checkpoint().await?.last().map(|mark| mark.id.index),
        Some(1)
    );
    let (log, state) = tokio::join!(log.shutdown(), state.shutdown());
    log?;
    state?;
    Ok(())
}

async fn failed_final_checkpoint<W: CommittedStore>(log: W, state: W) -> TestResult {
    let (node, routes, log_control, state_control) = node(log, state).await?;
    let before = (log_control.snapshot()?, state_control.snapshot()?);
    let notice = NodeRetirement::with_evidence(node.completed.clone(), node.evidence.clone());
    state_control.fail_read();
    assert_eq!(
        tokio::time::timeout(DEADLINE, node.shutdown()).await?,
        Err(Error::OwnerFailure)
    );
    assert!(!state_control.read_fault_pending());
    assert!(log_control.retired());
    assert!(state_control.retired());
    assert_eq!(notice.joined_evidence().err(), Some(Error::OwnerFailure));
    assert_eq!(log_control.snapshot()?, before.0);
    assert_eq!(state_control.snapshot()?, before.1);
    drop(routes);
    Ok(())
}

#[tokio::test]
async fn a_published_node_requires_a_healthy_final_checkpoint_after_actual_joins() -> TestResult {
    failed_final_checkpoint(MemoryReplicaStore::new(), MemoryReplicaStore::new()).await
}

#[tokio::test]
async fn a_published_durable_node_cannot_launder_a_final_checkpoint_read_failure() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    failed_final_checkpoint(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
    )
    .await
}

#[tokio::test]
async fn failed_real_new_joins_both_owners_without_publishing_an_endpoint() -> TestResult {
    failed_start(MemoryReplicaStore::new(), MemoryReplicaStore::new(), false).await?;
    Ok(())
}

#[tokio::test]
async fn failed_real_new_allows_immediate_matching_fjall_recovery() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    let snapshots = failed_start(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
        false,
    )
    .await?;
    reopen_failed_start(&log, &state, snapshots).await
}

#[tokio::test]
async fn panicked_startup_storage_still_joins_the_healthy_sibling() -> TestResult {
    failed_start(MemoryReplicaStore::new(), MemoryReplicaStore::new(), true).await?;
    Ok(())
}

#[tokio::test]
async fn panicked_startup_storage_preserves_both_durable_prefixes() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    let snapshots = failed_start(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
        true,
    )
    .await?;
    reopen_failed_start(&log, &state, snapshots).await
}

async fn core_fatal<W: CommittedStore>(
    log: W,
    state: W,
) -> TestResult<(StoreSnapshot, StoreSnapshot)> {
    let (node, routes, log_control, state_control) = node(log, state).await?;
    let before = state_control.snapshot()?;
    let mut completed = node.completed.clone();
    let notice = NodeRetirement::with_evidence(node.completed.clone(), node.evidence.clone());
    let generation = node.generation.clone();
    state_control.panic_commit();
    let source = routes.begin_node(7)?;
    let mut factory = source.factory();
    let mut network = factory
        .new_client(FOLLOWER, &BasicNode::new(stable_label(FOLLOWER)))
        .await;
    let _ = tokio::time::timeout(
        DEADLINE,
        network.append_entries(
            AppendEntriesRequest::<LogTypes> {
                vote: vote(),
                prev_log_id: Some(id(1)),
                entries: vec![send(2)?],
                leader_commit: Some(id(2)),
            },
            RPCOption::new(DEADLINE),
        ),
    )
    .await?;
    assert_eq!(
        tokio::time::timeout(DEADLINE, wait_completed(&mut completed)).await?,
        Err(Error::OwnerFailure)
    );
    assert_eq!(notice.joined_evidence().err(), Some(Error::OwnerFailure));
    assert!(!generation.is_live());
    assert!(log_control.retired());
    assert!(state_control.retired());
    assert_eq!(state_control.snapshot()?, before);
    let result = (log_control.snapshot()?, state_control.snapshot()?);
    drop(node);
    drop(network);
    drop(factory);
    drop(source);
    drop(routes);
    drop(completed);
    drop(generation);
    drop(log_control);
    drop(state_control);
    Ok(result)
}

#[tokio::test]
async fn an_idle_core_fatal_triggers_owned_whole_node_cleanup() -> TestResult {
    core_fatal(MemoryReplicaStore::new(), MemoryReplicaStore::new()).await?;
    Ok(())
}

#[tokio::test]
async fn an_idle_core_fatal_is_joined_before_durable_prefix_recovery() -> TestResult {
    let log = DurableProvider::temporary()?;
    let state = DurableProvider::temporary()?;
    let snapshots = core_fatal(
        FjallReplicaStore::open(log.path())?,
        FjallReplicaStore::open(state.path())?,
    )
    .await?;
    reopen_failed_start(&log, &state, snapshots).await
}
