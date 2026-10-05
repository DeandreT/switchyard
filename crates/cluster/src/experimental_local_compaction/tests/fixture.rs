use super::{
    TestResult,
    observed::{Control, Observed},
};
use crate::{
    ExperimentalCompactionLogStore, ExperimentalLocalCompactionPair, ExperimentalStateMachine,
    LocalCompactionError, LogEntry, LogId, LogProfile, LogVote, QueueLogCommand,
};
use domain::{CommittedSend, CommittedStreamId, EntityPath, NamespaceName, QueueConfig, Timestamp};
use openraft::{BasicNode, EntryPayload, Membership, storage::RaftStateMachine};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use storage::{BoundedStateStore, CatalogCommittedStore, CommittedStore};

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([93; 16])?)
}
pub(super) fn profile() -> TestResult<LogProfile> {
    Ok(LogProfile::new(7, stream()?)?)
}
pub(super) fn id(index: u64) -> LogId {
    if index == 0 {
        LogId::default()
    } else {
        LogId::new(openraft::CommittedLeaderId::new(1, 7), index)
    }
}
pub(super) fn entry(index: u64) -> TestResult<LogEntry> {
    let payload = match index {
        0 => EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([7, 8, 9])],
            BTreeMap::from([
                (7, BasicNode::new("node-7")),
                (8, BasicNode::new("node-8")),
                (9, BasicNode::new("node-9")),
            ]),
        )),
        1 => EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(10),
            QueueConfig::default(),
        )),
        _ => EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(10 + index),
            CommittedSend {
                message_id: format!("PRIVATE-{index}"),
                body: vec![index as u8; 32],
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    };
    Ok(LogEntry {
        log_id: id(index),
        payload,
    })
}

pub(super) async fn seed<L, S>(
    log_writer: L,
    state_writer: S,
    applied: u64,
    tail: u64,
) -> TestResult<(
    ExperimentalCompactionLogStore,
    ExperimentalStateMachine,
    Arc<Control>,
    Arc<Control>,
)>
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    seed_inner(log_writer, state_writer, applied, tail, false).await
}

pub(super) async fn seed_with_export<L, S>(
    log_writer: L,
    state_writer: S,
    applied: u64,
    tail: u64,
) -> TestResult<(
    ExperimentalCompactionLogStore,
    ExperimentalStateMachine,
    Arc<Control>,
    Arc<Control>,
)>
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    seed_inner(log_writer, state_writer, applied, tail, true).await
}

async fn seed_inner<L, S>(
    log_writer: L,
    state_writer: S,
    applied: u64,
    tail: u64,
    export: bool,
) -> TestResult<(
    ExperimentalCompactionLogStore,
    ExperimentalStateMachine,
    Arc<Control>,
    Arc<Control>,
)>
where
    L: CommittedStore,
    S: CatalogCommittedStore,
    S::Reader: BoundedStateStore,
{
    let (log_writer, log_control) = Observed::new(log_writer);
    let (state_writer, state_control) = Observed::new(state_writer);
    let stream = stream()?;
    let mut log = ExperimentalCompactionLogStore::create(log_writer, profile()?)?;
    let created = if export {
        ExperimentalStateMachine::create_catalog_and_export_for_test(state_writer, stream)
    } else {
        ExperimentalStateMachine::create_with_snapshot_catalog(state_writer, stream)
    };
    let mut state = match created {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error.into());
        }
    };
    let result: TestResult = async {
        log.append((0..=tail).map(entry).collect::<TestResult<Vec<_>>>()?)
            .await?;
        log.save_vote(LogVote::new_committed(1, 7)).await?;
        state
            .apply((0..=applied).map(entry).collect::<TestResult<Vec<_>>>()?)
            .await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
        a?;
        b?;
        return Err(error);
    }
    Ok((log, state, log_control, state_control))
}

pub(super) async fn sources_or_cleanup<T>(
    result: TestResult<T>,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
) -> TestResult<(ExperimentalCompactionLogStore, ExperimentalStateMachine, T)> {
    match result {
        Ok(value) => Ok((log, state, value)),
        Err(error) => {
            let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
            a?;
            b?;
            Err(error)
        }
    }
}

pub(super) async fn wait_exit(
    mut exit: tokio::sync::watch::Receiver<Option<Result<(), LocalCompactionError>>>,
) -> TestResult<Result<(), LocalCompactionError>> {
    loop {
        if let Some(result) = *exit.borrow_and_update() {
            return Ok(result);
        }
        exit.changed().await?;
    }
}

pub(super) async fn pair(
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
) -> TestResult<ExperimentalLocalCompactionPair> {
    match ExperimentalLocalCompactionPair::prepare(7, log, state) {
        Ok(future) => Ok(future.await?),
        Err(error) => {
            let reason = error.reason();
            let (log, state) = error.into_sources();
            let (a, b) = tokio::join!(log.shutdown(), state.shutdown());
            a?;
            b?;
            Err(reason.into())
        }
    }
}

pub(super) async fn probe<F: std::future::Future>(
    future: std::pin::Pin<&mut F>,
) -> Option<F::Output> {
    tokio::select! { biased; output = future => Some(output), _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => None }
}
