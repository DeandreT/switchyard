use std::{sync::Arc, time::Duration};

use domain::{CommittedCheckpoint, CommittedEntryId};
use openraft::{
    BasicNode,
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::VoteRequest,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StoreSnapshot};
use testkit::DurableProvider;

use crate::experimental_runtime::{
    network::{Routes, stable_label},
    node::Node,
};
use crate::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogId, LogProfile,
    LogVote,
};

use super::{DEADLINE, TestResult, fixture, pending};

const IDS: [u64; 3] = [7, 8, 9];
const TARGET: usize = 1;

fn check(condition: bool, explanation: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(explanation.into())
    }
}

struct JoinedReplica {
    node_id: u64,
    log: StoreSnapshot,
    state: StoreSnapshot,
    checkpoint: CommittedCheckpoint,
    vote: LogVote,
}

async fn prepared<W: CommittedStore>(
    node_id: u64,
    log_writer: W,
    state_writer: W,
) -> TestResult<(
    ExperimentalReplicaStores,
    fixture::Control<W::Reader>,
    fixture::Control<W::Reader>,
)> {
    let (log_writer, log_control) = fixture::observed(log_writer);
    let (state_writer, state_control) = fixture::observed(state_writer);
    let mut log =
        ExperimentalLogStore::create(log_writer, LogProfile::new(node_id, fixture::stream()?)?)?;
    let mut state = match ExperimentalStateMachine::create(state_writer, fixture::stream()?) {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error.into());
        }
    };
    let setup: TestResult = async {
        log.blocking_append([fixture::initial()]).await?;
        log.save_vote(&LogVote::new(1, 7)).await?;
        state.apply([fixture::initial()]).await?;
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        let (log_result, state_result) = tokio::join!(log.shutdown(), state.shutdown());
        log_result?;
        state_result?;
        return Err(error);
    }
    let mut prepared = ExperimentalReplicaStores::prepare(node_id, log, state).await?;
    prepared.pause_runtime_ticks();
    Ok((prepared, log_control, state_control))
}

async fn custody<W: CommittedStore>(writers: [(W, W); 3]) -> TestResult<Vec<JoinedReplica>> {
    let routes = Routes::new(fixture::stream()?, IDS)?;
    let mut nodes = Vec::with_capacity(3);
    let mut controls = Vec::with_capacity(3);
    let mut source_factory = None;
    let outcome: TestResult = async {
        for (node_id, (log_writer, state_writer)) in IDS.into_iter().zip(writers) {
            let (prepared, log_control, state_control) =
                prepared(node_id, log_writer, state_writer).await?;
            let endpoint = routes.begin_node(node_id)?;
            if node_id == 7 {
                source_factory = Some(endpoint.factory());
            }
            let node = Node::start(prepared, endpoint, fixture::members()).await?;
            nodes.push(node);
            controls.push((log_control, state_control));
        }
        check(
            routes.workload()?.accepted_jobs == 0,
            "setup left native RPC work",
        )?;
        let old_log = controls[TARGET].0.snapshot()?;
        let old_state = controls[TARGET].1.snapshot()?;
        let gate = controls[TARGET].0.gate_commit();
        let scenario: TestResult = async {
            let mut client = source_factory
                .as_mut()
                .ok_or("missing exact source factory")?
                .new_client(8, &BasicNode::new(stable_label(8)))
                .await;
            let new_vote = LogVote::new(2, 7);
            let mut original = Box::pin(client.vote(
                VoteRequest::new(new_vote, Some(LogId::default())),
                RPCOption::new(Duration::from_secs(30)),
            ));
            pending(original.as_mut()).await?;
            gate.entered().await?;
            pending(original.as_mut()).await?;
            let cut = routes.isolate_for_test(7)?;
            drop(original);
            // The real RPC is already in the native commit, not just queued.
            // Losing this caller cannot cancel it or release its lease.
            check(
                routes.workload()?.accepted_jobs == 1,
                "lost RPC waiter refunded accepted work",
            )?;
            check(
                routes.workload()?.encoded_bytes == 64,
                "lost RPC waiter refunded accepted bytes",
            )?;
            check(
                controls[TARGET].0.snapshot()? == old_log,
                "gated vote changed storage before release",
            )?;
            check(
                controls[TARGET].1.snapshot()? == old_state,
                "vote changed domain state",
            )?;
            let mut settled = Box::pin(cut.settled());
            pending(settled.as_mut()).await?;
            check(
                routes.workload()?.accepted_jobs == 1,
                "cut refunded already-forwarded RPC",
            )?;
            gate.release();
            tokio::time::timeout(DEADLINE, settled).await??;
            check(
                routes.workload()?.accepted_jobs == 0,
                "real RPC completion retained job charge",
            )?;
            check(
                routes.workload()?.encoded_bytes == 0,
                "real RPC completion retained byte charge",
            )?;
            let persisted = controls[TARGET].0.snapshot()?;
            check(
                persisted != old_log,
                "sole native vote never reached physical storage",
            )?;
            check(
                persisted.entries().len() == old_log.entries().len(),
                "vote changed the log record count",
            )?;
            let changed_keys = persisted
                .entries()
                .iter()
                .filter_map(|(key, value)| {
                    (old_log
                        .entries()
                        .iter()
                        .find(|(old_key, _)| old_key == key)
                        .map(|(_, old_value)| old_value)
                        != Some(value))
                    .then_some(key.clone())
                })
                .collect::<Vec<_>>();
            check(
                changed_keys == vec![vec![2]],
                "vote changed more than canonical log progress",
            )?;
            check(
                controls[TARGET].1.snapshot()? == old_state,
                "voting changed applied domain records",
            )?;
            // No second Vote request can manufacture the saved-vote proof.
            cut.heal()?;
            drop(client);
            Ok(())
        }
        .await;
        // This is also executed before error cleanup. The backend Condvar is
        // never left gated while real core/storage retirement is awaited.
        gate.release();
        drop(gate);
        scenario
    }
    .await;

    // Retire every actual node before waiting; attempt all joins even if one
    // native owner reports failure. No whole-scenario timeout owns these Nodes.
    let notices = nodes.into_iter().map(Node::retire).collect::<Vec<_>>();
    let mut join_error = None;
    for notice in &notices {
        if let Err(error) = notice.clone().join().await {
            join_error.get_or_insert(error);
        }
    }
    outcome?;
    if let Some(error) = join_error {
        return Err(error.into());
    }
    check(
        notices.len() == 3 && controls.len() == 3,
        "not all real nodes were constructed",
    )?;
    let mut joined = Vec::with_capacity(3);
    for (index, (notice, (log_control, state_control))) in notices.iter().zip(&controls).enumerate()
    {
        let evidence = notice.joined_evidence()?;
        let expected_vote = LogVote::new(if index == TARGET { 2 } else { 1 }, 7);
        check(
            evidence.log.vote == Some(expected_vote),
            "actual final native report has the wrong persisted vote",
        )?;
        check(
            evidence.checkpoint.last().map(|mark| mark.id)
                == Some(CommittedEntryId {
                    term: 0,
                    node_id: 0,
                    index: 0,
                }),
            "voting advanced the applied domain prefix",
        )?;
        check(
            Arc::ptr_eq(&evidence, &notice.joined_evidence()?),
            "retirement replaced its final report",
        )?;
        joined.push(JoinedReplica {
            node_id: IDS[index],
            log: log_control.snapshot()?,
            state: state_control.snapshot()?,
            checkpoint: evidence.checkpoint.clone(),
            vote: expected_vote,
        });
    }
    drop(source_factory);
    drop(notices);
    drop(controls);
    drop(routes);
    Ok(joined)
}

#[tokio::test]
async fn partition_keeps_an_actual_backend_gated_rpc_after_its_waiter_is_lost() -> TestResult {
    custody(std::array::from_fn(|_| {
        (MemoryReplicaStore::new(), MemoryReplicaStore::new())
    }))
    .await?;
    Ok(())
}

#[tokio::test]
async fn partition_keeps_actual_rpc_custody_through_commit_and_fjall_joined_reopen() -> TestResult {
    let directories = (0..6)
        .map(|_| DurableProvider::temporary())
        .collect::<Result<Vec<_>, _>>()?;
    let joined = custody([
        (
            FjallReplicaStore::open(directories[0].path())?,
            FjallReplicaStore::open(directories[1].path())?,
        ),
        (
            FjallReplicaStore::open(directories[2].path())?,
            FjallReplicaStore::open(directories[3].path())?,
        ),
        (
            FjallReplicaStore::open(directories[4].path())?,
            FjallReplicaStore::open(directories[5].path())?,
        ),
    ])
    .await?;
    for (replica, pair) in joined.into_iter().zip(directories.chunks_exact(2)) {
        let log_writer = FjallReplicaStore::open(pair[0].path())?;
        let state_writer = FjallReplicaStore::open(pair[1].path())?;
        check(
            log_writer.reader().snapshot()? == replica.log,
            "joined durable log changed on reopen",
        )?;
        check(
            state_writer.reader().snapshot()? == replica.state,
            "joined durable state changed on reopen",
        )?;
        let mut log = ExperimentalLogStore::open(
            log_writer,
            LogProfile::new(replica.node_id, fixture::stream()?)?,
        )?;
        let state = match ExperimentalStateMachine::open(state_writer, fixture::stream()?) {
            Ok(state) => state,
            Err(error) => {
                let _ = log.shutdown().await;
                return Err(error.into());
            }
        };
        let observed: TestResult = async {
            check(
                log.read_vote().await? == Some(replica.vote),
                "physical reopen lost the saved vote",
            )?;
            check(
                log.get_log_state().await?.last_log_id == Some(LogId::default()),
                "physical reopen changed the log prefix",
            )?;
            check(
                state.checkpoint().await? == replica.checkpoint,
                "physical reopen changed the applied checkpoint",
            )
        }
        .await;
        let (log_result, state_result) = tokio::join!(log.shutdown(), state.shutdown());
        log_result?;
        state_result?;
        observed?;
    }
    Ok(())
}
