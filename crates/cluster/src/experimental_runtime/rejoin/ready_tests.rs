use std::{collections::BTreeMap, sync::Arc, time::Duration};

use domain::{CommittedCheckpoint, CommittedStreamId, EntityPath, NamespaceName, QueueConfig};
use openraft::{
    BasicNode, EntryPayload, Membership,
    error::RPCError,
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::VoteRequest,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StoreSnapshot};
use testkit::DurableProvider;

use crate::experimental_runtime::{
    client::{QueueIntent, QueueWriteError, QueueWriteRejection},
    continuity::RetirementEvidence,
    network::{Routes, stable_label},
    node::Node,
};
use crate::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogEntry, LogId,
    LogProfile, LogVote,
};

use super::{
    AttemptReceipt, Continuity, Floor, GuardedReadyNode, PublicationWait, RejoinState, WaitGuard,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const TARGET: u64 = 8;

fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([74; 16])?)
}

fn members() -> BTreeMap<u64, BasicNode> {
    [7, 8, 9]
        .into_iter()
        .map(|id| (id, BasicNode::new(stable_label(id))))
        .collect()
}

fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(Membership::new(
            vec![[7, 8, 9].into_iter().collect()],
            members(),
        )),
    }
}

struct JoinedState {
    log: StoreSnapshot,
    state: StoreSnapshot,
    checkpoint: CommittedCheckpoint,
}

async fn lost_ready<W: CommittedStore>(log_writer: W, state_writer: W) -> TestResult<JoinedState> {
    let log_reader = log_writer.reader();
    let state_reader = state_writer.reader();
    let mut log = ExperimentalLogStore::create(log_writer, LogProfile::new(TARGET, stream()?)?)?;
    let mut state = ExperimentalStateMachine::create(state_writer, stream()?)?;
    let old_vote = LogVote::new(1, 7);
    tokio::time::timeout(DEADLINE, log.blocking_append([initial()])).await??;
    tokio::time::timeout(DEADLINE, log.save_vote(&old_vote)).await??;
    tokio::time::timeout(DEADLINE, state.apply([initial()])).await??;
    let mut prepared = ExperimentalReplicaStores::prepare(TARGET, log, state).await?;
    let (comparison_log, comparison_checkpoint) = prepared.continuity_snapshot().await?;
    assert_eq!(comparison_log.vote, Some(old_vote));
    // This pre-engine report is comparison data, never an old joined-node
    // authority. The receipt below obtains its floor from actual retirement.
    let comparison = RetirementEvidence::checked(
        TARGET,
        stream()?,
        comparison_log,
        comparison_checkpoint.clone(),
    )?;
    prepared.pause_runtime_ticks();
    let routes = Routes::new(stream()?, [7, 8, 9])?;
    let pending = routes.begin_node(TARGET)?;
    let node = Node::start(prepared, pending, members()).await?;
    let stale_client = node.client();
    let source = routes.begin_node(7)?;
    let mut factory = source.factory();
    let mut network = factory
        .new_client(TARGET, &BasicNode::new(stable_label(TARGET)))
        .await;
    let new_vote = LogVote::new(2, 7);
    let vote_result = tokio::time::timeout(
        DEADLINE,
        network.vote(
            VoteRequest::new(new_vote, Some(initial().log_id)),
            RPCOption::new(DEADLINE),
        ),
    )
    .await;
    let response = match vote_result {
        Ok(Ok(response)) => response,
        error => {
            // A failed proof still joins the real node before releasing its
            // durable directory; no whole-owning-scenario timeout is used.
            node.retire().join().await?;
            return Err(format!("actual vote did not complete: {error:?}").into());
        }
    };
    if !response.vote_granted
        || response.vote != new_vote
        || response.last_log_id != Some(initial().log_id)
    {
        let diagnostic = format!("actual higher-term vote was not granted: {response:?}");
        node.retire().join().await?;
        return Err(diagnostic.into());
    }

    let (receipt, publisher) = AttemptReceipt::new(TARGET);
    let mut bookkeeping = RejoinState::default();
    bookkeeping
        .floors
        .insert(TARGET, Floor::Healthy(comparison.clone()));
    bookkeeping.active = Some(receipt.clone());
    let (reply, receiver) = tokio::sync::oneshot::channel();
    let ready = GuardedReadyNode {
        node: Some(node),
        shared: receipt.shared.clone(),
        runtime: tokio::runtime::Handle::current(),
        completion: Some(publisher),
    };
    assert!(reply.send(Ok(ready)).is_ok());
    let waiting = PublicationWait {
        guard: Some(WaitGuard {
            shared: receipt.shared.clone(),
            armed: true,
        }),
        receiver,
    };
    // The ready channel value is never polled. PublicationWait's Drop first
    // cancels its guard, then destroys the actual ready-node receiver value.
    drop(waiting);
    let canceled = receipt.shared.is_canceled();
    let observations: TestResult = async {
        if !canceled {
            return Err("lost publication waiter did not cancel".into());
        }
        let intent = QueueIntent::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("must-not-be-created")?,
            QueueConfig::default(),
        )?;
        let stale = stale_client.submit(intent).await;
        if !matches!(
            stale,
            Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
        ) {
            return Err(format!("stale client was not closed: {stale:?}").into());
        }
        let refused = tokio::time::timeout(
            DEADLINE,
            network.vote(
                VoteRequest::new(new_vote, Some(initial().log_id)),
                RPCOption::new(DEADLINE),
            ),
        )
        .await?;
        match refused {
            Err(RPCError::Unreachable(error))
                if error
                    .to_string()
                    .contains("the in-process peer is unavailable") =>
            {
                Ok(())
            }
            other => {
                Err(format!("retired native RPC target was not unavailable: {other:?}").into())
            }
        }
    }
    .await;

    // These awaits are the actual node/core plus BOTH native-owner join
    // barrier, not a synthetic watch receipt or writer-drop observation.
    let completion = receipt.join().await;
    observations?;
    assert_eq!(completion.cleanup, Ok(()));
    let Continuity::RetiredNewGeneration(evidence) = &completion.continuity else {
        panic!("lost ready node must produce a healthy new retirement floor");
    };
    assert_eq!(evidence.log.vote, Some(new_vote));
    assert_eq!(evidence.checkpoint, comparison_checkpoint);
    assert_ne!(evidence.log.vote, comparison.log.vote);
    assert!(!Arc::ptr_eq(evidence, &comparison));
    bookkeeping.fold_completed()?;
    assert!(bookkeeping.active.is_none());
    assert!(
        matches!(bookkeeping.floors.get(&TARGET), Some(Floor::Healthy(actual))
        if Arc::ptr_eq(actual, evidence))
    );
    assert_eq!(stale_client.workload().accepted_jobs, 0);
    assert_eq!(stale_client.workload().encoded_bytes, 0);
    assert_eq!(routes.workload()?.accepted_jobs, 0);
    let joined = JoinedState {
        log: log_reader.snapshot()?,
        state: state_reader.snapshot()?,
        checkpoint: evidence.checkpoint.clone(),
    };
    drop(stale_client);
    drop(network);
    drop(factory);
    drop(source);
    drop(routes);
    drop(log_reader);
    drop(state_reader);
    Ok(joined)
}

#[tokio::test]
async fn lost_actual_ready_publication_joins_and_replaces_the_advanced_floor() -> TestResult {
    lost_ready(MemoryReplicaStore::new(), MemoryReplicaStore::new()).await?;
    Ok(())
}

#[tokio::test]
async fn lost_actual_ready_publication_joins_before_fjall_reopen() -> TestResult {
    let log_directory = DurableProvider::temporary()?;
    let state_directory = DurableProvider::temporary()?;
    let joined = lost_ready(
        FjallReplicaStore::open(log_directory.path())?,
        FjallReplicaStore::open(state_directory.path())?,
    )
    .await?;
    let log_writer = FjallReplicaStore::open(log_directory.path())?;
    let state_writer = FjallReplicaStore::open(state_directory.path())?;
    assert_eq!(log_writer.reader().snapshot()?, joined.log);
    assert_eq!(state_writer.reader().snapshot()?, joined.state);
    let mut log = ExperimentalLogStore::open(log_writer, LogProfile::new(TARGET, stream()?)?)?;
    let state = ExperimentalStateMachine::open(state_writer, stream()?)?;
    assert_eq!(
        tokio::time::timeout(DEADLINE, log.read_vote()).await??,
        Some(LogVote::new(2, 7))
    );
    assert_eq!(
        tokio::time::timeout(DEADLINE, state.checkpoint()).await??,
        joined.checkpoint
    );
    let (log_result, state_result) = tokio::join!(log.shutdown(), state.shutdown());
    log_result?;
    state_result?;
    Ok(())
}
