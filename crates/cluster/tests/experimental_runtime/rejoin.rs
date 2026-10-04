use std::{
    fmt::Debug,
    future::Future,
    path::Path,
    time::{Duration, Instant},
};

use cluster::{
    ClientWorkload, ExperimentalRaftCluster, ExperimentalRaftHandle, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection, QueueWriteResult,
};
use domain::{CommittedEntryId, EntityIncarnation, MessageRecord, StateMachine};
use storage::{CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StoreSnapshot};

use super::{
    TestResult,
    fixture::{self, IDS, ReplicaControl},
};

const DEADLINE: Duration = Duration::from_secs(60);
const ORIGINAL_BODY: &[u8] = b"confirmed original before a node retires";
const SECOND_BODY: &[u8] = b"confirmed on the surviving majority";
const THIRD_BODY: &[u8] = b"confirmed after exact-history rejoin";

#[derive(Clone, Copy)]
enum StoppedNode {
    Leader,
    Follower,
}

struct Records {
    messages: [MessageRecord; 3],
    incarnation: EntityIncarnation,
}

fn check(condition: bool, explanation: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(explanation.into())
    }
}

fn equal<T: Debug + PartialEq>(actual: T, expected: T, explanation: &str) -> TestResult {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{explanation}: actual {actual:?}, expected {expected:?}").into())
    }
}

async fn within<F: Future>(future: F) -> TestResult<F::Output> {
    tokio::time::timeout(DEADLINE, future)
        .await
        .map_err(|_| "rejoin scenario operation reached its deadline".into())
}

fn node_index(id: u64) -> TestResult<usize> {
    IDS.iter()
        .position(|candidate| *candidate == id)
        .ok_or_else(|| "unexpected node identity".into())
}

fn control<W: CommittedStore>(
    controls: &[Option<ReplicaControl<W>>; 3],
    index: usize,
) -> TestResult<&ReplicaControl<W>> {
    controls[index]
        .as_ref()
        .ok_or_else(|| "missing live or joined replica control".into())
}

fn snapshot<W: CommittedStore>(control: &ReplicaControl<W>) -> TestResult<[StoreSnapshot; 2]> {
    Ok([
        control.log.reader().snapshot()?,
        control.state.reader().snapshot()?,
    ])
}

fn incarnation<W: CommittedStore>(control: &ReplicaControl<W>) -> TestResult<EntityIncarnation> {
    StateMachine::new(control.state.reader())
        .entity_incarnation(&fixture::namespace()?, &fixture::entity()?)?
        .ok_or_else(|| "confirmed queue has no incarnation".into())
}

async fn closed(handle: &ExperimentalRaftHandle) -> TestResult {
    equal(
        within(handle.submit(fixture::send(Vec::new(), "stale-generation")?)).await?,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed)),
        "retired handle retargeted the replacement generation",
    )?;
    equal(
        handle.workload(),
        ClientWorkload::default(),
        "closed handle retained work",
    )
}

async fn confirmed(
    cluster: &ExperimentalRaftCluster,
    body: &[u8],
    name: &str,
) -> TestResult<QueueWriteResult> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(id) = cluster.leader_hint() {
            let handle = cluster
                .handle(id)
                .ok_or("leader hint references a stopped node")?;
            match within(handle.submit(fixture::send(body.to_vec(), name)?)).await? {
                Ok(result) => return Ok(result),
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {}
                // Unknown is returned once. Neither this helper nor its caller
                // resends a submitted intent or calls Unknown an acknowledgement.
                Err(error) => return Err(error.into()),
            }
        }
        check(
            Instant::now() < deadline,
            "no stable writer confirmed the new intent",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn elected_survivors<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[Option<ReplicaControl<W>>; 3],
    stopped: u64,
    prior_term: u64,
) -> TestResult {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut identity = None;
        let mut agrees = true;
        for (index, id) in IDS.into_iter().enumerate() {
            if id == stopped {
                continue;
            }
            let position = control(controls, index)?.state.applied_position()?;
            if position
                .is_none_or(|position| position.term <= prior_term || position.node_id == stopped)
                || identity.is_some_and(|previous| Some(previous) != position)
            {
                agrees = false;
                break;
            }
            identity = position;
        }
        if agrees
            && let Some(identity) = identity
            && cluster.leader_hint() == Some(identity.node_id)
        {
            return Ok(());
        }
        check(
            Instant::now() < deadline,
            "surviving applied identities did not elect a leader",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn all_applied<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[Option<ReplicaControl<W>>; 3],
    at_least: CommittedEntryId,
) -> TestResult<CommittedEntryId> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut identity = None;
        let mut agrees = true;
        for index in 0..IDS.len() {
            let position = control(controls, index)?.state.applied_position()?;
            if position.is_none_or(|position| {
                position.index < at_least.index
                    || (position.index == at_least.index && position != at_least)
            }) || identity.is_some_and(|previous| Some(previous) != position)
            {
                agrees = false;
                break;
            }
            identity = position;
        }
        if agrees
            && let Some(identity) = identity
            && cluster.leader_hint() == Some(identity.node_id)
        {
            // Applied storage, not the routing hint alone, is the catch-up fence.
            // The following successful write still supplies the quorum receipt.
            return Ok(identity);
        }
        check(
            Instant::now() < deadline,
            "all three applied identities did not catch up",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn message_everywhere<W: CommittedStore>(
    controls: &[Option<ReplicaControl<W>>; 3],
    sequence: u64,
    expected_body: &[u8],
    expected_name: &str,
    excluded: Option<u64>,
) -> TestResult<MessageRecord> {
    let mut expected = None;
    for (index, id) in IDS.into_iter().enumerate() {
        if excluded == Some(id) {
            continue;
        }
        let message = within(control(controls, index)?.state.wait_message(sequence)).await??;
        equal(
            message.sequence.as_u64(),
            sequence,
            "unexpected message sequence",
        )?;
        equal(
            message.body.as_slice(),
            expected_body,
            "unexpected message body",
        )?;
        equal(
            message.message_id.as_str(),
            expected_name,
            "unexpected message identity",
        )?;
        if let Some(expected) = &expected {
            equal(
                &message,
                expected,
                "survivors disagree on the complete message record",
            )?;
        }
        expected = Some(message);
    }
    expected.ok_or_else(|| "no actual replica applied the confirmed message".into())
}

async fn scenario<W, Reopen>(
    cluster: &mut ExperimentalRaftCluster,
    controls: &mut [Option<ReplicaControl<W>>; 3],
    retained: &mut Vec<ExperimentalRaftHandle>,
    stop: StoppedNode,
    reopen: Reopen,
) -> TestResult<Records>
where
    W: CommittedStore,
    Reopen: FnOnce(u64, (W, W)) -> TestResult<(W, W)>,
{
    for id in IDS {
        retained.push(cluster.handle(id).ok_or("missing original handle")?);
    }
    let (_, initial) = within(fixture::create_queue(cluster)).await??;
    let original =
        within(initial.submit(fixture::send(ORIGINAL_BODY.to_vec(), "original")?)).await??;
    equal(
        original.outcome,
        QueueWriteOutcome::Sent { sequence: 1 },
        "original was not confirmed",
    )?;
    let first = message_everywhere(controls, 1, ORIGINAL_BODY, "original", None).await?;
    let original_incarnation = incarnation(control(controls, 0)?)?;
    for index in 1..IDS.len() {
        equal(
            incarnation(control(controls, index)?)?,
            original_incarnation,
            "original queue incarnations disagree",
        )?;
    }
    let active = all_applied(cluster, controls, original.entry).await?;
    let stopped = match stop {
        StoppedNode::Leader => active.node_id,
        StoppedNode::Follower => IDS
            .into_iter()
            .find(|id| *id != active.node_id)
            .ok_or("missing follower")?,
    };
    let index = node_index(stopped)?;
    within(cluster.stop_node(stopped)).await??;
    closed(&retained[index]).await?;
    check(
        cluster.handle(stopped).is_none(),
        "joined stop left a public live handle",
    )?;
    equal(
        cluster.node_ids().collect::<Vec<_>>(),
        IDS.into_iter()
            .filter(|id| *id != stopped)
            .collect::<Vec<_>>(),
        "joined stop left incorrect live node identities",
    )?;
    let retired_snapshot = snapshot(control(controls, index)?)?;
    if matches!(stop, StoppedNode::Leader) {
        elected_survivors(cluster, controls, stopped, active.term).await?;
    }
    let second = confirmed(cluster, SECOND_BODY, "while-stopped").await?;
    equal(
        second.outcome,
        QueueWriteOutcome::Sent { sequence: 2 },
        "surviving majority did not confirm the second send",
    )?;
    check(
        second.entry.node_id != stopped,
        "stopped node authored the second send",
    )?;
    check(
        second.entry.index > original.entry.index,
        "second receipt did not advance the log identity",
    )?;
    if matches!(stop, StoppedNode::Leader) {
        check(
            second.entry.term > active.term,
            "leader stop did not yield a higher-term receipt",
        )?;
    }
    let second_message =
        message_everywhere(controls, 2, SECOND_BODY, "while-stopped", Some(stopped)).await?;
    equal(
        snapshot(control(controls, index)?)?,
        retired_snapshot.clone(),
        "retired storage changed during surviving-majority writes",
    )?;
    check(
        control(controls, index)?.state.message(2)?.is_none(),
        "stopped node applied work after its join",
    )?;

    // Ownership extraction consumes the stopped controls and their read handles.
    // Memory recovers the exact old raw writers, never the ObservedWriter wrapper.
    // Fjall additionally drops those raw writers before reopening both directories.
    let retired = controls[index]
        .take()
        .ok_or("missing stopped control")?
        .into_backends()?;
    let (log, state) = reopen(stopped, retired)?;
    // Backend open/preparation has no forced I/O timeout. Once a prepared pair
    // exists, any pre-admission check failure explicitly joins its owners.
    let (prepared, replacement) = fixture::prepare_reopened(stopped, log, state).await?;
    let unchanged = snapshot(&replacement).and_then(|current| {
        equal(
            current,
            retired_snapshot,
            "open/prepare changed the retired history before a new engine started",
        )
    });
    if let Err(error) = unchanged {
        prepared.shutdown().await?;
        return Err(error);
    }
    controls[index] = Some(replacement);
    let attempt = match cluster.rejoin_node(stopped, prepared) {
        Ok(attempt) => attempt,
        Err(error) => {
            let reason = error.reason();
            error.into_stores().shutdown().await?;
            return Err(reason.into());
        }
    };
    // Poll exactly this accepted attempt; operation failures are not retry advice.
    within(attempt).await??;
    let replacement = cluster
        .handle(stopped)
        .ok_or("successful rejoin has no public handle")?;
    retained.push(replacement);
    equal(
        cluster.node_ids().collect::<Vec<_>>(),
        IDS.to_vec(),
        "rejoin did not restore the exact fixed voter identities",
    )?;
    closed(&retained[index]).await?;
    equal(
        within(control(controls, index)?.state.wait_message(2)).await??,
        second_message.clone(),
        "rejoined node failed to catch up the confirmed surviving history",
    )?;
    all_applied(cluster, controls, second.entry).await?;
    let third = confirmed(cluster, THIRD_BODY, "after-rejoin").await?;
    equal(
        third.outcome,
        QueueWriteOutcome::Sent { sequence: 3 },
        "rejoined cluster did not confirm the third send",
    )?;
    check(
        third.entry.index > second.entry.index,
        "third receipt did not advance the log identity",
    )?;
    let third_message = message_everywhere(controls, 3, THIRD_BODY, "after-rejoin", None).await?;
    closed(&retained[index]).await?;
    Ok(Records {
        messages: [first, second_message, third_message],
        incarnation: original_incarnation,
    })
}

async fn cycle<W, Reopen>(stores: [(W, W); 3], stop: StoppedNode, reopen: Reopen) -> TestResult
where
    W: CommittedStore,
    Reopen: FnOnce(u64, (W, W)) -> TestResult<(W, W)>,
{
    let (mut cluster, controls) = fixture::create(stores).await?;
    let mut controls = controls.map(Some);
    let mut retained = Vec::new();
    let outcome = scenario(&mut cluster, &mut controls, &mut retained, stop, reopen).await;
    // Always start and await joined cleanup, including Unknown or admission failure.
    let shutdown = cluster.shutdown().await;
    shutdown?;
    let records = outcome?;
    for handle in &retained {
        closed(handle).await?;
    }
    // Only stable, actually joined state is used for the final no-duplicate check.
    for index in 0..IDS.len() {
        let replica = control(&controls, index)?;
        for (offset, expected) in records.messages.iter().enumerate() {
            equal(
                replica.state.message(u64::try_from(offset + 1)?)?,
                Some(expected.clone()),
                "joined replica lost or rewrote a confirmed message",
            )?;
        }
        check(
            replica.state.message(4)?.is_none(),
            "an original or submitted intent was duplicated",
        )?;
        equal(
            incarnation(replica)?,
            records.incarnation,
            "rejoin reinitialized the original queue identity",
        )?;
    }
    Ok(())
}

async fn memory(stop: StoppedNode) -> TestResult {
    let stores = std::array::from_fn(|_| (MemoryReplicaStore::new(), MemoryReplicaStore::new()));
    cycle(stores, stop, |_, exact_retired_backends| {
        Ok(exact_retired_backends)
    })
    .await
}

fn reopen_durable(path: &Path, id: u64) -> TestResult<(FjallReplicaStore, FjallReplicaStore)> {
    Ok((
        FjallReplicaStore::open(path.join(format!("log-{id}")))?,
        FjallReplicaStore::open(path.join(format!("state-{id}")))?,
    ))
}

async fn durable(stop: StoppedNode) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let stores = fixture::durable_stores(directory.path())?;
    let path = directory.path().to_owned();
    cycle(stores, stop, move |id, exact_retired_backends| {
        // No old control, reader, raw writer, or observation wrapper survives here.
        drop(exact_retired_backends);
        reopen_durable(&path, id)
    })
    .await
}

mod memory_cases {
    use super::*;

    #[tokio::test]
    async fn joined_leader_rejoins_exact_retired_history_then_all_three_apply() -> TestResult {
        memory(StoppedNode::Leader).await
    }

    #[tokio::test]
    async fn joined_follower_rejoins_exact_retired_history_then_all_three_apply() -> TestResult {
        memory(StoppedNode::Follower).await
    }
}

mod durable_cases {
    use super::*;

    #[tokio::test]
    async fn joined_leader_reopens_both_directories_and_rejoins_without_resending() -> TestResult {
        durable(StoppedNode::Leader).await
    }

    #[tokio::test]
    async fn joined_follower_reopens_both_directories_and_rejoins_without_resending() -> TestResult
    {
        durable(StoppedNode::Follower).await
    }
}
