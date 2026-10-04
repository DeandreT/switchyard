use std::{
    fmt::Debug,
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::{Duration, Instant},
};

use cluster::{
    ExperimentalRaftCluster, ExperimentalRaftHandle, ExperimentalReplicaStores, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection, QueueWriteResult, ReplicaProgress, ReplicaRuntimeError,
};
use storage::{
    CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StoreSnapshot, WriteBatch,
};

use super::{
    TestResult,
    fixture::{self, IDS, ReplicaControl},
};

#[path = "rejoin_gate_fixture.rs"]
mod gates;

const DEADLINE: Duration = Duration::from_secs(60);
const PENDING_OBSERVATION: Duration = Duration::from_millis(100);
const ORIGINAL: &[u8] = b"confirmed before cancelled candidate validation";
const WHILE_CANCELLED: &[u8] = b"confirmed on survivors while cancelled validation remains blocked";
const AFTER_RETRY: &[u8] = b"confirmed after preserving the prior retirement floor";

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
        .map_err(|_| "rejoin lifecycle observation reached its deadline".into())
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
                // Never repeat an Unknown intent or count it as confirmation.
                Err(error) => return Err(error.into()),
            }
        }
        check(
            Instant::now() < deadline,
            "no writer confirmed the new intent",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn closed(handle: &ExperimentalRaftHandle) -> TestResult {
    equal(
        within(handle.submit(fixture::send(Vec::new(), "stale-cancelled-generation")?)).await?,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed)),
        "an old handle retargeted a replacement node",
    )
}

fn snapshot<R: StateStore>(pair: &gates::Pair<R>) -> TestResult<[StoreSnapshot; 2]> {
    Ok([pair.log.snapshot()?, pair.state.snapshot()?])
}

fn counts<R: StateStore>(pair: &gates::Pair<R>) -> [gates::Counts; 2] {
    [pair.log.counts(), pair.state.counts()]
}

struct Frozen {
    progress: ReplicaProgress,
    snapshots: [StoreSnapshot; 2],
    counts: [gates::Counts; 2],
}

fn freeze<R: StateStore>(
    stores: &ExperimentalReplicaStores,
    pair: &gates::Pair<R>,
) -> TestResult<Frozen> {
    Ok(Frozen {
        progress: stores.progress().clone(),
        snapshots: snapshot(pair)?,
        counts: counts(pair),
    })
}

async fn refused<R: StateStore>(
    cluster: &mut ExperimentalRaftCluster,
    node_id: u64,
    stores: ExperimentalReplicaStores,
    pair: gates::Pair<R>,
    expected: ReplicaRuntimeError,
) -> TestResult {
    let before = match freeze(&stores, &pair) {
        Ok(before) => before,
        Err(error) => {
            stores.shutdown().await?;
            return Err(error);
        }
    };
    let error = match cluster.rejoin_node(node_id, stores) {
        Err(error) => error,
        Ok(attempt) => {
            let result = within(attempt).await;
            return Err(
                format!("synchronous refusal unexpectedly admitted work: {result:?}").into(),
            );
        }
    };
    let reason = error.reason();
    let returned = error.into_stores();
    let observations = (|| {
        equal(reason, expected, "incorrect synchronous admission refusal")?;
        equal(
            returned.progress(),
            &before.progress,
            "refusal changed the returned Prepared progress",
        )?;
        equal(
            counts(&pair),
            before.counts,
            "synchronous refusal accessed a candidate backend",
        )?;
        equal(
            snapshot(&pair)?,
            before.snapshots.clone(),
            "synchronous refusal changed candidate history",
        )
    })();
    // Even a failed assertion explicitly joins the untouched returned pair.
    let joined = returned.shutdown().await;
    joined?;
    observations?;
    check(
        pair.log.writer_dropped() && pair.state.writer_dropped(),
        "returned Prepared shutdown missed a physical writer",
    )?;
    equal(
        counts(&pair),
        before.counts,
        "ordinary Prepared shutdown added storage work",
    )?;
    equal(
        snapshot(&pair)?,
        before.snapshots,
        "joined refused pair changed history",
    )
}

async fn pristine(
    node_id: u64,
) -> TestResult<(
    ExperimentalReplicaStores,
    gates::Pair<<MemoryReplicaStore as CommittedStore>::Reader>,
)> {
    gates::prepare(
        node_id,
        MemoryReplicaStore::new(),
        MemoryReplicaStore::new(),
        true,
    )
    .await
}

fn copy_memory(
    snapshots: &[StoreSnapshot; 2],
) -> TestResult<(MemoryReplicaStore, MemoryReplicaStore)> {
    let copy = |snapshot: &StoreSnapshot| -> TestResult<MemoryReplicaStore> {
        let mut writer = MemoryReplicaStore::new();
        let mut batch = WriteBatch::default();
        for (key, value) in snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        writer.commit(batch)?;
        Ok(writer)
    };
    Ok((copy(&snapshots[0])?, copy(&snapshots[1])?))
}

struct Stopped {
    node_id: u64,
    index: usize,
    old_handle: ExperimentalRaftHandle,
    snapshots: [StoreSnapshot; 2],
}

async fn stop_seeded_follower<W: CommittedStore>(
    cluster: &mut ExperimentalRaftCluster,
    controls: &[Option<ReplicaControl<W>>; 3],
) -> TestResult<Stopped> {
    within(fixture::create_queue(cluster)).await??;
    let receipt = confirmed(cluster, ORIGINAL, "before-cancellation").await?;
    equal(
        receipt.outcome,
        QueueWriteOutcome::Sent { sequence: 1 },
        "original intent was not confirmed",
    )?;
    let mut expected = None;
    for control in controls {
        let message = within(
            control
                .as_ref()
                .ok_or("missing original control")?
                .state
                .wait_message(1),
        )
        .await??;
        equal(
            message.body.as_slice(),
            ORIGINAL,
            "original replica has incorrect content",
        )?;
        if let Some(expected) = &expected {
            equal(
                &message,
                expected,
                "original replicas disagree on the complete message record",
            )?;
        }
        expected = Some(message);
    }
    let node_id = IDS
        .into_iter()
        .find(|id| *id != receipt.entry.node_id)
        .ok_or("missing follower")?;
    let index = IDS
        .iter()
        .position(|id| *id == node_id)
        .ok_or("unknown stopped identity")?;
    let old_handle = cluster.handle(node_id).ok_or("missing follower handle")?;
    within(cluster.stop_node(node_id)).await??;
    closed(&old_handle).await?;
    let control = controls[index].as_ref().ok_or("missing retired control")?;
    Ok(Stopped {
        node_id,
        index,
        old_handle,
        snapshots: [
            control.log.reader().snapshot()?,
            control.state.reader().snapshot()?,
        ],
    })
}

async fn unpolled<R: StateStore>(
    cluster: &mut ExperimentalRaftCluster,
    node_id: u64,
    stores: ExperimentalReplicaStores,
    pair: gates::Pair<R>,
    expected: &[StoreSnapshot; 2],
) -> TestResult {
    let before = match freeze(&stores, &pair) {
        Ok(before) => before,
        Err(error) => {
            stores.shutdown().await?;
            return Err(error);
        }
    };
    if let Err(error) = equal(
        &before.snapshots,
        expected,
        "unpolled candidate did not have exact retired history",
    ) {
        stores.shutdown().await?;
        return Err(error);
    }
    // Arm only after preparation; constructing and dropping the factory future
    // cannot enqueue even the first validation read.
    let log_read = pair.log.gate_next_read();
    let state_read = pair.state.gate_next_read();
    let attempt = match cluster.rejoin_node(node_id, stores) {
        Ok(attempt) => attempt,
        Err(error) => {
            let reason = error.reason();
            log_read.release();
            state_read.release();
            error.into_stores().shutdown().await?;
            return Err(reason.into());
        }
    };
    drop(attempt);
    // Releasing unused read gates cannot manufacture an owner error. Counts
    // still detect any incorrectly scheduled read during the entire lifetime.
    log_read.release();
    state_read.release();
    within(pair.log.dropped()).await?;
    within(pair.state.dropped()).await?;
    check(
        !log_read.has_entered() && !state_read.has_entered(),
        "an unpolled admission started backend validation",
    )?;
    equal(
        counts(&pair),
        before.counts,
        "an unpolled admission started storage work",
    )?;
    equal(
        snapshot(&pair)?,
        before.snapshots,
        "an unpolled admission changed retired storage",
    )?;
    check(
        cluster.handle(node_id).is_none(),
        "an unpolled admission published a node",
    )?;
    equal(
        cluster.node_ids().collect::<Vec<_>>(),
        IDS.into_iter()
            .filter(|id| *id != node_id)
            .collect::<Vec<_>>(),
        "unpolled admission changed public node identities",
    )
}

#[tokio::test]
async fn rejoin_sync_refusals_and_unpolled_admission_are_storage_inert() -> TestResult {
    let backends = std::array::from_fn(|_| (MemoryReplicaStore::new(), MemoryReplicaStore::new()));
    let (mut cluster, controls) = fixture::create(backends).await?;
    let mut controls = controls.map(Some);
    let result = async {
        let (stores, pair) = pristine(99).await?;
        refused(
            &mut cluster,
            99,
            stores,
            pair,
            ReplicaRuntimeError::ProfileMismatch,
        )
        .await?;
        let (stores, pair) = pristine(IDS[0]).await?;
        refused(
            &mut cluster,
            IDS[0],
            stores,
            pair,
            ReplicaRuntimeError::NodeRunning,
        )
        .await?;
        let stopped = stop_seeded_follower(&mut cluster, &controls).await?;
        let (log, state) = controls[stopped.index]
            .take()
            .ok_or("missing retired writers")?
            .into_backends()?;
        let (stores, pair) = gates::prepare(stopped.node_id, log, state, false).await?;
        unpolled(
            &mut cluster,
            stopped.node_id,
            stores,
            pair,
            &stopped.snapshots,
        )
        .await?;
        closed(&stopped.old_handle).await?;
        // An unpolled attempt leaves no pending receipt. A second exact
        // candidate is synchronously accepted, also without starting it.
        let (log, state) = copy_memory(&stopped.snapshots)?;
        let (stores, pair) = gates::prepare(stopped.node_id, log, state, false).await?;
        unpolled(
            &mut cluster,
            stopped.node_id,
            stores,
            pair,
            &stopped.snapshots,
        )
        .await
    }
    .await;
    let shutdown = cluster.shutdown().await;
    shutdown?;
    result
}

struct Paused<R: StateStore> {
    node_id: u64,
    index: usize,
    old_handle: ExperimentalRaftHandle,
    snapshots: [StoreSnapshot; 2],
    before: [gates::Counts; 2],
    pair: gates::Pair<R>,
    read: gates::Gate,
    log_drop: gates::Gate,
    state_drop: gates::Gate,
}

impl<R: StateStore> Paused<R> {
    fn release_all(&self) {
        self.read.release();
        self.log_drop.release();
        self.state_drop.release();
    }

    fn unchanged(&self) -> TestResult {
        let current = counts(&self.pair);
        check(
            current[0].reads > self.before[0].reads,
            "first poll did not reach a real candidate log read",
        )?;
        equal(
            current[0].commits,
            self.before[0].commits,
            "cancelled validation wrote candidate log history",
        )?;
        equal(
            current[1].commits,
            self.before[1].commits,
            "cancelled validation wrote candidate state history",
        )?;
        equal(
            snapshot(&self.pair)?,
            self.snapshots.clone(),
            "NoEngineStart cancellation changed retired history",
        )
    }
}

async fn cancel_in_validation<W, Reopen>(
    cluster: &mut ExperimentalRaftCluster,
    controls: &mut [Option<ReplicaControl<W>>; 3],
    reopen: Reopen,
) -> TestResult<Paused<W::Reader>>
where
    W: CommittedStore,
    Reopen: FnOnce(u64, (W, W)) -> TestResult<(W, W)>,
{
    let stopped = stop_seeded_follower(cluster, controls).await?;
    let backends = controls[stopped.index]
        .take()
        .ok_or("missing retired writers")?
        .into_backends()?;
    let (log, state) = reopen(stopped.node_id, backends)?;
    let (stores, pair) = gates::prepare(stopped.node_id, log, state, false).await?;
    let unchanged = snapshot(&pair).and_then(|current| {
        equal(
            current,
            stopped.snapshots.clone(),
            "candidate preparation changed exact retired state",
        )
    });
    if let Err(error) = unchanged {
        stores.shutdown().await?;
        return Err(error);
    }
    let before = counts(&pair);
    // All three gates are armed only once the real candidate is Prepared.
    let read = pair.log.gate_next_read();
    let log_drop = pair.log.gate_writer_drop();
    let state_drop = pair.state.gate_writer_drop();
    let attempt = match cluster.rejoin_node(stopped.node_id, stores) {
        Ok(attempt) => attempt,
        Err(error) => {
            let reason = error.reason();
            read.release();
            log_drop.release();
            state_drop.release();
            error.into_stores().shutdown().await?;
            return Err(reason.into());
        }
    };
    let mut attempt = Box::pin(attempt);
    let reached = async {
        fixture::pending(attempt.as_mut()).await?;
        within(read.entered()).await?;
        check(
            !pair.log.writer_dropped() && !pair.state.writer_dropped(),
            "candidate was destroyed during a blocked validation read",
        )
    }
    .await;
    // Losing this waiter does not own cancellation of native storage work.
    drop(attempt);
    reached?;
    check(
        cluster.handle(stopped.node_id).is_none(),
        "blocked validation published a replacement node",
    )?;
    Ok(Paused {
        node_id: stopped.node_id,
        index: stopped.index,
        old_handle: stopped.old_handle,
        snapshots: stopped.snapshots,
        before,
        pair,
        read,
        log_drop,
        state_drop,
    })
}

type BarrierResult = Result<(), ReplicaRuntimeError>;

async fn barrier_pending<F: Future<Output = BarrierResult> + ?Sized>(
    mut barrier: Pin<&mut F>,
    completed: &mut Option<BarrierResult>,
) -> TestResult {
    if completed.is_some() {
        return Err("joined cleanup already completed while a candidate writer was blocked".into());
    }
    poll_fn(|cx| match barrier.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(result) => {
            *completed = Some(result);
            Poll::Ready(Err(
                "joined cleanup completed before both native candidate owners exited".into(),
            ))
        }
    })
    .await
}

async fn barrier_stays_pending<F: Future<Output = BarrierResult> + ?Sized>(
    mut barrier: Pin<&mut F>,
    completed: &mut Option<BarrierResult>,
) -> TestResult {
    barrier_pending(barrier.as_mut(), completed).await?;
    // This bounds only observation of a borrowed public waiter. The owning
    // future remains retained, and no native I/O gate has a timeout.
    match tokio::time::timeout(PENDING_OBSERVATION, barrier.as_mut()).await {
        Err(_) => Ok(()),
        Ok(result) => {
            *completed = Some(result);
            Err("joined cleanup finished while a candidate owner remained blocked".into())
        }
    }
}

async fn require_both_native_exits<F, R>(
    mut barrier: Pin<&mut F>,
    completed: &mut Option<BarrierResult>,
    paused: &Paused<R>,
    release_log_first: bool,
) -> TestResult
where
    F: Future<Output = BarrierResult> + ?Sized,
    R: StateStore,
{
    barrier_stays_pending(barrier.as_mut(), completed).await?;
    check(
        !paused.pair.log.writer_dropped() && !paused.pair.state.writer_dropped(),
        "cleanup skipped the blocked validation read",
    )?;
    paused.read.release();
    within(paused.log_drop.entered()).await?;
    within(paused.state_drop.entered()).await?;
    barrier_stays_pending(barrier.as_mut(), completed).await?;
    if release_log_first {
        paused.log_drop.release();
        within(paused.pair.log.dropped()).await?;
        check(
            !paused.pair.state.writer_dropped(),
            "state Drop did not remain independently blocked",
        )?;
        barrier_stays_pending(barrier.as_mut(), completed).await?;
        paused.state_drop.release();
    } else {
        paused.state_drop.release();
        within(paused.pair.state.dropped()).await?;
        check(
            !paused.pair.log.writer_dropped(),
            "log Drop did not remain independently blocked",
        )?;
        barrier_stays_pending(barrier.as_mut(), completed).await?;
        paused.log_drop.release();
    }
    Ok(())
}

async fn pending_refusal<R: StateStore>(
    cluster: &mut ExperimentalRaftCluster,
    paused: &Paused<R>,
) -> TestResult {
    let (stores, pair) = pristine(paused.node_id).await?;
    refused(
        cluster,
        paused.node_id,
        stores,
        pair,
        ReplicaRuntimeError::RejoinInProgress,
    )
    .await?;
    check(
        !paused.pair.log.writer_dropped() && !paused.pair.state.writer_dropped(),
        "pending refusal consumed the prior attempt's owners",
    )
}

async fn cancellation_shutdown<W, Reopen>(
    backends: [(W, W); 3],
    reopen: Reopen,
    release_log_first: bool,
) -> TestResult
where
    W: CommittedStore,
    Reopen: FnOnce(u64, (W, W)) -> TestResult<(W, W)>,
{
    let (cluster, controls) = fixture::create(backends).await?;
    let mut cluster = Some(cluster);
    let mut controls = controls.map(Some);
    let outcome = async {
        let live = cluster.as_mut().ok_or("missing cluster before shutdown")?;
        let paused = cancel_in_validation(live, &mut controls, reopen).await?;
        pending_refusal(live, &paused).await?;
        let mut shutdown = Box::pin(cluster.take().ok_or("missing owned cluster")?.shutdown());
        let mut completed = None;
        let proof = require_both_native_exits(
            shutdown.as_mut(),
            &mut completed,
            &paused,
            release_log_first,
        )
        .await;
        // Failed observations release every barrier before actual cleanup.
        paused.release_all();
        let joined = match completed {
            Some(result) => result,
            None => shutdown.await,
        };
        joined?;
        within(paused.pair.log.dropped()).await?;
        within(paused.pair.state.dropped()).await?;
        proof?;
        paused.unchanged()?;
        check(
            paused.pair.state.message(2)?.is_none(),
            "cancelled candidate applied an extra message",
        )?;
        closed(&paused.old_handle).await
    }
    .await;
    // Before shutdown takes ownership, every error still joins the cluster and
    // its independently retained candidate attempt. No outer scenario timeout.
    if let Some(cluster) = cluster.take() {
        cluster.shutdown().await?;
    }
    outcome
}

async fn retry_after_cancellation<W, Reopen, Retry>(
    backends: [(W, W); 3],
    reopen: Reopen,
    retry: Retry,
) -> TestResult
where
    W: CommittedStore,
    Reopen: FnOnce(u64, (W, W)) -> TestResult<(W, W)>,
    Retry: FnOnce(u64, &[StoreSnapshot; 2]) -> TestResult<(W, W)>,
{
    let (mut cluster, controls) = fixture::create(backends).await?;
    let mut controls = controls.map(Some);
    let outcome = async {
        let paused = cancel_in_validation(&mut cluster, &mut controls, reopen).await?;
        pending_refusal(&mut cluster, &paused).await?;
        let surviving = confirmed(&cluster, WHILE_CANCELLED, "while-cancelled-validation").await?;
        equal(
            surviving.outcome,
            QueueWriteOutcome::Sent { sequence: 2 },
            "survivors did not confirm work while candidate validation was blocked",
        )?;
        check(
            surviving.entry.node_id != paused.node_id,
            "blocked candidate authored a surviving-majority receipt",
        )?;
        let mut second_message = None;
        for (index, control) in controls.iter().enumerate() {
            if index != paused.index {
                let message = within(
                    control
                        .as_ref()
                        .ok_or("missing surviving control")?
                        .state
                        .wait_message(2),
                )
                .await??;
                equal(
                    message.body.as_slice(),
                    WHILE_CANCELLED,
                    "survivor applied incorrect content during cancellation",
                )?;
                equal(
                    message.message_id.as_str(),
                    "while-cancelled-validation",
                    "survivor applied incorrect cancelled-phase identity",
                )?;
                if let Some(expected) = &second_message {
                    equal(
                        &message,
                        expected,
                        "survivors disagree on the complete cancelled-phase record",
                    )?;
                }
                second_message = Some(message);
            }
        }
        let second_message =
            second_message.ok_or("no surviving replica applied the second receipt")?;
        paused.unchanged()?;
        check(
            paused.pair.state.message(2)?.is_none(),
            "blocked cancelled candidate applied surviving work",
        )?;
        let mut stop = Box::pin(cluster.stop_node(paused.node_id));
        let mut completed = None;
        let proof = require_both_native_exits(stop.as_mut(), &mut completed, &paused, false).await;
        paused.release_all();
        let joined = match completed {
            Some(result) => result,
            None => stop.as_mut().await,
        };
        // Release the stop future's mutable cluster borrow before re-admission.
        drop(stop);
        joined?;
        proof?;
        paused.unchanged()?;
        check(
            paused.pair.log.writer_dropped() && paused.pair.state.writer_dropped(),
            "successful stop omitted a native candidate writer",
        )?;
        let node_id = paused.node_id;
        let index = paused.index;
        let snapshots = paused.snapshots.clone();
        let old_handle = paused.old_handle.clone();
        // In the durable case this releases both candidate readers before the
        // same physical directories are reopened. No observed wrapper survives.
        drop(paused);
        let (log, state) = retry(node_id, &snapshots)?;
        let (stores, replacement) = gates::prepare(node_id, log, state, false).await?;
        let unchanged = snapshot(&replacement).and_then(|current| {
            equal(
                current,
                snapshots,
                "exact retry candidate changed history before a new engine",
            )
        });
        if let Err(error) = unchanged {
            stores.shutdown().await?;
            return Err(error);
        }
        let attempt = match cluster.rejoin_node(node_id, stores) {
            Ok(attempt) => attempt,
            Err(error) => {
                let reason = error.reason();
                error.into_stores().shutdown().await?;
                return Err(reason.into());
            }
        };
        within(attempt).await??;
        check(
            cluster.handle(node_id).is_some(),
            "same-history retry did not publish a new handle",
        )?;
        equal(
            cluster.node_ids().collect::<Vec<_>>(),
            IDS.to_vec(),
            "retry did not restore exactly three voters",
        )?;
        closed(&old_handle).await?;
        equal(
            within(replacement.state.wait_message(2)).await??,
            second_message.clone(),
            "same-history retry failed to catch up the actual surviving-majority receipt",
        )?;
        let receipt = confirmed(&cluster, AFTER_RETRY, "after-cancellation-retry").await?;
        equal(
            receipt.outcome,
            QueueWriteOutcome::Sent { sequence: 3 },
            "retry did not confirm the next sequence",
        )?;
        check(
            receipt.entry.index > surviving.entry.index,
            "retry receipt did not advance beyond surviving history",
        )?;
        let message = within(replacement.state.wait_message(3)).await??;
        equal(
            message.body.as_slice(),
            AFTER_RETRY,
            "replacement applied incorrect retry content",
        )?;
        equal(
            message.message_id.as_str(),
            "after-cancellation-retry",
            "replacement applied incorrect message identity",
        )?;
        for (other, control) in controls.iter().enumerate() {
            if other != index {
                equal(
                    within(
                        control
                            .as_ref()
                            .ok_or("missing surviving control")?
                            .state
                            .wait_message(3),
                    )
                    .await??,
                    message.clone(),
                    "all three replicas did not apply the same confirmed retry record",
                )?;
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            replacement,
            [second_message, message],
            old_handle,
        ))
    }
    .await;
    let shutdown = cluster.shutdown().await;
    shutdown?;
    let (replacement, messages, old_handle) = outcome?;
    check(
        replacement.log.writer_dropped() && replacement.state.writer_dropped(),
        "final cluster shutdown missed replacement writers",
    )?;
    for (index, message) in messages.iter().enumerate() {
        equal(
            replacement.state.message(index as u64 + 2)?,
            Some(message.clone()),
            "joined replacement changed a confirmed record",
        )?;
    }
    check(
        replacement.state.message(4)?.is_none(),
        "joined replacement contains an extra retry message",
    )?;
    for control in controls.iter().flatten() {
        for (index, message) in messages.iter().enumerate() {
            equal(
                control.state.message(index as u64 + 2)?,
                Some(message.clone()),
                "joined survivor changed a confirmed record",
            )?;
        }
        check(
            control.state.message(4)?.is_none(),
            "joined survivor contains an extra retry message",
        )?;
    }
    closed(&old_handle).await
}

#[tokio::test]
async fn cancelled_validation_shutdown_joins_both_memory_candidate_owners() -> TestResult {
    let stores = std::array::from_fn(|_| (MemoryReplicaStore::new(), MemoryReplicaStore::new()));
    cancellation_shutdown(stores, |_, stores| Ok(stores), true).await
}

#[tokio::test]
async fn cancelled_validation_shutdown_joins_both_durable_candidate_owners() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let reopen = |node_id, stores: (FjallReplicaStore, FjallReplicaStore)| {
        drop(stores);
        Ok((
            FjallReplicaStore::open(directory.path().join(format!("log-{node_id}")))?,
            FjallReplicaStore::open(directory.path().join(format!("state-{node_id}")))?,
        ))
    };
    cancellation_shutdown(fixture::durable_stores(directory.path())?, reopen, false).await
}

#[tokio::test]
async fn cancelled_validation_stop_preserves_floor_for_exact_memory_retry() -> TestResult {
    let stores = std::array::from_fn(|_| (MemoryReplicaStore::new(), MemoryReplicaStore::new()));
    // This copies every exact retired row into new memory writers; it is not a
    // durability or physical-reopen assertion. Fjall below supplies that case.
    retry_after_cancellation(
        stores,
        |_, stores| Ok(stores),
        |_, snapshots| copy_memory(snapshots),
    )
    .await
}

#[tokio::test]
async fn cancelled_validation_stop_preserves_floor_for_exact_durable_retry() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let reopen = |node_id, stores: (FjallReplicaStore, FjallReplicaStore)| {
        drop(stores);
        Ok((
            FjallReplicaStore::open(directory.path().join(format!("log-{node_id}")))?,
            FjallReplicaStore::open(directory.path().join(format!("state-{node_id}")))?,
        ))
    };
    let retry = |node_id, _: &[StoreSnapshot; 2]| {
        Ok((
            FjallReplicaStore::open(directory.path().join(format!("log-{node_id}")))?,
            FjallReplicaStore::open(directory.path().join(format!("state-{node_id}")))?,
        ))
    };
    retry_after_cancellation(fixture::durable_stores(directory.path())?, reopen, retry).await
}
