use std::{
    error::Error,
    fmt::Debug,
    future::Future,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use domain::{
    CommittedCheckpoint, CommittedEntryId, CommittedSend, CommittedStreamId, EntityIncarnation,
    EntityPath, MessageRecord, NamespaceName, QueueConfig, SequenceNumber, StateMachine,
};
use storage::{
    CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore, StorageError, StoreSnapshot,
    WriteBatch,
};
use tokio::sync::Notify;

use crate::{
    ClientWorkload, ExperimentalLogStore, ExperimentalRaftCluster, ExperimentalReplicaStores,
    ExperimentalStateMachine, LogProfile, QueueIntent, QueueWriteError, QueueWriteOutcome,
    QueueWriteRejection, QueueWriteResult,
};

use super::network::TestIsolation;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const IDS: [u64; 3] = [7, 8, 9];
const DEADLINE: Duration = Duration::from_secs(60);
const ORIGINAL: &[u8] = b"confirmed before isolating the live leader";
const DURING_PARTITION: &[u8] = b"confirmed by both live majority survivors";
const AFTER_HEAL: &[u8] = b"confirmed after healing all original node generations";

fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}

fn entity() -> TestResult<EntityPath> {
    Ok(EntityPath::new("orders")?)
}

fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([61; 16])?)
}

fn send(body: &[u8], name: &str) -> TestResult<QueueIntent> {
    Ok(QueueIntent::send(
        namespace()?,
        entity()?,
        CommittedSend {
            message_id: name.to_owned(),
            body: body.to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    )?)
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
        .map_err(|_| "partition observation reached its deadline".into())
}

struct Observed<W: CommittedStore> {
    shared: Arc<Mutex<W>>,
    reader: W::Reader,
    changed: Arc<Notify>,
    owner_retired: Arc<AtomicBool>,
}

struct Control<W: CommittedStore> {
    shared: Arc<Mutex<W>>,
    reader: W::Reader,
    changed: Arc<Notify>,
    owner_retired: Arc<AtomicBool>,
}

struct ReadOnlyCheckpoint<W: CommittedStore> {
    shared: Arc<Mutex<W>>,
    reader: W::Reader,
}

impl<W: CommittedStore> CommittedStore for ReadOnlyCheckpoint<W> {
    type Reader = W::Reader;

    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .is_initialized()
    }

    fn commit(&mut self, _batch: WriteBatch) -> Result<(), StorageError> {
        Err(StorageError::ReplicaWriteRequired)
    }
}

fn observed<W: CommittedStore>(writer: W) -> (Observed<W>, Control<W>) {
    let reader = writer.reader();
    let shared = Arc::new(Mutex::new(writer));
    let changed = Arc::new(Notify::new());
    let owner_retired = Arc::new(AtomicBool::new(false));
    (
        Observed {
            shared: shared.clone(),
            reader: reader.clone(),
            changed: changed.clone(),
            owner_retired: owner_retired.clone(),
        },
        Control {
            shared,
            reader,
            changed,
            owner_retired,
        },
    )
}

impl<W: CommittedStore> CommittedStore for Observed<W> {
    type Reader = W::Reader;

    fn reader(&self) -> Self::Reader {
        self.reader.clone()
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.shared
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.shared
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .commit(batch)?;
        self.changed.notify_waiters();
        Ok(())
    }
}

impl<W: CommittedStore> Drop for Observed<W> {
    fn drop(&mut self) {
        self.owner_retired.store(true, Ordering::Release);
    }
}

impl<W: CommittedStore> Control<W> {
    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.reader.snapshot()?)
    }

    fn retired(&self) -> bool {
        self.owner_retired.load(Ordering::Acquire)
    }

    fn checkpoint(&self) -> TestResult<CommittedCheckpoint> {
        // Test-only read observation of the actual stored full checkpoint.
        // It supplies no writer, runtime health authority, or retirement floor.
        let machine = domain::CommittedStateMachine::open(
            ReadOnlyCheckpoint {
                shared: self.shared.clone(),
                reader: self.reader.clone(),
            },
            stream()?,
        )?;
        Ok(machine.checkpoint()?)
    }

    fn message(&self, sequence: u64) -> TestResult<Option<MessageRecord>> {
        Ok(StateMachine::new(self.reader.clone()).message(
            &namespace()?,
            &entity()?,
            SequenceNumber::new(sequence),
        )?)
    }

    fn incarnation(&self) -> TestResult<EntityIncarnation> {
        StateMachine::new(self.reader.clone())
            .entity_incarnation(&namespace()?, &entity()?)?
            .ok_or_else(|| "confirmed queue has no incarnation".into())
    }

    async fn wait_message(&self, sequence: u64) -> TestResult<MessageRecord> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(message) = self.message(sequence)? {
                return Ok(message);
            }
            changed.await;
        }
    }

    fn into_writer(self) -> TestResult<W> {
        let Self {
            shared,
            reader,
            changed,
            owner_retired,
        } = self;
        drop(reader);
        drop(changed);
        drop(owner_retired);
        // Only successful joined cleanup authorizes extracting this unique W.
        Arc::try_unwrap(shared)
            .map_err(|_| "joined store still has an owner or checkpoint observer")?
            .into_inner()
            .map_err(|_| "joined store writer lock was poisoned".into())
    }
}

struct ReplicaControl<W: CommittedStore> {
    log: Control<W>,
    state: Control<W>,
}

impl<W: CommittedStore> ReplicaControl<W> {
    fn snapshot(&self) -> TestResult<[StoreSnapshot; 2]> {
        Ok([self.log.snapshot()?, self.state.snapshot()?])
    }

    fn into_writers(self) -> TestResult<(W, W)> {
        Ok((self.log.into_writer()?, self.state.into_writer()?))
    }
}

async fn prepare<W: CommittedStore>(
    node_id: u64,
    log: W,
    state: W,
    create: bool,
) -> TestResult<(ExperimentalReplicaStores, ReplicaControl<W>)> {
    let (log, log_control) = observed(log);
    let (state, state_control) = observed(state);
    let profile = LogProfile::new(node_id, stream()?)?;
    let log = if create {
        ExperimentalLogStore::create(log, profile)?
    } else {
        ExperimentalLogStore::open(log, profile)?
    };
    let state = if create {
        ExperimentalStateMachine::create(state, stream()?)
    } else {
        ExperimentalStateMachine::open(state, stream()?)
    };
    let state = match state {
        Ok(state) => state,
        Err(error) => {
            log.shutdown().await?;
            return Err(error.into());
        }
    };
    let prepared = ExperimentalReplicaStores::prepare(node_id, log, state).await?;
    Ok((
        prepared,
        ReplicaControl {
            log: log_control,
            state: state_control,
        },
    ))
}

async fn join_prepared(stores: Vec<ExperimentalReplicaStores>) -> TestResult {
    let mut failure = None;
    for stores in stores {
        if let Err(error) = stores.shutdown().await {
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

async fn create<W: CommittedStore>(
    writers: [(W, W); 3],
) -> TestResult<(ExperimentalRaftCluster, [ReplicaControl<W>; 3])> {
    let mut stores = Vec::new();
    let mut controls = Vec::new();
    let preparation = async {
        for (id, (log, state)) in IDS.into_iter().zip(writers) {
            let (prepared, control) = prepare(id, log, state, true).await?;
            stores.push(prepared);
            controls.push(control);
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }
    .await;
    if let Err(error) = preparation {
        join_prepared(stores).await?;
        return Err(error);
    }
    let stores: [ExperimentalReplicaStores; 3] = stores
        .try_into()
        .map_err(|_| "expected three prepared pairs")?;
    let controls = controls
        .try_into()
        .map_err(|_| "expected three replica controls")?;
    // The factory owns partial-startup cleanup. Do not time out native startup.
    Ok((ExperimentalRaftCluster::create(stores).await?, controls))
}

async fn bootstrap_queue(cluster: &ExperimentalRaftCluster) -> TestResult {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(id) = cluster.leader_hint() {
            let handle = cluster
                .handle(id)
                .ok_or("routing hint references absent initial node")?;
            let intent =
                QueueIntent::create_queue(namespace()?, entity()?, QueueConfig::default())?;
            match within(handle.submit(intent)).await? {
                Ok(result) => {
                    return equal(
                        result.outcome,
                        QueueWriteOutcome::QueueCreated,
                        "initial queue was not confirmed",
                    );
                }
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {}
                Err(error) => return Err(error.into()),
            }
        }
        check(
            Instant::now() < deadline,
            "no initial queue proposer became available",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn first_send(cluster: &ExperimentalRaftCluster) -> TestResult<QueueWriteResult> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(id) = cluster.leader_hint() {
            let handle = cluster
                .handle(id)
                .ok_or("routing hint references absent initial node")?;
            match within(handle.submit(send(ORIGINAL, "before-live-isolation")?)).await? {
                Ok(result) => return Ok(result),
                Err(QueueWriteError::KnownRejected(
                    QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
                )) => {}
                // Unknown is never retried and never counted as acknowledgement.
                Err(error) => return Err(error.into()),
            }
        }
        check(Instant::now() < deadline, "initial send was not confirmed")?;
        tokio::task::yield_now().await;
    }
}

fn covers(checkpoint: &CommittedCheckpoint, entry: CommittedEntryId) -> bool {
    checkpoint.last().is_some_and(|mark| {
        mark.id.index > entry.index || (mark.id.index == entry.index && mark.id == entry)
    })
}

async fn applied_fence<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[ReplicaControl<W>; 3],
    excluded: Option<u64>,
    at_least: CommittedEntryId,
    higher_than_term: Option<u64>,
) -> TestResult<CommittedCheckpoint> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut common = None;
        let mut agrees = true;
        for (id, control) in IDS.into_iter().zip(controls) {
            if excluded == Some(id) {
                continue;
            }
            let checkpoint = control.state.checkpoint()?;
            if !covers(&checkpoint, at_least)
                || checkpoint.last().is_none_or(|mark| {
                    excluded == Some(mark.id.node_id)
                        || higher_than_term.is_some_and(|term| mark.id.term <= term)
                })
                || common
                    .as_ref()
                    .is_some_and(|previous| previous != &checkpoint)
            {
                agrees = false;
                break;
            }
            common = Some(checkpoint);
        }
        if agrees && let Some(common) = common {
            let origin = common
                .last()
                .ok_or("fence lacks applied identity")?
                .id
                .node_id;
            if cluster
                .nodes
                .get(&origin)
                .is_some_and(|node| node.leader_hint() == Some(origin))
            {
                // The full checkpoints establish applied history. This hint
                // selects a still-live candidate; the subsequent receipt proves
                // that the real engine actually acknowledged a quorum write.
                return Ok(common);
            }
        }
        check(
            Instant::now() < deadline,
            "actual applied checkpoints did not reach a common native identity",
        )?;
        tokio::task::yield_now().await;
    }
}

async fn send_after_fence<W: CommittedStore>(
    cluster: &ExperimentalRaftCluster,
    controls: &[ReplicaControl<W>; 3],
    excluded: Option<u64>,
    at_least: CommittedEntryId,
    higher_than_term: Option<u64>,
    body: &[u8],
    name: &str,
) -> TestResult<QueueWriteResult> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let checkpoint =
            applied_fence(cluster, controls, excluded, at_least, higher_than_term).await?;
        let origin = checkpoint
            .last()
            .ok_or("missing applied proposer identity")?
            .id;
        let handle = cluster
            .handle(origin.node_id)
            .ok_or("applied proposer references absent live node")?;
        match within(handle.submit(send(body, name)?)).await? {
            Ok(result) => {
                equal(
                    result.entry.node_id,
                    origin.node_id,
                    "receipt did not come from the selected actual proposer",
                )?;
                check(
                    result.entry.index > origin.index,
                    "new receipt did not advance past the native checkpoint fence",
                )?;
                return Ok(result);
            }
            Err(QueueWriteError::KnownRejected(
                QueueWriteRejection::NotLeader | QueueWriteRejection::QuorumUnavailable,
            )) => {}
            Err(error) => return Err(error.into()),
        }
        check(
            Instant::now() < deadline,
            "no fenced proposer confirmed the new intent",
        )?;
    }
}

async fn messages<W: CommittedStore>(
    controls: &[ReplicaControl<W>; 3],
    excluded: Option<u64>,
    sequence: u64,
    body: &[u8],
    name: &str,
) -> TestResult<MessageRecord> {
    let mut expected = None;
    for (id, control) in IDS.into_iter().zip(controls) {
        if excluded == Some(id) {
            continue;
        }
        let message = within(control.state.wait_message(sequence)).await??;
        equal(
            message.sequence.as_u64(),
            sequence,
            "unexpected confirmed sequence",
        )?;
        equal(message.body.as_slice(), body, "unexpected confirmed body")?;
        equal(
            message.message_id.as_str(),
            name,
            "unexpected confirmed message identity",
        )?;
        if let Some(expected) = &expected {
            equal(
                &message,
                expected,
                "actual replicas disagree on the complete confirmed record",
            )?;
        }
        expected = Some(message);
    }
    expected.ok_or_else(|| "no actual replica observed the confirmed record".into())
}

fn all_owners_live<W: CommittedStore>(controls: &[ReplicaControl<W>; 3]) -> TestResult {
    check(
        controls
            .iter()
            .all(|control| !control.log.retired() && !control.state.retired()),
        "a storage owner retired during the supposedly live partition",
    )
}

struct Records {
    messages: [MessageRecord; 3],
    incarnation: EntityIncarnation,
    last_receipt: CommittedEntryId,
}

async fn scenario<W: CommittedStore>(
    cluster: &mut ExperimentalRaftCluster,
    controls: &[ReplicaControl<W>; 3],
) -> TestResult<Records> {
    bootstrap_queue(cluster).await?;
    let original = first_send(cluster).await?;
    equal(
        original.outcome,
        QueueWriteOutcome::Sent { sequence: 1 },
        "baseline send was not confirmed",
    )?;
    let first = messages(controls, None, 1, ORIGINAL, "before-live-isolation").await?;
    let baseline = applied_fence(cluster, controls, None, original.entry, None).await?;
    let prior = baseline.last().ok_or("baseline has no applied mark")?.id;
    let isolated = prior.node_id;
    let isolated_index = IDS
        .iter()
        .position(|id| *id == isolated)
        .ok_or("baseline names a foreign node")?;
    let old_handle = cluster
        .handle(isolated)
        .ok_or("missing original leader handle")?;
    let incarnation = controls[0].state.incarnation()?;
    for control in controls {
        equal(
            control.state.incarnation()?,
            incarnation,
            "baseline queue incarnations disagree",
        )?;
    }
    all_owners_live(controls)?;
    let mut isolation: Option<TestIsolation> = Some(cluster.routes.isolate_for_test(isolated)?);
    // Admission is cut synchronously. Only this traffic receipt, not a global
    // workload observation or timer delay, fences previously admitted work.
    within(isolation.as_ref().ok_or("missing cut receipt")?.settled()).await??;
    equal(
        cluster.node_ids().collect::<Vec<_>>(),
        IDS.to_vec(),
        "isolation stopped or replaced a node",
    )?;
    all_owners_live(controls)?;
    let elected = applied_fence(
        cluster,
        controls,
        Some(isolated),
        original.entry,
        Some(prior.term),
    )
    .await?;
    let elected_id = elected
        .last()
        .ok_or("survivors lack an actual elected mark")?
        .id;
    check(
        elected_id.term > prior.term && elected_id.node_id != isolated,
        "survivors did not apply a higher-term majority identity",
    )?;
    let second = send_after_fence(
        cluster,
        controls,
        Some(isolated),
        original.entry,
        Some(prior.term),
        DURING_PARTITION,
        "during-live-isolation",
    )
    .await?;
    equal(
        second.outcome,
        QueueWriteOutcome::Sent { sequence: 2 },
        "live majority did not confirm the second send",
    )?;
    check(
        second.entry.term > prior.term && second.entry.node_id != isolated,
        "majority receipt did not come from the higher-term surviving leader",
    )?;
    check(
        second.entry.index > original.entry.index,
        "majority receipt did not advance retained history",
    )?;
    let second_message = messages(
        controls,
        Some(isolated),
        2,
        DURING_PARTITION,
        "during-live-isolation",
    )
    .await?;
    applied_fence(
        cluster,
        controls,
        Some(isolated),
        second.entry,
        Some(prior.term),
    )
    .await?;
    let minority =
        within(old_handle.submit(send(b"must not be acknowledged", "isolated-minority")?)).await?;
    match minority {
        Err(QueueWriteError::KnownRejected(
            QueueWriteRejection::QuorumUnavailable | QueueWriteRejection::NotLeader,
        )) => {}
        // Unknown is neither an acceptable minority refusal nor retry advice.
        result => return Err(format!(
            "live minority did not give an exact unsubmitted quorum/not-leader refusal: {result:?}"
        )
        .into()),
    }
    equal(
        old_handle.workload(),
        ClientWorkload::default(),
        "minority refusal retained an ingress charge",
    )?;
    equal(
        controls[isolated_index].state.message(1)?,
        Some(first.clone()),
        "isolation lost the original confirmed record",
    )?;
    check(
        controls[isolated_index].state.message(2)?.is_none(),
        "the isolated live node applied surviving work across the settled cut",
    )?;
    all_owners_live(controls)?;
    // No stop, rejoin, new writer, or new Raft factory occurs in this scenario.
    isolation
        .take()
        .ok_or("missing isolation to heal")?
        .heal()?;
    applied_fence(cluster, controls, None, second.entry, None).await?;
    equal(
        within(controls[isolated_index].state.wait_message(2)).await??,
        second_message.clone(),
        "healed original node did not catch up actual majority history",
    )?;
    let third = send_after_fence(
        cluster,
        controls,
        None,
        second.entry,
        None,
        AFTER_HEAL,
        "after-live-heal",
    )
    .await?;
    equal(
        third.outcome,
        QueueWriteOutcome::Sent { sequence: 3 },
        "healed three-live-node cluster did not confirm the third send",
    )?;
    check(
        third.entry.index > second.entry.index,
        "post-heal receipt did not advance majority history",
    )?;
    let third_message = messages(controls, None, 3, AFTER_HEAL, "after-live-heal").await?;
    equal(
        messages(controls, None, 1, ORIGINAL, "before-live-isolation").await?,
        first.clone(),
        "healing changed the original complete record",
    )?;
    equal(
        messages(controls, None, 2, DURING_PARTITION, "during-live-isolation").await?,
        second_message.clone(),
        "healing changed the majority complete record",
    )?;
    applied_fence(cluster, controls, None, third.entry, None).await?;
    all_owners_live(controls)?;
    for control in controls {
        equal(
            control.state.incarnation()?,
            incarnation,
            "partition or healing recreated the queue",
        )?;
    }
    Ok(Records {
        messages: [first, second_message, third_message],
        incarnation,
        last_receipt: third.entry,
    })
}

struct Frozen {
    snapshots: [[StoreSnapshot; 2]; 3],
    checkpoints: [CommittedCheckpoint; 3],
    records: Records,
}

fn verify_records<W: CommittedStore>(
    controls: &[ReplicaControl<W>; 3],
    records: &Records,
) -> TestResult {
    for control in controls {
        equal(
            control.state.incarnation()?,
            records.incarnation,
            "joined storage changed the queue incarnation",
        )?;
        for (index, message) in records.messages.iter().enumerate() {
            equal(
                control.state.message(index as u64 + 1)?,
                Some(message.clone()),
                "joined storage changed a complete confirmed record",
            )?;
        }
        check(
            control.state.message(4)?.is_none(),
            "frozen storage contains an extra or retried message",
        )?;
        check(
            covers(&control.state.checkpoint()?, records.last_receipt),
            "frozen applied checkpoint fell behind the acknowledged final identity",
        )?;
    }
    Ok(())
}

async fn joined_cycle<W: CommittedStore>(
    writers: [(W, W); 3],
) -> TestResult<([ReplicaControl<W>; 3], Frozen)> {
    let (mut cluster, controls) = create(writers).await?;
    let mut handles = Vec::new();
    let outcome = async {
        for id in IDS {
            handles.push(
                cluster
                    .handle(id)
                    .ok_or("missing original client generation")?,
            );
        }
        scenario(&mut cluster, &controls).await
    }
    .await;
    // A scenario error first drops/heals its exact-generation cut, then every
    // started node is joined. There is deliberately no owning-scenario timeout.
    let shutdown = cluster.shutdown().await;
    shutdown?;
    let records = outcome?;
    check(
        controls
            .iter()
            .all(|control| control.log.retired() && control.state.retired()),
        "successful joined shutdown skipped an actual storage adapter owner",
    )?;
    verify_records(&controls, &records)?;
    let snapshots = [
        controls[0].snapshot()?,
        controls[1].snapshot()?,
        controls[2].snapshot()?,
    ];
    let checkpoints = [
        controls[0].state.checkpoint()?,
        controls[1].state.checkpoint()?,
        controls[2].state.checkpoint()?,
    ];
    for handle in handles {
        equal(
            within(handle.submit(send(b"closed", "after-joined-partition-shutdown")?)).await?,
            Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed)),
            "an original generation stayed open after joined shutdown",
        )?;
        equal(
            handle.workload(),
            ClientWorkload::default(),
            "joined handle retained accepted work",
        )?;
    }
    for index in 0..IDS.len() {
        equal(
            controls[index].snapshot()?,
            snapshots[index].clone(),
            "closed handles changed frozen storage",
        )?;
        equal(
            controls[index].state.checkpoint()?,
            checkpoints[index].clone(),
            "closed handles changed the frozen full checkpoint",
        )?;
    }
    Ok((
        controls,
        Frozen {
            snapshots,
            checkpoints,
            records,
        },
    ))
}

fn durable_writers(path: &Path) -> TestResult<[(FjallReplicaStore, FjallReplicaStore); 3]> {
    let mut stores = Vec::new();
    for id in IDS {
        stores.push((
            FjallReplicaStore::open(path.join(format!("log-{id}")))?,
            FjallReplicaStore::open(path.join(format!("state-{id}")))?,
        ));
    }
    stores
        .try_into()
        .map_err(|_| "expected six durable writers".into())
}

async fn verify_durable_reopen(path: &Path, frozen: &Frozen) -> TestResult {
    let mut stores = Vec::new();
    let mut controls = Vec::new();
    let recovery = async {
        // All old controls, readers, and unique writers have been released.
        // These are real physical opens of all six directories, not wrapper reuse.
        for (index, (log, state)) in durable_writers(path)?.into_iter().enumerate() {
            let (prepared, control) = prepare(IDS[index], log, state, false).await?;
            stores.push(prepared);
            controls.push(control);
            let control = &controls[index];
            equal(
                control.snapshot()?,
                frozen.snapshots[index].clone(),
                "durable open/preparation changed exact retired vote/log/state content",
            )?;
            equal(
                control.state.checkpoint()?,
                frozen.checkpoints[index].clone(),
                "durable recovery changed the full applied checkpoint",
            )?;
        }
        let controls: &[ReplicaControl<FjallReplicaStore>; 3] = controls
            .as_slice()
            .try_into()
            .map_err(|_| "expected three recovered controls")?;
        verify_records(controls, &frozen.records)
    }
    .await;
    // Recovered owners are also all joined on every validation failure.
    let joined = join_prepared(stores).await;
    joined?;
    recovery?;
    for (index, control) in controls.iter().enumerate() {
        check(
            control.log.retired() && control.state.retired(),
            "reopened pair cleanup skipped a storage owner",
        )?;
        equal(
            control.snapshot()?,
            frozen.snapshots[index].clone(),
            "joined reopened storage changed exact retired history",
        )?;
        equal(
            control.state.checkpoint()?,
            frozen.checkpoints[index].clone(),
            "joined reopened storage changed its full checkpoint",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn three_live_memory_nodes_partition_then_heal_without_resending_confirmed_work() -> TestResult
{
    let stores = std::array::from_fn(|_| (MemoryReplicaStore::new(), MemoryReplicaStore::new()));
    joined_cycle(stores).await?;
    Ok(())
}

#[tokio::test]
async fn three_live_durable_nodes_partition_heal_and_reopen_all_six_exact_histories() -> TestResult
{
    let directory = testkit::DurableProvider::temporary()?;
    let (controls, frozen) = joined_cycle(durable_writers(directory.path())?).await?;
    let mut retired = Vec::new();
    for control in controls {
        retired.push(control.into_writers()?);
    }
    drop(retired);
    verify_durable_reopen(directory.path(), &frozen).await
}
