use std::{
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use cluster::{ExperimentalLogStore, LogEntry, LogVote, QueueLogCommand};
use domain::{
    CommittedApplyResult, CommittedCheckpointUpdate, CommittedEntryId, CommittedEntryMark,
    CommittedStateMachine, EntityPath, NamespaceName, QueueConfig, SequenceNumber, StateMachine,
    Timestamp,
};
use openraft::{
    EntryPayload, RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, FjallReplicaStore, MemoryReplicaStore, StateStore};

use super::{DEADLINE, TestResult, fixture::*};

const DIRECTORY: &str = "SWITCHYARD_LOG_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_LOG_TEST_EXIT_STAGE";
const OPERATION: &str = "SWITCHYARD_LOG_TEST_OPERATION";
const CHILD_TEST: &str = "crash::child_process_exit_boundary";

#[tokio::test]
async fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let fault = match std::env::var(STAGE)?.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown log test exit stage".into()),
    };
    let operation = std::env::var(OPERATION)?;
    let (writer, control) = observed(FjallReplicaStore::open(directory)?);
    let mut store = ExperimentalLogStore::open(writer, profile()?)?;
    control.fault(fault);
    execute(&mut store, &operation).await?;
    Err("the selected log process exit did not occur".into())
}

async fn execute(store: &mut ExperimentalLogStore, operation: &str) -> TestResult {
    match operation {
        "append" => {
            store
                .blocking_append([send(3, b"persisted before callback".to_vec())?])
                .await?
        }
        "vote" => store.save_vote(&LogVote::new_committed(4, 7)).await?,
        "truncate" => store.truncate(id(1, 2)).await?,
        "purge" => store.purge(id(1, 1)).await?,
        "bridge" => {
            store
                .blocking_append([send(1, b"bridge first".to_vec())?])
                .await?
        }
        _ => return Err("unknown log crash operation".into()),
    }
    Ok(())
}

async fn seeded<W: CommittedStore>(writer: W) -> TestResult<ExperimentalLogStore> {
    let mut store = ExperimentalLogStore::create(writer, profile()?)?;
    store
        .blocking_append([blank(0), blank(1), blank(2)])
        .await?;
    store.save_vote(&LogVote::new_committed(3, 7)).await?;
    Ok(store)
}

async fn recover_exit(operation: &str, stage: &str, code: i32, committed: bool) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let writer = FjallReplicaStore::open(directory.path())?;
    let raw = writer.reader();
    let baseline = seeded(writer).await?;
    let baseline_snapshot = raw.snapshot()?;
    baseline.shutdown().await?;
    drop(raw);

    let writer = MemoryReplicaStore::new();
    let expected_reader = writer.reader();
    let mut expected = seeded(writer).await?;
    assert_eq!(expected_reader.snapshot()?, baseline_snapshot);
    if committed {
        execute(&mut expected, operation).await?;
    }
    let expected_snapshot = expected_reader.snapshot()?;
    let expected_vote = expected.read_vote().await?;
    let expected_state = expected.get_log_state().await?;
    let expected_entries = expected.try_get_log_entries(..).await?;

    let status = run_child(directory.path(), stage, operation)?;
    assert_eq!(status.code(), Some(code));
    let writer = FjallReplicaStore::open(directory.path())?;
    let raw = writer.reader();
    let mut recovered = ExperimentalLogStore::open(writer, profile()?)?;
    assert_eq!(raw.snapshot()?, expected_snapshot);
    assert_eq!(recovered.read_vote().await?, expected_vote);
    assert_eq!(recovered.get_log_state().await?, expected_state);
    assert_eq!(recovered.try_get_log_entries(..).await?, expected_entries);

    if committed && operation == "append" {
        recovered
            .blocking_append([send(3, b"persisted before callback".to_vec())?])
            .await?;
        assert_eq!(raw.snapshot()?, expected_snapshot);
    }
    let next_index = expected_state
        .last_log_id
        .ok_or("missing expected durable log tail")?
        .index
        .checked_add(1)
        .ok_or("unexpected index exhaustion")?;
    let continuation = LogEntry {
        log_id: id(5, next_index),
        payload: EntryPayload::Blank,
    };
    recovered.blocking_append([continuation.clone()]).await?;
    expected.blocking_append([continuation]).await?;
    let final_snapshot = expected_reader.snapshot()?;
    let final_state = expected.get_log_state().await?;
    assert_eq!(raw.snapshot()?, final_snapshot);
    recovered.shutdown().await?;
    drop(raw);
    expected.shutdown().await?;

    let writer = FjallReplicaStore::open(directory.path())?;
    let raw = writer.reader();
    let mut reopened = ExperimentalLogStore::open(writer, profile()?)?;
    assert_eq!(raw.snapshot()?, final_snapshot);
    assert_eq!(reopened.get_log_state().await?, final_state);
    assert_eq!(reopened.read_vote().await?, expected_vote);
    reopened.shutdown().await?;
    Ok(())
}

macro_rules! crash_cases {
    ($($name:ident => ($operation:literal, $stage:literal, $code:literal, $committed:literal)),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() -> TestResult {
                tokio::time::timeout(DEADLINE, recover_exit($operation, $stage, $code, $committed)).await??;
                Ok(())
            }
        )+
    };
}

crash_cases!(
    append_exit_before_commit_preserves_exact_baseline => ("append", "before", 77, false),
    append_exit_after_sync_recovers_exact_rows_and_progress => ("append", "after", 78, true),
    vote_exit_before_commit_preserves_exact_baseline => ("vote", "before", 77, false),
    vote_exit_after_sync_recovers_exact_vote_and_progress => ("vote", "after", 78, true),
    truncate_exit_before_commit_preserves_exact_baseline => ("truncate", "before", 77, false),
    truncate_exit_after_sync_recovers_exact_suffix_removal => ("truncate", "after", 78, true),
    purge_exit_before_commit_preserves_exact_baseline => ("purge", "before", 77, false),
    purge_exit_after_sync_recovers_exact_prefix_and_boundary => ("purge", "after", 78, true),
);

fn create_entry() -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1, 0),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(1),
            QueueConfig::default(),
        )),
    })
}

fn apply_entry<W: CommittedStore>(
    machine: &mut CommittedStateMachine<W>,
    entry: &LogEntry,
    previous: Option<CommittedEntryMark>,
) -> TestResult<CommittedApplyResult> {
    let EntryPayload::Normal(command) = &entry.payload else {
        return Err("the queue bridge requires a normal typed entry".into());
    };
    let update = CommittedCheckpointUpdate {
        stream: profile()?.stream(),
        expected_previous: previous,
        entry: CommittedEntryId {
            term: entry.log_id.leader_id.term,
            node_id: entry.log_id.leader_id.node_id,
            index: entry.log_id.index,
        },
    };
    Ok(machine.apply_committed(&update, &command.clone().into_committed_work())?)
}

#[tokio::test]
async fn persisted_callback_loss_can_recover_into_a_separate_committed_domain_writer() -> TestResult
{
    tokio::time::timeout(DEADLINE, recover_into_domain()).await??;
    Ok(())
}

async fn recover_into_domain() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let log_path = directory.path().join("log");
    let domain_path = directory.path().join("domain");
    let mut log = ExperimentalLogStore::create(FjallReplicaStore::open(&log_path)?, profile()?)?;
    let create = create_entry()?;
    log.blocking_append([create.clone()]).await?;
    let mut machine =
        CommittedStateMachine::create(FjallReplicaStore::open(&domain_path)?, profile()?.stream())?;
    apply_entry(&mut machine, &create, None)?;
    let baseline_checkpoint = machine.checkpoint()?;
    let baseline_snapshot = machine.reader().snapshot()?;
    log.shutdown().await?;
    drop(machine);

    assert_eq!(run_child(&log_path, "after", "bridge")?.code(), Some(78));
    let mut log = ExperimentalLogStore::open(FjallReplicaStore::open(&log_path)?, profile()?)?;
    let entries = log.try_get_log_entries(..).await?;
    assert_eq!(
        entries,
        vec![create.clone(), send(1, b"bridge first".to_vec())?]
    );
    let mut machine =
        CommittedStateMachine::open(FjallReplicaStore::open(&domain_path)?, profile()?.stream())?;
    assert_eq!(machine.checkpoint()?, baseline_checkpoint);
    assert_eq!(machine.reader().snapshot()?, baseline_snapshot);

    let mut expected =
        CommittedStateMachine::create(MemoryReplicaStore::new(), profile()?.stream())?;
    apply_entry(&mut expected, &entries[0], None)?;
    apply_entry(&mut expected, &entries[1], baseline_checkpoint.last())?;
    assert!(matches!(
        apply_entry(&mut machine, &entries[1], baseline_checkpoint.last())?,
        CommittedApplyResult::Applied { .. }
    ));
    assert_eq!(machine.checkpoint()?, expected.checkpoint()?);
    assert_eq!(machine.reader().snapshot()?, expected.reader().snapshot()?);
    let committed_snapshot = machine.reader().snapshot()?;
    drop(machine);

    let mut machine =
        CommittedStateMachine::open(FjallReplicaStore::open(&domain_path)?, profile()?.stream())?;
    let checkpoint = machine.checkpoint()?;
    assert_eq!(
        apply_entry(&mut machine, &entries[1], checkpoint.previous())?,
        CommittedApplyResult::AlreadyApplied {
            position: checkpoint
                .last()
                .ok_or("missing committed bridge position")?
        }
    );
    assert_eq!(machine.reader().snapshot()?, committed_snapshot);
    let next = send(2, b"bridge second".to_vec())?;
    log.blocking_append([next.clone()]).await?;
    let next = log
        .try_get_log_entries(2..=2)
        .await?
        .pop()
        .ok_or("missing continued log entry")?;
    apply_entry(&mut machine, &next, checkpoint.last())?;
    apply_entry(&mut expected, &next, checkpoint.last())?;
    assert_eq!(machine.reader().snapshot()?, expected.reader().snapshot()?);
    let view = StateMachine::new(machine.reader());
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    assert_eq!(
        view.message(&namespace, &entity, SequenceNumber::new(1))?
            .ok_or("missing first bridge allocation")?
            .body,
        b"bridge first"
    );
    assert_eq!(
        view.message(&namespace, &entity, SequenceNumber::new(2))?
            .ok_or("missing next bridge allocation")?
            .body,
        b"bridge second"
    );
    assert_eq!(
        view.message(&namespace, &entity, SequenceNumber::new(3))?,
        None
    );
    let final_snapshot = machine.reader().snapshot()?;
    let final_checkpoint = machine.checkpoint()?;
    drop(view);
    drop(machine);
    log.shutdown().await?;
    let reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(&domain_path)?, profile()?.stream())?;
    assert_eq!(reopened.checkpoint()?, final_checkpoint);
    assert_eq!(reopened.reader().snapshot()?, final_snapshot);
    Ok(())
}

fn run_child(directory: &Path, stage: &str, operation: &str) -> TestResult<ExitStatus> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        .env(OPERATION, operation)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut child = ChildGuard(Some(child));
    let deadline = Instant::now() + DEADLINE;
    loop {
        let Some(process) = child.0.as_mut() else {
            return Err("log test child is unavailable".into());
        };
        if let Some(status) = process.try_wait()? {
            child.0.take();
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("log test child exceeded its deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct ChildGuard(Option<Child>);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
