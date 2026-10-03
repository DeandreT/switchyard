use std::{
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use cluster::{
    ExperimentalLogStore, ExperimentalStateMachine, LogApplication, LogEntry, LogProfile,
};
use domain::QueueConfig;
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, FjallReplicaStore, StateStore};

use super::{DEADLINE, TestResult, fixture::*};

const DIRECTORY: &str = "SWITCHYARD_STATE_TEST_DIRECTORY";
const LOG_DIRECTORY: &str = "SWITCHYARD_STATE_TEST_LOG_DIRECTORY";
const STAGE: &str = "SWITCHYARD_STATE_TEST_EXIT_STAGE";
const CHILD_TEST: &str = "crash::child_process_exit_boundary";

fn remaining_entries() -> TestResult<Vec<LogEntry>> {
    Ok(vec![
        send(1, 2, b"first committed send".to_vec())?,
        send(2, 3, b"second committed send".to_vec())?,
        blank(3),
    ])
}

fn profile() -> TestResult<LogProfile> {
    Ok(LogProfile::new(7, stream()?)?)
}

#[tokio::test]
async fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let fault = match std::env::var(STAGE)?.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown state-machine test exit stage".into()),
    };
    let entries = if let Some(path) = std::env::var_os(LOG_DIRECTORY) {
        let mut log = ExperimentalLogStore::open(FjallReplicaStore::open(path)?, profile()?)?;
        let entries = log.try_get_log_entries(1..=3).await?;
        assert_eq!(entries, remaining_entries()?);
        log.shutdown().await?;
        entries
    } else {
        remaining_entries()?
    };
    let (writer, control) = observed(FjallReplicaStore::open(directory)?);
    let mut machine = ExperimentalStateMachine::open(writer, stream()?)?;
    control.fault_after(2, fault);
    machine.apply(entries).await?;
    Err("the selected state-machine process exit did not occur".into())
}

async fn recover_exit(
    stage: &str,
    code: i32,
    second_committed: bool,
    with_log: bool,
) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let state_path = directory.path().join("state");
    let log_path = directory.path().join("log");
    let create = create(0, 1, QueueConfig::default())?;
    let mut entries = vec![create.clone()];
    entries.extend(remaining_entries()?);

    let writer = FjallReplicaStore::open(&state_path)?;
    let reader = writer.reader();
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    assert_eq!(
        machine.apply([create.clone()]).await?,
        vec![LogApplication::QueueCreated]
    );
    let baseline = reader.snapshot()?;
    let baseline_state = machine.applied_state().await?;
    assert_eq!(baseline_state.0, Some(id(1, 0)));
    assert_eq!(baseline, model(&entries[..1]).await?.0);
    machine.shutdown().await?;
    drop(reader);

    let log_baseline = if with_log {
        let writer = FjallReplicaStore::open(&log_path)?;
        let reader = writer.reader();
        let mut log = ExperimentalLogStore::create(writer, profile()?)?;
        log.blocking_append(entries.clone()).await?;
        assert_eq!(log.try_get_log_entries(..).await?, entries);
        let snapshot = reader.snapshot()?;
        log.shutdown().await?;
        drop(reader);
        Some(snapshot)
    } else {
        None
    };

    // These exits select before commit or after SyncAll, not a torn journal/power-loss boundary.
    assert_eq!(
        run_child(&state_path, with_log.then_some(log_path.as_path()), stage)?.code(),
        Some(code)
    );
    let prefix = if second_committed { 3 } else { 2 };
    let (expected_snapshot, expected_state) = model(&entries[..prefix]).await?;
    let writer = FjallReplicaStore::open(&state_path)?;
    let reader = writer.reader();
    let mut recovered = ExperimentalStateMachine::open(writer, stream()?)?;
    assert_eq!(reader.snapshot()?, expected_snapshot);
    assert_eq!(recovered.applied_state().await?, expected_state);
    assert_eq!(
        message(&reader, 1)?
            .ok_or("missing recovered first send")?
            .body,
        b"first committed send"
    );
    assert_eq!(message(&reader, 2)?.is_some(), second_committed);
    assert!(message(&reader, 3)?.is_none());
    assert_eq!(
        counters(&reader)?
            .ok_or("missing recovered queue counters")?
            .next_sequence,
        if second_committed { 3 } else { 2 }
    );

    let mut log = if let Some(baseline) = log_baseline {
        let writer = FjallReplicaStore::open(&log_path)?;
        let reader = writer.reader();
        let mut log = ExperimentalLogStore::open(writer, profile()?)?;
        assert_eq!(reader.snapshot()?, baseline);
        assert_eq!(log.try_get_log_entries(..).await?, entries);
        Some(log)
    } else {
        None
    };
    let replay = if let Some(log) = log.as_mut() {
        log.try_get_log_entries((prefix - 1) as u64..prefix as u64)
            .await?
    } else {
        entries[prefix - 1..prefix].to_vec()
    };
    assert_eq!(
        recovered.apply(replay).await?,
        vec![LogApplication::AlreadyApplied {
            entry: entry_id((prefix - 1) as u64)
        }]
    );
    assert_eq!(reader.snapshot()?, expected_snapshot);
    assert_eq!(recovered.applied_state().await?, expected_state);

    let remaining = if let Some(log) = log.as_mut() {
        log.try_get_log_entries(prefix as u64..=3).await?
    } else {
        entries[prefix..].to_vec()
    };
    let expected_response = if second_committed {
        vec![LogApplication::CheckpointOnly]
    } else {
        vec![
            LogApplication::Sent { sequence: 2 },
            LogApplication::CheckpointOnly,
        ]
    };
    assert_eq!(recovered.apply(remaining).await?, expected_response);
    assert_eq!(reader.snapshot()?, model(&entries).await?.0);

    let continuation = send(4, 4, b"continued after recovery".to_vec())?;
    let next = if let Some(log) = log.as_mut() {
        log.blocking_append([continuation.clone()]).await?;
        log.try_get_log_entries(4..=4).await?
    } else {
        vec![continuation.clone()]
    };
    assert_eq!(
        recovered.apply(next).await?,
        vec![LogApplication::Sent { sequence: 3 }]
    );
    entries.push(continuation);
    let (final_snapshot, final_state) = model(&entries).await?;
    assert_eq!(reader.snapshot()?, final_snapshot);
    assert_eq!(recovered.applied_state().await?, final_state);
    assert_eq!(
        message(&reader, 3)?
            .ok_or("missing continued queue allocation")?
            .body,
        b"continued after recovery"
    );
    assert!(message(&reader, 4)?.is_none());
    assert_eq!(
        counters(&reader)?
            .ok_or("missing continued queue counters")?
            .next_sequence,
        4
    );
    recovered.shutdown().await?;
    drop(reader);
    if let Some(log) = log {
        log.shutdown().await?;
    }

    let writer = FjallReplicaStore::open(&state_path)?;
    let reader = writer.reader();
    let mut reopened = ExperimentalStateMachine::open(writer, stream()?)?;
    assert_eq!(reader.snapshot()?, final_snapshot);
    assert_eq!(reopened.applied_state().await?, final_state);
    reopened.shutdown().await?;
    drop(reader);
    if with_log {
        let mut reopened =
            ExperimentalLogStore::open(FjallReplicaStore::open(&log_path)?, profile()?)?;
        assert_eq!(reopened.try_get_log_entries(..).await?, entries);
        reopened.shutdown().await?;
    }
    Ok(())
}

macro_rules! crash_cases {
    ($($name:ident => ($stage:literal, $code:literal, $committed:literal, $with_log:literal)),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() -> TestResult {
                tokio::time::timeout(DEADLINE, recover_exit($stage, $code, $committed, $with_log)).await??;
                Ok(())
            }
        )+
    };
}

crash_cases!(
    exit_before_middle_commit_recovers_exact_applied_prefix => ("before", 79, false, false),
    exit_after_middle_sync_recovers_exact_applied_prefix => ("after", 80, true, false),
    actual_log_entries_resume_after_middle_precommit_exit => ("before", 79, false, true),
    actual_log_entries_resume_after_middle_sync_exit => ("after", 80, true, true),
);

fn run_child(
    directory: &Path,
    log_directory: Option<&Path>,
    stage: &str,
) -> TestResult<ExitStatus> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        .env_remove(LOG_DIRECTORY)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(path) = log_directory {
        command.env(LOG_DIRECTORY, path);
    }
    let mut child = ChildGuard(Some(command.spawn()?));
    let deadline = Instant::now() + DEADLINE;
    loop {
        let Some(process) = child.0.as_mut() else {
            return Err("state-machine test child is unavailable".into());
        };
        if let Some(status) = process.try_wait()? {
            child.0.take();
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("state-machine test child exceeded its deadline".into());
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
