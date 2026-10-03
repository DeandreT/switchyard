use std::{
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use domain::{
    CommittedApplyResult, CommittedCheckpointUpdate, CommittedEntryId, CommittedStateMachine,
    QueueConfig,
};
use storage::{FjallReplicaStore, MemoryReplicaStore, StateStore};

use super::{TestResult, fixture::*};

const DIRECTORY: &str = "SWITCHYARD_COMMITTED_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_COMMITTED_TEST_EXIT_STAGE";
const CHILD_TEST: &str = "crash::child_process_exit_boundary";
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let stage = std::env::var(STAGE)?;
    let fault = match stage.as_str() {
        "before" => CommitFault::ExitBefore,
        "after" => CommitFault::ExitAfter,
        _ => return Err("unknown committed test exit stage".into()),
    };
    let (writer, control) = observed(FjallReplicaStore::open(directory)?);
    let mut machine = CommittedStateMachine::open(writer, stream()?)?;
    let request = update(&machine, 1)?;
    control.fault(fault);
    machine.apply_committed(&request, &send(2, "one", b"durable payload")?)?;
    Err("the injected process exit did not occur".into())
}

#[test]
fn exit_before_commit_recovers_without_adopting_a_message() -> TestResult {
    recover_exit("before", 71, false)
}

#[test]
fn exit_after_sync_commit_recovers_progress_and_skips_replay() -> TestResult {
    recover_exit("after", 72, true)
}

fn recover_exit(stage: &str, code: i32, committed: bool) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let mut baseline =
        CommittedStateMachine::create(FjallReplicaStore::open(directory.path())?, stream()?)?;
    apply(&mut baseline, 0, &create(1, QueueConfig::default())?)?;
    let baseline_checkpoint = baseline.checkpoint()?;
    let baseline_snapshot = baseline.reader().snapshot()?;
    drop(baseline);

    let mut expected = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    apply(&mut expected, 0, &create(1, QueueConfig::default())?)?;
    assert_eq!(expected.checkpoint()?, baseline_checkpoint);
    assert_eq!(expected.reader().snapshot()?, baseline_snapshot);
    apply(&mut expected, 1, &send(2, "one", b"durable payload")?)?;
    let committed_checkpoint = expected.checkpoint()?;
    let committed_snapshot = expected.reader().snapshot()?;

    let status = run_child(directory.path(), stage)?;
    assert_eq!(status.code(), Some(code));

    let mut machine =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    let checkpoint = machine.checkpoint()?;
    let last = checkpoint.last().ok_or("missing durable progress")?;
    assert_eq!(last.id.index, u64::from(committed));
    if committed {
        assert_eq!(checkpoint, committed_checkpoint);
        assert_eq!(machine.reader().snapshot()?, committed_snapshot);
        assert_eq!(
            record(&machine.reader(), 1)?
                .ok_or("missing committed message")?
                .body,
            b"durable payload"
        );
        assert_eq!(
            counters(&machine.reader())?
                .ok_or("missing committed counters")?
                .next_sequence,
            2
        );
    } else {
        assert_eq!(checkpoint, baseline_checkpoint);
        assert_eq!(machine.reader().snapshot()?, baseline_snapshot);
        assert_eq!(record(&machine.reader(), 1)?, None);
        assert_eq!(counters(&machine.reader())?, None);
    }

    let request = CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: if committed {
            checkpoint.previous()
        } else {
            checkpoint.last()
        },
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index: 1,
        },
    };
    let before = machine.reader().snapshot()?;
    let result = machine.apply_committed(&request, &send(2, "one", b"durable payload")?)?;
    if committed {
        assert_eq!(
            result,
            CommittedApplyResult::AlreadyApplied { position: last }
        );
        assert_eq!(machine.reader().snapshot()?, before);
    } else {
        assert!(matches!(result, CommittedApplyResult::Applied { .. }));
    }
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing recovered message")?
            .body,
        b"durable payload"
    );
    assert_eq!(record(&machine.reader(), 2)?, None);
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing recovered counters")?
            .next_sequence,
        2
    );
    assert_eq!(machine.checkpoint()?, committed_checkpoint);
    assert_eq!(machine.reader().snapshot()?, committed_snapshot);

    apply(&mut machine, 2, &send(3, "two", b"continued")?)?;
    apply(&mut expected, 2, &send(3, "two", b"continued")?)?;
    assert_eq!(
        record(&machine.reader(), 2)?
            .ok_or("missing next recovered allocation")?
            .body,
        b"continued"
    );
    assert_eq!(record(&machine.reader(), 3)?, None);
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing next recovered counters")?
            .next_sequence,
        3
    );
    assert_eq!(machine.checkpoint()?, expected.checkpoint()?);
    assert_eq!(machine.reader().snapshot()?, expected.reader().snapshot()?);
    let expected_checkpoint = machine.checkpoint()?;
    let expected_snapshot = machine.reader().snapshot()?;
    drop(machine);

    let reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    assert_eq!(reopened.checkpoint()?, expected_checkpoint);
    assert_eq!(reopened.reader().snapshot()?, expected_snapshot);
    Ok(())
}

fn run_child(directory: &Path, stage: &str) -> TestResult<ExitStatus> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut child = ChildGuard(Some(child));
    let deadline = Instant::now() + DEADLINE;
    loop {
        let Some(process) = child.0.as_mut() else {
            return Err("committed test child is unavailable".into());
        };
        if let Some(status) = process.try_wait()? {
            child.0.take();
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("committed test child exceeded its deadline".into());
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
