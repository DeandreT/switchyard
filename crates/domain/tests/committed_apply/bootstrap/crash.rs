use std::{
    collections::VecDeque,
    io::{self, Read},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use domain::{CommittedApplyError, CommittedImageBootstrapError, CommittedStateMachine};
use storage::{CommittedStore, FjallReplicaStore, StateStore};

use super::{
    TestResult,
    observed::{Fault, observed},
    request, source, stream,
};

const DIRECTORY: &str = "SWITCHYARD_BOOTSTRAP_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_BOOTSTRAP_EXIT_STAGE";
const CHILD_TEST: &str = "bootstrap::crash::child_process_exit_boundary";
const DEADLINE: Duration = Duration::from_secs(30);
const OUTPUT_BYTES: usize = 16 * 1024;

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let fault = match std::env::var(STAGE)?.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown bootstrap child stage".into()),
    };
    let selected = source(false)?;
    let (writer, control) = observed(FjallReplicaStore::open(directory)?);
    control.fault(fault);
    CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected))?;
    Err("the bootstrap process exit did not occur".into())
}

#[test]
fn exit_before_commit_reopens_exactly_pristine_without_a_checkpoint() -> TestResult {
    recover("before", 71, false)
}

#[test]
fn exit_after_sync_commit_reopens_the_complete_image_and_never_bootstraps_again() -> TestResult {
    recover("after", 72, true)
}

fn recover(stage: &str, code: i32, installed: bool) -> TestResult {
    let selected = source(false)?;
    let directory = testkit::DurableProvider::temporary()?;
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        assert!(!writer.is_initialized()?);
        assert!(writer.reader().snapshot()?.entries().is_empty());
    }
    let (status, output) = run_child(directory.path(), stage)?;
    assert_eq!(status.code(), Some(code), "bootstrap child: {output}");
    {
        let writer = FjallReplicaStore::open(directory.path())?;
        assert_eq!(writer.is_initialized()?, installed);
        if installed {
            assert_eq!(writer.reader().snapshot()?, selected.snapshot);
            assert_eq!(
                CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected))
                    .err(),
                Some(CommittedImageBootstrapError::TargetNotPristine)
            );
        } else {
            assert!(writer.reader().snapshot()?.entries().is_empty());
            assert_eq!(
                CommittedStateMachine::open(writer, stream()?).err(),
                Some(CommittedApplyError::NotInitialized)
            );
        }
    }
    // Every physical handle above was released before this second directory
    // open. Only after proving the actual pristine state may the before case
    // make a new explicitly selected bootstrap attempt.
    let writer = FjallReplicaStore::open(directory.path())?;
    let machine = if installed {
        CommittedStateMachine::open(writer, stream()?)?
    } else {
        CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected))?
    };
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    assert_eq!(machine.reader().snapshot()?, selected.snapshot);
    drop(machine);
    let reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    assert_eq!(reopened.checkpoint()?, selected.checkpoint);
    assert_eq!(reopened.reader().snapshot()?, selected.snapshot);
    Ok(())
}

struct Captured(Vec<u8>);

fn capture(mut pipe: impl Read) -> io::Result<Captured> {
    let mut tail = VecDeque::with_capacity(OUTPUT_BYTES);
    let mut chunk = [0; 4096];
    loop {
        let count = pipe.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        for byte in &chunk[..count] {
            if tail.len() == OUTPUT_BYTES {
                tail.pop_front();
            }
            tail.push_back(*byte);
        }
    }
    Ok(Captured(tail.into_iter().collect()))
}

struct ChildGuard {
    child: Option<Child>,
    stdout: Option<JoinHandle<io::Result<Captured>>>,
    stderr: Option<JoinHandle<io::Result<Captured>>>,
}

impl ChildGuard {
    fn reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn output(&mut self) -> TestResult<String> {
        let stdout = self
            .stdout
            .take()
            .ok_or("missing bootstrap stdout reader")?;
        let stderr = self
            .stderr
            .take()
            .ok_or("missing bootstrap stderr reader")?;
        let stdout = stdout
            .join()
            .map_err(|_| "bootstrap stdout reader panicked");
        let stderr = stderr
            .join()
            .map_err(|_| "bootstrap stderr reader panicked");
        let stdout = stdout??;
        let stderr = stderr??;
        Ok(format!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&stdout.0),
            String::from_utf8_lossy(&stderr.0)
        ))
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.reap();
        if let Some(reader) = self.stdout.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr.take() {
            let _ = reader.join();
        }
    }
}

fn run_child(directory: &Path, stage: &str) -> TestResult<(ExitStatus, String)> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1", "--nocapture"])
        .env(DIRECTORY, directory)
        .env(STAGE, stage)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut child = ChildGuard {
        child: Some(child),
        stdout: None,
        stderr: None,
    };
    let stdout = child
        .child
        .as_mut()
        .ok_or("missing bootstrap child")?
        .stdout
        .take()
        .ok_or("missing bootstrap stdout")?;
    child.stdout = Some(thread::spawn(move || capture(stdout)));
    let stderr = child
        .child
        .as_mut()
        .ok_or("missing bootstrap child")?
        .stderr
        .take()
        .ok_or("missing bootstrap stderr")?;
    child.stderr = Some(thread::spawn(move || capture(stderr)));
    let deadline = Instant::now() + DEADLINE;
    let status: TestResult<ExitStatus> = loop {
        match child
            .child
            .as_mut()
            .ok_or("missing bootstrap child")?
            .try_wait()
        {
            Ok(Some(status)) => {
                child.child.take();
                break Ok(status);
            }
            Err(error) => break Err(error.into()),
            Ok(None) => {}
        }
        if Instant::now() >= deadline {
            break Err("bootstrap child exceeded its deadline".into());
        }
        thread::sleep(Duration::from_millis(10));
    };
    if status.is_err() {
        child.reap();
    }
    let output = child.output();
    match status {
        Ok(status) => Ok((status, output?)),
        Err(error) => {
            let output =
                output.unwrap_or_else(|_| "bootstrap child diagnostics unavailable".into());
            Err(format!("{error}\n{output}").into())
        }
    }
}
