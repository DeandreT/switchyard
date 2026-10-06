use std::{
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use super::*;

const DIRECTORY: &str = "SWITCHYARD_PROTECTED_FIXTURE_PARENT";
const STAGE: &str = "SWITCHYARD_PROTECTED_FIXTURE_STAGE";
const CHILD_TEST: &str = "durable::protected_state::tests::crash::child_process_exit_boundary";

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(parent) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let stage = std::env::var(STAGE)?;
    let selected = match stage.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown protected child stage".into()),
    };
    let path = std::path::Path::new(&parent).join("selected");
    let mut writer = FjallProtectedStateStore::create_new(&path)?;
    token(&mut writer, 1)?;
    let reader = writer.reader();
    fault(&writer, selected);
    token(&mut writer, 2)?;
    // The selected fault exits while original writer/reader/entry handles live.
    drop(reader);
    Err("protected child failed to reach its exit boundary".into())
}

struct Observation {
    original: Child,
    polled: std::io::Result<Option<ExitStatus>>,
    timed_out: bool,
    kill: Option<std::io::Result<()>>,
    waited: std::io::Result<ExitStatus>,
}

fn run(parent: &std::path::Path, after: bool) -> TestResult<Observation> {
    let mut original = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(DIRECTORY, parent)
        .env(STAGE, if after { "after" } else { "before" })
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut timed_out = false;
    let polled = loop {
        match original.try_wait() {
            Ok(Some(status)) => break Ok(Some(status)),
            Err(error) => break Err(error),
            Ok(None) => {
                if Instant::now() >= deadline {
                    timed_out = true;
                    break Ok(None);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    };
    // Retain both original cleanup outcomes, even when kill itself fails.
    // Kernel cleanup can block; this deadline is not a hard reap guarantee.
    let kill = if matches!(&polled, Ok(Some(_))) {
        None
    } else {
        Some(original.kill())
    };
    let waited = original.wait();
    Ok(Observation {
        original,
        polled,
        timed_out,
        kill,
        waited,
    })
}

fn parent_case(after: bool) -> TestResult {
    let parent = tempfile::TempDir::new()?;
    let observed = run(parent.path(), after)?;
    // No assertion or Recover occurs before the original kill/wait attempts.
    // Recover requires actual successful reap, not Drop, timeout or a signal.
    assert!(!observed.timed_out);
    assert!(matches!(&observed.polled, Ok(Some(_))));
    assert!(observed.kill.is_none());
    assert!(observed.waited.is_ok());
    assert_eq!(
        observed.waited.as_ref().ok().and_then(ExitStatus::code),
        Some(if after { 72 } else { 71 })
    );
    assert!(observed.original.id() > 0);
    let (one, two) = twice(&parent.path().join("selected"))?;
    assert_token(&one, if after { 2 } else { 1 });
    assert_token(&two, if after { 2 } else { 1 });
    drop(observed);
    drop(parent);
    Ok(())
}

#[test]
fn abrupt_before_backend_exit_reopens_exact_old_generation() -> TestResult {
    parent_case(false)
}

#[test]
fn abrupt_after_sync_exit_reopens_exact_new_generation() -> TestResult {
    parent_case(true)
}
