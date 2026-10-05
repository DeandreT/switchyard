use std::{
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use super::*;

const DIRECTORY: &str = "SWITCHYARD_PAIRED_FIXTURE_DIRECTORY";
const STAGE: &str = "SWITCHYARD_PAIRED_FIXTURE_STAGE";
const CHILD_TEST: &str = "durable::paired_prototype::tests::crash::child_process_exit_boundary";
const CASES: [(&str, CreationStep, bool); 6] = [
    ("log-before", CreationStep::LogPrepared, false),
    ("log-after", CreationStep::LogPrepared, true),
    ("state-before", CreationStep::StateReady, false),
    ("state-after", CreationStep::StateReady, true),
    ("ready-before", CreationStep::LogReady, false),
    ("ready-after", CreationStep::LogReady, true),
];

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(parent) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let stage = std::env::var(STAGE)?;
    let (_locations, mut pair) = native(std::path::Path::new(&parent))?;
    if let Some((_, step, after)) = CASES.iter().find(|(name, _, _)| *name == stage) {
        let fault = if *after {
            CommitFault::AfterSyncErr
        } else {
            CommitFault::BeforeBackendErr
        };
        assert_eq!(pair.create(Some((*step, fault))), Err(Error::CommitUnknown));
        // No capsule/database/keyspace destructor after the selected real-call boundary.
        std::process::exit(if *after { 122 } else { 121 });
    }
    if stage == "composite-before" || stage == "composite-after" {
        pair.create(None)?;
        let after = stage == "composite-after";
        let fault = if after {
            CommitFault::AfterSyncErr
        } else {
            CommitFault::BeforeBackendErr
        };
        assert_eq!(
            pair.composite(&composite_parts(), fault),
            Err(Error::CommitUnknown)
        );
        std::process::exit(if after { 122 } else { 121 });
    }
    Err("unknown paired fixture child boundary".into())
}

fn run(parent: &std::path::Path, stage: &str) -> TestResult<ExitStatus> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(DIRECTORY, parent)
        .env(STAGE, stage)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut guard = ChildGuard(Some(child));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let child = guard.0.as_mut().ok_or("paired child unavailable")?;
        if let Some(status) = child.try_wait()? {
            guard.0.take();
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("paired child exceeded deadline".into());
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

#[test]
fn abrupt_creation_exits_before_and_after_each_sync_reopen_exact_retained_prefixes() -> TestResult {
    for (index, (stage, _, after)) in CASES.into_iter().enumerate() {
        let parent = tempfile::TempDir::new()?;
        assert_eq!(
            run(parent.path(), stage)?.code(),
            Some(if after { 122 } else { 121 })
        );
        // The exact child status was reachable only after BOTH successful acquisitions.
        let locations = ControlledLocations {
            paths: [parent.path().join("state"), parent.path().join("log")],
        };
        let value = twice(&locations)?;
        assert_prefix(&value, index / 2 + usize::from(after));
    }
    Ok(())
}

#[test]
fn abrupt_composite_exits_recover_whole_old_or_whole_selected_not_checkpoint_only() -> TestResult {
    for after in [false, true] {
        let parent = tempfile::TempDir::new()?;
        assert_eq!(
            run(
                parent.path(),
                if after {
                    "composite-after"
                } else {
                    "composite-before"
                }
            )?
            .code(),
            Some(if after { 122 } else { 121 })
        );
        let locations = ControlledLocations {
            paths: [parent.path().join("state"), parent.path().join("log")],
        };
        let value = twice(&locations)?;
        if after {
            assert_selected(&value);
        } else {
            assert_prefix(&value, 3);
        }
        assert_eq!(value.log.status, RoleStatus::Paired(CreationPhase::Ready));
    }
    Ok(())
}
