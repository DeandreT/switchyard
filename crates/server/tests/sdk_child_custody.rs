#![cfg(target_os = "linux")]

#[allow(dead_code)]
#[path = "sdk_child/mod.rs"]
mod sdk_child;

use rustix::{
    fd::OwnedFd,
    fs::{OFlags, fcntl_getfl, fcntl_setfl},
    process::{
        Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, pidfd_open, pidfd_send_signal, waitid,
    },
};
use sdk_child::{
    child::{self, Limits},
    identity::{self, Artifact, AssemblyIdentity, Record},
};
use std::{
    future::{Future, poll_fn},
    io,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    task::Poll,
    time::Duration,
};
use tokio::io::{Interest, unix::AsyncFd};

fn shell(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.arg("-c").arg(script).arg("custody-control");
    command
}

fn limits(deadline: Duration, stream_bytes: usize) -> Limits {
    Limits {
        deadline,
        stream_bytes,
    }
}

async fn pid_at(path: &Path) -> io::Result<Pid> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(path)
                && let Some(record) = text.strip_suffix('\n')
                && record.bytes().all(|byte| byte.is_ascii_digit())
                && let Ok(raw) = record.parse::<i32>()
                && let Some(pid) = Pid::from_raw(raw)
            {
                return Ok(pid);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("control child did not publish its original pid"))?
}

fn assert_reaped(pid: Pid) {
    assert!(
        matches!(
            waitid(
                WaitId::Pid(pid),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG
            ),
            Err(rustix::io::Errno::CHILD)
        ),
        "original child remains waitable"
    );
}

struct ProcessControl {
    _directory: tempfile::TempDir,
    leader: PathBuf,
    descendant: PathBuf,
    acknowledge: PathBuf,
    acknowledged: PathBuf,
    release: PathBuf,
}

impl ProcessControl {
    fn new() -> io::Result<Self> {
        let directory = tempfile::tempdir()?;
        Ok(Self {
            leader: directory.path().join("leader"),
            descendant: directory.path().join("descendant"),
            acknowledge: directory.path().join("acknowledge"),
            acknowledged: directory.path().join("acknowledged"),
            release: directory.path().join("release"),
            _directory: directory,
        })
    }

    fn command(&self, body: &str) -> Command {
        let mut command = shell(&format!(
            "printf '%s\\n' \"$$\" > \"$1\"; \
             /bin/sh -c 'printf \"%s\\n\" \"$$\" > \"$1\"; \
             while [ ! -e \"$2\" ]; do /bin/sleep 0.01; done; \
             printf acknowledged > \"$3\"; exec /bin/sleep 60' \
             descendant-control \"$2\" \"$3\" \"$4\" & \
             while [ ! -e \"$5\" ]; do /bin/sleep 0.01; done; {body}"
        ));
        command.args([
            &self.leader,
            &self.descendant,
            &self.acknowledge,
            &self.acknowledged,
            &self.release,
        ]);
        command
    }

    async fn witness(&self) -> io::Result<ProcessWitness> {
        let leader = pid_at(&self.leader).await?;
        let descendant = pid_at(&self.descendant).await?;
        let witness = ProcessWitness {
            leader_pid: leader,
            leader: pidfd_open(leader, PidfdFlags::empty())?,
            descendant: {
                let fd = pidfd_open(descendant, PidfdFlags::empty())?;
                fcntl_setfl(&fd, fcntl_getfl(&fd)? | OFlags::NONBLOCK)?;
                AsyncFd::with_interest(fd, Interest::READABLE)?
            },
        };
        // The exact descendant acknowledges only after its identity fd has been captured.
        std::fs::write(&self.acknowledge, b"acknowledge")?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if std::fs::read(&self.acknowledged).ok().as_deref()
                    == Some(b"acknowledged".as_slice())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("captured descendant did not acknowledge"))?;
        Ok(witness)
    }

    fn release(&self) -> io::Result<()> {
        std::fs::write(&self.release, b"release")
    }
}

struct ProcessWitness {
    leader_pid: Pid,
    leader: OwnedFd,
    descendant: AsyncFd<OwnedFd>,
}

impl ProcessWitness {
    async fn assert_descendant_exited(&self) -> io::Result<()> {
        let ready = tokio::time::timeout(Duration::from_secs(5), self.descendant.readable())
            .await
            .map_err(|_| io::Error::other("exact same-group descendant remains alive"))??;
        assert!(ready.ready().is_readable() || ready.ready().is_read_closed());
        Ok(())
    }
}

impl Drop for ProcessWitness {
    fn drop(&mut self) {
        // Failure cleanup only; the positive exit assertions run before this fallback.
        let _ = pidfd_send_signal(&self.leader, Signal::KILL);
        let _ = pidfd_send_signal(self.descendant.get_ref(), Signal::KILL);
    }
}

struct Watchdog {
    state: Arc<AtomicU8>,
    stop: Option<mpsc::Sender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl Watchdog {
    fn new(witness: &ProcessWitness) -> io::Result<Self> {
        let leader = witness.leader.try_clone()?;
        let descendant = witness.descendant.get_ref().try_clone()?;
        let state = Arc::new(AtomicU8::new(0));
        let thread_state = Arc::clone(&state);
        let (stop, stopped) = mpsc::channel();
        let task = std::thread::Builder::new()
            .name("sdk-control-watchdog".to_owned())
            .spawn(move || {
                if matches!(
                    stopped.recv_timeout(Duration::from_secs(5)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) && thread_state
                    .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let _ = pidfd_send_signal(&leader, Signal::KILL);
                    let _ = pidfd_send_signal(&descendant, Signal::KILL);
                }
            })?;
        Ok(Self {
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }

    fn stop_and_join(&mut self) -> io::Result<()> {
        let _ = self
            .state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.join()
                .map_err(|_| io::Error::other("process control watchdog panicked"))?;
        }
        if self.state.load(Ordering::Acquire) == 2 {
            return Err(io::Error::other(
                "process control required watchdog cleanup",
            ));
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.stop_and_join()
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        if self.task.is_some() {
            let _ = self.stop_and_join();
        }
    }
}

#[test]
fn cleanup_state_retries_never_resignal_a_potentially_reaped_group() {
    child::assert_wait_retry_never_resignals();
}

#[tokio::test]
async fn child_drains_both_streams_and_preserves_the_original_exit() -> io::Result<()> {
    let output = child::run(
        shell("printf stdout; printf stderr >&2; exit 23"),
        limits(Duration::from_secs(5), 128),
    )
    .await?;
    assert_eq!(output.status.code(), Some(23));
    assert_eq!(output.stdout, b"stdout");
    assert_eq!(output.stderr, b"stderr");
    Ok(())
}

#[tokio::test]
async fn child_missing_executable_fails_instead_of_skipping() {
    let missing = tempfile::tempdir().expect("temporary directory");
    let error = child::run(
        Command::new(missing.path().join("absent-dotnet")),
        limits(Duration::from_secs(5), 128),
    )
    .await
    .expect_err("missing executable must fail");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[tokio::test]
async fn child_deadline_kills_and_reaps_the_original() -> io::Result<()> {
    let control = ProcessControl::new()?;
    let mut original = Box::pin(child::run(
        control.command("exec /bin/sleep 60"),
        limits(Duration::from_millis(150), 128),
    ));
    poll_fn(|cx| {
        assert!(original.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let witness = control.witness().await?;
    let watchdog = Watchdog::new(&witness)?;
    control.release()?;
    let error = original.await.expect_err("deadline must fail");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_reaped(witness.leader_pid);
    witness.assert_descendant_exited().await?;
    watchdog.finish()?;
    Ok(())
}

#[tokio::test]
async fn child_output_overflow_kills_and_reaps_each_stream() -> io::Result<()> {
    for redirect in ["", ">&2"] {
        let control = ProcessControl::new()?;
        let command = control.command(&format!(
            "while :; do printf 01234567890123456789 {redirect}; done"
        ));
        let mut original = Box::pin(child::run(command, limits(Duration::from_secs(5), 128)));
        poll_fn(|cx| {
            assert!(original.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let witness = control.witness().await?;
        let watchdog = Watchdog::new(&witness)?;
        control.release()?;
        let error = original.await.expect_err("output ceiling must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_reaped(witness.leader_pid);
        witness.assert_descendant_exited().await?;
        watchdog.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_original_observation_kills_and_reaps_without_a_detached_reader() -> io::Result<()>
{
    let control = ProcessControl::new()?;
    let mut original = Box::pin(child::run(
        control.command("exec /bin/sleep 60"),
        limits(Duration::from_secs(5), 128),
    ));
    poll_fn(|cx| {
        assert!(original.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let witness = control.witness().await?;
    let watchdog = Watchdog::new(&witness)?;
    control.release()?;
    drop(original);
    assert_reaped(witness.leader_pid);
    witness.assert_descendant_exited().await?;
    watchdog.finish()?;
    Ok(())
}

#[tokio::test]
async fn exited_leader_does_not_leave_a_same_group_descendant_holding_the_pipes() -> io::Result<()>
{
    let control = ProcessControl::new()?;
    let mut original = Box::pin(child::run(
        control.command("printf descendant-started; exit 0"),
        limits(Duration::from_secs(3), 128),
    ));
    poll_fn(|cx| {
        assert!(original.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let witness = control.witness().await?;
    let watchdog = Watchdog::new(&witness)?;
    control.release()?;
    let output = tokio::time::timeout(Duration::from_secs(5), original)
        .await
        .map_err(|_| io::Error::other("same-group descendant retained an original pipe"))??;
    assert!(output.status.success());
    assert_eq!(output.stdout, b"descendant-started");
    assert!(output.stderr.is_empty());
    assert_reaped(witness.leader_pid);
    witness.assert_descendant_exited().await?;
    watchdog.finish()?;
    Ok(())
}

#[tokio::test]
async fn cancelled_borrower_can_retry_the_same_original_child() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("pid");
    let release = directory.path().join("release");
    let mut command = shell(
        "printf '%s\\n' \"$$\" > \"$1\"; while [ ! -e \"$2\" ]; do sleep 0.01; done; printf 'original-result:%s' \"$$\"",
    );
    command.arg(&path).arg(&release);
    let mut original = Box::pin(child::run(command, limits(Duration::from_secs(5), 128)));
    {
        let mut borrowed = original.as_mut();
        poll_fn(|cx| {
            assert!(borrowed.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    let pid = pid_at(&path).await?;
    std::fs::write(&release, b"release")?;
    let output = original.await?;
    assert!(output.status.success());
    assert_eq!(output.stdout, format!("original-result:{pid}").as_bytes());
    assert_reaped(pid);
    Ok(())
}

fn custody_fixture() -> io::Result<(tempfile::TempDir, Vec<Artifact>, Record)> {
    let directory = tempfile::tempdir()?;
    let mut artifacts = Vec::new();
    for (role, name) in [
        ("entry", "Control"),
        ("service_bus", "Azure.Messaging.ServiceBus"),
        ("core", "Azure.Core"),
    ] {
        let path = directory.path().join(format!("{name}.dll"));
        std::fs::write(&path, role)?;
        artifacts.push(Artifact::read(role, &path)?);
    }
    let assemblies = artifacts
        .iter()
        .map(|artifact| AssemblyIdentity {
            role: artifact.role.to_owned(),
            full_name: format!(
                "{}, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null",
                artifact.simple_name
            ),
            informational_version: "fixture-only-not-a-package-pin".to_owned(),
            location: artifact.path.to_string_lossy().into_owned(),
            sha256: artifact.sha256.clone(),
        })
        .collect();
    Ok((
        directory,
        artifacts,
        Record {
            schema: 1,
            kind: "start".to_owned(),
            nonce: "a".repeat(64),
            assemblies,
        },
    ))
}

fn records(start: &Record) -> Vec<u8> {
    let mut complete = start.clone();
    complete.kind = "complete".to_owned();
    format!(
        "{}{}\nworkflow marker\n{}{}\n",
        identity::PREFIX,
        serde_json::to_string(start).unwrap(),
        identity::PREFIX,
        serde_json::to_string(&complete).unwrap()
    )
    .into_bytes()
}

#[test]
fn identity_parser_accepts_only_two_bound_matching_artifact_observations() -> io::Result<()> {
    let (_directory, artifacts, start) = custody_fixture()?;
    let verified = identity::verify(&records(&start), &start.nonce, &artifacts)?;
    assert_eq!(verified.len(), 2);
    assert_eq!(verified[0], start);
    assert_eq!(verified[1].kind, "complete");
    Ok(())
}

#[test]
fn identity_parser_refuses_missing_duplicate_wrong_and_oversized_records() -> io::Result<()> {
    let (_directory, artifacts, start) = custody_fixture()?;
    let valid = records(&start);
    assert!(identity::verify(b"workflow marker only\n", &start.nonce, &artifacts).is_err());
    let mut duplicate = valid.clone();
    duplicate.extend_from_slice(&valid);
    assert!(identity::verify(&duplicate, &start.nonce, &artifacts).is_err());
    assert!(identity::verify(&valid, &"b".repeat(64), &artifacts).is_err());
    assert!(
        identity::verify(
            &valid[..valid.iter().position(|byte| *byte == b'\n').unwrap() + 1],
            &start.nonce,
            &artifacts
        )
        .is_err()
    );
    for mutate in 0..9 {
        let mut changed = start.clone();
        match mutate {
            0 => changed.schema = 2,
            1 => changed.kind = "complete".to_owned(),
            2 => changed.assemblies.swap(0, 1),
            3 => changed.assemblies[1].sha256 = "b".repeat(64),
            4 => changed.assemblies[1].full_name = "Wrong, Version=1.0.0.0".to_owned(),
            5 => changed.assemblies[1].informational_version.clear(),
            6 => changed.assemblies[1].location = artifacts[0].path.to_string_lossy().into_owned(),
            7 => changed.assemblies[1].informational_version = "x".repeat(1025),
            8 => changed.assemblies[1].location = "x".repeat(identity::RECORD_BYTES),
            _ => unreachable!(),
        }
        assert!(
            identity::verify(&records(&changed), &start.nonce, &artifacts).is_err(),
            "accepted changed identity field {mutate}"
        );
    }
    let mut duplicate_field = String::from_utf8(valid.clone()).unwrap();
    duplicate_field = duplicate_field.replacen("\"schema\":1", "\"schema\":1,\"schema\":1", 1);
    assert!(identity::verify(duplicate_field.as_bytes(), &start.nonce, &artifacts).is_err());
    let mut unknown_field = String::from_utf8(valid).unwrap();
    unknown_field = unknown_field.replacen("\"schema\":1", "\"schema\":1,\"unknown\":true", 1);
    assert!(identity::verify(unknown_field.as_bytes(), &start.nonce, &artifacts).is_err());
    Ok(())
}

#[test]
fn identity_parser_refuses_artifact_mutation_after_launch() -> io::Result<()> {
    let (_directory, artifacts, start) = custody_fixture()?;
    std::fs::write(&artifacts[1].path, b"replaced after launch")?;
    assert!(identity::verify(&records(&start), &start.nonce, &artifacts).is_err());
    Ok(())
}

#[test]
fn identity_parser_requires_lf_records_and_refuses_truncated_duplicate_tails() -> io::Result<()> {
    let (_directory, artifacts, start) = custody_fixture()?;
    let valid = records(&start);
    assert!(identity::verify(&valid[..valid.len() - 1], &start.nonce, &artifacts).is_err());
    let mut unterminated_duplicate = valid.clone();
    unterminated_duplicate
        .extend_from_slice(&valid[..valid.iter().position(|byte| *byte == b'\n').unwrap()]);
    assert!(identity::verify(&unterminated_duplicate, &start.nonce, &artifacts).is_err());
    for tail in [
        identity::PREFIX,
        &identity::PREFIX[..identity::PREFIX.len() - 1],
        &identity::PREFIX[..1],
        "SWITCHYARD_SDK_CUSTODY {\"schema\":",
    ] {
        let mut truncated = valid.clone();
        truncated.extend_from_slice(tail.as_bytes());
        assert!(identity::verify(&truncated, &start.nonce, &artifacts).is_err());
    }
    let crlf = String::from_utf8(valid.clone())
        .unwrap()
        .replace('\n', "\r\n");
    assert_eq!(
        identity::verify(crlf.as_bytes(), &start.nonce, &artifacts)?.len(),
        2
    );
    let mut ordinary_tail = valid;
    ordinary_tail.extend_from_slice(b"ordinary workflow text without a final newline");
    assert_eq!(
        identity::verify(&ordinary_tail, &start.nonce, &artifacts)?.len(),
        2
    );
    Ok(())
}
