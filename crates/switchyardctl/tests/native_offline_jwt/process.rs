use std::{
    error::Error,
    fmt, io,
    os::unix::process::CommandExt,
    process::{ExitStatus, Output, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use rustix::process::{Pid, Signal, kill_process_group};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const RUN_DEADLINE: Duration = Duration::from_secs(20);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    ProcessGroup,
    Pipe,
    Reader,
    Timeout,
    Wait,
    Cleanup,
    SensitiveOutput,
}

struct Diagnostic {
    stage: &'static str,
    failure: Option<Failure>,
    status: Option<ExitStatus>,
    output_eof: usize,
    group_signal_success: Option<bool>,
    cleanup_failure: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    original: Option<Box<dyn Error>>,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "native JWT CLI gate failed: stage={} kind={:?} status_present={} status_success={:?} output_eof={} group_signal_success={:?} cleanup_failure={}",
            self.stage,
            self.failure,
            self.status.is_some(),
            self.status.map(|status| status.success()),
            self.output_eof,
            self.group_signal_success,
            self.cleanup_failure,
        )
    }
}

impl fmt::Debug for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for Diagnostic {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.original.as_deref()
    }
}

pub(super) fn at<T, E: Error + 'static>(
    stage: &'static str,
    result: Result<T, E>,
) -> TestResult<T> {
    result.map_err(|error| retained(stage, Box::new(error)))
}

pub(super) fn retained(stage: &'static str, original: Box<dyn Error>) -> Box<dyn Error> {
    Box::new(Diagnostic {
        stage,
        failure: None,
        status: None,
        output_eof: 0,
        group_signal_success: None,
        cleanup_failure: false,
        stdout: Vec::new(),
        stderr: Vec::new(),
        original: Some(original),
    })
}

#[derive(Default)]
struct Capture(Mutex<Vec<u8>>);

impl Capture {
    fn bytes(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

fn capture(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    captured: Arc<Capture>,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        let mut buffer = [0; 4096];
        loop {
            let count = reader.read(&mut buffer).await?;
            if count == 0 {
                return Ok(());
            }
            let mut bytes = captured
                .0
                .lock()
                .map_err(|_| io::Error::other("capture lock"))?;
            if count > MAX_OUTPUT_BYTES.saturating_sub(bytes.len()) {
                return Err(io::Error::other("capture limit"));
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
    })
}

struct OwnedCli {
    child: Child,
    group: Option<Pid>,
    readers: Vec<JoinHandle<io::Result<()>>>,
    stdout: Arc<Capture>,
    stderr: Arc<Capture>,
    output_eof: usize,
    readers_drained: usize,
    reaped: bool,
    group_signal_success: Option<bool>,
}

impl OwnedCli {
    fn spawn(mut command: Command) -> TestResult<Self> {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .as_std_mut()
            .process_group(0);
        let mut child = at("spawn", command.spawn())?;
        let group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .filter(|id| *id > 1)
            .and_then(Pid::from_raw);
        let stdout = Arc::new(Capture::default());
        let stderr = Arc::new(Capture::default());
        let mut readers = Vec::with_capacity(2);
        if let Some(reader) = child.stdout.take() {
            readers.push(capture(reader, stdout.clone()));
        }
        if let Some(reader) = child.stderr.take() {
            readers.push(capture(reader, stderr.clone()));
        }
        Ok(Self {
            child,
            group,
            readers,
            stdout,
            stderr,
            output_eof: 0,
            readers_drained: 0,
            reaped: false,
            group_signal_success: None,
        })
    }

    fn signal_group(&mut self) -> Result<(), Failure> {
        let group = self.group.ok_or(Failure::ProcessGroup)?;
        let original = self
            .child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .and_then(Pid::from_raw);
        if self.reaped || original != Some(group) || group == Pid::INIT {
            return Err(Failure::ProcessGroup);
        }
        let result = kill_process_group(group, Signal::KILL);
        self.group_signal_success = Some(result.is_ok());
        result.map_err(|_| Failure::Cleanup)
    }

    async fn collect(&mut self) -> Result<(), Failure> {
        for reader in &mut self.readers {
            let observed = (&mut *reader).await;
            self.readers_drained += 1;
            match observed {
                Ok(Ok(())) => self.output_eof += 1,
                _ => return Err(Failure::Reader),
            }
        }
        Ok(())
    }

    async fn wait(&mut self) -> Result<ExitStatus, Failure> {
        match timeout(CLEANUP_DEADLINE, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.reaped = true;
                Ok(status)
            }
            _ => Err(Failure::Wait),
        }
    }

    async fn finish_readers(&mut self) -> bool {
        let mut failed = false;
        // Already collected readers, including failures, must not be polled twice.
        for reader in &mut self.readers[self.readers_drained..] {
            match timeout(CLEANUP_DEADLINE, &mut *reader).await {
                Ok(Ok(Ok(()))) => self.output_eof += 1,
                Ok(_) => failed = true,
                Err(_) => {
                    reader.abort();
                    let _ = (&mut *reader).await;
                    failed = true;
                }
            }
            self.readers_drained += 1;
        }
        failed || self.output_eof != 2
    }

    fn diagnostic(
        &self,
        failure: Failure,
        status: Option<ExitStatus>,
        cleanup_failure: bool,
    ) -> Box<dyn Error> {
        Box::new(Diagnostic {
            stage: "owned-cli",
            failure: Some(failure),
            status,
            output_eof: self.output_eof,
            group_signal_success: self.group_signal_success,
            cleanup_failure,
            stdout: self.stdout.bytes(),
            stderr: self.stderr.bytes(),
            original: None,
        })
    }
}

impl Drop for OwnedCli {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.signal_group();
            let _ = self.child.start_kill();
        }
        for reader in &self.readers {
            reader.abort();
        }
    }
}

pub(super) async fn run(command: Command, sensitive: &[&str]) -> TestResult<Output> {
    run_with_deadline(command, sensitive, RUN_DEADLINE).await
}

async fn run_with_deadline(
    command: Command,
    sensitive: &[&str],
    deadline: Duration,
) -> TestResult<Output> {
    let mut owned = OwnedCli::spawn(command)?;
    let mut failure = if owned.group.is_none() {
        Some(Failure::ProcessGroup)
    } else if owned.readers.len() != 2 {
        Some(Failure::Pipe)
    } else {
        None
    };
    if failure.is_none() {
        failure = match timeout(deadline, owned.collect()).await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some(Failure::Timeout),
        };
    }
    // Read both streams to actual EOF before the first wait can reap the leader.
    // On a fault, synchronously signal that still-reserved group first.
    let mut cleanup_failure = false;
    if failure.is_some() {
        cleanup_failure = owned.signal_group().is_err();
        let _ = owned.child.start_kill();
    }
    let mut status = match owned.wait().await {
        Ok(status) => Some(status),
        Err(error) => {
            failure.get_or_insert(error);
            None
        }
    };
    if status.is_none() {
        cleanup_failure |= owned.signal_group().is_err();
        let _ = owned.child.start_kill();
        status = owned.wait().await.ok();
        cleanup_failure |= status.is_none();
    }
    cleanup_failure |= owned.finish_readers().await;
    let stdout = owned.stdout.bytes();
    let stderr = owned.stderr.bytes();
    if sensitive
        .iter()
        .filter(|needle| !needle.is_empty())
        .any(|needle| {
            [stdout.as_slice(), stderr.as_slice()].iter().any(|bytes| {
                bytes
                    .windows(needle.len())
                    .any(|window| window == needle.as_bytes())
            })
        })
    {
        failure.get_or_insert(Failure::SensitiveOutput);
    }
    if cleanup_failure {
        failure.get_or_insert(Failure::Cleanup);
    }
    if let Some(failure) = failure {
        return Err(owned.diagnostic(failure, status, cleanup_failure));
    }
    Ok(Output {
        status: status.ok_or_else(|| owned.diagnostic(Failure::Wait, status, true))?,
        stdout,
        stderr,
    })
}

#[test]
fn child_diagnostics_retain_evidence_without_disclosing_credentials() {
    let error = Diagnostic {
        stage: "owned-cli",
        failure: Some(Failure::Reader),
        status: None,
        output_eof: 0,
        group_signal_success: None,
        cleanup_failure: false,
        stdout: b"sentinel-secret-stdout".to_vec(),
        stderr: b"sentinel-secret-stderr".to_vec(),
        original: Some(Box::new(io::Error::other("sentinel-secret-original"))),
    };
    assert!(!format!("{error}").contains("sentinel"));
    assert!(!format!("{error:?}").contains("sentinel"));
    assert_eq!(
        error.source().unwrap().to_string(),
        "sentinel-secret-original"
    );
    assert_eq!(error.stdout, b"sentinel-secret-stdout");
    assert_eq!(error.stderr, b"sentinel-secret-stderr");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_signals_the_unreaped_group_then_waits_and_observes_both_eofs() {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("printf retained-stdout; printf retained-stderr >&2; sleep 60 &");
    let error = run_with_deadline(command, &[], Duration::from_secs(1))
        .await
        .unwrap_err();
    let retained = error
        .downcast_ref::<Diagnostic>()
        .expect("the original owned child diagnostic");
    assert_eq!(retained.failure, Some(Failure::Timeout));
    assert!(retained.status.is_some_and(|status| status.success()));
    assert_eq!(retained.output_eof, 2);
    assert_eq!(retained.group_signal_success, Some(true));
    assert!(!retained.cleanup_failure);
    assert_eq!(retained.stdout, b"retained-stdout");
    assert_eq!(retained.stderr, b"retained-stderr");
}
