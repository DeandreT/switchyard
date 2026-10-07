use std::{
    error::Error,
    fmt,
    future::Future,
    io,
    os::unix::process::{CommandExt, ExitStatusExt},
    panic::{AssertUnwindSafe, resume_unwind},
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::FutureExt;
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const RUN_DEADLINE: Duration = Duration::from_secs(20);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Spawn,
    ProcessGroup,
    Pipe,
    PrematureEof,
    OutputLimit,
    Reader,
    Body,
    Timeout,
    Cleanup,
    SensitiveOutput,
}

struct FailureError {
    stage: &'static str,
    failure: Failure,
    status: Option<ExitStatus>,
    capture_eof: usize,
    group_signal_success: Option<bool>,
    cleanup_failure: Option<Failure>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    original: Option<Box<dyn Error>>,
}

impl fmt::Display for FailureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "native CLI JWT gate failed: stage={} kind={:?} status_present={} status_success={:?} output_eof={} group_signal_success={:?} cleanup={:?}",
            self.stage,
            self.failure,
            self.status.is_some(),
            self.status.map(|status| status.success()),
            self.capture_eof,
            self.group_signal_success,
            self.cleanup_failure,
        )
    }
}

impl fmt::Debug for FailureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for FailureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.original.as_deref()
    }
}

pub(super) fn at<T, E: Error + 'static>(
    stage: &'static str,
    result: Result<T, E>,
) -> TestResult<T> {
    result.map_err(|original| retained(stage, Box::new(original)))
}

pub(super) fn retained(stage: &'static str, original: Box<dyn Error>) -> Box<dyn Error> {
    Box::new(FailureError {
        stage,
        failure: Failure::Body,
        status: None,
        capture_eof: 0,
        group_signal_success: None,
        cleanup_failure: None,
        stdout: Vec::new(),
        stderr: Vec::new(),
        original: Some(original),
    })
}

#[derive(Default)]
struct Capture(Mutex<Vec<u8>>);

impl Capture {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().expect("bounded child capture").clone()
    }
}

fn capture(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    bytes: Arc<Capture>,
    events: mpsc::Sender<Result<(), Failure>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let observed = async {
            let mut buffer = [0; 4096];
            loop {
                let count = reader
                    .read(&mut buffer)
                    .await
                    .map_err(|_| Failure::Reader)?;
                if count == 0 {
                    return Ok(());
                }
                let mut retained = bytes.0.lock().map_err(|_| Failure::Reader)?;
                if count > MAX_OUTPUT_BYTES.saturating_sub(retained.len()) {
                    return Err(Failure::OutputLimit);
                }
                retained.extend_from_slice(&buffer[..count]);
            }
        }
        .await;
        let _ = events.send(observed).await;
    })
}

struct OwnedChild {
    child: Child,
    group: Option<i32>,
    readers: Vec<JoinHandle<()>>,
    completed: mpsc::Receiver<Result<(), Failure>>,
    stdout: Arc<Capture>,
    stderr: Arc<Capture>,
    capture_eof: usize,
    reaped: bool,
    group_signal_success: Option<bool>,
    cleanup_failure: Option<Failure>,
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.start_kill();
        }
        for reader in &self.readers {
            reader.abort();
        }
    }
}

impl OwnedChild {
    fn start(mut command: Command) -> TestResult<Self> {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .as_std_mut()
            .process_group(0);
        let mut child = command.spawn().map_err(|error| {
            Box::new(FailureError {
                stage: "spawn",
                failure: Failure::Spawn,
                status: None,
                capture_eof: 0,
                group_signal_success: None,
                cleanup_failure: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                original: Some(Box::new(error)),
            }) as Box<dyn Error>
        })?;
        let group = child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .filter(|pid| *pid > 1);
        let stdout = Arc::new(Capture::default());
        let stderr = Arc::new(Capture::default());
        let (events, completed) = mpsc::channel(2);
        let mut readers = Vec::with_capacity(2);
        if let Some(pipe) = child.stdout.take() {
            readers.push(capture(pipe, stdout.clone(), events.clone()));
        }
        if let Some(pipe) = child.stderr.take() {
            readers.push(capture(pipe, stderr.clone(), events));
        }
        Ok(Self {
            child,
            group,
            readers,
            completed,
            stdout,
            stderr,
            capture_eof: 0,
            reaped: false,
            group_signal_success: None,
            cleanup_failure: None,
        })
    }

    async fn stop(&mut self) -> (Option<ExitStatus>, Option<Failure>) {
        if self.reaped {
            self.cleanup_failure = Some(Failure::Cleanup);
            return (None, self.cleanup_failure);
        }
        let mut failure = None;
        if let Some(group) = self
            .group
            .filter(|group| *group > 1)
            .and_then(Pid::from_raw)
        {
            // Signal synchronously before reaping the original leader. Its unreaped
            // PID reserves this exact group identifier; PID 1 is never permitted.
            let signalled = kill_process_group(group, Signal::KILL).is_ok();
            self.group_signal_success = Some(signalled);
            if !signalled {
                failure = Some(Failure::Cleanup);
            }
        } else {
            failure = Some(Failure::ProcessGroup);
        }
        let _ = self.child.start_kill();
        let status = match timeout(CLEANUP_DEADLINE, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.reaped = true;
                Some(status)
            }
            _ => {
                failure.get_or_insert(Failure::Cleanup);
                None
            }
        };
        for reader in &mut self.readers {
            match timeout(CLEANUP_DEADLINE, &mut *reader).await {
                Ok(Ok(())) => (),
                Ok(Err(_)) => {
                    failure.get_or_insert(Failure::Reader);
                }
                Err(_) => {
                    reader.abort();
                    let _ = (&mut *reader).await;
                    failure.get_or_insert(Failure::Cleanup);
                }
            }
        }
        while let Ok(event) = self.completed.try_recv() {
            match event {
                Ok(()) => self.capture_eof += 1,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if self.capture_eof != 2 || !status.is_some_and(|status| status.signal() == Some(9)) {
            failure.get_or_insert(Failure::Cleanup);
        }
        self.cleanup_failure = failure;
        (status, failure)
    }

    fn error(
        &self,
        failure: Failure,
        status: Option<ExitStatus>,
        original: Option<Box<dyn Error>>,
    ) -> Box<dyn Error> {
        Box::new(FailureError {
            stage: "owned-child",
            failure,
            status,
            capture_eof: self.capture_eof,
            group_signal_success: self.group_signal_success,
            cleanup_failure: self.cleanup_failure,
            stdout: self.stdout.bytes(),
            stderr: self.stderr.bytes(),
            original,
        })
    }
}

pub(super) async fn run(
    command: Command,
    sensitive: &[&str],
    body: impl Future<Output = TestResult>,
) -> TestResult {
    let mut owned = OwnedChild::start(command)?;
    let body = AssertUnwindSafe(timeout(RUN_DEADLINE, body)).catch_unwind();
    tokio::pin!(body);
    let mut fault = if owned.group.is_none() {
        Some(Failure::ProcessGroup)
    } else if owned.readers.len() != 2 {
        Some(Failure::Pipe)
    } else {
        None
    };
    let observed = if fault.is_some() {
        None
    } else {
        tokio::select! {
            result = &mut body => Some(result),
            event = owned.completed.recv() => {
                fault = Some(match event {
                    Some(Ok(())) => {
                        owned.capture_eof += 1;
                        Failure::PrematureEof
                    }
                    Some(Err(error)) => error,
                    None => Failure::Reader,
                });
                None
            }
        }
    };
    let (status, cleanup) = owned.stop().await;
    let stdout = owned.stdout.bytes();
    let stderr = owned.stderr.bytes();
    let sensitive_output = sensitive.iter().any(|needle| {
        !needle.is_empty()
            && [stdout.as_slice(), stderr.as_slice()].iter().any(|bytes| {
                bytes
                    .windows(needle.len())
                    .any(|window| window == needle.as_bytes())
            })
    });
    if let Some(Err(payload)) = observed {
        resume_unwind(payload);
    }
    if let Some(failure) = fault {
        return Err(owned.error(failure, status, None));
    }
    match observed {
        Some(Ok(Ok(Err(original)))) => {
            return Err(owned.error(Failure::Body, status, Some(original)));
        }
        Some(Ok(Err(original))) => {
            return Err(owned.error(Failure::Timeout, status, Some(Box::new(original))));
        }
        Some(Ok(Ok(Ok(())))) => (),
        _ => return Err(owned.error(Failure::Body, status, None)),
    }
    if sensitive_output {
        return Err(owned.error(Failure::SensitiveOutput, status, None));
    }
    if let Some(failure) = cleanup {
        return Err(owned.error(failure, status, None));
    }
    Ok(())
}

#[test]
fn child_failure_diagnostics_redact_original_errors_and_owned_output() {
    let error = FailureError {
        stage: "owned-child",
        failure: Failure::Body,
        status: None,
        capture_eof: 1,
        group_signal_success: Some(true),
        cleanup_failure: None,
        stdout: b"sentinel-secret-stdout".to_vec(),
        stderr: b"sentinel-secret-stderr".to_vec(),
        original: Some(Box::new(io::Error::other("sentinel-secret-original"))),
    };
    assert!(!format!("{error}").contains("sentinel"));
    assert!(!format!("{error:?}").contains("sentinel"));
    assert_eq!(error.stdout, b"sentinel-secret-stdout");
    assert_eq!(error.stderr, b"sentinel-secret-stderr");
    assert_eq!(
        error.source().unwrap().to_string(),
        "sentinel-secret-original"
    );
}
