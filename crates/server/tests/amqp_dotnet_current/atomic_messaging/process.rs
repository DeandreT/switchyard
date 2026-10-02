use std::{
    fmt, io,
    os::unix::process::CommandExt,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::mpsc,
    task::JoinHandle,
};

use super::*;

const BUILD_DEADLINE: Duration = Duration::from_secs(180);
const RUN_DEADLINE: Duration = Duration::from_secs(180);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const DIAGNOSTIC_TAIL_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub(crate) struct Output {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Nonzero,
    Timeout,
    OutputLimit,
    Reader,
    Wait,
    Cleanup,
}

#[derive(Debug)]
struct RunError {
    label: String,
    failure: Failure,
    status: Option<ExitStatus>,
    capture_eof: usize,
    stdout: String,
    stderr: String,
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failed: {:?}, status={:?}, output_eof={}\nstdout:\n{}\nstderr:\n{}",
            self.label, self.failure, self.status, self.capture_eof, self.stdout, self.stderr,
        )
    }
}
impl Error for RunError {}

#[derive(Default)]
struct Capture(Mutex<Vec<u8>>);

struct Readers(Vec<JoinHandle<()>>);

impl Drop for Readers {
    fn drop(&mut self) {
        for reader in &self.0 {
            reader.abort();
        }
    }
}

impl Capture {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("bounded child capture")).into_owned()
    }
}

fn capture(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    bytes: Arc<Capture>,
    events: mpsc::Sender<Result<(), Failure>>,
    maximum: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = async {
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
                if count > maximum.saturating_sub(retained.len()) {
                    let tail_limit = maximum.min(DIAGNOSTIC_TAIL_BYTES);
                    let old_tail = tail_limit.saturating_sub(count.min(tail_limit));
                    let discard = retained.len().saturating_sub(old_tail);
                    retained.drain(..discard);
                    retained.extend_from_slice(&buffer[count.saturating_sub(tail_limit)..count]);
                    return Err(Failure::OutputLimit);
                }
                retained.extend_from_slice(&buffer[..count]);
            }
        }
        .await;
        let _ = events.send(result).await;
    })
}

async fn run(
    mut command: Command,
    label: &str,
    deadline: Duration,
    maximum: usize,
) -> Result<Output, Box<dyn Error>> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .as_std_mut()
        .process_group(0);
    let mut child = command.spawn()?;
    let group = child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .filter(|pid| *pid > 1)
        .ok_or_else(|| io::Error::other("child process group cannot be represented"))?;
    let stdout = Arc::new(Capture::default());
    let stderr = Arc::new(Capture::default());
    let (events, mut completed) = mpsc::channel(2);
    let mut readers = Readers(vec![
        capture(
            child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("missing child stdout"))?,
            stdout.clone(),
            events.clone(),
            maximum,
        ),
        capture(
            child
                .stderr
                .take()
                .ok_or_else(|| io::Error::other("missing child stderr"))?,
            stderr.clone(),
            events,
            maximum,
        ),
    ]);
    let timeout = tokio::time::sleep(deadline);
    tokio::pin!(timeout);
    let mut failure = None;
    let mut capture_eof = 0;
    let mut status = loop {
        tokio::select! {
            result = child.wait(), if capture_eof == 2 => match result {
                Ok(status) => {
                    if !status.success() { failure = Some(Failure::Nonzero); }
                    break Some(status);
                }
                Err(_) => { failure = Some(Failure::Wait); break None; }
            },
            _ = &mut timeout => { failure = Some(Failure::Timeout); break None; },
            event = completed.recv(), if capture_eof < 2 => match event {
                Some(Err(error)) => { failure = Some(error); break None; }
                Some(Ok(())) => capture_eof += 1,
                None => { failure = Some(Failure::Reader); break None; }
            },
        }
    };
    if status.is_none() {
        // Do not reap the leader while a descendant may still hold its pipes.
        // Until this wait, the owned leader also reserves this exact group ID.
        let mut signal = Command::new("/bin/kill");
        signal
            .arg("-KILL")
            .arg("--")
            .arg(format!("-{group}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let _ = tokio::time::timeout(CLEANUP_DEADLINE, signal.status()).await;
        let _ = child.start_kill();
        match tokio::time::timeout(CLEANUP_DEADLINE, child.wait()).await {
            Ok(Ok(reaped)) => status = Some(reaped),
            _ => {
                failure.get_or_insert(Failure::Cleanup);
            }
        }
    }
    for reader in &mut readers.0 {
        match tokio::time::timeout(CLEANUP_DEADLINE, &mut *reader).await {
            Ok(Ok(())) => {}
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
    while let Ok(result) = completed.try_recv() {
        match result {
            Err(error) => {
                failure.get_or_insert(error);
            }
            Ok(()) => capture_eof += 1,
        }
    }
    let stdout = stdout.text();
    let stderr = stderr.text();
    if let Some(failure) = failure {
        return Err(Box::new(RunError {
            label: label.into(),
            failure,
            status,
            capture_eof,
            stdout,
            stderr,
        }));
    }
    Ok(Output {
        status: status.ok_or_else(|| io::Error::other("child finished without an exit status"))?,
        stdout,
        stderr,
    })
}

pub(super) async fn build_client(sdk_version: &str) -> TestResult<tempfile::TempDir> {
    // tempfile and the child inherit the gate's SSD-backed TMPDIR.
    let artifacts = tempfile::TempDir::new()?;
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj");
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .arg("build")
        .arg(project)
        .arg("--configuration")
        .arg("Release")
        .arg("--maxcpucount:2")
        .arg("--disable-build-servers")
        .arg("--output")
        .arg(artifacts.path().join("bin"))
        .arg(format!("-p:ServiceBusSdkVersion={sdk_version}"))
        .arg(format!(
            "-p:BaseIntermediateOutputPath={}/obj/",
            artifacts.path().display()
        ))
        .arg(format!(
            "-p:MSBuildProjectExtensionsPath={}/obj/",
            artifacts.path().display()
        ));
    run(
        command,
        "official transaction client build",
        BUILD_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await?;
    Ok(artifacts)
}

pub(super) async fn run_client(
    dll: &Path,
    atomic_endpoint: &str,
    ordinary_endpoint: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("atomic-messaging")
        .arg(HOST)
        .arg(atomic_endpoint)
        .arg(ordinary_endpoint)
        .arg(SEND_QUEUE)
        .arg(HELD_QUEUE)
        .arg(CONTROL_QUEUE)
        .arg(RULE)
        .arg(KEY);
    run(
        command,
        "official warmed transaction client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

pub(crate) async fn build_rule_action_client(sdk_version: &str) -> TestResult<tempfile::TempDir> {
    build_client(sdk_version).await
}

pub(crate) async fn run_rule_action_client(
    dll: &Path,
    endpoint: &str,
    topic: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("rule-actions")
        .arg(HOST)
        .arg(endpoint)
        .arg(topic)
        .arg(RULE)
        .arg(KEY);
    run(
        command,
        "official SQL REMOVE action client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    #[tokio::test]
    async fn nonzero_exit_is_reaped_and_preserves_output() {
        let error = run(
            shell("printf failure-output; exit 7"),
            "nonzero",
            Duration::from_secs(3),
            1024,
        )
        .await
        .expect_err("nonzero must fail");
        let error = error
            .downcast_ref::<RunError>()
            .expect("typed runner error");
        assert_eq!(error.failure, Failure::Nonzero);
        assert_eq!(error.status.expect("child reaped").code(), Some(7));
        assert_eq!(error.stdout, "failure-output");
    }

    #[tokio::test]
    async fn timed_out_child_is_killed_and_reaped() {
        let error = run(
            shell("exec sleep 30"),
            "timeout",
            Duration::from_millis(50),
            1024,
        )
        .await
        .expect_err("timeout must fail");
        let error = error
            .downcast_ref::<RunError>()
            .expect("typed runner error");
        assert_eq!(error.failure, Failure::Timeout);
        assert!(!error.status.expect("killed child reaped").success());
    }

    #[tokio::test]
    async fn excessive_output_is_refused_with_a_bounded_diagnostic_tail() {
        let error = run(
            shell("printf '%01000d' 0; exec sleep 30"),
            "output",
            Duration::from_secs(3),
            64,
        )
        .await
        .expect_err("bounded output must fail");
        let error = error
            .downcast_ref::<RunError>()
            .expect("typed runner error");
        assert_eq!(error.failure, Failure::OutputLimit);
        assert!(error.status.is_some(), "refused child was reaped");
        assert!(error.stdout.len() <= 64);
        assert!(error.stdout.ends_with('0'));
    }

    #[tokio::test]
    async fn exited_parent_with_pipe_holding_descendant_is_cleaned_before_reaping() {
        let error = run(
            shell("sleep 30 & exit 0"),
            "descendant",
            Duration::from_millis(100),
            1024,
        )
        .await
        .expect_err("inherited output pipe must remain bounded");
        let error = error
            .downcast_ref::<RunError>()
            .expect("typed runner error");
        assert_eq!(error.failure, Failure::Timeout);
        assert_eq!(
            error
                .status
                .expect("parent reaped after group cleanup")
                .code(),
            Some(0)
        );
        assert_eq!(
            error.capture_eof, 2,
            "both inherited pipes closed after group cleanup"
        );
    }
}
