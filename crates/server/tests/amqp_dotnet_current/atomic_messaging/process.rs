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

pub(super) async fn run_evidence_selftest(dll: &Path) -> TestResult<Output> {
    run(
        evidence_selftest_command(dll),
        "official atomic diagnostic self-test",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

fn evidence_selftest_command(dll: &Path) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .arg(dll)
        .arg("atomic-evidence-selftest");
    command
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

pub(crate) async fn run_receive_batch_client(
    dll: &Path,
    endpoint: &str,
    topic: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    run(
        receive_batch_command(dll, endpoint, topic, queue, ca_file, ca_directory),
        "official same-receiver receive-batch client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

fn receive_batch_command(
    dll: &Path,
    endpoint: &str,
    topic: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("receive-batch")
        .arg(HOST)
        .arg(endpoint)
        .arg(topic)
        .arg(queue)
        .arg(RULE)
        .arg(KEY);
    command
}

pub(crate) async fn build_retained_client() -> TestResult<tempfile::TempDir> {
    build_client(CURRENT_SDK).await
}

pub(crate) fn retained_client_command(
    dll: &Path,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("retained-ingress")
        .arg(HOST)
        .arg(endpoint)
        .arg(queue)
        .arg(RULE)
        .arg(KEY);
    command
}

pub(crate) async fn run_retained_client(
    dll: &Path,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    run(
        retained_client_command(dll, endpoint, queue, ca_file, ca_directory),
        "official retained Memory transaction client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

pub(crate) async fn build_offline_jwt_client(
    sdk_version: &'static str,
) -> TestResult<tempfile::TempDir> {
    build_client(sdk_version).await
}

pub(crate) fn offline_jwt_client_command(
    dll: &Path,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("offline-jwt")
        .arg(HOST)
        .arg(endpoint)
        .arg(queue);
    command
}

pub(crate) async fn run_offline_jwt_client(
    dll: &Path,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    run(
        offline_jwt_client_command(dll, endpoint, queue, ca_file, ca_directory),
        "offline JWT client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OfflineJwtChildDiagnostic {
    stage: &'static str,
    exception: &'static str,
    credential_requested: bool,
    scope_refused: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OfflineJwtFailureSummary {
    failure: &'static str,
    status_present: bool,
    status_success: Option<bool>,
    capture_complete: bool,
    child: Option<OfflineJwtChildDiagnostic>,
}

fn offline_jwt_child_diagnostic(stderr: &str) -> Option<OfflineJwtChildDiagnostic> {
    if stderr.len() > MAX_OUTPUT_BYTES {
        return None;
    }
    let mut found = None;
    for line in stderr.lines() {
        let Some(fields) = line.strip_prefix("offline JWT SDK diagnostic ") else {
            continue;
        };
        if found.is_some() || fields.len() > 192 {
            return None;
        }
        let mut fields = fields.split(' ');
        let stage = fields.next()?.strip_prefix("stage=")?;
        let stage = [
            "arguments",
            "credential",
            "client",
            "send",
            "sender-disposal",
            "listen",
            "listen-unexpected-message",
            "listen-denial-missing",
            "receiver-disposal",
            "client-disposal",
            "credential-check",
        ]
        .into_iter()
        .find(|allowed| *allowed == stage)?;
        let exception = fields.next()?.strip_prefix("exception=")?;
        let exception = [
            "unauthorized",
            "service-bus",
            "cancelled",
            "argument",
            "invalid-operation",
            "cryptographic",
            "tls",
            "io",
            "other",
        ]
        .into_iter()
        .find(|allowed| *allowed == exception)?;
        let credential_requested = match fields.next()?.strip_prefix("credential_requested=")? {
            "true" => true,
            "false" => false,
            _ => return None,
        };
        let scope_refused = match fields.next()?.strip_prefix("scope_refused=")? {
            "true" => true,
            "false" => false,
            _ => return None,
        };
        if fields.next().is_some() {
            return None;
        }
        found = Some(OfflineJwtChildDiagnostic {
            stage,
            exception,
            credential_requested,
            scope_refused,
        });
    }
    found
}

pub(crate) fn offline_jwt_failure_summary(
    error: &(dyn Error + 'static),
) -> Option<OfflineJwtFailureSummary> {
    let error = error.downcast_ref::<RunError>()?;
    Some(OfflineJwtFailureSummary {
        failure: match error.failure {
            Failure::Nonzero => "nonzero",
            Failure::Timeout => "timeout",
            Failure::OutputLimit => "output-limit",
            Failure::Reader => "reader",
            Failure::Wait => "wait",
            Failure::Cleanup => "cleanup",
        },
        status_present: error.status.is_some(),
        status_success: error.status.map(|status| status.success()),
        capture_complete: error.capture_eof == 2,
        child: offline_jwt_child_diagnostic(&error.stderr),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    #[test]
    fn evidence_selftest_has_no_endpoint_or_credential_arguments() {
        let command = evidence_selftest_command(Path::new("conformance.dll"));
        let command = command.as_std();
        assert_eq!(command.get_program(), "dotnet");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["conformance.dll", "atomic-evidence-selftest"],
        );
        assert_eq!(
            command.get_envs().collect::<Vec<_>>(),
            [(
                std::ffi::OsStr::new("DOTNET_PROCESSOR_COUNT"),
                Some(std::ffi::OsStr::new("2"))
            )],
        );
    }

    #[test]
    fn receive_batch_arguments_and_trust_are_local_to_the_child() {
        let command = receive_batch_command(
            Path::new("conformance.dll"),
            "sb://localhost:1234",
            "batch-topic",
            "batch-queue",
            Path::new("local-ca.pem"),
            Path::new("empty-ca-directory"),
        );
        let command = command.as_std();
        assert_eq!(command.get_program(), "dotnet");
        let arguments: Vec<_> = command.get_args().collect();
        assert_eq!(
            arguments,
            [
                "conformance.dll",
                "receive-batch",
                HOST,
                "sb://localhost:1234",
                "batch-topic",
                "batch-queue",
                RULE,
                KEY,
            ]
        );
        let environment: std::collections::BTreeMap<_, _> = command.get_envs().collect();
        assert_eq!(
            environment.get(std::ffi::OsStr::new("DOTNET_PROCESSOR_COUNT")),
            Some(&Some(std::ffi::OsStr::new("2")))
        );
        assert_eq!(
            environment.get(std::ffi::OsStr::new("SSL_CERT_FILE")),
            Some(&Some(std::ffi::OsStr::new("local-ca.pem")))
        );
        assert_eq!(
            environment.get(std::ffi::OsStr::new("SSL_CERT_DIR")),
            Some(&Some(std::ffi::OsStr::new("empty-ca-directory")))
        );
        assert_eq!(environment.len(), 3);
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

    #[test]
    fn offline_jwt_summary_redacts_unknown_output_and_non_runner_errors() {
        use std::os::unix::process::ExitStatusExt;

        let error = RunError {
            label: "secret-label".into(),
            failure: Failure::Nonzero,
            status: Some(ExitStatus::from_raw(7 << 8)),
            capture_eof: 2,
            stdout: "secret-token secret-body".into(),
            stderr: "secret-key secret-arguments secret-exception-message".into(),
        };
        let summary = offline_jwt_failure_summary(&error).expect("original runner type");
        assert_eq!(summary.failure, "nonzero");
        assert!(summary.status_present);
        assert_eq!(summary.status_success, Some(false));
        assert!(summary.capture_complete);
        assert!(summary.child.is_none());
        let printed = format!("{summary:?}");
        assert!(!printed.contains("secret"));
        assert!(!printed.contains('7'));

        let other = io::Error::other("secret-non-runner-error");
        assert!(offline_jwt_failure_summary(&other).is_none());
        for (failure, label) in [
            (Failure::Timeout, "timeout"),
            (Failure::OutputLimit, "output-limit"),
            (Failure::Reader, "reader"),
            (Failure::Wait, "wait"),
            (Failure::Cleanup, "cleanup"),
        ] {
            let error = RunError {
                label: "secret-label".into(),
                failure,
                status: None,
                capture_eof: 1,
                stdout: "secret-stdout".into(),
                stderr: "secret-stderr".into(),
            };
            let summary = offline_jwt_failure_summary(&error).expect("runner type");
            assert_eq!(summary.failure, label);
            assert!(!summary.status_present);
            assert_eq!(summary.status_success, None);
            assert!(!summary.capture_complete);
            assert!(summary.child.is_none());
            assert!(!format!("{summary:?}").contains("secret"));
        }
    }

    #[test]
    fn offline_jwt_summary_accepts_only_exact_static_child_labels() {
        let prefix = "offline JWT SDK diagnostic ";
        for stage in [
            "arguments",
            "credential",
            "client",
            "send",
            "sender-disposal",
            "listen",
            "listen-unexpected-message",
            "listen-denial-missing",
            "receiver-disposal",
            "client-disposal",
            "credential-check",
        ] {
            for exception in [
                "unauthorized",
                "service-bus",
                "cancelled",
                "argument",
                "invalid-operation",
                "cryptographic",
                "tls",
                "io",
                "other",
            ] {
                for (requested, refused) in
                    [(false, false), (true, false), (false, true), (true, true)]
                {
                    let line = format!(
                        "{prefix}stage={stage} exception={exception} credential_requested={requested} scope_refused={refused}"
                    );
                    let child = offline_jwt_child_diagnostic(&format!(
                        "secret-before\n{line}\r\nsecret-after\n"
                    ))
                    .expect("only finite diagnostic labels");
                    assert_eq!(child.stage, stage);
                    assert_eq!(child.exception, exception);
                    assert_eq!(child.credential_requested, requested);
                    assert_eq!(child.scope_refused, refused);
                    assert!(!format!("{child:?}").contains("secret"));
                }
            }
        }
        let valid = format!(
            "{prefix}stage=send exception=service-bus credential_requested=true scope_refused=false"
        );
        for output in [
            "secret-token".to_owned(),
            valid.replace("stage=send", "stage=secret-token"),
            valid.replace("exception=service-bus", "exception=secret-error"),
            valid.replace("credential_requested=true", "credential_requested=secret"),
            valid.replace("scope_refused=false", "scope_refused=secret"),
            format!("{valid} secret-key"),
            format!("{valid}\n{valid}\n"),
            valid.replace(" exception=", "  exception="),
            format!("{prefix}{}", "secret".repeat(40)),
        ] {
            assert!(offline_jwt_child_diagnostic(&output).is_none());
        }
        assert!(offline_jwt_child_diagnostic(&"secret".repeat(MAX_OUTPUT_BYTES / 6 + 1)).is_none());
    }
}
