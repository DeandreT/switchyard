//! Bounded actual child observation; no hidden tonic-descendant join claim.
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    Command, CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, Timestamp,
};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    Broker, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener, NativeAdminService,
};
use sha2::Sha256;
use std::{
    error::Error,
    future::{Future, poll_fn},
    io,
    process::{Output, Stdio},
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage::MemoryStore;
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    process::Command as Process,
    task::JoinHandle,
    time::timeout,
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type ListenerJoin = Result<Result<(), NativeAdminError>, tokio::task::JoinError>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "maintenance-cli-test-secret";
const CHILD_DEADLINE: Duration = Duration::from_secs(15);
const PIPE_LIMIT: usize = 8 * 1024;

fn token() -> TestResult<String> {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expiry = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 300;
    let mut signature = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    signature.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(signature.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage"
    ))
}

struct Node {
    broker: Option<Broker>,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    credentials: TempDir,
    arguments: Vec<String>,
    clock: ManualClock,
}
impl Node {
    async fn start() -> TestResult<Self> {
        let store = MemoryStore::default();
        StateMachine::new(store.clone()).apply(&Command::new(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(StateMachine::new(store), clock.clone()));
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?])?;
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?)
            .with_development_maintenance_readiness()
            .with_shared_access_policy(policy, HOST)?;
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let certificate = cert.pem();
        let credentials = TempDir::new()?;
        let ca = credentials.path().join("ca.pem");
        let sas = credentials.path().join("sas");
        std::fs::write(&ca, &certificate)?;
        std::fs::write(&sas, token()?)?;
        let admin = NativeAdminListener::new(service)
            .with_tls(certificate.as_bytes(), key_pair.serialize_pem().as_bytes())?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let arguments = vec![
            "--endpoint".into(),
            format!("https://{}", socket.local_addr()?),
            "--namespace".into(),
            "tenant".into(),
            "--ca-certificate".into(),
            ca.display().to_string(),
            "--tls-server-name".into(),
            "localhost".into(),
            "--token-file".into(),
            sas.display().to_string(),
            "maintenance-clock".into(),
        ];
        let listener = tokio::spawn(admin.serve(socket));
        Ok(Self {
            broker: Some(broker),
            listener: Some(listener),
            credentials,
            arguments,
            clock,
        })
    }
    async fn finish(mut self) -> ListenerJoin {
        let listener = self.listener.take().expect("original listener token");
        listener.abort();
        let joined = listener.await;
        drop(self.broker.take());
        joined
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
    }
}

struct PipeObservation {
    bytes: Vec<u8>,
    error: Option<io::Error>,
}

async fn read_output(reader: impl AsyncRead + Unpin) -> PipeObservation {
    let mut bytes = Vec::new();
    let error = reader
        .take((PIPE_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .err();
    let error = error.or_else(|| {
        (bytes.len() > PIPE_LIMIT).then(|| io::Error::other("CLI pipe exceeded test bound"))
    });
    PipeObservation { bytes, error }
}

async fn run(arguments: &[String]) -> TestResult<Output> {
    let mut child = Process::new(env!("CARGO_BIN_EXE_switchyardctl"))
        .args(arguments)
        .env("TOKIO_WORKER_THREADS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    if stdout.is_none() || stderr.is_none() {
        let _ = child.start_kill();
        let reaped = child.wait().await;
        reaped?;
        return Err("CLI pipes were unavailable".into());
    }
    let mut stdout_future = Box::pin(read_output(stdout.take().expect("verified stdout pipe")));
    let mut stderr_future = Box::pin(read_output(stderr.take().expect("verified stderr pipe")));
    let mut status = None;
    let mut stdout_result = None;
    let mut stderr_result = None;
    let observed = {
        let mut wait = Box::pin(child.wait());
        // Publish every original Ready result outside the disposable observer.
        let combined = poll_fn(|cx| {
            if status.is_none()
                && let Poll::Ready(value) = wait.as_mut().poll(cx)
            {
                status = Some(value);
            }
            if stdout_result.is_none()
                && let Poll::Ready(value) = stdout_future.as_mut().poll(cx)
            {
                stdout_result = Some(value);
            }
            if stderr_result.is_none()
                && let Poll::Ready(value) = stderr_future.as_mut().poll(cx)
            {
                stderr_result = Some(value);
            }
            let failed = status.as_ref().is_some_and(Result::is_err)
                || stdout_result
                    .as_ref()
                    .is_some_and(|value: &PipeObservation| value.error.is_some())
                || stderr_result
                    .as_ref()
                    .is_some_and(|value: &PipeObservation| value.error.is_some());
            if failed || (status.is_some() && stdout_result.is_some() && stderr_result.is_some()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        timeout(CHILD_DEADLINE, combined).await
    };
    let failed = observed.is_err()
        || status.as_ref().is_some_and(Result::is_err)
        || stdout_result
            .as_ref()
            .is_some_and(|value| value.error.is_some())
        || stderr_result
            .as_ref()
            .is_some_and(|value| value.error.is_some());
    if failed {
        let _ = child.start_kill();
        // Pending pipe futures and already published outputs remain owned here.
        let reaped = child.wait().await;
        if observed.is_err() {
            reaped?;
            return Err(
                io::Error::new(io::ErrorKind::TimedOut, "CLI test deadline expired").into(),
            );
        }
        if let Some(Err(error)) = status {
            let _ = reaped;
            return Err(error.into());
        }
        if let Some(error) = stdout_result.as_mut().and_then(|value| value.error.take()) {
            let _ = reaped;
            return Err(error.into());
        }
        if let Some(error) = stderr_result.as_mut().and_then(|value| value.error.take()) {
            let _ = reaped;
            return Err(error.into());
        }
        reaped?;
        return Err("CLI observation failed".into());
    }
    Ok(Output {
        status: status.ok_or("missing reaped status")??,
        stdout: stdout_result.ok_or("missing stdout")?.bytes,
        stderr: stderr_result.ok_or("missing stderr")?.bytes,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_cli_queries_the_opted_authenticated_method() -> TestResult {
    let node = Node::start().await?;
    let observed = async {
        let ready = run(&node.arguments).await?;
        node.clock.set(0);
        let unsafe_state = run(&node.arguments).await?;
        let mut unauthorized = node.arguments.clone();
        let token_flag = unauthorized
            .iter()
            .position(|value| value == "--token-file")
            .ok_or("token option")?;
        unauthorized.drain(token_flag..token_flag + 2);
        let denied = run(&unauthorized).await?;
        let credentials_exist = node.credentials.path().exists();
        Ok::<_, Box<dyn Error>>((ready, unsafe_state, denied, credentials_exist))
    }
    .await;
    let cleanup = node.finish().await;
    let (ready, unsafe_state, denied, credentials_exist) = observed?;
    assert!(matches!(cleanup, Err(error) if error.is_cancelled()));
    assert!(credentials_exist);
    assert!(ready.status.success());
    assert!(ready.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ready.stdout)?,
        serde_json::json!({"scope":"development_maintenance_clock", "state":"ready"})
    );
    assert_eq!(unsafe_state.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unsafe_state.stdout)?,
        serde_json::json!({"scope":"development_maintenance_clock", "state":"unsafe"})
    );
    assert!(
        String::from_utf8_lossy(&unsafe_state.stderr).contains("maintenance clock is not ready")
    );
    assert_eq!(denied.status.code(), Some(1));
    assert!(denied.stdout.is_empty());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("Unauthenticated"));
    for output in [ready, unsafe_state, denied] {
        assert!(output.stdout.len() < 128);
        assert!(output.stderr.len() <= PIPE_LIMIT);
        assert!(!String::from_utf8_lossy(&output.stderr).contains(KEY));
        assert!(!String::from_utf8_lossy(&output.stdout).contains(HOST));
    }
    Ok(())
}
