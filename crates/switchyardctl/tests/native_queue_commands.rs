//! The installed CLI contract is exercised against real HTTP/2 listeners.

use std::{
    error::Error,
    io,
    process::{Output, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{EntityPath, NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde_json::Value;
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use sha2::Sha256;
use storage::MemoryStore;
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    process::Command,
    task::JoinHandle,
    time::timeout,
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-cli-test-secret";

struct Node {
    broker: Broker,
    arguments: Vec<String>,
    listener: JoinHandle<()>,
    credentials: TempDir,
}

impl Node {
    async fn start(tls: bool) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let mut service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        let credentials = TempDir::new()?;
        let mut arguments = vec!["--namespace".to_owned(), "tenant".to_owned()];
        let identity = if tls {
            let policy = SharedAccessPolicy::new([SharedAccessRule::new(
                "manage",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?])?;
            service = service.with_shared_access_policy(policy, HOST)?;
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".to_owned()])?;
            let pem = cert.pem();
            let ca_path = credentials.path().join("ca.pem");
            let token_path = credentials.path().join("token");
            std::fs::write(&ca_path, &pem)?;
            std::fs::write(&token_path, format!("{}\n", token()))?;
            arguments.extend([
                "--ca-certificate".to_owned(),
                ca_path.display().to_string(),
                "--tls-server-name".to_owned(),
                "localhost".to_owned(),
                "--token-file".to_owned(),
                token_path.display().to_string(),
            ]);
            Some((pem, key_pair.serialize_pem()))
        } else {
            arguments.push("--allow-insecure".to_owned());
            None
        };
        let mut admin = NativeAdminListener::new(service);
        if let Some((certificate, key)) = identity {
            admin = admin.with_tls(certificate.as_bytes(), key.as_bytes())?;
        }
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        arguments.extend([
            "--endpoint".to_owned(),
            format!(
                "{}://{}",
                if tls { "https" } else { "http" },
                socket.local_addr()?
            ),
        ]);
        let listener = tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        });
        Ok(Self {
            broker,
            arguments,
            listener,
            credentials,
        })
    }

    async fn run(&self, command: &[&str]) -> TestResult<Output> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|value| (*value).to_owned()));
        run(arguments).await
    }

    async fn json(&self, command: &[&str]) -> TestResult<Value> {
        let output = self.run(command).await?;
        assert!(
            output.status.success(),
            "CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn run(arguments: Vec<String>) -> TestResult<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_switchyardctl"))
        .args(arguments)
        .env("TOKIO_WORKER_THREADS", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let collected = timeout(Duration::from_secs(30), async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(stdout), read_output(stderr))?;
        Ok::<_, io::Error>(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match collected {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            Err(error.into())
        }
        Err(_) => {
            child.kill().await?;
            Err(io::Error::new(io::ErrorKind::TimedOut, "CLI test process timed out").into())
        }
    }
}

async fn read_output(reader: impl AsyncRead + Unpin) -> io::Result<Vec<u8>> {
    const MAXIMUM: usize = 4 * 1024 * 1024;
    let mut output = Vec::new();
    reader
        .take(MAXIMUM as u64 + 1)
        .read_to_end(&mut output)
        .await?;
    if output.len() > MAXIMUM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CLI output exceeds the test limit",
        ));
    }
    Ok(output)
}

fn token() -> String {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock follows the epoch")
        .as_secs()
        + 300;
    let mut signature = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("a test HMAC key");
    signature.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(signature.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

#[tokio::test]
async fn queue_commands_commit_and_print_complete_configuration() -> TestResult {
    let node = Node::start(false).await?;
    let created = node
        .json(&[
            "queue",
            "create",
            "orders",
            "--lock-duration-millis",
            "30000",
            "--default-ttl-millis",
            "60000",
            "--dead-lettering-on-message-expiration",
        ])
        .await?;
    assert_eq!(created["path"], "orders");
    assert_eq!(created["queue_config"]["lock_duration_millis"], 30_000);
    assert_eq!(
        created["queue_config"]["default_time_to_live_millis"],
        60_000
    );
    assert_eq!(node.json(&["queue", "get", "orders"]).await?, created);
    let updated = node
        .json(&[
            "queue",
            "update",
            "orders",
            "--ttl-unlimited",
            "--dead-lettering-on-message-expiration=false",
        ])
        .await?;
    assert!(updated["queue_config"]["default_time_to_live_millis"].is_null());
    assert_eq!(
        updated["queue_config"]["dead_lettering_on_message_expiration"],
        false
    );
    assert_eq!(updated["queue_config"]["lock_duration_millis"], 30_000);
    let stored = node
        .broker
        .handle()
        .queue_config(NamespaceName::new("tenant")?, EntityPath::new("orders")?)
        .await?
        .expect("CLI updates reached the owner");
    assert_eq!(stored.default_time_to_live_millis, None);
    assert!(!stored.dead_lettering_on_message_expiration);

    node.json(&["queue", "create", "another"]).await?;
    let first = node.json(&["queue", "list", "--page-size", "1"]).await?;
    assert_eq!(first["entities"][0]["path"], "another");
    let cursor = first["next_page_token"].as_str().expect("a continuation");
    assert!(!cursor.is_empty());
    let second = node
        .json(&["queue", "list", "--page-size", "1", "--page-token", cursor])
        .await?;
    assert_eq!(second["entities"][0]["path"], "orders");
    assert_eq!(second["next_page_token"], "");
    let missing = node.run(&["queue", "get", "missing"]).await?;
    assert!(!missing.status.success());
    assert!(missing.stdout.is_empty());
    Ok(())
}

#[tokio::test]
async fn tls_and_token_files_are_used_without_disclosing_credentials() -> TestResult {
    let node = Node::start(true).await?;
    assert_eq!(
        node.json(&["queue", "create", "secure"]).await?["path"],
        "secure"
    );
    let token_path = node.credentials.path().join("token");
    let invalid = "SharedAccessSignature secret-that-must-not-be-printed";
    std::fs::write(token_path, invalid)?;
    let denied = node.run(&["queue", "get", "secure"]).await?;
    assert!(!denied.status.success());
    assert!(denied.stdout.is_empty());
    let error = String::from_utf8(denied.stderr)?;
    assert!(!error.contains(invalid));
    assert!(!error.contains(KEY));
    Ok(())
}

#[tokio::test]
async fn plaintext_tokens_and_implicit_insecure_connections_fail_before_networking() -> TestResult {
    let credentials = TempDir::new()?;
    let token_path = credentials.path().join("token");
    std::fs::write(&token_path, token())?;
    for extras in [
        Vec::<String>::new(),
        vec![
            "--allow-insecure".to_owned(),
            "--token-file".to_owned(),
            token_path.display().to_string(),
        ],
    ] {
        let mut arguments = vec!["--endpoint".to_owned(), "http://127.0.0.1:1".to_owned()];
        arguments.extend(extras);
        arguments.extend(["queue".to_owned(), "get".to_owned(), "orders".to_owned()]);
        let rejected = run(arguments).await?;
        assert!(!rejected.status.success());
        assert!(rejected.stdout.is_empty());
        let error = String::from_utf8(rejected.stderr)?;
        assert!(!error.contains("secret"));
        assert!(
            error.contains("allow-insecure") || error.contains("tokens require HTTPS"),
            "validation should precede networking: {error}"
        );
    }
    Ok(())
}
