//! Topology CLI commands use the real native listener and preserve queue clients.

use std::{
    error::Error,
    io,
    process::{Output, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde_json::Value;
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use sha2::Sha256;
use storage::{MemoryStore, StateStore};
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
const KEY: &str = "native-topic-cli-test-secret";
const DEADLINE: Duration = Duration::from_secs(30);

struct Node {
    broker: Broker,
    store: MemoryStore,
    clock: ManualClock,
    arguments: Vec<String>,
    listener: JoinHandle<()>,
    credentials: TempDir,
}

impl Node {
    async fn start(tls: bool) -> TestResult<Self> {
        let store = MemoryStore::default();
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let mut service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        let credentials = TempDir::new()?;
        let mut arguments = vec!["--namespace".into(), "tenant".into()];
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
                generate_simple_self_signed(vec!["localhost".into()])?;
            let pem = cert.pem();
            let ca = credentials.path().join("ca.pem");
            let token_file = credentials.path().join("token");
            std::fs::write(&ca, &pem)?;
            std::fs::write(&token_file, format!("{}\n", token()))?;
            arguments.extend([
                "--ca-certificate".into(),
                ca.display().to_string(),
                "--tls-server-name".into(),
                "localhost".into(),
                "--token-file".into(),
                token_file.display().to_string(),
            ]);
            Some((pem, key_pair.serialize_pem()))
        } else {
            arguments.push("--allow-insecure".into());
            None
        };
        let mut admin = NativeAdminListener::new(service);
        if let Some((cert, key)) = identity {
            admin = admin.with_tls(cert.as_bytes(), key.as_bytes())?;
        }
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        arguments.extend([
            "--endpoint".into(),
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
            store,
            clock,
            arguments,
            listener,
            credentials,
        })
    }

    async fn run(&self, command: &[&str]) -> TestResult<Output> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|value| (*value).into()));
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
    let stdout = child.stdout.take().expect("stdout");
    let stderr = child.stderr.take().expect("stderr");
    let collected = timeout(DEADLINE, async {
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
            Err(io::Error::new(io::ErrorKind::TimedOut, "CLI topology process timed out").into())
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
            "CLI output exceeds its test bound",
        ));
    }
    Ok(output)
}

fn token() -> String {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("epoch")
        .as_secs()
        + 300;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

fn failed(output: &Output) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_topology_commands_print_complete_configs_and_preserve_queue_workflows() -> TestResult
{
    let node = Node::start(false).await?;
    let parent = node
        .json(&[
            "topic",
            "create",
            "Orders",
            "--default-ttl-millis",
            "20000",
            "--max-message-bytes",
            "32768",
            "--requires-duplicate-detection=false",
            "--duplicate-detection-history-time-window-millis",
            "60000",
        ])
        .await?;
    assert_eq!(parent["kind"], "topic");
    assert_eq!(parent["path"], "Orders");
    assert!(parent["queue_config"].is_null());
    assert!(parent["subscription_config"].is_null());
    assert_eq!(
        parent["topic_config"]["default_time_to_live_millis"],
        20_000
    );
    assert_eq!(parent["topic_config"]["max_message_bytes"], 32_768);
    assert_eq!(
        parent["topic_config"]["requires_duplicate_detection"],
        false
    );
    assert_eq!(
        parent["topic_config"]["duplicate_detection_history_time_window_millis"],
        60_000
    );
    assert_eq!(node.json(&["topic", "get", "Orders"]).await?, parent);
    let child = node
        .json(&[
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--lock-duration-millis",
            "30000",
            "--max-delivery-count",
            "7",
            "--ttl-unlimited",
            "--max-message-bytes",
            "8192",
            "--requires-session=false",
            "--dead-lettering-on-message-expiration=false",
            "--dead-lettering-on-filter-evaluation-exceptions=false",
        ])
        .await?;
    assert_eq!(child["kind"], "subscription");
    assert_eq!(child["path"], "Orders/subscriptions/Alpha");
    assert!(child["queue_config"].is_null());
    assert!(child["topic_config"].is_null());
    let config = &child["subscription_config"];
    assert_eq!(config["lock_duration_millis"], 30_000);
    assert_eq!(config["max_delivery_count"], 7);
    assert_eq!(config["max_message_bytes"], 8_192);
    assert_eq!(config["requires_session"], false);
    assert_eq!(config["dead_lettering_on_message_expiration"], false);
    assert_eq!(
        config["dead_lettering_on_filter_evaluation_exceptions"],
        false
    );
    assert!(config["default_time_to_live_millis"].is_null());
    assert!(config.get("requires_duplicate_detection").is_none());
    assert_eq!(
        node.json(&["subscription", "get", "Orders", "Alpha"])
            .await?,
        child
    );
    let default_filter_policy = node
        .json(&["subscription", "create", "Orders", "beta"])
        .await?;
    assert_eq!(
        default_filter_policy["subscription_config"]["dead_lettering_on_filter_evaluation_exceptions"],
        true
    );
    node.json(&["topic", "create", "Orders-extra", "--ttl-unlimited"])
        .await?;
    let enabled_filter_policy = node
        .json(&[
            "subscription",
            "create",
            "Orders-extra",
            "Enabled",
            "--dead-letter-on-filter-exceptions",
        ])
        .await?;
    assert_eq!(
        enabled_filter_policy["subscription_config"]["dead_lettering_on_filter_evaluation_exceptions"],
        true
    );
    assert_eq!(
        node.json(&["subscription", "get", "Orders-extra", "Enabled"])
            .await?,
        enabled_filter_policy
    );
    let ordinary = node
        .json(&["queue", "create", "work", "--lock-duration-millis", "30000"])
        .await?;
    assert_eq!(ordinary["kind"], "queue");
    assert_eq!(ordinary["queue_config"]["lock_duration_millis"], 30_000);
    assert!(ordinary.get("topic_config").is_none());
    assert!(ordinary.get("subscription_config").is_none());
    assert_eq!(node.json(&["queue", "get", "work"]).await?, ordinary);
    let updated = node
        .json(&["queue", "update", "work", "--ttl-unlimited"])
        .await?;
    assert_eq!(updated["kind"], "queue");
    let before = node.store.snapshot()?;
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    let queues = node.json(&["queue", "list", "--page-size", "1"]).await?;
    assert_eq!(queues["entities"].as_array().expect("queues").len(), 1);
    assert_eq!(queues["entities"][0]["path"], "work");
    assert_eq!(queues["next_page_token"], "");
    let first = node.json(&["topic", "list", "--page-size", "1"]).await?;
    assert_eq!(first["entities"][0]["path"], "Orders");
    let cursor = first["next_page_token"].as_str().expect("topic cursor");
    assert!(cursor.starts_with("topic.v1."));
    let last = node
        .json(&["topic", "list", "--page-size", "1", "--page-token", cursor])
        .await?;
    assert_eq!(last["entities"][0]["path"], "Orders-extra");
    assert_eq!(last["next_page_token"], "");
    let first = node
        .json(&["subscription", "list", "Orders", "--page-size", "1"])
        .await?;
    assert_eq!(first["entities"][0], child);
    let child_cursor = first["next_page_token"].as_str().expect("child cursor");
    assert!(child_cursor.starts_with("subscription.v1."));
    let last = node
        .json(&[
            "subscription",
            "list",
            "Orders",
            "--page-size",
            "1",
            "--page-token",
            child_cursor,
        ])
        .await?;
    assert_eq!(last["entities"][0]["path"], "Orders/subscriptions/beta");
    assert_eq!(last["next_page_token"], "");
    for command in [
        vec!["queue", "get", "Orders"],
        vec!["topic", "get", "work"],
        vec!["queue", "list", "--page-token", cursor],
        vec![
            "subscription",
            "list",
            "Orders-extra",
            "--page-token",
            child_cursor,
        ],
    ] {
        failed(&node.run(&command).await?);
    }
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_path_and_flag_errors_are_local_and_never_need_a_listener() -> TestResult {
    let oversized = "x".repeat(513);
    let parent = "p".repeat(
        domain::MAX_ENTITY_PATH_BYTES
            - domain::SUBSCRIPTION_PATH_SEGMENT.len()
            - 1
            - domain::DEAD_LETTER_QUEUE_SUFFIX.len()
            + 1,
    );
    let commands = [
        vec!["topic", "create", "Orders", "--requires-session"],
        vec![
            "topic",
            "create",
            "Orders",
            "--lock-duration-millis",
            "30000",
        ],
        vec![
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--requires-duplicate-detection",
        ],
        vec![
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--default-ttl-millis",
            "1",
            "--ttl-unlimited",
        ],
        vec!["subscription", "create", "Orders", "bad_"],
        vec![
            "subscription",
            "create",
            "Orders/subscriptions/nested",
            "Alpha",
        ],
        vec!["subscription", "get", "Orders", "/leaf"],
        vec!["subscription", "create", &parent, "b"],
        vec!["topic", "get", "Orders/subscriptions/Alpha"],
        vec!["queue", "get", "Orders/subscriptions/Alpha"],
        vec!["topic", "list", "--page-size", "1025"],
        vec!["subscription", "list", "Orders", "--page-token", &oversized],
    ];
    for command in commands {
        let mut arguments = vec![
            "--endpoint".into(),
            "http://127.0.0.1:1".into(),
            "--namespace".into(),
            "tenant".into(),
            "--allow-insecure".into(),
        ];
        arguments.extend(command.into_iter().map(String::from));
        let output = run(arguments).await?;
        failed(&output);
        let error = String::from_utf8(output.stderr)?;
        assert!(
            !error.contains("could not establish"),
            "local validation attempted a connection: {error}"
        );
        assert!(
            !error.contains("administration request failed"),
            "local validation reached a request: {error}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_commands_use_tls_token_files_without_disclosing_credentials() -> TestResult {
    let node = Node::start(true).await?;
    let created = node.json(&["topic", "create", "Secure"]).await?;
    assert_eq!(node.json(&["topic", "get", "Secure"]).await?, created);
    let child = node
        .json(&["subscription", "create", "Secure", "Alpha"])
        .await?;
    assert_eq!(
        node.json(&["subscription", "get", "Secure", "Alpha"])
            .await?,
        child
    );
    assert_eq!(
        node.json(&["subscription", "list", "Secure"]).await?["entities"][0],
        child
    );
    let before = node.store.snapshot()?;
    let invalid = "SharedAccessSignature secret-that-must-not-be-disclosed";
    std::fs::write(node.credentials.path().join("token"), invalid)?;
    let output = node.run(&["topic", "get", "Secure"]).await?;
    failed(&output);
    let error = String::from_utf8(output.stderr)?;
    assert!(!error.contains(KEY));
    assert!(!error.contains(invalid));
    assert_eq!(node.store.snapshot()?, before);
    Ok(())
}
