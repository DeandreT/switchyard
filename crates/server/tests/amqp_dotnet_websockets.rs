//! Opt-in AMQP-over-WebSockets gate for the current official .NET client.

use std::{error::Error, path::PathBuf, process::Command, time::Duration};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, CommandOutcome, QueueConfig, ReceiveMode, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";
const CASE_QUEUE: &str = "Case-Identity-Queue";
const CASE_TOPIC: &str = "Case-Identity-Topic";
const CASE_SUBSCRIPTION: &str = "Case-Identity-Subscription";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_dotnet_client_uses_amqp_over_websockets() -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let namespace = domain::NamespaceName::new("tenant")?;
    for (path, config) in [
        ("websocket-orders", QueueConfig::default()),
        ("websocket-batch", QueueConfig::default()),
        ("websocket-schedule", QueueConfig::default()),
        (
            "websocket-dedupe",
            QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 10 * 60 * 1_000,
                ..QueueConfig::default()
            },
        ),
        (
            "websocket-sessions",
            QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        ),
        (CASE_QUEUE, QueueConfig::default()),
    ] {
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new(path)?,
            CommandKind::CreateQueue { config },
        )?;
    }
    let topic = domain::EntityPath::new("websocket-events")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    for name in ["first", "second"] {
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )?;
    }
    let case_topic = domain::EntityPath::new(CASE_TOPIC)?;
    broker.handle().submit_blocking(
        namespace.clone(),
        case_topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    broker.handle().submit_blocking(
        namespace.clone(),
        case_topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new(CASE_SUBSCRIPTION)?,
            config: SubscriptionConfig::default(),
        },
    )?;

    let rule = SharedAccessRule::new(
        RULE,
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::SEND | PermissionSet::LISTEN,
    )?;
    let authentication =
        protocol_amqp::SharedAccessAuthentication::new(SharedAccessPolicy::new([rule])?, HOST)?
            .with_authorization_timeout(Duration::from_secs(20));
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec![String::from("localhost")])?;
    // The .NET ClientWebSocket transport uses the platform trust chain rather
    // than ServiceBusClientOptions.CertificateValidationCallback. Scope this
    // generated root to the child process instead of mutating machine trust.
    let trust = tempfile::tempdir()?;
    let certificate_path = trust.path().join("switchyard-test-root.pem");
    std::fs::write(&certificate_path, cert.pem())?;
    let tls = protocol_amqp::tls_server_config(
        cert.pem().as_bytes(),
        key_pair.serialize_pem().as_bytes(),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let handle = broker.handle();
    tokio::spawn(async move {
        let _ = protocol_amqp::AmqpListener::new(handle, namespace)
            .with_tls(tls)
            .with_shared_access_authentication(authentication)
            .serve_websockets(listener)
            .await;
    });

    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/dotnet-websockets/Switchyard.Conformance.DotNetWebSockets.csproj");
    let output = tokio::task::spawn_blocking(move || {
        let _trust = trust;
        let build = Command::new("dotnet")
            .arg("build")
            .arg(&project)
            .arg("--configuration")
            .arg("Release")
            .arg("--maxcpucount:2")
            .output()?;
        if !build.status.success() {
            return Ok::<_, std::io::Error>(build);
        }
        Command::new("dotnet")
            .arg("run")
            .arg("--project")
            .arg(&project)
            .arg("--configuration")
            .arg("Release")
            .arg("--no-build")
            .arg("--")
            .env("SSL_CERT_FILE", &certificate_path)
            .arg(HOST)
            .arg(format!("sb://localhost:{}", address.port()))
            .arg("websocket-orders")
            .arg("websocket-batch")
            .arg("websocket-schedule")
            .arg("websocket-dedupe")
            .arg("websocket-sessions")
            .arg("websocket-events")
            .arg("first")
            .arg("second")
            .arg(CASE_QUEUE)
            .arg(CASE_TOPIC)
            .arg(CASE_SUBSCRIPTION)
            .arg(RULE)
            .arg(KEY)
            .output()
    })
    .await??;

    assert!(
        output.status.success(),
        "official .NET WebSocket gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(
            "official .NET Service Bus client AMQP-over-WebSockets batch/prefetch, send/receive/complete, defer/peek/deferred-receive, schedule/cancel, duplicate detection, topic fan-out, case-insensitive queue/topic/subscription identity, and session attach passed"
        ),
        "the client exited without reporting the completed WebSocket workflow"
    );

    assert_eq!(
        broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("websocket-orders")?,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the official client returned before completion emptied the queue"
    );
    assert_eq!(
        broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("websocket-batch")?,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the official client returned before completing the WebSocket batch"
    );
    Ok(())
}
