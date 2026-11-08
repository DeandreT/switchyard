//! Opt-in Memory/Fjall WSS workflows for two fixed official .NET package selections.

use std::{
    error::Error,
    path::{Path, PathBuf},
    time::Duration,
};

#[allow(dead_code)]
mod sdk_child;

use sdk_child::{
    matrix::{self, Backend},
    pins::SdkPin,
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName, TopicConfig,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, SystemClock};
use storage::{FjallStore, MemoryStore, StateStore};
use tokio::net::TcpListener;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";
const CASE_QUEUE: &str = "Case-Identity-Queue";
const CASE_TOPIC: &str = "Case-Identity-Topic";
const CASE_SUBSCRIPTION: &str = "Case-Identity-Subscription";
const FILTER_TOPIC: &str = "Filtered-WebSocket-Events";
const FILTER_SUBSCRIPTION: &str = "Filtered-WebSocket-Subscription";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux, dotnet 10 and a NuGet restore"]
async fn declared_current_memory_wss() -> Result<(), Box<dyn Error>> {
    run_wss_workflows(SdkPin::DeclaredCurrent, Backend::Memory).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux, dotnet 10 and a NuGet restore"]
async fn previous_memory_wss() -> Result<(), Box<dyn Error>> {
    run_wss_workflows(SdkPin::Previous, Backend::Memory).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux, dotnet 10 and a NuGet restore"]
async fn declared_current_fjall_wss() -> Result<(), Box<dyn Error>> {
    run_wss_workflows(SdkPin::DeclaredCurrent, Backend::Fjall).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux, dotnet 10 and a NuGet restore"]
async fn previous_fjall_wss() -> Result<(), Box<dyn Error>> {
    run_wss_workflows(SdkPin::Previous, Backend::Fjall).await
}

async fn run_wss_workflows(pin: SdkPin, backend: Backend) -> Result<(), Box<dyn Error>> {
    println!(
        "SDK_MATRIX_CELL pin={} backend={} transport=wss",
        pin.selector(),
        backend.name()
    );
    match backend {
        Backend::Memory => run_wss_store(pin, MemoryStore::default(), None).await,
        Backend::Fjall => {
            let directory = tempfile::tempdir()?;
            run_wss_store(
                pin,
                FjallStore::open(directory.path())?,
                Some(directory.path()),
            )
            .await
        }
    }
}

async fn run_wss_store<S: StateStore>(
    pin: SdkPin,
    store: S,
    directory: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        SystemClock,
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
    let filter_topic = domain::EntityPath::new(FILTER_TOPIC)?;
    broker.handle().submit_blocking(
        namespace.clone(),
        filter_topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    broker.handle().submit_blocking(
        namespace.clone(),
        filter_topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new(FILTER_SUBSCRIPTION)?,
            config: SubscriptionConfig::default(),
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
    let mut service = protocol_amqp::AmqpListener::new(
        sdk_child::diagnostics::ObservedBroker(broker.handle()),
        namespace,
    )
    .with_tls(tls)
    .with_shared_access_authentication(authentication)
    .into_wss_service(listener);
    let timer = matrix::Timer::start(broker.handle())?;

    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../conformance/dotnet-websockets");
    let diagnostic_entities = [
        "websocket-events",
        "websocket-events/subscriptions/first",
        "websocket-events/subscriptions/second",
        FILTER_TOPIC,
        "Filtered-WebSocket-Events/subscriptions/Filtered-WebSocket-Subscription",
    ];
    sdk_child::diagnostics::state(&store, "before-child", &diagnostic_entities);
    let output = matrix::serve(&mut service, async {
        let _trust = trust;
        let mut run = sdk_child::SdkRun::prepare(
            &source,
            "Switchyard.Conformance.DotNetWebSockets.csproj",
            pin,
        )?;
        run.build().await?;
        sdk_child::diagnostics::emit("child-launch", pin.selector());
        let args = [
            HOST.into(),
            format!("sb://localhost:{}", address.port()).into(),
            "websocket-orders".into(),
            "websocket-batch".into(),
            "websocket-schedule".into(),
            "websocket-dedupe".into(),
            "websocket-sessions".into(),
            "websocket-events".into(),
            "first".into(),
            "second".into(),
            CASE_QUEUE.into(),
            CASE_TOPIC.into(),
            CASE_SUBSCRIPTION.into(),
            FILTER_TOPIC.into(),
            FILTER_SUBSCRIPTION.into(),
            RULE.into(),
            KEY.into(),
        ];
        let verified = run
            .run(&args, &[("SSL_CERT_FILE", certificate_path.as_os_str())])
            .await?;
        println!(
            "SDK_PIN_VERIFIED {} {}",
            pin.selector(),
            serde_json::to_string(&verified.packages).map_err(std::io::Error::other)?
        );
        println!(
            "SDK_CUSTODY_VERIFIED {}",
            serde_json::to_string(&verified.records).map_err(std::io::Error::other)?
        );
        println!(
            "SDK_CHILD_STDOUT\n{}",
            String::from_utf8_lossy(&verified.output.stdout)
        );
        println!(
            "SDK_CHILD_STDERR\n{}",
            String::from_utf8_lossy(&verified.output.stderr)
        );
        Ok::<_, std::io::Error>(verified.output)
    })
    .await;
    sdk_child::diagnostics::emit(
        "child-returned-before-retirement",
        format!("success={}", output.is_ok()),
    );
    sdk_child::diagnostics::state(&store, "before-retirement", &diagnostic_entities);
    let quiesced = matrix::quiesce(service, timer).await;
    sdk_child::diagnostics::state(
        &store,
        "after-original-protocol-timer-joins",
        &diagnostic_entities,
    );
    let native = (|| -> Result<(), Box<dyn Error>> {
        for path in ["websocket-orders", "websocket-batch"] {
            matrix::empty(&broker, domain::EntityPath::new(path)?)?;
        }
        Ok(())
    })();
    let finished = matrix::finish(broker, store, directory).await;
    let output = output?;
    quiesced?;
    native?;
    finished?;

    assert!(
        output.status.success(),
        "official .NET WebSocket gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(
            "official .NET Service Bus client AMQP-over-WebSockets batch/prefetch, send/receive/complete, defer/peek/deferred-receive, schedule/cancel, duplicate detection, immediate and scheduled topic fan-out, case-insensitive queue/topic/subscription identity, durable correlation rule management and filtered fan-out, and session attach passed"
        ),
        "the client exited without reporting the completed WebSocket workflow"
    );

    Ok(())
}
