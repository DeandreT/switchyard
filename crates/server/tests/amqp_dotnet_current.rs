//! Opt-in gate for the current stable official .NET Service Bus client.

use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};

#[allow(dead_code)]
mod sdk_child;

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, CommandOutcome, QueueConfig, ReceiveMode, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, Shutdown, SystemClock, TimerWorker};
use storage::MemoryStore;
use tokio::net::TcpListener;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";
const CASE_QUEUE: &str = "Case-Identity-Queue";
const CASE_TOPIC: &str = "Case-Identity-Topic";
const CASE_SUBSCRIPTION: &str = "Case-Identity-Subscription";
const FILTER_TOPIC: &str = "Filtered-Events";
const FILTER_SUBSCRIPTION: &str = "Filtered-Subscription";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_exercises_settlement_and_session_workflows()
-> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        SystemClock,
    ));
    let namespace = domain::NamespaceName::new("tenant")?;
    for (path, config) in [
        ("orders", QueueConfig::default()),
        ("batch-orders", QueueConfig::default()),
        ("peek-orders", QueueConfig::default()),
        ("scheduled-orders", QueueConfig::default()),
        (
            "dedupe-orders",
            QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 10 * 60 * 1_000,
                ..QueueConfig::default()
            },
        ),
        (
            "sessions",
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
    let topic = domain::EntityPath::new("events")?;
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
    for name in ["accounting", "analytics"] {
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
            .serve(listener)
            .await;
    });

    let timer_shutdown = Arc::new(Shutdown::default());
    let timer_stop = Arc::clone(&timer_shutdown);
    let timer_broker = broker.handle();
    let timer = std::thread::spawn(move || {
        TimerWorker::new(&timer_broker).run(Duration::from_millis(25), &timer_stop);
    });

    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../conformance/dotnet-current");
    let output = async {
        let mut run =
            sdk_child::SdkRun::prepare(&source, "Switchyard.Conformance.DotNetCurrent.csproj")?;
        run.build().await?;
        let args = [
            HOST.into(),
            format!("sb://localhost:{}", address.port()).into(),
            "orders".into(),
            "batch-orders".into(),
            "peek-orders".into(),
            "scheduled-orders".into(),
            "dedupe-orders".into(),
            "sessions".into(),
            "events".into(),
            "accounting".into(),
            "analytics".into(),
            CASE_QUEUE.into(),
            CASE_TOPIC.into(),
            CASE_SUBSCRIPTION.into(),
            FILTER_TOPIC.into(),
            FILTER_SUBSCRIPTION.into(),
            RULE.into(),
            KEY.into(),
        ];
        let verified = run.run(&args, &[]).await?;
        println!(
            "SDK_CUSTODY_VERIFIED {}",
            serde_json::to_string(&verified.records).map_err(std::io::Error::other)?
        );
        Ok::<_, std::io::Error>(verified.output)
    }
    .await;
    timer_shutdown.signal();
    timer
        .join()
        .map_err(|_| std::io::Error::other("the test timer worker panicked"))?;
    let output = output?;

    assert!(
        output.status.success(),
        "official .NET gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(concat!(
            "batch send/prefetch/concurrent settlement, envelope fidelity, ",
            "send/receive/renew/complete, abandon/redelivery/property-update, ",
            "dead-letter/DLQ receive/complete, ",
            "defer/deferred-receive/management-disposition, ",
            "peek/browse pagination, schedule/cancel/timer activation, duplicate detection, immediate and scheduled topic fan-out, ",
            "case-insensitive queue/topic/subscription identity, ",
            "durable correlation rule management and filtered fan-out, ",
            "and session renew/state/peek passed"
        )),
        "the client exited without reporting the completed workflow"
    );
    let namespace = domain::NamespaceName::new("tenant")?;
    let orders = domain::EntityPath::new("orders")?;
    assert_eq!(
        broker.handle().submit_blocking(
            namespace.clone(),
            orders.clone(),
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the SDK returned from completion before the broker removed the message"
    );
    assert_eq!(
        broker.handle().submit_blocking(
            namespace,
            orders.dead_letter_queue()?,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the SDK returned from DLQ completion before the broker removed the message"
    );
    assert_eq!(
        broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("batch-orders")?,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the batch queue was not empty after concurrent and receive-delete workflows"
    );
    assert_eq!(
        broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("scheduled-orders")?,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::Received(None),
        "the schedule queue retained an activated or cancelled message"
    );
    Ok(())
}
