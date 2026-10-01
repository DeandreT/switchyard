//! Opt-in gates for the current and previous official .NET Service Bus clients.

use std::{
    error::Error, path::PathBuf, process::Command, sync::Arc, thread::JoinHandle, time::Duration,
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, CommandOutcome, QueueConfig, ReceiveMode, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, BrokerHandle, LocalProposer, Shutdown, SystemClock, TimerWorker};
use storage::{MemoryStore, StateStore};
use tokio::net::TcpListener;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";
const CURRENT_SDK: &str = "7.21.0";
const PREVIOUS_SDK: &str = "7.20.2";

struct TestTimer {
    shutdown: Arc<Shutdown>,
    thread: Option<JoinHandle<()>>,
}

impl TestTimer {
    fn start(handle: BrokerHandle) -> Self {
        let shutdown = Arc::new(Shutdown::default());
        let worker_shutdown = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || {
            TimerWorker::new(&handle).run(Duration::from_millis(100), &worker_shutdown);
        });
        Self {
            shutdown,
            thread: Some(thread),
        }
    }
}

impl Drop for TestTimer {
    fn drop(&mut self) {
        self.shutdown.signal();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_completes_message_and_session_workflows()
-> Result<(), Box<dyn Error>> {
    run_client_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_completes_message_and_session_workflows()
-> Result<(), Box<dyn Error>> {
    run_client_gate(PREVIOUS_SDK).await
}

async fn run_client_gate(sdk_version: &'static str) -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let store = MemoryStore::default();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        SystemClock,
    ));
    let namespace = domain::NamespaceName::new("tenant")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        domain::EntityPath::new("orders")?,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    broker.handle().submit_blocking(
        namespace.clone(),
        domain::EntityPath::new("sessions")?,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    )?;
    let session_peek_queue = domain::EntityPath::new("sessions-peek")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        session_peek_queue.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    )?;

    broker.handle().submit_blocking(
        namespace.clone(),
        domain::EntityPath::new("duplicates")?,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis: 20_000,
                ..QueueConfig::default()
            },
        },
    )?;
    for (path, config) in [
        ("orders-batches", QueueConfig::default()),
        (
            "sessions-batches",
            QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        ),
        (
            "duplicates-batches",
            QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis: 300_000,
                ..QueueConfig::default()
            },
        ),
    ] {
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new(path)?,
            CommandKind::CreateQueue { config },
        )?;
    }
    let topic = domain::EntityPath::new("orders-topics")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis: 300_000,
                ..TopicConfig::default()
            },
        },
    )?;
    for name in ["Alpha", "beta"] {
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )?;
    }
    let session_topic = domain::EntityPath::new("orders-topic-sessions")?;
    broker.handle().submit_blocking(
        namespace.clone(),
        session_topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    for (name, requires_session) in [("Alpha", true), ("beta", true), ("ordinary", false)] {
        broker.handle().submit_blocking(
            namespace.clone(),
            session_topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig {
                    requires_session,
                    ..SubscriptionConfig::default()
                },
            },
        )?;
    }
    let _timer = TestTimer::start(broker.handle());

    let rule = SharedAccessRule::new(
        RULE,
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::MANAGE,
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

    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj");
    let output = tokio::task::spawn_blocking(move || {
        let artifacts = tempfile::TempDir::new()?;
        let output_directory = artifacts.path().join("bin");
        let intermediate_directory = artifacts.path().join("obj");
        let build = Command::new("dotnet")
            .env("DOTNET_PROCESSOR_COUNT", "2")
            .arg("build")
            .arg(&project)
            .arg("--configuration")
            .arg("Release")
            .arg("--maxcpucount:2")
            .arg("--disable-build-servers")
            .arg("--output")
            .arg(&output_directory)
            .arg(format!("-p:ServiceBusSdkVersion={sdk_version}"))
            .arg(format!(
                "-p:BaseIntermediateOutputPath={}/",
                intermediate_directory.display()
            ))
            .arg(format!(
                "-p:MSBuildProjectExtensionsPath={}/",
                intermediate_directory.display()
            ))
            .output()?;
        if !build.status.success() {
            return Ok::<_, std::io::Error>(build);
        }
        Command::new("dotnet")
            .env("DOTNET_PROCESSOR_COUNT", "2")
            .arg(output_directory.join("Switchyard.Conformance.DotNetCurrent.dll"))
            .arg(HOST)
            .arg(format!("sb://localhost:{}", address.port()))
            .arg("orders")
            .arg("sessions")
            .arg("duplicates")
            .arg(RULE)
            .arg(KEY)
            .output()
    })
    .await??;

    assert!(
        output.status.success(),
        "official .NET {sdk_version} gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("session renew/state/deferred receive passed"),
        "the client exited without reporting the completed workflow"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(
            "topic fanout/subscription peek/renew/defer/complete/dead-letter/batch passed"
        ),
        "the client exited without completing topic workflows"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("topic session accept/next/FIFO/renew/state/deferred/SDLQ/isolation passed"),
        "the client exited without completing topic session workflows"
    );
    for marker in [
        "required subscription management-only global/session-filtered peek passed",
        "required queue management-only global/session-filtered peek passed",
    ] {
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(marker),
            "the client exited without completing {marker}"
        );
    }
    assert_eq!(
        broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
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
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("duplicates")?,
            CommandKind::Peek {
                from_sequence: domain::SequenceNumber::new(0),
                max_messages: 10,
                session_id: None,
            },
        )?,
        CommandOutcome::Peeked(Vec::new()),
        "duplicate workflows left retained messages in the broker"
    );
    for name in ["Alpha", "beta"] {
        let entity = topic.subscription(&SubscriptionName::new(name)?)?;
        for target in [entity.clone(), entity.dead_letter_queue()?] {
            assert_eq!(
                broker.handle().submit_blocking(
                    domain::NamespaceName::new("tenant")?,
                    target.clone(),
                    CommandKind::Peek {
                        from_sequence: domain::SequenceNumber::new(0),
                        max_messages: 10,
                        session_id: None
                    }
                )?,
                CommandOutcome::Peeked(vec![]),
                "topic SDK workflow left retained messages in {target}"
            );
        }
    }
    for name in ["Alpha", "beta", "ordinary"] {
        let entity = session_topic.subscription(&SubscriptionName::new(name)?)?;
        for target in [entity.clone(), entity.dead_letter_queue()?] {
            assert!(
                store
                    .scan_prefix(
                        &domain::keys::message_prefix(
                            &domain::NamespaceName::new("tenant")?,
                            &target
                        ),
                        1
                    )?
                    .is_empty(),
                "topic session SDK workflow left retained messages in {target}"
            );
            assert!(
                store
                    .scan_prefix(
                        &domain::keys::session_lock_prefix(
                            &domain::NamespaceName::new("tenant")?,
                            &target
                        ),
                        1
                    )?
                    .is_empty(),
                "topic session SDK cleanup left an active hold in {target}"
            );
        }
    }
    assert!(
        store
            .scan_prefix(
                &domain::keys::message_prefix(
                    &domain::NamespaceName::new("tenant")?,
                    &session_peek_queue
                ),
                1
            )?
            .is_empty(),
        "session queue browse workflow left retained messages"
    );
    assert!(
        store
            .scan_prefix(
                &domain::keys::session_lock_prefix(
                    &domain::NamespaceName::new("tenant")?,
                    &session_peek_queue
                ),
                1
            )?
            .is_empty(),
        "session queue browse cleanup left an active hold"
    );
    Ok(())
}
