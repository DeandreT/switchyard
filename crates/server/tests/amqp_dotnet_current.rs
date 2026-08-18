//! Opt-in gate for the current stable official .NET Service Bus client.

use std::{error::Error, path::PathBuf, process::Command, time::Duration};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{CommandKind, CommandOutcome, QueueConfig, ReceiveMode, StateMachine};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_completes_message_and_session_workflows()
-> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
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
            .arg(HOST)
            .arg(format!("sb://localhost:{}", address.port()))
            .arg("orders")
            .arg("sessions")
            .arg(RULE)
            .arg(KEY)
            .output()
    })
    .await??;

    assert!(
        output.status.success(),
        "official .NET gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("session renew/state passed"),
        "the client exited without reporting the completed workflow"
    );
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
    Ok(())
}
