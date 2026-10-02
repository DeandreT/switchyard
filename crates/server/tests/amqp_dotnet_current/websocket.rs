use std::{fs, path::Path, process::Output};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

use super::*;

const QUEUE: &str = "websocket-orders";
const SESSION_QUEUE: &str = "websocket-sessions";
const TOPIC: &str = "websocket-topic";
const SUCCESS: &str =
    "official .NET WSS CBS/queue/peek/renew/redelivery/session/state/FIFO/topic passed";
const UNTRUSTED: &str = "official .NET WSS untrusted certificate rejected passed";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_uses_wss_with_isolated_trust() -> Result<(), Box<dyn Error>> {
    run_websocket_gate(CURRENT_SDK).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires dotnet and a NuGet restore"]
async fn previous_stable_dotnet_client_uses_wss_with_isolated_trust() -> Result<(), Box<dyn Error>>
{
    run_websocket_gate(PREVIOUS_SDK).await
}

struct ListenerTasks(Vec<tokio::task::JoinHandle<()>>);

impl Drop for ListenerTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn run_websocket_gate(sdk_version: &'static str) -> Result<(), Box<dyn Error>> {
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
    for (path, requires_session) in [(QUEUE, false), (SESSION_QUEUE, true)] {
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new(path)?,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session,
                    ..QueueConfig::default()
                },
            },
        )?;
    }
    let topic = domain::EntityPath::new(TOPIC)?;
    broker.handle().submit_blocking(
        namespace.clone(),
        topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
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

    let rule = SharedAccessRule::new(
        RULE,
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::MANAGE,
    )?;
    let authentication =
        protocol_amqp::SharedAccessAuthentication::new(SharedAccessPolicy::new([rule])?, HOST)?
            .with_authorization_timeout(Duration::from_secs(10));
    let (tls, ca_pem) = signed_localhost_config()?;
    let wss_listener = TcpListener::bind("127.0.0.1:0").await?;
    let wss_address = wss_listener.local_addr()?;
    let wss = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
        .with_tls(tls)
        .with_websocket()
        .with_shared_access_authentication(authentication);
    let _listeners = ListenerTasks(vec![tokio::spawn(async move {
        let _ = wss.serve(wss_listener).await;
    })]);

    let artifacts = build_client(sdk_version).await?;
    let certificate_directory = tempfile::TempDir::new()?;
    let ca_file = certificate_directory.path().join("trusted-ca.pem");
    let empty_ca_file = certificate_directory.path().join("untrusted.pem");
    let empty_ca_directory = certificate_directory.path().join("empty-ca-directory");
    fs::write(&ca_file, ca_pem)?;
    fs::write(&empty_ca_file, [])?;
    fs::create_dir(&empty_ca_directory)?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    // The SDK must replace the supplied transport path and retain the logical namespace.
    let wss_endpoint = format!("wss://localhost:{}/ignored-custom-path", wss_address.port());
    let before = store.snapshot()?;
    let rejected = run_client(
        &dll,
        "websocket-untrusted",
        &wss_endpoint,
        &empty_ca_file,
        &empty_ca_directory,
    )
    .await?;
    assert_completed(sdk_version, "untrusted WSS", &rejected, UNTRUSTED);
    assert_eq!(
        store.snapshot()?,
        before,
        "untrusted WSS reached broker mutation"
    );

    let output = run_client(
        &dll,
        "websocket",
        &wss_endpoint,
        &ca_file,
        &empty_ca_directory,
    )
    .await?;
    assert_completed(sdk_version, "WSS", &output, SUCCESS);
    wait_clean(&store, &namespace, &topic).await?;
    Ok(())
}

pub(super) fn signed_localhost_config() -> Result<(rustls::ServerConfig, String), Box<dyn Error>> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Switchyard isolated WebSocket test CA");
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;

    let mut leaf_params = CertificateParams::new(vec![String::from("localhost")])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = KeyPair::generate()?;
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key)?;
    let chain = format!("{}{}", leaf.pem(), ca.pem());
    let config =
        protocol_amqp::tls_server_config(chain.as_bytes(), leaf_key.serialize_pem().as_bytes())?;
    Ok((config, ca.pem()))
}

async fn build_client(sdk_version: &'static str) -> Result<tempfile::TempDir, Box<dyn Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj");
    let (artifacts, output) = tokio::task::spawn_blocking(move || {
        let artifacts = tempfile::TempDir::new()?;
        let output = Command::new("dotnet")
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
            ))
            .output()?;
        Ok::<_, std::io::Error>((artifacts, output))
    })
    .await??;
    assert!(
        output.status.success(),
        "official .NET {sdk_version} WebSocket build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(artifacts)
}

async fn run_client(
    dll: &Path,
    mode: &str,
    endpoint: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> Result<Output, Box<dyn Error>> {
    let dll = dll.to_owned();
    let mode = mode.to_owned();
    let endpoint = endpoint.to_owned();
    let ca_file = ca_file.to_owned();
    let ca_directory = ca_directory.to_owned();
    Ok(tokio::task::spawn_blocking(move || {
        Command::new("dotnet")
            .env("DOTNET_PROCESSOR_COUNT", "2")
            // Linux .NET reads these OpenSSL trust paths in this child process only.
            .env("SSL_CERT_FILE", ca_file)
            .env("SSL_CERT_DIR", ca_directory)
            .arg(dll)
            .arg(mode)
            .arg(HOST)
            .arg(endpoint)
            .arg(QUEUE)
            .arg(SESSION_QUEUE)
            .arg(TOPIC)
            .arg(RULE)
            .arg(KEY)
            .output()
    })
    .await??)
}

fn assert_completed(sdk_version: &str, label: &str, output: &Output, marker: &str) {
    assert!(
        output.status.success(),
        "official .NET {sdk_version} {label} gate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(marker),
        "official .NET {sdk_version} {label} exited without {marker}"
    );
}

async fn wait_clean(
    store: &MemoryStore,
    namespace: &domain::NamespaceName,
    topic: &domain::EntityPath,
) -> Result<(), Box<dyn Error>> {
    let mut entities = vec![
        domain::EntityPath::new(QUEUE)?,
        domain::EntityPath::new(SESSION_QUEUE)?,
        topic.clone(),
    ];
    for name in ["Alpha", "beta"] {
        entities.push(topic.subscription(&SubscriptionName::new(name)?)?);
    }
    let shadows = entities
        .iter()
        .filter(|entity| *entity != topic)
        .map(domain::EntityPath::dead_letter_queue)
        .collect::<Result<Vec<_>, _>>()?;
    entities.extend(shadows);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut clean = true;
            for entity in &entities {
                for prefix in [
                    domain::keys::message_prefix(namespace, entity),
                    domain::keys::scheduled_prefix(namespace, entity),
                    domain::keys::session_lock_prefix(namespace, entity),
                ] {
                    clean &= store.scan_prefix(&prefix, 1)?.is_empty();
                }
            }
            if clean {
                return Ok::<_, storage::StorageError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}
