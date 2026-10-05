#![forbid(unsafe_code)]

mod logging;

#[cfg(test)]
#[path = "main/maintenance_tests.rs"]
mod maintenance_tests;

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    thread,
    time::Duration,
};

use clap::{Parser, ValueEnum};
use cluster::{ClusterConfig, DeploymentMode};
use protocol_amqp::{
    AmqpListener, SharedAccessAuthentication, namespace_from_hostname, tls_server_config,
};
use rustls::ServerConfig;
use server::{
    Broker, DEFAULT_SWEEP_INTERVAL, LocalProposer, NativeAdminListener, NativeAdminService,
    NodeState, Shutdown, StartupError, StorageChoice, SystemClock, TimerWorker,
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    name = "switchyard",
    version,
    about = "Azure Service Bus-compatible message broker"
)]
struct Arguments {
    #[arg(long, value_enum, default_value_t = ModeArgument::Development)]
    mode: ModeArgument,

    #[arg(long, value_enum, default_value_t = StorageArgument::Memory)]
    storage: StorageArgument,

    /// Where the durable backend keeps its state. Required with `--storage fjall`.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 1)]
    voters: u16,

    #[arg(long, default_value_t = DEFAULT_SWEEP_INTERVAL.as_millis() as u64)]
    sweep_interval_millis: u64,

    /// Where to accept AMQP connections. Defaults to port 5671 with TLS and
    /// port 5672 for development plaintext.
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Enable native HTTP/2 administration at this address. Uses the same TLS
    /// identity and shared-access policy as AMQP when configured.
    #[arg(long)]
    admin_listen: Option<SocketAddr>,

    /// Enable the descriptive development clock probe on --admin-listen.
    #[arg(long)]
    development_maintenance_readiness: bool,

    /// Enable AMQP over WebSockets at this address. Uses the same TLS identity
    /// and shared-access policy as the other listeners when configured.
    #[arg(long)]
    websocket_listen: Option<SocketAddr>,

    /// Enable experimental same-queue atomic messaging on a separate raw
    /// TCP/TLS address. Development only; same-queue SDK send and held Complete scopes.
    #[arg(long)]
    experimental_atomic_messaging_listen: Option<SocketAddr>,

    /// PEM certificate chain for the configured TLS listeners.
    #[arg(long, value_name = "PATH")]
    tls_certificate: Option<PathBuf>,

    /// PEM private key corresponding to --tls-certificate.
    #[arg(long, value_name = "PATH")]
    tls_private_key: Option<PathBuf>,

    /// Name of the namespace-wide shared-access rule.
    #[arg(long)]
    shared_access_key_name: Option<String>,

    /// File containing the shared-access key. The key is never accepted as a
    /// command-line value because process arguments are commonly observable.
    #[arg(long, value_name = "PATH")]
    shared_access_key_file: Option<PathBuf>,

    /// The namespace this node serves. A hostname is accepted and its first
    /// label taken, so a deployment can name namespaces in DNS.
    #[arg(long, default_value = "development")]
    namespace: String,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ModeArgument {
    Development,
    Production,
}

impl From<ModeArgument> for DeploymentMode {
    fn from(value: ModeArgument) -> Self {
        match value {
            ModeArgument::Development => Self::Development,
            ModeArgument::Production => Self::Production,
        }
    }
}

fn validate_experimental_atomic_messaging_listener(
    mode: DeploymentMode,
    address: Option<SocketAddr>,
) -> Result<(), StartupError> {
    if mode == DeploymentMode::Production && address.is_some() {
        return Err(StartupError::ExperimentalAtomicMessagingInProduction);
    }
    Ok(())
}

fn validate_development_maintenance_readiness(
    mode: DeploymentMode,
    enabled: bool,
    admin: Option<SocketAddr>,
) -> Result<(), StartupError> {
    if enabled && mode == DeploymentMode::Production {
        return Err(StartupError::DevelopmentMaintenanceReadinessInProduction);
    }
    if enabled && admin.is_none() {
        return Err(StartupError::DevelopmentMaintenanceReadinessRequiresAdminListener);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum StorageArgument {
    Memory,
    Fjall,
}

fn storage_choice(arguments: &Arguments) -> Result<StorageChoice, StartupError> {
    match arguments.storage {
        StorageArgument::Memory => Ok(StorageChoice::Memory),
        StorageArgument::Fjall => arguments
            .data_dir
            .clone()
            .map(|directory| StorageChoice::Durable { directory })
            .ok_or(StartupError::MissingDataDirectory),
    }
}

struct LoadedTls {
    amqp: ServerConfig,
    certificate_chain: Vec<u8>,
    private_key: Vec<u8>,
}

fn load_tls_config(
    mode: DeploymentMode,
    certificate_path: Option<&Path>,
    private_key_path: Option<&Path>,
) -> Result<Option<LoadedTls>, StartupError> {
    let (certificate_path, private_key_path) = match (certificate_path, private_key_path) {
        (None, None) if mode == DeploymentMode::Production => {
            return Err(StartupError::TlsRequiredInProduction);
        }
        (None, None) => return Ok(None),
        (Some(certificate), Some(private_key)) => (certificate, private_key),
        _ => return Err(StartupError::IncompleteTlsConfiguration),
    };

    let certificate_chain = read_tls_file(certificate_path)?;
    let private_key = read_tls_file(private_key_path)?;
    Ok(Some(LoadedTls {
        amqp: tls_server_config(&certificate_chain, &private_key)?,
        certificate_chain,
        private_key,
    }))
}

fn read_tls_file(path: &Path) -> Result<Vec<u8>, StartupError> {
    fs::read(path).map_err(|error| StartupError::ReadTlsCredentials {
        path: path.to_path_buf(),
        detail: error.to_string(),
    })
}

fn listen_address(configured: Option<SocketAddr>, tls: bool) -> SocketAddr {
    configured.unwrap_or_else(|| {
        SocketAddr::from((
            [127, 0, 0, 1],
            if tls {
                protocol_amqp::AMQP_TLS_PORT
            } else {
                5672
            },
        ))
    })
}

fn amqp_listener(
    broker: server::BrokerHandle,
    namespace: domain::NamespaceName,
    tls: Option<&LoadedTls>,
    authentication: Option<&SharedAccessAuthentication>,
) -> AmqpListener<server::BrokerHandle> {
    let mut listener = AmqpListener::new(broker, namespace);
    if let Some(tls) = tls {
        listener = listener.with_tls(tls.amqp.clone());
    }
    if let Some(authentication) = authentication {
        listener = listener.with_shared_access_authentication(authentication.clone());
    }
    listener
}

fn load_shared_access_authentication(
    mode: DeploymentMode,
    tls: bool,
    namespace: &str,
    key_name: Option<&str>,
    key_path: Option<&Path>,
) -> Result<Option<SharedAccessAuthentication>, StartupError> {
    let (key_name, key_path) = match (key_name, key_path) {
        (None, None) if mode == DeploymentMode::Production => {
            return Err(StartupError::AuthenticationRequiredInProduction);
        }
        (None, None) => return Ok(None),
        (Some(key_name), Some(key_path)) => (key_name, key_path),
        _ => return Err(StartupError::IncompleteSharedAccessPolicy),
    };
    if !tls {
        return Err(StartupError::AuthenticationRequiresTls);
    }

    let key = fs::read_to_string(key_path).map_err(|error| StartupError::ReadSharedAccessKey {
        path: key_path.to_path_buf(),
        detail: error.to_string(),
    })?;
    let key = key.trim_end_matches(['\r', '\n']);
    let host = if namespace.contains('.') {
        namespace.to_ascii_lowercase()
    } else {
        format!("{namespace}.servicebus.windows.net")
    };
    let rule = auth::SharedAccessRule::new(
        key_name,
        auth::ResourceScope::namespace(&host)?,
        auth::SharedAccessKey::new(key)?,
        None,
        auth::PermissionSet::MANAGE,
    )?;
    Ok(Some(SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([rule])?,
        host,
    )?))
}

/// Reports why startup failed in the words the error was written in, rather
/// than in the derived debug form `Termination` would print.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("switchyard: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), StartupError> {
    logging::initialize()?;
    run_with_arguments(Arguments::parse())
}

fn run_with_arguments(arguments: Arguments) -> Result<(), StartupError> {
    let mode = DeploymentMode::from(arguments.mode);
    validate_experimental_atomic_messaging_listener(
        mode,
        arguments.experimental_atomic_messaging_listen,
    )?;
    validate_development_maintenance_readiness(
        mode,
        arguments.development_maintenance_readiness,
        arguments.admin_listen,
    )?;
    let cluster = ClusterConfig {
        mode,
        voters: arguments.voters,
    };
    // Refuse an unsafe or malformed listener before opening a data directory.
    let tls = load_tls_config(
        mode,
        arguments.tls_certificate.as_deref(),
        arguments.tls_private_key.as_deref(),
    )?;
    let shared_access_authentication = load_shared_access_authentication(
        mode,
        tls.is_some(),
        &arguments.namespace,
        arguments.shared_access_key_name.as_deref(),
        arguments.shared_access_key_file.as_deref(),
    )?;
    let listen = listen_address(arguments.listen, tls.is_some());
    let state = server::open(cluster, storage_choice(&arguments)?)?;

    info!(
        ?mode,
        voters = arguments.voters,
        storage = ?arguments.storage,
        "configuration is valid"
    );
    let namespace = namespace_from_hostname(&arguments.namespace)?;
    let broker = match state {
        NodeState::Memory(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
        NodeState::Durable(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
    };

    // Nothing settles a message before its batch is fsynced, so an interrupt at
    // any point loses no acknowledged state. That is why there is no signal
    // handler yet: an abrupt stop is already safe.
    let shutdown = Arc::new(Shutdown::default());
    let interval = Duration::from_millis(arguments.sweep_interval_millis);
    let sweeper = {
        let handle = broker.handle();
        let shutdown = Arc::clone(&shutdown);
        thread::Builder::new()
            .name(String::from("switchyard-timer"))
            .spawn(move || TimerWorker::new(&handle).run(interval, &shutdown))
            .map_err(|error| StartupError::Runtime(error.to_string()))?
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| StartupError::Runtime(error.to_string()))?;
    let served = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|error| StartupError::Listen {
                address: listen.to_string(),
                detail: error.to_string(),
            })?;
        let websocket = if let Some(address) = arguments.websocket_listen {
            let socket = tokio::net::TcpListener::bind(address)
                .await
                .map_err(|error| StartupError::Listen {
                    address: address.to_string(),
                    detail: error.to_string(),
                })?;
            let listener = amqp_listener(
                broker.handle(),
                namespace.clone(),
                tls.as_ref(),
                shared_access_authentication.as_ref(),
            )
            .with_websocket();
            Some((listener, socket))
        } else {
            None
        };
        let experimental = if let Some(address) = arguments.experimental_atomic_messaging_listen {
            let socket = tokio::net::TcpListener::bind(address)
                .await
                .map_err(|error| StartupError::Listen {
                    address: address.to_string(),
                    detail: error.to_string(),
                })?;
            let listener = amqp_listener(
                broker.handle(),
                namespace.clone(),
                tls.as_ref(),
                shared_access_authentication.as_ref(),
            );
            Some((listener, socket))
        } else {
            None
        };
        let native = if let Some(address) = arguments.admin_listen {
            let socket = tokio::net::TcpListener::bind(address)
                .await
                .map_err(|error| StartupError::Listen {
                    address: address.to_string(),
                    detail: error.to_string(),
                })?;
            let mut service = NativeAdminService::new(broker.handle(), namespace.clone());
            if arguments.development_maintenance_readiness {
                service = service.with_development_maintenance_readiness();
            }
            if let Some(authentication) = &shared_access_authentication {
                service = service.with_shared_access_policy(
                    authentication.policy().clone(),
                    authentication.audience_host(),
                )?;
            }
            let mut admin = NativeAdminListener::new(service);
            if let Some(tls) = &tls {
                admin = admin.with_tls(&tls.certificate_chain, &tls.private_key)?;
            }
            info!(address = %socket.local_addr().map_err(|error| StartupError::Runtime(error.to_string()))?, namespace = %namespace, tls = tls.is_some(), "accepting native administration connections");
            Some((admin, socket))
        } else {
            None
        };
        info!(address = %listener.local_addr().map_err(|error| StartupError::Runtime(error.to_string()))?, namespace = %namespace, tls = tls.is_some(), "accepting AMQP connections");
        if let Some((_, socket)) = &websocket {
            info!(address = %socket.local_addr().map_err(|error| StartupError::Runtime(error.to_string()))?, namespace = %namespace, tls = tls.is_some(), "accepting AMQP WebSocket connections");
        }
        if let Some((_, socket)) = &experimental {
            info!(address = %socket.local_addr().map_err(|error| StartupError::Runtime(error.to_string()))?, namespace = %namespace, tls = tls.is_some(), sdk_transaction_scopes = "experimental-same-queue", "accepting experimental atomic messaging connections");
        }
        let amqp = amqp_listener(
            broker.handle(),
            namespace,
            tls.as_ref(),
            shared_access_authentication.as_ref(),
        );
        // Bind every configured socket before any listener can accept a client.
        let serve_native = async {
            match native {
                Some((admin, socket)) => admin.serve(socket).await,
                None => std::future::pending().await,
            }
        };
        let serve_websocket = async {
            match websocket {
                Some((websocket, socket)) => websocket.serve(socket).await,
                None => std::future::pending().await,
            }
        };
        let serve_experimental = async {
            match experimental {
                Some((experimental, socket)) => {
                    experimental.serve_atomic_messaging_ingress(socket).await
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            result = amqp.serve(listener) => {
                result.map_err(|error| StartupError::Runtime(error.to_string()))
            }
            result = serve_native => {
                result.map_err(|error| StartupError::Runtime(error.to_string()))
            }
            result = serve_websocket => {
                result.map_err(|error| StartupError::Runtime(error.to_string()))
            }
            result = serve_experimental => {
                result.map_err(|error| StartupError::Runtime(error.to_string()))
            }
        }
    });

    shutdown.signal();
    let _ = sweeper.join();
    served
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_refuses_plaintext_before_startup() {
        assert!(matches!(
            load_tls_config(DeploymentMode::Production, None, None),
            Err(StartupError::TlsRequiredInProduction)
        ));
    }

    #[test]
    fn a_partial_tls_identity_is_refused() {
        assert!(matches!(
            load_tls_config(
                DeploymentMode::Development,
                Some(Path::new("certificate.pem")),
                None
            ),
            Err(StartupError::IncompleteTlsConfiguration)
        ));
    }

    #[test]
    fn listener_defaults_follow_the_transport() {
        assert_eq!(listen_address(None, false).port(), 5672);
        assert_eq!(
            listen_address(None, true).port(),
            protocol_amqp::AMQP_TLS_PORT
        );
    }

    #[test]
    fn websocket_listener_is_opt_in_and_has_an_independent_address() {
        let defaults = Arguments::try_parse_from(["switchyard"]).expect("default arguments");
        assert!(defaults.websocket_listen.is_none());
        let configured = Arguments::try_parse_from([
            "switchyard",
            "--listen",
            "127.0.0.1:5672",
            "--websocket-listen",
            "127.0.0.1:8080",
            "--admin-listen",
            "127.0.0.1:9080",
        ])
        .expect("independent listener arguments");
        assert_eq!(configured.listen.expect("raw listener").port(), 5672);
        assert_eq!(
            configured
                .websocket_listen
                .expect("WebSocket listener")
                .port(),
            8080
        );
        assert_eq!(
            configured.admin_listen.expect("admin listener").port(),
            9080
        );
    }

    #[test]
    fn experimental_atomic_messaging_listener_is_opt_in_and_independent() {
        let defaults = Arguments::try_parse_from(["switchyard"]).expect("default arguments");
        assert!(defaults.experimental_atomic_messaging_listen.is_none());
        let configured = Arguments::try_parse_from([
            "switchyard",
            "--listen",
            "127.0.0.1:5672",
            "--websocket-listen",
            "127.0.0.1:8080",
            "--admin-listen",
            "127.0.0.1:9080",
            "--experimental-atomic-messaging-listen",
            "127.0.0.1:5673",
        ])
        .expect("separate experimental listener arguments");
        assert_eq!(configured.listen.expect("ordinary listener").port(), 5672);
        assert_eq!(
            configured
                .websocket_listen
                .expect("WebSocket listener")
                .port(),
            8080
        );
        assert_eq!(
            configured.admin_listen.expect("admin listener").port(),
            9080
        );
        assert_eq!(
            configured
                .experimental_atomic_messaging_listen
                .expect("experimental listener")
                .port(),
            5673
        );
        assert!(
            validate_experimental_atomic_messaging_listener(
                DeploymentMode::Development,
                configured.experimental_atomic_messaging_listen,
            )
            .is_ok()
        );
    }

    #[test]
    fn malformed_experimental_listener_address_is_refused_by_the_parser() {
        assert!(
            Arguments::try_parse_from([
                "switchyard",
                "--experimental-atomic-messaging-listen",
                "not-a-socket-address",
            ])
            .is_err()
        );
    }

    #[test]
    fn production_without_experimental_listener_retains_its_existing_validation() {
        assert!(
            validate_experimental_atomic_messaging_listener(DeploymentMode::Production, None,)
                .is_ok()
        );
        let arguments = Arguments::try_parse_from(["switchyard", "--mode", "production"])
            .expect("ordinary production arguments");
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::TlsRequiredInProduction)
        );
    }

    #[test]
    fn experimental_listener_is_refused_before_credentials_or_storage_are_opened() {
        let directory = tempfile::TempDir::new().expect("temporary startup directory");
        let store = directory.path().join("unopened-store");
        let certificate = directory.path().join("missing-certificate.pem");
        let private_key = directory.path().join("missing-private-key.pem");
        let shared_key = directory.path().join("missing-shared-key");
        let arguments = Arguments::try_parse_from([
            "switchyard",
            "--mode",
            "production",
            "--voters",
            "2",
            "--experimental-atomic-messaging-listen",
            "127.0.0.1:0",
            "--storage",
            "fjall",
            "--data-dir",
            store.to_str().expect("test store path"),
            "--tls-certificate",
            certificate.to_str().expect("test certificate path"),
            "--tls-private-key",
            private_key.to_str().expect("test private-key path"),
            "--shared-access-key-name",
            "rule",
            "--shared-access-key-file",
            shared_key.to_str().expect("test shared-key path"),
        ])
        .expect("unsupported production listener arguments");
        assert_eq!(
            run_with_arguments(arguments),
            Err(StartupError::ExperimentalAtomicMessagingInProduction)
        );
        assert!(!store.exists());
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("startup directory")
                .count(),
            0
        );
        assert_eq!(
            StartupError::ExperimentalAtomicMessagingInProduction.to_string(),
            "--experimental-atomic-messaging-listen is only available in development mode"
        );
    }

    #[test]
    fn production_requires_authentication_after_tls_is_configured() {
        assert!(matches!(
            load_shared_access_authentication(
                DeploymentMode::Production,
                true,
                "tenant.servicebus.windows.net",
                None,
                None,
            ),
            Err(StartupError::AuthenticationRequiredInProduction)
        ));
    }

    #[test]
    fn authentication_is_never_sent_over_plaintext() {
        assert!(matches!(
            load_shared_access_authentication(
                DeploymentMode::Development,
                false,
                "tenant.servicebus.windows.net",
                Some("rule"),
                Some(Path::new("key")),
            ),
            Err(StartupError::AuthenticationRequiresTls)
        ));
    }
}
