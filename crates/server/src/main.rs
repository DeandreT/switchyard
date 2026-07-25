#![forbid(unsafe_code)]

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
use protocol_amqp::{AmqpListener, namespace_from_hostname, tls_server_config};
use rustls::ServerConfig;
use server::{
    Broker, DEFAULT_SWEEP_INTERVAL, LocalProposer, NodeState, Shutdown, StartupError,
    StorageChoice, SystemClock, TimerWorker,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

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

    /// PEM certificate chain for AMQP over TLS.
    #[arg(long, value_name = "PATH")]
    tls_certificate: Option<PathBuf>,

    /// PEM private key corresponding to --tls-certificate.
    #[arg(long, value_name = "PATH")]
    tls_private_key: Option<PathBuf>,

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

fn load_tls_config(
    mode: DeploymentMode,
    certificate_path: Option<&Path>,
    private_key_path: Option<&Path>,
) -> Result<Option<ServerConfig>, StartupError> {
    let (certificate_path, private_key_path) = match (certificate_path, private_key_path) {
        (None, None) if mode == DeploymentMode::Production => {
            return Err(StartupError::TlsRequiredInProduction);
        }
        (None, None) => return Ok(None),
        (Some(certificate), Some(private_key)) => (certificate, private_key),
        _ => return Err(StartupError::IncompleteTlsConfiguration),
    };

    let certificate = read_tls_file(certificate_path)?;
    let private_key = read_tls_file(private_key_path)?;
    Ok(Some(tls_server_config(&certificate, &private_key)?))
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let arguments = Arguments::parse();
    let mode = DeploymentMode::from(arguments.mode);
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
        info!(address = %listen, namespace = %namespace, tls = tls.is_some(), "accepting AMQP connections");
        let amqp = AmqpListener::new(broker.handle(), namespace);
        let amqp = match tls {
            Some(config) => amqp.with_tls(config),
            None => amqp,
        };
        amqp.serve(listener)
            .await
            .map_err(|error| StartupError::Runtime(error.to_string()))
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
}
