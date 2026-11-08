#![forbid(unsafe_code)]

use std::{
    fs,
    io::{self, Read, Write},
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    process::{self, ExitCode},
    time::Duration,
};

#[cfg(debug_assertions)]
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use cluster::{ClusterConfig, DeploymentMode};
use protocol_amqp::{
    AmqpListener, AmqpListenerExit, AmqpListenerFailure, AmqpListenerRetirement,
    DEFAULT_ADMISSION_TIMEOUT, SharedAccessAuthentication, namespace_from_hostname,
    tls_server_config,
};
use rustls::ServerConfig;
use server::{
    AdminListenerService, AdminRetirement, AdminTlsConfig, Broker, BrokerHandle,
    DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_SWEEP_INTERVAL, LocalProposer, NodeState,
    SHUTDOWN_EXPIRED_EXIT_CODE, ShutdownBudget, ShutdownExpired, ShutdownTrigger, StartupError,
    StorageChoice, SystemClock, TimerThread, serve_transports, shut_down_with_admin,
    stop_timer_and_broker,
};
#[cfg(debug_assertions)]
use server::{Shutdown, TimerWorker};
use tokio::time::Instant;
use tracing::{error, info};
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

    /// AMQP transport exposed by this listener.
    #[arg(long, value_enum, default_value_t = TransportArgument::AmqpTcp)]
    transport: TransportArgument,

    /// Where to accept AMQP connections. Defaults to 443 for WebSockets, 5671
    /// for AMQP/TLS, and 5672 for development plaintext.
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// PEM certificate chain for AMQP over TLS or WebSockets.
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

    /// Most live sockets the listener holds at once, counting TLS, WebSocket
    /// upgrade and AMQP handshakes as well as open connections. At the limit
    /// new peers wait in the OS backlog until a held connection ends.
    /// Unbounded when omitted.
    #[arg(long, value_name = "COUNT")]
    max_connections: Option<NonZeroUsize>,

    /// Seconds an accepted socket has to finish TLS, any WebSocket upgrade,
    /// SASL and AMQP Open, as one budget that is not reset between stages.
    /// A connection still handshaking at the deadline is closed.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = default_admission_timeout_secs()
    )]
    admission_timeout_secs: NonZeroU64,

    /// Seconds the whole shutdown may take after SIGTERM or SIGINT: stopping
    /// admission, draining connections, stopping the timer and the broker. A
    /// shutdown that overruns exits with status 3 without waiting further.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = default_shutdown_timeout_secs()
    )]
    shutdown_timeout_secs: NonZeroU64,

    /// The namespace this node serves. A hostname is accepted and its first
    /// label taken, so a deployment can name namespaces in DNS.
    #[arg(long, default_value = "development")]
    namespace: String,

    /// Explicitly enable native EntityService administration on this TLS/h2 address.
    #[arg(long)]
    admin_listen: Option<SocketAddr>,

    /// PEM certificate chain for the native administration listener.
    #[arg(long, value_name = "PATH")]
    admin_tls_certificate: Option<PathBuf>,

    /// PEM private key for --admin-tls-certificate (never a command-line secret).
    #[arg(long, value_name = "PATH")]
    admin_tls_private_key: Option<PathBuf>,

    /// Offline JWT policy JSON with pinned public keys and local scope/rights bindings.
    #[arg(long, value_name = "PATH")]
    admin_jwt_policy_file: Option<PathBuf>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TransportArgument {
    #[value(name = "amqp-tcp")]
    AmqpTcp,
    #[value(name = "amqp-websockets")]
    AmqpWebSockets,
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

fn load_admin_config(arguments: &Arguments) -> Result<Option<AdminTlsConfig>, StartupError> {
    let invalid = || {
        StartupError::AdminConfiguration(String::from(
            "requires explicit --admin-listen, --admin-tls-certificate, --admin-tls-private-key and --admin-jwt-policy-file",
        ))
    };
    let paths = match (
        arguments.admin_listen,
        arguments.admin_tls_certificate.as_deref(),
        arguments.admin_tls_private_key.as_deref(),
        arguments.admin_jwt_policy_file.as_deref(),
    ) {
        (None, None, None, None) => return Ok(None),
        (Some(_), Some(cert), Some(key), Some(policy)) => (cert, key, policy),
        _ => return Err(invalid()),
    };
    let tls = tls_server_config(&read_tls_file(paths.0)?, &read_tls_file(paths.1)?)?;
    // Match the offline profile's 64 KiB bound, reading one extra byte so an
    // oversized file is refused by the policy parser rather than truncated.
    let mut json = String::new();
    fs::File::open(paths.2)
        .and_then(|file| file.take(64 * 1024 + 1).read_to_string(&mut json))
        .map_err(|_| {
            StartupError::AdminConfiguration(String::from(
                "could not read native administration JWT policy",
            ))
        })?;
    let policy = auth::JwtPolicy::from_json(&json)
        .map_err(|error| StartupError::AdminConfiguration(error.to_string()))?;
    let host = if arguments.namespace.contains('.') {
        arguments.namespace.to_ascii_lowercase()
    } else {
        format!("{}.servicebus.windows.net", arguments.namespace)
    };
    let root = auth::ResourceScope::namespace(&host)?;
    AdminTlsConfig::new(tls, policy, root)
        .map(Some)
        .map_err(|error| StartupError::AdminConfiguration(error.to_owned()))
}

fn listen_address(
    configured: Option<SocketAddr>,
    transport: TransportArgument,
    tls: bool,
) -> SocketAddr {
    configured.unwrap_or_else(|| {
        SocketAddr::from((
            [127, 0, 0, 1],
            match (transport, tls) {
                (TransportArgument::AmqpWebSockets, _) => protocol_amqp::AMQP_WEBSOCKET_PORT,
                (TransportArgument::AmqpTcp, true) => protocol_amqp::AMQP_TLS_PORT,
                (TransportArgument::AmqpTcp, false) => 5672,
            },
        ))
    })
}

fn validate_transport_security(
    transport: TransportArgument,
    tls: bool,
) -> Result<(), StartupError> {
    if transport == TransportArgument::AmqpWebSockets && !tls {
        Err(StartupError::WebSocketsRequireTls)
    } else {
        Ok(())
    }
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
/// than in the derived debug form `Termination` would print. A shutdown that
/// finishes within its budget exits successfully.
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("switchyard: {error}");
            ExitCode::FAILURE
        }
    }
}

fn default_admission_timeout_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_ADMISSION_TIMEOUT.as_secs())
        .expect("the default budget is whole nonzero seconds")
}

fn default_shutdown_timeout_secs() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_SHUTDOWN_TIMEOUT.as_secs())
        .expect("the default budget is whole nonzero seconds")
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
    validate_transport_security(arguments.transport, tls.is_some())?;
    let shared_access_authentication = load_shared_access_authentication(
        mode,
        tls.is_some(),
        &arguments.namespace,
        arguments.shared_access_key_name.as_deref(),
        arguments.shared_access_key_file.as_deref(),
    )?;
    let listen = listen_address(arguments.listen, arguments.transport, tls.is_some());
    let admin_config = load_admin_config(&arguments)?;
    let namespace = namespace_from_hostname(&arguments.namespace)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| StartupError::Runtime(error.to_string()))?;
    let state = server::open(cluster, storage_choice(&arguments)?)?;

    info!(
        ?mode,
        voters = arguments.voters,
        storage = ?arguments.storage,
        transport = ?arguments.transport,
        "configuration is valid"
    );
    let broker = match state {
        NodeState::Memory(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
        NodeState::Durable(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
    };
    let handle = broker.handle();
    let mut broker = Some(broker);
    let mut timer = Some(
        spawn_timer(
            handle.clone(),
            Duration::from_millis(arguments.sweep_interval_millis),
        )
        .map_err(|error| StartupError::Runtime(error.to_string()))?,
    );
    let shutdown_timeout = Duration::from_secs(arguments.shutdown_timeout_secs.get());

    let served = runtime.block_on(async {
        // Registered before the listener exists, so a signal during startup
        // waits for the listener rather than killing the process outright.
        let signals =
            ProcessSignals::register().map_err(|error| StartupError::Runtime(error.to_string()))?;
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|error| StartupError::Listen {
                address: listen.to_string(),
                detail: error.to_string(),
            })?;
        let address = listener.local_addr().unwrap_or(listen);
        // Bind both sockets before starting either transport's serving tasks.
        // A second-bind failure drops the first socket, with no detached service.
        let mut admin = match (arguments.admin_listen, admin_config) {
            (Some(address), Some(config)) => {
                let socket = tokio::net::TcpListener::bind(address).await.map_err(|error| StartupError::Listen { address: address.to_string(), detail: error.to_string() })?;
                let service = AdminListenerService::new(socket, config, handle.clone(), namespace.clone(), Duration::from_secs(arguments.admission_timeout_secs.get()));
                info!(address = %service.local_addr().map_err(|error| StartupError::Runtime(error.to_string()))?, "native administration TLS listener bound; authenticated RPC required for readiness");
                Some(service)
            },
            _ => None,
        };
        info!(address = %address, namespace = %namespace, tls = tls.is_some(), transport = ?arguments.transport, max_connections = ?arguments.max_connections, admission_timeout_secs = arguments.admission_timeout_secs, shutdown_timeout_secs = arguments.shutdown_timeout_secs, "accepting AMQP connections");
        let amqp = AmqpListener::new(handle, namespace).with_admission_timeout(
            Duration::from_secs(arguments.admission_timeout_secs.get()),
        );
        let amqp = match arguments.max_connections {
            Some(limit) => amqp.with_max_connections(limit),
            None => amqp,
        };
        let amqp = match tls {
            Some(config) => amqp.with_tls(config),
            None => amqp,
        };
        let amqp = match shared_access_authentication {
            Some(authentication) => amqp.with_shared_access_authentication(authentication),
            None => amqp,
        };
        let mut service = match arguments.transport {
            TransportArgument::AmqpTcp => amqp.into_tcp_service(listener),
            TransportArgument::AmqpWebSockets => amqp.into_wss_service(listener),
        };
        let trigger = ShutdownTrigger::default();
        let (stop_signals, stopped) = tokio::sync::oneshot::channel();
        let signal_task = tokio::spawn({
            let trigger = trigger.clone();
            let retirement = service.retirement_handle();
            let admin_retirement = admin.as_ref().map(AdminListenerService::retirement_handle);
            async move {
                tokio::select! {
                    () = forward_signals(signals, trigger, retirement, admin_retirement) => {},
                    _ = stopped => {},
                }
            }
        });

        // Both original owners stay here if select cancels the other accept
        // borrower. Either terminal transport retires the other immediately.
        serve_transports(&mut service, admin.as_mut(), &trigger).await;
        let started = trigger.started().unwrap_or_else(Instant::now);
        let budget = ShutdownBudget::new(started, shutdown_timeout);
        service.retirement_handle().request();
        if let Some(admin) = admin.as_ref() { admin.retirement_handle().request(); }
        let result = match shut_down_with_admin(&budget, &mut service, admin.as_mut(), &mut timer, &mut broker).await {
            Ok(exit) => {
                info!(
                    elapsed_millis =
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "shutdown complete"
                );
                match exit {
                    Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error)))) => {
                        Err(StartupError::Runtime(error.to_string()))
                    }
                    _ => match admin.as_mut().and_then(AdminListenerService::take_failure) {
                        Some(error) => Err(StartupError::Runtime(error.to_string())),
                        None => Ok(()),
                    },
                }
            }
            Err(expired) => abandon_shutdown(expired, shutdown_timeout),
        };
        let _ = stop_signals.send(());
        signal_task.await.map_err(|error| StartupError::Runtime(error.to_string()))?;
        result
    });

    // Reached with the timer or broker still running only when startup failed
    // inside the runtime; a completed shutdown has already stopped both. The
    // signal handlers are still installed, so this teardown gets the same
    // budget rather than an unbounded wait nobody can interrupt.
    if timer.is_some() || broker.is_some() {
        let budget = ShutdownBudget::new(Instant::now(), shutdown_timeout);
        let stopped = runtime.block_on(stop_timer_and_broker(&budget, &mut timer, &mut broker));
        if let Err(expired) = stopped {
            if let Err(error) = &served {
                eprintln!("switchyard: {error}");
            }
            abandon_shutdown(expired, shutdown_timeout);
        }
    }
    served
}

/// The variable a debug build reads to hold the timer stage open for that
/// many milliseconds after the timer is told to stop. Tests use it to stall
/// shutdown from outside the process; release builds ignore it.
#[cfg(debug_assertions)]
const STALL_TIMER_VARIABLE: &str = "SWITCHYARD_TEST_STALL_TIMER_MILLIS";

fn spawn_timer(handle: BrokerHandle, interval: Duration) -> io::Result<TimerThread> {
    #[cfg(debug_assertions)]
    if let Some(stall) = std::env::var(STALL_TIMER_VARIABLE)
        .ok()
        .and_then(|millis| millis.parse().ok())
        .map(Duration::from_millis)
    {
        let latch = Arc::new(Shutdown::default());
        let thread = std::thread::Builder::new()
            .name(String::from("switchyard-timer"))
            .spawn({
                let latch = Arc::clone(&latch);
                move || {
                    TimerWorker::new(&handle).run(interval, &latch);
                    std::thread::sleep(stall);
                }
            })?;
        return Ok(TimerThread::new(latch, thread));
    }
    TimerThread::spawn(handle, interval)
}

/// Leaves the remaining stages running and exits at once with the status
/// reserved for an expired shutdown. Nothing is settled on the way out; work
/// the broker never committed was never acknowledged.
fn abandon_shutdown(expired: ShutdownExpired, budget: Duration) -> ! {
    error!(
        stage = %expired.stage,
        budget_secs = budget.as_secs(),
        "shutdown budget expired; exiting without finishing the remaining stages"
    );
    eprintln!("switchyard: {expired}");
    let _ = io::stdout().flush();
    process::exit(i32::from(SHUTDOWN_EXPIRED_EXIT_CODE))
}

/// SIGTERM and SIGINT, the two requests to stop.
#[cfg(unix)]
struct ProcessSignals {
    terminate: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ProcessSignals {
    fn register() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    async fn next(&mut self) -> Option<&'static str> {
        tokio::select! {
            received = self.terminate.recv() => received.map(|()| "SIGTERM"),
            received = self.interrupt.recv() => received.map(|()| "SIGINT"),
        }
    }
}

/// Ctrl-C, the only request to stop a Windows host delivers. Registered
/// here, before the listener binds, like the Unix signals.
#[cfg(windows)]
struct ProcessSignals {
    ctrl_c: tokio::signal::windows::CtrlC,
}

#[cfg(windows)]
impl ProcessSignals {
    fn register() -> io::Result<Self> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
        })
    }

    async fn next(&mut self) -> Option<&'static str> {
        self.ctrl_c.recv().await.map(|()| "ctrl-c")
    }
}

/// Starts shutdown on the first signal; later ones join it.
async fn forward_signals(
    mut signals: ProcessSignals,
    trigger: ShutdownTrigger,
    retirement: AmqpListenerRetirement,
    admin_retirement: Option<AdminRetirement>,
) {
    while let Some(signal) = signals.next().await {
        if trigger.request() {
            info!(signal = %signal, "shutting down");
            retirement.request();
            if let Some(admin) = admin_retirement.as_ref() {
                admin.request();
            }
        } else {
            info!(signal = %signal, "shutdown already in progress");
        }
    }
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
        assert_eq!(
            listen_address(None, TransportArgument::AmqpTcp, false).port(),
            5672
        );
        assert_eq!(
            listen_address(None, TransportArgument::AmqpTcp, true).port(),
            protocol_amqp::AMQP_TLS_PORT
        );
        assert_eq!(
            listen_address(None, TransportArgument::AmqpWebSockets, true).port(),
            protocol_amqp::AMQP_WEBSOCKET_PORT
        );
    }

    #[test]
    fn the_connection_limit_is_unbounded_unless_a_positive_count_is_given() {
        let parse = |extra: &[&str]| {
            Arguments::try_parse_from(["switchyard"].iter().chain(extra))
                .map(|arguments| arguments.max_connections)
        };
        assert_eq!(parse(&[]).unwrap(), None);
        assert_eq!(
            parse(&["--max-connections", "64"]).unwrap(),
            NonZeroUsize::new(64)
        );
        for invalid in ["0", "-1", "many", ""] {
            assert!(
                parse(&["--max-connections", invalid]).is_err(),
                "{invalid:?} is refused"
            );
        }
    }

    #[test]
    fn the_admission_timeout_defaults_to_sixty_seconds_and_refuses_zero() {
        let parse = |extra: &[&str]| {
            Arguments::try_parse_from(["switchyard"].iter().chain(extra))
                .map(|arguments| arguments.admission_timeout_secs.get())
        };
        assert_eq!(parse(&[]).unwrap(), 60);
        assert_eq!(
            Duration::from_secs(parse(&[]).unwrap()),
            DEFAULT_ADMISSION_TIMEOUT
        );
        assert_eq!(parse(&["--admission-timeout-secs", "5"]).unwrap(), 5);
        for invalid in ["0", "-1", "soon", "1.5", ""] {
            assert!(
                parse(&["--admission-timeout-secs", invalid]).is_err(),
                "{invalid:?} is refused"
            );
        }
    }

    #[test]
    fn the_shutdown_timeout_defaults_to_thirty_seconds_and_refuses_zero() {
        let parse = |extra: &[&str]| {
            Arguments::try_parse_from(["switchyard"].iter().chain(extra))
                .map(|arguments| arguments.shutdown_timeout_secs.get())
        };
        assert_eq!(parse(&[]).unwrap(), 30);
        assert_eq!(
            Duration::from_secs(parse(&[]).unwrap()),
            DEFAULT_SHUTDOWN_TIMEOUT
        );
        assert_eq!(parse(&["--shutdown-timeout-secs", "5"]).unwrap(), 5);
        for invalid in ["0", "-1", "soon", "1.5", ""] {
            assert!(
                parse(&["--shutdown-timeout-secs", invalid]).is_err(),
                "{invalid:?} is refused"
            );
        }
    }

    #[test]
    fn an_expired_shutdown_has_its_own_exit_status() {
        assert_ne!(SHUTDOWN_EXPIRED_EXIT_CODE, 0);
        assert_ne!(
            ExitCode::from(SHUTDOWN_EXPIRED_EXIT_CODE),
            ExitCode::FAILURE,
            "a refused startup and an overrun shutdown are told apart"
        );
    }

    #[test]
    fn websockets_are_never_exposed_without_tls() {
        assert_eq!(
            validate_transport_security(TransportArgument::AmqpWebSockets, false),
            Err(StartupError::WebSocketsRequireTls)
        );
        assert_eq!(
            validate_transport_security(TransportArgument::AmqpWebSockets, true),
            Ok(())
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
