#![forbid(unsafe_code)]

use std::{path::PathBuf, process::ExitCode, time::Duration};

use clap::{Parser, ValueEnum};
use cluster::{ClusterConfig, DeploymentMode};
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
    let state = server::open(cluster, storage_choice(&arguments)?)?;

    info!(
        ?mode,
        voters = arguments.voters,
        storage = ?arguments.storage,
        "configuration is valid"
    );
    println!(
        "Switchyard pre-alpha: expiry timers are running; protocol listeners are not implemented \
         yet, so nothing can connect. Interrupt to stop."
    );

    // Nothing here settles a message before its batch is fsynced, so an
    // interrupt at any point loses no acknowledged state. That is why there is
    // no signal handler yet: an abrupt stop is already safe.
    let shutdown = Shutdown::default();
    let interval = Duration::from_millis(arguments.sweep_interval_millis);
    let broker = match state {
        NodeState::Memory(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
        NodeState::Durable(machine) => Broker::spawn(LocalProposer::new(machine, SystemClock)),
    };
    TimerWorker::new(&broker.handle()).run(interval, &shutdown);
    Ok(())
}
