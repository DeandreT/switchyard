#![forbid(unsafe_code)]

use std::{error::Error, fmt};

use clap::{Parser, ValueEnum};
use cluster::{ClusterConfig, DeploymentMode};
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

    #[arg(long, default_value_t = 1)]
    voters: u16,
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

#[derive(Debug)]
struct StartupError(&'static str);

impl fmt::Display for StartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for StartupError {}

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let arguments = Arguments::parse();
    let mode = DeploymentMode::from(arguments.mode);
    ClusterConfig {
        mode,
        voters: arguments.voters,
    }
    .validate()?;

    if mode == DeploymentMode::Production && arguments.storage == StorageArgument::Memory {
        return Err(Box::new(StartupError(
            "production mode cannot use in-memory storage",
        )));
    }
    if arguments.storage == StorageArgument::Fjall {
        return Err(Box::new(StartupError(
            "the Fjall backend has not been implemented yet",
        )));
    }

    info!(
        ?mode,
        voters = arguments.voters,
        storage = ?arguments.storage,
        "configuration is valid"
    );
    println!(
        "Switchyard pre-alpha configuration validated; protocol listeners are not implemented yet"
    );
    Ok(())
}
