#![forbid(unsafe_code)]

mod native;

use std::{path::PathBuf, process::ExitCode};

use admin_api::PROTOBUF_PACKAGE;
use clap::{Parser, Subcommand};
use cluster::{StoreUpgradeOutcome, upgrade_store};
use storage::{STORE_FORMAT_V2, STORE_FORMAT_V3};

#[derive(Debug, Parser)]
#[command(name = "switchyardctl", version, about = "Administer Switchyard")]
struct Arguments {
    #[command(flatten)]
    connection: native::Connection,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Native TLS queue Create/Get/List.
    Queue(native::EntityCommands),
    /// Native TLS topic Create/Get/List.
    Topic(native::EntityCommands),
    /// Native TLS explicit subscription Create/Get/List.
    Subscription(native::SubscriptionCommands),
    /// Print the native API package and implementation status.
    Compatibility,
    /// Offline maintenance of a durable store directory.
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Upgrade an existing store from format 2 to 3 after a bounded content
    /// preflight. Stop every server and writer using the directory first.
    Upgrade {
        /// The existing durable data directory; it is never created.
        #[arg(long)]
        data_dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match arguments.command {
        Command::Compatibility => {
            println!(
                "{PROTOBUF_PACKAGE}: HTTPS Create/Get/List queues, topics and explicit subscriptions; namespace-root Manage JWT required (not AMQP CBS); offline store upgrade 2 to 3; no Delete/Update or other native services"
            );
        }
        Command::Store {
            command: StoreCommand::Upgrade { data_dir },
        } => match upgrade_store(&data_dir) {
            Ok(StoreUpgradeOutcome::Upgraded) => println!(
                "upgraded store format {STORE_FORMAT_V2} to {STORE_FORMAT_V3}: {}",
                data_dir.display()
            ),
            Ok(StoreUpgradeOutcome::AlreadyCurrent) => println!(
                "store format is already {STORE_FORMAT_V3}: {}",
                data_dir.display()
            ),
            Err(error) => {
                eprintln!("switchyardctl: store upgrade refused: {error}");
                return ExitCode::FAILURE;
            }
        },
        remote => {
            let operation = match remote {
                Command::Queue(command) => command.operation(native::Kind::Queue),
                Command::Topic(command) => command.operation(native::Kind::Topic),
                Command::Subscription(command) => command.operation(),
                _ => unreachable!(),
            };
            if let Err(error) = native::run(arguments.connection, operation) {
                eprintln!("switchyardctl: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
