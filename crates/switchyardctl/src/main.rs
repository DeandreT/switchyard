#![forbid(unsafe_code)]

use admin_api::PROTOBUF_PACKAGE;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "switchyardctl", version, about = "Administer Switchyard")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the native API package and implementation status.
    Compatibility,
}

fn main() {
    let arguments = Arguments::parse();
    match arguments.command {
        Command::Compatibility => {
            println!("{PROTOBUF_PACKAGE}: contract scaffolded, transport not implemented");
        }
    }
}
