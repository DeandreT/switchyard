//! The broker process: the runtime that drives the deterministic state machine.
//!
//! The `domain` crate decides what a command means and `storage` decides where
//! the result lives. Neither reads a clock, opens a directory, or runs a thread.
//! This crate does all three: it picks a backend, stamps commands with real
//! time, and runs the worker that proposes expiry.

#![forbid(unsafe_code)]

mod broker;
mod clock;
mod proposer;
mod timer;

use std::path::PathBuf;

use cluster::{ClusterConfig, DeploymentMode};
use domain::StateMachine;
use storage::{FjallStore, MemoryStore, StorageError};
use thiserror::Error;

pub use crate::{
    broker::{Broker, BrokerHandle, SubmitError},
    clock::{Clock, ManualClock, SystemClock},
    proposer::{DEFAULT_MAX_CLOCK_REGRESSION_MILLIS, LocalProposer, ProposeError},
    timer::{
        DEFAULT_SWEEP_INTERVAL, MAX_QUEUES_PER_SWEEP, MAX_ROUNDS_PER_INDEX, Shutdown, SweepReport,
        TimerWorker,
    },
};

/// Which backend a node runs its state on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageChoice {
    /// Loses everything when the process exits. Refused in production.
    Memory,
    Durable {
        directory: PathBuf,
    },
}

/// A node's state machine, over whichever backend was configured.
///
/// The two backends are separate types rather than one boxed trait object,
/// because `StateStore` is what every generic in the runtime is written against
/// and erasing it here would erase it everywhere above.
pub enum NodeState {
    Memory(StateMachine<MemoryStore>),
    Durable(StateMachine<FjallStore>),
}

/// Validates a configuration and opens the state it names.
///
/// Production is refused an in-memory store here rather than at the point a
/// message is lost.
pub fn open(cluster: ClusterConfig, storage: StorageChoice) -> Result<NodeState, StartupError> {
    cluster.validate()?;
    match (cluster.mode, storage) {
        (DeploymentMode::Production, StorageChoice::Memory) => {
            Err(StartupError::MemoryStorageInProduction)
        }
        (_, StorageChoice::Memory) => {
            Ok(NodeState::Memory(StateMachine::new(MemoryStore::default())))
        }
        (_, StorageChoice::Durable { directory }) => Ok(NodeState::Durable(StateMachine::new(
            FjallStore::open(directory)?,
        ))),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StartupError {
    #[error("production mode cannot run on in-memory storage")]
    MemoryStorageInProduction,
    #[error("the durable backend needs a data directory")]
    MissingDataDirectory,
    #[error("could not listen on {address}: {detail}")]
    Listen { address: String, detail: String },
    #[error("production mode requires a TLS certificate and private key")]
    TlsRequiredInProduction,
    #[error("TLS configuration requires both --tls-certificate and --tls-private-key")]
    IncompleteTlsConfiguration,
    #[error("could not read TLS credentials from {path}: {detail}")]
    ReadTlsCredentials { path: PathBuf, detail: String },
    #[error(transparent)]
    TlsConfiguration(#[from] protocol_amqp::TlsConfigurationError),
    #[error("the runtime could not be started: {0}")]
    Runtime(String),
    #[error(transparent)]
    Protocol(#[from] protocol_amqp::ProtocolError),
    #[error(transparent)]
    Cluster(#[from] cluster::ClusterConfigError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn development() -> ClusterConfig {
        ClusterConfig {
            mode: DeploymentMode::Development,
            voters: 1,
        }
    }

    #[test]
    fn production_cannot_run_on_memory_storage() {
        let production = ClusterConfig {
            mode: DeploymentMode::Production,
            voters: 3,
        };
        assert_eq!(
            open(production, StorageChoice::Memory).err(),
            Some(StartupError::MemoryStorageInProduction)
        );
    }

    #[test]
    fn an_invalid_cluster_is_refused_before_any_directory_is_touched() {
        let directory = TempDir::new().expect("a temporary directory");
        let two_voters = ClusterConfig {
            mode: DeploymentMode::Production,
            voters: 2,
        };
        assert_eq!(
            open(
                two_voters,
                StorageChoice::Durable {
                    directory: directory.path().to_path_buf()
                }
            )
            .err(),
            Some(StartupError::Cluster(
                cluster::ClusterConfigError::ProductionRequiresOddQuorum
            ))
        );
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("the directory is readable")
                .count(),
            0,
            "a rejected configuration should not have created a store"
        );
    }

    #[test]
    fn development_opens_a_durable_store_when_asked() -> Result<(), StartupError> {
        let directory = TempDir::new().expect("a temporary directory");
        let state = open(
            development(),
            StorageChoice::Durable {
                directory: directory.path().to_path_buf(),
            },
        )?;
        assert!(matches!(state, NodeState::Durable(_)));
        Ok(())
    }

    #[test]
    fn development_defaults_to_memory() -> Result<(), StartupError> {
        assert!(matches!(
            open(development(), StorageChoice::Memory)?,
            NodeState::Memory(_)
        ));
        Ok(())
    }
}
