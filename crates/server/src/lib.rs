//! The broker process: the runtime that drives the deterministic state machine.
//!
//! The `domain` crate decides what a command means and `storage` decides where
//! the result lives. Neither reads a clock, opens a directory, or runs a thread.
//! This crate does all three: it picks a backend, stamps commands with real
//! time, and runs the worker that proposes expiry.

#![forbid(unsafe_code)]

mod atom_admin;
mod broker;
mod clock;
mod maintenance;
mod native_admin;
mod native_admin_listener;
mod proposer;
mod timer;

use std::path::PathBuf;

use cluster::{ClusterConfig, DeploymentMode};
use domain::StateMachine;
use storage::{FjallStore, MemoryStore, StorageError};
use thiserror::Error;

pub use crate::{
    atom_admin::{AtomAdminError, AtomAdminListener},
    broker::{
        AtomQueueOwnerError, AtomRuleDefinition, AtomRuleOwnerError, AtomSubscriptionOwnerError,
        Broker, BrokerHandle, GuardedAtomicSubmitError, NativeAtomicMessagingCompletion,
        NativeAtomicSubmitError, SubmitError,
    },
    clock::{Clock, ManualClock, SystemClock},
    maintenance::MaintenanceClockAssessment,
    native_admin::{MAX_NATIVE_QUEUE_SCAN_ROUNDS, MAX_NATIVE_QUEUE_SCAN_ROWS, NativeAdminService},
    native_admin_listener::{
        DEFAULT_NATIVE_ADMIN_CONNECTION_LIMIT, NATIVE_ADMIN_DEVELOPMENT_PORT,
        NATIVE_ADMIN_REQUEST_LIMIT, NATIVE_ADMIN_RESPONSE_LIMIT, NATIVE_ADMIN_TLS_PORT,
        NativeAdminError, NativeAdminListener,
    },
    proposer::{AdminTarget, DEFAULT_MAX_CLOCK_REGRESSION_MILLIS, LocalProposer, ProposeError},
    timer::{
        DEFAULT_SWEEP_INTERVAL, MAX_QUEUES_PER_SWEEP, MAX_ROUNDS_PER_INDEX, MAX_TOPICS_PER_SWEEP,
        Shutdown, SweepReport, TimerWorker,
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

/// Validates a configuration and opens development state on the chosen backend.
///
/// Production is refused before opening storage: memory cannot provide durable
/// state, and the quorum replication required for durable production is not yet
/// implemented. Cluster validation takes precedence over either refusal.
pub fn open(cluster: ClusterConfig, storage: StorageChoice) -> Result<NodeState, StartupError> {
    validate_storage_configuration(cluster, &storage)?;
    match storage {
        StorageChoice::Memory => Ok(NodeState::Memory(StateMachine::new(MemoryStore::default()))),
        StorageChoice::Durable { directory } => Ok(NodeState::Durable(StateMachine::new(
            FjallStore::open(directory)?,
        ))),
    }
}

/// Checks cluster and backend policy without opening or inspecting storage.
///
/// This does not check directory access, store format, or runtime readiness.
/// Cluster validation takes precedence over the production backend refusals.
pub fn validate_storage_configuration(
    cluster: ClusterConfig,
    storage: &StorageChoice,
) -> Result<(), StartupError> {
    cluster.validate()?;
    match (cluster.mode, storage) {
        (DeploymentMode::Production, StorageChoice::Memory) => {
            Err(StartupError::MemoryStorageInProduction)
        }
        (DeploymentMode::Production, StorageChoice::Durable { .. }) => {
            Err(StartupError::ReplicationUnavailableInProduction)
        }
        _ => Ok(()),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StartupError {
    #[error("the diagnostic logger could not be initialized")]
    LoggingInitialization,
    #[error("production mode cannot run on in-memory storage")]
    MemoryStorageInProduction,
    #[error("production mode requires quorum replication, which is not implemented")]
    ReplicationUnavailableInProduction,
    #[error("--experimental-atomic-messaging-listen is only available in development mode")]
    ExperimentalAtomicMessagingInProduction,
    #[error("--development-maintenance-readiness is only available in development mode")]
    DevelopmentMaintenanceReadinessInProduction,
    #[error("--development-maintenance-readiness requires --admin-listen")]
    DevelopmentMaintenanceReadinessRequiresAdminListener,
    #[error("the durable backend needs a data directory")]
    MissingDataDirectory,
    #[error("--sweep-interval-millis must be greater than zero")]
    ZeroSweepInterval,
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
    #[error("production mode requires a shared-access policy")]
    AuthenticationRequiredInProduction,
    #[error(
        "shared-access authentication requires both --shared-access-key-name and --shared-access-key-file"
    )]
    IncompleteSharedAccessPolicy,
    #[error("shared-access authentication cannot be enabled on a plaintext listener")]
    AuthenticationRequiresTls,
    #[error("could not read the shared-access key from {path}: {detail}")]
    ReadSharedAccessKey { path: PathBuf, detail: String },
    #[error("offline JWT authentication requires TLS")]
    OfflineJwtRequiresTls,
    #[error("offline JWT authentication requires configured shared-access authentication")]
    OfflineJwtRequiresSharedAccess,
    #[error("could not read the offline JWT policy file")]
    ReadOfflineJwtPolicy,
    #[error("the offline JWT policy must be a regular file")]
    OfflineJwtPolicyNotRegularFile,
    #[error("the offline JWT policy file exceeds 64 KiB")]
    OfflineJwtPolicyTooLarge,
    #[error("the offline JWT policy file must contain UTF-8 JSON")]
    OfflineJwtPolicyNotUtf8,
    #[error(transparent)]
    OfflineJwtPolicyConfiguration(#[from] auth::JwtError),
    #[error("Atom administration options require --atom-admin-listen")]
    AtomAdminRequiresListener,
    #[error("Atom administration requires an audience host, key name and key file")]
    IncompleteAtomAdminConfiguration,
    #[error("Atom administration requires TLS")]
    AtomAdminRequiresTls,
    #[error("the Atom administration audience must be a namespace host")]
    AtomAdminInvalidAudience,
    #[error("the Atom administration key name cannot be empty")]
    AtomAdminInvalidKeyName,
    #[error("could not read the Atom administration key file")]
    ReadAtomAdminKey,
    #[error("the Atom administration key must be a regular file")]
    AtomAdminKeyNotRegularFile,
    #[error("the Atom administration key file exceeds 8 KiB")]
    AtomAdminKeyTooLarge,
    #[error("the Atom administration key file must contain UTF-8")]
    AtomAdminKeyNotUtf8,
    #[error("the Atom administration key cannot be empty")]
    AtomAdminInvalidKey,
    #[error("the Atom administration configuration is invalid")]
    AtomAdminConfiguration,
    #[error(transparent)]
    AuthPolicy(#[from] auth::PolicyError),
    #[error(transparent)]
    AuthScope(#[from] auth::ResourceScopeError),
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
    fn production_is_refused_before_a_durable_directory_or_its_parent_is_created() {
        for voters in [3, 5] {
            let root = TempDir::new().expect("a temporary startup directory");
            let parent = root.path().join("unopened-parent");
            let directory = parent.join("unopened-store");
            assert_eq!(
                open(
                    ClusterConfig {
                        mode: DeploymentMode::Production,
                        voters,
                    },
                    StorageChoice::Durable {
                        directory: directory.clone(),
                    },
                )
                .err(),
                Some(StartupError::ReplicationUnavailableInProduction),
            );
            assert!(!directory.exists());
            assert!(!parent.exists());
            assert_eq!(
                std::fs::read_dir(root.path())
                    .expect("the startup directory is readable")
                    .count(),
                0,
            );
        }
    }

    #[test]
    fn production_replication_refusal_has_a_static_description() {
        assert_eq!(
            StartupError::ReplicationUnavailableInProduction.to_string(),
            "production mode requires quorum replication, which is not implemented",
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
    fn an_invalid_development_cluster_is_refused_before_directory_creation() {
        let root = TempDir::new().expect("a temporary startup directory");
        let parent = root.path().join("unopened-parent");
        let directory = parent.join("unopened-store");
        assert_eq!(
            open(
                ClusterConfig {
                    mode: DeploymentMode::Development,
                    voters: 2,
                },
                StorageChoice::Durable {
                    directory: directory.clone(),
                },
            )
            .err(),
            Some(StartupError::Cluster(
                cluster::ClusterConfigError::DevelopmentRequiresOneVoter,
            )),
        );
        assert!(!directory.exists());
        assert!(!parent.exists());
        assert_eq!(
            std::fs::read_dir(root.path())
                .expect("the startup directory is readable")
                .count(),
            0,
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
