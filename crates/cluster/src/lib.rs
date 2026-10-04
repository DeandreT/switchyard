#![forbid(unsafe_code)]

pub mod experimental_log;
mod experimental_owner;
pub mod experimental_replica;
pub mod experimental_runtime;
pub mod experimental_state_machine;
mod snapshot_data;

pub use snapshot_data::{BoundedSnapshotData, MAX_SNAPSHOT_BYTES};

pub use experimental_log::{
    ExperimentalLogStore, LogCodecError, LogEntry, LogId, LogProfile, LogResource, LogStorageError,
    LogTypes, LogVote, LogWorkload, MAX_APPEND_BYTES, MAX_APPEND_ENTRIES, MAX_LIMITED_BYTES,
    MAX_LIMITED_ENTRIES, MAX_LOG_BODY_BYTES, MAX_LOG_ENTRY_BYTES, MAX_LOG_MEMBERSHIP_BYTES,
    MAX_LOG_METADATA_BYTES, MAX_LOG_OWNER_BYTES, MAX_LOG_OWNER_JOBS, MAX_LOG_QUEUE_BYTES,
    MAX_RETAINED_BYTES, MAX_RETAINED_ENTRIES, QueueLogCommand, ReadOnlyLogReader,
};

pub use experimental_state_machine::{
    AppliedState, BuiltNativeSnapshotCatalog, DecodedNativeSnapshotPair,
    EncodedNativeSnapshotMetadata, ExperimentalStateMachine, LogApplication, LogQueueConfigRefusal,
    LogQueueRefusal, MAX_APPLY_BYTES, MAX_APPLY_ENTRIES,
    MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES, MAX_NATIVE_SNAPSHOT_METADATA_BYTES,
    MAX_STATE_MACHINE_OWNER_BYTES, MAX_STATE_MACHINE_OWNER_JOBS, NativeSnapshotMetadataError,
    RetainedNativeSnapshotCatalog, StateMachineCatalogError, StateMachineError,
    StateMachineImageBootstrapError, StateMachineImageExportError, StateMachineWorkload,
    UnsupportedSnapshotBuilder,
};

pub use experimental_replica::{
    ExperimentalReplicaStores, MAX_REPLICA_PAYLOAD_ENTRIES, ReplicaPreparationError,
    ReplicaProgress,
};

pub use experimental_runtime::{
    ClientWorkload, ExperimentalRaftCluster, ExperimentalRaftHandle, QueueIntent, QueueWriteError,
    QueueWriteOutcome, QueueWriteRejection, QueueWriteResult, QueueWriteUnknown,
    RejoinAdmissionError, ReplicaRuntimeError, TransportWorkload,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PRODUCTION_MINIMUM_VOTERS: u16 = 3;
pub const PRODUCTION_REPLICATION_FACTOR: u16 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    Development,
    Production,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub mode: DeploymentMode,
    pub voters: u16,
}

impl ClusterConfig {
    pub fn validate(self) -> Result<Self, ClusterConfigError> {
        match self.mode {
            DeploymentMode::Development if self.voters == 1 => Ok(self),
            DeploymentMode::Development => Err(ClusterConfigError::DevelopmentRequiresOneVoter),
            DeploymentMode::Production
                if self.voters >= PRODUCTION_MINIMUM_VOTERS && self.voters % 2 == 1 =>
            {
                Ok(self)
            }
            DeploymentMode::Production => Err(ClusterConfigError::ProductionRequiresOddQuorum),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ClusterConfigError {
    #[error("development mode requires exactly one voter")]
    DevelopmentRequiresOneVoter,
    #[error("production mode requires an odd voter count of at least three")]
    ProductionRequiresOddQuorum,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_rejects_two_voters() {
        let config = ClusterConfig {
            mode: DeploymentMode::Production,
            voters: 2,
        };
        assert_eq!(
            config.validate(),
            Err(ClusterConfigError::ProductionRequiresOddQuorum)
        );
    }

    #[test]
    fn production_accepts_three_voters() {
        let config = ClusterConfig {
            mode: DeploymentMode::Production,
            voters: 3,
        };
        assert_eq!(config.validate(), Ok(config));
    }
}
