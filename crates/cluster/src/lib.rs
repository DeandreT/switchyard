#![forbid(unsafe_code)]

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
