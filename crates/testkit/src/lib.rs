//! Deterministic fixtures for exercising the broker without a cluster.
//!
//! The fixture drives a [`StateMachine`] over a memory store and requires every
//! command to carry an explicit timestamp. Tests advance time by choosing that
//! value, so lock expiry and time-to-live behavior are reproducible instead of
//! depending on how long a test takes to run.

#![forbid(unsafe_code)]

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, IdentifierError, NamespaceName,
    QueueConfig, StateMachine, Timestamp,
};
use storage::MemoryStore;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct QueueFixture {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub machine: StateMachine<MemoryStore>,
}

impl QueueFixture {
    /// Builds a fixture and creates the queue at time zero.
    pub fn new(namespace: &str, entity: &str, config: QueueConfig) -> Result<Self, FixtureError> {
        let fixture = Self {
            namespace: NamespaceName::new(namespace)?,
            entity: EntityPath::new(entity)?,
            machine: StateMachine::new(MemoryStore::default()),
        };
        fixture.at(0, CommandKind::CreateQueue { config })?;
        Ok(fixture)
    }

    pub fn with_defaults(namespace: &str, entity: &str) -> Result<Self, FixtureError> {
        Self::new(namespace, entity, QueueConfig::default())
    }

    /// Applies a command stamped at `millis` since the epoch.
    pub fn at(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&self.command(millis, kind))
    }

    pub fn command(&self, millis: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            self.entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        )
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FixtureError {
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_the_queue_it_is_asked_for() -> Result<(), FixtureError> {
        let fixture = QueueFixture::with_defaults("tenant", "orders")?;
        assert_eq!(
            fixture
                .machine
                .queue_config(&fixture.namespace, &fixture.entity)?,
            Some(QueueConfig::default())
        );
        Ok(())
    }
}
