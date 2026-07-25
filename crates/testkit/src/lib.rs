//! Deterministic fixtures for exercising the broker without a cluster.
//!
//! The fixture drives a [`StateMachine`] over an injected store and requires
//! every command to carry an explicit timestamp. Tests advance time by choosing
//! that value, so lock expiry and time-to-live behavior are reproducible instead
//! of depending on how long a test takes to run.
//!
//! The store arrives through a [`StoreProvider`] rather than being constructed
//! inline. One suite therefore runs against every backend, and a test can
//! restart the machine over state a previous handle left behind.

#![forbid(unsafe_code)]

use std::{fmt::Debug, path::Path};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, IdentifierError, NamespaceName,
    QueueConfig, StateMachine, Timestamp,
};
use storage::{FjallStore, MemoryStore, StateStore, StorageError};
use tempfile::TempDir;
use thiserror::Error;

/// Opens the store a fixture runs on.
pub trait StoreProvider {
    type Store: StateStore + Debug;

    /// Opens the store this provider stands for.
    ///
    /// Calling it again reaches the same state. For [`DurableProvider`] that
    /// means reopening the directory, so everything read afterward came back off
    /// disk — and the previous handle has to be dropped first, because a store
    /// directory has a single owner.
    fn open(&self) -> Result<Self::Store, StorageError>;
}

/// Runs a fixture on a shared in-memory keyspace.
#[derive(Clone, Debug, Default)]
pub struct MemoryProvider {
    store: MemoryStore,
}

impl MemoryProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StoreProvider for MemoryProvider {
    type Store = MemoryStore;

    fn open(&self) -> Result<MemoryStore, StorageError> {
        Ok(self.store.clone())
    }
}

/// Runs a fixture on a real store directory, removed when the provider drops.
#[derive(Debug)]
pub struct DurableProvider {
    directory: TempDir,
}

impl DurableProvider {
    pub fn temporary() -> Result<Self, FixtureError> {
        Ok(Self {
            directory: TempDir::new().map_err(|error| {
                FixtureError::TemporaryDirectory(format!(
                    "could not create a temporary store directory: {error}"
                ))
            })?,
        })
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }
}

impl StoreProvider for DurableProvider {
    type Store = FjallStore;

    fn open(&self) -> Result<FjallStore, StorageError> {
        FjallStore::open(self.directory.path())
    }
}

#[derive(Debug)]
pub struct QueueFixture<P: StoreProvider> {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub machine: StateMachine<P::Store>,
    provider: P,
}

impl<P: StoreProvider> QueueFixture<P> {
    /// Builds a fixture over `provider` and creates the queue at time zero.
    pub fn new(
        provider: P,
        namespace: &str,
        entity: &str,
        config: QueueConfig,
    ) -> Result<Self, FixtureError> {
        let fixture = Self {
            namespace: NamespaceName::new(namespace)?,
            entity: EntityPath::new(entity)?,
            machine: StateMachine::new(provider.open()?),
            provider,
        };
        fixture.at(0, CommandKind::CreateQueue { config })?;
        Ok(fixture)
    }

    pub fn with_defaults(provider: P, namespace: &str, entity: &str) -> Result<Self, FixtureError> {
        Self::new(provider, namespace, entity, QueueConfig::default())
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

    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// Drops this fixture's store handle and opens a new one over the same
    /// provider, which is what a restart looks like from the state machine's
    /// side. On the durable backend every record the machine reads afterward is
    /// one it recovered; on the memory backend the keyspace was never on disk,
    /// so this only proves a fresh handle sees the same state.
    pub fn restart(self) -> Result<Self, FixtureError> {
        let Self {
            namespace,
            entity,
            machine,
            provider,
        } = self;
        drop(machine);

        Ok(Self {
            namespace,
            entity,
            machine: StateMachine::new(provider.open()?),
            provider,
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FixtureError {
    #[error("{0}")]
    TemporaryDirectory(String),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_the_queue_it_is_asked_for() -> Result<(), FixtureError> {
        let fixture = QueueFixture::with_defaults(MemoryProvider::new(), "tenant", "orders")?;
        assert_eq!(
            fixture
                .machine
                .queue_config(&fixture.namespace, &fixture.entity)?,
            Some(QueueConfig::default())
        );
        Ok(())
    }

    #[test]
    fn a_durable_fixture_keeps_its_queue_across_a_restart() -> Result<(), FixtureError> {
        let fixture =
            QueueFixture::with_defaults(DurableProvider::temporary()?, "tenant", "orders")?;
        let directory = fixture.provider().path().to_path_buf();

        let restarted = fixture.restart()?;
        assert_eq!(restarted.provider().path(), directory);
        assert_eq!(
            restarted
                .machine
                .queue_config(&restarted.namespace, &restarted.entity)?,
            Some(QueueConfig::default())
        );
        Ok(())
    }
}
