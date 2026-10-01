//! Persisted incarnations fence admitted endpoints without changing old commands.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, DeleteEntityTarget, DeliveryBudget,
    EntityBinding, EntityIncarnation, EntityIncarnationKind, EntityPath, FencedCommand,
    IngressEnvelope, LockToken, MessageEnvelope, NamespaceName, QueueConfig, QueueConfigUpdate,
    ReceiveMode, RuleFilter, RuleName, ScheduledEnvelope, ScheduledMessage, SequenceNumber,
    SessionHold, SessionId, SettlementDisposition, SubscriptionConfig, SubscriptionConfigUpdate,
    SubscriptionName, Timestamp, TopicConfig, TopicConfigUpdate, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Mutex<Observations>>,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut observed = self.observations.lock().expect("observations");
            observed.commits += 1;
            observed.puts.extend(
                batch
                    .mutations()
                    .iter()
                    .filter_map(|mutation| match mutation {
                        Mutation::Put { key, .. } => Some(key.clone()),
                        Mutation::Delete { .. } => None,
                    }),
            );
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected incarnation failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    observations: Arc<Mutex<Observations>>,
    fail_next: Arc<AtomicBool>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;

    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            observations: Arc::clone(&self.observations),
            fail_next: Arc::clone(&self.fail_next),
        })
    }
}

fn fixture<P: StoreProvider>(provider: P) -> TestResult<QueueFixture<ObservedProvider<P>>> {
    Ok(QueueFixture::with_defaults(
        ObservedProvider {
            inner: provider,
            observations: Arc::new(Mutex::new(Observations::default())),
            fail_next: Arc::new(AtomicBool::new(false)),
        },
        "tenant",
        "orders",
    )?)
}

fn at<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandOutcome, BrokerError> {
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(millis),
        kind,
    ))
}

fn bind<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    target: &EntityPath,
    owner: &EntityPath,
    kind: EntityIncarnationKind,
) -> TestResult<EntityBinding> {
    Ok(fixture
        .machine
        .bind_entity(&fixture.namespace, target, owner, kind)?
        .expect("admitted entity"))
}

fn incarnation<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    owner: &EntityPath,
) -> TestResult<EntityIncarnation> {
    Ok(codec::decode(
        &fixture
            .machine
            .store()
            .get(&keys::entity_incarnation(&fixture.namespace, owner))?
            .expect("persisted incarnation"),
    )?)
}

fn fenced<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    binding: &EntityBinding,
    entity: &EntityPath,
    millis: u64,
    kind: CommandKind,
) -> FencedCommand {
    FencedCommand::new(
        binding.clone(),
        Command::new(
            fixture.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ),
    )
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn create_topic<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    topic: &EntityPath,
    millis: u64,
) -> TestResult {
    assert_eq!(
        at(
            fixture,
            topic,
            millis,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        )?,
        CommandOutcome::TopicCreated,
    );
    Ok(())
}

fn subscribe<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    topic: &EntityPath,
    name: &SubscriptionName,
    millis: u64,
) -> TestResult<EntityPath> {
    assert_eq!(
        at(
            fixture,
            topic,
            millis,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig::default(),
            },
        )?,
        CommandOutcome::SubscriptionCreated,
    );
    Ok(topic.subscription(name)?)
}

fn reset<P: StoreProvider>(fixture: &QueueFixture<ObservedProvider<P>>) {
    *fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations") = Observations::default();
}

fn reject_fenced<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    command: &FencedCommand,
    error: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(fixture);
    assert_eq!(fixture.machine.apply_fenced(command), Err(error.clone()));
    assert_eq!(
        fixture.machine.apply_fenced_with_effects(command),
        Err(error)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    Ok(())
}

#[path = "entity_binding/atomicity.rs"]
mod atomicity;
#[path = "entity_binding/lifecycle.rs"]
mod lifecycle;
#[path = "entity_binding/replay.rs"]
mod replay;
