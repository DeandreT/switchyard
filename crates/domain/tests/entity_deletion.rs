//! Bounded destructive deletion retains allocation fences across recreation.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandApplication, CommandKind, CommandOutcome, DeleteEntityTarget,
    Delivery, EntityDeleteLimit, EntityIncarnation, EntityPath, MAX_ENTITY_DELETE_KEY_BYTES,
    MAX_ENTITY_DELETE_KEYS, MAX_ENTITY_DELETE_VALUE_BYTES, MAX_SEQUENCE_NUMBER,
    MAX_TOPIC_SUBSCRIPTIONS, NamespaceName, QueueConfig, QueueCounters, ReceiveMode, RuleFilter,
    RuleName, ScheduledMessage, SequenceNumber, SessionHold, SessionId, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn topic<P: StoreProvider>(provider: P) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    fixture.at(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                ..TopicConfig::default()
            },
        },
    )?;
    Ok(fixture)
}

fn subscribe<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    config: SubscriptionConfig,
    millis: u64,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    fixture.at(
        millis,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config,
        },
    )?;
    Ok(fixture.entity.subscription(&name)?)
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

fn apply<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandApplication, BrokerError> {
    fixture
        .machine
        .apply_with_effects(&fixture.command(millis, kind))
}

fn delete<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    target: DeleteEntityTarget,
    outcome: CommandOutcome,
    paths: &[EntityPath],
) -> TestResult<CommandApplication> {
    let application = apply(fixture, millis, CommandKind::DeleteEntity { target })?;
    assert_eq!(application.outcome, outcome);
    let mut expected = paths.to_vec();
    expected.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    expected.dedup();
    assert_eq!(application.entity_deletions, Some(expected));
    assert_eq!(application.subscription_enqueues, None);
    assert!(!application.dead_letters_enqueued);
    Ok(application)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    session: Option<&SessionId>,
) -> TestResult<SequenceNumber> {
    let application = apply(
        fixture,
        millis,
        CommandKind::Send {
            message_id: id.into(),
            body: b"payload".to_vec(),
            time_to_live_millis: Some(1_000),
            session_id: session.cloned(),
        },
    )?;
    assert_eq!(application.entity_deletions, None);
    let CommandOutcome::Sent { sequence } = application.outcome else {
        panic!("sent outcome")
    };
    Ok(sequence)
}

fn schedule<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    session: Option<&SessionId>,
) -> TestResult<SequenceNumber> {
    let CommandOutcome::Scheduled { sequences } = fixture.at(
        millis,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: id.into(),
                body: b"payload".to_vec(),
                time_to_live_millis: Some(1_000),
                session_id: session.cloned(),
                enqueue_at: Timestamp::from_millis(1_000),
            }],
        },
    )?
    else {
        panic!("scheduled outcome")
    };
    Ok(sequences[0])
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    session: &SessionId,
) -> TestResult<SessionHold> {
    let CommandOutcome::SessionAccepted(Some(accepted)) = at(
        fixture,
        entity,
        millis,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("accepted session")
    };
    Ok(accepted.hold())
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    session: Option<&SessionHold>,
) -> TestResult<Delivery> {
    let CommandOutcome::Received(Some(delivery)) = at(
        fixture,
        entity,
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: session.cloned(),
        },
    )?
    else {
        panic!("received delivery")
    };
    Ok(delivery)
}

fn counter_bytes<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
) -> TestResult<Option<Value>> {
    Ok(fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, entity))?)
}

fn counter<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
) -> TestResult<QueueCounters> {
    Ok(codec::decode(
        &counter_bytes(fixture, entity)?.expect("allocation counter"),
    )?)
}

fn assert_exact_purge<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    before: &StoreSnapshot,
    paths: &[EntityPath],
    extra_prefixes: &[Key],
    extra_keys: &[Key],
    millis: u64,
) -> TestResult {
    let namespace = fixture.namespace.as_str();
    let mut expected = before
        .entries()
        .iter()
        .filter(|(key, _)| {
            if key == &keys::clock() {
                return false;
            }
            if extra_keys.contains(key)
                || extra_prefixes.iter().any(|prefix| key.starts_with(prefix))
            {
                return false;
            }
            let Some((key_namespace, entity)) = keys::entity_scope_parts(key) else {
                return true;
            };
            let Some(path) = paths
                .iter()
                .find(|path| key_namespace == namespace && path.as_str() == entity)
            else {
                return true;
            };
            key == &keys::queue_counters(&fixture.namespace, path)
                || key == &keys::entity_incarnation(&fixture.namespace, path)
        })
        .cloned()
        .collect::<Vec<_>>();
    for path in paths {
        let key = keys::entity_incarnation(&fixture.namespace, path);
        if let Some((_, value)) = expected.iter_mut().find(|(candidate, _)| *candidate == key) {
            let live: EntityIncarnation = codec::decode(value)?;
            *value = codec::encode(&EntityIncarnation::new(
                live.generation(),
                live.kind(),
                true,
            )?)?;
        }
    }
    expected.push((
        keys::clock(),
        codec::encode(&Timestamp::from_millis(millis))?,
    ));
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        fixture.machine.store().snapshot()?.entries(),
        expected.as_slice()
    );
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(millis)
    );
    Ok(())
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    kind: CommandKind,
    error: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    assert_eq!(at(fixture, entity, millis, kind), Err(error));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    Ok(())
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    deleted: Vec<Key>,
    puts: Vec<Key>,
    scans: Vec<(Key, usize, usize)>,
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
        let result = self.inner.scan_from(prefix, start, limit)?;
        self.observations.lock().expect("observations").scans.push((
            prefix.to_vec(),
            limit,
            result.iter().map(|(_, value)| value.len()).sum(),
        ));
        Ok(result)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut observed = self.observations.lock().expect("observations");
            observed.commits += 1;
            for mutation in batch.mutations() {
                match mutation {
                    Mutation::Put { key, .. } => observed.puts.push(key.clone()),
                    Mutation::Delete { key } => observed.deleted.push(key.clone()),
                }
            }
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected delete failure".into(),
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
            observations: self.observations.clone(),
            fail_next: self.fail_next.clone(),
        })
    }
}

fn observed_queue<P: StoreProvider>(
    provider: P,
    config: QueueConfig,
) -> TestResult<QueueFixture<ObservedProvider<P>>> {
    Ok(QueueFixture::new(
        ObservedProvider {
            inner: provider,
            observations: Arc::new(Mutex::new(Observations::default())),
            fail_next: Arc::new(AtomicBool::new(false)),
        },
        "tenant",
        "orders",
        config,
    )?)
}

fn reset<P: StoreProvider>(fixture: &QueueFixture<ObservedProvider<P>>) {
    *fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations") = Observations::default();
}

#[path = "entity_deletion/atomicity.rs"]
mod atomicity;
#[path = "entity_deletion/fencing.rs"]
mod fencing;
#[path = "entity_deletion/finite_binding.rs"]
mod finite_binding;
#[path = "entity_deletion/lifecycle.rs"]
mod lifecycle;
#[path = "entity_deletion/limits.rs"]
mod limits;
