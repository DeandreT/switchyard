//! Topology updates replace configuration without rewriting accepted state.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandApplication, CommandKind, CommandOutcome, Delivery, EntityPath,
    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS, MAX_LOCK_DURATION_MILLIS, MAX_TOPIC_SUBSCRIPTIONS,
    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS, MessageRecord, MessageState, QueueConfig,
    QueueConfigError, QueueCounters, QueueTimeToLiveUpdate, ReceiveMode, RuleFilter, RuleName,
    ScheduledMessage, SequenceNumber, SessionHold, SessionId, SqlFilter, SubscriptionConfig,
    SubscriptionConfigUpdate, SubscriptionImmutableProperty, SubscriptionName, Timestamp,
    TopicConfig, TopicConfigUpdate, TopicImmutableProperty, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn topic<P: StoreProvider>(provider: P, config: TopicConfig) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    fixture.at(0, CommandKind::CreateTopic { config })?;
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

fn apply<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandApplication, BrokerError> {
    fixture
        .machine
        .apply_with_effects(&fixture.command(millis, kind))
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

fn topic_update<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    update: TopicConfigUpdate,
) -> Result<CommandApplication, BrokerError> {
    let application = apply(fixture, millis, CommandKind::UpdateTopic { update })?;
    assert_eq!(application.outcome, CommandOutcome::TopicUpdated);
    assert!(!application.dead_letters_enqueued);
    assert_eq!(application.subscription_enqueues, None);
    Ok(application)
}

fn subscription_update<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    millis: u64,
    update: SubscriptionConfigUpdate,
) -> Result<CommandApplication, BrokerError> {
    let application = apply(
        fixture,
        millis,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new(name).expect("valid fixture name"),
            update,
        },
    )?;
    assert_eq!(application.outcome, CommandOutcome::SubscriptionUpdated);
    assert!(!application.dead_letters_enqueued);
    assert_eq!(application.subscription_enqueues, None);
    Ok(application)
}

fn topic_config<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<TopicConfig> {
    Ok(fixture
        .machine
        .topic_config(&fixture.namespace, &fixture.entity)?
        .expect("topic"))
}

fn subscription_config<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
) -> TestResult<SubscriptionConfig> {
    Ok(fixture
        .machine
        .subscription_config(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new(name)?,
        )?
        .expect("subscription"))
}

fn assert_projections<P: StoreProvider>(fixture: &QueueFixture<P>, name: &str) -> TestResult {
    let config = subscription_config(fixture, name)?;
    let entity = fixture.entity.subscription(&SubscriptionName::new(name)?)?;
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &entity)?,
        Some(config.to_queue_config())
    );
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &entity.dead_letter_queue()?)?,
        Some(config.to_queue_config().dead_letter_shadow())
    );
    assert!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?)?
            .is_none()
    );
    Ok(())
}

fn retained<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    names: &[&str],
) -> TestResult<Vec<(Key, Value)>> {
    let mut excluded = vec![
        keys::clock(),
        keys::topic_config(&fixture.namespace, &fixture.entity),
    ];
    for name in names {
        let name = SubscriptionName::new(*name)?;
        let entity = fixture.entity.subscription(&name)?;
        excluded.extend([
            keys::subscription(&fixture.namespace, &fixture.entity, &name),
            keys::queue_config(&fixture.namespace, &entity),
            keys::queue_config(&fixture.namespace, &entity.dead_letter_queue()?),
        ]);
    }
    Ok(fixture
        .machine
        .store()
        .snapshot()?
        .entries()
        .iter()
        .filter(|(key, _)| !excluded.contains(key))
        .cloned()
        .collect())
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    session: Option<&SessionId>,
) -> TestResult<SequenceNumber> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: session.cloned(),
        },
    )?
    else {
        panic!("send outcome")
    };
    Ok(sequence)
}

fn schedule<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    due: u64,
) -> TestResult<SequenceNumber> {
    let CommandOutcome::Scheduled { sequences } = fixture.at(
        millis,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: id.to_owned(),
                body: b"payload".to_vec(),
                time_to_live_millis: ttl,
                session_id: None,
                enqueue_at: Timestamp::from_millis(due),
            }],
        },
    )?
    else {
        panic!("scheduled outcome")
    };
    Ok(sequences[0])
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

fn record<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> TestResult<Option<MessageRecord>> {
    Ok(fixture
        .machine
        .message(&fixture.namespace, entity, sequence)?)
}

fn counters<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
) -> TestResult<Option<QueueCounters>> {
    Ok(fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, entity))?
        .map(|bytes| codec::decode(&bytes))
        .transpose()?)
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    assert_eq!(apply(fixture, millis, kind), Err(expected));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    Ok(())
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
    gets: Vec<Key>,
    scans: Vec<(Key, usize)>,
    scan_details: Vec<(Key, Key, usize, usize)>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations
            .lock()
            .expect("observations")
            .gets
            .push(key.to_vec());
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
        self.observations
            .lock()
            .expect("observations")
            .scans
            .push((prefix.to_vec(), limit));
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.observations
            .lock()
            .expect("observations")
            .scan_details
            .push((prefix.to_vec(), start.to_vec(), limit, rows.len()));
        Ok(rows)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut observations = self.observations.lock().expect("observations");
            observations.commits += 1;
            observations
                .puts
                .extend(
                    batch
                        .mutations()
                        .iter()
                        .filter_map(|mutation| match mutation {
                            Mutation::Put { key, .. } => Some(key.clone()),
                            _ => None,
                        }),
                );
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected update failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
            observations: self.observations.clone(),
        })
    }
}

fn observed<P: StoreProvider>(
    provider: P,
    config: TopicConfig,
) -> TestResult<QueueFixture<ObservedProvider<P>>> {
    topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: Arc::new(Mutex::new(Observations::default())),
        },
        config,
    )
}

fn reset<P: StoreProvider>(fixture: &QueueFixture<ObservedProvider<P>>) {
    *fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations") = Observations::default();
}

#[path = "topic_updates/atomicity.rs"]
mod atomicity;
#[path = "topic_updates/lifecycle.rs"]
mod lifecycle;
#[path = "topic_updates/scheduling.rs"]
mod scheduling;
