use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    AnnotationKey, BrokerError, Command, CommandApplication, CommandKind, CommandOutcome,
    CorrelationFilter, DeadLetterReason, Delivery, EntityPath, IngressEnvelope, MessageBody,
    MessageEnvelope, MessageIdentifier, MessageProperties, MessageRecord, MessageState,
    MessageValue, QueueCounters, RuleDefinition, RuleFilter, RuleName, ScheduledEnvelope,
    SequenceNumber, SessionId, SqlCompileError, SqlCompileLimit, SqlFilter, SqlProgram,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const SQL_ERROR_REASON: &str = "SwitchyardSqlFilterError";
const SQL_DIVISION_DESCRIPTION: &str = "SQL filter integer arithmetic divided by zero.";

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

fn add<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    subscription: &str,
    name: &str,
    filter: RuleFilter,
    millis: u64,
) -> TestResult {
    assert_eq!(
        fixture.at(
            millis,
            CommandKind::CreateRule {
                subscription: SubscriptionName::new(subscription)?,
                name: RuleName::new(name)?,
                filter,
            }
        )?,
        CommandOutcome::RuleCreated
    );
    Ok(())
}

fn sql(expression: &str) -> TestResult<RuleFilter> {
    Ok(RuleFilter::Sql(SqlFilter::new(expression)?))
}

fn remove<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    subscription: &str,
    name: &str,
    millis: u64,
) -> TestResult {
    assert_eq!(
        fixture.at(
            millis,
            CommandKind::DeleteRule {
                subscription: SubscriptionName::new(subscription)?,
                name: RuleName::new(name)?,
            }
        )?,
        CommandOutcome::RuleDeleted
    );
    Ok(())
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

fn member(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_time: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                subject: Some("retained-subject".into()),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([
                ("color".into(), MessageValue::String("Red".into())),
                ("number".into(), MessageValue::Int(7)),
            ]),
            message_annotations: BTreeMap::from([(
                AnnotationKey::Symbol("producer-note".into()),
                MessageValue::String("retained".into()),
            )]),
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

fn publish<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    messages: Vec<IngressEnvelope>,
) -> Result<CommandApplication, BrokerError> {
    apply(fixture, millis, CommandKind::SendBatch { messages })
}

fn scheduled(message: IngressEnvelope, due: u64) -> ScheduledEnvelope {
    ScheduledEnvelope {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        enqueue_at: Timestamp::from_millis(due),
        envelope: message.envelope,
    }
}

fn effects(application: &CommandApplication, targets: &[EntityPath]) {
    let mut targets = targets.to_vec();
    targets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    targets.dedup();
    assert_eq!(application.subscription_enqueues, Some(targets));
    assert!(!application.dead_letters_enqueued);
}

fn record<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    sequence: u64,
) -> TestResult<Option<MessageRecord>> {
    Ok(fixture
        .machine
        .message(&fixture.namespace, entity, SequenceNumber::new(sequence))?)
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
) -> TestResult<Vec<Delivery>> {
    let before = fixture.machine.store().snapshot()?;
    let applied = fixture.machine.last_applied_time()?;
    let CommandOutcome::Peeked(deliveries) = at(
        fixture,
        entity,
        millis,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 256,
            session_id: None,
        },
    )?
    else {
        panic!("peek result")
    };
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    assert!(deliveries.iter().all(|delivery| delivery.lock.is_none()));
    Ok(deliveries)
}

fn rules<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
) -> TestResult<Vec<RuleDefinition>> {
    let before = fixture.machine.store().snapshot()?;
    let applied = fixture.machine.last_applied_time()?;
    let rules = fixture.machine.rules(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new(name)?,
    )?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    Ok(rules)
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
    let applied = fixture.machine.last_applied_time()?;
    assert_eq!(apply(fixture, millis, kind), Err(expected));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    Ok(())
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
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
                detail: "injected SQL failure".into(),
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

#[path = "subscription_sql/atomicity.rs"]
mod atomicity;
#[path = "subscription_sql/lifecycle.rs"]
mod lifecycle;
#[path = "subscription_sql/limits.rs"]
mod limits;
#[path = "subscription_sql/scheduling.rs"]
mod scheduling;
