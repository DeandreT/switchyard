//! Topic schedules retain one parent record until bounded, atomic fanout.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandApplication, CommandKind, CommandOutcome, DeadLetterReason,
    Delivery, DeliveryBudget, EntityPath, IngressBatchLimit, IngressEnvelope,
    MAX_INGRESS_BATCH_CONTENT_BYTES, MAX_INGRESS_BATCH_MESSAGES, MAX_SEQUENCE_NUMBER,
    MAX_TOPIC_FANOUT_CONTENT_BYTES, MAX_TOPIC_FANOUT_COPIES, MAX_TOPIC_FANOUT_VALUE_ITEMS,
    MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties, MessageRecord,
    MessageState, MessageValue, QueueCounterKind, QueueCounters, ReceiveMode, ScheduledEnvelope,
    ScheduledMessage, SequenceNumber, SessionId, SubscriptionConfig, SubscriptionName,
    TIMER_SCAN_LIMIT, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const MISSING_SESSION_DESCRIPTION: &str =
    "Session enabled entity doesn't allow a message whose session identifier is null.";

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

fn member(id: &str, enqueue_at: Option<u64>) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_time: enqueue_at.map(Timestamp::from_millis),
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                subject: Some("scheduled-copy".into()),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([("original".into(), MessageValue::Bool(true))]),
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

fn scheduled(message: IngressEnvelope, enqueue_at: u64) -> ScheduledEnvelope {
    ScheduledEnvelope {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        enqueue_at: Timestamp::from_millis(enqueue_at),
        envelope: message.envelope,
    }
}

fn schedule(id: &str, enqueue_at: u64) -> CommandKind {
    CommandKind::ScheduleEnvelopes {
        messages: vec![scheduled(member(id, None), enqueue_at)],
    }
}

fn immediate(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
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

fn effects(application: &CommandApplication, targets: &[EntityPath]) {
    let mut expected = targets.to_vec();
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    expected.dedup();
    assert_eq!(application.subscription_enqueues, Some(expected));
    assert!(
        !application.dead_letters_enqueued,
        "topic parent has no DLQ shadow"
    );
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

fn pending<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequence: u64,
    due: u64,
) -> TestResult<MessageRecord> {
    let record = record(fixture, &fixture.entity, sequence)?.expect("parent pending schedule");
    assert!(
        matches!(record.state, MessageState::Scheduled { enqueue_at, .. } if enqueue_at == Timestamp::from_millis(due))
    );
    assert_eq!(record.expires_at, None);
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::scheduled(
                &fixture.namespace,
                &fixture.entity,
                Timestamp::from_millis(due),
                SequenceNumber::new(sequence)
            ))?
            .is_some()
    );
    Ok(record)
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

fn activate<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    expected: u32,
    targets: &[EntityPath],
) -> TestResult {
    let application = apply(fixture, millis, CommandKind::ActivateScheduled)?;
    assert_eq!(
        application.outcome,
        CommandOutcome::ScheduledActivated {
            activated: expected
        }
    );
    effects(&application, targets);
    Ok(())
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

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    from: u64,
    count: u32,
    budget: Option<DeliveryBudget>,
) -> TestResult<Vec<Delivery>> {
    let before = fixture.machine.store().snapshot()?;
    let applied = fixture.machine.last_applied_time()?;
    let kind = match budget {
        Some(budget) => CommandKind::PeekBounded {
            from_sequence: SequenceNumber::new(from),
            max_messages: count,
            session_id: None,
            budget,
        },
        None => CommandKind::Peek {
            from_sequence: SequenceNumber::new(from),
            max_messages: count,
            session_id: None,
        },
    };
    let CommandOutcome::Peeked(deliveries) = at(fixture, entity, millis, kind)? else {
        panic!("peek outcome")
    };
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    assert!(deliveries.iter().all(|delivery| delivery.lock.is_none()));
    Ok(deliveries)
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
    scans: Vec<(Key, usize, usize)>,
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
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.observations.lock().expect("observations").scans.push((
            prefix.to_vec(),
            limit,
            rows.len(),
        ));
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
                detail: "injected scheduling failure".into(),
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

#[path = "topic_scheduling/atomicity.rs"]
mod atomicity;
#[path = "topic_scheduling/lifecycle.rs"]
mod lifecycle;
#[path = "topic_scheduling/limits.rs"]
mod limits;
