use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandApplication, CommandKind, CommandOutcome, CorrelationFilter,
    DeadLetterReason, Delivery, EntityIncarnationKind, EntityPath, FencedCommand,
    IngressBatchLimit, IngressEnvelope, MAX_TOPIC_FANOUT_CONTENT_BYTES, MAX_TOPIC_FANOUT_COPIES,
    MAX_TOPIC_FANOUT_VALUE_ITEMS, MessageBody, MessageEnvelope, MessageIdentifier,
    MessageProperties, MessageRecord, MessageState, MessageValue, QueueCounters, RuleDefinition,
    RuleFilter, RuleName, ScheduledEnvelope, SequenceNumber, SessionId, SqlAction, SqlCompileError,
    SqlFilter, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
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
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    fixture.at(
        0,
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

fn add<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &str,
    name: &str,
    filter: RuleFilter,
    action: Option<&str>,
    millis: u64,
) -> TestResult {
    let subscription = SubscriptionName::new(child)?;
    let name = RuleName::new(name)?;
    let kind = match action {
        Some(source) => CommandKind::CreateRuleWithAction {
            subscription,
            name,
            filter,
            action: SqlAction::new(source)?,
        },
        None => CommandKind::CreateRule {
            subscription,
            name,
            filter,
        },
    };
    assert_eq!(fixture.at(millis, kind)?, CommandOutcome::RuleCreated);
    Ok(())
}

fn remove<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &str,
    name: &str,
    millis: u64,
) -> TestResult {
    assert_eq!(
        fixture.at(
            millis,
            CommandKind::DeleteRule {
                subscription: SubscriptionName::new(child)?,
                name: RuleName::new(name)?,
            }
        )?,
        CommandOutcome::RuleDeleted
    );
    Ok(())
}

fn rules<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &str,
) -> TestResult<Vec<RuleDefinition>> {
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    let result = fixture.machine.rules(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new(child)?,
    )?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    Ok(result)
}

fn record<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &EntityPath,
    sequence: u64,
) -> TestResult<Option<MessageRecord>> {
    Ok(fixture
        .machine
        .message(&fixture.namespace, child, SequenceNumber::new(sequence))?)
}

fn records<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &EntityPath,
) -> TestResult<Vec<MessageRecord>> {
    fixture
        .machine
        .store()
        .scan_prefix(
            &keys::message_prefix(&fixture.namespace, child),
            MAX_TOPIC_FANOUT_COPIES + 1,
        )?
        .into_iter()
        .map(|(_, bytes)| Ok(codec::decode(&bytes)?))
        .collect()
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &EntityPath,
    millis: u64,
) -> TestResult<Vec<Delivery>> {
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    let outcome = fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        child.clone(),
        Timestamp::from_millis(millis),
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 256,
            session_id: None,
        },
    ))?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    let CommandOutcome::Peeked(deliveries) = outcome else {
        panic!("peek outcome")
    };
    Ok(deliveries)
}

fn counters<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    child: &EntityPath,
) -> TestResult<Option<QueueCounters>> {
    fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, child))?
        .map(|value| Ok(codec::decode(&value)?))
        .transpose()
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let time = fixture.machine.last_applied_time()?;
    assert_eq!(apply(fixture, millis, kind), Err(expected));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, time);
    Ok(())
}

fn effects(application: &CommandApplication, children: &[EntityPath]) {
    let mut expected = children.to_vec();
    expected.sort();
    expected.dedup();
    assert_eq!(application.subscription_enqueues, Some(expected));
    assert!(!application.dead_letters_enqueued);
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
                correlation_id: Some(MessageIdentifier::String("correlation".into())),
                subject: Some("subject".into()),
                to: Some("destination".into()),
                reply_to: Some("reply".into()),
                reply_to_group_id: Some("reply-session".into()),
                content_type: Some("application/octet-stream".into()),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([
                ("color".into(), MessageValue::String("Red".into())),
                ("number".into(), MessageValue::Int(7)),
            ]),
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

fn rich(message: IngressEnvelope) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        envelope: Box::new(message.envelope),
    }
}

fn scheduled(message: IngressEnvelope, due: u64) -> ScheduledEnvelope {
    ScheduledEnvelope {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        envelope: message.envelope,
        enqueue_at: Timestamp::from_millis(due),
    }
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
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
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.lock().expect("observations").commits += 1;
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected action failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

#[derive(Clone, Debug)]
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

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

#[path = "rule_actions/atomicity.rs"]
mod atomicity;
#[path = "rule_actions/lifecycle.rs"]
mod lifecycle;
#[path = "rule_actions/limits.rs"]
mod limits;
#[path = "rule_actions/literal_set.rs"]
mod literal_set;
#[path = "rule_actions/scheduling.rs"]
mod scheduling;
