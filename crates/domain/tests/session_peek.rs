//! Entity-wide session browsing changes neither ownership nor persisted time.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    AcceptedSession, BrokerError, Command, CommandKind, CommandOutcome, Delivery, DeliveryBudget,
    EntityPath, IngressEnvelope, MessageEnvelope, MessageState, MessageStatus, QueueConfig,
    ReceiveMode, ScheduledMessage, SequenceNumber, SessionHold, SessionId, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const MAX_INSPECTED: usize = 256;
const ENTRY_OVERHEAD: u64 = 64;

#[derive(Clone, Debug)]
struct Scan {
    prefix: Vec<u8>,
    start: Vec<u8>,
    limit: usize,
    returned: usize,
}

#[derive(Debug, Default)]
struct Observations {
    writes: AtomicUsize,
    scans: Mutex<Vec<Scan>>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.observations.scans.lock().expect("scans").push(Scan {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
            returned: rows.len(),
        });
        Ok(rows)
    }
}

struct ObservedProvider<P> {
    inner: P,
    observations: Arc<Observations>,
}
impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            observations: self.observations.clone(),
        })
    }
}

struct Node<P: StoreProvider> {
    fixture: QueueFixture<ObservedProvider<P>>,
    queue: EntityPath,
    topic: EntityPath,
    subscription: EntityPath,
    ordinary_subscription: EntityPath,
    observations: Arc<Observations>,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let observations = Arc::new(Observations::default());
        let fixture = QueueFixture::with_defaults(
            ObservedProvider {
                inner: provider,
                observations: observations.clone(),
            },
            "tenant",
            "ordinary-queue",
        )?;
        let queue = EntityPath::new("session-queue")?;
        let topic = EntityPath::new("session-topic")?;
        let subscription_name = SubscriptionName::new("required")?;
        let ordinary_name = SubscriptionName::new("ordinary")?;
        let subscription = topic.subscription(&subscription_name)?;
        let ordinary_subscription = topic.subscription(&ordinary_name)?;
        let node = Self {
            fixture,
            queue,
            topic,
            subscription,
            ordinary_subscription,
            observations,
        };
        node.at(
            &node.queue,
            0,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    lock_duration_millis: 100,
                    ..QueueConfig::default()
                },
            },
        )?;
        node.at(
            &node.topic,
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        for (name, requires_session) in [(subscription_name, true), (ordinary_name, false)] {
            node.at(
                &node.topic,
                0,
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig {
                        requires_session,
                        lock_duration_millis: 100,
                        ..SubscriptionConfig::default()
                    },
                },
            )?;
        }
        Ok(node)
    }

    fn targets(&self) -> [EntityPath; 2] {
        [self.queue.clone(), self.subscription.clone()]
    }

    fn at(
        &self,
        target: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.fixture.machine.apply(&Command::new(
            self.fixture.namespace.clone(),
            target.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }

    fn source<'a>(&'a self, target: &'a EntityPath) -> &'a EntityPath {
        if target == &self.queue {
            target
        } else {
            &self.topic
        }
    }

    fn send(
        &self,
        target: &EntityPath,
        millis: u64,
        id: &str,
        session: &str,
        ttl: Option<u64>,
        body_bytes: usize,
    ) -> TestResult<SequenceNumber> {
        let CommandOutcome::Sent { sequence } = self.at(
            self.source(target),
            millis,
            CommandKind::Send {
                message_id: id.into(),
                body: vec![0; body_bytes],
                time_to_live_millis: ttl,
                session_id: Some(SessionId::new(session)?),
            },
        )?
        else {
            panic!("sent outcome")
        };
        Ok(sequence)
    }

    fn accept(
        &self,
        target: &EntityPath,
        millis: u64,
        session: &str,
    ) -> TestResult<AcceptedSession> {
        let CommandOutcome::SessionAccepted(Some(accepted)) = self.at(
            target,
            millis,
            CommandKind::AcceptSession {
                session_id: Some(SessionId::new(session)?),
                lock_duration_millis: None,
            },
        )?
        else {
            panic!("accepted outcome")
        };
        Ok(accepted)
    }

    fn receive(
        &self,
        target: &EntityPath,
        millis: u64,
        session: &SessionHold,
        lock_millis: Option<u64>,
    ) -> TestResult<Delivery> {
        let CommandOutcome::Received(Some(delivery)) = self.at(
            target,
            millis,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: lock_millis,
                session: Some(session.clone()),
            },
        )?
        else {
            panic!("received outcome")
        };
        Ok(delivery)
    }

    fn readonly(
        &self,
        target: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        let snapshot = self.fixture.machine.store().snapshot()?;
        let applied = self.fixture.machine.last_applied_time()?;
        let writes = self.observations.writes.load(Ordering::SeqCst);
        let outcome = self.at(target, millis, kind);
        assert_eq!(self.fixture.machine.store().snapshot()?, snapshot);
        assert_eq!(self.fixture.machine.last_applied_time()?, applied);
        assert_eq!(self.observations.writes.load(Ordering::SeqCst), writes);
        outcome
    }

    fn peek(
        &self,
        target: &EntityPath,
        millis: u64,
        from: u64,
        count: u32,
        session: Option<&str>,
        budget: Option<u64>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let session_id = session.map(|value| SessionId::new(value).expect("session id"));
        let command = match budget {
            Some(max_bytes) => CommandKind::PeekBounded {
                from_sequence: SequenceNumber::new(from),
                max_messages: count,
                session_id,
                budget: DeliveryBudget {
                    max_bytes,
                    per_message_overhead_bytes: ENTRY_OVERHEAD,
                },
            },
            None => CommandKind::Peek {
                from_sequence: SequenceNumber::new(from),
                max_messages: count,
                session_id,
            },
        };
        let CommandOutcome::Peeked(deliveries) = self.readonly(target, millis, command)? else {
            panic!("peeked outcome")
        };
        assert!(deliveries.iter().all(|delivery| delivery.lock.is_none()));
        Ok(deliveries)
    }

    fn scans(&self) -> Vec<Scan> {
        std::mem::take(&mut *self.observations.scans.lock().expect("scans"))
    }
}

fn sequences(deliveries: &[Delivery]) -> Vec<u64> {
    deliveries
        .iter()
        .map(|delivery| delivery.sequence.as_u64())
        .collect()
}

#[path = "session_peek/bounds.rs"]
mod bounds;
#[path = "session_peek/semantics.rs"]
mod semantics;
