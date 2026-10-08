//! Captured authority on both stores. Raw head edits below are fault injection,
//! not an entity deletion or recreation API.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, Delivery, EntityBinding,
    EntityBindingKind, EntityPath, LockToken, MAX_ENTITY_PATH_BYTES, MAX_NAMESPACE_NAME_BYTES,
    MAX_SUBSCRIPTION_NAME_CHARACTERS, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use serde::Serialize;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

// Only the private head schema is mirrored, solely to inject invalid or drifted rows.
#[derive(Clone, Copy, Serialize)]
enum TestKind {
    Queue,
    Topic,
    Subscription,
    Unknown,
}

#[derive(Serialize)]
struct TestHead {
    generation: u64,
    kind: TestKind,
    retired: bool,
}

fn head(kind: TestKind, generation: u64, retired: bool) -> TestResult<Value> {
    Ok(codec::encode(&TestHead {
        generation,
        kind,
        retired,
    })?)
}

fn test_kind(kind: EntityBindingKind) -> TestKind {
    match kind {
        EntityBindingKind::Queue => TestKind::Queue,
        EntityBindingKind::Topic => TestKind::Topic,
        EntityBindingKind::Subscription => TestKind::Subscription,
    }
}

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    applies: usize,
    snapshots: usize,
    scans: usize,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    trace: Arc<Mutex<Trace>>,
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.trace
            .lock()
            .expect("trace lock")
            .gets
            .push(key.to_vec());
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.trace.lock().expect("trace lock").applies += 1;
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.trace.lock().expect("trace lock").snapshots += 1;
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.trace.lock().expect("trace lock").scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Fixture<P: StoreProvider> {
    namespace: NamespaceName,
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> TestResult<Self> {
        Ok(Self {
            namespace: NamespaceName::new("tenant")?,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        })
    }

    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }

    fn reset(&self) {
        *self.machine.store().trace.lock().expect("trace lock") = Trace::default();
    }

    fn trace(&self) -> Trace {
        self.machine
            .store()
            .trace
            .lock()
            .expect("trace lock")
            .clone()
    }

    fn command(&self, entity: &EntityPath, millis: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        )
    }

    fn at(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&self.command(entity, millis, kind))
    }

    fn bound(
        &self,
        binding: &EntityBinding,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply_bound(&BoundCommand::new(
            binding.clone(),
            self.command(binding.target(), millis, kind),
        ))
    }

    fn populate(
        &self,
        queue: &EntityPath,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> TestResult<EntityPath> {
        assert_eq!(
            self.at(
                queue,
                1,
                CommandKind::CreateQueue {
                    config: QueueConfig::default()
                }
            )?,
            CommandOutcome::QueueCreated
        );
        assert_eq!(
            self.at(
                topic,
                2,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated
        );
        let child = topic.subscription(name)?;
        assert_eq!(
            self.at(
                topic,
                3,
                CommandKind::CreateSubscription {
                    name: name.clone(),
                    config: SubscriptionConfig::default(),
                }
            )?,
            CommandOutcome::SubscriptionCreated {
                entity: child.clone()
            }
        );
        Ok(child)
    }

    fn populated(&self) -> TestResult<Vec<EntityBinding>> {
        let queue = EntityPath::new("orders")?;
        let topic = EntityPath::new("events")?;
        let child = self.populate(&queue, &topic, &SubscriptionName::new("alpha")?)?;
        self.capture_five(&queue, &topic, &child)
    }

    fn capture_five(
        &self,
        queue: &EntityPath,
        topic: &EntityPath,
        child: &EntityPath,
    ) -> TestResult<Vec<EntityBinding>> {
        [
            (queue.clone(), queue, EntityBindingKind::Queue),
            (queue.dead_letter_queue()?, queue, EntityBindingKind::Queue),
            (topic.clone(), topic, EntityBindingKind::Topic),
            (child.clone(), child, EntityBindingKind::Subscription),
            (
                child.dead_letter_queue()?,
                child,
                EntityBindingKind::Subscription,
            ),
        ]
        .into_iter()
        .map(|(target, owner, kind)| {
            let binding = self.read_only(|| self.machine.bind_entity(&self.namespace, &target))?;
            assert_eq!(binding.namespace(), &self.namespace);
            assert_eq!(binding.target(), &target);
            assert_eq!(binding.owner(), owner);
            assert_eq!(binding.kind(), kind);
            assert_eq!(binding.generation(), 1);
            self.read_only(|| self.machine.validate_binding(&binding))?;
            Ok(binding)
        })
        .collect()
    }

    fn unchanged(&self, before: &StoreSnapshot) -> TestResult<Trace> {
        let trace = self.trace();
        assert!(
            !trace.gets.contains(&keys::clock()),
            "authority must precede Clock"
        );
        assert_eq!(trace.applies, 0, "no attempted mutation");
        assert_eq!(trace.snapshots, 0, "no snapshot fallback");
        assert_eq!(trace.scans, 0, "no global or parent topology walk");
        assert_eq!(
            &self.raw().snapshot()?,
            before,
            "all rows and Clock remain exact"
        );
        Ok(trace)
    }

    fn read_only<T>(&self, read: impl FnOnce() -> Result<T, BrokerError>) -> TestResult<T> {
        let before = self.raw().snapshot()?;
        self.reset();
        let result = read()?;
        self.unchanged(&before)?;
        Ok(result)
    }

    fn reject<T>(
        &self,
        action: impl FnOnce() -> Result<T, BrokerError>,
        expected: BrokerError,
    ) -> TestResult<Trace> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(action().err(), Some(expected));
        self.unchanged(&before)
    }

    fn replace(&self, key: &Key, value: Option<Value>) -> TestResult {
        let mut batch = WriteBatch::default();
        match value {
            Some(value) => batch.push_put(key.clone(), value),
            None => batch.push_delete(key.clone()),
        }
        self.raw().apply(batch)?;
        Ok(())
    }

    fn restart(self) -> TestResult<Self> {
        let Self {
            namespace,
            machine,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            namespace,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        })
    }
}

fn send(body: &str) -> CommandKind {
    CommandKind::Send {
        message_id: body.to_owned(),
        body: body.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn receive(mode: ReceiveMode) -> CommandKind {
    CommandKind::Receive {
        mode,
        lock_duration_millis: None,
        session: None,
    }
}

fn complete() -> CommandKind {
    CommandKind::Complete {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
    }
}

fn delivery(outcome: CommandOutcome) -> TestResult<Delivery> {
    match outcome {
        CommandOutcome::Received(Some(delivery)) => Ok(delivery),
        other => Err(format!("expected delivery, got {other:?}").into()),
    }
}

fn dead_letter(delivery: &Delivery) -> CommandKind {
    CommandKind::DeadLetter {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("peek-lock delivery").token,
        reason: String::from("test"),
        description: String::from("bound authority"),
        replacement_envelope: None,
    }
}

fn healthy_owner_and_shadow_bindings_run_without_factory_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider)?;
    let bindings = fixture.populated()?;
    let [queue, queue_dlq, topic, child, child_dlq] = bindings.as_slice() else {
        unreachable!()
    };
    fixture.reject(
        || {
            fixture
                .machine
                .bind_entity(&fixture.namespace, &EntityPath::new("absent").unwrap())
        },
        BrokerError::EntityNotFound,
    )?;
    assert_eq!(
        fixture.bound(queue, 10, send("queue-body"))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let locked = delivery(fixture.bound(queue, 11, receive(ReceiveMode::PeekLock))?)?;
    assert_eq!(
        fixture.bound(queue, 12, dead_letter(&locked))?,
        CommandOutcome::DeadLettered
    );
    let drained =
        delivery(fixture.bound(queue_dlq, 13, receive(ReceiveMode::ReceiveAndDelete))?)?;
    assert_eq!(drained.body, b"queue-body");
    assert_eq!(drained.sequence, locked.sequence);
    assert!(drained.lock.is_none());
    assert!(drained.dead_letter.is_some());
    assert_eq!(
        fixture.bound(topic, 14, send("topic-body"))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: vec![child.target().clone()],
        }
    );
    let locked = delivery(fixture.bound(child, 15, receive(ReceiveMode::PeekLock))?)?;
    assert_eq!(
        fixture.bound(child, 16, dead_letter(&locked))?,
        CommandOutcome::DeadLettered
    );
    let drained =
        delivery(fixture.bound(child_dlq, 17, receive(ReceiveMode::ReceiveAndDelete))?)?;
    assert_eq!(drained.body, b"topic-body");
    assert_eq!(drained.sequence, locked.sequence);
    assert!(drained.lock.is_none());
    assert!(drained.dead_letter.is_some());
    Ok(())
}

fn scope_mismatch_precedes_authority_and_regressed_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider)?;
    let bindings = fixture.populated()?;
    let binding = &bindings[0];
    fixture.replace(
        &keys::entity_metadata(&fixture.namespace, binding.owner()),
        None,
    )?;
    for (namespace, target) in [
        (NamespaceName::new("other")?, binding.target().clone()),
        (
            fixture.namespace.clone(),
            binding.target().dead_letter_queue()?,
        ),
    ] {
        let envelope = BoundCommand::new(
            binding.clone(),
            Command::new(namespace, target, Timestamp::UNIX_EPOCH, complete()),
        );
        let trace = fixture.reject(
            || fixture.machine.apply_bound(&envelope),
            BrokerError::InvalidEntityBinding,
        )?;
        assert!(
            trace.gets.is_empty(),
            "scope mismatch must not consult authority rows"
        );
    }
    Ok(())
}

fn corrupt_owner_heads_refuse_bound_work_before_clock<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider)?;
    for binding in fixture.populated()? {
        let key = keys::entity_metadata(&fixture.namespace, binding.owner());
        let original = fixture.raw().get(&key)?.expect("created owner head");
        let kind = test_kind(binding.kind());
        let wrong_kind = if binding.kind() == EntityBindingKind::Queue {
            TestKind::Topic
        } else {
            TestKind::Queue
        };
        let mut noncanonical = head(kind, 1, false)?;
        noncanonical.splice(1..2, [0x81, 0]);
        let mut trailing = original.clone();
        trailing.push(0);
        let faults = [
            None,
            Some(Vec::new()),
            Some(vec![1, 0xff]),
            Some(head(kind, 0, false)?),
            Some(head(kind, 1, true)?),
            Some(head(wrong_kind, 1, false)?),
            Some(head(TestKind::Unknown, 1, false)?),
            Some(vec![99, 1, 0, 0]),
            Some(noncanonical),
            Some(trailing),
            Some(vec![0; 33]),
        ];
        for fault in faults {
            fixture.replace(&key, fault)?;
            fixture.reject(
                || fixture.machine.validate_binding(&binding),
                BrokerError::EntityMetadataCorrupt,
            )?;
            fixture.reject(
                || fixture.bound(&binding, 0, complete()),
                BrokerError::EntityMetadataCorrupt,
            )?;
            fixture.reject(
                || {
                    fixture
                        .machine
                        .bind_entity(&fixture.namespace, binding.target())
                },
                BrokerError::EntityMetadataCorrupt,
            )?;
        }
        fixture.replace(&key, Some(original))?;
        let shadow_key =
            keys::entity_metadata(&fixture.namespace, &binding.owner().dead_letter_queue()?);
        assert!(fixture.raw().get(&shadow_key)?.is_none());
        fixture.replace(&shadow_key, Some(Vec::new()))?;
        fixture.reject(
            || fixture.machine.validate_binding(&binding),
            BrokerError::EntityMetadataCorrupt,
        )?;
        fixture.reject(
            || fixture.bound(&binding, 0, complete()),
            BrokerError::EntityMetadataCorrupt,
        )?;
        fixture.replace(&shadow_key, None)?;
    }
    Ok(())
}

fn generation_drift_and_disappeared_targets_are_stale_before_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider)?;
    for binding in fixture.populated()? {
        let head_key = keys::entity_metadata(&fixture.namespace, binding.owner());
        let original = fixture.raw().get(&head_key)?.expect("created owner head");
        fixture.replace(&head_key, Some(head(test_kind(binding.kind()), 2, false)?))?;
        fixture.reject(
            || fixture.machine.validate_binding(&binding),
            BrokerError::StaleEntityBinding,
        )?;
        fixture.reject(
            || fixture.bound(&binding, 0, complete()),
            BrokerError::StaleEntityBinding,
        )?;
        let fresh = fixture.read_only(|| {
            fixture
                .machine
                .bind_entity(&fixture.namespace, binding.target())
        })?;
        assert_eq!(
            fresh.generation(),
            2,
            "generation is captured, not hardcoded to one"
        );
        assert_eq!(fresh.target(), binding.target());
        assert_eq!(fresh.owner(), binding.owner());
        assert_eq!(fresh.kind(), binding.kind());
        fixture.read_only(|| fixture.machine.validate_binding(&fresh))?;
        fixture.replace(&head_key, Some(original))?;

        let config_key = match binding.kind() {
            EntityBindingKind::Topic => keys::topic_config(&fixture.namespace, binding.target()),
            EntityBindingKind::Queue | EntityBindingKind::Subscription => {
                keys::queue_config(&fixture.namespace, binding.target())
            }
        };
        let original = fixture
            .raw()
            .get(&config_key)?
            .expect("created target config");
        fixture.replace(&config_key, None)?;
        fixture.reject(
            || fixture.machine.validate_binding(&binding),
            BrokerError::StaleEntityBinding,
        )?;
        fixture.reject(
            || fixture.bound(&binding, 0, complete()),
            BrokerError::StaleEntityBinding,
        )?;
        fixture.replace(&config_key, Some(original))?;
    }
    Ok(())
}

fn bound_complete_preserves_legacy_command_bytes_and_blocks_drift<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider)?;
    let bindings = fixture.populated()?;
    let binding = &bindings[0];
    fixture.bound(binding, 10, send("complete-body"))?;
    let locked = delivery(fixture.bound(binding, 11, receive(ReceiveMode::PeekLock))?)?;
    assert_eq!(locked.sequence, SequenceNumber::new(1));
    assert_eq!(
        locked.lock.expect("peek-lock delivery").token,
        LockToken::new(1)
    );
    let legacy = fixture.command(binding.target(), 42, complete());
    let golden = [
        1, 6, b't', b'e', b'n', b'a', b'n', b't', 6, b'o', b'r', b'd', b'e', b'r', b's', 42, 11, 1,
        1,
    ];
    assert_eq!(codec::encode(&legacy)?, golden);
    assert_eq!(codec::decode::<Command>(&golden)?, legacy);
    let envelope = BoundCommand::new(binding.clone(), legacy.clone());
    assert_eq!(envelope.binding(), binding);
    assert_eq!(envelope.command(), &legacy);
    assert_eq!(codec::encode(envelope.command())?, golden);

    let key = keys::entity_metadata(&fixture.namespace, binding.owner());
    let original = fixture.raw().get(&key)?.expect("created owner head");
    fixture.replace(&key, Some(head(TestKind::Queue, 2, false)?))?;
    fixture.reject(
        || fixture.bound(binding, 0, complete()),
        BrokerError::StaleEntityBinding,
    )?;
    fixture.replace(&key, Some(original))?;
    fixture.reset();
    assert_eq!(
        fixture.machine.apply_bound(&envelope)?,
        CommandOutcome::Completed
    );
    assert!(fixture.trace().gets.contains(&keys::clock()));
    assert_eq!(fixture.trace().applies, 1);
    assert_eq!(codec::encode(envelope.command())?, golden);
    assert_eq!(
        fixture.bound(binding, 43, receive(ReceiveMode::ReceiveAndDelete))?,
        CommandOutcome::Received(None)
    );
    Ok(())
}

fn maximum_composite_bindings_survive_reopen<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider)?;
    fixture.namespace = NamespaceName::new("N".repeat(MAX_NAMESPACE_NAME_BYTES))?;
    let queue = EntityPath::new("q".repeat(MAX_ENTITY_PATH_BYTES))?;
    let topic = EntityPath::new("t".repeat(MAX_ENTITY_PATH_BYTES))?;
    let name = SubscriptionName::new("s".repeat(MAX_SUBSCRIPTION_NAME_CHARACTERS))?;
    let child = fixture.populate(&queue, &topic, &name)?;
    let bindings = fixture.capture_five(&queue, &topic, &child)?;
    assert!(child.as_str().len() > MAX_ENTITY_PATH_BYTES);
    assert!(bindings[1].target().as_str().len() > MAX_ENTITY_PATH_BYTES);
    assert!(bindings[4].target().as_str().len() > child.as_str().len());
    fixture.bound(&bindings[0], 10, send("queue-max"))?;
    fixture.bound(&bindings[2], 11, send("topic-max"))?;
    let fixture = fixture.restart()?;
    for binding in &bindings {
        fixture.read_only(|| fixture.machine.validate_binding(binding))?;
        let recaptured = fixture.read_only(|| {
            fixture
                .machine
                .bind_entity(&fixture.namespace, binding.target())
        })?;
        assert_eq!(&recaptured, binding);
    }
    let received =
        delivery(fixture.bound(&bindings[0], 12, receive(ReceiveMode::ReceiveAndDelete))?)?;
    assert_eq!(received.body, b"queue-max");
    let received =
        delivery(fixture.bound(&bindings[3], 13, receive(ReceiveMode::ReceiveAndDelete))?)?;
    assert_eq!(received.body, b"topic-max");
    for binding in [&bindings[1], &bindings[4]] {
        assert_eq!(
            fixture.bound(binding, 14, receive(ReceiveMode::ReceiveAndDelete))?,
            CommandOutcome::Received(None)
        );
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(#[test] fn $case() -> super::TestResult {
                super::$case(::testkit::MemoryProvider::new())
            })+
        }
        mod durable {
            $(#[test] fn $case() -> super::TestResult {
                super::$case(::testkit::DurableProvider::temporary()?)
            })+
        }
    };
}

for_each_backend! {
    healthy_owner_and_shadow_bindings_run_without_factory_mutation,
    scope_mismatch_precedes_authority_and_regressed_clock,
    corrupt_owner_heads_refuse_bound_work_before_clock,
    generation_drift_and_disappeared_targets_are_stale_before_clock,
    bound_complete_preserves_legacy_command_bytes_and_blocks_drift,
    maximum_composite_bindings_survive_reopen,
}
