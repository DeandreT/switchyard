//! Creation and present-config owner metadata on both stores, not stale-link authority.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig,
    QueueConfigError, RuleDefinition, RuleFilter, RuleName, SequenceNumber, StateMachine,
    SubscriptionConfig, SubscriptionConfigError, SubscriptionName, Timestamp, TopicConfig,
    TopicConfigError, codec, keys,
};
use serde::{Deserialize, Serialize};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum TestKind {
    Queue,
    Topic,
    Subscription,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TestHead {
    generation: u64,
    kind: TestKind,
    retired: bool,
}

fn head(kind: TestKind) -> Result<Value, domain::CodecError> {
    codec::encode(&TestHead {
        generation: 1,
        kind,
        retired: false,
    })
}

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    scans: Vec<(Key, Key, usize)>,
    batches: Vec<WriteBatch>,
    snapshots: usize,
}

#[derive(Debug, Default)]
struct Controls {
    trace: Trace,
    fail_next_apply: bool,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    controls: Arc<Mutex<Controls>>,
}

fn injected_apply_error() -> StorageError {
    StorageError::Backend {
        operation: "apply test batch",
        detail: String::from("injected before commit"),
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.controls
            .lock()
            .expect("controls lock")
            .trace
            .gets
            .push(key.to_vec());
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let fail = {
            let mut controls = self.controls.lock().expect("controls lock");
            controls.trace.batches.push(batch.clone());
            std::mem::take(&mut controls.fail_next_apply)
        };
        if fail {
            return Err(injected_apply_error());
        }
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.controls.lock().expect("controls lock").trace.snapshots += 1;
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.controls
            .lock()
            .expect("controls lock")
            .trace
            .scans
            .push((prefix.to_vec(), start.to_vec(), limit));
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Fixture<P: StoreProvider> {
    namespace: NamespaceName,
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            namespace: NamespaceName::new("tenant")?,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            provider,
        })
    }

    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }

    fn reset(&self) {
        self.machine
            .store()
            .controls
            .lock()
            .expect("controls lock")
            .trace = Trace::default();
    }

    fn trace(&self) -> Trace {
        self.machine
            .store()
            .controls
            .lock()
            .expect("controls lock")
            .trace
            .clone()
    }

    fn at(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }

    fn create_queue(&self, entity: &EntityPath, millis: u64) -> Result<(), BrokerError> {
        assert_eq!(
            self.at(
                entity,
                millis,
                CommandKind::CreateQueue {
                    config: QueueConfig::default()
                }
            )?,
            CommandOutcome::QueueCreated
        );
        Ok(())
    }

    fn create_topic(&self, entity: &EntityPath, millis: u64) -> Result<(), BrokerError> {
        assert_eq!(
            self.at(
                entity,
                millis,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated
        );
        Ok(())
    }

    fn subscribe(
        &self,
        topic: &EntityPath,
        name: &SubscriptionName,
        millis: u64,
    ) -> Result<EntityPath, Box<dyn Error>> {
        let entity = topic.subscription(name)?;
        assert_eq!(
            self.at(
                topic,
                millis,
                CommandKind::CreateSubscription {
                    name: name.clone(),
                    config: SubscriptionConfig::default()
                }
            )?,
            CommandOutcome::SubscriptionCreated {
                entity: entity.clone()
            }
        );
        Ok(entity)
    }

    fn populated(&self) -> Result<(EntityPath, EntityPath, EntityPath), Box<dyn Error>> {
        let queue = EntityPath::new("orders")?;
        let topic = EntityPath::new("events")?;
        self.create_queue(&queue, 1)?;
        self.create_topic(&topic, 2)?;
        let child = self.subscribe(&topic, &SubscriptionName::new("alpha")?, 3)?;
        Ok((queue, topic, child))
    }

    fn restore(&self, snapshot: &StoreSnapshot) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for (key, _) in self.raw().snapshot()?.entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        self.raw().apply(batch)
    }

    fn reject(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
        expected: BrokerError,
    ) -> Result<Trace, Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(self.at(entity, millis, kind), Err(expected));
        let trace = self.trace();
        assert_eq!(trace.gets.first(), Some(&keys::clock()));
        assert!(trace.batches.is_empty(), "refusal must not attempt apply");
        assert_eq!(trace.snapshots, 0, "no snapshot fallback");
        assert_eq!(
            self.raw().snapshot()?,
            before,
            "all rows, counters and Clock remain exact"
        );
        Ok(trace)
    }

    fn reject_read<T>(
        &self,
        read: impl FnOnce() -> Result<T, BrokerError>,
        expected: BrokerError,
    ) -> Result<(), Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(read().err(), Some(expected));
        let trace = self.trace();
        assert!(trace.batches.is_empty());
        assert_eq!(trace.snapshots, 0);
        assert!(
            !trace.gets.contains(&keys::clock()),
            "getter must not consult Clock"
        );
        assert_eq!(self.raw().snapshot()?, before);
        Ok(())
    }

    fn assert_batch(&self, mut expected: Vec<(Key, Value)>) {
        let trace = self.trace();
        assert_eq!(
            trace.batches.len(),
            1,
            "creation must commit one complete batch"
        );
        assert_eq!(trace.snapshots, 0);
        let mut actual: Vec<_> = trace.batches[0]
            .mutations()
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put { key, value } => (key.clone(), value.clone()),
                Mutation::Delete { .. } => panic!("creation must not delete prior rows"),
            })
            .collect();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
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
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            provider,
        })
    }
}

fn shadow(config: QueueConfig) -> QueueConfig {
    QueueConfig {
        max_delivery_count: u32::MAX,
        default_time_to_live_millis: None,
        requires_session: false,
        requires_duplicate_detection: false,
        ..config
    }
}

type FreshCreate = (EntityPath, EntityPath, CommandKind, TestKind);

fn fresh_specs(topic: &EntityPath) -> Result<Vec<FreshCreate>, Box<dyn Error>> {
    let queue = EntityPath::new("fresh-queue")?;
    let fresh_topic = EntityPath::new("fresh-topic")?;
    let name = SubscriptionName::new("fresh-child")?;
    let child = topic.subscription(&name)?;
    Ok(vec![
        (
            queue.clone(),
            queue,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            TestKind::Queue,
        ),
        (
            fresh_topic.clone(),
            fresh_topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
            TestKind::Topic,
        ),
        (
            child,
            topic.clone(),
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default(),
            },
            TestKind::Subscription,
        ),
    ])
}

fn creates_one_owner_head_and_shared_shadows_atomically<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let queue = EntityPath::new("orders")?;
    let queue_shadow = queue.dead_letter_queue()?;
    let queue_config = QueueConfig {
        lock_duration_millis: 15_000,
        max_delivery_count: 7,
        max_message_bytes: 4_096,
        default_time_to_live_millis: Some(3_000),
        requires_session: true,
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    fixture.reset();
    assert_eq!(
        fixture.at(
            &queue,
            1,
            CommandKind::CreateQueue {
                config: queue_config
            }
        )?,
        CommandOutcome::QueueCreated
    );
    fixture.assert_batch(vec![
        (
            keys::entity_metadata(&fixture.namespace, &queue),
            head(TestKind::Queue)?,
        ),
        (
            keys::queue_config(&fixture.namespace, &queue),
            codec::encode(&queue_config)?,
        ),
        (
            keys::queue_config(&fixture.namespace, &queue_shadow),
            codec::encode(&shadow(queue_config))?,
        ),
        (keys::clock(), codec::encode(&Timestamp::from_millis(1))?),
    ]);
    let topic = EntityPath::new("events")?;
    fixture.reset();
    fixture.create_topic(&topic, 2)?;
    fixture.assert_batch(vec![
        (
            keys::entity_metadata(&fixture.namespace, &topic),
            head(TestKind::Topic)?,
        ),
        (
            keys::topic_config(&fixture.namespace, &topic),
            codec::encode(&TopicConfig::default())?,
        ),
        (keys::clock(), codec::encode(&Timestamp::from_millis(2))?),
    ]);
    let name = SubscriptionName::new("alpha")?;
    fixture.reset();
    let child = fixture.subscribe(&topic, &name, 3)?;
    let child_shadow = child.dead_letter_queue()?;
    let child_config = QueueConfig::default();
    let rule = RuleDefinition {
        name: RuleName::new(domain::DEFAULT_RULE_NAME)?,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(3),
    };
    fixture.assert_batch(vec![
        (
            keys::entity_metadata(&fixture.namespace, &child),
            head(TestKind::Subscription)?,
        ),
        (
            keys::topic_subscription(&fixture.namespace, &topic, &name),
            codec::encode(&child)?,
        ),
        (
            keys::queue_config(&fixture.namespace, &child),
            codec::encode(&child_config)?,
        ),
        (
            keys::queue_config(&fixture.namespace, &child_shadow),
            codec::encode(&shadow(child_config))?,
        ),
        (
            keys::subscription_rule(&fixture.namespace, &child, &rule.name),
            codec::encode(&rule)?,
        ),
        (keys::clock(), codec::encode(&Timestamp::from_millis(3))?),
    ]);
    let mut actual_heads: Vec<_> = fixture
        .raw()
        .snapshot()?
        .entries()
        .iter()
        .filter(|(key, _)| key.first() == Some(&0x11))
        .cloned()
        .collect();
    let mut expected_heads = vec![
        (
            keys::entity_metadata(&fixture.namespace, &queue),
            head(TestKind::Queue)?,
        ),
        (
            keys::entity_metadata(&fixture.namespace, &topic),
            head(TestKind::Topic)?,
        ),
        (
            keys::entity_metadata(&fixture.namespace, &child),
            head(TestKind::Subscription)?,
        ),
    ];
    actual_heads.sort();
    expected_heads.sort();
    assert_eq!(actual_heads, expected_heads);
    for owner in [&queue, &topic, &child] {
        assert_eq!(
            fixture.raw().get(&keys::entity_metadata(
                &fixture.namespace,
                &owner.dead_letter_queue()?
            ))?,
            None
        );
    }
    for (entity, config) in [(&queue, queue_config), (&child, child_config)] {
        assert_eq!(
            fixture.machine.queue_config(&fixture.namespace, entity)?,
            Some(config)
        );
        assert_eq!(
            fixture
                .machine
                .queue_config(&fixture.namespace, &entity.dead_letter_queue()?)?,
            Some(shadow(config))
        );
    }
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &topic)?,
        Some(TopicConfig::default())
    );
    Ok(())
}

fn corrupt_heads(kind: TestKind, canonical: &[u8]) -> Result<Vec<Option<Value>>, Box<dyn Error>> {
    assert_eq!(canonical, head(kind)?.as_slice());
    let wrong = if kind == TestKind::Queue {
        TestKind::Topic
    } else {
        TestKind::Queue
    };
    let mut wrong_version = canonical.to_vec();
    wrong_version[0] = 99;
    let mut noncanonical = vec![codec::VALUE_FORMAT_V1, 0x81, 0];
    noncanonical.extend_from_slice(&canonical[2..]);
    let mut trailing = canonical.to_vec();
    trailing.push(0);
    let mut oversized = canonical.to_vec();
    oversized.resize(1_024, 0);
    let mut bad_bool = canonical.to_vec();
    *bad_bool.last_mut().expect("retired field") = 2;
    Ok(vec![
        None,
        Some(Vec::new()),
        Some(wrong_version),
        Some(canonical[..canonical.len() - 1].to_vec()),
        Some(codec::encode(&TestHead {
            generation: 0,
            kind,
            retired: false,
        })?),
        Some(codec::encode(&TestHead {
            generation: 1,
            kind,
            retired: true,
        })?),
        Some(head(wrong)?),
        Some(head(TestKind::Unknown)?),
        Some(noncanonical),
        Some(trailing),
        Some(oversized),
        Some(bad_bool),
    ])
}

fn present_configs_require_canonical_live_heads_and_no_shadow_head<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let (queue, topic, child) = fixture.populated()?;
    let healthy = fixture.raw().snapshot()?;
    for (owner, kind) in [
        (&queue, TestKind::Queue),
        (&topic, TestKind::Topic),
        (&child, TestKind::Subscription),
    ] {
        let key = keys::entity_metadata(&fixture.namespace, owner);
        let canonical = fixture.raw().get(&key)?.expect("head from real creation");
        for value in corrupt_heads(kind, &canonical)? {
            fixture.restore(&healthy)?;
            let batch = match value {
                Some(value) => WriteBatch::default().put(key.clone(), value),
                None => WriteBatch::default().delete(key.clone()),
            };
            fixture.raw().apply(batch)?;
            if kind == TestKind::Topic {
                fixture.reject_read(
                    || fixture.machine.topic_config(&fixture.namespace, owner),
                    BrokerError::EntityMetadataCorrupt,
                )?;
            } else {
                fixture.reject_read(
                    || fixture.machine.queue_config(&fixture.namespace, owner),
                    BrokerError::EntityMetadataCorrupt,
                )?;
                fixture.reject_read(
                    || {
                        fixture.machine.queue_config(
                            &fixture.namespace,
                            &owner.dead_letter_queue().expect("shadow"),
                        )
                    },
                    BrokerError::EntityMetadataCorrupt,
                )?;
            }
            if kind != TestKind::Queue {
                fixture.reject_read(
                    || fixture.machine.subscriptions(&fixture.namespace, &topic, 0),
                    BrokerError::EntityMetadataCorrupt,
                )?;
            }
        }
        fixture.restore(&healthy)?;
        fixture.raw().apply(WriteBatch::default().put(
            keys::entity_metadata(&fixture.namespace, &owner.dead_letter_queue()?),
            canonical,
        ))?;
        if kind == TestKind::Topic {
            fixture.reject_read(
                || fixture.machine.topic_config(&fixture.namespace, owner),
                BrokerError::EntityMetadataCorrupt,
            )?;
        } else {
            fixture.reject_read(
                || fixture.machine.queue_config(&fixture.namespace, owner),
                BrokerError::EntityMetadataCorrupt,
            )?;
            fixture.reject_read(
                || {
                    fixture.machine.queue_config(
                        &fixture.namespace,
                        &owner.dead_letter_queue().expect("shadow"),
                    )
                },
                BrokerError::EntityMetadataCorrupt,
            )?;
        }
        fixture.restore(&healthy)?;
    }
    Ok(())
}

fn absence_and_structural_topology_errors_keep_priority_over_heads<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let (_, topic, child) = fixture.populated()?;
    let healthy = fixture.raw().snapshot()?;
    let absent = EntityPath::new("absent")?;
    fixture.raw().apply(WriteBatch::default().put(
        keys::entity_metadata(&fixture.namespace, &absent),
        Vec::new(),
    ))?;
    fixture.reset();
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &absent)?,
        None
    );
    assert_eq!(
        fixture.trace().gets,
        vec![keys::queue_config(&fixture.namespace, &absent)]
    );
    fixture.reset();
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &absent)?,
        None
    );
    assert_eq!(
        fixture.trace().gets,
        vec![keys::topic_config(&fixture.namespace, &absent)]
    );
    fixture.restore(&healthy)?;
    fixture.raw().apply(
        WriteBatch::default()
            .delete(keys::queue_config(&fixture.namespace, &child))
            .delete(keys::entity_metadata(&fixture.namespace, &child)),
    )?;
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &child)?,
        None
    );
    fixture.reject_read(
        || fixture.machine.subscriptions(&fixture.namespace, &topic, 1),
        BrokerError::DanglingSubscription {
            entity: child.clone(),
        },
    )?;
    fixture.restore(&healthy)?;
    fixture.raw().apply(
        WriteBatch::default()
            .delete(keys::entity_metadata(&fixture.namespace, &topic))
            .put(
                keys::topic_config(&fixture.namespace, &topic),
                codec::encode(&TopicConfig {
                    max_message_bytes: 0,
                    ..TopicConfig::default()
                })?,
            ),
    )?;
    fixture.reject_read(
        || fixture.machine.topic_config(&fixture.namespace, &topic),
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture.reject_read(
        || fixture.machine.subscriptions(&fixture.namespace, &topic, 1),
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture.restore(&healthy)?;
    fixture.raw().apply(
        WriteBatch::default()
            .delete(keys::entity_metadata(&fixture.namespace, &child))
            .put(
                keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
                codec::encode(&QueueConfig::default())?,
            ),
    )?;
    fixture.reject_read(
        || fixture.machine.subscriptions(&fixture.namespace, &topic, 1),
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture.restore(&healthy)?;
    fixture
        .raw()
        .apply(WriteBatch::default().delete(keys::topic_config(&fixture.namespace, &topic)))?;
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &topic)?,
        None
    );
    fixture.reject_read(
        || fixture.machine.subscriptions(&fixture.namespace, &topic, 1),
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture.reject(&topic, 4, scheduled_send(20), BrokerError::QueueNotFound)?;
    Ok(())
}

fn fresh_create_refuses_orphans_without_relaxing_priorities<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let (queue, topic, child) = fixture.populated()?;
    let healthy = fixture.raw().snapshot()?;
    for (owner, target, command, kind) in fresh_specs(&topic)? {
        let canonical = fixture
            .raw()
            .get(&keys::entity_metadata(
                &fixture.namespace,
                match kind {
                    TestKind::Queue => &queue,
                    TestKind::Topic => &topic,
                    _ => &child,
                },
            ))?
            .expect("real head");
        for location in [owner.clone(), owner.dead_letter_queue()?] {
            for value in [canonical.clone(), Vec::new()] {
                fixture.restore(&healthy)?;
                fixture.raw().apply(
                    WriteBatch::default()
                        .put(keys::entity_metadata(&fixture.namespace, &location), value),
                )?;
                fixture.reject(
                    &target,
                    4,
                    command.clone(),
                    BrokerError::EntityMetadataCorrupt,
                )?;
            }
        }
        if kind != TestKind::Topic {
            let shadow = owner.dead_letter_queue()?;
            for occupied_key in [
                keys::queue_config(&fixture.namespace, &shadow),
                keys::topic_config(&fixture.namespace, &shadow),
            ] {
                fixture.restore(&healthy)?;
                fixture
                    .raw()
                    .apply(WriteBatch::default().put(occupied_key, vec![255]))?;
                let expected = if kind == TestKind::Queue {
                    BrokerError::EntityMetadataCorrupt
                } else {
                    BrokerError::TopicTopologyCorrupt
                };
                fixture.reject(&target, 4, command.clone(), expected)?;
            }
        }
        fixture.restore(&healthy)?;
        let head_key = keys::entity_metadata(&fixture.namespace, &owner);
        fixture
            .raw()
            .apply(WriteBatch::default().put(head_key.clone(), Vec::new()))?;
        let (invalid, expected) = match command {
            CommandKind::CreateQueue { .. } => (
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        lock_duration_millis: 0,
                        ..QueueConfig::default()
                    },
                },
                BrokerError::QueueConfig(QueueConfigError::LockDurationTooShort),
            ),
            CommandKind::CreateTopic { .. } => (
                CommandKind::CreateTopic {
                    config: TopicConfig {
                        max_message_bytes: 0,
                        ..TopicConfig::default()
                    },
                },
                BrokerError::TopicConfig(TopicConfigError::MaxMessageBytesTooSmall),
            ),
            CommandKind::CreateSubscription { name, .. } => (
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig {
                        lock_duration_millis: 0,
                        ..SubscriptionConfig::default()
                    },
                },
                BrokerError::SubscriptionConfig(SubscriptionConfigError::LockDurationTooShort),
            ),
            _ => unreachable!("creation specs"),
        };
        let trace = fixture.reject(&target, 4, invalid, expected)?;
        assert!(
            !trace.gets.contains(&head_key),
            "numeric error precedes proposed owner-head admission"
        );
    }
    for (owner, target, command, expected) in [
        (
            queue.clone(),
            queue.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig {
                    lock_duration_millis: 0,
                    ..QueueConfig::default()
                },
            },
            BrokerError::QueueAlreadyExists,
        ),
        (
            topic.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig {
                    max_message_bytes: 0,
                    ..TopicConfig::default()
                },
            },
            BrokerError::TopicAlreadyExists,
        ),
        (
            child.clone(),
            topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("alpha")?,
                config: SubscriptionConfig {
                    lock_duration_millis: 0,
                    ..SubscriptionConfig::default()
                },
            },
            BrokerError::SubscriptionAlreadyExists,
        ),
    ] {
        fixture.restore(&healthy)?;
        fixture.raw().apply(WriteBatch::default().put(
            keys::entity_metadata(&fixture.namespace, &owner),
            Vec::new(),
        ))?;
        fixture.reject(&target, 4, command, expected)?;
    }
    fixture.restore(&healthy)?;
    let future = EntityPath::new("clock-priority")?;
    fixture.raw().apply(WriteBatch::default().put(
        keys::entity_metadata(&fixture.namespace, &future),
        Vec::new(),
    ))?;
    let trace = fixture.reject(
        &future,
        2,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(3),
            proposed: Timestamp::from_millis(2),
        },
    )?;
    assert_eq!(trace.gets, vec![keys::clock()]);
    assert!(trace.scans.is_empty());
    for (reserved, command, expected) in [
        (
            queue.dead_letter_queue()?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            child,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            BrokerError::EntityPathReserved,
        ),
        (
            EntityPath::new("events/$management")?,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
            BrokerError::EntityPathReserved,
        ),
    ] {
        fixture.raw().apply(WriteBatch::default().put(
            keys::entity_metadata(&fixture.namespace, &reserved),
            Vec::new(),
        ))?;
        let trace = fixture.reject(&reserved, 4, command, expected)?;
        assert_eq!(trace.gets, vec![keys::clock()]);
        assert!(trace.scans.is_empty());
    }
    Ok(())
}

fn scheduled_send(due: u64) -> CommandKind {
    CommandKind::Send {
        message_id: String::from("scheduled-head-proof"),
        body: b"scheduled body".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: Some(Timestamp::from_millis(due)),
        envelope: None,
    }
}

fn scheduled_publication_checks_parent_but_defers_child_head_until_due<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let topic = EntityPath::new("events")?;
    fixture.create_topic(&topic, 1)?;
    let child = fixture.subscribe(&topic, &SubscriptionName::new("alpha")?, 2)?;
    let healthy = fixture.raw().snapshot()?;
    let parent_key = keys::entity_metadata(&fixture.namespace, &topic);
    let child_key = keys::entity_metadata(&fixture.namespace, &child);
    let child_head = fixture.raw().get(&child_key)?.expect("real child head");
    fixture
        .raw()
        .apply(WriteBatch::default().delete(parent_key.clone()))?;
    fixture.reject(
        &topic,
        10,
        scheduled_send(20),
        BrokerError::EntityMetadataCorrupt,
    )?;
    fixture.restore(&healthy)?;
    fixture
        .raw()
        .apply(WriteBatch::default().delete(child_key.clone()))?;
    fixture.reset();
    assert_eq!(
        fixture.at(&topic, 10, scheduled_send(20))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: Vec::new()
        }
    );
    let trace = fixture.trace();
    assert_eq!(trace.batches.len(), 1);
    assert_eq!(trace.snapshots, 0);
    assert!(trace.gets.contains(&parent_key));
    assert!(!trace.gets.contains(&child_key));
    assert!(!trace.scans.iter().any(|(prefix, _, _)| prefix
        == &keys::topic_subscription_prefix(&fixture.namespace, &topic)
        || prefix == &keys::subscription_rule_prefix(&fixture.namespace, &child)));
    let scheduled = fixture.raw().snapshot()?;
    fixture.reset();
    assert_eq!(
        fixture.at(&topic, 19, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new()
        }
    );
    assert!(fixture.trace().batches.is_empty());
    assert!(!fixture.trace().gets.contains(&child_key));
    assert_eq!(fixture.raw().snapshot()?, scheduled);
    fixture.reject(
        &topic,
        20,
        CommandKind::ActivateScheduled,
        BrokerError::EntityMetadataCorrupt,
    )?;
    fixture
        .raw()
        .apply(WriteBatch::default().put(child_key.clone(), child_head.clone()))?;
    fixture.reset();
    assert_eq!(
        fixture.at(&topic, 20, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![child.clone()]
        }
    );
    assert_eq!(fixture.trace().batches.len(), 1);
    assert!(fixture.trace().gets.contains(&child_key));
    assert_eq!(fixture.raw().get(&child_key)?, Some(child_head));
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &topic, SequenceNumber::new(1))?,
        None
    );
    let active = fixture
        .machine
        .message(&fixture.namespace, &child, SequenceNumber::new(2))?
        .expect("one activated copy");
    assert_eq!(active.body, b"scheduled body");
    let final_image = fixture.raw().snapshot()?;
    fixture.reset();
    assert_eq!(
        fixture.at(&topic, 21, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new()
        }
    );
    assert!(fixture.trace().batches.is_empty());
    assert_eq!(fixture.raw().snapshot()?, final_image);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, final_image);
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &topic, 1)?,
        vec![child]
    );
    Ok(())
}

fn maximum_composite_paths_and_exact_heads_survive_reopen<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let queue = EntityPath::new("q".repeat(domain::MAX_ENTITY_PATH_BYTES))?;
    let topic = EntityPath::new("t".repeat(domain::MAX_ENTITY_PATH_BYTES))?;
    let name = SubscriptionName::new("s".repeat(domain::MAX_SUBSCRIPTION_NAME_CHARACTERS))?;
    fixture.create_queue(&queue, 1)?;
    fixture.create_topic(&topic, 2)?;
    let child = fixture.subscribe(&topic, &name, 3)?;
    assert!(child.as_str().len() > domain::MAX_ENTITY_PATH_BYTES);
    assert!(child.dead_letter_queue()?.as_str().len() > child.as_str().len());
    for (owner, kind) in [
        (&queue, TestKind::Queue),
        (&topic, TestKind::Topic),
        (&child, TestKind::Subscription),
    ] {
        let captured = fixture
            .raw()
            .get(&keys::entity_metadata(&fixture.namespace, owner))?
            .expect("real maximum-path head");
        assert_eq!(captured, head(kind)?);
        let decoded: TestHead = codec::decode(&captured)?;
        assert_eq!(
            decoded,
            TestHead {
                generation: 1,
                kind,
                retired: false
            }
        );
    }
    let before = fixture.raw().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, before);
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &topic)?,
        Some(TopicConfig::default())
    );
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &topic, 1)?,
        vec![child.clone()]
    );
    for owner in [&queue, &child] {
        assert_eq!(
            fixture.machine.queue_config(&fixture.namespace, owner)?,
            Some(QueueConfig::default())
        );
        assert_eq!(
            fixture
                .machine
                .queue_config(&fixture.namespace, &owner.dead_letter_queue()?)?,
            Some(shadow(QueueConfig::default()))
        );
    }
    assert_eq!(fixture.raw().snapshot()?, before);
    Ok(())
}

fn failed_create_apply_preserves_heads_shadows_and_clock_then_retries<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let topic = EntityPath::new("events")?;
    fixture.create_topic(&topic, 1)?;
    let healthy = fixture.raw().snapshot()?;
    for (owner, target, command, kind) in fresh_specs(&topic)? {
        fixture.restore(&healthy)?;
        fixture.reset();
        fixture
            .machine
            .store()
            .controls
            .lock()
            .expect("controls lock")
            .fail_next_apply = true;
        assert_eq!(
            fixture.at(&target, 2, command.clone()),
            Err(BrokerError::Storage(injected_apply_error()))
        );
        let trace = fixture.trace();
        assert_eq!(trace.batches.len(), 1, "one attempted atomic commit");
        assert_eq!(trace.snapshots, 0);
        assert!(trace.batches[0].mutations().iter().any(|mutation| matches!(mutation, Mutation::Put { key, value } if key == &keys::entity_metadata(&fixture.namespace, &owner) && value == &head(kind).expect("canonical head"))));
        assert!(trace.batches[0].mutations().iter().any(
            |mutation| matches!(mutation, Mutation::Put { key, .. } if key == &keys::clock())
        ));
        assert_eq!(fixture.raw().snapshot()?, healthy);
        assert_eq!(
            fixture
                .raw()
                .get(&keys::entity_metadata(&fixture.namespace, &owner))?,
            None
        );
        assert_eq!(
            fixture.raw().get(&keys::entity_metadata(
                &fixture.namespace,
                &owner.dead_letter_queue()?
            ))?,
            None
        );
        fixture.reset();
        let expected = match kind {
            TestKind::Queue => CommandOutcome::QueueCreated,
            TestKind::Topic => CommandOutcome::TopicCreated,
            TestKind::Subscription => CommandOutcome::SubscriptionCreated {
                entity: owner.clone(),
            },
            TestKind::Unknown => unreachable!("creation specs"),
        };
        assert_eq!(fixture.at(&target, 2, command)?, expected);
        assert_eq!(fixture.trace().batches.len(), 1);
        assert_eq!(
            fixture
                .raw()
                .get(&keys::entity_metadata(&fixture.namespace, &owner))?,
            Some(head(kind)?)
        );
        assert_eq!(
            fixture.raw().get(&keys::entity_metadata(
                &fixture.namespace,
                &owner.dead_letter_queue()?
            ))?,
            None
        );
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+
        }
        mod durable {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+
        }
    };
}

for_each_backend! {
    creates_one_owner_head_and_shared_shadows_atomically,
    present_configs_require_canonical_live_heads_and_no_shadow_head,
    absence_and_structural_topology_errors_keep_priority_over_heads,
    fresh_create_refuses_orphans_without_relaxing_priorities,
    scheduled_publication_checks_parent_but_defers_child_head_until_due,
    maximum_composite_paths_and_exact_heads_survive_reopen,
    failed_create_apply_preserves_heads_shadows_and_clock_then_retries,
}
