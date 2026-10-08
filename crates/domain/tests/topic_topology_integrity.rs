//! Listed topic topology health and atomic routing on both storage backends.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, MessageInput, NamespaceName,
    QueueConfig, ReceiveMode, RuleName, SequenceNumber, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Scan {
    prefix: Key,
    start: Key,
    limit: usize,
    returned: usize,
}

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    scans: Vec<Scan>,
    batches: Vec<WriteBatch>,
    snapshots: usize,
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
        self.trace
            .lock()
            .expect("trace lock")
            .batches
            .push(batch.clone());
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
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.trace.lock().expect("trace lock").scans.push(Scan {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
            returned: rows.len(),
        });
        Ok(rows)
    }
}

struct Fixture<P: StoreProvider> {
    namespace: NamespaceName,
    topic: EntityPath,
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let fixture = Self {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new("events")?,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        };
        assert_eq!(
            fixture.at(
                0,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated,
        );
        Ok(fixture)
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

    fn at(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.topic, millis, kind)
    }

    fn at_entity(
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

    fn subscribe(&self, millis: u64, name: &str) -> Result<EntityPath, Box<dyn Error>> {
        let name = SubscriptionName::new(name)?;
        let entity = self.topic.subscription(&name)?;
        assert_eq!(
            self.at(
                millis,
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig::default(),
                }
            )?,
            CommandOutcome::SubscriptionCreated {
                entity: entity.clone()
            },
        );
        Ok(entity)
    }

    fn list(&self, limit: usize) -> Result<Vec<EntityPath>, BrokerError> {
        self.machine
            .subscriptions(&self.namespace, &self.topic, limit)
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
        assert_eq!(self.at_entity(entity, millis, kind), Err(expected));
        let trace = self.trace();
        assert_eq!(trace.gets.first(), Some(&keys::clock()));
        assert!(
            trace.batches.is_empty(),
            "rejection must not attempt a commit"
        );
        assert_eq!(trace.snapshots, 0, "no snapshot fallback");
        assert_eq!(
            self.raw().snapshot()?,
            before,
            "Clock, counters and every raw row stay exact"
        );
        Ok(trace)
    }

    fn reject_list(&self, limit: usize, expected: BrokerError) -> Result<Trace, Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(self.list(limit), Err(expected));
        let trace = self.trace();
        assert!(trace.batches.is_empty());
        assert_eq!(trace.snapshots, 0);
        assert_eq!(self.raw().snapshot()?, before);
        Ok(trace)
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            namespace,
            topic,
            machine,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            namespace,
            topic,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        })
    }
}

fn input(id: &str, due: Option<u64>) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: b"payload".to_vec(),
        scheduled_enqueue_at: due.map(Timestamp::from_millis),
        ..MessageInput::default()
    }
}

fn send(input: MessageInput) -> CommandKind {
    CommandKind::Send {
        message_id: input.message_id,
        body: input.body,
        time_to_live_millis: input.time_to_live_millis,
        session_id: input.session_id,
        scheduled_enqueue_at: input.scheduled_enqueue_at,
        envelope: input.envelope,
    }
}

fn assert_membership_scan(
    trace: &Trace,
    namespace: &NamespaceName,
    topic: &EntityPath,
    returned: usize,
) {
    let prefix = keys::topic_subscription_prefix(namespace, topic);
    assert_eq!(
        trace.scans,
        vec![Scan {
            start: prefix.clone(),
            prefix,
            limit: domain::MAX_TOPIC_SUBSCRIPTIONS + 1,
            returned,
        }]
    );
}

fn membership_keys_and_values_require_exact_canonical_bytes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let child = fixture.subscribe(1, "alpha")?;
    let name = SubscriptionName::new("alpha")?;
    let key = keys::topic_subscription(&fixture.namespace, &fixture.topic, &name);
    let healthy = fixture.raw().snapshot()?;
    let canonical = codec::encode(&child)?;
    let uppercase = codec::encode(&child.as_str().to_ascii_uppercase())?;
    assert_eq!(
        codec::decode::<EntityPath>(&uppercase)?,
        child,
        "decoded equality would hide corruption"
    );
    let mut trailing = canonical.clone();
    trailing.push(0);
    let foreign = EntityPath::new("elsewhere/subscriptions/alpha")?;
    for value in [codec::encode(&foreign)?, uppercase, vec![255], trailing] {
        fixture
            .raw()
            .apply(WriteBatch::default().put(key.clone(), value))?;
        let trace = fixture.reject_list(10, BrokerError::TopicTopologyCorrupt)?;
        assert_membership_scan(&trace, &fixture.namespace, &fixture.topic, 1);
        fixture.restore(&healthy)?;
    }
    let prefix = keys::topic_subscription_prefix(&fixture.namespace, &fixture.topic);
    for suffix in [b"ALPHA".as_slice(), b"alpha\0", b"", b"alpha/other", &[255]] {
        let mut malformed = prefix.clone();
        malformed.extend_from_slice(suffix);
        fixture.raw().apply(
            WriteBatch::default()
                .delete(key.clone())
                .put(malformed, canonical.clone()),
        )?;
        fixture.reject_list(10, BrokerError::MalformedIndexKey)?;
        fixture.restore(&healthy)?;
    }
    assert_eq!(fixture.list(10)?, vec![child]);
    Ok(())
}

fn parent_backing_and_shadow_profiles_are_proved_before_routing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let child = fixture.subscribe(1, "alpha")?;
    let shadow = child.dead_letter_queue()?;
    let parent_key = keys::topic_config(&fixture.namespace, &fixture.topic);
    let child_key = keys::queue_config(&fixture.namespace, &child);
    let shadow_key = keys::queue_config(&fixture.namespace, &shadow);
    let backing: QueueConfig = codec::decode(&fixture.raw().get(&child_key)?.expect("backing"))?;
    let shadow_config: QueueConfig =
        codec::decode(&fixture.raw().get(&shadow_key)?.expect("shadow"))?;
    let healthy = fixture.raw().snapshot()?;
    fixture
        .raw()
        .apply(WriteBatch::default().delete(child_key.clone()))?;
    fixture.reject_list(
        1,
        BrokerError::DanglingSubscription {
            entity: child.clone(),
        },
    )?;
    fixture.restore(&healthy)?;

    fixture
        .raw()
        .apply(WriteBatch::default().delete(parent_key.clone()))?;
    fixture.reject_list(1, BrokerError::TopicTopologyCorrupt)?;
    fixture.reject(
        &fixture.topic,
        2,
        send(input("missing-parent", None)),
        BrokerError::QueueNotFound,
    )?;
    fixture.restore(&healthy)?;

    let mut faults = vec![
        WriteBatch::default().put(
            parent_key.clone(),
            codec::encode(&TopicConfig {
                max_message_bytes: 0,
                ..TopicConfig::default()
            })?,
        ),
        WriteBatch::default().put(
            parent_key.clone(),
            codec::encode(&TopicConfig {
                default_time_to_live_millis: Some(0),
                ..TopicConfig::default()
            })?,
        ),
        WriteBatch::default().put(
            keys::queue_config(&fixture.namespace, &fixture.topic),
            codec::encode(&backing)?,
        ),
        WriteBatch::default().delete(shadow_key.clone()),
        WriteBatch::default().put(
            keys::topic_config(&fixture.namespace, &child),
            codec::encode(&TopicConfig::default())?,
        ),
        WriteBatch::default().put(
            keys::topic_config(&fixture.namespace, &shadow),
            codec::encode(&TopicConfig::default())?,
        ),
    ];
    for corrupted in [
        QueueConfig {
            lock_duration_millis: 0,
            ..backing
        },
        QueueConfig {
            lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS + 1,
            ..backing
        },
        QueueConfig {
            max_delivery_count: 0,
            ..backing
        },
        QueueConfig {
            default_time_to_live_millis: Some(0),
            ..backing
        },
        QueueConfig {
            max_message_bytes: backing.max_message_bytes + 1,
            ..backing
        },
        QueueConfig {
            requires_session: true,
            ..backing
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..backing
        },
        QueueConfig {
            duplicate_detection_history_millis: backing.duplicate_detection_history_millis + 1,
            ..backing
        },
    ] {
        faults.push(WriteBatch::default().put(child_key.clone(), codec::encode(&corrupted)?));
    }
    for corrupted in [
        QueueConfig {
            lock_duration_millis: shadow_config.lock_duration_millis + 1,
            ..shadow_config
        },
        QueueConfig {
            max_delivery_count: 10,
            ..shadow_config
        },
        QueueConfig {
            default_time_to_live_millis: Some(1),
            ..shadow_config
        },
        QueueConfig {
            max_message_bytes: shadow_config.max_message_bytes + 1,
            ..shadow_config
        },
        QueueConfig {
            requires_session: true,
            ..shadow_config
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..shadow_config
        },
        QueueConfig {
            duplicate_detection_history_millis: shadow_config.duplicate_detection_history_millis
                + 1,
            ..shadow_config
        },
    ] {
        faults.push(WriteBatch::default().put(shadow_key.clone(), codec::encode(&corrupted)?));
    }
    for fault in faults {
        fixture.raw().apply(fault)?;
        fixture.reject_list(1, BrokerError::TopicTopologyCorrupt)?;
        fixture.reject(
            &fixture.topic,
            2,
            send(input("blocked", None)),
            BrokerError::TopicTopologyCorrupt,
        )?;
        fixture.restore(&healthy)?;
    }
    fixture
        .raw()
        .apply(WriteBatch::default().put(parent_key.clone(), Vec::new()))?;
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.topic),
        Err(domain::CodecError::EmptyEnvelope.into())
    );
    fixture.restore(&healthy)?;
    for reserved in [&child, &shadow, &EntityPath::new("events/$management")?] {
        fixture.raw().apply(WriteBatch::default().put(
            keys::topic_config(&fixture.namespace, reserved),
            codec::encode(&TopicConfig::default())?,
        ))?;
        assert_eq!(
            fixture.machine.topic_config(&fixture.namespace, reserved),
            Err(BrokerError::TopicTopologyCorrupt)
        );
        fixture.restore(&healthy)?;
    }
    let absent = EntityPath::new("absent")?;
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &absent)?,
        None
    );
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &absent, 10),
        Err(BrokerError::TopicNotFound)
    );
    assert_eq!(fixture.list(1)?, vec![child]);
    Ok(())
}

fn late_bad_member_blocks_pages_publish_batches_and_activation_atomically<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    fixture.subscribe(1, "alpha")?;
    fixture.subscribe(2, "middle")?;
    let late = fixture.subscribe(3, "zulu")?;
    assert_eq!(
        fixture.at(10, send(input("due", Some(20))))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: Vec::new(),
        }
    );
    let key = keys::queue_config(&fixture.namespace, &late);
    let config: QueueConfig = codec::decode(&fixture.raw().get(&key)?.expect("late backing"))?;
    fixture.raw().apply(WriteBatch::default().put(
        key,
        codec::encode(&QueueConfig {
            requires_session: true,
            ..config
        })?,
    ))?;
    for limit in [0, 1, 10] {
        let trace = fixture.reject_list(limit, BrokerError::TopicTopologyCorrupt)?;
        assert_membership_scan(&trace, &fixture.namespace, &fixture.topic, 3);
    }
    for kind in [
        send(input("immediate", None)),
        CommandKind::SendBatch {
            messages: vec![
                input("scheduled-first", Some(30)),
                input("immediate-last", None),
            ],
        },
        CommandKind::ActivateScheduled,
    ] {
        fixture.reject(&fixture.topic, 20, kind, BrokerError::TopicTopologyCorrupt)?;
    }
    let fixture = fixture.restart()?;
    fixture.reject(
        &fixture.topic,
        20,
        CommandKind::ActivateScheduled,
        BrokerError::TopicTopologyCorrupt,
    )?;
    Ok(())
}

fn creation_refusals_preserve_config_clock_and_duplicate_priorities<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let child = fixture.subscribe(10, "alpha")?;
    let orphan_topic = EntityPath::new("orphan")?;
    let name = SubscriptionName::new("ghost")?;
    let orphan_child = orphan_topic.subscription(&name)?;
    fixture.raw().apply(WriteBatch::default().put(
        keys::topic_subscription(&fixture.namespace, &orphan_topic, &name),
        codec::encode(&orphan_child)?,
    ))?;
    let invalid = TopicConfig {
        max_message_bytes: 0,
        ..TopicConfig::default()
    };
    let trace = fixture.reject(
        &orphan_topic,
        9,
        CommandKind::CreateTopic { config: invalid },
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        },
    )?;
    assert_eq!(trace.gets, vec![keys::clock()]);
    assert!(trace.scans.is_empty());
    let trace = fixture.reject(
        &orphan_topic,
        11,
        CommandKind::CreateTopic { config: invalid },
        invalid.validate().unwrap_err().into(),
    )?;
    assert!(
        trace.scans.is_empty(),
        "desired numeric config precedes orphan probe"
    );
    let trace = fixture.reject(
        &orphan_topic,
        11,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        BrokerError::TopicTopologyCorrupt,
    )?;
    let prefix = keys::topic_subscription_prefix(&fixture.namespace, &orphan_topic);
    assert_eq!(
        trace.scans,
        vec![Scan {
            start: prefix.clone(),
            prefix,
            limit: 1,
            returned: 1
        }]
    );

    let proposed_name = SubscriptionName::new("occupied")?;
    let proposed = fixture.topic.subscription(&proposed_name)?;
    let shadow = proposed.dead_letter_queue()?;
    let baseline = fixture.raw().snapshot()?;
    for occupied_key in [
        keys::queue_config(&fixture.namespace, &shadow),
        keys::topic_config(&fixture.namespace, &shadow),
    ] {
        fixture
            .raw()
            .apply(WriteBatch::default().put(occupied_key, vec![255]))?;
        fixture.reject(
            &fixture.topic,
            11,
            CommandKind::CreateSubscription {
                name: proposed_name.clone(),
                config: SubscriptionConfig::default(),
            },
            BrokerError::TopicTopologyCorrupt,
        )?;
        let invalid = SubscriptionConfig {
            lock_duration_millis: 0,
            ..SubscriptionConfig::default()
        };
        fixture.reject(
            &fixture.topic,
            11,
            CommandKind::CreateSubscription {
                name: proposed_name.clone(),
                config: invalid,
            },
            invalid.validate().unwrap_err().into(),
        )?;
        fixture.restore(&baseline)?;
    }
    let trace = fixture.reject(
        &fixture.topic,
        11,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("alpha")?,
            config: SubscriptionConfig {
                lock_duration_millis: 0,
                ..SubscriptionConfig::default()
            },
        },
        BrokerError::SubscriptionAlreadyExists,
    )?;
    assert!(
        trace.scans.is_empty(),
        "existing member remains the first create-subscription refusal"
    );
    assert_eq!(fixture.list(10)?, vec![child]);
    Ok(())
}

fn all_scheduled_publish_defers_topology_until_atomic_repairable_activation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let first = fixture.subscribe(1, "alpha")?;
    let late = fixture.subscribe(2, "zulu")?;
    let key = keys::topic_subscription(
        &fixture.namespace,
        &fixture.topic,
        &SubscriptionName::new("zulu")?,
    );
    let healthy_value = fixture.raw().get(&key)?.expect("healthy member");
    fixture
        .raw()
        .apply(WriteBatch::default().put(key.clone(), vec![255]))?;
    fixture.reset();
    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: vec![input("one", Some(100)), input("two", Some(100))]
            }
        )?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
            subscriptions: Vec::new(),
        }
    );
    let trace = fixture.trace();
    assert!(
        trace.scans.is_empty(),
        "all scheduled publication does not snapshot membership or rules"
    );
    assert_eq!(trace.batches.len(), 1);
    for child in [&first, &late] {
        let rule_prefix = keys::subscription_rule_prefix(&fixture.namespace, child);
        assert!(trace.gets.iter().all(|key| !key.starts_with(&rule_prefix)));
    }
    let before = fixture.raw().snapshot()?;
    fixture.reset();
    assert_eq!(
        fixture.at(99, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new()
        }
    );
    assert_eq!(
        fixture.raw().snapshot()?,
        before,
        "not-due activation does not commit Clock"
    );
    let trace = fixture.trace();
    assert!(trace.batches.is_empty());
    let scheduled = keys::scheduled_prefix(&fixture.namespace, &fixture.topic);
    assert_eq!(
        trace.scans,
        vec![Scan {
            start: scheduled.clone(),
            prefix: scheduled,
            limit: 1,
            returned: 1
        }]
    );
    fixture.reject(
        &fixture.topic,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture
        .raw()
        .apply(WriteBatch::default().put(key, healthy_value))?;
    fixture.reset();
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 2,
            deliverable_entities: vec![first.clone(), late.clone()],
        }
    );
    assert_eq!(fixture.trace().batches.len(), 1);
    for child in [&first, &late] {
        assert_eq!(
            fixture
                .machine
                .ready_sequences(&fixture.namespace, child, 10)?,
            vec![SequenceNumber::new(3), SequenceNumber::new(4)]
        );
    }
    for sequence in [SequenceNumber::new(1), SequenceNumber::new(2)] {
        assert!(
            fixture
                .raw()
                .get(&keys::message(&fixture.namespace, &fixture.topic, sequence))?
                .is_none()
        );
        assert!(
            fixture
                .raw()
                .get(&keys::scheduled(
                    &fixture.namespace,
                    &fixture.topic,
                    Timestamp::from_millis(100),
                    sequence
                ))?
                .is_none()
        );
    }
    let counters: domain::QueueCounters = codec::decode(
        &fixture
            .raw()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.topic))?
            .expect("topic counters"),
    )?;
    assert_eq!(counters.next_sequence, 5);
    let fixture = fixture.restart()?;
    let final_state = fixture.raw().snapshot()?;
    assert_eq!(
        fixture.at(101, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new()
        }
    );
    assert_eq!(fixture.raw().snapshot()?, final_state);
    Ok(())
}

fn topology_proof_stays_inside_listed_parent_and_namespace_scopes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let child = fixture.subscribe(1, "alpha")?;
    let neighbor = EntityPath::new("events-neighbor")?;
    let other_namespace = NamespaceName::new("tenant-neighbor")?;
    let orphan = fixture
        .topic
        .subscription(&SubscriptionName::new("unindexed")?)?;
    let mut batch = WriteBatch::default();
    for (namespace, topic) in [
        (&fixture.namespace, &neighbor),
        (&other_namespace, &fixture.topic),
    ] {
        batch.push_put(
            keys::topic_subscription(namespace, topic, &SubscriptionName::new("bad")?),
            vec![255],
        );
        batch.push_put(keys::topic_config(namespace, topic), vec![255]);
    }
    batch.push_put(keys::queue_config(&fixture.namespace, &orphan), vec![255]);
    batch.push_put(
        keys::subscription_rule(&fixture.namespace, &orphan, &RuleName::new("orphan")?),
        vec![255],
    );
    batch.push_put(
        keys::message(&fixture.namespace, &orphan, SequenceNumber::new(99)),
        vec![255],
    );
    fixture.raw().apply(batch)?;
    fixture.reset();
    assert_eq!(fixture.list(10)?, vec![child.clone()]);
    assert_membership_scan(&fixture.trace(), &fixture.namespace, &fixture.topic, 1);
    assert_eq!(
        fixture.at(2, send(input("routed", None)))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: vec![child.clone()],
        }
    );
    let retained = fixture.raw().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, retained);
    assert_eq!(fixture.list(10)?, vec![child.clone()]);
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &child, 10)?,
        vec![SequenceNumber::new(1)]
    );
    assert_eq!(
        fixture.raw().get(&keys::message(
            &fixture.namespace,
            &orphan,
            SequenceNumber::new(99)
        ))?,
        Some(vec![255])
    );
    Ok(())
}

fn full_membership_lookahead_preserves_limit_and_admission_priority<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let backing = QueueConfig::default();
    let shadow_config = QueueConfig {
        max_delivery_count: u32::MAX,
        ..backing
    };
    let mut batch = WriteBatch::default();
    let mut entities = Vec::with_capacity(domain::MAX_TOPIC_SUBSCRIPTIONS);
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        let name = SubscriptionName::new(format!("s{index:04}"))?;
        let child = fixture.topic.subscription(&name)?;
        batch.push_put(
            keys::topic_subscription(&fixture.namespace, &fixture.topic, &name),
            codec::encode(&child)?,
        );
        batch.push_put(
            keys::queue_config(&fixture.namespace, &child),
            codec::encode(&backing)?,
        );
        batch.push_put(
            keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
            codec::encode(&shadow_config)?,
        );
        entities.push(child);
    }
    fixture.raw().apply(batch)?;
    fixture.reset();
    assert_eq!(fixture.list(10)?, entities[..10]);
    let trace = fixture.trace();
    assert_membership_scan(
        &trace,
        &fixture.namespace,
        &fixture.topic,
        domain::MAX_TOPIC_SUBSCRIPTIONS,
    );
    let mut expected_gets = vec![
        keys::topic_config(&fixture.namespace, &fixture.topic),
        keys::queue_config(&fixture.namespace, &fixture.topic),
    ];
    for child in &entities {
        let shadow = child.dead_letter_queue()?;
        expected_gets.push(keys::queue_config(&fixture.namespace, child));
        expected_gets.push(keys::queue_config(&fixture.namespace, &shadow));
        expected_gets.push(keys::topic_config(&fixture.namespace, child));
        expected_gets.push(keys::topic_config(&fixture.namespace, &shadow));
    }
    assert_eq!(trace.gets, expected_gets);
    let invalid = SubscriptionConfig {
        lock_duration_millis: 0,
        ..SubscriptionConfig::default()
    };
    fixture.reject(
        &fixture.topic,
        1,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("new")?,
            config: invalid,
        },
        BrokerError::SubscriptionLimitExceeded {
            maximum: domain::MAX_TOPIC_SUBSCRIPTIONS,
        },
    )?;
    let extra = SubscriptionName::new("zzzz")?;
    fixture.raw().apply(WriteBatch::default().put(
        keys::topic_subscription(&fixture.namespace, &fixture.topic, &extra),
        vec![255],
    ))?;
    let trace = fixture.reject_list(
        1,
        BrokerError::SubscriptionLimitExceeded {
            maximum: domain::MAX_TOPIC_SUBSCRIPTIONS,
        },
    )?;
    assert_membership_scan(
        &trace,
        &fixture.namespace,
        &fixture.topic,
        domain::MAX_TOPIC_SUBSCRIPTIONS + 1,
    );
    assert_eq!(
        trace.gets,
        vec![
            keys::topic_config(&fixture.namespace, &fixture.topic),
            keys::queue_config(&fixture.namespace, &fixture.topic)
        ]
    );
    let trace = fixture.reject(
        &fixture.topic,
        1,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("s0000")?,
            config: invalid,
        },
        BrokerError::SubscriptionAlreadyExists,
    )?;
    assert!(trace.scans.is_empty());
    fixture.raw().apply(WriteBatch::default().put(
        keys::topic_config(&fixture.namespace, &fixture.topic),
        codec::encode(&TopicConfig {
            max_message_bytes: 0,
            ..TopicConfig::default()
        })?,
    ))?;
    fixture.reject_list(1, BrokerError::TopicTopologyCorrupt)?;
    Ok(())
}

fn healthy_public_pages_and_maximum_composite_addresses_survive_reopen<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    let mut children = Vec::new();
    for index in 0..12 {
        children.push(fixture.subscribe(index + 1, &format!("s{index:02}"))?);
    }
    fixture.reset();
    assert_eq!(fixture.list(10)?, children[..10]);
    assert_membership_scan(&fixture.trace(), &fixture.namespace, &fixture.topic, 12);
    fixture.reset();
    assert!(fixture.list(0)?.is_empty());
    assert_membership_scan(&fixture.trace(), &fixture.namespace, &fixture.topic, 12);
    assert_eq!(fixture.list(usize::MAX)?, children);
    let topic = EntityPath::new("t".repeat(domain::MAX_ENTITY_PATH_BYTES))?;
    let name = SubscriptionName::new("s".repeat(domain::MAX_SUBSCRIPTION_NAME_CHARACTERS))?;
    let parent = TopicConfig {
        default_time_to_live_millis: Some(5_000),
        max_message_bytes: 1_024,
    };
    let receive = SubscriptionConfig {
        lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(2_000),
    };
    assert_eq!(
        fixture.at_entity(&topic, 13, CommandKind::CreateTopic { config: parent })?,
        CommandOutcome::TopicCreated
    );
    let child = topic.subscription(&name)?;
    assert_eq!(
        fixture.at_entity(
            &topic,
            14,
            CommandKind::CreateSubscription {
                name,
                config: receive
            }
        )?,
        CommandOutcome::SubscriptionCreated {
            entity: child.clone()
        }
    );
    let shadow = child.dead_letter_queue()?;
    let expected = QueueConfig {
        lock_duration_millis: receive.lock_duration_millis,
        max_delivery_count: receive.max_delivery_count,
        default_time_to_live_millis: receive.default_time_to_live_millis,
        max_message_bytes: parent.max_message_bytes,
        ..QueueConfig::default()
    };
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &topic)?,
        Some(parent)
    );
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &child)?,
        Some(expected)
    );
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &shadow)?,
        Some(QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            ..expected
        })
    );
    assert!(child.as_str().len() > domain::MAX_ENTITY_PATH_BYTES);
    assert!(shadow.as_str().len() > child.as_str().len());
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &topic, 10)?,
        vec![child.clone()]
    );
    let queues = fixture.machine.queues(100)?;
    assert!(queues.contains(&(fixture.namespace.clone(), child.clone())));
    assert!(queues.contains(&(fixture.namespace.clone(), shadow)));
    assert_eq!(
        fixture.at_entity(&topic, 15, send(input("maximum", None)))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: vec![child.clone()]
        }
    );
    let CommandOutcome::Received(Some(delivery)) = fixture.at_entity(
        &child,
        16,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected the maximum-address copy");
    };
    assert_eq!(delivery.body, b"payload");
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(2_015)));
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> {
                super::$case(::testkit::MemoryProvider::new())
            })+
        }
        mod durable {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> {
                super::$case(::testkit::DurableProvider::temporary()?)
            })+
        }
    };
}

for_each_backend! {
    membership_keys_and_values_require_exact_canonical_bytes,
    parent_backing_and_shadow_profiles_are_proved_before_routing,
    late_bad_member_blocks_pages_publish_batches_and_activation_atomically,
    creation_refusals_preserve_config_clock_and_duplicate_priorities,
    all_scheduled_publish_defers_topology_until_atomic_repairable_activation,
    topology_proof_stays_inside_listed_parent_and_namespace_scopes,
    full_membership_lookahead_preserves_limit_and_admission_priority,
    healthy_public_pages_and_maximum_composite_addresses_survive_reopen,
}
