//! Profile-only mutations, complete listed-topology proof, and real backend reopen.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, DurableProposal, EntityPath,
    IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MAX_LOCK_DURATION_MILLIS,
    MAX_TOPIC_SUBSCRIPTIONS, MessageInput, NamespaceName, QueueConfig, QueueTimeToLiveUpdate,
    ReceiveMode, SequenceNumber, StateMachine, SubscriptionConfig, SubscriptionConfigError,
    SubscriptionConfigUpdate, SubscriptionName, Timestamp, TopicConfig, TopicConfigError,
    TopicConfigUpdate, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;

#[derive(Clone, Debug, Default)]
struct Trace {
    scans: Vec<(Key, usize, usize)>,
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
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.trace.lock().unwrap().batches.push(batch.clone());
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.trace.lock().unwrap().snapshots += 1;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.trace
            .lock()
            .unwrap()
            .scans
            .push((prefix.to_vec(), limit, rows.len()));
        Ok(rows)
    }
}

struct Fixture<P: StoreProvider> {
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
}

fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn topic() -> EntityPath {
    EntityPath::new("events").unwrap()
}
fn name(value: &str) -> SubscriptionName {
    SubscriptionName::new(value).unwrap()
}
fn child(value: &str) -> EntityPath {
    topic().subscription(&name(value)).unwrap()
}
fn command(at: u64, kind: CommandKind) -> Command {
    Command::new(ns(), topic(), Timestamp::from_millis(at), kind)
}
fn topic_update(size: Option<usize>, ttl: Option<QueueTimeToLiveUpdate>) -> CommandKind {
    CommandKind::UpdateTopic {
        update: TopicConfigUpdate {
            max_message_bytes: size,
            default_time_to_live_millis: ttl,
        },
    }
}
fn sub_update(value: &str, update: SubscriptionConfigUpdate) -> CommandKind {
    CommandKind::UpdateSubscription {
        name: name(value),
        update,
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
fn put<T: serde::Serialize>(key: Key, value: &T) -> Mutation {
    Mutation::Put {
        key,
        value: codec::encode(value).unwrap(),
    }
}
fn put_topic(key: Key, value: &TopicConfig) -> Mutation {
    Mutation::Put {
        key,
        value: codec::topic::encode(value).unwrap(),
    }
}
fn remaining(snapshot: &StoreSnapshot, changed: &[Key]) -> Vec<(Key, Value)> {
    snapshot
        .entries()
        .iter()
        .filter(|(key, _)| !changed.contains(key))
        .cloned()
        .collect()
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let fixture = Self {
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        };
        fixture.at(
            0,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    default_time_to_live_millis: Some(1_000),
                    ..TopicConfig::default()
                },
            },
        )?;
        Ok(fixture)
    }
    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }
    fn reset(&self) {
        *self.machine.store().trace.lock().unwrap() = Trace::default();
    }
    fn trace(&self) -> Trace {
        self.machine.store().trace.lock().unwrap().clone()
    }
    fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&command(at, kind))
    }
    fn on(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            ns(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }
    fn subscribe(&self, value: &str, ttl: u64) -> TestResult {
        self.at(
            0,
            CommandKind::CreateSubscription {
                name: name(value),
                config: SubscriptionConfig {
                    lock_duration_millis: 100,
                    default_time_to_live_millis: Some(ttl),
                    ..SubscriptionConfig::default()
                },
            },
        )?;
        Ok(())
    }
    fn config(&self, entity: &EntityPath) -> QueueConfig {
        self.machine.queue_config(&ns(), entity).unwrap().unwrap()
    }
    fn proof(&self, count: usize) {
        assert_eq!(
            self.trace().scans,
            vec![(
                keys::topic_subscription_prefix(&ns(), &topic()),
                MAX_TOPIC_SUBSCRIPTIONS + 1,
                count
            )]
        );
        assert_eq!(self.trace().snapshots, 0);
    }
    fn reject(&self, kind: CommandKind, expected: BrokerError, count: usize) -> TestResult {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(self.at(100, kind), Err(expected));
        assert!(self.trace().batches.is_empty());
        self.proof(count);
        assert_eq!(self.raw().snapshot()?, before);
        Ok(())
    }
    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self { machine, provider } = self;
        drop(machine); // No cloned durable handle survives this point.
        Ok(Self {
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                trace: Arc::new(Mutex::new(Trace::default())),
            }),
            provider,
        })
    }
}

fn retention_and_exact_mutations<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider)?;
    fixture.subscribe("alpha", 700)?;
    fixture.subscribe("beta", 900)?;
    let alpha = child("alpha");
    let beta = child("beta");
    fixture.at(
        1,
        CommandKind::SendBatch {
            messages: (0..5)
                .map(|i| MessageInput {
                    message_id: format!("m{i}"),
                    body: b"retained payload".to_vec(),
                    ..MessageInput::default()
                })
                .chain(std::iter::once(MessageInput {
                    message_id: "scheduled".into(),
                    body: vec![7; 16],
                    scheduled_enqueue_at: Some(Timestamp::from_millis(500)),
                    ..MessageInput::default()
                }))
                .collect(),
        },
    )?;
    let receive = || CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: None,
    };
    let CommandOutcome::Received(Some(locked)) = fixture.on(&alpha, 2, receive())? else {
        panic!("locked")
    };
    let CommandOutcome::Received(Some(deferred)) = fixture.on(&alpha, 3, receive())? else {
        panic!("deferred")
    };
    fixture.on(
        &alpha,
        4,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.unwrap().token,
            replacement_envelope: None,
        },
    )?;
    let CommandOutcome::Received(Some(doomed)) = fixture.on(&alpha, 5, receive())? else {
        panic!("doomed")
    };
    fixture.on(
        &alpha,
        6,
        CommandKind::DeadLetter {
            sequence: doomed.sequence,
            lock_token: doomed.lock.unwrap().token,
            reason: "test".into(),
            description: "retained".into(),
            replacement_envelope: None,
        },
    )?;
    let bindings = [
        topic(),
        alpha.clone(),
        alpha.dead_letter_queue()?,
        beta.clone(),
        beta.dead_letter_queue()?,
    ]
    .iter()
    .map(|entity| fixture.machine.bind_entity(&ns(), entity))
    .collect::<Result<Vec<_>, _>>()?;
    let original_alpha = fixture.config(&alpha);
    let original_beta = fixture.config(&beta);
    let before = fixture.raw().snapshot()?;
    let locked_record = fixture.machine.message(&ns(), &alpha, locked.sequence)?;
    fixture.reset();
    let update = topic_update(Some(8), Some(QueueTimeToLiveUpdate::Finite { millis: 500 }));
    assert_eq!(
        fixture
            .machine
            .apply_bound(&BoundCommand::new(bindings[0].clone(), command(10, update)))?,
        CommandOutcome::TopicUpdated
    );
    fixture.proof(2);
    let topic_config = TopicConfig {
        max_message_bytes: 8,
        default_time_to_live_millis: Some(500),
        ..TopicConfig::default()
    };
    let alpha_config = QueueConfig {
        max_message_bytes: 8,
        ..original_alpha
    };
    let beta_config = QueueConfig {
        max_message_bytes: 8,
        ..original_beta
    };
    let expected = [
        put_topic(keys::topic_config(&ns(), &topic()), &topic_config),
        put(keys::queue_config(&ns(), &alpha), &alpha_config),
        put(
            keys::queue_config(&ns(), &alpha.dead_letter_queue()?),
            &shadow(alpha_config),
        ),
        put(keys::queue_config(&ns(), &beta), &beta_config),
        put(
            keys::queue_config(&ns(), &beta.dead_letter_queue()?),
            &shadow(beta_config),
        ),
        put(keys::clock(), &Timestamp::from_millis(10)),
    ];
    assert_eq!(fixture.trace().batches.len(), 1);
    assert_eq!(fixture.trace().batches[0].mutations(), expected.as_slice());
    let changed = expected
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } => key.clone(),
            _ => panic!("put"),
        })
        .collect::<Vec<_>>();
    let after_topic = fixture.raw().snapshot()?;
    assert_eq!(
        remaining(&before, &changed),
        remaining(&after_topic, &changed)
    );
    assert_eq!(
        fixture.config(&alpha).default_time_to_live_millis,
        Some(700)
    );
    assert_eq!(fixture.config(&beta).default_time_to_live_millis, Some(900));

    fixture.reset();
    let patch = SubscriptionConfigUpdate {
        lock_duration_millis: Some(40),
        max_delivery_count: Some(3),
        default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 200 }),
    };
    assert_eq!(
        fixture.machine.apply_bound(&BoundCommand::new(
            bindings[0].clone(),
            command(11, sub_update("alpha", patch))
        ))?,
        CommandOutcome::SubscriptionUpdated
    );
    fixture.proof(2);
    let updated = QueueConfig {
        lock_duration_millis: 40,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(200),
        ..alpha_config
    };
    assert_eq!(
        fixture.trace().batches[0].mutations(),
        &[
            put(keys::queue_config(&ns(), &alpha), &updated),
            put(
                keys::queue_config(&ns(), &alpha.dead_letter_queue()?),
                &shadow(updated)
            ),
            put(keys::clock(), &Timestamp::from_millis(11)),
        ]
    );
    let changed = [
        keys::queue_config(&ns(), &alpha),
        keys::queue_config(&ns(), &alpha.dead_letter_queue()?),
        keys::clock(),
    ];
    let retained = fixture.raw().snapshot()?;
    assert_eq!(
        remaining(&after_topic, &changed),
        remaining(&retained, &changed)
    );
    assert_eq!(
        fixture.machine.message(&ns(), &alpha, locked.sequence)?,
        locked_record
    );
    for binding in &bindings {
        fixture.machine.validate_binding(binding)?;
    }
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, retained);
    for binding in &bindings {
        fixture.machine.validate_binding(binding)?;
    }
    for kind in [
        topic_update(None, None),
        topic_update(Some(8), Some(QueueTimeToLiveUpdate::Finite { millis: 500 })),
        sub_update("alpha", SubscriptionConfigUpdate::default()),
        sub_update("alpha", patch),
    ] {
        fixture.reset();
        fixture.at(100, kind)?;
        fixture.proof(2);
        assert!(fixture.trace().batches.is_empty());
        assert_eq!(fixture.raw().snapshot()?, retained);
        assert_eq!(
            fixture.machine.last_applied_time()?,
            Timestamp::from_millis(11)
        );
    }
    // Parent-only and child-only fields do not rewrite an unchanged DLQ.
    fixture.reset();
    fixture.at(
        12,
        topic_update(None, Some(QueueTimeToLiveUpdate::Unlimited)),
    )?;
    assert_eq!(
        fixture.trace().batches[0].mutations(),
        &[
            put_topic(
                keys::topic_config(&ns(), &topic()),
                &TopicConfig {
                    default_time_to_live_millis: None,
                    ..topic_config
                }
            ),
            put(keys::clock(), &Timestamp::from_millis(12)),
        ]
    );
    fixture.reset();
    fixture.at(
        13,
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                max_delivery_count: Some(4),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..SubscriptionConfigUpdate::default()
            },
        ),
    )?;
    assert_eq!(
        fixture.trace().batches[0].mutations(),
        &[
            put(
                keys::queue_config(&ns(), &alpha),
                &QueueConfig {
                    max_delivery_count: 4,
                    default_time_to_live_millis: None,
                    ..updated
                }
            ),
            put(keys::clock(), &Timestamp::from_millis(13)),
        ]
    );
    // Existing large records remain, while future admissions use the new size.
    assert!(matches!(
        fixture.at(
            14,
            CommandKind::SendBatch {
                messages: vec![MessageInput {
                    body: vec![0; 9],
                    ..MessageInput::default()
                }]
            }
        ),
        Err(BrokerError::MessageTooLarge {
            maximum_bytes: 8,
            ..
        })
    ));
    fixture.at(
        14,
        topic_update(None, Some(QueueTimeToLiveUpdate::Finite { millis: 300 })),
    )?;
    fixture.at(
        15,
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                max_delivery_count: Some(1),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 75 }),
                ..SubscriptionConfigUpdate::default()
            },
        ),
    )?;
    fixture.at(
        16,
        CommandKind::SendBatch {
            messages: vec![MessageInput {
                body: b"new".to_vec(),
                ..MessageInput::default()
            }],
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .message(&ns(), &alpha, SequenceNumber::new(7))?
            .unwrap()
            .expires_at,
        Some(Timestamp::from_millis(91))
    );
    assert_eq!(
        fixture
            .machine
            .message(&ns(), &beta, SequenceNumber::new(7))?
            .unwrap()
            .expires_at,
        Some(Timestamp::from_millis(316))
    );
    let CommandOutcome::Received(Some(future_lock)) = fixture.on(&alpha, 17, receive())? else {
        panic!("ready retained record")
    };
    assert_eq!(future_lock.lock.unwrap().lock_duration_millis, 40);
    assert_eq!(
        future_lock.lock.unwrap().locked_until,
        Timestamp::from_millis(57)
    );
    assert_eq!(future_lock.expires_at, Some(Timestamp::from_millis(701)));
    assert_eq!(
        fixture.on(
            &alpha,
            18,
            CommandKind::Abandon {
                sequence: future_lock.sequence,
                lock_token: future_lock.lock.unwrap().token,
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    Ok(())
}

fn input_refusals<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider)?;
    fixture.subscribe("alpha", 700)?;
    for (kind, error) in [
        (
            topic_update(Some(0), None),
            BrokerError::TopicConfig(TopicConfigError::MaxMessageBytesTooSmall),
        ),
        (
            topic_update(None, Some(QueueTimeToLiveUpdate::Finite { millis: 0 })),
            BrokerError::TopicConfig(TopicConfigError::TimeToLiveTooShort),
        ),
        (
            sub_update(
                "alpha",
                SubscriptionConfigUpdate {
                    lock_duration_millis: Some(0),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            BrokerError::SubscriptionConfig(SubscriptionConfigError::LockDurationTooShort),
        ),
        (
            sub_update(
                "alpha",
                SubscriptionConfigUpdate {
                    lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS + 1),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            BrokerError::SubscriptionConfig(SubscriptionConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            }),
        ),
        (
            sub_update(
                "alpha",
                SubscriptionConfigUpdate {
                    max_delivery_count: Some(0),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            BrokerError::SubscriptionConfig(SubscriptionConfigError::MaxDeliveryCountTooSmall),
        ),
        (
            sub_update(
                "alpha",
                SubscriptionConfigUpdate {
                    default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            BrokerError::SubscriptionConfig(SubscriptionConfigError::TimeToLiveTooShort),
        ),
        (
            sub_update(
                "missing",
                SubscriptionConfigUpdate {
                    lock_duration_millis: Some(0),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            BrokerError::SubscriptionNotFound,
        ),
    ] {
        fixture.reject(kind, error, 1)?;
    }
    fixture.at(
        1,
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS),
                max_delivery_count: Some(u32::MAX),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite {
                    millis: u64::MAX,
                }),
            },
        ),
    )?;
    fixture.at(
        2,
        topic_update(
            Some(usize::MAX),
            Some(QueueTimeToLiveUpdate::Finite { millis: u64::MAX }),
        ),
    )?;
    Ok(())
}

// Populate valid listed profiles in one setup batch, avoiding quadratic creation.
fn seed_children<S: StateStore>(store: &S, count: usize) -> TestResult {
    let source = child("alpha");
    let config = store.get(&keys::queue_config(&ns(), &source))?.unwrap();
    let shadow_config = store
        .get(&keys::queue_config(&ns(), &source.dead_letter_queue()?))?
        .unwrap();
    let owner = store.get(&keys::entity_metadata(&ns(), &source))?.unwrap();
    let mut batch = WriteBatch::default();
    for i in 1..count {
        let name = name(&format!("s{i:04}"));
        let entity = topic().subscription(&name)?;
        batch.push_put(
            keys::topic_subscription(&ns(), &topic(), &name),
            codec::encode(&entity)?,
        );
        batch.push_put(keys::queue_config(&ns(), &entity), config.clone());
        batch.push_put(
            keys::queue_config(&ns(), &entity.dead_letter_queue()?),
            shadow_config.clone(),
        );
        batch.push_put(keys::entity_metadata(&ns(), &entity), owner.clone());
    }
    store.apply(batch)?;
    Ok(())
}

fn proof_boundaries<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(provider)?;
    fixture.subscribe("alpha", 700)?;
    seed_children(fixture.raw(), MAX_TOPIC_SUBSCRIPTIONS)?;
    for kind in [
        topic_update(None, None),
        sub_update("alpha", SubscriptionConfigUpdate::default()),
    ] {
        fixture.reset();
        fixture.at(1, kind)?;
        fixture.proof(MAX_TOPIC_SUBSCRIPTIONS);
        assert!(fixture.trace().batches.is_empty());
    }
    fixture.reset();
    fixture.at(1, topic_update(Some(128), None))?;
    fixture.proof(MAX_TOPIC_SUBSCRIPTIONS);
    assert_eq!(
        fixture.trace().batches[0].mutations().len(),
        2 * MAX_TOPIC_SUBSCRIPTIONS + 2
    );
    let last = child("s1999");
    assert_eq!(fixture.config(&last).max_message_bytes, 128);
    assert_eq!(
        fixture.config(&last.dead_letter_queue()?).max_message_bytes,
        128
    );
    fixture.reset();
    fixture.at(
        2,
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(50),
                ..SubscriptionConfigUpdate::default()
            },
        ),
    )?;
    fixture.proof(MAX_TOPIC_SUBSCRIPTIONS);
    assert_eq!(fixture.trace().batches[0].mutations().len(), 3);
    let key = keys::queue_config(&ns(), &last.dead_letter_queue()?);
    let healthy = fixture.raw().get(&key)?.unwrap();
    fixture.raw().apply(WriteBatch::default().put(
        key.clone(),
        codec::encode(&QueueConfig {
            max_message_bytes: 129,
            ..fixture.config(&last.dead_letter_queue()?)
        })?,
    ))?;
    for kind in [
        topic_update(None, None),
        topic_update(Some(0), None),
        topic_update(Some(64), None),
        sub_update("alpha", SubscriptionConfigUpdate::default()),
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
        ),
        sub_update("missing", SubscriptionConfigUpdate::default()),
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(70),
                ..SubscriptionConfigUpdate::default()
            },
        ),
    ] {
        fixture.reject(
            kind,
            BrokerError::TopicTopologyCorrupt,
            MAX_TOPIC_SUBSCRIPTIONS,
        )?;
    }
    let corrupted = fixture.raw().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, corrupted);
    fixture.reject(
        topic_update(None, None),
        BrokerError::TopicTopologyCorrupt,
        MAX_TOPIC_SUBSCRIPTIONS,
    )?;
    fixture
        .raw()
        .apply(WriteBatch::default().put(key, healthy))?;
    seed_children(fixture.raw(), MAX_TOPIC_SUBSCRIPTIONS + 1)?;
    for kind in [
        topic_update(None, None),
        topic_update(Some(0), None),
        sub_update("alpha", SubscriptionConfigUpdate::default()),
    ] {
        fixture.reject(
            kind,
            BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            },
            MAX_TOPIC_SUBSCRIPTIONS + 1,
        )?;
    }
    Ok(())
}

fn indexed_priority<P: StoreProvider>(provider: P) -> TestResult {
    let store = provider.open()?;
    let mut writer = IndexedWriter::open(store.clone())?;
    let proposals = [
        command(
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        command(
            0,
            CommandKind::CreateSubscription {
                name: name("alpha"),
                config: SubscriptionConfig::default(),
            },
        ),
        command(
            0,
            CommandKind::CreateSubscription {
                name: name("zlast"),
                config: SubscriptionConfig::default(),
            },
        ),
    ];
    for (index, command) in proposals.into_iter().enumerate() {
        writer.apply(index as u64 + 1, &DurableProposal::unbound(command))?;
    }
    let key = keys::queue_config(&ns(), &child("zlast").dead_letter_queue()?);
    let healthy = store.get(&key)?.unwrap();
    store.apply(WriteBatch::default().delete(key.clone()))?;
    let corrupted = store.snapshot()?;
    for kind in [
        topic_update(None, None),
        topic_update(Some(0), None),
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
        ),
        sub_update("missing", SubscriptionConfigUpdate::default()),
    ] {
        assert_eq!(
            writer.apply(4, &DurableProposal::unbound(command(10, kind))),
            Err(IndexedApplyError::Domain(BrokerError::TopicTopologyCorrupt))
        );
        assert_eq!(writer.applied_index()?, 3);
        assert_eq!(store.snapshot()?, corrupted);
    }
    store.apply(WriteBatch::default().put(key, healthy))?;
    // Invalid stored limits must never leak through the input-error classifier.
    for (key, bad_value) in [
        (
            keys::topic_config(&ns(), &topic()),
            codec::topic::encode(&TopicConfig {
                max_message_bytes: 0,
                ..TopicConfig::default()
            })?,
        ),
        (
            keys::queue_config(&ns(), &child("zlast")),
            codec::encode(&QueueConfig {
                lock_duration_millis: 0,
                ..QueueConfig::default()
            })?,
        ),
    ] {
        let healthy = store.get(&key)?.unwrap();
        store.apply(WriteBatch::default().put(key.clone(), bad_value))?;
        let corrupted = store.snapshot()?;
        for kind in [
            topic_update(Some(0), None),
            sub_update(
                "alpha",
                SubscriptionConfigUpdate {
                    max_delivery_count: Some(0),
                    ..SubscriptionConfigUpdate::default()
                },
            ),
            topic_update(None, None),
            sub_update("alpha", SubscriptionConfigUpdate::default()),
        ] {
            assert_eq!(
                writer.apply(4, &DurableProposal::unbound(command(10, kind))),
                Err(IndexedApplyError::Domain(BrokerError::TopicTopologyCorrupt))
            );
            assert_eq!(writer.applied_index()?, 3);
            assert_eq!(store.snapshot()?, corrupted);
        }
        store.apply(WriteBatch::default().put(key, healthy))?;
    }
    let before = store.snapshot()?;
    let kinds = [
        topic_update(Some(0), None),
        sub_update(
            "alpha",
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
        ),
        sub_update("missing", SubscriptionConfigUpdate::default()),
    ];
    for (offset, kind) in kinds.into_iter().enumerate() {
        assert!(matches!(
            writer.apply(
                offset as u64 + 4,
                &DurableProposal::unbound(command(10, kind))
            )?,
            IndexedApplyOutcome::Refused(_)
        ));
    }
    assert_eq!(
        writer.apply(
            7,
            &DurableProposal::unbound(command(100, topic_update(None, None)))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::TopicUpdated)
    );
    let after = store.snapshot()?;
    let domain = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| key[0] != 0xF1)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(domain(&before), domain(&after));
    // alpha + the seeded names + the existing zlast exceed the cap by one.
    // A reader cap error is fatal here, never a healthy-creation refusal.
    seed_children(&store, MAX_TOPIC_SUBSCRIPTIONS)?;
    let after = store.snapshot()?;
    for kind in [
        topic_update(None, None),
        sub_update("alpha", SubscriptionConfigUpdate::default()),
    ] {
        assert_eq!(
            writer.apply(8, &DurableProposal::unbound(command(101, kind))),
            Err(IndexedApplyError::Domain(
                BrokerError::SubscriptionLimitExceeded {
                    maximum: MAX_TOPIC_SUBSCRIPTIONS,
                }
            ))
        );
        assert_eq!(writer.applied_index()?, 7);
        assert_eq!(store.snapshot()?, after);
    }
    drop(writer);
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(reopened.snapshot()?, after);
    let writer = IndexedWriter::open(reopened)?;
    assert_eq!(writer.applied_index()?, 7);
    Ok(())
}

#[test]
fn memory_retains_exact_state_and_mutations() -> TestResult {
    retention_and_exact_mutations(MemoryProvider::new())
}
#[test]
fn fjall_retains_exact_state_and_mutations() -> TestResult {
    retention_and_exact_mutations(DurableProvider::temporary()?)
}
#[test]
fn memory_input_refusals() -> TestResult {
    input_refusals(MemoryProvider::new())
}
#[test]
fn fjall_input_refusals() -> TestResult {
    input_refusals(DurableProvider::temporary()?)
}
#[test]
fn memory_complete_proof_boundaries() -> TestResult {
    proof_boundaries(MemoryProvider::new())
}
#[test]
fn fjall_complete_proof_boundaries() -> TestResult {
    proof_boundaries(DurableProvider::temporary()?)
}
#[test]
fn memory_indexed_corruption_priority() -> TestResult {
    indexed_priority(MemoryProvider::new())
}
#[test]
fn fjall_indexed_corruption_priority() -> TestResult {
    indexed_priority(DurableProvider::temporary()?)
}
