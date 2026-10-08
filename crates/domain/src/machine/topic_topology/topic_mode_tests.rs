use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

use crate::topic_mode::NonFiniteTopicMode;
use crate::{
    BrokerError, Command, CommandKind, CommandOutcome, DeleteEntityTarget, Delivery,
    EntityIncarnation, EntityIncarnationKind, EntityPath, FencedCommand, MAX_ENTITY_PATH_BYTES,
    MessageState, NamespaceName, QueueConfig, QueueCounters, ReceiveMode, RuleFilter, RuleName,
    ScheduledMessage, SequenceNumber, SessionId, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, TopicConfigUpdate, codec, keys,
};

#[derive(Clone, Debug)]
struct ProbeStore<S> {
    inner: S,
    batches: Arc<Mutex<Vec<WriteBatch>>>,
    fail_apply: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for ProbeStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_apply.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "apply topic-mode test batch",
                detail: "injected before backend apply".into(),
            });
        }
        self.inner.apply(batch.clone())?;
        self.batches.lock().unwrap().push(batch);
        Ok(())
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
}

struct Fixture<P: StoreProvider> {
    provider: P,
    machine: StateMachine<ProbeStore<P::Store>>,
    namespace: NamespaceName,
    topic: EntityPath,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Self {
        Self {
            machine: StateMachine::new(ProbeStore {
                inner: provider.open().unwrap(),
                batches: Arc::default(),
                fail_apply: Arc::default(),
            }),
            provider,
            namespace: NamespaceName::new("tenant").unwrap(),
            topic: EntityPath::new("events").unwrap(),
        }
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
    fn create_topic(&self) {
        assert_eq!(
            self.at(
                &self.topic,
                100,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            ),
            Ok(CommandOutcome::TopicCreated)
        );
    }
    fn subscribe(&self, name: &str, config: SubscriptionConfig) -> EntityPath {
        let name = SubscriptionName::new(name).unwrap();
        let millis = self.machine.last_applied_time().unwrap().as_millis();
        assert_eq!(
            self.at(
                &self.topic,
                millis,
                CommandKind::CreateSubscription {
                    name: name.clone(),
                    config
                }
            ),
            Ok(CommandOutcome::SubscriptionCreated)
        );
        self.topic.subscription(&name).unwrap()
    }
    fn rows(&self) -> StoreSnapshot {
        self.machine.store().inner.snapshot().unwrap()
    }
    fn raw(&self, batch: WriteBatch) {
        self.machine.store().inner.apply(batch).unwrap();
    }
    fn mode(&self) -> Vec<u8> {
        self.machine
            .store()
            .get(&keys::topic_mode(&self.namespace, &self.topic))
            .unwrap()
            .unwrap()
    }
    fn refuse(&self, entity: &EntityPath, kind: CommandKind, expected: BrokerError) {
        let before = self.rows();
        let batches = self.machine.store().batches.lock().unwrap().len();
        assert_eq!(self.at(entity, 1_000, kind), Err(expected));
        assert_eq!(self.rows(), before);
        assert_eq!(self.machine.store().batches.lock().unwrap().len(), batches);
    }
    fn restart(self) -> Self {
        let Self {
            provider,
            machine,
            namespace,
            topic,
        } = self;
        let batches = machine.store().batches.clone();
        let fail_apply = machine.store().fail_apply.clone();
        drop(machine);
        Self {
            machine: StateMachine::new(ProbeStore {
                inner: provider.open().unwrap(),
                batches,
                fail_apply,
            }),
            provider,
            namespace,
            topic,
        }
    }
}

fn send(id: &str, session_id: Option<SessionId>) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: vec![1, 2, 3],
        time_to_live_millis: None,
        session_id,
    }
}

fn receive(session: Option<crate::SessionHold>) -> CommandKind {
    CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session,
    }
}

fn delivered(outcome: CommandOutcome) -> Delivery {
    let CommandOutcome::Received(Some(delivery)) = outcome else {
        panic!("expected delivery")
    };
    delivery
}

fn no_accounting<P: StoreProvider>(fixture: &Fixture<P>, entities: &[EntityPath]) {
    for entity in entities {
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::queue_capacity_mode(&fixture.namespace, entity))
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::queue_capacity_usage(&fixture.namespace, entity))
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::message_charge_prefix(&fixture.namespace, entity), 1)
                .unwrap()
                .is_empty()
        );
    }
}

fn atomic_generation_and_full_length<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    let before = fixture.rows();
    fixture
        .machine
        .store()
        .fail_apply
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture.at(
            &fixture.topic,
            100,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        ),
        Err(BrokerError::Storage(_))
    ));
    assert_eq!(fixture.rows(), before);
    fixture.create_topic();
    let mode = fixture.mode();
    assert_eq!(
        NonFiniteTopicMode::decode(&mode, 1),
        NonFiniteTopicMode::non_finite(1)
    );
    let expected = WriteBatch::default()
        .put(
            keys::entity_incarnation(&fixture.namespace, &fixture.topic),
            codec::encode(&EntityIncarnation::new(1, EntityIncarnationKind::Topic, false).unwrap())
                .unwrap(),
        )
        .put(
            keys::topic_config(&fixture.namespace, &fixture.topic),
            codec::encode(&TopicConfig::default()).unwrap(),
        )
        .put(
            keys::topic_mode(&fixture.namespace, &fixture.topic),
            mode.clone(),
        )
        .put(
            keys::clock(),
            codec::encode(&Timestamp::from_millis(100)).unwrap(),
        );
    assert_eq!(
        fixture.machine.store().batches.lock().unwrap().as_slice(),
        &[expected]
    );
    let shadow = fixture.topic.dead_letter_queue().unwrap();
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::queue_config(&fixture.namespace, &shadow))
            .unwrap()
            .is_none()
    );
    no_accounting(&fixture, &[fixture.topic.clone(), shadow]);
    assert_eq!(
        fixture.at(
            &fixture.topic,
            101,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    max_message_bytes: Some(4_096),
                    ..TopicConfigUpdate::default()
                }
            }
        ),
        Ok(CommandOutcome::TopicUpdated)
    );
    assert_eq!(fixture.mode(), mode);
    let before_delete = fixture.rows();
    fixture
        .machine
        .store()
        .fail_apply
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture.at(
            &fixture.topic,
            102,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        ),
        Err(BrokerError::Storage(_))
    ));
    assert_eq!(fixture.rows(), before_delete);
    assert_eq!(
        fixture.at(
            &fixture.topic,
            102,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        ),
        Ok(CommandOutcome::TopicDeleted)
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::topic_mode(&fixture.namespace, &fixture.topic))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture.at(
            &fixture.topic,
            103,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        ),
        Ok(CommandOutcome::TopicCreated)
    );
    assert_eq!(
        NonFiniteTopicMode::decode(&fixture.mode(), 2),
        NonFiniteTopicMode::non_finite(2)
    );
    let long = EntityPath::new("x".repeat(MAX_ENTITY_PATH_BYTES)).unwrap();
    assert!(long.dead_letter_queue().is_err());
    assert_eq!(
        fixture.at(
            &long,
            104,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        ),
        Ok(CommandOutcome::TopicCreated)
    );
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &long),
        Ok(Some(TopicConfig::default()))
    );
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        NonFiniteTopicMode::decode(&fixture.mode(), 2),
        NonFiniteTopicMode::non_finite(2)
    );
    assert!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &long,
                &long,
                EntityIncarnationKind::Topic
            )
            .unwrap()
            .is_some()
    );
}

fn corrupt_modes_fence_live_operations<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    fixture.create_topic();
    let healthy = fixture.mode();
    let mut schema = healthy.clone();
    schema[5] = 2;
    for value in [
        None,
        Some(vec![255]),
        Some(schema),
        Some(NonFiniteTopicMode::non_finite(2).unwrap().encode().unwrap()),
        Some(
            crate::queue_capacity::QueueCapacityMode::non_finite(1)
                .unwrap()
                .encode()
                .unwrap(),
        ),
        Some(vec![0; 65]),
    ] {
        let key = keys::topic_mode(&fixture.namespace, &fixture.topic);
        fixture.raw(match value {
            Some(value) => WriteBatch::default().put(key, value),
            None => WriteBatch::default().delete(key),
        });
        let before = fixture.rows();
        assert_eq!(
            fixture
                .machine
                .topic_config(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture.machine.bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture.machine.validate_capacity_binding_profile(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        fixture.refuse(
            &fixture.topic,
            send("bad", None),
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate::default(),
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("new").unwrap(),
                config: SubscriptionConfig::default(),
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic,
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
            BrokerError::TopicAlreadyExists,
        );
        assert_eq!(
            fixture
                .machine
                .topic_config_topology(&fixture.namespace, &fixture.topic),
            Ok(Some(TopicConfig::default()))
        );
        assert_eq!(fixture.rows(), before);
    }
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        healthy,
    ));
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            )
            .unwrap()
            .is_some()
    );
}

fn complete_child_requires_parent_but_absence_does_not<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    fixture.create_topic();
    let child = fixture.subscribe("active", SubscriptionConfig::default());
    let shadow = child.dead_letter_queue().unwrap();
    let healthy = fixture.mode();
    fixture.raw(WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)));
    let name = SubscriptionName::new("active").unwrap();
    assert_eq!(
        fixture
            .machine
            .subscription_config(&fixture.namespace, &fixture.topic, &name),
        Err(BrokerError::TopicCapacityCorrupt)
    );
    for target in [&child, &shadow] {
        assert_eq!(
            fixture.machine.bind_entity(
                &fixture.namespace,
                target,
                &child,
                EntityIncarnationKind::Subscription
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        fixture.refuse(target, receive(None), BrokerError::TopicCapacityCorrupt);
    }
    fixture.refuse(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: name,
            name: RuleName::new("extra").unwrap(),
            filter: RuleFilter::True,
        },
        BrokerError::TopicCapacityCorrupt,
    );
    let missing = SubscriptionName::new("missing").unwrap();
    assert_eq!(
        fixture
            .machine
            .subscription_config(&fixture.namespace, &fixture.topic, &missing),
        Ok(None)
    );
    let missing_child = fixture.topic.subscription(&missing).unwrap();
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &missing_child,
            &missing_child,
            EntityIncarnationKind::Subscription
        ),
        Ok(None)
    );
    let absent_parent = EntityPath::new("absent").unwrap();
    assert_eq!(
        fixture
            .machine
            .subscription_config(&fixture.namespace, &absent_parent, &missing),
        Ok(None)
    );
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        healthy.clone(),
    ));
    for target in [
        child.clone(),
        shadow,
        fixture.topic.dead_letter_queue().unwrap(),
        missing_child.clone(),
        missing_child.dead_letter_queue().unwrap(),
    ] {
        let key = keys::topic_mode(&fixture.namespace, &target);
        fixture.raw(WriteBatch::default().put(key.clone(), healthy.clone()));
        let before = fixture.rows();
        assert_eq!(
            fixture
                .machine
                .topic_config(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture.machine.bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture
                .machine
                .subscriptions(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        fixture.refuse(
            &fixture.topic,
            send("ghost", None),
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic,
            },
            BrokerError::TopicCapacityCorrupt,
        );
        assert_eq!(
            fixture.machine.subscription_config(
                &fixture.namespace,
                &fixture.topic,
                &SubscriptionName::new("active").unwrap()
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        let missing_result =
            fixture
                .machine
                .subscription_config(&fixture.namespace, &fixture.topic, &missing);
        if target == missing_child || target == missing_child.dead_letter_queue().unwrap() {
            assert_eq!(missing_result, Err(BrokerError::TopicCapacityCorrupt));
        } else {
            assert_eq!(missing_result, Ok(None));
        }
        assert_eq!(fixture.rows(), before);
        fixture.raw(WriteBatch::default().delete(key));
    }
    assert_eq!(
        fixture
            .machine
            .subscription_config(&fixture.namespace, &fixture.topic, &missing),
        Ok(None)
    );
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &child,
                &child,
                EntityIncarnationKind::Subscription
            )
            .unwrap()
            .is_some()
    );
}

fn orphan_and_queue_ghost_modes_never_become_runtime<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    let ghost = fixture
        .topic
        .subscription(&SubscriptionName::new("ghost").unwrap())
        .unwrap();
    let mode = NonFiniteTopicMode::non_finite(1).unwrap().encode().unwrap();
    for target in [
        fixture.topic.clone(),
        fixture.topic.dead_letter_queue().unwrap(),
        ghost.clone(),
        ghost.dead_letter_queue().unwrap(),
    ] {
        let key = keys::topic_mode(&fixture.namespace, &target);
        fixture.raw(WriteBatch::default().put(key.clone(), mode.clone()));
        let before = fixture.rows();
        assert_eq!(
            fixture.machine.bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture
                .machine
                .topic_config(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture
                .machine
                .describe_queue_capacity(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic,
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::ExpireSessionLocks,
            BrokerError::TopicCapacityCorrupt,
        );
        assert_eq!(fixture.rows(), before);
        fixture.raw(WriteBatch::default().delete(key));
    }
    let before = fixture.rows();
    assert_eq!(
        fixture.at(&fixture.topic, 100, CommandKind::ExpireSessionLocks),
        Ok(CommandOutcome::SessionLocksExpired { released: 0 })
    );
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        fixture.at(
            &fixture.topic,
            100,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Ok(CommandOutcome::QueueCreated)
    );
    for target in [
        fixture.topic.clone(),
        fixture.topic.dead_letter_queue().unwrap(),
        ghost.clone(),
        ghost.dead_letter_queue().unwrap(),
    ] {
        let key = keys::topic_mode(&fixture.namespace, &target);
        fixture.raw(WriteBatch::default().put(key.clone(), mode.clone()));
        assert_eq!(
            fixture.machine.bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Queue
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture
                .machine
                .describe_queue_capacity(&fixture.namespace, &fixture.topic),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        assert_eq!(
            fixture.machine.validate_capacity_binding_profile(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Queue
            ),
            Err(BrokerError::TopicCapacityCorrupt)
        );
        fixture.refuse(
            &fixture.topic,
            send("queue-ghost", None),
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.refuse(
            &fixture.topic,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
            BrokerError::TopicCapacityCorrupt,
        );
        fixture.raw(WriteBatch::default().delete(key));
    }
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Queue
            )
            .unwrap()
            .is_some()
    );
}

fn retired_generations_and_identity_priority<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    fixture.create_topic();
    let old = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &fixture.topic,
            &fixture.topic,
            EntityIncarnationKind::Topic,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture.at(
            &fixture.topic,
            101,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        ),
        Ok(CommandOutcome::TopicDeleted)
    );
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        NonFiniteTopicMode::non_finite(1).unwrap().encode().unwrap(),
    ));
    fixture.refuse(
        &fixture.topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        BrokerError::TopicCapacityCorrupt,
    );
    fixture.refuse(
        &fixture.topic,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
        BrokerError::TopicCapacityCorrupt,
    );
    fixture.refuse(
        &fixture.topic,
        CommandKind::ExpireSessionLocks,
        BrokerError::TopicCapacityCorrupt,
    );
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &fixture.topic,
            &fixture.topic,
            EntityIncarnationKind::Topic
        ),
        Err(BrokerError::TopicCapacityCorrupt)
    );
    fixture.raw(WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)));
    let before = fixture.rows();
    assert_eq!(
        fixture.at(&fixture.topic, 102, CommandKind::ExpireSessionLocks),
        Ok(CommandOutcome::SessionLocksExpired { released: 0 })
    );
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        fixture.at(
            &fixture.topic,
            102,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        ),
        Ok(CommandOutcome::TopicCreated)
    );
    fixture.raw(WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)));
    let before = fixture.rows();
    let stale = FencedCommand::new(old, fixture.command(&fixture.topic, 0, send("stale", None)));
    assert_eq!(
        fixture.machine.apply_fenced(&stale),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(
        fixture
            .machine
            .describe_queue_capacity(&fixture.namespace, &fixture.topic),
        Err(BrokerError::EntityKindMismatch)
    );
    fixture.refuse(
        &fixture.topic,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        BrokerError::EntityKindMismatch,
    );
    assert_eq!(fixture.rows(), before);
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        NonFiniteTopicMode::non_finite(2).unwrap().encode().unwrap(),
    ));
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &fixture.topic,
                &fixture.topic,
                EntityIncarnationKind::Topic
            )
            .unwrap()
            .unwrap()
            .generation(),
        2
    );
}

fn healthy_topic_delete_purges_opaque_runtime_atomically<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    fixture.create_topic();
    let child = fixture.subscribe("active", SubscriptionConfig::default());
    let unrelated = EntityPath::new("unrelated").unwrap();
    assert_eq!(
        fixture.at(
            &unrelated,
            100,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Ok(CommandOutcome::QueueCreated)
    );
    let unrelated_keys = [
        keys::queue_config(&fixture.namespace, &unrelated),
        keys::queue_capacity_mode(&fixture.namespace, &unrelated),
        keys::entity_incarnation(&fixture.namespace, &unrelated),
    ];
    let preserved: Vec<_> = unrelated_keys
        .iter()
        .map(|key| (key.clone(), fixture.machine.store().get(key).unwrap()))
        .collect();
    for owner in [&fixture.topic, &child, &child.dead_letter_queue().unwrap()] {
        let mut batch = WriteBatch::default().put(
            keys::queue_counters(&fixture.namespace, owner),
            codec::encode(&QueueCounters::default()).unwrap(),
        );
        for (prefix, _) in keys::entity_runtime_prefixes(&fixture.namespace, owner) {
            let mut key = prefix;
            key.extend_from_slice(b"opaque tail");
            batch.push_put(key, vec![255]);
        }
        fixture.raw(batch);
    }
    let before = fixture.rows();
    fixture
        .machine
        .store()
        .fail_apply
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture.at(
            &fixture.topic,
            101,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        ),
        Err(BrokerError::Storage(_))
    ));
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        fixture.at(
            &fixture.topic,
            101,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        ),
        Ok(CommandOutcome::TopicDeleted)
    );
    for owner in [&fixture.topic, &child, &child.dead_letter_queue().unwrap()] {
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::topic_mode(&fixture.namespace, owner))
                .unwrap()
                .is_none()
        );
        for (prefix, _) in keys::entity_runtime_prefixes(&fixture.namespace, owner) {
            assert!(
                fixture
                    .machine
                    .store()
                    .scan_prefix(&prefix, 1)
                    .unwrap()
                    .is_empty()
            );
        }
    }
    for (key, value) in preserved {
        assert_eq!(fixture.machine.store().get(&key).unwrap(), value);
    }
    assert!(fixture.machine.store().batches.lock().unwrap().last().unwrap().mutations().iter().any(|mutation| matches!(mutation, Mutation::Delete { key } if key == &keys::topic_mode(&fixture.namespace, &fixture.topic))));
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &fixture.topic,
            &fixture.topic,
            EntityIncarnationKind::Topic
        ),
        Ok(None)
    );
}

fn non_finite_publish_schedule_dlq_and_session_paths<P: StoreProvider>(provider: P) {
    let fixture = Fixture::new(provider);
    fixture.create_topic();
    let child = fixture.subscribe("active", SubscriptionConfig::default());
    let mode = fixture.mode();
    assert_eq!(
        fixture.at(&fixture.topic, 100, send("immediate", None)),
        Ok(CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        })
    );
    let delivery = delivered(fixture.at(&child, 101, receive(None)).unwrap());
    fixture.raw(WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)));
    fixture.refuse(
        &child,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.as_ref().unwrap().token,
        },
        BrokerError::TopicCapacityCorrupt,
    );
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        mode.clone(),
    ));
    assert_eq!(
        fixture.at(
            &child,
            102,
            CommandKind::DeadLetter {
                sequence: delivery.sequence,
                lock_token: delivery.lock.as_ref().unwrap().token,
                reason: "manual".into(),
                description: "mode remains non-finite".into()
            }
        ),
        Ok(CommandOutcome::DeadLettered)
    );
    let shadow = child.dead_letter_queue().unwrap();
    let dlq = delivered(fixture.at(&shadow, 103, receive(None)).unwrap());
    assert!(dlq.dead_letter.is_some());
    assert_eq!(
        fixture.at(
            &shadow,
            104,
            CommandKind::Complete {
                sequence: dlq.sequence,
                lock_token: dlq.lock.unwrap().token
            }
        ),
        Ok(CommandOutcome::Completed)
    );
    let scheduled = |id: &str, due: u64| ScheduledMessage {
        message_id: id.into(),
        body: vec![4],
        time_to_live_millis: None,
        session_id: None,
        enqueue_at: Timestamp::from_millis(due),
    };
    assert_eq!(
        fixture.at(
            &fixture.topic,
            105,
            CommandKind::Schedule {
                messages: vec![scheduled("cancel", 200), scheduled("activate", 200)]
            }
        ),
        Ok(CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(2), SequenceNumber::new(3)]
        })
    );
    fixture.raw(WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)));
    fixture.refuse(
        &fixture.topic,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(2)],
        },
        BrokerError::TopicCapacityCorrupt,
    );
    fixture.refuse(
        &fixture.topic,
        CommandKind::ActivateScheduled,
        BrokerError::TopicCapacityCorrupt,
    );
    fixture.raw(WriteBatch::default().put(
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        mode.clone(),
    ));
    assert_eq!(
        fixture.at(
            &fixture.topic,
            106,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(2)]
            }
        ),
        Ok(CommandOutcome::ScheduledCancelled { cancelled: 1 })
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.topic, SequenceNumber::new(2))
            .unwrap()
            .is_none()
    );
    let pending = fixture
        .machine
        .message(&fixture.namespace, &fixture.topic, SequenceNumber::new(3))
        .unwrap()
        .expect("uncancelled scheduled source");
    assert!(
        matches!(pending.state, MessageState::Scheduled { enqueue_at, .. } if enqueue_at == Timestamp::from_millis(200))
    );
    assert_eq!(
        fixture
            .machine
            .read::<QueueCounters>(&keys::queue_counters(&fixture.namespace, &fixture.topic))
            .unwrap()
            .unwrap()
            .next_sequence,
        4
    );
    assert_eq!(
        fixture.at(&fixture.topic, 200, CommandKind::ActivateScheduled),
        Ok(CommandOutcome::ScheduledActivated { activated: 1 })
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.topic, SequenceNumber::new(3))
            .unwrap()
            .is_none()
    );
    let ready = fixture
        .machine
        .message(&fixture.namespace, &child, SequenceNumber::new(4))
        .unwrap()
        .expect("fresh activated copy");
    assert_eq!(ready.sequence, SequenceNumber::new(4));
    assert_eq!(ready.message_id, "activate");
    assert_eq!(ready.body, vec![4]);
    assert_eq!(ready.state, MessageState::Ready);
    assert_eq!(
        ready.scheduled_enqueue_time,
        Some(Timestamp::from_millis(200))
    );
    assert_eq!(
        fixture
            .machine
            .read::<QueueCounters>(&keys::queue_counters(&fixture.namespace, &fixture.topic))
            .unwrap()
            .unwrap()
            .next_sequence,
        5
    );
    let activated = delivered(fixture.at(&child, 201, receive(None)).unwrap());
    assert_eq!(activated.sequence, SequenceNumber::new(4));
    assert_eq!(
        fixture.at(
            &child,
            202,
            CommandKind::Complete {
                sequence: activated.sequence,
                lock_token: activated.lock.unwrap().token
            }
        ),
        Ok(CommandOutcome::Completed)
    );
    assert_eq!(fixture.mode(), mode);
    no_accounting(&fixture, &[fixture.topic.clone(), child.clone(), shadow]);
    let session_child = fixture.subscribe(
        "session",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    );
    let session_id = SessionId::new("group").unwrap();
    assert_eq!(
        fixture.at(
            &fixture.topic,
            203,
            send("session", Some(session_id.clone()))
        ),
        Ok(CommandOutcome::Sent {
            sequence: SequenceNumber::new(5)
        })
    );
    assert_eq!(
        fixture
            .machine
            .read::<QueueCounters>(&keys::queue_counters(&fixture.namespace, &fixture.topic))
            .unwrap()
            .unwrap()
            .next_sequence,
        6
    );
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture
        .at(
            &session_child,
            204,
            CommandKind::AcceptSession {
                session_id: Some(session_id),
                lock_duration_millis: None,
            },
        )
        .unwrap()
    else {
        panic!("expected session")
    };
    let session_delivery = delivered(
        fixture
            .at(&session_child, 205, receive(Some(accepted.hold())))
            .unwrap(),
    );
    assert_eq!(session_delivery.sequence, SequenceNumber::new(5));
    assert_eq!(
        fixture.at(
            &session_child,
            206,
            CommandKind::Complete {
                sequence: session_delivery.sequence,
                lock_token: session_delivery.lock.unwrap().token
            }
        ),
        Ok(CommandOutcome::Completed)
    );
    assert_eq!(fixture.mode(), mode);
    no_accounting(
        &fixture,
        &[
            fixture.topic.clone(),
            child,
            session_child.clone(),
            session_child.dead_letter_queue().unwrap(),
        ],
    );
    let before = fixture.rows();
    let fixture = fixture.restart();
    assert_eq!(fixture.rows(), before);
    assert_eq!(fixture.mode(), mode);
    assert!(
        fixture
            .machine
            .bind_entity(
                &fixture.namespace,
                &session_child,
                &session_child,
                EntityIncarnationKind::Subscription
            )
            .unwrap()
            .is_some()
    );
}

macro_rules! paired {
    ($memory:ident, $durable:ident, $case:ident) => {
        #[test]
        fn $memory() {
            $case(MemoryProvider::new());
        }
        #[test]
        fn $durable() {
            $case(DurableProvider::temporary().unwrap());
        }
    };
}

paired!(
    topic_mode_atomic_generation_and_full_length_memory,
    topic_mode_atomic_generation_and_full_length_durable,
    atomic_generation_and_full_length
);
paired!(
    topic_mode_corrupt_live_operations_memory,
    topic_mode_corrupt_live_operations_durable,
    corrupt_modes_fence_live_operations
);
paired!(
    topic_mode_complete_child_and_absence_memory,
    topic_mode_complete_child_and_absence_durable,
    complete_child_requires_parent_but_absence_does_not
);
paired!(
    topic_mode_orphan_and_queue_ghost_memory,
    topic_mode_orphan_and_queue_ghost_durable,
    orphan_and_queue_ghost_modes_never_become_runtime
);
paired!(
    topic_mode_retired_identity_priority_memory,
    topic_mode_retired_identity_priority_durable,
    retired_generations_and_identity_priority
);
paired!(
    topic_mode_opaque_topic_delete_memory,
    topic_mode_opaque_topic_delete_durable,
    healthy_topic_delete_purges_opaque_runtime_atomically
);
paired!(
    topic_mode_non_finite_lifecycle_memory,
    topic_mode_non_finite_lifecycle_durable,
    non_finite_publish_schedule_dlq_and_session_paths
);
