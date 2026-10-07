use super::*;

use std::{collections::BTreeSet, sync::Mutex, thread::ThreadId};

use domain::{
    Command, EntityIncarnation, RuleFilter, RuleName, SequenceNumber, SubscriptionConfig,
    SubscriptionName, TopicConfig, codec,
};
use server::AtomSubscriptionOwnerError;
use storage::{Key, Mutation, StorageError, StoreSnapshot, Value};

#[derive(Clone, Default)]
struct Observation {
    armed: bool,
    committed: bool,
    forbid_clock: bool,
    commits: usize,
    mutations: Vec<Mutation>,
    threads: Vec<ThreadId>,
    fail_next: bool,
}

#[derive(Clone)]
struct Store<S> {
    inner: S,
    observation: Arc<Mutex<Observation>>,
}

impl<S: StateStore> Store<S> {
    fn arm(&self, forbid_clock: bool) {
        *self.observation.lock().unwrap() = Observation {
            armed: true,
            forbid_clock,
            ..Observation::default()
        };
    }
    fn disarm(&self) -> Observation {
        let mut state = self.observation.lock().unwrap();
        let observed = state.clone();
        state.armed = false;
        observed
    }
    fn read(&self, key: Option<&[u8]>) {
        let mut state = self.observation.lock().unwrap();
        if state.armed {
            assert!(
                !state.committed,
                "read after committed subscription mutation"
            );
            assert!(
                !state.forbid_clock || key != Some(keys::clock().as_slice()),
                "read consulted stored Clock"
            );
            state.threads.push(std::thread::current().id());
        }
    }
}

impl<S: StateStore> StateStore for Store<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.read(Some(key));
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.read(None);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        assert!(
            !self.observation.lock().unwrap().armed,
            "owner used snapshot fallback"
        );
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let fail = {
            let mut state = self.observation.lock().unwrap();
            if state.armed {
                assert!(!state.committed, "multiple subscription commits");
                state.commits += 1;
                state.mutations = batch.mutations().to_vec();
                state.threads.push(std::thread::current().id());
            }
            std::mem::take(&mut state.fail_next)
        };
        if fail {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "subscription preapply failure".into(),
            });
        }
        self.inner.apply(batch)?;
        self.observation.lock().unwrap().committed = true;
        Ok(())
    }
}

struct Fixture<S: StateStore> {
    broker: Broker,
    store: Store<S>,
    clock: ProbeClock,
    namespace: NamespaceName,
    topic: EntityPath,
    name: SubscriptionName,
}

impl<S: StateStore> Fixture<S> {
    fn new(inner: S) -> TestResult<Self> {
        let store = Store {
            inner,
            observation: Arc::default(),
        };
        let clock = ProbeClock {
            manual: ManualClock::at(1_000),
            reads: Arc::default(),
            forbidden: Arc::default(),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let fixture = Self {
            broker,
            store,
            clock,
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new("orders")?,
            name: SubscriptionName::new("worker")?,
        };
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        Ok(fixture)
    }
    fn handle(&self) -> server::BrokerHandle {
        self.broker.handle()
    }
    fn child(&self) -> TestResult<EntityPath> {
        Ok(self.topic.subscription(&self.name)?)
    }
    fn submit(&self, entity: &EntityPath, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self
            .handle()
            .submit_blocking(self.namespace.clone(), entity.clone(), kind)?)
    }
    fn create(
        &self,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
        self.handle().create_atom_subscription_blocking(
            self.namespace.clone(),
            self.topic.clone(),
            self.name.clone(),
            config,
        )
    }
    fn get(&self) -> Result<Option<SubscriptionConfig>, AtomSubscriptionOwnerError> {
        self.handle().get_atom_subscription_blocking(
            self.namespace.clone(),
            self.topic.clone(),
            self.name.clone(),
        )
    }
    fn delete(&self) -> Result<CommandOutcome, AtomSubscriptionOwnerError> {
        self.handle().delete_atom_subscription_blocking(
            self.namespace.clone(),
            self.topic.clone(),
            self.name.clone(),
        )
    }
}

fn wrapped(error: BrokerError) -> AtomSubscriptionOwnerError {
    AtomSubscriptionOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(error)))
}

fn assert_owner(state: &Observation, commits: usize) {
    assert_eq!(state.commits, commits);
    let first = state.threads.first().expect("owner operations observed");
    assert_ne!(*first, std::thread::current().id());
    assert!(state.threads.iter().all(|thread| thread == first));
}

async fn creation_returns_prepared_exact_topology_without_postcommit_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    let config = SubscriptionConfig {
        lock_duration_millis: 5_000,
        max_delivery_count: 4,
        default_time_to_live_millis: Some(60_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    };
    let before = fixture.store.snapshot()?;
    fixture.store.arm(false);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        fixture.create(config),
        Err(AtomSubscriptionOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    assert_owner(&fixture.store.disarm(), 1);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.store.arm(false);
    assert_eq!(fixture.create(config)?, config);
    assert_owner(&fixture.store.disarm(), 1);
    let child = fixture.child()?;
    let shadow = child.dead_letter_queue()?;
    let machine = StateMachine::new(fixture.store.inner.clone());
    let rules = machine.rules(&fixture.namespace, &fixture.topic, &fixture.name)?;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].name.as_str(), "$Default");
    assert_eq!(rules[0].filter, RuleFilter::True);
    assert_eq!(rules[0].action, None);
    assert_eq!(rules[0].created_at, Timestamp::from_millis(1_000));
    assert_eq!(
        machine.subscription_config(&fixture.namespace, &fixture.topic, &fixture.name)?,
        Some(config)
    );
    for entity in [&fixture.topic, &child, &shadow] {
        assert_eq!(
            fixture
                .store
                .inner
                .get(&keys::queue_capacity_mode(&fixture.namespace, entity))?,
            None
        );
        assert_eq!(
            fixture
                .store
                .inner
                .get(&keys::queue_capacity_usage(&fixture.namespace, entity))?,
            None
        );
    }
    let oracle_store = storage::MemoryStore::default();
    let oracle = StateMachine::new(oracle_store.clone());
    oracle.apply(&Command::new(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        Timestamp::from_millis(1_000),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    ))?;
    oracle.apply(&Command::new(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        Timestamp::from_millis(1_000),
        CommandKind::CreateSubscription {
            name: fixture.name.clone(),
            config,
        },
    ))?;
    assert_eq!(
        fixture.store.snapshot()?,
        oracle_store.snapshot()?,
        "same-domain canonical replay, not an independent oracle"
    );
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(fixture.get()?, Some(config));
    assert_owner(&fixture.store.disarm(), 0);
    drop(machine);
    let snapshot = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, snapshot);
    Ok(())
}

async fn create_refusals_are_preflighted_and_conflicts_keep_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for config in [
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            max_message_bytes: 4_096,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            lock_duration_millis: 4_999,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            lock_duration_millis: 300_001,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            max_delivery_count: 0,
            ..SubscriptionConfig::default()
        },
    ] {
        fixture.store.arm(true);
        assert_eq!(
            fixture.create(config),
            Err(AtomSubscriptionOwnerError::UnsupportedDefinition)
        );
        assert_eq!(fixture.store.disarm().commits, 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(
        fixture.handle().create_atom_subscription_blocking(
            fixture.namespace.clone(),
            EntityPath::new("missing")?,
            fixture.name.clone(),
            SubscriptionConfig::default()
        ),
        Err(wrapped(BrokerError::TopicNotFound))
    );
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        fixture.create(SubscriptionConfig::default()),
        Err(wrapped(BrokerError::SubscriptionAlreadyExists))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

async fn get_proves_child_topology_and_parent_identity_before_projection<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let child = fixture.child()?;
    let parent_key = keys::entity_incarnation(&fixture.namespace, &fixture.topic);
    let parent = fixture.store.inner.get(&parent_key)?.unwrap();
    let child_inc = fixture
        .store
        .inner
        .get(&keys::entity_incarnation(&fixture.namespace, &child))?
        .unwrap();
    let parent_identity = codec::decode::<EntityIncarnation>(&parent)?;
    let retired = codec::encode(&EntityIncarnation::new(
        parent_identity.generation(),
        parent_identity.kind(),
        true,
    )?)?;
    let faults = [
        (
            parent_key.clone(),
            None,
            BrokerError::DanglingEntityMetadata,
        ),
        (
            parent_key,
            Some(retired),
            BrokerError::DanglingEntityMetadata,
        ),
        (
            keys::entity_incarnation(&fixture.namespace, &fixture.topic),
            Some(child_inc),
            BrokerError::DanglingEntityMetadata,
        ),
        (
            keys::entity_incarnation(&fixture.namespace, &child),
            None,
            BrokerError::DanglingEntityMetadata,
        ),
        (
            keys::subscription(&fixture.namespace, &fixture.topic, &fixture.name),
            None,
            BrokerError::DanglingSubscriptionMetadata,
        ),
        (
            keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
            None,
            BrokerError::DanglingSubscriptionMetadata,
        ),
        (
            keys::queue_capacity_mode(&fixture.namespace, &fixture.topic),
            Some(vec![255]),
            BrokerError::QueueCapacityCorrupt,
        ),
        (
            keys::queue_capacity_usage(&fixture.namespace, &child),
            Some(vec![255]),
            BrokerError::QueueCapacityCorrupt,
        ),
    ];
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for (key, value, expected) in faults {
        let original = fixture.store.inner.get(&key)?;
        fixture.store.inner.apply(match value {
            Some(value) => WriteBatch::default().put(key.clone(), value),
            None => WriteBatch::default().delete(key.clone()),
        })?;
        let before = fixture.store.snapshot()?;
        fixture.store.arm(true);
        assert_eq!(fixture.get(), Err(wrapped(expected)), "fault key: {key:?}");
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
        fixture.store.inner.apply(match original {
            Some(value) => WriteBatch::default().put(key, value),
            None => WriteBatch::default().delete(key),
        })?;
    }
    Ok(())
}

async fn native_custom_rules_are_readable_but_hidden_limits_and_sessions_are_refused<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("extra")?,
            filter: RuleFilter::False,
        },
    )?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(fixture.get()?, Some(SubscriptionConfig::default()));
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    for (name, config) in [
        (
            "session",
            SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        ),
        (
            "small",
            SubscriptionConfig {
                max_message_bytes: 4_096,
                ..SubscriptionConfig::default()
            },
        ),
        (
            "lock",
            SubscriptionConfig {
                lock_duration_millis: 4_999,
                ..SubscriptionConfig::default()
            },
        ),
    ] {
        let name = SubscriptionName::new(name)?;
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config,
            },
        )?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        let before = fixture.store.snapshot()?;
        for delete in [false, true] {
            fixture.store.arm(true);
            let result = if delete {
                fixture
                    .handle()
                    .delete_atom_subscription_blocking(
                        fixture.namespace.clone(),
                        fixture.topic.clone(),
                        name.clone(),
                    )
                    .map(|_| None)
            } else {
                fixture.handle().get_atom_subscription_blocking(
                    fixture.namespace.clone(),
                    fixture.topic.clone(),
                    name.clone(),
                )
            };
            assert_eq!(
                result,
                Err(AtomSubscriptionOwnerError::UnsupportedDefinition)
            );
            assert_owner(&fixture.store.disarm(), 0);
            assert_eq!(fixture.store.snapshot()?, before);
        }
        let mode =
            keys::queue_capacity_mode(&fixture.namespace, &fixture.topic.subscription(&name)?);
        fixture
            .store
            .inner
            .apply(WriteBatch::default().put(mode.clone(), vec![255]))?;
        let corrupt = fixture.store.snapshot()?;
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().get_atom_subscription_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                name.clone()
            ),
            Err(wrapped(BrokerError::QueueCapacityCorrupt))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(
            fixture.store.snapshot()?,
            corrupt,
            "stored corruption wins over wire-profile refusal"
        );
        fixture
            .store
            .inner
            .apply(WriteBatch::default().delete(mode))?;
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
    }
    Ok(())
}

async fn deletion_purges_retained_copies_and_only_committed_wakeups<P: StoreProvider>(
    provider: P,
) -> TestResult {
    use protocol_amqp::Broker as _;
    use std::{future::poll_fn, task::Poll};
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let sibling = SubscriptionName::new("sibling")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: sibling.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("extra")?,
            filter: RuleFilter::True,
        },
    )?;
    fixture.submit(
        &fixture.topic,
        CommandKind::Send {
            message_id: "first".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    let child = fixture.child()?;
    let shadow = child.dead_letter_queue()?;
    let CommandOutcome::Received(Some(delivery)) = fixture.submit(
        &child,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("delivery");
    };
    fixture.submit(
        &child,
        CommandKind::DeadLetter {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("peek lock").token,
            reason: "test".into(),
            description: "retained".into(),
        },
    )?;
    fixture.submit(
        &fixture.topic,
        CommandKind::Send {
            message_id: "second".into(),
            body: vec![2],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    let before = fixture.store.snapshot()?;
    let handle = fixture.handle();
    let mut child_wait = Box::pin(handle.deliverable(&fixture.namespace, &child));
    let mut shadow_wait = Box::pin(handle.deliverable(&fixture.namespace, &shadow));
    let sibling_entity = fixture.topic.subscription(&sibling)?;
    let mut sibling_wait = Box::pin(handle.deliverable(&fixture.namespace, &sibling_entity));
    fixture.store.arm(false);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        fixture.delete(),
        Err(AtomSubscriptionOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    assert_owner(&fixture.store.disarm(), 1);
    assert_eq!(fixture.store.snapshot()?, before);
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.store.arm(false);
    assert_eq!(fixture.delete()?, CommandOutcome::SubscriptionDeleted);
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let oracle_store = storage::MemoryStore::default();
    oracle_store.apply(
        before
            .entries()
            .iter()
            .fold(WriteBatch::default(), |batch, (key, value)| {
                batch.put(key.clone(), value.clone())
            }),
    )?;
    StateMachine::new(oracle_store.clone()).apply(&Command::new(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        Timestamp::from_millis(1_000),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Subscription {
                name: fixture.name.clone(),
            },
        },
    ))?;
    assert_eq!(
        fixture.store.snapshot()?,
        oracle_store.snapshot()?,
        "same-domain deletion replay, not an independent oracle"
    );
    tokio::time::timeout(DEADLINE, &mut child_wait).await?;
    tokio::time::timeout(DEADLINE, &mut shadow_wait).await?;
    poll_fn(|cx| {
        assert!(sibling_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let deleted = observed
        .mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Delete { key } => Some(key),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for key in [
        keys::queue_config(&fixture.namespace, &child),
        keys::queue_config(&fixture.namespace, &shadow),
        keys::subscription(&fixture.namespace, &fixture.topic, &fixture.name),
        keys::rule(
            &fixture.namespace,
            &fixture.topic,
            &fixture.name,
            &RuleName::new("$Default")?,
        ),
    ] {
        assert!(deleted.contains(&key));
    }
    drop(child_wait);
    drop(shadow_wait);
    drop(sibling_wait);
    drop(handle);
    let snapshot = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, snapshot);
    Ok(())
}

async fn absent_deletion_reuses_original_orphan_diagnostics_and_stamp<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    for orphan in [false, true] {
        if orphan {
            fixture.store.inner.apply(WriteBatch::default().put(
                keys::message(
                    &fixture.namespace,
                    &fixture.child()?,
                    SequenceNumber::new(1),
                ),
                vec![255],
            ))?;
        }
        let before = fixture.store.snapshot()?;
        let reads = fixture.clock.reads.load(Ordering::SeqCst);
        let original = StateMachine::new(fixture.store.inner.clone())
            .apply(&Command::new(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                Timestamp::from_millis(1_000),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Subscription {
                        name: fixture.name.clone(),
                    },
                },
            ))
            .expect_err("absent deletion refuses");
        fixture.store.arm(false);
        assert_eq!(fixture.delete(), Err(wrapped(original)));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    Ok(())
}

async fn subscription_count_limit_is_a_refusal_without_a_partial_batch<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        fixture.handle().create_atom_subscription_blocking(
            fixture.namespace.clone(),
            fixture.topic.clone(),
            SubscriptionName::new(format!("worker{index}"))?,
            SubscriptionConfig::default(),
        )?;
    }
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(
        fixture.create(SubscriptionConfig::default()),
        Err(wrapped(BrokerError::SubscriptionLimitExceeded {
            maximum: domain::MAX_TOPIC_SUBSCRIPTIONS
        }))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

async fn child_recreation_rejects_old_fences_before_clock_and_by_name_deletes_current<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let target = AdminTarget::Subscription {
        topic: fixture.topic.clone(),
        name: fixture.name.clone(),
    };
    let old = bounded(
        "old subscription identity",
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target.clone()),
    )
    .await?
    .unwrap()
    .binding;
    assert_eq!(fixture.delete()?, CommandOutcome::SubscriptionDeleted);
    fixture.create(SubscriptionConfig::default())?;
    let current = bounded(
        "current subscription identity",
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target),
    )
    .await?
    .unwrap()
    .binding;
    assert_eq!(current.generation(), old.generation() + 1);
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        fixture.handle().submit_fenced_blocking(
            old,
            fixture.topic.clone(),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Subscription {
                    name: fixture.name.clone()
                }
            }
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::EntityBindingStale
        )))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(fixture.delete()?, CommandOutcome::SubscriptionDeleted);
    assert_owner(&fixture.store.disarm(), 1);
    let record = StateMachine::new(fixture.store.inner.clone())
        .entity_incarnation(&fixture.namespace, &fixture.child()?)?
        .unwrap();
    assert!(record.is_retired());
    assert_eq!(record.generation(), current.generation());
    Ok(())
}

macro_rules! for_each_subscription_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::default()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_subscription_backend! {
    creation_returns_prepared_exact_topology_without_postcommit_reads,
    create_refusals_are_preflighted_and_conflicts_keep_state,
    get_proves_child_topology_and_parent_identity_before_projection,
    native_custom_rules_are_readable_but_hidden_limits_and_sessions_are_refused,
    deletion_purges_retained_copies_and_only_committed_wakeups,
    absent_deletion_reuses_original_orphan_diagnostics_and_stamp,
    subscription_count_limit_is_a_refusal_without_a_partial_batch,
    child_recreation_rejects_old_fences_before_clock_and_by_name_deletes_current,
}
