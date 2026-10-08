use super::*;

use std::{collections::BTreeSet, sync::Mutex, thread::ThreadId};

use domain::{MessageRecord, QueueCapacityView, codec};
use storage::{Key, Mutation, StorageError, StoreSnapshot, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
struct DefinitionScan {
    prefix: Key,
    start: Key,
    limit: usize,
    returned: usize,
}

#[derive(Clone, Debug, Default)]
struct DefinitionObservation {
    commits: usize,
    scans: Vec<DefinitionScan>,
    snapshots: usize,
    mutations: Vec<Mutation>,
    threads: Vec<ThreadId>,
    guard_after_commit: bool,
    committed: bool,
}

#[derive(Clone)]
struct DefinitionStore<S> {
    inner: S,
    observation: Arc<Mutex<DefinitionObservation>>,
}

impl<S: StateStore> DefinitionStore<S> {
    fn arm(&self) {
        *self.observation.lock().unwrap() = DefinitionObservation {
            guard_after_commit: true,
            ..DefinitionObservation::default()
        };
    }

    fn disarm(&self) -> DefinitionObservation {
        let mut observation = self.observation.lock().unwrap();
        let result = observation.clone();
        observation.guard_after_commit = false;
        result
    }

    fn observe_read(&self) {
        let mut observation = self.observation.lock().unwrap();
        assert!(
            !observation.guard_after_commit || !observation.committed,
            "definition returned its prepared view by rereading after commit"
        );
        observation.threads.push(std::thread::current().id());
    }
}

impl<S: StateStore> StateStore for DefinitionStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observe_read();
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observe_read();
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.observation.lock().unwrap().scans.push(DefinitionScan {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
            returned: rows.len(),
        });
        Ok(rows)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observe_read();
        self.observation.lock().unwrap().snapshots += 1;
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        {
            let mut observation = self.observation.lock().unwrap();
            observation.commits += 1;
            observation.threads.push(std::thread::current().id());
            observation.mutations = batch.mutations().to_vec();
        }
        self.inner.apply(batch)?;
        self.observation.lock().unwrap().committed = true;
        Ok(())
    }
}

struct DefinitionFixture<S: StateStore> {
    _broker: Broker,
    handle: server::BrokerHandle,
    store: DefinitionStore<S>,
    clock: ProbeClock,
    namespace: NamespaceName,
    entity: EntityPath,
    created: QueueCapacityView,
}

impl<S: StateStore> DefinitionFixture<S> {
    fn new(store: S) -> TestResult<Self> {
        let store = DefinitionStore {
            inner: store,
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
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let created = handle.create_finite_queue_blocking(
            namespace.clone(),
            entity.clone(),
            QueueConfig {
                default_time_to_live_millis: Some(5_000),
                ..QueueConfig::default()
            },
            FiniteQueueCapacity::new(4_096)?,
        )?;
        Ok(Self {
            _broker: broker,
            handle,
            store,
            clock,
            namespace,
            entity,
            created,
        })
    }

    fn send(&self, id: &str, body: &[u8]) -> Result<CommandOutcome, SubmitError> {
        self.handle.submit_blocking(
            self.namespace.clone(),
            self.entity.clone(),
            CommandKind::Send {
                message_id: id.into(),
                body: body.to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )
    }

    fn record(&self, outcome: CommandOutcome) -> TestResult<MessageRecord> {
        let CommandOutcome::Sent { sequence } = outcome else {
            panic!("enqueued message")
        };
        Ok(codec::decode(
            &self
                .store
                .get(&keys::message(&self.namespace, &self.entity, sequence))?
                .unwrap(),
        )?)
    }
}

fn finite_usage(view: &QueueCapacityView) -> (u64, u64) {
    let QueueCapacityStatus::FiniteV1 {
        reserved_bytes,
        message_count,
        ..
    } = view.capacity
    else {
        panic!("finite owner view")
    };
    (reserved_bytes, message_count)
}

fn expected_capacity_error(error: BrokerError) -> SubmitError {
    SubmitError::Propose(ProposeError::Broker(error))
}

fn assert_owner_observation<S: StateStore>(
    fixture: &DefinitionFixture<S>,
    observation: &DefinitionObservation,
    commits: usize,
    probes: usize,
) {
    assert_eq!(observation.commits, commits);
    assert_eq!(observation.snapshots, 0);
    let prefix = keys::subscription_topic_mode_prefix(&fixture.namespace, &fixture.entity);
    assert_eq!(
        observation.scans,
        vec![
            DefinitionScan {
                prefix: prefix.clone(),
                start: prefix,
                limit: 1,
                returned: 0,
            };
            probes
        ]
    );
    let owner = observation.threads.first().expect("owner reads");
    assert_ne!(*owner, std::thread::current().id());
    assert!(observation.threads.iter().all(|thread| thread == owner));
}

fn without_definition_rows(
    snapshot: &StoreSnapshot,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> TestResult<Vec<(Key, Value)>> {
    let changed = BTreeSet::from([
        keys::clock(),
        keys::queue_config(namespace, entity),
        keys::queue_config(namespace, &entity.dead_letter_queue()?),
        keys::queue_capacity_mode(namespace, entity),
    ]);
    Ok(snapshot
        .entries()
        .iter()
        .filter(|(key, _)| !changed.contains(key))
        .cloned()
        .collect())
}

async fn combined_definition_changes_commit_once_and_affect_only_future_sends<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = DefinitionFixture::new(provider.open()?)?;
    let retained = fixture.send("retained", &[1; 8])?;
    let retained = fixture.record(retained)?;
    let current = fixture
        .handle
        .describe_queue_capacity_blocking(fixture.namespace.clone(), fixture.entity.clone())?
        .unwrap();
    let before = fixture.store.snapshot()?;
    let config = QueueConfig {
        lock_duration_millis: 200,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(50),
        max_message_bytes: 2,
        duplicate_detection_history_time_window_millis: 20_000,
        dead_lettering_on_message_expiration: true,
        ..QueueConfig::default()
    };
    let limit = FiniteQueueCapacity::new(2_048)?;
    fixture.clock.manual.set(2_000);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    let updated = bounded(
        "set complete finite definition",
        fixture
            .handle
            .set_finite_queue_definition_fenced(current.binding.clone(), config, limit),
    )
    .await?;
    let observation = fixture.store.disarm();
    assert_owner_observation(&fixture, &observation, 1, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(updated.binding, current.binding);
    assert_eq!(updated.config, config);
    let (reserved_bytes, message_count) = finite_usage(&current);
    assert_eq!(
        updated.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit,
            reserved_bytes,
            message_count,
        }
    );
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .handle
            .describe_queue_capacity_blocking(fixture.namespace.clone(), fixture.entity.clone())?,
        Some(updated.clone())
    );
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    let expected_keys = BTreeSet::from([
        keys::queue_config(&fixture.namespace, &fixture.entity),
        keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?),
        keys::queue_capacity_mode(&fixture.namespace, &fixture.entity),
        keys::clock(),
    ]);
    assert_eq!(observation.mutations.len(), 4);
    let actual_keys: BTreeSet<_> = observation
        .mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } => key.clone(),
            Mutation::Delete { .. } => panic!("definition only replaces metadata"),
        })
        .collect();
    assert_eq!(actual_keys, expected_keys);
    let after = fixture.store.snapshot()?;
    assert_eq!(
        without_definition_rows(&after, &fixture.namespace, &fixture.entity)?,
        without_definition_rows(&before, &fixture.namespace, &fixture.entity)?
    );
    assert_eq!(
        QueueConfig::decode(
            &fixture
                .store
                .get(&keys::queue_config(&fixture.namespace, &fixture.entity))?
                .unwrap()
        )?,
        config
    );
    assert_eq!(
        QueueConfig::decode(
            &fixture
                .store
                .get(&keys::queue_config(
                    &fixture.namespace,
                    &fixture.entity.dead_letter_queue()?
                ))?
                .unwrap()
        )?,
        config.dead_letter_shadow()
    );
    assert_eq!(
        codec::decode::<MessageRecord>(
            &fixture
                .store
                .get(&keys::message(
                    &fixture.namespace,
                    &fixture.entity,
                    retained.sequence
                ))?
                .unwrap()
        )?,
        retained
    );
    let before = fixture.store.snapshot()?;
    assert_eq!(
        fixture.send("oversize", &[1; 3]),
        Err(expected_capacity_error(BrokerError::MessageTooLarge {
            body_bytes: 3,
            maximum_bytes: 2,
        }))
    );
    assert_eq!(fixture.store.snapshot()?, before);
    let future = fixture.send("future", &[1; 2])?;
    assert_eq!(
        fixture.record(future)?.expires_at,
        Some(Timestamp::from_millis(2_050))
    );

    let current = fixture
        .handle
        .describe_queue_capacity_blocking(fixture.namespace.clone(), fixture.entity.clone())?
        .unwrap();
    let unlimited = QueueConfig {
        lock_duration_millis: 300,
        default_time_to_live_millis: None,
        ..config
    };
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_010);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    let updated = fixture.handle.set_finite_queue_definition_fenced_blocking(
        current.binding.clone(),
        unlimited,
        FiniteQueueCapacity::new(8_192)?,
    )?;
    let observation = fixture.store.disarm();
    assert_owner_observation(&fixture, &observation, 1, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(updated.binding, current.binding);
    assert_eq!(updated.config, unlimited);
    assert_eq!(finite_usage(&updated), finite_usage(&current));
    let (reserved_bytes, message_count) = finite_usage(&current);
    assert_eq!(
        updated.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(8_192)?,
            reserved_bytes,
            message_count,
        }
    );
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .handle
            .describe_queue_capacity_blocking(fixture.namespace.clone(), fixture.entity.clone())?,
        Some(updated)
    );
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    assert_eq!(
        without_definition_rows(
            &fixture.store.snapshot()?,
            &fixture.namespace,
            &fixture.entity
        )?,
        without_definition_rows(&before, &fixture.namespace, &fixture.entity)?
    );
    let future = fixture.send("unlimited", &[1; 2])?;
    assert_eq!(fixture.record(future)?.expires_at, None);
    Ok(())
}

async fn refused_definitions_preserve_config_limit_usage_and_runtime<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = DefinitionFixture::new(provider.open()?)?;
    fixture.send("retained", &[1; 2])?;
    let current = fixture
        .handle
        .describe_queue_capacity_blocking(fixture.namespace.clone(), fixture.entity.clone())?
        .unwrap();
    let config = QueueConfig {
        lock_duration_millis: 200,
        max_message_bytes: 1,
        ..current.config
    };
    let limit = FiniteQueueCapacity::new(finite_usage(&current).0 - 1)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_000);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    assert_eq!(
        tokio::time::timeout(
            DEADLINE,
            fixture.handle.set_finite_queue_definition_fenced(
                current.binding.clone(),
                config,
                limit,
            )
        )
        .await?,
        Err(expected_capacity_error(BrokerError::QueueCapacityFull))
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    assert_eq!(
        fixture.handle.set_finite_queue_definition_fenced_blocking(
            current.binding.clone(),
            config,
            limit,
        ),
        Err(expected_capacity_error(BrokerError::QueueCapacityFull))
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);

    fixture.store.apply(WriteBatch::default().put(
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
        vec![255],
    ))?;
    let damaged = fixture.store.snapshot()?;
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm();
    assert_eq!(
        fixture.handle.set_finite_queue_definition_fenced_blocking(
            current.binding,
            config,
            FiniteQueueCapacity::new(8_192)?,
        ),
        Err(expected_capacity_error(BrokerError::QueueCapacityCorrupt))
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 1);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads);
    assert_eq!(fixture.store.snapshot()?, damaged);
    Ok(())
}

async fn same_definition_is_noop_and_recreated_identity_fences_before_host_clock<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = DefinitionFixture::new(provider.open()?)?;
    let initial = fixture.created.clone();
    let limit = FiniteQueueCapacity::new(4_096)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.manual.set(2_000);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    assert_eq!(
        bounded(
            "same complete finite definition",
            fixture.handle.set_finite_queue_definition_fenced(
                initial.binding.clone(),
                initial.config,
                limit,
            )
        )
        .await?,
        initial
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm();
    assert_eq!(
        fixture.handle.set_finite_queue_definition_fenced_blocking(
            initial.binding.clone(),
            initial.config,
            limit,
        )?,
        initial
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 4);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);

    fixture.handle.submit_fenced_blocking(
        initial.binding.clone(),
        fixture.entity.clone(),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    let recreated = fixture.handle.create_finite_queue_blocking(
        fixture.namespace.clone(),
        fixture.entity.clone(),
        QueueConfig::default(),
        limit,
    )?;
    assert_eq!(
        recreated.binding.generation(),
        initial.binding.generation() + 1
    );
    let before = fixture.store.snapshot()?;
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let invalid_config = QueueConfig {
        max_message_bytes: 0,
        requires_session: true,
        ..QueueConfig::default()
    };
    fixture.store.arm();
    assert_eq!(
        tokio::time::timeout(
            DEADLINE,
            fixture.handle.set_finite_queue_definition_fenced(
                initial.binding.clone(),
                invalid_config,
                FiniteQueueCapacity::new(1)?,
            )
        )
        .await?,
        Err(expected_capacity_error(BrokerError::EntityBindingStale))
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 0);
    fixture.store.arm();
    assert_eq!(
        fixture.handle.set_finite_queue_definition_fenced_blocking(
            initial.binding,
            invalid_config,
            FiniteQueueCapacity::new(1)?,
        ),
        Err(expected_capacity_error(BrokerError::EntityBindingStale))
    );
    assert_owner_observation(&fixture, &fixture.store.disarm(), 0, 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

macro_rules! for_each_definition_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_definition_backend! {
    combined_definition_changes_commit_once_and_affect_only_future_sends,
    refused_definitions_preserve_config_limit_usage_and_runtime,
    same_definition_is_noop_and_recreated_identity_fences_before_host_clock,
}
