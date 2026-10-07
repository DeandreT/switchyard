use super::*;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
    thread::ThreadId,
};

use domain::{QueueCapacityView, QueueImmutableProperty, SequenceNumber, TopicConfig, codec};
use protocol_amqp::Broker as _;
use server::AtomQueueOwnerError;
use storage::{Key, Mutation, StorageError, StoreSnapshot, Value};

const MIB: u64 = 1_048_576;

#[derive(Clone, Default)]
struct Observation {
    armed: bool,
    committed: bool,
    forbid_clock: bool,
    forbid_ledger_gets: bool,
    commits: usize,
    gets: Vec<Key>,
    scans: Vec<usize>,
    threads: Vec<ThreadId>,
    mutations: Vec<Mutation>,
    fail_next: bool,
}

#[derive(Clone)]
struct OwnerStore<S> {
    inner: S,
    observation: Arc<Mutex<Observation>>,
}

impl<S: StateStore> OwnerStore<S> {
    fn arm(&self, forbid_clock: bool, forbid_ledger_gets: bool) {
        *self.observation.lock().unwrap() = Observation {
            armed: true,
            forbid_clock,
            forbid_ledger_gets,
            ..Observation::default()
        };
    }

    fn disarm(&self) -> Observation {
        let mut observation = self.observation.lock().unwrap();
        let result = observation.clone();
        observation.armed = false;
        result
    }

    fn read(&self, key: Option<&[u8]>, scan: Option<usize>) {
        let mut observation = self.observation.lock().unwrap();
        if !observation.armed {
            return;
        }
        assert!(
            !observation.committed,
            "owner reread after a committed mutation"
        );
        observation.threads.push(std::thread::current().id());
        if let Some(key) = key {
            assert!(
                !observation.forbid_clock || key != keys::clock(),
                "pure owner read consulted stored Clock"
            );
            if observation.forbid_ledger_gets {
                let namespace = NamespaceName::new("tenant").unwrap();
                let entity = EntityPath::new("orders").unwrap();
                let usage = keys::queue_capacity_usage(&namespace, &entity)[0];
                let charge = keys::message_charge_prefix(&namespace, &entity)[0];
                assert!(
                    !matches!(key.first(), Some(tag) if *tag == usage || *tag == charge),
                    "deletion decoded opaque ledger data"
                );
            }
            observation.gets.push(key.to_vec());
        }
        if let Some(limit) = scan {
            observation.scans.push(limit);
        }
    }
}

impl<S: StateStore> StateStore for OwnerStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.read(Some(key), None);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.read(None, Some(limit));
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        assert!(
            !self.observation.lock().unwrap().armed,
            "owner used a snapshot fallback"
        );
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut observation = self.observation.lock().unwrap();
        if observation.armed {
            assert!(!observation.committed, "owner committed more than once");
            observation.threads.push(std::thread::current().id());
            observation.commits += 1;
            observation.mutations = batch.mutations().to_vec();
        }
        if observation.fail_next {
            observation.fail_next = false;
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "preapply owner failure".into(),
            });
        }
        drop(observation);
        self.inner.apply(batch)?;
        self.observation.lock().unwrap().committed = true;
        Ok(())
    }
}

struct Fixture<S: StateStore> {
    _broker: Broker,
    handle: server::BrokerHandle,
    store: OwnerStore<S>,
    clock: ProbeClock,
    namespace: NamespaceName,
    entity: EntityPath,
}

fn supported_config() -> QueueConfig {
    QueueConfig {
        duplicate_detection_history_time_window_millis: 60_000,
        ..QueueConfig::default()
    }
}

impl<S: StateStore> Fixture<S> {
    fn new(inner: S) -> TestResult<Self> {
        let store = OwnerStore {
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
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        Ok(Self {
            _broker: broker,
            handle,
            store,
            clock,
            namespace,
            entity,
        })
    }

    fn create(&self, entity: &EntityPath) -> TestResult<QueueCapacityView> {
        Ok(self.handle.create_finite_queue_blocking(
            self.namespace.clone(),
            entity.clone(),
            supported_config(),
            FiniteQueueCapacity::new(4 * MIB)?,
        )?)
    }

    fn send(&self, id: &str, bytes: usize) -> TestResult<SequenceNumber> {
        match self.handle.submit_blocking(
            self.namespace.clone(),
            self.entity.clone(),
            CommandKind::Send {
                message_id: id.into(),
                body: vec![1; bytes],
                time_to_live_millis: None,
                session_id: None,
            },
        )? {
            CommandOutcome::Sent { sequence } => Ok(sequence),
            _ => panic!("send outcome"),
        }
    }

    fn get(&self) -> Result<Option<QueueCapacityView>, AtomQueueOwnerError> {
        self.handle
            .get_atom_finite_queue_blocking(self.namespace.clone(), self.entity.clone())
    }

    fn update(
        &self,
        config: QueueConfig,
        limit: u64,
    ) -> Result<QueueCapacityView, AtomQueueOwnerError> {
        self.handle.update_atom_finite_queue_blocking(
            self.namespace.clone(),
            self.entity.clone(),
            config,
            FiniteQueueCapacity::new(limit).unwrap(),
        )
    }
}

fn wrapped(error: BrokerError) -> AtomQueueOwnerError {
    AtomQueueOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(error)))
}

fn assert_owner(observation: &Observation, commits: usize) {
    assert_eq!(observation.commits, commits);
    let owner = observation
        .threads
        .first()
        .expect("owner operations observed");
    assert_ne!(*owner, std::thread::current().id());
    assert!(observation.threads.iter().all(|thread| thread == owner));
}

fn projected_snapshot(before: &StoreSnapshot, mutations: &[Mutation]) -> Vec<(Key, Value)> {
    let mut rows = before.entries().iter().cloned().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                rows.remove(key);
            }
        }
    }
    rows.into_iter().collect()
}

async fn by_name_reads_and_complete_updates_stay_in_one_owner_turn<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    let created = fixture.handle.create_finite_queue_blocking(
        fixture.namespace.clone(),
        fixture.entity.clone(),
        QueueConfig {
            default_time_to_live_millis: Some(5_000),
            ..supported_config()
        },
        FiniteQueueCapacity::new(4 * MIB)?,
    )?;
    let sequence = fixture.send("retained", 2_048)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(true, false);
    let current = bounded(
        "read finite owner",
        fixture
            .handle
            .get_atom_finite_queue(fixture.namespace.clone(), fixture.entity.clone()),
    )
    .await?
    .unwrap();
    let observed = fixture.store.disarm();
    assert_owner(&observed, 0);
    assert!(observed.scans.is_empty());
    assert_eq!(current.binding, created.binding);
    assert_eq!(fixture.get()?, Some(current.clone()));
    assert_eq!(fixture.store.snapshot()?, before);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.clock.manual.set(2_000);
    let desired = QueueConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 3,
        max_message_bytes: 1_024,
        default_time_to_live_millis: None,
        dead_lettering_on_message_expiration: true,
        ..supported_config()
    };
    fixture.store.arm(false, false);
    let updated = bounded(
        "replace finite owner",
        fixture.handle.update_atom_finite_queue(
            fixture.namespace.clone(),
            fixture.entity.clone(),
            desired,
            FiniteQueueCapacity::new(2 * MIB)?,
        ),
    )
    .await?;
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    assert!(observed.scans.is_empty());
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads + 1);
    assert_eq!(updated.binding, current.binding);
    assert_eq!(updated.config, desired);
    assert_eq!(
        updated.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(2 * MIB)?,
            reserved_bytes: 2_573,
            message_count: 1
        }
    );
    let changed = observed
        .mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key.clone(),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        changed,
        BTreeSet::from([
            keys::clock(),
            keys::queue_config(&fixture.namespace, &fixture.entity),
            keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?),
            keys::queue_capacity_mode(&fixture.namespace, &fixture.entity)
        ])
    );
    let after = fixture.store.snapshot()?;
    assert_eq!(
        after.entries(),
        projected_snapshot(&before, &observed.mutations)
    );
    for key in [
        keys::message(&fixture.namespace, &fixture.entity, sequence),
        keys::message_charge(&fixture.namespace, &fixture.entity, sequence),
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
    ] {
        assert_eq!(
            before
                .entries()
                .iter()
                .find(|(candidate, _)| candidate == &key),
            after
                .entries()
                .iter()
                .find(|(candidate, _)| candidate == &key)
        );
    }
    fixture.clock.manual.set(3_000);
    fixture.store.arm(false, false);
    assert_eq!(fixture.update(desired, 2 * MIB)?, updated);
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, after);
    fixture.clock.manual.set(0);
    fixture.store.arm(false, false);
    assert!(matches!(
        fixture.update(desired, 2 * MIB),
        Err(AtomQueueOwnerError::Submit(SubmitError::Propose(
            ProposeError::ClockWentBackward { .. }
        )))
    ));
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, after);
    fixture.clock.manual.set(3_000);
    assert!(matches!(
        fixture.handle.submit_blocking(
            fixture.namespace.clone(),
            fixture.entity.clone(),
            CommandKind::Send {
                message_id: "too-large".into(),
                body: vec![0; 1_025],
                time_to_live_millis: None,
                session_id: None
            }
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::MessageTooLarge { .. }
        )))
    ));
    drop(fixture);
    let reopened = provider.open()?;
    assert_eq!(reopened.snapshot()?, after);
    let proposer = LocalProposer::new(StateMachine::new(reopened), ManualClock::at(3_000));
    assert_eq!(
        proposer
            .get_atom_finite_queue(&NamespaceName::new("tenant")?, &EntityPath::new("orders")?)?,
        Some(updated)
    );
    Ok(())
}

async fn desired_definition_refusals_precede_stamping_and_preserve_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(&fixture.entity)?;
    for index in 0..5 {
        fixture.send(&format!("large-{index}"), 256 * 1_024)?;
    }
    let before = fixture.store.snapshot()?;
    let current = fixture.get()?.unwrap();
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let immutable = QueueConfig {
        requires_session: true,
        requires_duplicate_detection: true,
        lock_duration_millis: 0,
        ..supported_config()
    };
    for (desired, limit, expected) in [
        (
            immutable,
            MIB + 1,
            wrapped(BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresSession,
            }),
        ),
        (
            QueueConfig {
                requires_duplicate_detection: true,
                lock_duration_millis: 0,
                ..supported_config()
            },
            MIB + 1,
            wrapped(BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresDuplicateDetection,
            }),
        ),
        (
            QueueConfig {
                lock_duration_millis: 0,
                ..supported_config()
            },
            MIB,
            wrapped(BrokerError::QueueConfig(
                domain::QueueConfigError::LockDurationTooShort,
            )),
        ),
        (
            QueueConfig {
                lock_duration_millis: 4_999,
                ..supported_config()
            },
            MIB,
            AtomQueueOwnerError::UnsupportedDefinition,
        ),
        (
            supported_config(),
            MIB + 1,
            AtomQueueOwnerError::UnsupportedDefinition,
        ),
        (
            QueueConfig {
                max_message_bytes: 1_025,
                ..supported_config()
            },
            MIB,
            AtomQueueOwnerError::UnsupportedDefinition,
        ),
    ] {
        fixture.store.arm(true, false);
        assert_eq!(fixture.update(desired, limit), Err(expected));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false, false);
    assert_eq!(
        fixture.update(
            QueueConfig {
                max_delivery_count: 2,
                ..supported_config()
            },
            MIB
        ),
        Err(wrapped(BrokerError::QueueCapacityFull))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(fixture.get()?, Some(current));
    assert_eq!(fixture.store.snapshot()?, before);
    let usage_key = keys::queue_capacity_usage(&fixture.namespace, &fixture.entity);
    fixture
        .store
        .apply(WriteBatch::default().put(usage_key, vec![255]))?;
    let corrupt = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true, false);
    assert_eq!(
        fixture.update(immutable, MIB + 1),
        Err(wrapped(BrokerError::QueueCapacityCorrupt))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, corrupt);
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, corrupt);
    Ok(())
}

async fn reads_keep_original_topology_and_capacity_refusal_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    let empty = fixture.store.snapshot()?;
    fixture.store.arm(true, false);
    assert_eq!(fixture.get()?, None);
    assert_eq!(
        fixture.update(supported_config(), MIB),
        Err(wrapped(BrokerError::QueueNotFound))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, empty);
    for key in [
        keys::queue_capacity_mode(&fixture.namespace, &fixture.entity),
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity.dead_letter_queue()?),
    ] {
        fixture
            .store
            .apply(WriteBatch::default().put(key.clone(), vec![255]))?;
        let orphan = fixture.store.snapshot()?;
        fixture.store.arm(true, false);
        assert_eq!(
            fixture.get(),
            Err(wrapped(BrokerError::QueueCapacityCorrupt))
        );
        assert_eq!(
            fixture.update(supported_config(), MIB),
            Err(wrapped(BrokerError::QueueCapacityCorrupt))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, orphan);
        fixture.store.apply(WriteBatch::default().delete(key))?;
    }
    assert_eq!(fixture.store.snapshot()?, empty);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.create(&fixture.entity)?;
    let healthy = fixture.store.snapshot()?;
    let config_key = keys::queue_config(&fixture.namespace, &fixture.entity);
    let mode_key = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    let shadow_key = keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?);
    let incarnation_key = keys::entity_incarnation(&fixture.namespace, &fixture.entity);
    for (batch, expected) in [
        (
            WriteBatch::default().delete(mode_key.clone()).put(
                config_key.clone(),
                codec::encode(&QueueConfig {
                    max_delivery_count: 0,
                    ..supported_config()
                })?,
            ),
            wrapped(BrokerError::QueueConfig(
                domain::QueueConfigError::MaxDeliveryCountTooSmall,
            )),
        ),
        (
            WriteBatch::default()
                .delete(mode_key.clone())
                .delete(shadow_key.clone()),
            wrapped(BrokerError::QueueCapacityCorrupt),
        ),
        (
            WriteBatch::default()
                .delete(mode_key.clone())
                .delete(incarnation_key.clone()),
            wrapped(BrokerError::QueueCapacityCorrupt),
        ),
        (
            WriteBatch::default().delete(mode_key.clone()),
            wrapped(BrokerError::QueueCapacityCorrupt),
        ),
    ] {
        fixture.store.apply(batch)?;
        let bad = fixture.store.snapshot()?;
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        fixture.store.arm(true, false);
        let AtomQueueOwnerError::Submit(metadata_error) = &expected else {
            panic!("original metadata diagnostic must remain a submit error");
        };
        assert_eq!(
            fixture.handle.admin_entity_metadata_blocking(
                fixture.namespace.clone(),
                AdminTarget::Primary(fixture.entity.clone()),
            ),
            Err(metadata_error.clone()),
            "the unchanged metadata API is the refusal-priority oracle"
        );
        assert_eq!(fixture.get(), Err(expected.clone()));
        assert_eq!(
            fixture
                .handle
                .atom_finite_queues_page_blocking(fixture.namespace.clone(), 0, 1),
            Err(expected)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, bad);
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
        let mut restore = WriteBatch::default();
        for (key, value) in healthy.entries() {
            restore.push_put(key.clone(), value.clone());
        }
        fixture.store.apply(restore)?;
        assert_eq!(fixture.store.snapshot()?, healthy);
    }
    let topic = EntityPath::new("topic")?;
    fixture.handle.submit_blocking(
        fixture.namespace.clone(),
        topic.clone(),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .handle
            .get_atom_finite_queue_blocking(fixture.namespace.clone(), topic.clone()),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    fixture.store.apply(WriteBatch::default().put(
        keys::queue_capacity_mode(&fixture.namespace, &topic),
        vec![255],
    ))?;
    let orphan = fixture.store.snapshot()?;
    fixture.store.arm(true, false);
    assert_eq!(
        fixture
            .handle
            .get_atom_finite_queue_blocking(fixture.namespace.clone(), topic),
        Err(wrapped(BrokerError::QueueCapacityCorrupt))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, orphan);
    assert!(fixture.clock.reads.load(Ordering::SeqCst) > reads);
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, orphan);
    Ok(())
}

async fn pages_validate_skipped_candidates_and_fill_or_prove_exhaustion<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    let mut expected = Vec::new();
    for index in 0..115 {
        let entity = EntityPath::new(format!("q-{index:03}"))?;
        expected.push(fixture.create(&entity)?);
    }
    let foreign = NamespaceName::new("foreign")?;
    fixture.handle.create_finite_queue_blocking(
        foreign,
        EntityPath::new("q-000")?,
        supported_config(),
        FiniteQueueCapacity::new(MIB)?,
    )?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true, false);
    let page = bounded(
        "filled visible owner page",
        fixture
            .handle
            .atom_finite_queues_page(fixture.namespace.clone(), 10, 100),
    )
    .await?;
    let observation = fixture.store.disarm();
    assert_owner(&observation, 0);
    assert_eq!(page, expected[10..110]);
    assert!(observation.scans.len() >= 2);
    assert!(observation.scans.iter().all(|limit| *limit <= 129));
    assert_eq!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 100, 100)?,
        expected[100..]
    );
    assert!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 115, 100)?
            .is_empty()
    );
    assert!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 1_000, 1)?
            .is_empty()
    );
    for (skip, top) in [(0, 0), (0, 101), (1_001, 1), (usize::MAX, usize::MAX)] {
        assert_eq!(
            fixture
                .handle
                .atom_finite_queues_page_blocking(fixture.namespace.clone(), skip, top),
            Err(AtomQueueOwnerError::InvalidPageBounds)
        );
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    let first = &expected[0].binding;
    fixture.store.apply(WriteBatch::default().put(
        keys::queue_capacity_usage(&fixture.namespace, first.owner()),
        vec![255],
    ))?;
    let corrupt = fixture.store.snapshot()?;
    fixture.store.arm(true, false);
    assert_eq!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 1, 1),
        Err(wrapped(BrokerError::QueueCapacityCorrupt))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, corrupt);
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, corrupt);
    Ok(())
}

async fn unsupported_owners_are_refused_including_skipped_rows_and_literal_collection<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.handle.submit_blocking(
        fixture.namespace.clone(),
        fixture.entity.clone(),
        CommandKind::CreateQueue {
            config: supported_config(),
        },
    )?;
    let before = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true, false);
    assert_eq!(
        fixture.get(),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    assert_eq!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 1, 1),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    assert_eq!(
        fixture.update(supported_config(), MIB),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.handle.submit_blocking(
        fixture.namespace.clone(),
        fixture.entity.clone(),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    let reserved = EntityPath::new("$Resources/queues")?;
    fixture.create(&reserved)?;
    let sibling = EntityPath::new("$resources/queues")?;
    let sibling_view = fixture.create(&sibling)?;
    let percent = EntityPath::new("$Resources/%71ueues")?;
    let percent_view = fixture.create(&percent)?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture
            .handle
            .get_atom_finite_queue_blocking(fixture.namespace.clone(), reserved),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    assert_eq!(
        fixture
            .handle
            .get_atom_finite_queue_blocking(fixture.namespace.clone(), sibling)?,
        Some(sibling_view)
    );
    assert_eq!(
        fixture
            .handle
            .get_atom_finite_queue_blocking(fixture.namespace.clone(), percent)?,
        Some(percent_view)
    );
    assert_eq!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 1, 1),
        Err(AtomQueueOwnerError::UnsupportedDefinition)
    );
    let after = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, after);
    Ok(())
}

async fn raw_page_work_exhaustion_refuses_without_a_short_feed<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    let mut batch = WriteBatch::default();
    for index in 0..4_097 {
        let excluded = EntityPath::new(format!("a-{index:04}/$deadletterqueue"))?;
        batch.push_put(keys::queue_config(&fixture.namespace, &excluded), vec![255]);
    }
    fixture.store.apply(batch)?;
    fixture.create(&EntityPath::new("z-visible")?)?;
    let before = fixture.store.snapshot()?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(true, false);
    assert_eq!(
        fixture
            .handle
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 0, 1),
        Err(AtomQueueOwnerError::WorkLimitExceeded)
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 0);
    assert!(observed.scans.len() <= 64);
    assert!(observed.scans.iter().all(|limit| *limit <= 129));
    assert!(observed.gets.is_empty());
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, before);
    Ok(())
}

async fn deletion_keeps_corrupt_ledger_opaque_and_publishes_only_committed_wakeups<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    use std::{future::poll_fn, task::Poll};
    let fixture = Fixture::new(provider.open()?)?;
    let created = fixture.create(&fixture.entity)?;
    let sequence = fixture.send("opaque", 5)?;
    let shadow = fixture.entity.dead_letter_queue()?;
    fixture.store.apply(
        WriteBatch::default()
            .put(
                keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
                vec![255],
            )
            .put(
                keys::queue_capacity_usage(&fixture.namespace, &shadow),
                vec![255],
            )
            .put(
                keys::message_charge(&fixture.namespace, &fixture.entity, sequence),
                vec![255],
            )
            .put(
                keys::message_charge(&fixture.namespace, &shadow, SequenceNumber::new(99)),
                vec![255],
            ),
    )?;
    let before = fixture.store.snapshot()?;
    assert_eq!(
        fixture.get(),
        Err(wrapped(BrokerError::QueueCapacityCorrupt))
    );
    let mut primary_wait = Box::pin(
        fixture
            .handle
            .deliverable(&fixture.namespace, &fixture.entity),
    );
    let mut shadow_wait = Box::pin(fixture.handle.deliverable(&fixture.namespace, &shadow));
    let foreign_namespace = NamespaceName::new("foreign")?;
    let mut foreign_wait = Box::pin(
        fixture
            .handle
            .deliverable(&foreign_namespace, &fixture.entity),
    );
    fixture.store.arm(false, true);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        fixture
            .handle
            .delete_atom_finite_queue_blocking(fixture.namespace.clone(), fixture.entity.clone()),
        Err(AtomQueueOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    assert_owner(&fixture.store.disarm(), 1);
    assert_eq!(fixture.store.snapshot()?, before);
    poll_fn(|context| {
        assert!(primary_wait.as_mut().poll(context).is_pending());
        assert!(shadow_wait.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false, true);
    assert_eq!(
        bounded(
            "opaque finite owner deletion",
            fixture
                .handle
                .delete_atom_finite_queue(fixture.namespace.clone(), fixture.entity.clone())
        )
        .await?,
        CommandOutcome::QueueDeleted
    );
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let after = fixture.store.snapshot()?;
    assert_eq!(
        after.entries(),
        projected_snapshot(&before, &observed.mutations)
    );
    let deleted = observed
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::Delete { key } => Some(key),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for key in [
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
        keys::queue_capacity_usage(&fixture.namespace, &shadow),
        keys::message_charge(&fixture.namespace, &fixture.entity, sequence),
        keys::message_charge(&fixture.namespace, &shadow, SequenceNumber::new(99)),
    ] {
        assert!(deleted.contains(&key));
    }
    tokio::time::timeout(DEADLINE, &mut primary_wait).await?;
    tokio::time::timeout(DEADLINE, &mut shadow_wait).await?;
    poll_fn(|context| {
        assert!(foreign_wait.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(primary_wait);
    drop(shadow_wait);
    drop(foreign_wait);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true, true);
    assert_eq!(
        fixture
            .handle
            .delete_atom_finite_queue_blocking(fixture.namespace.clone(), fixture.entity.clone()),
        Err(wrapped(BrokerError::QueueNotFound))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.store.snapshot()?, after);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    let recreated = fixture.create(&fixture.entity)?;
    assert_eq!(
        recreated.binding.generation(),
        created.binding.generation() + 1
    );
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    assert_eq!(fixture.get()?, Some(recreated.clone()));
    let final_snapshot = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, final_snapshot);
    Ok(())
}

macro_rules! for_each_atom_owner_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread")] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_atom_owner_backend! {
    by_name_reads_and_complete_updates_stay_in_one_owner_turn,
    desired_definition_refusals_precede_stamping_and_preserve_state,
    reads_keep_original_topology_and_capacity_refusal_priority,
    pages_validate_skipped_candidates_and_fill_or_prove_exhaustion,
    unsupported_owners_are_refused_including_skipped_rows_and_literal_collection,
    raw_page_work_exhaustion_refuses_without_a_short_feed,
    deletion_keeps_corrupt_ledger_opaque_and_publishes_only_committed_wakeups,
}
