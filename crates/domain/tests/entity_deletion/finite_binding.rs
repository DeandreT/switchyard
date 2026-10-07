use domain::{
    EntityBinding, EntityIncarnationKind, FencedCommand, FiniteQueueCapacity,
    QueueCapacityCommandV1, QueueConfigError, StateMachine,
};

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadEvent {
    Get(Key),
    Scan {
        prefix: Key,
        start: Key,
        limit: usize,
    },
}

#[derive(Clone, Debug, Default)]
struct ProofObservations {
    armed: bool,
    forbid_clock: bool,
    forbid_writes: bool,
    forbid_scans: bool,
    applied: bool,
    reads: Vec<ReadEvent>,
    batches: Vec<Vec<Mutation>>,
}

#[derive(Clone, Debug)]
struct ProofStore<S> {
    inner: S,
    observations: Arc<Mutex<ProofObservations>>,
    forbidden_tags: [u8; 2],
}

impl<S: StateStore> ProofStore<S> {
    fn new(inner: S, namespace: &NamespaceName, entity: &EntityPath) -> TestResult<Self> {
        entity.dead_letter_queue()?;
        Ok(Self {
            inner,
            observations: Arc::new(Mutex::new(ProofObservations::default())),
            forbidden_tags: [
                keys::queue_capacity_usage(namespace, entity)[0],
                keys::message_charge_prefix(namespace, entity)[0],
            ],
        })
    }

    fn arm(&self, forbid_clock: bool, forbid_writes: bool, forbid_scans: bool) {
        *self.observations.lock().expect("proof observations") = ProofObservations {
            armed: true,
            forbid_clock,
            forbid_writes,
            forbid_scans,
            ..ProofObservations::default()
        };
    }

    fn disarm(&self) -> ProofObservations {
        std::mem::take(&mut *self.observations.lock().expect("proof observations"))
    }
}

impl<S: StateStore> StateStore for ProofStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let mut observed = self.observations.lock().expect("proof observations");
        if observed.armed {
            assert!(!observed.applied, "post-commit point read");
            assert!(
                !observed.forbid_clock || key != keys::clock().as_slice(),
                "stored clock read by binding proof"
            );
            assert!(
                key.first()
                    .is_none_or(|tag| !self.forbidden_tags.contains(tag)),
                "Usage/Charge point read"
            );
            observed.reads.push(ReadEvent::Get(key.to_vec()));
        }
        drop(observed);
        self.inner.get(key)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        assert!(
            !self.observations.lock().expect("proof observations").armed,
            "snapshot by proof or after commit"
        );
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let mut observed = self.observations.lock().expect("proof observations");
        if observed.armed {
            assert!(!observed.applied, "post-commit scan");
            assert!(!observed.forbid_scans, "runtime scan by live binding proof");
            observed.reads.push(ReadEvent::Scan {
                prefix: prefix.to_vec(),
                start: start.to_vec(),
                limit,
            });
        }
        drop(observed);
        self.inner.scan_from(prefix, start, limit)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut observed = self.observations.lock().expect("proof observations");
        if observed.armed {
            assert!(
                !observed.forbid_writes,
                "mutation by read-only binding proof"
            );
            assert!(!observed.applied, "second commit");
            observed.batches.push(batch.mutations().to_vec());
            observed.applied = true;
        }
        drop(observed);
        self.inner.apply(batch)
    }
}

fn finite<P: StoreProvider>(provider: P) -> TestResult<(QueueFixture<P>, EntityBinding)> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    let view = fixture
        .machine
        .apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
            namespace: fixture.namespace.clone(),
            entity: fixture.entity.clone(),
            issued_at: Timestamp::from_millis(2),
            config: QueueConfig::default(),
            limit: FiniteQueueCapacity::new(16_384)?,
        })?;
    Ok((fixture, view.binding))
}

fn guarded<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<ProofStore<P::Store>> {
    ProofStore::new(
        fixture.machine.store().clone(),
        &fixture.namespace,
        &fixture.entity,
    )
}

fn fenced_delete<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    binding: EntityBinding,
    millis: u64,
) -> FencedCommand {
    FencedCommand {
        binding,
        command: fixture.command(
            millis,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        ),
    }
}

fn live_finite_binding_is_read_only_mode_only_and_reopens<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, expected) = finite(provider)?;
    let before = fixture.machine.store().snapshot()?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?,
        expected
    );
    let observed = store.disarm();
    assert!(observed.batches.is_empty());
    let shadow = fixture.entity.dead_letter_queue()?;
    assert_eq!(
        observed.reads,
        vec![
            ReadEvent::Get(keys::queue_config(&fixture.namespace, &fixture.entity)),
            ReadEvent::Get(keys::topic_config(&fixture.namespace, &fixture.entity)),
            ReadEvent::Get(keys::entity_incarnation(
                &fixture.namespace,
                &fixture.entity
            )),
            ReadEvent::Get(keys::entity_incarnation(
                &fixture.namespace,
                &fixture.entity
            )),
            ReadEvent::Get(keys::entity_incarnation(
                &fixture.namespace,
                &fixture.entity
            )),
            ReadEvent::Get(keys::queue_config(&fixture.namespace, &fixture.entity)),
            ReadEvent::Get(keys::queue_config(&fixture.namespace, &shadow)),
            ReadEvent::Get(keys::topic_config(&fixture.namespace, &fixture.entity)),
            ReadEvent::Get(keys::topic_config(&fixture.namespace, &shadow)),
            ReadEvent::Get(keys::queue_capacity_mode(
                &fixture.namespace,
                &fixture.entity
            )),
            ReadEvent::Get(keys::queue_capacity_mode(&fixture.namespace, &shadow)),
        ]
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    drop(machine);
    drop(store);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?,
        expected
    );
    Ok(())
}

fn corrupt_mode_refuses_but_opaque_usage_and_charges_can_be_purged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, expected) = finite(provider)?;
    let session = SessionId::new("optional-metadata")?;
    let sequence = send(&fixture, 3, "retained", Some(&session))?;
    let mode_key = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    let mode = fixture
        .machine
        .store()
        .get(&mode_key)?
        .expect("finite mode");
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(mode_key.clone(), vec![255]))?;
    let before = fixture.machine.store().snapshot()?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    let observed = store.disarm();
    assert!(observed.batches.is_empty());
    store.arm(true, true, true);
    assert_eq!(
        machine.apply_fenced_with_effects(&fenced_delete(&fixture, expected.clone(), 0)),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(store.disarm().batches.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let shadow = fixture.entity.dead_letter_queue()?;
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(mode_key, mode)
            .put(
                keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
                b"opaque bad usage".to_vec(),
            )
            .put(
                keys::queue_capacity_usage(&fixture.namespace, &shadow),
                b"opaque shadow usage".to_vec(),
            )
            .put(
                keys::message_charge(&fixture.namespace, &fixture.entity, sequence),
                b"opaque bad charge".to_vec(),
            )
            .put(
                keys::message_charge(&fixture.namespace, &shadow, sequence),
                b"opaque shadow charge".to_vec(),
            ),
    )?;
    assert_eq!(
        fixture
            .machine
            .describe_queue_capacity(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &fixture.entity,
            &fixture.entity,
            EntityIncarnationKind::Queue,
        ),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    let before = fixture.machine.store().snapshot()?;
    store.arm(true, true, true);
    let binding = machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?;
    assert_eq!(binding, expected);
    assert!(store.disarm().batches.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    store.arm(false, false, false);
    let applied = machine.apply_fenced_with_effects(&fenced_delete(&fixture, binding, 10))?;
    let observed = store.disarm();
    assert_eq!(applied.outcome, CommandOutcome::QueueDeleted);
    assert_eq!(
        applied.entity_deletions,
        Some(vec![fixture.entity.clone(), shadow.clone()])
    );
    assert_eq!(applied.subscription_enqueues, None);
    assert!(!applied.dead_letters_enqueued);
    assert_eq!(observed.batches.len(), 1);
    assert_exact_purge(
        &fixture,
        &before,
        &[fixture.entity.clone(), shadow],
        &[],
        &[],
        10,
    )?;
    let deleted = fixture.machine.store().snapshot()?;
    drop(machine);
    drop(store);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, deleted);
    assert_eq!(
        fixture
            .machine
            .bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueNotFound)
    );
    Ok(())
}

fn absent_case<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    store: &ProofStore<P::Store>,
    machine: &StateMachine<ProofStore<P::Store>>,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    store.arm(true, true, false);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(expected.clone())
    );
    let proof = store.disarm();
    assert!(proof.batches.is_empty());
    store.arm(false, true, false);
    assert_eq!(
        machine.apply(&fixture.command(
            10,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        )),
        Err(expected)
    );
    let original = store.disarm();
    assert!(original.batches.is_empty());
    assert_eq!(original.reads.first(), Some(&ReadEvent::Get(keys::clock())));
    assert_eq!(proof.reads, original.reads[1..]);
    for event in &proof.reads {
        if let ReadEvent::Scan { limit, .. } = event {
            assert_eq!(*limit, 1);
        }
    }
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn absent_target_preserves_original_multifault_priority_and_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("missing")?;
    let shadow = fixture.entity.dead_letter_queue()?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    absent_case(&fixture, &store, &machine, BrokerError::QueueNotFound)?;
    let mut rule = keys::topic_rule_prefix(&fixture.namespace, &fixture.entity);
    rule.extend_from_slice(b"orphan");
    let subscription = keys::subscription(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("Ghost")?,
    );
    let mode = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    let usage = keys::queue_capacity_usage(&fixture.namespace, &fixture.entity);
    let incarnation = keys::entity_incarnation(&fixture.namespace, &fixture.entity);
    let shadow_config = keys::queue_config(&fixture.namespace, &shadow);
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(rule.clone(), vec![1])
            .put(subscription.clone(), vec![1])
            .put(mode.clone(), vec![255])
            .put(usage.clone(), vec![255])
            .put(
                incarnation.clone(),
                codec::encode(&EntityIncarnation::new(
                    1,
                    EntityIncarnationKind::Queue,
                    false,
                )?)?,
            )
            .put(shadow_config.clone(), vec![255]),
    )?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingRuleMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(rule))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingSubscriptionMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(subscription))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::QueueCapacityCorrupt,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(mode))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingEntityMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(usage))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingEntityMetadata,
    )?;
    fixture.machine.store().apply(WriteBatch::default().put(
        incarnation,
        codec::encode(&EntityIncarnation::new(
            1,
            EntityIncarnationKind::Queue,
            true,
        )?)?,
    ))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingEntityMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_config))?;
    let mut shadow_rule = keys::topic_rule_prefix(&fixture.namespace, &shadow);
    shadow_rule.extend_from_slice(b"orphan");
    let shadow_mode = keys::queue_capacity_mode(&fixture.namespace, &shadow);
    let shadow_usage = keys::queue_capacity_usage(&fixture.namespace, &shadow);
    let shadow_charge = keys::message_charge(&fixture.namespace, &shadow, SequenceNumber::new(1));
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(shadow_rule.clone(), vec![1])
            .put(shadow_mode.clone(), vec![255])
            .put(shadow_usage.clone(), vec![255])
            .put(shadow_charge.clone(), vec![255]),
    )?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingRuleMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_rule))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::QueueCapacityCorrupt,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_mode))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingEntityMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_usage))?;
    absent_case(
        &fixture,
        &store,
        &machine,
        BrokerError::DanglingEntityMetadata,
    )?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_charge))?;
    absent_case(&fixture, &store, &machine, BrokerError::QueueNotFound)?;
    let before = fixture.machine.store().snapshot()?;
    drop(machine);
    drop(store);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueNotFound)
    );
    Ok(())
}

fn positive_metadata_and_target_refusals_precede_mode_or_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, _) = finite(provider)?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    for (path, error) in [
        (
            fixture.entity.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            fixture
                .entity
                .subscription(&SubscriptionName::new("Ghost")?)?,
            BrokerError::SubscriptionPathIsReserved,
        ),
    ] {
        store.arm(true, true, true);
        assert_eq!(
            machine.bind_finite_queue_for_deletion(&fixture.namespace, &path),
            Err(error)
        );
        let observed = store.disarm();
        assert!(observed.reads.is_empty());
        assert!(observed.batches.is_empty());
    }
    let config_key = keys::queue_config(&fixture.namespace, &fixture.entity);
    let config = fixture
        .machine
        .store()
        .get(&config_key)?
        .expect("queue config");
    let mode_key = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    let mode = fixture
        .machine
        .store()
        .get(&mode_key)?
        .expect("finite mode");
    let topic_key = keys::topic_config(&fixture.namespace, &fixture.entity);
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(config_key.clone(), vec![255])
            .put(topic_key.clone(), vec![255])
            .put(mode_key.clone(), vec![255]),
    )?;
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(store.disarm().reads.len(), 2);
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(config_key.clone()))?;
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::EntityKindMismatch)
    );
    assert_eq!(store.disarm().reads.len(), 2);
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(topic_key).put(
            config_key.clone(),
            codec::encode(&QueueConfig {
                max_delivery_count: 0,
                ..QueueConfig::default()
            })?,
        ))?;
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueConfig(
            QueueConfigError::MaxDeliveryCountTooSmall
        ))
    );
    assert_eq!(store.disarm().reads.len(), 2);
    let incarnation_key = keys::entity_incarnation(&fixture.namespace, &fixture.entity);
    let incarnation = fixture
        .machine
        .store()
        .get(&incarnation_key)?
        .expect("live incarnation");
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(config_key, config)
            .delete(incarnation_key.clone()),
    )?;
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(store.disarm().reads.len(), 3);
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(incarnation_key, incarnation)
            .put(mode_key, mode),
    )?;
    store.arm(true, true, true);
    machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?;
    assert!(store.disarm().batches.is_empty());
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &EntityPath::new("anchor")?),
        Err(BrokerError::QueueCapacityNotSupported)
    );
    assert!(store.disarm().batches.is_empty());
    Ok(())
}

fn successful_binding_is_not_a_successful_purge<P: StoreProvider>(provider: P) -> TestResult {
    let (fixture, expected) = finite(provider)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message_charge(&fixture.namespace, &fixture.entity, SequenceNumber::new(1)),
        b"orphan without source counter".to_vec(),
    ))?;
    let before = fixture.machine.store().snapshot()?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    store.arm(true, true, true);
    let binding = machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?;
    assert_eq!(binding, expected);
    assert!(store.disarm().batches.is_empty());
    store.arm(false, true, false);
    assert_eq!(
        machine.apply_fenced_with_effects(&fenced_delete(&fixture, binding, 10)),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert!(store.disarm().batches.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    drop(machine);
    drop(store);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?,
        expected
    );
    Ok(())
}

fn recreated_queue_refuses_the_old_binding_before_stored_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, old) = finite(provider)?;
    fixture
        .machine
        .apply_fenced(&fenced_delete(&fixture, old.clone(), 10))?;
    let fresh = fixture
        .machine
        .apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
            namespace: fixture.namespace.clone(),
            entity: fixture.entity.clone(),
            issued_at: Timestamp::from_millis(11),
            config: QueueConfig::default(),
            limit: FiniteQueueCapacity::new(16_384)?,
        })?
        .binding;
    assert!(fresh.generation() > old.generation());
    let before = fixture.machine.store().snapshot()?;
    let store = guarded(&fixture)?;
    let machine = StateMachine::new(store.clone());
    store.arm(true, true, true);
    assert_eq!(
        machine.bind_finite_queue_for_deletion(&fixture.namespace, &fixture.entity)?,
        fresh
    );
    assert!(store.disarm().batches.is_empty());
    store.arm(true, true, true);
    assert_eq!(
        machine.apply_fenced_with_effects(&fenced_delete(&fixture, old, 0)),
        Err(BrokerError::EntityBindingStale)
    );
    assert!(store.disarm().batches.is_empty());
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    live_finite_binding_is_read_only_mode_only_and_reopens,
    corrupt_mode_refuses_but_opaque_usage_and_charges_can_be_purged,
    absent_target_preserves_original_multifault_priority_and_reads,
    positive_metadata_and_target_refusals_precede_mode_or_clock,
    successful_binding_is_not_a_successful_purge,
    recreated_queue_refuses_the_old_binding_before_stored_clock,
}
