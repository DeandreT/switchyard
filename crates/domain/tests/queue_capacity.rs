//! Logical reservations share one owner across the primary queue and its DLQ.

#[path = "queue_capacity/definition.rs"]
mod definition;

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    AtomicMessagingCommand, BrokerError, Command, CommandKind, CommandOutcome, DeleteEntityTarget,
    Delivery, EntityBinding, EntityIncarnation, EntityIncarnationKind, EntityPath,
    FiniteQueueCapacity, IngressEnvelope, LockToken, MessageEnvelope, MessageRecord, MessageState,
    MessageValue, NamespaceName, QueueCapacityCommandV1, QueueCapacityStatus, QueueCapacityView,
    QueueConfig, QueueCounters, ReceiveMode, ScheduledMessage, SequenceNumber, SessionId,
    SettlementDisposition, StateMachine, Timestamp, codec, keys,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Default)]
struct Observation {
    reads: Vec<Key>,
    scans: usize,
    commits: usize,
    mutations: Vec<Mutation>,
    before_commit: Option<StoreSnapshot>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    observation: Arc<Mutex<Observation>>,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observation.lock().unwrap().reads.push(key.to_vec());
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observation.lock().unwrap().scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let before = {
            let mut observation = self.observation.lock().unwrap();
            observation.commits += 1;
            observation.mutations = batch.mutations().to_vec();
            observation.before_commit.clone()
        };
        if let Some(before) = before {
            assert_eq!(self.inner.snapshot()?, before);
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected capacity failure before apply".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    observation: Arc<Mutex<Observation>>,
    fail_next: Arc<AtomicBool>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            observation: self.observation.clone(),
            fail_next: self.fail_next.clone(),
        })
    }
}

struct Fixture<P: StoreProvider> {
    provider: P,
    machine: StateMachine<P::Store>,
    namespace: NamespaceName,
    entity: EntityPath,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P, limit: u64, config: QueueConfig) -> TestResult<Self> {
        let fixture = Self {
            machine: StateMachine::new(provider.open()?),
            provider,
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
        };
        fixture.create(0, limit, config)?;
        Ok(fixture)
    }

    fn create(
        &self,
        millis: u64,
        limit: u64,
        config: QueueConfig,
    ) -> Result<QueueCapacityView, BrokerError> {
        self.machine
            .apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
                namespace: self.namespace.clone(),
                entity: self.entity.clone(),
                issued_at: Timestamp::from_millis(millis),
                config,
                limit: FiniteQueueCapacity::new(limit)?,
            })
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

    fn apply(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at(&self.entity, millis, kind)
    }

    fn usage(&self) -> TestResult<(u64, u64)> {
        let view = self
            .machine
            .describe_queue_capacity(&self.namespace, &self.entity)?
            .unwrap();
        let QueueCapacityStatus::FiniteV1 {
            reserved_bytes,
            message_count,
            ..
        } = view.capacity
        else {
            panic!("finite profile")
        };
        let raw = self
            .machine
            .store()
            .get(&keys::queue_capacity_usage(&self.namespace, &self.entity))?
            .unwrap();
        let (schema, generation, model, bytes, count): (u8, u64, u8, u64, u64) =
            codec::decode(&raw)?;
        assert_eq!(
            (schema, generation, model),
            (1, view.binding.generation(), 1)
        );
        assert_eq!((bytes, count), (reserved_bytes, message_count));
        Ok((bytes, count))
    }

    fn binding(&self) -> TestResult<EntityBinding> {
        Ok(self
            .machine
            .describe_queue_capacity(&self.namespace, &self.entity)?
            .unwrap()
            .binding)
    }

    fn set_limit(&self, millis: u64, limit: u64) -> TestResult<QueueCapacityView> {
        Ok(self
            .machine
            .apply_queue_capacity(&QueueCapacityCommandV1::SetLimitFenced {
                binding: self.binding()?,
                issued_at: Timestamp::from_millis(millis),
                limit: FiniteQueueCapacity::new(limit)?,
            })?)
    }

    fn restart(self) -> TestResult<Self> {
        let Self {
            provider,
            machine,
            namespace,
            entity,
        } = self;
        drop(machine);
        Ok(Self {
            machine: StateMachine::new(provider.open()?),
            provider,
            namespace,
            entity,
        })
    }
}

fn send_kind(id: &str, body: &[u8], ttl: Option<u64>) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: body.to_vec(),
        time_to_live_millis: ttl,
        session_id: None,
    }
}

fn receive<P: StoreProvider>(
    fixture: &Fixture<P>,
    entity: &EntityPath,
    millis: u64,
    mode: ReceiveMode,
) -> TestResult<Delivery> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        entity,
        millis,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("one delivery")
    };
    Ok(delivery)
}

fn settle<P: StoreProvider>(
    fixture: &Fixture<P>,
    entity: &EntityPath,
    millis: u64,
    delivery: &Delivery,
    disposition: SettlementDisposition,
    properties: BTreeMap<String, MessageValue>,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(
        entity,
        millis,
        CommandKind::Settle {
            sequence: delivery.sequence,
            lock_token: delivery.lock.as_ref().unwrap().token,
            disposition,
            properties_to_modify: properties,
        },
    )
}

fn create_and_send_have_an_independent_complete_record_image<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(10, send_kind("one", &[1, 2], None))?;
    let shadow = fixture.entity.dead_letter_queue()?;
    let record = MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: "one".into(),
        body: vec![1, 2],
        enqueued_at: Timestamp::from_millis(10),
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    };
    let mut expected = vec![
        (keys::clock(), codec::encode(&Timestamp::from_millis(10))?),
        (
            keys::queue_config(&fixture.namespace, &fixture.entity),
            codec::encode(&QueueConfig::default())?,
        ),
        (
            keys::queue_config(&fixture.namespace, &shadow),
            codec::encode(&QueueConfig::default().dead_letter_shadow())?,
        ),
        (
            keys::entity_incarnation(&fixture.namespace, &fixture.entity),
            codec::encode(&EntityIncarnation::new(
                1,
                EntityIncarnationKind::Queue,
                false,
            )?)?,
        ),
        (
            keys::queue_capacity_mode(&fixture.namespace, &fixture.entity),
            codec::encode(&(1_u8, 1_u64, 1_u8, 522_u64))?,
        ),
        (
            keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
            codec::encode(&(1_u8, 1_u64, 1_u8, 522_u64, 1_u64))?,
        ),
        (
            keys::message_charge(&fixture.namespace, &fixture.entity, record.sequence),
            codec::encode(&(1_u8, 1_u64, 1_u8, 10_u64, 0_u64, 0_u64, 522_u64))?,
        ),
        (
            keys::queue_counters(&fixture.namespace, &fixture.entity),
            codec::encode(&QueueCounters {
                next_sequence: 2,
                next_lock_token: 1,
            })?,
        ),
        (
            keys::message(&fixture.namespace, &fixture.entity, record.sequence),
            codec::encode(&record)?,
        ),
        (
            keys::ready(&fixture.namespace, &fixture.entity, record.sequence),
            Vec::new(),
        ),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(fixture.machine.store().snapshot()?.entries(), &expected);
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(20, send_kind("two", &[], None)),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.usage()?, (522, 1));
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?.entries(), &expected);
    Ok(())
}

fn locks_renewal_deferral_and_abandon_retain_credit_until_completion<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    assert_eq!(fixture.usage()?, (522, 1));
    fixture.apply(
        3,
        CommandKind::RenewLock {
            sequence: held.sequence,
            lock_token: held.lock.as_ref().unwrap().token,
            lock_duration_millis: Some(100),
        },
    )?;
    settle(
        &fixture,
        &fixture.entity,
        4,
        &held,
        SettlementDisposition::Abandon,
        BTreeMap::new(),
    )?;
    let held = receive(&fixture, &fixture.entity, 5, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        6,
        &held,
        SettlementDisposition::Defer,
        BTreeMap::new(),
    )?;
    assert_eq!(fixture.usage()?, (522, 1));
    let CommandOutcome::DeferredReceived(deliveries) = fixture.apply(
        7,
        CommandKind::ReceiveDeferred {
            sequences: vec![held.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
    )?
    else {
        panic!("deferred delivery")
    };
    settle(
        &fixture,
        &fixture.entity,
        8,
        &deliveries[0],
        SettlementDisposition::Complete,
        BTreeMap::new(),
    )?;
    assert_eq!(fixture.usage()?, (0, 0));
    assert_eq!(
        fixture.machine.store().get(&keys::message_charge(
            &fixture.namespace,
            &fixture.entity,
            held.sequence
        ))?,
        None
    );
    Ok(())
}

fn complete_refunds_original_charge_despite_unretained_property_growth<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    let properties = BTreeMap::from([("label".into(), MessageValue::String("x".repeat(500)))]);
    settle(
        &fixture,
        &fixture.entity,
        3,
        &held,
        SettlementDisposition::Complete,
        properties,
    )?;
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn retained_property_growth_is_atomic_and_a_later_shrink_returns_credit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    let properties = BTreeMap::from([("label".into(), MessageValue::String("x".repeat(500)))]);
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        settle(
            &fixture,
            &fixture.entity,
            3,
            &held,
            SettlementDisposition::Abandon,
            properties.clone()
        ),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.set_limit(3, 2_000)?;
    settle(
        &fixture,
        &fixture.entity,
        4,
        &held,
        SettlementDisposition::Abandon,
        properties,
    )?;
    let grown = fixture.usage()?.0;
    assert!(grown > 522);
    let held = receive(&fixture, &fixture.entity, 5, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        6,
        &held,
        SettlementDisposition::Defer,
        BTreeMap::from([("label".into(), MessageValue::String("x".into()))]),
    )?;
    assert_eq!(fixture.usage()?, (grown - 499, 1));
    Ok(())
}

fn automatic_dead_lettering_at_full_capacity_preserves_shared_credit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(
        provider,
        524,
        QueueConfig {
            max_delivery_count: 1,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let mut input = send_kind("one", &[1, 2], Some(2));
    let CommandKind::Send { session_id, .. } = &mut input else {
        unreachable!()
    };
    *session_id = Some(SessionId::new("s")?);
    // The original optional session adds six bytes, so make room explicitly.
    fixture.set_limit(0, 528)?;
    fixture.apply(1, input)?;
    assert_eq!(fixture.usage()?, (528, 1));
    fixture.apply(3, CommandKind::ExpireMessages)?;
    let shadow = fixture.entity.dead_letter_queue()?;
    assert_eq!(fixture.usage()?, (528, 1));
    let record = fixture
        .machine
        .message(&fixture.namespace, &shadow, SequenceNumber::new(1))?
        .unwrap();
    assert_eq!(record.session_id, None);
    let raw = fixture
        .machine
        .store()
        .get(&keys::message_charge(
            &fixture.namespace,
            &shadow,
            record.sequence,
        ))?
        .unwrap();
    let (_, _, _, producer, session, projected, charged): (u8, u64, u8, u64, u64, u64, u64) =
        codec::decode(&raw)?;
    assert_eq!((producer, session, projected, charged), (10, 6, 137, 528));
    let fixture = fixture.restart()?;
    let drained = receive(&fixture, &shadow, 4, ReceiveMode::ReceiveAndDelete)?;
    assert!(drained.dead_letter.is_some());
    assert_eq!(fixture.usage()?, (0, 0));
    fixture.apply(5, send_kind("two", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 6, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        7,
        &held,
        SettlementDisposition::Abandon,
        BTreeMap::new(),
    )?;
    assert_eq!(fixture.usage()?, (522, 1));
    receive(&fixture, &shadow, 8, ReceiveMode::ReceiveAndDelete)?;
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn expiration_drop_refunds_the_original_reservation<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = Fixture::new(
        provider,
        522,
        QueueConfig {
            dead_lettering_on_message_expiration: false,
            ..QueueConfig::default()
        },
    )?;
    fixture.apply(1, send_kind("one", &[1, 2], Some(2)))?;
    fixture.apply(3, CommandKind::ExpireMessages)?;
    assert_eq!(fixture.usage()?, (0, 0));
    assert_eq!(
        fixture.machine.ready_sequences(
            &fixture.namespace,
            &fixture.entity.dead_letter_queue()?,
            10
        )?,
        Vec::new()
    );
    Ok(())
}

fn schedules_reserve_immediately_and_rekey_without_double_charging<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    let scheduled = ScheduledMessage {
        message_id: "one".into(),
        body: vec![1, 2],
        enqueue_at: Timestamp::from_millis(10),
        time_to_live_millis: None,
        session_id: None,
    };
    let CommandOutcome::Scheduled { sequences } = fixture.apply(
        1,
        CommandKind::Schedule {
            messages: vec![scheduled.clone()],
        },
    )?
    else {
        panic!("schedule")
    };
    assert_eq!(fixture.usage()?, (522, 1));
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(2, send_kind("two", &[], None)),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    fixture.apply(10, CommandKind::ActivateScheduled)?;
    assert_eq!(fixture.usage()?, (522, 1));
    assert_eq!(
        fixture.machine.store().get(&keys::message_charge(
            &fixture.namespace,
            &fixture.entity,
            sequences[0]
        ))?,
        None
    );
    let delivery = receive(&fixture, &fixture.entity, 11, ReceiveMode::ReceiveAndDelete)?;
    assert_eq!(delivery.sequence, SequenceNumber::new(2));
    assert_eq!(fixture.usage()?, (0, 0));
    let CommandOutcome::Scheduled { sequences } = fixture.apply(
        12,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                enqueue_at: Timestamp::from_millis(100),
                ..scheduled
            }],
        },
    )?
    else {
        panic!("schedule")
    };
    assert_eq!(
        fixture.apply(
            13,
            CommandKind::CancelScheduled {
                sequences: vec![sequences[0], sequences[0]],
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn explicit_dead_letter_growth_is_atomic_and_draining_refunds_original_credit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    let disposition = SettlementDisposition::DeadLetter {
        reason: "custom".into(),
        description: "x".repeat(500),
    };
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        settle(
            &fixture,
            &fixture.entity,
            3,
            &held,
            disposition.clone(),
            BTreeMap::new()
        ),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.set_limit(3, 2_000)?;
    settle(
        &fixture,
        &fixture.entity,
        4,
        &held,
        disposition,
        BTreeMap::new(),
    )?;
    // P=10, original S=0, D=81+6+500; the projected DLQ properties replace the prepaid 256.
    assert_eq!(fixture.usage()?, (853, 1));
    let shadow = fixture.entity.dead_letter_queue()?;
    let fixture = fixture.restart()?;
    let held = receive(&fixture, &shadow, 5, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &shadow,
        6,
        &held,
        SettlementDisposition::Defer,
        BTreeMap::new(),
    )?;
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        &shadow,
        7,
        CommandKind::ReceiveDeferred {
            sequences: vec![held.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
    )?
    else {
        panic!("deferred DLQ delivery")
    };
    settle(
        &fixture,
        &shadow,
        8,
        &deliveries[0],
        SettlementDisposition::Complete,
        BTreeMap::new(),
    )?;
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn locked_and_deferred_expiration_preserve_or_refund_the_same_charge<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(
        provider,
        522,
        QueueConfig {
            lock_duration_millis: 30_000,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    fixture.apply(1, send_kind("one", &[1, 2], Some(2)))?;
    receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    fixture.apply(30_002, CommandKind::ExpireLocks)?;
    assert_eq!(fixture.usage()?, (522, 1));
    let shadow = fixture.entity.dead_letter_queue()?;
    receive(&fixture, &shadow, 30_003, ReceiveMode::ReceiveAndDelete)?;
    fixture.apply(30_004, send_kind("two", &[1, 2], Some(2)))?;
    let held = receive(&fixture, &fixture.entity, 30_005, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        30_005,
        &held,
        SettlementDisposition::Defer,
        BTreeMap::new(),
    )?;
    assert_eq!(
        fixture.apply(
            30_006,
            CommandKind::ReceiveDeferred {
                sequences: vec![held.sequence],
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session_id: None,
            }
        )?,
        CommandOutcome::DeferredReceived(Vec::new())
    );
    assert_eq!(fixture.usage()?, (522, 1));
    receive(&fixture, &shadow, 30_007, ReceiveMode::ReceiveAndDelete)?;
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn batch_capacity_failure_and_late_content_refusal_leave_every_row_unchanged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 1_000, QueueConfig::default())?;
    let input = |id: &str| IngressEnvelope {
        message_id: id.into(),
        body: vec![1, 2],
        time_to_live_millis: None,
        session_id: None,
        envelope: MessageEnvelope::default(),
        scheduled_enqueue_time: None,
    };
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(
            1,
            CommandKind::SendBatch {
                messages: vec![input("one"), input("two")]
            }
        ),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
        vec![255],
    ))?;
    let before = fixture.machine.store().snapshot()?;
    let mut invalid = input("two");
    invalid
        .envelope
        .application_properties
        .insert("bad".into(), MessageValue::List(Vec::new()));
    assert!(matches!(
        fixture.apply(
            2,
            CommandKind::SendBatch {
                messages: vec![input("one"), invalid]
            }
        ),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn atomic_actions_use_ordered_credit_and_late_failures_roll_back_the_overlay<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    let complete = CommandKind::Complete {
        sequence: held.sequence,
        lock_token: held.lock.as_ref().unwrap().token,
    };
    let atomic = |millis: u64, kinds: Vec<CommandKind>| -> TestResult<AtomicMessagingCommand> {
        Ok(AtomicMessagingCommand {
            binding: fixture.binding()?,
            issued_at: Timestamp::from_millis(millis),
            commands: kinds
                .into_iter()
                .map(|kind| {
                    Command::new(
                        fixture.namespace.clone(),
                        fixture.entity.clone(),
                        Timestamp::from_millis(millis),
                        kind,
                    )
                })
                .collect(),
        })
    };
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.machine.apply_atomic_messaging(&atomic(
            3,
            vec![send_kind("two", &[1, 2], None), complete.clone()]
        )?),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let wrong = CommandKind::Complete {
        sequence: SequenceNumber::new(99),
        lock_token: LockToken::new(99),
    };
    assert!(
        fixture
            .machine
            .apply_atomic_messaging(&atomic(
                4,
                vec![complete.clone(), send_kind("two", &[1, 2], None), wrong]
            )?)
            .is_err()
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let result = fixture
        .machine
        .apply_atomic_messaging(&atomic(5, vec![complete, send_kind("two", &[1, 2], None)])?)?;
    assert_eq!(result.outcomes.len(), 2);
    assert_eq!(fixture.usage()?, (522, 1));
    assert_eq!(
        receive(&fixture, &fixture.entity, 6, ReceiveMode::ReceiveAndDelete)?.message_id,
        "two"
    );
    assert_eq!(fixture.usage()?, (0, 0));
    Ok(())
}

fn limit_changes_are_fenced_noops_and_never_promote_nonfinite_queues<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 1_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let before = fixture.machine.store().snapshot()?;
    fixture.set_limit(100, 1_000)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert!(matches!(
        fixture
            .set_limit(2, 521)
            .unwrap_err()
            .downcast_ref::<BrokerError>(),
        Some(BrokerError::QueueCapacityFull)
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.set_limit(2, 522)?;
    assert_eq!(fixture.usage()?, (522, 1));
    fixture.apply(
        3,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    fixture.create(4, 522, QueueConfig::default())?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture
            .machine
            .apply_queue_capacity(&QueueCapacityCommandV1::SetLimitFenced {
                binding,
                issued_at: Timestamp::UNIX_EPOCH,
                limit: FiniteQueueCapacity::new(1000)?,
            }),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let ordinary = EntityPath::new("ordinary")?;
    fixture.at(
        &ordinary,
        5,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let view = fixture
        .machine
        .describe_queue_capacity(&fixture.namespace, &ordinary)?
        .unwrap();
    assert_eq!(view.capacity, QueueCapacityStatus::NonFinite);
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture
            .machine
            .apply_queue_capacity(&QueueCapacityCommandV1::SetLimitFenced {
                binding: view.binding,
                issued_at: Timestamp::from_millis(6),
                limit: FiniteQueueCapacity::new(1000)?,
            }),
        Err(BrokerError::QueueCapacityNotSupported)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn deletion_purges_opaque_sidecars_and_recreation_rejects_old_generation_credit<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    let binding = fixture.binding()?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let old_charge = fixture
        .machine
        .store()
        .get(&keys::message_charge(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(1),
        ))?
        .unwrap();
    let shadow = fixture.entity.dead_letter_queue()?;
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(
                keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
                vec![255],
            )
            .put(
                keys::message_charge(&fixture.namespace, &fixture.entity, SequenceNumber::new(1)),
                vec![255],
            )
            .put(
                keys::message_charge(&fixture.namespace, &shadow, SequenceNumber::new(99)),
                vec![255],
            ),
    )?;
    fixture.machine.apply_fenced(&domain::FencedCommand {
        binding,
        command: Command::new(
            fixture.namespace.clone(),
            fixture.entity.clone(),
            Timestamp::from_millis(2),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        ),
    })?;
    assert_eq!(
        fixture.machine.store().get(&keys::queue_capacity_mode(
            &fixture.namespace,
            &fixture.entity
        ))?,
        None
    );
    for entity in [&fixture.entity, &shadow] {
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::queue_capacity_usage(&fixture.namespace, entity), 10)?
                .is_empty()
        );
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::message_charge_prefix(&fixture.namespace, entity), 10)?
                .is_empty()
        );
    }
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .create(3, 522, QueueConfig::default())?
            .binding
            .generation(),
        2
    );
    fixture.apply(4, send_kind("two", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 5, ReceiveMode::PeekLock)?;
    assert_eq!(held.sequence, SequenceNumber::new(2));
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message_charge(&fixture.namespace, &fixture.entity, held.sequence),
        old_charge,
    ))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        settle(
            &fixture,
            &fixture.entity,
            6,
            &held,
            SettlementDisposition::Complete,
            BTreeMap::new()
        ),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn malformed_mode_and_understated_touched_usage_cannot_pass_noop_or_renewal<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    let mode_key = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    let mode = fixture.machine.store().get(&mode_key)?.unwrap();
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(mode_key.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(1, CommandKind::ExpireMessages),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .describe_queue_capacity(&fixture.namespace, &fixture.entity),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(mode_key, mode))?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
        codec::encode(&(1_u8, 1_u64, 1_u8, 0_u64, 0_u64))?,
    ))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(
            3,
            CommandKind::RenewLock {
                sequence: held.sequence,
                lock_token: held.lock.as_ref().unwrap().token,
                lock_duration_millis: None
            }
        ),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn finite_profile_rejects_required_sessions_and_duplicate_detection_without_writes<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    for config in [
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    ] {
        let before = machine.store().snapshot()?;
        assert_eq!(
            machine.apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
                namespace: namespace.clone(),
                entity: entity.clone(),
                issued_at: Timestamp::from_millis(1),
                config,
                limit: FiniteQueueCapacity::new(1000)?
            }),
            Err(BrokerError::QueueCapacityNotSupported)
        );
        assert_eq!(machine.store().snapshot()?, before);
    }
    Ok(())
}

fn empty_finite_queue_deletion_needs_no_allocated_counters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 522, QueueConfig::default())?;
    let binding = fixture.binding()?;
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?,
        None
    );
    fixture.machine.apply_fenced(&domain::FencedCommand {
        binding,
        command: Command::new(
            fixture.namespace.clone(),
            fixture.entity.clone(),
            Timestamp::from_millis(1),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        ),
    })?;
    assert_eq!(
        fixture.machine.store().get(&keys::queue_capacity_usage(
            &fixture.namespace,
            &fixture.entity
        ))?,
        None
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .create(2, 522, QueueConfig::default())?
            .binding
            .generation(),
        2
    );
    assert_eq!(fixture.usage()?, (0, 0));
    fixture.apply(3, send_kind("one", &[1, 2], None))?;
    assert_eq!(
        receive(&fixture, &fixture.entity, 4, ReceiveMode::ReceiveAndDelete)?.sequence,
        SequenceNumber::new(1)
    );
    Ok(())
}

fn oversized_finite_schedule_is_refused_before_work_and_nonfinite_keeps_legacy_behavior<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 1_000_000, QueueConfig::default())?;
    let messages = (0..1_025)
        .map(|i| ScheduledMessage {
            message_id: format!("m{i}"),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            enqueue_at: Timestamp::from_millis(100),
        })
        .collect::<Vec<_>>();
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(
            1,
            CommandKind::Schedule {
                messages: messages.clone()
            }
        ),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let ordinary = EntityPath::new("ordinary")?;
    fixture.at(
        &ordinary,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let CommandOutcome::Scheduled { sequences } =
        fixture.at(&ordinary, 2, CommandKind::Schedule { messages })?
    else {
        panic!("legacy schedule")
    };
    assert_eq!(sequences.len(), 1_025);
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&keys::queue_capacity_usage(&fixture.namespace, &ordinary))?,
        None
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::message_charge_prefix(&fixture.namespace, &ordinary),
                1
            )?
            .is_empty()
    );
    Ok(())
}

fn ordinary_queue_operations_add_no_scans_and_keep_one_final_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observation = Arc::new(Mutex::new(Observation::default()));
    let fixture = Fixture::new(
        ObservedProvider {
            inner: provider,
            observation: observation.clone(),
            fail_next: Arc::new(AtomicBool::new(false)),
        },
        1_000,
        QueueConfig::default(),
    )?;
    let ordinary = EntityPath::new("ordinary")?;
    fixture.at(
        &ordinary,
        0,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    for (millis, kind) in [
        (1, send_kind("one", &[1, 2], None)),
        (
            2,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        ),
        (3, CommandKind::ExpireMessages),
    ] {
        *observation.lock().unwrap() = Observation::default();
        fixture.at(&ordinary, millis, kind.clone())?;
        let legacy = observation.lock().unwrap().clone();
        *observation.lock().unwrap() = Observation::default();
        fixture.apply(millis, kind)?;
        let finite = observation.lock().unwrap().clone();
        assert_eq!(
            finite.scans, legacy.scans,
            "finite accounting adds only point reads"
        );
        assert_eq!(finite.commits, legacy.commits);
        assert_eq!(finite.commits, usize::from(millis != 3));
        assert!(finite.reads.len() <= 40);
        let capacity_reads = finite
            .reads
            .iter()
            .filter(|key| matches!(key.first(), Some(0x16..=0x18)))
            .count();
        assert!(capacity_reads <= 10);
        if millis != 3 {
            assert_eq!(
                finite
                    .mutations
                    .iter()
                    .filter(|mutation| match mutation {
                        Mutation::Put { key, .. } | Mutation::Delete { key } =>
                            key == &keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
                    })
                    .count(),
                1
            );
        } else {
            assert!(finite.mutations.is_empty());
        }
    }
    Ok(())
}

fn injected_commit_refusal_keeps_business_and_capacity_rows_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observation = Arc::new(Mutex::new(Observation::default()));
    let fail_next = Arc::new(AtomicBool::new(false));
    let fixture = Fixture::new(
        ObservedProvider {
            inner: provider,
            observation: observation.clone(),
            fail_next: fail_next.clone(),
        },
        522,
        QueueConfig::default(),
    )?;
    let before = fixture.machine.store().snapshot()?;
    *observation.lock().unwrap() = Observation {
        before_commit: Some(before.clone()),
        ..Observation::default()
    };
    fail_next.store(true, Ordering::Relaxed);
    assert!(matches!(
        fixture.apply(1, send_kind("one", &[1, 2], None)),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    let observed = observation.lock().unwrap().clone();
    assert_eq!(observed.commits, 1);
    assert!(observed.mutations.iter().any(|mutation| matches!(mutation, Mutation::Put { key, .. } if key == &keys::queue_capacity_usage(&fixture.namespace, &fixture.entity))));
    assert!(observed.mutations.iter().any(|mutation| matches!(mutation, Mutation::Put { key, .. } if key == &keys::message(&fixture.namespace, &fixture.entity, SequenceNumber::new(1)))));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observation.lock().unwrap() = Observation::default();
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let held = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    let before = fixture.machine.store().snapshot()?;
    *observation.lock().unwrap() = Observation {
        before_commit: Some(before.clone()),
        ..Observation::default()
    };
    fail_next.store(true, Ordering::Relaxed);
    assert!(matches!(
        settle(
            &fixture,
            &fixture.entity,
            3,
            &held,
            SettlementDisposition::Complete,
            BTreeMap::new()
        ),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    assert_eq!(observation.lock().unwrap().commits, 1);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn idle_absent_session_sweeps_preserve_noop_and_refuse_orphan_capacity<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 1_024, QueueConfig::default())?;
    let missing = EntityPath::new("missing")?;
    for target in [&missing, &fixture.entity] {
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(target, 1, CommandKind::ExpireSessionLocks)?,
            CommandOutcome::SessionLocksExpired { released: 0 }
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    fixture.apply(
        2,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    let fixture = fixture.restart()?;
    for owner in [&missing, &fixture.entity] {
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(owner, 3, CommandKind::ExpireSessionLocks)?,
            CommandOutcome::SessionLocksExpired { released: 0 }
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let shadow = owner.dead_letter_queue()?;
        for target in [owner, &shadow] {
            for key in [
                keys::queue_config(&fixture.namespace, target),
                keys::topic_config(&fixture.namespace, target),
                keys::queue_capacity_mode(&fixture.namespace, target),
                keys::queue_capacity_usage(&fixture.namespace, target),
            ] {
                fixture
                    .machine
                    .store()
                    .apply(WriteBatch::default().put(key.clone(), vec![255]))?;
                let damaged = fixture.machine.store().snapshot()?;
                assert_eq!(
                    fixture.at(owner, 3, CommandKind::ExpireSessionLocks),
                    Err(BrokerError::QueueCapacityCorrupt)
                );
                assert_eq!(fixture.machine.store().snapshot()?, damaged);
                fixture
                    .machine
                    .store()
                    .apply(WriteBatch::default().delete(key))?;
                assert_eq!(fixture.machine.store().snapshot()?, before);
            }
        }
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    create_and_send_have_an_independent_complete_record_image,
    locks_renewal_deferral_and_abandon_retain_credit_until_completion,
    complete_refunds_original_charge_despite_unretained_property_growth,
    retained_property_growth_is_atomic_and_a_later_shrink_returns_credit,
    automatic_dead_lettering_at_full_capacity_preserves_shared_credit,
    expiration_drop_refunds_the_original_reservation,
    schedules_reserve_immediately_and_rekey_without_double_charging,
    explicit_dead_letter_growth_is_atomic_and_draining_refunds_original_credit,
    locked_and_deferred_expiration_preserve_or_refund_the_same_charge,
    batch_capacity_failure_and_late_content_refusal_leave_every_row_unchanged,
    atomic_actions_use_ordered_credit_and_late_failures_roll_back_the_overlay,
    limit_changes_are_fenced_noops_and_never_promote_nonfinite_queues,
    deletion_purges_opaque_sidecars_and_recreation_rejects_old_generation_credit,
    malformed_mode_and_understated_touched_usage_cannot_pass_noop_or_renewal,
    finite_profile_rejects_required_sessions_and_duplicate_detection_without_writes,
    oversized_finite_schedule_is_refused_before_work_and_nonfinite_keeps_legacy_behavior,
    ordinary_queue_operations_add_no_scans_and_keep_one_final_commit,
    injected_commit_refusal_keeps_business_and_capacity_rows_atomic,
    empty_finite_queue_deletion_needs_no_allocated_counters,
    idle_absent_session_sweeps_preserve_noop_and_refuse_orphan_capacity,
}
