use super::*;
use domain::{QueueConfigError, QueueImmutableProperty, SubscriptionName, TopicConfig};

#[derive(Clone, Debug, Default)]
struct DefinitionObservation {
    armed: bool,
    applied: bool,
    forbid_clock: bool,
    fail_before_apply: bool,
    reads: Vec<Key>,
    topic_mode_probe: Option<Key>,
    mode_probes: Vec<(Key, Key, usize, usize)>,
    commits: usize,
    mutations: Vec<Mutation>,
    before: Option<StoreSnapshot>,
}

#[derive(Clone, Debug)]
struct DefinitionStore<S> {
    inner: S,
    observation: Arc<Mutex<DefinitionObservation>>,
}

impl<S: StateStore> StateStore for DefinitionStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let mut observed = self.observation.lock().unwrap();
        if observed.armed {
            assert!(!observed.applied, "definition must not reread after apply");
            assert!(!observed.forbid_clock || key != keys::clock().as_slice());
            observed.reads.push(key.to_vec());
        }
        drop(observed);
        self.inner.get(key)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let armed = {
            let observed = self.observation.lock().unwrap();
            if observed.armed {
                assert!(!observed.applied, "definition must not scan after apply");
                assert_eq!(observed.topic_mode_probe.as_deref(), Some(prefix));
                assert_eq!(start, prefix);
                assert_eq!(limit, 1);
            }
            observed.armed
        };
        let rows = self.inner.scan_from(prefix, start, limit)?;
        if armed {
            assert!(
                rows.is_empty(),
                "healthy definition has no descendant TopicMode"
            );
            let mut observed = self.observation.lock().unwrap();
            assert!(observed.armed && !observed.applied);
            observed
                .mode_probes
                .push((prefix.to_vec(), start.to_vec(), limit, rows.len()));
        }
        Ok(rows)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        let observed = self.observation.lock().unwrap();
        assert!(
            !observed.armed || !observed.applied,
            "definition must not snapshot after apply"
        );
        drop(observed);
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let (before, fail) = {
            let mut observed = self.observation.lock().unwrap();
            if observed.armed {
                assert!(!observed.applied, "definition has only one apply boundary");
                observed.applied = true;
                observed.commits += 1;
                observed.mutations = batch.mutations().to_vec();
                (observed.before.clone(), observed.fail_before_apply)
            } else {
                (None, false)
            }
        };
        if let Some(before) = before {
            assert_eq!(self.inner.snapshot()?, before);
        }
        if fail {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected definition refusal before physical apply".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct DefinitionProvider<P> {
    inner: P,
    observation: Arc<Mutex<DefinitionObservation>>,
}

impl<P: StoreProvider> StoreProvider for DefinitionProvider<P> {
    type Store = DefinitionStore<P::Store>;

    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(DefinitionStore {
            inner: self.inner.open()?,
            observation: self.observation.clone(),
        })
    }
}

type ObservedFixture<P> = (
    Fixture<DefinitionProvider<P>>,
    Arc<Mutex<DefinitionObservation>>,
);

fn observed_fixture<P: StoreProvider>(
    provider: P,
    limit: u64,
    config: QueueConfig,
) -> TestResult<ObservedFixture<P>> {
    let observation = Arc::new(Mutex::new(DefinitionObservation::default()));
    let fixture = Fixture::new(
        DefinitionProvider {
            inner: provider,
            observation: observation.clone(),
        },
        limit,
        config,
    )?;
    observation.lock().unwrap().topic_mode_probe = Some(keys::subscription_topic_mode_prefix(
        &fixture.namespace,
        &fixture.entity,
    ));
    Ok((fixture, observation))
}

fn arm(
    observation: &Arc<Mutex<DefinitionObservation>>,
    before: StoreSnapshot,
    forbid_clock: bool,
    fail_before_apply: bool,
) {
    let topic_mode_probe = observation.lock().unwrap().topic_mode_probe.clone();
    *observation.lock().unwrap() = DefinitionObservation {
        armed: true,
        forbid_clock,
        fail_before_apply,
        before: Some(before),
        topic_mode_probe,
        ..DefinitionObservation::default()
    };
}

fn disarm(observation: &Arc<Mutex<DefinitionObservation>>) -> DefinitionObservation {
    let mut observed = observation.lock().unwrap();
    let result = observed.clone();
    observed.armed = false;
    observed.applied = false;
    result
}

fn assert_mode_probes(observed: &DefinitionObservation, count: usize) {
    let prefix = observed
        .topic_mode_probe
        .as_ref()
        .expect("fixed owner scope");
    assert_eq!(observed.mode_probes.len(), count);
    assert!(
        observed
            .mode_probes
            .iter()
            .all(|(query, start, limit, returned)| query == prefix
                && start == query
                && *limit == 1
                && *returned == 0)
    );
}

fn instruction(
    binding: EntityBinding,
    millis: u64,
    config: QueueConfig,
    limit: u64,
) -> Result<QueueCapacityCommandV1, BrokerError> {
    Ok(QueueCapacityCommandV1::SetDefinitionFenced {
        binding,
        issued_at: Timestamp::from_millis(millis),
        config,
        limit: FiniteQueueCapacity::new(limit)?,
    })
}

fn define<P: StoreProvider>(
    fixture: &Fixture<P>,
    binding: &EntityBinding,
    millis: u64,
    config: QueueConfig,
    limit: u64,
) -> Result<QueueCapacityView, BrokerError> {
    fixture
        .machine
        .apply_queue_capacity(&instruction(binding.clone(), millis, config, limit)?)
}

fn mutations(
    binding: &EntityBinding,
    millis: u64,
    config: Option<QueueConfig>,
    limit: Option<u64>,
) -> TestResult<Vec<Mutation>> {
    let mut expected = Vec::new();
    if let Some(config) = config {
        expected.push(Mutation::Put {
            key: keys::queue_config(binding.namespace(), binding.owner()),
            value: codec::encode(&config)?,
        });
        expected.push(Mutation::Put {
            key: keys::queue_config(binding.namespace(), &binding.owner().dead_letter_queue()?),
            value: codec::encode(&config.dead_letter_shadow())?,
        });
    }
    if let Some(limit) = limit {
        expected.push(Mutation::Put {
            key: keys::queue_capacity_mode(binding.namespace(), binding.owner()),
            value: codec::encode(&(1_u8, binding.generation(), 1_u8, limit))?,
        });
    }
    if !expected.is_empty() {
        expected.push(Mutation::Put {
            key: keys::clock(),
            value: codec::encode(&Timestamp::from_millis(millis))?,
        });
    }
    Ok(expected)
}

fn expected_snapshot(before: &StoreSnapshot, mutations: &[Mutation]) -> Vec<(Key, Value)> {
    let mut expected = before.entries().iter().cloned().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => {
                expected.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                expected.remove(key);
            }
        }
    }
    expected.into_iter().collect()
}

fn whole_definition_is_one_batch_and_returns_the_prepared_view<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let original = QueueConfig {
        default_time_to_live_millis: Some(10_000),
        ..QueueConfig::default()
    };
    let (fixture, observation) = observed_fixture(provider, 2_000, original)?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let before = fixture.machine.store().snapshot()?;
    let usage = fixture.usage()?;
    let desired = QueueConfig {
        lock_duration_millis: 25,
        max_delivery_count: 3,
        default_time_to_live_millis: None,
        max_message_bytes: 800,
        requires_session: false,
        requires_duplicate_detection: false,
        duplicate_detection_history_time_window_millis: 20_000,
        dead_lettering_on_message_expiration: true,
    };
    arm(&observation, before.clone(), false, false);
    let result = define(&fixture, &binding, 2, desired, 3_000)?;
    let observed = disarm(&observation);
    assert_mode_probes(&observed, 3);
    let expected = mutations(&binding, 2, Some(desired), Some(3_000))?;
    assert_eq!(observed.commits, 1);
    assert_eq!(observed.mutations, expected);
    assert!(observed.reads.contains(&keys::clock()));
    assert_eq!(result.binding, binding);
    assert_eq!(result.config, desired);
    assert_eq!(
        result.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(3_000)?,
            reserved_bytes: usage.0,
            message_count: usage.1,
        }
    );
    assert_eq!(
        fixture.machine.store().snapshot()?.entries(),
        &expected_snapshot(&before, &expected)
    );
    let snapshot = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(
        fixture
            .machine
            .describe_queue_capacity(&fixture.namespace, &fixture.entity)?,
        Some(result)
    );
    Ok(())
}

fn config_only_and_limit_only_do_not_rewrite_usage_or_other_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, observation) = observed_fixture(provider, 2_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let desired = QueueConfig {
        lock_duration_millis: 100,
        ..QueueConfig::default()
    };
    for (millis, limit, config_change, limit_change) in [
        (2, 2_000, Some(desired), None),
        (3, 3_000, None, Some(3_000)),
    ] {
        let before = fixture.machine.store().snapshot()?;
        arm(&observation, before.clone(), false, false);
        define(&fixture, &binding, millis, desired, limit)?;
        let observed = disarm(&observation);
        assert_mode_probes(&observed, 3);
        let expected = mutations(&binding, millis, config_change, limit_change)?;
        assert_eq!(observed.commits, 1);
        assert_eq!(observed.mutations, expected);
        assert_eq!(
            fixture.machine.store().snapshot()?.entries(),
            &expected_snapshot(&before, &expected)
        );
    }
    assert_eq!(fixture.usage()?, (522, 1));
    Ok(())
}

fn unchanged_definition_keeps_every_row_and_still_checks_clock_regression<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, observation) = observed_fixture(provider, 2_000, QueueConfig::default())?;
    fixture.apply(10, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let before = fixture.machine.store().snapshot()?;
    for millis in [10, 20] {
        arm(&observation, before.clone(), false, false);
        let result = define(&fixture, &binding, millis, QueueConfig::default(), 2_000)?;
        let observed = disarm(&observation);
        assert_mode_probes(&observed, 3);
        assert_eq!(observed.commits, 0);
        assert!(observed.mutations.is_empty());
        assert!(observed.reads.contains(&keys::clock()));
        assert_eq!(result.config, QueueConfig::default());
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    assert_eq!(
        define(&fixture, &binding, 9, QueueConfig::default(), 2_000),
        Err(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn desired_validation_and_immutable_priority_precede_limit_refusal<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 2_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let before = fixture.machine.store().snapshot()?;
    for (config, error) in [
        (
            QueueConfig {
                requires_session: true,
                requires_duplicate_detection: true,
                max_delivery_count: 0,
                ..QueueConfig::default()
            },
            BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresSession,
            },
        ),
        (
            QueueConfig {
                requires_duplicate_detection: true,
                max_delivery_count: 0,
                ..QueueConfig::default()
            },
            BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresDuplicateDetection,
            },
        ),
        (
            QueueConfig {
                max_delivery_count: 0,
                ..QueueConfig::default()
            },
            BrokerError::QueueConfig(QueueConfigError::MaxDeliveryCountTooSmall),
        ),
        (
            QueueConfig {
                default_time_to_live_millis: Some(0),
                ..QueueConfig::default()
            },
            BrokerError::QueueConfig(QueueConfigError::TimeToLiveTooShort),
        ),
    ] {
        assert_eq!(define(&fixture, &binding, 2, config, 1), Err(error));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn below_usage_rolls_back_the_complete_desired_definition<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 2_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let desired = QueueConfig {
        lock_duration_millis: 100,
        max_message_bytes: 1,
        ..QueueConfig::default()
    };
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        define(&fixture, &binding, 2, desired, 521),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let result = define(&fixture, &binding, 2, desired, 522)?;
    assert_eq!(result.config, desired);
    assert_eq!(
        result.capacity,
        QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(522)?,
            reserved_bytes: 522,
            message_count: 1,
        }
    );
    Ok(())
}

fn corrupt_profiles_are_not_repaired_by_definition_updates<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 2_000, QueueConfig::default())?;
    let binding = fixture.binding()?;
    let shadow = fixture.entity.dead_letter_queue()?;
    let damage = [
        (
            keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
            Some(vec![255]),
        ),
        (
            keys::queue_capacity_usage(&fixture.namespace, &fixture.entity),
            Some(codec::encode(&(1_u8, 1_u64, 1_u8, 1_u64, 0_u64))?),
        ),
        (
            keys::queue_capacity_mode(&fixture.namespace, &fixture.entity),
            None,
        ),
        (
            keys::queue_config(&fixture.namespace, &shadow),
            Some(codec::encode(&QueueConfig::default())?),
        ),
        (
            keys::queue_capacity_usage(&fixture.namespace, &shadow),
            Some(codec::encode(&(1_u8, 1_u64, 1_u8, 0_u64, 0_u64))?),
        ),
    ];
    for (key, value) in damage {
        let original = fixture.machine.store().get(&key)?;
        let batch = match value {
            Some(value) => WriteBatch::default().put(key.clone(), value),
            None => WriteBatch::default().delete(key.clone()),
        };
        fixture.machine.store().apply(batch)?;
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            define(
                &fixture,
                &binding,
                1,
                QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                },
                3_000
            ),
            Err(BrokerError::QueueCapacityCorrupt)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let restore = match original {
            Some(value) => WriteBatch::default().put(key, value),
            None => WriteBatch::default().delete(key),
        };
        fixture.machine.store().apply(restore)?;
    }
    Ok(())
}

fn every_retained_state_and_original_deadline_is_byte_preserved<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(
        provider,
        20_000,
        QueueConfig {
            default_time_to_live_millis: Some(1_000),
            ..QueueConfig::default()
        },
    )?;
    let session = SessionId::new("producer-session")?;
    let send = |id: &str| CommandKind::Send {
        message_id: id.into(),
        body: vec![1, 2, 3],
        time_to_live_millis: None,
        session_id: Some(session.clone()),
    };
    fixture.apply(1, send("locked"))?;
    let locked = receive(&fixture, &fixture.entity, 2, ReceiveMode::PeekLock)?;
    fixture.apply(3, send("deferred"))?;
    let deferred = receive(&fixture, &fixture.entity, 4, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        5,
        &deferred,
        SettlementDisposition::Defer,
        BTreeMap::new(),
    )?;
    fixture.apply(6, send("dead"))?;
    let dead = receive(&fixture, &fixture.entity, 7, ReceiveMode::PeekLock)?;
    settle(
        &fixture,
        &fixture.entity,
        8,
        &dead,
        SettlementDisposition::DeadLetter {
            reason: "manual".into(),
            description: "unchanged".into(),
        },
        BTreeMap::new(),
    )?;
    fixture.apply(9, send("ready"))?;
    let CommandOutcome::Scheduled { sequences } = fixture.apply(
        10,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: "scheduled".into(),
                body: vec![1, 2, 3],
                enqueue_at: Timestamp::from_millis(500),
                time_to_live_millis: Some(2_000),
                session_id: Some(session.clone()),
            }],
        },
    )?
    else {
        panic!("scheduled message")
    };
    let binding = fixture.binding()?;
    let before = fixture.machine.store().snapshot()?;
    let usage = fixture.usage()?;
    let desired = QueueConfig {
        lock_duration_millis: 1,
        max_delivery_count: 1,
        default_time_to_live_millis: Some(1),
        max_message_bytes: 1,
        duplicate_detection_history_time_window_millis: 20_000,
        dead_lettering_on_message_expiration: true,
        ..QueueConfig::default()
    };
    let expected = mutations(&binding, 11, Some(desired), Some(30_000))?;
    define(&fixture, &binding, 11, desired, 30_000)?;
    assert_eq!(
        fixture.machine.store().snapshot()?.entries(),
        &expected_snapshot(&before, &expected)
    );
    assert_eq!(fixture.usage()?, usage);
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, locked.sequence)?
        .unwrap();
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(1_001)));
    assert_eq!(record.session_id, Some(session));
    assert!(matches!(record.state, MessageState::Locked { .. }));
    assert!(matches!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, deferred.sequence)?
            .unwrap()
            .state,
        MessageState::Deferred
    ));
    assert!(
        fixture
            .machine
            .message(
                &fixture.namespace,
                &fixture.entity.dead_letter_queue()?,
                dead.sequence
            )?
            .unwrap()
            .dead_letter
            .is_some()
    );
    assert!(matches!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?
            .unwrap()
            .state,
        MessageState::Scheduled { .. }
    ));
    let after = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, after);
    assert_eq!(fixture.usage()?, usage);
    Ok(())
}

fn smaller_message_limit_only_restricts_future_submissions<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 2_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("old", &[1, 2, 3], None))?;
    let binding = fixture.binding()?;
    define(
        &fixture,
        &binding,
        2,
        QueueConfig {
            max_message_bytes: 1,
            ..QueueConfig::default()
        },
        2_000,
    )?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.apply(3, send_kind("new", &[1, 2], None)),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 2,
            maximum_bytes: 1
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let old = receive(&fixture, &fixture.entity, 3, ReceiveMode::ReceiveAndDelete)?;
    assert_eq!(old.body, vec![1, 2, 3]);
    assert_eq!(fixture.usage()?, (0, 0));
    fixture.apply(4, send_kind("new", &[1], None))?;
    Ok(())
}

fn stale_and_invalid_bindings_are_refused_before_stored_clock_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, observation) = observed_fixture(provider, 2_000, QueueConfig::default())?;
    let binding = fixture.binding()?;
    fixture.apply(
        1,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    fixture.create(2, 2_000, QueueConfig::default())?;
    let before = fixture.machine.store().snapshot()?;
    arm(&observation, before.clone(), true, false);
    assert_eq!(
        define(&fixture, &binding, 0, QueueConfig::default(), 3_000),
        Err(BrokerError::EntityBindingStale)
    );
    let observed = disarm(&observation);
    assert_mode_probes(&observed, 0);
    assert_eq!(observed.commits, 0);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let invalid: EntityBinding = codec::decode(&codec::encode(&(
        &fixture.namespace,
        &fixture.entity,
        &fixture.entity,
        EntityIncarnationKind::Queue,
        0_u64,
    ))?)?;
    arm(&observation, before.clone(), true, false);
    assert_eq!(
        define(&fixture, &invalid, 0, QueueConfig::default(), 3_000),
        Err(BrokerError::InvalidEntityBinding)
    );
    let observed = disarm(&observation);
    assert_mode_probes(&observed, 0);
    assert_eq!(observed.commits, 0);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn only_existing_finite_primary_queue_bindings_are_supported<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider, 2_000, QueueConfig::default())?;
    let binding = fixture.binding()?;
    let ordinary = EntityPath::new("ordinary")?;
    fixture.at(
        &ordinary,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let nonfinite = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &ordinary,
            &ordinary,
            EntityIncarnationKind::Queue,
        )?
        .unwrap();
    let topic = EntityPath::new("topic")?;
    fixture.at(
        &topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let topic_binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &topic,
            &topic,
            EntityIncarnationKind::Topic,
        )?
        .unwrap();
    let child = topic.subscription(&SubscriptionName::new("child")?)?;
    let subscription = EntityBinding::new(
        fixture.namespace.clone(),
        child.clone(),
        child,
        EntityIncarnationKind::Subscription,
        1,
    )?;
    let shadow = EntityBinding::new(
        fixture.namespace.clone(),
        fixture.entity.dead_letter_queue()?,
        fixture.entity.clone(),
        EntityIncarnationKind::Queue,
        binding.generation(),
    )?;
    let before = fixture.machine.store().snapshot()?;
    for unsupported in [nonfinite, topic_binding, subscription, shadow] {
        assert_eq!(
            define(&fixture, &unsupported, 2, QueueConfig::default(), 3_000),
            Err(BrokerError::QueueCapacityNotSupported)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let wrong_kind = EntityBinding::new(
        fixture.namespace.clone(),
        topic.clone(),
        topic,
        EntityIncarnationKind::Queue,
        1,
    )?;
    assert_eq!(
        define(&fixture, &wrong_kind, 0, QueueConfig::default(), 3_000),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn preapply_storage_refusal_preserves_all_rows_reopens_and_retries<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let (fixture, observation) = observed_fixture(provider, 2_000, QueueConfig::default())?;
    fixture.apply(1, send_kind("one", &[1, 2], None))?;
    let binding = fixture.binding()?;
    let desired = QueueConfig {
        lock_duration_millis: 100,
        default_time_to_live_millis: Some(50),
        ..QueueConfig::default()
    };
    let before = fixture.machine.store().snapshot()?;
    let expected = mutations(&binding, 2, Some(desired), Some(3_000))?;
    arm(&observation, before.clone(), false, true);
    assert!(matches!(
        define(&fixture, &binding, 2, desired, 3_000),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    let observed = disarm(&observation);
    assert_mode_probes(&observed, 3);
    assert_eq!(observed.commits, 1);
    assert_eq!(observed.mutations, expected);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    arm(&observation, before.clone(), false, false);
    let result = define(&fixture, &binding, 2, desired, 3_000)?;
    let observed = disarm(&observation);
    assert_mode_probes(&observed, 3);
    assert_eq!(observed.commits, 1);
    assert_eq!(observed.mutations, expected);
    assert_eq!(
        fixture.machine.store().snapshot()?.entries(),
        &expected_snapshot(&before, &expected)
    );
    assert_eq!(result.config, desired);
    let after = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, after);
    Ok(())
}

macro_rules! definition_backends {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

definition_backends! {
    whole_definition_is_one_batch_and_returns_the_prepared_view,
    config_only_and_limit_only_do_not_rewrite_usage_or_other_metadata,
    unchanged_definition_keeps_every_row_and_still_checks_clock_regression,
    desired_validation_and_immutable_priority_precede_limit_refusal,
    below_usage_rolls_back_the_complete_desired_definition,
    corrupt_profiles_are_not_repaired_by_definition_updates,
    every_retained_state_and_original_deadline_is_byte_preserved,
    smaller_message_limit_only_restricts_future_submissions,
    stale_and_invalid_bindings_are_refused_before_stored_clock_reads,
    only_existing_finite_primary_queue_bindings_are_supported,
    preapply_storage_refusal_preserves_all_rows_reopens_and_retries,
}
