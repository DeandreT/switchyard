use super::*;

fn failed_commit<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    command: &Command,
) -> TestResult<StoreSnapshot> {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(fixture);
    fixture
        .machine
        .store()
        .fail_next
        .store(true, Ordering::Relaxed);
    assert_eq!(
        fixture.machine.apply(command),
        Err(BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected incarnation failure".into(),
        }))
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        1
    );
    Ok(before)
}

fn create_delete_recreate_and_update_failures_never_publish_partial_generations<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider)?;
    for (index, kind) in [
        EntityIncarnationKind::Queue,
        EntityIncarnationKind::Topic,
        EntityIncarnationKind::Subscription,
    ]
    .into_iter()
    .enumerate()
    {
        let base = index as u64 * 10 + 1;
        let primary = EntityPath::new(format!("created{index}"))?;
        let name = SubscriptionName::new("Alpha")?;
        let (owner, create, delete, update) = match kind {
            EntityIncarnationKind::Queue => (
                primary.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue,
                },
                CommandKind::UpdateQueue {
                    update: QueueConfigUpdate {
                        max_delivery_count: Some(2),
                        ..QueueConfigUpdate::default()
                    },
                },
            ),
            EntityIncarnationKind::Topic => (
                primary.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Topic,
                },
                CommandKind::UpdateTopic {
                    update: TopicConfigUpdate {
                        max_message_bytes: Some(8_192),
                        ..TopicConfigUpdate::default()
                    },
                },
            ),
            EntityIncarnationKind::Subscription => {
                create_topic(&fixture, &primary, base)?;
                (
                    primary.subscription(&name)?,
                    CommandKind::CreateSubscription {
                        name: name.clone(),
                        config: SubscriptionConfig::default(),
                    },
                    CommandKind::DeleteEntity {
                        target: DeleteEntityTarget::Subscription { name: name.clone() },
                    },
                    CommandKind::UpdateSubscription {
                        name,
                        update: SubscriptionConfigUpdate {
                            max_delivery_count: Some(2),
                            ..SubscriptionConfigUpdate::default()
                        },
                    },
                )
            }
        };
        let incarnation_key = keys::entity_incarnation(&fixture.namespace, &owner);
        assert_eq!(fixture.machine.store().get(&incarnation_key)?, None);
        let create = Command::new(
            fixture.namespace.clone(),
            primary.clone(),
            Timestamp::from_millis(base),
            create,
        );
        let before = failed_commit(&fixture, &create)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        reset(&fixture);
        fixture.machine.apply(&create)?;
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(1, kind, false)?
        );
        {
            let observations = fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations");
            assert_eq!(observations.commits, 1);
            assert_eq!(
                observations
                    .puts
                    .iter()
                    .filter(|key| **key == incarnation_key)
                    .count(),
                1
            );
        }
        let old = bind(&fixture, &owner, &owner, kind)?;
        let delete = Command::new(
            fixture.namespace.clone(),
            primary.clone(),
            Timestamp::from_millis(base + 1),
            delete,
        );
        let before = failed_commit(&fixture, &delete)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(bind(&fixture, &owner, &owner, kind)?, old);
        fixture.machine.apply(&delete)?;
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(1, kind, true)?
        );
        let recreate = Command {
            issued_at: Timestamp::from_millis(base + 2),
            ..create
        };
        let before = failed_commit(&fixture, &recreate)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(1, kind, true)?
        );
        fixture.machine.apply(&recreate)?;
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(2, kind, false)?
        );
        let fresh = bind(&fixture, &owner, &owner, kind)?;
        let update = Command::new(
            fixture.namespace.clone(),
            primary.clone(),
            Timestamp::from_millis(base + 3),
            update,
        );
        let before = failed_commit(&fixture, &update)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(bind(&fixture, &owner, &owner, kind)?, fresh);
        fixture.machine.apply(&update)?;
        assert_eq!(bind(&fixture, &owner, &owner, kind)?, fresh);
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(2, kind, false)?
        );
    }
    Ok(())
}

fn exhausted_generations_can_retire_but_never_wrap_on_recreation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider)?;
    for (index, kind) in [
        EntityIncarnationKind::Queue,
        EntityIncarnationKind::Topic,
        EntityIncarnationKind::Subscription,
    ]
    .into_iter()
    .enumerate()
    {
        let base = index as u64 * 10 + 1;
        let primary = EntityPath::new(format!("exhausted{index}"))?;
        let name = SubscriptionName::new("Alpha")?;
        let (owner, create, delete) = match kind {
            EntityIncarnationKind::Queue => (
                primary.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue,
                },
            ),
            EntityIncarnationKind::Topic => (
                primary.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Topic,
                },
            ),
            EntityIncarnationKind::Subscription => {
                create_topic(&fixture, &primary, base)?;
                (
                    primary.subscription(&name)?,
                    CommandKind::CreateSubscription {
                        name: name.clone(),
                        config: SubscriptionConfig::default(),
                    },
                    CommandKind::DeleteEntity {
                        target: DeleteEntityTarget::Subscription { name },
                    },
                )
            }
        };
        at(&fixture, &primary, base, create.clone())?;
        let key = keys::entity_incarnation(&fixture.namespace, &owner);
        fixture.machine.store().apply(WriteBatch::default().put(
            key.clone(),
            codec::encode(&EntityIncarnation::new(u64::MAX, kind, false)?)?,
        ))?;
        let old = bind(&fixture, &owner, &owner, kind)?;
        assert_eq!(old.generation(), u64::MAX);
        at(&fixture, &primary, base + 1, delete)?;
        assert_eq!(
            incarnation(&fixture, &owner)?,
            EntityIncarnation::new(u64::MAX, kind, true)?
        );
        let before = fixture.machine.store().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let clock = fixture.machine.last_applied_time()?;
        reset(&fixture);
        assert_eq!(
            at(&fixture, &primary, base + 2, create),
            Err(BrokerError::EntityIncarnationExhausted)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, clock);
        assert_eq!(
            fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations")
                .commits,
            0
        );
        assert_eq!(incarnation(&fixture, &owner)?.generation(), u64::MAX);
        reject_fenced(
            &fixture,
            &fenced(
                &fixture,
                &old,
                &owner,
                0,
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(1),
                    max_messages: 1,
                    session_id: None,
                },
            ),
            BrokerError::EntityBindingStale,
        )?;
    }
    Ok(())
}

fn admission_and_fenced_replay_refuse_corrupt_identity_without_repair<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let owner = fixture.entity.clone();
    let key = keys::entity_incarnation(&fixture.namespace, &owner);
    let original = fixture.machine.store().get(&key)?.expect("live identity");
    let binding = bind(&fixture, &owner, &owner, EntityIncarnationKind::Queue)?;
    assert_eq!(
        EntityIncarnation::new(0, EntityIncarnationKind::Queue, false),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(
        EntityBinding::new(
            fixture.namespace.clone(),
            owner.clone(),
            owner.clone(),
            EntityIncarnationKind::Queue,
            0
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        EntityBinding::new(
            fixture.namespace.clone(),
            owner.dead_letter_queue()?,
            owner.clone(),
            EntityIncarnationKind::Topic,
            1
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        EntityBinding::new(
            fixture.namespace.clone(),
            owner.clone(),
            EntityPath::new("neighbor")?,
            EntityIncarnationKind::Queue,
            1
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    for damage in 0..5 {
        let mutation = match damage {
            0 => WriteBatch::default().delete(key.clone()),
            1 => WriteBatch::default().put(key.clone(), vec![0xff]),
            2 => WriteBatch::default().put(
                key.clone(),
                codec::encode(&(0u64, EntityIncarnationKind::Queue, false))?,
            ),
            3 => WriteBatch::default().put(
                key.clone(),
                codec::encode(&EntityIncarnation::new(
                    1,
                    EntityIncarnationKind::Topic,
                    false,
                )?)?,
            ),
            _ => WriteBatch::default().put(
                key.clone(),
                codec::encode(&EntityIncarnation::new(
                    1,
                    EntityIncarnationKind::Queue,
                    true,
                )?)?,
            ),
        };
        fixture.machine.store().apply(mutation)?;
        let before = fixture.machine.store().snapshot()?;
        let clock = fixture.machine.last_applied_time()?;
        reset(&fixture);
        let admitted = fixture.machine.bind_entity(
            &fixture.namespace,
            &owner,
            &owner,
            EntityIncarnationKind::Queue,
        );
        let guarded = fixture.machine.apply_fenced(&fenced(
            &fixture,
            &binding,
            &owner,
            10,
            send("must not appear"),
        ));
        let deleted = fixture.at(
            10,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        );
        if damage == 1 {
            assert!(matches!(admitted, Err(BrokerError::Codec(_))));
            assert!(matches!(guarded, Err(BrokerError::Codec(_))));
            assert!(matches!(deleted, Err(BrokerError::Codec(_))));
        } else {
            assert_eq!(admitted, Err(BrokerError::DanglingEntityMetadata));
            assert_eq!(guarded, Err(BrokerError::DanglingEntityMetadata));
            assert_eq!(deleted, Err(BrokerError::DanglingEntityMetadata));
        }
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, clock);
        assert_eq!(
            fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations")
                .commits,
            0
        );
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().put(key.clone(), original.clone()))?;
        assert_eq!(
            bind(&fixture, &owner, &owner, EntityIncarnationKind::Queue)?,
            binding
        );
    }
    let shadow = owner.dead_letter_queue()?;
    let shadow_key = keys::queue_config(&fixture.namespace, &shadow);
    let shadow_config = fixture
        .machine
        .store()
        .get(&shadow_key)?
        .expect("shadow metadata");
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(shadow_key.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &shadow,
            &owner,
            EntityIncarnationKind::Queue
        ),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(shadow_key, shadow_config))?;
    assert_eq!(
        bind(&fixture, &shadow, &owner, EntityIncarnationKind::Queue)?.generation(),
        1
    );
    Ok(())
}

fn topic_cascade_retires_every_owner_in_one_commit_and_recreates_independent_children<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider)?;
    let topic = EntityPath::new("events")?;
    create_topic(&fixture, &topic, 1)?;
    let parent = bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?;
    let mut children = Vec::new();
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        let name = SubscriptionName::new(format!("member{index:02}"))?;
        let child = subscribe(&fixture, &topic, &name, 1)?;
        let shadow = child.dead_letter_queue()?;
        children.push((
            name,
            bind(
                &fixture,
                &child,
                &child,
                EntityIncarnationKind::Subscription,
            )?,
            bind(
                &fixture,
                &shadow,
                &child,
                EntityIncarnationKind::Subscription,
            )?,
        ));
    }
    let delete = Command::new(
        fixture.namespace.clone(),
        topic.clone(),
        Timestamp::from_millis(2),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
    );
    let before = failed_commit(&fixture, &delete)?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?,
        parent
    );
    for (_, child, shadow) in &children {
        assert_eq!(
            bind(&fixture, child.target(), child.owner(), child.kind())?,
            *child
        );
        assert_eq!(
            bind(&fixture, shadow.target(), shadow.owner(), shadow.kind())?,
            *shadow
        );
    }
    reset(&fixture);
    let application = fixture.machine.apply_with_effects(&delete)?;
    assert_eq!(application.outcome, CommandOutcome::TopicDeleted);
    let mut paths = vec![topic.clone()];
    for (_, child, shadow) in &children {
        paths.extend([child.target().clone(), shadow.target().clone()]);
    }
    paths.sort();
    assert_eq!(application.entity_deletions, Some(paths));
    let owner_keys = std::iter::once(&parent)
        .chain(children.iter().map(|(_, child, _)| child))
        .map(|binding| keys::entity_incarnation(&fixture.namespace, binding.owner()))
        .collect::<Vec<_>>();
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(observations.commits, 1);
        assert_eq!(observations.puts.len(), owner_keys.len() + 1);
        for key in &owner_keys {
            assert_eq!(
                observations
                    .puts
                    .iter()
                    .filter(|actual| *actual == key)
                    .count(),
                1
            );
        }
        assert_eq!(
            observations
                .puts
                .iter()
                .filter(|key| **key == keys::clock())
                .count(),
            1
        );
    }
    assert!(incarnation(&fixture, &topic)?.is_retired());
    for (_, child, _) in &children {
        assert_eq!(
            incarnation(&fixture, child.owner())?,
            EntityIncarnation::new(1, EntityIncarnationKind::Subscription, true)?
        );
    }
    create_topic(&fixture, &topic, 3)?;
    assert_eq!(
        bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?.generation(),
        2
    );
    for (name, child, shadow) in children {
        subscribe(&fixture, &topic, &name, 3)?;
        assert_eq!(
            bind(&fixture, child.target(), child.owner(), child.kind())?.generation(),
            2
        );
        assert_eq!(
            bind(&fixture, shadow.target(), shadow.owner(), shadow.kind())?.generation(),
            2
        );
        assert_eq!(
            fixture
                .machine
                .rules_fenced(&child, &fixture.namespace, &topic, &name),
            Err(BrokerError::EntityBindingStale)
        );
        reject_fenced(
            &fixture,
            &fenced(
                &fixture,
                &shadow,
                shadow.target(),
                0,
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(1),
                    max_messages: 1,
                    session_id: None,
                },
            ),
            BrokerError::EntityBindingStale,
        )?;
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    create_delete_recreate_and_update_failures_never_publish_partial_generations,
    exhausted_generations_can_retire_but_never_wrap_on_recreation,
    admission_and_fenced_replay_refuse_corrupt_identity_without_repair,
    topic_cascade_retires_every_owner_in_one_commit_and_recreates_independent_children,
}
