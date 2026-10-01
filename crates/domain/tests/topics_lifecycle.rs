//! Durable topic topology, with data-plane entrypoints deliberately unavailable.

use std::error::Error;

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, IdentifierError,
    MAX_ENTITY_PATH_BYTES, MAX_SUBSCRIPTION_NAME_BYTES, MAX_TOPIC_SUBSCRIPTIONS, NamespaceName,
    QueueConfig, QueueConfigError, QueueConfigUpdate, ReceiveMode, SequenceNumber, SessionId,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn topic<P: StoreProvider>(provider: P) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    assert_eq!(
        fixture.at(
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        )?,
        CommandOutcome::TopicCreated
    );
    Ok(fixture)
}

fn at<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    namespace: &str,
    entity: &EntityPath,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandOutcome, BrokerError> {
    fixture.machine.apply(&Command::new(
        NamespaceName::new(namespace)?,
        entity.clone(),
        Timestamp::from_millis(millis),
        kind,
    ))
}

fn create<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    config: SubscriptionConfig,
    millis: u64,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    assert_eq!(
        fixture.at(
            millis,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config
            }
        )?,
        CommandOutcome::SubscriptionCreated
    );
    Ok(fixture.entity.subscription(&name)?)
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    command: Command,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(fixture.machine.apply(&command), Err(expected));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn shadow(config: SubscriptionConfig) -> QueueConfig {
    QueueConfig {
        max_delivery_count: u32::MAX,
        default_time_to_live_millis: None,
        requires_session: false,
        requires_duplicate_detection: false,
        dead_lettering_on_message_expiration: false,
        ..config.to_queue_config()
    }
}

fn typed_topology_and_backing_queues_survive_restart<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = topic(provider)?;
    let config = SubscriptionConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(100),
        max_message_bytes: 4096,
        dead_lettering_on_message_expiration: true,
        ..SubscriptionConfig::default()
    };
    let entity = create(&fixture, "billing", config, 1)?;
    let dlq = entity.dead_letter_queue()?;
    assert_eq!(entity.as_str(), "orders/subscriptions/billing");
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.entity)?,
        Some(TopicConfig::default())
    );
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?,
        None
    );
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?)?,
        None
    );
    assert_eq!(
        fixture.machine.subscription_config(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("billing")?
        )?,
        Some(config)
    );
    for (path, expected) in [(&entity, config.to_queue_config()), (&dlq, shadow(config))] {
        assert_eq!(
            fixture.machine.queue_config(&fixture.namespace, path)?,
            Some(expected)
        );
        assert!(!expected.requires_duplicate_detection);
        assert_eq!(
            fixture
                .machine
                .store()
                .get(&keys::queue_counters(&fixture.namespace, path))?,
            None,
            "creation leaves allocation counters lazy"
        );
        assert_eq!(
            at(
                &fixture,
                "tenant",
                path,
                1,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None
                }
            )?,
            CommandOutcome::Received(None)
        );
    }
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let definitions = fixture
        .machine
        .subscriptions(&fixture.namespace, &fixture.entity)?;
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, SubscriptionName::new("billing")?);
    assert_eq!(definitions[0].entity, entity);
    assert_eq!(definitions[0].config, config);
    Ok(())
}

fn sorted_memberships_are_exactly_scoped_by_parent_and_namespace<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    for name in ["zulu", "Alpha", "alpha"] {
        create(&fixture, name, SubscriptionConfig::default(), 1)?;
    }
    for (namespace, parent) in [("tenant", "orders-extra"), ("tenant-extra", "orders")] {
        let parent = EntityPath::new(parent)?;
        at(
            &fixture,
            namespace,
            &parent,
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        at(
            &fixture,
            namespace,
            &parent,
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("foreign")?,
                config: SubscriptionConfig::default(),
            },
        )?;
    }
    let definitions = fixture
        .machine
        .subscriptions(&fixture.namespace, &fixture.entity)?;
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Alpha", "alpha", "zulu"]
    );
    assert!(definitions.iter().all(|definition| {
        definition
            .entity
            .as_str()
            .starts_with("orders/subscriptions/")
    }));
    let absent = EntityPath::new("absent")?;
    assert_eq!(
        fixture.machine.topic_config(&fixture.namespace, &absent)?,
        None
    );
    assert_eq!(
        fixture.machine.subscription_config(
            &fixture.namespace,
            &absent,
            &SubscriptionName::new("missing")?
        )?,
        None
    );
    assert_eq!(
        fixture.machine.subscriptions(&fixture.namespace, &absent),
        Err(BrokerError::TopicNotFound)
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity)?,
        definitions
    );
    Ok(())
}

fn missing_duplicate_config_and_type_collisions_are_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let missing = EntityPath::new("missing")?;
    let mut command = fixture.command(
        10,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("billing")?,
            config: SubscriptionConfig::default(),
        },
    );
    command.entity = missing;
    reject(&fixture, command, BrokerError::TopicNotFound)?;
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        BrokerError::TopicAlreadyExists,
    )?;
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
        BrokerError::EntityPathAlreadyExists,
    )?;
    let mut command = fixture.command(
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    command.entity = EntityPath::new("anchor")?;
    reject(&fixture, command, BrokerError::EntityPathAlreadyExists)?;
    create(&fixture, "billing", SubscriptionConfig::default(), 1)?;
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("billing")?,
                config: SubscriptionConfig::default(),
            },
        ),
        BrokerError::SubscriptionAlreadyExists,
    )?;
    let mut command = fixture.command(
        10,
        CommandKind::CreateTopic {
            config: TopicConfig {
                max_message_bytes: 0,
                ..TopicConfig::default()
            },
        },
    );
    command.entity = EntityPath::new("invalid")?;
    reject(
        &fixture,
        command,
        BrokerError::TopicConfig(QueueConfigError::MaxMessageBytesTooSmall),
    )?;
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("invalid")?,
                config: SubscriptionConfig {
                    lock_duration_millis: 0,
                    ..SubscriptionConfig::default()
                },
            },
        ),
        BrokerError::SubscriptionConfig(QueueConfigError::LockDurationTooShort),
    )?;
    Ok(())
}

fn canonical_subscription_and_dlq_paths_are_reserved_before_other_validation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    for path in [
        "orders/subscriptions/a",
        "orders/SuBsCrIpTiOnS/",
        "orders/subscriptions/a/nested",
    ] {
        let path = EntityPath::new(path)?;
        assert!(path.is_subscription_path());
        for kind in [
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ] {
            let mut command = fixture.command(10, kind);
            command.entity = path.clone();
            reject(&fixture, command, BrokerError::SubscriptionPathIsReserved)?;
        }
    }
    let mut command = fixture.command(
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    command.entity = fixture.entity.dead_letter_queue()?;
    reject(&fixture, command, BrokerError::DeadLetterQueueIsReserved)?;
    for dlq_collision in [false, true] {
        for topic_collision in [false, true] {
            let name = SubscriptionName::new(format!(
                "collision-{}-{}",
                u8::from(dlq_collision),
                u8::from(topic_collision)
            ))?;
            let path = fixture.entity.subscription(&name)?;
            let occupied = if dlq_collision {
                path.dead_letter_queue()?
            } else {
                path
            };
            let (key, bytes) = if topic_collision {
                (
                    keys::topic_config(&fixture.namespace, &occupied),
                    codec::encode(&TopicConfig::default())?,
                )
            } else {
                (
                    keys::queue_config(&fixture.namespace, &occupied),
                    codec::encode(&QueueConfig::default())?,
                )
            };
            fixture
                .machine
                .store()
                .apply(WriteBatch::default().put(key, bytes))?;
            reject(
                &fixture,
                fixture.command(
                    10,
                    CommandKind::CreateSubscription {
                        name,
                        config: SubscriptionConfig::default(),
                    },
                ),
                BrokerError::EntityPathAlreadyExists,
            )?;
        }
    }
    for topic_collision in [false, true] {
        let parent = EntityPath::new(format!("occupied-shadow-{}", u8::from(topic_collision)))?;
        let shadow = parent.dead_letter_queue()?;
        let (key, bytes) = if topic_collision {
            (
                keys::topic_config(&fixture.namespace, &shadow),
                codec::encode(&TopicConfig::default())?,
            )
        } else {
            (
                keys::queue_config(&fixture.namespace, &shadow),
                codec::encode(&QueueConfig::default())?,
            )
        };
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().put(key, bytes))?;
        let mut command = fixture.command(
            10,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        );
        command.entity = parent;
        reject(&fixture, command, BrokerError::EntityPathAlreadyExists)?;
    }
    Ok(())
}

fn composed_path_and_membership_boundaries_leave_no_partial_topology<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider)?;
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        create(
            &fixture,
            &format!("sub-{index:02}"),
            SubscriptionConfig::default(),
            1,
        )?;
    }
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity)?
            .len(),
        MAX_TOPIC_SUBSCRIPTIONS
    );
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("overflow")?,
                config: SubscriptionConfig::default(),
            },
        ),
        BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS,
        },
    )?;
    let name = SubscriptionName::new("n".repeat(MAX_SUBSCRIPTION_NAME_BYTES))?;
    let parent_bytes = MAX_ENTITY_PATH_BYTES
        - "/subscriptions/".len()
        - name.as_str().len()
        - domain::DEAD_LETTER_QUEUE_SUFFIX.len();
    for extra in [0, 1] {
        fixture.entity = EntityPath::new("p".repeat(parent_bytes + extra))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let command = fixture.command(
            10,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig::default(),
            },
        );
        if extra == 0 {
            fixture.machine.apply(&command)?;
            assert_eq!(
                fixture
                    .entity
                    .subscription(&name)?
                    .dead_letter_queue()?
                    .as_str()
                    .len(),
                MAX_ENTITY_PATH_BYTES
            );
        } else {
            reject(
                &fixture,
                command,
                BrokerError::Identifier(IdentifierError::TooLong {
                    kind: "entity path",
                    maximum: MAX_ENTITY_PATH_BYTES,
                }),
            )?;
        }
    }
    fixture.entity = EntityPath::new("t".repeat(MAX_ENTITY_PATH_BYTES))?;
    fixture.at(
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.entity)?,
        Some(TopicConfig::default())
    );
    reject(
        &fixture,
        fixture.command(
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("child")?,
                config: SubscriptionConfig::default(),
            },
        ),
        BrokerError::Identifier(IdentifierError::TooLong {
            kind: "entity path",
            maximum: MAX_ENTITY_PATH_BYTES,
        }),
    )?;
    Ok(())
}

fn immediate_topic_ingress_preserves_scheduling_and_subscription_refusal_guards<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let subscription = create(&fixture, "billing", SubscriptionConfig::default(), 1)?;
    let kinds = [
        CommandKind::Send {
            message_id: "message".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
        CommandKind::SendEnvelope {
            message_id: "message".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::default(),
        },
        CommandKind::SendBatch { messages: vec![] },
        CommandKind::Schedule { messages: vec![] },
        CommandKind::ScheduleEnvelopes { messages: vec![] },
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        if index < 2 {
            assert_eq!(
                fixture.at(1, kind.clone())?,
                CommandOutcome::Sent {
                    sequence: SequenceNumber::new(index as u64 + 1)
                }
            );
        } else if index == 2 {
            let before = fixture.machine.store().snapshot()?;
            assert_eq!(
                fixture.at(1, kind.clone())?,
                CommandOutcome::BatchSent { sequences: vec![] }
            );
            assert_eq!(fixture.machine.store().snapshot()?, before);
        } else {
            reject(
                &fixture,
                fixture.command(10, kind.clone()),
                BrokerError::TopicDataPlaneNotImplemented,
            )?;
        }
        let mut command = fixture.command(10, kind);
        command.entity = subscription.clone();
        reject(&fixture, command, BrokerError::SubscriptionPathIsReserved)?;
    }
    let mut command = fixture.command(
        10,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate::default(),
        },
    );
    command.entity = subscription.clone();
    reject(&fixture, command, BrokerError::SubscriptionPathIsReserved)?;
    command = fixture.command(
        10,
        CommandKind::Send {
            message_id: "x".repeat(129),
            body: vec![],
            time_to_live_millis: None,
            session_id: None,
        },
    );
    command.entity = EntityPath::new("absent/SUBSCRIPTIONS/child")?;
    reject(&fixture, command, BrokerError::SubscriptionPathIsReserved)?;
    let CommandOutcome::Peeked(deliveries) = at(
        &fixture,
        "tenant",
        &subscription,
        1,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(1),
            max_messages: 2,
            session_id: None,
        },
    )?
    else {
        panic!("subscription copies remain after every refusal")
    };
    assert_eq!(
        deliveries
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    assert!(
        deliveries
            .iter()
            .all(|delivery| delivery.message_id == "message")
    );
    Ok(())
}

fn session_configs_are_valid_topology_and_children_never_inherit_topic_dedup<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider)?;
    fixture.entity = EntityPath::new("sessions")?;
    let topic_config = TopicConfig {
        requires_duplicate_detection: true,
        default_time_to_live_millis: Some(200),
        ..TopicConfig::default()
    };
    fixture.at(
        1,
        CommandKind::CreateTopic {
            config: topic_config,
        },
    )?;
    let config = SubscriptionConfig {
        requires_session: true,
        ..SubscriptionConfig::default()
    };
    let subscription = create(&fixture, "ordered", config, 1)?;
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.entity)?,
        Some(topic_config)
    );
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &subscription)?,
        Some(config.to_queue_config())
    );
    assert!(!config.to_queue_config().requires_duplicate_detection);
    assert_eq!(
        at(
            &fixture,
            "tenant",
            &subscription,
            2,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None
            }
        ),
        Err(BrokerError::SessionRequired)
    );
    let CommandOutcome::SessionAccepted(Some(accepted)) = at(
        &fixture,
        "tenant",
        &subscription,
        2,
        CommandKind::AcceptSession {
            session_id: Some(SessionId::new("cart")?),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("empty session can be held")
    };
    assert_eq!(
        at(
            &fixture,
            "tenant",
            &subscription,
            2,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: Some(accepted.hold())
            }
        )?,
        CommandOutcome::Received(None)
    );
    let dlq = subscription.dead_letter_queue()?;
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &dlq)?,
        Some(shadow(config))
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.machine.subscription_config(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("ordered")?
        )?,
        Some(config)
    );
    Ok(())
}

fn dangling_or_malformed_membership_never_becomes_a_partial_success<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider)?;
    let name = SubscriptionName::new("billing")?;
    for damage in 0..10 {
        fixture.entity = EntityPath::new(format!("corruption-{damage}"))?;
        fixture.at(
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let entity = create(&fixture, name.as_str(), SubscriptionConfig::default(), 1)?;
        let dlq = entity.dead_letter_queue()?;
        let membership = keys::subscription(&fixture.namespace, &fixture.entity, &name);
        let mut batch = WriteBatch::default();
        match damage {
            0 => batch.push_delete(keys::queue_config(&fixture.namespace, &entity)),
            1 => batch.push_delete(keys::queue_config(&fixture.namespace, &dlq)),
            2 => batch.push_delete(keys::topic_config(&fixture.namespace, &fixture.entity)),
            3 => batch.push_delete(membership.clone()),
            4 => batch.push_put(
                keys::queue_config(&fixture.namespace, &entity),
                codec::encode(&QueueConfig {
                    max_delivery_count: 99,
                    ..QueueConfig::default()
                })?,
            ),
            5 => batch.push_put(
                keys::queue_config(&fixture.namespace, &dlq),
                codec::encode(&QueueConfig::default())?,
            ),
            6 => batch.push_put(
                membership.clone(),
                codec::encode(&SubscriptionConfig {
                    max_delivery_count: 0,
                    ..SubscriptionConfig::default()
                })?,
            ),
            7 => batch.push_put(membership.clone(), vec![255]),
            8 => batch.push_put(
                keys::topic_config(&fixture.namespace, &fixture.entity),
                codec::encode(&TopicConfig {
                    max_message_bytes: 0,
                    ..TopicConfig::default()
                })?,
            ),
            9 => batch.push_put(
                keys::topic_config(&fixture.namespace, &fixture.entity),
                vec![255],
            ),
            _ => unreachable!(),
        }
        fixture.machine.store().apply(batch)?;
        let before = fixture.machine.store().snapshot()?;
        let result =
            fixture
                .machine
                .subscription_config(&fixture.namespace, &fixture.entity, &name);
        if matches!(damage, 7 | 9) {
            assert!(matches!(result, Err(BrokerError::Codec(_))));
        } else {
            assert_eq!(result, Err(BrokerError::DanglingSubscriptionMetadata));
        }
        if damage != 3 {
            let result = fixture
                .machine
                .subscriptions(&fixture.namespace, &fixture.entity);
            if matches!(damage, 7 | 9) {
                assert!(matches!(result, Err(BrokerError::Codec(_))));
            } else {
                assert_eq!(result, Err(BrokerError::DanglingSubscriptionMetadata));
            }
        }
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    for malformed in [0, 1, 2] {
        fixture.entity = EntityPath::new(format!("malformed-{malformed}"))?;
        fixture.at(
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        create(&fixture, name.as_str(), SubscriptionConfig::default(), 1)?;
        let mut key = keys::subscription_prefix(&fixture.namespace, &fixture.entity);
        key.extend_from_slice(match malformed {
            0 => b"bad/name\0",
            1 => b"missing-terminator",
            _ => b"valid\0junk",
        });
        fixture.machine.store().apply(
            WriteBatch::default().put(key, codec::encode(&SubscriptionConfig::default())?),
        )?;
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture
                .machine
                .subscriptions(&fixture.namespace, &fixture.entity),
            Err(BrokerError::MalformedIndexKey)
        );
        reject(
            &fixture,
            fixture.command(
                10,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("new")?,
                    config: SubscriptionConfig::default(),
                },
            ),
            BrokerError::MalformedIndexKey,
        )?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    fixture.entity = EntityPath::new("overfull")?;
    fixture.at(
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        create(
            &fixture,
            &format!("sub-{index:02}"),
            SubscriptionConfig::default(),
            1,
        )?;
    }
    let extra = SubscriptionName::new("sub-extra")?;
    let entity = fixture.entity.subscription(&extra)?;
    let dlq = entity.dead_letter_queue()?;
    let config = SubscriptionConfig::default();
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(
                keys::subscription(&fixture.namespace, &fixture.entity, &extra),
                codec::encode(&config)?,
            )
            .put(
                keys::queue_config(&fixture.namespace, &entity),
                codec::encode(&config.to_queue_config())?,
            )
            .put(
                keys::queue_config(&fixture.namespace, &dlq),
                codec::encode(&shadow(config))?,
            ),
    )?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity),
        Err(BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS
        })
    );
    reject(
        &fixture,
        fixture.command(
            10,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("new")?,
                config,
            },
        ),
        BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS,
        },
    )?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity),
        Err(BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS
        })
    );
    Ok(())
}

#[test]
fn serialized_subscription_names_cannot_bypass_segment_validation() -> TestResult {
    for invalid in [
        "",
        "a/b",
        "a\\b",
        "\0forged",
        "$deadletterqueue",
        "two words",
        "\u{e9}",
        ".",
        "..",
        "-x",
        "x_",
    ] {
        assert!(SubscriptionName::new(invalid).is_err());
        let bytes = postcard::to_stdvec(&invalid)?;
        assert!(postcard::from_bytes::<SubscriptionName>(&bytes).is_err());
        let mut command = vec![31];
        command.extend_from_slice(&bytes);
        command.extend_from_slice(&postcard::to_stdvec(&SubscriptionConfig::default())?);
        assert!(postcard::from_bytes::<CommandKind>(&command).is_err());
        let bytes = codec::encode(&invalid)?;
        assert!(codec::decode::<SubscriptionName>(&bytes).is_err());
    }
    let too_long = "x".repeat(MAX_SUBSCRIPTION_NAME_BYTES + 1);
    assert!(postcard::from_bytes::<SubscriptionName>(&postcard::to_stdvec(&too_long)?).is_err());
    let boundary = SubscriptionName::new("x".repeat(MAX_SUBSCRIPTION_NAME_BYTES))?;
    assert_eq!(
        postcard::from_bytes::<SubscriptionName>(&postcard::to_stdvec(&boundary)?)?,
        boundary
    );
    assert_eq!(SubscriptionName::new("a.b-c_d")?.as_str(), "a.b-c_d");
    Ok(())
}

#[test]
fn topology_commands_append_without_changing_existing_wire_shapes() -> TestResult {
    for (kind, expected) in [
        (CommandKind::Schedule { messages: vec![] }, vec![2, 0]),
        (
            CommandKind::ScheduleEnvelopes { messages: vec![] },
            vec![23, 0],
        ),
        (CommandKind::SendBatch { messages: vec![] }, vec![29, 0]),
    ] {
        assert_eq!(postcard::to_stdvec(&kind)?, expected);
        assert_eq!(postcard::from_bytes::<CommandKind>(&expected)?, kind);
    }
    for (kind, discriminant) in [
        (
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
            30,
        ),
        (
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("billing")?,
                config: SubscriptionConfig::default(),
            },
            31,
        ),
    ] {
        let encoded = postcard::to_stdvec(&kind)?;
        assert_eq!(encoded[0], discriminant);
        assert_eq!(postcard::from_bytes::<CommandKind>(&encoded)?, kind);
    }
    Ok(())
}

#[path = "topics_lifecycle/atomic_creation.rs"]
mod atomic_creation;

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    typed_topology_and_backing_queues_survive_restart,
    sorted_memberships_are_exactly_scoped_by_parent_and_namespace,
    missing_duplicate_config_and_type_collisions_are_atomic,
    canonical_subscription_and_dlq_paths_are_reserved_before_other_validation,
    composed_path_and_membership_boundaries_leave_no_partial_topology,
    immediate_topic_ingress_preserves_scheduling_and_subscription_refusal_guards,
    session_configs_are_valid_topology_and_children_never_inherit_topic_dedup,
    dangling_or_malformed_membership_never_becomes_a_partial_success,
}
