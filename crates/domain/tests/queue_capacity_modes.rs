//! Mandatory queue capacity modes and non-finite topic profiles: accepted and
//! rejected combinations, queue record V3 and proposal V7 fences, and
//! identical create/update/reopen behavior on Memory and a reopened Fjall.
//!
//! A finite size is recorded profile state only; no admission charges it.

use std::error::Error;

use domain::{
    AdminCatalogError, BoundCommand, BrokerError, CodecError, Command, CommandKind, CommandOutcome,
    DurableProposal, DurableProposalError, EntityPath, MAX_QUEUE_CAPACITY_BYTES, NamespaceName,
    QUEUE_CAPACITY_GRANULARITY_BYTES, QueueCapacity, QueueConfig, QueueConfigError,
    QueueConfigUpdate, QueueTimeToLiveUpdate, ReceiveMode, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicCapacity, TopicConfig, codec, keys,
    snapshot_validation::{SnapshotCatalogError, validate_catalog, validate_complete_state},
};
use serde::{Deserialize, Serialize};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type Result<T = (), E = Box<dyn Error>> = std::result::Result<T, E>;

const MIB: u64 = QUEUE_CAPACITY_GRANULARITY_BYTES;

/// The pre-policy queue profile, as a legacy generic reader decodes it.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct LegacyQueue {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
}

fn finite(max_size_bytes: u64) -> QueueConfig {
    QueueConfig {
        capacity: QueueCapacity::Finite { max_size_bytes },
        ..QueueConfig::default()
    }
}

/// The exact shadow every queue casts, finite or not.
fn shadow_bytes() -> Vec<u8> {
    codec::queue::encode(&QueueConfig {
        max_delivery_count: u32::MAX,
        ..QueueConfig::default()
    })
    .unwrap()
}

// ---- records ---------------------------------------------------------------

#[test]
fn non_finite_profiles_keep_their_exact_bytes() {
    assert_eq!(QueueConfig::default().capacity, QueueCapacity::NonFinite);
    assert_eq!(TopicConfig::default().capacity, TopicCapacity::NonFinite);
    assert_eq!(
        codec::queue::encode(&QueueConfig::default()).unwrap(),
        [
            1, 0xe0, 0xd4, 0x03, 10, 0, 0x80, 0x80, 0x10, 0, 0, 0xc0, 0xcf, 0x24
        ]
    );
    let disabled = QueueConfig {
        dead_lettering_on_message_expiration: false,
        ..QueueConfig::default()
    };
    let bytes = codec::queue::encode(&disabled).unwrap();
    assert_eq!(bytes[0], codec::queue::VALUE_FORMAT_V2);
    assert_eq!(codec::queue::decode(&bytes).unwrap(), disabled);
    // Topics have no finite form, so topic records are unchanged.
    assert_eq!(
        codec::topic::encode(&TopicConfig::default()).unwrap(),
        codec::encode(&(None::<u64>, 256_usize * 1024)).unwrap()
    );
    assert_eq!(
        SubscriptionConfig::default()
            .validate()
            .map(|_| QueueConfig::default().capacity),
        Ok(QueueCapacity::NonFinite)
    );
}

#[test]
fn finite_profiles_use_a_v3_record_that_older_readers_refuse() {
    let config = finite(MIB);
    let bytes = codec::queue::encode(&config).unwrap();
    assert_eq!(
        bytes,
        [
            3, 0xe0, 0xd4, 0x03, 10, 0, 0x80, 0x80, 0x10, 0, 0, 0xc0, 0xcf, 0x24, 1, 0x80, 0x80,
            0x40
        ]
    );
    assert_eq!(bytes[0], codec::queue::VALUE_FORMAT_V3);
    assert_eq!(codec::queue::decode(&bytes).unwrap(), config);
    // Generic pre-policy readers and the V1/V2-only profile reader both refuse.
    assert_eq!(
        codec::decode::<LegacyQueue>(&bytes),
        Err(CodecError::UnsupportedVersion { version: 3 })
    );
    for cut in 0..bytes.len() {
        assert!(codec::queue::decode(&bytes[..cut]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(codec::queue::decode(&trailing), Err(CodecError::Decode));
    // A nonminimal size varint is not canonical.
    let mut nonminimal = bytes[..bytes.len() - 1].to_vec();
    nonminimal.extend([0xc0, 0x00]);
    assert_eq!(codec::queue::decode(&nonminimal), Err(CodecError::Decode));
    let mut boolean = bytes.clone();
    boolean[14] = 2;
    assert_eq!(codec::queue::decode(&boolean), Err(CodecError::Decode));

    // Every expiry policy and invalid policy is kept verbatim for callers.
    for config in [
        QueueConfig {
            dead_lettering_on_message_expiration: false,
            ..finite(MAX_QUEUE_CAPACITY_BYTES)
        },
        QueueConfig {
            requires_session: true,
            requires_duplicate_detection: true,
            ..finite(0)
        },
        finite(u64::MAX),
    ] {
        let bytes = codec::queue::encode(&config).unwrap();
        assert_eq!(bytes[0], codec::queue::VALUE_FORMAT_V3);
        assert_eq!(codec::queue::decode(&bytes).unwrap(), config);
    }
    assert_eq!(
        codec::queue::decode(&[4, 0]),
        Err(CodecError::UnsupportedVersion { version: 4 })
    );
}

// ---- accepted and rejected combinations -------------------------------------

#[test]
fn validation_specifies_the_accepted_finite_profiles() {
    for config in [
        QueueConfig::default(),
        finite(MIB),
        finite(1024 * MIB),
        finite(MAX_QUEUE_CAPACITY_BYTES),
        QueueConfig {
            default_time_to_live_millis: Some(1),
            dead_lettering_on_message_expiration: false,
            ..finite(2 * MIB)
        },
        // Sessions and duplicate detection remain supported when non-finite.
        QueueConfig {
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    ] {
        assert_eq!(config.validate(), Ok(config));
    }
    let range = QueueConfigError::CapacityOutOfRange {
        granularity_bytes: MIB,
        maximum_bytes: 5 * 1024 * MIB,
    };
    for (config, error) in [
        (finite(0), range),
        (finite(1), range),
        (finite(MIB + 1), range),
        (finite(MAX_QUEUE_CAPACITY_BYTES + MIB), range),
        (finite(u64::MAX), range),
        (
            QueueConfig {
                requires_session: true,
                ..finite(MIB)
            },
            QueueConfigError::FiniteCapacityWithSessions,
        ),
        (
            QueueConfig {
                requires_duplicate_detection: true,
                ..finite(MIB)
            },
            QueueConfigError::FiniteCapacityWithDuplicateDetection,
        ),
        // Existing policy refusals keep their priority.
        (
            QueueConfig {
                lock_duration_millis: 0,
                ..finite(0)
            },
            QueueConfigError::LockDurationTooShort,
        ),
    ] {
        assert_eq!(config.validate(), Err(error));
    }
}

#[test]
fn updates_keep_the_capacity_mode_and_may_resize() {
    let update = |capacity| QueueConfigUpdate {
        capacity: Some(capacity),
        ..QueueConfigUpdate::default()
    };
    let resized = QueueCapacity::Finite {
        max_size_bytes: 2 * MIB,
    };
    assert_eq!(
        update(resized).apply(finite(MIB)),
        Ok(finite(2 * MIB)),
        "a finite size may change"
    );
    assert_eq!(
        QueueConfigUpdate::default().apply(finite(MIB)),
        Ok(finite(MIB))
    );
    assert_eq!(
        update(QueueCapacity::NonFinite).apply(QueueConfig::default()),
        Ok(QueueConfig::default())
    );
    assert_eq!(
        update(resized).apply(QueueConfig::default()),
        Err(QueueConfigError::CapacityModeImmutable)
    );
    assert_eq!(
        update(QueueCapacity::NonFinite).apply(finite(MIB)),
        Err(QueueConfigError::CapacityModeImmutable)
    );
    assert_eq!(
        update(QueueCapacity::Finite { max_size_bytes: 0 }).apply(finite(MIB)),
        Err(QueueConfigError::CapacityOutOfRange {
            granularity_bytes: MIB,
            maximum_bytes: MAX_QUEUE_CAPACITY_BYTES,
        })
    );
}

// ---- durable proposals -------------------------------------------------------

fn command(entity: &str, kind: CommandKind) -> Command {
    Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new(entity).unwrap(),
        Timestamp::from_millis(7),
        kind,
    )
}

fn v7_kinds() -> Vec<CommandKind> {
    vec![
        CommandKind::CreateQueue {
            config: finite(MIB),
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                capacity: Some(QueueCapacity::Finite {
                    max_size_bytes: 2 * MIB,
                }),
                ..QueueConfigUpdate::default()
            },
        },
        // Every field populated, so each frozen position is pinned.
        CommandKind::CreateQueue {
            config: QueueConfig {
                lock_duration_millis: 1,
                max_delivery_count: 2,
                default_time_to_live_millis: Some(3),
                max_message_bytes: 4,
                requires_session: false,
                requires_duplicate_detection: false,
                duplicate_detection_history_millis: 20_000,
                dead_lettering_on_message_expiration: false,
                capacity: QueueCapacity::Finite {
                    max_size_bytes: 3 * MIB,
                },
            },
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                lock_duration_millis: Some(1),
                max_delivery_count: Some(2),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 3 }),
                max_message_bytes: Some(4),
                requires_session: Some(false),
                requires_duplicate_detection: Some(false),
                duplicate_detection_history_millis: Some(20_000),
                dead_lettering_on_message_expiration: Some(false),
                capacity: Some(QueueCapacity::Finite {
                    max_size_bytes: 4 * MIB,
                }),
            },
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_delivery_count: Some(3),
                dead_lettering_on_message_expiration: Some(false),
                capacity: Some(QueueCapacity::NonFinite),
                ..QueueConfigUpdate::default()
            },
        },
        // Invalid intent survives replication for deterministic refusal.
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                lock_duration_millis: 0,
                ..finite(0)
            },
        },
    ]
}

/// What a reader built before V7 does: its version gate refuses first.
fn pre_v7_reader(bytes: &[u8]) -> std::result::Result<DurableProposal, DurableProposalError> {
    let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(1..=6).contains(&version) {
        return Err(DurableProposalError::UnsupportedVersion(version));
    }
    DurableProposal::decode(bytes)
}

#[test]
fn v7_goldens_round_trip_and_older_readers_refuse() {
    let goldens = [
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 7, 0, 0, 0, 24, 0, 1, b'n', 1, b'q', 7, 0, 0xe0, 0xd4,
            0x03, 10, 0, 0x80, 0x80, 0x10, 0, 0, 0xc0, 0xcf, 0x24, 1, 0x80, 0x80, 0x40,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 7, 0, 0, 0, 20, 0, 1, b'n', 1, b'q', 7, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 0x80, 0x80, 0x80, 0x01,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 7, 0, 0, 0, 22, 0, 1, b'n', 1, b'q', 7, 0, 1, 2, 1, 3,
            4, 0, 0, 0xa0, 0x9c, 0x01, 0, 0x80, 0x80, 0xc0, 0x01,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 7, 0, 0, 0, 31, 0, 1, b'n', 1, b'q', 7, 1, 1, 1, 1, 2,
            1, 0, 3, 1, 4, 1, 0, 1, 0, 1, 0xa0, 0x9c, 0x01, 1, 0, 1, 0x80, 0x80, 0x80, 0x02,
        ],
    ];
    assert_eq!(goldens.len(), 4);
    for (kind, golden) in v7_kinds().into_iter().zip(goldens.iter()) {
        let proposal = DurableProposal::unbound(command("q", kind));
        assert_eq!(&proposal.encode().unwrap(), golden);
    }
    for kind in v7_kinds() {
        let proposal = DurableProposal::unbound(command("q", kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], 7_u32.to_be_bytes());
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
        assert_eq!(
            pre_v7_reader(&bytes),
            Err(DurableProposalError::UnsupportedVersion(7))
        );
        for cut in 0..bytes.len() {
            assert!(DurableProposal::decode(&bytes[..cut]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        let length = trailing.len() as u32 - 12;
        trailing[8..12].copy_from_slice(&length.to_be_bytes());
        assert_eq!(
            DurableProposal::decode(&trailing),
            Err(DurableProposalError::Malformed)
        );
        // Relabelled as V6 (SQL rules) it is not that format; 10 is unknown.
        let mut other = bytes.clone();
        other[4..8].copy_from_slice(&6_u32.to_be_bytes());
        assert!(DurableProposal::decode(&other).is_err());
        other[4..8].copy_from_slice(&11_u32.to_be_bytes());
        assert_eq!(
            DurableProposal::decode(&other),
            Err(DurableProposalError::UnsupportedVersion(11))
        );
    }
}

#[test]
fn non_finite_intent_keeps_its_existing_version() {
    for (kind, version) in [
        (
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            1,
        ),
        (
            CommandKind::CreateQueue {
                config: QueueConfig {
                    dead_lettering_on_message_expiration: false,
                    ..QueueConfig::default()
                },
            },
            5,
        ),
        (
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    max_delivery_count: Some(3),
                    ..QueueConfigUpdate::default()
                },
            },
            1,
        ),
        (
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(true),
                    ..QueueConfigUpdate::default()
                },
            },
            5,
        ),
    ] {
        let proposal = DurableProposal::unbound(command("q", kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], u32::to_be_bytes(version));
        assert_eq!(pre_v7_reader(&bytes).unwrap(), proposal);
    }
}

#[test]
fn v7_retains_queue_authority_rules() {
    let machine = StateMachine::new(storage::MemoryStore::default());
    machine
        .apply(&command(
            "q",
            CommandKind::CreateQueue {
                config: finite(MIB),
            },
        ))
        .unwrap();
    let namespace = NamespaceName::new("n").unwrap();
    let queue = EntityPath::new("q").unwrap();
    let binding = machine.bind_entity(&namespace, &queue).unwrap();
    let bound = DurableProposal::bound(BoundCommand::new(
        binding,
        command("q", v7_kinds().swap_remove(1)),
    ))
    .unwrap();
    let bytes = bound.encode().unwrap();
    assert_eq!(bytes[4..8], 7_u32.to_be_bytes());
    assert_eq!(DurableProposal::decode(&bytes).unwrap(), bound);
}

// ---- paired lifecycles --------------------------------------------------------

fn queue<P: StoreProvider>(provider: P, config: QueueConfig) -> Result<QueueFixture<P>> {
    Ok(QueueFixture::new(provider, "tenant", "orders", config)?)
}

fn update(capacity: Option<QueueCapacity>) -> CommandKind {
    CommandKind::UpdateQueue {
        update: QueueConfigUpdate {
            capacity,
            ..QueueConfigUpdate::default()
        },
    }
}

fn image<S: StateStore>(machine: &StateMachine<S>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(machine.store().snapshot()?.entries().to_vec())
}

fn send_and_receive<P: StoreProvider>(fixture: &QueueFixture<P>, millis: u64) -> Result {
    fixture.at(
        millis,
        CommandKind::Send {
            message_id: format!("m-{millis}"),
            body: vec![7; 4096],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    let CommandOutcome::Received(Some(_)) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("a finite queue still delivers");
    };
    Ok(())
}

fn finite_queues_create_update_and_reopen<P: StoreProvider>(provider: P) -> Result {
    let fixture = queue(provider, finite(MIB))?;
    let key = keys::queue_config(&fixture.namespace, &fixture.entity);
    let shadow_key = keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?);
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(codec::queue::encode(&finite(MIB))?)
    );
    assert_eq!(
        fixture.machine.store().get(&shadow_key)?,
        Some(shadow_bytes())
    );
    // No admission yet: a recorded size does not change message behavior.
    send_and_receive(&fixture, 1)?;

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?,
        Some(finite(MIB))
    );
    assert_eq!(
        fixture.at(
            10,
            update(Some(QueueCapacity::Finite {
                max_size_bytes: MAX_QUEUE_CAPACITY_BYTES,
            })),
        )?,
        CommandOutcome::QueueUpdated
    );
    // Other settings keep the size; the shadow never changes.
    assert_eq!(
        fixture.at(
            11,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    max_delivery_count: Some(3),
                    ..QueueConfigUpdate::default()
                },
            },
        )?,
        CommandOutcome::QueueUpdated
    );
    let expected = QueueConfig {
        max_delivery_count: 3,
        ..finite(MAX_QUEUE_CAPACITY_BYTES)
    };
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(codec::queue::encode(&expected)?)
    );
    assert_eq!(
        fixture.machine.store().get(&shadow_key)?,
        Some(shadow_bytes())
    );

    // Mode changes and invalid sizes refuse without any write.
    let before = image(&fixture.machine)?;
    for (kind, error) in [
        (
            update(Some(QueueCapacity::NonFinite)),
            QueueConfigError::CapacityModeImmutable,
        ),
        (
            update(Some(QueueCapacity::Finite {
                max_size_bytes: MIB + 1,
            })),
            QueueConfigError::CapacityOutOfRange {
                granularity_bytes: MIB,
                maximum_bytes: MAX_QUEUE_CAPACITY_BYTES,
            },
        ),
    ] {
        assert_eq!(fixture.at(20, kind), Err(BrokerError::QueueConfig(error)));
        assert_eq!(image(&fixture.machine)?, before);
    }
    send_and_receive(&fixture, 30)?;
    validate_complete_state(&image(&fixture.machine)?).expect("finite records validate");
    Ok(())
}

fn rejected_profiles_write_nothing<P: StoreProvider>(provider: P) -> Result {
    let machine = StateMachine::new(provider.open()?);
    let at = |entity: &str, kind: CommandKind| machine.apply(&command(entity, kind));
    let empty = image(&machine)?;
    for (config, error) in [
        (finite(0), None),
        (finite(MIB + 1), None),
        (
            QueueConfig {
                requires_session: true,
                ..finite(MIB)
            },
            Some(QueueConfigError::FiniteCapacityWithSessions),
        ),
        (
            QueueConfig {
                requires_duplicate_detection: true,
                ..finite(MIB)
            },
            Some(QueueConfigError::FiniteCapacityWithDuplicateDetection),
        ),
    ] {
        let result = at("q", CommandKind::CreateQueue { config });
        match error {
            Some(error) => assert_eq!(result, Err(BrokerError::QueueConfig(error))),
            None => assert!(matches!(
                result,
                Err(BrokerError::QueueConfig(
                    QueueConfigError::CapacityOutOfRange { .. }
                ))
            )),
        }
        assert_eq!(image(&machine)?, empty);
    }
    // A non-finite queue cannot become finite; naming its own mode is a no-op.
    at(
        "q",
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let before = image(&machine)?;
    assert_eq!(
        at(
            "q",
            update(Some(QueueCapacity::Finite {
                max_size_bytes: MIB
            }))
        ),
        Err(BrokerError::QueueConfig(
            QueueConfigError::CapacityModeImmutable
        ))
    );
    assert_eq!(image(&machine)?, before);
    assert_eq!(
        at("q", update(Some(QueueCapacity::NonFinite)))?,
        CommandOutcome::QueueUpdated
    );
    assert_eq!(image(&machine)?, before);
    Ok(())
}

fn non_finite_behavior_is_unchanged<P: StoreProvider>(provider: P) -> Result {
    let fixture = queue(provider, QueueConfig::default())?;
    let namespace = fixture.namespace.clone();
    let key = keys::queue_config(&namespace, &fixture.entity);
    send_and_receive(&fixture, 1)?;
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("worker")?;
    let on = |entity: &EntityPath, kind: CommandKind| {
        fixture.machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(2),
            kind,
        ))
    };
    on(
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    on(
        &topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    let child = topic.subscription(&name)?;
    let fixture = fixture.restart()?;
    let store = fixture.machine.store();
    assert_eq!(
        store.get(&key)?,
        Some(codec::queue::encode(&QueueConfig::default())?)
    );
    assert_eq!(store.get(&key)?.map(|raw| raw[0]), Some(1));
    assert_eq!(
        store.get(&keys::topic_config(&namespace, &topic))?,
        Some(codec::topic::encode(&TopicConfig::default())?)
    );
    let backing = fixture
        .machine
        .queue_config(&namespace, &child)?
        .expect("backing");
    assert_eq!(backing.capacity, QueueCapacity::NonFinite);
    assert_eq!(
        store
            .get(&keys::queue_config(&namespace, &child))?
            .map(|raw| raw[0]),
        Some(1)
    );
    validate_complete_state(&image(&fixture.machine)?).expect("legacy records validate");
    Ok(())
}

fn snapshot_validation_fences_finite_records<P: StoreProvider>(provider: P) -> Result {
    let fixture = queue(provider, finite(MIB))?;
    let namespace = fixture.namespace.clone();
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("worker")?;
    for kind in [
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    ] {
        fixture.machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1),
            kind,
        ))?;
    }
    let image = image(&fixture.machine)?;
    validate_complete_state(&image).expect("a finite queue beside a topic validates");
    let row = |key: &[u8]| image.iter().position(|(k, _)| k == key).unwrap();
    let replaced = |row: usize, value: Vec<u8>| {
        let mut image = image.clone();
        image[row].1 = value;
        validate_catalog(&image).map(drop)
    };

    let parent = row(&keys::queue_config(&namespace, &fixture.entity));
    assert_eq!(
        replaced(
            parent,
            codec::queue::encode(&QueueConfig {
                requires_session: true,
                ..finite(MIB)
            })?
        ),
        Err(SnapshotCatalogError::InvalidValue {
            row: parent,
            detail: "queue profile violates current policy",
        })
    );
    let mut nonminimal = image[parent].1[..image[parent].1.len() - 1].to_vec();
    nonminimal.extend([0xc0, 0x00]);
    assert_eq!(
        replaced(parent, nonminimal),
        Err(SnapshotCatalogError::InvalidValue {
            row: parent,
            detail: "catalog value is not canonical",
        })
    );

    // Only an ordinary parent may be finite: never a shadow or a backing.
    let shadow = row(&keys::queue_config(
        &namespace,
        &fixture.entity.dead_letter_queue()?,
    ));
    let finite_shadow = QueueConfig {
        max_delivery_count: u32::MAX,
        ..finite(MIB)
    };
    assert!(matches!(
        replaced(shadow, codec::queue::encode(&finite_shadow)?),
        Err(SnapshotCatalogError::InconsistentCatalog { .. })
    ));
    let child = topic.subscription(&name)?;
    let backing = row(&keys::queue_config(&namespace, &child));
    let mut finite_backing = codec::queue::decode(&image[backing].1)?;
    finite_backing.capacity = QueueCapacity::Finite {
        max_size_bytes: MIB,
    };
    assert!(matches!(
        replaced(backing, codec::queue::encode(&finite_backing)?),
        Err(SnapshotCatalogError::InconsistentCatalog { .. })
    ));
    Ok(())
}

fn admin_reads_refuse_finite_shadows_and_backings<P: StoreProvider>(provider: P) -> Result {
    let fixture = queue(provider, finite(MIB))?;
    let namespace = fixture.namespace.clone();
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("worker")?;
    for kind in [
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    ] {
        fixture.machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1),
            kind,
        ))?;
    }
    let machine = &fixture.machine;
    assert_eq!(
        machine
            .admin_queue(&namespace, &fixture.entity)?
            .map(|queue| queue.config),
        Some(finite(MIB))
    );
    assert!(
        machine
            .admin_subscription(&namespace, &topic, &name)?
            .is_some()
    );

    // A finite shadow is not the exact shadow of its finite parent.
    let shadow_key = keys::queue_config(&namespace, &fixture.entity.dead_letter_queue()?);
    let shadow = machine.store().get(&shadow_key)?.expect("shadow");
    machine.store().apply(WriteBatch::default().put(
        shadow_key.clone(),
        codec::queue::encode(&QueueConfig {
            max_delivery_count: u32::MAX,
            ..finite(MIB)
        })?,
    ))?;
    assert_eq!(
        machine.admin_queue(&namespace, &fixture.entity),
        Err(AdminCatalogError::CorruptCatalog)
    );
    machine
        .store()
        .apply(WriteBatch::default().put(shadow_key, shadow))?;
    assert!(machine.admin_queue(&namespace, &fixture.entity)?.is_some());

    // A finite backing queue is not the inherited subscription profile.
    let child = topic.subscription(&name)?;
    let backing_key = keys::queue_config(&namespace, &child);
    let mut backing = machine.queue_config(&namespace, &child)?.expect("backing");
    backing.capacity = QueueCapacity::Finite {
        max_size_bytes: MIB,
    };
    machine
        .store()
        .apply(WriteBatch::default().put(backing_key, codec::queue::encode(&backing)?))?;
    assert_eq!(
        machine.admin_subscription(&namespace, &topic, &name),
        Err(AdminCatalogError::CorruptCatalog)
    );
    Ok(())
}

// ---- instantiation -----------------------------------------------------------

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> super::Result {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> super::Result {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    finite_queues_create_update_and_reopen,
    rejected_profiles_write_nothing,
    non_finite_behavior_is_unchanged,
    snapshot_validation_fences_finite_records,
    admin_reads_refuse_finite_shadows_and_backings,
}
