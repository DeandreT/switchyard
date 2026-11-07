//! Compatibility fences for `dead_lettering_on_message_expiration`: queue
//! profile records and durable proposal V5.

use domain::{
    BoundCommand, CodecError, Command, CommandKind, DurableProposal, DurableProposalError,
    EntityPath, NamespaceName, QueueConfig, QueueConfigUpdate, QueueTimeToLiveUpdate, StateMachine,
    SubscriptionConfig, SubscriptionConfigUpdate, SubscriptionName, Timestamp, TopicConfig, codec,
};
use serde::{Deserialize, Serialize};
use storage::MemoryStore;

/// The pre-policy record shape, as a legacy reader decodes it.
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

fn legacy(config: QueueConfig) -> LegacyQueue {
    LegacyQueue {
        lock_duration_millis: config.lock_duration_millis,
        max_delivery_count: config.max_delivery_count,
        default_time_to_live_millis: config.default_time_to_live_millis,
        max_message_bytes: config.max_message_bytes,
        requires_session: config.requires_session,
        requires_duplicate_detection: config.requires_duplicate_detection,
        duplicate_detection_history_millis: config.duplicate_detection_history_millis,
    }
}

fn configs() -> Vec<QueueConfig> {
    vec![
        QueueConfig::default(),
        QueueConfig {
            lock_duration_millis: 1,
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: Some(u64::MAX),
            max_message_bytes: 1,
            requires_session: true,
            requires_duplicate_detection: true,
            duplicate_detection_history_millis: 20_000,
            dead_lettering_on_message_expiration: true,
        },
        // Invalid policy is a caller concern; the codec keeps it verbatim.
        QueueConfig {
            lock_duration_millis: 0,
            max_delivery_count: 0,
            default_time_to_live_millis: Some(0),
            max_message_bytes: 0,
            duplicate_detection_history_millis: 0,
            ..QueueConfig::default()
        },
    ]
}

#[test]
fn legacy_representable_profiles_keep_their_exact_bytes() {
    for config in configs() {
        let bytes = codec::encode(&legacy(config)).unwrap();
        assert_eq!(codec::queue::encode(&config).unwrap(), bytes);
        assert_eq!(codec::queue::decode(&bytes).unwrap(), config);
        for cut in 0..bytes.len() {
            assert!(codec::queue::decode(&bytes[..cut]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(codec::queue::decode(&trailing), Err(CodecError::Decode));
    }
    assert_eq!(
        codec::queue::encode(&QueueConfig::default()).unwrap(),
        [
            1, 0xe0, 0xd4, 0x03, 10, 0, 0x80, 0x80, 0x10, 0, 0, 0xc0, 0xcf, 0x24
        ]
    );
}

#[test]
fn disabled_profiles_use_a_fenced_version_that_legacy_readers_refuse() {
    for config in configs() {
        let config = QueueConfig {
            dead_lettering_on_message_expiration: false,
            ..config
        };
        let bytes = codec::queue::encode(&config).unwrap();
        assert_eq!(bytes[0], codec::queue::VALUE_FORMAT_V2);
        assert_eq!(bytes.last(), Some(&0));
        assert_eq!(codec::queue::decode(&bytes).unwrap(), config);
        // A pre-policy reader decodes profiles generically and must refuse
        // rather than silently drop the setting.
        assert_eq!(
            codec::decode::<LegacyQueue>(&bytes),
            Err(CodecError::UnsupportedVersion { version: 2 })
        );
        for cut in 0..bytes.len() {
            assert!(codec::queue::decode(&bytes[..cut]).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(codec::queue::decode(&trailing), Err(CodecError::Decode));
        // The enabled value has exactly one record shape: the legacy one.
        let mut redundant = bytes.clone();
        *redundant.last_mut().unwrap() = 1;
        assert_eq!(codec::queue::decode(&redundant), Err(CodecError::Decode));
        let mut boolean = bytes;
        *boolean.last_mut().unwrap() = 2;
        assert_eq!(codec::queue::decode(&boolean), Err(CodecError::Decode));
    }
    assert_eq!(
        codec::queue::decode(&[3, 0]),
        Err(CodecError::UnsupportedVersion { version: 3 })
    );
    assert_eq!(codec::queue::decode(&[]), Err(CodecError::EmptyEnvelope));
}

fn command(entity: &str, kind: CommandKind) -> Command {
    Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new(entity).unwrap(),
        Timestamp::from_millis(7),
        kind,
    )
}

fn v5_kinds() -> Vec<(&'static str, CommandKind)> {
    vec![
        (
            "q",
            CommandKind::CreateQueue {
                config: QueueConfig {
                    lock_duration_millis: 1,
                    max_delivery_count: 2,
                    default_time_to_live_millis: Some(3),
                    max_message_bytes: 4,
                    requires_session: true,
                    requires_duplicate_detection: false,
                    duplicate_detection_history_millis: 5,
                    dead_lettering_on_message_expiration: false,
                },
            },
        ),
        (
            "q",
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                    dead_lettering_on_message_expiration: Some(true),
                    ..QueueConfigUpdate::default()
                },
            },
        ),
        (
            "t",
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                config: SubscriptionConfig {
                    lock_duration_millis: 0,
                    dead_lettering_on_message_expiration: false,
                    ..SubscriptionConfig::default()
                },
            },
        ),
        (
            "t",
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                update: SubscriptionConfigUpdate {
                    dead_lettering_on_message_expiration: Some(false),
                    ..SubscriptionConfigUpdate::default()
                },
            },
        ),
    ]
}

/// What a reader built before V5 does: its version gate refuses first.
fn pre_v5_reader(bytes: &[u8]) -> Result<DurableProposal, DurableProposalError> {
    let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(1..=4).contains(&version) {
        return Err(DurableProposalError::UnsupportedVersion(version));
    }
    DurableProposal::decode(bytes)
}

#[test]
fn v5_goldens_round_trip_and_older_readers_refuse() {
    let goldens = [
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 5, 0, 0, 0, 16, 0, 1, b'n', 1, b'q', 7, 0, 1, 2, 1, 3,
            4, 1, 0, 5, 0,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 5, 0, 0, 0, 16, 0, 1, b'n', 1, b'q', 7, 1, 0, 0, 1, 1,
            0, 0, 0, 0, 1,
        ],
    ];
    for ((entity, kind), golden) in v5_kinds().into_iter().zip(goldens.iter()) {
        let proposal = DurableProposal::unbound(command(entity, kind));
        assert_eq!(&proposal.encode().unwrap(), golden);
    }
    for (entity, kind) in v5_kinds() {
        let proposal = DurableProposal::unbound(command(entity, kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], 5_u32.to_be_bytes());
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
        assert_eq!(
            pre_v5_reader(&bytes),
            Err(DurableProposalError::UnsupportedVersion(5))
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
    }
}

#[test]
fn legacy_intent_keeps_its_existing_version() {
    for (entity, kind, version) in [
        (
            "q",
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
            1,
        ),
        (
            "q",
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    max_delivery_count: Some(3),
                    ..QueueConfigUpdate::default()
                },
            },
            1,
        ),
        (
            "t",
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                config: SubscriptionConfig::default(),
            },
            1,
        ),
        (
            "t",
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                update: SubscriptionConfigUpdate::default(),
            },
            2,
        ),
    ] {
        let proposal = DurableProposal::unbound(command(entity, kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], u32::to_be_bytes(version));
        assert_eq!(pre_v5_reader(&bytes).unwrap(), proposal);
    }
}

#[test]
fn v5_retains_each_command_familys_authority_rules() {
    let machine = StateMachine::new(MemoryStore::default());
    let namespace = NamespaceName::new("n").unwrap();
    let queue = EntityPath::new("q").unwrap();
    let topic = EntityPath::new("t").unwrap();
    for (entity, kind) in [
        (
            &queue,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
        (
            &topic,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    ] {
        machine
            .apply(&Command::new(
                namespace.clone(),
                entity.clone(),
                Timestamp::UNIX_EPOCH,
                kind,
            ))
            .unwrap();
    }
    for (entity, kind) in v5_kinds() {
        let command = command(entity, kind);
        let binding = machine
            .bind_entity(&command.namespace, &command.entity)
            .unwrap();
        let bound = DurableProposal::bound(BoundCommand::new(binding, command.clone())).unwrap();
        let bytes = bound.encode().unwrap();
        assert_eq!(bytes[4..8], 5_u32.to_be_bytes());
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), bound);
    }
    // Subscription updates stay topic-owned: a queue binding is refused.
    let binding = machine.bind_entity(&namespace, &queue).unwrap();
    let mut update = command(
        "q",
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("s").unwrap(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_message_expiration: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        },
    );
    update.entity = queue.clone();
    assert_eq!(
        DurableProposal::bound(BoundCommand::new(binding, update)),
        Err(DurableProposalError::InvalidAuthority)
    );
}
