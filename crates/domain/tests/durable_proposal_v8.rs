//! Proposal V8: subscription filter-error discard intent.

use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, DurableProposalError, EntityPath,
    NamespaceName, QueueTimeToLiveUpdate, StateMachine, SubscriptionConfig,
    SubscriptionConfigUpdate, SubscriptionName, Timestamp, TopicConfig,
};
use storage::MemoryStore;

fn command(kind: CommandKind) -> Command {
    Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("t").unwrap(),
        Timestamp::from_millis(7),
        kind,
    )
}

fn name() -> SubscriptionName {
    SubscriptionName::new("s").unwrap()
}

fn v8_kinds() -> [CommandKind; 3] {
    [
        CommandKind::CreateSubscription {
            name: name(),
            config: SubscriptionConfig {
                dead_lettering_on_filter_evaluation_exceptions: false,
                ..SubscriptionConfig::default()
            },
        },
        CommandKind::UpdateSubscription {
            name: name(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(true),
                ..SubscriptionConfigUpdate::default()
            },
        },
        CommandKind::UpdateSubscription {
            name: name(),
            update: SubscriptionConfigUpdate {
                lock_duration_millis: Some(1),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                dead_lettering_on_message_expiration: Some(false),
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        },
    ]
}

/// What a reader built before V8 does: its version gate refuses first.
fn pre_v8_reader(bytes: &[u8]) -> Result<DurableProposal, DurableProposalError> {
    let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if !(1..=7).contains(&version) {
        return Err(DurableProposalError::UnsupportedVersion(version));
    }
    DurableProposal::decode(bytes)
}

#[test]
fn v8_goldens_round_trip_and_older_readers_refuse() {
    let goldens = [
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 8, 0, 0, 0, 15, 0, 1, b'n', 1, b't', 7, 0, 1, b's',
            0xe0, 0xd4, 0x03, 10, 0, 1,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 8, 0, 0, 0, 14, 0, 1, b'n', 1, b't', 7, 1, 1, b's', 0,
            0, 0, 0, 1,
        ],
        vec![
            b'S', b'W', b'D', b'P', 0, 0, 0, 8, 0, 0, 0, 17, 0, 1, b'n', 1, b't', 7, 1, 1, b's', 1,
            1, 0, 1, 1, 1, 0, 0,
        ],
    ];
    for (kind, golden) in v8_kinds().into_iter().zip(goldens.iter()) {
        let proposal = DurableProposal::unbound(command(kind));
        assert_eq!(&proposal.encode().unwrap(), golden);
    }
    for kind in v8_kinds() {
        let proposal = DurableProposal::unbound(command(kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], 8_u32.to_be_bytes());
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
        assert_eq!(
            pre_v8_reader(&bytes),
            Err(DurableProposalError::UnsupportedVersion(8))
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
        let mut unknown = bytes.clone();
        unknown[4..8].copy_from_slice(&11_u32.to_be_bytes());
        assert_eq!(
            DurableProposal::decode(&unknown),
            Err(DurableProposalError::UnsupportedVersion(11))
        );
    }
}

#[test]
fn subscription_intent_without_the_setting_keeps_its_version() {
    for (kind, version) in [
        (
            CommandKind::CreateSubscription {
                name: name(),
                config: SubscriptionConfig::default(),
            },
            1,
        ),
        (
            CommandKind::CreateSubscription {
                name: name(),
                config: SubscriptionConfig {
                    dead_lettering_on_message_expiration: false,
                    ..SubscriptionConfig::default()
                },
            },
            5,
        ),
        (
            CommandKind::UpdateSubscription {
                name: name(),
                update: SubscriptionConfigUpdate::default(),
            },
            2,
        ),
        (
            CommandKind::UpdateSubscription {
                name: name(),
                update: SubscriptionConfigUpdate {
                    dead_lettering_on_message_expiration: Some(false),
                    ..SubscriptionConfigUpdate::default()
                },
            },
            5,
        ),
    ] {
        let proposal = DurableProposal::unbound(command(kind));
        let bytes = proposal.encode().unwrap();
        assert_eq!(bytes[4..8], u32::to_be_bytes(version));
        assert_eq!(pre_v8_reader(&bytes).unwrap(), proposal);
    }
}

#[test]
fn v8_retains_topic_owned_subscription_authority() {
    let machine = StateMachine::new(MemoryStore::default());
    let namespace = NamespaceName::new("n").unwrap();
    let topic = EntityPath::new("t").unwrap();
    machine
        .apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))
        .unwrap();
    for kind in v8_kinds() {
        let command = command(kind);
        let binding = machine.bind_entity(&namespace, &topic).unwrap();
        let bound = DurableProposal::bound(BoundCommand::new(binding, command)).unwrap();
        let bytes = bound.encode().unwrap();
        assert_eq!(bytes[4..8], 8_u32.to_be_bytes());
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), bound);
    }
}
