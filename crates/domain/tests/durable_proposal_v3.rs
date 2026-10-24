use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, DurableProposalError, EntityPath,
    NamespaceName, QueueConfig, StateMachine, Timestamp, TopicConfig,
};
use storage::MemoryStore;

fn create(config: TopicConfig) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("t").unwrap(),
        Timestamp::from_millis(7),
        CommandKind::CreateTopic { config },
    ))
}

#[test]
fn canonical_v3_golden_and_invalid_intent() {
    let proposal = create(TopicConfig {
        max_message_bytes: 42,
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: 20_000,
        ..TopicConfig::default()
    });
    let bytes = proposal.encode().unwrap();
    assert_eq!(
        bytes,
        [
            b'S', b'W', b'D', b'P', 0, 0, 0, 3, 0, 0, 0, 13, 0, 1, b'n', 1, b't', 7, 0, 0, 42, 1,
            0xa0, 0x9c, 1
        ]
    );
    assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
    for enabled in [false, true] {
        for window in [
            0,
            19_999,
            20_000,
            600_000,
            604_800_000,
            604_800_001,
            u64::MAX,
        ] {
            let p = create(TopicConfig {
                max_message_bytes: 0,
                default_time_to_live_millis: Some(0),
                requires_duplicate_detection: enabled,
                duplicate_detection_history_millis: window,
            });
            let bytes = p.encode().unwrap();
            let version = if !enabled && window == 600_000 { 1 } else { 3 };
            assert_eq!(bytes[7], version);
            assert_eq!(DurableProposal::decode(&bytes).unwrap(), p);
        }
    }
    for cut in 0..bytes.len() {
        assert!(DurableProposal::decode(&bytes[..cut]).is_err());
    }
    let mut trailing = bytes.clone();
    for index in [12, 18, 21] {
        let mut bad_tag = bytes.clone();
        bad_tag[index] = 127;
        assert_eq!(
            DurableProposal::decode(&bad_tag),
            Err(DurableProposalError::Malformed)
        );
    }
    trailing.push(0);
    assert_eq!(
        DurableProposal::decode(&trailing),
        Err(DurableProposalError::Malformed)
    );
    trailing[8..12].copy_from_slice(&14_u32.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&trailing),
        Err(DurableProposalError::Malformed)
    );
    let mut unknown = bytes.clone();
    unknown[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&unknown),
        Err(DurableProposalError::UnsupportedVersion(u32::MAX))
    );
    let mut alias = bytes;
    alias.splice(20..21, [0xaa, 0]); // nonminimal varint for message size 42
    alias[8..12].copy_from_slice(&14_u32.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&alias),
        Err(DurableProposalError::NonCanonical)
    );
}

#[test]
fn v3_retains_creation_authority_rules_including_queue_bound_refused_intent() {
    let machine = StateMachine::new(MemoryStore::default());
    let p = create(TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    });
    let command = p.command();
    machine
        .apply(&Command::new(
            command.namespace.clone(),
            command.entity.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .unwrap();
    let binding = machine
        .bind_entity(&command.namespace, &command.entity)
        .unwrap();
    // Unlike V2 update-only rules, V1 creation intent can retain a queue owner.
    let bound = DurableProposal::bound(BoundCommand::new(binding, command.clone())).unwrap();
    assert_eq!(
        DurableProposal::decode(&bound.encode().unwrap()).unwrap(),
        bound
    );
}
