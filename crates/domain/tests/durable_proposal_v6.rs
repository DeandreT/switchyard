use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, DurableProposalError, EntityPath,
    NamespaceName, RuleFilter, RuleName, SqlRuleFilter, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig,
};
use storage::MemoryStore;

fn subscription() -> EntityPath {
    EntityPath::new("t")
        .unwrap()
        .subscription(&SubscriptionName::new("s").unwrap())
        .unwrap()
}

fn create(filter: RuleFilter) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("n").unwrap(),
        subscription(),
        Timestamp::from_millis(7),
        CommandKind::CreateRule {
            name: RuleName::new("Hot").unwrap(),
            filter,
        },
    ))
}

#[test]
fn canonical_v6_golden_and_unchanged_legacy_rule_versions() {
    let proposal = create(RuleFilter::Sql(SqlRuleFilter::new("a = 1")));
    let bytes = proposal.encode().unwrap();
    let mut expected = vec![
        b'S', b'W', b'D', b'P', 0, 0, 0, 6, 0, 0, 0, 37, 0, 1, b'n', 17,
    ];
    expected.extend(b"t/subscriptions/s");
    expected.extend([7, 0, 3, b'h', b'o', b't', 3, b'H', b'o', b't', 5]);
    expected.extend(b"a = 1");
    assert_eq!(bytes, expected);
    assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);

    // Rule intent that predates SQL keeps its V1 bytes.
    for filter in [RuleFilter::True, RuleFilter::False] {
        let bytes = create(filter.clone()).encode().unwrap();
        assert_eq!(bytes[7], 1);
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), create(filter));
    }

    for cut in 0..bytes.len() {
        assert!(DurableProposal::decode(&bytes[..cut]).is_err());
    }
    let mut bad_tag = bytes.clone();
    bad_tag[12] = 127;
    assert_eq!(
        DurableProposal::decode(&bad_tag),
        Err(DurableProposalError::Malformed)
    );
    let mut bad_kind = bytes.clone();
    bad_kind[34] = 1;
    assert_eq!(
        DurableProposal::decode(&bad_kind),
        Err(DurableProposalError::Malformed)
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        DurableProposal::decode(&trailing),
        Err(DurableProposalError::Malformed)
    );
    let mut unknown = bytes.clone();
    unknown[4..8].copy_from_slice(&8_u32.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&unknown),
        Err(DurableProposalError::UnsupportedVersion(8))
    );
    // Nonminimal varint for issued_at 7.
    let mut alias = bytes;
    alias.splice(33..34, [0x87, 0]);
    alias[8..12].copy_from_slice(&38_u32.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&alias),
        Err(DurableProposalError::NonCanonical)
    );
}

#[test]
fn uncompilable_intent_survives_and_bound_authority_round_trips() {
    let invalid = create(RuleFilter::Sql(SqlRuleFilter::new("a =")));
    assert_eq!(
        DurableProposal::decode(&invalid.encode().unwrap()).unwrap(),
        invalid
    );

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
    machine
        .apply(&Command::new(
            namespace.clone(),
            topic,
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("s").unwrap(),
                config: SubscriptionConfig::default(),
            },
        ))
        .unwrap();
    let binding = machine.bind_entity(&namespace, &subscription()).unwrap();
    let command = create(RuleFilter::Sql(SqlRuleFilter::new("a = 1")))
        .command()
        .clone();
    let bound = DurableProposal::bound(BoundCommand::new(binding, command)).unwrap();
    let bytes = bound.encode().unwrap();
    assert_eq!(bytes[7], 6);
    assert_eq!(DurableProposal::decode(&bytes).unwrap(), bound);
}
