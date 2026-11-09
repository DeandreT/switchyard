use super::*;
use crate::{Command, EntityPath, NamespaceName, RawMessage, Timestamp};

fn proposal(kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        Timestamp::from_millis(7),
        kind,
    ))
}

fn decode(value: &Proposal) -> Result<DurableProposal, Error> {
    DurableProposal::decode(&super::super::encode_version(value, 9).unwrap())
}

#[test]
fn literal_v9_golden_composes_v4_without_reinterpreting_it() {
    let original = proposal(CommandKind::SendContent {
        content: MessageContent::AmqpTextIds(vec![0xff]),
    });
    // V9 payload = bytes(canonical V4 envelope) | bytes([1]).
    let golden =
        b"SWDP\0\0\0\x09\0\0\0\x19\x16SWDP\0\0\0\x04\0\0\0\x0a\0\x01n\x01q\x07\0\0\x01\xff\x01\x01";
    let encoded = original.encode().unwrap();
    assert_eq!(encoded, golden);
    assert_eq!(DurableProposal::decode(&encoded), Ok(original.clone()));
    assert_eq!(
        DurableProposal::decode(&encoded).unwrap().encode().unwrap(),
        encoded
    );
    assert!(matches!(
        super::super::v4::Proposal::capture(&original),
        Err(Error::UnsupportedCommandVersion(4))
    ));
    let nested = Proposal::capture(&original).unwrap();
    let legacy = DurableProposal::decode(&nested.legacy_v4.0).unwrap();
    assert!(matches!(
        legacy.command().kind,
        CommandKind::SendContent {
            content: MessageContent::Amqp(_)
        }
    ));
}

#[test]
fn mixed_batches_keep_each_childs_semantics() {
    let original = proposal(CommandKind::SendContentBatch {
        messages: vec![
            MessageContent::Amqp(vec![0xff]),
            MessageContent::AmqpTextIds(vec![0xfe]),
            MessageContent::Raw(RawMessage::default()),
        ],
    });
    assert_eq!(
        DurableProposal::decode(&original.encode().unwrap()),
        Ok(original)
    );
}

#[test]
fn v9_refuses_unknown_versions_and_older_schema_dispatch() {
    let proposal = proposal(CommandKind::SendContent {
        content: MessageContent::AmqpTextIds(vec![0xff]),
    });
    let bytes = proposal.encode().unwrap();
    for version in [0_u32, 11, u32::MAX] {
        let mut unknown = bytes.clone();
        unknown[4..8].copy_from_slice(&version.to_be_bytes());
        assert_eq!(
            DurableProposal::decode(&unknown),
            Err(Error::UnsupportedVersion(version))
        );
    }
    // Recognizing a version never allows a V9 payload to masquerade as its
    // older schema, including merged capacity and filter-policy intent.
    for version in 1_u32..=8 {
        let mut older = bytes.clone();
        older[4..8].copy_from_slice(&version.to_be_bytes());
        assert!(DurableProposal::decode(&older).is_err());
    }
}

#[test]
fn invalid_modes_and_recursive_or_non_send_envelopes_refuse() {
    let original = proposal(CommandKind::SendContent {
        content: MessageContent::AmqpTextIds(vec![0xff]),
    });
    for modes in [vec![], vec![0], vec![2], vec![1, 1]] {
        let mut stored = Proposal::capture(&original).unwrap();
        stored.text_id_children = Bytes(modes);
        assert!(decode(&stored).is_err());
    }
    for bytes in [original.encode().unwrap(), b"SWDP\0\0\0\x09".to_vec()] {
        let mut stored = Proposal::capture(&original).unwrap();
        stored.legacy_v4 = Bytes(bytes);
        assert_eq!(decode(&stored), Err(Error::Malformed));
    }
    let mut stored = Proposal::capture(&original).unwrap();
    stored.legacy_v4 = Bytes(
        proposal(CommandKind::SendContent {
            content: MessageContent::Raw(RawMessage::default()),
        })
        .encode()
        .unwrap(),
    );
    assert_eq!(decode(&stored), Err(Error::Malformed));
    let mut stored = Proposal::capture(&original).unwrap();
    stored.legacy_v4 = Bytes(
        proposal(CommandKind::AbandonWithPatch {
            sequence: crate::SequenceNumber::new(1),
            lock_token: crate::LockToken::new(1),
            patch: vec![],
        })
        .encode()
        .unwrap(),
    );
    assert_eq!(decode(&stored), Err(Error::UnsupportedCommandVersion(9)));
}

#[test]
fn v9_is_bounded_and_preserves_bound_authority() {
    let namespace = NamespaceName::new("n").unwrap();
    let entity = EntityPath::new("q").unwrap();
    let machine = crate::StateMachine::new(storage::MemoryStore::default());
    machine
        .apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: crate::QueueConfig::default(),
            },
        ))
        .unwrap();
    let binding = machine.bind_entity(&namespace, &entity).unwrap();
    let command = Command::new(
        namespace,
        entity,
        Timestamp::from_millis(7),
        CommandKind::SendContent {
            content: MessageContent::AmqpTextIds(vec![0xff]),
        },
    );
    let original = DurableProposal::bound(crate::BoundCommand::new(binding, command)).unwrap();
    assert_eq!(
        DurableProposal::decode(&original.encode().unwrap()),
        Ok(original)
    );
    let oversized = proposal(CommandKind::SendContent {
        content: MessageContent::AmqpTextIds(vec![0; MAX_DURABLE_PROPOSAL_BYTES]),
    });
    assert_eq!(oversized.encode(), Err(Error::TooLarge));
}
