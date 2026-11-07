use domain::{
    BoundCommand, Command, CommandKind, DurableProposal, DurableProposalError, EntityPath,
    MAX_DURABLE_PROPOSAL_BYTES, MessageContent, MessageInput, NamespaceName, QueueConfig,
    RawMessage, SessionId, StateMachine, Timestamp,
};
use storage::MemoryStore;

fn proposal(kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        Timestamp::from_millis(7),
        kind,
    ))
}

fn header(length: u8) -> Vec<u8> {
    vec![b'S', b'W', b'D', b'P', 0, 0, 0, 4, 0, 0, 0, length]
}

fn framed(payload: &[u8]) -> Vec<u8> {
    let mut bytes = header(payload.len() as u8);
    bytes.extend_from_slice(payload);
    bytes
}

/// A data section holding one byte, which is complete AMQP content.
const DATA: [u8; 6] = [0x00, 0x53, 0x75, 0xa0, 0x01, 0x2a];

fn raw() -> MessageContent {
    MessageContent::Raw(RawMessage {
        message_id: "m".into(),
        body: vec![1, 2],
        time_to_live_millis: Some(5),
        session_id: Some(SessionId::new("S").unwrap()),
        scheduled_enqueue_at: Some(Timestamp::from_millis(9)),
    })
}

fn goldens() -> Vec<(CommandKind, Vec<u8>)> {
    let amqp = [&[0, 1, b'n', 1, b'q', 7, 0, 0, 6][..], &DATA].concat();
    vec![
        (
            CommandKind::SendContent {
                content: MessageContent::Amqp(DATA.to_vec()),
            },
            framed(&amqp),
        ),
        (
            CommandKind::SendContent { content: raw() },
            framed(&[
                0, 1, b'n', 1, b'q', 7, 0, 1, 1, b'm', 2, 1, 2, 1, 5, 1, 1, b'S', 1, 9,
            ]),
        ),
        (
            CommandKind::SendContentBatch {
                messages: vec![
                    MessageContent::Amqp(vec![0xff]),
                    MessageContent::Raw(RawMessage::default()),
                ],
            },
            framed(&[0, 1, b'n', 1, b'q', 7, 1, 2, 0, 1, 0xff, 1, 0, 0, 0, 0, 0]),
        ),
        (
            CommandKind::SendContentBatch { messages: vec![] },
            framed(&[0, 1, b'n', 1, b'q', 7, 1, 0]),
        ),
    ]
}

#[test]
fn content_sends_have_literal_v4_goldens_and_exact_identity() {
    for (kind, golden) in goldens() {
        let original = proposal(kind);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes, golden, "{:?}", original.command().kind);
        let decoded = DurableProposal::decode(&bytes).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.encode().unwrap(), bytes);
    }
}

#[test]
fn legacy_sends_keep_their_v1_envelope() {
    for kind in [
        CommandKind::Send {
            message_id: "m".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
        CommandKind::SendBatch {
            messages: vec![MessageInput::default()],
        },
    ] {
        assert_eq!(proposal(kind).encode().unwrap()[4..8], [0, 0, 0, 1]);
    }
}

#[test]
fn decoding_retains_content_the_state_machine_will_refuse() {
    // Neither framing nor AMQP policy is applied to the content while decoding.
    for content in [
        MessageContent::Amqp(Vec::new()),
        MessageContent::Amqp(vec![0x00, 0x53, 0x75, 0xa0, 0x09]),
        MessageContent::Amqp(vec![0x40; 1024]),
        MessageContent::Raw(RawMessage {
            message_id: "x".repeat(4096),
            ..RawMessage::default()
        }),
    ] {
        let original = proposal(CommandKind::SendContent { content });
        let bytes = original.encode().unwrap();
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), original);
    }
}

#[test]
fn malformed_truncated_and_noncanonical_v4_bytes_refuse() {
    let (_, bytes) = goldens().remove(1);
    for cut in 0..bytes.len() {
        assert!(DurableProposal::decode(&bytes[..cut]).is_err(), "{cut}");
    }
    // Authority, kind and content tags outside their enums.
    for index in [12, 18, 19] {
        let mut bad = bytes.clone();
        bad[index] = 9;
        assert_eq!(
            DurableProposal::decode(&bad),
            Err(DurableProposalError::Malformed),
            "{index}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        DurableProposal::decode(&trailing),
        Err(DurableProposalError::Malformed)
    );
    trailing[8..12].copy_from_slice(&(bytes.len() as u32 - 11).to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&trailing),
        Err(DurableProposalError::Malformed)
    );

    // A two-byte varint for the one-byte time to live.
    let mut alias = bytes.clone();
    alias.splice(26..27, [0x85, 0x00]);
    let length = alias.len() as u32 - 12;
    alias[8..12].copy_from_slice(&length.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&alias),
        Err(DurableProposalError::NonCanonical)
    );
    let mut upper = bytes.clone();
    upper[16] = b'Q';
    assert_eq!(
        DurableProposal::decode(&upper),
        Err(DurableProposalError::NonCanonical)
    );
    let mut unknown = bytes;
    unknown[4..8].copy_from_slice(&5_u32.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&unknown),
        Err(DurableProposalError::UnsupportedVersion(5))
    );
}

#[test]
fn oversized_content_refuses_before_encoding_and_decoding() {
    let content = |size| CommandKind::SendContent {
        content: MessageContent::Amqp(vec![0; size]),
    };
    assert_eq!(
        proposal(content(MAX_DURABLE_PROPOSAL_BYTES + 1)).encode(),
        Err(DurableProposalError::TooLarge)
    );
    // Fixed fields and the length prefix push the largest encodable content
    // just below the envelope limit.
    assert_eq!(
        proposal(content(MAX_DURABLE_PROPOSAL_BYTES - 16)).encode(),
        Err(DurableProposalError::TooLarge)
    );
    let fits = proposal(content(MAX_DURABLE_PROPOSAL_BYTES - 24))
        .encode()
        .unwrap();
    assert!(fits.len() <= MAX_DURABLE_PROPOSAL_BYTES);
    assert!(DurableProposal::decode(&fits).is_ok());
    let mut over = fits;
    over.resize(MAX_DURABLE_PROPOSAL_BYTES + 1, 0);
    assert_eq!(
        DurableProposal::decode(&over),
        Err(DurableProposalError::TooLarge)
    );
    let batch = CommandKind::SendContentBatch {
        messages: vec![MessageContent::Amqp(Vec::new()); MAX_DURABLE_PROPOSAL_BYTES + 1],
    };
    assert_eq!(
        proposal(batch).encode(),
        Err(DurableProposalError::TooLarge)
    );
}

#[test]
fn v4_keeps_send_authority_rules_and_original_generation() {
    let machine = StateMachine::new(MemoryStore::default());
    let original = proposal(CommandKind::SendContent {
        content: MessageContent::Amqp(DATA.to_vec()),
    });
    let command = original.command();
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
    let bound = DurableProposal::bound(BoundCommand::new(binding, command.clone())).unwrap();
    let bytes = bound.encode().unwrap();
    assert_eq!(bytes[4..8], [0, 0, 0, 4]);
    assert_eq!(bytes[12], 1, "bound authority");
    assert_eq!(DurableProposal::decode(&bytes).unwrap(), bound);
}
