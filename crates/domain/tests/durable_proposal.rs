use std::collections::BTreeMap;

use domain::{
    BoundCommand, Command, CommandKind, CorrelationFilter, CorrelationValue, DurableProposal,
    DurableProposalAuthority, DurableProposalError, EntityBindingKind, EntityPath,
    FilterProperties, LockToken, MAX_DURABLE_PROPOSAL_BYTES, MessageEnvelope, MessageInput,
    NamespaceName, QueueConfig, QueueConfigUpdate, QueueTimeToLiveUpdate, ReceiveMode, RuleFilter,
    RuleName, SequenceNumber, SessionHold, SessionId, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig,
};
use storage::{MemoryStore, StateStore, WriteBatch};

fn command(kind: CommandKind) -> Command {
    Command::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        Timestamp::from_millis(7),
        kind,
    )
}

fn hold() -> SessionHold {
    SessionHold::new(SessionId::new("S").unwrap(), LockToken::new(0))
}

fn input() -> MessageInput {
    MessageInput {
        message_id: "m".to_owned(),
        body: vec![0, 255],
        time_to_live_millis: Some(3),
        session_id: Some(SessionId::new("S").unwrap()),
        scheduled_enqueue_at: Some(Timestamp::from_millis(4)),
        envelope: None,
    }
}

fn variants() -> Vec<CommandKind> {
    let message = input();
    vec![
        CommandKind::CreateQueue {
            config: QueueConfig {
                lock_duration_millis: 1,
                max_delivery_count: 2,
                default_time_to_live_millis: Some(3),
                max_message_bytes: 4,
                requires_session: true,
                requires_duplicate_detection: false,
                duplicate_detection_history_millis: 5,
            },
        },
        CommandKind::CreateTopic {
            config: TopicConfig {
                default_time_to_live_millis: Some(3),
                max_message_bytes: 4,
            },
        },
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("s").unwrap(),
            config: SubscriptionConfig {
                lock_duration_millis: 1,
                max_delivery_count: 2,
                default_time_to_live_millis: Some(3),
            },
        },
        CommandKind::CreateRule {
            name: RuleName::new("$Default").unwrap(),
            filter: RuleFilter::False,
        },
        CommandKind::DeleteRule {
            name: RuleName::new("Rule").unwrap(),
        },
        CommandKind::ListRules {
            skip: 1,
            max_rules: 2,
        },
        CommandKind::Send {
            message_id: message.message_id,
            body: message.body,
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: message.envelope,
        },
        CommandKind::SendBatch {
            messages: vec![input()],
        },
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(0), SequenceNumber::new(129)],
        },
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 2,
            session: Some(hold()),
        },
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: Some(0),
            session: Some(hold()),
        },
        CommandKind::Complete {
            sequence: SequenceNumber::new(0),
            lock_token: LockToken::new(0),
        },
        CommandKind::Abandon {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(2),
            replacement_envelope: None,
        },
        CommandKind::Defer {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(2),
            replacement_envelope: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::DeadLetter {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(2),
            reason: "R".to_owned(),
            description: "D".to_owned(),
            replacement_envelope: None,
        },
        CommandKind::RenewLock {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(2),
            lock_duration_millis: Some(0),
        },
        CommandKind::AcceptSession {
            session_id: Some(SessionId::new("S").unwrap()),
            lock_duration_millis: Some(0),
        },
        CommandKind::ReleaseSession { session: hold() },
        CommandKind::RenewSessionLock {
            session: hold(),
            lock_duration_millis: Some(1),
        },
        CommandKind::SetSessionState {
            session: hold(),
            state: vec![0, 255],
        },
        CommandKind::GetSessionState { session: hold() },
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                lock_duration_millis: Some(0),
                max_delivery_count: Some(0),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                max_message_bytes: Some(0),
                requires_session: Some(false),
                requires_duplicate_detection: Some(false),
                duplicate_detection_history_millis: Some(0),
            },
        },
    ]
}

fn bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn all_twenty_eight_command_variants_have_literal_v1_goldens() {
    let expected = [
        "53574450000000010000000f00016e017107000102010304010005",
        "53574450000000010000000a00016e01710701010304",
        "53574450000000010000000d00016e01710702017301020103",
        "53574450000000010000001a00016e01710703082464656661756c74082444656661756c7401",
        "53574450000000010000001100016e017107040472756c650452756c65",
        "53574450000000010000000900016e017107050102",
        "53574450000000010000001400016e01710706016d0200ff0103010153010400",
        "53574450000000010000001500016e0171070701016d0200ff0103010153010400",
        "53574450000000010000000b00016e0171070802008101",
        "53574450000000010000000d00016e01710709000201015300",
        "53574450000000010000000e00016e0171070a01010001015300",
        "53574450000000010000000900016e0171070b0000",
        "53574450000000010000000a00016e0171070c010200",
        "53574450000000010000000a00016e0171070d010200",
        "53574450000000010000000d00016e0171070e020102000000",
        "53574450000000010000000e00016e0171070f01020152014400",
        "53574450000000010000000b00016e0171071001020100",
        "53574450000000010000000c00016e017107110101530100",
        "53574450000000010000000a00016e01710712015300",
        "53574450000000010000000c00016e017107130153000101",
        "53574450000000010000000d00016e017107140153000200ff",
        "53574450000000010000000a00016e01710715015300",
        "53574450000000010000000700016e01710716",
        "53574450000000010000000700016e01710717",
        "53574450000000010000000700016e01710718",
        "53574450000000010000000700016e01710719",
        "53574450000000010000000700016e0171071a",
        "53574450000000010000001600016e0171071b010001000100000100010001000100",
    ];
    let variants = variants();
    assert_eq!(variants.len(), expected.len());
    for (index, (kind, expected)) in variants.into_iter().zip(expected).enumerate() {
        let original = DurableProposal::unbound(command(kind));
        let encoded = original.encode().unwrap();
        assert_eq!(encoded, bytes(expected), "V1 command tag {index}");
        let decoded = DurableProposal::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.authority(), DurableProposalAuthority::Unbound);
    }
}

fn properties() -> FilterProperties {
    FilterProperties {
        correlation_id: Some("C".to_owned()),
        message_id: Some("M".to_owned()),
        to: Some("T".to_owned()),
        reply_to: Some("R".to_owned()),
        subject: Some("U".to_owned()),
        session_id: Some("S".to_owned()),
        reply_to_session_id: Some("Q".to_owned()),
        content_type: Some("X".to_owned()),
        application_properties: BTreeMap::from([
            ("A".to_owned(), CorrelationValue::new(vec![0, 255]).unwrap()),
            ("a".to_owned(), CorrelationValue::new(Vec::new()).unwrap()),
            ("z".to_owned(), CorrelationValue::new(vec![128]).unwrap()),
        ]),
    }
}

#[test]
fn nested_fields_and_original_spelling_round_trip_without_semantic_validation() {
    let p = properties();
    let envelope = MessageEnvelope::new(vec![0, 255, 128]).with_filter_properties(p.clone());
    let mut message = input();
    message.message_id = "x".repeat(129);
    message.time_to_live_millis = Some(0);
    message.envelope = Some(envelope.clone());
    let correlation = CorrelationFilter {
        correlation_id: p.correlation_id,
        message_id: p.message_id,
        to: p.to,
        reply_to: p.reply_to,
        subject: p.subject,
        session_id: p.session_id,
        reply_to_session_id: p.reply_to_session_id,
        content_type: p.content_type,
        application_properties: p.application_properties,
    };
    assert!(
        RuleFilter::Correlation(correlation.clone())
            .validate()
            .is_err()
    );
    let kinds = [
        CommandKind::CreateRule {
            name: RuleName::new("Mixed.Rule").unwrap(),
            filter: RuleFilter::Correlation(correlation),
        },
        CommandKind::CreateRule {
            name: RuleName::new("Empty").unwrap(),
            filter: RuleFilter::Correlation(CorrelationFilter::default()),
        },
        CommandKind::CreateRule {
            name: RuleName::new("True").unwrap(),
            filter: RuleFilter::True,
        },
        CommandKind::SendBatch {
            messages: vec![message.clone(), MessageInput::default()],
        },
        CommandKind::Send {
            message_id: message.message_id,
            body: message.body,
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: message.envelope,
        },
        CommandKind::Abandon {
            sequence: SequenceNumber::new(u64::MAX),
            lock_token: LockToken::new(0),
            replacement_envelope: Some(envelope.clone()),
        },
        CommandKind::Defer {
            sequence: SequenceNumber::new(0),
            lock_token: LockToken::new(u64::MAX),
            replacement_envelope: Some(envelope.clone()),
        },
        CommandKind::DeadLetter {
            sequence: SequenceNumber::new(0),
            lock_token: LockToken::new(0),
            reason: String::new(),
            description: String::new(),
            replacement_envelope: Some(envelope),
        },
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..QueueConfigUpdate::default()
            },
        },
    ];
    for kind in kinds {
        let original = DurableProposal::unbound(command(kind));
        let decoded = DurableProposal::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
        if let CommandKind::CreateRule { name, .. } = &decoded.command().kind {
            let CommandKind::CreateRule { name: before, .. } = &original.command().kind else {
                unreachable!()
            };
            assert_eq!(name.display_name(), before.display_name());
            assert_eq!(name.as_str(), before.as_str());
        }
    }
}

#[test]
fn nested_envelope_rule_and_bound_identity_have_independent_literal_goldens() {
    let p = properties();
    let mut message = input();
    message.envelope =
        Some(MessageEnvelope::new(vec![0, 255, 128]).with_filter_properties(p.clone()));
    let send = DurableProposal::unbound(command(CommandKind::Send {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        scheduled_enqueue_at: message.scheduled_enqueue_at,
        envelope: message.envelope,
    }));
    assert_eq!(
        send.encode().unwrap(),
        bytes(
            "53574450000000010000003d00016e01710706016d0200ff01030101530104010300ff8001014301014d0101540101520101550101530101510101580301410200ff016100017a0180"
        )
    );
    let rule = DurableProposal::unbound(command(CommandKind::CreateRule {
        name: RuleName::new("Mixed.Rule").unwrap(),
        filter: RuleFilter::Correlation(CorrelationFilter {
            correlation_id: p.correlation_id,
            message_id: p.message_id,
            to: p.to,
            reply_to: p.reply_to,
            subject: p.subject,
            session_id: p.session_id,
            reply_to_session_id: p.reply_to_session_id,
            content_type: p.content_type,
            application_properties: p.application_properties,
        }),
    }));
    assert_eq!(
        rule.encode().unwrap(),
        bytes(
            "53574450000000010000004300016e017107030a6d697865642e72756c650a4d697865642e52756c650201014301014d0101540101520101550101530101510101580301410200ff016100017a0180"
        )
    );
    let true_rule = DurableProposal::unbound(command(CommandKind::CreateRule {
        name: RuleName::new("True").unwrap(),
        filter: RuleFilter::True,
    }));
    assert_eq!(
        true_rule.encode().unwrap(),
        bytes("53574450000000010000001200016e017107030474727565045472756500")
    );
    let unlimited = DurableProposal::unbound(command(CommandKind::UpdateQueue {
        update: QueueConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
            ..QueueConfigUpdate::default()
        },
    }));
    assert_eq!(
        unlimited.encode().unwrap(),
        bytes("53574450000000010000000f00016e0171071b0000010100000000")
    );
    let machine = StateMachine::new(MemoryStore::default());
    let create = command(CommandKind::CreateQueue {
        config: QueueConfig::default(),
    });
    machine.apply(&create).unwrap();
    let binding = machine
        .bind_entity(&create.namespace, &create.entity)
        .unwrap();
    let bound = DurableProposal::bound(BoundCommand::new(
        binding,
        command(CommandKind::ExpireLocks),
    ))
    .unwrap();
    assert_eq!(
        bound.encode().unwrap(),
        bytes("53574450000000010000000f01016e017101710001016e01710716")
    );
    for original in [send, rule, true_rule, unlimited, bound] {
        assert_eq!(
            DurableProposal::decode(&original.encode().unwrap()).unwrap(),
            original
        );
    }
}

#[test]
fn captured_bound_authority_round_trips_all_five_target_shapes_without_rebinding() {
    let store = MemoryStore::default();
    let machine = StateMachine::new(store.clone());
    let namespace = NamespaceName::new("n").unwrap();
    let queue = EntityPath::new("q").unwrap();
    let topic = EntityPath::new("t").unwrap();
    machine
        .apply(&Command::new(
            namespace.clone(),
            queue.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .unwrap();
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
    let name = SubscriptionName::new("s").unwrap();
    machine
        .apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig::default(),
            },
        ))
        .unwrap();
    let subscription = topic.subscription(&name).unwrap();
    for (target, owner, kind, expected) in [
        (
            queue.clone(),
            queue.clone(),
            EntityBindingKind::Queue,
            "53574450000000010000001801016e017101710001016e0171ffffffffffffffffff0116",
        ),
        (
            queue.dead_letter_queue().unwrap(),
            queue.clone(),
            EntityBindingKind::Queue,
            "53574450000000010000003a01016e12712f24646561646c6574746572717565756501710001016e12712f24646561646c65747465727175657565ffffffffffffffffff0116",
        ),
        (
            topic.clone(),
            topic.clone(),
            EntityBindingKind::Topic,
            "53574450000000010000001801016e017401740101016e0174ffffffffffffffffff0116",
        ),
        (
            subscription.clone(),
            subscription.clone(),
            EntityBindingKind::Subscription,
            "53574450000000010000004801016e11742f737562736372697074696f6e732f7311742f737562736372697074696f6e732f730201016e11742f737562736372697074696f6e732f73ffffffffffffffffff0116",
        ),
        (
            subscription.dead_letter_queue().unwrap(),
            subscription.clone(),
            EntityBindingKind::Subscription,
            "53574450000000010000006a01016e22742f737562736372697074696f6e732f732f24646561646c6574746572717565756511742f737562736372697074696f6e732f730201016e22742f737562736372697074696f6e732f732f24646561646c65747465727175657565ffffffffffffffffff0116",
        ),
    ] {
        let binding = machine.bind_entity(&namespace, &target).unwrap();
        let instruction = Command::new(
            namespace.clone(),
            target.clone(),
            Timestamp::from_millis(u64::MAX),
            CommandKind::ExpireLocks,
        );
        let original =
            DurableProposal::bound(BoundCommand::new(binding.clone(), instruction)).unwrap();
        let encoded = original.encode().unwrap();
        assert_eq!(
            encoded,
            bytes(expected),
            "V1 bound target {}",
            target.as_str()
        );
        let before = store.snapshot().unwrap();
        let mut batch = WriteBatch::default();
        for (key, _) in before.entries() {
            batch.push_delete(key.clone());
        }
        store.apply(batch).unwrap();
        let decoded = DurableProposal::decode(&encoded).unwrap();
        let DurableProposalAuthority::Bound(captured) = decoded.authority() else {
            panic!("bound authority")
        };
        assert_eq!(captured, &binding);
        assert_eq!(captured.namespace(), &namespace);
        assert_eq!(captured.target(), &target);
        assert_eq!(captured.owner(), &owner);
        assert_eq!(captured.kind(), kind);
        assert_eq!(captured.generation(), 1);
        assert_eq!(decoded.command().issued_at.as_millis(), u64::MAX);
        assert!(store.snapshot().unwrap().entries().is_empty());
        // Restore the exact existing catalog only for the next independent capture.
        let mut restore = WriteBatch::default();
        for (key, value) in before.entries() {
            restore.push_put(key.clone(), value.clone());
        }
        store.apply(restore).unwrap();
    }
}

#[test]
fn mismatched_bound_command_scope_is_rejected_before_encoding() {
    let machine = StateMachine::new(MemoryStore::default());
    let original = command(CommandKind::CreateQueue {
        config: QueueConfig::default(),
    });
    machine.apply(&original).unwrap();
    let binding = machine
        .bind_entity(&original.namespace, &original.entity)
        .unwrap();
    for changed_namespace in [false, true] {
        let mut changed = command(CommandKind::ExpireLocks);
        if changed_namespace {
            changed.namespace = NamespaceName::new("other").unwrap();
        } else {
            changed.entity = EntityPath::new("other").unwrap();
        }
        assert_eq!(
            DurableProposal::bound(BoundCommand::new(binding.clone(), changed)),
            Err(DurableProposalError::InvalidAuthority)
        );
    }
}

#[test]
fn decoded_binding_keeps_original_generation_when_the_catalog_has_advanced() {
    #[derive(serde::Serialize)]
    struct ExistingOwnerHead {
        generation: u64,
        kind: u8,
        retired: bool,
    }
    let store = MemoryStore::default();
    let machine = StateMachine::new(store.clone());
    let create = command(CommandKind::CreateQueue {
        config: QueueConfig::default(),
    });
    machine.apply(&create).unwrap();
    let captured = machine
        .bind_entity(&create.namespace, &create.entity)
        .unwrap();
    let proposal = DurableProposal::bound(BoundCommand::new(
        captured.clone(),
        command(CommandKind::ExpireLocks),
    ))
    .unwrap();
    let encoded = proposal.encode().unwrap();
    // Controlled same-kind head drift uses the current, unchanged owner record.
    let mut advance = WriteBatch::default();
    advance.push_put(
        domain::keys::entity_metadata(&create.namespace, &create.entity),
        domain::codec::encode(&ExistingOwnerHead {
            generation: 2,
            kind: 0,
            retired: false,
        })
        .unwrap(),
    );
    store.apply(advance).unwrap();
    assert_eq!(
        machine
            .bind_entity(&create.namespace, &create.entity)
            .unwrap()
            .generation(),
        2
    );
    let before = store.snapshot().unwrap();
    let decoded = DurableProposal::decode(&encoded).unwrap();
    let DurableProposalAuthority::Bound(binding) = decoded.authority() else {
        panic!("captured binding")
    };
    assert_eq!(binding, &captured);
    assert_eq!(binding.generation(), 1);
    assert_eq!(
        machine.validate_binding(binding),
        Err(domain::BrokerError::StaleEntityBinding)
    );
    assert_eq!(store.snapshot().unwrap(), before);
}

#[test]
fn framing_versions_tags_lengths_and_trailing_data_are_strict() {
    let original = DurableProposal::unbound(command(CommandKind::ExpireLocks))
        .encode()
        .unwrap();
    for length in 0..original.len() {
        assert!(DurableProposal::decode(&original[..length]).is_err());
    }
    for index in [0, 1, 2, 3] {
        let mut changed = original.clone();
        changed[index] ^= 1;
        assert_eq!(
            DurableProposal::decode(&changed),
            Err(DurableProposalError::Malformed)
        );
    }
    let mut changed = original.clone();
    changed[7] = 2;
    assert_eq!(
        DurableProposal::decode(&changed),
        Err(DurableProposalError::UnsupportedVersion(2))
    );
    for index in [8, 9, 10, 11] {
        let mut changed = original.clone();
        changed[index] ^= 1;
        assert_eq!(
            DurableProposal::decode(&changed),
            Err(DurableProposalError::Malformed)
        );
    }
    let mut changed = original.clone();
    changed.push(0);
    assert_eq!(
        DurableProposal::decode(&changed),
        Err(DurableProposalError::Malformed)
    );
    changed[11] += 1;
    assert_eq!(
        DurableProposal::decode(&changed),
        Err(DurableProposalError::Malformed)
    );
    for index in [12, 18] {
        let mut changed = original.clone();
        changed[index] = 127;
        assert_eq!(
            DurableProposal::decode(&changed),
            Err(DurableProposalError::Malformed)
        );
    }
}

#[test]
fn overlong_integer_and_length_aliases_are_not_canonical() {
    let original = DurableProposal::unbound(command(CommandKind::ExpireLocks))
        .encode()
        .unwrap();
    for index in [12, 13, 15, 17, 18] {
        let mut changed = original.clone();
        changed[index] |= 128;
        changed.insert(index + 1, 0);
        changed[11] += 1;
        assert_eq!(
            DurableProposal::decode(&changed),
            Err(DurableProposalError::NonCanonical),
            "alias at {index}"
        );
    }
}

#[test]
fn exact_one_mib_envelope_fits_and_one_additional_byte_refuses() {
    fn state(size: usize) -> DurableProposal {
        DurableProposal::unbound(command(CommandKind::SetSessionState {
            session: hold(),
            state: vec![0; size],
        }))
    }
    let overhead = state(128 * 128).encode().unwrap().len() - 128 * 128;
    let payload = MAX_DURABLE_PROPOSAL_BYTES - overhead;
    let maximum = state(payload);
    let encoded = maximum.encode().unwrap();
    assert_eq!(encoded.len(), MAX_DURABLE_PROPOSAL_BYTES);
    assert_eq!(DurableProposal::decode(&encoded).unwrap(), maximum);
    assert_eq!(
        state(payload + 1).encode(),
        Err(DurableProposalError::TooLarge)
    );
    let mut oversized = encoded;
    oversized.push(0);
    assert_eq!(
        DurableProposal::decode(&oversized),
        Err(DurableProposalError::TooLarge)
    );
}

#[test]
fn original_timestamp_extremes_and_semantically_invalid_config_are_preserved() {
    for issued_at in [Timestamp::UNIX_EPOCH, Timestamp::from_millis(u64::MAX)] {
        let mut original = command(CommandKind::CreateQueue {
            config: QueueConfig {
                lock_duration_millis: 0,
                max_delivery_count: 0,
                default_time_to_live_millis: Some(0),
                max_message_bytes: 0,
                requires_session: true,
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 0,
            },
        });
        original.issued_at = issued_at;
        let proposal = DurableProposal::unbound(original.clone());
        assert_eq!(
            DurableProposal::decode(&proposal.encode().unwrap())
                .unwrap()
                .command(),
            &original
        );
        let machine = StateMachine::new(MemoryStore::default());
        assert!(machine.apply(&original).is_err());
    }
}

#[test]
fn oversized_caller_data_and_collection_counts_refuse_before_dto_copying() {
    let excessive = DurableProposal::unbound(command(CommandKind::SetSessionState {
        session: hold(),
        state: vec![0; MAX_DURABLE_PROPOSAL_BYTES + 1],
    }));
    assert_eq!(excessive.encode(), Err(DurableProposalError::TooLarge));
    let excessive = DurableProposal::unbound(command(CommandKind::CancelScheduled {
        sequences: vec![SequenceNumber::new(0); MAX_DURABLE_PROPOSAL_BYTES + 1],
    }));
    assert_eq!(excessive.encode(), Err(DurableProposalError::TooLarge));
}

#[test]
fn ordinary_command_and_generic_value_goldens_remain_unchanged() {
    let original = command(CommandKind::Complete {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(2),
    });
    let expected = bytes("016e0171070b0102");
    assert_eq!(postcard::to_allocvec(&original).unwrap(), expected);
    assert_eq!(
        domain::codec::encode(&original).unwrap(),
        bytes("01016e0171070b0102")
    );
    let proposal = DurableProposal::unbound(original.clone());
    let _ = DurableProposal::decode(&proposal.encode().unwrap()).unwrap();
    assert_eq!(postcard::to_allocvec(&original).unwrap(), expected);
}

#[test]
fn typed_oversized_correlation_value_is_preserved_as_refusal_intent() {
    let oversized = vec![0; domain::MAX_CORRELATION_VALUE_BYTES + 1];
    assert!(CorrelationValue::new(oversized.clone()).is_err());
    // Existing scalar serde permits this typed intent; the proposal must not
    // silently replace that policy with today's public constructor validation.
    let value: CorrelationValue =
        postcard::from_bytes(&postcard::to_allocvec(&oversized).unwrap()).unwrap();
    let filter = RuleFilter::Correlation(CorrelationFilter {
        application_properties: BTreeMap::from([("x".to_owned(), value)]),
        ..CorrelationFilter::default()
    });
    assert!(filter.validate().is_err());
    let original = DurableProposal::unbound(command(CommandKind::CreateRule {
        name: RuleName::new("oversized").unwrap(),
        filter,
    }));
    assert_eq!(
        DurableProposal::decode(&original.encode().unwrap()).unwrap(),
        original
    );
}
