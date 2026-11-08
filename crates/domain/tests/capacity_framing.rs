use domain::{
    BoundCommand, CodecError, Command, CommandKind, DurableProposal, DurableProposalError,
    EntityPath, MAX_DURABLE_PROPOSAL_BYTES, NamespaceName, QUEUE_CAPACITY_GRANULARITY_BYTES,
    QueueCapacity, QueueConfig, StateMachine, Timestamp,
    capacity_fence::{self, CapacityIntent},
    codec, keys,
    retained_accounting::{AccountingError, CapacityOwner},
};

fn owner(generation: u64) -> CapacityOwner {
    CapacityOwner::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        generation,
    )
    .unwrap()
}
fn finite() -> QueueConfig {
    QueueConfig {
        capacity: QueueCapacity::Finite {
            max_size_bytes: QUEUE_CAPACITY_GRANULARITY_BYTES,
        },
        ..QueueConfig::default()
    }
}
fn command(entity: &EntityPath, kind: CommandKind) -> Command {
    Command::new(
        owner(1).namespace().clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        kind,
    )
}

#[test]
fn profile_v4_parent_and_publicly_nonfinite_shadow_are_exact_key_generation_bound() {
    let owner = owner(7);
    for (entity, config) in [
        (owner.parent(), finite()),
        (
            owner.shadow(),
            QueueConfig {
                max_delivery_count: u32::MAX,
                ..QueueConfig::default()
            },
        ),
    ] {
        let inner = codec::queue::encode(&config).unwrap();
        let framed = capacity_fence::encode_profile(&owner, entity, &config).unwrap();
        assert_eq!(framed[0], 4);
        assert_eq!(&framed[1..9], &7_u64.to_be_bytes());
        assert_eq!(&framed[9..13], &(inner.len() as u32).to_be_bytes());
        assert_eq!(&framed[13..13 + inner.len()], inner);
        assert_eq!(framed.len(), inner.len() + 45);
        assert_eq!(
            capacity_fence::decode_profile(&owner, entity, &framed),
            Ok(config)
        );
        assert_eq!(
            codec::queue::decode(&framed),
            Err(CodecError::UnsupportedVersion { version: 4 })
        );
        for index in 0..framed.len() {
            let mut corrupt = framed.clone();
            corrupt[index] ^= 1;
            assert!(
                capacity_fence::decode_profile(&owner, entity, &corrupt).is_err(),
                "byte {index}"
            );
        }
        for cut in 0..framed.len() {
            assert!(capacity_fence::decode_profile(&owner, entity, &framed[..cut]).is_err());
        }
        let other = if entity == owner.parent() {
            owner.shadow()
        } else {
            owner.parent()
        };
        assert!(capacity_fence::decode_profile(&owner, other, &framed).is_err());
        let newer =
            CapacityOwner::new(owner.namespace().clone(), owner.parent().clone(), 8).unwrap();
        assert!(capacity_fence::decode_profile(&newer, entity, &framed).is_err());
        let foreign = CapacityOwner::new(
            NamespaceName::new("other").unwrap(),
            owner.parent().clone(),
            7,
        )
        .unwrap();
        assert!(capacity_fence::decode_profile(&foreign, entity, &framed).is_err());
        let mut trailing = framed;
        trailing.push(0);
        assert!(capacity_fence::decode_profile(&owner, entity, &trailing).is_err());
    }
    assert!(
        capacity_fence::encode_profile(&owner, owner.parent(), &QueueConfig::default()).is_err()
    );
    assert!(capacity_fence::encode_profile(&owner, owner.shadow(), &finite()).is_err());
    for config in [
        QueueConfig {
            requires_session: true,
            ..finite()
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..finite()
        },
    ] {
        assert!(capacity_fence::encode_profile(&owner, owner.parent(), &config).is_err());
    }
}

#[test]
fn isolated_key_codec_rejects_double_frames_malformed_and_unsupported_state() {
    let owner = owner(1);
    for entity in [owner.parent(), owner.shadow()] {
        let key = keys::queue_counters(owner.namespace(), entity);
        let framed = capacity_fence::data_key(&key).unwrap();
        assert_eq!(framed, [&[0x13], key.as_slice()].concat());
        assert_eq!(capacity_fence::original_key(&framed), Ok(key.as_slice()));
        assert!(capacity_fence::data_key(&framed).is_err());
    }
    for bad in [
        vec![],
        vec![0x13],
        vec![2],
        keys::queue_config(owner.namespace(), owner.parent()),
        keys::session(
            owner.namespace(),
            owner.parent(),
            &domain::SessionId::new("s").unwrap(),
        ),
    ] {
        assert_eq!(
            capacity_fence::data_key(&bad),
            Err(AccountingError::InvalidKey)
        );
    }
}

#[test]
fn v11_retains_original_v7_bytes_and_bound_parent_shadow_authority_without_relabeling() {
    let owner = owner(1);
    let machine = StateMachine::new(storage::MemoryStore::default());
    let create = DurableProposal::unbound(command(
        owner.parent(),
        CommandKind::CreateQueue { config: finite() },
    ));
    let historical = create.encode().unwrap();
    assert_eq!(&historical[4..8], &7_u32.to_be_bytes());
    let wrapped = CapacityIntent::capture(owner.clone(), create.clone()).unwrap();
    let bytes = wrapped.encode().unwrap();
    assert!(bytes.ends_with(&historical));
    assert_eq!(CapacityIntent::decode(&bytes), Ok(wrapped));
    assert_eq!(
        DurableProposal::decode(&bytes),
        Err(DurableProposalError::UnsupportedVersion(11))
    );
    assert_eq!(DurableProposal::decode(&historical), Ok(create.clone()));
    machine.apply(create.command()).unwrap();
    for entity in [owner.parent(), owner.shadow()] {
        let binding = machine.bind_entity(owner.namespace(), entity).unwrap();
        let proposal = DurableProposal::bound(BoundCommand::new(
            binding,
            command(entity, CommandKind::ExpireMessages),
        ))
        .unwrap();
        let wrapped = CapacityIntent::capture(owner.clone(), proposal.clone()).unwrap();
        let bytes = wrapped.encode().unwrap();
        assert_eq!(
            CapacityIntent::decode(&bytes).unwrap().encode().unwrap(),
            bytes
        );
        assert_eq!(
            CapacityIntent::capture(super_owner(2), proposal),
            Err(DurableProposalError::InvalidAuthority)
        );
        let mut zero = bytes.clone();
        zero[12..20].fill(0);
        assert!(CapacityIntent::decode(&zero).is_err());
        let mut stale = bytes.clone();
        stale[12..20].copy_from_slice(&2_u64.to_be_bytes());
        assert!(CapacityIntent::decode(&stale).is_err());
        let mut nested = bytes.clone();
        let inner_start = 24 + owner.namespace().as_str().len() + owner.parent().as_str().len();
        nested[inner_start + 4..inner_start + 8].copy_from_slice(&11_u32.to_be_bytes());
        assert_eq!(
            CapacityIntent::decode(&nested),
            Err(DurableProposalError::UnsupportedVersion(11))
        );
        nested[inner_start + 4..inner_start + 8].copy_from_slice(&12_u32.to_be_bytes());
        assert_eq!(
            CapacityIntent::decode(&nested),
            Err(DurableProposalError::UnsupportedVersion(12))
        );
        for cut in 0..bytes.len() {
            assert!(CapacityIntent::decode(&bytes[..cut]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(CapacityIntent::decode(&trailing).is_err());
    }
    let foreign = DurableProposal::unbound(Command::new(
        NamespaceName::new("other").unwrap(),
        owner.parent().clone(),
        Timestamp::from_millis(1),
        CommandKind::ExpireMessages,
    ));
    assert!(CapacityIntent::capture(owner.clone(), foreign).is_err());
    let wrong_target = DurableProposal::unbound(command(
        &EntityPath::new("other").unwrap(),
        CommandKind::ExpireMessages,
    ));
    assert!(CapacityIntent::capture(owner, wrong_target).is_err());
}

fn super_owner(generation: u64) -> CapacityOwner {
    owner(generation)
}

fn large_intent(body_len: usize) -> CapacityIntent {
    let owner = owner(1);
    let original = DurableProposal::unbound(command(
        owner.parent(),
        CommandKind::Send {
            message_id: "id".into(),
            body: vec![0; body_len],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    ));
    CapacityIntent::capture(owner, original).unwrap()
}

#[test]
fn v11_limit_includes_every_outer_and_inner_byte() {
    let body = MAX_DURABLE_PROPOSAL_BYTES - 256;
    let encoded = large_intent(body).encode().unwrap();
    let exact_body = body + MAX_DURABLE_PROPOSAL_BYTES - encoded.len();
    let exact = large_intent(exact_body).encode().unwrap();
    assert_eq!(exact.len(), MAX_DURABLE_PROPOSAL_BYTES);
    assert_eq!(
        CapacityIntent::decode(&exact).unwrap().encode().unwrap(),
        exact
    );
    assert_eq!(
        large_intent(exact_body + 1).encode(),
        Err(DurableProposalError::TooLarge)
    );
    let mut oversized = exact;
    oversized.push(0);
    assert_eq!(
        CapacityIntent::decode(&oversized),
        Err(DurableProposalError::TooLarge)
    );
}
