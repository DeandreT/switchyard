use super::*;
use storage::MemoryStore;

fn fixture() -> Result<(StateMachine<MemoryStore>, NamespaceName, EntityPath), BrokerError> {
    Ok((
        StateMachine::new(MemoryStore::default()),
        NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
    ))
}

fn apply(
    machine: &StateMachine<MemoryStore>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    at: u64,
    kind: CommandKind,
) -> Result<CommandOutcome, BrokerError> {
    machine.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(at),
        kind,
    ))
}

fn create_queue(
    machine: &StateMachine<MemoryStore>,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<EntityBinding, BrokerError> {
    apply(
        machine,
        namespace,
        entity,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    machine
        .bind_entity(namespace, entity, entity, EntityIncarnationKind::Queue)?
        .ok_or(BrokerError::QueueNotFound)
}

#[test]
fn replacement_changes_identity_before_a_regressed_replay_can_mutate() -> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    let binding = create_queue(&machine, &namespace, &entity)?;
    let legacy = Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::Send {
            message_id: "old".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
    );
    apply(
        &machine,
        &namespace,
        &entity,
        2,
        CommandKind::DeleteEntity {
            target: crate::DeleteEntityTarget::Queue,
        },
    )?;
    let retired = machine
        .entity_incarnation(&namespace, &entity)?
        .ok_or(BrokerError::DanglingEntityMetadata)?;
    assert_eq!(retired.generation(), 1);
    assert!(retired.is_retired());
    apply(
        &machine,
        &namespace,
        &entity,
        3,
        CommandKind::CreateTopic {
            config: crate::TopicConfig::default(),
        },
    )?;
    let replacement = machine
        .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Topic)?
        .ok_or(BrokerError::TopicNotFound)?;
    assert_eq!(replacement.generation(), 2);
    let before = machine.store.snapshot()?;
    assert_eq!(
        machine.apply_fenced(&FencedCommand::new(binding, legacy)),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(machine.store.snapshot()?, before);
    Ok(())
}

#[test]
fn child_and_shadow_bindings_have_independent_physical_authority() -> Result<(), BrokerError> {
    let (machine, namespace, topic) = fixture()?;
    let name = crate::SubscriptionName::new("Alpha")?;
    apply(
        &machine,
        &namespace,
        &topic,
        1,
        CommandKind::CreateTopic {
            config: crate::TopicConfig::default(),
        },
    )?;
    apply(
        &machine,
        &namespace,
        &topic,
        2,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: crate::SubscriptionConfig::default(),
        },
    )?;
    let child = topic.subscription(&name)?;
    let shadow = child.dead_letter_queue()?;
    let binding = machine
        .bind_entity(
            &namespace,
            &child,
            &child,
            EntityIncarnationKind::Subscription,
        )?
        .ok_or(BrokerError::SubscriptionNotFound)?;
    let dead_letter = machine
        .bind_entity(
            &namespace,
            &shadow,
            &child,
            EntityIncarnationKind::Subscription,
        )?
        .ok_or(BrokerError::SubscriptionNotFound)?;
    assert_eq!(binding.generation(), dead_letter.generation());
    assert_eq!(machine.entity_incarnation(&namespace, &shadow)?, None);
    let before = machine.store.snapshot()?;
    assert_eq!(
        machine
            .rules_fenced(&binding, &namespace, &topic, &name)?
            .len(),
        1
    );
    assert_eq!(
        machine.rules_fenced(&dead_letter, &namespace, &topic, &name),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        machine.validate_fenced_intent(
            &binding,
            &namespace,
            &shadow,
            &CommandKind::CancelScheduled {
                sequences: Vec::new()
            }
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(machine.store.snapshot()?, before);
    apply(
        &machine,
        &namespace,
        &topic,
        3,
        CommandKind::DeleteEntity {
            target: crate::DeleteEntityTarget::Subscription { name: name.clone() },
        },
    )?;
    apply(
        &machine,
        &namespace,
        &topic,
        4,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: crate::SubscriptionConfig::default(),
        },
    )?;
    assert_eq!(
        machine.rules_fenced(&binding, &namespace, &topic, &name),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(
        machine
            .entity_incarnation(&namespace, &topic)?
            .map(EntityIncarnation::generation),
        Some(1)
    );
    assert_eq!(
        machine
            .entity_incarnation(&namespace, &child)?
            .map(EntityIncarnation::generation),
        Some(2)
    );
    Ok(())
}

#[test]
fn exceptional_replay_paths_distinguish_replacement_from_live_corruption() -> Result<(), BrokerError>
{
    let (machine, namespace, entity) = fixture()?;
    let binding = create_queue(&machine, &namespace, &entity)?;
    let command = FencedCommand::new(
        binding.clone(),
        Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(2),
            CommandKind::CancelScheduled {
                sequences: Vec::new(),
            },
        ),
    );
    let key = keys::entity_incarnation(&namespace, &entity);
    for record in [
        EntityIncarnation::new(1, EntityIncarnationKind::Queue, true)?,
        EntityIncarnation::new(1, EntityIncarnationKind::Topic, false)?,
    ] {
        machine
            .store
            .apply(WriteBatch::default().put(key.clone(), codec::encode(&record)?))?;
        let before = machine.store.snapshot()?;
        assert_eq!(
            machine.apply_fenced(&command),
            Err(BrokerError::DanglingEntityMetadata)
        );
        assert_eq!(
            machine.bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue),
            Err(BrokerError::DanglingEntityMetadata)
        );
        assert_eq!(machine.store.snapshot()?, before);
    }
    machine.store.apply(WriteBatch::default().put(
        key.clone(),
        codec::encode(&EntityIncarnation::new(
            2,
            EntityIncarnationKind::Queue,
            false,
        )?)?,
    ))?;
    assert_eq!(
        machine.apply_fenced(&command),
        Err(BrokerError::EntityBindingStale)
    );
    machine.store.apply(WriteBatch::default().delete(key))?;
    assert_eq!(
        machine.apply_fenced(&command),
        Err(BrokerError::DanglingEntityMetadata)
    );
    Ok(())
}

#[test]
fn missing_primary_and_child_deletion_refuse_live_orphan_identities() -> Result<(), BrokerError> {
    let (machine, namespace, topic) = fixture()?;
    let name = crate::SubscriptionName::new("Alpha")?;
    let record = EntityIncarnation::new(1, EntityIncarnationKind::Topic, false)?;
    machine.store.apply(WriteBatch::default().put(
        keys::entity_incarnation(&namespace, &topic),
        codec::encode(&record)?,
    ))?;
    let before = machine.store.snapshot()?;
    assert_eq!(
        apply(
            &machine,
            &namespace,
            &topic,
            1,
            CommandKind::DeleteEntity {
                target: crate::DeleteEntityTarget::Topic
            }
        ),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(machine.store.snapshot()?, before);
    machine.store.apply(WriteBatch::default().put(
        keys::entity_incarnation(&namespace, &topic),
        codec::encode(&record.retire())?,
    ))?;
    apply(
        &machine,
        &namespace,
        &topic,
        1,
        CommandKind::CreateTopic {
            config: crate::TopicConfig::default(),
        },
    )?;
    let child = topic.subscription(&name)?;
    machine.store.apply(WriteBatch::default().put(
        keys::entity_incarnation(&namespace, &child),
        codec::encode(&EntityIncarnation::new(
            1,
            EntityIncarnationKind::Subscription,
            false,
        )?)?,
    ))?;
    let before = machine.store.snapshot()?;
    assert_eq!(
        apply(
            &machine,
            &namespace,
            &topic,
            2,
            CommandKind::DeleteEntity {
                target: crate::DeleteEntityTarget::Subscription { name }
            }
        ),
        Err(BrokerError::DanglingEntityMetadata)
    );
    assert_eq!(machine.store.snapshot()?, before);
    Ok(())
}

#[test]
fn exhausted_and_malformed_records_never_mint_replacement_identity() -> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    let key = keys::entity_incarnation(&namespace, &entity);
    let exhausted = EntityIncarnation::new(u64::MAX, EntityIncarnationKind::Queue, true)?;
    machine
        .store
        .apply(WriteBatch::default().put(key.clone(), codec::encode(&exhausted)?))?;
    let before = machine.store.snapshot()?;
    assert_eq!(
        apply(
            &machine,
            &namespace,
            &entity,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Err(BrokerError::EntityIncarnationExhausted)
    );
    assert_eq!(machine.store.snapshot()?, before);
    machine
        .store
        .apply(WriteBatch::default().put(key, vec![0xff]))?;
    let before = machine.store.snapshot()?;
    assert!(matches!(
        apply(
            &machine,
            &namespace,
            &entity,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Err(BrokerError::Codec(_))
    ));
    assert_eq!(machine.store.snapshot()?, before);
    Ok(())
}

#[test]
fn identity_guards_do_not_become_operation_permission_filters() -> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    let binding = create_queue(&machine, &namespace, &entity)?;
    let before = machine.store.snapshot()?;
    for kind in [
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
        CommandKind::DeleteEntity {
            target: crate::DeleteEntityTarget::Auto,
        },
        CommandKind::ActivateScheduled,
    ] {
        machine.validate_fenced_intent(&binding, &namespace, &entity, &kind)?;
    }
    assert_eq!(machine.store.snapshot()?, before);
    Ok(())
}
