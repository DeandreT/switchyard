use super::*;
use storage::MemoryStore;

#[test]
fn duplicate_keys_are_charged_once_and_key_count_has_an_exact_boundary() -> Result<(), BrokerError>
{
    let mut plan = DeletionPlan::default();
    for value in 0..MAX_ENTITY_DELETE_KEYS as u64 {
        let key = value.to_be_bytes().to_vec();
        plan.add(key.clone())?;
        plan.add(key)?;
    }
    assert_eq!(plan.keys.len(), MAX_ENTITY_DELETE_KEYS);
    assert_eq!(plan.key_bytes, MAX_ENTITY_DELETE_KEYS * 8);
    assert_eq!(
        plan.add((MAX_ENTITY_DELETE_KEYS as u64).to_be_bytes().to_vec()),
        Err(delete_limit(
            EntityDeleteLimit::Keys,
            MAX_ENTITY_DELETE_KEYS
        ))
    );
    assert_eq!(plan.keys.len(), MAX_ENTITY_DELETE_KEYS);
    Ok(())
}

#[test]
fn key_bytes_are_checked_before_retaining_the_candidate() -> Result<(), BrokerError> {
    let mut plan = DeletionPlan::default();
    plan.add(vec![0; MAX_ENTITY_DELETE_KEY_BYTES])?;
    assert_eq!(plan.key_bytes, MAX_ENTITY_DELETE_KEY_BYTES);
    assert_eq!(
        plan.add(vec![1]),
        Err(delete_limit(
            EntityDeleteLimit::KeyBytes,
            MAX_ENTITY_DELETE_KEY_BYTES
        ))
    );
    assert_eq!(plan.keys.len(), 1);
    assert_eq!(plan.key_bytes, MAX_ENTITY_DELETE_KEY_BYTES);
    Ok(())
}

#[test]
fn scan_value_bytes_charge_repeated_rows_and_reject_checked_overflow() -> Result<(), BrokerError> {
    let mut plan = DeletionPlan::default();
    plan.charge_values(MAX_ENTITY_DELETE_VALUE_BYTES / 2)?;
    plan.charge_values(MAX_ENTITY_DELETE_VALUE_BYTES / 2)?;
    for extra in [1, usize::MAX] {
        assert_eq!(
            plan.charge_values(extra),
            Err(delete_limit(
                EntityDeleteLimit::ValueBytes,
                MAX_ENTITY_DELETE_VALUE_BYTES
            ))
        );
        assert_eq!(plan.value_bytes, MAX_ENTITY_DELETE_VALUE_BYTES);
    }
    Ok(())
}

fn fixture() -> Result<(StateMachine<MemoryStore>, NamespaceName, EntityPath), BrokerError> {
    Ok((
        StateMachine::new(MemoryStore::default()),
        NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
    ))
}

#[test]
fn unused_queue_deletes_without_creating_counters_and_reports_committed_scopes()
-> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    machine.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ))?;
    let application = machine.apply_with_effects(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(2),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Auto,
        },
    ))?;
    assert_eq!(application.outcome, CommandOutcome::QueueDeleted);
    assert_eq!(
        application.entity_deletions,
        Some(vec![entity.clone(), entity.dead_letter_queue()?])
    );
    assert_eq!(application.subscription_enqueues, None);
    assert!(!application.dead_letters_enqueued);
    assert_eq!(
        machine
            .store
            .get(&keys::queue_counters(&namespace, &entity))?,
        None
    );
    assert_eq!(machine.last_applied_time()?, Timestamp::from_millis(2));
    Ok(())
}

#[test]
fn counter_validation_preserves_exhaustion_and_refuses_invalid_numeric_fences()
-> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    machine.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ))?;
    let counter_key = keys::queue_counters(&namespace, &entity);
    let delete = Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(2),
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    );
    for counters in [
        QueueCounters {
            next_sequence: 0,
            next_lock_token: 1,
        },
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 2,
            next_lock_token: 1,
        },
        QueueCounters {
            next_sequence: 1,
            next_lock_token: 0,
        },
    ] {
        machine
            .store
            .apply(WriteBatch::default().put(counter_key.clone(), codec::encode(&counters)?))?;
        let before = machine.store.snapshot()?;
        assert_eq!(
            machine.apply(&delete),
            Err(BrokerError::DanglingEntityMetadata)
        );
        assert_eq!(machine.store.snapshot()?, before);
    }
    let counters = QueueCounters {
        next_sequence: MAX_SEQUENCE_NUMBER + 1,
        next_lock_token: u64::MAX,
    };
    let bytes = codec::encode(&counters)?;
    machine
        .store
        .apply(WriteBatch::default().put(counter_key.clone(), bytes.clone()))?;
    assert_eq!(machine.apply(&delete)?, CommandOutcome::QueueDeleted);
    assert_eq!(machine.store.get(&counter_key)?, Some(bytes));
    Ok(())
}

#[test]
fn empty_scan_values_still_bound_descendant_discovery_by_unique_keys() -> Result<(), BrokerError> {
    let (machine, namespace, entity) = fixture()?;
    machine.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateTopic {
            config: crate::TopicConfig::default(),
        },
    ))?;
    let name = SubscriptionName::new("Alpha")?;
    machine.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: crate::SubscriptionConfig::default(),
        },
    ))?;
    let child = entity.subscription(&name)?;
    let mut batch = WriteBatch::default();
    for sequence in 0..=MAX_ENTITY_DELETE_KEYS as u64 {
        batch.push_put(
            keys::ready(&namespace, &child, SequenceNumber::new(sequence)),
            Vec::new(),
        );
    }
    machine.store.apply(batch)?;
    let before = machine.store.snapshot()?;
    assert_eq!(
        machine.apply(&Command::new(
            namespace,
            entity,
            Timestamp::from_millis(2),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic
            }
        )),
        Err(delete_limit(
            EntityDeleteLimit::Keys,
            MAX_ENTITY_DELETE_KEYS
        ))
    );
    assert_eq!(machine.store.snapshot()?, before);
    Ok(())
}

#[test]
fn full_length_topics_delete_without_requiring_an_unrepresentable_shadow() -> Result<(), BrokerError>
{
    for length in [
        crate::MAX_ENTITY_PATH_BYTES - crate::DEAD_LETTER_QUEUE_SUFFIX.len() + 1,
        crate::MAX_ENTITY_PATH_BYTES,
    ] {
        let (machine, namespace, _) = fixture()?;
        let entity = EntityPath::new("t".repeat(length))?;
        assert!(entity.dead_letter_queue().is_err());
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(1),
            CommandKind::CreateTopic {
                config: crate::TopicConfig::default(),
            },
        ))?;
        let application = machine.apply_with_effects(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(2),
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Topic,
            },
        ))?;
        assert_eq!(application.outcome, CommandOutcome::TopicDeleted);
        assert_eq!(application.entity_deletions, Some(vec![entity.clone()]));
        assert_eq!(machine.topic_config(&namespace, &entity)?, None);
        let before = machine.store.snapshot()?;
        for (target, expected) in [
            (DeleteEntityTarget::Auto, BrokerError::QueueNotFound),
            (DeleteEntityTarget::Queue, BrokerError::QueueNotFound),
            (DeleteEntityTarget::Topic, BrokerError::TopicNotFound),
        ] {
            assert_eq!(
                machine.apply(&Command::new(
                    namespace.clone(),
                    entity.clone(),
                    Timestamp::from_millis(3),
                    CommandKind::DeleteEntity { target },
                )),
                Err(expected)
            );
            assert_eq!(machine.store.snapshot()?, before);
        }
    }
    Ok(())
}

#[test]
fn deletion_command_appends_without_changing_existing_patch_ordinals()
-> Result<(), Box<dyn std::error::Error>> {
    for (target, suffix) in [
        (DeleteEntityTarget::Auto, vec![0]),
        (DeleteEntityTarget::Queue, vec![1]),
        (DeleteEntityTarget::Topic, vec![2]),
        (
            DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
            vec![3, 5, b'A', b'l', b'p', b'h', b'a'],
        ),
    ] {
        let kind = CommandKind::DeleteEntity { target };
        let mut expected = vec![36];
        expected.extend(suffix);
        assert_eq!(postcard::to_stdvec(&kind)?, expected);
        assert_eq!(postcard::from_bytes::<CommandKind>(&expected)?, kind);
    }
    assert_eq!(
        postcard::to_stdvec(&CommandKind::UpdateTopic {
            update: crate::TopicConfigUpdate::default()
        })?
        .first(),
        Some(&34)
    );
    assert_eq!(
        postcard::to_stdvec(&CommandKind::UpdateSubscription {
            name: SubscriptionName::new("Alpha")?,
            update: crate::SubscriptionConfigUpdate::default()
        })?
        .first(),
        Some(&35)
    );
    Ok(())
}
