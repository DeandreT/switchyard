use domain::{
    Command, CommandKind, EntityPath, MessageInput, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{
        SnapshotCompleteStateError, SnapshotDuplicateError, validate_catalog,
        validate_complete_state, validate_duplicate_rows, validate_message_rows,
    },
};
use storage::{Key, StateStore, Value};
use testkit::StoreProvider;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn with_rows(base: &[(Key, Value)], extra: Vec<(Key, Value)>) -> Vec<(Key, Value)> {
    let mut map: std::collections::BTreeMap<_, _> = base.iter().cloned().collect();
    map.extend(extra);
    map.into_iter().collect()
}
fn controls<P: StoreProvider>(provider: P) -> TestResult {
    let ns = NamespaceName::new("n")?;
    let topic = EntityPath::new("t")?;
    let queue = EntityPath::new("q")?;
    let disabled = EntityPath::new("disabled")?;
    let machine = StateMachine::new(provider.open()?);
    let apply = |entity: &EntityPath, at, kind| {
        machine.apply(&Command::new(
            ns.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    };
    apply(
        &topic,
        0,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 20_000,
                ..TopicConfig::default()
            },
        },
    )?;
    apply(
        &disabled,
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    apply(
        &queue,
        0,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                ..QueueConfig::default()
            },
        },
    )?;
    apply(
        &topic,
        1,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("s")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    let child = topic.subscription(&SubscriptionName::new("s")?)?;
    for entity in [&topic, &queue] {
        apply(
            entity,
            2,
            CommandKind::SendBatch {
                messages: vec![MessageInput {
                    message_id: "ID".into(),
                    ..MessageInput::default()
                }],
            },
        )?;
    }
    apply(
        &topic,
        3,
        CommandKind::SendBatch {
            messages: vec![MessageInput {
                message_id: "ID".into(),
                ..MessageInput::default()
            }],
        },
    )?;
    let base = machine.store().snapshot()?.entries().to_vec();
    let partial = validate_message_rows(&base)?;
    assert_eq!(partial.pending_duplicate_rows(), 4);
    assert!(!partial.allocation_relations_checked());
    let duplicate = validate_duplicate_rows(&base)?;
    assert_eq!(duplicate.lookup_rows(), 2);
    assert!(!duplicate.allocation_relations_checked());
    let complete = validate_complete_state(&base)?;
    assert_eq!(complete.lookup_rows(), 2);
    assert!(complete.allocation_relations_checked());
    // ACK 2 is a legal gap, not a missing message or allocation error.
    assert!(
        machine
            .message(&ns, &child, SequenceNumber::new(2))?
            .is_none()
    );
    for owner in [
        &disabled,
        &child,
        &child.dead_letter_queue()?,
        &queue.dead_letter_queue()?,
        &EntityPath::new("absent")?,
    ] {
        let key = keys::duplicate_id(&ns, owner, "x");
        let rows = with_rows(
            &base,
            vec![
                (key.clone(), codec::encode(&Timestamp::from_millis(20_000))?),
                (
                    keys::duplicate_expiry(&ns, owner, Timestamp::from_millis(20_000), "x"),
                    vec![],
                ),
            ],
        );
        let ordinal = rows.iter().position(|(k, _)| k == &key).unwrap();
        assert!(
            matches!(validate_duplicate_rows(&rows), Err(SnapshotDuplicateError::InconsistentDuplicate { row, .. }) if row == ordinal)
        );
        assert!(
            matches!(validate_complete_state(&rows), Err(SnapshotCompleteStateError::Duplicate(SnapshotDuplicateError::InconsistentDuplicate { row, .. })) if row == ordinal)
        );
    }
    let stale = with_rows(
        &base,
        vec![(
            keys::duplicate_expiry(&ns, &topic, Timestamp::from_millis(1), "ID"),
            vec![],
        )],
    );
    assert_eq!(
        validate_complete_state(&stale)?.stale_compatibility_rows(),
        1
    );
    let missing = base
        .iter()
        .filter(|(k, _)| {
            k != &keys::duplicate_expiry(&ns, &topic, Timestamp::from_millis(20_002), "ID")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(validate_duplicate_rows(&missing).is_err());
    let bad_message = with_rows(
        &missing,
        vec![(keys::ready(&ns, &child, SequenceNumber::new(99)), vec![])],
    );
    assert!(matches!(
        validate_complete_state(&bad_message),
        Err(SnapshotCompleteStateError::State(_))
    ));
    let mut corrupt = codec::topic::encode(&TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    })?;
    corrupt.push(0);
    let bad_catalog = with_rows(
        &bad_message,
        vec![(keys::topic_config(&ns, &topic), corrupt)],
    );
    assert!(validate_catalog(&bad_catalog).is_err());
    assert!(matches!(
        validate_complete_state(&bad_catalog),
        Err(SnapshotCompleteStateError::State(_))
    ));
    assert_eq!(machine.store().snapshot()?.entries(), base.as_slice());
    Ok(())
}
#[test]
fn memory() -> TestResult {
    controls(testkit::MemoryProvider::new())
}
#[test]
fn fjall() -> TestResult {
    controls(testkit::DurableProvider::temporary()?)
}
