use super::*;

fn unique_deleted_key_limit_is_exact_and_overflow_keeps_every_key<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = observed_queue(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let retained_counter = QueueCounters {
        next_sequence: MAX_ENTITY_DELETE_KEYS as u64 + 1,
        next_lock_token: 1,
    };
    let mut batch = WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&retained_counter)?,
    );
    for index in 0..MAX_ENTITY_DELETE_KEYS - 2 {
        batch.push_put(
            keys::duplicate_history(
                &fixture.namespace,
                &fixture.entity,
                &format!("id{index:04}"),
            ),
            codec::encode(&Timestamp::from_millis(10_000))?,
        );
    }
    let overflow = keys::duplicate_history(&fixture.namespace, &fixture.entity, "overflow");
    batch.push_put(
        overflow.clone(),
        codec::encode(&Timestamp::from_millis(10_000))?,
    );
    fixture.machine.store().apply(batch)?;
    reset(&fixture);
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        BrokerError::EntityDeleteTooLarge {
            limit: EntityDeleteLimit::Keys,
            maximum: MAX_ENTITY_DELETE_KEYS,
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(overflow))?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    let paths = vec![fixture.entity.clone(), fixture.entity.dead_letter_queue()?];
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 20)?;
    assert_eq!(counter(&fixture, &fixture.entity)?, retained_counter);
    let observed = fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations");
    assert_eq!(observed.deleted.len(), MAX_ENTITY_DELETE_KEYS);
    assert_eq!(
        observed
            .deleted
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        MAX_ENTITY_DELETE_KEYS
    );
    assert!(observed.scans.iter().all(|(_, limit, _)| *limit == 1));
    Ok(())
}

fn unique_key_byte_limit_counts_metadata_and_rejects_one_more_byte<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed_queue(provider, QueueConfig::default())?;
    fixture.namespace = NamespaceName::new("n".repeat(domain::MAX_NAMESPACE_NAME_BYTES))?;
    fixture.entity = EntityPath::new(
        "q".repeat(domain::MAX_ENTITY_PATH_BYTES - domain::DEAD_LETTER_QUEUE_SUFFIX.len()),
    )?;
    fixture.at(
        0,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                ..QueueConfig::default()
            },
        },
    )?;
    let shadow = fixture.entity.dead_letter_queue()?;
    let metadata_bytes = keys::queue_config(&fixture.namespace, &fixture.entity).len()
        + keys::queue_config(&fixture.namespace, &shadow).len();
    let prefix = keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity);
    let full_size = prefix.len() + domain::MAX_MESSAGE_ID_LENGTH;
    let available = MAX_ENTITY_DELETE_KEY_BYTES - metadata_bytes;
    let full_count = available / full_size;
    let remainder = available % full_size;
    let minimum = prefix.len() + 1;
    let mut shortening = minimum.saturating_sub(remainder);
    let mut runtime = Vec::new();
    for index in 0..full_count {
        let shrink = shortening.min(domain::MAX_MESSAGE_ID_LENGTH - 4);
        shortening -= shrink;
        let mut id = format!("{index:04}");
        id.push_str(&"x".repeat(domain::MAX_MESSAGE_ID_LENGTH - shrink - id.len()));
        runtime.push(keys::duplicate_history(
            &fixture.namespace,
            &fixture.entity,
            &id,
        ));
    }
    assert_eq!(shortening, 0);
    let last_size = if remainder < minimum {
        minimum
    } else {
        remainder
    };
    runtime.push(keys::duplicate_history(
        &fixture.namespace,
        &fixture.entity,
        &"z".repeat(last_size - prefix.len()),
    ));
    assert_eq!(
        runtime.iter().map(Vec::len).sum::<usize>() + metadata_bytes,
        MAX_ENTITY_DELETE_KEY_BYTES
    );
    assert!(runtime.len() + 2 < MAX_ENTITY_DELETE_KEYS);
    let value = codec::encode(&Timestamp::from_millis(10_000))?;
    let mut batch = WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: runtime.len() as u64 + 1,
            next_lock_token: 1,
        })?,
    );
    for key in &runtime {
        batch.push_put(key.clone(), value.clone());
    }
    fixture.machine.store().apply(batch)?;
    let original = runtime
        .iter()
        .find(|key| key.len() < full_size)
        .expect("shortened boundary key")
        .clone();
    let mut overflow = original.clone();
    overflow.push(b'x');
    fixture.machine.store().apply(
        WriteBatch::default()
            .delete(original.clone())
            .put(overflow.clone(), value.clone()),
    )?;
    reset(&fixture);
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        BrokerError::EntityDeleteTooLarge {
            limit: EntityDeleteLimit::KeyBytes,
            maximum: MAX_ENTITY_DELETE_KEY_BYTES,
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(overflow).put(original, value))?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    let paths = vec![fixture.entity.clone(), shadow];
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 20)?;
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .deleted
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        MAX_ENTITY_DELETE_KEY_BYTES
    );
    Ok(())
}

fn scanned_value_limit_is_exact_and_one_byte_overflow_is_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = observed_queue(provider, QueueConfig::default())?;
    let count = 16;
    let chunk = MAX_ENTITY_DELETE_VALUE_BYTES / count;
    let mut batch = WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueCounters {
            next_sequence: count as u64 + 1,
            next_lock_token: 1,
        })?,
    );
    for sequence in 1..=count {
        batch.push_put(
            keys::message(
                &fixture.namespace,
                &fixture.entity,
                SequenceNumber::new(sequence as u64),
            ),
            vec![b'x'; chunk],
        );
    }
    let first = keys::message(&fixture.namespace, &fixture.entity, SequenceNumber::new(1));
    batch.push_put(first.clone(), vec![b'x'; chunk + 1]);
    fixture.machine.store().apply(batch)?;
    reset(&fixture);
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        BrokerError::EntityDeleteTooLarge {
            limit: EntityDeleteLimit::ValueBytes,
            maximum: MAX_ENTITY_DELETE_VALUE_BYTES,
        },
    )?;
    {
        let observed = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(observed.commits, 0);
        assert_eq!(
            observed
                .scans
                .iter()
                .map(|(_, _, bytes)| bytes)
                .sum::<usize>(),
            MAX_ENTITY_DELETE_VALUE_BYTES + 1
        );
    }
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(first, vec![b'x'; chunk]))?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    let paths = vec![fixture.entity.clone(), fixture.entity.dead_letter_queue()?];
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 20)?;
    let observed = fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations");
    assert_eq!(
        observed
            .scans
            .iter()
            .map(|(_, _, bytes)| bytes)
            .sum::<usize>(),
        MAX_ENTITY_DELETE_VALUE_BYTES
    );
    assert!(observed.scans.iter().all(|(_, limit, _)| *limit == 1));
    Ok(())
}

fn topic_budget_includes_repeated_discovery_and_is_shared_across_children<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let bytes = 5 * 1024 * 1024;
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(
                keys::queue_counters(&fixture.namespace, &fixture.entity),
                codec::encode(&QueueCounters {
                    next_sequence: 2,
                    next_lock_token: 1,
                })?,
            )
            .put(
                keys::message(&fixture.namespace, &alpha, SequenceNumber::new(1)),
                vec![b'x'; bytes],
            )
            .put(
                keys::message(&fixture.namespace, &beta, SequenceNumber::new(1)),
                vec![b'x'; bytes],
            ),
    )?;
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
        BrokerError::EntityDeleteTooLarge {
            limit: EntityDeleteLimit::ValueBytes,
            maximum: MAX_ENTITY_DELETE_VALUE_BYTES,
        },
    )?;
    let before = fixture.machine.store().snapshot()?;
    let name = SubscriptionName::new("Alpha")?;
    let paths = vec![alpha.clone(), alpha.dead_letter_queue()?];
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Subscription { name: name.clone() },
        CommandOutcome::SubscriptionDeleted,
        &paths,
    )?;
    assert_exact_purge(
        &fixture,
        &before,
        &paths,
        &[keys::rule_prefix(
            &fixture.namespace,
            &fixture.entity,
            &name,
        )],
        &[keys::subscription(
            &fixture.namespace,
            &fixture.entity,
            &name,
        )],
        20,
    )?;
    let before = fixture.machine.store().snapshot()?;
    let paths = vec![
        fixture.entity.clone(),
        beta.clone(),
        beta.dead_letter_queue()?,
    ];
    delete(
        &fixture,
        21,
        DeleteEntityTarget::Topic,
        CommandOutcome::TopicDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 21)?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    unique_deleted_key_limit_is_exact_and_overflow_keeps_every_key,
    unique_key_byte_limit_counts_metadata_and_rejects_one_more_byte,
    scanned_value_limit_is_exact_and_one_byte_overflow_is_atomic,
    topic_budget_includes_repeated_discovery_and_is_shared_across_children,
}
