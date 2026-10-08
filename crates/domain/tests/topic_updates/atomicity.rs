use super::*;

fn immutable_properties_reject_both_directions_but_allow_identical_restatements<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for (index, (duplicates, sessions)) in
        [(false, false), (false, true), (true, false), (true, true)]
            .into_iter()
            .enumerate()
    {
        let base = 10 + index as u64 * 10;
        fixture.entity = EntityPath::new(format!("case-{index}"))?;
        fixture.at(
            base,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: duplicates,
                    ..TopicConfig::default()
                },
            },
        )?;
        subscribe(
            &fixture,
            "Alpha",
            SubscriptionConfig {
                requires_session: sessions,
                ..SubscriptionConfig::default()
            },
            base,
        )?;
        reject(
            &fixture,
            base + 1,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    requires_duplicate_detection: Some(!duplicates),
                    max_message_bytes: Some(0),
                    ..TopicConfigUpdate::default()
                },
            },
            BrokerError::TopicPropertyIsImmutable {
                property: TopicImmutableProperty::RequiresDuplicateDetection,
            },
        )?;
        reject(
            &fixture,
            base + 1,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("Alpha")?,
                update: SubscriptionConfigUpdate {
                    requires_session: Some(!sessions),
                    lock_duration_millis: Some(0),
                    dead_lettering_on_filter_evaluation_exceptions: Some(false),
                    ..SubscriptionConfigUpdate::default()
                },
            },
            BrokerError::SubscriptionPropertyIsImmutable {
                property: SubscriptionImmutableProperty::RequiresSession,
            },
        )?;
        topic_update(
            &fixture,
            base + 1,
            TopicConfigUpdate {
                requires_duplicate_detection: Some(duplicates),
                max_message_bytes: Some(128),
                ..TopicConfigUpdate::default()
            },
        )?;
        subscription_update(
            &fixture,
            "Alpha",
            base + 1,
            SubscriptionConfigUpdate {
                requires_session: Some(sessions),
                max_delivery_count: Some(4),
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        )?;
        assert_eq!(
            topic_config(&fixture)?.requires_duplicate_detection,
            duplicates
        );
        assert_eq!(
            subscription_config(&fixture, "Alpha")?.requires_session,
            sessions
        );
        assert_eq!(
            subscription_config(&fixture, "Alpha")?.max_delivery_count,
            4
        );
        assert_projections(&fixture, "Alpha")?;
    }
    Ok(())
}

fn invalid_partial_configuration_rolls_back_all_fields_and_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    send(&fixture, 10, "retained", None, None)?;
    for (patch, error) in [
        (
            TopicConfigUpdate {
                max_message_bytes: Some(0),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            TopicConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
        (
            TopicConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS - 1,
                ),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
        (
            TopicConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS + 1,
                ),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
    ] {
        reject(
            &fixture,
            20,
            CommandKind::UpdateTopic { update: patch },
            BrokerError::TopicConfig(error),
        )?;
    }
    for (patch, error) in [
        (
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooShort,
        ),
        (
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS + 1),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            },
        ),
        (
            SubscriptionConfigUpdate {
                max_delivery_count: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::MaxDeliveryCountTooSmall,
        ),
        (
            SubscriptionConfigUpdate {
                max_message_bytes: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            SubscriptionConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
    ] {
        reject(
            &fixture,
            20,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("Alpha")?,
                update: SubscriptionConfigUpdate {
                    dead_lettering_on_filter_evaluation_exceptions: Some(false),
                    ..patch
                },
            },
            BrokerError::SubscriptionConfig(error),
        )?;
    }
    let snapshot = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

fn reserved_missing_wrong_kind_and_clock_regression_updates_do_not_mutate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 10)?;
    let before = fixture.machine.store().snapshot()?;
    for kind in [
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("Alpha")?,
            update: SubscriptionConfigUpdate::default(),
        },
    ] {
        for (entity, expected) in [
            (
                fixture.entity.dead_letter_queue()?,
                BrokerError::DeadLetterQueueIsReserved,
            ),
            (
                alpha.dead_letter_queue()?,
                BrokerError::DeadLetterQueueIsReserved,
            ),
            (alpha.clone(), BrokerError::SubscriptionPathIsReserved),
            (EntityPath::new("missing")?, BrokerError::TopicNotFound),
            (EntityPath::new("anchor")?, BrokerError::TopicNotFound),
            (EntityPath::new("Orders")?, BrokerError::TopicNotFound),
        ] {
            let mut command = fixture.command(20, kind.clone());
            command.entity = entity;
            assert_eq!(fixture.machine.apply(&command), Err(expected));
            assert_eq!(fixture.machine.store().snapshot()?, before);
        }
        let mut other_namespace = fixture.command(20, kind.clone());
        other_namespace.namespace = domain::NamespaceName::new("neighbor")?;
        assert_eq!(
            fixture.machine.apply(&other_namespace),
            Err(BrokerError::TopicNotFound)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        reject(
            &fixture,
            9,
            kind,
            BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(10),
                proposed: Timestamp::from_millis(9),
            },
        )?;
    }
    reject(
        &fixture,
        20,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("alpha")?,
            update: SubscriptionConfigUpdate::default(),
        },
        BrokerError::SubscriptionNotFound,
    )?;
    reject(
        &fixture,
        20,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("missing")?,
            update: SubscriptionConfigUpdate::default(),
        },
        BrokerError::SubscriptionNotFound,
    )?;
    Ok(())
}

fn config_only_updates_preserve_opaque_rules_and_ignore_unrelated_subscriptions<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = observed(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let rule_key = keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("Alpha")?,
        &RuleName::new("$Default")?,
    );
    let opaque = vec![255, 0, 254];
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(rule_key.clone(), opaque.clone()))?;
    reset(&fixture);
    topic_update(
        &fixture,
        1,
        TopicConfigUpdate {
            max_message_bytes: Some(128),
            ..TopicConfigUpdate::default()
        },
    )?;
    subscription_update(
        &fixture,
        "Alpha",
        2,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        let prefix = keys::rule_prefix(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("Alpha")?,
        );
        assert!(!observations.gets.iter().any(|key| key.starts_with(&prefix)));
        assert!(
            !observations
                .scans
                .iter()
                .any(|(key, _)| key.starts_with(&prefix))
        );
    }
    assert_eq!(fixture.machine.store().get(&rule_key)?, Some(opaque));
    assert_projections(&fixture, "Alpha")?;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(keys::queue_config(
            &fixture.namespace,
            &beta.dead_letter_queue()?,
        )))?;
    let damaged = fixture.machine.store().snapshot()?;
    reset(&fixture);
    subscription_update(&fixture, "Alpha", 100, SubscriptionConfigUpdate::default())?;
    assert_eq!(fixture.machine.store().snapshot()?, damaged);
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
    subscription_update(
        &fixture,
        "Alpha",
        3,
        SubscriptionConfigUpdate {
            max_delivery_count: Some(3),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &alpha)?
            .expect("backing")
            .max_delivery_count,
        3
    );
    reject(
        &fixture,
        100,
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
        BrokerError::DanglingSubscriptionMetadata,
    )?;
    reject(
        &fixture,
        100,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("beta")?,
            update: SubscriptionConfigUpdate::default(),
        },
        BrokerError::DanglingSubscriptionMetadata,
    )?;
    Ok(())
}

fn failed_config_commits_keep_all_projection_keys_and_live_state_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let sequence = send(&fixture, 10, "retained", None, None)?;
    let delivery = receive(&fixture, &alpha, 11, None)?;
    for (millis, kind, names, expected_keys) in [
        (
            20,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
                    max_message_bytes: Some(128),
                    duplicate_detection_history_time_window_millis: Some(20_000),
                    ..TopicConfigUpdate::default()
                },
            },
            Vec::new(),
            vec![
                keys::topic_config(&fixture.namespace, &fixture.entity),
                keys::clock(),
            ],
        ),
        (
            21,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("Alpha")?,
                update: SubscriptionConfigUpdate {
                    lock_duration_millis: Some(25),
                    max_delivery_count: Some(1),
                    max_message_bytes: Some(1),
                    dead_lettering_on_filter_evaluation_exceptions: Some(false),
                    ..SubscriptionConfigUpdate::default()
                },
            },
            vec!["Alpha"],
            vec![
                keys::subscription(
                    &fixture.namespace,
                    &fixture.entity,
                    &SubscriptionName::new("Alpha")?,
                ),
                keys::queue_config(&fixture.namespace, &alpha),
                keys::queue_config(&fixture.namespace, &alpha.dead_letter_queue()?),
                keys::clock(),
            ],
        ),
    ] {
        let before = fixture.machine.store().snapshot()?;
        let clock = fixture.machine.last_applied_time()?;
        let unchanged = retained(&fixture, &names)?;
        reset(&fixture);
        fixture
            .machine
            .store()
            .fail_next
            .store(true, Ordering::Relaxed);
        assert_eq!(
            apply(&fixture, millis, kind.clone()),
            Err(BrokerError::Storage(StorageError::Backend {
                operation: "commit",
                detail: "injected update failure".into(),
            }))
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, clock);
        {
            let observations = fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations");
            assert_eq!(observations.commits, 1);
            let mut actual = observations.puts.clone();
            actual.sort();
            let mut expected = expected_keys;
            expected.sort();
            assert_eq!(actual, expected);
        }
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        reset(&fixture);
        let application = apply(&fixture, millis, kind)?;
        assert!(!application.dead_letters_enqueued);
        assert_eq!(application.subscription_enqueues, None);
        assert_eq!(
            fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations")
                .commits,
            1
        );
        assert_eq!(retained(&fixture, &names)?, unchanged);
        assert_projections(&fixture, "Alpha")?;
    }
    assert_eq!(
        at(
            &fixture,
            &alpha,
            22,
            CommandKind::Complete {
                sequence,
                lock_token: delivery.lock.expect("original lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn corrupt_target_topology_is_not_repaired_by_changed_or_noop_updates<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed(provider, TopicConfig::default())?;
    for case in 0..11_u64 {
        let base = 10 + case * 10;
        fixture.entity = EntityPath::new(format!("damaged-{case}"))?;
        fixture.at(
            base,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), base)?;
        let name = SubscriptionName::new("Alpha")?;
        let membership = keys::subscription(&fixture.namespace, &fixture.entity, &name);
        let backing = keys::queue_config(&fixture.namespace, &alpha);
        let shadow = keys::queue_config(&fixture.namespace, &alpha.dead_letter_queue()?);
        let parent = keys::topic_config(&fixture.namespace, &fixture.entity);
        let (damage, expected, topic_also_rejects) = match case {
            0 => (
                WriteBatch::default().put(
                    keys::queue_config(&fixture.namespace, &fixture.entity),
                    codec::encode(&QueueConfig::default())?,
                ),
                BrokerError::DanglingEntityMetadata,
                true,
            ),
            1 => (
                WriteBatch::default().put(
                    parent,
                    codec::encode(&TopicConfig {
                        max_message_bytes: 0,
                        ..TopicConfig::default()
                    })?,
                ),
                BrokerError::TopicConfig(QueueConfigError::MaxMessageBytesTooSmall),
                true,
            ),
            2 => (
                WriteBatch::default().put(parent, vec![codec::ACTIVE_VALUE_FORMAT]),
                BrokerError::Codec(domain::CodecError::Decode),
                true,
            ),
            3 => (
                WriteBatch::default().delete(backing),
                BrokerError::DanglingSubscriptionMetadata,
                true,
            ),
            4 => (
                WriteBatch::default().delete(shadow),
                BrokerError::DanglingSubscriptionMetadata,
                true,
            ),
            5 => (
                WriteBatch::default().put(
                    membership,
                    codec::encode(&SubscriptionConfig {
                        max_delivery_count: 0,
                        ..SubscriptionConfig::default()
                    })?,
                ),
                BrokerError::DanglingSubscriptionMetadata,
                true,
            ),
            6 => (
                WriteBatch::default().put(
                    backing,
                    codec::encode(&QueueConfig {
                        lock_duration_millis: 5,
                        ..QueueConfig::default()
                    })?,
                ),
                BrokerError::DanglingSubscriptionMetadata,
                true,
            ),
            7 => (
                WriteBatch::default().put(
                    keys::topic_config(&fixture.namespace, &alpha),
                    codec::encode(&TopicConfig::default())?,
                ),
                BrokerError::DanglingEntityMetadata,
                true,
            ),
            8 => (
                WriteBatch::default().put(
                    keys::topic_config(&fixture.namespace, &alpha.dead_letter_queue()?),
                    codec::encode(&TopicConfig::default())?,
                ),
                BrokerError::DanglingEntityMetadata,
                true,
            ),
            9 => (
                WriteBatch::default().delete(membership),
                BrokerError::DanglingSubscriptionMetadata,
                false,
            ),
            10 => (
                WriteBatch::default().delete(parent),
                BrokerError::DanglingSubscriptionMetadata,
                true,
            ),
            _ => unreachable!(),
        };
        fixture.machine.store().apply(damage)?;
        reset(&fixture);
        for update in [
            SubscriptionConfigUpdate::default(),
            SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        ] {
            reject(
                &fixture,
                base + 1,
                CommandKind::UpdateSubscription {
                    name: name.clone(),
                    update,
                },
                expected.clone(),
            )?;
        }
        if topic_also_rejects {
            for update in [
                TopicConfigUpdate::default(),
                TopicConfigUpdate {
                    max_message_bytes: Some(128),
                    ..TopicConfigUpdate::default()
                },
            ] {
                reject(
                    &fixture,
                    base + 1,
                    CommandKind::UpdateTopic { update },
                    expected.clone(),
                )?;
            }
        }
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
    }
    Ok(())
}

fn topic_validation_is_bounded_and_targeted_sub_update_does_not_scan_sibling_members<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = observed(provider, TopicConfig::default())?;
    subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let mut batch = WriteBatch::default();
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        let name = SubscriptionName::new(format!("extra{index:02}"))?;
        let entity = fixture.entity.subscription(&name)?;
        let config = SubscriptionConfig::default();
        batch.push_put(
            keys::subscription(&fixture.namespace, &fixture.entity, &name),
            codec::encode(&config)?,
        );
        batch.push_put(
            keys::queue_config(&fixture.namespace, &entity),
            codec::encode(&config.to_queue_config())?,
        );
        batch.push_put(
            keys::queue_config(&fixture.namespace, &entity.dead_letter_queue()?),
            codec::encode(&config.to_queue_config().dead_letter_shadow())?,
        );
    }
    fixture.machine.store().apply(batch)?;
    reset(&fixture);
    reject(
        &fixture,
        100,
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
        BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS,
        },
    )?;
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(
            observations.scans,
            vec![(
                keys::subscription_prefix(&fixture.namespace, &fixture.entity),
                MAX_TOPIC_SUBSCRIPTIONS + 1
            )]
        );
        assert_eq!(observations.commits, 0);
    }
    reset(&fixture);
    subscription_update(&fixture, "Alpha", 100, SubscriptionConfigUpdate::default())?;
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        let prefix = keys::subscription_topic_mode_prefix(&fixture.namespace, &fixture.entity);
        assert_eq!(observations.scans, vec![(prefix.clone(), 1); 3]);
        assert_eq!(
            observations.scan_details,
            vec![(prefix.clone(), prefix, 1, 0); 3]
        );
        assert_eq!(observations.commits, 0);
    }
    assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    subscription_update(
        &fixture,
        "Alpha",
        1,
        SubscriptionConfigUpdate {
            max_delivery_count: Some(3),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_projections(&fixture, "Alpha")?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    immutable_properties_reject_both_directions_but_allow_identical_restatements,
    invalid_partial_configuration_rolls_back_all_fields_and_clock,
    reserved_missing_wrong_kind_and_clock_regression_updates_do_not_mutate,
    config_only_updates_preserve_opaque_rules_and_ignore_unrelated_subscriptions,
    failed_config_commits_keep_all_projection_keys_and_live_state_atomic,
    corrupt_target_topology_is_not_repaired_by_changed_or_noop_updates,
    topic_validation_is_bounded_and_targeted_sub_update_does_not_scan_sibling_members,
}
