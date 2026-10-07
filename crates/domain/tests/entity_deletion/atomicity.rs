use super::*;

fn missing_kind_reserved_and_regressed_deletions_leave_state_unchanged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 10)?;
    for (target, error) in [
        (DeleteEntityTarget::Auto, BrokerError::QueueNotFound),
        (DeleteEntityTarget::Queue, BrokerError::QueueNotFound),
        (DeleteEntityTarget::Topic, BrokerError::TopicNotFound),
        (
            DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
            BrokerError::TopicNotFound,
        ),
    ] {
        for path in ["missing", "Orders"] {
            reject(
                &fixture,
                &EntityPath::new(path)?,
                20,
                CommandKind::DeleteEntity {
                    target: target.clone(),
                },
                error.clone(),
            )?;
        }
        let mut other_namespace = fixture.command(20, CommandKind::DeleteEntity { target });
        other_namespace.namespace = NamespaceName::new("neighbor")?;
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(fixture.machine.apply(&other_namespace), Err(error));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        BrokerError::EntityKindMismatch,
    )?;
    reject(
        &fixture,
        &EntityPath::new("anchor")?,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
        BrokerError::EntityKindMismatch,
    )?;
    reject(
        &fixture,
        &EntityPath::new("anchor")?,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
        },
        BrokerError::EntityKindMismatch,
    )?;
    reject(
        &fixture,
        &fixture.entity,
        20,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("alpha")?,
            },
        },
        BrokerError::SubscriptionNotFound,
    )?;
    for (path, error) in [
        (child.clone(), BrokerError::SubscriptionPathIsReserved),
        (
            child.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            fixture.entity.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
    ] {
        reject(
            &fixture,
            &path,
            20,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Auto,
            },
            error,
        )?;
    }
    reject(
        &fixture,
        &fixture.entity,
        9,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        },
    )?;
    Ok(())
}

fn orphan_metadata_and_counter_evidence_refuse_without_repair<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    for case in 0..13_u64 {
        let base = 10 + case * 10;
        fixture.entity = EntityPath::new(format!("damaged{case:02}"))?;
        let queue = case < 7;
        if queue {
            fixture.at(
                base,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
        } else {
            fixture.at(
                base,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
        }
        let shadow = fixture.entity.dead_letter_queue()?;
        let mut target = if queue {
            DeleteEntityTarget::Queue
        } else {
            DeleteEntityTarget::Topic
        };
        let (damage, error) = match case {
            0 => (
                WriteBatch::default().delete(keys::queue_config(&fixture.namespace, &shadow)),
                BrokerError::DanglingEntityMetadata,
            ),
            1 => (
                WriteBatch::default().put(
                    keys::topic_config(&fixture.namespace, &fixture.entity),
                    codec::encode(&TopicConfig::default())?,
                ),
                BrokerError::DanglingEntityMetadata,
            ),
            2 => (
                WriteBatch::default().put(
                    keys::queue_counters(&fixture.namespace, &fixture.entity),
                    codec::encode(&QueueCounters {
                        next_sequence: 0,
                        next_lock_token: 1,
                    })?,
                ),
                BrokerError::DanglingEntityMetadata,
            ),
            3 => (
                WriteBatch::default().put(
                    keys::queue_counters(&fixture.namespace, &fixture.entity),
                    vec![codec::ACTIVE_VALUE_FORMAT],
                ),
                BrokerError::Codec(domain::CodecError::Decode),
            ),
            4 => (
                WriteBatch::default().put(
                    keys::ready(&fixture.namespace, &fixture.entity, SequenceNumber::new(1)),
                    Vec::new(),
                ),
                BrokerError::DanglingEntityMetadata,
            ),
            5 => (
                WriteBatch::default().put(
                    keys::rule(
                        &fixture.namespace,
                        &fixture.entity,
                        &SubscriptionName::new("ghost")?,
                        &RuleName::new("rule")?,
                    ),
                    vec![255],
                ),
                BrokerError::DanglingRuleMetadata,
            ),
            6 => (
                WriteBatch::default().put(
                    keys::session(&fixture.namespace, &shadow, &SessionId::new("cart")?),
                    vec![255],
                ),
                BrokerError::DanglingEntityMetadata,
            ),
            7 => {
                let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), base)?;
                (
                    WriteBatch::default().delete(keys::queue_config(
                        &fixture.namespace,
                        &child.dead_letter_queue()?,
                    )),
                    BrokerError::DanglingSubscriptionMetadata,
                )
            }
            8 => {
                let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), base)?;
                (
                    WriteBatch::default().put(
                        keys::topic_config(&fixture.namespace, &child),
                        codec::encode(&TopicConfig::default())?,
                    ),
                    BrokerError::DanglingEntityMetadata,
                )
            }
            9 => {
                let child = fixture
                    .entity
                    .subscription(&SubscriptionName::new("ghost")?)?;
                (
                    WriteBatch::default().put(
                        keys::ready(&fixture.namespace, &child, SequenceNumber::new(1)),
                        Vec::new(),
                    ),
                    BrokerError::DanglingSubscriptionMetadata,
                )
            }
            10 => (
                WriteBatch::default().put(
                    keys::rule(
                        &fixture.namespace,
                        &fixture.entity,
                        &SubscriptionName::new("ghost")?,
                        &RuleName::new("rule")?,
                    ),
                    vec![255],
                ),
                BrokerError::DanglingRuleMetadata,
            ),
            11 => {
                let name = SubscriptionName::new("Alpha")?;
                let child = subscribe(
                    &fixture,
                    "Alpha",
                    SubscriptionConfig {
                        requires_session: true,
                        ..SubscriptionConfig::default()
                    },
                    base,
                )?;
                let session = SessionId::new("cart")?;
                send(&fixture, base, "live", Some(&session))?;
                let hold = accept(&fixture, &child, base, &session)?;
                at(
                    &fixture,
                    &child,
                    base,
                    CommandKind::ReleaseSession { session: hold },
                )?;
                target = DeleteEntityTarget::Subscription { name };
                (
                    WriteBatch::default().delete(keys::queue_counters(&fixture.namespace, &child)),
                    BrokerError::DanglingEntityMetadata,
                )
            }
            12 => {
                let name = SubscriptionName::new("Alpha")?;
                subscribe(&fixture, "Alpha", SubscriptionConfig::default(), base)?;
                send(&fixture, base, "live", None)?;
                target = DeleteEntityTarget::Subscription { name };
                (
                    WriteBatch::default()
                        .delete(keys::queue_counters(&fixture.namespace, &fixture.entity)),
                    BrokerError::DanglingEntityMetadata,
                )
            }
            _ => unreachable!(),
        };
        fixture.machine.store().apply(damage)?;
        reject(
            &fixture,
            &fixture.entity,
            base + 1,
            CommandKind::DeleteEntity { target },
            error,
        )?;
        if case == 2 {
            for counters in [
                QueueCounters {
                    next_sequence: MAX_SEQUENCE_NUMBER + 2,
                    next_lock_token: 1,
                },
                QueueCounters {
                    next_sequence: 1,
                    next_lock_token: 0,
                },
            ] {
                fixture.machine.store().apply(WriteBatch::default().put(
                    keys::queue_counters(&fixture.namespace, &fixture.entity),
                    codec::encode(&counters)?,
                ))?;
                reject(
                    &fixture,
                    &fixture.entity,
                    base + 1,
                    CommandKind::DeleteEntity {
                        target: DeleteEntityTarget::Queue,
                    },
                    BrokerError::DanglingEntityMetadata,
                )?;
            }
        }
    }
    fixture.entity = EntityPath::new("lazy")?;
    fixture.at(
        200,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, None);
    delete(
        &fixture,
        201,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &[fixture.entity.clone(), fixture.entity.dead_letter_queue()?],
    )?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, None);
    fixture.entity = EntityPath::new("active-copy")?;
    fixture.at(
        202,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 202)?;
    send(&fixture, 203, "fresh", None)?;
    assert_eq!(counter_bytes(&fixture, &child)?, None);
    delete(
        &fixture,
        204,
        DeleteEntityTarget::Subscription {
            name: SubscriptionName::new("Alpha")?,
        },
        CommandOutcome::SubscriptionDeleted,
        &[child.clone(), child.dead_letter_queue()?],
    )?;
    Ok(())
}

fn opaque_owned_message_and_rule_values_can_be_purged_without_decoding<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let sequence = send(&fixture, 10, "opaque", None)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::message(&fixture.namespace, &fixture.entity, sequence),
        vec![255, 0, 255],
    ))?;
    let before = fixture.machine.store().snapshot()?;
    let paths = vec![fixture.entity.clone(), fixture.entity.dead_letter_queue()?];
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 20)?;
    fixture.at(
        21,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 21)?;
    let sequence = send(&fixture, 22, "opaque-copy", None)?;
    let pending = schedule(&fixture, 23, "opaque-future", None)?;
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(
                keys::message(&fixture.namespace, &child, sequence),
                vec![255],
            )
            .put(
                keys::message(&fixture.namespace, &fixture.entity, pending),
                vec![255],
            )
            .put(
                keys::rule(
                    &fixture.namespace,
                    &fixture.entity,
                    &SubscriptionName::new("Alpha")?,
                    &RuleName::new("$Default")?,
                ),
                vec![255, 0, 255],
            ),
    )?;
    let before = fixture.machine.store().snapshot()?;
    let paths = vec![
        fixture.entity.clone(),
        child.clone(),
        child.dead_letter_queue()?,
    ];
    delete(
        &fixture,
        24,
        DeleteEntityTarget::Topic,
        CommandOutcome::TopicDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 24)?;
    Ok(())
}

fn failed_queue_and_topic_deletion_commits_reopen_and_retry_atomically<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed_queue(provider, QueueConfig::default())?;
    send(&fixture, 10, "locked", None)?;
    receive(&fixture, &fixture.entity, 11, None)?;
    for (millis, target, outcome, paths) in [
        (
            20,
            DeleteEntityTarget::Queue,
            CommandOutcome::QueueDeleted,
            vec![fixture.entity.clone(), fixture.entity.dead_letter_queue()?],
        ),
        (
            30,
            DeleteEntityTarget::Topic,
            CommandOutcome::TopicDeleted,
            vec![
                fixture.entity.clone(),
                fixture
                    .entity
                    .subscription(&SubscriptionName::new("Alpha")?)?,
                fixture
                    .entity
                    .subscription(&SubscriptionName::new("Alpha")?)?
                    .dead_letter_queue()?,
                fixture
                    .entity
                    .subscription(&SubscriptionName::new("beta")?)?,
                fixture
                    .entity
                    .subscription(&SubscriptionName::new("beta")?)?
                    .dead_letter_queue()?,
            ],
        ),
    ] {
        if millis == 30 {
            fixture.at(
                21,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
            let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 21)?;
            subscribe(&fixture, "beta", SubscriptionConfig::default(), 21)?;
            send(&fixture, 22, "locked", None)?;
            receive(&fixture, &alpha, 23, None)?;
            schedule(&fixture, 24, "future", None)?;
        }
        let before = fixture.machine.store().snapshot()?;
        let clock = fixture.machine.last_applied_time()?;
        reset(&fixture);
        fixture
            .machine
            .store()
            .fail_next
            .store(true, Ordering::Relaxed);
        assert_eq!(
            apply(
                &fixture,
                millis,
                CommandKind::DeleteEntity {
                    target: target.clone()
                }
            ),
            Err(BrokerError::Storage(StorageError::Backend {
                operation: "commit",
                detail: "injected delete failure".into()
            }))
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, clock);
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
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        reset(&fixture);
        delete(&fixture, millis, target, outcome, &paths)?;
        assert_exact_purge(&fixture, &before, &paths, &[], &[], millis)?;
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
    }
    Ok(())
}

fn queue_topology_errors_precede_identity_and_capacity_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = observed_queue(provider, QueueConfig::default())?;
    let identity = keys::entity_incarnation(&fixture.namespace, &fixture.entity);
    let rule = keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("ghost")?,
        &RuleName::new("rule")?,
    );
    let mode = keys::queue_capacity_mode(&fixture.namespace, &fixture.entity);
    for malformed in [false, true] {
        let mut damage = WriteBatch::default()
            .put(rule.clone(), vec![255])
            .delete(mode.clone());
        if malformed {
            damage.push_put(identity.clone(), vec![255]);
        } else {
            damage.push_delete(identity.clone());
        }
        fixture.machine.store().apply(damage)?;
        reset(&fixture);
        reject(
            &fixture,
            &fixture.entity,
            20,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
            BrokerError::DanglingRuleMetadata,
        )?;
        fixture
            .machine
            .store()
            .apply(WriteBatch::default().delete(rule.clone()))?;
        reset(&fixture);
        reject(
            &fixture,
            &fixture.entity,
            20,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
            if malformed {
                BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 })
            } else {
                BrokerError::DanglingEntityMetadata
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
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    missing_kind_reserved_and_regressed_deletions_leave_state_unchanged,
    orphan_metadata_and_counter_evidence_refuse_without_repair,
    opaque_owned_message_and_rule_values_can_be_purged_without_decoding,
    failed_queue_and_topic_deletion_commits_reopen_and_retry_atomically,
    queue_topology_errors_precede_identity_and_capacity_metadata,
}
