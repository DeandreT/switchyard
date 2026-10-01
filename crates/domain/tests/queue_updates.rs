//! Queue updates replace configuration without rewriting already accepted state.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, Delivery, DeliveryBudget, EntityPath,
    LockToken, MAX_DUPLICATE_DETECTION_WINDOW_MILLIS, MAX_LOCK_DURATION_MILLIS,
    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS, MessageEnvelope, QueueConfig, QueueConfigError,
    QueueConfigUpdate, QueueImmutableProperty, QueueTimeToLiveUpdate, ReceiveMode,
    ScheduledMessage, SequenceNumber, SessionHold, SessionId, SettlementDisposition, Timestamp,
    codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn update<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    update: QueueConfigUpdate,
) -> Result<CommandOutcome, BrokerError> {
    let application = fixture
        .machine
        .apply_with_effects(&fixture.command(millis, CommandKind::UpdateQueue { update }))?;
    assert!(!application.dead_letters_enqueued);
    assert_eq!(application.outcome, CommandOutcome::QueueUpdated);
    Ok(application.outcome)
}

fn config<P: StoreProvider>(fixture: &QueueFixture<P>) -> QueueConfig {
    fixture
        .machine
        .queue_config(&fixture.namespace, &fixture.entity)
        .expect("configuration read")
        .expect("queue")
}

fn assert_shadow<P: StoreProvider>(fixture: &QueueFixture<P>) -> Result<(), Box<dyn Error>> {
    let parent = config(fixture);
    let shadow = fixture
        .machine
        .queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?)?
        .expect("shadow");
    assert_eq!(
        shadow,
        QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: false,
            ..parent
        }
    );
    Ok(())
}

fn retained_entries<P: StoreProvider>(
    fixture: &QueueFixture<P>,
) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let parent = keys::queue_config(&fixture.namespace, &fixture.entity);
    let shadow = keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?);
    Ok(fixture
        .machine
        .store()
        .snapshot()?
        .entries()
        .iter()
        .filter(|(key, _)| key != &parent && key != &shadow && key != &keys::clock())
        .cloned()
        .collect())
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    session: Option<&SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"pay".to_vec(),
            time_to_live_millis: ttl,
            session_id: session.cloned(),
        },
    )?
    else {
        panic!("sent outcome")
    };
    Ok(sequence)
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    hold: Option<&SessionHold>,
) -> Result<Delivery, BrokerError> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: hold.cloned(),
        },
    )?
    else {
        panic!("live delivery")
    };
    Ok(delivery)
}

fn partial_updates_preserve_other_settings_and_synchronize_shadow_after_restart<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let original = QueueConfig {
        max_delivery_count: 7,
        default_time_to_live_millis: Some(500),
        max_message_bytes: 700,
        dead_lettering_on_message_expiration: true,
        ..QueueConfig::default()
    };
    let mut fixture = QueueFixture::new(provider, "tenant", "orders", original)?;
    let mut unrelated = fixture.command(
        0,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    unrelated.namespace = domain::NamespaceName::new("neighbor")?;
    unrelated.entity = EntityPath::new("other")?;
    fixture.machine.apply(&unrelated)?;
    let retained = retained_entries(&fixture)?;
    update(
        &fixture,
        10,
        QueueConfigUpdate {
            lock_duration_millis: Some(20),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(
        config(&fixture),
        QueueConfig {
            lock_duration_millis: 20,
            ..original
        }
    );
    assert_eq!(retained_entries(&fixture)?, retained);
    assert_shadow(&fixture)?;
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_shadow(&fixture)?;
    update(
        &fixture,
        11,
        QueueConfigUpdate {
            max_message_bytes: Some(800),
            max_delivery_count: Some(2),
            dead_lettering_on_message_expiration: Some(false),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(
        config(&fixture),
        QueueConfig {
            lock_duration_millis: 20,
            max_message_bytes: 800,
            max_delivery_count: 2,
            dead_lettering_on_message_expiration: false,
            ..original
        }
    );
    assert_shadow(&fixture)?;
    Ok(())
}

fn no_op_updates_leave_the_store_and_clock_byte_for_byte_unchanged<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: Some(500),
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let original = config(&fixture);
    let snapshot = fixture.machine.store().snapshot()?;
    for patch in [
        QueueConfigUpdate::default(),
        QueueConfigUpdate {
            lock_duration_millis: Some(original.lock_duration_millis),
            max_delivery_count: Some(original.max_delivery_count),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 500 }),
            max_message_bytes: Some(original.max_message_bytes),
            requires_session: Some(true),
            requires_duplicate_detection: Some(true),
            duplicate_detection_history_time_window_millis: Some(
                original.duplicate_detection_history_time_window_millis,
            ),
            dead_lettering_on_message_expiration: Some(false),
        },
    ] {
        update(&fixture, 100, patch)?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    update(
        &fixture,
        1,
        QueueConfigUpdate {
            max_delivery_count: Some(1),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(1)
    );
    Ok(())
}

fn ttl_finite_unchanged_and_unlimited_apply_only_to_newly_accepted_messages<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: Some(50),
            ..QueueConfig::default()
        },
    )?;
    let old = send(&fixture, 10, "old", None, None)?;
    let CommandOutcome::Scheduled { sequences } = fixture.at(
        10,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: String::from("scheduled-old"),
                body: b"pay".to_vec(),
                time_to_live_millis: Some(100),
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
            }],
        },
    )?
    else {
        panic!("scheduled")
    };
    let old_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, old)?;
    let scheduled_record =
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?;
    update(
        &fixture,
        20,
        QueueConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 10 }),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, old)?,
        old_record
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?,
        scheduled_record
    );
    let capped = send(&fixture, 21, "capped", Some(100), None)?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, capped)?
            .expect("record")
            .expires_at,
        Some(Timestamp::from_millis(31))
    );
    update(
        &fixture,
        22,
        QueueConfigUpdate {
            max_delivery_count: Some(3),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(config(&fixture).default_time_to_live_millis, Some(10));
    update(
        &fixture,
        23,
        QueueConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(config(&fixture).default_time_to_live_millis, None);
    let unlimited = send(&fixture, 24, "unlimited", None, None)?;
    let explicit = send(&fixture, 24, "explicit", Some(7), None)?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, unlimited)?
            .expect("record")
            .expires_at,
        None
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, explicit)?
            .expect("record")
            .expires_at,
        Some(Timestamp::from_millis(31))
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let activated = fixture
        .machine
        .store()
        .scan_prefix(
            &keys::message_prefix(&fixture.namespace, &fixture.entity),
            usize::MAX,
        )?
        .into_iter()
        .map(|(_, value)| domain::MessageRecord::decode(&value))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|record| record.message_id == "scheduled-old")
        .expect("activated record");
    assert_eq!(activated.expires_at, Some(Timestamp::from_millis(150)));
    assert_shadow(&fixture)?;
    Ok(())
}

fn creation_only_properties_reject_changes_but_allow_identical_restated_values<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    let mut index = 0;
    for sessions in [false, true] {
        for duplicates in [false, true] {
            let base = 10 + index * 10;
            fixture.entity = EntityPath::new(format!("case-{index}"))?;
            fixture.at(
                base,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session: sessions,
                        requires_duplicate_detection: duplicates,
                        ..QueueConfig::default()
                    },
                },
            )?;
            let snapshot = fixture.machine.store().snapshot()?;
            for (patch, property) in [
                (
                    QueueConfigUpdate {
                        requires_session: Some(!sessions),
                        max_delivery_count: Some(4),
                        ..QueueConfigUpdate::default()
                    },
                    QueueImmutableProperty::RequiresSession,
                ),
                (
                    QueueConfigUpdate {
                        requires_duplicate_detection: Some(!duplicates),
                        max_delivery_count: Some(4),
                        ..QueueConfigUpdate::default()
                    },
                    QueueImmutableProperty::RequiresDuplicateDetection,
                ),
            ] {
                assert_eq!(
                    update(&fixture, base + 1, patch),
                    Err(BrokerError::QueuePropertyIsImmutable { property })
                );
                assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            }
            update(
                &fixture,
                base + 1,
                QueueConfigUpdate {
                    requires_session: Some(sessions),
                    requires_duplicate_detection: Some(duplicates),
                    max_delivery_count: Some(4),
                    ..QueueConfigUpdate::default()
                },
            )?;
            assert_eq!(config(&fixture).requires_session, sessions);
            assert_eq!(config(&fixture).requires_duplicate_detection, duplicates);
            assert_eq!(config(&fixture).max_delivery_count, 4);
            assert_shadow(&fixture)?;
            index += 1;
        }
    }
    Ok(())
}

fn invalid_projected_configuration_rolls_back_every_field_and_clock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, "retained", None, None)?;
    let snapshot = fixture.machine.store().snapshot()?;
    for (patch, error) in [
        (
            QueueConfigUpdate {
                lock_duration_millis: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooShort,
        ),
        (
            QueueConfigUpdate {
                lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS + 1),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            },
        ),
        (
            QueueConfigUpdate {
                max_delivery_count: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::MaxDeliveryCountTooSmall,
        ),
        (
            QueueConfigUpdate {
                max_message_bytes: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
        (
            QueueConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS - 1,
                ),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
        (
            QueueConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS + 1,
                ),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
    ] {
        let patch = QueueConfigUpdate {
            dead_lettering_on_message_expiration: Some(true),
            ..patch
        };
        assert_eq!(
            update(&fixture, 20, patch),
            Err(BrokerError::QueueConfig(error))
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    }
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_shadow(&fixture)?;
    Ok(())
}

fn reserved_missing_and_invalid_noop_updates_validate_before_any_mutation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let invalid = QueueConfigUpdate {
        requires_session: Some(true),
        lock_duration_millis: Some(0),
        ..QueueConfigUpdate::default()
    };
    let snapshot = fixture.machine.store().snapshot()?;
    for entity in [
        fixture.entity.dead_letter_queue()?,
        EntityPath::new("missing/$deadletterqueue")?,
    ] {
        let mut command = fixture.command(10, CommandKind::UpdateQueue { update: invalid });
        command.entity = entity;
        assert_eq!(
            fixture.machine.apply(&command),
            Err(BrokerError::DeadLetterQueueIsReserved)
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    }
    let mut command = fixture.command(10, CommandKind::UpdateQueue { update: invalid });
    command.entity = EntityPath::new("missing")?;
    assert_eq!(
        fixture.machine.apply(&command),
        Err(BrokerError::QueueNotFound)
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_config(&fixture.namespace, &fixture.entity),
        codec::encode(&QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        })?,
    ))?;
    let invalid_snapshot = fixture.machine.store().snapshot()?;
    assert_eq!(
        update(&fixture, 10, QueueConfigUpdate::default()),
        Err(BrokerError::QueueConfig(
            QueueConfigError::LockDurationTooShort
        ))
    );
    assert_eq!(fixture.machine.store().snapshot()?, invalid_snapshot);
    Ok(())
}

fn every_live_state_and_existing_deadline_is_retained_across_update_and_restart<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 100,
            default_time_to_live_millis: Some(1_000),
            requires_session: true,
            requires_duplicate_detection: true,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let session = SessionId::new("cart")?;
    let locked = send(&fixture, 10, "locked", None, Some(&session))?;
    let deferred = send(&fixture, 10, "deferred", None, Some(&session))?;
    let rejected = send(&fixture, 10, "rejected", None, Some(&session))?;
    let ready = send(&fixture, 10, "ready", None, Some(&session))?;
    fixture.at(
        10,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: String::from("scheduled"),
                body: b"payload".to_vec(),
                time_to_live_millis: None,
                session_id: Some(session.clone()),
                enqueue_at: Timestamp::from_millis(200),
            }],
        },
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        11,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("accepted session")
    };
    let hold = accepted.hold();
    let delivery = receive(&fixture, 12, Some(&hold))?;
    assert_eq!(delivery.sequence, locked);
    let locked_token = delivery.lock.expect("lock").token;
    let delivery = receive(&fixture, 12, Some(&hold))?;
    assert_eq!(delivery.sequence, deferred);
    fixture.at(
        12,
        CommandKind::Defer {
            sequence: deferred,
            lock_token: delivery.lock.expect("lock").token,
        },
    )?;
    let delivery = receive(&fixture, 12, Some(&hold))?;
    assert_eq!(delivery.sequence, rejected);
    fixture.at(
        12,
        CommandKind::DeadLetter {
            sequence: rejected,
            lock_token: delivery.lock.expect("lock").token,
            reason: String::from("invalid"),
            description: String::from("retained rejection"),
        },
    )?;
    fixture.at(
        12,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: b"retained state".to_vec(),
        },
    )?;
    let retained = retained_entries(&fixture)?;
    let locked_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, locked)?;
    let deferred_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, deferred)?;
    let ready_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, ready)?;
    update(
        &fixture,
        20,
        QueueConfigUpdate {
            lock_duration_millis: Some(20),
            max_delivery_count: Some(1),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
            max_message_bytes: Some(2),
            requires_session: Some(true),
            requires_duplicate_detection: Some(true),
            duplicate_detection_history_time_window_millis: Some(20_000),
            dead_lettering_on_message_expiration: Some(false),
        },
    )?;
    assert_eq!(retained_entries(&fixture)?, retained);
    assert_shadow(&fixture)?;
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, locked)?,
        locked_record
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, deferred)?,
        deferred_record
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, ready)?,
        ready_record
    );
    assert_eq!(
        fixture.at(
            21,
            CommandKind::GetSessionState {
                session: hold.clone()
            }
        )?,
        CommandOutcome::SessionState(b"retained state".to_vec())
    );
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Complete {
                sequence: locked,
                lock_token: locked_token
            }
        )?,
        CommandOutcome::Completed
    );
    // Oversized content admitted before the update remains deliverable.
    let delivery = receive(&fixture, 21, Some(&hold))?;
    assert_eq!(delivery.sequence, ready);
    assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(1_010)));
    assert_eq!(
        delivery.lock.expect("new lock").locked_until,
        Timestamp::from_millis(41)
    );
    assert_eq!(
        fixture.at(
            22,
            CommandKind::Abandon {
                sequence: ready,
                lock_token: delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false
        }
    );
    Ok(())
}

fn duplicate_window_updates_preserve_old_history_and_only_change_new_retention<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 40_000,
            ..QueueConfig::default()
        },
    )?;
    send(&fixture, 10, "old", None, None)?;
    let old_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "old");
    let old_deadline = fixture.machine.store().get(&old_key)?.expect("history");
    update(
        &fixture,
        20,
        QueueConfigUpdate {
            duplicate_detection_history_time_window_millis: Some(20_000),
            ..QueueConfigUpdate::default()
        },
    )?;
    assert_eq!(
        fixture.machine.store().get(&old_key)?,
        Some(old_deadline.clone())
    );
    send(&fixture, 20, "new", None, None)?;
    let new_deadline: Timestamp = codec::decode(
        &fixture
            .machine
            .store()
            .get(&keys::duplicate_history(
                &fixture.namespace,
                &fixture.entity,
                "new",
            ))?
            .expect("new history"),
    )?;
    assert_eq!(new_deadline, Timestamp::from_millis(20_020));
    assert_eq!(
        fixture.at(20_020, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(fixture.machine.store().get(&old_key)?, Some(old_deadline));
    let duplicate = send(&fixture, 30_000, "old", None, None)?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, duplicate)?
            .is_none()
    );
    let accepted = send(&fixture, 40_010, "old", None, None)?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, accepted)?
            .is_some()
    );
    let replacement: Timestamp =
        codec::decode(&fixture.machine.store().get(&old_key)?.expect("replacement"))?;
    assert_eq!(replacement, Timestamp::from_millis(60_010));
    Ok(())
}

fn expiration_policy_updates_use_existing_deadlines_without_reindexing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    for (index, enabled) in [true, false].into_iter().enumerate() {
        let base = 10 + index as u64 * 100;
        fixture.entity = EntityPath::new(format!("case-{index}"))?;
        fixture.at(
            base,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    dead_lettering_on_message_expiration: !enabled,
                    ..QueueConfig::default()
                },
            },
        )?;
        let sequence = send(&fixture, base, "expires", Some(5), None)?;
        let retained = retained_entries(&fixture)?;
        update(
            &fixture,
            base + 2,
            QueueConfigUpdate {
                dead_lettering_on_message_expiration: Some(enabled),
                ..QueueConfigUpdate::default()
            },
        )?;
        assert_eq!(retained_entries(&fixture)?, retained);
        let application = fixture
            .machine
            .apply_with_effects(&fixture.command(base + 5, CommandKind::ExpireMessages))?;
        assert_eq!(application.dead_letters_enqueued, enabled);
        assert_eq!(
            application.outcome,
            CommandOutcome::MessagesExpired {
                dead_lettered: u32::from(enabled),
                dropped: u32::from(!enabled),
                processed: 1
            }
        );
        assert_eq!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    &fixture.entity.dead_letter_queue()?,
                    sequence
                )?
                .is_some(),
            enabled
        );
    }
    Ok(())
}

fn legacy_stored_configurations_update_without_changing_the_value_format<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    for version in [
        codec::VALUE_FORMAT_V5,
        codec::VALUE_FORMAT_V6,
        codec::VALUE_FORMAT_V7,
        codec::VALUE_FORMAT_V8,
        codec::VALUE_FORMAT_V9,
    ] {
        let base = 10 + u64::from(version) * 10;
        fixture.entity = EntityPath::new(format!("legacy-{version}"))?;
        let original = QueueConfig {
            lock_duration_millis: 100,
            max_delivery_count: 3,
            default_time_to_live_millis: Some(500),
            max_message_bytes: 1_024,
            requires_session: true,
            requires_duplicate_detection: version >= codec::VALUE_FORMAT_V6,
            duplicate_detection_history_time_window_millis:
                domain::DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS,
            dead_lettering_on_message_expiration: true,
        };
        fixture.at(base, CommandKind::CreateQueue { config: original })?;
        let mut bytes = vec![version];
        let payload = match version {
            codec::VALUE_FORMAT_V5 => postcard::to_stdvec(&(
                original.lock_duration_millis,
                original.max_delivery_count,
                original.default_time_to_live_millis,
                original.max_message_bytes,
                original.requires_session,
            ))?,
            codec::VALUE_FORMAT_V6 | codec::VALUE_FORMAT_V7 => postcard::to_stdvec(&(
                original.lock_duration_millis,
                original.max_delivery_count,
                original.default_time_to_live_millis,
                original.max_message_bytes,
                original.requires_session,
                original.requires_duplicate_detection,
                original.duplicate_detection_history_time_window_millis,
            ))?,
            _ => postcard::to_stdvec(&original)?,
        };
        bytes.extend(payload);
        fixture.machine.store().apply(WriteBatch::default().put(
            keys::queue_config(&fixture.namespace, &fixture.entity),
            bytes,
        ))?;
        assert_eq!(config(&fixture), original);
        let snapshot = fixture.machine.store().snapshot()?;
        update(&fixture, base + 1, QueueConfigUpdate::default())?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        update(
            &fixture,
            base + 2,
            QueueConfigUpdate {
                lock_duration_millis: Some(25),
                requires_session: Some(true),
                requires_duplicate_detection: Some(original.requires_duplicate_detection),
                ..QueueConfigUpdate::default()
            },
        )?;
        let bytes = fixture
            .machine
            .store()
            .get(&keys::queue_config(&fixture.namespace, &fixture.entity))?
            .expect("configuration");
        assert_eq!(bytes[0], codec::ACTIVE_VALUE_FORMAT);
        assert_eq!(codec::ACTIVE_VALUE_FORMAT, codec::VALUE_FORMAT_V9);
        assert_eq!(
            config(&fixture),
            QueueConfig {
                lock_duration_millis: 25,
                ..original
            }
        );
        assert_shadow(&fixture)?;
        fixture = fixture.restart()?;
        assert_eq!(
            config(&fixture),
            QueueConfig {
                lock_duration_millis: 25,
                ..original
            }
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct FailingStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for FailingStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: String::from("injected failure"),
            });
        }
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

struct FailingProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
}
impl<P: StoreProvider> StoreProvider for FailingProvider<P> {
    type Store = FailingStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(FailingStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
        })
    }
}

fn storage_failure_keeps_parent_shadow_clock_and_live_state_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fail_next = Arc::new(AtomicBool::new(false));
    let mut fixture = QueueFixture::with_defaults(
        FailingProvider {
            inner: provider,
            fail_next: fail_next.clone(),
        },
        "tenant",
        "orders",
    )?;
    let sequence = send(&fixture, 10, "retained", Some(1_000), None)?;
    let delivery = receive(&fixture, 11, None)?;
    let snapshot = fixture.machine.store().snapshot()?;
    let patch = QueueConfigUpdate {
        lock_duration_millis: Some(20),
        max_message_bytes: Some(1),
        ..QueueConfigUpdate::default()
    };
    fail_next.store(true, Ordering::Relaxed);
    assert_eq!(
        update(&fixture, 20, patch),
        Err(BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: String::from("injected failure")
        }))
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let retained = retained_entries(&fixture)?;
    update(&fixture, 20, patch)?;
    assert_eq!(retained_entries(&fixture)?, retained);
    assert_shadow(&fixture)?;
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Complete {
                sequence,
                lock_token: delivery.lock.expect("unchanged lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

#[test]
fn update_command_appends_and_ttl_states_have_distinct_postcard_shapes()
-> Result<(), Box<dyn Error>> {
    for (patch, expected) in [
        (
            QueueConfigUpdate::default(),
            vec![28, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..QueueConfigUpdate::default()
            },
            vec![28, 0, 0, 1, 0, 0, 0, 0, 0, 0],
        ),
        (
            QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 7 }),
                ..QueueConfigUpdate::default()
            },
            vec![28, 0, 0, 1, 1, 7, 0, 0, 0, 0, 0],
        ),
    ] {
        let kind = CommandKind::UpdateQueue { update: patch };
        assert_eq!(postcard::to_stdvec(&kind)?, expected);
        assert_eq!(postcard::from_bytes::<CommandKind>(&expected)?, kind);
        let command = Command::new(
            domain::NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(11),
            kind,
        );
        assert_eq!(
            postcard::from_bytes::<Command>(&postcard::to_stdvec(&command)?)?,
            command
        );
    }
    Ok(())
}

#[test]
fn all_previous_command_discriminants_and_ownership_shapes_remain_frozen()
-> Result<(), Box<dyn Error>> {
    let token = LockToken::new(3);
    let sequence = SequenceNumber::new(7);
    let session = SessionHold::new(SessionId::new("cart")?, token);
    let budget = DeliveryBudget {
        max_bytes: 1_024,
        per_message_overhead_bytes: 64,
    };
    let kinds = [
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
        CommandKind::Send {
            message_id: String::from("id"),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
        CommandKind::Schedule {
            messages: Vec::new(),
        },
        CommandKind::CancelScheduled {
            sequences: vec![sequence],
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::Peek {
            from_sequence: sequence,
            max_messages: 2,
            session_id: None,
        },
        CommandKind::Complete {
            sequence,
            lock_token: token,
        },
        CommandKind::Abandon {
            sequence,
            lock_token: token,
        },
        CommandKind::DeadLetter {
            sequence,
            lock_token: token,
            reason: String::from("r"),
            description: String::from("d"),
        },
        CommandKind::Defer {
            sequence,
            lock_token: token,
        },
        CommandKind::RenewLock {
            sequence,
            lock_token: token,
            lock_duration_millis: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: Some(9),
            session_id: Some(session.session_id.clone()),
        },
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
        CommandKind::ReleaseSession {
            session: session.clone(),
        },
        CommandKind::RenewSessionLock {
            session: session.clone(),
            lock_duration_millis: None,
        },
        CommandKind::SetSessionState {
            session: session.clone(),
            state: vec![1],
        },
        CommandKind::GetSessionState {
            session: session.clone(),
        },
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
        CommandKind::SendEnvelope {
            message_id: String::from("id"),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(MessageEnvelope::default()),
        },
        CommandKind::ScheduleEnvelopes {
            messages: Vec::new(),
        },
        CommandKind::Settle {
            sequence,
            lock_token: token,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: Default::default(),
        },
        CommandKind::ReceiveDeferredBounded {
            sequences: vec![sequence],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: Some(9),
            session_id: Some(session.session_id.clone()),
            budget,
        },
        CommandKind::PeekBounded {
            from_sequence: sequence,
            max_messages: 2,
            session_id: Some(session.session_id.clone()),
            budget,
        },
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![sequence],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: Some(9),
            session: Some(session),
            budget,
        },
    ];
    for (index, kind) in kinds.iter().enumerate() {
        let bytes = postcard::to_stdvec(kind)?;
        assert_eq!(bytes[0] as usize, index);
        assert_eq!(postcard::from_bytes::<CommandKind>(&bytes)?, *kind);
    }
    assert_eq!(
        postcard::to_stdvec(&kinds[1])?,
        vec![1, 2, 105, 100, 1, 1, 0, 0]
    );
    assert_eq!(
        postcard::to_stdvec(&kinds[11])?,
        vec![11, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116]
    );
    assert_eq!(
        postcard::to_stdvec(&kinds[25])?,
        vec![25, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116, 128, 8, 64]
    );
    assert_eq!(
        postcard::to_stdvec(&kinds[26])?,
        vec![26, 7, 2, 1, 4, 99, 97, 114, 116, 128, 8, 64]
    );
    assert_eq!(
        postcard::to_stdvec(&kinds[27])?,
        vec![27, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116, 3, 128, 8, 64]
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    partial_updates_preserve_other_settings_and_synchronize_shadow_after_restart,
    no_op_updates_leave_the_store_and_clock_byte_for_byte_unchanged,
    ttl_finite_unchanged_and_unlimited_apply_only_to_newly_accepted_messages,
    creation_only_properties_reject_changes_but_allow_identical_restated_values,
    invalid_projected_configuration_rolls_back_every_field_and_clock,
    reserved_missing_and_invalid_noop_updates_validate_before_any_mutation,
    every_live_state_and_existing_deadline_is_retained_across_update_and_restart,
    duplicate_window_updates_preserve_old_history_and_only_change_new_retention,
    expiration_policy_updates_use_existing_deadlines_without_reindexing,
    legacy_stored_configurations_update_without_changing_the_value_format,
    storage_failure_keeps_parent_shadow_clock_and_live_state_atomic,
}
