//! Duplicate history is independent of delivery state and bounded by broker time.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, MAX_MESSAGE_ID_LENGTH,
    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS, QueueConfig, ReceiveMode, ScheduledMessage,
    SequenceNumber, SessionId, TIMER_SCAN_LIMIT, Timestamp, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

const WINDOW: u64 = MIN_DUPLICATE_DETECTION_WINDOW_MILLIS;

fn config() -> QueueConfig {
    QueueConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: WINDOW,
        ..QueueConfig::default()
    }
}

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(provider, "tenant", "orders", config())?)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    body: &[u8],
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: body.to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected send outcome, got {other:?}"),
    }
}

fn scheduled(id: &str, enqueue_at: u64) -> ScheduledMessage {
    ScheduledMessage {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        enqueue_at: Timestamp::from_millis(enqueue_at),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn schedule<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    messages: Vec<ScheduledMessage>,
) -> Result<Vec<SequenceNumber>, BrokerError> {
    match fixture.at(millis, CommandKind::Schedule { messages })? {
        CommandOutcome::Scheduled { sequences } => Ok(sequences),
        other => panic!("expected schedule outcome, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn history_deadline<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    id: &str,
) -> Result<Option<Timestamp>, Box<dyn Error>> {
    fixture
        .machine
        .store()
        .get(&keys::duplicate_history(
            &fixture.namespace,
            &fixture.entity,
            id,
        ))?
        .map(|bytes| codec::decode(&bytes).map_err(Into::into))
        .transpose()
}

fn duplicate_submissions_are_accepted_without_replacing_the_original<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    assert_eq!(
        send(&fixture, 10, "same-id", b"first")?,
        SequenceNumber::new(1)
    );
    assert_eq!(
        send(&fixture, 11, "same-id", b"changed body")?,
        SequenceNumber::new(2)
    );
    assert_eq!(
        send(&fixture, 12, "same-id", b"third")?,
        SequenceNumber::new(3)
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![SequenceNumber::new(1)]
    );
    for sequence in [SequenceNumber::new(2), SequenceNumber::new(3)] {
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?
                .is_none()
        );
    }
    let delivery = receive(&fixture, 20)?.expect("original retained");
    assert_eq!(delivery.body, b"first");
    assert_eq!(delivery.delivery_count, 1);
    assert_eq!(receive(&fixture, 21)?, None);
    Ok(())
}

fn disabled_detection_retains_repeated_message_ids<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(provider, "tenant", "orders", QueueConfig::default())?;
    send(&fixture, 10, "same-id", b"first")?;
    send(&fixture, 11, "same-id", b"second")?;
    assert_eq!(
        receive(&fixture, 20)?.expect("first retained").body,
        b"first"
    );
    assert_eq!(
        receive(&fixture, 21)?.expect("second retained").body,
        b"second"
    );
    assert_eq!(history_deadline(&fixture, "same-id")?, None);
    Ok(())
}

fn an_elapsed_window_accepts_a_new_original_without_waiting_for_cleanup<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "same-id", b"first")?;
    send(&fixture, 10 + WINDOW - 1, "same-id", b"dropped")?;
    assert_eq!(
        history_deadline(&fixture, "same-id")?,
        Some(Timestamp::from_millis(10 + WINDOW))
    );
    let renewed = send(&fixture, 10 + WINDOW, "same-id", b"new original")?;
    assert_eq!(renewed, SequenceNumber::new(3));
    assert_eq!(
        history_deadline(&fixture, "same-id")?,
        Some(Timestamp::from_millis(10 + 2 * WINDOW))
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_expiry_prefix(&fixture.namespace, &fixture.entity),
                10
            )?
            .len(),
        1
    );
    assert_eq!(
        receive(&fixture, 11 + WINDOW)?
            .expect("first still queued")
            .body,
        b"first"
    );
    assert_eq!(
        receive(&fixture, 12 + WINDOW)?
            .expect("new original queued")
            .body,
        b"new original"
    );
    Ok(())
}

fn consuming_a_message_does_not_forget_its_history<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "same-id", b"first")?;
    receive(&fixture, 20)?.expect("original consumed");
    send(&fixture, 30, "same-id", b"dropped")?;
    assert_eq!(receive(&fixture, 31)?, None);
    assert_eq!(
        history_deadline(&fixture, "same-id")?,
        Some(Timestamp::from_millis(10 + WINDOW))
    );
    send(&fixture, 10 + WINDOW, "same-id", b"after window")?;
    assert!(receive(&fixture, 11 + WINDOW)?.is_some());
    Ok(())
}

fn expired_messages_leave_duplicate_history_in_the_parent_only<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: true,
            ..config()
        },
    )?;
    let CommandOutcome::Sent { sequence } = fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("same-id"),
            body: b"first".to_vec(),
            time_to_live_millis: Some(1),
            session_id: None,
        },
    )?
    else {
        panic!("expected send outcome");
    };
    assert_eq!(
        fixture.at(11, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1,
        }
    );
    send(&fixture, 12, "same-id", b"dropped")?;
    assert_eq!(receive(&fixture, 13)?, None);
    assert!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
            .is_some()
    );
    assert!(history_deadline(&fixture, "same-id")?.is_some());
    let dlq = fixture.entity.dead_letter_queue()?;
    assert!(
        !fixture
            .machine
            .queue_config(&fixture.namespace, &dlq)?
            .expect("shadow config")
            .requires_duplicate_detection
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_prefix(&fixture.namespace, &dlq),
                10
            )?
            .is_empty()
    );
    Ok(())
}

fn cancelling_a_schedule_does_not_forget_its_history<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handle = schedule(&fixture, 10, vec![scheduled("same-id", 100_000)])?[0];
    fixture.at(
        20,
        CommandKind::CancelScheduled {
            sequences: vec![handle],
        },
    )?;
    send(&fixture, 30, "same-id", b"dropped")?;
    assert_eq!(receive(&fixture, 31)?, None);
    assert_eq!(
        history_deadline(&fixture, "same-id")?,
        Some(Timestamp::from_millis(10 + WINDOW))
    );
    send(&fixture, 10 + WINDOW, "same-id", b"after window")?;
    assert_eq!(
        receive(&fixture, 11 + WINDOW)?
            .expect("history elapsed")
            .body,
        b"after window"
    );
    assert_eq!(
        fixture.at(100_000, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    Ok(())
}

fn scheduled_and_ordinary_submissions_share_one_history<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let ordinary = send(&fixture, 10, "ordinary-first", b"original")?;
    let dropped_schedule = schedule(&fixture, 11, vec![scheduled("ordinary-first", 100)])?[0];
    let original_schedule = schedule(&fixture, 12, vec![scheduled("scheduled-first", 100)])?[0];
    let dropped_send = send(&fixture, 13, "scheduled-first", b"dropped")?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, dropped_schedule)?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, dropped_send)?
            .is_none()
    );
    assert_eq!(
        fixture.at(
            14,
            CommandKind::CancelScheduled {
                sequences: vec![dropped_schedule]
            }
        ),
        Err(BrokerError::MessageNotFound {
            sequence: dropped_schedule
        })
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, original_schedule)?
            .is_some()
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert_eq!(
        receive(&fixture, 101)?
            .expect("ordinary original retained")
            .sequence,
        ordinary
    );
    assert_eq!(
        receive(&fixture, 102)?
            .expect("scheduled original activated")
            .message_id,
        "scheduled-first"
    );
    assert_eq!(receive(&fixture, 103)?, None);
    Ok(())
}

fn duplicate_ids_within_a_schedule_batch_keep_only_the_first<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handles = schedule(
        &fixture,
        10,
        vec![
            scheduled("same-id", 100),
            scheduled("same-id", 50),
            scheduled("another-id", 100),
        ],
    )?;
    assert_eq!(
        handles,
        vec![
            SequenceNumber::new(1),
            SequenceNumber::new(2),
            SequenceNumber::new(3)
        ]
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, handles[1])?
            .is_none()
    );
    assert_eq!(
        fixture.at(50, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    assert_eq!(
        receive(&fixture, 101)?.expect("first activated").message_id,
        "same-id"
    );
    assert_eq!(
        receive(&fixture, 102)?
            .expect("different id activated")
            .message_id,
        "another-id"
    );
    assert_eq!(receive(&fixture, 103)?, None);
    Ok(())
}

fn activation_does_not_deduplicate_previously_accepted_schedules<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    schedule(&fixture, 10, vec![scheduled("same-id", 100_000)])?;
    schedule(&fixture, 10 + WINDOW, vec![scheduled("same-id", 100_000)])?;
    assert_eq!(
        fixture.at(100_000, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    assert_eq!(
        receive(&fixture, 100_001)?
            .expect("first schedule activated")
            .sequence,
        SequenceNumber::new(3)
    );
    assert_eq!(
        receive(&fixture, 100_002)?
            .expect("second schedule activated")
            .sequence,
        SequenceNumber::new(4)
    );
    assert_eq!(receive(&fixture, 100_003)?, None);
    Ok(())
}

fn anonymous_messages_bypass_duplicate_detection<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "", b"first")?;
    send(&fixture, 11, "", b"second")?;
    schedule(&fixture, 12, vec![scheduled("", 100), scheduled("", 100)])?;
    assert_eq!(history_deadline(&fixture, "")?, None);
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    for millis in 101..105 {
        assert!(receive(&fixture, millis)?.is_some());
    }
    assert_eq!(receive(&fixture, 105)?, None);
    Ok(())
}

fn history_is_isolated_by_namespace_and_entity<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let mut other_entity = fixture.command(10, CommandKind::CreateQueue { config: config() });
    other_entity.entity = domain::EntityPath::new("other-orders")?;
    fixture.machine.apply(&other_entity)?;
    let mut other_namespace = fixture.command(10, CommandKind::CreateQueue { config: config() });
    other_namespace.namespace = domain::NamespaceName::new("other-tenant")?;
    fixture.machine.apply(&other_namespace)?;
    for (namespace, entity) in [
        (&fixture.namespace, &fixture.entity),
        (&other_entity.namespace, &other_entity.entity),
        (&other_namespace.namespace, &other_namespace.entity),
    ] {
        let mut command = fixture.command(
            20,
            CommandKind::Send {
                message_id: String::from("same-id"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
            },
        );
        command.namespace = namespace.clone();
        command.entity = entity.clone();
        assert_eq!(
            fixture.machine.apply(&command)?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            }
        );
        assert_eq!(
            fixture.machine.apply(&command)?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            }
        );
        assert_eq!(
            fixture.machine.ready_sequences(namespace, entity, 10)?,
            vec![SequenceNumber::new(1)]
        );
    }
    Ok(())
}

fn history_keys_preserve_embedded_zero_bytes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let ids = ["id", "id\0one", "id\0two"];
    for id in ids {
        send(&fixture, 10, id, b"original")?;
        send(&fixture, 10, id, b"dropped")?;
    }
    for (index, id) in ids.into_iter().enumerate() {
        assert_eq!(
            receive(&fixture, 20 + index as u64)?
                .expect("distinct identifier retained")
                .message_id,
            id
        );
    }
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 3 }
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity),
                10
            )?
            .is_empty()
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_expiry_prefix(&fixture.namespace, &fixture.entity),
                10
            )?
            .is_empty()
    );
    Ok(())
}

fn message_id_limits<P: StoreProvider>(provider: P, enabled: bool) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: enabled,
            ..config()
        },
    )?;
    let ascii_limit = "a".repeat(MAX_MESSAGE_ID_LENGTH);
    send(&fixture, 10, &ascii_limit, b"ASCII limit")?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        send(
            &fixture,
            11,
            &"a".repeat(MAX_MESSAGE_ID_LENGTH + 1),
            b"too long"
        ),
        Err(BrokerError::MessageIdTooLong {
            length: MAX_MESSAGE_ID_LENGTH + 1,
            maximum: MAX_MESSAGE_ID_LENGTH
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let astral_limit = "\u{1f642}".repeat(MAX_MESSAGE_ID_LENGTH / 2);
    send(&fixture, 12, &astral_limit, b"UTF-16 limit")?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        send(
            &fixture,
            13,
            &"\u{1f642}".repeat(MAX_MESSAGE_ID_LENGTH / 2 + 1),
            b"too long"
        ),
        Err(BrokerError::MessageIdTooLong {
            length: MAX_MESSAGE_ID_LENGTH + 2,
            maximum: MAX_MESSAGE_ID_LENGTH
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        receive(&fixture, 20)?
            .expect("ASCII limit accepted")
            .message_id,
        ascii_limit
    );
    assert_eq!(
        receive(&fixture, 21)?
            .expect("UTF-16 limit accepted")
            .message_id,
        astral_limit
    );
    Ok(())
}

fn identifier_limits_apply_with_detection_enabled<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    message_id_limits(provider, true)
}

fn identifier_limits_apply_with_detection_disabled<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    message_id_limits(provider, false)
}

fn invalid_schedule_batches_do_not_record_ids_or_advance_counters<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let before = fixture.machine.store().snapshot()?;
    let too_long = "a".repeat(MAX_MESSAGE_ID_LENGTH + 1);
    assert_eq!(
        schedule(
            &fixture,
            10,
            vec![scheduled("valid-id", 100), scheduled(&too_long, 100)]
        ),
        Err(BrokerError::MessageIdTooLong {
            length: MAX_MESSAGE_ID_LENGTH + 1,
            maximum: MAX_MESSAGE_ID_LENGTH
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        send(&fixture, 11, "valid-id", b"accepted")?,
        SequenceNumber::new(1)
    );
    assert!(receive(&fixture, 12)?.is_some());
    Ok(())
}

fn duplicate_payloads_are_validated_before_being_dropped<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 4,
            ..config()
        },
    )?;
    send(&fixture, 10, "same-id", b"okay")?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        send(&fixture, 11, "same-id", b"large"),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 5,
            maximum_bytes: 4
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn nonpartitioned_session_queues_deduplicate_across_sessions<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..config()
        },
    )?;
    let first_session = SessionId::new("first")?;
    let second_session = SessionId::new("second")?;
    for session in [&first_session, &second_session] {
        fixture.at(
            10,
            CommandKind::Send {
                message_id: String::from("same-id"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: Some(session.clone()),
            },
        )?;
    }
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &first_session,
            10
        )?,
        vec![SequenceNumber::new(1)]
    );
    assert!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &second_session, 10)?
            .is_empty()
    );
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            11,
            CommandKind::Send {
                message_id: String::from("same-id"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None
            }
        ),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn history_cleanup_is_bounded_and_resumable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let messages = (0..=TIMER_SCAN_LIMIT)
        .map(|index| scheduled(&format!("id-{index}"), 100_000))
        .collect();
    schedule(&fixture, 10, messages)?;
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired {
            expired: TIMER_SCAN_LIMIT as u32
        }
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity),
                TIMER_SCAN_LIMIT + 1
            )?
            .len(),
        1
    );
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 0 }
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::scheduled_prefix(&fixture.namespace, &fixture.entity),
                TIMER_SCAN_LIMIT + 1
            )?
            .len(),
        TIMER_SCAN_LIMIT + 1
    );
    Ok(())
}

fn cleanup_does_not_remove_a_more_recent_retention<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "same-id", b"first")?;
    send(&fixture, 10 + WINDOW, "same-id", b"second")?;
    let mut batch = WriteBatch::default();
    batch.push_put(
        keys::duplicate_history_expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(10 + WINDOW),
            "same-id",
        ),
        Vec::new(),
    );
    fixture.machine.store().apply(batch)?;
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(
        history_deadline(&fixture, "same-id")?,
        Some(Timestamp::from_millis(10 + 2 * WINDOW))
    );
    send(&fixture, 11 + WINDOW, "same-id", b"dropped")?;
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?
            .len(),
        2
    );
    Ok(())
}

fn history_survives_restart_after_the_original_was_consumed<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "same-id", b"first")?;
    receive(&fixture, 20)?.expect("original consumed");
    let fixture = fixture.restart()?;
    assert_eq!(
        send(&fixture, 30, "same-id", b"dropped")?,
        SequenceNumber::new(2)
    );
    assert_eq!(receive(&fixture, 31)?, None);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(10 + WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(
        send(&fixture, 11 + WINDOW, "same-id", b"after cleanup")?,
        SequenceNumber::new(3)
    );
    assert_eq!(
        receive(&fixture, 12 + WINDOW)?
            .expect("new original accepted")
            .body,
        b"after cleanup"
    );
    Ok(())
}

fn an_idle_history_sweep_commits_nothing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(10, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 0 }
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    send(&fixture, 10, "same-id", b"first")?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(11, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 0 }
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn stored_legacy_configurations_are_migrated_on_the_machine_read_path<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..config()
        },
    )?;
    let legacy = QueueConfig {
        lock_duration_millis: 20_000,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(500),
        dead_lettering_on_message_expiration: true,
        max_message_bytes: 512,
        requires_session: true,
        ..QueueConfig::default()
    };
    let payload = postcard::to_stdvec(&(
        legacy.lock_duration_millis,
        legacy.max_delivery_count,
        legacy.default_time_to_live_millis,
        legacy.max_message_bytes,
        legacy.requires_session,
    ))?;
    let session = SessionId::new("cart")?;
    for version in codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V5 {
        let mut envelope = vec![version];
        envelope.extend_from_slice(&payload);
        let mut batch = WriteBatch::default();
        batch.push_put(
            keys::queue_config(&fixture.namespace, &fixture.entity),
            envelope,
        );
        batch.push_put(
            keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?),
            codec::encode(&legacy.dead_letter_shadow())?,
        );
        fixture.machine.store().apply(batch)?;
        assert_eq!(
            fixture
                .machine
                .queue_config(&fixture.namespace, &fixture.entity)?,
            Some(legacy)
        );
        for _ in 0..2 {
            fixture.at(
                10 + u64::from(version),
                CommandKind::Send {
                    message_id: format!("same-id-{version}"),
                    body: Vec::new(),
                    time_to_live_millis: None,
                    session_id: Some(session.clone()),
                },
            )?;
        }
    }
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?,
        Some(legacy)
    );
    assert_eq!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &session, 16)?
            .len(),
        10
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity),
                16
            )?
            .is_empty()
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }
        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    duplicate_submissions_are_accepted_without_replacing_the_original,
    disabled_detection_retains_repeated_message_ids,
    an_elapsed_window_accepts_a_new_original_without_waiting_for_cleanup,
    consuming_a_message_does_not_forget_its_history,
    expired_messages_leave_duplicate_history_in_the_parent_only,
    cancelling_a_schedule_does_not_forget_its_history,
    scheduled_and_ordinary_submissions_share_one_history,
    duplicate_ids_within_a_schedule_batch_keep_only_the_first,
    activation_does_not_deduplicate_previously_accepted_schedules,
    anonymous_messages_bypass_duplicate_detection,
    history_is_isolated_by_namespace_and_entity,
    history_keys_preserve_embedded_zero_bytes,
    identifier_limits_apply_with_detection_enabled,
    identifier_limits_apply_with_detection_disabled,
    invalid_schedule_batches_do_not_record_ids_or_advance_counters,
    duplicate_payloads_are_validated_before_being_dropped,
    nonpartitioned_session_queues_deduplicate_across_sessions,
    history_cleanup_is_bounded_and_resumable,
    cleanup_does_not_remove_a_more_recent_retention,
    history_survives_restart_after_the_original_was_consumed,
    an_idle_history_sweep_commits_nothing,
    stored_legacy_configurations_are_migrated_on_the_machine_read_path,
}
