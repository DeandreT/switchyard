//! Expiration drops by default; older queue settings retain their DLQ policy.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, MessageBody,
    MessageEnvelope, MessageIdentifier, MessageProperties, MessageStatus, MessageValue,
    QueueConfig, ReceiveMode, ScheduledEnvelope, ScheduledMessage, SequenceNumber, SessionHold,
    SessionId, TIMER_SCAN_LIMIT, Timestamp, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn rich(id: &str) -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(id.to_owned())),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([(String::from("producer"), MessageValue::Uint(7))]),
        body: MessageBody::Data(vec![b"pay".to_vec(), b"load".to_vec()]),
        ..MessageEnvelope::default()
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    typed: bool,
) -> Result<SequenceNumber, BrokerError> {
    let kind = if typed {
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: None,
            envelope: Box::new(rich(id)),
        }
    } else {
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: None,
        }
    };
    match fixture.at(millis, kind)? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected sent outcome, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn assert_removed<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequence: SequenceNumber,
) -> Result<(), Box<dyn Error>> {
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .is_none()
    );
    for prefix in [
        keys::ready_prefix(&fixture.namespace, &fixture.entity),
        keys::entity_session_ready_prefix(&fixture.namespace, &fixture.entity),
        keys::lock_prefix(&fixture.namespace, &fixture.entity),
        keys::expiry_prefix(&fixture.namespace, &fixture.entity),
    ] {
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(&prefix, usize::MAX)?
                .iter()
                .all(|(key, _)| keys::trailing_sequence(key) != Some(sequence))
        );
    }
    let shadow = fixture.entity.dead_letter_queue()?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &shadow, sequence)?
            .is_none()
    );
    Ok(())
}

fn default_policy_lazily_drops_legacy_and_rich_messages_without_acquiring_locks<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert!(
        !fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?
            .expect("queue")
            .dead_lettering_on_message_expiration
    );
    let first = send(&fixture, 10, "legacy", Some(5), false)?;
    let second = send(&fixture, 11, "rich", Some(5), true)?;
    let counters = fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?;
    assert_eq!(receive(&fixture, 20)?, None);
    assert_removed(&fixture, first)?;
    assert_removed(&fixture, second)?;
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?,
        counters
    );
    let fixture = fixture.restart()?;
    assert_eq!(receive(&fixture, 21)?, None);
    Ok(())
}

fn default_policy_timer_drop_is_bounded_and_reports_processed_entries<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for index in 0..=TIMER_SCAN_LIMIT {
        send(
            &fixture,
            10,
            &format!("message-{index}"),
            Some(5),
            index % 2 == 0,
        )?;
    }
    assert_eq!(
        fixture.at(15, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: TIMER_SCAN_LIMIT as u32,
            processed: TIMER_SCAN_LIMIT as u32,
        }
    );
    assert_eq!(
        fixture.at(15, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 1,
            processed: 1,
        }
    );
    assert_eq!(
        fixture.at(16, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    assert_eq!(receive(&fixture, 17)?, None);
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::message_prefix(&fixture.namespace, &fixture.entity.dead_letter_queue()?),
                usize::MAX
            )?
            .is_empty()
    );
    Ok(())
}

fn live_locks_allow_renewal_and_completion_but_release_drops_expired_content<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let first = send(&fixture, 10, "complete", Some(5), true)?;
    let delivery = receive(&fixture, 11)?.expect("live delivery");
    let token = delivery.lock.expect("lock").token;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0
        }
    );
    fixture.at(
        21,
        CommandKind::RenewLock {
            sequence: first,
            lock_token: token,
            lock_duration_millis: Some(100),
        },
    )?;
    assert_eq!(
        fixture.at(
            22,
            CommandKind::Complete {
                sequence: first,
                lock_token: token
            }
        )?,
        CommandOutcome::Completed
    );
    assert_removed(&fixture, first)?;

    let second = send(&fixture, 30, "lock-expiry", Some(5), false)?;
    let delivery = receive(&fixture, 31)?.expect("live delivery");
    let deadline = delivery.lock.expect("lock").locked_until.as_millis();
    assert_eq!(
        fixture.at(deadline, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0,
            dropped: 1
        }
    );
    assert_removed(&fixture, second)?;

    let third = send(&fixture, 140, "abandon", Some(5), true)?;
    let delivery = receive(&fixture, 141)?.expect("live delivery");
    assert_eq!(
        fixture.at(
            146,
            CommandKind::Abandon {
                sequence: third,
                lock_token: delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false,
            dropped: true
        }
    );
    assert_removed(&fixture, third)?;
    Ok(())
}

fn ttl_drop_precedes_delivery_limit_without_disabling_other_dead_letter_paths<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?;
    let expired = send(&fixture, 10, "expired-limit", Some(5), false)?;
    let delivery = receive(&fixture, 11)?.expect("live delivery");
    assert_eq!(
        fixture.at(
            16,
            CommandKind::Abandon {
                sequence: expired,
                lock_token: delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false,
            dropped: true
        }
    );
    assert_removed(&fixture, expired)?;
    let max_delivery = send(&fixture, 20, "delivery-limit", None, true)?;
    let delivery = receive(&fixture, 21)?.expect("live delivery");
    assert_eq!(
        fixture.at(
            22,
            CommandKind::Abandon {
                sequence: max_delivery,
                lock_token: delivery.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false
        }
    );
    let shadow = fixture.entity.dead_letter_queue()?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &shadow, max_delivery)?
            .expect("DLQ record")
            .dead_letter
            .expect("reason")
            .reason,
        DeadLetterReason::MaxDeliveryCountExceeded
    );
    let explicit = send(&fixture, 30, "explicit", None, false)?;
    let delivery = receive(&fixture, 31)?.expect("live delivery");
    assert_eq!(
        fixture.at(
            32,
            CommandKind::DeadLetter {
                sequence: explicit,
                lock_token: delivery.lock.expect("lock").token,
                reason: String::from("explicit"),
                description: String::new()
            }
        )?,
        CommandOutcome::DeadLettered
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &shadow, explicit)?
            .expect("DLQ record")
            .dead_letter
            .expect("reason")
            .reason,
        DeadLetterReason::Application(String::from("explicit"))
    );
    Ok(())
}

fn expired_deferred_messages_remain_peekable_until_explicit_retrieval_drops_them<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let start = 10 + index as u64 * 20;
        let sequence = send(
            &fixture,
            start,
            &format!("deferred-{index}"),
            Some(5),
            index == 0,
        )?;
        let delivery = receive(&fixture, start + 1)?.expect("live delivery");
        fixture.at(
            start + 2,
            CommandKind::Defer {
                sequence,
                lock_token: delivery.lock.expect("lock").token,
            },
        )?;
        assert_eq!(
            fixture.at(start + 10, CommandKind::ExpireMessages)?,
            CommandOutcome::MessagesExpired {
                dead_lettered: 0,
                dropped: 0,
                processed: 0
            }
        );
        let CommandOutcome::Peeked(deliveries) = fixture.at(
            start + 10,
            CommandKind::Peek {
                from_sequence: sequence,
                max_messages: 1,
                session_id: None,
            },
        )?
        else {
            panic!("peek outcome")
        };
        assert_eq!(deliveries[0].status, MessageStatus::Deferred);
        assert_eq!(
            fixture.at(
                start + 11,
                CommandKind::ReceiveDeferred {
                    sequences: vec![sequence],
                    mode,
                    lock_duration_millis: Some(100),
                    session_id: None
                }
            )?,
            CommandOutcome::DeferredReceived(Vec::new())
        );
        assert_removed(&fixture, sequence)?;
        let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
        assert_eq!(
            fixture.at(
                start + 12,
                CommandKind::ReceiveDeferred {
                    sequences: vec![sequence],
                    mode,
                    lock_duration_millis: None,
                    session_id: None
                }
            ),
            Err(BrokerError::MessageNotFound { sequence })
        );
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
    }
    Ok(())
}

fn scheduled_legacy_and_rich_messages_start_their_lifetime_at_activation_then_drop<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    fixture.at(
        10,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: String::from("legacy"),
                body: Vec::new(),
                time_to_live_millis: Some(5),
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
            }],
        },
    )?;
    fixture.at(
        11,
        CommandKind::ScheduleEnvelopes {
            messages: vec![ScheduledEnvelope {
                message_id: String::from("rich"),
                body: b"payload".to_vec(),
                time_to_live_millis: Some(5),
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
                envelope: rich("rich"),
            }],
        },
    )?;
    assert_eq!(
        fixture.at(99, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0
        }
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(101, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    assert_eq!(
        fixture.at(105, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0
        }
    );
    assert_eq!(
        fixture.at(106, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 2,
            processed: 2
        }
    );
    assert_eq!(receive(&fixture, 107)?, None);
    assert_removed(&fixture, SequenceNumber::new(3))?;
    assert_removed(&fixture, SequenceNumber::new(4))?;
    Ok(())
}

fn lazy_session_expiry_removes_only_the_expired_message_and_preserves_the_session_hold<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let session = SessionId::new("cart")?;
    for (id, ttl) in [("expired", Some(5)), ("live", None)] {
        fixture.at(
            10,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: id.as_bytes().to_vec(),
                time_to_live_millis: ttl,
                session_id: Some(session.clone()),
            },
        )?;
    }
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        11,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: Some(100),
        },
    )?
    else {
        panic!("session accepted")
    };
    let hold = SessionHold {
        session_id: session,
        token: accepted.lock.token,
    };
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: Some(hold.clone()),
        },
    )?
    else {
        panic!("unexpired session message")
    };
    assert_eq!(delivery.message_id, "live");
    assert_removed(&fixture, SequenceNumber::new(1))?;
    fixture.at(
        21,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("lock").token,
        },
    )?;
    assert!(matches!(
        fixture.at(
            22,
            CommandKind::RenewSessionLock {
                session: hold,
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::SessionLockRenewed { .. }
    ));
    Ok(())
}

fn dropping_expired_content_keeps_duplicate_history_until_its_own_deadline<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        },
    )?;
    let sequence = send(&fixture, 10, "same", Some(5), true)?;
    fixture.at(15, CommandKind::ExpireMessages)?;
    assert_removed(&fixture, sequence)?;
    let history = keys::duplicate_history(&fixture.namespace, &fixture.entity, "same");
    assert!(fixture.machine.store().get(&history)?.is_some());
    assert_eq!(
        send(&fixture, 16, "same", None, false)?,
        SequenceNumber::new(2)
    );
    assert_eq!(receive(&fixture, 17)?, None);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(20_010, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 1 }
    );
    assert_eq!(
        send(&fixture, 20_011, "same", None, true)?,
        SequenceNumber::new(3)
    );
    assert!(receive(&fixture, 20_012)?.is_some());
    Ok(())
}

fn malformed_expiry_index_rolls_back_prior_drops_and_restart_keeps_the_policy<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let first = send(&fixture, 10, "valid", Some(10), false)?;
    let second = send(&fixture, 10, "mismatched", Some(11), true)?;
    let malformed = keys::expiry(
        &fixture.namespace,
        &fixture.entity,
        Timestamp::from_millis(20),
        second,
    );
    let mut batch = WriteBatch::default();
    batch.push_put(malformed.clone(), Vec::new());
    fixture.machine.store().apply(batch)?;
    let fixture = fixture.restart()?;
    assert!(
        !fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?
            .expect("queue config")
            .dead_lettering_on_message_expiration
    );
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    let mut batch = WriteBatch::default();
    batch.push_delete(malformed);
    fixture.machine.store().apply(batch)?;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 1,
            processed: 1
        }
    );
    assert_removed(&fixture, first)?;
    assert_eq!(
        fixture.at(21, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 1,
            processed: 1
        }
    );
    assert_removed(&fixture, second)?;
    Ok(())
}

fn genuine_v6_and_v7_queue_settings_keep_expiration_dead_letters_and_duplicate_detection<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let legacy = QueueConfig {
        lock_duration_millis: 50,
        max_delivery_count: 4,
        default_time_to_live_millis: Some(7),
        max_message_bytes: 256 * 1024,
        requires_session: false,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        dead_lettering_on_message_expiration: true,
    };
    let payload = postcard::to_stdvec(&(
        legacy.lock_duration_millis,
        legacy.max_delivery_count,
        legacy.default_time_to_live_millis,
        legacy.max_message_bytes,
        legacy.requires_session,
        legacy.requires_duplicate_detection,
        legacy.duplicate_detection_history_time_window_millis,
    ))?;
    for (index, version) in [codec::VALUE_FORMAT_V6, codec::VALUE_FORMAT_V7]
        .into_iter()
        .enumerate()
    {
        let mut stored = vec![version];
        stored.extend_from_slice(&payload);
        let mut batch = WriteBatch::default();
        batch.push_put(
            keys::queue_config(&fixture.namespace, &fixture.entity),
            stored,
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
        let start = 10 + index as u64 * 20;
        let sequence = send(
            &fixture,
            start,
            &format!("legacy-{version}"),
            Some(5),
            index == 1,
        )?;
        assert_eq!(
            fixture.at(start + 5, CommandKind::ExpireMessages)?,
            CommandOutcome::MessagesExpired {
                dead_lettered: 1,
                dropped: 0,
                processed: 1
            }
        );
        let shadow = fixture.entity.dead_letter_queue()?;
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &shadow, sequence)?
                .expect("legacy policy still dead-letters")
                .dead_letter
                .expect("DLQ reason")
                .reason,
            DeadLetterReason::TimeToLiveExpired
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    &format!("legacy-{version}")
                ))?
                .is_some()
        );
        assert!(
            !fixture
                .machine
                .queue_config(&fixture.namespace, &shadow)?
                .expect("shadow config")
                .dead_lettering_on_message_expiration
        );
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    default_policy_lazily_drops_legacy_and_rich_messages_without_acquiring_locks,
    default_policy_timer_drop_is_bounded_and_reports_processed_entries,
    live_locks_allow_renewal_and_completion_but_release_drops_expired_content,
    ttl_drop_precedes_delivery_limit_without_disabling_other_dead_letter_paths,
    expired_deferred_messages_remain_peekable_until_explicit_retrieval_drops_them,
    scheduled_legacy_and_rich_messages_start_their_lifetime_at_activation_then_drop,
    lazy_session_expiry_removes_only_the_expired_message_and_preserves_the_session_hold,
    dropping_expired_content_keeps_duplicate_history_until_its_own_deadline,
    malformed_expiry_index_rolls_back_prior_drops_and_restart_keeps_the_policy,
    genuine_v6_and_v7_queue_settings_keep_expiration_dead_letters_and_duplicate_detection,
}
