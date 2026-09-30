//! Identifier allocation refuses exhausted counters without committing partial work.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, DeliveryBudget, EntityPath, LockToken,
    MAX_SEQUENCE_NUMBER, MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties,
    MessageState, NamespaceName, QueueConfig, QueueConfigUpdate, QueueCounterKind, QueueCounters,
    ReceiveMode, ScheduledEnvelope, ScheduledMessage, SequenceNumber, SessionHold, SessionId,
    Timestamp, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Entries = Vec<(Key, Value)>;

fn snapshot<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<Entries> {
    Ok(fixture.machine.store().snapshot()?.entries().to_vec())
}

fn counters<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<QueueCounters> {
    let bytes = fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
        .expect("seeded or allocated counters");
    Ok(codec::decode(&bytes)?)
}

fn seed<P: StoreProvider>(fixture: &QueueFixture<P>, counters: QueueCounters) -> TestResult {
    let mut batch = WriteBatch::default();
    batch.push_put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&counters)?,
    );
    fixture.machine.store().apply(batch)?;
    Ok(())
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
    counter: QueueCounterKind,
) -> TestResult {
    let before = snapshot(fixture)?;
    assert_eq!(
        fixture
            .machine
            .apply_with_effects(&fixture.command(millis, kind)),
        Err(BrokerError::QueueCounterExhausted { counter })
    );
    assert_eq!(
        snapshot(fixture)?,
        before,
        "records, indexes, history, counters, and clock are unchanged"
    );
    Ok(())
}

fn content(id: &str) -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(id.to_owned())),
            ..MessageProperties::default()
        },
        body: MessageBody::Data(vec![b"payload".to_vec()]),
        ..MessageEnvelope::default()
    }
}

fn send_kind(id: &str, ttl: Option<u64>, session: Option<SessionId>, rich: bool) -> CommandKind {
    if rich {
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: session,
            envelope: Box::new(content(id)),
        }
    } else {
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: session,
        }
    }
}

fn sent<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
) -> TestResult<SequenceNumber> {
    let CommandOutcome::Sent { sequence } = fixture.at(millis, send_kind(id, ttl, None, false))?
    else {
        panic!("sent result");
    };
    Ok(sequence)
}

fn schedule_kind(ids: &[&str], enqueue_at: u64, rich: bool) -> CommandKind {
    if rich {
        CommandKind::ScheduleEnvelopes {
            messages: ids
                .iter()
                .map(|id| ScheduledEnvelope {
                    message_id: (*id).to_owned(),
                    body: b"payload".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(enqueue_at),
                    envelope: content(id),
                })
                .collect(),
        }
    } else {
        CommandKind::Schedule {
            messages: ids
                .iter()
                .map(|id| ScheduledMessage {
                    message_id: (*id).to_owned(),
                    body: b"payload".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(enqueue_at),
                })
                .collect(),
        }
    }
}

fn receive_kind(mode: ReceiveMode, session: Option<SessionHold>) -> CommandKind {
    CommandKind::Receive {
        mode,
        lock_duration_millis: None,
        session,
    }
}

fn received<P: StoreProvider>(fixture: &QueueFixture<P>, millis: u64) -> TestResult<Delivery> {
    let CommandOutcome::Received(Some(delivery)) =
        fixture.at(millis, receive_kind(ReceiveMode::PeekLock, None))?
    else {
        panic!("received message");
    };
    Ok(delivery)
}

fn deferred_kind(sequences: Vec<SequenceNumber>, mode: ReceiveMode, variant: u8) -> CommandKind {
    let budget = DeliveryBudget {
        max_bytes: 1024 * 1024,
        per_message_overhead_bytes: 64,
    };
    match variant {
        0 => CommandKind::ReceiveDeferred {
            sequences,
            mode,
            lock_duration_millis: None,
            session_id: None,
        },
        1 => CommandKind::ReceiveDeferredBounded {
            sequences,
            mode,
            lock_duration_millis: None,
            session_id: None,
            budget,
        },
        _ => CommandKind::ReceiveDeferredHeld {
            sequences,
            mode,
            lock_duration_millis: None,
            session: None,
            budget,
        },
    }
}

fn defer<P: StoreProvider>(fixture: &QueueFixture<P>, millis: u64) -> TestResult<SequenceNumber> {
    let delivery = received(fixture, millis)?;
    fixture.at(
        millis,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: delivery.lock.unwrap().token,
        },
    )?;
    Ok(delivery.sequence)
}

fn send_final_boundary<P: StoreProvider>(provider: P, rich: bool) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        },
    )?;
    assert_eq!(
        fixture.at(1, send_kind("last", None, None, rich))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(MAX_SEQUENCE_NUMBER)
        }
    );
    assert_eq!(counters(&fixture)?.next_sequence, MAX_SEQUENCE_NUMBER + 1);
    let record = fixture
        .machine
        .message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(MAX_SEQUENCE_NUMBER),
        )?
        .unwrap();
    assert_eq!(record.message_id, "last");
    assert_eq!(record.envelope.is_some(), rich);
    let fixture = fixture.restart()?;
    for millis in [2, 3] {
        reject(
            &fixture,
            millis,
            send_kind("refused", None, None, rich),
            QueueCounterKind::Sequence,
        )?;
    }
    let before = snapshot(&fixture)?;
    assert!(matches!(
        fixture.at(3, send_kind(&"x".repeat(129), None, None, rich)),
        Err(BrokerError::MessageIdTooLong { .. })
    ));
    assert_eq!(snapshot(&fixture)?, before);
    let CommandOutcome::Received(Some(delivery)) =
        fixture.at(2, receive_kind(ReceiveMode::ReceiveAndDelete, None))?
    else {
        panic!("nonallocating receive");
    };
    assert_eq!(delivery.sequence.as_u64(), MAX_SEQUENCE_NUMBER);
    assert_eq!(counters(&fixture)?.next_sequence, MAX_SEQUENCE_NUMBER + 1);
    Ok(())
}

fn legacy_send_boundary<P: StoreProvider>(provider: P) -> TestResult {
    send_final_boundary(provider, false)
}
fn rich_send_boundary<P: StoreProvider>(provider: P) -> TestResult {
    send_final_boundary(provider, true)
}

fn schedule_final_boundary<P: StoreProvider>(provider: P, rich: bool) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        },
    )?;
    reject(
        &fixture,
        1,
        schedule_kind(&["first", "second"], 100, rich),
        QueueCounterKind::Sequence,
    )?;
    reject(
        &fixture,
        2,
        schedule_kind(&["same", "same"], 100, rich),
        QueueCounterKind::Sequence,
    )?;
    assert_eq!(
        fixture.at(1, schedule_kind(&["last"], 100, rich))?,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(MAX_SEQUENCE_NUMBER)]
        }
    );
    let fixture = fixture.restart()?;
    reject(
        &fixture,
        100,
        CommandKind::ActivateScheduled,
        QueueCounterKind::Sequence,
    )?;
    assert_eq!(
        fixture.at(
            2,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(MAX_SEQUENCE_NUMBER)]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(100, schedule_kind(&[], 101, rich))?,
        CommandOutcome::Scheduled { sequences: vec![] }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(snapshot(&fixture)?, before);
    assert_eq!(counters(&fixture)?.next_sequence, MAX_SEQUENCE_NUMBER + 1);
    Ok(())
}

fn legacy_schedule_boundary<P: StoreProvider>(provider: P) -> TestResult {
    schedule_final_boundary(provider, false)
}
fn rich_schedule_boundary<P: StoreProvider>(provider: P) -> TestResult {
    schedule_final_boundary(provider, true)
}

fn activation_final_boundary<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER - 1,
            next_lock_token: 1,
        },
    )?;
    assert_eq!(
        fixture.at(1, schedule_kind(&["scheduled"], 10, true))?,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(MAX_SEQUENCE_NUMBER - 1)]
        }
    );
    assert_eq!(
        fixture.at(10, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert!(
        fixture
            .machine
            .message(
                &fixture.namespace,
                &fixture.entity,
                SequenceNumber::new(MAX_SEQUENCE_NUMBER - 1)
            )?
            .is_none()
    );
    let record = fixture
        .machine
        .message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(MAX_SEQUENCE_NUMBER),
        )?
        .unwrap();
    assert_eq!(record.message_id, "scheduled");
    assert_eq!(record.enqueued_at, Timestamp::from_millis(10));
    assert_eq!(counters(&fixture)?.next_sequence, MAX_SEQUENCE_NUMBER + 1);
    let fixture = fixture.restart()?;
    reject(
        &fixture,
        11,
        send_kind("later", None, None, false),
        QueueCounterKind::Sequence,
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(11, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(snapshot(&fixture)?, before);
    Ok(())
}

fn activation_batch_rollback<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert_eq!(
        fixture.at(1, schedule_kind(&["first", "second"], 10, false))?,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        }
    );
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::ActivateScheduled,
        QueueCounterKind::Sequence,
    )?;
    let before = snapshot(&fixture)?;
    let fixture = fixture.restart()?;
    assert_eq!(snapshot(&fixture)?, before);
    reject(
        &fixture,
        10,
        CommandKind::ActivateScheduled,
        QueueCounterKind::Sequence,
    )?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 2 }
    );
    Ok(())
}

fn duplicate_acknowledgements_still_consume_sequences<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let original = sent(&fixture, 1, "duplicate", None)?;
    let original_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, original)?
        .unwrap();
    let history = fixture.machine.store().scan_prefix(
        &keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity),
        usize::MAX,
    )?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER - 1,
            next_lock_token: 1,
        },
    )?;
    assert_eq!(
        fixture.at(2, send_kind("duplicate", None, None, true))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(MAX_SEQUENCE_NUMBER - 1)
        }
    );
    assert_eq!(
        fixture.at(3, schedule_kind(&["duplicate"], 100, true))?,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(MAX_SEQUENCE_NUMBER)]
        }
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(
            &keys::duplicate_history_prefix(&fixture.namespace, &fixture.entity),
            usize::MAX
        )?,
        history
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, original)?
            .unwrap(),
        original_record
    );
    for sequence in [MAX_SEQUENCE_NUMBER - 1, MAX_SEQUENCE_NUMBER] {
        assert!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    &fixture.entity,
                    SequenceNumber::new(sequence)
                )?
                .is_none()
        );
    }
    reject(
        &fixture,
        4,
        send_kind("duplicate", None, None, false),
        QueueCounterKind::Sequence,
    )?;
    reject(
        &fixture,
        4,
        schedule_kind(&["duplicate"], 100, false),
        QueueCounterKind::Sequence,
    )?;
    Ok(())
}

fn ordinary_lock_boundary_and_nonallocating_settlement<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let first = sent(&fixture, 1, "first", None)?;
    let second = sent(&fixture, 2, "second", None)?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 3,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let delivery = received(&fixture, 3)?;
    assert_eq!(delivery.sequence, first);
    let lock = delivery.lock.unwrap();
    assert_eq!(lock.token, LockToken::new(u64::MAX - 1));
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    let fixture = fixture.restart()?;
    reject(
        &fixture,
        4,
        receive_kind(ReceiveMode::PeekLock, None),
        QueueCounterKind::LockToken,
    )?;
    assert_eq!(
        fixture.at(
            4,
            CommandKind::RenewLock {
                sequence: first,
                lock_token: lock.token,
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(104)
        }
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?
            .unwrap()
            .state,
        MessageState::Locked {
            token: lock.token,
            locked_until: Timestamp::from_millis(104)
        }
    );
    assert_eq!(
        fixture.at(
            5,
            CommandKind::Abandon {
                sequence: first,
                lock_token: lock.token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false,
            dropped: false
        }
    );
    reject(
        &fixture,
        6,
        receive_kind(ReceiveMode::PeekLock, None),
        QueueCounterKind::LockToken,
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(
            6,
            CommandKind::Complete {
                sequence: first,
                lock_token: lock.token
            }
        ),
        Err(BrokerError::MessageNotLocked { sequence: first })
    );
    assert_eq!(snapshot(&fixture)?, before);
    for (millis, sequence) in [(6, first), (7, second)] {
        let CommandOutcome::Received(Some(delivery)) =
            fixture.at(millis, receive_kind(ReceiveMode::ReceiveAndDelete, None))?
        else {
            panic!("nonallocating delivery");
        };
        assert_eq!(delivery.sequence, sequence);
        assert!(delivery.lock.is_none());
    }
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(8, receive_kind(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(snapshot(&fixture)?, before);
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    Ok(())
}

fn lazy_expiry_rollback<P: StoreProvider>(provider: P, dead_letter: bool) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: dead_letter,
            ..QueueConfig::default()
        },
    )?;
    let expired = sent(&fixture, 1, "expired", Some(5))?;
    let live = sent(&fixture, 2, "live", None)?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 3,
            next_lock_token: u64::MAX,
        },
    )?;
    reject(
        &fixture,
        10,
        receive_kind(ReceiveMode::PeekLock, None),
        QueueCounterKind::LockToken,
    )?;
    let before = snapshot(&fixture)?;
    let fixture = fixture.restart()?;
    assert_eq!(snapshot(&fixture)?, before);
    reject(
        &fixture,
        10,
        receive_kind(ReceiveMode::PeekLock, None),
        QueueCounterKind::LockToken,
    )?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, expired)?
            .is_some()
    );
    let application = fixture.machine.apply_with_effects(
        &fixture.command(10, receive_kind(ReceiveMode::ReceiveAndDelete, None)),
    )?;
    assert_eq!(application.dead_letters_enqueued, dead_letter);
    let CommandOutcome::Received(Some(delivery)) = application.outcome else {
        panic!("live nonallocating delivery");
    };
    assert_eq!(delivery.sequence, live);
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, expired)?
            .is_none()
    );
    assert_eq!(
        fixture
            .machine
            .message(
                &fixture.namespace,
                &fixture.entity.dead_letter_queue()?,
                expired
            )?
            .is_some(),
        dead_letter
    );
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    Ok(())
}

fn lazy_dead_letter_rollback<P: StoreProvider>(provider: P) -> TestResult {
    lazy_expiry_rollback(provider, true)
}
fn lazy_drop_rollback<P: StoreProvider>(provider: P) -> TestResult {
    lazy_expiry_rollback(provider, false)
}

fn deferred_batch_all_variants_roll_back_cleanup_and_partial_locks<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let expired = sent(&fixture, 1, "expired", Some(10))?;
    let first = sent(&fixture, 2, "first", None)?;
    let second = sent(&fixture, 3, "second", None)?;
    assert_eq!(defer(&fixture, 4)?, expired);
    assert_eq!(defer(&fixture, 5)?, first);
    assert_eq!(defer(&fixture, 6)?, second);
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 4,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let sequences = vec![expired, first, second];
    for variant in 0..3 {
        reject(
            &fixture,
            20,
            deferred_kind(sequences.clone(), ReceiveMode::PeekLock, variant),
            QueueCounterKind::LockToken,
        )?;
    }
    let before = snapshot(&fixture)?;
    let fixture = fixture.restart()?;
    assert_eq!(snapshot(&fixture)?, before);
    reject(
        &fixture,
        20,
        deferred_kind(sequences.clone(), ReceiveMode::PeekLock, 2),
        QueueCounterKind::LockToken,
    )?;
    let application = fixture.machine.apply_with_effects(&fixture.command(
        20,
        deferred_kind(sequences, ReceiveMode::ReceiveAndDelete, 2),
    ))?;
    assert!(application.dead_letters_enqueued);
    let CommandOutcome::DeferredReceived(deliveries) = application.outcome else {
        panic!("deferred deliveries");
    };
    assert_eq!(
        deliveries
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert!(deliveries.iter().all(|delivery| delivery.lock.is_none()));
    assert_eq!(
        counters(&fixture)?.next_lock_token,
        u64::MAX - 1,
        "failed batches never consumed the final token"
    );
    let before = snapshot(&fixture)?;
    for variant in 0..3 {
        assert_eq!(
            fixture.at(21, deferred_kind(vec![], ReceiveMode::PeekLock, variant))?,
            CommandOutcome::DeferredReceived(vec![])
        );
    }
    assert_eq!(snapshot(&fixture)?, before);
    Ok(())
}

fn deferred_final_token_and_receive_delete_at_exhaustion<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let sequence = sent(&fixture, 1, "deferred", None)?;
    assert_eq!(defer(&fixture, 2)?, sequence);
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 2,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let CommandOutcome::DeferredReceived(deliveries) =
        fixture.at(3, deferred_kind(vec![sequence], ReceiveMode::PeekLock, 2))?
    else {
        panic!("one held deferred delivery");
    };
    let lock = deliveries[0].lock.unwrap();
    assert_eq!(lock.token, LockToken::new(u64::MAX - 1));
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    fixture.at(
        4,
        CommandKind::RenewLock {
            sequence,
            lock_token: lock.token,
            lock_duration_millis: None,
        },
    )?;
    fixture.at(
        5,
        CommandKind::Defer {
            sequence,
            lock_token: lock.token,
        },
    )?;
    for variant in 0..3 {
        reject(
            &fixture,
            6,
            deferred_kind(vec![sequence], ReceiveMode::PeekLock, variant),
            QueueCounterKind::LockToken,
        )?;
    }
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(
            6,
            CommandKind::Complete {
                sequence,
                lock_token: lock.token
            }
        ),
        Err(BrokerError::MessageNotLocked { sequence })
    );
    assert_eq!(snapshot(&fixture)?, before);
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        6,
        deferred_kind(vec![sequence], ReceiveMode::ReceiveAndDelete, 0),
    )?
    else {
        panic!("nonallocating deferred receive");
    };
    assert_eq!(deliveries.len(), 1);
    assert!(deliveries[0].lock.is_none());
    Ok(())
}

fn session_final_token_preserves_live_hold_operations<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let session_id = SessionId::new("customer")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 1,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        1,
        CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("final session token");
    };
    let hold = accepted.hold();
    assert_eq!(hold.token, LockToken::new(u64::MAX - 1));
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    fixture.at(
        2,
        send_kind("session-message", None, Some(session_id.clone()), true),
    )?;
    reject(
        &fixture,
        3,
        receive_kind(ReceiveMode::PeekLock, Some(hold.clone())),
        QueueCounterKind::LockToken,
    )?;
    assert!(matches!(
        fixture.at(
            3,
            receive_kind(ReceiveMode::ReceiveAndDelete, Some(hold.clone()))
        )?,
        CommandOutcome::Received(Some(_))
    ));
    fixture.at(
        4,
        CommandKind::RenewSessionLock {
            session: hold.clone(),
            lock_duration_millis: Some(100),
        },
    )?;
    fixture.at(
        4,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: b"checkpoint".to_vec(),
        },
    )?;
    assert_eq!(
        fixture.at(
            4,
            CommandKind::GetSessionState {
                session: hold.clone()
            }
        )?,
        CommandOutcome::SessionState(b"checkpoint".to_vec())
    );
    let stale = SessionHold {
        session_id: session_id.clone(),
        token: LockToken::new(u64::MAX - 2),
    };
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(5, receive_kind(ReceiveMode::PeekLock, Some(stale))),
        Err(BrokerError::SessionLockNotHeld {
            session_id: session_id.clone()
        })
    );
    assert_eq!(snapshot(&fixture)?, before);
    fixture.at(
        5,
        CommandKind::ReleaseSession {
            session: hold.clone(),
        },
    )?;
    reject(
        &fixture,
        6,
        CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: None,
        },
        QueueCounterKind::LockToken,
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(
            6,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None
            }
        )?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(snapshot(&fixture)?, before);
    fixture.at(
        7,
        send_kind("ready-session", None, Some(session_id.clone()), false),
    )?;
    reject(
        &fixture,
        8,
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
        QueueCounterKind::LockToken,
    )?;
    let fixture = fixture.restart()?;
    reject(
        &fixture,
        8,
        CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: None,
        },
        QueueCounterKind::LockToken,
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(8, receive_kind(ReceiveMode::ReceiveAndDelete, Some(hold))),
        Err(BrokerError::SessionLockNotHeld { session_id })
    );
    assert_eq!(snapshot(&fixture)?, before);
    Ok(())
}

fn session_and_message_locks_share_the_last_two_tokens<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let session_id = SessionId::new("customer")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 1,
            next_lock_token: u64::MAX - 2,
        },
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        1,
        CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("session acquired");
    };
    let hold = accepted.hold();
    assert_eq!(hold.token, LockToken::new(u64::MAX - 2));
    fixture.at(2, send_kind("first", None, Some(session_id.clone()), false))?;
    fixture.at(2, send_kind("second", None, Some(session_id), false))?;
    let CommandOutcome::Received(Some(delivery)) =
        fixture.at(3, receive_kind(ReceiveMode::PeekLock, Some(hold.clone())))?
    else {
        panic!("last message token");
    };
    let lock = delivery.lock.unwrap();
    assert_eq!(lock.token, LockToken::new(u64::MAX - 1));
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    fixture.at(
        4,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: lock.token,
        },
    )?;
    let held_deferred = |mode| CommandKind::ReceiveDeferredHeld {
        sequences: vec![delivery.sequence],
        mode,
        lock_duration_millis: None,
        session: Some(hold.clone()),
        budget: DeliveryBudget {
            max_bytes: 1024 * 1024,
            per_message_overhead_bytes: 64,
        },
    };
    reject(
        &fixture,
        4,
        held_deferred(ReceiveMode::PeekLock),
        QueueCounterKind::LockToken,
    )?;
    reject(
        &fixture,
        4,
        receive_kind(ReceiveMode::PeekLock, Some(hold.clone())),
        QueueCounterKind::LockToken,
    )?;
    assert!(matches!(
        fixture.at(
            4,
            receive_kind(ReceiveMode::ReceiveAndDelete, Some(hold.clone()))
        )?,
        CommandOutcome::Received(Some(_))
    ));
    let CommandOutcome::DeferredReceived(deliveries) =
        fixture.at(4, held_deferred(ReceiveMode::ReceiveAndDelete))?
    else {
        panic!("nonallocating held deferred receive");
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].sequence, delivery.sequence);
    assert!(deliveries[0].lock.is_none());
    fixture.at(5, CommandKind::ReleaseSession { session: hold })?;
    assert_eq!(counters(&fixture)?.next_lock_token, u64::MAX);
    Ok(())
}

fn expired_session_hold_precedes_counter_and_ttl_cleanup<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let session_id = SessionId::new("customer")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: 1,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        1,
        CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: Some(5),
        },
    )?
    else {
        panic!("session acquired");
    };
    fixture.at(
        2,
        send_kind("expired", Some(1), Some(session_id.clone()), false),
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(
            7,
            receive_kind(ReceiveMode::PeekLock, Some(accepted.hold()))
        ),
        Err(BrokerError::SessionLockExpired {
            session_id,
            locked_until: Timestamp::from_millis(6)
        })
    );
    assert_eq!(snapshot(&fixture)?, before);
    assert_eq!(
        fixture.at(7, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    let application = fixture
        .machine
        .apply_with_effects(&fixture.command(7, CommandKind::ExpireMessages))?;
    assert!(application.dead_letters_enqueued);
    assert_eq!(
        application.outcome,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1
        }
    );
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(
            8,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None
            }
        )?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(snapshot(&fixture)?, before);
    Ok(())
}

fn exhausted_counters_do_not_block_cleanup_or_shadow_allocations<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 10,
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let locked = sent(&fixture, 1, "locked", None)?;
    let expired = sent(&fixture, 1, "expired", Some(3))?;
    let delivery = received(&fixture, 2)?;
    assert_eq!(delivery.sequence, locked);
    let CommandOutcome::Scheduled { sequences } =
        fixture.at(2, schedule_kind(&["scheduled"], 30, false))?
    else {
        panic!("scheduled");
    };
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 1,
            next_lock_token: u64::MAX,
        },
    )?;
    fixture.at(
        3,
        CommandKind::RenewLock {
            sequence: locked,
            lock_token: delivery.lock.unwrap().token,
            lock_duration_millis: Some(10),
        },
    )?;
    fixture.at(
        3,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_delivery_count: Some(2),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    assert_eq!(
        fixture.at(5, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1
        }
    );
    assert_eq!(
        fixture.at(13, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0
        }
    );
    assert!(matches!(
        fixture.at(13, receive_kind(ReceiveMode::ReceiveAndDelete, None))?,
        CommandOutcome::Received(Some(_))
    ));
    assert_eq!(
        fixture.at(14, CommandKind::CancelScheduled { sequences })?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(
        fixture.at(20_003, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { expired: 3 }
    );
    let before = snapshot(&fixture)?;
    for kind in [
        CommandKind::ExpireMessages,
        CommandKind::ExpireLocks,
        CommandKind::ExpireSessionLocks,
        CommandKind::ExpireDuplicateHistory,
        CommandKind::ActivateScheduled,
    ] {
        fixture.at(20_004, kind)?;
    }
    assert_eq!(snapshot(&fixture)?, before);
    let mut command = fixture.command(20_004, receive_kind(ReceiveMode::PeekLock, None));
    command.entity = fixture.entity.dead_letter_queue()?;
    let CommandOutcome::Received(Some(dead_letter)) = fixture.machine.apply(&command)? else {
        panic!("shadow uses its own counters");
    };
    assert_eq!(dead_letter.sequence, expired);
    assert_eq!(dead_letter.lock.unwrap().token, LockToken::new(1));
    assert_eq!(
        counters(&fixture)?,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 1,
            next_lock_token: u64::MAX
        }
    );
    command.issued_at = Timestamp::from_millis(20_005);
    command.kind = CommandKind::Complete {
        sequence: expired,
        lock_token: dead_letter.lock.unwrap().token,
    };
    assert_eq!(fixture.machine.apply(&command)?, CommandOutcome::Completed);
    Ok(())
}

fn legacy_unsigned_exhaustion_is_persistent_and_entity_scoped<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: u64::MAX,
            next_lock_token: u64::MAX,
        },
    )?;
    reject(
        &fixture,
        1,
        send_kind("refused", None, None, false),
        QueueCounterKind::Sequence,
    )?;
    let fixture = fixture.restart()?;
    reject(
        &fixture,
        1,
        send_kind("refused", None, None, true),
        QueueCounterKind::Sequence,
    )?;
    let before = snapshot(&fixture)?;
    assert_eq!(
        fixture.at(1, receive_kind(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(snapshot(&fixture)?, before);
    for (namespace, entity, millis) in [("neighbor", "orders", 1), ("tenant", "other", 3)] {
        let mut command = fixture.command(
            millis,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        );
        command.namespace = NamespaceName::new(namespace)?;
        command.entity = EntityPath::new(entity)?;
        assert_eq!(
            fixture.machine.apply(&command)?,
            CommandOutcome::QueueCreated
        );
        command.issued_at = Timestamp::from_millis(millis + 1);
        command.kind = send_kind("first", None, None, false);
        assert_eq!(
            fixture.machine.apply(&command)?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            }
        );
    }
    reject(
        &fixture,
        5,
        send_kind("still-refused", None, None, false),
        QueueCounterKind::Sequence,
    )?;
    assert_eq!(
        counters(&fixture)?,
        QueueCounters {
            next_sequence: u64::MAX,
            next_lock_token: u64::MAX
        }
    );
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
                detail: "injected failure".to_owned(),
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

fn failed_commits_retry_each_final_identifier_exactly_once<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let fixture = QueueFixture::new(
        FailingProvider {
            inner: provider,
            fail_next: fail_next.clone(),
        },
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    seed(
        &fixture,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: u64::MAX - 1,
        },
    )?;
    let before = snapshot(&fixture)?;
    let storage_failure = BrokerError::Storage(StorageError::Backend {
        operation: "commit",
        detail: "injected failure".to_owned(),
    });
    fail_next.store(true, Ordering::Relaxed);
    assert_eq!(
        fixture
            .machine
            .apply_with_effects(&fixture.command(1, send_kind("final", None, None, true))),
        Err(storage_failure.clone())
    );
    assert_eq!(snapshot(&fixture)?, before);
    let fixture = fixture.restart()?;
    assert_eq!(snapshot(&fixture)?, before);
    let application = fixture
        .machine
        .apply_with_effects(&fixture.command(1, send_kind("final", None, None, true)))?;
    assert!(!application.dead_letters_enqueued);
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(MAX_SEQUENCE_NUMBER)
        }
    );
    assert_eq!(
        counters(&fixture)?,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 1,
            next_lock_token: u64::MAX - 1
        }
    );
    reject(
        &fixture,
        2,
        send_kind("final", None, None, true),
        QueueCounterKind::Sequence,
    )?;
    let before = snapshot(&fixture)?;
    fail_next.store(true, Ordering::Relaxed);
    assert_eq!(
        fixture
            .machine
            .apply_with_effects(&fixture.command(2, receive_kind(ReceiveMode::PeekLock, None))),
        Err(storage_failure)
    );
    assert_eq!(snapshot(&fixture)?, before);
    let fixture = fixture.restart()?;
    assert_eq!(snapshot(&fixture)?, before);
    let delivery = received(&fixture, 2)?;
    assert_eq!(delivery.sequence.as_u64(), MAX_SEQUENCE_NUMBER);
    assert_eq!(delivery.lock.unwrap().token, LockToken::new(u64::MAX - 1));
    assert_eq!(
        counters(&fixture)?,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER + 1,
            next_lock_token: u64::MAX
        }
    );
    assert_eq!(
        fixture.at(
            3,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: delivery.lock.unwrap().token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

macro_rules! dual_backend {
    ($name:ident) => {
        mod $name {
            #[test]
            fn memory() -> super::TestResult {
                super::$name(testkit::MemoryProvider::new())
            }
            #[test]
            fn durable() -> super::TestResult {
                super::$name(testkit::DurableProvider::temporary()?)
            }
        }
    };
}

dual_backend!(legacy_send_boundary);
dual_backend!(rich_send_boundary);
dual_backend!(legacy_schedule_boundary);
dual_backend!(rich_schedule_boundary);
dual_backend!(activation_final_boundary);
dual_backend!(activation_batch_rollback);
dual_backend!(duplicate_acknowledgements_still_consume_sequences);
dual_backend!(ordinary_lock_boundary_and_nonallocating_settlement);
dual_backend!(lazy_dead_letter_rollback);
dual_backend!(lazy_drop_rollback);
dual_backend!(deferred_batch_all_variants_roll_back_cleanup_and_partial_locks);
dual_backend!(deferred_final_token_and_receive_delete_at_exhaustion);
dual_backend!(session_final_token_preserves_live_hold_operations);
dual_backend!(session_and_message_locks_share_the_last_two_tokens);
dual_backend!(expired_session_hold_precedes_counter_and_ttl_cleanup);
dual_backend!(exhausted_counters_do_not_block_cleanup_or_shadow_allocations);
dual_backend!(legacy_unsigned_exhaustion_is_persistent_and_entity_scoped);
dual_backend!(failed_commits_retry_each_final_identifier_exactly_once);
