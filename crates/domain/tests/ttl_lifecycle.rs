//! Entity TTL ceilings and expiry suspended by live message locks or deferral.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, MessageBody,
    MessageEnvelope, MessageIdentifier, MessageProperties, MessageState, MessageStatus,
    QueueConfig, ReceiveMode, ScheduledEnvelope, ScheduledMessage, SequenceNumber,
    TIMER_SCAN_LIMIT, Timestamp, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn queue<P: StoreProvider>(
    provider: P,
    ttl: Option<u64>,
) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: ttl,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?)
}

fn envelope(id: &str) -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(id.to_owned())),
            ..MessageProperties::default()
        },
        body: MessageBody::Data(vec![b"pay".to_vec(), b"load".to_vec()]),
        ..MessageEnvelope::default()
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    rich: bool,
) -> Result<SequenceNumber, BrokerError> {
    let kind = if rich {
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: None,
            envelope: Box::new(envelope(id)),
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
    mode: ReceiveMode,
    lock: u64,
    dlq: bool,
) -> Result<Option<Delivery>, Box<dyn Error>> {
    let mut command = fixture.command(
        millis,
        CommandKind::Receive {
            mode,
            lock_duration_millis: Some(lock),
            session: None,
        },
    );
    if dlq {
        command.entity = fixture.entity.dead_letter_queue()?;
    }
    match fixture.machine.apply(&command)? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 32,
            session_id: None,
        },
    )? {
        CommandOutcome::Peeked(deliveries) => Ok(deliveries),
        other => panic!("expected peek outcome, got {other:?}"),
    }
}

fn has_expiry_index<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequence: SequenceNumber,
) -> Result<bool, Box<dyn Error>> {
    let Some(record) = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
    else {
        return Ok(false);
    };
    let Some(expires_at) = record.expires_at else {
        return Ok(false);
    };
    Ok(fixture
        .machine
        .store()
        .get(&keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            expires_at,
            sequence,
        ))?
        .is_some())
}

fn expiry_reason<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequence: SequenceNumber,
) -> Result<DeadLetterReason, Box<dyn Error>> {
    Ok(fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("dead-lettered record")
        .dead_letter
        .expect("reason retained")
        .reason)
}

fn ordinary_and_rich_messages_use_the_entity_ttl_ceiling<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, Some(50))?;
    for rich in [false, true] {
        for (index, (requested, effective)) in [
            (None, 50),
            (Some(25), 25),
            (Some(50), 50),
            (Some(75), 50),
            (Some(0), 0),
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("{rich}-{index}");
            let sequence = send(&fixture, 10, &id, requested, rich)?;
            let record = fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?
                .expect("accepted record");
            assert_eq!(
                record.expires_at,
                Some(Timestamp::from_millis(10 + effective))
            );
            assert_eq!(record.time_to_live_millis(), Some(effective));
            assert!(has_expiry_index(&fixture, sequence)?);
            assert_eq!(record.envelope, rich.then(|| Box::new(envelope(&id))));
        }
    }
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(10, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 2,
            dropped: 0,
            processed: 2,
        }
    );
    let browsed = peek(&fixture, 11)?;
    assert_eq!(browsed.len(), 8);
    assert!(
        browsed
            .iter()
            .all(|delivery| delivery.time_to_live_millis.is_some_and(|ttl| ttl <= 50))
    );
    Ok(())
}

fn unbounded_entities_preserve_explicit_ttl_and_anonymous_infinite_lifetimes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    for rich in [false, true] {
        for (index, requested) in [None, Some(75), Some(0)].into_iter().enumerate() {
            let sequence = send(&fixture, 10, &format!("{rich}-{index}"), requested, rich)?;
            let record = fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?
                .expect("accepted record");
            assert_eq!(record.time_to_live_millis(), requested);
            assert_eq!(
                record.expires_at,
                requested.map(|ttl| Timestamp::from_millis(10 + ttl))
            );
            assert_eq!(has_expiry_index(&fixture, sequence)?, requested.is_some());
        }
    }
    Ok(())
}

fn scheduled_and_rich_pending_messages_expose_the_capped_effective_ttl<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, Some(50))?;
    let requests = [None, Some(25), Some(75), Some(0)];
    let expected = [50, 25, 50, 0];
    for rich in [false, true] {
        let kind = if rich {
            CommandKind::ScheduleEnvelopes {
                messages: requests
                    .iter()
                    .enumerate()
                    .map(|(index, requested)| {
                        let id = format!("rich-{index}");
                        ScheduledEnvelope {
                            message_id: id.clone(),
                            body: b"payload".to_vec(),
                            time_to_live_millis: *requested,
                            session_id: None,
                            enqueue_at: Timestamp::from_millis(100),
                            envelope: envelope(&id),
                        }
                    })
                    .collect(),
            }
        } else {
            CommandKind::Schedule {
                messages: requests
                    .iter()
                    .enumerate()
                    .map(|(index, requested)| ScheduledMessage {
                        message_id: format!("legacy-{index}"),
                        body: b"payload".to_vec(),
                        time_to_live_millis: *requested,
                        session_id: None,
                        enqueue_at: Timestamp::from_millis(100),
                    })
                    .collect(),
            }
        };
        fixture.at(10, kind)?;
    }
    let fixture = fixture.restart()?;
    let pending = peek(&fixture, 20)?;
    assert_eq!(pending.len(), 8);
    for (delivery, ttl) in pending.iter().zip(expected.into_iter().cycle()) {
        assert_eq!(delivery.status, MessageStatus::Scheduled);
        assert_eq!(delivery.expires_at, None);
        assert_eq!(delivery.time_to_live_millis, Some(ttl));
        assert!(!has_expiry_index(&fixture, delivery.sequence)?);
    }
    assert_eq!(
        fixture.at(200, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 8 }
    );
    for (index, ttl) in expected.into_iter().cycle().take(8).enumerate() {
        let sequence = SequenceNumber::new(9 + index as u64);
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .expect("activated record");
        assert_eq!(record.enqueued_at, Timestamp::from_millis(200));
        assert_eq!(record.expires_at, Some(Timestamp::from_millis(200 + ttl)));
        assert_eq!(record.time_to_live_millis(), Some(ttl));
        assert!(has_expiry_index(&fixture, sequence)?);
    }
    assert_eq!(
        fixture.at(200, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 2,
            dropped: 0,
            processed: 2,
        }
    );
    Ok(())
}

fn past_schedules_obey_the_same_ttl_ceiling<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, Some(50))?;
    for rich in [false, true] {
        let kind = if rich {
            CommandKind::ScheduleEnvelopes {
                messages: vec![ScheduledEnvelope {
                    message_id: String::from("rich"),
                    body: Vec::new(),
                    time_to_live_millis: Some(75),
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(5),
                    envelope: envelope("rich"),
                }],
            }
        } else {
            CommandKind::Schedule {
                messages: vec![ScheduledMessage {
                    message_id: String::from("legacy"),
                    body: Vec::new(),
                    time_to_live_millis: Some(75),
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(5),
                }],
            }
        };
        let CommandOutcome::Scheduled { sequences } = fixture.at(10, kind)? else {
            panic!("scheduling result");
        };
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?
            .expect("immediately active record");
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.time_to_live_millis(), Some(50));
        assert_eq!(record.expires_at, Some(Timestamp::from_millis(60)));
    }
    Ok(())
}

fn live_locks_suspend_expiry_and_allow_renewal_then_completion<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "locked", Some(10), true)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    let token = delivery.lock.expect("a lock").token;
    assert!(!has_expiry_index(&fixture, sequence)?);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    let browsed = peek(&fixture, 21)?;
    assert_eq!(browsed.len(), 1);
    assert_eq!(browsed[0].status, MessageStatus::Active);
    assert_eq!(browsed[0].envelope, Some(Box::new(envelope("locked"))));
    assert_eq!(
        fixture.at(
            30,
            CommandKind::RenewLock {
                sequence,
                lock_token: token,
                lock_duration_millis: Some(200)
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(230)
        }
    );
    assert_eq!(
        fixture.at(
            31,
            CommandKind::Complete {
                sequence,
                lock_token: token
            }
        )?,
        CommandOutcome::Completed
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
            .is_none()
    );
    Ok(())
}

fn abandoning_a_live_lock_after_ttl_immediately_dead_letters<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "abandon-expired", Some(10), true)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Abandon {
                sequence,
                lock_token: delivery.lock.expect("a lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, sequence)?,
        DeadLetterReason::TimeToLiveExpired
    );
    let delivery = receive(&fixture, 22, ReceiveMode::ReceiveAndDelete, 100, true)?
        .expect("expired message drained");
    assert_eq!(
        delivery.envelope,
        Some(Box::new(envelope("abandon-expired")))
    );
    assert_eq!(delivery.time_to_live_millis, None);
    Ok(())
}

fn lock_expiry_after_ttl_dead_letters_without_a_second_message_sweep<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "lock-expired", Some(10), false)?;
    receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(111, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, sequence)?,
        DeadLetterReason::TimeToLiveExpired
    );
    assert!(!has_expiry_index(&fixture, sequence)?);
    Ok(())
}

fn abandoning_before_ttl_restores_the_expiry_index<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "abandon-live", Some(10), false)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    assert!(!has_expiry_index(&fixture, sequence)?);
    fixture.at(
        12,
        CommandKind::Abandon {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    assert!(has_expiry_index(&fixture, sequence)?);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, sequence)?,
        DeadLetterReason::TimeToLiveExpired
    );
    Ok(())
}

fn lock_expiry_before_ttl_restores_the_expiry_index<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "lock-live", Some(100), false)?;
    receive(&fixture, 11, ReceiveMode::PeekLock, 10, false)?.expect("message locked");
    assert!(!has_expiry_index(&fixture, sequence)?);
    assert_eq!(
        fixture.at(21, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    assert!(has_expiry_index(&fixture, sequence)?);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, sequence)?,
        DeadLetterReason::TimeToLiveExpired
    );
    Ok(())
}

fn deferred_expiry_is_lazy_and_peekable_until_explicit_receive<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "deferred", Some(10), true)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    fixture.at(
        12,
        CommandKind::Defer {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    assert!(!has_expiry_index(&fixture, sequence)?);
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(21, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    let browsed = peek(&fixture, 21)?;
    assert_eq!(browsed.len(), 1);
    assert_eq!(browsed[0].status, MessageStatus::Deferred);
    assert_eq!(browsed[0].envelope, Some(Box::new(envelope("deferred"))));
    assert_eq!(
        fixture.at(
            22,
            CommandKind::ReceiveDeferred {
                sequences: vec![sequence],
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session_id: None
            }
        )?,
        CommandOutcome::DeferredReceived(Vec::new())
    );
    assert_eq!(
        expiry_reason(&fixture, sequence)?,
        DeadLetterReason::TimeToLiveExpired
    );
    Ok(())
}

fn a_live_expired_lock_can_be_successfully_deferred<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "defer-after-ttl", Some(10), false)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    fixture.at(
        21,
        CommandKind::Defer {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    assert_eq!(
        fixture.at(22, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    assert_eq!(peek(&fixture, 23)?[0].status, MessageStatus::Deferred);
    assert!(!has_expiry_index(&fixture, sequence)?);
    Ok(())
}

fn deferred_receive_locks_also_suspend_expiry<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let sequence = send(&fixture, 10, "deferred-lock", Some(100), false)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    fixture.at(
        12,
        CommandKind::Defer {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        13,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session_id: None,
        },
    )?
    else {
        panic!("deferred receive outcome");
    };
    assert!(!has_expiry_index(&fixture, sequence)?);
    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    assert_eq!(
        fixture.at(
            112,
            CommandKind::Complete {
                sequence,
                lock_token: deliveries[0].lock.expect("a lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn legacy_protected_expiry_entries_are_repaired_without_reaping_live_locks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let protected = send(&fixture, 10, "protected", Some(10), false)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    let expired = send(&fixture, 12, "ready-expired", Some(10), false)?;
    let mut legacy = WriteBatch::default();
    let stale_key = keys::expiry(
        &fixture.namespace,
        &fixture.entity,
        Timestamp::from_millis(20),
        protected,
    );
    legacy.push_put(stale_key.clone(), Vec::new());
    fixture.machine.store().apply(legacy)?;
    assert_eq!(
        fixture.at(22, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 2,
        }
    );
    assert_eq!(fixture.machine.store().get(&stale_key)?, None);
    assert_eq!(
        expiry_reason(&fixture, expired)?,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(
        fixture.at(
            23,
            CommandKind::Complete {
                sequence: protected,
                lock_token: delivery.lock.expect("a lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn protected_locks_do_not_pin_the_bounded_expiry_sweep<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    for index in 0..=TIMER_SCAN_LIMIT {
        send(&fixture, 10, &format!("protected-{index}"), Some(10), false)?;
    }
    for _ in 0..=TIMER_SCAN_LIMIT {
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    }
    let expired = send(&fixture, 12, "ready-expired", Some(10), false)?;
    assert_eq!(
        fixture.at(22, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, expired)?,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::lock_prefix(&fixture.namespace, &fixture.entity),
                usize::MAX
            )?
            .len(),
        TIMER_SCAN_LIMIT + 1
    );
    Ok(())
}

fn expired_ttl_takes_precedence_over_delivery_limit_on_release<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let abandoned = send(&fixture, 10, "abandon-limit", Some(10), true)?;
    let lock_expired = send(&fixture, 10, "lock-limit", Some(10), false)?;
    let first = receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("first delivery");
    let second =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("second delivery");
    assert_eq!(first.delivery_count, 1);
    assert_eq!(second.delivery_count, 1);
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Abandon {
                sequence: abandoned,
                lock_token: first.lock.expect("a lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, abandoned)?,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(
        fixture.at(111, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        expiry_reason(&fixture, lock_expired)?,
        DeadLetterReason::TimeToLiveExpired
    );
    Ok(())
}

fn legacy_deferred_expiry_entries_are_repaired_without_eager_expiration<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    let deferred = send(&fixture, 10, "deferred-legacy", Some(10), false)?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::PeekLock, 100, false)?.expect("message locked");
    fixture.at(
        12,
        CommandKind::Defer {
            sequence: deferred,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    let ready = send(&fixture, 13, "ready-expired", Some(10), false)?;
    let stale_key = keys::expiry(
        &fixture.namespace,
        &fixture.entity,
        Timestamp::from_millis(20),
        deferred,
    );
    let mut legacy = WriteBatch::default();
    legacy.push_put(stale_key.clone(), Vec::new());
    fixture.machine.store().apply(legacy)?;
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(23, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 2,
        }
    );
    assert_eq!(fixture.machine.store().get(&stale_key)?, None);
    assert_eq!(
        expiry_reason(&fixture, ready)?,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(peek(&fixture, 24)?[0].status, MessageStatus::Deferred);
    assert_eq!(
        fixture.at(
            25,
            CommandKind::ReceiveDeferred {
                sequences: vec![deferred],
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session_id: None
            }
        )?,
        CommandOutcome::DeferredReceived(Vec::new())
    );
    assert_eq!(
        expiry_reason(&fixture, deferred)?,
        DeadLetterReason::TimeToLiveExpired
    );
    Ok(())
}

fn a_mismatched_expiry_index_rejects_the_sweep_atomically<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider, None)?;
    send(&fixture, 10, "valid-expired", Some(10), false)?;
    let second = send(&fixture, 10, "mismatched", Some(11), false)?;
    let mut invalid = WriteBatch::default();
    invalid.push_put(
        keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(20),
            second,
        ),
        Vec::new(),
    );
    fixture.machine.store().apply(invalid)?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        fixture.at(20, CommandKind::ExpireMessages),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
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
    ordinary_and_rich_messages_use_the_entity_ttl_ceiling,
    unbounded_entities_preserve_explicit_ttl_and_anonymous_infinite_lifetimes,
    scheduled_and_rich_pending_messages_expose_the_capped_effective_ttl,
    past_schedules_obey_the_same_ttl_ceiling,
    live_locks_suspend_expiry_and_allow_renewal_then_completion,
    abandoning_a_live_lock_after_ttl_immediately_dead_letters,
    lock_expiry_after_ttl_dead_letters_without_a_second_message_sweep,
    abandoning_before_ttl_restores_the_expiry_index,
    lock_expiry_before_ttl_restores_the_expiry_index,
    deferred_expiry_is_lazy_and_peekable_until_explicit_receive,
    a_live_expired_lock_can_be_successfully_deferred,
    deferred_receive_locks_also_suspend_expiry,
    legacy_protected_expiry_entries_are_repaired_without_reaping_live_locks,
    protected_locks_do_not_pin_the_bounded_expiry_sweep,
    expired_ttl_takes_precedence_over_delivery_limit_on_release,
    legacy_deferred_expiry_entries_are_repaired_without_eager_expiration,
    a_mismatched_expiry_index_rejects_the_sweep_atomically,
}
