//! Deferred-message lifecycle over both storage backends.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, DeliveryOrigin,
    MAX_DEFERRED_RECEIVE_BATCH, MessageEnvelope, MessageState, QueueConfig, ReceiveMode,
    SequenceNumber, SessionHold, SessionId,
};
use testkit::{QueueFixture, StoreProvider};

const LOCK_MILLIS: u64 = 1_000;

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            ..QueueConfig::default()
        },
    )?)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    id: &str,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: id.as_bytes().to_vec(),
            time_to_live_millis: ttl,
            session_id,
            envelope: Some(MessageEnvelope::new(format!("envelope:{id}").into_bytes())),
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected send, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    mode: ReceiveMode,
    session: Option<SessionHold>,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive, got {other:?}"),
    }
}

fn defer<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    delivery: &Delivery,
    replacement_envelope: Option<MessageEnvelope>,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(
        at,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("peek-lock delivery").token,
            replacement_envelope,
        },
    )
}

fn receive_deferred<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    session: Option<SessionHold>,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        at,
        CommandKind::ReceiveDeferred {
            sequences,
            mode,
            lock_duration_millis: None,
            session,
        },
    )? {
        CommandOutcome::DeferredReceived(deliveries) => Ok(deliveries),
        other => panic!("expected deferred receive, got {other:?}"),
    }
}

fn complete<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    delivery: &Delivery,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(
        at,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("peek-lock delivery").token,
        },
    )
}

fn accepting_session<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    session_id: SessionId,
) -> Result<SessionHold, BrokerError> {
    match fixture.at(
        at,
        CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: Some(10_000),
        },
    )? {
        CommandOutcome::SessionAccepted(Some(accepted)) => Ok(accepted.hold()),
        other => panic!("expected accepted session, got {other:?}"),
    }
}

fn deferral_hides_a_message_and_preserves_an_updated_envelope<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    send(&fixture, 11, "sentinel", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first is ready");
    let updated = MessageEnvelope::new(b"updated-envelope".to_vec());
    assert_eq!(
        defer(&fixture, 30, &first, Some(updated.clone()))?,
        CommandOutcome::Deferred
    );

    assert_eq!(
        fixture
            .machine
            .deferred_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![sequence]
    );
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("deferred message remains durable");
    assert_eq!(record.state, MessageState::Deferred);
    assert_eq!(record.envelope, Some(updated.clone()));

    let ordinary = receive(&fixture, 31, ReceiveMode::PeekLock, None)?.expect("sentinel remains");
    assert_eq!(ordinary.message_id, "sentinel");
    assert_eq!(receive(&fixture, 32, ReceiveMode::PeekLock, None)?, None);

    let deferred =
        receive_deferred(&fixture, 40, vec![sequence], ReceiveMode::PeekLock, None)?.remove(0);
    assert_eq!(deferred.origin, DeliveryOrigin::Deferred);
    assert_eq!(deferred.envelope, Some(updated));
    assert_eq!(
        complete(&fixture, 41, &deferred)?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn receive_and_delete_consumes_a_deferred_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first is ready");
    defer(&fixture, 30, &first, None)?;

    let delivery = receive_deferred(
        &fixture,
        40,
        vec![sequence],
        ReceiveMode::ReceiveAndDelete,
        None,
    )?
    .remove(0);
    assert_eq!(delivery.origin, DeliveryOrigin::Deferred);
    assert_eq!(delivery.lock, None);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?,
        None
    );
    Ok(())
}

fn a_deferred_batch_is_atomic_and_keeps_caller_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let first_sequence = send(&fixture, 10, "first", None, None)?;
    let second_sequence = send(&fixture, 11, "second", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first");
    defer(&fixture, 21, &first, None)?;
    let second = receive(&fixture, 22, ReceiveMode::PeekLock, None)?.expect("second");
    defer(&fixture, 23, &second, None)?;

    let deliveries = receive_deferred(
        &fixture,
        30,
        vec![second_sequence, first_sequence],
        ReceiveMode::PeekLock,
        None,
    )?;
    assert_eq!(
        deliveries
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![second_sequence, first_sequence]
    );
    assert_ne!(
        deliveries[0].lock.expect("locked").token,
        deliveries[1].lock.expect("locked").token
    );
    Ok(())
}

fn one_invalid_sequence_rejects_the_whole_deferred_batch<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let first_sequence = send(&fixture, 10, "first", None, None)?;
    let second_sequence = send(&fixture, 11, "second", None, None)?;
    let ordinary_sequence = send(&fixture, 12, "ordinary", None, None)?;
    for at in [20, 22] {
        let delivery = receive(&fixture, at, ReceiveMode::PeekLock, None)?.expect("ready");
        defer(&fixture, at + 1, &delivery, None)?;
    }

    assert_eq!(
        fixture.at(
            30,
            CommandKind::ReceiveDeferred {
                sequences: vec![first_sequence, ordinary_sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ),
        Err(BrokerError::MessageNotDeferred {
            sequence: ordinary_sequence
        })
    );
    assert_eq!(
        fixture
            .machine
            .deferred_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![first_sequence, second_sequence]
    );
    let recovered = receive_deferred(
        &fixture,
        31,
        vec![first_sequence, second_sequence],
        ReceiveMode::PeekLock,
        None,
    )?;
    assert_eq!(recovered.len(), 2);
    Ok(())
}

fn deferred_receive_bounds_and_duplicate_validation_write_nothing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first");
    defer(&fixture, 21, &first, None)?;

    assert_eq!(
        fixture.at(
            30,
            CommandKind::ReceiveDeferred {
                sequences: Vec::new(),
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ),
        Err(BrokerError::EmptyDeferredReceive)
    );
    assert_eq!(
        fixture.at(
            31,
            CommandKind::ReceiveDeferred {
                sequences: vec![sequence, sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ),
        Err(BrokerError::DuplicateDeferredSequence { sequence })
    );
    let too_many = vec![SequenceNumber::new(999); MAX_DEFERRED_RECEIVE_BATCH + 1];
    assert_eq!(
        fixture.at(
            32,
            CommandKind::ReceiveDeferred {
                sequences: too_many,
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ),
        Err(BrokerError::DeferredReceiveBatchTooLarge {
            count: MAX_DEFERRED_RECEIVE_BATCH + 1,
            maximum: MAX_DEFERRED_RECEIVE_BATCH,
        })
    );
    assert_eq!(
        fixture
            .machine
            .deferred_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![sequence]
    );
    Ok(())
}

fn abandon_and_lock_expiry_return_to_the_deferred_set<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first");
    defer(&fixture, 21, &first, None)?;

    let deferred =
        receive_deferred(&fixture, 30, vec![sequence], ReceiveMode::PeekLock, None)?.remove(0);
    assert_eq!(
        fixture.at(
            31,
            CommandKind::Abandon {
                sequence,
                lock_token: deferred.lock.expect("locked").token,
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    assert_eq!(receive(&fixture, 32, ReceiveMode::PeekLock, None)?, None);

    let again =
        receive_deferred(&fixture, 40, vec![sequence], ReceiveMode::PeekLock, None)?.remove(0);
    let locked_until = again.lock.expect("locked").locked_until;
    assert_eq!(
        fixture.at(locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
        }
    );
    assert_eq!(
        receive(
            &fixture,
            locked_until.as_millis() + 1,
            ReceiveMode::PeekLock,
            None
        )?,
        None
    );
    assert_eq!(
        fixture
            .machine
            .deferred_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![sequence]
    );
    Ok(())
}

fn deferred_state_survives_a_restart<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("first");
    defer(&fixture, 21, &first, None)?;

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .deferred_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![sequence]
    );
    let delivery =
        receive_deferred(&fixture, 30, vec![sequence], ReceiveMode::PeekLock, None)?.remove(0);
    assert_eq!(delivery.origin, DeliveryOrigin::Deferred);
    Ok(())
}

fn session_holds_isolate_deferred_messages<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            lock_duration_millis: LOCK_MILLIS,
            ..QueueConfig::default()
        },
    )?;
    let a_id = SessionId::new("a")?;
    let b_id = SessionId::new("b")?;
    let a_sequence = send(&fixture, 10, "a-message", None, Some(a_id.clone()))?;
    let b_sequence = send(&fixture, 11, "b-message", None, Some(b_id.clone()))?;
    let a_hold = accepting_session(&fixture, 20, a_id)?;
    let b_hold = accepting_session(&fixture, 21, b_id)?;
    let a = receive(&fixture, 22, ReceiveMode::PeekLock, Some(a_hold.clone()))?.expect("a");
    defer(&fixture, 23, &a, None)?;
    let b = receive(&fixture, 24, ReceiveMode::PeekLock, Some(b_hold.clone()))?.expect("b");
    defer(&fixture, 25, &b, None)?;

    assert_eq!(
        fixture.at(
            30,
            CommandKind::ReceiveDeferred {
                sequences: vec![a_sequence, b_sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: Some(a_hold.clone()),
            }
        ),
        Err(BrokerError::DeferredMessageSessionMismatch {
            sequence: b_sequence
        })
    );
    assert_eq!(
        receive_deferred(
            &fixture,
            31,
            vec![a_sequence],
            ReceiveMode::PeekLock,
            Some(a_hold),
        )?[0]
            .session_id,
        Some(SessionId::new("a")?)
    );
    assert_eq!(
        receive_deferred(
            &fixture,
            32,
            vec![b_sequence],
            ReceiveMode::PeekLock,
            Some(b_hold),
        )?[0]
            .session_id,
        Some(SessionId::new("b")?)
    );
    Ok(())
}

fn ttl_does_not_invalidate_a_live_lock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "perishable", Some(100), None)?;
    let delivery = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready");

    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    assert_eq!(
        complete(&fixture, 111, &delivery)?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn ttl_applies_when_an_expired_lock_is_abandoned<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "perishable", Some(100), None)?;
    let delivery = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready");
    assert_eq!(
        fixture.at(
            111,
            CommandKind::Abandon {
                sequence,
                lock_token: delivery.lock.expect("locked").token,
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    let dead = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("expired lock moves to the DLQ");
    assert_eq!(
        dead.dead_letter_info().map(|info| &info.reason),
        Some(&DeadLetterReason::TimeToLiveExpired)
    );
    Ok(())
}

fn deferred_ttl_is_checked_only_on_explicit_retrieval<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "perishable", Some(100), None)?;
    let delivery = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready");
    defer(&fixture, 30, &delivery, None)?;

    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?,
        None
    );
    assert!(
        receive_deferred(&fixture, 111, vec![sequence], ReceiveMode::PeekLock, None,)?.is_empty()
    );
    assert!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
            .is_some()
    );
    Ok(())
}

fn an_oversized_replacement_rejects_deferral_without_losing_the_lock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 16,
            ..QueueConfig::default()
        },
    )?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let delivery = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready");
    assert_eq!(
        defer(
            &fixture,
            30,
            &delivery,
            Some(MessageEnvelope::new(vec![0; 17])),
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 17,
            maximum_bytes: 16,
        })
    );
    assert_eq!(
        complete(&fixture, 31, &delivery)?,
        CommandOutcome::Completed
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?,
        None
    );
    Ok(())
}

fn abandoning_a_modified_envelope_preserves_ready_and_deferred_origin<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    let ready_sequence = send(&fixture, 10, "ready", None, None)?;
    let ready = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready message");
    let ready_envelope = MessageEnvelope::new(b"ready-modified".to_vec());
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Abandon {
                sequence: ready_sequence,
                lock_token: ready.lock.expect("locked").token,
                replacement_envelope: Some(ready_envelope.clone()),
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    let ready_again =
        receive(&fixture, 22, ReceiveMode::PeekLock, None)?.expect("ready redelivery");
    assert_eq!(ready_again.origin, DeliveryOrigin::Ready);
    assert_eq!(ready_again.envelope, Some(ready_envelope));
    complete(&fixture, 23, &ready_again)?;

    let deferred_sequence = send(&fixture, 30, "deferred", None, None)?;
    let ordinary = receive(&fixture, 31, ReceiveMode::PeekLock, None)?.expect("ordinary delivery");
    defer(&fixture, 32, &ordinary, None)?;
    let deferred = receive_deferred(
        &fixture,
        33,
        vec![deferred_sequence],
        ReceiveMode::PeekLock,
        None,
    )?
    .remove(0);
    let deferred_envelope = MessageEnvelope::new(b"deferred-modified".to_vec());
    assert_eq!(
        fixture.at(
            34,
            CommandKind::Abandon {
                sequence: deferred_sequence,
                lock_token: deferred.lock.expect("locked").token,
                replacement_envelope: Some(deferred_envelope.clone()),
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    assert_eq!(receive(&fixture, 35, ReceiveMode::PeekLock, None)?, None);
    let deferred_again = receive_deferred(
        &fixture,
        36,
        vec![deferred_sequence],
        ReceiveMode::PeekLock,
        None,
    )?
    .remove(0);
    assert_eq!(deferred_again.origin, DeliveryOrigin::Deferred);
    assert_eq!(deferred_again.envelope, Some(deferred_envelope));
    complete(&fixture, 37, &deferred_again)?;
    Ok(())
}

fn modified_envelopes_reach_the_dead_letter_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    let expired_sequence = send(&fixture, 10, "expired", Some(10), None)?;
    let expired = receive(&fixture, 11, ReceiveMode::PeekLock, None)?.expect("expired delivery");
    let abandoned_envelope = MessageEnvelope::new(b"abandoned-to-dlq".to_vec());
    assert_eq!(
        fixture.at(
            20,
            CommandKind::Abandon {
                sequence: expired_sequence,
                lock_token: expired.lock.expect("locked").token,
                replacement_envelope: Some(abandoned_envelope.clone()),
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, expired_sequence)?
            .expect("abandoned message reaches DLQ")
            .envelope,
        Some(abandoned_envelope)
    );

    let explicit_sequence = send(&fixture, 30, "explicit", None, None)?;
    let explicit = receive(&fixture, 31, ReceiveMode::PeekLock, None)?.expect("explicit delivery");
    let dead_letter_envelope = MessageEnvelope::new(b"explicit-to-dlq".to_vec());
    assert_eq!(
        fixture.at(
            32,
            CommandKind::DeadLetter {
                sequence: explicit_sequence,
                lock_token: explicit.lock.expect("locked").token,
                reason: String::from("modified"),
                description: String::from("replacement envelope"),
                replacement_envelope: Some(dead_letter_envelope.clone()),
            }
        )?,
        CommandOutcome::DeadLettered
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, explicit_sequence)?
            .expect("explicit dead letter reaches DLQ")
            .envelope,
        Some(dead_letter_envelope)
    );
    Ok(())
}

fn oversized_abandon_and_dead_letter_retain_the_live_lock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 16,
            ..QueueConfig::default()
        },
    )?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let delivery = receive(&fixture, 20, ReceiveMode::PeekLock, None)?.expect("ready");
    let lock = delivery.lock.expect("locked");
    let oversized = MessageEnvelope::new(vec![0; 17]);
    let expected = Err(BrokerError::MessageTooLarge {
        body_bytes: 17,
        maximum_bytes: 16,
    });

    assert_eq!(
        fixture.at(
            30,
            CommandKind::Abandon {
                sequence,
                lock_token: lock.token,
                replacement_envelope: Some(oversized.clone()),
            }
        ),
        expected
    );
    assert_eq!(
        fixture.at(
            31,
            CommandKind::DeadLetter {
                sequence,
                lock_token: lock.token,
                reason: String::from("oversized"),
                description: String::from("must be rejected atomically"),
                replacement_envelope: Some(oversized),
            }
        ),
        expected
    );
    assert_eq!(
        fixture.at(
            32,
            CommandKind::Complete {
                sequence,
                lock_token: lock.token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn delivery_locks_report_the_effective_receive_duration<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    const ORDINARY_MILLIS: u64 = 123;
    const DEFERRED_MILLIS: u64 = 456;

    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first", None, None)?;
    let ordinary = match fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(ORDINARY_MILLIS),
            session: None,
        },
    )? {
        CommandOutcome::Received(Some(delivery)) => delivery,
        other => panic!("expected ordinary delivery, got {other:?}"),
    };
    let ordinary_lock = ordinary.lock.expect("locked");
    assert_eq!(ordinary_lock.lock_duration_millis, ORDINARY_MILLIS);
    assert_eq!(ordinary_lock.locked_until.as_millis(), 20 + ORDINARY_MILLIS);
    defer(&fixture, 21, &ordinary, None)?;

    let deferred = match fixture.at(
        30,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(DEFERRED_MILLIS),
            session: None,
        },
    )? {
        CommandOutcome::DeferredReceived(mut deliveries) => deliveries.remove(0),
        other => panic!("expected deferred delivery, got {other:?}"),
    };
    let deferred_lock = deferred.lock.expect("locked");
    assert_eq!(deferred_lock.lock_duration_millis, DEFERRED_MILLIS);
    assert_eq!(deferred_lock.locked_until.as_millis(), 30 + DEFERRED_MILLIS);
    complete(&fixture, 31, &deferred)?;
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
    deferral_hides_a_message_and_preserves_an_updated_envelope,
    receive_and_delete_consumes_a_deferred_message,
    a_deferred_batch_is_atomic_and_keeps_caller_order,
    one_invalid_sequence_rejects_the_whole_deferred_batch,
    deferred_receive_bounds_and_duplicate_validation_write_nothing,
    abandon_and_lock_expiry_return_to_the_deferred_set,
    deferred_state_survives_a_restart,
    session_holds_isolate_deferred_messages,
    ttl_does_not_invalidate_a_live_lock,
    ttl_applies_when_an_expired_lock_is_abandoned,
    deferred_ttl_is_checked_only_on_explicit_retrieval,
    an_oversized_replacement_rejects_deferral_without_losing_the_lock,
    abandoning_a_modified_envelope_preserves_ready_and_deferred_origin,
    modified_envelopes_reach_the_dead_letter_queue,
    oversized_abandon_and_dead_letter_retain_the_live_lock,
    delivery_locks_report_the_effective_receive_duration,
}
