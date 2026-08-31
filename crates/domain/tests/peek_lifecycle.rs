//! Read-only message browsing over both storage backends.

use std::error::Error;

use domain::{
    AcceptedSession, BrokerError, Command, CommandKind, CommandOutcome, Delivery, DeliveryOrigin,
    LockToken, MAX_PEEK_BATCH, MAX_PEEK_SCAN, MessageEnvelope, MessageInput, QueueConfig,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, Timestamp,
};
use storage::StateStore;
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

fn session_queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            requires_session: true,
            ..QueueConfig::default()
        },
    )?)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    message_id: &str,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Send {
            message_id: message_id.to_owned(),
            body: message_id.as_bytes().to_vec(),
            time_to_live_millis: ttl,
            session_id,
            envelope: Some(MessageEnvelope::new(
                format!("envelope:{message_id}").into_bytes(),
            )),
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected send, got {other:?}"),
    }
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    from_sequence: SequenceNumber,
    max_messages: u32,
    session: Option<SessionHold>,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Peek {
            from_sequence,
            max_messages,
            session,
        },
    )? {
        CommandOutcome::Peeked(deliveries) => Ok(deliveries),
        other => panic!("expected peek, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    session: Option<SessionHold>,
) -> Result<Delivery, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session,
        },
    )? {
        CommandOutcome::Received(Some(delivery)) => Ok(delivery),
        other => panic!("expected delivery, got {other:?}"),
    }
}

fn defer<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    delivery: &Delivery,
) -> Result<(), BrokerError> {
    let outcome = fixture.at(
        at,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("locked delivery").token,
            replacement_envelope: None,
        },
    )?;
    assert_eq!(outcome, CommandOutcome::Deferred);
    Ok(())
}

fn receive_deferred<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    sequence: SequenceNumber,
) -> Result<Delivery, BrokerError> {
    match fixture.at(
        at,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::DeferredReceived(mut deliveries) => Ok(deliveries.remove(0)),
        other => panic!("expected deferred delivery, got {other:?}"),
    }
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    session_id: SessionId,
) -> Result<AcceptedSession, BrokerError> {
    match fixture.at(
        at,
        CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: None,
        },
    )? {
        CommandOutcome::SessionAccepted(Some(accepted)) => Ok(accepted),
        other => panic!("expected accepted session, got {other:?}"),
    }
}

fn input(index: usize, ttl: Option<u64>) -> MessageInput {
    MessageInput {
        message_id: format!("message-{index}"),
        body: Vec::new(),
        time_to_live_millis: ttl,
        session_id: None,
        envelope: None,
    }
}

fn peek_is_inclusive_ordered_and_pages_across_sequence_gaps<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let first = send(&fixture, 10, "first", None, None)?;
    let second = send(&fixture, 11, "second", None, None)?;
    let third = send(&fixture, 12, "third", None, None)?;

    assert_eq!(
        peek(&fixture, 20, SequenceNumber::new(0), 2, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(
        peek(&fixture, 21, second, 2, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![second, third]
    );

    for at in [22, 23] {
        assert!(matches!(
            fixture.at(
                at,
                CommandKind::Receive {
                    mode: ReceiveMode::ReceiveAndDelete,
                    lock_duration_millis: None,
                    session: None,
                }
            )?,
            CommandOutcome::Received(Some(_))
        ));
    }
    assert_eq!(
        peek(&fixture, 24, second, 2, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![third]
    );
    assert!(peek(&fixture, 25, SequenceNumber::new(4), 2, None)?.is_empty());
    Ok(())
}

fn peek_changes_no_record_counter_or_applied_clock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first", None, None)?;
    let locked = receive(&fixture, 20, None)?;
    let before = fixture.machine.store().snapshot()?;
    let before_clock = fixture.machine.last_applied_time()?;

    let result = peek(&fixture, 100, SequenceNumber::new(0), 1, None)?;
    assert_eq!(result[0].lock, None);
    assert_eq!(result[0].delivery_count, locked.delivery_count);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, before_clock);

    // The peek timestamp was not committed, and the original live lock is
    // untouched and remains settleable by a command stamped earlier than it.
    assert_eq!(
        fixture.at(
            21,
            CommandKind::Complete {
                sequence: locked.sequence,
                lock_token: locked.lock.expect("locked").token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn peek_exposes_ready_locked_and_deferred_origins_without_locks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    let locked_ready_sequence = send(&fixture, 10, "locked-ready", None, None)?;
    receive(&fixture, 11, None)?;

    let deferred_sequence = send(&fixture, 12, "deferred", None, None)?;
    let deferred = receive(&fixture, 13, None)?;
    defer(&fixture, 14, &deferred)?;

    let locked_deferred_sequence = send(&fixture, 15, "locked-deferred", None, None)?;
    let locked_deferred = receive(&fixture, 16, None)?;
    defer(&fixture, 17, &locked_deferred)?;
    receive_deferred(&fixture, 18, locked_deferred_sequence)?;

    let ready_sequence = send(&fixture, 19, "ready", None, None)?;
    let deliveries = peek(&fixture, 20, SequenceNumber::new(0), 10, None)?;
    assert_eq!(
        deliveries
            .iter()
            .map(|delivery| (
                delivery.sequence,
                delivery.origin,
                delivery.delivery_count,
                delivery.lock,
            ))
            .collect::<Vec<_>>(),
        vec![
            (locked_ready_sequence, DeliveryOrigin::Ready, 1, None),
            (deferred_sequence, DeliveryOrigin::Deferred, 1, None),
            (locked_deferred_sequence, DeliveryOrigin::Deferred, 2, None,),
            (ready_sequence, DeliveryOrigin::Ready, 0, None),
        ]
    );
    assert_eq!(
        deliveries[3]
            .envelope
            .as_ref()
            .map(MessageEnvelope::as_bytes),
        Some(b"envelope:ready".as_slice())
    );
    Ok(())
}

fn peek_skips_expired_ready_but_keeps_locked_and_deferred_records<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let locked_sequence = send(&fixture, 10, "locked", Some(10), None)?;
    receive(&fixture, 11, None)?;
    let deferred_sequence = send(&fixture, 12, "deferred", Some(10), None)?;
    let deferred = receive(&fixture, 13, None)?;
    defer(&fixture, 14, &deferred)?;
    let expired_ready_sequence = send(&fixture, 15, "expired-ready", Some(5), None)?;

    assert_eq!(
        peek(&fixture, 25, SequenceNumber::new(0), 10, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![locked_sequence, deferred_sequence]
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, expired_ready_sequence)?
            .is_some(),
        "peek must not reap the skipped expired record"
    );
    Ok(())
}

fn regular_peek_crosses_sessions_while_a_held_session_filters<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    let a_id = SessionId::new("a")?;
    let b_id = SessionId::new("b")?;
    let a_first = send(&fixture, 10, "a-first", None, Some(a_id.clone()))?;
    let b_first = send(&fixture, 11, "b-first", None, Some(b_id.clone()))?;
    let a_second = send(&fixture, 12, "a-second", None, Some(a_id.clone()))?;
    let a = accept(&fixture, 20, a_id.clone())?;
    let b = accept(&fixture, 21, b_id.clone())?;

    assert_eq!(
        peek(&fixture, 22, SequenceNumber::new(0), 10, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![a_first, b_first, a_second]
    );
    assert_eq!(
        peek(&fixture, 23, SequenceNumber::new(0), 10, Some(a.hold()),)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![a_first, a_second]
    );
    assert_eq!(
        peek(&fixture, 24, SequenceNumber::new(0), 10, Some(b.hold()),)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![b_first]
    );
    assert_eq!(
        fixture.at(
            25,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 10,
                session: Some(SessionHold::new(a_id.clone(), LockToken::new(999))),
            }
        ),
        Err(BrokerError::SessionLockNotHeld {
            session_id: a_id.clone()
        })
    );
    assert_eq!(
        fixture.at(
            a.lock.locked_until.as_millis(),
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 10,
                session: Some(a.hold()),
            }
        ),
        Err(BrokerError::SessionLockExpired {
            session_id: a_id,
            locked_until: a.lock.locked_until,
        })
    );
    Ok(())
}

fn session_peek_continues_past_other_session_scan_chunks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    let other_id = SessionId::new("other")?;
    let target_id = SessionId::new("target")?;
    let messages = (0..MAX_PEEK_SCAN)
        .map(|index| {
            let mut message = input(index, None);
            message.session_id = Some(other_id.clone());
            message
        })
        .collect();
    fixture.at(10, CommandKind::SendBatch { messages })?;
    let target = send(&fixture, 11, "target", None, Some(target_id.clone()))?;
    let target_session = accept(&fixture, 12, target_id)?;

    assert_eq!(
        peek(
            &fixture,
            13,
            SequenceNumber::new(0),
            1,
            Some(target_session.hold()),
        )?[0]
            .sequence,
        target
    );
    Ok(())
}

fn a_plain_queue_rejects_a_session_filtered_peek<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let session_id = SessionId::new("not-supported")?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 1,
                session: Some(SessionHold::new(session_id, LockToken::new(1))),
            }
        ),
        Err(BrokerError::SessionNotSupported)
    );
    Ok(())
}

fn a_dead_letter_queue_is_browsed_in_its_own_scope<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let dead_sequence = send(&fixture, 10, "dead", None, None)?;
    let dead = receive(&fixture, 11, None)?;
    fixture.at(
        12,
        CommandKind::DeadLetter {
            sequence: dead_sequence,
            lock_token: dead.lock.expect("locked").token,
            reason: String::from("Invalid"),
            description: String::from("invalid payload"),
            replacement_envelope: None,
        },
    )?;
    let live_sequence = send(&fixture, 13, "live", None, None)?;

    assert_eq!(
        peek(&fixture, 14, SequenceNumber::new(0), 10, None)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![live_sequence]
    );

    let dlq = fixture.entity.dead_letter_queue()?;
    let outcome = fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        dlq,
        Timestamp::from_millis(15),
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 10,
            session: None,
        },
    ))?;
    let CommandOutcome::Peeked(dead_letters) = outcome else {
        panic!("expected DLQ peek, got {outcome:?}");
    };
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].sequence, dead_sequence);
    assert!(dead_letters[0].dead_letter.is_some());
    assert_eq!(dead_letters[0].lock, None);
    Ok(())
}

fn peek_enforces_the_official_page_bound<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(MAX_PEEK_BATCH, 250);
    assert!(MAX_PEEK_SCAN >= usize::try_from(MAX_PEEK_BATCH)?);
    let fixture = queue(provider)?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 0,
                session: None,
            }
        ),
        Err(BrokerError::EmptyPeek)
    );
    let messages = (0..=usize::try_from(MAX_PEEK_BATCH)?)
        .map(|index| input(index, None))
        .collect();
    fixture.at(12, CommandKind::SendBatch { messages })?;
    let first_page = peek(&fixture, 13, SequenceNumber::new(0), 500, None)?;
    assert_eq!(first_page.len(), usize::try_from(MAX_PEEK_BATCH)?);
    assert_eq!(first_page[0].sequence, SequenceNumber::new(1));
    assert_eq!(first_page[249].sequence, SequenceNumber::new(250));
    assert_eq!(
        peek(&fixture, 14, SequenceNumber::new(251), 1, None)?[0].sequence,
        SequenceNumber::new(251)
    );
    Ok(())
}

fn peek_continues_past_filtered_scan_chunks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let messages = (0..MAX_PEEK_SCAN)
        .map(|index| input(index, Some(1)))
        .collect();
    fixture.at(10, CommandKind::SendBatch { messages })?;
    let live = send(&fixture, 10, "live-after-expired", None, None)?;

    assert_eq!(
        peek(&fixture, 20, SequenceNumber::new(0), 1, None)?[0].sequence,
        live
    );
    assert_eq!(peek(&fixture, 20, live, 1, None)?[0].sequence, live);
    Ok(())
}

fn peek_state_survives_a_restart<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let locked_sequence = send(&fixture, 10, "locked", None, None)?;
    receive(&fixture, 11, None)?;
    let deferred_sequence = send(&fixture, 12, "deferred", None, None)?;
    let deferred = receive(&fixture, 13, None)?;
    defer(&fixture, 14, &deferred)?;

    let fixture = fixture.restart()?;
    assert_eq!(
        peek(&fixture, 15, SequenceNumber::new(0), 10, None)?
            .iter()
            .map(|delivery| (delivery.sequence, delivery.origin))
            .collect::<Vec<_>>(),
        vec![
            (locked_sequence, DeliveryOrigin::Ready),
            (deferred_sequence, DeliveryOrigin::Deferred),
        ]
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
    peek_is_inclusive_ordered_and_pages_across_sequence_gaps,
    peek_changes_no_record_counter_or_applied_clock,
    peek_exposes_ready_locked_and_deferred_origins_without_locks,
    peek_skips_expired_ready_but_keeps_locked_and_deferred_records,
    regular_peek_crosses_sessions_while_a_held_session_filters,
    session_peek_continues_past_other_session_scan_chunks,
    a_plain_queue_rejects_a_session_filtered_peek,
    a_dead_letter_queue_is_browsed_in_its_own_scope,
    peek_enforces_the_official_page_bound,
    peek_continues_past_filtered_scan_chunks,
    peek_state_survives_a_restart,
}
