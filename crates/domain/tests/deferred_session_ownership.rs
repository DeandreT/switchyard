//! Deferred session receives require a live hold before any message is read.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, DeliveryBudget, EntityPath, LockToken,
    MessageState, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId, Timestamp,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

fn held_receive(
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    max_bytes: u64,
) -> CommandKind {
    CommandKind::ReceiveDeferredHeld {
        sequences,
        mode,
        lock_duration_millis: Some(100),
        session,
        budget: DeliveryBudget {
            max_bytes,
            per_message_overhead_bytes: 64,
        },
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    session: &SessionId,
    ttl: Option<u64>,
) -> Result<SequenceNumber, BrokerError> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: vec![7; 512],
            time_to_live_millis: ttl,
            session_id: Some(session.clone()),
        },
    )?
    else {
        panic!("session message sent")
    };
    Ok(sequence)
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: &SessionId,
    duration: u64,
) -> Result<SessionHold, BrokerError> {
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        millis,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: Some(duration),
        },
    )?
    else {
        panic!("session accepted")
    };
    Ok(accepted.hold())
}

fn defer<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    hold: &SessionHold,
) -> Result<SequenceNumber, BrokerError> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: Some(hold.clone()),
        },
    )?
    else {
        panic!("session message received")
    };
    fixture.at(
        millis,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("message lock").token,
        },
    )?;
    Ok(delivery.sequence)
}

fn cost<P: StoreProvider>(fixture: &QueueFixture<P>, sequences: &[SequenceNumber]) -> u64 {
    sequences
        .iter()
        .map(|sequence| {
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, *sequence)
                .expect("message read")
                .expect("stored message")
                .delivery_size_upper_bound()
                + 64
        })
        .sum()
}

fn deliveries(outcome: CommandOutcome) -> Vec<Delivery> {
    let CommandOutcome::DeferredReceived(deliveries) = outcome else {
        panic!("deferred response")
    };
    deliveries
}

fn finish<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    deliveries: Vec<Delivery>,
    mode: ReceiveMode,
) -> Result<(), Box<dyn Error>> {
    for delivery in deliveries {
        assert_eq!(delivery.delivery_count, 2);
        if mode == ReceiveMode::PeekLock {
            fixture.at(
                millis,
                CommandKind::Complete {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.expect("message lock").token,
                },
            )?;
        } else {
            assert_eq!(delivery.lock, None);
            assert!(
                fixture
                    .machine
                    .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
                    .is_none()
            );
        }
    }
    Ok(())
}

fn live_holds_allow_atomic_bounded_retry_after_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let session = SessionId::new(format!("cart-{index}"))?;
        let first = send(&fixture, base, "first", &session, None)?;
        let second = send(&fixture, base, "second", &session, None)?;
        let hold = accept(&fixture, base + 1, &session, 1_000)?;
        assert_eq!(defer(&fixture, base + 2, &hold)?, first);
        assert_eq!(defer(&fixture, base + 2, &hold)?, second);
        let exact = cost(&fixture, &[first, second]);
        let snapshot = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                base + 3,
                held_receive(vec![first, second], mode, Some(hold.clone()), exact - 1)
            ),
            Err(BrokerError::MessageTooLarge {
                body_bytes: exact as usize,
                maximum_bytes: (exact - 1) as usize
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        let received = deliveries(fixture.at(
            base + 3,
            held_receive(vec![first, second], mode, Some(hold.clone()), exact),
        )?);
        assert_eq!(
            received
                .iter()
                .map(|delivery| delivery.sequence)
                .collect::<Vec<_>>(),
            vec![first, second]
        );
        assert!(
            received
                .iter()
                .all(|delivery| delivery.session_id.as_ref() == Some(&session))
        );
        finish(&fixture, base + 4, received, mode)?;
        assert_eq!(
            fixture.at(base + 4, CommandKind::GetSessionState { session: hold })?,
            CommandOutcome::SessionState(Vec::new())
        );
    }
    Ok(())
}

fn queue_and_hold_agreement_precedes_missing_records_and_invalid_budgets<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "ordinary")?;
    let session_entity = EntityPath::new("sessions")?;
    let mut create = fixture.command(
        0,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    );
    create.entity = session_entity.clone();
    fixture.machine.apply(&create)?;
    let hold = SessionHold::new(SessionId::new("unheld")?, LockToken::new(99));
    let snapshot = fixture.machine.store().snapshot()?;
    for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
        for sequences in [
            Vec::new(),
            vec![SequenceNumber::new(999)],
            vec![SequenceNumber::new(999); 2],
        ] {
            let mut command = fixture.command(10, held_receive(sequences.clone(), mode, None, 0));
            command.entity = session_entity.clone();
            assert_eq!(
                fixture.machine.apply(&command),
                Err(BrokerError::SessionRequired)
            );
            let mut command = fixture.command(
                10,
                held_receive(sequences.clone(), mode, Some(hold.clone()), 0),
            );
            command.entity = session_entity.clone();
            assert_eq!(
                fixture.machine.apply(&command),
                Err(BrokerError::SessionLockNotHeld {
                    session_id: hold.session_id.clone()
                })
            );
            assert_eq!(
                fixture.at(10, held_receive(sequences, mode, Some(hold.clone()), 0)),
                Err(BrokerError::SessionNotSupported)
            );
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        }
        assert_eq!(
            fixture.at(10, held_receive(Vec::new(), mode, None, 0))?,
            CommandOutcome::DeferredReceived(Vec::new())
        );
    }
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn non_session_queues_accept_none_in_both_receive_modes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "ordinary")?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let CommandOutcome::Sent { sequence } = fixture.at(
            base,
            CommandKind::Send {
                message_id: format!("message-{index}"),
                body: vec![1; 512],
                time_to_live_millis: None,
                session_id: None,
            },
        )?
        else {
            panic!("ordinary message sent")
        };
        let CommandOutcome::Received(Some(delivery)) = fixture.at(
            base + 1,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("ordinary message received")
        };
        fixture.at(
            base + 1,
            CommandKind::Defer {
                sequence,
                lock_token: delivery.lock.expect("message lock").token,
            },
        )?;
        let received = deliveries(fixture.at(
            base + 2,
            held_receive(vec![sequence], mode, None, cost(&fixture, &[sequence])),
        )?);
        assert_eq!(received[0].session_id, None);
        finish(&fixture, base + 3, received, mode)?;
    }
    Ok(())
}

fn expired_holds_cannot_clean_up_expired_deferred_messages<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let session = SessionId::new(format!("expired-{index}"))?;
        let sequence = send(&fixture, base, "expired", &session, Some(5))?;
        let hold = accept(&fixture, base + 1, &session, 5)?;
        assert_eq!(defer(&fixture, base + 2, &hold)?, sequence);
        let snapshot = fixture.machine.store().snapshot()?;
        let expected = BrokerError::SessionLockExpired {
            session_id: session.clone(),
            locked_until: Timestamp::from_millis(base + 6),
        };
        for sequences in [
            vec![sequence],
            vec![SequenceNumber::new(999)],
            vec![sequence; 2],
            Vec::new(),
        ] {
            assert_eq!(
                fixture.at(
                    base + 6,
                    held_receive(sequences, mode, Some(hold.clone()), 0)
                ),
                Err(expected.clone())
            );
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        }
        fixture = fixture.restart()?;
        assert_eq!(
            fixture.at(
                base + 6,
                held_receive(vec![sequence], mode, Some(hold), u64::MAX)
            ),
            Err(expected)
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?
                .expect("retained expired message")
                .state,
            MessageState::Deferred
        );
        assert!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    &fixture.entity.dead_letter_queue()?,
                    sequence
                )?
                .is_none()
        );
        let replacement = accept(&fixture, base + 7, &session, 1_000)?;
        let exact = cost(&fixture, &[sequence]);
        assert!(
            deliveries(fixture.at(
                base + 8,
                held_receive(vec![sequence], mode, Some(replacement), exact)
            )?)
            .is_empty()
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
                .message(
                    &fixture.namespace,
                    &fixture.entity.dead_letter_queue()?,
                    sequence
                )?
                .is_some()
        );
    }
    Ok(())
}

fn released_holds_and_old_tokens_after_reacceptance_cannot_receive<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let session = SessionId::new(format!("cart-{index}"))?;
        let sequence = send(&fixture, base, "deferred", &session, None)?;
        let old = accept(&fixture, base + 1, &session, 1_000)?;
        defer(&fixture, base + 2, &old)?;
        fixture.at(
            base + 3,
            CommandKind::ReleaseSession {
                session: old.clone(),
            },
        )?;
        let released = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                base + 4,
                held_receive(vec![sequence], mode, Some(old.clone()), u64::MAX)
            ),
            Err(BrokerError::SessionLockNotHeld {
                session_id: session.clone()
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, released);
        let replacement = accept(&fixture, base + 4, &session, 1_000)?;
        assert_ne!(old.token, replacement.token);
        let replaced = fixture.machine.store().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(
            fixture.at(
                base + 5,
                held_receive(vec![sequence], mode, Some(old), u64::MAX)
            ),
            Err(BrokerError::SessionLockNotHeld {
                session_id: session
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, replaced);
        let exact = cost(&fixture, &[sequence]);
        let received = deliveries(fixture.at(
            base + 5,
            held_receive(vec![sequence], mode, Some(replacement), exact),
        )?);
        finish(&fixture, base + 6, received, mode)?;
    }
    Ok(())
}

fn mixed_session_batches_reject_in_either_order_without_allocating_locks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let session_a = SessionId::new(format!("a-{index}"))?;
        let session_b = SessionId::new(format!("b-{index}"))?;
        let a = send(&fixture, base, "a", &session_a, None)?;
        let b = send(&fixture, base, "b", &session_b, None)?;
        let hold_a = accept(&fixture, base + 1, &session_a, 1_000)?;
        let hold_b = accept(&fixture, base + 1, &session_b, 1_000)?;
        defer(&fixture, base + 2, &hold_a)?;
        defer(&fixture, base + 2, &hold_b)?;
        let snapshot = fixture.machine.store().snapshot()?;
        for sequences in [vec![b, a], vec![a, b]] {
            assert_eq!(
                fixture.at(
                    base + 3,
                    held_receive(sequences, mode, Some(hold_a.clone()), u64::MAX)
                ),
                Err(BrokerError::MessageNotDeferred { sequence: b })
            );
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        }
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        for (sequence, hold) in [(a, hold_a), (b, hold_b)] {
            let exact = cost(&fixture, &[sequence]);
            let received = deliveries(fixture.at(
                base + 3,
                held_receive(vec![sequence], mode, Some(hold), exact),
            )?);
            assert_eq!(received[0].sequence, sequence);
            finish(&fixture, base + 3, received, mode)?;
        }
    }
    Ok(())
}

#[test]
fn held_command_appends_without_changing_existing_command_shapes() -> Result<(), Box<dyn Error>> {
    let session = SessionId::new("cart")?;
    let budget = DeliveryBudget {
        max_bytes: 1_024,
        per_message_overhead_bytes: 64,
    };
    let legacy = CommandKind::ReceiveDeferred {
        sequences: vec![SequenceNumber::new(7)],
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: Some(9),
        session_id: Some(session.clone()),
    };
    assert_eq!(
        postcard::to_stdvec(&legacy)?,
        vec![11, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116]
    );
    let bounded = CommandKind::ReceiveDeferredBounded {
        sequences: vec![SequenceNumber::new(7)],
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: Some(9),
        session_id: Some(session.clone()),
        budget,
    };
    assert_eq!(
        postcard::to_stdvec(&bounded)?,
        vec![25, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116, 128, 8, 64]
    );
    let peek = CommandKind::PeekBounded {
        from_sequence: SequenceNumber::new(7),
        max_messages: 2,
        session_id: Some(session.clone()),
        budget,
    };
    assert_eq!(
        postcard::to_stdvec(&peek)?,
        vec![26, 7, 2, 1, 4, 99, 97, 114, 116, 128, 8, 64]
    );
    let held = CommandKind::ReceiveDeferredHeld {
        sequences: vec![SequenceNumber::new(7)],
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: Some(9),
        session: Some(SessionHold::new(session, LockToken::new(11))),
        budget,
    };
    assert_eq!(
        postcard::to_stdvec(&held)?,
        vec![27, 1, 7, 1, 1, 9, 1, 4, 99, 97, 114, 116, 11, 128, 8, 64]
    );
    assert_eq!(
        postcard::from_bytes::<CommandKind>(&postcard::to_stdvec(&held)?)?,
        held
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
    live_holds_allow_atomic_bounded_retry_after_restart,
    queue_and_hold_agreement_precedes_missing_records_and_invalid_budgets,
    non_session_queues_accept_none_in_both_receive_modes,
    expired_holds_cannot_clean_up_expired_deferred_messages,
    released_holds_and_old_tokens_after_reacceptance_cannot_receive,
    mixed_session_batches_reject_in_either_order_without_allocating_locks,
}
