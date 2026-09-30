//! A deferred batch cannot address one message more than once.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, LockToken, MessageBody, MessageEnvelope,
    MessageIdentifier, MessageProperties, MessageState, MessageValue, QueueConfig, ReceiveMode,
    SequenceNumber, SessionHold, SessionId, Timestamp, keys,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

fn rich_message() -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(String::from("rich"))),
            subject: Some(String::from("deferred payload")),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([(String::from("attempt"), MessageValue::Uint(7))]),
        body: MessageBody::Data(vec![b"rich-".to_vec(), b"payload".to_vec()]),
        ..MessageEnvelope::default()
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    id: &str,
    rich: bool,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    let kind = if rich {
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: b"rich-payload".to_vec(),
            time_to_live_millis: ttl,
            session_id,
            envelope: Box::new(rich_message()),
        }
    } else {
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"legacy-payload".to_vec(),
            time_to_live_millis: ttl,
            session_id,
        }
    };
    match fixture.at(10, kind)? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send outcome, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: Option<SessionHold>,
) -> Result<Delivery, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(500),
            session,
        },
    )? {
        CommandOutcome::Received(Some(delivery)) => Ok(delivery),
        other => panic!("expected one locked message, got {other:?}"),
    }
}

fn defer_pair<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    first: SequenceNumber,
    second: SequenceNumber,
    session: Option<SessionHold>,
) -> Result<LockToken, BrokerError> {
    let first_delivery = receive(fixture, 12, session.clone())?;
    assert_eq!(first_delivery.sequence, first);
    fixture.at(
        13,
        CommandKind::Defer {
            sequence: first,
            lock_token: first_delivery.lock.expect("peek-lock delivery").token,
        },
    )?;
    let second_delivery = receive(fixture, 14, session)?;
    assert_eq!(second_delivery.sequence, second);
    let last_token = second_delivery.lock.expect("peek-lock delivery").token;
    fixture.at(
        15,
        CommandKind::Defer {
            sequence: second,
            lock_token: last_token,
        },
    )?;
    Ok(last_token)
}

fn reject_duplicates<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    session_id: Option<SessionId>,
) -> Result<(), Box<dyn Error>> {
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        fixture.at(
            millis,
            CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis: Some(500),
                session_id,
            }
        ),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before,
        "rejection must leave records, counters, indexes and applied time unchanged"
    );
    Ok(())
}

fn unique_receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    session_id: Option<SessionId>,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::ReceiveDeferred {
            sequences,
            mode,
            lock_duration_millis: Some(500),
            session_id,
        },
    )? {
        CommandOutcome::DeferredReceived(deliveries) => Ok(deliveries),
        other => panic!("expected a deferred receive outcome, got {other:?}"),
    }
}

fn live_batch<P: StoreProvider>(
    provider: P,
    mode: ReceiveMode,
    session_queue: bool,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: session_queue,
            ..QueueConfig::default()
        },
    )?;
    let session_id = session_queue.then(|| SessionId::new("cart").expect("valid session"));
    let first = send(&fixture, "rich", true, Some(1_000), session_id.clone())?;
    let second = send(&fixture, "legacy", false, Some(1_000), session_id.clone())?;
    let session = if session_queue {
        match fixture.at(
            11,
            CommandKind::AcceptSession {
                session_id: session_id.clone(),
                lock_duration_millis: Some(1_000),
            },
        )? {
            CommandOutcome::SessionAccepted(Some(accepted)) => Some(accepted.hold()),
            other => panic!("expected an accepted session, got {other:?}"),
        }
    } else {
        None
    };
    let previous_token = defer_pair(&fixture, first, second, session.clone())?;
    reject_duplicates(
        &fixture,
        20,
        vec![first, second, first],
        mode,
        session_id.clone(),
    )?;
    let fixture = fixture.restart()?;
    let deliveries = unique_receive(&fixture, 21, vec![first, second], mode, session_id.clone())?;
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].sequence, first);
    assert_eq!(deliveries[1].sequence, second);
    assert_eq!(deliveries[0].envelope, Some(Box::new(rich_message())));
    assert_eq!(deliveries[0].body, b"rich-payload");
    assert_eq!(deliveries[1].envelope, None);
    assert_eq!(deliveries[1].body, b"legacy-payload");
    for (index, delivery) in deliveries.iter().enumerate() {
        assert_eq!(delivery.delivery_count, 2);
        assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(1_010)));
        assert_eq!(delivery.session_id, session_id);
        match mode {
            ReceiveMode::PeekLock => {
                let lock = delivery.lock.expect("unique retrieval acquires one lock");
                assert_eq!(
                    lock.token,
                    LockToken::new(previous_token.as_u64() + 1 + index as u64)
                );
                assert_eq!(lock.locked_until, Timestamp::from_millis(521));
                assert_eq!(
                    fixture.at(
                        22 + index as u64,
                        CommandKind::Complete {
                            sequence: delivery.sequence,
                            lock_token: lock.token,
                        }
                    )?,
                    CommandOutcome::Completed
                );
            }
            ReceiveMode::ReceiveAndDelete => assert_eq!(delivery.lock, None),
        }
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
                .is_none()
        );
    }
    if let Some(session) = session {
        fixture.at(
            24,
            CommandKind::RenewSessionLock {
                session,
                lock_duration_millis: Some(1_000),
            },
        )?;
    }
    Ok(())
}

fn expired_batch<P: StoreProvider>(
    provider: P,
    mode: ReceiveMode,
    dead_letter_on_expiry: bool,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: dead_letter_on_expiry,
            ..QueueConfig::default()
        },
    )?;
    let first = send(&fixture, "rich", true, Some(10), None)?;
    let second = send(&fixture, "legacy", false, Some(1_000), None)?;
    let held = send(&fixture, "held", false, Some(1_000), None)?;
    defer_pair(&fixture, first, second, None)?;
    let held_delivery = receive(&fixture, 16, None)?;
    assert_eq!(held_delivery.sequence, held);
    let held_lock = held_delivery.lock.expect("unrelated live lock");
    reject_duplicates(&fixture, 30, vec![first, second, first], mode, None)?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?
            .expect("expired content is unchanged by an invalid request")
            .state,
        MessageState::Deferred
    );
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
    let fixture = fixture.restart()?;
    let deliveries = unique_receive(&fixture, 31, vec![first, second], mode, None)?;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].sequence, second);
    assert_eq!(deliveries[0].delivery_count, 2);
    match mode {
        ReceiveMode::PeekLock => {
            let lock = deliveries[0].lock.expect("the surviving message is locked");
            assert_eq!(lock.token, LockToken::new(held_lock.token.as_u64() + 1));
            fixture.at(
                32,
                CommandKind::Complete {
                    sequence: second,
                    lock_token: lock.token,
                },
            )?;
        }
        ReceiveMode::ReceiveAndDelete => assert_eq!(deliveries[0].lock, None),
    }
    assert_eq!(
        fixture.at(
            33,
            CommandKind::Complete {
                sequence: held,
                lock_token: held_lock.token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?
            .is_none()
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, first)?
            .is_some(),
        dead_letter_on_expiry
    );
    Ok(())
}

fn duplicate_live_peek_lock_batch_is_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    live_batch(provider, ReceiveMode::PeekLock, false)
}

fn duplicate_live_receive_and_delete_batch_is_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    live_batch(provider, ReceiveMode::ReceiveAndDelete, false)
}

fn duplicate_session_peek_lock_batch_is_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    live_batch(provider, ReceiveMode::PeekLock, true)
}

fn duplicate_session_receive_and_delete_batch_is_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    live_batch(provider, ReceiveMode::ReceiveAndDelete, true)
}

fn duplicate_expired_peek_lock_batch_does_not_drop<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    expired_batch(provider, ReceiveMode::PeekLock, false)
}

fn duplicate_expired_receive_and_delete_batch_does_not_drop<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    expired_batch(provider, ReceiveMode::ReceiveAndDelete, false)
}

fn duplicate_expired_peek_lock_batch_does_not_dead_letter<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    expired_batch(provider, ReceiveMode::PeekLock, true)
}

fn duplicate_expired_receive_and_delete_batch_does_not_dead_letter<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    expired_batch(provider, ReceiveMode::ReceiveAndDelete, true)
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
    duplicate_live_peek_lock_batch_is_atomic,
    duplicate_live_receive_and_delete_batch_is_atomic,
    duplicate_session_peek_lock_batch_is_atomic,
    duplicate_session_receive_and_delete_batch_is_atomic,
    duplicate_expired_peek_lock_batch_does_not_drop,
    duplicate_expired_receive_and_delete_batch_does_not_drop,
    duplicate_expired_peek_lock_batch_does_not_dead_letter,
    duplicate_expired_receive_and_delete_batch_does_not_dead_letter,
}
