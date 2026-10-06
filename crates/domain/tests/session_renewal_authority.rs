//! Protocol renewal presents the delivery's original session authority.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, LockToken, MessageBody, MessageEnvelope,
    MessageState, MessageValue, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    SettlementDisposition, Timestamp,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;

fn fixture<P: StoreProvider>(
    provider: P,
    sessions: bool,
) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: sessions,
            lock_duration_millis: 30_000,
            ..QueueConfig::default()
        },
    )?)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    id: &str,
    session: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        at,
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: vec![1, 2, 3],
            time_to_live_millis: Some(1),
            session_id: session,
            envelope: Box::new(MessageEnvelope {
                body: MessageBody::Data(vec![vec![1, 2, 3]]),
                application_properties: BTreeMap::from([(
                    "original".to_owned(),
                    MessageValue::String("preserved".to_owned()),
                )]),
                ..MessageEnvelope::default()
            }),
        },
    )?
    else {
        panic!("sent message")
    };
    Ok(sequence)
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    id: &SessionId,
    duration: u64,
) -> Result<SessionHold, BrokerError> {
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        at,
        CommandKind::AcceptSession {
            session_id: Some(id.clone()),
            lock_duration_millis: Some(duration),
        },
    )?
    else {
        panic!("accepted session")
    };
    Ok(accepted.hold())
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    session: Option<SessionHold>,
) -> Result<Delivery, BrokerError> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        at,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(10_000),
            session,
        },
    )?
    else {
        panic!("received message")
    };
    Ok(delivery)
}

fn renewal(
    delivery: &Delivery,
    session: Option<SessionHold>,
    duration: Option<u64>,
) -> CommandKind {
    CommandKind::RenewLockHeld {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("message lock").token,
        session,
        lock_duration_millis: duration,
    }
}

fn live_original_hold_renews_only_message_deadline_and_preserves_token<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider, true)?;
    let id = SessionId::new("A")?;
    send(&fixture, 10, "message", Some(id.clone()))?;
    let hold = accept(&fixture, 10, &id, 50_000)?;
    let delivery = receive(&fixture, 10, Some(hold.clone()))?;
    let original = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
        .expect("record");
    let session = fixture
        .machine
        .session(&fixture.namespace, &fixture.entity, &id)?;
    let token = delivery.lock.expect("lock").token;
    assert_eq!(
        fixture.at(13, renewal(&delivery, Some(hold.clone()), Some(20_000)))?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(20_013)
        }
    );
    assert_eq!(
        fixture.at(14, renewal(&delivery, Some(hold), None))?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(30_014)
        }
    );
    let mut renewed = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
        .expect("renewed record");
    assert_eq!(
        renewed.state,
        MessageState::Locked {
            token,
            locked_until: Timestamp::from_millis(30_014)
        }
    );
    renewed.state = original.state.clone();
    assert_eq!(renewed, original);
    assert_eq!(
        fixture
            .machine
            .session(&fixture.namespace, &fixture.entity, &id)?,
        session
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&domain::keys::lock(
                &fixture.namespace,
                &fixture.entity,
                Timestamp::from_millis(10_010),
                delivery.sequence
            ))?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&domain::keys::lock(
                &fixture.namespace,
                &fixture.entity,
                Timestamp::from_millis(20_013),
                delivery.sequence
            ))?
            .is_none()
    );
    assert_eq!(
        fixture.machine.store().get(&domain::keys::lock(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(30_014),
            delivery.sequence
        ))?,
        Some(Vec::new())
    );
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(14)
    );
    Ok(())
}

fn expired_released_reaccepted_holds_cannot_extend_message_lock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider, true)?;
    for (index, phase) in ["expired", "released", "reaccepted"]
        .into_iter()
        .enumerate()
    {
        let at = 10 + index as u64 * 100;
        let id = SessionId::new(phase)?;
        if phase == "reaccepted" {
            fixture.at(
                at,
                CommandKind::Send {
                    message_id: phase.to_owned(),
                    body: vec![1, 2, 3],
                    time_to_live_millis: None,
                    session_id: Some(id.clone()),
                },
            )?;
        } else {
            send(&fixture, at, phase, Some(id.clone()))?;
        }
        let hold = accept(&fixture, at, &id, 10)?;
        let delivery = receive(&fixture, at, Some(hold.clone()))?;
        let proposed = match phase {
            "expired" => at + 10,
            "released" => {
                fixture.at(
                    at + 1,
                    CommandKind::ReleaseSession {
                        session: hold.clone(),
                    },
                )?;
                at + 2
            }
            "reaccepted" => {
                let before = fixture.machine.store().snapshot()?;
                assert_eq!(
                    accept(&fixture, at + 10, &id, 1_000),
                    Err(BrokerError::SessionTakeoverPending {
                        session_id: id.clone()
                    })
                );
                assert_eq!(fixture.machine.store().snapshot()?, before);
                assert_eq!(
                    fixture.at(
                        at + 10,
                        CommandKind::Defer {
                            sequence: delivery.sequence,
                            lock_token: delivery.lock.expect("original message lock").token,
                        }
                    )?,
                    CommandOutcome::Deferred
                );
                let current = accept(&fixture, at + 10, &id, 1_000)?;
                assert_ne!(current.token, hold.token);
                let CommandOutcome::DeferredReceived(relocked) = fixture.at(
                    at + 10,
                    CommandKind::ReceiveDeferredHeld {
                        sequences: vec![delivery.sequence],
                        mode: ReceiveMode::PeekLock,
                        lock_duration_millis: Some(10_000),
                        session: Some(current),
                        budget: domain::DeliveryBudget {
                            max_bytes: u64::MAX,
                            per_message_overhead_bytes: 0,
                        },
                    },
                )?
                else {
                    panic!("genuine replacement delivery")
                };
                assert_eq!(relocked.len(), 1);
                assert_ne!(relocked[0].lock, delivery.lock);
                at + 11
            }
            _ => unreachable!(),
        };
        let before = fixture.machine.store().snapshot()?;
        let expected = if phase == "expired" {
            BrokerError::SessionLockExpired {
                session_id: id,
                locked_until: Timestamp::from_millis(at + 10),
            }
        } else {
            BrokerError::SessionLockNotHeld { session_id: id }
        };
        assert_eq!(
            fixture.at(proposed, renewal(&delivery, Some(hold), None)),
            Err(expected)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    Ok(())
}

fn wrong_or_missing_original_hold_refuses_before_any_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, true)?;
    let a = SessionId::new("A")?;
    let b = SessionId::new("B")?;
    send(&fixture, 10, "message", Some(a.clone()))?;
    let first = accept(&fixture, 10, &a, 20_000)?;
    let other = accept(&fixture, 10, &b, 20_000)?;
    let delivery = receive(&fixture, 10, Some(first.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(11, renewal(&delivery, None, None)),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.at(11, renewal(&delivery, Some(other), None)),
        Err(BrokerError::SessionLockNotHeld { session_id: b })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let mut invalid = renewal(&delivery, Some(first.clone()), None);
    let CommandKind::RenewLockHeld { lock_token, .. } = &mut invalid else {
        unreachable!()
    };
    *lock_token = LockToken::new(u64::MAX);
    assert_eq!(
        fixture.at(11, invalid),
        Err(BrokerError::LockTokenMismatch {
            sequence: delivery.sequence
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let mut absent = renewal(&delivery, None, None);
    let CommandKind::RenewLockHeld { sequence, .. } = &mut absent else {
        unreachable!()
    };
    *sequence = SequenceNumber::new(u64::MAX);
    assert_eq!(fixture.at(11, absent), Err(BrokerError::SessionRequired));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.at(11, renewal(&delivery, Some(first), None))?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(30_011)
        }
    );
    Ok(())
}

fn ordinary_metadata_and_dead_letters_renew_without_session_authority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, false)?;
    let id = SessionId::new("metadata")?;
    send(&fixture, 10, "message", Some(id.clone()))?;
    let delivery = receive(&fixture, 10, None)?;
    assert_eq!(delivery.session_id, Some(id.clone()));
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            11,
            renewal(
                &delivery,
                Some(SessionHold::new(id, LockToken::new(99))),
                None
            )
        ),
        Err(BrokerError::SessionNotSupported)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.at(11, renewal(&delivery, None, None))?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(30_011)
        }
    );
    fixture.at(
        12,
        CommandKind::DeadLetter {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("lock").token,
            reason: "reason".to_owned(),
            description: "description".to_owned(),
        },
    )?;
    let shadow = fixture.entity.dead_letter_queue()?;
    let mut receiving = fixture.command(
        13,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(1_000),
            session: None,
        },
    );
    receiving.entity = shadow.clone();
    let CommandOutcome::Received(Some(dead_letter)) = fixture.machine.apply(&receiving)? else {
        panic!("DLQ delivery")
    };
    assert_eq!(dead_letter.session_id, None);
    let mut renewing = fixture.command(14, renewal(&dead_letter, None, Some(2_000)));
    renewing.entity = shadow.clone();
    assert_eq!(
        fixture.machine.apply(&renewing)?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(2_014)
        }
    );
    let record = fixture
        .machine
        .message(&fixture.namespace, &shadow, dead_letter.sequence)?
        .expect("DLQ record");
    assert_eq!(record.session_id, None);
    assert_eq!(
        record.state,
        MessageState::Locked {
            token: dead_letter.lock.expect("DLQ lock").token,
            locked_until: Timestamp::from_millis(2_014)
        }
    );
    Ok(())
}

fn held_and_message_deadline_guards_are_independent_and_legacy_renewal_remains_trusted<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider, true)?;
    let id = SessionId::new("live-session")?;
    send(&fixture, 10, "first", Some(id.clone()))?;
    let hold = accept(&fixture, 10, &id, 20_000)?;
    let delivery = receive(&fixture, 10, Some(hold.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(10_010, renewal(&delivery, Some(hold), None)),
        Err(BrokerError::LockExpired {
            sequence: delivery.sequence,
            locked_until: Timestamp::from_millis(10_010)
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let id = SessionId::new("legacy")?;
    send(&fixture, 10_011, "second", Some(id.clone()))?;
    let hold = accept(&fixture, 10_011, &id, 1)?;
    let delivery = receive(&fixture, 10_011, Some(hold.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(10_012, renewal(&delivery, Some(hold), None)),
        Err(BrokerError::SessionLockExpired {
            session_id: id,
            locked_until: Timestamp::from_millis(10_012)
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.at(
            10_012,
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("lock").token,
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(10_112)
        }
    );
    Ok(())
}

#[test]
fn held_renewal_is_appended_without_rewriting_legacy_command_bytes() -> TestResult {
    let legacy = CommandKind::RenewLock {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        lock_duration_millis: None,
    };
    assert_eq!(postcard::to_stdvec(&legacy)?, vec![10, 7, 9, 0]);
    let held = CommandKind::RenewLockHeld {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        session: None,
        lock_duration_millis: None,
    };
    assert_eq!(postcard::to_stdvec(&held)?, vec![39, 7, 9, 0, 0]);
    for session in [
        None,
        Some(SessionHold::new(SessionId::new("A")?, LockToken::new(11))),
    ] {
        let command = CommandKind::RenewLockHeld {
            sequence: SequenceNumber::new(7),
            lock_token: LockToken::new(9),
            session,
            lock_duration_millis: Some(13),
        };
        assert_eq!(
            postcard::from_bytes::<CommandKind>(&postcard::to_stdvec(&command)?)?,
            command
        );
    }
    let old_tail = CommandKind::SettleHeld {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        session: None,
        disposition: SettlementDisposition::Complete,
        properties_to_modify: BTreeMap::new(),
    };
    assert_eq!(postcard::to_stdvec(&old_tail)?, vec![38, 7, 9, 0, 0, 0]);
    let session_renewal = CommandKind::RenewSessionLock {
        session: SessionHold::new(SessionId::new("A")?, LockToken::new(11)),
        lock_duration_millis: None,
    };
    assert_eq!(
        postcard::to_stdvec(&session_renewal)?,
        vec![14, 1, b'A', 11, 0]
    );
    Ok(())
}

#[test]
fn held_renewal_does_not_widen_the_closed_atomic_profile() -> TestResult {
    let fixture = fixture(testkit::MemoryProvider::new(), false)?;
    send(&fixture, 100, "existing", None)?;
    let binding = fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &fixture.entity,
            &fixture.entity,
            domain::EntityIncarnationKind::Queue,
        )?
        .expect("binding");
    let snapshot = fixture.machine.store().snapshot()?;
    let mut usage = domain::AtomicMessagingInputUsage::default();
    usage
        .try_extend(&CommandKind::Complete {
            sequence: SequenceNumber::new(7),
            lock_token: LockToken::new(9),
        })
        .expect("trusted completion");
    let before = usage;
    for session in [
        None,
        Some(SessionHold::new(
            SessionId::new("A").expect("session"),
            LockToken::new(11),
        )),
    ] {
        let command = CommandKind::RenewLockHeld {
            sequence: SequenceNumber::new(7),
            lock_token: LockToken::new(9),
            session,
            lock_duration_millis: None,
        };
        assert_eq!(
            usage.try_extend(&command),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(usage, before);
        assert_eq!(
            domain::validate_atomic_messaging_kinds(std::slice::from_ref(&command)),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        let envelope = domain::AtomicMessagingCommand {
            binding: binding.clone(),
            issued_at: Timestamp::from_millis(200),
            commands: vec![
                fixture.command(
                    200,
                    CommandKind::Send {
                        message_id: "uncommitted-prefix".to_owned(),
                        body: vec![7],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                ),
                fixture.command(200, command),
            ],
        };
        assert_eq!(
            fixture.machine.validate_atomic_messaging(&envelope),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(
            fixture.machine.apply_atomic_messaging(&envelope),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        assert_eq!(
            fixture.machine.last_applied_time()?,
            Timestamp::from_millis(100)
        );
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend!(
    live_original_hold_renews_only_message_deadline_and_preserves_token,
    expired_released_reaccepted_holds_cannot_extend_message_lock,
    wrong_or_missing_original_hold_refuses_before_any_mutation,
    ordinary_metadata_and_dead_letters_renew_without_session_authority,
    held_and_message_deadline_guards_are_independent_and_legacy_renewal_remains_trusted,
);
