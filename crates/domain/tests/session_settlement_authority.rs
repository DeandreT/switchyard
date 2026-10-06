//! Protocol settlement presents the delivery's original session authority.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, LockToken, MessageBody, MessageEnvelope,
    MessageState, MessageValue, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    SettlementDisposition, Timestamp,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;

fn dispositions() -> [SettlementDisposition; 4] {
    [
        SettlementDisposition::Complete,
        SettlementDisposition::Abandon,
        SettlementDisposition::Defer,
        SettlementDisposition::DeadLetter {
            reason: "application reason".to_owned(),
            description: "application description".to_owned(),
        },
    ]
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
        panic!("the named session was not accepted")
    };
    Ok(accepted.hold())
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
            time_to_live_millis: None,
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
        panic!("the fixture message was not sent")
    };
    Ok(sequence)
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    hold: Option<SessionHold>,
) -> Result<Delivery, BrokerError> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        at,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(10_000),
            session: hold,
        },
    )?
    else {
        panic!("the fixture message was not received")
    };
    Ok(delivery)
}

fn command(
    delivery: &Delivery,
    hold: Option<SessionHold>,
    disposition: SettlementDisposition,
) -> CommandKind {
    CommandKind::SettleHeld {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("the message lock").token,
        session: hold,
        disposition,
        properties_to_modify: BTreeMap::from([(
            "modified".to_owned(),
            MessageValue::String("committed".to_owned()),
        )]),
    }
}

fn session_fixture<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?)
}

fn live_original_holds_settle_all_four_dispositions_with_properties<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = session_fixture(provider)?;
    for (index, disposition) in dispositions().into_iter().enumerate() {
        let at = 10 + index as u64 * 100;
        let id = SessionId::new(format!("session-{index}"))?;
        let sequence = send(&fixture, at, &format!("message-{index}"), Some(id.clone()))?;
        let hold = accept(&fixture, at + 1, &id, 10_000)?;
        let delivery = receive(&fixture, at + 2, Some(hold.clone()))?;
        assert_eq!(delivery.sequence, sequence);
        let snapshot = fixture.machine.store().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        let outcome = fixture.at(
            at + 3,
            command(&delivery, Some(hold.clone()), disposition.clone()),
        )?;
        assert!(matches!(
            (&disposition, outcome),
            (SettlementDisposition::Complete, CommandOutcome::Completed)
                | (
                    SettlementDisposition::Abandon,
                    CommandOutcome::Abandoned {
                        dead_lettered: false,
                        dropped: false
                    }
                )
                | (SettlementDisposition::Defer, CommandOutcome::Deferred)
                | (
                    SettlementDisposition::DeadLetter { .. },
                    CommandOutcome::DeadLettered
                )
        ));
        let entity = if matches!(disposition, SettlementDisposition::DeadLetter { .. }) {
            fixture.entity.dead_letter_queue()?
        } else {
            fixture.entity.clone()
        };
        let record = fixture
            .machine
            .message(&fixture.namespace, &entity, sequence)?;
        if disposition == SettlementDisposition::Complete {
            assert!(record.is_none());
        } else {
            let record = record.expect("the settled record");
            let properties = &record
                .envelope
                .as_ref()
                .expect("the envelope")
                .application_properties;
            assert_eq!(
                properties.get("original"),
                Some(&MessageValue::String("preserved".to_owned()))
            );
            assert_eq!(
                properties.get("modified"),
                Some(&MessageValue::String("committed".to_owned()))
            );
            if matches!(disposition, SettlementDisposition::DeadLetter { .. }) {
                assert_eq!(record.session_id, None);
                assert!(record.dead_letter.is_some());
            } else {
                assert_eq!(record.session_id, Some(id));
            }
        }
        assert_eq!(
            fixture.at(at + 4, CommandKind::GetSessionState { session: hold })?,
            CommandOutcome::SessionState(Vec::new())
        );
    }
    Ok(())
}

fn expired_released_and_reaccepted_original_holds_refuse_every_disposition<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = session_fixture(provider)?;
    for (index, disposition) in dispositions().into_iter().enumerate() {
        let at = 10 + index as u64 * 100;
        let id = SessionId::new(format!("session-{index}"))?;
        send(&fixture, at, &format!("message-{index}"), Some(id.clone()))?;
        let hold = accept(&fixture, at + 1, &id, 10)?;
        let delivery = receive(&fixture, at + 2, Some(hold.clone()))?;
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                at + 11,
                command(&delivery, Some(hold.clone()), disposition.clone())
            ),
            Err(BrokerError::SessionLockExpired {
                session_id: id.clone(),
                locked_until: Timestamp::from_millis(at + 11)
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let replacement = accept(&fixture, at + 11, &id, 50)?;
        assert_ne!(replacement.token, hold.token);
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                at + 12,
                command(&delivery, Some(hold.clone()), disposition.clone())
            ),
            Err(BrokerError::SessionLockNotHeld {
                session_id: id.clone()
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        fixture.at(
            at + 13,
            CommandKind::ReleaseSession {
                session: replacement.clone(),
            },
        )?;
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                at + 14,
                command(&delivery, Some(replacement.clone()), disposition.clone())
            ),
            Err(BrokerError::SessionLockNotHeld {
                session_id: id.clone()
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let third = accept(&fixture, at + 15, &id, 50)?;
        assert_ne!(third.token, replacement.token);
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(at + 16, command(&delivery, Some(replacement), disposition)),
            Err(BrokerError::SessionLockNotHeld { session_id: id })
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert!(matches!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
                .expect("the unchanged message")
                .state,
            MessageState::Locked { .. }
        ));
    }
    Ok(())
}

fn wrong_session_missing_hold_and_property_errors_do_not_mutate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = session_fixture(provider)?;
    let id = SessionId::new("original")?;
    let other_id = SessionId::new("other")?;
    send(&fixture, 10, "message", Some(id.clone()))?;
    let hold = accept(&fixture, 11, &id, 100)?;
    let other = accept(&fixture, 11, &other_id, 100)?;
    let delivery = receive(&fixture, 12, Some(hold.clone()))?;
    for disposition in dispositions() {
        for (authority, expected) in [
            (None, BrokerError::SessionRequired),
            (
                Some(other.clone()),
                BrokerError::SessionLockNotHeld {
                    session_id: other_id.clone(),
                },
            ),
            (
                Some(SessionHold::new(id.clone(), LockToken::new(u64::MAX))),
                BrokerError::SessionLockNotHeld {
                    session_id: id.clone(),
                },
            ),
        ] {
            let before = fixture.machine.store().snapshot()?;
            assert_eq!(
                fixture.at(13, command(&delivery, authority, disposition.clone())),
                Err(expected)
            );
            assert_eq!(fixture.machine.store().snapshot()?, before);
        }
        let mut invalid = command(&delivery, Some(hold.clone()), disposition);
        let CommandKind::SettleHeld {
            properties_to_modify,
            ..
        } = &mut invalid
        else {
            unreachable!()
        };
        properties_to_modify.insert("compound".to_owned(), MessageValue::List(Vec::new()));
        let before = fixture.machine.store().snapshot()?;
        assert!(matches!(
            fixture.at(13, invalid),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    fixture.at(
        14,
        command(&delivery, Some(hold), SettlementDisposition::Complete),
    )?;
    Ok(())
}

fn ordinary_metadata_and_dlq_settle_without_session_authority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, disposition) in dispositions().into_iter().enumerate() {
        let at = 10 + index as u64 * 100;
        let id = SessionId::new(format!("metadata-{index}"))?;
        send(&fixture, at, &format!("ordinary-{index}"), Some(id.clone()))?;
        let delivery = receive(&fixture, at + 1, None)?;
        assert_eq!(delivery.session_id, Some(id.clone()));
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                at + 2,
                command(
                    &delivery,
                    Some(SessionHold::new(id, LockToken::new(99))),
                    disposition.clone()
                )
            ),
            Err(BrokerError::SessionNotSupported)
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
        let outcome = fixture.at(at + 2, command(&delivery, None, disposition.clone()))?;
        assert!(matches!(
            (&disposition, outcome),
            (SettlementDisposition::Complete, CommandOutcome::Completed)
                | (
                    SettlementDisposition::Abandon,
                    CommandOutcome::Abandoned {
                        dead_lettered: false,
                        dropped: false
                    }
                )
                | (SettlementDisposition::Defer, CommandOutcome::Deferred)
                | (
                    SettlementDisposition::DeadLetter { .. },
                    CommandOutcome::DeadLettered
                )
        ));
        if disposition == SettlementDisposition::Abandon {
            let returned = receive(&fixture, at + 3, None)?;
            assert_eq!(returned.sequence, delivery.sequence);
            fixture.at(
                at + 4,
                command(&returned, None, SettlementDisposition::Complete),
            )?;
        }
        if matches!(disposition, SettlementDisposition::DeadLetter { .. }) {
            let shadow = fixture.entity.dead_letter_queue()?;
            let mut receive = fixture.command(
                at + 3,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            );
            receive.entity = shadow.clone();
            let CommandOutcome::Received(Some(dead_letter)) = fixture.machine.apply(&receive)?
            else {
                panic!("the shadow was not drained")
            };
            assert_eq!(dead_letter.session_id, None);
            let mut reserved = fixture.command(
                at + 4,
                command(&dead_letter, None, dispositions()[3].clone()),
            );
            reserved.entity = shadow.clone();
            let before = fixture.machine.store().snapshot()?;
            assert_eq!(
                fixture.machine.apply(&reserved),
                Err(BrokerError::DeadLetterQueueIsReserved)
            );
            assert_eq!(fixture.machine.store().snapshot()?, before);
            let mut complete = fixture.command(
                at + 4,
                command(&dead_letter, None, SettlementDisposition::Complete),
            );
            complete.entity = shadow.clone();
            assert_eq!(fixture.machine.apply(&complete)?, CommandOutcome::Completed);
            assert!(
                fixture
                    .machine
                    .message(&fixture.namespace, &shadow, dead_letter.sequence)?
                    .is_none()
            );
        }
    }
    Ok(())
}

fn renewal_retains_the_original_hold_and_legacy_settlement_stays_trusted<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = session_fixture(provider)?;
    let id = SessionId::new("renewed")?;
    send(&fixture, 10, "renewed-message", Some(id.clone()))?;
    let hold = accept(&fixture, 11, &id, 10)?;
    let delivery = receive(&fixture, 12, Some(hold.clone()))?;
    fixture.at(
        20,
        CommandKind::RenewSessionLock {
            session: hold.clone(),
            lock_duration_millis: Some(10),
        },
    )?;
    assert_eq!(
        fixture.at(
            21,
            command(&delivery, Some(hold), SettlementDisposition::Complete)
        )?,
        CommandOutcome::Completed
    );
    let id = SessionId::new("legacy")?;
    send(&fixture, 22, "legacy-message", Some(id.clone()))?;
    let hold = accept(&fixture, 23, &id, 1)?;
    let delivery = receive(&fixture, 23, Some(hold))?;
    assert_eq!(
        fixture.at(
            24,
            CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("the lock").token,
                disposition: SettlementDisposition::Complete,
                properties_to_modify: BTreeMap::new()
            }
        )?,
        CommandOutcome::Completed
    );
    let id = SessionId::new("independent-message-deadline")?;
    send(&fixture, 30, "message-deadline", Some(id.clone()))?;
    let hold = accept(&fixture, 31, &id, 20_000)?;
    let delivery = receive(&fixture, 32, Some(hold.clone()))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            10_032,
            command(&delivery, Some(hold), SettlementDisposition::Complete)
        ),
        Err(BrokerError::LockExpired {
            sequence: delivery.sequence,
            locked_until: Timestamp::from_millis(10_032)
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

#[test]
fn held_settlement_is_appended_without_rewriting_legacy_command_bytes() -> TestResult {
    let legacy = CommandKind::Settle {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        disposition: SettlementDisposition::Complete,
        properties_to_modify: BTreeMap::new(),
    };
    assert_eq!(postcard::to_stdvec(&legacy)?, vec![24, 7, 9, 0, 0]);
    let held = CommandKind::SettleHeld {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        session: None,
        disposition: SettlementDisposition::Complete,
        properties_to_modify: BTreeMap::new(),
    };
    assert_eq!(postcard::to_stdvec(&held)?, vec![38, 7, 9, 0, 0, 0]);
    assert_eq!(
        postcard::from_bytes::<CommandKind>(&postcard::to_stdvec(&held)?)?,
        held
    );
    let previous_tail = CommandKind::CreateRuleWithAction {
        subscription: domain::SubscriptionName::new("s")?,
        name: domain::RuleName::new("r")?,
        filter: domain::RuleFilter::True,
        action: domain::SqlAction::with_semantic_version("REMOVE x", 1)?,
    };
    assert_eq!(
        postcard::to_stdvec(&previous_tail)?,
        vec![
            37, 1, b's', 1, b'r', 0, 1, 8, b'R', b'E', b'M', b'O', b'V', b'E', b' ', b'x'
        ]
    );
    Ok(())
}

#[test]
fn held_settlement_does_not_widen_the_closed_atomic_profile() {
    let mut usage = domain::AtomicMessagingInputUsage::default();
    usage
        .try_extend(&CommandKind::Complete {
            sequence: SequenceNumber::new(7),
            lock_token: LockToken::new(9),
        })
        .expect("trusted atomic completion");
    let before = usage;
    for session in [
        None,
        Some(SessionHold::new(
            SessionId::new("A").expect("session"),
            LockToken::new(11),
        )),
    ] {
        let held = CommandKind::SettleHeld {
            sequence: SequenceNumber::new(7),
            lock_token: LockToken::new(9),
            session,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        };
        assert_eq!(
            usage.try_extend(&held),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(usage, before);
        assert_eq!(
            domain::validate_atomic_messaging_kinds(&[held]),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
    }
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult {
            super::$case(testkit::MemoryProvider::new())
        })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult {
            super::$case(testkit::DurableProvider::temporary()?)
        })+ }
    };
}

for_each_backend!(
    live_original_holds_settle_all_four_dispositions_with_properties,
    expired_released_and_reaccepted_original_holds_refuse_every_disposition,
    wrong_session_missing_hold_and_property_errors_do_not_mutate,
    ordinary_metadata_and_dlq_settle_without_session_authority,
    renewal_retains_the_original_hold_and_legacy_settlement_stays_trusted,
);
