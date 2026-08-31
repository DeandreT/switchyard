//! Queue delivery semantics, driven entirely by injected timestamps.
//!
//! Every case is generic over the backend its state lives on and is run twice,
//! once in memory and once on a real store directory. Delivery semantics are a
//! property of the state machine, not of where its records are kept, so a case
//! that passes on one backend and fails on the other is a storage bug.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, DeliveryLock, LockToken,
    MessageEnvelope, MessageState, QueueConfig, ReceiveMode, SequenceNumber, Timestamp,
};
use testkit::{QueueFixture, StoreProvider};

const LOCK_MILLIS: u64 = 30_000;

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            max_delivery_count: 2,
            ..QueueConfig::default()
        },
    )?)
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: id.as_bytes().to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send outcome, got {other:?}"),
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
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected a receive outcome, got {other:?}"),
    }
}

fn locked(delivery: &Delivery) -> DeliveryLock {
    delivery.lock.expect("peek-lock delivery carries a lock")
}

// ---- the suite -------------------------------------------------------------

fn a_peek_lock_delivery_hides_the_message_from_other_receivers<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;

    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(delivery.delivery_count, 1);
    assert_eq!(delivery.body, b"first".to_vec());
    assert_eq!(
        locked(&delivery).locked_until,
        Timestamp::from_millis(20 + LOCK_MILLIS)
    );
    assert_eq!(locked(&delivery).lock_duration_millis, LOCK_MILLIS);

    // The lock is still held, so a second receiver sees an empty queue.
    assert_eq!(receive(&fixture, 21)?, None);
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 16)?
            .is_empty()
    );
    Ok(())
}

fn messages_are_delivered_in_send_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    for (index, id) in ["first", "second", "third"].iter().enumerate() {
        let sequence = send(&fixture, 10 + index as u64, id)?;
        assert_eq!(sequence, SequenceNumber::new(index as u64 + 1));
    }

    let mut delivered = Vec::new();
    for tick in 0..3 {
        let delivery = receive(&fixture, 100 + tick)?.expect("a message is ready");
        delivered.push(delivery.message_id);
    }
    assert_eq!(delivered, vec!["first", "second", "third"]);
    Ok(())
}

fn completing_a_lock_removes_the_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: locked(&delivery).token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, delivery.sequence)?,
        None
    );
    // Redelivery cannot happen after settlement, even once the lock elapses.
    fixture.at(20 + LOCK_MILLIS + 1, CommandKind::ExpireLocks)?;
    assert_eq!(receive(&fixture, 20 + LOCK_MILLIS + 2)?, None);
    Ok(())
}

fn a_foreign_lock_token_cannot_settle_a_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: LockToken::new(locked(&delivery).token.as_u64() + 1),
            }
        ),
        Err(BrokerError::LockTokenMismatch {
            sequence: delivery.sequence
        })
    );
    // The rejection left the lock intact.
    assert_eq!(
        fixture.at(
            31,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: locked(&delivery).token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn renewing_a_lock_moves_its_deadline_without_changing_its_token<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    const RENEW_MILLIS: u64 = 60_000;

    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    let lock = locked(&delivery);
    let renewed_until = Timestamp::from_millis(100 + RENEW_MILLIS);

    assert_eq!(
        fixture.at(
            100,
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: lock.token,
                lock_duration_millis: Some(RENEW_MILLIS),
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: renewed_until,
            lock_duration_millis: RENEW_MILLIS,
        }
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
            .expect("the renewed message remains stored")
            .state,
        MessageState::Locked {
            token: lock.token,
            locked_until: renewed_until,
            origin: domain::DeliveryOrigin::Ready,
        }
    );

    // The stale deadline index was removed, so its sweep cannot release the
    // renewed delivery.
    assert_eq!(
        fixture.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0,
        }
    );
    assert_eq!(
        fixture.at(
            lock.locked_until.as_millis() + 1,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: lock.token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn a_foreign_lock_token_cannot_renew_a_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    let lock = locked(&delivery);

    assert_eq!(
        fixture.at(
            30,
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: LockToken::new(lock.token.as_u64() + 1),
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::LockTokenMismatch {
            sequence: delivery.sequence
        })
    );
    assert_eq!(
        fixture.at(
            31,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: lock.token,
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn an_elapsed_lock_cannot_be_renewed<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    let lock = locked(&delivery);

    assert_eq!(
        fixture.at(
            lock.locked_until.as_millis(),
            CommandKind::RenewLock {
                sequence: delivery.sequence,
                lock_token: lock.token,
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::LockExpired {
            sequence: delivery.sequence,
            locked_until: lock.locked_until,
        })
    );
    Ok(())
}

fn settling_after_the_lock_elapsed_is_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    let lock = locked(&delivery);

    assert_eq!(
        fixture.at(
            lock.locked_until.as_millis(),
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: lock.token,
            }
        ),
        Err(BrokerError::LockExpired {
            sequence: delivery.sequence,
            locked_until: lock.locked_until
        })
    );
    Ok(())
}

fn abandoning_returns_the_message_and_keeps_its_delivery_count<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    let first = receive(&fixture, 20)?.expect("the queue holds one message");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::Abandon {
                sequence: first.sequence,
                lock_token: locked(&first).token,
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );

    let second = receive(&fixture, 40)?.expect("the abandoned message is ready again");
    assert_eq!(second.sequence, first.sequence);
    assert_eq!(second.delivery_count, 2);
    Ok(())
}

fn a_protocol_envelope_survives_restart_redelivery_and_the_dead_letter_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let envelope = MessageEnvelope::new(vec![0, 0x53, 0x77, 0xa1, 3, b'a', b'm', b'q']);
    let CommandOutcome::Sent { sequence } = fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("enveloped"),
            body: b"normalized-body".to_vec(),
            time_to_live_millis: Some(1_000),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: Some(envelope.clone()),
        },
    )?
    else {
        panic!("expected a send outcome");
    };

    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .expect("the sent message is durable")
            .envelope,
        Some(envelope.clone())
    );

    let fixture = fixture.restart()?;
    let first = receive(&fixture, 20)?.expect("the restarted queue retains the message");
    assert_eq!(first.expires_at, Some(Timestamp::from_millis(1_010)));
    assert_eq!(first.envelope, Some(envelope.clone()));
    fixture.at(
        30,
        CommandKind::Abandon {
            sequence,
            lock_token: locked(&first).token,
            replacement_envelope: None,
        },
    )?;

    let second = receive(&fixture, 40)?.expect("the envelope is redelivered");
    assert_eq!(second.envelope, Some(envelope.clone()));
    fixture.at(
        50,
        CommandKind::DeadLetter {
            sequence,
            lock_token: locked(&second).token,
            reason: String::from("SchemaMismatch"),
            description: String::from("message could not be processed"),
            replacement_envelope: None,
        },
    )?;

    let dead_letter_queue = fixture.entity.dead_letter_queue()?;
    let stored = fixture
        .machine
        .message(&fixture.namespace, &dead_letter_queue, sequence)?
        .expect("the envelope moved into the dead-letter queue");
    assert_eq!(stored.envelope, Some(envelope.clone()));
    assert_eq!(stored.expires_at, None);

    let outcome = fixture.machine.apply(&domain::Command::new(
        fixture.namespace.clone(),
        dead_letter_queue,
        Timestamp::from_millis(60),
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ))?;
    let CommandOutcome::Received(Some(dead_lettered)) = outcome else {
        panic!("expected the envelope from the dead-letter queue, got {outcome:?}");
    };
    assert_eq!(dead_lettered.envelope, Some(envelope));
    assert_eq!(dead_lettered.expires_at, None);
    assert_eq!(
        dead_lettered
            .dead_letter
            .as_ref()
            .map(|info| info.reason.as_str()),
        Some("SchemaMismatch")
    );
    Ok(())
}

fn abandoning_at_the_delivery_limit_dead_letters_the_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first")?;

    let first = receive(&fixture, 20)?.expect("the queue holds one message");
    fixture.at(
        21,
        CommandKind::Abandon {
            sequence,
            lock_token: locked(&first).token,
            replacement_envelope: None,
        },
    )?;

    // max_delivery_count is 2, so the second abandon exhausts it.
    let second = receive(&fixture, 30)?.expect("the message is ready again");
    assert_eq!(second.delivery_count, 2);
    assert_eq!(
        fixture.at(
            31,
            CommandKind::Abandon {
                sequence,
                lock_token: locked(&second).token,
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );

    assert_eq!(receive(&fixture, 40)?, None);
    let dead = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("the message is in the dead-letter keyspace");
    assert_eq!(
        dead.dead_letter_info().map(|info| info.reason.clone()),
        Some(DeadLetterReason::MaxDeliveryCountExceeded)
    );
    assert_eq!(
        dead.dead_letter_info().map(|info| info.reason.as_str()),
        Some("MaxDeliveryCountExceeded")
    );
    Ok(())
}

fn an_elapsed_lock_returns_the_message_to_the_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    let locked_until = locked(&delivery).locked_until;

    // Before the deadline the sweep changes nothing.
    assert_eq!(
        fixture.at(locked_until.as_millis() - 1, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0
        }
    );
    assert_eq!(receive(&fixture, locked_until.as_millis() - 1)?, None);

    assert_eq!(
        fixture.at(locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0
        }
    );
    let redelivered = receive(&fixture, locked_until.as_millis() + 1)?
        .expect("the message returned to the queue");
    assert_eq!(redelivered.sequence, sequence);
    assert_eq!(redelivered.delivery_count, 2);
    Ok(())
}

fn an_elapsed_lock_dead_letters_at_the_delivery_limit<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first")?;

    let first = receive(&fixture, 20)?.expect("the queue holds one message");
    let first_deadline = locked(&first).locked_until.as_millis();
    fixture.at(first_deadline, CommandKind::ExpireLocks)?;

    let second = receive(&fixture, first_deadline + 1)?.expect("the message returned");
    assert_eq!(second.delivery_count, 2);
    let second_deadline = locked(&second).locked_until.as_millis();

    assert_eq!(
        fixture.at(second_deadline, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 1
        }
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![sequence]
    );
    Ok(())
}

fn receive_and_delete_removes_the_message_before_returning_it<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first")?;

    let outcome = fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?;
    let CommandOutcome::Received(Some(delivery)) = outcome else {
        panic!("expected a delivery, got {outcome:?}");
    };

    assert_eq!(delivery.sequence, sequence);
    assert_eq!(delivery.body, b"first".to_vec());
    // At-most-once: nothing remains to redeliver.
    assert_eq!(delivery.lock, None);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?,
        None
    );
    assert_eq!(receive(&fixture, 30)?, None);
    Ok(())
}

fn the_time_to_live_sweep_dead_letters_expired_messages<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(100),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    let CommandOutcome::Sent { sequence } = sequence else {
        panic!("expected a send outcome");
    };

    assert_eq!(
        fixture.at(109, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );

    let dead = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("the expired message is dead-lettered");
    assert_eq!(
        dead.dead_letter_info().map(|info| info.reason.as_str()),
        Some("TTLExpiredException")
    );
    assert_eq!(receive(&fixture, 120)?, None);
    Ok(())
}

fn a_receive_never_hands_out_an_expired_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(100),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    send(&fixture, 11, "durable")?;

    // The timer has not run, so the expired message is still at the head of the
    // ready index. The receive must skip and dead-letter it.
    let delivery = receive(&fixture, 200)?.expect("the durable message is deliverable");
    assert_eq!(delivery.message_id, "durable");
    assert_eq!(
        fixture
            .machine
            .dead_lettered_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![SequenceNumber::new(1)]
    );
    Ok(())
}

fn an_application_can_dead_letter_a_locked_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = send(&fixture, 10, "first")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::DeadLetter {
                sequence,
                lock_token: locked(&delivery).token,
                reason: String::from("SchemaMismatch"),
                description: String::from("order payload failed validation"),
                replacement_envelope: None,
            }
        )?,
        CommandOutcome::DeadLettered
    );

    let dead = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("the message is dead-lettered");
    let info = dead
        .dead_letter_info()
        .expect("dead-letter info is recorded");
    assert_eq!(info.reason.as_str(), "SchemaMismatch");
    assert_eq!(info.description, "order payload failed validation");
    assert_eq!(info.dead_lettered_at, Timestamp::from_millis(30));
    // Dead-lettered is a queue, not a state: the record waits there ready.
    assert!(matches!(dead.state, MessageState::Ready));
    Ok(())
}

fn a_command_that_moves_time_backward_is_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 100, "first")?;

    assert_eq!(
        fixture.at(
            99,
            CommandKind::Send {
                message_id: String::from("second"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }
        ),
        Err(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(100),
            proposed: Timestamp::from_millis(99)
        })
    );
    // The rejected command wrote nothing.
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![SequenceNumber::new(1)]
    );
    Ok(())
}

fn a_send_larger_than_the_queue_limit_is_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 8,
            ..QueueConfig::default()
        },
    )?;

    assert_eq!(
        fixture.at(
            10,
            CommandKind::Send {
                message_id: String::from("oversized"),
                body: vec![0; 9],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 9,
            maximum_bytes: 8
        })
    );
    assert_eq!(
        fixture.at(
            11,
            CommandKind::Send {
                message_id: String::from("oversized-envelope"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(vec![0; 9])),
            }
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 9,
            maximum_bytes: 8
        })
    );
    assert_eq!(
        fixture.at(
            12,
            CommandKind::Send {
                message_id: String::from("exact-envelope"),
                // When a lossless envelope is present it is the complete wire
                // message, so its length is the authoritative size.
                body: vec![0; 9],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(vec![0; 8])),
            }
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    Ok(())
}

fn commands_against_a_missing_queue_are_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let elsewhere = domain::EntityPath::new("invoices")?;
    let command = domain::Command::new(
        fixture.namespace.clone(),
        elsewhere,
        Timestamp::from_millis(10),
        CommandKind::Send {
            message_id: String::from("first"),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    );

    assert_eq!(
        fixture.machine.apply(&command),
        Err(BrokerError::QueueNotFound)
    );
    Ok(())
}

fn creating_a_queue_twice_is_rejected<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Err(BrokerError::QueueAlreadyExists)
    );
    Ok(())
}

fn an_invalid_queue_configuration_is_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let Err(error) = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 0,
            ..QueueConfig::default()
        },
    ) else {
        panic!("a queue that can never deliver is invalid");
    };

    assert_eq!(
        error,
        testkit::FixtureError::Broker(BrokerError::QueueConfig(
            domain::QueueConfigError::MaxDeliveryCountTooSmall
        ))
    );
    Ok(())
}

fn two_queues_in_one_namespace_do_not_share_messages<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let orders = queue(provider)?;
    send(&orders, 10, "for-orders")?;

    let invoices = domain::EntityPath::new("invoices")?;
    orders.machine.apply(&domain::Command::new(
        orders.namespace.clone(),
        invoices.clone(),
        Timestamp::from_millis(11),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ))?;

    assert_eq!(
        orders
            .machine
            .ready_sequences(&orders.namespace, &invoices, 16)?,
        Vec::new()
    );
    assert_eq!(
        orders
            .machine
            .ready_sequences(&orders.namespace, &orders.entity, 16)?,
        vec![SequenceNumber::new(1)]
    );
    Ok(())
}

fn a_command_that_changes_nothing_commits_nothing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    // An empty receive and an idle sweep decide nothing from their timestamps,
    // so they leave no trace — not even the clock. On the durable backend each
    // of these would otherwise be an fsync.
    assert_eq!(receive(&fixture, 50)?, None);
    fixture.at(60, CommandKind::ExpireLocks)?;
    fixture.at(70, CommandKind::ExpireMessages)?;
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(0)
    );

    // Time not having advanced is observable: a send stamped before the empty
    // receive is still accepted, because nothing was decided at the later time.
    send(&fixture, 10, "first")?;
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(10)
    );
    Ok(())
}

fn a_restart_preserves_locks_counters_and_queue_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 10, "first")?;
    send(&fixture, 11, "second")?;
    let delivery = receive(&fixture, 20)?.expect("the queue holds two messages");

    let fixture = fixture.restart()?;

    // A lock token issued before the restart still settles the message it names,
    // so the lock itself was replicated state rather than something the running
    // process held.
    assert_eq!(
        fixture.at(
            30,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: locked(&delivery).token,
            }
        )?,
        CommandOutcome::Completed
    );
    // The applied clock, the ready index, and the sequence counter all came back
    // where the machine left them.
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(30)
    );
    let next = receive(&fixture, 40)?.expect("the second message is still ready");
    assert_eq!(next.message_id, "second");
    assert_eq!(send(&fixture, 50, "third")?, SequenceNumber::new(3));
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

/// Runs every named case against both backends, so the two suites cannot drift.
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
    a_peek_lock_delivery_hides_the_message_from_other_receivers,
    messages_are_delivered_in_send_order,
    completing_a_lock_removes_the_message,
    a_foreign_lock_token_cannot_settle_a_message,
    renewing_a_lock_moves_its_deadline_without_changing_its_token,
    a_foreign_lock_token_cannot_renew_a_message,
    an_elapsed_lock_cannot_be_renewed,
    settling_after_the_lock_elapsed_is_rejected,
    abandoning_returns_the_message_and_keeps_its_delivery_count,
    a_protocol_envelope_survives_restart_redelivery_and_the_dead_letter_queue,
    abandoning_at_the_delivery_limit_dead_letters_the_message,
    an_elapsed_lock_returns_the_message_to_the_queue,
    an_elapsed_lock_dead_letters_at_the_delivery_limit,
    receive_and_delete_removes_the_message_before_returning_it,
    the_time_to_live_sweep_dead_letters_expired_messages,
    a_receive_never_hands_out_an_expired_message,
    an_application_can_dead_letter_a_locked_message,
    a_command_that_moves_time_backward_is_rejected,
    a_send_larger_than_the_queue_limit_is_rejected,
    commands_against_a_missing_queue_are_rejected,
    creating_a_queue_twice_is_rejected,
    an_invalid_queue_configuration_is_rejected,
    two_queues_in_one_namespace_do_not_share_messages,
    a_command_that_changes_nothing_commits_nothing,
    a_restart_preserves_locks_counters_and_queue_order,
}
