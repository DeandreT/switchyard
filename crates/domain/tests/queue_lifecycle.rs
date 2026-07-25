//! Queue delivery semantics, driven entirely by injected timestamps.

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, DeliveryLock, LockToken,
    MessageState, QueueConfig, ReceiveMode, SequenceNumber, Timestamp,
};
use testkit::{FixtureError, QueueFixture};

const LOCK_MILLIS: u64 = 30_000;

fn queue() -> Result<QueueFixture, FixtureError> {
    QueueFixture::new(
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            max_delivery_count: 2,
            ..QueueConfig::default()
        },
    )
}

fn send(fixture: &QueueFixture, millis: u64, id: &str) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: id.as_bytes().to_vec(),
            time_to_live_millis: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send outcome, got {other:?}"),
    }
}

fn receive(fixture: &QueueFixture, millis: u64) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected a receive outcome, got {other:?}"),
    }
}

fn locked(delivery: &Delivery) -> DeliveryLock {
    delivery.lock.expect("peek-lock delivery carries a lock")
}

#[test]
fn a_peek_lock_delivery_hides_the_message_from_other_receivers()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    send(&fixture, 10, "first")?;

    let delivery = receive(&fixture, 20)?.expect("the queue holds one message");
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(delivery.delivery_count, 1);
    assert_eq!(delivery.body, b"first".to_vec());
    assert_eq!(
        locked(&delivery).locked_until,
        Timestamp::from_millis(20 + LOCK_MILLIS)
    );

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

#[test]
fn messages_are_delivered_in_send_order() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn completing_a_lock_removes_the_message() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn a_foreign_lock_token_cannot_settle_a_message() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn settling_after_the_lock_elapsed_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn abandoning_returns_the_message_and_keeps_its_delivery_count()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    send(&fixture, 10, "first")?;
    let first = receive(&fixture, 20)?.expect("the queue holds one message");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::Abandon {
                sequence: first.sequence,
                lock_token: locked(&first).token,
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

#[test]
fn abandoning_at_the_delivery_limit_dead_letters_the_message()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    let sequence = send(&fixture, 10, "first")?;

    let first = receive(&fixture, 20)?.expect("the queue holds one message");
    fixture.at(
        21,
        CommandKind::Abandon {
            sequence,
            lock_token: locked(&first).token,
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

#[test]
fn an_elapsed_lock_returns_the_message_to_the_queue() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn an_elapsed_lock_dead_letters_at_the_delivery_limit() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn receive_and_delete_removes_the_message_before_returning_it()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    let sequence = send(&fixture, 10, "first")?;

    let outcome = fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
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

#[test]
fn the_time_to_live_sweep_dead_letters_expired_messages() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = queue()?;
    let sequence = fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(100),
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

#[test]
fn a_receive_never_hands_out_an_expired_message() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(100),
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

#[test]
fn an_application_can_dead_letter_a_locked_message() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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
    assert!(matches!(dead.state, MessageState::DeadLettered(_)));
    Ok(())
}

#[test]
fn a_command_that_moves_time_backward_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    send(&fixture, 100, "first")?;

    assert_eq!(
        fixture.at(
            99,
            CommandKind::Send {
                message_id: String::from("second"),
                body: Vec::new(),
                time_to_live_millis: None,
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

#[test]
fn a_send_larger_than_the_queue_limit_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = QueueFixture::new(
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
                message_id: String::from("exact"),
                body: vec![0; 8],
                time_to_live_millis: None,
            }
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    Ok(())
}

#[test]
fn commands_against_a_missing_queue_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
    let elsewhere = domain::EntityPath::new("invoices")?;
    let command = domain::Command::new(
        fixture.namespace.clone(),
        elsewhere,
        Timestamp::from_millis(10),
        CommandKind::Send {
            message_id: String::from("first"),
            body: Vec::new(),
            time_to_live_millis: None,
        },
    );

    assert_eq!(
        fixture.machine.apply(&command),
        Err(BrokerError::QueueNotFound)
    );
    Ok(())
}

#[test]
fn creating_a_queue_twice_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = queue()?;
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

#[test]
fn an_invalid_queue_configuration_is_rejected() {
    let error = QueueFixture::new(
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 0,
            ..QueueConfig::default()
        },
    )
    .expect_err("a queue that can never deliver is invalid");

    assert_eq!(
        error,
        FixtureError::Broker(BrokerError::QueueConfig(
            domain::QueueConfigError::MaxDeliveryCountTooSmall
        ))
    );
}

#[test]
fn two_queues_in_one_namespace_do_not_share_messages() -> Result<(), Box<dyn std::error::Error>> {
    let orders = queue()?;
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
