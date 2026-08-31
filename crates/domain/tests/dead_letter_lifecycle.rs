//! The dead-letter queue as a queue: drained with the same receive and
//! settlement machinery as its parent, run against both backends.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, EntityPath, QueueConfig,
    ReceiveMode, SequenceNumber, Timestamp,
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
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?)
}

/// Sends one message and dead-letters it through the application path,
/// returning its sequence.
fn dead_letter_one<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<SequenceNumber, Box<dyn Error>> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        millis,
        CommandKind::Send {
            message_id: String::from("poison"),
            body: b"poison".to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?
    else {
        panic!("expected a send outcome");
    };
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        millis + 1,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected a delivery");
    };
    fixture.at(
        millis + 2,
        CommandKind::DeadLetter {
            sequence,
            lock_token: delivery.lock.expect("peek-lock carries a lock").token,
            reason: String::from("SchemaMismatch"),
            description: String::from("failed validation"),
            replacement_envelope: None,
        },
    )?;
    Ok(sequence)
}

/// A command against the entity's dead-letter queue.
fn at_dlq<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandOutcome, BrokerError> {
    let dlq = fixture.entity.dead_letter_queue().expect("a valid shadow");
    fixture.machine.apply(&domain::Command::new(
        fixture.namespace.clone(),
        dlq,
        Timestamp::from_millis(millis),
        kind,
    ))
}

fn receive_dlq<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match at_dlq(
        fixture,
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

// ---- the suite -------------------------------------------------------------

fn a_dead_lettered_message_is_drained_from_its_shadow_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = dead_letter_one(&fixture, 10)?;

    // The shadow queue delivers it like any queue, and it remembers why it was
    // dead-lettered.
    let delivery = receive_dlq(&fixture, 20)?.expect("the dead-lettered message is ready");
    assert_eq!(delivery.sequence, sequence);
    assert_eq!(delivery.body, b"poison".to_vec());
    let record = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("the record is in the shadow queue");
    assert_eq!(
        record.dead_letter_info().map(|info| info.reason.as_str()),
        Some("SchemaMismatch")
    );

    // Completing removes it permanently.
    assert_eq!(
        at_dlq(
            &fixture,
            21,
            CommandKind::Complete {
                sequence,
                lock_token: delivery.lock.expect("peek-lock carries a lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(receive_dlq(&fixture, 22)?, None);
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?,
        None
    );
    Ok(())
}

fn abandoning_in_the_dead_letter_queue_never_cascades<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let sequence = dead_letter_one(&fixture, 10)?;

    // The parent's delivery limit is 1 and this message has already exceeded
    // it. However often it is abandoned in the shadow queue, it returns there —
    // there is no shadow of a shadow.
    for round in 0..3_u64 {
        let millis = 100 + round * 10;
        let delivery = receive_dlq(&fixture, millis)?.expect("the message is ready again");
        assert_eq!(
            at_dlq(
                &fixture,
                millis + 1,
                CommandKind::Abandon {
                    sequence,
                    lock_token: delivery.lock.expect("peek-lock carries a lock").token,
                    replacement_envelope: None,
                }
            )?,
            CommandOutcome::Abandoned {
                dead_lettered: false
            }
        );
    }
    assert!(receive_dlq(&fixture, 200)?.is_some());
    Ok(())
}

fn an_elapsed_lock_in_the_dead_letter_queue_returns_the_message<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    dead_letter_one(&fixture, 10)?;

    let delivery = receive_dlq(&fixture, 100)?.expect("the message is ready");
    let deadline = delivery
        .lock
        .expect("peek-lock carries a lock")
        .locked_until
        .as_millis();

    assert_eq!(
        at_dlq(&fixture, deadline, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0
        }
    );
    assert!(receive_dlq(&fixture, deadline + 1)?.is_some());
    Ok(())
}

fn the_dead_letter_queue_ignores_time_to_live<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: Vec::new(),
            time_to_live_millis: Some(100),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;

    // The lifetime dead-letters it; nothing expires it out of the shadow queue,
    // however far time runs on.
    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );
    assert_eq!(
        at_dlq(&fixture, u64::MAX / 2, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    let delivery = receive_dlq(&fixture, u64::MAX / 2 + 1)?.expect("still deliverable");
    assert_eq!(
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, delivery.sequence)?
            .and_then(|record| record.dead_letter_info().map(|info| info.reason.clone())),
        Some(DeadLetterReason::TimeToLiveExpired)
    );
    Ok(())
}

fn a_session_message_dead_letters_out_of_its_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: Vec::new(),
            time_to_live_millis: Some(100),
            session_id: Some(domain::SessionId::new("cart-1")?),
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    fixture.at(110, CommandKind::ExpireMessages)?;

    // The shadow queue is sessionless even when its parent is not: the message
    // arrives stripped of its session and is drained without one.
    let delivery = receive_dlq(&fixture, 120)?.expect("the dead-lettered message is ready");
    assert_eq!(delivery.session_id, None);
    Ok(())
}

fn reserved_paths_cannot_be_created_or_sent_to<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    assert_eq!(
        at_dlq(
            &fixture,
            10,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Err(BrokerError::DeadLetterQueueIsReserved)
    );
    assert_eq!(
        at_dlq(
            &fixture,
            11,
            CommandKind::Send {
                message_id: String::from("smuggled"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }
        ),
        Err(BrokerError::DeadLetterQueueIsReserved)
    );
    Ok(())
}

fn a_parent_whose_shadow_path_would_be_too_long_is_refused<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    // Valid as an entity path on its own, but its shadow would exceed the
    // limit; refused at creation rather than at the first dead-lettering.
    let parent = "q".repeat(domain::MAX_ENTITY_PATH_BYTES - 1);
    let Err(error) = QueueFixture::new(provider, "tenant", &parent, QueueConfig::default()) else {
        panic!("a queue that could never dead-letter is refused");
    };
    assert!(matches!(
        error,
        testkit::FixtureError::Broker(BrokerError::Identifier(_))
    ));
    let _ = EntityPath::new(parent).expect("the parent alone is a valid path");
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

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
    a_dead_lettered_message_is_drained_from_its_shadow_queue,
    abandoning_in_the_dead_letter_queue_never_cascades,
    an_elapsed_lock_in_the_dead_letter_queue_returns_the_message,
    the_dead_letter_queue_ignores_time_to_live,
    a_session_message_dead_letters_out_of_its_session,
    reserved_paths_cannot_be_created_or_sent_to,
    a_parent_whose_shadow_path_would_be_too_long_is_refused,
}
