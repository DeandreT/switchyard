//! Scheduled delivery semantics over every storage backend.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, DeliveryOrigin, MessageInput, MessageState,
    QueueConfig, ReceiveMode, SequenceNumber, SessionId, TIMER_SCAN_LIMIT, Timestamp,
};
use testkit::{QueueFixture, StoreProvider};

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::with_defaults(
        provider,
        "tenant",
        "scheduled",
    )?)
}

fn session_queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "scheduled-sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?)
}

fn input(
    message_id: impl Into<String>,
    enqueue_at: Option<u64>,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> MessageInput {
    let message_id = message_id.into();
    MessageInput {
        body: message_id.as_bytes().to_vec(),
        message_id,
        time_to_live_millis: ttl,
        session_id,
        scheduled_enqueue_at: enqueue_at.map(Timestamp::from_millis),
        envelope: None,
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    issued_at: u64,
    message: MessageInput,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        issued_at,
        CommandKind::Send {
            message_id: message.message_id,
            body: message.body,
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: message.envelope,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected send, got {other:?}"),
    }
}

fn send_batch<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    issued_at: u64,
    messages: Vec<MessageInput>,
) -> Result<Vec<SequenceNumber>, BrokerError> {
    match fixture.at(issued_at, CommandKind::SendBatch { messages })? {
        CommandOutcome::BatchSent { sequences } => Ok(sequences),
        other => panic!("expected batch send, got {other:?}"),
    }
}

fn activate<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    issued_at: u64,
) -> Result<u32, BrokerError> {
    match fixture.at(issued_at, CommandKind::ActivateScheduled)? {
        CommandOutcome::ScheduledActivated { activated } => Ok(activated),
        other => panic!("expected scheduled activation, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    issued_at: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        issued_at,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive, got {other:?}"),
    }
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    issued_at: u64,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        issued_at,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 250,
            session: None,
        },
    )? {
        CommandOutcome::Peeked(deliveries) => Ok(deliveries),
        other => panic!("expected peek, got {other:?}"),
    }
}

fn future_messages_are_browseable_but_not_ready_or_expiring<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let placeholder = send(&fixture, 10, input("future", Some(100), Some(50), None))?;

    assert_eq!(placeholder, SequenceNumber::new(1));
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![placeholder]
    );
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?
            .is_empty()
    );
    assert_eq!(receive(&fixture, 99)?, None);
    assert_eq!(
        fixture.at(99, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );

    let browsed = peek(&fixture, 99)?;
    assert_eq!(browsed.len(), 1);
    assert_eq!(browsed[0].sequence, placeholder);
    assert_eq!(browsed[0].origin, DeliveryOrigin::Scheduled);
    assert_eq!(
        browsed[0].scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(browsed[0].enqueued_at, Timestamp::from_millis(10));
    assert_eq!(browsed[0].expires_at, Some(Timestamp::from_millis(150)));
    assert_eq!(browsed[0].lock, None);
    Ok(())
}

fn cancellation_is_atomic_and_consumes_no_new_sequences<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let first = send(&fixture, 10, input("first", Some(100), None, None))?;
    let second = send(&fixture, 11, input("second", Some(100), None, None))?;
    let active = send(&fixture, 12, input("active", None, None, None))?;

    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: vec![first, active],
            },
        ),
        Err(BrokerError::MessageNotScheduled { sequence: active })
    );
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![first, second],
        "the rejected cancellation must remove nothing"
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?
            .is_some()
    );
    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: vec![first, first],
            },
        ),
        Err(BrokerError::DuplicateScheduledSequence { sequence: first })
    );
    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: Vec::new(),
            },
        ),
        Err(BrokerError::EmptyScheduledCancellation)
    );
    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: vec![second, first],
            },
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 2 }
    );
    assert!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?
            .is_empty()
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?,
        None
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, second)?,
        None
    );
    assert_eq!(
        send(&fixture, 21, input("after", None, None, None))?,
        SequenceNumber::new(4),
        "cancellation must not reuse placeholder sequence numbers"
    );
    Ok(())
}

fn activation_uses_deadline_order_and_assigns_new_sequences<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let late = send(&fixture, 10, input("late", Some(200), None, None))?;
    let early_b = send(&fixture, 11, input("early-b", Some(100), None, None))?;
    let early_c = send(&fixture, 12, input("early-c", Some(100), None, None))?;
    let active = send(&fixture, 13, input("active", None, None, None))?;
    assert_eq!(
        (
            late.as_u64(),
            early_b.as_u64(),
            early_c.as_u64(),
            active.as_u64()
        ),
        (1, 2, 3, 4)
    );

    assert_eq!(activate(&fixture, 99)?, 0);
    assert_eq!(activate(&fixture, 100)?, 2);
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![late]
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![active, SequenceNumber::new(5), SequenceNumber::new(6)]
    );
    let first = receive(&fixture, 101)?.expect("the immediate message is first");
    let second = receive(&fixture, 102)?.expect("the first due message follows");
    let third = receive(&fixture, 103)?.expect("the second due message follows");
    assert_eq!(
        [
            first.message_id.as_str(),
            second.message_id.as_str(),
            third.message_id.as_str()
        ],
        ["active", "early-b", "early-c"]
    );
    assert_eq!(second.sequence, SequenceNumber::new(5));
    assert_eq!(third.sequence, SequenceNumber::new(6));
    assert_eq!(second.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(
        second.scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );

    assert_eq!(activate(&fixture, 200)?, 1);
    let last = receive(&fixture, 201)?.expect("the late message activated");
    assert_eq!(last.sequence, SequenceNumber::new(7));
    assert_eq!(last.message_id, "late");
    Ok(())
}

fn time_to_live_starts_at_actual_activation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let placeholder = send(&fixture, 10, input("ttl", Some(100), Some(50), None))?;
    assert_eq!(activate(&fixture, 120)?, 1);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, placeholder)?,
        None
    );

    let active_sequence = SequenceNumber::new(2);
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, active_sequence)?
        .expect("activation writes the active record");
    assert_eq!(record.enqueued_at, Timestamp::from_millis(120));
    assert_eq!(
        record.scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(170)));
    assert_eq!(
        fixture.at(169, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    assert_eq!(
        fixture.at(170, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );
    assert_eq!(receive(&fixture, 171)?, None);
    Ok(())
}

fn scheduled_sessions_are_not_available_before_activation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    let session_id = SessionId::new("cart-1")?;
    let placeholder = send(
        &fixture,
        10,
        input("session-message", Some(100), None, Some(session_id.clone())),
    )?;
    assert!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &session_id, 10)?
            .is_empty()
    );
    assert_eq!(
        fixture.at(
            20,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None,
            },
        )?,
        CommandOutcome::SessionAccepted(None)
    );

    assert_eq!(activate(&fixture, 100)?, 1);
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &session_id,
            10
        )?,
        vec![SequenceNumber::new(placeholder.as_u64() + 1)]
    );
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        101,
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("the activated session should be available");
    };
    assert_eq!(accepted.session_id, session_id);
    Ok(())
}

fn scheduled_state_and_cancellation_survive_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let first = send(&fixture, 10, input("first", Some(100), None, None))?;
    let second = send(&fixture, 11, input("second", Some(200), None, None))?;

    let fixture = fixture.restart()?;
    assert_eq!(
        peek(&fixture, 20)?
            .iter()
            .map(|delivery| (delivery.sequence, delivery.origin))
            .collect::<Vec<_>>(),
        vec![
            (first, DeliveryOrigin::Scheduled),
            (second, DeliveryOrigin::Scheduled),
        ]
    );
    assert_eq!(
        fixture.at(
            21,
            CommandKind::CancelScheduled {
                sequences: vec![second],
            },
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(activate(&fixture, 100)?, 1);
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(3))?
        .expect("the first placeholder activates after restart");
    assert_eq!(record.message_id, "first");
    assert_eq!(record.state, MessageState::Ready);
    Ok(())
}

fn activation_is_bounded_and_resumable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let messages = (0..=TIMER_SCAN_LIMIT)
        .map(|index| input(format!("scheduled-{index}"), Some(100), None, None))
        .collect();
    send_batch(&fixture, 10, messages)?;

    assert_eq!(activate(&fixture, 100)?, TIMER_SCAN_LIMIT as u32);
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, TIMER_SCAN_LIMIT + 1)?
            .len(),
        1
    );
    assert_eq!(activate(&fixture, 100)?, 1);
    assert!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 1)?
            .is_empty()
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, TIMER_SCAN_LIMIT + 1)?
            .len(),
        TIMER_SCAN_LIMIT + 1
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
    future_messages_are_browseable_but_not_ready_or_expiring,
    cancellation_is_atomic_and_consumes_no_new_sequences,
    activation_uses_deadline_order_and_assigns_new_sequences,
    time_to_live_starts_at_actual_activation,
    scheduled_sessions_are_not_available_before_activation,
    scheduled_state_and_cancellation_survive_restart,
    activation_is_bounded_and_resumable,
}
