//! Scheduled messages retain cancellation handles until deterministic activation.

use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterReason, Delivery, MessageState,
    MessageStatus, QueueConfig, ReceiveMode, ScheduledMessage, SequenceNumber, SessionId,
    TIMER_SCAN_LIMIT, Timestamp, keys,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::with_defaults(provider, "tenant", "orders")?)
}

fn message(id: &str, enqueue_at: u64) -> ScheduledMessage {
    ScheduledMessage {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        enqueue_at: Timestamp::from_millis(enqueue_at),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn schedule<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    messages: Vec<ScheduledMessage>,
) -> Result<Vec<SequenceNumber>, BrokerError> {
    match fixture.at(millis, CommandKind::Schedule { messages })? {
        CommandOutcome::Scheduled { sequences } => Ok(sequences),
        other => panic!("expected schedule outcome, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn scheduled_messages_are_peekable_but_not_deliverable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handle = schedule(&fixture, 10, vec![message("later", 100)])?[0];
    assert_eq!(receive(&fixture, 20)?, None);
    assert_eq!(
        fixture.at(99, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    let CommandOutcome::Peeked(peeked) = fixture.at(
        99,
        CommandKind::Peek {
            from_sequence: handle,
            max_messages: 1,
            session_id: None,
        },
    )?
    else {
        panic!("expected peek outcome");
    };
    assert_eq!(peeked.len(), 1);
    assert_eq!(peeked[0].sequence, handle);
    assert_eq!(peeked[0].status, MessageStatus::Scheduled);
    assert_eq!(
        peeked[0].scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(peeked[0].delivery_count, 0);
    assert_eq!(peeked[0].lock, None);
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let delivery = receive(&fixture, 101)?.expect("activation made the message ready");
    assert_eq!(delivery.message_id, "later");
    assert_ne!(delivery.sequence, handle);
    assert_eq!(delivery.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(delivery.status, MessageStatus::Active);
    assert_eq!(
        delivery.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    Ok(())
}

fn activation_assigns_queue_positions_in_due_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handles = schedule(
        &fixture,
        10,
        vec![message("late", 300), message("early", 100)],
    )?;
    fixture.at(
        20,
        CommandKind::Send {
            message_id: String::from("already-active"),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert_eq!(
        fixture.at(150, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert_eq!(
        fixture.at(300, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let mut ids = Vec::new();
    let mut sequences = Vec::new();
    for millis in 301..304 {
        let delivery = receive(&fixture, millis)?.expect("three active messages");
        ids.push(delivery.message_id);
        sequences.push(delivery.sequence);
    }
    assert_eq!(ids, vec!["already-active", "early", "late"]);
    assert_eq!(
        sequences,
        vec![
            SequenceNumber::new(3),
            SequenceNumber::new(4),
            SequenceNumber::new(5)
        ]
    );
    for handle in handles {
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, handle)?
                .is_none()
        );
    }
    Ok(())
}

fn scheduling_a_batch_is_atomic<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 8,
            ..QueueConfig::default()
        },
    )?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        schedule(
            &fixture,
            10,
            vec![message("valid", 100), message("too-large", 100)]
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 9,
            maximum_bytes: 8
        })
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    assert_eq!(
        schedule(&fixture, 10, vec![message("valid", 100)])?,
        vec![SequenceNumber::new(1)]
    );
    Ok(())
}

fn cancellation_removes_both_record_and_timer_entry<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handles = schedule(
        &fixture,
        10,
        vec![message("cancel", 100), message("keep", 100)],
    )?;
    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: vec![handles[0], handles[0]]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, handles[0])?
            .is_none()
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::scheduled_prefix(&fixture.namespace, &fixture.entity),
                10
            )?
            .len(),
        1
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert_eq!(
        receive(&fixture, 101)?
            .expect("uncancelled message activated")
            .message_id,
        "keep"
    );
    Ok(())
}

fn cancellation_rejects_a_stale_handle_atomically<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handle = schedule(&fixture, 10, vec![message("keep", 100)])?[0];
    let missing = SequenceNumber::new(999);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        fixture.at(
            20,
            CommandKind::CancelScheduled {
                sequences: vec![handle, missing]
            }
        ),
        Err(BrokerError::MessageNotFound { sequence: missing })
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    fixture.at(100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![handle]
            }
        ),
        Err(BrokerError::MessageNotFound { sequence: handle })
    );
    let active = SequenceNumber::new(2);
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![active]
            }
        ),
        Err(BrokerError::MessageNotScheduled { sequence: active })
    );
    assert!(receive(&fixture, 102)?.is_some());
    Ok(())
}

fn lifetime_starts_at_activation_not_scheduling<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: Some(50),
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let handles = schedule(
        &fixture,
        10,
        vec![
            message("default", 100),
            ScheduledMessage {
                time_to_live_millis: Some(25),
                ..message("shorter", 100)
            },
        ],
    )?;
    assert_eq!(
        fixture.at(200, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    for handle in handles {
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, handle)?
            .expect("scheduled message persists");
        assert_eq!(record.expires_at, None);
        assert!(!record.is_expired_at(Timestamp::from_millis(200)));
    }
    fixture.at(200, CommandKind::ActivateScheduled)?;
    let default = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(3))?
        .expect("activated default lifetime");
    assert_eq!(default.enqueued_at, Timestamp::from_millis(200));
    assert_eq!(default.expires_at, Some(Timestamp::from_millis(250)));
    let shorter = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(4))?
        .expect("activated shorter lifetime");
    assert_eq!(shorter.expires_at, Some(Timestamp::from_millis(225)));
    assert_eq!(
        fixture.at(225, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1,
        }
    );
    let dead = fixture
        .machine
        .dead_lettered_message(&fixture.namespace, &fixture.entity, shorter.sequence)?
        .expect("shorter TTL expired");
    assert_eq!(
        dead.dead_letter_info().expect("reason preserved").reason,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(
        receive(&fixture, 226)?
            .expect("default still live")
            .message_id,
        "default"
    );
    Ok(())
}

fn past_schedules_become_active_immediately<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handles = schedule(
        &fixture,
        100,
        vec![message("past", 99), message("now", 100)],
    )?;
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(
        fixture.at(
            100,
            CommandKind::CancelScheduled {
                sequences: vec![handles[0]]
            }
        ),
        Err(BrokerError::MessageNotScheduled {
            sequence: handles[0]
        })
    );
    for (index, id) in ["past", "now"].into_iter().enumerate() {
        let delivery = receive(&fixture, 101 + index as u64)?.expect("already active");
        assert_eq!(delivery.sequence, handles[index]);
        assert_eq!(delivery.message_id, id);
        assert_eq!(delivery.enqueued_at, Timestamp::from_millis(100));
    }
    Ok(())
}

fn activation_is_bounded_and_resumable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let messages = (0..=TIMER_SCAN_LIMIT)
        .map(|index| message(&format!("m-{index}"), 100))
        .collect();
    schedule(&fixture, 10, messages)?;
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: TIMER_SCAN_LIMIT as u32
        }
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, TIMER_SCAN_LIMIT + 1)?
            .len(),
        TIMER_SCAN_LIMIT
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
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

fn scheduled_sessions_are_available_only_after_activation<P: StoreProvider>(
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
    let session = SessionId::new("cart")?;
    schedule(
        &fixture,
        10,
        vec![ScheduledMessage {
            session_id: Some(session.clone()),
            ..message("later", 100)
        }],
    )?;
    assert_eq!(
        fixture.at(
            20,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None
            }
        )?,
        CommandOutcome::SessionAccepted(None)
    );
    assert!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &session, 10)?
            .is_empty()
    );
    fixture.at(100, CommandKind::ActivateScheduled)?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        101,
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("activated message made its session available");
    };
    assert_eq!(accepted.session_id, session);
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &session,
            10
        )?,
        vec![SequenceNumber::new(2)]
    );
    Ok(())
}

fn scheduling_requires_session_ids_on_session_queues<P: StoreProvider>(
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
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        schedule(
            &fixture,
            10,
            vec![
                ScheduledMessage {
                    session_id: Some(SessionId::new("cart")?),
                    ..message("valid", 100)
                },
                message("invalid", 100),
            ],
        ),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn scheduling_refuses_session_ids_on_plain_queues<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let cart = SessionId::new("cart")?;
    let handles = schedule(
        &fixture,
        10,
        vec![
            message("unnamed", 100),
            ScheduledMessage {
                session_id: Some(cart.clone()),
                ..message("named", 100)
            },
        ],
    )?;
    assert_eq!(
        handles,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    assert!(receive(&fixture, 99)?.is_none());
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    assert_eq!(receive(&fixture, 101)?.expect("unnamed").session_id, None);
    assert_eq!(
        receive(&fixture, 102)?.expect("named").session_id,
        Some(cart)
    );
    Ok(())
}

fn scheduled_state_and_cancellation_survive_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let handles = schedule(
        &fixture,
        10,
        vec![message("cancel", 100), message("keep", 100)],
    )?;
    let fixture = fixture.restart()?;
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, handles[0])?
        .expect("recovered scheduled record");
    assert!(matches!(record.state, MessageState::Scheduled { .. }));
    fixture.at(
        20,
        CommandKind::CancelScheduled {
            sequences: vec![handles[0]],
        },
    )?;
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let fixture = fixture.restart()?;
    let delivery = receive(&fixture, 101)?.expect("activation recovered");
    assert_eq!(delivery.message_id, "keep");
    assert_eq!(delivery.sequence, SequenceNumber::new(3));
    assert_eq!(receive(&fixture, 102)?, None);
    Ok(())
}

fn idle_scheduling_commands_commit_nothing<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    schedule(&fixture, 10, Vec::new())?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::CancelScheduled {
                sequences: Vec::new()
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 0 }
    );
    assert_eq!(
        fixture.at(10, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn dead_letter_queues_refuse_scheduling<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let mut command = fixture.command(
        10,
        CommandKind::Schedule {
            messages: vec![message("blocked", 100)],
        },
    );
    command.entity = fixture.entity.dead_letter_queue()?;
    assert_eq!(
        fixture.machine.apply(&command),
        Err(BrokerError::DeadLetterQueueIsReserved)
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
    scheduled_messages_are_peekable_but_not_deliverable,
    activation_assigns_queue_positions_in_due_order,
    scheduling_a_batch_is_atomic,
    cancellation_removes_both_record_and_timer_entry,
    cancellation_rejects_a_stale_handle_atomically,
    lifetime_starts_at_activation_not_scheduling,
    past_schedules_become_active_immediately,
    activation_is_bounded_and_resumable,
    scheduled_sessions_are_available_only_after_activation,
    scheduling_requires_session_ids_on_session_queues,
    scheduling_refuses_session_ids_on_plain_queues,
    scheduled_state_and_cancellation_survive_restart,
    idle_scheduling_commands_commit_nothing,
    dead_letter_queues_refuse_scheduling,
}
