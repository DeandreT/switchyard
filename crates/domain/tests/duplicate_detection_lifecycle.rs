use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, MAX_MESSAGE_ID_CHARACTERS, MessageInput,
    QueueConfig, ReceiveMode, SequenceNumber, SessionId, TIMER_SCAN_LIMIT, Timestamp, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

const WINDOW: u64 = 20_000;

fn deduplicating_config() -> QueueConfig {
    QueueConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: WINDOW,
        ..QueueConfig::default()
    }
}

fn queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        deduplicating_config(),
    )?)
}

fn input(message_id: impl Into<String>, body: impl Into<Vec<u8>>) -> MessageInput {
    MessageInput {
        message_id: message_id.into(),
        body: body.into(),
        ..MessageInput::default()
    }
}

fn scheduled_input(
    message_id: impl Into<String>,
    body: impl Into<Vec<u8>>,
    scheduled_at: u64,
) -> MessageInput {
    MessageInput {
        scheduled_enqueue_at: Some(Timestamp::from_millis(scheduled_at)),
        ..input(message_id, body)
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    message: MessageInput,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(
        at,
        CommandKind::Send {
            message_id: message.message_id,
            body: message.body,
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: message.envelope,
        },
    )
}

fn send_batch<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    messages: Vec<MessageInput>,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(at, CommandKind::SendBatch { messages })
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    mode: ReceiveMode,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn duplicate_detection_can_be_disabled_and_empty_ids_are_unique<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert_eq!(
        send(&fixture, 1, input("same", b"first".to_vec()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    assert_eq!(
        send(&fixture, 2, input("same", b"second".to_vec()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );

    let enabled = fixture.machine.apply(&domain::Command::new(
        fixture.namespace.clone(),
        domain::EntityPath::new("enabled")?,
        Timestamp::from_millis(3),
        CommandKind::CreateQueue {
            config: deduplicating_config(),
        },
    ))?;
    assert_eq!(enabled, CommandOutcome::QueueCreated);
    let enabled_entity = domain::EntityPath::new("enabled")?;
    assert!(
        !fixture
            .machine
            .queue_config(&fixture.namespace, &enabled_entity.dead_letter_queue()?)?
            .expect("every queue has a dead-letter shadow")
            .requires_duplicate_detection
    );
    for sequence in 1..=2 {
        assert_eq!(
            fixture.machine.apply(&domain::Command::new(
                fixture.namespace.clone(),
                domain::EntityPath::new("enabled")?,
                Timestamp::from_millis(3 + sequence),
                CommandKind::Send {
                    message_id: String::new(),
                    body: vec![sequence as u8],
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
            ))?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(sequence)
            }
        );
    }
    assert_eq!(
        fixture.machine.ready_sequences(
            &fixture.namespace,
            &domain::EntityPath::new("enabled")?,
            8
        )?,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    Ok(())
}

fn first_copy_wins_and_ids_use_unicode_scalar_limits<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    assert_eq!(
        send(&fixture, 100, input("same", b"first".to_vec()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    assert_eq!(
        send(&fixture, 101, input("same", b"ignored".to_vec()))?,
        CommandOutcome::DuplicateSuppressed {
            sequence: SequenceNumber::new(2)
        }
    );
    assert_eq!(
        send(&fixture, 102, input("different", b"second".to_vec()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(3)
        }
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 8)?,
        vec![SequenceNumber::new(1), SequenceNumber::new(3)]
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, SequenceNumber::new(1))?
            .expect("the first copy is durable")
            .body,
        b"first".to_vec()
    );

    for exact_id in ["Case", "case", "\u{e9}", "e\u{301}"] {
        assert!(matches!(
            send(&fixture, 103, input(exact_id, Vec::new()))?,
            CommandOutcome::Sent { .. }
        ));
    }

    let maximum = "é".repeat(MAX_MESSAGE_ID_CHARACTERS);
    assert!(matches!(
        send(&fixture, 104, input(maximum, Vec::new()))?,
        CommandOutcome::Sent { .. }
    ));
    let too_long = "é".repeat(MAX_MESSAGE_ID_CHARACTERS + 1);
    assert_eq!(
        send(&fixture, 105, input(too_long, Vec::new())),
        Err(BrokerError::MessageIdTooLong {
            characters: MAX_MESSAGE_ID_CHARACTERS + 1,
            maximum: MAX_MESSAGE_ID_CHARACTERS,
        })
    );
    Ok(())
}

fn session_id_is_not_part_of_nonpartitioned_identity<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "sessions",
        QueueConfig {
            requires_session: true,
            ..deduplicating_config()
        },
    )?;
    let first_session = SessionId::new("one")?;
    let second_session = SessionId::new("two")?;
    assert!(matches!(
        send(
            &fixture,
            100,
            MessageInput {
                session_id: Some(first_session.clone()),
                ..input("same", Vec::new())
            }
        )?,
        CommandOutcome::Sent { .. }
    ));
    assert!(matches!(
        send(
            &fixture,
            101,
            MessageInput {
                session_id: Some(second_session.clone()),
                ..input("same", Vec::new())
            }
        )?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &first_session,
            8
        )?,
        vec![SequenceNumber::new(1)]
    );
    assert!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &second_session, 8)?
            .is_empty()
    );
    Ok(())
}

fn batches_filter_existing_and_intra_batch_duplicates_atomically<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 100, input("existing", Vec::new()))?;

    assert_eq!(
        send_batch(
            &fixture,
            101,
            vec![
                input("existing", b"ignored".to_vec()),
                input("new", b"first-new".to_vec()),
                input("new", b"ignored-new".to_vec()),
                input("other", b"other".to_vec()),
            ],
        )?,
        CommandOutcome::BatchSent {
            sequences: (2..=5).map(SequenceNumber::new).collect(),
            stored: 2,
        }
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 8)?,
        vec![
            SequenceNumber::new(1),
            SequenceNumber::new(3),
            SequenceNumber::new(5),
        ]
    );
    assert_eq!(
        send_batch(
            &fixture,
            102,
            vec![input("existing", Vec::new()), input("other", Vec::new())]
        )?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(6), SequenceNumber::new(7)],
            stored: 0,
        }
    );

    let overlong = "x".repeat(MAX_MESSAGE_ID_CHARACTERS + 1);
    assert!(matches!(
        send_batch(
            &fixture,
            103,
            vec![
                input("not-poisoned", Vec::new()),
                input(overlong, Vec::new())
            ]
        ),
        Err(BrokerError::MessageIdTooLong { .. })
    ));
    assert_eq!(
        send(&fixture, 104, input("not-poisoned", Vec::new()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(8)
        }
    );
    Ok(())
}

fn history_outlives_completion_dead_lettering_and_ttl<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;

    send(&fixture, 100, input("completed", Vec::new()))?;
    let completed = receive(&fixture, 101, ReceiveMode::PeekLock)?.expect("message to complete");
    fixture.at(
        102,
        CommandKind::Complete {
            sequence: completed.sequence,
            lock_token: completed.lock.expect("peek lock").token,
        },
    )?;
    assert!(matches!(
        send(&fixture, 103, input("completed", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));

    send(&fixture, 110, input("dead-lettered", Vec::new()))?;
    let dead_lettered =
        receive(&fixture, 111, ReceiveMode::PeekLock)?.expect("message to dead-letter");
    fixture.at(
        112,
        CommandKind::DeadLetter {
            sequence: dead_lettered.sequence,
            lock_token: dead_lettered.lock.expect("peek lock").token,
            reason: String::from("test"),
            description: String::new(),
            replacement_envelope: None,
        },
    )?;
    assert!(matches!(
        send(&fixture, 113, input("dead-lettered", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));

    send(
        &fixture,
        120,
        MessageInput {
            time_to_live_millis: Some(1),
            ..input("expired", Vec::new())
        },
    )?;
    assert_eq!(
        fixture.at(121, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );
    assert!(matches!(
        send(&fixture, 122, input("expired", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    Ok(())
}

fn scheduled_and_immediate_sends_share_history_and_cancellation_keeps_it<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let scheduled = send(
        &fixture,
        100,
        scheduled_input("scheduled-first", Vec::new(), 10_000),
    )?;
    let CommandOutcome::Sent {
        sequence: scheduled_sequence,
    } = scheduled
    else {
        panic!("the first scheduled copy must be stored")
    };
    assert!(matches!(
        send(&fixture, 101, input("scheduled-first", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));

    assert!(matches!(
        send(&fixture, 102, input("immediate-first", Vec::new()))?,
        CommandOutcome::Sent { .. }
    ));
    assert!(matches!(
        send(
            &fixture,
            103,
            scheduled_input("immediate-first", Vec::new(), 10_000)
        )?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));

    assert_eq!(
        fixture.at(
            104,
            CommandKind::CancelScheduled {
                sequences: vec![scheduled_sequence]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert!(matches!(
        send(&fixture, 105, input("scheduled-first", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    Ok(())
}

fn activation_does_not_refresh_history<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(
        &fixture,
        100,
        scheduled_input("scheduled", Vec::new(), 1_000),
    )?;
    assert_eq!(
        fixture.at(1_000, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );

    // The original send's deadline is 20_100. If activation refreshed the
    // history, this copy would still be suppressed until 21_000.
    assert!(matches!(
        send(&fixture, 20_100, input("scheduled", Vec::new()))?,
        CommandOutcome::Sent { .. }
    ));
    Ok(())
}

fn duplicate_hits_do_not_extend_and_stale_history_is_replaced_before_sweep<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 100, input("reusable", Vec::new()))?;
    assert!(matches!(
        send(&fixture, 20_000, input("reusable", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    assert!(matches!(
        send(&fixture, 20_100, input("reusable", Vec::new()))?,
        CommandOutcome::Sent { .. }
    ));
    assert_eq!(
        fixture.machine.duplicate_history_deadline(
            &fixture.namespace,
            &fixture.entity,
            "reusable"
        )?,
        Some(Timestamp::from_millis(40_100))
    );
    Ok(())
}

fn cleanup_is_bounded_and_cannot_erase_a_new_generation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    let messages = (0..=TIMER_SCAN_LIMIT)
        .map(|index| input(format!("message-{index}"), Vec::new()))
        .collect();
    assert!(matches!(
        send_batch(&fixture, 100, messages)?,
        CommandOutcome::BatchSent { stored, .. } if stored as usize == TIMER_SCAN_LIMIT + 1
    ));
    assert_eq!(
        fixture.at(20_100, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired {
            removed: TIMER_SCAN_LIMIT as u32
        }
    );
    assert_eq!(
        fixture.at(20_100, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 1 }
    );

    send(&fixture, 20_101, input("generation", Vec::new()))?;
    let current_deadline = Timestamp::from_millis(40_101);
    let stale_deadline = Timestamp::from_millis(20_101);
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::duplicate_expiry(
            &fixture.namespace,
            &fixture.entity,
            stale_deadline,
            "generation",
        ),
        Vec::new(),
    ))?;
    assert_eq!(
        fixture.at(20_101, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 1 }
    );
    assert_eq!(
        fixture.machine.duplicate_history_deadline(
            &fixture.namespace,
            &fixture.entity,
            "generation"
        )?,
        Some(current_deadline)
    );
    assert!(matches!(
        send(&fixture, 20_102, input("generation", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    Ok(())
}

fn history_and_sequence_slots_survive_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    send(&fixture, 100, input("durable", Vec::new()))?;
    let fixture = fixture.restart()?;
    assert_eq!(
        send(&fixture, 101, input("durable", Vec::new()))?,
        CommandOutcome::DuplicateSuppressed {
            sequence: SequenceNumber::new(2)
        }
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        send(&fixture, 102, input("other", Vec::new()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(3)
        }
    );
    assert_eq!(
        fixture.machine.duplicate_history_deadline(
            &fixture.namespace,
            &fixture.entity,
            "durable"
        )?,
        Some(Timestamp::from_millis(20_100))
    );
    Ok(())
}

fn malformed_history_values_fail_without_mutation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = queue(provider)?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::duplicate_id(&fixture.namespace, &fixture.entity, "corrupt"),
        vec![99, 0],
    ))?;
    assert!(matches!(
        send(&fixture, 100, input("corrupt", Vec::new())),
        Err(BrokerError::Codec(_))
    ));
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 1)?
            .is_empty()
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
    duplicate_detection_can_be_disabled_and_empty_ids_are_unique,
    first_copy_wins_and_ids_use_unicode_scalar_limits,
    session_id_is_not_part_of_nonpartitioned_identity,
    batches_filter_existing_and_intra_batch_duplicates_atomically,
    history_outlives_completion_dead_lettering_and_ttl,
    scheduled_and_immediate_sends_share_history_and_cancellation_keeps_it,
    activation_does_not_refresh_history,
    duplicate_hits_do_not_extend_and_stale_history_is_replaced_before_sweep,
    cleanup_is_bounded_and_cannot_erase_a_new_generation,
    history_and_sequence_slots_survive_restart,
    malformed_history_values_fail_without_mutation,
}
