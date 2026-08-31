use std::error::Error;

use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, MessageEnvelope, MessageInput, QueueConfig,
    ReceiveMode, SequenceNumber, SessionId, Timestamp,
};
use testkit::{QueueFixture, StoreProvider};

fn input(message_id: &str, body: &[u8]) -> MessageInput {
    MessageInput {
        message_id: message_id.to_owned(),
        body: body.to_vec(),
        ..MessageInput::default()
    }
}

fn send_batch<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    messages: Vec<MessageInput>,
) -> Result<Vec<SequenceNumber>, BrokerError> {
    match fixture.at(at, CommandKind::SendBatch { messages })? {
        CommandOutcome::BatchSent { sequences } => Ok(sequences),
        other => panic!("expected a batch send outcome, got {other:?}"),
    }
}

fn send_one<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    message_id: &str,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Send {
            message_id: message_id.to_owned(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            envelope: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send outcome, got {other:?}"),
    }
}

fn receive_and_delete<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        at,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected a receive outcome, got {other:?}"),
    }
}

fn a_batch_commits_messages_with_consecutive_sequences<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let sequences = send_batch(
        &fixture,
        10,
        vec![
            input("first", b"one"),
            input("second", b"two"),
            input("third", b"three"),
        ],
    )?;

    assert_eq!(
        sequences,
        vec![
            SequenceNumber::new(1),
            SequenceNumber::new(2),
            SequenceNumber::new(3),
        ]
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        sequences
    );

    for expected in ["first", "second", "third"] {
        let delivery = receive_and_delete(&fixture, 20)?.expect("the batch message is ready");
        assert_eq!(delivery.message_id, expected);
    }
    assert_eq!(receive_and_delete(&fixture, 20)?, None);
    Ok(())
}

fn one_invalid_child_rejects_the_whole_batch<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 4,
            ..QueueConfig::default()
        },
    )?;

    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: vec![
                    input("valid-before", b"1234"),
                    input("too-large", b"12345"),
                    input("valid-after", b"ok"),
                ],
            },
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 5,
            maximum_bytes: 4,
        })
    );
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?
            .is_empty()
    );
    for sequence in 1..=3 {
        assert_eq!(
            fixture.machine.message(
                &fixture.namespace,
                &fixture.entity,
                SequenceNumber::new(sequence),
            )?,
            None
        );
    }
    assert_eq!(
        send_one(&fixture, 11, "after-rejection")?,
        SequenceNumber::new(1),
        "a rejected batch must not consume sequence numbers"
    );
    Ok(())
}

fn a_batch_preserves_ttl_sessions_and_envelopes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "sessions",
        QueueConfig {
            default_time_to_live_millis: Some(100),
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let session_id = SessionId::new("cart-1")?;
    let envelope = MessageEnvelope::new(vec![0, 0x53, 0x77, 0xa1, 3, b'a', b'm', b'q']);
    let first = MessageInput {
        message_id: String::from("first"),
        body: b"normalized".to_vec(),
        time_to_live_millis: Some(25),
        session_id: Some(session_id.clone()),
        envelope: Some(envelope.clone()),
    };
    let second = MessageInput {
        message_id: String::from("second"),
        body: b"second-body".to_vec(),
        time_to_live_millis: None,
        session_id: Some(session_id.clone()),
        envelope: None,
    };

    let sequences = send_batch(&fixture, 10, vec![first, second])?;
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &session_id,
            10,
        )?,
        sequences
    );
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?
            .is_empty(),
        "session messages must not enter the entity-wide ready index"
    );

    let first = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequences[0])?
        .expect("the first batch child was stored");
    assert_eq!(first.session_id, Some(session_id.clone()));
    assert_eq!(first.expires_at, Some(Timestamp::from_millis(35)));
    assert_eq!(first.envelope, Some(envelope));
    assert_eq!(first.body, b"normalized".to_vec());

    let second = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequences[1])?
        .expect("the second batch child was stored");
    assert_eq!(second.session_id, Some(session_id));
    assert_eq!(second.expires_at, Some(Timestamp::from_millis(110)));
    assert_eq!(second.envelope, None);
    assert_eq!(second.body, b"second-body".to_vec());

    assert_eq!(
        fixture.at(34, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 0 }
    );
    assert_eq!(
        fixture.at(35, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?
            .is_none(),
        "the explicit-TTL child must be present in the expiry index"
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[1])?
            .is_some(),
        "the default-TTL child must remain live"
    );
    Ok(())
}

fn a_batch_survives_a_restart_with_its_counter<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelope = MessageEnvelope::new(vec![0, 0x53, 0x77, 0x40]);
    let sequences = send_batch(
        &fixture,
        10,
        vec![
            MessageInput {
                envelope: Some(envelope.clone()),
                ..input("first", b"one")
            },
            input("second", b"two"),
        ],
    )?;

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        sequences
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequences[0])?
            .expect("the first message survived")
            .envelope,
        Some(envelope)
    );
    assert_eq!(
        send_one(&fixture, 20, "after-restart")?,
        SequenceNumber::new(3)
    );
    Ok(())
}

fn an_empty_batch_is_rejected<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: Vec::new(),
            },
        ),
        Err(BrokerError::EmptyMessageBatch)
    );
    assert_eq!(send_one(&fixture, 11, "first")?, SequenceNumber::new(1));
    Ok(())
}

fn a_session_batch_cannot_mix_sessions<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let first_session = SessionId::new("cart-1")?;
    let second_session = SessionId::new("cart-2")?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: vec![
                    MessageInput {
                        session_id: Some(first_session.clone()),
                        ..input("first", b"one")
                    },
                    MessageInput {
                        session_id: Some(second_session),
                        ..input("second", b"two")
                    },
                ],
            },
        ),
        Err(BrokerError::MessageBatchSessionMismatch)
    );
    assert!(
        fixture
            .machine
            .session_ready_sequences(&fixture.namespace, &fixture.entity, &first_session, 10,)?
            .is_empty(),
        "a mixed-session batch must commit no child"
    );
    assert_eq!(
        send_batch(
            &fixture,
            11,
            vec![MessageInput {
                session_id: Some(first_session),
                ..input("after-rejection", b"one")
            }],
        )?,
        vec![SequenceNumber::new(1)],
        "a mixed-session rejection must consume no sequence numbers"
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
    a_batch_commits_messages_with_consecutive_sequences,
    one_invalid_child_rejects_the_whole_batch,
    a_batch_preserves_ttl_sessions_and_envelopes,
    a_batch_survives_a_restart_with_its_counter,
    an_empty_batch_is_rejected,
    a_session_batch_cannot_mix_sessions,
}
