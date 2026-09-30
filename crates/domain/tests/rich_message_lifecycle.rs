//! Typed producer content survives storage and every message state transition.

use std::{collections::BTreeMap, error::Error};

use domain::{
    AnnotationKey, BrokerError, CommandKind, CommandOutcome, Delivery, MAX_MESSAGE_VALUE_DEPTH,
    MAX_MESSAGE_VALUE_ITEMS, MessageBody, MessageDescriptor, MessageEnvelope, MessageHeader,
    MessageIdentifier, MessageProperties, MessageState, MessageStatus, MessageValue, QueueConfig,
    ReceiveMode, ScheduledEnvelope, SequenceNumber, SessionHold, SessionId, Timestamp, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn rich(body: MessageBody) -> MessageEnvelope {
    MessageEnvelope {
        header: Some(MessageHeader {
            durable: true,
            priority: 7,
            first_acquirer: true,
        }),
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::Uuid([1; 16])),
            correlation_id: Some(MessageIdentifier::Binary(vec![0, 1, 255])),
            user_id: Some(vec![1, 0, 2]),
            to: Some(String::from("target")),
            subject: Some(String::from("subject")),
            reply_to: Some(String::from("reply")),
            content_type: Some(String::from("application/example")),
            content_encoding: Some(String::new()),
            reply_to_group_id: Some(String::from("response-session")),
            creation_time: Some(123),
            absolute_expiry_time: Some(456),
            group_sequence: Some(42),
        },
        application_properties: BTreeMap::from([
            (String::from("empty"), MessageValue::String(String::new())),
            (
                String::from("signed-zero"),
                MessageValue::Float(0x8000_0000),
            ),
            (
                String::from("nan"),
                MessageValue::Double(0x7ff8_0000_0000_0001),
            ),
        ]),
        message_annotations: BTreeMap::from([
            (
                AnnotationKey::Symbol(String::from("nested")),
                MessageValue::Map(vec![
                    (
                        MessageValue::Uint(9),
                        MessageValue::Array(vec![MessageValue::Long(-1), MessageValue::Long(2)]),
                    ),
                    (
                        MessageValue::Symbol(String::from("named")),
                        MessageValue::Described {
                            descriptor: MessageDescriptor::Name(String::from("urn:example:value")),
                            value: Box::new(MessageValue::Decimal128([3; 16])),
                        },
                    ),
                ]),
            ),
            (
                AnnotationKey::Symbol(String::from("custom-annotation")),
                MessageValue::Timestamp(-1),
            ),
            (AnnotationKey::Ulong(12), MessageValue::Binary(vec![0, 255])),
        ]),
        footer: BTreeMap::from([(AnnotationKey::Ulong(99), MessageValue::Uuid([4; 16]))]),
        body,
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
    envelope: MessageEnvelope,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: b"compatibility-view".to_vec(),
            time_to_live_millis: ttl,
            session_id,
            envelope: Box::new(envelope),
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected sent outcome, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    dead_letter: bool,
) -> Result<Option<Delivery>, Box<dyn Error>> {
    let mut command = fixture.command(
        millis,
        CommandKind::Receive {
            mode,
            lock_duration_millis: Some(5),
            session,
        },
    );
    if dead_letter {
        command.entity = fixture.entity.dead_letter_queue()?;
    }
    match fixture.machine.apply(&command)? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected receive outcome, got {other:?}"),
    }
}

fn peek<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> Result<Vec<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 32,
            session_id: None,
        },
    )? {
        CommandOutcome::Peeked(deliveries) => Ok(deliveries),
        other => panic!("expected peek outcome, got {other:?}"),
    }
}

fn every_body_kind_survives_peek_and_receive_and_delete<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelopes = [
        rich(MessageBody::Empty),
        rich(MessageBody::Data(vec![
            b"first".to_vec(),
            Vec::new(),
            b"last".to_vec(),
        ])),
        rich(MessageBody::Sequence(vec![
            vec![MessageValue::Bool(true), MessageValue::Int(-3)],
            Vec::new(),
        ])),
        rich(MessageBody::Value(MessageValue::Described {
            descriptor: MessageDescriptor::Code(512),
            value: Box::new(MessageValue::Map(vec![(
                MessageValue::String(String::from("key")),
                MessageValue::List(vec![MessageValue::Null, MessageValue::Char('x')]),
            )])),
        })),
    ];
    for (index, envelope) in envelopes.iter().enumerate() {
        send(
            &fixture,
            10 + index as u64,
            &format!("body-{index}"),
            None,
            None,
            envelope.clone(),
        )?;
    }
    let deliveries = peek(&fixture, 20)?;
    assert_eq!(deliveries.len(), envelopes.len());
    for (delivery, expected) in deliveries.iter().zip(&envelopes) {
        assert_eq!(delivery.envelope.as_deref(), Some(expected));
        assert_eq!(delivery.expires_at, None);
        assert_eq!(delivery.time_to_live_millis, None);
        assert_eq!(delivery.lock, None);
    }
    let fixture = fixture.restart()?;
    for (index, expected) in envelopes.iter().enumerate() {
        let delivery = receive(
            &fixture,
            30 + index as u64,
            ReceiveMode::ReceiveAndDelete,
            None,
            false,
        )?
        .expect("a ready message");
        assert_eq!(delivery.envelope.as_deref(), Some(expected));
        assert_eq!(delivery.body, b"compatibility-view");
        assert_eq!(delivery.lock, None);
    }
    assert!(peek(&fixture, 40)?.is_empty());
    Ok(())
}

fn content_survives_abandon_defer_and_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelope = rich(MessageBody::Value(MessageValue::String(String::from(
        "value-body",
    ))));
    let sequence = send(
        &fixture,
        10,
        "state-changes",
        Some(1_000),
        None,
        envelope.clone(),
    )?;
    let first = receive(&fixture, 20, ReceiveMode::PeekLock, None, false)?.expect("ready message");
    assert_eq!(first.envelope.as_deref(), Some(&envelope));
    assert_eq!(first.time_to_live_millis, Some(1_000));
    assert_eq!(first.expires_at, Some(Timestamp::from_millis(1_010)));
    fixture.at(
        21,
        CommandKind::Abandon {
            sequence,
            lock_token: first.lock.expect("a lock").token,
        },
    )?;
    let second =
        receive(&fixture, 22, ReceiveMode::PeekLock, None, false)?.expect("abandoned message");
    assert_eq!(second.envelope.as_deref(), Some(&envelope));
    assert_eq!(second.delivery_count, 2);
    fixture.at(
        23,
        CommandKind::Defer {
            sequence,
            lock_token: second.lock.expect("a lock").token,
        },
    )?;
    let fixture = fixture.restart()?;
    let browsed = peek(&fixture, 24)?;
    assert_eq!(browsed[0].status, MessageStatus::Deferred);
    assert_eq!(browsed[0].envelope.as_deref(), Some(&envelope));
    assert_eq!(browsed[0].expires_at, first.expires_at);
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        25,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
    )?
    else {
        panic!("expected deferred receive");
    };
    let delivery = &deliveries[0];
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    assert_eq!(delivery.time_to_live_millis, Some(1_000));
    assert_eq!(delivery.delivery_count, 3);
    fixture.at(
        26,
        CommandKind::Complete {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
        },
    )?;
    assert!(peek(&fixture, 27)?.is_empty());
    Ok(())
}

fn dead_lettering_preserves_content_but_clears_authoritative_session_and_lifetime<
    P: StoreProvider,
>(
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
    let session_id = SessionId::new("cart")?;
    let envelope = rich(MessageBody::Data(vec![b"payload".to_vec()]));
    let sequence = send(
        &fixture,
        10,
        "dead-letter",
        Some(1_000),
        Some(session_id.clone()),
        envelope.clone(),
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        20,
        CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("expected accepted session");
    };
    let delivery = receive(
        &fixture,
        21,
        ReceiveMode::PeekLock,
        Some(accepted.hold()),
        false,
    )?
    .expect("session message");
    fixture.at(
        22,
        CommandKind::DeadLetter {
            sequence,
            lock_token: delivery.lock.expect("a lock").token,
            reason: String::from("custom"),
            description: String::from("preserve producer content"),
        },
    )?;
    let fixture = fixture.restart()?;
    let delivery =
        receive(&fixture, 23, ReceiveMode::ReceiveAndDelete, None, true)?.expect("dead letter");
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    assert_eq!(
        delivery
            .envelope
            .as_ref()
            .expect("typed content")
            .properties
            .creation_time,
        Some(123)
    );
    assert_eq!(delivery.session_id, None);
    assert_eq!(delivery.expires_at, None);
    assert_eq!(delivery.time_to_live_millis, None);
    assert_eq!(
        delivery
            .dead_letter
            .expect("reason retained")
            .reason
            .as_str(),
        "custom"
    );
    Ok(())
}

fn lock_and_message_expiry_preserve_content<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let envelope = rich(MessageBody::Sequence(vec![vec![MessageValue::Ulong(7)]]));
    send(&fixture, 10, "timers", Some(100), None, envelope.clone())?;
    receive(&fixture, 11, ReceiveMode::PeekLock, None, false)?.expect("ready message");
    assert_eq!(
        fixture.at(16, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    let delivery =
        receive(&fixture, 17, ReceiveMode::PeekLock, None, false)?.expect("expired lock released");
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    assert_eq!(delivery.delivery_count, 2);
    assert_eq!(
        fixture.at(110, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            dropped: 0,
            processed: 0,
        }
    );
    let delivery = receive(&fixture, 111, ReceiveMode::ReceiveAndDelete, None, true)?
        .expect("expired message dead-lettered");
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    assert_eq!(delivery.expires_at, None);
    Ok(())
}

fn scheduled_content_and_effective_lifetime_survive_activation_and_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            default_time_to_live_millis: Some(50),
            ..QueueConfig::default()
        },
    )?;
    let envelope = rich(MessageBody::Value(MessageValue::Binary(vec![0, 1, 2])));
    let CommandOutcome::Scheduled { sequences } = fixture.at(
        10,
        CommandKind::ScheduleEnvelopes {
            messages: vec![ScheduledEnvelope {
                message_id: String::from("later"),
                body: b"view".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
                envelope: envelope.clone(),
            }],
        },
    )?
    else {
        panic!("expected scheduling outcome");
    };
    let handle = sequences[0];
    let fixture = fixture.restart()?;
    let browsed = peek(&fixture, 20)?;
    assert_eq!(browsed[0].status, MessageStatus::Scheduled);
    assert_eq!(browsed[0].envelope.as_deref(), Some(&envelope));
    assert_eq!(browsed[0].expires_at, None);
    assert_eq!(browsed[0].time_to_live_millis, Some(50));
    assert_eq!(
        fixture.at(110, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let fixture = fixture.restart()?;
    let delivery = receive(&fixture, 120, ReceiveMode::ReceiveAndDelete, None, false)?
        .expect("activated message");
    assert_ne!(delivery.sequence, handle);
    assert_eq!(delivery.enqueued_at, Timestamp::from_millis(110));
    assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(160)));
    assert_eq!(delivery.time_to_live_millis, Some(50));
    assert_eq!(
        delivery.scheduled_enqueue_time,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    Ok(())
}

fn retained_metadata_is_validated_before_duplicate_drop<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 128,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let small = MessageEnvelope::default();
    send(&fixture, 10, "same", None, None, small.clone())?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    let mut oversized = small;
    oversized
        .application_properties
        .insert(String::from("large"), MessageValue::Binary(vec![0; 200]));
    let expected_size = oversized.content_size() + 5 + "same".len();
    assert_eq!(
        send(&fixture, 11, "same", None, None, oversized),
        Err(BrokerError::MessageTooLarge {
            body_bytes: expected_size,
            maximum_bytes: 128
        })
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn rich_schedule_validation_is_atomic<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 128,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let mut oversized = MessageEnvelope::default();
    oversized.footer.insert(
        AnnotationKey::Symbol(String::from("large")),
        MessageValue::Binary(vec![0; 200]),
    );
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    let make = |id: &str, envelope| ScheduledEnvelope {
        message_id: id.to_owned(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        enqueue_at: Timestamp::from_millis(100),
        envelope,
    };
    let result = fixture.at(
        10,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                make("first", MessageEnvelope::default()),
                make("second", oversized),
            ],
        },
    );
    assert!(matches!(result, Err(BrokerError::MessageTooLarge { .. })));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    let sequence = send(
        &fixture,
        11,
        "first",
        None,
        None,
        MessageEnvelope::default(),
    )?;
    assert_eq!(sequence, SequenceNumber::new(1));
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .is_some()
    );
    Ok(())
}

fn compatibility_body_is_not_double_counted<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let envelope = MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(String::from("same"))),
            ..MessageProperties::default()
        },
        body: MessageBody::Data(vec![vec![0; 100]]),
        ..MessageEnvelope::default()
    };
    let maximum = envelope.content_size();
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: maximum,
            ..QueueConfig::default()
        },
    )?;
    fixture.at(
        10,
        CommandKind::SendEnvelope {
            message_id: String::from("same"),
            body: vec![0; 100],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(envelope.clone()),
        },
    )?;
    let delivery = receive(&fixture, 11, ReceiveMode::ReceiveAndDelete, None, false)?
        .expect("content fits once");
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    assert!(matches!(
        fixture.at(
            12,
            CommandKind::SendEnvelope {
                message_id: String::from("same"),
                body: vec![0; maximum + 1],
                time_to_live_millis: None,
                session_id: None,
                envelope: Box::new(envelope)
            }
        ),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    Ok(())
}

fn legacy_messages_still_deliver_without_an_envelope<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("legacy"),
            body: b"bytes".to_vec(),
            time_to_live_millis: Some(100),
            session_id: None,
        },
    )?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::ReceiveAndDelete, None, false)?.expect("legacy message");
    assert_eq!(delivery.envelope, None);
    assert_eq!(delivery.body, b"bytes");
    assert_eq!(delivery.expires_at, Some(Timestamp::from_millis(110)));
    assert_eq!(delivery.time_to_live_millis, Some(100));
    Ok(())
}

fn genuine_version_6_messages_read_and_transition_through_the_machine<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("old"),
            body: b"old bytes".to_vec(),
            time_to_live_millis: Some(100),
            session_id: None,
        },
    )?;
    let sequence = SequenceNumber::new(1);
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("message");
    let mut old = vec![codec::VALUE_FORMAT_V6];
    old.extend(postcard::to_stdvec(&(
        record.sequence,
        &record.message_id,
        &record.body,
        record.enqueued_at,
        record.expires_at,
        record.delivery_count,
        &record.state,
        &record.session_id,
        &record.dead_letter,
        record.scheduled_enqueue_time,
    ))?);
    let mut batch = WriteBatch::default();
    batch.push_put(
        keys::message(&fixture.namespace, &fixture.entity, sequence),
        old,
    );
    fixture.machine.store().apply(batch)?;
    let fixture = fixture.restart()?;
    assert_eq!(peek(&fixture, 11)?[0].envelope, None);
    let delivery =
        receive(&fixture, 12, ReceiveMode::PeekLock, None, false)?.expect("migrated message");
    assert_eq!(delivery.envelope, None);
    assert_eq!(delivery.time_to_live_millis, Some(100));
    let migrated = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
        .expect("locked record");
    assert!(matches!(migrated.state, MessageState::Locked { .. }));
    let stored = fixture
        .machine
        .store()
        .get(&keys::message(
            &fixture.namespace,
            &fixture.entity,
            sequence,
        ))?
        .expect("stored record");
    assert_eq!(stored[0], codec::ACTIVE_VALUE_FORMAT);
    Ok(())
}

fn typed_string_identifiers_cannot_bypass_length_validation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, id) in ["x".repeat(128), "\u{1f600}".repeat(64)]
        .into_iter()
        .enumerate()
    {
        let envelope = MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id)),
                ..MessageProperties::default()
            },
            ..MessageEnvelope::default()
        };
        send(
            &fixture,
            10 + index as u64,
            "short-alias",
            None,
            None,
            envelope,
        )?;
    }
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    for id in ["x".repeat(129), "\u{1f600}".repeat(65)] {
        let length = id.encode_utf16().count();
        let envelope = MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id)),
                ..MessageProperties::default()
            },
            ..MessageEnvelope::default()
        };
        assert_eq!(
            send(&fixture, 20, "short-alias", None, None, envelope.clone()),
            Err(BrokerError::MessageIdTooLong {
                length,
                maximum: 128
            })
        );
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
        let make = |id: &str, envelope| ScheduledEnvelope {
            message_id: id.to_owned(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            enqueue_at: Timestamp::from_millis(100),
            envelope,
        };
        assert_eq!(
            fixture.at(
                20,
                CommandKind::ScheduleEnvelopes {
                    messages: vec![
                        make("first", MessageEnvelope::default()),
                        make("alias", envelope)
                    ]
                }
            ),
            Err(BrokerError::MessageIdTooLong {
                length,
                maximum: 128
            })
        );
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
    }
    Ok(())
}

fn unsupported_compound_content_is_rejected_atomically_before_duplicates<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    send(
        &fixture,
        10,
        "existing",
        None,
        None,
        MessageEnvelope::default(),
    )?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    let body = |value| MessageEnvelope {
        body: MessageBody::Value(value),
        ..MessageEnvelope::default()
    };
    let application = |value| MessageEnvelope {
        application_properties: BTreeMap::from([(String::from("invalid"), value)]),
        ..MessageEnvelope::default()
    };
    let described = |descriptor, value| MessageValue::Described {
        descriptor: MessageDescriptor::Code(descriptor),
        value: Box::new(value),
    };
    let envelopes = vec![
        body(
            (0..MAX_MESSAGE_VALUE_DEPTH + 1)
                .fold(MessageValue::Null, |value, _| described(1, value)),
        ),
        body(MessageValue::Array(vec![
            MessageValue::Null;
            MAX_MESSAGE_VALUE_ITEMS
        ])),
        body(MessageValue::Symbol(String::from("\u{e9}"))),
        body(MessageValue::Described {
            descriptor: MessageDescriptor::Name(String::from("\u{e9}")),
            value: Box::new(MessageValue::Null),
        }),
        MessageEnvelope {
            message_annotations: BTreeMap::from([(
                AnnotationKey::Symbol(String::from("\u{e9}")),
                MessageValue::Null,
            )]),
            ..MessageEnvelope::default()
        },
        MessageEnvelope {
            footer: BTreeMap::from([(
                AnnotationKey::Symbol(String::from("\u{e9}")),
                MessageValue::Null,
            )]),
            ..MessageEnvelope::default()
        },
        MessageEnvelope {
            properties: MessageProperties {
                content_type: Some(String::from("\u{e9}")),
                ..MessageProperties::default()
            },
            ..MessageEnvelope::default()
        },
        MessageEnvelope {
            properties: MessageProperties {
                content_encoding: Some(String::from("\u{e9}")),
                ..MessageProperties::default()
            },
            ..MessageEnvelope::default()
        },
        body(MessageValue::Array(Vec::new())),
        body(MessageValue::Array(vec![
            MessageValue::Uint(1),
            MessageValue::Ulong(2),
        ])),
        body(MessageValue::Array(vec![
            described(5, MessageValue::Long(1)),
            described(6, MessageValue::Long(2)),
        ])),
        body(MessageValue::Array(vec![
            described(5, MessageValue::Long(1)),
            described(5, MessageValue::Int(2)),
        ])),
        body(MessageValue::Map(vec![
            (
                MessageValue::String(String::from("key")),
                MessageValue::Int(1),
            ),
            (
                MessageValue::String(String::from("key")),
                MessageValue::Int(2),
            ),
        ])),
        body(MessageValue::Map(vec![
            (MessageValue::Float(0), MessageValue::Int(1)),
            (MessageValue::Float(0x8000_0000), MessageValue::Int(2)),
        ])),
        body(MessageValue::Map(vec![
            (
                MessageValue::Double(0x7ff8_0000_0000_0001),
                MessageValue::Int(1),
            ),
            (
                MessageValue::Double(0xfff8_0000_0000_0002),
                MessageValue::Int(2),
            ),
        ])),
        body(MessageValue::Map(vec![
            (
                MessageValue::List(vec![MessageValue::Double(0)]),
                MessageValue::Int(1),
            ),
            (
                MessageValue::List(vec![MessageValue::Double(0x8000_0000_0000_0000)]),
                MessageValue::Int(2),
            ),
        ])),
        application(MessageValue::List(Vec::new())),
        application(MessageValue::Map(Vec::new())),
        application(MessageValue::Array(vec![MessageValue::Int(1)])),
        application(described(8, MessageValue::List(vec![MessageValue::Int(1)]))),
    ];
    for envelope in envelopes {
        assert!(matches!(
            send(&fixture, 11, "existing", None, None, envelope.clone()),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
        let make = |id: &str, envelope| ScheduledEnvelope {
            message_id: id.to_owned(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            enqueue_at: Timestamp::from_millis(100),
            envelope,
        };
        assert!(matches!(
            fixture.at(
                12,
                CommandKind::ScheduleEnvelopes {
                    messages: vec![
                        make("new", MessageEnvelope::default()),
                        make("existing", envelope)
                    ]
                }
            ),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
    }
    Ok(())
}

fn homogeneous_arrays_and_scalar_described_application_properties_are_preserved<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let described = |value| MessageValue::Described {
        descriptor: MessageDescriptor::Name(String::from("com.microsoft:timespan")),
        value: Box::new(MessageValue::Long(value)),
    };
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([(String::from("duration"), described(42))]),
        body: MessageBody::Value(MessageValue::List(vec![
            MessageValue::Array(vec![MessageValue::Uint(0), MessageValue::Uint(256)]),
            MessageValue::Array(vec![described(3), described(7)]),
            MessageValue::Map(vec![(
                MessageValue::Float(0x8000_0000),
                MessageValue::Double(0x7ff8_0000_0000_0001),
            )]),
        ])),
        ..MessageEnvelope::default()
    };
    send(&fixture, 10, "valid", None, None, envelope.clone())?;
    let delivery =
        receive(&fixture, 11, ReceiveMode::ReceiveAndDelete, None, false)?.expect("valid content");
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    every_body_kind_survives_peek_and_receive_and_delete,
    content_survives_abandon_defer_and_restart,
    dead_lettering_preserves_content_but_clears_authoritative_session_and_lifetime,
    lock_and_message_expiry_preserve_content,
    scheduled_content_and_effective_lifetime_survive_activation_and_restart,
    retained_metadata_is_validated_before_duplicate_drop,
    rich_schedule_validation_is_atomic,
    compatibility_body_is_not_double_counted,
    legacy_messages_still_deliver_without_an_envelope,
    genuine_version_6_messages_read_and_transition_through_the_machine,
    typed_string_identifiers_cannot_bypass_length_validation,
    unsupported_compound_content_is_rejected_atomically_before_duplicates,
    homogeneous_arrays_and_scalar_described_application_properties_are_preserved,
}
