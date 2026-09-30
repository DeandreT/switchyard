//! Settlement property updates commit with their message-state transition.

use std::{collections::BTreeMap, error::Error};

use domain::{
    AnnotationKey, BROKER_HEADER_RESERVE_BYTES, BrokerError, CommandKind, CommandOutcome,
    DeadLetterReason, Delivery, LockToken, MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_PROPERTY_BYTES,
    MAX_MESSAGE_VALUE_ITEMS, MessageBody, MessageDescriptor, MessageEnvelope, MessageHeader,
    MessageIdentifier, MessageProperties, MessageState, MessageValue, QueueConfig, ReceiveMode,
    SequenceNumber, SettlementDisposition, keys,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

type Updates = BTreeMap<String, MessageValue>;

fn rich() -> MessageEnvelope {
    MessageEnvelope {
        header: Some(MessageHeader {
            durable: true,
            priority: 7,
            first_acquirer: false,
        }),
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::Uuid([1; 16])),
            correlation_id: Some(MessageIdentifier::Binary(vec![2, 3])),
            subject: Some(String::from("producer-subject")),
            creation_time: Some(-123),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([
            (
                String::from("status"),
                MessageValue::String(String::from("old")),
            ),
            (String::from("keep"), MessageValue::Uint(42)),
            (
                String::from("nullable"),
                MessageValue::String(String::from("old")),
            ),
        ]),
        message_annotations: BTreeMap::from([(
            AnnotationKey::Symbol(String::from("producer")),
            MessageValue::List(vec![MessageValue::Double(0x7ff8_0000_0000_0001)]),
        )]),
        footer: BTreeMap::from([(AnnotationKey::Ulong(7), MessageValue::Binary(vec![4, 5]))]),
        body: MessageBody::Sequence(vec![
            vec![
                MessageValue::Float(0x8000_0000),
                MessageValue::String(String::from("body")),
            ],
            Vec::new(),
        ]),
    }
}

fn updates() -> Updates {
    BTreeMap::from([
        (
            String::from("status"),
            MessageValue::String(String::from("new")),
        ),
        (String::from("nullable"), MessageValue::Null),
        (String::from("nan"), MessageValue::Float(0x7fc0_1234)),
        (
            String::from("duration"),
            MessageValue::Described {
                descriptor: MessageDescriptor::Name(String::from("com.microsoft:timespan")),
                value: Box::new(MessageValue::Long(-7)),
            },
        ),
    ])
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    ttl: Option<u64>,
    envelope: MessageEnvelope,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::SendEnvelope {
            message_id: String::from("id"),
            body: b"compatibility-body".to_vec(),
            time_to_live_millis: ttl,
            session_id: None,
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
    dead_letter: bool,
) -> Result<Delivery, Box<dyn Error>> {
    let mut command = fixture.command(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: None,
        },
    );
    if dead_letter {
        command.entity = fixture.entity.dead_letter_queue()?;
    }
    match fixture.machine.apply(&command)? {
        CommandOutcome::Received(Some(delivery)) => Ok(delivery),
        other => panic!("expected delivery, got {other:?}"),
    }
}

fn settle<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    delivery: &Delivery,
    disposition: SettlementDisposition,
    properties_to_modify: Updates,
) -> Result<CommandOutcome, BrokerError> {
    fixture.at(
        millis,
        CommandKind::Settle {
            sequence: delivery.sequence,
            lock_token: delivery.lock.as_ref().expect("peek-lock delivery").token,
            disposition,
            properties_to_modify,
        },
    )
}

fn rich_updates_survive_abandon_defer_and_restart_without_changing_other_content<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let mut expected = rich();
    send(&fixture, 10, None, expected.clone())?;
    let first = receive(&fixture, 11, false)?;
    let patch = updates();
    expected.application_properties.extend(patch.clone());
    assert_eq!(
        settle(&fixture, 12, &first, SettlementDisposition::Abandon, patch)?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    let fixture = fixture.restart()?;
    let second = receive(&fixture, 13, false)?;
    assert_eq!(second.envelope.as_deref(), Some(&expected));
    assert_eq!(second.body, b"compatibility-body");
    assert_eq!(second.delivery_count, 2);
    let patch = BTreeMap::from([(String::from("later"), MessageValue::Timestamp(-99))]);
    expected.application_properties.extend(patch.clone());
    assert_eq!(
        settle(&fixture, 14, &second, SettlementDisposition::Defer, patch)?,
        CommandOutcome::Deferred
    );
    let fixture = fixture.restart()?;
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, second.sequence)?
        .expect("deferred record");
    assert_eq!(record.state, MessageState::Deferred);
    assert_eq!(record.envelope.as_deref(), Some(&expected));
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        15,
        CommandKind::ReceiveDeferred {
            sequences: vec![second.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session_id: None,
        },
    )?
    else {
        panic!("expected deferred delivery");
    };
    assert_eq!(deliveries[0].envelope.as_deref(), Some(&expected));
    assert_eq!(
        settle(
            &fixture,
            16,
            &deliveries[0],
            SettlementDisposition::Complete,
            Updates::new()
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn legacy_payload_and_even_empty_identifier_are_materialized_only_for_nonempty_updates<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, id) in ["", "legacy-id"].into_iter().enumerate() {
        let start = 10 + index as u64 * 10;
        fixture.at(
            start,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: b"legacy-body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )?;
        let delivery = receive(&fixture, start + 1, false)?;
        assert_eq!(delivery.envelope, None);
        settle(
            &fixture,
            start + 2,
            &delivery,
            SettlementDisposition::Abandon,
            Updates::new(),
        )?;
        let delivery = receive(&fixture, start + 3, false)?;
        assert_eq!(delivery.envelope, None);
        settle(
            &fixture,
            start + 4,
            &delivery,
            SettlementDisposition::Defer,
            updates(),
        )?;
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
            .expect("converted legacy record");
        let envelope = record
            .envelope
            .as_deref()
            .expect("nonempty patch materializes content");
        assert_eq!(
            envelope.body,
            MessageBody::Data(vec![b"legacy-body".to_vec()])
        );
        assert_eq!(
            envelope.properties.message_id,
            Some(MessageIdentifier::String(id.to_owned()))
        );
        assert_eq!(envelope.application_properties, updates());
    }
    let fixture = fixture.restart()?;
    let CommandOutcome::Peeked(deliveries) = fixture.at(
        30,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 10,
            session_id: None,
        },
    )?
    else {
        panic!("expected peek results");
    };
    assert_eq!(deliveries.len(), 2);
    assert!(
        deliveries
            .iter()
            .all(|delivery| delivery.envelope.is_some())
    );
    Ok(())
}

fn lock_validation_precedes_invalid_property_validation_without_writes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, None, rich())?;
    let delivery = receive(&fixture, 11, false)?;
    let lock = delivery.lock.as_ref().expect("lock");
    let invalid = BTreeMap::from([(String::from("bad"), MessageValue::List(Vec::new()))]);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert_eq!(
        fixture.at(
            12,
            CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: LockToken::new(lock.token.as_u64() + 1),
                disposition: SettlementDisposition::Abandon,
                properties_to_modify: invalid.clone(),
            }
        ),
        Err(BrokerError::LockTokenMismatch {
            sequence: delivery.sequence
        })
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    assert_eq!(
        settle(
            &fixture,
            lock.locked_until.as_millis(),
            &delivery,
            SettlementDisposition::Defer,
            invalid
        ),
        Err(BrokerError::LockExpired {
            sequence: delivery.sequence,
            locked_until: lock.locked_until,
        })
    );
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn invalid_shapes_and_individual_property_growth_leave_every_settlement_unchanged<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, None, rich())?;
    let delivery = receive(&fixture, 11, false)?;
    for (index, disposition) in [
        SettlementDisposition::Complete,
        SettlementDisposition::Abandon,
        SettlementDisposition::Defer,
        SettlementDisposition::DeadLetter {
            reason: String::from("reason"),
            description: String::from("description"),
        },
    ]
    .into_iter()
    .enumerate()
    {
        for (offset, invalid) in [
            BTreeMap::from([(String::from("bad"), MessageValue::Map(Vec::new()))]),
            BTreeMap::from([(
                String::from("status"),
                MessageValue::Binary(vec![0; MAX_MESSAGE_PROPERTY_BYTES]),
            )]),
        ]
        .into_iter()
        .enumerate()
        {
            let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
            let result = settle(
                &fixture,
                12 + index as u64 * 2 + offset as u64,
                &delivery,
                disposition.clone(),
                invalid,
            );
            assert!(matches!(
                result,
                Err(BrokerError::InvalidMessageContent { .. }
                    | BrokerError::MessagePropertyTooLarge { .. })
            ));
            assert_eq!(
                fixture.machine.store().scan_prefix(&[], usize::MAX)?,
                before
            );
        }
    }
    assert_eq!(
        settle(
            &fixture,
            21,
            &delivery,
            SettlementDisposition::Complete,
            updates()
        )?,
        CommandOutcome::Completed
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
            .is_none()
    );
    Ok(())
}

fn header_envelope(header_bytes: usize) -> MessageEnvelope {
    let mut envelope = MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(String::from("id"))),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([
            (
                String::from("first"),
                MessageValue::Binary(vec![0; MAX_MESSAGE_PROPERTY_BYTES - 15]),
            ),
            (String::from("second"), MessageValue::Binary(Vec::new())),
        ]),
        ..MessageEnvelope::default()
    };
    let remainder = header_bytes - envelope.header_content_size();
    envelope.application_properties.insert(
        String::from("second"),
        MessageValue::Binary(vec![0; remainder]),
    );
    assert_eq!(envelope.header_content_size(), header_bytes);
    assert_eq!(envelope.validate(), Ok(()));
    envelope
}

fn merged_header_growth_includes_existing_values_and_broker_reserve<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, None, header_envelope(64_000))?;
    let delivery = receive(&fixture, 11, false)?;
    let projected = header_envelope(MAX_MESSAGE_HEADER_BYTES - BROKER_HEADER_RESERVE_BYTES + 1);
    let patch = BTreeMap::from([(
        String::from("second"),
        projected.application_properties["second"].clone(),
    )]);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        settle(
            &fixture,
            12,
            &delivery,
            SettlementDisposition::Abandon,
            patch
        ),
        Err(BrokerError::MessageHeaderTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );

    let patch = BTreeMap::from([(String::from("second"), MessageValue::Null)]);
    settle(
        &fixture,
        13,
        &delivery,
        SettlementDisposition::Abandon,
        patch,
    )?;
    let delivery = receive(&fixture, 14, false)?;
    assert_eq!(
        delivery
            .envelope
            .as_deref()
            .expect("rich content")
            .application_properties["second"],
        MessageValue::Null
    );
    Ok(())
}

fn merged_content_growth_cannot_bypass_the_queue_total_message_limit<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 2_048,
            ..QueueConfig::default()
        },
    )?;
    send(&fixture, 10, None, rich())?;
    let delivery = receive(&fixture, 11, false)?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        settle(
            &fixture,
            12,
            &delivery,
            SettlementDisposition::Defer,
            BTreeMap::from([(String::from("large"), MessageValue::Binary(vec![0; 3_000]))])
        ),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn updates_are_applied_before_automatic_ttl_and_delivery_limit_dead_lettering<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?;
    for (index, ttl, reason) in [
        (0_u64, Some(5), DeadLetterReason::TimeToLiveExpired),
        (1, None, DeadLetterReason::MaxDeliveryCountExceeded),
    ] {
        let start = 10 + index * 20;
        let mut expected = rich();
        send(&fixture, start, ttl, expected.clone())?;
        let delivery = receive(&fixture, start + 1, false)?;
        let patch = updates();
        expected.application_properties.extend(patch.clone());
        assert_eq!(
            settle(
                &fixture,
                start + 6,
                &delivery,
                SettlementDisposition::Abandon,
                patch
            )?,
            CommandOutcome::Abandoned {
                dead_lettered: true
            }
        );
        let dead = receive(&fixture, start + 7, true)?;
        assert_eq!(dead.envelope.as_deref(), Some(&expected));
        assert_eq!(
            dead.dead_letter.as_ref().expect("dead-letter info").reason,
            reason
        );
        assert_eq!(dead.expires_at, None);
        let mut complete = fixture.command(
            start + 8,
            CommandKind::Complete {
                sequence: dead.sequence,
                lock_token: dead.lock.expect("DLQ lock").token,
            },
        );
        complete.entity = fixture.entity.dead_letter_queue()?;
        fixture.machine.apply(&complete)?;
    }
    Ok(())
}

fn explicit_dead_lettering_preserves_updates_and_uses_authoritative_info<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let mut expected = rich();
    send(&fixture, 10, None, expected.clone())?;
    let delivery = receive(&fixture, 11, false)?;
    let patch = updates();
    expected.application_properties.extend(patch.clone());
    assert_eq!(
        settle(
            &fixture,
            12,
            &delivery,
            SettlementDisposition::DeadLetter {
                reason: String::from("application-reason"),
                description: String::from("application-description"),
            },
            patch
        )?,
        CommandOutcome::DeadLettered
    );
    let fixture = fixture.restart()?;
    let dead = receive(&fixture, 13, true)?;
    assert_eq!(dead.envelope.as_deref(), Some(&expected));
    let info = dead.dead_letter.expect("authoritative DLQ information");
    assert_eq!(
        info.reason,
        DeadLetterReason::Application(String::from("application-reason"))
    );
    assert_eq!(info.description, "application-description");
    Ok(())
}

fn broker_dead_letter_metadata_does_not_spend_the_retained_value_node_budget<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?;
    for (index, ttl, reason) in [
        (0_u64, Some(5), DeadLetterReason::TimeToLiveExpired),
        (1, None, DeadLetterReason::MaxDeliveryCountExceeded),
    ] {
        let start = 10 + index * 20;
        let envelope = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Array(vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS - 1
            ])),
            ..MessageEnvelope::default()
        };
        send(&fixture, start, ttl, envelope)?;
        if ttl.is_some() {
            assert_eq!(
                fixture.at(start + 6, CommandKind::ExpireMessages)?,
                CommandOutcome::MessagesExpired { dead_lettered: 1 }
            );
        } else {
            let delivery = receive(&fixture, start + 1, false)?;
            assert_eq!(
                settle(
                    &fixture,
                    start + 6,
                    &delivery,
                    SettlementDisposition::Abandon,
                    Updates::new(),
                )?,
                CommandOutcome::Abandoned {
                    dead_lettered: true
                }
            );
        }
        let dead = receive(&fixture, start + 7, true)?;
        assert_eq!(
            dead.dead_letter.as_ref().expect("DLQ reason").reason,
            reason
        );
        let MessageBody::Value(MessageValue::Array(values)) =
            &dead.envelope.as_deref().expect("retained envelope").body
        else {
            panic!("array payload survives dead-lettering");
        };
        assert_eq!(values.len(), MAX_MESSAGE_VALUE_ITEMS - 1);
        assert!(values.iter().all(|value| *value == MessageValue::Null));
        let mut command = fixture.command(
            start + 8,
            CommandKind::Complete {
                sequence: dead.sequence,
                lock_token: dead.lock.expect("DLQ lock").token,
            },
        );
        command.entity = fixture.entity.dead_letter_queue()?;
        fixture.machine.apply(&command)?;
    }
    Ok(())
}

fn dead_letter_text_limits_use_utf16_and_apply_to_legacy_and_rich_commands<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, None, rich())?;
    let delivery = receive(&fixture, 11, false)?;
    let too_long = "\u{1f600}".repeat(2_048) + "x";
    assert_eq!(too_long.encode_utf16().count(), 4_097);
    for (index, reason, description) in [
        (0_u64, too_long.clone(), String::new()),
        (1, String::new(), too_long),
    ] {
        let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
        assert!(matches!(
            settle(
                &fixture,
                12 + index * 2,
                &delivery,
                SettlementDisposition::DeadLetter {
                    reason: reason.clone(),
                    description: description.clone(),
                },
                Updates::new()
            ),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
        assert!(matches!(
            fixture.at(
                13 + index * 2,
                CommandKind::DeadLetter {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.as_ref().expect("lock").token,
                    reason,
                    description,
                }
            ),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
    }
    assert_eq!(
        settle(
            &fixture,
            16,
            &delivery,
            SettlementDisposition::DeadLetter {
                reason: "\u{1f600}".repeat(2_048),
                description: "x".repeat(4_096),
            },
            Updates::new()
        )?,
        CommandOutcome::DeadLettered
    );
    Ok(())
}

fn maximum_ingress_header_reserve_already_covers_automatic_dead_letter_properties<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?;
    for (index, ttl, reason) in [
        (0_u64, Some(5), DeadLetterReason::TimeToLiveExpired),
        (1, None, DeadLetterReason::MaxDeliveryCountExceeded),
    ] {
        let start = 10 + index * 20;
        let mut envelope = MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(String::from("id"))),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([
                (String::from("first"), MessageValue::Binary(vec![0; 20_000])),
                (
                    String::from("second"),
                    MessageValue::Binary(vec![0; 20_000]),
                ),
                (String::from("third"), MessageValue::Binary(Vec::new())),
            ]),
            ..MessageEnvelope::default()
        };
        let header_bytes = MAX_MESSAGE_HEADER_BYTES - BROKER_HEADER_RESERVE_BYTES;
        let remaining = header_bytes - envelope.header_content_size();
        envelope.application_properties.insert(
            String::from("third"),
            MessageValue::Binary(vec![0; remaining]),
        );
        assert_eq!(envelope.header_content_size(), header_bytes);
        assert_eq!(envelope.validate(), Ok(()));
        send(&fixture, start, ttl, envelope.clone())?;
        if ttl.is_some() {
            assert_eq!(
                fixture.at(start + 6, CommandKind::ExpireMessages)?,
                CommandOutcome::MessagesExpired { dead_lettered: 1 }
            );
        } else {
            let delivery = receive(&fixture, start + 1, false)?;
            assert_eq!(
                settle(
                    &fixture,
                    start + 6,
                    &delivery,
                    SettlementDisposition::Abandon,
                    Updates::new(),
                )?,
                CommandOutcome::Abandoned {
                    dead_lettered: true
                }
            );
        }
        let dead = receive(&fixture, start + 7, true)?;
        assert_eq!(
            dead.dead_letter.as_ref().expect("DLQ reason").reason,
            reason
        );
        assert_eq!(dead.envelope.as_deref(), Some(&envelope));
        let mut command = fixture.command(
            start + 8,
            CommandKind::Complete {
                sequence: dead.sequence,
                lock_token: dead.lock.expect("DLQ lock").token,
            },
        );
        command.entity = fixture.entity.dead_letter_queue()?;
        fixture.machine.apply(&command)?;
    }
    Ok(())
}

fn canonical_dead_letter_properties_must_fit_the_projected_header<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([
            (String::from("first"), MessageValue::Binary(vec![0; 29_000])),
            (
                String::from("second"),
                MessageValue::Binary(vec![0; 29_000]),
            ),
        ]),
        ..MessageEnvelope::default()
    };
    send(&fixture, 10, None, envelope)?;
    let delivery = receive(&fixture, 11, false)?;
    let reason = "r".repeat(4_096);
    let description = "d".repeat(4_096);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        settle(
            &fixture,
            12,
            &delivery,
            SettlementDisposition::DeadLetter {
                reason: reason.clone(),
                description: description.clone(),
            },
            Updates::new()
        ),
        Err(BrokerError::MessageHeaderTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    assert!(matches!(
        fixture.at(
            13,
            CommandKind::DeadLetter {
                sequence: delivery.sequence,
                lock_token: delivery.lock.as_ref().expect("lock").token,
                reason,
                description,
            }
        ),
        Err(BrokerError::MessageHeaderTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn dead_letter_receivers_cannot_cascade_a_message_into_another_shadow_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    send(&fixture, 10, None, rich())?;
    let source = receive(&fixture, 11, false)?;
    settle(
        &fixture,
        12,
        &source,
        SettlementDisposition::DeadLetter {
            reason: String::from("original-reason"),
            description: String::from("original-description"),
        },
        Updates::new(),
    )?;
    let delivery = receive(&fixture, 13, true)?;
    let dead_letter_queue = fixture.entity.dead_letter_queue()?;
    let forbidden_shadow = dead_letter_queue.dead_letter_queue()?;
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    for (millis, kind) in [
        (
            14,
            CommandKind::DeadLetter {
                sequence: delivery.sequence,
                lock_token: delivery.lock.as_ref().expect("DLQ lock").token,
                reason: String::from("second-reason"),
                description: String::from("second-description"),
            },
        ),
        (
            15,
            CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: delivery.lock.as_ref().expect("DLQ lock").token,
                disposition: SettlementDisposition::DeadLetter {
                    reason: String::from("second-reason"),
                    description: String::from("second-description"),
                },
                properties_to_modify: updates(),
            },
        ),
    ] {
        let mut command = fixture.command(millis, kind);
        command.entity = dead_letter_queue.clone();
        assert_eq!(
            fixture.machine.apply(&command),
            Err(BrokerError::DeadLetterQueueIsReserved)
        );
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
        assert_eq!(
            fixture
                .machine
                .store()
                .get(&keys::queue_config(&fixture.namespace, &forbidden_shadow))?,
            None
        );
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(
                    &keys::message_prefix(&fixture.namespace, &forbidden_shadow),
                    usize::MAX,
                )?
                .is_empty()
        );
    }
    let mut complete = fixture.command(
        16,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery
                .lock
                .expect("original DLQ lock remains valid")
                .token,
        },
    );
    complete.entity = dead_letter_queue.clone();
    assert_eq!(fixture.machine.apply(&complete)?, CommandOutcome::Completed);
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &dead_letter_queue, delivery.sequence)?
            .is_none()
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    rich_updates_survive_abandon_defer_and_restart_without_changing_other_content,
    legacy_payload_and_even_empty_identifier_are_materialized_only_for_nonempty_updates,
    lock_validation_precedes_invalid_property_validation_without_writes,
    invalid_shapes_and_individual_property_growth_leave_every_settlement_unchanged,
    merged_header_growth_includes_existing_values_and_broker_reserve,
    merged_content_growth_cannot_bypass_the_queue_total_message_limit,
    updates_are_applied_before_automatic_ttl_and_delivery_limit_dead_lettering,
    explicit_dead_lettering_preserves_updates_and_uses_authoritative_info,
    broker_dead_letter_metadata_does_not_spend_the_retained_value_node_budget,
    dead_letter_text_limits_use_utf16_and_apply_to_legacy_and_rich_commands,
    maximum_ingress_header_reserve_already_covers_automatic_dead_letter_properties,
    canonical_dead_letter_properties_must_fit_the_projected_header,
    dead_letter_receivers_cannot_cascade_a_message_into_another_shadow_queue,
}
