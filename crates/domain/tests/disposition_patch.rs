//! Copy-local property patches on abandon, defer and dead-letter: coalescing,
//! explicit nulls, projection refresh, V1 conversion, sibling isolation and
//! refusal without effects, over every storage backend and across reopen.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CONTENT_LIMITS, Command, CommandKind, CommandOutcome, CorrelationFilter,
    CorrelationValue, DeadLetterReason, Delivery, EntityPath, FilterProperties, MessageContent,
    MessageContentError, MessageEnvelope, MessageRecord, MessageRecordFormat, MessageState,
    NamespaceName, OverlayProperty, PROPERTY_PATCH_HEADROOM_BYTES, QueueConfig, RawMessage,
    ReceiveMode, RuleFilter, SequenceNumber, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, keys,
    legacy_content::{LegacyContentError, LegacyContentField, validate_legacy_content},
    snapshot_validation::validate_complete_state,
};
use message_content::{
    AnnotationKey, Binary, Body, Content, Field, MessageId, Properties, Symbol, Text, Value, Width,
    encode_content, encode_value,
};
use storage::{Key, StateStore, Value as Stored};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(DurableProvider::temporary()?)
        }
    };
}

fn text(value: &str) -> Text {
    Text {
        text: value.into(),
        width: Width::One,
    }
}

fn string(value: &str) -> Value {
    Value::String(text(value))
}

fn encoded(value: &Value) -> Vec<u8> {
    encode_value(value, CONTENT_LIMITS).expect("test value encodes")
}

fn entry(name: &str, value: &Value) -> OverlayProperty {
    OverlayProperty {
        name: name.into(),
        value: encoded(value),
    }
}

/// A message with the given application properties, a data body and a footer,
/// so the tests can see that neither body nor footer bytes move.
fn message(id: &str, properties: &[(&str, Value)], body: Vec<u8>) -> Vec<u8> {
    let content = Content {
        properties: Some(Properties {
            message_id: Field::Value(MessageId::String(text(id))),
            ..Properties::default()
        }),
        application_properties: Some(
            properties
                .iter()
                .map(|(name, value)| (text(name), value.clone()))
                .collect(),
        ),
        body: Body::Data(vec![Binary {
            bytes: body,
            width: Width::Four,
        }]),
        footer: Some(vec![(
            AnnotationKey::Symbol(Symbol {
                text: "x-footer".into(),
                width: Width::One,
            }),
            Value::UInt(9),
        )]),
        ..Content::default()
    };
    encode_content(&content, CONTENT_LIMITS).expect("test content encodes")
}

/// The projection a content send of these bytes derives.
fn derived(bytes: &[u8]) -> FilterProperties {
    MessageContent::Amqp(bytes.to_vec())
        .derive()
        .expect("derives")
        .envelope
        .expect("AMQP content has an envelope")
        .filter_properties()
        .clone()
}

fn row<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> (Stored, MessageRecord) {
    let raw = machine
        .store()
        .get(&keys::message(namespace, entity, sequence))
        .unwrap()
        .expect("stored row");
    let record = MessageRecord::decode(&raw).unwrap();
    assert_eq!(domain::codec::message::encode(&record).unwrap(), raw);
    (raw, record)
}

fn image<S: StateStore>(machine: &StateMachine<S>) -> Vec<(Key, Stored)> {
    machine.store().snapshot().unwrap().entries().to_vec()
}

/// Snapshot validation and the store-upgrade content check both accept it.
fn validated<S: StateStore>(machine: &StateMachine<S>) -> usize {
    let image = image(machine);
    let rows = validate_complete_state(&image).unwrap().message_rows();
    assert_eq!(
        validate_legacy_content(&image).unwrap().message_rows(),
        rows
    );
    rows
}

fn locked(outcome: CommandOutcome) -> Delivery {
    match outcome {
        CommandOutcome::Received(Some(delivery)) => delivery,
        CommandOutcome::DeferredReceived(mut deliveries) if deliveries.len() == 1 => {
            deliveries.remove(0)
        }
        other => panic!("expected one delivery, got {other:?}"),
    }
}

fn token(delivery: &Delivery) -> domain::LockToken {
    delivery.lock.expect("peek-lock").token
}

const RECEIVE: CommandKind = CommandKind::Receive {
    mode: ReceiveMode::PeekLock,
    lock_duration_millis: None,
    session: None,
};

fn region(value: &str) -> RuleFilter {
    RuleFilter::Correlation(CorrelationFilter {
        application_properties: BTreeMap::from([(
            "region".to_owned(),
            CorrelationValue::new(encoded(&string(value))).unwrap(),
        )]),
        ..CorrelationFilter::default()
    })
}

fn lifecycle<P: StoreProvider>(provider: P) -> TestResult {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let mut machine = StateMachine::new(provider.open()?);
    let at = |machine: &StateMachine<P::Store>, entity: &EntityPath, millis, kind| {
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    at(
        &machine,
        &topic,
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let mut copies = Vec::new();
    for name in ["patched", "sibling"] {
        let CommandOutcome::SubscriptionCreated { entity } = at(
            &machine,
            &topic,
            0,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )?
        else {
            panic!("expected a subscription");
        };
        copies.push(entity);
    }
    let (patched, sibling) = (copies[0].clone(), copies[1].clone());
    let dead_letters = patched.dead_letter_queue()?;
    let original = message(
        "m-1",
        &[("region", string("West")), ("keep", Value::Int(1))],
        b"body".to_vec(),
    );
    at(
        &machine,
        &topic,
        1,
        CommandKind::SendContent {
            content: MessageContent::Amqp(original.clone()),
        },
    )?;
    let sequence = SequenceNumber::new(1);
    let sibling_row = row(&machine, &namespace, &sibling, sequence).0;

    // A repeated name coalesces into its first position; null is a value.
    let first = locked(at(&machine, &patched, 2, RECEIVE)?);
    assert_eq!(
        at(
            &machine,
            &patched,
            3,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token: token(&first),
                patch: vec![
                    entry("region", &string("East")),
                    entry("added", &Value::Null),
                    entry("region", &string("North")),
                ],
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    let (_, record) = row(&machine, &namespace, &patched, sequence);
    assert_eq!(record.state, MessageState::Ready);
    let envelope = record.envelope.expect("AMQP content");
    assert_eq!(envelope.as_bytes(), original, "original bytes never change");
    assert_eq!(
        envelope.overlay(),
        [
            entry("region", &string("North")),
            OverlayProperty {
                name: "added".into(),
                value: vec![0x40],
            },
        ]
    );

    drop(machine);
    machine = StateMachine::new(provider.open()?);
    let second = locked(at(&machine, &patched, 4, RECEIVE)?);
    assert_eq!(second.delivery_count, 2);
    at(
        &machine,
        &patched,
        5,
        CommandKind::DeferWithPatch {
            sequence,
            lock_token: token(&second),
            patch: vec![entry("added", &Value::Int(7)), entry("z", &string("s"))],
        },
    )?;
    assert_eq!(
        row(&machine, &namespace, &patched, sequence).1.state,
        MessageState::Deferred
    );

    drop(machine);
    machine = StateMachine::new(provider.open()?);
    let third = locked(at(
        &machine,
        &patched,
        6,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?);
    assert_eq!(
        at(
            &machine,
            &patched,
            7,
            CommandKind::DeadLetterWithPatch {
                sequence,
                lock_token: token(&third),
                reason: "bad".into(),
                description: "why".into(),
                patch: vec![entry("region", &string("South"))],
            },
        )?,
        CommandOutcome::DeadLettered
    );

    drop(machine);
    machine = StateMachine::new(provider.open()?);
    let (raw, record) = row(&machine, &namespace, &dead_letters, sequence);
    assert_eq!(raw[0], 2);
    assert_eq!(record.format, MessageRecordFormat::V2);
    assert_eq!(
        record.dead_letter.map(|info| info.reason),
        Some(DeadLetterReason::Application("bad".into()))
    );
    assert!(record.body.is_empty());
    let envelope = record.envelope.expect("AMQP content");
    assert_eq!(envelope.as_bytes(), original);
    assert_eq!(
        envelope.overlay(),
        [
            entry("region", &string("South")),
            entry("added", &Value::Int(7)),
            entry("z", &string("s")),
        ]
    );
    // The stored projection is exactly what a send of the effective message
    // derives: overlay values replace or extend the original's properties.
    let effective = message(
        "m-1",
        &[
            ("region", string("South")),
            ("keep", Value::Int(1)),
            ("added", Value::Int(7)),
            ("z", string("s")),
        ],
        b"body".to_vec(),
    );
    assert_eq!(envelope.filter_properties(), &derived(&effective));

    // The sibling copy is untouched, byte for byte.
    let (raw, sibling_record) = row(&machine, &namespace, &sibling, sequence);
    assert_eq!(raw, sibling_row);
    let sibling_envelope = sibling_record.envelope.expect("AMQP content");
    assert!(sibling_envelope.overlay().is_empty());
    assert_eq!(sibling_envelope.filter_properties(), &derived(&original));

    // Rule matching reads the refreshed projection.
    for (filter, on_patched, on_sibling) in [
        (region("South"), true, false),
        (region("West"), false, true),
    ] {
        assert_eq!(filter.matches(envelope.filter_properties()), on_patched);
        assert_eq!(
            filter.matches(sibling_envelope.filter_properties()),
            on_sibling
        );
    }
    assert_eq!(validated(&machine), 2);
    Ok(())
}
paired!(
    repeated_patches_survive_reopen_on_memory,
    repeated_patches_survive_reopen_on_fjall,
    lifecycle
);

fn legacy<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let (namespace, queue) = (fixture.namespace.clone(), fixture.entity.clone());
    let original = message("legacy", &[("region", string("West"))], b"*".to_vec());
    let legacy_send = |envelope: Vec<u8>| CommandKind::Send {
        message_id: "legacy".into(),
        body: b"*".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: Some(MessageEnvelope::new(envelope)),
    };
    fixture.at(1, legacy_send(original.clone()))?;
    fixture.at(1, legacy_send(vec![0xff]))?;
    fixture.at(
        1,
        CommandKind::SendContent {
            content: MessageContent::Raw(RawMessage {
                message_id: "raw".into(),
                body: b"raw".to_vec(),
                ..RawMessage::default()
            }),
        },
    )?;
    let (v1, raw, unparseable) = (
        SequenceNumber::new(1),
        SequenceNumber::new(3),
        SequenceNumber::new(2),
    );
    assert_eq!(row(&fixture.machine, &namespace, &queue, v1).0[0], 1);

    // An empty patch changes no content and keeps the V1 format.
    let delivery = locked(fixture.at(2, RECEIVE)?);
    fixture.at(
        3,
        CommandKind::AbandonWithPatch {
            sequence: v1,
            lock_token: token(&delivery),
            patch: Vec::new(),
        },
    )?;
    let (stored, record) = row(&fixture.machine, &namespace, &queue, v1);
    assert_eq!((stored[0], record.body.as_slice()), (1, &b"*"[..]));

    // A patch converts V1 to V2: envelope becomes the original, body drops.
    let delivery = locked(fixture.at(4, RECEIVE)?);
    assert_eq!(delivery.sequence, v1);
    fixture.at(
        5,
        CommandKind::AbandonWithPatch {
            sequence: v1,
            lock_token: token(&delivery),
            patch: vec![entry("region", &string("East"))],
        },
    )?;
    let fixture = fixture.restart()?;
    let (stored, record) = row(&fixture.machine, &namespace, &queue, v1);
    assert_eq!(stored[0], 2);
    assert!(record.body.is_empty());
    let envelope = record.envelope.expect("AMQP content");
    assert_eq!(envelope.as_bytes(), original);
    assert_eq!(
        envelope.filter_properties(),
        &derived(&message(
            "legacy",
            &[("region", string("East"))],
            b"*".to_vec()
        ))
    );

    // An unparseable V1 envelope and a copy without AMQP content refuse with
    // no effects; the legacy replacement-free disposition still applies.
    let delivery = locked(fixture.at(6, RECEIVE)?);
    assert_eq!(delivery.sequence, v1);
    let delivery = locked(fixture.at(6, RECEIVE)?);
    assert_eq!(delivery.sequence, unparseable);
    let before = image(&fixture.machine);
    assert_eq!(
        fixture.at(
            7,
            CommandKind::DeadLetterWithPatch {
                sequence: unparseable,
                lock_token: token(&delivery),
                reason: "r".into(),
                description: "d".into(),
                patch: vec![entry("p", &Value::Null)],
            },
        ),
        Err(BrokerError::MessageContent(MessageContentError::Amqp(
            message_content::Error::InvalidConstructor
        )))
    );
    assert_eq!(image(&fixture.machine), before);
    fixture.at(
        8,
        CommandKind::DeadLetter {
            sequence: unparseable,
            lock_token: token(&delivery),
            reason: "r".into(),
            description: "d".into(),
            replacement_envelope: None,
        },
    )?;
    let dead_letters = queue.dead_letter_queue()?;
    assert_eq!(
        row(&fixture.machine, &namespace, &dead_letters, unparseable).0[0],
        1,
        "legacy dispositions keep the V1 format"
    );
    let delivery = locked(fixture.at(9, RECEIVE)?);
    assert_eq!(delivery.sequence, raw);
    let before = image(&fixture.machine);
    assert_eq!(
        fixture.at(
            10,
            CommandKind::DeferWithPatch {
                sequence: raw,
                lock_token: token(&delivery),
                patch: vec![entry("p", &Value::Null)],
            },
        ),
        Err(BrokerError::MessageContent(
            MessageContentError::PatchWithoutAmqpContent
        ))
    );
    assert_eq!(image(&fixture.machine), before);
    // Snapshot validation accepts the image; the store-upgrade content check
    // reports only the deliberately unparseable legacy envelope.
    let image = image(&fixture.machine);
    assert_eq!(validate_complete_state(&image)?.message_rows(), 3);
    assert!(matches!(
        validate_legacy_content(&image),
        Err(LegacyContentError::Content {
            field: LegacyContentField::Envelope,
            error: message_content::Error::InvalidConstructor,
            ..
        })
    ));
    Ok(())
}
paired!(
    patching_converts_legacy_records_on_memory,
    patching_converts_legacy_records_on_fjall,
    legacy
);

#[test]
fn refused_patches_leave_the_copy_lock_and_delivery_count_untouched() -> TestResult {
    const LIMIT: usize = 1024;
    let fixture = QueueFixture::new(
        MemoryProvider::new(),
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: LIMIT,
            ..QueueConfig::default()
        },
    )?;
    // Pad the body until the message sits exactly at the entity limit.
    let mut body = Vec::new();
    let original = loop {
        let bytes = message("m", &[("region", string("West"))], body.clone());
        if bytes.len() == LIMIT {
            break bytes;
        }
        body.push(0);
    };
    fixture.at(
        1,
        CommandKind::SendContent {
            content: MessageContent::Amqp(original.clone()),
        },
    )?;
    let sequence = SequenceNumber::new(1);
    let delivery = locked(fixture.at(2, RECEIVE)?);
    let lock_token = token(&delivery);
    let abandon = |patch| CommandKind::AbandonWithPatch {
        sequence,
        lock_token,
        patch,
    };
    let binary = |length| {
        Value::Binary(Binary {
            bytes: vec![7; length],
            width: Width::Four,
        })
    };
    let maximum_bytes = LIMIT + PROPERTY_PATCH_HEADROOM_BYTES;
    let before = image(&fixture.machine);
    for (patch, error) in [
        (
            vec![
                entry("p", &Value::Null),
                OverlayProperty {
                    name: "q".into(),
                    value: vec![0x40, 0x40],
                },
            ],
            BrokerError::MessageContent(MessageContentError::PatchValueInvalid(
                message_content::Error::TrailingBytes,
            )),
        ),
        (
            vec![OverlayProperty {
                name: "q".into(),
                value: Vec::new(),
            }],
            BrokerError::MessageContent(MessageContentError::PatchValueInvalid(
                message_content::Error::Truncated,
            )),
        ),
        (
            // One byte past the headroom: original + name + encoded value.
            vec![entry("p", &binary(PROPERTY_PATCH_HEADROOM_BYTES - 5))],
            BrokerError::MessageTooLarge {
                body_bytes: maximum_bytes + 1,
                maximum_bytes,
            },
        ),
    ] {
        assert_eq!(fixture.at(3, abandon(patch)), Err(error));
        assert_eq!(image(&fixture.machine), before, "refusal has no effects");
    }
    // A mismatched token refuses before the patch is examined.
    assert_eq!(
        fixture.at(
            3,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token: domain::LockToken::new(99),
                patch: vec![OverlayProperty {
                    name: "q".into(),
                    value: Vec::new(),
                }],
            },
        ),
        Err(BrokerError::LockTokenMismatch { sequence })
    );

    // A message admitted at the limit can still be dead-lettered with a patch
    // that fills the headroom exactly.
    let fill = entry("p", &binary(PROPERTY_PATCH_HEADROOM_BYTES - 6));
    assert_eq!(
        fixture.at(
            4,
            CommandKind::DeadLetterWithPatch {
                sequence,
                lock_token,
                reason: "r".into(),
                description: "d".into(),
                patch: vec![fill.clone()],
            },
        )?,
        CommandOutcome::DeadLettered
    );
    let dead_letters = fixture.entity.dead_letter_queue()?;
    let (_, record) = row(
        &fixture.machine,
        &fixture.namespace,
        &dead_letters,
        sequence,
    );
    let envelope = record.envelope.expect("AMQP content");
    assert_eq!(envelope.as_bytes(), original);
    assert_eq!(envelope.overlay(), [fill]);
    assert_eq!(validated(&fixture.machine), 1);
    Ok(())
}

fn patched_queue<P: StoreProvider>(
    provider: P,
    config: QueueConfig,
) -> TestResult<QueueFixture<P>> {
    Ok(QueueFixture::new(provider, "tenant", "orders", config)?)
}

fn send_amqp<P: StoreProvider>(fixture: &QueueFixture<P>, at: u64, bytes: &[u8]) -> TestResult {
    fixture.at(
        at,
        CommandKind::SendContent {
            content: MessageContent::Amqp(bytes.to_vec()),
        },
    )?;
    Ok(())
}

fn replacement<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = patched_queue(provider, QueueConfig::default())?;
    let (namespace, queue) = (fixture.namespace.clone(), fixture.entity.clone());
    let original = message("m-1", &[("region", string("West"))], b"one".to_vec());
    send_amqp(&fixture, 1, &original)?;
    send_amqp(&fixture, 1, &message("m-2", &[], b"two".to_vec()))?;
    let (first, second) = (SequenceNumber::new(1), SequenceNumber::new(2));
    let delivery = locked(fixture.at(2, RECEIVE)?);
    fixture.at(
        3,
        CommandKind::AbandonWithPatch {
            sequence: first,
            lock_token: token(&delivery),
            patch: vec![entry("region", &string("East"))],
        },
    )?;
    let fixture = fixture.restart()?;
    let redelivered = locked(fixture.at(4, RECEIVE)?);
    assert_eq!(redelivered.sequence, first);
    let other = locked(fixture.at(4, RECEIVE)?);
    assert_eq!(other.sequence, second);

    // A whole-envelope replacement would drop the patch, so it refuses.
    let rebuilt = Some(MessageEnvelope::new(original.clone()));
    let before = image(&fixture.machine);
    for kind in [
        CommandKind::Abandon {
            sequence: first,
            lock_token: token(&redelivered),
            replacement_envelope: rebuilt.clone(),
        },
        CommandKind::Defer {
            sequence: first,
            lock_token: token(&redelivered),
            replacement_envelope: rebuilt.clone(),
        },
        CommandKind::DeadLetter {
            sequence: first,
            lock_token: token(&redelivered),
            reason: "r".into(),
            description: "d".into(),
            replacement_envelope: rebuilt,
        },
    ] {
        assert_eq!(
            fixture.at(5, kind),
            Err(BrokerError::MessageContent(
                MessageContentError::ReplacementWouldDropOverlay
            ))
        );
        assert_eq!(image(&fixture.machine), before, "refusal has no effects");
    }

    // Replacement still applies to a copy without an overlay.
    let replaced = message("m-2", &[("region", string("North"))], b"two".to_vec());
    fixture.at(
        6,
        CommandKind::Defer {
            sequence: second,
            lock_token: token(&other),
            replacement_envelope: Some(MessageEnvelope::new(replaced.clone())),
        },
    )?;
    let (stored, record) = row(&fixture.machine, &namespace, &queue, second);
    assert_eq!(stored[0], 2);
    assert_eq!(record.state, MessageState::Deferred);
    let envelope = record.envelope.expect("AMQP content");
    assert_eq!(envelope.as_bytes(), replaced);
    assert!(envelope.overlay().is_empty());

    // A legacy disposition without a replacement keeps the overlay.
    fixture.at(
        7,
        CommandKind::Abandon {
            sequence: first,
            lock_token: token(&redelivered),
            replacement_envelope: None,
        },
    )?;
    let fixture = fixture.restart()?;
    let envelope = row(&fixture.machine, &namespace, &queue, first)
        .1
        .envelope
        .expect("AMQP content");
    assert_eq!(envelope.as_bytes(), original);
    assert_eq!(envelope.overlay(), [entry("region", &string("East"))]);
    assert_eq!(validated(&fixture.machine), 2);
    Ok(())
}
paired!(
    replacement_refuses_on_a_patched_copy_on_memory,
    replacement_refuses_on_a_patched_copy_on_fjall,
    replacement
);

/// An array of `count` `uint0` elements, each re-encoded four bytes wide.
fn zero_width_array(count: u32) -> Vec<u8> {
    let mut bytes = vec![0xf0, 0, 0, 0, 5];
    bytes.extend(count.to_be_bytes());
    bytes.push(0x43);
    bytes
}

#[test]
fn projection_stage_refusals_have_no_effects() -> TestResult {
    let fixture = patched_queue(MemoryProvider::new(), QueueConfig::default())?;
    send_amqp(&fixture, 1, &message("m", &[], Vec::new()))?;
    let sequence = SequenceNumber::new(1);
    let delivery = locked(fixture.at(2, RECEIVE)?);
    let before = image(&fixture.machine);
    let value = |name: &str, value: Vec<u8>| OverlayProperty {
        name: name.into(),
        value,
    };
    for (patch, error) in [
        (
            // [list0, list0]: bounded AMQP, but not a projectable value.
            vec![value("a", vec![0xe0, 0x02, 0x02, 0x45])],
            MessageContentError::UnsupportedArrayElement,
        ),
        (
            // Each value fits its own ceiling; together they re-encode past
            // the budget, which is linear in the small effective length.
            vec![
                value("a", zero_width_array(60_000)),
                value("b", zero_width_array(60_000)),
            ],
            MessageContentError::ProjectionTooLarge,
        ),
    ] {
        assert_eq!(
            fixture.at(
                3,
                CommandKind::DeferWithPatch {
                    sequence,
                    lock_token: token(&delivery),
                    patch,
                },
            ),
            Err(BrokerError::MessageContent(error))
        );
        assert_eq!(image(&fixture.machine), before, "refusal has no effects");
    }
    Ok(())
}

#[test]
fn an_abandon_that_dead_letters_carries_its_patch() -> TestResult {
    for (config, abandon_at, reason) in [
        (
            QueueConfig {
                max_delivery_count: 1,
                ..QueueConfig::default()
            },
            3,
            DeadLetterReason::MaxDeliveryCountExceeded,
        ),
        (
            QueueConfig {
                default_time_to_live_millis: Some(10),
                ..QueueConfig::default()
            },
            20,
            DeadLetterReason::TimeToLiveExpired,
        ),
    ] {
        let fixture = patched_queue(MemoryProvider::new(), config)?;
        let original = message("m", &[("region", string("West"))], b"x".to_vec());
        send_amqp(&fixture, 1, &original)?;
        let sequence = SequenceNumber::new(1);
        let delivery = locked(fixture.at(2, RECEIVE)?);
        assert_eq!(
            fixture.at(
                abandon_at,
                CommandKind::AbandonWithPatch {
                    sequence,
                    lock_token: token(&delivery),
                    patch: vec![entry("region", &string("East"))],
                },
            )?,
            CommandOutcome::Abandoned {
                dead_lettered: true
            }
        );
        let dead_letters = fixture.entity.dead_letter_queue()?;
        let record = row(
            &fixture.machine,
            &fixture.namespace,
            &dead_letters,
            sequence,
        )
        .1;
        assert_eq!(record.dead_letter.map(|info| info.reason), Some(reason));
        let envelope = record.envelope.expect("AMQP content");
        assert_eq!(envelope.as_bytes(), original);
        assert_eq!(envelope.overlay(), [entry("region", &string("East"))]);
        assert_eq!(
            envelope.filter_properties(),
            &derived(&message("m", &[("region", string("East"))], b"x".to_vec()))
        );
        assert_eq!(validated(&fixture.machine), 1);
    }
    Ok(())
}

#[test]
fn distinct_names_accumulate_until_the_headroom_refuses() -> TestResult {
    const LIMIT: usize = 1024;
    const VALUE: usize = 20_000;
    let fixture = patched_queue(
        MemoryProvider::new(),
        QueueConfig {
            max_message_bytes: LIMIT,
            ..QueueConfig::default()
        },
    )?;
    let original = message("m", &[], Vec::new());
    send_amqp(&fixture, 1, &original)?;
    let sequence = SequenceNumber::new(1);
    let property = |name: &str| {
        entry(
            name,
            &Value::Binary(Binary {
                bytes: vec![7; VALUE],
                width: Width::Four,
            }),
        )
    };
    // One-byte name plus a five-byte binary header per entry.
    let per_entry = 1 + 5 + VALUE;
    let maximum_bytes = LIMIT + PROPERTY_PATCH_HEADROOM_BYTES;
    let mut at = 2;
    let mut overlay = Vec::new();
    for name in ["a", "b", "c", "d"] {
        let delivery = locked(fixture.at(at, RECEIVE)?);
        let patch = CommandKind::AbandonWithPatch {
            sequence,
            lock_token: token(&delivery),
            patch: vec![property(name)],
        };
        let effective = original.len() + per_entry * (overlay.len() + 1);
        if effective <= maximum_bytes {
            fixture.at(at + 1, patch)?;
            overlay.push(property(name));
        } else {
            let before = image(&fixture.machine);
            assert_eq!(
                fixture.at(at + 1, patch),
                Err(BrokerError::MessageTooLarge {
                    body_bytes: effective,
                    maximum_bytes,
                })
            );
            assert_eq!(image(&fixture.machine), before, "refusal has no effects");
        }
        at += 2;
    }
    assert_eq!(
        overlay.len(),
        3,
        "the fourth distinct name passes the headroom"
    );
    let record = row(
        &fixture.machine,
        &fixture.namespace,
        &fixture.entity,
        sequence,
    )
    .1;
    assert_eq!(record.envelope.expect("AMQP content").overlay(), overlay);
    Ok(())
}

#[test]
fn a_large_entity_limit_still_caps_accumulation_at_the_content_ceiling() -> TestResult {
    // Far above the content-safety ceiling, so the ceiling binds instead.
    const LIMIT: usize = 100 * 1024 * 1024;
    // Each value encodes to exactly the 256 KiB correlation-value ceiling.
    const VALUE: usize = 256 * 1024 - 5;
    let fixture = patched_queue(
        MemoryProvider::new(),
        QueueConfig {
            max_message_bytes: LIMIT,
            ..QueueConfig::default()
        },
    )?;
    let original = message("m", &[], Vec::new());
    send_amqp(&fixture, 1, &original)?;
    let sequence = SequenceNumber::new(1);
    let property = |index: usize| {
        entry(
            &format!("p{index:02}"),
            &Value::Binary(Binary {
                bytes: vec![7; VALUE],
                width: Width::Four,
            }),
        )
    };
    // A three-byte name plus a five-byte binary header per entry.
    let per_entry = 3 + 5 + VALUE;
    let maximum_bytes = CONTENT_LIMITS.max_bytes;
    assert!(maximum_bytes < LIMIT + PROPERTY_PATCH_HEADROOM_BYTES);
    // 63 entries fit under 16 MiB and a 64th does not: two patches accumulate
    // to the cap, and a third distinct name refuses.
    let mut at = 2;
    let mut overlay = Vec::new();
    for range in [0..32, 32..63] {
        let delivery = locked(fixture.at(at, RECEIVE)?);
        let patch: Vec<_> = range.map(property).collect();
        fixture.at(
            at + 1,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token: token(&delivery),
                patch: patch.clone(),
            },
        )?;
        overlay.extend(patch);
        at += 2;
    }
    assert!(original.len() + per_entry * overlay.len() <= maximum_bytes);

    let delivery = locked(fixture.at(at, RECEIVE)?);
    let before = image(&fixture.machine);
    assert_eq!(
        fixture.at(
            at + 1,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token: token(&delivery),
                patch: vec![property(63)],
            },
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: original.len() + per_entry * 64,
            maximum_bytes,
        }),
        "the refusal reports the capped maximum"
    );
    assert_eq!(image(&fixture.machine), before, "refusal has no effects");
    let record = row(
        &fixture.machine,
        &fixture.namespace,
        &fixture.entity,
        sequence,
    )
    .1;
    assert_eq!(record.envelope.expect("AMQP content").overlay(), overlay);
    Ok(())
}

#[test]
fn a_v1_record_without_an_envelope_refuses_a_patch() -> TestResult {
    let fixture = patched_queue(MemoryProvider::new(), QueueConfig::default())?;
    fixture.at(
        1,
        CommandKind::Send {
            message_id: "raw".into(),
            body: b"raw".to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    let sequence = SequenceNumber::new(1);
    assert_eq!(
        row(
            &fixture.machine,
            &fixture.namespace,
            &fixture.entity,
            sequence
        )
        .0[0],
        1
    );
    let delivery = locked(fixture.at(2, RECEIVE)?);
    let before = image(&fixture.machine);
    assert_eq!(
        fixture.at(
            3,
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token: token(&delivery),
                patch: vec![entry("p", &Value::Null)],
            },
        ),
        Err(BrokerError::MessageContent(
            MessageContentError::PatchWithoutAmqpContent
        ))
    );
    assert_eq!(image(&fixture.machine), before);
    Ok(())
}
