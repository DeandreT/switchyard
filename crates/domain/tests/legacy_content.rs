//! Bounded legacy-content preflight: locations, fixed bounds and refusals.

use std::collections::BTreeMap;

use domain::{
    CorrelationFilter, CorrelationValue, DeadLetterInfo, DeadLetterReason, DeliveryOrigin,
    EntityPath, FilterProperties, LockToken, MessageEnvelope, MessageRecord, MessageRecordFormat,
    MessageState, NamespaceName, OverlayProperty, RuleDefinition, RuleFilter, RuleName,
    SequenceNumber, SessionId, SessionRecord, SubscriptionName, Timestamp, codec, keys,
    legacy_content::{
        ContentError, ContentLimits, LEGACY_CONTENT_LIMITS, LegacyContentError, LegacyContentField,
        validate_legacy_content,
    },
};
use storage::{Key, Value};

const MIB: usize = 1024 * 1024;

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}

fn path(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}

/// `amqp-value` section holding a short string.
fn value_envelope() -> Vec<u8> {
    vec![0x00, 0x53, 0x77, 0xa1, 2, b'h', b'i']
}

/// One `data` section of exactly `payload` bytes (8 bytes of framing).
fn data_envelope(payload: usize) -> Vec<u8> {
    let mut bytes = vec![0x00, 0x53, 0x75, 0xb0];
    bytes.extend_from_slice(&u32::try_from(payload).unwrap().to_be_bytes());
    bytes.resize(bytes.len() + payload, 0x5a);
    bytes
}

/// An AMQP int and string, as the protocol adapter encodes correlation scalars.
fn scalars() -> BTreeMap<String, CorrelationValue> {
    BTreeMap::from([
        (
            String::from("a"),
            CorrelationValue::new(vec![0x71, 0, 0, 0, 5]).unwrap(),
        ),
        (
            String::from("b"),
            CorrelationValue::new(vec![0xa1, 1, b'x']).unwrap(),
        ),
    ])
}

fn envelope(bytes: Vec<u8>, properties: BTreeMap<String, CorrelationValue>) -> MessageEnvelope {
    MessageEnvelope::new(bytes).with_filter_properties(FilterProperties {
        subject: Some(String::from("retained")),
        application_properties: properties,
        ..FilterProperties::default()
    })
}

fn record(sequence: u64, state: MessageState, envelope: Option<MessageEnvelope>) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: format!("m-{sequence}"),
        // Raw bodies are opaque, never AMQP: invalid AMQP bytes are retained.
        body: vec![0xff, 0x00, 0xee],
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state,
        session_id: None,
        dead_letter: None,
        envelope,
        format: MessageRecordFormat::V1,
    }
}

fn message_row(entity: &EntityPath, record: &MessageRecord) -> (Key, Value) {
    (
        keys::message(&namespace(), entity, record.sequence),
        codec::message::encode(record).unwrap(),
    )
}

fn rule_row(properties: BTreeMap<String, CorrelationValue>) -> (Key, Value) {
    let subscription = path("orders")
        .subscription(&SubscriptionName::new("audit").unwrap())
        .unwrap();
    let name = RuleName::new("match").unwrap();
    let definition = RuleDefinition {
        name: name.clone(),
        filter: RuleFilter::Correlation(CorrelationFilter {
            application_properties: properties,
            ..CorrelationFilter::default()
        }),
        created_at: Timestamp::from_millis(2),
    };
    (
        keys::subscription_rule(&namespace(), &subscription, &name),
        codec::encode(&definition).unwrap(),
    )
}

/// Each envelope-bearing message state and owner kind, plus opaque companions.
fn image(rule_properties: BTreeMap<String, CorrelationValue>) -> Vec<(Key, Value)> {
    let queue = path("queue");
    let locked = MessageState::Locked {
        token: LockToken::new(1),
        locked_until: Timestamp::from_millis(9),
        origin: DeliveryOrigin::Deferred,
    };
    let mut dead = record(
        6,
        MessageState::Ready,
        Some(envelope(value_envelope(), scalars())),
    );
    dead.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::MaxDeliveryCountExceeded,
        description: String::from("retained"),
        dead_lettered_at: Timestamp::from_millis(3),
    });
    let session = SessionRecord {
        lock: None,
        // Session state is opaque, never AMQP.
        state: vec![0xff, 0xfe],
    };
    let mut rows = vec![
        (
            keys::clock(),
            codec::encode(&Timestamp::from_millis(9)).unwrap(),
        ),
        message_row(
            &queue,
            &record(
                1,
                MessageState::Ready,
                Some(envelope(value_envelope(), scalars())),
            ),
        ),
        message_row(
            &queue,
            &record(
                2,
                MessageState::Scheduled,
                Some(envelope(value_envelope(), scalars())),
            ),
        ),
        message_row(
            &queue,
            &record(
                3,
                MessageState::Deferred,
                Some(envelope(value_envelope(), scalars())),
            ),
        ),
        message_row(
            &queue,
            &record(4, locked, Some(envelope(value_envelope(), scalars()))),
        ),
        message_row(&queue, &record(5, MessageState::Ready, None)),
        message_row(&queue.dead_letter_queue().unwrap(), &dead),
        message_row(
            &path("topic"),
            &record(
                7,
                MessageState::Scheduled,
                Some(envelope(value_envelope(), scalars())),
            ),
        ),
        (
            keys::session(&namespace(), &queue, &SessionId::new("s").unwrap()),
            codec::encode(&session).unwrap(),
        ),
        rule_row(rule_properties),
        // Opaque external owners, including historical refused proposal intent.
        (b"\xF0switchyard/journal\0\x01".to_vec(), vec![0xff; 4]),
        (b"\xF1owner".to_vec(), vec![0xff]),
    ];
    rows.sort();
    rows
}

fn row_of(rows: &[(Key, Value)], sequence: u64, entity: &EntityPath) -> usize {
    let key = keys::message(&namespace(), entity, SequenceNumber::new(sequence));
    rows.iter()
        .position(|(candidate, _)| *candidate == key)
        .unwrap()
}

fn replace_message(
    rows: &mut [(Key, Value)],
    entity: &EntityPath,
    record: &MessageRecord,
) -> usize {
    let row = row_of(rows, record.sequence.as_u64(), entity);
    rows[row].1 = codec::message::encode(record).unwrap();
    row
}

#[test]
fn every_content_location_is_checked_and_opaque_bytes_are_retained() {
    let rows = image(scalars());
    let report = validate_legacy_content(&rows).unwrap();
    assert_eq!(report.message_rows(), 7);
    assert_eq!(report.rule_rows(), 1);
    assert_eq!(report.envelopes(), 6);
    assert_eq!(report.values(), 6 * 2 + 2);
    assert_eq!(validate_legacy_content(&[]).unwrap().envelopes(), 0);
}

#[test]
fn unsafe_content_refuses_at_its_row_and_field_without_payload_text() {
    let queue = path("queue");
    let topic = path("topic");
    let dead = queue.dead_letter_queue().unwrap();
    let mut malformed = value_envelope();
    malformed.extend_from_slice(b"SECRET\xff");
    let locked = MessageState::Locked {
        token: LockToken::new(1),
        locked_until: Timestamp::from_millis(9),
        origin: DeliveryOrigin::Ready,
    };
    for (entity, state, sequence) in [
        (&queue, MessageState::Ready, 1),
        (&queue, MessageState::Scheduled, 2),
        (&queue, MessageState::Deferred, 3),
        (&queue, locked, 4),
        (&dead, MessageState::Ready, 6),
        (&topic, MessageState::Scheduled, 7),
    ] {
        let mut rows = image(scalars());
        let changed = record(
            sequence,
            state.clone(),
            Some(envelope(malformed.clone(), scalars())),
        );
        let row = replace_message(&mut rows, entity, &changed);
        let error = validate_legacy_content(&rows).unwrap_err();
        assert!(matches!(
            error,
            LegacyContentError::Content {
                row: found,
                field: LegacyContentField::Envelope,
                ..
            } if found == row
        ));
        assert!(!error.to_string().contains("SECRET"));

        let mut properties = scalars();
        properties.insert(
            String::from("c"),
            CorrelationValue::new(vec![0xa1, 9]).unwrap(),
        );
        let changed = record(
            sequence,
            state,
            Some(envelope(value_envelope(), properties)),
        );
        let mut rows = image(scalars());
        let row = replace_message(&mut rows, entity, &changed);
        assert_eq!(
            validate_legacy_content(&rows),
            Err(LegacyContentError::Content {
                row,
                field: LegacyContentField::FilterProperty { ordinal: 2 },
                error: ContentError::Truncated,
            })
        );
    }

    let mut properties = scalars();
    properties.insert(
        String::from("0"),
        CorrelationValue::new(b"SECRET".to_vec()).unwrap(),
    );
    let rows = image(properties);
    let row = rows.iter().position(|(key, _)| key[0] == 0x10).unwrap();
    let error = validate_legacy_content(&rows).unwrap_err();
    assert!(matches!(
        error,
        LegacyContentError::Content {
            row: found,
            field: LegacyContentField::RuleProperty { ordinal: 0 },
            ..
        } if found == row
    ));
    assert!(!error.to_string().contains("SECRET"));
}

#[test]
fn outer_records_are_canonical_before_content_is_decoded() {
    let base = image(scalars());
    let message = row_of(&base, 1, &path("queue"));
    let rule = base.iter().position(|(key, _)| key[0] == 0x10).unwrap();
    for row in [message, rule] {
        let mut trailing = base.clone();
        trailing[row].1.push(0);
        assert_eq!(
            validate_legacy_content(&trailing),
            Err(LegacyContentError::InvalidRecord {
                row,
                detail: "value is not canonical"
            })
        );
        for value in [vec![], vec![2, 0], vec![1, 0xff, 0xff]] {
            let mut broken = base.clone();
            broken[row].1 = value;
            assert_eq!(
                validate_legacy_content(&broken),
                Err(LegacyContentError::InvalidRecord {
                    row,
                    detail: "value cannot be decoded"
                })
            );
        }
    }

    let mut extra = base.clone();
    let mut key = extra[message].0.clone();
    key.push(0);
    extra.push((key, extra[message].1.clone()));
    extra.sort();
    let row = extra
        .iter()
        .position(|(key, _)| key.len() == base[message].0.len() + 1)
        .unwrap();
    assert_eq!(
        validate_legacy_content(&extra),
        Err(LegacyContentError::InvalidKey {
            row,
            detail: "message key is not canonical"
        })
    );
    let mut rule_key = base.clone();
    rule_key[rule].0.push(0);
    assert_eq!(
        validate_legacy_content(&rule_key),
        Err(LegacyContentError::InvalidKey {
            row: rule,
            detail: "rule key is not canonical"
        })
    );

    for tag in [0x12, 0xEF, 0xF2, 0xFF] {
        let mut rows = base.clone();
        rows.push((vec![tag], Vec::new()));
        rows.sort();
        let row = rows.iter().position(|(key, _)| key == &vec![tag]).unwrap();
        assert_eq!(
            validate_legacy_content(&rows),
            Err(LegacyContentError::UnrecognizedTag { row, tag })
        );
    }
    let mut empty = base.clone();
    empty.insert(0, (Vec::new(), Vec::new()));
    assert_eq!(
        validate_legacy_content(&empty),
        Err(LegacyContentError::EmptyKey { row: 0 })
    );
    let mut reversed = base.clone();
    reversed.swap(0, 1);
    assert_eq!(
        validate_legacy_content(&reversed),
        Err(LegacyContentError::InputOrder { row: 1 })
    );
}

fn single(bytes: Vec<u8>) -> Result<(), LegacyContentError> {
    let record = record(
        1,
        MessageState::Ready,
        Some(envelope(bytes, BTreeMap::new())),
    );
    validate_legacy_content(&[message_row(&path("queue"), &record)]).map(|_| ())
}

fn refusal(error: ContentError) -> Result<(), LegacyContentError> {
    Err(LegacyContentError::Content {
        row: 0,
        field: LegacyContentField::Envelope,
        error,
    })
}

#[test]
fn the_fixed_profile_is_exact() {
    assert_eq!(
        LEGACY_CONTENT_LIMITS,
        ContentLimits {
            max_bytes: 16 * MIB,
            max_depth: 128,
            max_nodes: 4_194_304,
            max_work: 268_435_456,
        }
    );
}

#[test]
fn encoded_bytes_are_bounded_exactly() {
    assert_eq!(single(data_envelope(16 * MIB - 8)), Ok(()));
    assert_eq!(
        single(data_envelope(16 * MIB - 7)),
        refusal(ContentError::ByteLimit)
    );

    // Standalone values have their own per-content bound, independent of the
    // current correlation admission cap, which is never reapplied here.
    let value = |payload: usize| {
        let mut bytes = vec![0xb0];
        bytes.extend_from_slice(&u32::try_from(payload).unwrap().to_be_bytes());
        bytes.resize(bytes.len() + payload, 0);
        let retained = postcard::to_stdvec(&bytes).unwrap();
        let value = postcard::from_bytes::<CorrelationValue>(&retained).unwrap();
        BTreeMap::from([(String::from("a"), value)])
    };
    for (payload, expected) in [
        (16 * MIB - 5, None),
        (16 * MIB - 4, Some(ContentError::ByteLimit)),
    ] {
        let record = record(
            1,
            MessageState::Ready,
            Some(envelope(value_envelope(), value(payload))),
        );
        assert_eq!(
            validate_legacy_content(&[message_row(&path("queue"), &record)]).err(),
            expected.map(|error| LegacyContentError::Content {
                row: 0,
                field: LegacyContentField::FilterProperty { ordinal: 0 },
                error,
            })
        );
    }
}

/// An `amqp-value` section whose innermost list sits at nesting `depth`.
fn nested(depth: usize) -> Vec<u8> {
    // Section described node at depth 1, its value at depth 2.
    let mut value = vec![0x45];
    for _ in 2..depth {
        let mut wrapper = vec![0xd0];
        wrapper.extend_from_slice(&u32::try_from(value.len() + 4).unwrap().to_be_bytes());
        wrapper.extend_from_slice(&1_u32.to_be_bytes());
        wrapper.extend_from_slice(&value);
        value = wrapper;
    }
    let mut bytes = vec![0x00, 0x53, 0x77];
    bytes.extend_from_slice(&value);
    bytes
}

#[test]
fn nesting_depth_is_bounded_exactly() {
    assert_eq!(single(nested(128)), Ok(()));
    assert_eq!(single(nested(129)), refusal(ContentError::DepthLimit));
}

/// An `amqp-value` list of `count` nulls: section, descriptor, list and elements.
fn nulls(count: usize) -> Vec<u8> {
    let mut bytes = vec![0x00, 0x53, 0x77, 0xd0];
    bytes.extend_from_slice(&u32::try_from(count + 4).unwrap().to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(count).unwrap().to_be_bytes());
    bytes.resize(bytes.len() + count, 0x40);
    bytes
}

#[test]
fn nodes_are_bounded_exactly() {
    assert_eq!(single(nulls(4_194_304 - 3)), Ok(()));
    assert_eq!(
        single(nulls(4_194_304 - 2)),
        refusal(ContentError::NodeLimit)
    );
}

/// An `amqp-value` array of `count` zero-payload `true` elements whose
/// constructor has `layers` described layers; the first descriptor is a
/// symbol of `pad` bytes, so `pad` adjusts work one byte at a time.
fn described_array(count: usize, layers: usize, pad: usize) -> Vec<u8> {
    let mut constructor = Vec::new();
    for layer in 0..layers {
        constructor.push(0x00);
        if layer == 0 {
            constructor.push(0xa3);
            constructor.push(u8::try_from(pad).unwrap());
            constructor.resize(constructor.len() + pad, b'p');
        } else {
            constructor.extend_from_slice(&[0x53, 0x01]);
        }
    }
    constructor.push(0x41);
    let mut bytes = vec![0x00, 0x53, 0x77, 0xf0];
    bytes.extend_from_slice(&u32::try_from(constructor.len() + 4).unwrap().to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(count).unwrap().to_be_bytes());
    bytes.extend_from_slice(&constructor);
    bytes
}

/// Exact work of a small input, found with otherwise unconstrained limits.
fn work(bytes: &[u8]) -> usize {
    let limits = |max_work| ContentLimits {
        max_work,
        ..LEGACY_CONTENT_LIMITS
    };
    let (mut low, mut high) = (0, LEGACY_CONTENT_LIMITS.max_work);
    while low < high {
        let middle = low + (high - low) / 2;
        if message_content::decode_content(bytes, limits(middle)).is_ok() {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

#[test]
fn work_is_bounded_exactly() {
    const LAYERS: usize = 99;
    let fixed = work(&described_array(0, LAYERS, 0));
    let per_element = work(&described_array(1, LAYERS, 0)) - fixed;
    assert_eq!(per_element, LAYERS + 1);
    assert_eq!(
        work(&described_array(2, LAYERS, 3)),
        fixed + 2 * per_element + 3
    );
    let available = LEGACY_CONTENT_LIMITS.max_work - fixed;
    let count = available / per_element;
    let pad = available % per_element;
    assert!(count < LEGACY_CONTENT_LIMITS.max_nodes);
    assert_eq!(single(described_array(count, LAYERS, pad)), Ok(()));
    assert_eq!(
        single(described_array(count, LAYERS, pad + 1)),
        refusal(ContentError::WorkLimit)
    );
}

fn overlay(values: [Vec<u8>; 2]) -> Vec<OverlayProperty> {
    let [first, second] = values;
    vec![
        OverlayProperty {
            name: String::from("z"),
            value: first,
        },
        OverlayProperty {
            name: String::from("a"),
            value: second,
        },
    ]
}

/// A V2 record: raw content without an envelope, or AMQP content without a body.
fn version_two(envelope: Option<MessageEnvelope>) -> MessageRecord {
    MessageRecord {
        body: if envelope.is_some() {
            Vec::new()
        } else {
            vec![0xff, 0x00, 0xee]
        },
        format: MessageRecordFormat::V2,
        ..record(1, MessageState::Ready, envelope)
    }
}

fn amqp(
    original: Vec<u8>,
    values: [Vec<u8>; 2],
    properties: BTreeMap<String, CorrelationValue>,
) -> MessageEnvelope {
    envelope(original, properties).with_overlay(overlay(values))
}

#[test]
fn version_two_rows_check_original_overlay_and_projection() {
    let queue = path("queue");
    let int = vec![0x71, 0, 0, 0, 1];
    let null = vec![0x40];
    let raw = message_row(&queue, &version_two(None));
    assert_eq!(raw.1[0], 2, "frozen V2 framing");
    let report = validate_legacy_content(std::slice::from_ref(&raw)).unwrap();
    assert_eq!(
        (report.message_rows(), report.envelopes(), report.values()),
        (1, 0, 0)
    );

    let safe = version_two(Some(amqp(
        value_envelope(),
        [int.clone(), null.clone()],
        scalars(),
    )));
    let report = validate_legacy_content(&[message_row(&queue, &safe)]).unwrap();
    assert_eq!((report.envelopes(), report.values()), (1, 2 + 2));

    let mut bad_properties = scalars();
    bad_properties.insert(
        String::from("c"),
        CorrelationValue::new(vec![0xa1, 9]).unwrap(),
    );
    for (envelope, field, error) in [
        (
            amqp(
                [value_envelope(), b"SECRET".to_vec()].concat(),
                [int.clone(), null.clone()],
                scalars(),
            ),
            LegacyContentField::Envelope,
            ContentError::InvalidSection,
        ),
        (
            amqp(
                value_envelope(),
                [int.clone(), b"SECRET".to_vec()],
                scalars(),
            ),
            LegacyContentField::OverlayProperty { ordinal: 1 },
            ContentError::TrailingBytes,
        ),
        (
            amqp(
                value_envelope(),
                [int.clone(), null.clone()],
                bad_properties,
            ),
            LegacyContentField::FilterProperty { ordinal: 2 },
            ContentError::Truncated,
        ),
    ] {
        let row = message_row(&queue, &version_two(Some(envelope)));
        let refused = validate_legacy_content(&[row]).unwrap_err();
        assert_eq!(
            refused,
            LegacyContentError::Content {
                row: 0,
                field,
                error
            }
        );
        assert!(!refused.to_string().contains("SECRET"));
    }

    let mut trailing = message_row(&queue, &safe);
    trailing.1.push(0);
    assert_eq!(
        validate_legacy_content(&[trailing]),
        Err(LegacyContentError::InvalidRecord {
            row: 0,
            detail: "value is not canonical"
        })
    );
}
