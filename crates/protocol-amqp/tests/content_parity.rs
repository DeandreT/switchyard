//! Fields the state machine derives from original AMQP content match what
//! ingress derives for a legacy send, so moving the derivation changes neither
//! normalized fields nor rule matching.

use amqp::{
    ApplicationProperties, Array, Binary, Body, Header, Message, MessageAnnotations, MessageId,
    OrderedMap, Properties, Symbol, Uuid, Value, decode_message, encode_message,
};
use domain::{MessageContent, MessageInput};
use protocol_amqp::read_incoming;
use serde_amqp::{
    described::Described,
    descriptor::Descriptor,
    primitives::{Dec32, Dec64, Dec128, Timestamp},
};

/// Ingress decodes the received bytes, then reads the decoded message.
fn assert_parity(encoded: &[u8]) {
    let decoded = decode_message(encoded).expect("ingress accepts the message");
    let legacy = MessageInput::from(read_incoming(&decoded, encoded).expect("ingress reads it"));
    let derived = MessageContent::Amqp(encoded.to_vec())
        .derive()
        .expect("the domain derives it");
    assert_eq!(derived.message_id, legacy.message_id);
    assert_eq!(derived.session_id, legacy.session_id);
    assert_eq!(derived.time_to_live_millis, legacy.time_to_live_millis);
    assert_eq!(derived.scheduled_enqueue_at, legacy.scheduled_enqueue_at);
    assert_eq!(derived.envelope, legacy.envelope);
    assert!(derived.body.is_empty(), "the envelope is the only copy");
    assert_legacy_content_accepts(&derived);
}

/// Every record a content send writes must satisfy the same bounded-content
/// contract that snapshot capture, compaction and store upgrade enforce, or
/// those offline paths would reject it. This checks each parity input end to
/// end: the derived V2 record decodes under `validate_legacy_content`.
fn assert_legacy_content_accepts(input: &MessageInput) {
    use domain::{
        EntityPath, MessageRecord, MessageRecordFormat, MessageState, NamespaceName,
        SequenceNumber, codec, keys, legacy_content::validate_legacy_content,
    };
    let record = MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: input.message_id.clone(),
        body: input.body.clone(),
        enqueued_at: domain::Timestamp::from_millis(0),
        scheduled_enqueue_at: input.scheduled_enqueue_at,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: input.session_id.clone(),
        dead_letter: None,
        envelope: input.envelope.clone(),
        format: MessageRecordFormat::V2,
    };
    let encoded = codec::message::encode(&record).expect("the derived record encodes");
    let namespace = NamespaceName::new("tenant").expect("namespace");
    let entity = EntityPath::new("orders").expect("entity");
    let image = vec![(keys::message(&namespace, &entity, record.sequence), encoded)];
    validate_legacy_content(&image).expect("legacy content accepts the derived record");
}

fn message(message: &Message) {
    assert_parity(&encode_message(message).expect("the test message encodes"));
}

fn uuid(seed: u8) -> Uuid {
    Uuid::from([seed; 16])
}

fn described(descriptor: Descriptor, value: Value) -> Value {
    Value::from(Described { descriptor, value })
}

fn map(entries: Vec<(Value, Value)>) -> Value {
    Value::Map(entries.into_iter().collect::<OrderedMap<_, _>>())
}

fn every_value_family() -> ApplicationProperties {
    let long_text = "é".repeat(200);
    let values = vec![
        ("null", Value::Null),
        ("true", Value::Bool(true)),
        ("false", Value::Bool(false)),
        ("ubyte", Value::Ubyte(7)),
        ("ushort", Value::Ushort(700)),
        ("uint-0", Value::Uint(0)),
        ("uint-small", Value::Uint(7)),
        ("uint", Value::Uint(70_000)),
        ("ulong-0", Value::Ulong(0)),
        ("ulong-small", Value::Ulong(255)),
        ("ulong", Value::Ulong(256)),
        ("byte", Value::Byte(-7)),
        ("short", Value::Short(-700)),
        ("int-small", Value::Int(-128)),
        ("int", Value::Int(128)),
        ("long-small", Value::Long(127)),
        ("long", Value::Long(-129)),
        ("float", Value::from(1.5_f32)),
        ("double", Value::from(-2.25_f64)),
        ("decimal32", Value::Decimal32(Dec32::from([1, 2, 3, 4]))),
        ("decimal64", Value::Decimal64(Dec64::from([5; 8]))),
        ("decimal128", Value::Decimal128(Dec128::from([6; 16]))),
        ("char", Value::Char('é')),
        (
            "timestamp",
            Value::Timestamp(Timestamp::from_milliseconds(-5)),
        ),
        ("uuid", Value::Uuid(uuid(9))),
        ("binary", Value::Binary(Binary::from(vec![1, 2, 3]))),
        ("binary-254", Value::Binary(Binary::from(vec![0; 254]))),
        ("binary-255", Value::Binary(Binary::from(vec![0; 255]))),
        ("string", Value::String("West".into())),
        ("string-long", Value::String(long_text.clone())),
        ("symbol", Value::Symbol(Symbol::from("orders"))),
        ("symbol-long", Value::Symbol(Symbol::from("s".repeat(300)))),
        ("list-empty", Value::List(vec![])),
        (
            "list",
            Value::List(vec![
                Value::Int(1),
                Value::String("two".into()),
                Value::Null,
            ]),
        ),
        (
            "list-long",
            Value::List(vec![Value::String(long_text.clone()), Value::Uint(300)]),
        ),
        ("map-empty", map(vec![])),
        (
            "map",
            map(vec![
                (Value::String("a".into()), Value::Long(1)),
                (Value::Symbol(Symbol::from("b")), Value::List(vec![])),
            ]),
        ),
        (
            "map-long",
            map(vec![(
                Value::Int(1),
                Value::Binary(Binary::from(vec![9; 300])),
            )]),
        ),
        ("array-empty", Value::Array(Array(vec![]))),
        (
            "array-int",
            Value::Array(Array(vec![Value::Int(1), Value::Int(-1), Value::Int(1000)])),
        ),
        (
            "array-ulong",
            Value::Array(Array(vec![Value::Ulong(0), Value::Ulong(7)])),
        ),
        (
            "array-bool",
            Value::Array(Array(vec![Value::Bool(true), Value::Bool(false)])),
        ),
        (
            "array-string",
            Value::Array(Array(vec![
                Value::String("ab".into()),
                Value::String("c".into()),
            ])),
        ),
        (
            "array-symbol",
            Value::Array(Array(vec![
                Value::Symbol(Symbol::from("x")),
                Value::Symbol(Symbol::from("yz")),
            ])),
        ),
        (
            "array-binary",
            Value::Array(Array(vec![Value::Binary(Binary::from(vec![1]))])),
        ),
        (
            "array-timestamp",
            Value::Array(Array(vec![Value::Timestamp(Timestamp::from_milliseconds(
                1,
            ))])),
        ),
        (
            "array-uuid",
            Value::Array(Array(vec![Value::Uuid(uuid(1)), Value::Uuid(uuid(2))])),
        ),
        (
            "array-long",
            Value::Array(Array((0..100).map(Value::Long).collect())),
        ),
        (
            "described-code",
            described(Descriptor::Code(0x77), Value::String("value".into())),
        ),
        (
            "described-name",
            described(Descriptor::Name(Symbol::from("x:y")), Value::Uint(3)),
        ),
    ];
    let mut properties = ApplicationProperties::default();
    for (name, value) in values {
        properties.insert(name, value);
    }
    properties
}

#[test]
fn system_properties_and_every_application_value_family_match_ingress() {
    let mut annotations = MessageAnnotations::default();
    annotations.insert(
        "x-opt-scheduled-enqueue-time",
        Value::Timestamp(Timestamp::from_milliseconds(1_700_000_000_000)),
    );
    annotations.insert(7_u64, String::from("unrelated"));
    message(&Message {
        header: Some(Header {
            ttl: Some(60_000),
            ..Header::default()
        }),
        delivery_annotations: None,
        message_annotations: Some(annotations),
        properties: Some(Properties {
            message_id: Some(MessageId::String("order-7".into())),
            correlation_id: Some(MessageId::String("correlation-7".into())),
            to: Some("logical-destination".into()),
            reply_to: Some("logical-reply".into()),
            subject: Some("orders.created".into()),
            group_id: Some("cart-7".into()),
            reply_to_group_id: Some("reply-cart-7".into()),
            content_type: Some(Symbol::from("application/json")),
            content_encoding: Some(Symbol::from("gzip")),
            absolute_expiry_time: Some(9),
            creation_time: Some(-9),
            group_sequence: Some(3),
            user_id: Some(Binary::from(vec![1])),
        }),
        application_properties: Some(every_value_family()),
        body: Body::Data(vec![
            Binary::from(b"one".to_vec()),
            Binary::from(b"two".to_vec()),
        ]),
        footer: None,
    });
}

#[test]
fn every_identifier_type_and_body_form_match_ingress() {
    let identifiers = [
        MessageId::Ulong(42),
        MessageId::Uuid(uuid(0xab)),
        MessageId::Binary(Binary::from(vec![0x00, 0x7f, 0xff])),
        MessageId::String(String::new()),
    ];
    for identifier in identifiers {
        for body in [
            Body::Empty,
            Body::Data(vec![Binary::from(vec![])]),
            Body::Sequence(vec![vec![Value::Int(1)], vec![]]),
            Body::Value(Value::String("body".into())),
        ] {
            message(&Message {
                properties: Some(Properties {
                    message_id: Some(identifier.clone()),
                    correlation_id: Some(identifier.clone()),
                    ..Properties::default()
                }),
                body,
                ..Message::default()
            });
        }
    }
    // No properties section, an empty properties section and an empty header.
    message(&Message::default());
    message(&Message {
        header: Some(Header::default()),
        properties: Some(Properties::default()),
        application_properties: Some(ApplicationProperties::default()),
        ..Message::default()
    });
}

#[test]
fn repeated_keys_and_noncompact_encodings_match_ingress() {
    let mut bytes = Vec::new();
    // Message annotations repeating the schedule key; the last value wins.
    let schedule = b"x-opt-scheduled-enqueue-time";
    let mut annotations = Vec::new();
    for millis in [5_u64, 9] {
        annotations.extend([0xa3, schedule.len() as u8]);
        annotations.extend(schedule);
        annotations.push(0x83);
        annotations.extend(millis.to_be_bytes());
    }
    bytes.extend([0x00, 0x53, 0x72, 0xc1, annotations.len() as u8 + 1, 4]);
    bytes.extend(&annotations);
    // Properties with a wide message-id string and wide list framing.
    let mut fields = vec![0xb1, 0, 0, 0, 2, b'i', b'd'];
    fields.extend([0x40; 9]);
    fields.extend([0xb1, 0, 0, 0, 1, b's']);
    bytes.extend([0x00, 0x53, 0x73, 0xd0]);
    bytes.extend((fields.len() as u32 + 4).to_be_bytes());
    bytes.extend(11_u32.to_be_bytes());
    bytes.extend(&fields);
    // Application properties repeating "a", with wide and compact integers
    // and a wide string the canonical encoding narrows.
    let mut application = vec![0xa1, 1, b'a', 0x71, 0, 0, 0, 1];
    application.extend([0xa1, 1, b'b', 0xb1, 0, 0, 0, 1, b'x']);
    application.extend([0xa1, 1, b'a', 0x81, 0, 0, 0, 0, 0, 0, 0, 2]);
    application.extend([0xa1, 1, b'c', 0x70, 0, 0, 0, 0]);
    bytes.extend([0x00, 0x53, 0x74, 0xc1, application.len() as u8 + 1, 8]);
    bytes.extend(&application);
    bytes.extend([0x00, 0x53, 0x75, 0xb0, 0, 0, 0, 1, 0x2a]);
    assert_parity(&bytes);
}

/// Arrays as clients encode them, with a shared element constructor. Ingress
/// accepts these scalar arrays; it refuses compound and described elements.
#[test]
fn spec_encoded_scalar_arrays_match_ingress() {
    for array in [
        // ASCII string arrays, where char and byte lengths agree, so the
        // derived projection matches ingress byte for byte. Non-ASCII string
        // arrays are a deliberate divergence covered below.
        &[0xe0, 7, 2, 0xa1, 2, b'a', b'b', 1, b'c'][..],
        &[0xe0, 13, 2, 0xb1, 0, 0, 0, 2, b'a', b'b', 0, 0, 0, 1, b'c'],
        &[0xe0, 4, 2, 0x54, 1, 2],
        &[0xe0, 2, 2, 0x41],
    ] {
        let mut map = vec![0xa1, 1, b'k'];
        map.extend(array);
        let mut bytes = vec![0x00, 0x53, 0x74, 0xc1, map.len() as u8 + 1, 2];
        bytes.extend(&map);
        assert_parity(&bytes);
    }
}

#[test]
fn a_string_array_with_a_non_ascii_element_refuses_at_derive() {
    use domain::MessageContentError;
    // application-properties { "k": ["é", "x"] } as a str8-element array.
    let bytes = [
        0x00, 0x53, 0x74, // application-properties
        0xc1, 0x0d, 0x02, // map8, size 13, count 2
        0xa1, 0x01, b'k', // key "k"
        0xe0, 0x07, 0x02, 0xa1, 0x02, 0xc3, 0xa9, 0x01, 0x78, // ["é","x"]
    ];
    // Legacy ingress stores these (its serializer writes a character count for
    // array string elements), but those bytes are not decodable AMQP for a
    // non-ASCII element and would block snapshot capture and store upgrade, so
    // the content send refuses them instead.
    assert_eq!(
        MessageContent::Amqp(bytes.to_vec()).derive(),
        Err(MessageContentError::UnsupportedArrayElement)
    );
}
