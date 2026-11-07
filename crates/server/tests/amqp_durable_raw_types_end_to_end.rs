//! AMQP value types the Service Bus SDKs do not expose, sent as raw AMQP and
//! read back from the durable backend after restarts.
//!
//! Values compare by their encoding: `OrderedFloat` equality treats every NaN
//! as equal and `-0.0` as `0.0`, and would hide a lost payload or sign.

use amqp::{
    ApplicationProperties, Array, Binary, Body, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Fields, Footer, Message, MessageId, Modified, OrderedMap, Properties,
    Symbol, Uuid, Value, encode_message,
};
use domain::{CommandKind, QueueConfig};
use serde_amqp::primitives::{Dec32, Dec64, Dec128, Timestamp};

#[allow(dead_code)]
mod durable_node;

use durable_node::{DurableNode, Expect, TestResult, assert_renders, assert_stored, next};

const FLOAT_NAN: u32 = 0x7fc0_1234;
const DOUBLE_NAN: u64 = 0x7ff8_0000_dead_beef;
const PATCHED_FLOAT_NAN: u32 = 0x7fc0_4321;
/// 0001-01-01T00:00:00Z.
const PATCHED_TIMESTAMP: i64 = -62_135_596_800_000;

fn symbol(text: &str) -> Value {
    Value::Symbol(Symbol::from(text))
}

fn string(text: &str) -> Value {
    Value::String(text.to_owned())
}

fn value_map(entries: Vec<(Value, Value)>) -> Value {
    let mut map = OrderedMap::new();
    for (key, value) in entries {
        map.insert(key, value);
    }
    Value::Map(map)
}

fn float(bits: u32) -> Value {
    Value::from(f32::from_bits(bits))
}

fn double(bits: u64) -> Value {
    Value::from(f64::from_bits(bits))
}

fn timestamp(millis: i64) -> Value {
    Value::Timestamp(Timestamp::from_milliseconds(millis))
}

/// Every raw value under test, by name.
fn raw_values() -> Vec<(&'static str, Value)> {
    vec![
        ("as-string", string("orders")),
        ("as-symbol", symbol("orders")),
        (
            "string-and-symbol-keys",
            value_map(vec![
                (string("k"), Value::Int(1)),
                (symbol("k"), Value::Int(2)),
            ]),
        ),
        (
            "decimal32",
            Value::Decimal32(Dec32::from([0x22, 0x50, 0x00, 0x01])),
        ),
        (
            "decimal64",
            Value::Decimal64(Dec64::from([0x22, 0x38, 0, 0, 0, 0, 0, 0x7b])),
        ),
        (
            "decimal128",
            Value::Decimal128(Dec128::from([
                0x22, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0xd2,
            ])),
        ),
        ("float-nan-payload", float(FLOAT_NAN)),
        ("float-negative-zero", float((-0.0_f32).to_bits())),
        (
            "float-negative-infinity",
            float(f32::NEG_INFINITY.to_bits()),
        ),
        ("float-subnormal", float(1)),
        ("double-nan-payload", double(DOUBLE_NAN)),
        ("double-negative-zero", double((-0.0_f64).to_bits())),
        ("double-max", double(f64::MAX.to_bits())),
        ("before-epoch", timestamp(-86_400_000)),
        ("one-before-epoch", timestamp(-1)),
        ("char", Value::Char('\u{1f680}')),
        ("ulong-max", Value::Ulong(u64::MAX)),
        ("long-min", Value::Long(i64::MIN)),
        ("byte", Value::Byte(-128)),
        ("ushort", Value::Ushort(u16::MAX)),
        (
            "nested",
            value_map(vec![(
                string("level-1"),
                Value::List(vec![value_map(vec![
                    (
                        symbol("level-2"),
                        Value::List(vec![
                            symbol("deep"),
                            Value::Array(Array::from(vec![Value::Long(-1), Value::Long(2)])),
                            value_map(vec![(
                                Value::Ulong(1),
                                Value::Decimal64(Dec64::from([1; 8])),
                            )]),
                        ]),
                    ),
                    (
                        Value::Int(3),
                        Value::List(vec![Value::Null, double(DOUBLE_NAN)]),
                    ),
                ])]),
            )]),
        ),
        (
            "timestamps",
            Value::Array(Array::from(vec![timestamp(-1), timestamp(0), timestamp(1)])),
        ),
        (
            "doubles",
            Value::Array(Array::from(vec![
                double(DOUBLE_NAN),
                double((-0.0_f64).to_bits()),
            ])),
        ),
    ]
}

fn raw_message(message_id: MessageId, correlation_id: MessageId, body: Body) -> Message {
    let mut application = ApplicationProperties::default();
    for (name, value) in raw_values() {
        application.insert(name, value);
    }
    let mut footer = Footer::default();
    footer.insert("x-nan", double(DOUBLE_NAN));
    footer.insert("x-decimal", Value::Decimal128(Dec128::from([9; 16])));
    Message {
        properties: Some(Properties {
            message_id: Some(message_id),
            correlation_id: Some(correlation_id),
            creation_time: Some(-1),
            ..Properties::default()
        }),
        application_properties: Some(application),
        body,
        footer: Some(footer),
        ..Message::default()
    }
}

fn raw_body_values() -> Vec<Value> {
    raw_values().into_iter().map(|(_, value)| value).collect()
}

const UUID_BYTES: [u8; 16] = [
    0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x0f, 0xed, 0xcb, 0xa9, 0x87, 0x65, 0x43, 0x21,
];

fn messages() -> Vec<Message> {
    let uuid = Uuid::from(UUID_BYTES);
    vec![
        raw_message(
            MessageId::Ulong(u64::MAX),
            MessageId::Uuid(uuid.clone()),
            Body::Value(Value::List(raw_body_values())),
        ),
        raw_message(
            MessageId::Uuid(uuid),
            MessageId::Binary(Binary::from(vec![0_u8, 255, 0])),
            Body::Sequence(vec![raw_body_values(), vec![symbol("tail")]]),
        ),
        raw_message(
            MessageId::Binary(Binary::from(vec![0_u8, 255, 1])),
            MessageId::Ulong(0),
            Body::Data(vec![
                Binary::from(vec![0_u8; 3]),
                Binary::from(vec![255_u8]),
            ]),
        ),
    ]
}

/// A raw patch: string and symbol values swapped, bit patterns and nesting.
fn raw_patch() -> Vec<(&'static str, Value)> {
    vec![
        ("as-string", symbol("orders")),
        ("as-symbol", string("orders")),
        ("float-nan-payload", float(PATCHED_FLOAT_NAN)),
        ("double-negative-zero", double((-0.0_f64).to_bits())),
        ("before-epoch", timestamp(PATCHED_TIMESTAMP)),
        (
            "patched-nested",
            Value::List(vec![value_map(vec![(
                symbol("k"),
                Value::Array(Array::from(vec![
                    Value::Decimal32(Dec32::from([0x80, 0, 0, 0])),
                    Value::Decimal32(Dec32::from([0x7c, 0, 0, 0])),
                ])),
            )])]),
        ),
    ]
}

/// Spells out the bit patterns and type distinctions that `assert_renders`
/// already compares encoded, so a failure names the value that changed.
fn assert_bits(message: &Message, patched: bool) {
    let properties = message
        .application_properties
        .as_ref()
        .expect("application properties");
    let float_bits = |name: &str| match properties.get(name) {
        Some(Value::Float(value)) => value.0.to_bits(),
        other => panic!("{name} is {other:?}"),
    };
    let double_bits = |name: &str| match properties.get(name) {
        Some(Value::Double(value)) => value.0.to_bits(),
        other => panic!("{name} is {other:?}"),
    };
    assert_eq!(
        float_bits("float-nan-payload"),
        if patched {
            PATCHED_FLOAT_NAN
        } else {
            FLOAT_NAN
        }
    );
    assert_eq!(float_bits("float-negative-zero"), (-0.0_f32).to_bits());
    assert_eq!(double_bits("double-nan-payload"), DOUBLE_NAN);
    assert_eq!(double_bits("double-negative-zero"), (-0.0_f64).to_bits());
    let (string_kind, symbol_kind) = if patched {
        (symbol("orders"), string("orders"))
    } else {
        (string("orders"), symbol("orders"))
    };
    assert_eq!(properties.get("as-string"), Some(&string_kind));
    assert_eq!(properties.get("as-symbol"), Some(&symbol_kind));
    assert_eq!(
        properties.get("before-epoch"),
        Some(&timestamp(if patched {
            PATCHED_TIMESTAMP
        } else {
            -86_400_000
        }))
    );
}

fn patch_fields(patch: &[(&str, Value)]) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in patch {
        fields.insert(Symbol::from(*name), value.clone());
    }
    fields
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_types_round_trip_bit_for_bit_through_restarts_and_patches() -> TestResult {
    let mut node = DurableNode::create(vec![(
        "orders",
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )])
    .await?;
    let originals = messages();
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "raw-sender", "orders").await?;
        for message in &originals {
            let outcome = sender.send(message.clone()).await?;
            assert!(matches!(outcome, amqp::Outcome::Accepted(_)), "{outcome:?}");
        }
        connection.close().await?;
    }
    // The broker's own identifier is the text form ingress has always derived
    // from a typed one; filters and duplicate detection compare it.
    let expected_ids = [
        u64::MAX.to_string(),
        format!("Uuid({:?})", UUID_BYTES),
        String::from("00ff01"),
    ];
    node.reopen(|machine| {
        for (index, original) in originals.iter().enumerate() {
            let record = assert_stored(
                machine,
                "orders",
                index as i64 + 1,
                &encode_message(original)?,
                &[],
            )?;
            assert_eq!(record.message_id, expected_ids[index]);
        }
        Ok(())
    })
    .await?;

    let patch = raw_patch();
    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut receiver = Receiver::attach(&mut session, "raw-receiver", "orders").await?;
        for original in &originals {
            let delivery = next(&mut receiver).await?;
            assert_renders(
                delivery.message(),
                delivery.encoded_message(),
                original,
                Expect {
                    locked: true,
                    ..Expect::default()
                },
            );
            assert_eq!(
                delivery
                    .message()
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.message_id.clone()),
                original
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.message_id.clone()),
                "the typed message-id survives"
            );
            assert_bits(delivery.message(), false);
            // Deferring keeps the copy out of the link's way across restarts.
            receiver
                .modify(
                    &delivery,
                    Modified {
                        undeliverable_here: Some(true),
                        message_annotations: Some(patch_fields(&patch)),
                        ..Modified::default()
                    },
                )
                .await?;
        }
        durable_node::Management::attach(&mut session, "orders")
            .await?
            .wait_until("the deferrals apply", |entries| {
                (1..=3).all(|sequence| durable_node::holds(entries, sequence, Some(1)))
            })
            .await?;
        connection.close().await?;
    }

    node.reopen(|machine| {
        for (index, original) in originals.iter().enumerate() {
            assert_stored(
                machine,
                "orders",
                index as i64 + 1,
                &encode_message(original)?,
                &patch,
            )?;
        }
        Ok(())
    })
    .await?;

    {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut management = durable_node::Management::attach(&mut session, "orders").await?;
        let peeked = management.peek(1, 10).await?;
        assert_eq!(peeked.len(), originals.len());
        for (entry, original) in peeked.iter().zip(&originals) {
            assert_renders(
                &entry.message,
                &entry.encoded,
                original,
                Expect {
                    delivery_count: 1,
                    overlay: &patch,
                    state: Some(1),
                    ..Expect::default()
                },
            );
            assert_bits(&entry.message, true);
        }
        for (index, original) in originals.iter().enumerate() {
            let (status, entries) = management.receive_deferred(index as i64 + 1).await?;
            assert_eq!(status, 200);
            let [entry] = entries.try_into().map_err(|_| "one deferred message")?;
            assert_renders(
                &entry.message,
                &entry.encoded,
                original,
                Expect {
                    delivery_count: 1,
                    locked: true,
                    overlay: &patch,
                    state: Some(1),
                    ..Expect::default()
                },
            );
            assert_bits(&entry.message, true);
            let (status, response) = management
                .update_disposition("unused", entry.lock.ok_or("a lock")?, "completed", &[])
                .await?;
            assert_eq!(status, 200, "{response:?}");
        }
        connection.close().await?;
    }
    node.reopen(|machine| {
        assert!(
            machine
                .deferred_sequences(
                    &durable_node::namespace(),
                    &durable_node::entity("orders"),
                    10
                )?
                .is_empty()
        );
        Ok(())
    })
    .await
}
