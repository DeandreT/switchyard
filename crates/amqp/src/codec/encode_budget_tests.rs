use super::*;
use crate::value_codec::{MAX_VALUE_ELEMENTS, MAX_VALUE_NESTING};

fn value_message(value: Value) -> Message {
    Message {
        body: Body::Value(value),
        ..Message::default()
    }
}

fn parity(message: &Message) -> Vec<u8> {
    let legacy = encode_message_legacy(message).expect("legacy fixture encodes");
    let before = encoded_message_buffer_allocations();
    let prepared = prepare_message(message).expect("borrowed preparation");
    assert_eq!(prepared.encoded_len(), legacy.len());
    assert_eq!(encoded_message_buffer_allocations(), before);
    let encoded = prepared.encode().expect("borrowed output");
    assert_eq!(encoded, legacy);
    assert_eq!(
        encoded_message_buffer_allocations(),
        before + usize::from(!legacy.is_empty())
    );
    encoded
}

fn bound(message: &Message) {
    let expected = parity(message);
    assert_eq!(
        crate::encode_message_with_max_size(message, expected.len()).expect("exact cap"),
        expected
    );
    if !expected.is_empty() {
        let before = encoded_message_buffer_allocations();
        let error = crate::encode_message_with_max_size(message, expected.len() - 1)
            .expect_err("one byte short");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let detail = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<MessageSizeError>())
            .expect("typed exact-size refusal");
        assert_eq!(detail.size, expected.len());
        assert_eq!(detail.maximum, expected.len() - 1);
        assert_eq!(encoded_message_buffer_allocations(), before);
    }
}

fn scalars() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Ubyte(255),
        Value::Ushort(65535),
        Value::Uint(0),
        Value::Uint(1),
        Value::Uint(255),
        Value::Uint(256),
        Value::Uint(u32::MAX),
        Value::Ulong(0),
        Value::Ulong(255),
        Value::Ulong(256),
        Value::Ulong(u64::MAX),
        Value::Byte(-128),
        Value::Short(i16::MIN),
        Value::Int(-129),
        Value::Int(-128),
        Value::Int(127),
        Value::Int(128),
        Value::Int(i32::MIN),
        Value::Long(-129),
        Value::Long(-128),
        Value::Long(127),
        Value::Long(128),
        Value::Long(i64::MIN),
        Value::Float((-0.0_f32).into()),
        Value::Float(f32::INFINITY.into()),
        Value::Double((-0.0_f64).into()),
        Value::Double(f64::NEG_INFINITY.into()),
        Value::Decimal32([1, 2, 3, 4].into()),
        Value::Decimal64([7; 8].into()),
        Value::Decimal128([9; 16].into()),
        Value::Char('Z'),
        Value::Char('\u{1f642}'),
        Value::Timestamp(Timestamp::from_milliseconds(i64::MIN)),
        Value::Uuid([5; 16].into()),
        Value::Binary(vec![1, 2].into()),
        Value::String("text".into()),
        Value::String("caf\u{e9}".into()),
        Value::Symbol("name".into()),
    ]
}

#[test]
fn all_scalar_encodings_match_existing_canonical_bytes_and_exact_caps() {
    for value in scalars() {
        bound(&value_message(value));
    }
}

#[test]
fn binary_string_and_symbol_width_transitions_match_without_copying_during_measurement() {
    for length in [0, 1, 254, 255, 256, 257, 4096] {
        for value in [
            Value::Binary(vec![7; length].into()),
            Value::String("x".repeat(length)),
            Value::Symbol("s".repeat(length).into()),
        ] {
            bound(&value_message(value));
        }
        bound(&Message::data(vec![8; length]));
    }
}

#[test]
fn lists_and_maps_keep_empty_short_long_and_count_boundaries() {
    for count in [0, 1, 126, 127, 128, 254, 255, 256, 300] {
        bound(&value_message(Value::List(vec![Value::Null; count])));
        let mut map = OrderedMap::new();
        for index in 0..count {
            map.insert(Value::Uint(index as u32), Value::Null);
        }
        bound(&value_message(Value::Map(map)));
    }
}

#[test]
fn nested_long_parent_child_expansion_stays_inside_the_single_measured_buffer() {
    let mut map = OrderedMap::new();
    map.insert(
        Value::String("nested".into()),
        Value::List(vec![Value::Binary(vec![3; 1024].into()); 3]),
    );
    map.insert(
        Value::String("array".into()),
        Value::Array(
            vec![
                Value::List(vec![Value::String("a".repeat(512))]),
                Value::List(vec![Value::String("b".repeat(1024))]),
            ]
            .into(),
        ),
    );
    let mut value = Value::Map(map);
    for _ in 0..12 {
        value = Value::List(vec![Value::Null, value]);
    }
    bound(&value_message(value));
}

#[test]
fn array_elements_keep_fixed_constructors_and_canonical_payloads_for_every_scalar_type() {
    for value in scalars() {
        bound(&value_message(Value::Array(
            vec![value.clone(), value].into(),
        )));
    }
    for values in [
        vec![Value::Uint(0), Value::Uint(255), Value::Uint(u32::MAX)],
        vec![Value::Int(-128), Value::Int(128), Value::Int(i32::MIN)],
        vec![Value::String(String::new()), Value::String("x".repeat(256))],
        vec![
            Value::Binary(Vec::new().into()),
            Value::Binary(vec![0; 256].into()),
        ],
    ] {
        bound(&value_message(Value::Array(values.into())));
    }
}

#[test]
fn array_collections_and_nested_arrays_share_only_the_correct_outer_constructor() {
    for count in [1, 254, 255, 256] {
        bound(&value_message(Value::Array(
            vec![Value::Null; count].into(),
        )));
    }
    let mut small = OrderedMap::new();
    small.insert(Value::String("one".into()), Value::Uint(0));
    let mut large = OrderedMap::new();
    large.insert(
        Value::String("two".into()),
        Value::Binary(vec![5; 1024].into()),
    );
    for values in [
        vec![Value::List(Vec::new()), Value::List(vec![Value::Null; 300])],
        vec![Value::Map(small), Value::Map(large)],
        vec![
            Value::Array(vec![Value::Uint(0), Value::Uint(256)].into()),
            Value::Array(vec![Value::String("other inner constructor".into())].into()),
        ],
    ] {
        bound(&value_message(Value::Array(values.into())));
    }
}

#[test]
fn described_and_named_constructor_chains_match_for_scalars_and_shared_arrays() {
    for descriptor in [
        Descriptor::Code(0),
        Descriptor::Code(255),
        Descriptor::Code(256),
        Descriptor::Code(u64::MAX),
        Descriptor::Name("type".into()),
        Descriptor::Name("n".repeat(256).into()),
    ] {
        let nested = |value| {
            Value::Described(Box::new(Described {
                descriptor: descriptor.clone(),
                value: Value::Described(Box::new(Described {
                    descriptor: Descriptor::Code(7),
                    value,
                })),
            }))
        };
        bound(&value_message(nested(Value::Uint(0))));
        bound(&value_message(Value::Array(
            vec![nested(Value::Uint(0)), nested(Value::Uint(256))].into(),
        )));
    }
}

#[test]
fn malformed_arrays_and_symbols_fail_before_any_output_buffer_reservation() {
    let described = |code, value| {
        Value::Described(Box::new(Described {
            descriptor: Descriptor::Code(code),
            value,
        }))
    };
    for value in [
        Value::Array(Vec::<Value>::new().into()),
        Value::Array(vec![Value::Uint(0), Value::Ulong(0)].into()),
        Value::Array(vec![described(1, Value::Uint(0)), described(2, Value::Uint(0))].into()),
        Value::Array(
            vec![
                described(1, Value::Uint(0)),
                described(1, Value::String("x".into())),
            ]
            .into(),
        ),
        Value::Array(
            vec![
                described(1, described(2, Value::Null)),
                described(1, described(3, Value::Null)),
            ]
            .into(),
        ),
        Value::Symbol("caf\u{e9}".into()),
        Value::Described(Box::new(Described {
            descriptor: Descriptor::Name("caf\u{e9}".into()),
            value: Value::Null,
        })),
    ] {
        let message = value_message(value);
        let before = encoded_message_buffer_allocations();
        let old = encode_message_legacy(&message).expect_err("invalid fixture");
        let new = encode_message_with_max_size(&message, 0)
            .expect_err("preparation rejects malformed value");
        assert_eq!(new.kind(), old.kind());
        assert_eq!(new.to_string(), old.to_string());
        assert_eq!(encoded_message_buffer_allocations(), before);
    }
}

#[test]
fn every_message_section_and_ordered_metadata_map_obeys_exact_caps() {
    let mut annotations = Annotations::new();
    annotations.insert(AnnotationKey::Ulong(0), Value::Binary(vec![8; 256].into()));
    annotations.insert(
        AnnotationKey::Symbol("key".into()),
        Value::String("retained".into()),
    );
    let mut application = ApplicationProperties::default();
    application.insert("first", Value::Uint(1));
    application.insert("second", Value::List(vec![Value::String("x".repeat(256))]));
    let properties = Properties {
        message_id: Some(MessageId::String("message".into())),
        user_id: Some(vec![2; 256].into()),
        to: Some("queue".into()),
        subject: Some("subject".into()),
        reply_to: Some("reply".into()),
        correlation_id: Some(MessageId::Uuid([3; 16].into())),
        content_type: Some("application/octet-stream".into()),
        content_encoding: Some("binary".into()),
        absolute_expiry_time: Some(i64::MIN),
        creation_time: Some(i64::MAX),
        group_id: Some("group".into()),
        group_sequence: Some(u32::MAX),
        reply_to_group_id: Some(String::new()),
    };
    let sections = [
        Message {
            header: Some(Header::default()),
            ..Message::default()
        },
        Message {
            delivery_annotations: Some(annotations.clone()),
            ..Message::default()
        },
        Message {
            message_annotations: Some(annotations.clone()),
            ..Message::default()
        },
        Message {
            properties: Some(properties.clone()),
            ..Message::default()
        },
        Message {
            application_properties: Some(application.clone()),
            ..Message::default()
        },
        Message {
            body: Body::Data(vec![Vec::new().into(), vec![4; 256].into()]),
            ..Message::default()
        },
        Message {
            body: Body::Sequence(vec![Vec::new(), vec![Value::Null; 300]]),
            ..Message::default()
        },
        Message {
            footer: Some(annotations.clone()),
            ..Message::default()
        },
    ];
    for section in &sections {
        bound(section);
    }
    bound(&Message {
        header: Some(Header {
            durable: true,
            priority: 255,
            ttl: Some(u32::MAX),
            first_acquirer: true,
            delivery_count: 256,
        }),
        delivery_annotations: Some(annotations.clone()),
        message_annotations: Some(annotations.clone()),
        properties: Some(properties),
        application_properties: Some(application),
        body: Body::Data(vec![vec![5; 512].into(), vec![6; 256].into()]),
        footer: Some(annotations),
    });
}

#[test]
fn properties_trim_only_trailing_nulls_and_preserve_every_message_id_type() {
    bound(&Message {
        properties: Some(Properties::default()),
        ..Message::default()
    });
    for id in [
        MessageId::Ulong(0),
        MessageId::Ulong(u64::MAX),
        MessageId::Uuid([1; 16].into()),
        MessageId::Binary(vec![7; 256].into()),
        MessageId::String(String::new()),
    ] {
        bound(&Message {
            properties: Some(Properties {
                message_id: Some(id.clone()),
                ..Properties::default()
            }),
            ..Message::default()
        });
        bound(&Message {
            properties: Some(Properties {
                correlation_id: Some(id),
                reply_to_group_id: Some(String::new()),
                ..Properties::default()
            }),
            ..Message::default()
        });
    }
}

#[test]
fn each_individual_property_preserves_its_position_and_present_empty_or_zero_value() {
    for index in 0..13 {
        let mut properties = Properties::default();
        match index {
            0 => properties.message_id = Some(MessageId::Ulong(0)),
            1 => properties.user_id = Some(Vec::new().into()),
            2 => properties.to = Some(String::new()),
            3 => properties.subject = Some(String::new()),
            4 => properties.reply_to = Some(String::new()),
            5 => properties.correlation_id = Some(MessageId::Binary(Vec::new().into())),
            6 => properties.content_type = Some("".into()),
            7 => properties.content_encoding = Some("".into()),
            8 => properties.absolute_expiry_time = Some(0),
            9 => properties.creation_time = Some(0),
            10 => properties.group_id = Some(String::new()),
            11 => properties.group_sequence = Some(0),
            12 => properties.reply_to_group_id = Some(String::new()),
            _ => unreachable!("thirteen properties"),
        }
        bound(&Message {
            properties: Some(properties),
            ..Message::default()
        });
    }
}

#[test]
fn empty_metadata_sections_are_present_and_invalid_metadata_symbols_fail_during_measurement() {
    for message in [
        Message {
            delivery_annotations: Some(Annotations::new()),
            ..Message::default()
        },
        Message {
            message_annotations: Some(Annotations::new()),
            ..Message::default()
        },
        Message {
            application_properties: Some(ApplicationProperties::default()),
            ..Message::default()
        },
        Message {
            footer: Some(Annotations::new()),
            ..Message::default()
        },
    ] {
        assert!(!parity(&message).is_empty());
        bound(&message);
    }
    let mut annotations = Annotations::new();
    annotations.insert(AnnotationKey::Symbol("caf\u{e9}".into()), Value::Null);
    for message in [
        Message {
            delivery_annotations: Some(annotations.clone()),
            ..Message::default()
        },
        Message {
            message_annotations: Some(annotations.clone()),
            ..Message::default()
        },
        Message {
            footer: Some(annotations),
            ..Message::default()
        },
        Message {
            properties: Some(Properties {
                content_type: Some("caf\u{e9}".into()),
                ..Properties::default()
            }),
            ..Message::default()
        },
        Message {
            properties: Some(Properties {
                content_encoding: Some("caf\u{e9}".into()),
                ..Properties::default()
            }),
            ..Message::default()
        },
    ] {
        let before = encoded_message_buffer_allocations();
        assert!(prepare_message(&message).is_err());
        assert_eq!(encoded_message_buffer_allocations(), before);
    }
}

#[test]
fn prepared_write_checks_capacity_and_reservation_failure_before_running_the_writer() {
    use super::value_encoder::{Field, write_prepared};

    let called = std::cell::Cell::new(false);
    let before = encoded_message_buffer_allocations();
    let error = write_prepared(usize::MAX, |_| {
        called.set(true);
        Ok(())
    })
    .expect_err("impossible allocation");
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert!(!called.get());
    assert_eq!(encoded_message_buffer_allocations(), before);
    assert!(
        write_prepared(1, |encoder| encoder.field(Field::Binary(&[1, 2])))
            .expect_err("writer cannot expand beyond its prepared maximum")
            .to_string()
            .contains("length exceeded")
    );
    assert!(
        write_prepared(2, |encoder| encoder.field(Field::Null))
            .expect_err("writer must finish at its exact prepared length")
            .to_string()
            .contains("length changed")
    );
}

#[test]
fn empty_message_and_null_value_are_distinct_and_zero_limit_allocates_nothing() {
    let empty = Message::default();
    let before = encoded_message_buffer_allocations();
    assert!(
        encode_message_with_max_size(&empty, 0)
            .expect("empty output")
            .is_empty()
    );
    assert_eq!(encoded_message_buffer_allocations(), before);
    let null = value_message(Value::Null);
    assert_eq!(parity(&null), vec![0x00, 0x53, 0x77, 0x40]);
    bound(&null);
}

#[test]
fn oversized_binary_is_measured_exactly_without_allocating_a_temporary_output() {
    let message = Message::data(vec![3; 8 * 1024 * 1024]);
    let before = encoded_message_buffer_allocations();
    let error = encode_message_with_max_size(&message, 64).expect_err("early size check");
    let size = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<MessageSizeError>())
        .expect("typed size");
    assert_eq!(size.size, 8 * 1024 * 1024 + 8);
    assert_eq!(size.maximum, 64);
    assert_eq!(encoded_message_buffer_allocations(), before);
}

#[test]
fn zero_width_array_values_and_multiple_sections_share_one_bounded_visit_budget() {
    let message = value_message(Value::Array(
        vec![Value::Null; MAX_VALUE_ELEMENTS - 2].into(),
    ));
    assert!(prepare_message(&message).is_ok());
    bound(&message);
    let excessive = value_message(Value::Array(
        vec![Value::Null; MAX_VALUE_ELEMENTS - 1].into(),
    ));
    let before = encoded_message_buffer_allocations();
    let error = prepare_message(&excessive)
        .err()
        .expect("one too many visits");
    assert!(error.to_string().contains("element limit"));
    assert_eq!(encoded_message_buffer_allocations(), before);
    let sections = Message {
        body: Body::Sequence(vec![
            vec![Value::Null; MAX_VALUE_ELEMENTS / 2],
            vec![Value::Null; MAX_VALUE_ELEMENTS / 2],
        ]),
        ..Message::default()
    };
    assert!(prepare_message(&sections).is_err());
    assert_eq!(encoded_message_buffer_allocations(), before);
}

#[test]
fn exact_depth_is_allowed_and_one_more_level_fails_without_output_allocation() {
    let mut value = Value::Null;
    for _ in 0..MAX_VALUE_NESTING - 1 {
        value = Value::List(vec![value]);
    }
    let message = value_message(value);
    assert!(prepare_message(&message).is_ok());
    bound(&message);
    let Body::Value(value) = message.body else {
        panic!("value body")
    };
    let excessive = value_message(Value::List(vec![value]));
    let before = encoded_message_buffer_allocations();
    assert!(
        prepare_message(&excessive)
            .err()
            .expect("depth check")
            .to_string()
            .contains("nesting limit")
    );
    assert_eq!(encoded_message_buffer_allocations(), before);
}
