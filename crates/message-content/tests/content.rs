use message_content::{
    AnnotationKey, Array, Binary, Body, Constructor, Content, Descriptor, EncodedContent, Error,
    Field, Header, Limits, MessageId, PrimitiveConstructor, Properties, Symbol, Text, Value, Width,
    decode_content, encode_content,
};

fn limits() -> Limits {
    Limits {
        max_bytes: 100_000,
        max_depth: 128,
        max_nodes: 10_000,
        max_work: 200_000,
    }
}
fn text(s: &str) -> Text {
    Text {
        text: s.into(),
        width: Width::One,
    }
}
fn symbol(s: &str) -> Symbol {
    Symbol {
        text: s.into(),
        width: Width::One,
    }
}
fn binary(bytes: &[u8]) -> Binary {
    Binary {
        bytes: bytes.into(),
        width: Width::One,
    }
}

fn roundtrip(content: &Content) {
    let wire = encode_content(content, limits()).unwrap();
    assert_eq!(&decode_content(&wire, limits()).unwrap(), content);
}

#[test]
fn independent_message_fixture_has_all_header_fields_and_thirteen_properties() {
    // AMQP list8 composites, deliberately unlike the semantic encoder's list32 output.
    let wire = [
        0, 0x53, 0x70, 0xc0, 8, 5, 0x41, 0x50, 8, 0x52, 9, 0x42, 0x43, 0, 0x53, 0x71, 0xc1, 8, 2,
        0xa3, 1, b'k', 0xc0, 2, 1, 0x40, 0, 0x53, 0x72, 0xc1, 4, 2, 0x53, 7, 0x41, 0, 0x53, 0x73,
        0xc0, 64, 13, 0x53, 9, 0xa0, 1, 1, 0xa1, 1, b't', 0xa1, 1, b's', 0xa1, 1, b'r', 0x98, 0, 1,
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0xa3, 1, b'c', 0xa3, 1, b'e', 0x83, 255,
        255, 255, 255, 255, 255, 255, 254, 0x83, 255, 255, 255, 255, 255, 255, 255, 253, 0xa1, 1,
        b'g', 0x52, 2, 0xa1, 1, b'z', 0, 0x53, 0x74, 0xc1, 8, 2, 0xa1, 1, b'p', 0xc0, 2, 1, 0x40,
        0, 0x53, 0x75, 0xa0, 0, 0, 0x53, 0x75, 0xa0, 1, b'A', 0, 0x53, 0x78, 0xc1, 1, 0,
    ];
    let expected = Content {
        header: Some(Header {
            durable: Field::Value(true),
            priority: Field::Value(8),
            ttl: Field::Value(9),
            first_acquirer: Field::Value(false),
            delivery_count: Field::Value(0),
        }),
        delivery_annotations: Some(vec![(
            AnnotationKey::Symbol(symbol("k")),
            Value::List(vec![Value::Null]),
        )]),
        message_annotations: Some(vec![(AnnotationKey::ULong(7), Value::Bool(true))]),
        properties: Some(Properties {
            message_id: Field::Value(MessageId::ULong(9)),
            user_id: Field::Value(binary(&[1])),
            to: Field::Value(text("t")),
            subject: Field::Value(text("s")),
            reply_to: Field::Value(text("r")),
            correlation_id: Field::Value(MessageId::Uuid([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            ])),
            content_type: Field::Value(symbol("c")),
            content_encoding: Field::Value(symbol("e")),
            absolute_expiry_time: Field::Value(-2),
            creation_time: Field::Value(-3),
            group_id: Field::Value(text("g")),
            group_sequence: Field::Value(2),
            reply_to_group_id: Field::Value(text("z")),
        }),
        application_properties: Some(vec![(text("p"), Value::List(vec![Value::Null]))]),
        body: Body::Data(vec![binary(&[]), binary(b"A")]),
        footer: Some(vec![]),
    };
    assert_eq!(decode_content(&wire, limits()).unwrap(), expected);
    roundtrip(&expected);
    let retained = EncodedContent::from_bytes(&wire, limits()).unwrap();
    assert_eq!(retained.original_bytes(), wire);
    assert_ne!(
        encode_content(&retained.view(limits()).unwrap(), limits()).unwrap(),
        wire
    );
}

#[test]
fn ids_are_typed_without_coercion_in_both_identifier_fields() {
    for id in [
        MessageId::ULong(u64::MAX),
        MessageId::String(text("9")),
        MessageId::Uuid([7; 16]),
        MessageId::Binary(binary(b"9")),
    ] {
        roundtrip(&Content {
            properties: Some(Properties {
                message_id: Field::Value(id.clone()),
                user_id: Field::Null,
                to: Field::Null,
                subject: Field::Null,
                reply_to: Field::Null,
                correlation_id: Field::Value(id),
                ..Properties::default()
            }),
            ..Content::default()
        });
    }
    // Independent binary/string/UUID/ulong ID fixtures (each one is a complete properties list).
    for (id_wire, expected) in [
        (vec![0x53, 9], MessageId::ULong(9)),
        (vec![0xa1, 1, b'9'], MessageId::String(text("9"))),
        (vec![0xa0, 1, b'9'], MessageId::Binary(binary(b"9"))),
        (
            vec![0x98, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7],
            MessageId::Uuid([7; 16]),
        ),
    ] {
        let mut wire = vec![
            0,
            0x53,
            0x73,
            0xc0,
            u8::try_from(1 + id_wire.len()).unwrap(),
            1,
        ];
        wire.extend(id_wire);
        assert_eq!(
            decode_content(&wire, limits())
                .unwrap()
                .properties
                .unwrap()
                .message_id,
            Field::Value(expected)
        );
    }
}

#[test]
fn absent_and_null_are_distinct_without_materializing_defaults() {
    let empty = decode_content(&[0, 0x53, 0x70, 0x45], limits()).unwrap();
    let null = decode_content(&[0, 0x53, 0x70, 0xc0, 2, 1, 0x40], limits()).unwrap();
    assert_eq!(empty.header.as_ref().unwrap().durable, Field::Absent);
    assert_eq!(null.header.as_ref().unwrap().durable, Field::Null);
    assert_eq!(null.header.as_ref().unwrap().priority, Field::Absent);
    assert_ne!(empty, null);
    roundtrip(&empty);
    roundtrip(&null);
    let all_null = Content {
        header: Some(Header {
            durable: Field::Null,
            priority: Field::Null,
            ttl: Field::Null,
            first_acquirer: Field::Null,
            delivery_count: Field::Null,
        }),
        properties: Some(Properties {
            message_id: Field::Null,
            user_id: Field::Null,
            to: Field::Null,
            subject: Field::Null,
            reply_to: Field::Null,
            correlation_id: Field::Null,
            content_type: Field::Null,
            content_encoding: Field::Null,
            absolute_expiry_time: Field::Null,
            creation_time: Field::Null,
            group_id: Field::Null,
            group_sequence: Field::Null,
            reply_to_group_id: Field::Null,
        }),
        ..Content::default()
    };
    roundtrip(&all_null);
    let invalid = Content {
        header: Some(Header {
            priority: Field::Value(4),
            ..Header::default()
        }),
        ..Content::default()
    };
    assert_eq!(
        encode_content(&invalid, limits()),
        Err(Error::AbsentBeforePresent)
    );
}

#[test]
fn body_families_preserve_empty_and_each_ordered_section() {
    let fixtures = [
        (vec![], Body::Empty),
        (vec![0, 0x53, 0x75, 0xa0, 0], Body::Data(vec![binary(&[])])),
        (vec![0, 0x53, 0x76, 0x45], Body::Sequence(vec![vec![]])),
        (
            vec![0, 0x53, 0x76, 0xc0, 2, 1, 0x41, 0, 0x53, 0x76, 0x45],
            Body::Sequence(vec![vec![Value::Bool(true)], vec![]]),
        ),
        (vec![0, 0x53, 0x77, 0x40], Body::Value(Value::Null)),
    ];
    for (wire, body) in fixtures {
        let content = Content {
            body,
            ..Content::default()
        };
        assert_eq!(decode_content(&wire, limits()).unwrap(), content);
        roundtrip(&content);
    }
    for body in [Body::Data(vec![]), Body::Sequence(vec![])] {
        assert_eq!(
            encode_content(
                &Content {
                    body,
                    ..Content::default()
                },
                limits()
            ),
            Err(Error::InvalidSection)
        );
    }
}

#[test]
fn public_codec_empty_array_body_fixtures_keep_original_bytes_and_semantics() {
    // Source-derived compatibility fixtures, not old-binary execution captures:
    // crates/amqp/src/codec/message.rs::encode_message and codec.rs::append_value
    // delegate to pinned serde_amqp 0.14.1. Its ser.rs SeqSerializer::end/write_array
    // emits e0 01 00 for an empty array. de.rs::test_deserialize_empty_array_inner
    // explicitly covers absent and null-sentinel constructors at both widths.
    // All input bytes are literal and independent of this crate's encoder.
    for (wire, constructor) in [
        (vec![0, 0x53, 0x77, 0xe0, 1, 0], Constructor::Absent),
        (vec![0, 0x53, 0x77, 0xe0, 2, 0, 0x40], Constructor::Null),
        (
            vec![0, 0x53, 0x77, 0xf0, 0, 0, 0, 4, 0, 0, 0, 0],
            Constructor::Absent,
        ),
        (
            vec![0, 0x53, 0x77, 0xf0, 0, 0, 0, 5, 0, 0, 0, 0, 0x40],
            Constructor::Null,
        ),
    ] {
        let expected = Content {
            body: Body::Value(Value::Array(Array {
                constructor,
                elements: vec![],
            })),
            ..Content::default()
        };
        let retained = EncodedContent::from_bytes(&wire, limits()).unwrap();
        assert_eq!(retained.original_bytes(), wire);
        let view = retained.view(limits()).unwrap();
        assert_eq!(view, expected);
        roundtrip(&view);
        assert_eq!(retained.original_bytes(), wire);
    }
}

#[test]
fn public_codec_compound_property_fixture_keeps_nested_empty_arrays() {
    // Same source provenance as the body fixtures above. The existing public
    // application-properties codec retains compound Values, so p -> [[], []]
    // exercises both empty encodings below the map and list, not only a body root.
    let wire = [
        0, 0x53, 0x74, 0xc1, 14, 2, 0xa1, 1, b'p', 0xc0, 8, 2, 0xe0, 1, 0, 0xe0, 2, 0, 0x40,
    ];
    let expected = Content {
        application_properties: Some(vec![(
            text("p"),
            Value::List(vec![
                Value::Array(Array {
                    constructor: Constructor::Absent,
                    elements: vec![],
                }),
                Value::Array(Array {
                    constructor: Constructor::Null,
                    elements: vec![],
                }),
            ]),
        )]),
        ..Content::default()
    };
    let retained = EncodedContent::from_bytes(&wire, limits()).unwrap();
    assert_eq!(retained.original_bytes(), wire);
    let view = retained.view(limits()).unwrap();
    assert_eq!(view, expected);
    roundtrip(&view);
    assert_eq!(retained.original_bytes(), wire);
}

#[test]
fn annotations_footer_and_application_properties_allow_every_compound_family() {
    let compounds = vec![
        Value::List(vec![Value::Null, Value::Timestamp(-5)]),
        Value::Map(vec![(
            Value::List(vec![Value::UInt(3)]),
            Value::Symbol(symbol("v")),
        )]),
        Value::Array(Array {
            constructor: Constructor::Described {
                descriptor: Descriptor::ULong(99),
                constructor: Box::new(Constructor::Primitive(PrimitiveConstructor::True)),
            },
            elements: vec![Value::Bool(true)],
        }),
        Value::Described {
            descriptor: Descriptor::Symbol(symbol("d")),
            value: Box::new(Value::Null),
        },
    ];
    let annotations = compounds
        .iter()
        .enumerate()
        .map(|(i, v)| (AnnotationKey::ULong(i as u64), v.clone()))
        .collect::<Vec<_>>();
    roundtrip(&Content {
        delivery_annotations: Some(annotations.clone()),
        message_annotations: Some(annotations.clone()),
        footer: Some(annotations),
        application_properties: Some(
            compounds
                .into_iter()
                .enumerate()
                .map(|(i, v)| (text(&i.to_string()), v))
                .collect(),
        ),
        ..Content::default()
    });
}

#[test]
fn immutable_source_is_not_changed_by_input_or_transient_view_mutation() {
    let original = vec![0, 0x53, 0x77, 0x41];
    let mut input = original.clone();
    let retained = EncodedContent::from_bytes(&input, limits()).unwrap();
    input.fill(0);
    let mut view = retained.view(limits()).unwrap();
    view.body = Body::Value(Value::Bool(false));
    let clone = retained.clone();
    assert_eq!(clone.original_bytes(), original);
    assert_eq!(
        retained.view(limits()).unwrap().body,
        Body::Value(Value::Bool(true))
    );
    assert_eq!(
        EncodedContent::from_vec(original.clone(), limits())
            .unwrap()
            .original_bytes(),
        original
    );
    assert_eq!(
        retained.view(Limits {
            max_bytes: 3,
            ..limits()
        }),
        Err(Error::ByteLimit)
    );
}

#[test]
fn symbolic_and_full_width_section_descriptors_keep_exact_original_bytes() {
    let wire = [
        0, 0xa3, 16, b'a', b'm', b'q', b'p', b':', b'h', b'e', b'a', b'd', b'e', b'r', b':', b'l',
        b'i', b's', b't', 0x45, 0, 0x80, 0, 0, 0, 0, 0, 0, 0, 0x77, 0x40,
    ];
    let retained = EncodedContent::from_bytes(&wire, limits()).unwrap();
    assert_eq!(retained.original_bytes(), wire);
    let content = retained.view(limits()).unwrap();
    assert_eq!(content.header, Some(Header::default()));
    assert_eq!(content.body, Body::Value(Value::Null));
    roundtrip(&content);
}

#[test]
fn invalid_composite_field_counts_and_every_property_type_are_checked() {
    // No unknown tail fields in either fixed composite.
    for (code, count) in [(0x70, 6_u8), (0x73, 14_u8)] {
        let mut wire = vec![0, 0x53, code, 0xc0, count + 1, count];
        wire.extend(std::iter::repeat_n(0x40, usize::from(count)));
        assert_eq!(decode_content(&wire, limits()), Err(Error::InvalidField));
    }
    // False is not a valid member of any of the thirteen property field types.
    for index in 0..13_u8 {
        let mut wire = vec![0, 0x53, 0x73, 0xc0, index + 2, index + 1];
        wire.extend(std::iter::repeat_n(0x40, usize::from(index)));
        wire.push(0x42);
        assert_eq!(decode_content(&wire, limits()), Err(Error::InvalidField));
    }
}

#[test]
fn section_order_duplicates_mixed_bodies_and_invalid_keys_are_refused() {
    let header = [0, 0x53, 0x70, 0x45];
    let data = [0, 0x53, 0x75, 0xa0, 0];
    let sequence = [0, 0x53, 0x76, 0x45];
    let value = [0, 0x53, 0x77, 0x40];
    let footer = [0, 0x53, 0x78, 0xc1, 1, 0];
    for (wire, error) in [
        (
            [header.as_slice(), &header].concat(),
            Error::DuplicateSection,
        ),
        ([data.as_slice(), &header].concat(), Error::SectionOrder),
        ([data.as_slice(), &sequence].concat(), Error::MixedBody),
        ([value.as_slice(), &value].concat(), Error::MixedBody),
        ([value.as_slice(), &data].concat(), Error::MixedBody),
        ([footer.as_slice(), &data].concat(), Error::SectionOrder),
        (
            [footer.as_slice(), &footer].concat(),
            Error::DuplicateSection,
        ),
        (
            vec![0, 0x53, 0x71, 0xc1, 5, 2, 0xa1, 1, b'k', 0x40],
            Error::InvalidAnnotationKey,
        ),
        (
            vec![0, 0x53, 0x72, 0xc1, 3, 2, 0x43, 0x40],
            Error::InvalidAnnotationKey,
        ),
        (
            vec![0, 0x53, 0x78, 0xc1, 3, 2, 0x40, 0x40],
            Error::InvalidAnnotationKey,
        ),
        (
            vec![0, 0x53, 0x74, 0xc1, 5, 2, 0xa3, 1, b'k', 0x40],
            Error::InvalidApplicationKey,
        ),
        (vec![0, 0x53, 0x79, 0x40], Error::InvalidSection),
        (vec![0x40], Error::InvalidSection),
        (vec![0, 0x53, 0x75, 0x40], Error::InvalidSection),
        (vec![0, 0x53, 0x76, 0x40], Error::InvalidSection),
        (vec![0, 0x53, 0x70, 0xc0, 2, 1, 0x43], Error::InvalidField),
        (vec![0, 0x53, 0x73, 0xc0, 2, 1, 0x41], Error::InvalidField),
    ] {
        assert_eq!(
            decode_content(&wire, limits()),
            Err(error),
            "fixture {wire:02x?}"
        );
    }
    for code in [0x71, 0x72, 0x73, 0x74] {
        let payload = if code == 0x73 {
            vec![0x45]
        } else {
            vec![0xc1, 1, 0]
        };
        let mut one = vec![0, 0x53, code];
        one.extend(payload);
        assert_eq!(
            decode_content(&[one.as_slice(), &one].concat(), limits()),
            Err(Error::DuplicateSection)
        );
    }
}
