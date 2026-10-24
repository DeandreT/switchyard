use message_content::{
    Array, Binary, Constructor, Descriptor, Limits, PrimitiveConstructor as C, Symbol, Text, Value,
    Width, decode_value, encode_value, validate_value,
};

fn limits() -> Limits {
    Limits {
        max_bytes: 100_000,
        max_depth: 128,
        max_nodes: 10_000,
        max_work: 200_000,
    }
}

fn text(s: &str, width: Width) -> Text {
    Text {
        text: s.into(),
        width,
    }
}
fn symbol(s: &str, width: Width) -> Symbol {
    Symbol {
        text: s.into(),
        width,
    }
}
fn binary(bytes: &[u8], width: Width) -> Binary {
    Binary {
        bytes: bytes.into(),
        width,
    }
}

fn roundtrip(value: &Value) {
    validate_value(value, limits()).unwrap();
    let wire = encode_value(value, limits()).unwrap();
    assert_eq!(&decode_value(&wire, limits()).unwrap(), value);
}

#[test]
fn independent_scalar_wire_fixtures_cover_every_family_and_alternate_encoding() {
    let fixtures = vec![
        (vec![0x40], Value::Null),
        (vec![0x41], Value::Bool(true)),
        (vec![0x42], Value::Bool(false)),
        (vec![0x56, 0], Value::Bool(false)),
        (vec![0x56, 1], Value::Bool(true)),
        (vec![0x50, 0xfe], Value::UByte(254)),
        (vec![0x51, 0x80], Value::Byte(-128)),
        (vec![0x60, 0xab, 0xcd], Value::UShort(0xabcd)),
        (vec![0x61, 0xff, 0xfe], Value::Short(-2)),
        (vec![0x43], Value::UInt(0)),
        (vec![0x52, 127], Value::UInt(127)),
        (vec![0x70, 0x80, 0, 0, 1], Value::UInt(0x80000001)),
        (vec![0x44], Value::ULong(0)),
        (vec![0x53, 255], Value::ULong(255)),
        (
            vec![0x80, 0x80, 0, 0, 0, 0, 0, 0, 1],
            Value::ULong(0x8000000000000001),
        ),
        (vec![0x54, 0xff], Value::Int(-1)),
        (vec![0x71, 0x80, 0, 0, 0], Value::Int(i32::MIN)),
        (vec![0x55, 0xfe], Value::Long(-2)),
        (vec![0x81, 0x80, 0, 0, 0, 0, 0, 0, 0], Value::Long(i64::MIN)),
        (vec![0x72, 0x7f, 0xc0, 0, 0x42], Value::Float(0x7fc00042)),
        (
            vec![0x82, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0x42],
            Value::Double(0x7ff8000000000042),
        ),
        (
            vec![0x74, 0xde, 0xad, 0xbe, 0xef],
            Value::Decimal32(0xdeadbeef),
        ),
        (
            vec![0x84, 1, 2, 3, 4, 5, 6, 7, 8],
            Value::Decimal64(0x0102030405060708),
        ),
        (
            vec![0x94, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            Value::Decimal128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        ),
        (vec![0x73, 0, 1, 0xf6, 0x00], Value::Char('😀')),
        (
            vec![0x83, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe],
            Value::Timestamp(-2),
        ),
        (
            vec![0x98, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            Value::Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        ),
        (
            vec![0xa0, 2, 0, 255],
            Value::Binary(binary(&[0, 255], Width::One)),
        ),
        (
            vec![0xb0, 0, 0, 0, 2, 0, 255],
            Value::Binary(binary(&[0, 255], Width::Four)),
        ),
        (
            vec![0xa1, 2, 0xc3, 0xa9],
            Value::String(text("é", Width::One)),
        ),
        (
            vec![0xb1, 0, 0, 0, 1, b'x'],
            Value::String(text("x", Width::Four)),
        ),
        (vec![0xa3, 1, b'x'], Value::Symbol(symbol("x", Width::One))),
        (
            vec![0xb3, 0, 0, 0, 1, b'x'],
            Value::Symbol(symbol("x", Width::Four)),
        ),
    ];
    for (wire, expected) in fixtures {
        assert_eq!(
            decode_value(&wire, limits()).unwrap(),
            expected,
            "fixture {wire:02x?}"
        );
        roundtrip(&expected);
    }
}

#[test]
fn float_decimal_bits_include_negative_zero_infinities_and_nan_payloads() {
    for bits in [0, 0x80000000, 0x7f800000, 0xff800000, 0x7f800001, u32::MAX] {
        roundtrip(&Value::Float(bits));
        roundtrip(&Value::Decimal32(bits));
    }
    for bits in [
        0,
        0x8000000000000000,
        0x7ff0000000000000,
        0xfff0000000000000,
        0x7ff0000000000001,
        u64::MAX,
    ] {
        roundtrip(&Value::Double(bits));
        roundtrip(&Value::Decimal64(bits));
    }
    roundtrip(&Value::Decimal128([255; 16]));
    roundtrip(&Value::Timestamp(i64::MIN));
    roundtrip(&Value::Timestamp(i64::MAX));
}

#[test]
fn literal_compounds_have_arbitrary_keys_and_preserve_duplicate_entries() {
    let fixtures = [
        (vec![0x45], Value::List(vec![])),
        (
            vec![0xc0, 4, 3, 0x40, 0x41, 0x43],
            Value::List(vec![Value::Null, Value::Bool(true), Value::UInt(0)]),
        ),
        (
            vec![0xd0, 0, 0, 0, 5, 0, 0, 0, 1, 0x40],
            Value::List(vec![Value::Null]),
        ),
        (
            vec![0xc1, 6, 2, 0xc0, 2, 1, 0x40, 0x41],
            Value::Map(vec![(Value::List(vec![Value::Null]), Value::Bool(true))]),
        ),
        (
            vec![0xd1, 0, 0, 0, 6, 0, 0, 0, 2, 0x40, 0x43],
            Value::Map(vec![(Value::Null, Value::UInt(0))]),
        ),
        (
            vec![0xc1, 5, 4, 0x40, 0x41, 0x40, 0x42],
            Value::Map(vec![
                (Value::Null, Value::Bool(true)),
                (Value::Null, Value::Bool(false)),
            ]),
        ),
        (
            vec![0, 0x53, 7, 0, 0xa3, 1, b'd', 0x40],
            Value::Described {
                descriptor: Descriptor::ULong(7),
                value: Box::new(Value::Described {
                    descriptor: Descriptor::Symbol(symbol("d", Width::One)),
                    value: Box::new(Value::Null),
                }),
            },
        ),
    ];
    for (wire, value) in fixtures {
        assert_eq!(decode_value(&wire, limits()).unwrap(), value);
        roundtrip(&value);
    }
    roundtrip(&Value::Map(vec![(
        Value::Map(vec![(Value::Long(-4), Value::Null)]),
        Value::List(vec![
            Value::Symbol(symbol("s", Width::Four)),
            Value::String(text("s", Width::One)),
        ]),
    )]));
}

#[test]
fn literal_empty_and_zero_width_arrays_retain_shared_descriptors_and_constructor() {
    let fixtures = [
        (vec![0xe0, 1, 0], Constructor::Absent, vec![]),
        (vec![0xe0, 2, 0, 0x40], Constructor::Null, vec![]),
        (
            vec![0xf0, 0, 0, 0, 4, 0, 0, 0, 0],
            Constructor::Absent,
            vec![],
        ),
        (
            vec![0xf0, 0, 0, 0, 5, 0, 0, 0, 0, 0x40],
            Constructor::Null,
            vec![],
        ),
        (
            vec![0xe0, 2, 0, 0x45],
            Constructor::Primitive(C::List0),
            vec![],
        ),
        (
            vec![0xe0, 2, 3, 0x41],
            Constructor::Primitive(C::True),
            vec![Value::Bool(true); 3],
        ),
        (
            vec![0xf0, 0, 0, 0, 5, 0, 0, 0, 0, 0x70],
            Constructor::Primitive(C::UInt),
            vec![],
        ),
        (
            vec![0xe0, 5, 0, 0, 0x53, 0x23, 0x45],
            Constructor::Described {
                descriptor: Descriptor::ULong(0x23),
                constructor: Box::new(Constructor::Primitive(C::List0)),
            },
            vec![],
        ),
        (
            vec![0xe0, 5, 2, 0, 0x53, 0x23, 0x43],
            Constructor::Described {
                descriptor: Descriptor::ULong(0x23),
                constructor: Box::new(Constructor::Primitive(C::UInt0)),
            },
            vec![Value::UInt(0); 2],
        ),
        (
            vec![0xe0, 8, 0, 0, 0x53, 0x23, 0, 0x53, 0x24, 0x45],
            Constructor::Described {
                descriptor: Descriptor::ULong(0x23),
                constructor: Box::new(Constructor::Described {
                    descriptor: Descriptor::ULong(0x24),
                    constructor: Box::new(Constructor::Primitive(C::List0)),
                }),
            },
            vec![],
        ),
        (
            vec![0xe0, 6, 1, 0xc1, 3, 2, 0x40, 0x41],
            Constructor::Primitive(C::Map8),
            vec![Value::Map(vec![(Value::Null, Value::Bool(true))])],
        ),
        (
            vec![0xe0, 5, 1, 0xe0, 2, 0, 0x41],
            Constructor::Primitive(C::Array8),
            vec![Value::Array(Array {
                constructor: Constructor::Primitive(C::True),
                elements: vec![],
            })],
        ),
        (
            vec![0xe0, 4, 1, 0xe0, 1, 0],
            Constructor::Primitive(C::Array8),
            vec![Value::Array(Array {
                constructor: Constructor::Absent,
                elements: vec![],
            })],
        ),
        (
            vec![0xe0, 5, 1, 0xe0, 2, 0, 0x40],
            Constructor::Primitive(C::Array8),
            vec![Value::Array(Array {
                constructor: Constructor::Null,
                elements: vec![],
            })],
        ),
        (
            vec![0xe0, 10, 1, 0xf0, 0, 0, 0, 4, 0, 0, 0, 0],
            Constructor::Primitive(C::Array32),
            vec![Value::Array(Array {
                constructor: Constructor::Absent,
                elements: vec![],
            })],
        ),
        (
            vec![0xe0, 11, 1, 0xf0, 0, 0, 0, 5, 0, 0, 0, 0, 0x40],
            Constructor::Primitive(C::Array32),
            vec![Value::Array(Array {
                constructor: Constructor::Null,
                elements: vec![],
            })],
        ),
    ];
    for (wire, constructor, elements) in fixtures {
        let value = Value::Array(Array {
            constructor,
            elements,
        });
        assert_eq!(
            decode_value(&wire, limits()).unwrap(),
            value,
            "fixture {wire:02x?}"
        );
        roundtrip(&value);
    }
}

#[test]
fn compact_array_descendants_preserve_empty_constructor_markers() {
    let wire = [0xe0, 11, 1, 0xc0, 8, 2, 0xe0, 1, 0, 0xe0, 2, 0, 0x40];
    let expected = Value::Array(Array {
        constructor: Constructor::Primitive(C::List8),
        elements: vec![Value::List(vec![
            Value::Array(Array {
                constructor: Constructor::Absent,
                elements: vec![],
            }),
            Value::Array(Array {
                constructor: Constructor::Null,
                elements: vec![],
            }),
        ])],
    });
    assert_eq!(decode_value(&wire, limits()).unwrap(), expected);
    roundtrip(&expected);
}

#[test]
fn every_array_constructor_accepts_matching_values_and_preserves_empty_type() {
    let cases = vec![
        (C::True, Value::Bool(true)),
        (C::False, Value::Bool(false)),
        (C::Bool, Value::Bool(true)),
        (C::UInt0, Value::UInt(0)),
        (C::ULong0, Value::ULong(0)),
        (C::List0, Value::List(vec![])),
        (C::UByte, Value::UByte(255)),
        (C::Byte, Value::Byte(-128)),
        (C::SmallUInt, Value::UInt(255)),
        (C::SmallULong, Value::ULong(255)),
        (C::SmallInt, Value::Int(-128)),
        (C::SmallLong, Value::Long(-128)),
        (C::UShort, Value::UShort(u16::MAX)),
        (C::Short, Value::Short(i16::MIN)),
        (C::UInt, Value::UInt(u32::MAX)),
        (C::Int, Value::Int(i32::MIN)),
        (C::ULong, Value::ULong(u64::MAX)),
        (C::Long, Value::Long(i64::MIN)),
        (C::Float, Value::Float(u32::MAX)),
        (C::Double, Value::Double(u64::MAX)),
        (C::Decimal32, Value::Decimal32(u32::MAX)),
        (C::Decimal64, Value::Decimal64(u64::MAX)),
        (C::Decimal128, Value::Decimal128([255; 16])),
        (C::Uuid, Value::Uuid([255; 16])),
        (C::Char, Value::Char('é')),
        (C::Timestamp, Value::Timestamp(-1)),
        (C::Binary8, Value::Binary(binary(&[1], Width::One))),
        (C::Binary32, Value::Binary(binary(&[1], Width::Four))),
        (C::String8, Value::String(text("x", Width::One))),
        (C::String32, Value::String(text("x", Width::Four))),
        (C::Symbol8, Value::Symbol(symbol("x", Width::One))),
        (C::Symbol32, Value::Symbol(symbol("x", Width::Four))),
        (C::List8, Value::List(vec![Value::Null])),
        (C::List32, Value::List(vec![Value::Null])),
        (C::Map8, Value::Map(vec![(Value::Null, Value::Null)])),
        (C::Map32, Value::Map(vec![(Value::Null, Value::Null)])),
        (
            C::Array8,
            Value::Array(Array {
                constructor: Constructor::Primitive(C::False),
                elements: vec![Value::Bool(false)],
            }),
        ),
        (
            C::Array32,
            Value::Array(Array {
                constructor: Constructor::Primitive(C::False),
                elements: vec![Value::Bool(false)],
            }),
        ),
    ];
    for (code, value) in cases {
        roundtrip(&Value::Array(Array {
            constructor: Constructor::Primitive(code),
            elements: vec![value],
        }));
        roundtrip(&Value::Array(Array {
            constructor: Constructor::Primitive(code),
            elements: vec![],
        }));
    }
    roundtrip(&Value::Array(Array {
        constructor: Constructor::Described {
            descriptor: Descriptor::Symbol(symbol("type", Width::Four)),
            constructor: Box::new(Constructor::Primitive(C::String32)),
        },
        elements: vec![Value::String(text("value", Width::Four))],
    }));
}

#[test]
fn retained_short_array_constructors_reencode_compact_nested_values_at_the_boundary() {
    // Expanding 200 uint0 or list0 children to uint/list32 would no longer fit list8.
    for child in [0x43, 0x45] {
        let mut wire = vec![0xe0, 204, 1, 0xc0, 201, 200];
        wire.extend(std::iter::repeat_n(child, 200));
        let value = decode_value(&wire, limits()).unwrap();
        roundtrip(&value);
    }
    // Descriptor ulong0 is a one-byte constructor; preserving compact descendants
    // must not expand it to smallulong and overflow the retained list8 payload.
    let mut wire = vec![0xe0, 244, 1, 0xc0, 241, 80];
    for _ in 0..80 {
        wire.extend([0, 0x44, 0x40]);
    }
    roundtrip(&decode_value(&wire, limits()).unwrap());
    let nested = Value::Array(Array {
        constructor: Constructor::Primitive(C::List8),
        elements: vec![Value::List(vec![
            Value::Map(vec![(
                Value::UInt(0),
                Value::List(vec![])
            )]);
            30
        ])],
    });
    roundtrip(&nested);
}
