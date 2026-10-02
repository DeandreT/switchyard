use super::*;

fn cases() -> Vec<(Value, rule_scalar_value::Value)> {
    use rule_scalar_value::Value as S;
    vec![
        (json!({"type":"null"}), S::NullValue(v1::RuleNullValue {})),
        (json!({"type":"bool","value":false}), S::BoolValue(false)),
        (json!({"type":"ubyte","value":255}), S::UbyteValue(255)),
        (
            json!({"type":"ushort","value":65535}),
            S::UshortValue(65535),
        ),
        (
            json!({"type":"uint","value":4294967295_u32}),
            S::UintValue(u32::MAX),
        ),
        (
            json!({"type":"ulong","value":"18446744073709551615"}),
            S::UlongValue(u64::MAX),
        ),
        (json!({"type":"byte","value":-128}), S::ByteValue(-128)),
        (
            json!({"type":"short","value":-32768}),
            S::ShortValue(-32768),
        ),
        (
            json!({"type":"int","value":-2147483648_i32}),
            S::IntValue(i32::MIN),
        ),
        (
            json!({"type":"long","value":"-9223372036854775808"}),
            S::LongValue(i64::MIN),
        ),
        (
            json!({"type":"float_bits","value":"7fc00001"}),
            S::FloatBits(0x7fc0_0001),
        ),
        (
            json!({"type":"double_bits","value":"8000000000000000"}),
            S::DoubleBits(0x8000_0000_0000_0000),
        ),
        (
            json!({"type":"decimal32","value":"0001feff"}),
            S::Decimal32Bytes(vec![0, 1, 254, 255]),
        ),
        (
            json!({"type":"decimal64","value":"8080808080808080"}),
            S::Decimal64Bytes(vec![128; 8]),
        ),
        (
            json!({"type":"decimal128","value":"fefefefefefefefefefefefefefefefe"}),
            S::Decimal128Bytes(vec![254; 16]),
        ),
        (
            json!({"type":"char","value":128512}),
            S::CharCodepoint(0x1f600),
        ),
        (
            json!({"type":"timestamp","value":"-1"}),
            S::TimestampMillis(-1),
        ),
        (
            json!({"type":"uuid","value":"ffffffffffffffffffffffffffffffff"}),
            S::UuidBytes(vec![255; 16]),
        ),
        (
            json!({"type":"binary","value":"00ff80"}),
            S::BinaryValue(vec![0, 255, 128]),
        ),
        (
            json!({"type":"string","value":"literal \u{1f600}"}),
            S::StringValue("literal \u{1f600}".into()),
        ),
        (
            json!({"type":"symbol","value":"ASCII.symbol"}),
            S::SymbolValue("ASCII.symbol".into()),
        ),
    ]
}

#[test]
fn every_scalar_constructor_round_trips_with_exact_width_and_bits() {
    assert_eq!(cases().len(), 21);
    for (json, expected) in cases() {
        let actual = scalar_input(json.clone()).expect("valid scalar");
        assert_eq!(actual, scalar_value(expected));
        let encoded = serde_json::to_value(scalar::from_protobuf(actual).expect("valid response"))
            .expect("scalar output");
        assert_eq!(encoded, json);
    }
}

#[test]
fn correlation_round_trip_preserves_empty_strings_null_and_property_names() {
    let properties = cases()
        .into_iter()
        .enumerate()
        .map(|(index, (value, _))| json!({"name":format!("scalar{index:02}"),"value":value}))
        .collect::<Vec<_>>();
    let input = json!({
        "type":"correlation", "correlation_id":"", "message_id":"id", "to":"to", "reply_to":"reply",
        "subject":"subject", "session_id":"session", "reply_to_session_id":"reply-session", "content_type":"application/test",
        "properties":properties,
    });
    let proto = parse(input.clone()).expect("all scalars plus eight system conditions");
    assert_eq!(
        serde_json::to_value(filter::from_protobuf(proto).expect("filter output")).unwrap(),
        input
    );
    let input = json!({"type":"correlation","properties":[
        {"name":"Case","value":{"type":"null"}},
        {"name":"case","value":{"type":"string","value":""}},
    ]});
    let proto = parse(input).unwrap();
    let Some(rule_filter::Filter::CorrelationFilter(value)) = proto.filter else {
        panic!("correlation");
    };
    assert_eq!(value.properties.len(), 2);
    assert_eq!(value.properties[0].name, "Case");
    assert_eq!(value.properties[1].name, "case");
    assert_eq!(
        value.properties[0].value,
        Some(scalar_value(rule_scalar_value::Value::NullValue(
            v1::RuleNullValue {}
        )))
    );
}

#[test]
fn floating_bits_preserve_nan_payloads_infinities_and_signed_zero() {
    for bits in [
        0_u32,
        0x8000_0000,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_1234,
        u32::MAX,
    ] {
        let input = json!({"type":"float_bits","value":format!("{bits:08x}")});
        let proto = scalar_input(input.clone()).unwrap();
        assert_eq!(
            proto,
            scalar_value(rule_scalar_value::Value::FloatBits(bits))
        );
        assert_eq!(
            serde_json::to_value(scalar::from_protobuf(proto).unwrap()).unwrap(),
            input
        );
    }
    for bits in [
        0_u64,
        0x8000_0000_0000_0000,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_1234,
        u64::MAX,
    ] {
        let input = json!({"type":"double_bits","value":format!("{bits:016x}")});
        let proto = scalar_input(input.clone()).unwrap();
        assert_eq!(
            proto,
            scalar_value(rule_scalar_value::Value::DoubleBits(bits))
        );
        assert_eq!(
            serde_json::to_value(scalar::from_protobuf(proto).unwrap()).unwrap(),
            input
        );
    }
}

#[test]
fn integer_widths_are_checked_without_coercing_json_numbers() {
    for (kind, bad) in [
        ("ubyte", json!(-1)),
        ("ubyte", json!(256)),
        ("ushort", json!(65536)),
        ("uint", json!(4294967296_u64)),
        ("byte", json!(-129)),
        ("byte", json!(128)),
        ("short", json!(-32769)),
        ("short", json!(32768)),
        ("int", json!(-2147483649_i64)),
        ("int", json!(2147483648_i64)),
        ("ubyte", json!(1.0)),
        ("uint", json!("1")),
        ("bool", json!(1)),
    ] {
        assert!(
            scalar_input(json!({"type":kind,"value":bad})).is_err(),
            "{kind}"
        );
    }
    for (kind, value) in [
        ("ubyte", json!(0)),
        ("ushort", json!(0)),
        ("uint", json!(0)),
        ("byte", json!(127)),
        ("short", json!(32767)),
        ("int", json!(2147483647_i32)),
    ] {
        assert!(scalar_input(json!({"type":kind,"value":value})).is_ok());
    }
}

#[test]
fn sixty_four_bit_decimal_strings_are_canonical_and_overflow_checked() {
    for kind in ["ulong", "long", "timestamp"] {
        for invalid in [
            "", " ", "+1", "01", "-0", " 1", "1 ", "1.0", "1e2", "0x1", "--1",
        ] {
            assert!(
                scalar_input(json!({"type":kind,"value":invalid})).is_err(),
                "{kind}:{invalid}"
            );
        }
        assert!(scalar_input(json!({"type":kind,"value":1})).is_err());
    }
    for invalid in ["-1", "18446744073709551616"] {
        assert!(scalar_input(json!({"type":"ulong","value":invalid})).is_err());
    }
    for kind in ["long", "timestamp"] {
        for invalid in ["9223372036854775808", "-9223372036854775809"] {
            assert!(scalar_input(json!({"type":kind,"value":invalid})).is_err());
        }
        for valid in ["0", "-1", "9223372036854775807", "-9223372036854775808"] {
            assert!(scalar_input(json!({"type":kind,"value":valid})).is_ok());
        }
    }
    assert!(scalar_input(json!({"type":"ulong","value":"0"})).is_ok());
}

#[test]
fn hex_is_lowercase_exact_width_and_binary_is_even_length() {
    for (kind, width) in [
        ("float_bits", 8),
        ("double_bits", 16),
        ("decimal32", 8),
        ("decimal64", 16),
        ("decimal128", 32),
        ("uuid", 32),
    ] {
        for invalid in [
            "".into(),
            "0".repeat(width - 1),
            "0".repeat(width + 1),
            "A".repeat(width),
            "g".repeat(width),
            format!("0x{}", "0".repeat(width)),
        ] {
            assert!(
                scalar_input(json!({"type":kind,"value":invalid})).is_err(),
                "{kind}"
            );
        }
        assert!(scalar_input(json!({"type":kind,"value":0})).is_err());
    }
    for invalid in ["0", "ABC0", "0x00", "gg", "00 ff"] {
        assert!(scalar_input(json!({"type":"binary","value":invalid})).is_err());
    }
    for valid in ["", "00", "ff00fe"] {
        assert!(scalar_input(json!({"type":"binary","value":valid})).is_ok());
    }
}

#[test]
fn unicode_char_and_ascii_symbol_constraints_are_distinct() {
    for value in [0_u32, 0xd7ff, 0xe000, 0x10ffff] {
        assert!(scalar_input(json!({"type":"char","value":value})).is_ok());
    }
    for value in [0xd800_u32, 0xdfff, 0x110000, u32::MAX] {
        assert!(scalar_input(json!({"type":"char","value":value})).is_err());
    }
    assert!(scalar_input(json!({"type":"char","value":"A"})).is_err());
    assert!(scalar_input(json!({"type":"symbol","value":"\u{e9}"})).is_err());
    assert!(scalar_input(json!({"type":"string","value":"\u{e9}"})).is_ok());
    assert!(scalar_input(json!({"type":"symbol","value":""})).is_ok());
}

#[test]
fn missing_unknown_scalar_constructors_and_values_do_not_become_null() {
    for input in [
        json!({}),
        json!({"value":null}),
        json!({"type":"unknown","value":0}),
        json!({"type":"bool"}),
        json!({"type":"bool","value":null}),
        json!({"type":"null","value":null}),
        json!({"type":"null","extra":false}),
    ] {
        assert!(scalar_input(input).is_err());
    }
    assert!(scalar_input(json!({"type":"null"})).is_ok());
}

#[test]
fn malformed_protobuf_scalar_values_are_refused_before_output() {
    assert!(scalar::from_protobuf(v1::RuleScalarValue::default()).is_err());
    for value in [
        rule_scalar_value::Value::UbyteValue(256),
        rule_scalar_value::Value::UshortValue(65536),
        rule_scalar_value::Value::ByteValue(128),
        rule_scalar_value::Value::ShortValue(-32769),
        rule_scalar_value::Value::Decimal32Bytes(vec![0; 3]),
        rule_scalar_value::Value::Decimal64Bytes(vec![0; 7]),
        rule_scalar_value::Value::Decimal128Bytes(vec![0; 17]),
        rule_scalar_value::Value::UuidBytes(vec![0; 15]),
        rule_scalar_value::Value::CharCodepoint(0xd800),
        rule_scalar_value::Value::SymbolValue("\u{e9}".into()),
    ] {
        assert!(scalar::from_protobuf(scalar_value(value)).is_err());
    }
}
