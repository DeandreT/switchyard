use super::*;

#[test]
fn numeric_widths_compare_by_promotion_without_losing_integer_precision() {
    for value in [
        MessageValue::Byte(7),
        MessageValue::Short(7),
        MessageValue::Int(7),
        MessageValue::Long(7),
        MessageValue::Ubyte(7),
        MessageValue::Ushort(7),
        MessageValue::Uint(7),
        MessageValue::Ulong(7),
    ] {
        let envelope = envelope([("number", value)]);
        assert_truth("number = 7", &envelope, SqlTruth::True);
        assert_truth("number != 8", &envelope, SqlTruth::True);
    }
    let envelope = envelope([
        ("exact", MessageValue::Long(9_007_199_254_740_993)),
        ("uint", MessageValue::Uint(u32::MAX)),
        ("int", MessageValue::Int(1)),
        ("unsigned", MessageValue::Ulong(u64::MAX)),
        ("same", MessageValue::Ulong(u64::MAX)),
    ]);
    assert_truth("exact = 9007199254740993", &envelope, SqlTruth::True);
    assert_truth("exact = 9007199254740992", &envelope, SqlTruth::False);
    assert_truth("uint + int = 4294967296", &envelope, SqlTruth::True);
    assert_truth("unsigned = same", &envelope, SqlTruth::True);
    assert_truth("unsigned > 9223372036854775807", &envelope, SqlTruth::True);
}

#[test]
fn unsigned_literal_conversion_does_not_coerce_signed_runtime_properties() {
    let envelope = envelope([
        ("unsigned", MessageValue::Ulong(7)),
        ("signed", MessageValue::Long(7)),
        ("small", MessageValue::Int(7)),
    ]);
    assert_truth("unsigned = 7", &envelope, SqlTruth::True);
    assert_truth("7 = unsigned", &envelope, SqlTruth::True);
    assert_truth("unsigned + 1 = 8", &envelope, SqlTruth::True);
    for expression in [
        "unsigned = signed",
        "signed = unsigned",
        "unsigned = small",
        "unsigned + signed = 14",
        "unsigned = -1",
        "-unsigned = -7",
    ] {
        assert_error(expression, &envelope, SqlEvaluationError::TypeMismatch);
    }
}

#[test]
fn integer_arithmetic_is_checked_and_division_truncates_toward_zero() {
    let envelope = envelope([
        ("long_max", MessageValue::Long(i64::MAX)),
        ("long_min", MessageValue::Long(i64::MIN)),
        ("int_max", MessageValue::Int(i32::MAX)),
        ("one", MessageValue::Int(1)),
        ("unsigned_max", MessageValue::Ulong(u64::MAX)),
        ("unsigned_one", MessageValue::Ulong(1)),
    ]);
    for expression in [
        "long_max + 1 > 0",
        "long_min - 1 < 0",
        "long_max * 2 > 0",
        "-long_min > 0",
        "long_min / -1 > 0",
        "int_max + one > 0",
        "unsigned_max + unsigned_one > 0",
    ] {
        assert_error(expression, &envelope, SqlEvaluationError::NumericOverflow);
    }
    for expression in ["7 / 0 = 0", "7 % 0 = 0"] {
        assert_error(expression, &envelope, SqlEvaluationError::DivisionByZero);
    }
    for expression in [
        "7 / -2 = -3",
        "-7 / 2 = -3",
        "-7 % 2 = -1",
        "7 % -2 = 1",
        "-9223372036854775808 = long_min",
        "+one = 1",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
}

#[test]
fn floating_point_equality_uses_numeric_zero_and_nan_semantics() {
    let envelope = envelope([
        ("float_zero", MessageValue::Float((-0.0_f32).to_bits())),
        ("double_zero", MessageValue::Double(0.0_f64.to_bits())),
        ("float", MessageValue::Float(1.5_f32.to_bits())),
        ("double", MessageValue::Double(1.5_f64.to_bits())),
        ("nan", MessageValue::Double(f64::NAN.to_bits())),
        ("infinity", MessageValue::Double(f64::INFINITY.to_bits())),
    ]);
    for expression in [
        "float_zero = double_zero",
        "float_zero = 0",
        "float = double",
        "float + 0.5 = 2.0",
        "nan != nan",
        "infinity > 1.0",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
    for expression in [
        "nan = nan",
        "nan < 1.0",
        "nan >= 1.0",
        "float_zero != double_zero",
    ] {
        assert_truth(expression, &envelope, SqlTruth::False);
    }
}

#[test]
fn incompatible_and_unsupported_values_are_not_parsed_or_coerced() {
    for value in [
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::Char('7'),
        MessageValue::Timestamp(7),
        MessageValue::Uuid([0; 16]),
        MessageValue::Binary(vec![7]),
        MessageValue::Symbol("7".into()),
        MessageValue::List(vec![MessageValue::Long(7)]),
        MessageValue::Map(vec![(
            MessageValue::String("x".into()),
            MessageValue::Long(7),
        )]),
        MessageValue::Array(vec![MessageValue::Long(7)]),
    ] {
        assert_error(
            "value = 7",
            &envelope([("value", value)]),
            SqlEvaluationError::UnsupportedValue,
        );
    }
    let envelope = envelope([
        ("text", MessageValue::String("7".into())),
        ("flag", MessageValue::Bool(true)),
    ]);
    for expression in ["text = 7", "flag = 1", "flag + 1 = 2", "TRUE < FALSE"] {
        assert_error(expression, &envelope, SqlEvaluationError::TypeMismatch);
    }
    for identifier in [
        MessageIdentifier::Ulong(7),
        MessageIdentifier::Uuid([0; 16]),
        MessageIdentifier::Binary(vec![7]),
    ] {
        let envelope = MessageEnvelope {
            properties: MessageProperties {
                correlation_id: Some(identifier),
                ..MessageProperties::default()
            },
            ..MessageEnvelope::default()
        };
        assert_error(
            "sys.CorrelationId = '7'",
            &envelope,
            SqlEvaluationError::UnsupportedValue,
        );
    }
}

#[test]
fn boolean_operators_do_not_hide_errors_in_an_unselected_branch() {
    let envelope = envelope([
        ("left", MessageValue::Long(i64::MAX)),
        ("Color", MessageValue::Long(1)),
        ("COLOR", MessageValue::Long(1)),
    ]);
    for expression in ["TRUE OR 1 / 0 = 1", "FALSE AND 1 / 0 = 1"] {
        assert_error(expression, &envelope, SqlEvaluationError::DivisionByZero);
    }
    assert_error(
        "TRUE OR left + 1 = 0",
        &envelope,
        SqlEvaluationError::NumericOverflow,
    );
    assert_error(
        "FALSE AND color = 1",
        &envelope,
        SqlEvaluationError::AmbiguousProperty,
    );
}
