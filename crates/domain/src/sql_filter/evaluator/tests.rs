use std::collections::BTreeMap;

use crate::{MAX_TOPIC_RULE_COMPARISON_BYTES, MAX_TOPIC_RULE_MATCH_WORK};

use super::*;

fn program(nodes: Vec<SqlNode>) -> SqlProgram {
    SqlProgram {
        root: (nodes.len() - 1) as u16,
        metrics: SqlProgramMetrics {
            nodes: nodes.len(),
            ..SqlProgramMetrics::default()
        },
        nodes,
        in_operands: Vec::new(),
    }
}

fn literal(value: SqlLiteral) -> SqlNode {
    SqlNode::Literal(value)
}

fn property(name: &str) -> SqlNode {
    SqlNode::Property(SqlProperty::User(name.into()))
}

fn context(envelope: &MessageEnvelope) -> SqlMessageContext<'_> {
    SqlMessageContext {
        message_id: "authoritative",
        session_id: None,
        envelope: Some(envelope),
    }
}

fn binary_program(left: SqlNode, right: SqlNode, op: SqlBinaryOp) -> SqlProgram {
    program(vec![
        left,
        right,
        SqlNode::Binary {
            op,
            left: 0,
            right: 1,
        },
    ])
}

fn evaluate_program(
    program: &SqlProgram,
    envelope: &MessageEnvelope,
) -> Result<SqlTruth, SqlEvaluationError> {
    program.evaluate(context(envelope), &mut SqlEvaluationBudget::default())
}

#[test]
fn finite_error_cannot_hide_an_independent_like_limit() {
    let program = SqlProgram::compile("1 / 0 = 1 OR input LIKE pattern").expect("supported SQL");
    let mut envelope = MessageEnvelope::default();
    envelope
        .application_properties
        .insert("input".into(), MessageValue::String("x".into()));
    envelope.application_properties.insert(
        "pattern".into(),
        MessageValue::String("x".repeat(MAX_SQL_LIKE_PATTERN_BYTES)),
    );
    assert_eq!(
        evaluate_program(&program, &envelope),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::LikePatternBytes,
            maximum: MAX_SQL_LIKE_PATTERN_BYTES,
        })
    );
    envelope
        .application_properties
        .insert("pattern".into(), MessageValue::String("%".into()));
    assert_eq!(
        evaluate_program(&program, &envelope),
        Err(SqlEvaluationError::DivisionByZero)
    );
}

#[test]
fn ambiguous_leaf_preserves_dependent_comparison_and_like_admission() {
    let mut envelope = MessageEnvelope::default();
    envelope
        .application_properties
        .insert("value".into(), MessageValue::String("x".repeat(1000)));
    envelope
        .application_properties
        .insert("Value".into(), MessageValue::String("short".into()));
    let program = SqlProgram::compile("value = 'x'").expect("supported SQL");
    let mut budget = SqlEvaluationBudget::with_limits(MAX_TOPIC_RULE_MATCH_WORK, 100);
    assert_eq!(
        program.evaluate(context(&envelope), &mut budget),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 100,
        })
    );
    let program = SqlProgram::compile("value LIKE '%'").expect("supported SQL");
    let mut budget = SqlEvaluationBudget::with_limits(1000, MAX_TOPIC_RULE_COMPARISON_BYTES);
    assert_eq!(
        program.evaluate(context(&envelope), &mut budget),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 1000,
        })
    );
    assert_eq!(
        evaluate_program(&program, &envelope),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
}

#[test]
fn invalid_escape_still_admits_the_potential_like_product() {
    let program = SqlProgram::compile("input LIKE pattern ESCAPE escape").expect("supported SQL");
    let mut envelope = MessageEnvelope::default();
    for (key, value) in [
        ("input", "x".repeat(1000)),
        ("pattern", "%".into()),
        ("escape", "ab".into()),
    ] {
        envelope
            .application_properties
            .insert(key.into(), MessageValue::String(value));
    }
    let mut budget = SqlEvaluationBudget::with_limits(2000, MAX_TOPIC_RULE_COMPARISON_BYTES);
    assert_eq!(
        program.evaluate(context(&envelope), &mut budget),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 2000,
        })
    );
    assert_eq!(
        evaluate_program(&program, &envelope),
        Err(SqlEvaluationError::InvalidEscape)
    );
}

#[test]
fn kleene_boolean_tables_and_scalar_roots_are_explicit() {
    let envelope = MessageEnvelope::default();
    let values = [
        SqlLiteral::Bool(true),
        SqlLiteral::Bool(false),
        SqlLiteral::Null,
    ];
    let and = [
        [SqlTruth::True, SqlTruth::False, SqlTruth::Unknown],
        [SqlTruth::False, SqlTruth::False, SqlTruth::False],
        [SqlTruth::Unknown, SqlTruth::False, SqlTruth::Unknown],
    ];
    let or = [
        [SqlTruth::True, SqlTruth::True, SqlTruth::True],
        [SqlTruth::True, SqlTruth::False, SqlTruth::Unknown],
        [SqlTruth::True, SqlTruth::Unknown, SqlTruth::Unknown],
    ];
    for (left_index, left) in values.iter().enumerate() {
        for (right_index, right) in values.iter().enumerate() {
            for (op, expected) in [
                (SqlBinaryOp::And, and[left_index][right_index]),
                (SqlBinaryOp::Or, or[left_index][right_index]),
            ] {
                assert_eq!(
                    evaluate_program(
                        &binary_program(literal(left.clone()), literal(right.clone()), op),
                        &envelope
                    ),
                    Ok(expected)
                );
            }
        }
    }
    assert_eq!(
        evaluate_program(&program(vec![property("missing")]), &envelope),
        Ok(SqlTruth::Unknown)
    );
    assert_eq!(
        evaluate_program(&program(vec![literal(SqlLiteral::Int64(1))]), &envelope),
        Err(SqlEvaluationError::NonPredicate)
    );
    assert_eq!(
        evaluate_program(
            &program(vec![
                literal(SqlLiteral::Null),
                SqlNode::Unary {
                    op: SqlUnaryOp::Not,
                    input: 0
                }
            ]),
            &envelope
        ),
        Ok(SqlTruth::Unknown)
    );
}

#[test]
fn boolean_result_cannot_skip_a_runtime_error_in_another_node() {
    let expression = program(vec![
        literal(SqlLiteral::Bool(false)),
        literal(SqlLiteral::Int64(1)),
        literal(SqlLiteral::Int64(0)),
        SqlNode::Binary {
            op: SqlBinaryOp::Divide,
            left: 1,
            right: 2,
        },
        SqlNode::Binary {
            op: SqlBinaryOp::Eq,
            left: 3,
            right: 2,
        },
        SqlNode::Binary {
            op: SqlBinaryOp::And,
            left: 0,
            right: 4,
        },
    ]);
    assert_eq!(
        evaluate_program(&expression, &MessageEnvelope::default()),
        Err(SqlEvaluationError::DivisionByZero)
    );
}

#[test]
fn integer_comparisons_never_round_through_double() {
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([
            ("large".into(), MessageValue::Ulong(u64::MAX)),
            ("signed".into(), MessageValue::Long(i64::MAX)),
        ]),
        ..MessageEnvelope::default()
    };
    assert_eq!(
        evaluate_program(
            &binary_program(
                literal(SqlLiteral::Int64(9_007_199_254_740_993)),
                literal(SqlLiteral::Int64(9_007_199_254_740_992)),
                SqlBinaryOp::Eq
            ),
            &envelope
        ),
        Ok(SqlTruth::False)
    );
    assert_eq!(
        evaluate_program(
            &binary_program(
                property("large"),
                literal(SqlLiteral::Int64(i64::MAX)),
                SqlBinaryOp::Gt
            ),
            &envelope
        ),
        Ok(SqlTruth::True)
    );
    assert_eq!(
        evaluate_program(
            &binary_program(property("large"), property("signed"), SqlBinaryOp::Gt),
            &envelope
        ),
        Err(SqlEvaluationError::TypeMismatch)
    );
    assert_eq!(
        evaluate_program(
            &binary_program(
                property("large"),
                literal(SqlLiteral::Int64(-1)),
                SqlBinaryOp::Gt
            ),
            &envelope
        ),
        Err(SqlEvaluationError::TypeMismatch)
    );
}

#[test]
fn small_unsigned_and_signed_widths_promote_differently() {
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([
            ("uint".into(), MessageValue::Uint(u32::MAX)),
            ("unsigned".into(), MessageValue::Ushort(1)),
            ("signed".into(), MessageValue::Short(1)),
        ]),
        ..MessageEnvelope::default()
    };
    let unsigned = binary_program(property("uint"), property("unsigned"), SqlBinaryOp::Add);
    assert_eq!(
        evaluate_program(&unsigned, &envelope),
        Err(SqlEvaluationError::NumericOverflow)
    );
    let signed = program(vec![
        property("uint"),
        property("signed"),
        SqlNode::Binary {
            op: SqlBinaryOp::Add,
            left: 0,
            right: 1,
        },
        literal(SqlLiteral::Int64(i64::from(u32::MAX) + 1)),
        SqlNode::Binary {
            op: SqlBinaryOp::Eq,
            left: 2,
            right: 3,
        },
    ]);
    assert_eq!(evaluate_program(&signed, &envelope), Ok(SqlTruth::True));
    let negative = program(vec![
        property("uint"),
        SqlNode::Unary {
            op: SqlUnaryOp::Minus,
            input: 0,
        },
        literal(SqlLiteral::Int64(-i64::from(u32::MAX))),
        SqlNode::Binary {
            op: SqlBinaryOp::Eq,
            left: 1,
            right: 2,
        },
    ]);
    assert_eq!(evaluate_program(&negative, &envelope), Ok(SqlTruth::True));
}

#[test]
fn integer_overflow_and_zero_divisors_are_runtime_errors() {
    let envelope = MessageEnvelope::default();
    for (left, right, op, error) in [
        (
            i64::MAX,
            1,
            SqlBinaryOp::Add,
            SqlEvaluationError::NumericOverflow,
        ),
        (
            i64::MIN,
            1,
            SqlBinaryOp::Subtract,
            SqlEvaluationError::NumericOverflow,
        ),
        (
            i64::MIN,
            -1,
            SqlBinaryOp::Divide,
            SqlEvaluationError::NumericOverflow,
        ),
        (
            1,
            0,
            SqlBinaryOp::Divide,
            SqlEvaluationError::DivisionByZero,
        ),
        (
            1,
            0,
            SqlBinaryOp::Modulo,
            SqlEvaluationError::DivisionByZero,
        ),
    ] {
        assert_eq!(
            evaluate_program(
                &binary_program(
                    literal(SqlLiteral::Int64(left)),
                    literal(SqlLiteral::Int64(right)),
                    op
                ),
                &envelope
            ),
            Err(error)
        );
    }
    assert_eq!(
        evaluate_program(
            &program(vec![
                literal(SqlLiteral::Int64(i64::MIN)),
                SqlNode::Unary {
                    op: SqlUnaryOp::Minus,
                    input: 0
                }
            ]),
            &envelope
        ),
        Err(SqlEvaluationError::NumericOverflow)
    );
}

#[test]
fn ieee_nan_signed_zero_and_floating_zero_divisors_are_preserved() {
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([
            ("nan".into(), MessageValue::Double(f64::NAN.to_bits())),
            (
                "negative_zero".into(),
                MessageValue::Float((-0.0_f32).to_bits()),
            ),
        ]),
        ..MessageEnvelope::default()
    };
    for (op, expected) in [
        (SqlBinaryOp::Eq, SqlTruth::False),
        (SqlBinaryOp::Ne, SqlTruth::True),
        (SqlBinaryOp::Gt, SqlTruth::False),
    ] {
        assert_eq!(
            evaluate_program(
                &binary_program(property("nan"), property("nan"), op),
                &envelope
            ),
            Ok(expected)
        );
    }
    assert_eq!(
        evaluate_program(
            &binary_program(
                property("negative_zero"),
                literal(SqlLiteral::DoubleBits(0.0_f64.to_bits())),
                SqlBinaryOp::Eq
            ),
            &envelope
        ),
        Ok(SqlTruth::True)
    );
    let zero_divisor = program(vec![
        literal(SqlLiteral::DoubleBits(1.0_f64.to_bits())),
        literal(SqlLiteral::DoubleBits(0.0_f64.to_bits())),
        SqlNode::Binary {
            op: SqlBinaryOp::Divide,
            left: 0,
            right: 1,
        },
        literal(SqlLiteral::DoubleBits(f64::INFINITY.to_bits())),
        SqlNode::Binary {
            op: SqlBinaryOp::Eq,
            left: 2,
            right: 3,
        },
    ]);
    assert_eq!(
        evaluate_program(&zero_divisor, &envelope),
        Ok(SqlTruth::True)
    );
}

#[test]
fn unsupported_values_still_have_existence_and_null_semantics() {
    for value in [
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::Char('a'),
        MessageValue::Timestamp(0),
        MessageValue::Uuid([0; 16]),
        MessageValue::Binary(vec![0]),
        MessageValue::Symbol("symbol".into()),
        MessageValue::List(vec![MessageValue::Null]),
        MessageValue::Map(vec![]),
        MessageValue::Array(vec![]),
        MessageValue::Described {
            descriptor: crate::MessageDescriptor::Code(1),
            value: Box::new(MessageValue::Null),
        },
    ] {
        let envelope = MessageEnvelope {
            application_properties: BTreeMap::from([("value".into(), value)]),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            evaluate_program(
                &program(vec![SqlNode::Exists(SqlProperty::User("value".into()))]),
                &envelope
            ),
            Ok(SqlTruth::True)
        );
        assert_eq!(
            evaluate_program(
                &program(vec![
                    property("value"),
                    SqlNode::IsNull {
                        input: 0,
                        negated: false
                    }
                ]),
                &envelope
            ),
            Ok(SqlTruth::False)
        );
        assert_eq!(
            evaluate_program(
                &binary_program(
                    property("value"),
                    literal(SqlLiteral::Int64(0)),
                    SqlBinaryOp::Eq
                ),
                &envelope
            ),
            Err(SqlEvaluationError::UnsupportedValue)
        );
    }
}

#[test]
fn unicode_lowercase_lookup_does_not_hide_case_collisions() {
    let mut envelope = MessageEnvelope {
        application_properties: BTreeMap::from([("\u{00c5}ge".into(), MessageValue::Int(7))]),
        ..MessageEnvelope::default()
    };
    let expression = binary_program(
        property("\u{00e5}GE"),
        literal(SqlLiteral::Int64(7)),
        SqlBinaryOp::Eq,
    );
    assert_eq!(evaluate_program(&expression, &envelope), Ok(SqlTruth::True));
    envelope
        .application_properties
        .insert("\u{00e5}ge".into(), MessageValue::Null);
    assert_eq!(
        evaluate_program(&expression, &envelope),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
    assert_eq!(
        evaluate_program(
            &program(vec![SqlNode::Exists(SqlProperty::User(
                "\u{00c5}ge".into()
            ))]),
            &envelope
        ),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
    assert_eq!(
        evaluate_program(
            &program(vec![
                property("\u{00c5}ge"),
                SqlNode::IsNull {
                    input: 0,
                    negated: false
                }
            ]),
            &envelope
        ),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
}

#[test]
fn ids_come_from_context_and_optional_system_fields_are_null() {
    let mut envelope = MessageEnvelope::default();
    envelope.properties.message_id = Some(crate::MessageIdentifier::String("raw".into()));
    let session = SessionId::new("held").expect("session");
    let context = SqlMessageContext {
        message_id: "id",
        session_id: Some(&session),
        envelope: Some(&envelope),
    };
    for (property, expected) in [
        (SqlSystemProperty::MessageId, "id"),
        (SqlSystemProperty::SessionId, "held"),
    ] {
        let expression = binary_program(
            SqlNode::Property(SqlProperty::System(property)),
            literal(SqlLiteral::String(expected.into())),
            SqlBinaryOp::Eq,
        );
        assert_eq!(
            expression.evaluate(context, &mut SqlEvaluationBudget::default()),
            Ok(SqlTruth::True)
        );
    }
    let expression = program(vec![
        SqlNode::Property(SqlProperty::System(SqlSystemProperty::Subject)),
        SqlNode::IsNull {
            input: 0,
            negated: false,
        },
    ]);
    assert_eq!(evaluate_program(&expression, &envelope), Ok(SqlTruth::True));
    assert_eq!(
        evaluate_program(
            &program(vec![SqlNode::Exists(SqlProperty::System(
                SqlSystemProperty::Subject
            ))]),
            &envelope
        ),
        Ok(SqlTruth::False)
    );
}

#[test]
fn string_equality_is_ordinal_and_ordering_is_explicitly_unsupported() {
    let envelope = MessageEnvelope::default();
    assert_eq!(
        evaluate_program(
            &binary_program(
                literal(SqlLiteral::String("Case".into())),
                literal(SqlLiteral::String("case".into())),
                SqlBinaryOp::Eq
            ),
            &envelope
        ),
        Ok(SqlTruth::False)
    );
    assert_eq!(
        evaluate_program(
            &binary_program(
                literal(SqlLiteral::String("a".into())),
                literal(SqlLiteral::String("b".into())),
                SqlBinaryOp::Lt
            ),
            &envelope
        ),
        Err(SqlEvaluationError::StringOrderingUnsupported)
    );
}

#[test]
fn in_retains_unknown_and_checks_every_comparison() {
    let mut expression = program(vec![
        literal(SqlLiteral::Int64(1)),
        literal(SqlLiteral::Int64(1)),
        literal(SqlLiteral::Null),
        SqlNode::In {
            input: 0,
            start: 0,
            len: 2,
            negated: false,
        },
    ]);
    expression.in_operands = vec![1, 2];
    assert_eq!(
        evaluate_program(&expression, &MessageEnvelope::default()),
        Ok(SqlTruth::True)
    );
    expression.nodes[1] = literal(SqlLiteral::Int64(2));
    assert_eq!(
        evaluate_program(&expression, &MessageEnvelope::default()),
        Ok(SqlTruth::Unknown)
    );
    expression.nodes[1] = literal(SqlLiteral::Int64(1));
    expression.nodes[2] = literal(SqlLiteral::String("wrong-type".into()));
    assert_eq!(
        evaluate_program(&expression, &MessageEnvelope::default()),
        Err(SqlEvaluationError::TypeMismatch)
    );
}

#[test]
fn like_is_anchored_literal_safe_and_newline_unicode_aware() {
    let envelope = MessageEnvelope::default();
    for (input, pattern, expected) in [
        ("a\nb", "a_b", true),
        ("\u{1f600}", "_", true),
        ("a|b", "a|b", true),
        ("b", "a|b", false),
        ("prefix-a", "a", false),
        ("a\n", "a", false),
        ("Case", "case", false),
        ("", "%", true),
    ] {
        let expression = program(vec![
            literal(SqlLiteral::String(input.into())),
            literal(SqlLiteral::String(pattern.into())),
            SqlNode::Like {
                input: 0,
                pattern: 1,
                escape: None,
                negated: false,
            },
        ]);
        assert_eq!(
            evaluate_program(&expression, &envelope),
            Ok(if expected {
                SqlTruth::True
            } else {
                SqlTruth::False
            })
        );
    }
    let expression = program(vec![
        literal(SqlLiteral::String("a%_".into())),
        literal(SqlLiteral::String("a!%!_".into())),
        literal(SqlLiteral::String("!".into())),
        SqlNode::Like {
            input: 0,
            pattern: 1,
            escape: Some(2),
            negated: false,
        },
    ]);
    assert_eq!(evaluate_program(&expression, &envelope), Ok(SqlTruth::True));
}

#[test]
fn property_budget_exact_boundary_and_refusal_are_observable() {
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([("NaMe".into(), MessageValue::String("v".into()))]),
        ..MessageEnvelope::default()
    };
    let expression = binary_program(
        property("name"),
        literal(SqlLiteral::String("v".into())),
        SqlBinaryOp::Eq,
    );
    let mut budget = SqlEvaluationBudget::with_limits(31, 38);
    assert_eq!(
        expression.evaluate(context(&envelope), &mut budget),
        Ok(SqlTruth::True)
    );
    assert_eq!(
        budget.used(),
        SqlEvaluationUsage {
            work: 31,
            comparison_bytes: 38
        }
    );
    assert_eq!(
        expression.evaluate(
            context(&envelope),
            &mut SqlEvaluationBudget::with_limits(30, 38)
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 30
        })
    );
    assert_eq!(
        expression.evaluate(
            context(&envelope),
            &mut SqlEvaluationBudget::with_limits(31, 37)
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 37
        })
    );
}

#[test]
fn like_product_is_reserved_before_regex_allocation() {
    let expression = program(vec![
        literal(SqlLiteral::String("ab".into())),
        literal(SqlLiteral::String("a%".into())),
        SqlNode::Like {
            input: 0,
            pattern: 1,
            escape: None,
            negated: false,
        },
    ]);
    let envelope = MessageEnvelope::default();
    let mut budget = SqlEvaluationBudget::with_limits(31, 28);
    assert_eq!(
        expression.evaluate(context(&envelope), &mut budget),
        Ok(SqlTruth::True)
    );
    assert_eq!(
        budget.used(),
        SqlEvaluationUsage {
            work: 31,
            comparison_bytes: 28
        }
    );
    assert_eq!(
        expression.evaluate(
            context(&envelope),
            &mut SqlEvaluationBudget::with_limits(30, 28)
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 30
        })
    );
}
