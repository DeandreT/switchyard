//! Public, ephemeral predicate contracts; no persisted rule or wire integration.

use domain::{
    MAX_SQL_COMPARISON_BYTES, MAX_SQL_COMPILE_SOURCE_BYTES, MAX_SQL_EVALUATION_WORK,
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS,
    SqlCompileBudget, SqlCompileError, SqlCompileLimit, SqlEvaluationBudget, SqlEvaluationError,
    SqlEvaluationLimit, SqlMessageContext, SqlProgram, SqlProperty, SqlSystemProperty,
    SqlSystemValue, SqlTruth, SqlValue,
};

fn evaluate(
    expression: &str,
    properties: &[SqlProperty<'_>],
) -> Result<SqlTruth, SqlEvaluationError> {
    SqlProgram::compile(expression).unwrap().evaluate(
        SqlMessageContext {
            application_properties: properties,
            system_properties: &[],
        },
        &mut SqlEvaluationBudget::default(),
    )
}

fn property<'a>(name: &'a str, value: SqlValue<'a>) -> SqlProperty<'a> {
    SqlProperty { name, value }
}

#[test]
fn precedence_parentheses_and_only_true_matches() {
    for (expression, expected) in [
        ("TRUE OR FALSE AND FALSE", SqlTruth::True),
        ("(TRUE OR FALSE) AND FALSE", SqlTruth::False),
        ("NOT FALSE AND TRUE", SqlTruth::True),
        ("NOT (TRUE AND FALSE)", SqlTruth::True),
        (
            "2>1 AND 2>=2 AND 1<2 AND 1<=1 AND 1<>2 AND 1!=2",
            SqlTruth::True,
        ),
        ("NULL=1", SqlTruth::Unknown),
    ] {
        let actual = evaluate(expression, &[]).unwrap();
        assert_eq!(actual, expected, "{expression}");
        assert_eq!(actual.is_match(), actual == SqlTruth::True);
    }
    assert_eq!(evaluate("42", &[]), Err(SqlEvaluationError::NonPredicate));
    assert_eq!(
        evaluate("'text'", &[]),
        Err(SqlEvaluationError::NonPredicate)
    );
}

#[test]
fn all_three_valued_boolean_cells_are_deterministic() {
    let names = ["TRUE", "FALSE", "NULL"];
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
    for (left, left_name) in names.iter().enumerate() {
        for (right, right_name) in names.iter().enumerate() {
            assert_eq!(
                evaluate(&format!("{left_name} AND {right_name}"), &[]).unwrap(),
                and[left][right]
            );
            assert_eq!(
                evaluate(&format!("{left_name} OR {right_name}"), &[]).unwrap(),
                or[left][right]
            );
        }
    }
    assert_eq!(evaluate("NOT NULL", &[]).unwrap(), SqlTruth::Unknown);
}

#[test]
fn missing_user_null_and_exists_have_distinct_presence() {
    let values = [
        property("present", SqlValue::Null),
        property("other", SqlValue::Unsupported),
    ];
    for (expression, expected) in [
        ("missing=1", SqlTruth::Unknown),
        ("missing!=1", SqlTruth::Unknown),
        ("present=1", SqlTruth::Unknown),
        ("missing IS NULL", SqlTruth::True),
        ("present IS NULL", SqlTruth::True),
        ("present IS NOT NULL", SqlTruth::False),
        ("EXISTS(present)", SqlTruth::True),
        ("EXISTS(missing)", SqlTruth::False),
        ("NOT EXISTS(missing)", SqlTruth::True),
    ] {
        assert_eq!(
            evaluate(expression, &values).unwrap(),
            expected,
            "{expression}"
        );
    }
}

#[test]
fn quoted_unicode_and_case_insensitive_names_preserve_ordinal_strings() {
    let values = [
        property("a]b", SqlValue::String("O'Brien")),
        property("a\"b", SqlValue::Bool(true)),
        property("Property With Space", SqlValue::Int(3)),
        property("\u{00c4}pfel", SqlValue::String("red")),
    ];
    for expression in [
        "[a]]b]='O''Brien'",
        "\"a\"\"b\"=TRUE",
        "USER.[PROPERTY WITH SPACE]=3",
        "\u{00c4}PFEL='red'",
        "[\u{00c4}pfel]='red'",
        "[a]]b]!='o''brien'",
    ] {
        assert_eq!(
            evaluate(expression, &values).unwrap(),
            SqlTruth::True,
            "{expression}"
        );
    }
    let collisions = [
        property("Color", SqlValue::String("red")),
        property("color", SqlValue::String("blue")),
    ];
    assert_eq!(
        evaluate("color='red'", &collisions),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
    assert_eq!(
        evaluate("EXISTS(color)", &collisions),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
    assert_eq!(
        evaluate("EXISTS([\u{00e4}pfel])", &values).unwrap(),
        SqlTruth::False
    );
    let distinct = [
        property("\u{03c3}", SqlValue::Int(1)),
        property("\u{03a3}", SqlValue::Int(2)),
    ];
    assert_eq!(
        evaluate("[\u{03c3}]=1 AND [\u{03a3}]=2", &distinct).unwrap(),
        SqlTruth::True
    );
    assert_eq!(
        evaluate("'a'<'b'", &[]),
        Err(SqlEvaluationError::StringOrderingUnsupported)
    );
}

#[test]
fn system_whitelist_is_explicit_and_never_falls_back_to_user() {
    let users = [property("MessageId", SqlValue::String("user"))];
    let systems = [
        SqlSystemValue {
            property: SqlSystemProperty::MessageId,
            value: SqlValue::String("system"),
        },
        SqlSystemValue {
            property: SqlSystemProperty::Subject,
            value: SqlValue::Null,
        },
    ];
    let context = SqlMessageContext {
        application_properties: &users,
        system_properties: &systems,
    };
    for expression in [
        "sys.MessageId='system' AND MessageId='user'",
        "SYS.Label IS NULL",
        "EXISTS(sys.Subject)",
    ] {
        assert_eq!(
            SqlProgram::compile(expression)
                .unwrap()
                .evaluate(context, &mut SqlEvaluationBudget::default())
                .unwrap(),
            SqlTruth::True
        );
    }
    assert_eq!(
        evaluate("sys.MessageId='user'", &users),
        Err(SqlEvaluationError::MissingSystemProperty)
    );
    assert_eq!(
        evaluate("EXISTS(sys.MessageId)", &users),
        Err(SqlEvaluationError::MissingSystemProperty)
    );
    for name in [
        "CorrelationId",
        "MessageId",
        "To",
        "ReplyTo",
        "Subject",
        "Label",
        "SessionId",
        "ReplyToSessionId",
        "ContentType",
    ] {
        assert!(SqlProgram::compile(&format!("sys.{name} IS NULL")).is_ok());
    }
    for expression in ["sys.Other=1", "other.MessageId=1", "user.nested.key=1"] {
        assert!(matches!(
            SqlProgram::compile(expression),
            Err(SqlCompileError::Unsupported { .. })
        ));
    }
    let duplicate = [systems[0], systems[0]];
    assert_eq!(
        SqlProgram::compile("sys.MessageId='system'")
            .unwrap()
            .evaluate(
                SqlMessageContext {
                    application_properties: &[],
                    system_properties: &duplicate
                },
                &mut SqlEvaluationBudget::default(),
            ),
        Err(SqlEvaluationError::AmbiguousProperty)
    );
}

#[test]
fn numeric_widths_literal_origin_and_signed_extremes_are_preserved() {
    for value in [
        SqlValue::Byte(1),
        SqlValue::Ubyte(1),
        SqlValue::Short(1),
        SqlValue::Ushort(1),
        SqlValue::Int(1),
        SqlValue::Uint(1),
        SqlValue::Long(1),
        SqlValue::Ulong(1),
        SqlValue::Float(1.0),
        SqlValue::Double(1.0),
    ] {
        assert_eq!(
            evaluate("x=1", &[property("x", value)]).unwrap(),
            SqlTruth::True
        );
    }
    let values = [
        property("signed", SqlValue::Long(1)),
        property("unsigned", SqlValue::Ulong(1)),
        property("min", SqlValue::Long(i64::MIN)),
        property("max", SqlValue::Long(i64::MAX)),
    ];
    assert_eq!(
        evaluate("signed=unsigned", &values),
        Err(SqlEvaluationError::TypeMismatch)
    );
    assert_eq!(
        evaluate("unsigned=-1", &values),
        Err(SqlEvaluationError::TypeMismatch)
    );
    for expression in [
        "min=-9223372036854775808",
        "max=9223372036854775807",
        "-12 < -11",
        "+12=12",
        "-0.5=-5e-1",
    ] {
        assert_eq!(
            evaluate(expression, &values).unwrap(),
            SqlTruth::True,
            "{expression}"
        );
    }
    for expression in ["9223372036854775808=1", "-9223372036854775809=1", "1e309=1"] {
        assert!(matches!(
            SqlProgram::compile(expression),
            Err(SqlCompileError::Unsupported { .. })
        ));
    }
    // C# floating promotion may round a wide integer; do not silently use exact-decimal comparison.
    assert_eq!(
        evaluate(
            "large=9007199254740992.0",
            &[property("large", SqlValue::Long(9_007_199_254_740_993))]
        )
        .unwrap(),
        SqlTruth::True
    );
}

#[test]
fn typed_property_ieee_values_have_declared_comparison_semantics() {
    for nan in [SqlValue::Float(f32::NAN), SqlValue::Double(f64::NAN)] {
        let values = [property("x", nan)];
        for (expression, expected) in [
            ("x=1.0", SqlTruth::False),
            ("x!=1.0", SqlTruth::True),
            ("x>1.0", SqlTruth::False),
            ("x>=1.0", SqlTruth::False),
            ("x<1.0", SqlTruth::False),
            ("x<=1.0", SqlTruth::False),
        ] {
            assert_eq!(evaluate(expression, &values).unwrap(), expected);
        }
    }
    assert_eq!(
        evaluate("x>1.0", &[property("x", SqlValue::Double(f64::INFINITY))]).unwrap(),
        SqlTruth::True
    );
    assert_eq!(
        evaluate(
            "x<1.0",
            &[property("x", SqlValue::Float(f32::NEG_INFINITY))]
        )
        .unwrap(),
        SqlTruth::True
    );
    assert_eq!(
        evaluate("x=0.0", &[property("x", SqlValue::Float(-0.0))]).unwrap(),
        SqlTruth::True
    );
}

#[test]
fn incompatible_and_unsupported_inputs_are_errors_not_false_matches() {
    for expression in ["TRUE=1", "'1'=1", "TRUE>FALSE", "NOT 1", "TRUE AND 1"] {
        assert_eq!(
            evaluate(expression, &[]),
            Err(SqlEvaluationError::TypeMismatch),
            "{expression}"
        );
    }
    assert_eq!(
        evaluate("x=1", &[property("x", SqlValue::Unsupported)]),
        Err(SqlEvaluationError::UnsupportedValue)
    );
    assert_eq!(
        evaluate("FALSE AND x=1", &[property("x", SqlValue::Unsupported)]),
        Err(SqlEvaluationError::UnsupportedValue)
    );
    for expression in ["NULL=x", "EXISTS(x)", "x IS NULL", "x IS NOT NULL"] {
        assert_eq!(
            evaluate(expression, &[property("x", SqlValue::Unsupported)]),
            Err(SqlEvaluationError::UnsupportedValue)
        );
    }
}

#[test]
fn later_language_children_and_statement_extensions_are_refused() {
    for expression in [
        "x IN (1,2)",
        "x LIKE 'a%'",
        "x+1=2",
        "-x=1",
        "p('x')=1",
        "property('x')=1",
        "newid()=1",
        "CAST(x AS INT)=1",
        "x BETWEEN 1 AND 2",
        "x IN (SELECT y FROM t)",
        "CASE WHEN TRUE THEN TRUE ELSE FALSE END",
        "EXISTS(1)",
        "EXISTS(DISTINCT x)",
    ] {
        assert!(
            matches!(
                SqlProgram::compile(expression),
                Err(SqlCompileError::Unsupported { .. })
            ),
            "{expression}"
        );
    }
    for expression in [
        "",
        "-- only a comment",
        "TRUE;",
        "TRUE;FALSE",
        "TRUE trailing",
        "SELECT 1",
        "?=1",
        "_name=1",
        "EXISTS()",
        "'unclosed",
    ] {
        assert!(SqlProgram::compile(expression).is_err(), "{expression}");
    }
}

#[test]
fn source_utf16_and_physical_token_boundaries_precede_parsing() {
    let exact = format!("'{}'", "a".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 2));
    let metrics = SqlProgram::compile(&exact).unwrap().metrics();
    assert_eq!(metrics.source_utf16_units, MAX_SQL_EXPRESSION_UTF16_UNITS);
    assert_eq!(metrics.source_bytes, exact.len());
    for (source, kind) in [
        (
            "a".repeat(MAX_SQL_EXPRESSION_BYTES + 1),
            SqlCompileLimit::SourceBytes,
        ),
        (
            "a".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            SqlCompileLimit::SourceUtf16Units,
        ),
        (
            format!(
                "'{}'",
                "\u{1f600}".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS / 2)
            ),
            SqlCompileLimit::SourceUtf16Units,
        ),
    ] {
        let mut budget = SqlCompileBudget::default();
        assert!(
            matches!(SqlProgram::compile_with_budget(&source, &mut budget), Err(SqlCompileError::Limit { kind: actual, .. }) if actual == kind)
        );
        assert_eq!(budget.used().source_bytes, 0);
    }
    let exact_tokens = format!("{}TRUE", " ".repeat(MAX_SQL_EXPRESSION_TOKENS - 1));
    assert_eq!(
        SqlProgram::compile(&exact_tokens).unwrap().metrics().tokens,
        MAX_SQL_EXPRESSION_TOKENS
    );
    let too_many = format!(" {exact_tokens}");
    assert!(matches!(
        SqlProgram::compile(&too_many),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::PhysicalTokens,
            ..
        })
    ));
    let comments = format!("{}TRUE", "/*x*/".repeat(MAX_SQL_EXPRESSION_TOKENS));
    assert!(matches!(
        SqlProgram::compile(&comments),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::PhysicalTokens,
            ..
        })
    ));
    let deep = format!("{}TRUE{}", "(".repeat(40), ")".repeat(40));
    assert!(matches!(
        SqlProgram::compile(&deep),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::ParserDepth,
            ..
        })
    ));
}

#[test]
fn shared_compile_limits_are_checked_and_failed_work_is_not_refunded() {
    let mut source = SqlCompileBudget::with_limits(4, usize::MAX, usize::MAX);
    SqlProgram::compile_with_budget("TRUE", &mut source).unwrap();
    assert!(matches!(
        SqlProgram::compile_with_budget("TRUE", &mut source),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            maximum: 4
        })
    ));
    let mut tokens = SqlCompileBudget::with_limits(100, 1, 100);
    assert_eq!(
        SqlProgram::compile_with_budget("!", &mut tokens).unwrap_err(),
        SqlCompileError::Syntax
    );
    assert_eq!(tokens.used().source_bytes, 1);
    assert_eq!(tokens.used().tokens, 1);
    assert!(matches!(
        SqlProgram::compile_with_budget("TRUE", &mut tokens),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateTokens,
            maximum: 1
        })
    ));
    let mut nodes = SqlCompileBudget::with_limits(100, 100, 1);
    assert!(matches!(
        SqlProgram::compile_with_budget("TRUE=FALSE", &mut nodes),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 1
        })
    ));
    assert_eq!(nodes.used().nodes, 1);
    let mut clamped = SqlCompileBudget::with_limits(usize::MAX, usize::MAX, usize::MAX);
    for _ in 0..MAX_SQL_COMPILE_SOURCE_BYTES / 1_024 {
        SqlProgram::compile_with_budget(&format!("'{}'", "a".repeat(1_022)), &mut clamped).unwrap();
    }
    assert!(matches!(
        SqlProgram::compile_with_budget("TRUE", &mut clamped),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            maximum: MAX_SQL_COMPILE_SOURCE_BYTES
        })
    ));
}

#[test]
fn evaluation_limits_are_shared_and_outrank_finite_failures() {
    let program = SqlProgram::compile("TRUE").unwrap();
    let mut work = SqlEvaluationBudget::with_limits(1, usize::MAX);
    assert_eq!(
        program
            .evaluate(SqlMessageContext::default(), &mut work)
            .unwrap(),
        SqlTruth::True
    );
    assert_eq!(work.used().work, 1);
    assert_eq!(
        program.evaluate(SqlMessageContext::default(), &mut work),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 1
        })
    );
    let mut bytes = SqlEvaluationBudget::with_limits(100, 6);
    let comparison = SqlProgram::compile("'abc'='abc'").unwrap();
    assert_eq!(
        comparison
            .evaluate(SqlMessageContext::default(), &mut bytes)
            .unwrap(),
        SqlTruth::True
    );
    assert_eq!(bytes.used().comparison_bytes, 6);
    assert_eq!(
        comparison.evaluate(SqlMessageContext::default(), &mut bytes),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 6
        })
    );
    let expression = SqlProgram::compile("1='x' AND 'long'='long'").unwrap();
    let mut limited = SqlEvaluationBudget::with_limits(100, 1);
    assert_eq!(
        expression.evaluate(SqlMessageContext::default(), &mut limited),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 1
        })
    );
    assert_eq!(limited.used().comparison_bytes, 1);
    let input = [property("a very long name", SqlValue::Bool(true))];
    assert!(matches!(
        program.evaluate(
            SqlMessageContext {
                application_properties: &input,
                system_properties: &[]
            },
            &mut SqlEvaluationBudget::with_limits(100, 0)
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 0
        })
    ));
    let huge = "x".repeat(MAX_SQL_COMPARISON_BYTES + 1);
    assert!(matches!(
        SqlProgram::compile("x='a'").unwrap().evaluate(
            SqlMessageContext {
                application_properties: &[property("x", SqlValue::String(&huge))],
                system_properties: &[]
            },
            &mut SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX),
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: MAX_SQL_COMPARISON_BYTES
        })
    ));
    let excessive = vec![property("x", SqlValue::Null); MAX_SQL_EVALUATION_WORK];
    assert!(matches!(
        program.evaluate(
            SqlMessageContext {
                application_properties: &excessive,
                system_properties: &[]
            },
            &mut SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX)
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: MAX_SQL_EVALUATION_WORK
        })
    ));
}

#[test]
fn deterministic_malformed_and_typed_property_corpus_has_no_panics() {
    let alphabet = [
        'a',
        'Z',
        '0',
        '1',
        ' ',
        '\'',
        '"',
        '[',
        ']',
        '(',
        ')',
        '.',
        '=',
        '!',
        '-',
        ';',
        '\u{00e4}',
        '\u{1f600}',
    ];
    let mut state = 0x95_u64;
    for length in 0..96 {
        for _ in 0..8 {
            let mut expression = String::new();
            for _ in 0..length {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                expression.push(alphabet[(state as usize) % alphabet.len()]);
            }
            let first = SqlProgram::compile(&expression);
            let second = SqlProgram::compile(&expression);
            match (first, second) {
                (Err(left), Err(right)) => assert_eq!(left, right),
                (Ok(left), Ok(right)) => {
                    assert_eq!(left.metrics(), right.metrics());
                    assert_eq!(
                        left.evaluate(
                            SqlMessageContext::default(),
                            &mut SqlEvaluationBudget::default()
                        ),
                        right.evaluate(
                            SqlMessageContext::default(),
                            &mut SqlEvaluationBudget::default()
                        )
                    );
                }
                _ => panic!("nondeterministic compile: {expression:?}"),
            }
        }
    }
    for value in -100_i32..=100 {
        let values = [property("value", SqlValue::Int(value))];
        assert_eq!(
            evaluate(&format!("value={value}"), &values).unwrap(),
            SqlTruth::True
        );
        assert_eq!(
            evaluate(&format!("value>{value}"), &values).unwrap(),
            SqlTruth::False
        );
    }
}
