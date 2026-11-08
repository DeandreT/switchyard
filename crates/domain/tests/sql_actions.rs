//! Public, transport-free kernel contract. No store is read or mutated here.
use domain::{
    MAX_SQL_ACTION_PATCH_BYTES, MAX_SQL_ACTION_PATCH_PROPERTIES, MAX_SQL_ACTION_STATEMENTS,
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS,
    SqlActionCompileError, SqlActionError, SqlActionLimits, SqlActionPatch, SqlActionProgram,
    SqlActionProperty, SqlCompileBudget, SqlCompileError, SqlCompileLimit, SqlEvaluationBudget,
    SqlEvaluationError, SqlEvaluationLimit, SqlMessageContext, SqlProgram, SqlProperty, SqlValue,
};
use message_content::{Text, Value, Width};

fn context<'a>(properties: &'a [SqlProperty<'a>]) -> SqlMessageContext<'a> {
    SqlMessageContext {
        application_properties: properties,
        system_properties: &[],
    }
}

fn evaluate(
    source: &str,
    properties: &[SqlProperty<'_>],
) -> Result<SqlActionPatch, SqlActionError> {
    SqlActionProgram::compile(source)
        .unwrap()
        .evaluate(context(properties), &mut SqlEvaluationBudget::default())
}

fn string(value: &str) -> Value {
    Value::String(Text {
        text: value.into(),
        width: Width::One,
    })
}

fn set(name: &str, value: Value) -> SqlActionProperty {
    SqlActionProperty::Set {
        name: name.into(),
        value,
    }
}

fn remove(name: &str) -> SqlActionProperty {
    SqlActionProperty::Remove { name: name.into() }
}

#[test]
fn literals_quoted_names_scopes_comments_and_case_are_explicit() {
    let patch = evaluate(
        "sEt UsEr.[Odd name] = 'can''t; -- comment'; /* ; */ SET \"a\"\"b\" = TRUE; REMOVE [missing]",
        &[],
    ).unwrap();
    assert_eq!(
        patch.entries(),
        &[
            set("Odd name", string("can't; -- comment")),
            set("a\"b", Value::Bool(true)),
            remove("missing")
        ]
    );

    let properties = [
        SqlProperty {
            name: "Count",
            value: SqlValue::String("original"),
        },
        SqlProperty {
            name: "count",
            value: SqlValue::Byte(1),
        },
        SqlProperty {
            name: "é",
            value: SqlValue::Bool(true),
        },
    ];
    assert_eq!(
        evaluate("SET count = 127; REMOVE Count; SET É = 4", &properties)
            .unwrap()
            .entries(),
        &[
            set("count", Value::Byte(127)),
            remove("Count"),
            set("É", Value::Long(4)),
        ]
    );
}

#[test]
fn sequential_repeated_targets_keep_first_position_and_removal_resets_constructor() {
    let original = [SqlProperty {
        name: "x",
        value: SqlValue::Byte(2),
    }];
    assert_eq!(
        evaluate(
            "SET x=1; SET y=false; SET x=2; REMOVE x; SET x=128",
            &original
        )
        .unwrap()
        .entries(),
        &[set("x", Value::Long(128)), set("y", Value::Bool(false)),]
    );
    assert_eq!(
        evaluate("REMOVE x; REMOVE x; REMOVE absent", &original)
            .unwrap()
            .entries(),
        &[remove("x"), remove("absent")]
    );
    assert_eq!(
        evaluate("SET x=1; SET x='wrong family'", &original),
        Err(SqlActionError::TypeMismatch)
    );
}

#[test]
fn integer_constructors_keep_their_bounds_without_narrowing_or_wrapping() {
    let cases = [
        (
            SqlValue::Byte(0),
            i64::from(i8::MIN),
            i64::from(i8::MAX),
            Value::Byte(i8::MIN),
            Value::Byte(i8::MAX),
        ),
        (
            SqlValue::Ubyte(0),
            0,
            i64::from(u8::MAX),
            Value::UByte(0),
            Value::UByte(u8::MAX),
        ),
        (
            SqlValue::Short(0),
            i64::from(i16::MIN),
            i64::from(i16::MAX),
            Value::Short(i16::MIN),
            Value::Short(i16::MAX),
        ),
        (
            SqlValue::Ushort(0),
            0,
            i64::from(u16::MAX),
            Value::UShort(0),
            Value::UShort(u16::MAX),
        ),
        (
            SqlValue::Int(0),
            i64::from(i32::MIN),
            i64::from(i32::MAX),
            Value::Int(i32::MIN),
            Value::Int(i32::MAX),
        ),
        (
            SqlValue::Uint(0),
            0,
            i64::from(u32::MAX),
            Value::UInt(0),
            Value::UInt(u32::MAX),
        ),
        (
            SqlValue::Long(0),
            i64::MIN,
            i64::MAX,
            Value::Long(i64::MIN),
            Value::Long(i64::MAX),
        ),
        (
            SqlValue::Ulong(u64::MAX),
            0,
            i64::MAX,
            Value::ULong(0),
            Value::ULong(i64::MAX as u64),
        ),
    ];
    for (value, low, high, low_value, high_value) in cases {
        let input = [SqlProperty { name: "n", value }];
        for (number, expected) in [(low, low_value), (high, high_value)] {
            assert_eq!(
                evaluate(&format!("SET n={number}"), &input)
                    .unwrap()
                    .entries(),
                &[set("n", expected)]
            );
        }
        for overflow in [low.checked_sub(1), high.checked_add(1)]
            .into_iter()
            .flatten()
        {
            // Ulong has a wider positive range than the signed literal grammar.
            assert_eq!(
                evaluate(&format!("SET n={overflow}"), &input),
                Err(SqlActionError::NumericOverflow)
            );
        }
    }
    assert_eq!(
        evaluate(
            "SET low=-9223372036854775808; SET high=+9223372036854775807",
            &[]
        )
        .unwrap()
        .entries(),
        &[
            set("low", Value::Long(i64::MIN)),
            set("high", Value::Long(i64::MAX)),
        ]
    );
}

#[test]
fn null_absent_supported_and_unsupported_families_have_distinct_errors() {
    for value in [SqlValue::Null, SqlValue::String("old")] {
        assert_eq!(
            evaluate("SET x='new'", &[SqlProperty { name: "x", value }])
                .unwrap()
                .entries(),
            &[set("x", string("new"))]
        );
    }
    for value in [
        SqlValue::Unsupported,
        SqlValue::Float(1.0),
        SqlValue::Double(f64::NAN),
    ] {
        let input = [SqlProperty { name: "x", value }];
        assert_eq!(
            evaluate("SET x=1", &input),
            Err(SqlActionError::UnsupportedTargetType)
        );
        assert_eq!(
            evaluate("REMOVE x; SET x=true", &input).unwrap().entries(),
            &[set("x", Value::Bool(true))]
        );
        assert!(evaluate("SET unrelated=1", &input).is_ok());
    }
    for (source, value) in [
        ("SET x=1", SqlValue::Bool(false)),
        ("SET x=true", SqlValue::String("old")),
        ("SET x='s'", SqlValue::Long(1)),
    ] {
        assert_eq!(
            evaluate(source, &[SqlProperty { name: "x", value }]),
            Err(SqlActionError::TypeMismatch)
        );
    }
    assert_eq!(
        evaluate(
            "SET x=false",
            &[SqlProperty {
                name: "x",
                value: SqlValue::Bool(true)
            }]
        )
        .unwrap()
        .entries(),
        &[set("x", Value::Bool(false))]
    );
}

#[test]
fn failure_is_atomic_original_is_unchanged_and_independent_programs_do_not_share_patches() {
    let input = [SqlProperty {
        name: "n",
        value: SqlValue::Byte(1),
    }];
    let program = SqlActionProgram::compile("SET new='copy'; REMOVE absent; SET n=128").unwrap();
    let mut budget = SqlEvaluationBudget::default();
    assert_eq!(
        program.evaluate(context(&input), &mut budget),
        Err(SqlActionError::NumericOverflow)
    );
    assert!(budget.used().work > 0);
    assert_eq!(input[0].value, SqlValue::Byte(1));
    let first = evaluate("REMOVE n; SET n='isolated'", &input).unwrap();
    let second = evaluate("SET n=2", &input).unwrap();
    assert_eq!(first.into_entries(), vec![set("n", string("isolated"))]);
    assert_eq!(second.entries(), &[set("n", Value::Byte(2))]);
    assert!(
        SqlProgram::compile("n=1")
            .unwrap()
            .evaluate(context(&input), &mut SqlEvaluationBudget::default())
            .unwrap()
            .is_match()
    );
}

#[test]
fn ambiguous_exact_names_fail_only_when_read_and_case_variants_stay_distinct() {
    let input = [SqlProperty {
        name: "x",
        value: SqlValue::Byte(1),
    }; 2];
    assert_eq!(
        evaluate("SET x=2", &input),
        Err(SqlActionError::Evaluation(
            SqlEvaluationError::AmbiguousProperty
        ))
    );
    assert_eq!(
        evaluate("REMOVE x; SET x=2", &input).unwrap().entries(),
        &[set("x", Value::Long(2))]
    );
    assert!(evaluate("SET X=2", &input).is_ok());
}

#[test]
fn unsupported_grammar_and_system_targets_never_compile() {
    for source in [
        "",
        " ",
        ";",
        "REMOVE",
        "SET x",
        "SET x=",
        "SET x=1;;",
        "REMOVE x, y",
        "SET sys.Label='x'",
        "REMOVE SYS.[MessageId]",
        "SET other.x=1",
        "SET user.x.y=1",
        "REMOVE ''",
        "REMOVE []",
        "SET [a\nb]=1",
        "SET x=NULL",
        "SET x=1.0",
        "SET x=1e2",
        "SET x=9223372036854775808",
        "SET x=-9223372036854775809",
        "SET x=18446744073709551616",
        "SET x=1+2",
        "SET x=y",
        "SET x=NOT true",
        "SET x=--1",
        "SET x=(1)",
        "REMOVE (x)",
        "SET x=CAST(1 AS INT)",
        "SET x=@value",
        "DELETE x",
        "SET x=+'s'",
    ] {
        assert!(SqlActionProgram::compile(source).is_err(), "{source:?}");
    }
    assert!(matches!(
        SqlActionProgram::compile("SET sys.Label='x'"),
        Err(SqlActionCompileError::Sql(SqlCompileError::Unsupported {
            feature: "SQL action property scope"
        }))
    ));
    assert!(
        SqlProgram::compile("SET x=1").is_err(),
        "the filter grammar is unchanged"
    );
}

fn compile_limit(source: &str, kind: SqlCompileLimit, maximum: usize) {
    assert!(
        matches!(SqlActionProgram::compile(source), Err(SqlActionCompileError::Sql(SqlCompileError::Limit { kind: actual_kind, maximum: actual_maximum })) if actual_kind == kind && actual_maximum == maximum)
    );
}

#[test]
fn source_token_and_statement_limits_include_exact_boundaries() {
    let exact = format!("REMOVE[{}]", "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 8));
    let program = SqlActionProgram::compile(&exact).unwrap();
    assert_eq!(
        program.metrics().source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    compile_limit(
        &(exact + " "),
        SqlCompileLimit::SourceUtf16Units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
    );
    compile_limit(
        &"x".repeat(MAX_SQL_EXPRESSION_BYTES + 1),
        SqlCompileLimit::SourceBytes,
        MAX_SQL_EXPRESSION_BYTES,
    );
    compile_limit(
        &"x".repeat(MAX_SQL_EXPRESSION_BYTES),
        SqlCompileLimit::SourceUtf16Units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
    );
    let tokens = format!("REMOVE[x]{}", " ".repeat(MAX_SQL_EXPRESSION_TOKENS - 2));
    assert_eq!(
        SqlActionProgram::compile(&tokens).unwrap().metrics().tokens,
        MAX_SQL_EXPRESSION_TOKENS
    );
    compile_limit(
        &(tokens + " "),
        SqlCompileLimit::PhysicalTokens,
        MAX_SQL_EXPRESSION_TOKENS,
    );
    let statements = "REMOVE[x];".repeat(MAX_SQL_ACTION_STATEMENTS);
    assert_eq!(
        SqlActionProgram::compile(&statements)
            .unwrap()
            .metrics()
            .statements,
        MAX_SQL_ACTION_STATEMENTS
    );
    assert!(matches!(
        SqlActionProgram::compile(&(statements + "REMOVE[x]")),
        Err(SqlActionCompileError::Statements {
            maximum: MAX_SQL_ACTION_STATEMENTS
        })
    ));
}

#[test]
fn lexer_counts_trivia_incrementally_but_does_not_split_long_literal_tokens() {
    for trivia in [" ", "\r\n", "/**/", "-- comment\n"] {
        // A line comment and its terminating LF are separate physical tokens.
        let tokens_per_trivia = if trivia.starts_with("--") { 2 } else { 1 };
        let repeats = (MAX_SQL_EXPRESSION_TOKENS - 2) / tokens_per_trivia;
        let source = format!("REMOVE[x]{}", trivia.repeat(repeats));
        assert_eq!(
            SqlActionProgram::compile(&source).unwrap().metrics().tokens,
            MAX_SQL_EXPRESSION_TOKENS
        );
        let mut budget = SqlCompileBudget::default();
        assert!(matches!(
            SqlActionProgram::compile_with_budget(&(source + " "), &mut budget),
            Err(SqlActionCompileError::Sql(SqlCompileError::Limit {
                kind: SqlCompileLimit::PhysicalTokens,
                maximum: MAX_SQL_EXPRESSION_TOKENS
            }))
        ));
        assert_eq!(budget.used().tokens, MAX_SQL_EXPRESSION_TOKENS);
    }
    let source = format!("SET x='{}'", "s".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 8));
    let program = SqlActionProgram::compile(&source).unwrap();
    assert_eq!(
        program.metrics().source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    assert_eq!(program.metrics().tokens, 5);
    assert_eq!(
        program
            .evaluate(context(&[]), &mut SqlEvaluationBudget::default())
            .unwrap()
            .entries(),
        &[set(
            "x",
            Value::String(Text {
                text: "s".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 8),
                width: Width::Four
            })
        )]
    );

    // The source fits both source caps, but has 1,002 physical tokens. A zero
    // aggregate token budget must refuse before constructing even its first.
    let source = format!("REMOVE[x]{}", " ".repeat(1_000));
    let mut budget = SqlCompileBudget::with_limits(4_096, 0, 128);
    assert!(matches!(
        SqlActionProgram::compile_with_budget(&source, &mut budget),
        Err(SqlActionCompileError::Sql(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateTokens,
            maximum: 0
        }))
    ));
    assert_eq!(budget.used().tokens, 0);
    assert_eq!(budget.used().nodes, 0);
    assert_eq!(budget.used().source_bytes, source.len());
}

#[test]
fn shared_compile_budgets_charge_and_fail_without_reset_or_inflated_limits() {
    let source = "SET x=-1";
    let mut budget = SqlCompileBudget::with_limits(source.len(), 6, 3);
    SqlActionProgram::compile_with_budget(source, &mut budget).unwrap();
    let used = budget.used();
    assert_eq!(
        (used.source_bytes, used.tokens, used.nodes),
        (source.len(), 6, 3)
    );
    assert!(matches!(
        SqlActionProgram::compile_with_budget(source, &mut budget),
        Err(SqlActionCompileError::Sql(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            ..
        }))
    ));
    assert_eq!(budget.used(), used);
    for (mut budget, kind, maximum) in [
        (
            SqlCompileBudget::with_limits(100, 5, 10),
            SqlCompileLimit::AggregateTokens,
            5,
        ),
        (
            SqlCompileBudget::with_limits(100, 100, 2),
            SqlCompileLimit::AggregateNodes,
            2,
        ),
    ] {
        assert!(
            matches!(SqlActionProgram::compile_with_budget(source, &mut budget), Err(SqlActionCompileError::Sql(SqlCompileError::Limit { kind: actual, maximum: bound })) if actual == kind && bound == maximum)
        );
    }
}

#[test]
fn patch_limits_are_checked_before_materialization_and_at_every_statement() {
    assert_eq!(
        SqlActionLimits::with_limits(usize::MAX, usize::MAX),
        SqlActionLimits::with_limits(MAX_SQL_ACTION_PATCH_PROPERTIES, MAX_SQL_ACTION_PATCH_BYTES)
    );
    let program = SqlActionProgram::compile("SET x='hello'; SET x='a'").unwrap();
    let run = |limits| {
        program.evaluate_with_limits(context(&[]), &mut SqlEvaluationBudget::default(), limits)
    };
    // Name (5+1), string (5+5): the intermediate value costs 16,
    // though the final patch costs only 12.
    assert_eq!(
        run(SqlActionLimits::with_limits(1, 15)),
        Err(SqlActionError::PatchBytes { maximum: 15 })
    );
    assert_eq!(
        run(SqlActionLimits::with_limits(1, 16))
            .unwrap()
            .charged_bytes(),
        12
    );
    assert_eq!(
        run(SqlActionLimits::with_limits(0, 100)),
        Err(SqlActionError::PatchProperties { maximum: 0 })
    );
    let deletes = SqlActionProgram::compile("REMOVE a; REMOVE b").unwrap();
    assert_eq!(
        deletes.evaluate_with_limits(
            context(&[]),
            &mut SqlEvaluationBudget::default(),
            SqlActionLimits::with_limits(1, 100)
        ),
        Err(SqlActionError::PatchProperties { maximum: 1 })
    );
    assert_eq!(evaluate("REMOVE x", &[]).unwrap().charged_bytes(), 7);
    for length in [255, 256] {
        let patch = evaluate(&format!("SET x='{}'", "x".repeat(length)), &[]).unwrap();
        let SqlActionProperty::Set {
            value: Value::String(text),
            ..
        } = &patch.entries()[0]
        else {
            panic!("string result")
        };
        assert_eq!(
            text.width,
            if length == 255 {
                Width::One
            } else {
                Width::Four
            }
        );
    }
}

#[test]
fn work_comparison_and_materialization_are_precharged_with_exact_boundaries() {
    let program = SqlActionProgram::compile("SET x=2").unwrap();
    let input = [SqlProperty {
        name: "x",
        value: SqlValue::Byte(1),
    }];
    let mut baseline = SqlEvaluationBudget::default();
    let expected = program.evaluate(context(&input), &mut baseline).unwrap();
    let usage = baseline.used();
    assert_eq!(
        program
            .evaluate(
                context(&input),
                &mut SqlEvaluationBudget::with_limits(usage.work, usage.comparison_bytes)
            )
            .unwrap(),
        expected
    );
    for (work, bytes, kind, maximum) in [
        (
            usage.work - 1,
            usage.comparison_bytes,
            SqlEvaluationLimit::WorkUnits,
            usage.work - 1,
        ),
        (
            usage.work,
            usage.comparison_bytes - 1,
            SqlEvaluationLimit::ComparisonBytes,
            usage.comparison_bytes - 1,
        ),
    ] {
        assert_eq!(
            program.evaluate(
                context(&input),
                &mut SqlEvaluationBudget::with_limits(work, bytes)
            ),
            Err(SqlActionError::Evaluation(SqlEvaluationError::Limit {
                kind,
                maximum
            }))
        );
    }
    let huge_name = "x".repeat(100);
    let adversarial = [SqlProperty {
        name: &huge_name,
        value: SqlValue::Unsupported,
    }];
    assert_eq!(
        program.evaluate(
            context(&adversarial),
            &mut SqlEvaluationBudget::with_limits(50, 1000)
        ),
        Err(SqlActionError::Evaluation(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 50
        }))
    );
    // Reusing a spent aggregate budget cannot silently restart it.
    let mut exact = SqlEvaluationBudget::with_limits(usage.work, usage.comparison_bytes);
    program.evaluate(context(&input), &mut exact).unwrap();
    assert!(program.evaluate(context(&input), &mut exact).is_err());
    assert_eq!(exact.used(), usage);
}
