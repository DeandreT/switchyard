use std::collections::BTreeMap;

use super::*;
use crate::{BrokerError, MAX_SQL_EXPRESSION_TOKENS, MessageDescriptor, MessageValue, SqlProgram};

fn envelope(properties: impl IntoIterator<Item = (&'static str, MessageValue)>) -> MessageEnvelope {
    MessageEnvelope {
        application_properties: properties
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
        ..MessageEnvelope::default()
    }
}

fn run(
    source: &str,
    original: &MessageEnvelope,
) -> Result<(MessageEnvelope, Option<SqlActionError>), BrokerError> {
    let program = SqlActionProgram::compile(source).expect("supported local literal action");
    let checked = program.check("id", 0, Some(original))?;
    let mut result = original.clone();
    program.apply_checked(&checked, &mut result);
    Ok((result, checked.error()))
}

#[test]
fn literal_v2_retains_source_and_exact_scalar_constructors() -> Result<(), BrokerError> {
    let source = "  SET user.[a.b]='it''s'; SET enabled=TRUE;SET number=-7; ";
    let action = SqlAction::new(source).expect("v2 source");
    assert_eq!(action.expression(), source);
    assert_eq!(action.semantic_version(), 2);
    let (result, error) = run(source, &MessageEnvelope::default())?;
    assert_eq!(error, None);
    assert_eq!(
        result.application_properties,
        BTreeMap::from([
            ("a.b".into(), MessageValue::String("it's".into())),
            ("enabled".into(), MessageValue::Bool(true)),
            ("number".into(), MessageValue::Long(-7)),
        ])
    );
    Ok(())
}

#[test]
fn literal_v1_remove_remains_remove_only() -> Result<(), BrokerError> {
    let action = SqlAction::with_semantic_version("REMOVE a", 1).expect("v1 REMOVE");
    assert_eq!(action.semantic_version(), 1);
    assert!(SqlAction::with_semantic_version("SET a=1", 1).is_err());
    let program = SqlActionProgram::compile_version(action.expression(), 1).expect("v1 compile");
    let original = envelope([("a", MessageValue::Long(1)), ("A", MessageValue::Long(2))]);
    let checked = program.check("id", 0, Some(&original))?;
    let mut result = original.clone();
    program.apply_checked(&checked, &mut result);
    assert!(!result.application_properties.contains_key("a"));
    assert_eq!(result.application_properties["A"], MessageValue::Long(2));
    Ok(())
}

#[test]
fn literal_versioned_decode_does_not_compile_or_relabel() -> Result<(), crate::CodecError> {
    for version in [1_u32, 2] {
        let encoded = crate::codec::encode(&(version, "SET a=1"))?;
        let decoded = crate::codec::decode::<SqlAction>(&encoded)?;
        assert_eq!(decoded.semantic_version(), version);
        assert_eq!(crate::codec::encode(&decoded)?, encoded);
        assert_eq!(
            SqlActionProgram::compile_version(decoded.expression(), version).is_ok(),
            version == 2
        );
    }
    assert_eq!(
        crate::codec::decode::<SqlAction>(&crate::codec::encode(&(3_u32, "REMOVE a"))?),
        Err(crate::CodecError::Decode)
    );
    struct ForbiddenSource;
    impl From<ForbiddenSource> for String {
        fn from(_: ForbiddenSource) -> Self {
            panic!("unknown version must precede source conversion")
        }
    }
    assert!(matches!(
        SqlAction::with_semantic_version(ForbiddenSource, 3),
        Err(SqlCompileError::Unsupported {
            feature: "SQL action semantic version"
        })
    ));
    Ok(())
}

#[test]
fn literal_integer_extremes_are_checked() -> Result<(), BrokerError> {
    for (source, expected) in [
        ("SET a=-9223372036854775808", i64::MIN),
        ("SET a=9223372036854775807", i64::MAX),
        ("SET a=+7", 7),
        ("SET a=-0", 0),
    ] {
        let (result, error) = run(source, &MessageEnvelope::default())?;
        assert_eq!(error, None);
        assert_eq!(
            result.application_properties["a"],
            MessageValue::Long(expected)
        );
    }
    for source in [
        "SET a=9223372036854775808",
        "SET a=-9223372036854775809",
        "SET a=18446744073709551616",
        "SET a=1.0",
        "SET a=1e2",
        "SET a=--1",
    ] {
        assert!(SqlAction::new(source).is_err(), "{source}");
    }
    Ok(())
}

#[test]
fn literal_expressions_system_targets_and_null_are_refused() {
    for source in [
        "SET a=NULL",
        "SET a=b",
        "SET a=1+2",
        "SET a=newid()",
        "SET a=(1)",
        "SET a=-TRUE",
        "SET a=-(1)",
        "SET sys.Subject='x'",
        "SET foreign.a=1",
        "SET property('a')=1",
        "SET a=1,b=2",
    ] {
        assert!(SqlAction::new(source).is_err(), "{source}");
    }
}

#[test]
fn literal_set_and_remove_count_every_statement_and_operand() {
    let source = format!("{}SET[a]=-1", "REMOVE[a];".repeat(31));
    let mut budget = SqlCompileBudget::with_limits(4096, 128, 100);
    SqlActionProgram::compile_with_budget(&source, &mut budget).expect("32 mixed statements");
    assert_eq!(budget.used().nodes, 34);
    assert!(matches!(
        SqlActionProgram::compile(&"REMOVE[a];".repeat(33)),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::Nodes,
            maximum: 32
        })
    ));
    let mut budget = SqlCompileBudget::with_limits(4096, 128, 2);
    assert!(matches!(
        SqlActionProgram::compile_with_budget("SET[a]=-1", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 2
        })
    ));
    assert_eq!(budget.used().nodes, 2);
}

#[test]
fn literal_source_and_physical_token_limits_precede_parser_work() {
    for (source, kind) in [
        (
            "x".repeat(MAX_SQL_EXPRESSION_BYTES + 1),
            SqlCompileLimit::SourceBytes,
        ),
        (
            "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            SqlCompileLimit::SourceUtf16Units,
        ),
        (
            format!("{}SET a=1", " ".repeat(MAX_SQL_EXPRESSION_TOKENS + 1)),
            SqlCompileLimit::PhysicalTokens,
        ),
        (
            format!("SET a={}1{}", "(".repeat(40), ")".repeat(40)),
            SqlCompileLimit::ParserDepth,
        ),
    ] {
        assert!(
            matches!(SqlActionProgram::compile(&source), Err(SqlCompileError::Limit { kind: actual, .. }) if actual == kind)
        );
    }
}

#[test]
fn literal_aggregate_compile_budget_includes_values() {
    let mut budget = SqlCompileBudget::with_limits(4096, 128, 3);
    SqlProgram::compile_with_budget("TRUE", &mut budget).expect("one predicate node");
    SqlActionProgram::compile_with_budget("SET[a]=1", &mut budget)
        .expect("operation and literal nodes");
    assert_eq!(budget.used().nodes, 3);
    assert!(matches!(
        SqlActionProgram::compile_with_budget("REMOVE[a]", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 3
        })
    ));
    assert_eq!(budget.used().nodes, 3);
}

#[test]
fn literal_missing_and_null_targets_take_literal_types() -> Result<(), BrokerError> {
    let original = envelope([
        ("a", MessageValue::Null),
        ("b", MessageValue::Null),
        ("c", MessageValue::Null),
    ]);
    let (result, error) = run("SET a='x';SET b=FALSE;SET c=7;SET absent=9", &original)?;
    assert_eq!(error, None);
    assert_eq!(
        result.application_properties["a"],
        MessageValue::String("x".into())
    );
    assert_eq!(
        result.application_properties["b"],
        MessageValue::Bool(false)
    );
    assert_eq!(result.application_properties["c"], MessageValue::Long(7));
    assert_eq!(
        result.application_properties["absent"],
        MessageValue::Long(9)
    );
    assert!(
        original
            .application_properties
            .values()
            .all(|value| value == &MessageValue::Null)
    );
    Ok(())
}

#[test]
fn literal_existing_string_and_boolean_preserve_types() -> Result<(), BrokerError> {
    let original = envelope([
        ("text", MessageValue::String("old".into())),
        ("flag", MessageValue::Bool(true)),
    ]);
    let (result, error) = run("SET text='new';SET flag=FALSE", &original)?;
    assert_eq!(error, None);
    assert_eq!(
        result.application_properties["text"],
        MessageValue::String("new".into())
    );
    assert_eq!(
        result.application_properties["flag"],
        MessageValue::Bool(false)
    );
    for source in [
        "SET text=1",
        "SET text=TRUE",
        "SET flag=1",
        "SET flag='false'",
    ] {
        let (result, error) = run(source, &original)?;
        assert_eq!(error, Some(SqlActionError::TypeMismatch));
        assert_eq!(result, original);
    }
    Ok(())
}

#[test]
fn literal_integer_targets_preserve_all_checked_widths() -> Result<(), BrokerError> {
    for (original, maximum, expected) in [
        (MessageValue::Byte(0), 127, MessageValue::Byte(127)),
        (MessageValue::Ubyte(0), 255, MessageValue::Ubyte(255)),
        (MessageValue::Short(0), 32767, MessageValue::Short(32767)),
        (MessageValue::Ushort(0), 65535, MessageValue::Ushort(65535)),
        (
            MessageValue::Int(0),
            i32::MAX as i64,
            MessageValue::Int(i32::MAX),
        ),
        (
            MessageValue::Uint(0),
            u32::MAX as i64,
            MessageValue::Uint(u32::MAX),
        ),
        (
            MessageValue::Long(0),
            i64::MAX,
            MessageValue::Long(i64::MAX),
        ),
        (
            MessageValue::Ulong(0),
            i64::MAX,
            MessageValue::Ulong(i64::MAX as u64),
        ),
    ] {
        let original = envelope([("a", original)]);
        let (result, error) = run(&format!("SET a={maximum}"), &original)?;
        assert_eq!(error, None);
        assert_eq!(result.application_properties["a"], expected);
        if maximum < i64::MAX {
            let (result, error) = run(&format!("SET a={}", maximum + 1), &original)?;
            assert_eq!(error, Some(SqlActionError::NumericOverflow));
            assert_eq!(result, original);
        }
    }
    for original in [
        MessageValue::Ubyte(0),
        MessageValue::Ushort(0),
        MessageValue::Uint(0),
        MessageValue::Ulong(0),
    ] {
        let original = envelope([("a", original)]);
        assert_eq!(
            run("SET a=-1", &original)?.1,
            Some(SqlActionError::NumericOverflow)
        );
    }
    for (value, minimum) in [
        (MessageValue::Byte(0), -128),
        (MessageValue::Short(0), -32768),
        (MessageValue::Int(0), i32::MIN as i64),
    ] {
        let original = envelope([("a", value)]);
        assert_eq!(run(&format!("SET a={minimum}"), &original)?.1, None);
        assert_eq!(
            run(&format!("SET a={}", minimum - 1), &original)?.1,
            Some(SqlActionError::NumericOverflow)
        );
    }
    Ok(())
}

#[test]
fn literal_special_and_compound_targets_fail_without_inferred_clr_type() -> Result<(), BrokerError>
{
    for value in [
        MessageValue::Float(0),
        MessageValue::Double(0),
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::Uuid([0; 16]),
        MessageValue::Timestamp(0),
        MessageValue::Char('a'),
        MessageValue::Symbol("a".into()),
        MessageValue::Binary(vec![]),
        MessageValue::List(vec![]),
        MessageValue::Map(vec![]),
        MessageValue::Array(vec![]),
        MessageValue::Described {
            descriptor: MessageDescriptor::Code(1),
            value: Box::new(MessageValue::Null),
        },
    ] {
        let original = envelope([("a", value)]);
        let (result, error) = run("SET a='x'", &original)?;
        assert_eq!(error, Some(SqlActionError::UnsupportedTargetType));
        assert_eq!(result, original);
    }
    Ok(())
}

#[test]
fn literal_sequential_overrides_and_late_failure_are_atomic() -> Result<(), BrokerError> {
    let original = envelope([
        ("a", MessageValue::Byte(1)),
        ("kept", MessageValue::Bool(true)),
    ]);
    let (result, error) = run(
        "SET a=2;SET a=3;REMOVE kept;SET kept='now absent'",
        &original,
    )?;
    assert_eq!(error, None);
    assert_eq!(result.application_properties["a"], MessageValue::Byte(3));
    assert_eq!(
        result.application_properties["kept"],
        MessageValue::String("now absent".into())
    );
    for source in [
        "SET a=2;SET a=128",
        "REMOVE kept;SET a='bad'",
        "REMOVE a;SET a=TRUE;SET a=7",
    ] {
        let (result, error) = run(source, &original)?;
        assert!(error.is_some());
        assert_eq!(result, original);
    }
    Ok(())
}

#[test]
fn literal_case_exact_keys_and_static_errors_do_not_echo_values() -> Result<(), BrokerError> {
    let original = envelope([
        ("secret", MessageValue::Bool(true)),
        ("Secret", MessageValue::String("kept".into())),
    ]);
    let (result, error) = run("SET Secret='changed'", &original)?;
    assert_eq!(error, None);
    assert_eq!(
        result.application_properties["secret"],
        MessageValue::Bool(true)
    );
    let (_, error) = run("SET secret='private-value'", &original)?;
    let error = error.expect("incompatible family");
    assert_eq!(format!("{error:?}"), "TypeMismatch");
    assert_eq!(error.description(), "TypeMismatch");
    Ok(())
}
