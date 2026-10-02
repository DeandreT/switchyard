use std::collections::BTreeMap;

use super::*;
use crate::{
    AnnotationKey, MessageBody, MessageHeader, MessageProperties, MessageValue, SqlCompileUsage,
    SqlProgram,
};

#[test]
fn regular_unicode_and_quoted_targets_preserve_exact_spelling() {
    let program = SqlActionProgram::compile(
        "remove Secret; REMOVE USER.colour; REMOVE [name.with spaces]; \
         REMOVE user.[right]]bracket]; REMOVE \"quote\"\"name\"; REMOVE \u{0394}\u{03b5}\u{03af}\u{03b3}\u{03bc}\u{03b1}",
    )
    .expect("bounded static REMOVE targets");
    assert_eq!(
        program.targets(),
        [
            "Secret",
            "colour",
            "name.with spaces",
            "right]bracket",
            "quote\"name",
            "\u{0394}\u{03b5}\u{03af}\u{03b3}\u{03bc}\u{03b1}",
        ]
    );
}

#[test]
fn semicolon_separation_and_optional_final_semicolon_are_explicit() {
    for source in ["REMOVE a", "REMOVE a;", "REMOVE a; REMOVE b;"] {
        assert!(SqlActionProgram::compile(source).is_ok(), "{source}");
    }
    for source in [
        "",
        "  ",
        "; REMOVE a",
        "REMOVE",
        "REMOVE a;;",
        "REMOVE a REMOVE b",
        "REMOVE a, b",
        "REMOVE [unterminated",
        "REMOVE \"unterminated",
        "REMOVE user.",
    ] {
        assert!(
            matches!(
                SqlActionProgram::compile(source),
                Err(SqlCompileError::Syntax)
            ),
            "{source}"
        );
    }
}

#[test]
fn set_system_scope_and_dynamic_or_computed_targets_are_unsupported() {
    for source in [
        "SET a = 1",
        "REMOVE sys.CorrelationId",
        "REMOVE SYS.[ReplyTo]",
        "REMOVE foreign.a",
        "REMOVE user.a.b",
        "REMOVE property('a')",
        "REMOVE p('a')",
        "REMOVE newid()",
        "REMOVE a + b",
        "REMOVE (a)",
        "REMOVE 'a'",
        "REMOVE 1",
        "REMOVE TRUE",
    ] {
        assert!(
            matches!(
                SqlActionProgram::compile(source),
                Err(SqlCompileError::Unsupported { .. })
            ),
            "{source}"
        );
    }
}

#[test]
fn empty_or_control_character_property_names_are_unsupported() {
    for source in ["REMOVE []", "REMOVE \"\"", "REMOVE [bad\nname]"] {
        assert!(
            matches!(
                SqlActionProgram::compile(source),
                Err(SqlCompileError::Unsupported {
                    feature: "SQL action property name"
                })
            ),
            "{source}"
        );
    }
}

#[test]
fn quoted_scope_and_reserved_property_names_follow_identifier_grammar() {
    let program = SqlActionProgram::compile("REMOVE [user].[SELECT]; REMOVE \"USER\".\"TRUE\"")
        .expect("quoted static identifiers");
    assert_eq!(program.targets(), ["SELECT", "TRUE"]);
}

#[test]
fn removal_is_case_exact_and_missing_or_repeated_targets_are_harmless() {
    let program = SqlActionProgram::compile("REMOVE Secret; REMOVE missing; REMOVE Secret")
        .expect("repeated REMOVE");
    assert_eq!(program.targets(), ["Secret", "missing", "Secret"]);
    let mut envelope = MessageEnvelope {
        application_properties: BTreeMap::from([
            (String::from("Secret"), MessageValue::Null),
            (
                String::from("secret"),
                MessageValue::String(String::from("keep")),
            ),
        ]),
        ..MessageEnvelope::default()
    };
    program.apply(&mut envelope);
    assert_eq!(envelope.application_properties.len(), 1);
    assert_eq!(
        envelope.application_properties.get("secret"),
        Some(&MessageValue::String(String::from("keep")))
    );
    let first = envelope.clone();
    program.apply(&mut envelope);
    assert_eq!(envelope, first);
}

#[test]
fn removal_does_not_convert_values_or_modify_other_message_sections() {
    let mut envelope = MessageEnvelope {
        header: Some(MessageHeader {
            durable: true,
            ..MessageHeader::default()
        }),
        properties: MessageProperties {
            subject: Some(String::from("original")),
            content_type: Some(String::from("text/plain")),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([
            (
                String::from("compound"),
                MessageValue::List(vec![MessageValue::Long(7)]),
            ),
            (
                String::from("bits"),
                MessageValue::Double(0x8000_0000_0000_0000),
            ),
        ]),
        message_annotations: BTreeMap::from([(
            AnnotationKey::Symbol(String::from("original")),
            MessageValue::Bool(true),
        )]),
        footer: BTreeMap::from([(
            AnnotationKey::Symbol(String::from("footer")),
            MessageValue::Uint(3),
        )]),
        body: MessageBody::Data(vec![b"payload".to_vec()]),
    };
    let mut expected = envelope.clone();
    expected.application_properties.remove("compound");
    SqlActionProgram::compile("REMOVE compound")
        .expect("static target")
        .apply(&mut envelope);
    assert_eq!(envelope, expected);
}

#[test]
fn statement_limit_counts_repeated_targets() {
    let at_limit = "REMOVE[a];".repeat(MAX_SQL_ACTION_STATEMENTS);
    let program = SqlActionProgram::compile(&at_limit).expect("32 statements");
    assert_eq!(program.targets().len(), MAX_SQL_ACTION_STATEMENTS);
    assert!(matches!(
        SqlActionProgram::compile(&"REMOVE[a];".repeat(MAX_SQL_ACTION_STATEMENTS + 1)),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::Nodes,
            maximum: MAX_SQL_ACTION_STATEMENTS
        })
    ));
}

#[test]
fn whitespace_and_comments_consume_physical_tokens() {
    assert!(SqlActionProgram::compile(&format!("{}REMOVE a", " ".repeat(125))).is_ok());
    assert!(matches!(
        SqlActionProgram::compile(&format!("{}REMOVE a", " ".repeat(126))),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::PhysicalTokens,
            maximum: crate::MAX_SQL_EXPRESSION_TOKENS
        })
    ));
    assert!(SqlActionProgram::compile(&format!("{}REMOVE[a]", "/*x*/".repeat(126))).is_ok());
    assert!(matches!(
        SqlActionProgram::compile(&format!("{}REMOVE[a]", "/*x*/".repeat(127))),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::PhysicalTokens,
            maximum: crate::MAX_SQL_EXPRESSION_TOKENS
        })
    ));
}

#[test]
fn source_limits_precede_tokenization_and_count_utf16() {
    let exact = format!("REMOVE[{}]", "\u{1f600}".repeat(508));
    assert_eq!(exact.encode_utf16().count(), MAX_SQL_EXPRESSION_UTF16_UNITS);
    assert!(SqlAction::new(exact).is_ok());
    assert!(matches!(
        SqlAction::new(format!("REMOVE[{}]", "\u{1f600}".repeat(509))),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::SourceUtf16Units,
            maximum: MAX_SQL_EXPRESSION_UTF16_UNITS
        })
    ));
    assert!(matches!(
        SqlActionProgram::compile(&"x".repeat(MAX_SQL_EXPRESSION_BYTES + 1)),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::SourceBytes,
            maximum: MAX_SQL_EXPRESSION_BYTES
        })
    ));
}

#[test]
fn nested_unsupported_targets_still_obey_the_parser_depth_ceiling() {
    let source = format!("REMOVE {}a{}", "(".repeat(40), ")".repeat(40));
    assert!(matches!(
        SqlActionProgram::compile(&source),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::ParserDepth,
            maximum: crate::MAX_SQL_PARSER_DEPTH
        })
    ));
}

#[test]
fn aggregate_budget_is_shared_with_predicates_and_charges_each_statement() {
    let mut budget = SqlCompileBudget::with_limits(12, 4, 2);
    SqlProgram::compile_with_budget("TRUE", &mut budget).expect("predicate allowance");
    SqlActionProgram::compile_with_budget("REMOVE a", &mut budget).expect("action allowance");
    assert_eq!(
        budget.used(),
        SqlCompileUsage {
            source_bytes: 12,
            tokens: 4,
            nodes: 2
        }
    );
    let used = budget.used();
    assert!(matches!(
        SqlActionProgram::compile_with_budget("REMOVE b", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            maximum: 12
        })
    ));
    assert_eq!(budget.used(), used);

    let mut budget = SqlCompileBudget::with_limits(100, 100, 2);
    SqlActionProgram::compile_with_budget("REMOVE a;REMOVE a", &mut budget)
        .expect("each repeated statement charged once");
    assert_eq!(budget.used().nodes, 2);
    assert!(matches!(
        SqlActionProgram::compile_with_budget("REMOVE a", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 2
        })
    ));
    assert_eq!(budget.used().nodes, 2);
}

#[test]
fn failed_compilation_keeps_prior_aggregate_charges_without_exceeding_caps() {
    let mut budget = SqlCompileBudget::with_limits(100, 2, 3);
    assert!(matches!(
        SqlActionProgram::compile_with_budget("REMOVE a", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateTokens,
            maximum: 2
        })
    ));
    assert_eq!(
        budget.used(),
        SqlCompileUsage {
            source_bytes: 8,
            tokens: 0,
            nodes: 0
        }
    );

    let mut budget = SqlCompileBudget::with_limits(100, 100, 0);
    assert!(matches!(
        SqlActionProgram::compile_with_budget("REMOVE a", &mut budget),
        Err(SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 0
        })
    ));
    assert_eq!(
        budget.used(),
        SqlCompileUsage {
            source_bytes: 8,
            tokens: 3,
            nodes: 0
        }
    );
}

#[test]
fn serialization_stores_only_version_and_exact_source() -> Result<(), crate::CodecError> {
    let source = "  REMOVE Secret; /* original */ ";
    let action = SqlAction::new(source).expect("bounded action");
    assert_eq!(action.expression(), source);
    assert_eq!(action.semantic_version(), SQL_ACTION_SEMANTIC_VERSION);
    let expected = crate::codec::encode(&(1_u32, source))?;
    assert_eq!(crate::codec::encode(&action)?, expected);
    assert_eq!(crate::codec::decode::<SqlAction>(&expected)?, action);
    Ok(())
}

#[test]
fn deserialize_validates_source_and_version_without_compiling() -> Result<(), crate::CodecError> {
    for source in ["REMOVE", "SET a = 1", ""] {
        let encoded = crate::codec::encode(&(1_u32, source))?;
        let action = crate::codec::decode::<SqlAction>(&encoded)?;
        assert_eq!(action.expression(), source);
        assert!(action.validate_source().is_ok());
        assert!(SqlActionProgram::compile(source).is_err());
    }
    for (version, source) in [
        (0_u32, String::from("REMOVE a")),
        (2_u32, String::from("REMOVE a")),
        (1_u32, "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS + 1)),
        (1_u32, "x".repeat(MAX_SQL_EXPRESSION_BYTES + 1)),
    ] {
        assert_eq!(
            crate::codec::decode::<SqlAction>(&crate::codec::encode(&(version, source))?),
            Err(crate::CodecError::Decode)
        );
    }
    Ok(())
}
