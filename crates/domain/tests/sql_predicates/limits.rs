use super::*;

fn compile_limit(expression: &str, kind: SqlCompileLimit, maximum: usize) {
    assert_eq!(
        SqlProgram::compile(expression).expect_err("compile limit"),
        SqlCompileError::Limit { kind, maximum },
        "{expression:?}"
    );
}

#[test]
fn source_limits_count_utf16_and_bytes_before_tokenization() {
    let exact = format!("'{}'", "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 2));
    let program = SqlProgram::compile(&exact).expect("exact UTF-16 limit");
    assert_eq!(
        program.metrics().source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    assert_eq!(program.metrics().source_bytes, exact.len());
    compile_limit(
        &format!("'{}'", "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 1)),
        SqlCompileLimit::SourceUtf16Units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
    );
    let astral = format!(
        "'{}'",
        "\u{1f642}".repeat((MAX_SQL_EXPRESSION_UTF16_UNITS - 2) / 2)
    );
    let program = SqlProgram::compile(&astral).expect("exact UTF-16 astral limit");
    assert_eq!(
        program.metrics().source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    assert_eq!(program.metrics().source_bytes, astral.len());
    compile_limit(
        &format!(
            "'{}'",
            "\u{1f642}".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS / 2)
        ),
        SqlCompileLimit::SourceUtf16Units,
        MAX_SQL_EXPRESSION_UTF16_UNITS,
    );
    let oversized_bytes = format!("'{}'", "\u{754c}".repeat(MAX_SQL_EXPRESSION_BYTES / 3 + 1));
    assert!(oversized_bytes.len() > MAX_SQL_EXPRESSION_BYTES);
    compile_limit(
        &oversized_bytes,
        SqlCompileLimit::SourceBytes,
        MAX_SQL_EXPRESSION_BYTES,
    );
}

#[test]
fn spaces_and_comments_use_the_physical_token_allowance() {
    for separator in [" ", "/*x*/"] {
        let exact = format!("TRUE{}", separator.repeat(MAX_SQL_EXPRESSION_TOKENS - 1));
        let program = SqlProgram::compile(&exact).expect("exact physical token limit");
        assert_eq!(program.metrics().tokens, MAX_SQL_EXPRESSION_TOKENS);
        assert_eq!(program.metrics().nodes, 1);
        compile_limit(
            &format!("TRUE{}", separator.repeat(MAX_SQL_EXPRESSION_TOKENS)),
            SqlCompileLimit::PhysicalTokens,
            MAX_SQL_EXPRESSION_TOKENS,
        );
    }
}

#[test]
fn flat_depth_nested_parser_depth_and_in_lists_fail_before_unbounded_trees() {
    let sum = |count: usize| {
        std::iter::repeat_n("1", count)
            .collect::<Vec<_>>()
            .join("+")
    };
    let exact = format!(
        "{}={}",
        sum(MAX_SQL_EXPRESSION_DEPTH - 1),
        MAX_SQL_EXPRESSION_DEPTH - 1
    );
    let program = SqlProgram::compile(&exact).expect("exact native depth");
    assert_eq!(program.metrics().depth, MAX_SQL_EXPRESSION_DEPTH);
    assert_eq!(
        program.metrics().nodes,
        2 * (MAX_SQL_EXPRESSION_DEPTH - 1) + 1
    );
    assert!(program.metrics().nodes <= MAX_SQL_EXPRESSION_NODES);
    compile_limit(
        &format!(
            "{}={}",
            sum(MAX_SQL_EXPRESSION_DEPTH),
            MAX_SQL_EXPRESSION_DEPTH
        ),
        SqlCompileLimit::ExpressionDepth,
        MAX_SQL_EXPRESSION_DEPTH,
    );
    let nested = format!(
        "{}TRUE{}",
        "(".repeat(MAX_SQL_PARSER_DEPTH + 1),
        ")".repeat(MAX_SQL_PARSER_DEPTH + 1)
    );
    compile_limit(&nested, SqlCompileLimit::ParserDepth, MAX_SQL_PARSER_DEPTH);
    let list = |count: usize| {
        (0..count)
            .map(|number| number.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    let exact = format!("0 IN ({})", list(MAX_SQL_IN_ITEMS));
    let program = SqlProgram::compile(&exact).expect("exact IN limit");
    assert_eq!(program.metrics().nodes, MAX_SQL_IN_ITEMS + 2);
    assert_eq!(
        program.evaluate(
            context(&MessageEnvelope::default()),
            &mut SqlEvaluationBudget::default()
        ),
        Ok(SqlTruth::True)
    );
    compile_limit(
        &format!("0 IN ({})", list(MAX_SQL_IN_ITEMS + 1)),
        SqlCompileLimit::InItems,
        MAX_SQL_IN_ITEMS,
    );
    for expression in [
        format!("{}+", sum(MAX_SQL_EXPRESSION_DEPTH - 1)),
        format!("{}TRUE", "(".repeat(MAX_SQL_PARSER_DEPTH - 1)),
    ] {
        assert!(
            SqlProgram::compile(&expression).is_err(),
            "bounded failed tree: {expression}"
        );
    }
    assert_truth("1=1", &MessageEnvelope::default(), SqlTruth::True);
}

#[test]
fn aggregate_compile_usage_is_shared_and_failed_stages_remain_charged() {
    let expression = "1=1";
    let metrics = SqlProgram::compile(expression)
        .expect("small program")
        .metrics();
    assert_eq!(
        (metrics.source_bytes, metrics.tokens, metrics.nodes),
        (3, 3, 3)
    );
    let mut exact = SqlCompileBudget::with_limits(6, 6, 6);
    for _ in 0..2 {
        SqlProgram::compile_with_budget(expression, &mut exact).expect("shared exact capacity");
    }
    assert_eq!(
        exact.used(),
        SqlCompileUsage {
            source_bytes: 6,
            tokens: 6,
            nodes: 6
        }
    );
    assert_eq!(
        SqlProgram::compile_with_budget(expression, &mut exact).expect_err("source capacity"),
        SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateSourceBytes,
            maximum: 6
        }
    );
    assert_eq!(
        exact.used(),
        SqlCompileUsage {
            source_bytes: 6,
            tokens: 6,
            nodes: 6
        }
    );
    let mut tokens = SqlCompileBudget::with_limits(100, 3, 100);
    SqlProgram::compile_with_budget(expression, &mut tokens).expect("first tokens");
    assert_eq!(
        SqlProgram::compile_with_budget(expression, &mut tokens).expect_err("token capacity"),
        SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateTokens,
            maximum: 3
        }
    );
    assert_eq!(
        tokens.used(),
        SqlCompileUsage {
            source_bytes: 6,
            tokens: 3,
            nodes: 3
        }
    );
    let mut nodes = SqlCompileBudget::with_limits(100, 100, 2);
    assert_eq!(
        SqlProgram::compile_with_budget(expression, &mut nodes).expect_err("node capacity"),
        SqlCompileError::Limit {
            kind: SqlCompileLimit::AggregateNodes,
            maximum: 2
        }
    );
    assert_eq!(
        nodes.used(),
        SqlCompileUsage {
            source_bytes: 3,
            tokens: 3,
            nodes: 2
        }
    );
    let mut invalid = SqlCompileBudget::default();
    assert_eq!(
        SqlProgram::compile_with_budget("TRUE junk", &mut invalid).expect_err("trailing syntax"),
        SqlCompileError::Syntax
    );
    assert_eq!(
        invalid.used(),
        SqlCompileUsage {
            source_bytes: 9,
            tokens: 3,
            nodes: 0
        }
    );
}

#[test]
fn default_aggregate_source_and_token_caps_do_not_reset_between_programs() {
    let expression = format!("'{}'", "x".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 2));
    for mut source in [
        SqlCompileBudget::default(),
        SqlCompileBudget::with_limits(usize::MAX, usize::MAX, usize::MAX),
    ] {
        for _ in 0..MAX_SQL_COMPILE_SOURCE_BYTES / expression.len() {
            SqlProgram::compile_with_budget(&expression, &mut source).expect("source allowance");
        }
        assert_eq!(source.used().source_bytes, MAX_SQL_COMPILE_SOURCE_BYTES);
        assert_eq!(
            SqlProgram::compile_with_budget(&expression, &mut source)
                .expect_err("aggregate source cap"),
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateSourceBytes,
                maximum: MAX_SQL_COMPILE_SOURCE_BYTES
            }
        );
        assert!(source.used().nodes < MAX_SQL_COMPILE_NODES);
    }
    let expression = format!("TRUE{}", " ".repeat(MAX_SQL_EXPRESSION_TOKENS - 1));
    for mut tokens in [
        SqlCompileBudget::default(),
        SqlCompileBudget::with_limits(usize::MAX, usize::MAX, usize::MAX),
    ] {
        for _ in 0..MAX_SQL_COMPILE_TOKENS / MAX_SQL_EXPRESSION_TOKENS {
            SqlProgram::compile_with_budget(&expression, &mut tokens).expect("token allowance");
        }
        assert_eq!(tokens.used().tokens, MAX_SQL_COMPILE_TOKENS);
        assert_eq!(
            SqlProgram::compile_with_budget(&expression, &mut tokens)
                .expect_err("aggregate token cap"),
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateTokens,
                maximum: MAX_SQL_COMPILE_TOKENS
            }
        );
        assert!(tokens.used().nodes < MAX_SQL_COMPILE_NODES);
    }
}

#[test]
fn evaluation_usage_is_shared_and_exact_allowances_do_not_reset() {
    let envelope = envelope([
        ("value", MessageValue::String("target".into())),
        ("other", MessageValue::String("unselected".into())),
    ]);
    let program = SqlProgram::compile("TRUE OR value = 'target'").expect("program");
    let mut measured = SqlEvaluationBudget::default();
    assert_eq!(
        program.evaluate(context(&envelope), &mut measured),
        Ok(SqlTruth::True)
    );
    let usage = measured.used();
    assert_eq!(
        usage,
        SqlEvaluationUsage {
            work: 71,
            comparison_bytes: 102
        }
    );
    let mut shared = SqlEvaluationBudget::with_limits(2 * usage.work, 2 * usage.comparison_bytes);
    for _ in 0..2 {
        assert_eq!(
            program.evaluate(context(&envelope), &mut shared),
            Ok(SqlTruth::True)
        );
    }
    assert_eq!(
        shared.used(),
        SqlEvaluationUsage {
            work: 2 * usage.work,
            comparison_bytes: 2 * usage.comparison_bytes
        }
    );
    assert_eq!(
        program.evaluate(context(&envelope), &mut shared),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 2 * usage.work
        })
    );
    assert_eq!(
        shared.used(),
        SqlEvaluationUsage {
            work: 2 * usage.work,
            comparison_bytes: 2 * usage.comparison_bytes
        }
    );
    let mut exact = SqlEvaluationBudget::with_limits(usage.work, usage.comparison_bytes);
    assert_eq!(
        program.evaluate(context(&envelope), &mut exact),
        Ok(SqlTruth::True)
    );
    let mut short = SqlEvaluationBudget::with_limits(usage.work, usage.comparison_bytes - 1);
    assert_eq!(
        program.evaluate(context(&envelope), &mut short),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: usage.comparison_bytes - 1
        })
    );
}

#[test]
fn dynamic_like_bounds_and_product_work_are_charged_before_pattern_allocation() {
    let program = SqlProgram::compile("input LIKE pattern").expect("dynamic pattern");
    let small = envelope([
        ("input", MessageValue::String("xxxx".into())),
        ("pattern", MessageValue::String("x%".into())),
    ]);
    let mut budget = SqlEvaluationBudget::default();
    assert_eq!(
        program.evaluate(context(&small), &mut budget),
        Ok(SqlTruth::True)
    );
    assert_eq!(
        budget.used(),
        SqlEvaluationUsage {
            work: 201,
            comparison_bytes: 226
        }
    );
    let mut short = SqlEvaluationBudget::with_limits(200, 226);
    assert_eq!(
        program.evaluate(context(&small), &mut short),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: 200
        })
    );
    let exact = envelope([
        ("input", MessageValue::String(String::new())),
        (
            "pattern",
            MessageValue::String("x".repeat(MAX_SQL_LIKE_PATTERN_BYTES - 4)),
        ),
    ]);
    assert_eq!(
        program.evaluate(context(&exact), &mut SqlEvaluationBudget::default()),
        Ok(SqlTruth::False)
    );
    let oversized = envelope([
        ("input", MessageValue::String(String::new())),
        (
            "pattern",
            MessageValue::String("x".repeat(MAX_SQL_LIKE_PATTERN_BYTES - 3)),
        ),
    ]);
    let no_shortcut =
        SqlProgram::compile("FALSE AND input LIKE pattern").expect("unselected pattern");
    let mut bytes = SqlEvaluationBudget::with_limits(usize::MAX, 0);
    assert_eq!(
        no_shortcut.evaluate(context(&oversized), &mut bytes),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 0
        })
    );
    assert_eq!(
        no_shortcut.evaluate(context(&oversized), &mut SqlEvaluationBudget::default()),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::LikePatternBytes,
            maximum: MAX_SQL_LIKE_PATTERN_BYTES
        })
    );
}

#[test]
fn oversized_allowances_cannot_raise_project_evaluation_ceilings() {
    let program = SqlProgram::compile("value IS NULL").expect("lookup");
    let giant_key = MessageEnvelope {
        application_properties: [("x".repeat(MAX_TOPIC_RULE_MATCH_WORK), MessageValue::Null)]
            .into(),
        ..MessageEnvelope::default()
    };
    let mut raised = SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX);
    assert_eq!(
        program.evaluate(context(&giant_key), &mut raised),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::WorkUnits,
            maximum: MAX_TOPIC_RULE_MATCH_WORK
        })
    );
    let program = SqlProgram::compile("sys.MessageId = 'x'").expect("system string");
    let giant_id = "x".repeat(MAX_TOPIC_RULE_COMPARISON_BYTES + 1);
    let mut raised = SqlEvaluationBudget::with_limits(usize::MAX, usize::MAX);
    assert_eq!(
        program.evaluate(
            SqlMessageContext {
                message_id: &giant_id,
                session_id: None,
                envelope: None
            },
            &mut raised
        ),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: MAX_TOPIC_RULE_COMPARISON_BYTES
        })
    );
}

#[test]
fn precharge_precedes_lookup_type_checks_and_boolean_shortcuts() {
    let envelope = envelope([
        ("color", MessageValue::String("target".into())),
        ("COLOR", MessageValue::String("target".into())),
    ]);
    for expression in ["TRUE OR color = 'target'", "FALSE AND color = 'target'"] {
        let program = SqlProgram::compile(expression).expect("program");
        let mut work = SqlEvaluationBudget::with_limits(0, usize::MAX);
        assert_eq!(
            program.evaluate(context(&envelope), &mut work),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::WorkUnits,
                maximum: 0
            })
        );
        assert_eq!(work.used(), SqlEvaluationUsage::default());
        let mut bytes = SqlEvaluationBudget::with_limits(usize::MAX, 0);
        assert_eq!(
            program.evaluate(context(&envelope), &mut bytes),
            Err(SqlEvaluationError::Limit {
                kind: SqlEvaluationLimit::ComparisonBytes,
                maximum: 0
            })
        );
        assert!(bytes.used().work > 0);
        assert_eq!(bytes.used().comparison_bytes, 0);
        assert_eq!(
            program.evaluate(context(&envelope), &mut SqlEvaluationBudget::default()),
            Err(SqlEvaluationError::AmbiguousProperty)
        );
    }
    let program = SqlProgram::compile("TRUE OR value = '7'").expect("type mismatch");
    let unsupported = super::envelope([("value", MessageValue::List(vec![]))]);
    let mut bytes = SqlEvaluationBudget::with_limits(usize::MAX, 0);
    assert_eq!(
        program.evaluate(context(&unsupported), &mut bytes),
        Err(SqlEvaluationError::Limit {
            kind: SqlEvaluationLimit::ComparisonBytes,
            maximum: 0
        })
    );
    assert_eq!(
        program.evaluate(context(&unsupported), &mut SqlEvaluationBudget::default()),
        Err(SqlEvaluationError::UnsupportedValue)
    );
}
