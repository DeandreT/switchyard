use super::*;

#[test]
fn library_parser_preserves_precedence_and_postorder_edges() -> Result<(), SqlCompileError> {
    let program = SqlProgram::compile("quantity+2*3>=8 AND NOT archived")?;
    assert!(matches!(
        program.nodes[program.root as usize],
        SqlNode::Binary {
            op: SqlBinaryOp::And,
            ..
        }
    ));
    assert!(program.nodes.iter().any(|node| matches!(
        node,
        SqlNode::Binary {
            op: SqlBinaryOp::Multiply,
            ..
        }
    )));
    assert!(program.nodes.iter().any(|node| matches!(
        node,
        SqlNode::Unary {
            op: SqlUnaryOp::Not,
            ..
        }
    )));
    assert_prior_children(&program);
    assert_eq!(program.root as usize + 1, program.nodes.len());
    assert_eq!(program.metrics.nodes, program.nodes.len());
    Ok(())
}

#[test]
fn exists_adapter_handles_both_prefixes_without_subqueries() -> Result<(), SqlCompileError> {
    for expression in [
        "EXISTS(user.region)",
        "NOT EXISTS(region)",
        "NOT (EXISTS(p('a.b')))",
    ] {
        let program = SqlProgram::compile(expression)?;
        assert!(
            program
                .nodes
                .iter()
                .any(|node| matches!(node, SqlNode::Exists(SqlProperty::User(_))))
        );
        assert_prior_children(&program);
    }
    for expression in ["EXISTS(1+2)", "NOT EXISTS('region')", "EXISTS(SELECT 1)"] {
        assert!(SqlProgram::compile(expression).is_err(), "{expression}");
    }
    Ok(())
}

#[test]
fn delimited_unicode_and_literal_property_names_keep_their_spelling() -> Result<(), SqlCompileError>
{
    for (expression, expected) in [
        ("[a]]b]", "a]b"),
        ("\"a\"\"b\"", "a\"b"),
        ("[Property With Space]", "Property With Space"),
        ("[\u{00c4}pfel]", "\u{00c4}pfel"),
        ("\u{00c4}pfel", "\u{00c4}pfel"),
        ("p('sys.MessageId')", "sys.MessageId"),
        ("property('O''Brien')", "O'Brien"),
        ("[_name]", "_name"),
    ] {
        let program = SqlProgram::compile(expression)?;
        let SqlNode::Property(SqlProperty::User(name)) = &program.nodes[program.root as usize]
        else {
            panic!("not a user property: {expression}");
        };
        assert_eq!(name, expected);
    }
    Ok(())
}

#[test]
fn known_system_names_are_explicit_and_scopes_never_fall_back_to_user()
-> Result<(), SqlCompileError> {
    for name in [
        "CorrelationId",
        "MessageId",
        "To",
        "ReplyTo",
        "Label",
        "Subject",
        "SessionId",
        "ReplyToSessionId",
        "ContentType",
    ] {
        let program = SqlProgram::compile(&format!("SyS.{name}"))?;
        assert!(matches!(
            program.nodes[program.root as usize],
            SqlNode::Property(SqlProperty::System(_))
        ));
    }
    for expression in ["sys.NoSuchProperty", "other.MessageId", "user.nested.key"] {
        assert!(
            matches!(
                SqlProgram::compile(expression),
                Err(SqlCompileError::Unsupported { .. })
            ),
            "{expression}"
        );
    }
    Ok(())
}

#[test]
fn numeric_literals_retain_their_origin_and_accept_signed_minimum() -> Result<(), SqlCompileError> {
    for (expression, expected) in [
        ("0", 0),
        ("9223372036854775807", i64::MAX),
        ("-9223372036854775808", i64::MIN),
        ("-(9223372036854775808)", i64::MIN),
        ("-12", -12),
    ] {
        let program = SqlProgram::compile(expression)?;
        assert!(
            matches!(program.nodes[program.root as usize], SqlNode::Literal(SqlLiteral::Int64(value)) if value == expected)
        );
    }
    for expression in ["1.0", "1e0", ".5", "-0.0"] {
        let program = SqlProgram::compile(expression)?;
        assert!(
            program
                .nodes
                .iter()
                .any(|node| matches!(node, SqlNode::Literal(SqlLiteral::DoubleBits(_))))
        );
    }
    for expression in ["9223372036854775808", "-9223372036854775809", "1e309"] {
        assert!(
            matches!(
                SqlProgram::compile(expression),
                Err(SqlCompileError::Unsupported { .. })
            ),
            "{expression}"
        );
    }
    Ok(())
}

#[test]
fn supported_predicates_include_expression_patterns_and_nested_in_slices()
-> Result<(), SqlCompileError> {
    for expression in [
        "color NOT IN ('red','blue')",
        "missing IS NULL OR present IS NOT NULL",
        "text NOT LIKE pattern ESCAPE escape_key",
        "(quantity%3)+1<>2",
        "x IN (1,(y IN (2,3)),4)",
        "TRUE AND FALSE",
    ] {
        assert_prior_children(&SqlProgram::compile(expression)?);
    }
    let program = SqlProgram::compile("x IN (1,(y IN (2,3)),4)")?;
    let SqlNode::In { start, len, .. } = program.nodes[program.root as usize] else {
        panic!("not IN");
    };
    assert_eq!(len, 3);
    assert!(matches!(
        program.nodes[program.in_operands[start + 1] as usize],
        SqlNode::In { len: 2, .. }
    ));
    Ok(())
}

#[test]
fn unsupported_extensions_and_dynamic_functions_are_not_silently_accepted() {
    for expression in [
        "newid()",
        "lower(name)",
        "property(name)",
        "p('a'+'b')",
        "p(DISTINCT 'a')",
        "p('a','b')",
        "p(*)",
        "p('a') OVER ()",
        "other.p('a')",
        "CAST(x AS INT)",
        "x IN (SELECT y FROM table_name)",
        "(SELECT 1)",
        "x BETWEEN 1 AND 2",
        "x ILIKE 'a'",
        "x || 'b'",
        "CASE WHEN TRUE THEN 1 ELSE 0 END",
        "?",
        "N'abc'",
    ] {
        assert!(SqlProgram::compile(expression).is_err(), "{expression}");
    }
    for expression in ["1=1;", "1=1;1=0", "1=1 trailing", "", "-- comment only"] {
        assert!(
            matches!(
                SqlProgram::compile(expression),
                Err(SqlCompileError::Syntax)
            ),
            "{expression}"
        );
    }
    assert!(SqlProgram::compile("_name").is_err());
}

#[test]
fn source_limits_precede_tokenization_and_do_not_spend_shared_budget() -> Result<(), SqlCompileError>
{
    let exact = format!("'{}'", "a".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 2));
    assert_eq!(
        SqlProgram::compile(&exact)?.metrics.source_utf16_units,
        MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    let mut budget = SqlCompileBudget::default();
    assert_limit(
        SqlProgram::compile_with_budget(&"a".repeat(MAX_SQL_EXPRESSION_BYTES + 1), &mut budget),
        SqlCompileLimit::SourceBytes,
    );
    assert_limit(
        SqlProgram::compile_with_budget(
            &"a".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            &mut budget,
        ),
        SqlCompileLimit::SourceUtf16Units,
    );
    assert_eq!(budget.used(), super::super::SqlCompileUsage::default());
    let emoji = format!(
        "'{}'",
        "\u{1f600}".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS / 2)
    );
    assert_limit(
        SqlProgram::compile(&emoji),
        SqlCompileLimit::SourceUtf16Units,
    );
    Ok(())
}

#[test]
fn physical_whitespace_and_comments_count_toward_the_preparse_cap() -> Result<(), SqlCompileError> {
    let exact = format!("{}TRUE", " ".repeat(MAX_SQL_EXPRESSION_TOKENS - 1));
    assert_eq!(
        SqlProgram::compile(&exact)?.metrics.tokens,
        MAX_SQL_EXPRESSION_TOKENS
    );
    assert_limit(
        SqlProgram::compile(&format!(" {exact}")),
        SqlCompileLimit::PhysicalTokens,
    );
    let comments = format!("{}TRUE", "/*x*/".repeat(MAX_SQL_EXPRESSION_TOKENS));
    assert_limit(
        SqlProgram::compile(&comments),
        SqlCompileLimit::PhysicalTokens,
    );
    Ok(())
}

#[test]
fn parser_recursion_and_native_left_associative_depth_are_independent() {
    let nested = format!(
        "{}TRUE{}",
        "(".repeat(MAX_SQL_PARSER_DEPTH + 1),
        ")".repeat(MAX_SQL_PARSER_DEPTH + 1)
    );
    assert_limit(SqlProgram::compile(&nested), SqlCompileLimit::ParserDepth);
    let chain = std::iter::repeat_n("x", MAX_SQL_EXPRESSION_DEPTH + 1)
        .collect::<Vec<_>>()
        .join("+");
    assert_limit(
        SqlProgram::compile(&chain),
        SqlCompileLimit::ExpressionDepth,
    );
}

#[test]
fn in_lists_have_an_independent_item_cap() -> Result<(), SqlCompileError> {
    let items = std::iter::repeat_n("1", MAX_SQL_IN_ITEMS)
        .collect::<Vec<_>>()
        .join(",");
    assert_prior_children(&SqlProgram::compile(&format!("x IN ({items})"))?);
    assert_limit(
        SqlProgram::compile(&format!("x IN ({items},1)")),
        SqlCompileLimit::InItems,
    );
    Ok(())
}

#[test]
fn aggregate_source_tokens_and_nodes_fail_before_the_protected_stage() {
    let mut source = SqlCompileBudget::with_limits(3, usize::MAX, usize::MAX);
    assert_limit(
        SqlProgram::compile_with_budget("TRUE", &mut source),
        SqlCompileLimit::AggregateSourceBytes,
    );
    assert_eq!(source.used().source_bytes, 0);
    let mut tokens = SqlCompileBudget::with_limits(usize::MAX, 0, usize::MAX);
    assert_limit(
        SqlProgram::compile_with_budget("TRUE", &mut tokens),
        SqlCompileLimit::AggregateTokens,
    );
    assert_eq!(tokens.used().source_bytes, 4);
    assert_eq!(tokens.used().tokens, 0);
    assert_eq!(tokens.used().nodes, 0);
    let mut nodes = SqlCompileBudget::with_limits(usize::MAX, usize::MAX, 1);
    assert_limit(
        SqlProgram::compile_with_budget("x=1", &mut nodes),
        SqlCompileLimit::AggregateNodes,
    );
    assert_eq!(nodes.used().nodes, 1);
    assert!(SqlProgram::compile_with_budget("TRUE", &mut nodes).is_err());
    assert_eq!(nodes.used().nodes, 1);
}

#[test]
fn failed_compilations_still_account_for_completed_source_and_token_work() {
    let mut budget = SqlCompileBudget::default();
    assert!(SqlProgram::compile_with_budget("TRUE;", &mut budget).is_err());
    assert_eq!(budget.used().source_bytes, 5);
    assert_eq!(budget.used().tokens, 2);
    assert_eq!(budget.used().nodes, 0);
}

#[test]
fn arena_node_guard_refuses_before_charging_or_pushing_an_extra_node() -> Result<(), SqlCompileError>
{
    let mut budget = SqlCompileBudget::default();
    let mut builder = Builder {
        nodes: Vec::new(),
        in_operands: Vec::new(),
        depth: 0,
        budget: &mut budget,
    };
    for _ in 0..MAX_SQL_EXPRESSION_NODES {
        builder.push(SqlNode::Literal(SqlLiteral::Null), 1)?;
    }
    assert_limit(
        builder.push(SqlNode::Literal(SqlLiteral::Null), 1),
        SqlCompileLimit::Nodes,
    );
    assert_eq!(builder.nodes.len(), MAX_SQL_EXPRESSION_NODES);
    assert_eq!(builder.budget.used().nodes, MAX_SQL_EXPRESSION_NODES);
    Ok(())
}

fn assert_limit<T>(result: Result<T, SqlCompileError>, expected: SqlCompileLimit) {
    assert!(matches!(result, Err(SqlCompileError::Limit { kind, .. }) if kind == expected));
}

fn assert_prior_children(program: &SqlProgram) {
    for (index, node) in program.nodes.iter().enumerate() {
        let prior = |child: u16| assert!((child as usize) < index);
        match node {
            SqlNode::Literal(_) | SqlNode::Property(_) | SqlNode::Exists(_) => {}
            SqlNode::Unary { input, .. } | SqlNode::IsNull { input, .. } => prior(*input),
            SqlNode::Binary { left, right, .. } => {
                prior(*left);
                prior(*right);
            }
            SqlNode::In {
                input, start, len, ..
            } => {
                prior(*input);
                for operand in &program.in_operands[*start..*start + *len] {
                    prior(*operand);
                }
            }
            SqlNode::Like {
                input,
                pattern,
                escape,
                ..
            } => {
                prior(*input);
                prior(*pattern);
                if let Some(escape) = escape {
                    prior(*escape);
                }
            }
        }
    }
}
