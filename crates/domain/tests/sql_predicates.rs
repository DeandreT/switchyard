use domain::{
    MAX_SQL_COMPILE_NODES, MAX_SQL_COMPILE_SOURCE_BYTES, MAX_SQL_COMPILE_TOKENS,
    MAX_SQL_EXPRESSION_BYTES, MAX_SQL_EXPRESSION_DEPTH, MAX_SQL_EXPRESSION_NODES,
    MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS, MAX_SQL_IN_ITEMS,
    MAX_SQL_LIKE_PATTERN_BYTES, MAX_SQL_PARSER_DEPTH, MAX_TOPIC_RULE_COMPARISON_BYTES,
    MAX_TOPIC_RULE_MATCH_WORK, MessageEnvelope, MessageIdentifier, MessageProperties, MessageValue,
    SessionId, SqlCompileBudget, SqlCompileError, SqlCompileLimit, SqlCompileUsage,
    SqlEvaluationBudget, SqlEvaluationError, SqlEvaluationLimit, SqlEvaluationUsage,
    SqlMessageContext, SqlProgram, SqlTruth,
};

fn envelope(properties: impl IntoIterator<Item = (&'static str, MessageValue)>) -> MessageEnvelope {
    MessageEnvelope {
        application_properties: properties
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect(),
        ..MessageEnvelope::default()
    }
}

fn context<'a>(envelope: &'a MessageEnvelope) -> SqlMessageContext<'a> {
    SqlMessageContext {
        message_id: "authoritative-id",
        session_id: None,
        envelope: Some(envelope),
    }
}

fn run(expression: &str, envelope: &MessageEnvelope) -> Result<SqlTruth, SqlEvaluationError> {
    SqlProgram::compile(expression)
        .unwrap_or_else(|error| panic!("compile {expression:?}: {error:?}"))
        .evaluate(context(envelope), &mut SqlEvaluationBudget::default())
}

fn assert_truth(expression: &str, envelope: &MessageEnvelope, expected: SqlTruth) {
    assert_eq!(run(expression, envelope), Ok(expected), "{expression:?}");
}

fn assert_error(expression: &str, envelope: &MessageEnvelope, expected: SqlEvaluationError) {
    assert_eq!(run(expression, envelope), Err(expected), "{expression:?}");
}

#[path = "sql_predicates/limits.rs"]
mod limits;
#[path = "sql_predicates/numeric.rs"]
mod numeric;
#[path = "sql_predicates/semantics.rs"]
mod semantics;
