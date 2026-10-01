use super::*;

#[test]
fn system_fields_use_authoritative_ids_and_retained_optional_properties() {
    let session = SessionId::new("actual-session").expect("session");
    let envelope = MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String("stale-retained-id".into())),
            correlation_id: Some(MessageIdentifier::String("correlation".into())),
            to: Some("destination".into()),
            reply_to: Some("reply".into()),
            subject: Some("subject".into()),
            reply_to_group_id: Some("reply-session".into()),
            content_type: Some("application/json".into()),
            ..MessageProperties::default()
        },
        ..MessageEnvelope::default()
    };
    let context = SqlMessageContext {
        message_id: "actual-id",
        session_id: Some(&session),
        envelope: Some(&envelope),
    };
    for (field, expected) in [
        ("mEsSaGeId", "actual-id"),
        ("CorrelationId", "correlation"),
        ("To", "destination"),
        ("ReplyTo", "reply"),
        ("Subject", "subject"),
        ("Label", "subject"),
        ("SessionId", "actual-session"),
        ("ReplyToSessionId", "reply-session"),
        ("ContentType", "application/json"),
    ] {
        let expression = format!("sYs.{field} = '{expected}'");
        assert_eq!(
            SqlProgram::compile(&expression)
                .expect("system expression")
                .evaluate(context, &mut SqlEvaluationBudget::default()),
            Ok(SqlTruth::True),
            "{field}"
        );
    }
    assert_truth(
        "sys.MessageId = 'stale-retained-id'",
        &envelope,
        SqlTruth::False,
    );
    let absent = SqlMessageContext {
        message_id: "id",
        session_id: None,
        envelope: None,
    };
    for field in [
        "CorrelationId",
        "To",
        "ReplyTo",
        "Subject",
        "SessionId",
        "ReplyToSessionId",
        "ContentType",
    ] {
        assert_eq!(
            SqlProgram::compile(&format!("sys.{field} IS NULL"))
                .expect("known optional field")
                .evaluate(absent, &mut SqlEvaluationBudget::default()),
            Ok(SqlTruth::True),
            "{field}"
        );
    }
    assert!(matches!(
        SqlProgram::compile("sys.UnknownField IS NULL"),
        Err(SqlCompileError::Unsupported { .. })
    ));
}

#[test]
fn user_scope_identifiers_and_unicode_lowercase_are_explicit_and_unambiguous() {
    let envelope = envelope([
        ("CoLoR", MessageValue::String("Red".into())),
        ("with space", MessageValue::Long(7)),
        ("a]b", MessageValue::Long(8)),
        ("a\"b", MessageValue::Long(9)),
        ("\u{00e4}PFEL", MessageValue::String("fruit".into())),
        ("Subject", MessageValue::String("user-subject".into())),
        ("sys.MessageId", MessageValue::String("literal-key".into())),
        ("_name", MessageValue::Long(10)),
    ]);
    for expression in [
        "color = 'Red'",
        "UsEr.COLOR = 'Red'",
        "[with space] = 7",
        "[a]]b] = 8",
        "\"a\"\"b\" = 9",
        "p('CoLoR') = 'Red'",
        "property('with space') = 7",
        "[\u{00c4}pfel] = 'fruit'",
        "subject = 'user-subject' AND sys.Subject IS NULL",
        "p('sys.MessageId') = 'literal-key' AND sys.MessageId = 'authoritative-id'",
        "[_name] = 10",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
    assert_truth("color = 'red'", &envelope, SqlTruth::False);
    assert!(SqlProgram::compile("_name = 10").is_err());
    for properties in [
        vec![
            ("Color", MessageValue::Long(1)),
            ("COLOR", MessageValue::Long(1)),
        ],
        vec![
            ("\u{00c4}", MessageValue::Long(1)),
            ("\u{00e4}", MessageValue::Long(1)),
        ],
    ] {
        let envelope = super::envelope(properties);
        let key = envelope.application_properties.keys().next().expect("key");
        assert_error(
            &format!("[{key}] = 1"),
            &envelope,
            SqlEvaluationError::AmbiguousProperty,
        );
    }
}

#[test]
fn missing_null_exists_and_nonpredicate_results_remain_distinct() {
    let envelope = envelope([
        ("nil", MessageValue::Null),
        ("number", MessageValue::Long(1)),
        ("flag", MessageValue::Bool(true)),
    ]);
    for expression in [
        "missing IS NULL",
        "nil IS NULL",
        "EXISTS(nil)",
        "NOT EXISTS(missing)",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
    for expression in [
        "missing IS NOT NULL",
        "nil IS NOT NULL",
        "EXISTS(missing)",
        "NOT EXISTS(nil)",
    ] {
        assert_truth(expression, &envelope, SqlTruth::False);
    }
    for expression in [
        "missing = 1",
        "nil = 1",
        "NULL = NULL",
        "missing + 1 = 2",
        "NULL",
        "missing",
    ] {
        assert_truth(expression, &envelope, SqlTruth::Unknown);
    }
    assert_truth("flag", &envelope, SqlTruth::True);
    for expression in ["number", "1", "'text'"] {
        assert_error(expression, &envelope, SqlEvaluationError::NonPredicate);
    }
    assert!(SqlTruth::True.is_match());
    assert!(!SqlTruth::False.is_match());
    assert!(!SqlTruth::Unknown.is_match());
}

#[test]
fn presence_checks_do_not_require_supported_scalar_payloads() {
    for value in [
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::List(vec![MessageValue::Null]),
        MessageValue::Map(vec![(MessageValue::Null, MessageValue::Null)]),
        MessageValue::Array(vec![MessageValue::Null]),
    ] {
        let envelope = envelope([("present", value)]);
        assert_truth("EXISTS(present)", &envelope, SqlTruth::True);
        assert_truth("present IS NULL", &envelope, SqlTruth::False);
        assert_truth("present IS NOT NULL", &envelope, SqlTruth::True);
        assert_error(
            "present = 1",
            &envelope,
            SqlEvaluationError::UnsupportedValue,
        );
    }
    let collision = envelope([
        ("value", MessageValue::Null),
        ("VALUE", MessageValue::List(vec![])),
    ]);
    for expression in ["EXISTS(value)", "value IS NULL"] {
        assert_error(
            expression,
            &collision,
            SqlEvaluationError::AmbiguousProperty,
        );
    }
}

#[test]
fn complete_kleene_matrices_and_precedence_do_not_treat_unknown_as_false() {
    let envelope = MessageEnvelope::default();
    let terms = ["1=1", "1=0", "missing=1"];
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
    for (left_index, left) in terms.iter().enumerate() {
        for (right_index, right) in terms.iter().enumerate() {
            assert_truth(
                &format!("({left}) AND ({right})"),
                &envelope,
                and[left_index][right_index],
            );
            assert_truth(
                &format!("({left}) OR ({right})"),
                &envelope,
                or[left_index][right_index],
            );
        }
    }
    for (term, expected) in
        terms
            .into_iter()
            .zip([SqlTruth::False, SqlTruth::True, SqlTruth::Unknown])
    {
        assert_truth(&format!("NOT ({term})"), &envelope, expected);
    }
    assert_truth("TRUE OR FALSE AND FALSE", &envelope, SqlTruth::True);
    assert_truth("(TRUE OR FALSE) AND FALSE", &envelope, SqlTruth::False);
    assert_truth("NOT TRUE OR FALSE", &envelope, SqlTruth::False);
    assert_truth(
        "1 + 2 * 3 = 7 AND (1 + 2) * 3 = 9",
        &envelope,
        SqlTruth::True,
    );
}

#[test]
fn in_lists_preserve_unknown_and_do_not_stop_before_a_later_match() {
    let envelope = envelope([("value", MessageValue::Long(2))]);
    for expression in [
        "value IN (1,2,2)",
        "value NOT IN (1,3)",
        "value IN (missing,2)",
        "value IN (NULL,2)",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
    for expression in ["value NOT IN (1,2)", "value IN (1,3)"] {
        assert_truth(expression, &envelope, SqlTruth::False);
    }
    for expression in [
        "missing IN (1,2)",
        "value IN (1,missing)",
        "value NOT IN (1,NULL)",
        "NULL IN (NULL)",
    ] {
        assert_truth(expression, &envelope, SqlTruth::Unknown);
    }
}

#[test]
fn like_is_case_sensitive_anchored_newline_aware_and_uses_unicode_scalars() {
    let envelope = envelope([
        ("word", MessageValue::String("Abc".into())),
        ("multiline", MessageValue::String("a\nb".into())),
        ("astral", MessageValue::String("\u{1f642}".into())),
        ("combined", MessageValue::String("e\u{0301}".into())),
        ("literal", MessageValue::String("a_%'b".into())),
        ("empty", MessageValue::String(String::new())),
    ]);
    for expression in [
        "word LIKE 'A%'",
        "word LIKE '_bc'",
        "multiline LIKE 'a_b'",
        "multiline LIKE 'a%b'",
        "astral LIKE '_'",
        "combined LIKE '__'",
        "literal LIKE 'a!_!%''b' ESCAPE '!'",
        "empty LIKE '%'",
        "empty LIKE ''",
    ] {
        assert_truth(expression, &envelope, SqlTruth::True);
    }
    for expression in [
        "word LIKE 'a%'",
        "word LIKE 'bc'",
        "astral LIKE '__'",
        "combined LIKE '_'",
        "empty LIKE '_'",
    ] {
        assert_truth(expression, &envelope, SqlTruth::False);
    }
    assert_truth("missing LIKE '%'", &envelope, SqlTruth::Unknown);
    assert_truth("word NOT LIKE 'a%'", &envelope, SqlTruth::True);
    for expression in [
        "word LIKE 'A%' ESCAPE ''",
        "word LIKE 'A%' ESCAPE '!!'",
        "word LIKE 'A!' ESCAPE '!'",
    ] {
        assert_error(expression, &envelope, SqlEvaluationError::InvalidEscape);
    }
    for expression in ["word LIKE 1", "1 LIKE '%'", "word LIKE '%' ESCAPE 1"] {
        assert_error(expression, &envelope, SqlEvaluationError::TypeMismatch);
    }
}

#[test]
fn unsupported_language_constructs_are_not_silently_accepted() {
    for expression in [
        "newid() = newid()",
        "CAST(1 AS BIGINT) = 1",
        "value IN (SELECT 1)",
        "EXISTS(SELECT 1)",
        "property(name) = 1",
        "p(1) = 1",
        "other.value = 1",
        "sys.NotAField = 1",
        "LOWER(value) = 'x'",
    ] {
        assert!(
            matches!(
                SqlProgram::compile(expression),
                Err(SqlCompileError::Unsupported { .. }) | Err(SqlCompileError::Syntax)
            ),
            "{expression}"
        );
    }
    for expression in ["TRUE; FALSE", "SELECT 1", "SET value = 1", "TRUE junk"] {
        assert!(SqlProgram::compile(expression).is_err(), "{expression}");
    }
    assert_error(
        "'a' < 'b'",
        &MessageEnvelope::default(),
        SqlEvaluationError::StringOrderingUnsupported,
    );
}
