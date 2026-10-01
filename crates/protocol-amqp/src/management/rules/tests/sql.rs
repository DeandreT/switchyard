use super::*;

#[tokio::test]
async fn general_sql_is_validated_before_submission_and_preserves_exact_source() {
    let source = " [Colour] = 'Red' AND NOT EXISTS(disabled) ";
    let broker = ObservedBroker::default();
    let response =
        process_request_for(&sql("compiled", source), ENTITY, &broker, None, BUDGET).await;
    assert_eq!(response.status_code, 200);
    let commands = broker.submissions.lock().expect("commands");
    let CommandKind::CreateRule {
        filter: RuleFilter::Sql(filter),
        ..
    } = &commands[0].2
    else {
        panic!("SQL command");
    };
    assert_eq!(filter.expression(), source);
    assert_eq!(filter.semantic_version(), 1);
    assert!(broker.reads.lock().expect("reads").is_empty());
}

#[tokio::test]
async fn compilation_refusals_are_distinct_and_leave_the_owner_untouched() {
    let over_tokens = format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS));
    let over_in = format!(
        "x IN ({})",
        std::iter::repeat_n("1", domain::MAX_SQL_IN_ITEMS + 1)
            .collect::<Vec<_>>()
            .join(",")
    );
    for (source, status, condition) in [
        ("colour =".to_owned(), 400, crate::INVALID_FIELD),
        ("newid()=NULL".to_owned(), 501, crate::NOT_IMPLEMENTED),
        (
            "sys.NoSuchProperty='x'".to_owned(),
            501,
            crate::NOT_IMPLEMENTED,
        ),
        (
            "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
            403,
            crate::RESOURCE_LIMIT_EXCEEDED,
        ),
        (
            "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            403,
            crate::RESOURCE_LIMIT_EXCEEDED,
        ),
        (over_tokens, 403, crate::RESOURCE_LIMIT_EXCEEDED),
        (over_in, 403, crate::RESOURCE_LIMIT_EXCEEDED),
    ] {
        let broker = ObservedBroker::default();
        let response =
            process_request_for(&sql("refused", &source), ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, status, "{source:?}");
        assert_eq!(response.error_condition, Some(condition));
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
        let healthy =
            process_request_for(&sql("healthy", "amount>=1"), ENTITY, &broker, None, BUDGET).await;
        assert_eq!(healthy.status_code, 200);
        assert_eq!(broker.submissions.lock().expect("commands").len(), 1);
    }
}

#[tokio::test]
async fn domain_compilation_errors_use_the_same_wire_mapping_without_widening_old_fallbacks() {
    for (error, status, condition) in [
        (SqlCompileError::Syntax, 400, crate::INVALID_FIELD),
        (
            SqlCompileError::Unsupported {
                feature: "dynamic property",
            },
            501,
            crate::NOT_IMPLEMENTED,
        ),
        (
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateNodes,
                maximum: 1,
            },
            403,
            crate::RESOURCE_LIMIT_EXCEEDED,
        ),
    ] {
        let broker = ObservedBroker {
            rejection: Some(BrokerRejection::Refused(BrokerError::SqlRuleCompilation(
                error,
            ))),
            ..Default::default()
        };
        let response =
            process_request_for(&sql("valid", "amount>=1"), ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, status);
        assert_eq!(response.error_condition, Some(condition));
    }
    let old = ManagementResponse::from_rejection(
        MessageId::Ulong(1),
        None,
        &BrokerRejection::Refused(BrokerError::TopicDataPlaneNotImplemented),
    );
    assert_eq!(old.status_code, 500);
    assert_eq!(old.error_condition, Some(crate::NOT_IMPLEMENTED));
}

#[tokio::test]
async fn sql_enumeration_uses_the_original_expression_and_signed_compatibility_field() {
    let source = " colour='Red' OR amount IN (1,2) ";
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = vec![definition(
        "source",
        RuleFilter::Sql(SqlFilter::new(source).expect("SQL")),
    )];
    let response = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 200);
    let Value::Map(body) = &response.body else {
        panic!("body");
    };
    let Some(Value::List(entries)) = body.get(&Value::String(RULES.into())) else {
        panic!("rules");
    };
    let Value::Map(entry) = &entries[0] else {
        panic!("entry");
    };
    let description = fields(
        entry
            .get(&Value::String(RULE_DESCRIPTION.into()))
            .expect("description"),
        RULE_DESCRIPTION_CODE,
    );
    assert_eq!(
        fields(&description[0], SQL_FILTER_CODE),
        [Value::String(source.into()), Value::Int(20)]
    );
    assert!(fields(&description[1], EMPTY_ACTION_CODE).is_empty());
    assert_eq!(description[2], Value::String("source".into()));
    let encoded = serde_amqp::to_vec(&response.body).expect("encode");
    assert_eq!(
        serde_amqp::from_slice::<Value>(&encoded).expect("decode"),
        response.body
    );
    assert!(broker.submissions.lock().expect("commands").is_empty());
    assert_eq!(broker.reads.lock().expect("reads").len(), 1);
}

#[tokio::test]
async fn oversized_sql_pages_fail_as_a_whole_and_do_not_truncate_enumeration() {
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = vec![
        definition(
            "first",
            RuleFilter::Sql(SqlFilter::new("amount=1").expect("SQL")),
        ),
        definition(
            "second",
            RuleFilter::Sql(SqlFilter::new("amount=2").expect("SQL")),
        ),
    ];
    let response = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        DeliveryBudget {
            max_bytes: 16,
            ..BUDGET
        },
    )
    .await;
    assert_eq!(response.status_code, 403);
    assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
    assert_eq!(response.body, Value::Null);
    let healthy = process_request_for(
        &enumeration(Value::Int(100), Value::Int(0)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(healthy.status_code, 200);
    let Value::Map(body) = healthy.body else {
        panic!("body");
    };
    assert!(
        matches!(body.get(&Value::String(RULES.into())), Some(Value::List(entries)) if entries.len() == 2)
    );
}

#[tokio::test]
async fn general_sql_requires_listen_on_the_full_management_endpoint_before_compilation() {
    for (permissions, scope, expected) in [
        (PermissionSet::LISTEN, format!("{ENTITY}/$management"), 200),
        (PermissionSet::SEND, format!("{ENTITY}/$management"), 401),
        (PermissionSet::LISTEN, "Orders/$management".into(), 401),
        (
            PermissionSet::LISTEN,
            "Orders/subscriptions/beta/$management".into(),
            401,
        ),
    ] {
        let access = authorization(permissions, &scope);
        let broker = ObservedBroker::default();
        let response = process_request_for(
            &sql("compiled", "amount>=1"),
            ENTITY,
            &broker,
            Some(&access),
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, expected);
        if expected != 200 {
            let malformed = process_request_for(
                &sql("malformed", "colour ="),
                ENTITY,
                &broker,
                Some(&access),
                BUDGET,
            )
            .await;
            assert_eq!(malformed.status_code, 401);
            assert!(broker.submissions.lock().expect("commands").is_empty());
            assert!(broker.reads.lock().expect("reads").is_empty());
        }
    }
}
