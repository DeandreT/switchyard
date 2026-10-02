use super::*;

fn set_action(message: &mut Message, action: Option<Value>) {
    let Body::Value(Value::Map(body)) = &mut message.body else {
        panic!("request map")
    };
    let Some(Value::Map(description)) = body.get_mut(&Value::String(RULE_DESCRIPTION.into()))
    else {
        panic!("rule description")
    };
    match action {
        Some(action) => {
            description.insert(Value::String(SQL_ACTION.into()), action);
        }
        None => {
            description.shift_remove(&Value::String(SQL_ACTION.into()));
        }
    }
}

fn action_request(name: &str, source: &str) -> Message {
    let mut message = sql(name, "1=1");
    set_action(
        &mut message,
        Some(Value::Map(map([(
            EXPRESSION,
            Value::String(source.into()),
        )]))),
    );
    message
}

fn rule_fields(entry: &Value) -> &[Value] {
    let Value::Map(entry) = entry else {
        panic!("rule entry")
    };
    fields(
        get(entry, RULE_DESCRIPTION).expect("description"),
        RULE_DESCRIPTION_CODE,
    )
}

fn entries(response: &ManagementResponse) -> &[Value] {
    let Value::Map(body) = &response.body else {
        panic!("rule body")
    };
    let Some(Value::List(entries)) = get(body, RULES) else {
        panic!("rule entries")
    };
    entries
}

#[tokio::test]
async fn missing_and_null_actions_keep_the_original_command_variant() {
    for action in [None, Some(Value::Null)] {
        let mut message = sql("plain", "1=1");
        set_action(&mut message, action);
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, 200);
        assert!(matches!(
            &broker.submissions.lock().expect("commands")[..],
            [(namespace, topic, CommandKind::CreateRule { subscription, name, filter: RuleFilter::True })]
                if namespace.as_str() == "tenant"
                    && topic.as_str() == "Orders"
                    && subscription.as_str() == "Alpha"
                    && name.as_str() == "plain"
        ));
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn compiled_remove_actions_preserve_exact_source_and_typed_filters() {
    let source = " /* exact source */ remove USER.[CoLoUr]; REMOVE [RuleName]; ";
    for (mut message, expected) in [
        (sql("action", "1=1"), RuleFilter::True),
        (sql("action", "1=0"), RuleFilter::False),
        (
            sql("action", " colour = 'Red' "),
            RuleFilter::Sql(SqlFilter::new(" colour = 'Red' ").expect("SQL")),
        ),
        (
            add(
                "action",
                CORRELATION_FILTER,
                Value::Map(map([("label", Value::String("Order".into()))])),
            ),
            RuleFilter::Correlation(CorrelationFilter {
                subject: Some("Order".into()),
                ..CorrelationFilter::default()
            }),
        ),
    ] {
        set_action(
            &mut message,
            Some(Value::Map(map([(
                EXPRESSION,
                Value::String(source.into()),
            )]))),
        );
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, 200);
        assert_eq!(response.correlation_id, MessageId::Ulong(7));
        assert_eq!(response.tracking_id.as_deref(), Some("rule-trace"));
        let submissions = broker.submissions.lock().expect("commands");
        let [
            (
                namespace,
                topic,
                CommandKind::CreateRuleWithAction {
                    subscription,
                    name,
                    filter,
                    action,
                },
            ),
        ] = submissions.as_slice()
        else {
            panic!("one typed action command")
        };
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(topic.as_str(), "Orders");
        assert_eq!(subscription.as_str(), "Alpha");
        assert_eq!(name.as_str(), "action");
        assert_eq!(filter, &expected);
        assert_eq!(action.expression(), source);
        assert_eq!(action.semantic_version(), 1);
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn malformed_action_maps_are_not_treated_as_empty_actions() {
    let cases = [
        Value::Bool(false),
        Value::String("REMOVE private-marker".into()),
        Value::List(vec![]),
        Value::Map(map([])),
        Value::Map(map([(EXPRESSION, Value::Null)])),
        Value::Map(map([(EXPRESSION, Value::Int(20))])),
        Value::Map(map([(
            EXPRESSION,
            Value::Symbol(Symbol::from("REMOVE secret")),
        )])),
        Value::Map(map([
            (EXPRESSION, Value::String("REMOVE a".into())),
            ("parameters", Value::Null),
        ])),
        Value::Map(map([
            (EXPRESSION, Value::String("REMOVE a".into())),
            ("semantic-version", Value::Int(1)),
        ])),
        Value::Map(
            [
                (
                    Value::String(EXPRESSION.into()),
                    Value::String("REMOVE a".into()),
                ),
                (
                    Value::Symbol(Symbol::from(EXPRESSION)),
                    Value::String("REMOVE private-marker".into()),
                ),
            ]
            .into_iter()
            .collect(),
        ),
    ];
    for action in cases {
        let mut message = sql("bad", "1=1");
        set_action(&mut message, Some(action));
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, 400);
        assert_eq!(response.error_condition, Some(crate::INVALID_FIELD));
        assert_eq!(response.body, Value::Null);
        assert!(!response.status_description.contains("private-marker"));
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn action_syntax_and_unsupported_features_refuse_before_submission_without_source_echo() {
    for (source, status, condition) in [
        ("", 400, crate::INVALID_FIELD),
        ("   ", 400, crate::INVALID_FIELD),
        ("REMOVE", 400, crate::INVALID_FIELD),
        ("REMOVE [private-marker", 400, crate::INVALID_FIELD),
        ("REMOVE private_marker;;", 400, crate::INVALID_FIELD),
        (
            "SET private_marker = 'private-value'",
            501,
            crate::NOT_IMPLEMENTED,
        ),
        ("REMOVE sys.private_marker", 501, crate::NOT_IMPLEMENTED),
        ("REMOVE foreign.private_marker", 501, crate::NOT_IMPLEMENTED),
        (
            "REMOVE property('private-marker')",
            501,
            crate::NOT_IMPLEMENTED,
        ),
    ] {
        let broker = ObservedBroker::default();
        let response = process_request_for(
            &action_request("bad", source),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, status, "{source}");
        assert_eq!(response.error_condition, Some(condition));
        assert_eq!(response.body, Value::Null);
        assert!(!response.status_description.contains("private-marker"));
        assert!(!response.status_description.contains("private_marker"));
        assert!(!response.status_description.contains("private-value"));
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
        let healthy = process_request_for(
            &action_request("healthy", "REMOVE a"),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(healthy.status_code, 200);
        assert_eq!(broker.submissions.lock().expect("commands").len(), 1);
    }
}

#[tokio::test]
async fn source_token_and_statement_limits_are_checked_before_owner_work() {
    for source in [
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
        "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
        format!("REMOVE[{}]", "\u{1f600}".repeat(509)),
        format!("{}REMOVE[a]", "/*x*/".repeat(127)),
        "REMOVE[a];".repeat(33),
    ] {
        let broker = ObservedBroker::default();
        let response = process_request_for(
            &action_request("limit", &source),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, 403);
        assert_eq!(
            response.error_condition,
            Some(crate::RESOURCE_LIMIT_EXCEEDED)
        );
        assert_eq!(response.body, Value::Null);
        assert!(!response.status_description.contains(&source));
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
    let source = format!("REMOVE[{}]", "\u{1f600}".repeat(508));
    assert_eq!(
        source.encode_utf16().count(),
        domain::MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    let (_, _, action) =
        create_rule(&action_request("exact", &source)).expect("exact UTF-16 bound");
    assert_eq!(action.expect("action").expression(), source);
}

#[test]
fn action_validation_keeps_its_priority_over_filter_parsing() {
    let mut message = action_request("priority", "SET private_marker = 1");
    let Body::Value(Value::Map(body)) = &mut message.body else {
        panic!("map")
    };
    let Some(Value::Map(description)) = body.get_mut(&Value::String(RULE_DESCRIPTION.into()))
    else {
        panic!("description")
    };
    description.shift_remove(&Value::String(SQL_FILTER.into()));
    assert!(matches!(
        create_rule(&message),
        Err(RuleRequestError::Domain(BrokerError::SqlActionCompilation(
            SqlCompileError::Unsupported { .. }
        )))
    ));
}

fn binary_definition(size: usize, action: Option<SqlAction>) -> RuleDefinition {
    RuleDefinition {
        name: RuleName::new("boundary").expect("name"),
        filter: RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([("payload".into(), MessageValue::Binary(vec![0; size]))]),
            ..CorrelationFilter::default()
        }),
        created_at: Timestamp::UNIX_EPOCH,
        action,
    }
}

#[tokio::test]
async fn combined_filter_and_action_rule_obeys_the_exact_encoded_limit() {
    let source = "REMOVE payload";
    let action = SqlAction::new(source).expect("action");
    let mut lower = 0;
    let mut upper = domain::MAX_RULE_BYTES;
    while lower < upper {
        let middle = (lower + upper).div_ceil(2);
        if binary_definition(middle, Some(action.clone()))
            .encoded_size()
            .is_ok()
        {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    assert_eq!(
        binary_definition(lower, Some(action.clone()))
            .encoded_size()
            .expect("exact rule"),
        domain::MAX_RULE_BYTES
    );
    assert!(binary_definition(lower + 1, None).encoded_size().is_ok());
    for (size, expected) in [(lower, 200), (lower + 1, 403)] {
        let mut message = add(
            "boundary",
            CORRELATION_FILTER,
            Value::Map(map([(
                FILTER_PROPERTIES,
                Value::Map(map([("payload", Value::Binary(vec![0; size].into()))])),
            )])),
        );
        set_action(
            &mut message,
            Some(Value::Map(map([(
                EXPRESSION,
                Value::String(source.into()),
            )]))),
        );
        let broker = ObservedBroker::default();
        let response = process_request_for(&message, ENTITY, &broker, None, BUDGET).await;
        assert_eq!(response.status_code, expected);
        if expected == 403 {
            assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
            assert_eq!(response.body, Value::Null);
            assert!(broker.submissions.lock().expect("commands").is_empty());
        } else {
            assert!(matches!(
                &broker.submissions.lock().expect("commands")[..],
                [(_, _, CommandKind::CreateRuleWithAction { .. })]
            ));
        }
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
}

#[tokio::test]
async fn action_enumeration_uses_full_width_descriptor_exact_source_and_signed_twenty() {
    let source = " /* preserve */ REMOVE user.[private-marker]; ";
    let mut action = definition(
        "action",
        RuleFilter::Sql(SqlFilter::new(" colour = 'Red' ").expect("filter")),
    );
    action.action = Some(SqlAction::new(source).expect("action"));
    let definitions = vec![action, definition("plain", RuleFilter::True)];
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = definitions.clone();
    for _ in 0..2 {
        let response = process_request_for(
            &enumeration(Value::Int(100), Value::Int(0)),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, 200);
        let listed = entries(&response);
        assert_eq!(listed.len(), 2);
        let rule = rule_fields(&listed[0]);
        assert_eq!(
            fields(&rule[0], SQL_FILTER_CODE),
            [Value::String(" colour = 'Red' ".into()), Value::Int(20)]
        );
        assert_eq!(
            fields(&rule[1], 0x0000013700000006),
            [Value::String(source.into()), Value::Int(20)]
        );
        assert_ne!(SQL_ACTION_CODE, SQL_FILTER_CODE);
        assert_eq!(rule[2], Value::String("action".into()));
        assert_eq!(rule[3], Value::Timestamp(1_000.into()));
        assert!(fields(&rule_fields(&listed[1])[1], EMPTY_ACTION_CODE).is_empty());
        let decoded =
            amqp::decode_message(&encode_message(&response.into_message()).expect("encode"))
                .expect("decode");
        let Body::Value(Value::Map(body)) = &decoded.body else {
            panic!("decoded rule body")
        };
        let Some(Value::List(decoded_entries)) = get(body, RULES) else {
            panic!("decoded entries")
        };
        assert_eq!(
            fields(&rule_fields(&decoded_entries[0])[1], SQL_ACTION_CODE),
            [Value::String(source.into()), Value::Int(20)]
        );
    }
    assert_eq!(*broker.definitions.lock().expect("rules"), definitions);
    assert_eq!(broker.reads.lock().expect("reads").len(), 2);
    assert!(broker.submissions.lock().expect("commands").is_empty());
}

#[tokio::test]
async fn whole_requested_action_page_must_fit_the_response_budget() {
    let mut action = definition("action", RuleFilter::True);
    action.action = Some(SqlAction::new("REMOVE [private-marker]").expect("action"));
    let plain = definition("plain", RuleFilter::False);
    let expected = map_body(
        RULES,
        Value::List(vec![
            encoded_rule(&action).expect("action"),
            encoded_rule(&plain).expect("plain"),
        ]),
    );
    let size = serde_amqp::to_vec(&expected).expect("body size").len() as u64;
    let broker = ObservedBroker::default();
    *broker.definitions.lock().expect("rules") = vec![action, plain];
    for maximum in [size - 1, size] {
        let response = process_request_for(
            &enumeration(Value::Int(100), Value::Int(0)),
            ENTITY,
            &broker,
            None,
            DeliveryBudget {
                max_bytes: maximum,
                ..BUDGET
            },
        )
        .await;
        if maximum < size {
            assert_eq!(response.status_code, 403);
            assert_eq!(response.error_condition, Some(crate::MESSAGE_SIZE_EXCEEDED));
            assert_eq!(response.body, Value::Null);
            assert!(!response.status_description.contains("private-marker"));
        } else {
            assert_eq!(response.status_code, 200);
            assert_eq!(response.body, expected);
        }
    }
    assert!(broker.submissions.lock().expect("commands").is_empty());
    assert_eq!(broker.reads.lock().expect("reads").len(), 2);
}

#[tokio::test]
async fn action_domain_errors_keep_static_statuses_and_full_set_read_failure_precedes_paging() {
    for (error, status, condition) in [
        (SqlCompileError::Syntax, 400, crate::INVALID_FIELD),
        (
            SqlCompileError::Unsupported {
                feature: "SQL action statement",
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
            rejection: Some(BrokerRejection::Refused(BrokerError::SqlActionCompilation(
                error,
            ))),
            ..ObservedBroker::default()
        };
        let response = process_request_for(
            &action_request("refused", "REMOVE [private-marker]"),
            ENTITY,
            &broker,
            None,
            BUDGET,
        )
        .await;
        assert_eq!(response.status_code, status);
        assert_eq!(response.error_condition, Some(condition));
        assert_eq!(response.body, Value::Null);
        assert!(!response.status_description.contains("private-marker"));
        assert_eq!(broker.submissions.lock().expect("commands").len(), 1);
    }
    let broker = ObservedBroker {
        rejection: Some(BrokerRejection::Refused(BrokerError::DanglingRuleMetadata)),
        ..ObservedBroker::default()
    };
    let response = process_request_for(
        &enumeration(Value::Int(1), Value::Int(i32::MAX)),
        ENTITY,
        &broker,
        None,
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 500);
    assert_eq!(response.error_condition, Some(crate::INTERNAL_ERROR));
    assert_eq!(response.body, Value::Null);
    assert_eq!(broker.reads.lock().expect("reads").len(), 1);
    assert!(broker.submissions.lock().expect("commands").is_empty());
}

#[tokio::test]
async fn action_permission_denial_precedes_compilation_and_foreign_associated_names() {
    for (permissions, scope) in [
        (PermissionSet::SEND, format!("{ENTITY}/$management")),
        (
            PermissionSet::LISTEN,
            "Orders/subscriptions/beta/$management".into(),
        ),
        (
            PermissionSet::LISTEN,
            "orders/subscriptions/Alpha/$management".into(),
        ),
    ] {
        let authorization = authorization(permissions, &scope);
        let mut message = action_request("denied", "SET [private-marker] = 1");
        message
            .application_properties
            .as_mut()
            .expect("properties")
            .insert(ASSOCIATED_LINK_NAME_PROPERTY, "foreign-sibling");
        let broker = ObservedBroker::default();
        let response =
            process_request_for(&message, ENTITY, &broker, Some(&authorization), BUDGET).await;
        assert_eq!(response.status_code, 401);
        assert!(broker.submissions.lock().expect("commands").is_empty());
        assert!(broker.reads.lock().expect("reads").is_empty());
    }
    let authorization = authorization(PermissionSet::LISTEN, &format!("{ENTITY}/$management"));
    let broker = ObservedBroker::default();
    let response = process_request_for(
        &action_request("healthy", "REMOVE a"),
        ENTITY,
        &broker,
        Some(&authorization),
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 200);
}
