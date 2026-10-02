use super::*;

fn action_request() -> Message {
    request(
        ADD_RULE_OPERATION,
        [
            ("rule-name", Value::String("action".into())),
            (
                "rule-description",
                Value::Map(
                    [
                        (
                            Value::String("rule-name".into()),
                            Value::String("action".into()),
                        ),
                        (
                            Value::String("sql-filter".into()),
                            Value::Map(
                                [(
                                    Value::String("expression".into()),
                                    Value::String("1=1".into()),
                                )]
                                .into_iter()
                                .collect(),
                            ),
                        ),
                        (
                            Value::String("sql-rule-action".into()),
                            Value::Map(
                                [(
                                    Value::String("expression".into()),
                                    Value::String("REMOVE [private-marker]".into()),
                                )]
                                .into_iter()
                                .collect(),
                            ),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
        ],
    )
}

#[tokio::test]
async fn action_reply_routes_require_exact_child_namespace_kind_and_generation() {
    let current = identity(2);
    let queue = EntityPath::new("Orders").expect("queue");
    let mismatches = [
        identity(1),
        test_binding(&EntityPath::new("Orders/subscriptions/Beta").expect("sibling")),
        EntityBinding::new(
            NamespaceName::new("other").expect("namespace"),
            current.target().clone(),
            current.owner().clone(),
            current.kind(),
            2,
        )
        .expect("foreign namespace"),
        EntityBinding::new(
            current.namespace().clone(),
            queue.clone(),
            queue.clone(),
            domain::EntityIncarnationKind::Queue,
            2,
        )
        .expect("wrong kind"),
    ];
    let message = action_request();
    let authorization = authorization(PermissionSet::LISTEN);
    assert!(
        validate_reply_binding(&message, &current, &current, Some(&authorization))
            .await
            .is_ok()
    );
    for mismatch in mismatches {
        let error = validate_reply_binding(&message, &current, &mismatch, Some(&authorization))
            .await
            .expect_err("foreign route");
        assert_eq!(error.condition.as_symbol(), Symbol::from(crate::NOT_FOUND));
        assert!(
            !error
                .description
                .as_deref()
                .unwrap_or_default()
                .contains("private-marker")
        );
    }
}

#[tokio::test]
async fn action_listen_permission_precedes_foreign_reply_binding_and_source_compilation() {
    let message = action_request();
    let current = identity(2);
    for (permissions, condition) in [
        (PermissionSet::SEND, "amqp:unauthorized-access"),
        (PermissionSet::LISTEN, crate::NOT_FOUND),
    ] {
        let authorization = authorization(permissions);
        let error = validate_reply_binding(&message, &current, &identity(1), Some(&authorization))
            .await
            .expect_err("refusal");
        assert_eq!(error.condition.as_symbol(), Symbol::from(condition));
    }
    let authorization = authorization(PermissionSet::SEND);
    let response = process_request(
        &message,
        MessageId::Ulong(7),
        current.namespace(),
        current.target(),
        &BoundBroker::new(NoOwnerWork, current.clone()),
        &ConnectionManagement::default(),
        Some(&authorization),
        BUDGET,
    )
    .await;
    assert_eq!(response.status_code, 401);
    assert_eq!(response.error_condition, Some("amqp:unauthorized-access"));
}
