use super::{
    actions::{create_action, get_actions, list_actions},
    *,
};

pub(super) async fn exact_scope<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider).await?;
    let (mut entities, mut rules) = node.clients().await?;
    node.topology(&mut entities).await?;
    let before = node.snapshot()?;
    for token in [None, Some("not-a-token".into()), Some(sas("", "send"))] {
        let expected = if token
            .as_ref()
            .is_some_and(|token| token.contains("skn=send"))
        {
            Code::PermissionDenied
        } else {
            Code::Unauthenticated
        };
        let error = timeout(
            DEADLINE,
            rules.create_rule_with_action(request(
                create_action(
                    CHILD,
                    "Denied",
                    Filter::TrueFilter(TrueRuleFilter {}),
                    "SET private_source = 7",
                    None,
                ),
                token,
            )),
        )
        .await?
        .expect_err("authentication and Manage precede action compilation");
        assert_eq!(error.code(), expected);
        assert!(!error.message().contains("private_source"));
        assert!(!error.message().contains(KEY));
    }
    for path in [
        "Orders/subscriptions/Beta",
        "orders/subscriptions/Alpha",
        "Orders/subscriptions/alpha",
    ] {
        let error = timeout(
            DEADLINE,
            rules.create_rule_with_action(request(
                create_action(
                    path,
                    "Denied",
                    Filter::TrueFilter(TrueRuleFilter {}),
                    "SET private_source = 7",
                    None,
                ),
                Some(sas(CHILD, "manage")),
            )),
        )
        .await?
        .expect_err("an exact child grant cannot create elsewhere");
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(!error.message().contains("private_source"));
        for error in [
            timeout(
                DEADLINE,
                rules.get_rule(request(
                    get_actions(path, "$Default"),
                    Some(sas(CHILD, "manage")),
                )),
            )
            .await?
            .expect_err("action-aware Get preserves exact scope"),
            timeout(
                DEADLINE,
                rules.list_rules(request(list_actions(path), Some(sas(CHILD, "manage")))),
            )
            .await?
            .expect_err("action-aware List preserves exact scope"),
        ] {
            assert_eq!(error.code(), Code::PermissionDenied);
        }
    }
    let mut wrong_namespace = create_action(
        CHILD,
        "Denied",
        Filter::TrueFilter(TrueRuleFilter {}),
        "SET private_source = 7",
        None,
    );
    wrong_namespace.namespace = "other".into();
    assert_eq!(
        timeout(
            DEADLINE,
            rules.create_rule_with_action(request(wrong_namespace, Some(sas(CHILD, "manage"))))
        )
        .await?
        .expect_err("endpoint namespace is fixed")
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(node.snapshot()?, before);
    let source = "REMOVE [private-audit];";
    timeout(
        DEADLINE,
        rules.create_rule_with_action(request(
            create_action(
                "Orders/SUBSCRIPTIONS/Alpha",
                "Allowed",
                Filter::TrueFilter(TrueRuleFilter {}),
                source,
                None,
            ),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    let allowed = timeout(
        DEADLINE,
        rules.get_rule(request(
            get_actions(CHILD, "Allowed"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(allowed.subscription_path, CHILD);
    assert_eq!(allowed.action.as_ref().unwrap().expression, source);
    let before = node.snapshot()?;
    for token in [
        None,
        Some(sas("", "send")),
        Some(sas("Orders/subscriptions/Beta", "manage")),
    ] {
        let expected = if token.is_none() {
            Code::Unauthenticated
        } else {
            Code::PermissionDenied
        };
        for error in [
            timeout(
                DEADLINE,
                rules.get_rule(request(get_actions(CHILD, "Allowed"), token.clone())),
            )
            .await?
            .expect_err("action source reads require exact Manage"),
            timeout(
                DEADLINE,
                rules.list_rules(request(list_actions(CHILD), token.clone())),
            )
            .await?
            .expect_err("action lists require exact Manage"),
            timeout(
                DEADLINE,
                rules.delete_rule(request(delete(CHILD, "Allowed"), token)),
            )
            .await?
            .expect_err("action deletion requires exact Manage"),
        ] {
            assert_eq!(error.code(), expected);
            assert!(!error.message().contains(source));
            assert!(!error.message().contains(KEY));
        }
    }
    assert_eq!(node.snapshot()?, before);
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list_actions(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        2
    );
    timeout(
        DEADLINE,
        rules.delete_rule(request(
            delete(CHILD, "Allowed"),
            Some(sas(CHILD, "manage")),
        )),
    )
    .await??;
    assert_eq!(
        timeout(
            DEADLINE,
            rules.list_rules(request(list(CHILD), Some(sas(CHILD, "manage"))))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        1
    );
    Ok(())
}
