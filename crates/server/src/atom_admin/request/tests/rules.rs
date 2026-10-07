use super::*;

const RULE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><Parameters /></Filter><Name>Keep</Name></RuleDescription></content></entry>"#;
const MEMBER: &str = "/Orders/Subscriptions/Worker/Rules/Keep?api-version=2024-05";

#[test]
fn rule_routes_fold_only_reserved_structural_markers_before_scope_and_owner() {
    for path in [
        "/Orders/Subscriptions/Worker/Rules/Keep",
        "/Orders/subscriptions/Worker/rules/Keep",
        "/Orders/%53ubscriptions/Worker/%52ules/Keep",
    ] {
        let target = route::target(&path.parse().unwrap(), &HeaderMap::new()).unwrap();
        let (topic, subscription) = target.subscription().unwrap();
        assert_eq!(topic.as_str(), "Orders");
        assert_eq!(subscription.as_str(), "Worker");
        assert_eq!(target.rule_name().unwrap().as_str(), "Keep");
        let scope = target
            .scope(&ResourceScope::namespace(HOST).unwrap())
            .unwrap();
        assert_eq!(
            scope.path().collect::<Vec<_>>(),
            ["Orders", "subscriptions", "Worker", "rules", "Keep"]
        );
        assert!(matches!(target, Target::Rule { .. }));
    }
    let target = route::target(
        &"/Orders/Subscriptions/Worker/Rules".parse().unwrap(),
        &HeaderMap::new(),
    )
    .unwrap();
    assert!(matches!(target, Target::RuleCollection { .. }));
    assert_eq!(
        target
            .scope(&ResourceScope::namespace(HOST).unwrap())
            .unwrap()
            .path()
            .collect::<Vec<_>>(),
        ["Orders", "subscriptions", "Worker", "rules"]
    );
    assert!(target.rule_name().is_err());
}

#[test]
fn rule_names_and_topics_decode_once_without_folding_or_trimming() {
    let target = route::target(
        &"/Orders%252Fbranch/Subscriptions/Worker/Rules/%20K%CE%B1%252F%20"
            .parse()
            .unwrap(),
        &HeaderMap::new(),
    )
    .unwrap();
    assert_eq!(target.subscription().unwrap().0.as_str(), "Orders%2Fbranch");
    assert_eq!(target.rule_name().unwrap().as_str(), " K\u{03b1}%2F ");
    let scope = target
        .scope(&ResourceScope::namespace(HOST).unwrap())
        .unwrap();
    assert_eq!(
        scope.path().collect::<Vec<_>>(),
        [
            "Orders%2Fbranch",
            "subscriptions",
            "Worker",
            "rules",
            " K\u{03b1}%2F "
        ]
    );
    for name in [
        "%2F", "%5C", "%40", "%3F", "%23", "%2A", "%20", "%2E", "%2e%2E",
    ] {
        let uri: Uri = format!("/Orders/Subscriptions/Worker/Rules/{name}")
            .parse()
            .unwrap();
        assert!(
            route::target(&uri, &HeaderMap::new())
                .and_then(|target| target.rule_name())
                .is_err(),
            "{name}"
        );
    }
}

#[test]
fn new_rule_routes_preserve_old_terminal_queue_and_subscription_named_rules() {
    for path in ["/topic/Subscriptions", "/topic/subscriptions"] {
        let target = route::target(&path.parse().unwrap(), &HeaderMap::new()).unwrap();
        assert_eq!(target.entity().unwrap().as_str(), &path[1..]);
    }
    for path in ["/topic/Subscriptions/Rules", "/topic/subscriptions/rules"] {
        let target = route::target(&path.parse().unwrap(), &HeaderMap::new()).unwrap();
        assert!(matches!(target, Target::Subscription { .. }));
        assert_eq!(
            target.subscription().unwrap().1.as_str(),
            path.rsplit('/').next().unwrap()
        );
    }
    for path in [
        "/topic/SUBSCRIPTIONS/worker/Rules/Keep",
        "/topic/Subscriptions/worker/RULES/Keep",
    ] {
        assert!(matches!(
            route::target(&path.parse().unwrap(), &HeaderMap::new()).unwrap(),
            Target::Entity(_)
        ));
    }
}

#[test]
fn rule_collection_page_and_member_conditions_are_closed() {
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_static("untrusted.example"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/atom+xml"),
    );
    for (query, expected) in [
        (
            "api-version=2024-05",
            Ok(Operation::List { skip: 0, top: 100 }),
        ),
        (
            "api-version=2021-05&enrich=False&$skip=1000&$top=1",
            Ok(Operation::List { skip: 1000, top: 1 }),
        ),
        (
            "api-version=2024-05&enrich=True",
            Err(RequestFailure::BadRequest),
        ),
        (
            "api-version=2024-05&$skip=1001",
            Err(RequestFailure::BadRequest),
        ),
        (
            "api-version=2024-05&$top=0",
            Err(RequestFailure::BadRequest),
        ),
        (
            "api-version=2024-05&$top=101",
            Err(RequestFailure::BadRequest),
        ),
        (
            "api-version=2024-05&$top=+1",
            Err(RequestFailure::BadRequest),
        ),
        (
            "api-version=2024-05&$skip=0&%24skip=0",
            Err(RequestFailure::BadRequest),
        ),
    ] {
        let uri: Uri = format!("/Orders/Subscriptions/Worker/Rules?{query}")
            .parse()
            .unwrap();
        let target = route::target(&uri, &headers).unwrap();
        assert_eq!(
            route::operation(&Method::GET, Version::HTTP_11, &uri, &headers, &target),
            expected,
            "{query}"
        );
        for method in [Method::PUT, Method::DELETE] {
            assert!(route::operation(&method, Version::HTTP_11, &uri, &headers, &target).is_err());
        }
    }
    let uri: Uri = MEMBER.parse().unwrap();
    let target = route::target(&uri, &headers).unwrap();
    for (method, expected) in [
        (Method::GET, Operation::Get),
        (Method::PUT, Operation::Create),
        (Method::DELETE, Operation::Delete),
    ] {
        assert_eq!(
            route::operation(&method, Version::HTTP_11, &uri, &headers, &target),
            Ok(expected)
        );
        headers.insert("if-match", HeaderValue::from_static("*"));
        assert_eq!(
            route::operation(&method, Version::HTTP_11, &uri, &headers, &target),
            Err(RequestFailure::BadRequest)
        );
        headers.remove("if-match");
    }
}

#[tokio::test]
async fn rule_manage_authentication_precedes_operations_and_body_polling() {
    for permissions in [PermissionSet::MANAGE, PermissionSet::SEND] {
        let fixture = Fixture::new(permissions);
        for path in [MEMBER, "/Orders/Subscriptions/Worker/Rules?unknown=secret"] {
            let body = Frames::data(b"<secret malformed");
            let polls = body.polls.clone();
            let response = handle_with_epoch(
                request(
                    Method::POST,
                    path,
                    body,
                    if permissions == PermissionSet::SEND {
                        Some(token(&format!("https://{HOST}"), 200))
                    } else {
                        None
                    },
                ),
                &fixture.context,
                || Ok(100),
            )
            .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            let rendered = String::from_utf8(bytes(response).await.to_vec()).unwrap();
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("InvalidXml"));
            fixture.assert_untouched();
        }
    }
}

#[tokio::test]
async fn rule_literal_scope_rejects_alias_sibling_and_wrong_name_case_without_polling() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for audience in [
        format!("https://{HOST}/Orders/Subscriptions/Worker/Rules/Keep"),
        format!("https://{HOST}/Orders/subscriptions/Worker/rules/keep"),
        format!("https://{HOST}/Orders/subscriptions/Other/rules/Keep"),
        format!("https://{HOST}/orders/subscriptions/Worker/rules/Keep"),
    ] {
        let body = Frames::data(RULE);
        let polls = body.polls.clone();
        let response = handle_with_epoch(
            request(Method::PUT, MEMBER, body, Some(token(&audience, 200))),
            &fixture.context,
            || Ok(100),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        fixture.assert_untouched();
    }
}

#[tokio::test]
async fn rule_xml_name_mismatch_and_nonempty_reads_never_enter_the_owner() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    for (method, body) in [
        (
            Method::PUT,
            String::from_utf8(RULE.to_vec())
                .unwrap()
                .replace("<Name>Keep", "<Name>keep")
                .into_bytes(),
        ),
        (Method::GET, RULE.to_vec()),
        (Method::DELETE, RULE.to_vec()),
    ] {
        let response = handle_with_epoch(
            request(
                method,
                MEMBER,
                Full::new(Bytes::from(body)),
                Some(token(&format!("https://{HOST}"), 200)),
            ),
            &fixture.context,
            || Ok(100),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        fixture.assert_untouched();
    }
}

#[tokio::test]
async fn rule_grant_expiry_during_body_collection_rechecks_before_owner_admission() {
    let fixture = Fixture::new(PermissionSet::MANAGE);
    let epoch = Arc::new(AtomicU64::new(100));
    let mut body = Frames::data(RULE);
    body.advance = Some(epoch.clone());
    let response = handle_with_epoch(
        request(
            Method::PUT,
            MEMBER,
            body,
            Some(token(
                &format!("https://{HOST}/Orders/subscriptions/Worker/rules/Keep"),
                101,
            )),
        ),
        &fixture.context,
        || Ok(epoch.load(Ordering::SeqCst)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    fixture.assert_untouched();
}

#[tokio::test]
async fn rule_errors_are_typed_static_and_redacted() {
    for (error, status, text) in [
        (
            domain::BrokerError::RuleNotFound,
            StatusCode::NOT_FOUND,
            "requested rule was not found",
        ),
        (
            domain::BrokerError::RuleAlreadyExists,
            StatusCode::CONFLICT,
            "requested rule already exists",
        ),
        (
            domain::BrokerError::RuleLimitExceeded { maximum: 32 },
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceBusy",
        ),
        (
            domain::BrokerError::RuleTooLarge {
                maximum_bytes: 65536,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceBusy",
        ),
        (
            domain::BrokerError::RuleSetTooLarge {
                maximum_bytes: 262144,
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceBusy",
        ),
        (
            domain::BrokerError::Storage(StorageError::Backend {
                operation: "secret",
                detail: "secret token bytes".into(),
            }),
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        ),
    ] {
        let response = RequestFailure::from(crate::SubmitError::Propose(
            crate::ProposeError::Broker(error),
        ))
        .into_response();
        assert_eq!(response.status(), status);
        let body = String::from_utf8(bytes(response).await.to_vec()).unwrap();
        assert!(body.contains(text));
        assert!(!body.contains("secret"));
        assert!(!body.contains("requested queue"));
    }
    for (error, status) in [
        (xml::rules::RuleXmlError::Malformed, StatusCode::BAD_REQUEST),
        (
            xml::rules::RuleXmlError::UnsupportedDefinition,
            StatusCode::BAD_REQUEST,
        ),
        (
            xml::rules::RuleXmlError::WorkLimitExceeded,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            xml::rules::RuleXmlError::ReplyLimitExceeded,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let response = RequestFailure::from(error).into_response();
        assert_eq!(response.status(), status);
        assert!(
            !String::from_utf8(bytes(response).await.to_vec())
                .unwrap()
                .contains("secret")
        );
    }
}
