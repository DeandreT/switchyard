use super::*;

use domain::{CorrelationFilter, MessageValue, RuleFilter, RuleName, SqlAction, SqlFilter};
use server::AtomRuleDefinition;

fn definition(name: &str, keep: bool) -> Vec<u8> {
    let filter = if keep { "True" } else { "False" };
    let expression = if keep { "1=1" } else { "1=0" };
    format!(r#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="{filter}Filter"><SqlExpression>{expression}</SqlExpression><Parameters /></Filter><Name>{name}</Name></RuleDescription></content></entry>"#).into_bytes()
}

fn correlation_definition(name: &str, fields: &str, properties: &str) -> Vec<u8> {
    let name = quick_xml::escape::escape(name);
    format!(r#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="CorrelationFilter">{fields}<Properties>{properties}</Properties></Filter><Name>{name}</Name></RuleDescription></content></entry>"#).into_bytes()
}

fn correlation_property(key: &str, kind: &str, value: &str) -> String {
    let key = quick_xml::escape::escape(key);
    let value = quick_xml::escape::escape(value);
    format!(
        r#"<KeyValueOfstringanyType><Key>{key}</Key><Value xmlns:s="http://www.w3.org/2001/XMLSchema" i:type="s:{kind}">{value}</Value></KeyValueOfstringanyType>"#
    )
}

const COLLIDING_CORRELATION_KEYS: &[(&str, &str)] = &[
    ("a", "A"),
    ("\u{e9}", "\u{c9}"),
    ("\u{b5}", "\u{39c}"),
    ("\u{250}", "\u{2c6f}"),
    ("\u{3c2}", "\u{3a3}"),
    ("\u{1c8a}", "\u{1c89}"),
    ("\u{10428}", "\u{10400}"),
    ("\u{16e60}", "\u{16e40}"),
    (" Key \u{e9}", " KEY \u{c9}"),
];

const DISTINCT_CORRELATION_KEYS: &[(&str, &str)] = &[
    ("\u{131}", "I"),
    ("\u{17f}", "S"),
    ("\u{130}", "i"),
    ("\u{212a}", "K"),
    ("\u{df}", "\u{1e9e}"),
    ("\u{df}", "SS"),
    ("\u{fb00}", "ff"),
    ("\u{e9}", "e\u{301}"),
    ("\u{10d70}", "\u{10d50}"),
    ("\u{16ebb}", "\u{16ea0}"),
    (" Key ", "Key"),
    ("", " "),
];

fn correlation_keys(left: &str, right: &str) -> CorrelationFilter {
    assert_ne!(left, right, "fixture keys must remain ordinally distinct");
    CorrelationFilter {
        properties: [
            (left.into(), MessageValue::Int(1)),
            (right.into(), MessageValue::Long(2)),
        ]
        .into_iter()
        .collect(),
        ..CorrelationFilter::default()
    }
}

fn correlation_wire() -> (String, String, CorrelationFilter) {
    let fields = [
        ("CorrelationId", ""),
        ("MessageId", "Message-CaSe"),
        ("To", "https://example.invalid/?a=1&b=<x>"),
        ("ReplyTo", " reply \u{03BB} "),
        ("Label", "Subject\r\n&<>"),
        ("SessionId", ""),
        ("ReplyToSessionId", "Reply-Session"),
        ("ContentType", "application/CaseSensitive"),
    ]
    .into_iter()
    .map(|(name, text)| format!("<{name}>{}</{name}>", quick_xml::escape::escape(text)))
    .collect();
    let properties = [
        ("boolean", "boolean", "1"),
        ("double", "double", "1.2345678901234567e+0"),
        ("integer", "int", "-2147483648"),
        ("long", "long", "9223372036854775807"),
        ("negative-infinity", "double", "-INF"),
        ("negative-zero", "double", "-0"),
        ("positive-infinity", "double", "INF"),
        ("string-\u{03BB}&<", "string", " Red & <\u{03BB}> \r\n"),
        ("timestamp", "dateTime", "1970-01-01T01:30:00.123+01:30"),
    ]
    .into_iter()
    .map(|(key, kind, value)| correlation_property(key, kind, value))
    .collect();
    let filter = CorrelationFilter {
        correlation_id: Some(String::new()),
        message_id: Some("Message-CaSe".into()),
        to: Some("https://example.invalid/?a=1&b=<x>".into()),
        reply_to: Some(" reply \u{03BB} ".into()),
        subject: Some("Subject\r\n&<>".into()),
        session_id: Some(String::new()),
        reply_to_session_id: Some("Reply-Session".into()),
        content_type: Some("application/CaseSensitive".into()),
        properties: [
            ("boolean".into(), MessageValue::Bool(true)),
            (
                "double".into(),
                MessageValue::Double(1.234_567_890_123_456_7_f64.to_bits()),
            ),
            ("integer".into(), MessageValue::Int(i32::MIN)),
            ("long".into(), MessageValue::Long(i64::MAX)),
            (
                "negative-infinity".into(),
                MessageValue::Double(f64::NEG_INFINITY.to_bits()),
            ),
            (
                "negative-zero".into(),
                MessageValue::Double((-0.0_f64).to_bits()),
            ),
            (
                "positive-infinity".into(),
                MessageValue::Double(f64::INFINITY.to_bits()),
            ),
            (
                "string-\u{03BB}&<".into(),
                MessageValue::String(" Red & <\u{03BB}> \r\n".into()),
            ),
            ("timestamp".into(), MessageValue::Timestamp(123)),
        ]
        .into_iter()
        .collect(),
    };
    (fields, properties, filter)
}

async fn seed<P: StoreProvider>(node: &Node<P>, topic: &str, subscription: &str) -> TestResult {
    seed_topic(node, topic).await?;
    assert_eq!(
        node.handle()
            .submit(
                NamespaceName::new("tenant")?,
                EntityPath::new(topic)?,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(subscription)?,
                    config: configured()
                }
            )
            .await?,
        CommandOutcome::SubscriptionCreated
    );
    Ok(())
}

fn member(name: &str) -> String {
    format!("/orders/Subscriptions/worker/Rules/{name}?api-version=2024-05")
}

fn collection(skip: usize, top: usize) -> String {
    format!(
        "/orders/Subscriptions/worker/Rules?api-version=2024-05&enrich=False&$skip={skip}&$top={top}"
    )
}

fn assert_entry(body: &[u8], name: &str, keep: bool) -> TestResult {
    let body = std::str::from_utf8(body)?;
    assert!(body.contains(&format!("<title>{name}</title>")));
    assert!(body.contains(&format!("<Name>{name}</Name>")));
    assert!(body.contains(if keep { "TrueFilter" } else { "FalseFilter" }));
    assert!(body.contains(if keep {
        "<SqlExpression>1=1</SqlExpression>"
    } else {
        "<SqlExpression>1=0</SqlExpression>"
    }));
    for absent in [
        "<Action",
        "CreatedAt",
        "UpdatedAt",
        "SubscriptionDescription",
        "MessageCount",
        "SizeInBytes",
    ] {
        assert!(!body.contains(absent), "{absent}");
    }
    Ok(())
}

async fn rule_crud_empty_feed_and_default_recreation_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, body) = exchange(
            &node,
            Method::GET,
            &member("$Default"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_entry(&body, "$Default", true)?;
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        let (status, body) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&body)?.matches("<entry").count(), 1);
        let (status, body) = exchange(
            &node,
            Method::DELETE,
            &member("$Default"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let (status, body) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let text = std::str::from_utf8(&body)?;
        assert!(text.starts_with("<feed "));
        assert!(text.ends_with("></feed>"));
        assert!(!text.contains("<entry"));
        for (name, keep) in [("Keep", true), ("Drop", false)] {
            let (status, created) = exchange(
                &node,
                Method::PUT,
                &member(name),
                Some(management_token()),
                &definition(name, keep),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);
            assert_entry(&created, name, keep)?;
            let (status, read) = exchange(
                &node,
                Method::GET,
                &member(name),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(read, created);
            let before = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, body) = exchange(
                &node,
                Method::PUT,
                &member(name),
                Some(management_token()),
                &definition(name, !keep),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CONFLICT);
            assert!(std::str::from_utf8(&body)?.contains("rule already exists"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        }
        let (status, first) = exchange(
            &node,
            Method::GET,
            &collection(0, 1),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&first)?.matches("<entry").count(), 1);
        let (status, second) = exchange(
            &node,
            Method::GET,
            &collection(1, 1),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&second)?.matches("<entry").count(), 1);
        assert_ne!(first, second);
        let (status, beyond) = exchange(
            &node,
            Method::GET,
            &collection(2, 1),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(!std::str::from_utf8(&beyond)?.contains("<entry"));
        for method in [Method::GET, Method::DELETE] {
            let before = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, body) = exchange(
                &node,
                method.clone(),
                &member("Absent"),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(std::str::from_utf8(&body)?.contains("rule was not found"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(
                node.clock.0.load(Ordering::SeqCst),
                clock + usize::from(method == Method::DELETE)
            );
        }
        let (status, _) = exchange(
            &node,
            Method::PUT,
            &member("$Default"),
            Some(management_token()),
            &definition("$Default", false),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            node.handle()
                .get_atom_rule(
                    NamespaceName::new("tenant")?,
                    EntityPath::new("orders")?,
                    SubscriptionName::new("worker")?,
                    RuleName::new("$Default")?
                )
                .await?,
            Some(AtomRuleDefinition {
                name: RuleName::new("$Default")?,
                filter: RuleFilter::False,
                action: None,
            })
        );
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn rule_marker_authorization_preserves_literal_names<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "Orders%2Fbranch", "Rules").await?;
        let path =
            "/Orders%252Fbranch/Subscriptions/Rules/Rules/%20K%CE%B1%252F%20?api-version=2024-05";
        let canonical = format!(
            "https://{HOST}/Orders%252Fbranch/subscriptions/Rules/rules/%20K%CE%B1%252F%20"
        );
        let before = node.snapshot()?;
        let effects = node.effects();
        for audience in [
            canonical.replace("/subscriptions/", "/Subscriptions/"),
            canonical.replace("/rules/", "/Rules/"),
            canonical.replace("Orders", "orders"),
            canonical.replace("/Rules/rules/", "/rules/rules/"),
        ] {
            let (status, _) = exchange(
                &node,
                Method::PUT,
                path,
                Some(token(&audience, "manage", epoch() + 300)),
                b"<secret",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects);
        }
        let name = " K\u{03b1}%2F ";
        let body = definition(name, true);
        let scoped = token(&canonical, "manage", epoch() + 300);
        let (status, _) =
            exchange(&node, Method::PUT, path, Some(scoped.clone()), &body, false).await?;
        assert_eq!(status, StatusCode::CREATED);
        let path =
            "/Orders%252Fbranch/subscriptions/Rules/rules/%20K%CE%B1%252F%20?api-version=2024-05";
        let (status, body) = exchange(&node, Method::GET, path, Some(scoped), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        assert_entry(&body, name, true)?;
        assert_eq!(
            node.handle()
                .get_atom_rule(
                    NamespaceName::new("tenant")?,
                    EntityPath::new("Orders%2Fbranch")?,
                    SubscriptionName::new("Rules")?,
                    RuleName::new(name)?
                )
                .await?
                .unwrap()
                .name
                .as_str(),
            name
        );
        let (status, _) = exchange(
            &node,
            Method::GET,
            "/Orders%252Fbranch/Subscriptions/Rules?api-version=2024-05",
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "subscription named Rules remains a subscription member"
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn rule_auth_closed_xml_and_conditions_have_no_owner_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let before = node.snapshot()?;
        let effects = node.effects();
        for authorization in [
            None,
            Some(token(&format!("https://{HOST}"), "send", epoch() + 300)),
        ] {
            let (status, body) = exchange(
                &node,
                Method::PUT,
                &member("Keep"),
                authorization,
                b"<secret",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects);
        }
        let body = String::from_utf8(definition("Keep", true))?;
        for refused in [
            body.replace("<Name>Keep", "<Name>keep"),
            body.replace("TrueFilter", "CorrelationFilter"),
            body.replace("<SqlExpression>1=1", "<SqlExpression>1=0"),
            body.replace("<Parameters />", "<Parameters><Parameter /></Parameters>"),
            body.replace("</RuleDescription>", "<Action /></RuleDescription>"),
            body.replace(
                "</RuleDescription>",
                "<Unknown>secret</Unknown></RuleDescription>",
            ),
            body.replace("<Name>Keep", "<Name>bad@name"),
        ] {
            let (status, public) = exchange(
                &node,
                Method::PUT,
                &member("Keep"),
                Some(management_token()),
                refused.as_bytes(),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&public)?.contains("secret"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects);
        }
        for (method, path, body, conditional) in [
            (Method::PUT, member("Keep"), definition("Keep", true), true),
            (
                Method::GET,
                member("$Default"),
                definition("Keep", true),
                false,
            ),
            (
                Method::DELETE,
                member("$Default"),
                definition("Keep", true),
                false,
            ),
            (Method::GET, member("$Default"), Vec::new(), true),
            (Method::DELETE, member("$Default"), Vec::new(), true),
            (
                Method::GET,
                "/orders/Subscriptions/worker/Rules?api-version=2024-05&enrich=True".into(),
                Vec::new(),
                false,
            ),
            (Method::GET, collection(1001, 1), Vec::new(), false),
            (Method::GET, collection(0, 101), Vec::new(), false),
        ] {
            let (status, _) = exchange(
                &node,
                method,
                &path,
                Some(management_token()),
                &body,
                conditional,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects);
        }
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn rule_listing_never_hides_opaque_rules_outside_the_page<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        node.handle()
            .submit(
                NamespaceName::new("tenant")?,
                EntityPath::new("orders")?,
                CommandKind::CreateRuleWithAction {
                    subscription: SubscriptionName::new("worker")?,
                    name: RuleName::new("Opaque")?,
                    filter: RuleFilter::False,
                    action: SqlAction::with_semantic_version("REMOVE user.marker;", 1)?,
                },
            )
            .await?;
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        for path in [member("Opaque"), collection(0, 1), collection(1000, 1)] {
            let (status, body) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&body)?.contains("Opaque"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        let (status, _) = exchange(
            &node,
            Method::GET,
            &member("$Default"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            &member("Drop"),
            Some(management_token()),
            &definition("Drop", false),
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "other healthy action rows are opaque to ordinary rule mutations"
        );
        let (status, _) = exchange(
            &node,
            Method::DELETE,
            &member("Opaque"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let (status, feed) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&feed)?.matches("<entry").count(), 2);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn rule_native_count_limit_returns_service_busy_and_preserves_healthy_reads<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        for index in 0..domain::MAX_SUBSCRIPTION_RULES - 1 {
            let name = format!("Keep{index}");
            let (status, _) = exchange(
                &node,
                Method::PUT,
                &member(&name),
                Some(management_token()),
                &definition(&name, true),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);
        }
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, body) = exchange(
            &node,
            Method::PUT,
            &member("Overflow"),
            Some(management_token()),
            &definition("Overflow", true),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(std::str::from_utf8(&body)?.contains("ServiceBusy"));
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let (status, feed) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            std::str::from_utf8(&feed)?.matches("<entry").count(),
            domain::MAX_SUBSCRIPTION_RULES
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

fn sql_definition(name: &str, source: &str) -> Vec<u8> {
    String::from_utf8(definition(name, true))
        .unwrap()
        .replace("TrueFilter", "SqlFilter")
        .replace(
            "<SqlExpression>1=1</SqlExpression>",
            &format!(
                "<SqlExpression>{}</SqlExpression>",
                quick_xml::escape::escape(source)
            ),
        )
        .into_bytes()
}

fn assert_sql_image(
    before: &StoreSnapshot,
    after: &StoreSnapshot,
    batch: WriteBatch,
) -> TestResult {
    let expected = MemoryStore::default();
    expected.apply(
        before
            .entries()
            .iter()
            .fold(WriteBatch::default(), |batch, (key, value)| {
                batch.put(key.clone(), value.clone())
            }),
    )?;
    expected.apply(batch)?;
    assert_eq!(
        after,
        &expected.snapshot()?,
        "complete raw SQL mutation projection, not an independent domain oracle"
    );
    Ok(())
}

async fn sql_rule_crud_preserves_exact_source_and_retained_tls_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let child = topic.subscription(&subscription)?;
        node.handle()
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("sibling")?,
                    config: configured(),
                },
            )
            .await?;
        for sequence in 1..=3 {
            node.handle()
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::Send {
                        message_id: format!("sql-retained-{sequence}"),
                        body: vec![sequence as u8; 32],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )
                .await?;
            if sequence < 3 {
                let CommandOutcome::Received(Some(delivery)) = node
                    .handle()
                    .submit(
                        namespace.clone(),
                        child.clone(),
                        CommandKind::Receive {
                            mode: domain::ReceiveMode::PeekLock,
                            lock_duration_millis: None,
                            session: None,
                        },
                    )
                    .await?
                else {
                    panic!("trusted TLS SQL retention seed must be deliverable");
                };
                if sequence == 1 {
                    node.handle()
                        .submit(
                            namespace.clone(),
                            child.clone(),
                            CommandKind::DeadLetter {
                                sequence: delivery.sequence,
                                lock_token: delivery.lock.unwrap().token,
                                reason: "sql-retention".into(),
                                description: "trusted seed".into(),
                            },
                        )
                        .await?;
                }
            }
        }
        {
            let machine = StateMachine::new(node.store.as_ref().unwrap().inner.clone());
            let ready = machine
                .message(&namespace, &child, domain::SequenceNumber::new(3))?
                .unwrap();
            assert_eq!(ready.state, domain::MessageState::Ready);
            assert_eq!(ready.body, vec![3; 32]);
            assert_eq!(ready.expires_at, Some(Timestamp::from_millis(61_000)));
            let locked = machine
                .message(&namespace, &child, domain::SequenceNumber::new(2))?
                .unwrap();
            assert!(matches!(locked.state, domain::MessageState::Locked { .. }));
            let dead = machine
                .message(
                    &namespace,
                    &child.dead_letter_queue()?,
                    domain::SequenceNumber::new(1),
                )?
                .unwrap();
            assert_eq!(dead.body, vec![1; 32]);
            assert_eq!(
                dead.dead_letter.unwrap().reason,
                domain::DeadLetterReason::Application("sql-retention".into())
            );
        }
        let source = " \r\nuser.colour = 'Red & <x>' OR sys.Label IS NULL\r\n ";
        let name = RuleName::new("Sql")?;
        let filter = SqlFilter::new(source)?;
        let key = domain::keys::rule(&namespace, &topic, &subscription, &name);
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, created) = exchange(
            &node,
            Method::PUT,
            &member("Sql"),
            Some(management_token()),
            &sql_definition("Sql", source),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let text = std::str::from_utf8(&created)?;
        assert!(text.contains("<title>Sql</title>"));
        assert!(text.contains("<Name>Sql</Name>"));
        assert!(text.contains("i:type=\"SqlFilter\""));
        assert!(text.contains(&format!(
            "<SqlExpression>{}</SqlExpression>",
            quick_xml::escape::escape(source)
        )));
        assert!(text.contains("<Parameters></Parameters>"));
        for absent in ["<Action", "CreatedAt", "MessageCount", "SizeInBytes"] {
            assert!(!text.contains(absent), "{absent}");
        }
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        assert_sql_image(
            &before,
            &node.snapshot()?,
            WriteBatch::default()
                .put(
                    key.clone(),
                    domain::codec::encode(&domain::RuleDefinition {
                        name: name.clone(),
                        filter: RuleFilter::Sql(filter.clone()),
                        created_at: Timestamp::from_millis(1_000),
                        action: None,
                    })?,
                )
                .put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(1_000))?,
                ),
        )?;
        let retained = node.snapshot()?;
        let (status, read) = exchange(
            &node,
            Method::GET,
            &member("Sql"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(read, created);
        assert_eq!(
            node.handle()
                .get_atom_rule(
                    namespace.clone(),
                    topic.clone(),
                    subscription.clone(),
                    name.clone(),
                )
                .await?,
            Some(AtomRuleDefinition {
                name,
                filter: RuleFilter::Sql(filter),
                action: None,
            })
        );
        let (status, feed) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&feed)?.matches("<entry").count(), 2);
        assert!(std::str::from_utf8(&feed)?.contains("i:type=\"SqlFilter\""));
        assert_eq!(node.snapshot()?, retained);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            &member("Sql"),
            Some(management_token()),
            &sql_definition("Sql", "1=0"),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(node.snapshot()?, retained);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 2);
        let (status, body) = exchange(
            &node,
            Method::DELETE,
            &member("Sql"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 3);
        assert_sql_image(
            &retained,
            &node.snapshot()?,
            WriteBatch::default().delete(key).put(
                domain::keys::clock(),
                domain::codec::encode(&Timestamp::from_millis(1_000))?,
            ),
        )?;
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn sql_wire_refusals_and_stored_compile_health_keep_http_priorities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let token_limit = format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS));
        let too_wide = "a".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1);
        let before = node.snapshot()?;
        let effects = node.effects();
        for source in [
            "secret =",
            "lower(name)",
            token_limit.as_str(),
            too_wide.as_str(),
        ] {
            let (status, body) = exchange(
                &node,
                Method::PUT,
                &member("$Default"),
                Some(management_token()),
                &sql_definition("$Default", source),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(
                node.effects(),
                effects,
                "invalid wire SQL never invokes BrokerHandle, even for a duplicate"
            );
        }
        let valid = String::from_utf8(sql_definition("Sql", "1=1"))?;
        for refused in [
            valid.replace("<Parameters />", "<Parameters><Parameter /></Parameters>"),
            valid.replace("</RuleDescription>", "<Action /></RuleDescription>"),
            valid.replace("SqlFilter", "CorrelationFilter"),
        ] {
            let (status, _) = exchange(
                &node,
                Method::PUT,
                &member("Sql"),
                Some(management_token()),
                refused.as_bytes(),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects);
        }
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let corrupt = domain::keys::rule(
            &namespace,
            &topic,
            &subscription,
            &RuleName::new("corrupt")?,
        );
        let filter =
            domain::codec::decode::<SqlFilter>(&domain::codec::encode(&(1_u32, "secret ="))?)?;
        node.store
            .as_ref()
            .unwrap()
            .inner
            .apply(WriteBatch::default().put(
                corrupt,
                domain::codec::encode(&domain::RuleDefinition {
                    name: RuleName::new("corrupt")?,
                    filter: RuleFilter::Sql(filter),
                    created_at: Timestamp::from_millis(1_000),
                    action: None,
                })?,
            ))?;
        let stored = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        for path in [member("$Default"), member("Absent"), collection(1_000, 1)] {
            let (status, body) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert_eq!(node.snapshot()?, stored);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        for (method, body) in [
            (Method::PUT, sql_definition("$Default", "1=1")),
            (Method::DELETE, Vec::new()),
        ] {
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, public) = exchange(
                &node,
                method,
                &member("$Default"),
                Some(management_token()),
                &body,
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!std::str::from_utf8(&public)?.contains("secret"));
            assert_eq!(node.snapshot()?, stored);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        }
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn correlation_rule_crud_preserves_typed_source_and_retained_tls_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let child = topic.subscription(&subscription)?;
        node.handle()
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("sibling")?,
                    config: configured(),
                },
            )
            .await?;
        for sequence in 1..=3 {
            node.handle()
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::Send {
                        message_id: format!("correlation-retained-{sequence}"),
                        body: vec![sequence as u8; 32],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )
                .await?;
            if sequence < 3 {
                let CommandOutcome::Received(Some(delivery)) = node
                    .handle()
                    .submit(
                        namespace.clone(),
                        child.clone(),
                        CommandKind::Receive {
                            mode: domain::ReceiveMode::PeekLock,
                            lock_duration_millis: None,
                            session: None,
                        },
                    )
                    .await?
                else {
                    panic!("trusted TLS correlation retention seed must be deliverable");
                };
                if sequence == 1 {
                    node.handle()
                        .submit(
                            namespace.clone(),
                            child.clone(),
                            CommandKind::DeadLetter {
                                sequence: delivery.sequence,
                                lock_token: delivery.lock.unwrap().token,
                                reason: "correlation-retention".into(),
                                description: "trusted seed".into(),
                            },
                        )
                        .await?;
                }
            }
        }
        {
            let machine = StateMachine::new(node.store.as_ref().unwrap().inner.clone());
            let ready = machine
                .message(&namespace, &child, domain::SequenceNumber::new(3))?
                .unwrap();
            assert_eq!(ready.state, domain::MessageState::Ready);
            assert_eq!(ready.body, vec![3; 32]);
            assert_eq!(ready.expires_at, Some(Timestamp::from_millis(61_000)));
            let locked = machine
                .message(&namespace, &child, domain::SequenceNumber::new(2))?
                .unwrap();
            assert!(matches!(locked.state, domain::MessageState::Locked { .. }));
            let dead = machine
                .message(
                    &namespace,
                    &child.dead_letter_queue()?,
                    domain::SequenceNumber::new(1),
                )?
                .unwrap();
            assert_eq!(dead.body, vec![1; 32]);
            assert_eq!(
                dead.dead_letter.unwrap().reason,
                domain::DeadLetterReason::Application("correlation-retention".into())
            );
        }
        let (fields, properties, filter) = correlation_wire();
        let name = RuleName::new("Correlation")?;
        let key = domain::keys::rule(&namespace, &topic, &subscription, &name);
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, created) = exchange(
            &node,
            Method::PUT,
            &member("Correlation"),
            Some(management_token()),
            &correlation_definition("Correlation", &fields, &properties),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let text = std::str::from_utf8(&created)?;
        for required in [
            "<title>Correlation</title>",
            "<Name>Correlation</Name>",
            "i:type=\"CorrelationFilter\"",
            "<CorrelationId></CorrelationId>",
            "<SessionId></SessionId>",
            "<Label>Subject&#13;\n&amp;&lt;&gt;</Label>",
            "<Properties>",
            "l28:dateTime",
            "1970-01-01T00:00:00.123Z",
            "i:type=\"l28:boolean\">true</Value>",
            "i:type=\"l28:int\">-2147483648</Value>",
            "i:type=\"l28:long\">9223372036854775807</Value>",
            "i:type=\"l28:double\">-0e0</Value>",
            "i:type=\"l28:double\">INF</Value>",
            "i:type=\"l28:double\">-INF</Value>",
        ] {
            assert!(text.contains(required), "{required}");
        }
        for absent in [
            "<SqlExpression",
            "<Parameters",
            "<Action",
            "CreatedAt",
            "MessageCount",
            "SizeInBytes",
        ] {
            assert!(!text.contains(absent), "{absent}");
        }
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let rule = domain::RuleDefinition {
            name: name.clone(),
            filter: RuleFilter::Correlation(filter.clone()),
            created_at: Timestamp::from_millis(1_000),
            action: None,
        };
        assert_sql_image(
            &before,
            &node.snapshot()?,
            WriteBatch::default()
                .put(key.clone(), domain::codec::encode(&rule)?)
                .put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(1_000))?,
                ),
        )?;
        let retained = node.snapshot()?;
        let (status, read) = exchange(
            &node,
            Method::GET,
            &member("Correlation"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(read, created);
        assert_eq!(
            node.handle()
                .get_atom_rule(
                    namespace.clone(),
                    topic.clone(),
                    subscription.clone(),
                    name.clone()
                )
                .await?,
            Some(AtomRuleDefinition {
                name: name.clone(),
                filter: RuleFilter::Correlation(filter),
                action: None,
            })
        );
        let (status, feed) = exchange(
            &node,
            Method::GET,
            &collection(0, 100),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let feed = std::str::from_utf8(&feed)?;
        assert_eq!(feed.matches("<entry").count(), 2);
        assert!(feed.contains(text));
        let (status, page) = exchange(
            &node,
            Method::GET,
            &collection(1, 1),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&page)?.matches("<entry").count(), 1);
        assert!(std::str::from_utf8(&page)?.contains(text));
        assert_eq!(node.snapshot()?, retained);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            &member("Correlation"),
            Some(management_token()),
            &correlation_definition("Correlation", "", ""),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(node.snapshot()?, retained);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 2);
        let (status, body) = exchange(
            &node,
            Method::DELETE,
            &member("Correlation"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 3);
        assert_sql_image(
            &retained,
            &node.snapshot()?,
            WriteBatch::default().delete(key).put(
                domain::keys::clock(),
                domain::codec::encode(&Timestamp::from_millis(1_000))?,
            ),
        )?;
        let before = node.snapshot()?;
        let (status, body) = exchange(
            &node,
            Method::PUT,
            &member("EmptyCorrelation"),
            Some(management_token()),
            &correlation_definition("EmptyCorrelation", "", ""),
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let body = std::str::from_utf8(&body)?;
        assert!(body.contains("i:type=\"CorrelationFilter\""));
        assert!(body.contains("<Properties></Properties>"));
        assert!(!body.contains("SqlExpression"));
        assert_sql_image(
            &before,
            &node.snapshot()?,
            WriteBatch::default()
                .put(
                    domain::keys::rule(
                        &namespace,
                        &topic,
                        &subscription,
                        &RuleName::new("EmptyCorrelation")?,
                    ),
                    domain::codec::encode(&domain::RuleDefinition {
                        name: RuleName::new("EmptyCorrelation")?,
                        filter: RuleFilter::Correlation(CorrelationFilter::default()),
                        created_at: Timestamp::from_millis(1_000),
                        action: None,
                    })?,
                )
                .put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(1_000))?,
                ),
        )?;
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 4);
        for (index, &(left, right)) in DISTINCT_CORRELATION_KEYS.iter().enumerate() {
            let name = RuleName::new(format!("Keys-{index}"))?;
            let key = domain::keys::rule(&namespace, &topic, &subscription, &name);
            let filter = correlation_keys(left, right);
            let properties = format!(
                "{}{}",
                correlation_property(left, "int", "1"),
                correlation_property(right, "long", "2")
            );
            let before = node.snapshot()?;
            let clocks = node.clock.0.load(Ordering::SeqCst);
            let (status, created) = exchange(
                &node,
                Method::PUT,
                &member(name.as_str()),
                Some(management_token()),
                &correlation_definition(name.as_str(), "", &properties),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);
            let text = std::str::from_utf8(&created)?;
            assert!(text.contains("i:type=\"CorrelationFilter\""));
            for property in [left, right] {
                assert!(text.contains(&format!(
                    "<Key>{}</Key>",
                    quick_xml::escape::escape(property)
                )));
            }
            assert!(text.contains("i:type=\"l28:int\">1</Value>"));
            assert!(text.contains("i:type=\"l28:long\">2</Value>"));
            assert_sql_image(
                &before,
                &node.snapshot()?,
                WriteBatch::default()
                    .put(
                        key.clone(),
                        domain::codec::encode(&domain::RuleDefinition {
                            name: name.clone(),
                            filter: RuleFilter::Correlation(filter.clone()),
                            created_at: Timestamp::from_millis(1_000),
                            action: None,
                        })?,
                    )
                    .put(
                        domain::keys::clock(),
                        domain::codec::encode(&Timestamp::from_millis(1_000))?,
                    ),
            )?;
            let retained = node.snapshot()?;
            let (status, read) = exchange(
                &node,
                Method::GET,
                &member(name.as_str()),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(read, created);
            assert_eq!(
                node.handle()
                    .get_atom_rule(
                        namespace.clone(),
                        topic.clone(),
                        subscription.clone(),
                        name.clone()
                    )
                    .await?,
                Some(AtomRuleDefinition {
                    name: name.clone(),
                    filter: RuleFilter::Correlation(filter),
                    action: None,
                })
            );
            let (status, feed) = exchange(
                &node,
                Method::GET,
                &collection(0, 100),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            let feed = std::str::from_utf8(&feed)?;
            assert_eq!(feed.matches("<entry").count(), 3);
            assert!(feed.contains(text));
            assert_eq!(node.snapshot()?, retained);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clocks + 1);
            let (status, deleted) = exchange(
                &node,
                Method::DELETE,
                &member(name.as_str()),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert!(deleted.is_empty());
            assert_sql_image(
                &retained,
                &node.snapshot()?,
                WriteBatch::default().delete(key).put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(1_000))?,
                ),
            )?;
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clocks + 2);
        }
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn correlation_wire_refusals_and_stored_health_keep_http_priorities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let before = node.snapshot()?;
        let effects = node.effects();
        let valid = String::from_utf8(correlation_definition(
            "$Default",
            "",
            &correlation_property("value", "string", "secret"),
        ))?;
        let mut refused = vec![
            valid.replace("s:string", "s:duration"),
            valid.replace("http://www.w3.org/2001/XMLSchema\"", "urn:foreign\""),
            valid.replace("i:type=\"s:string\"", "i:type=\"s:unsignedInt\""),
            valid.replace("</RuleDescription>", "<Action /></RuleDescription>"),
            String::from_utf8(correlation_definition(
                "$Default",
                "<Label>first</Label><Label>second</Label>",
                "",
            ))?,
            String::from_utf8(correlation_definition(
                "$Default",
                "",
                &format!(
                    "{}{}",
                    correlation_property("duplicate", "int", "1"),
                    correlation_property("duplicate", "long", "2")
                ),
            ))?,
        ];
        for (kind, text) in [
            ("double", "NaN"),
            ("dateTime", "1970-01-01T00:00:00.0000001Z"),
            ("dateTime", "1970-01-01T00:00:00.123"),
            ("dateTime", "9999-12-31T23:59:59.999-00:01"),
            ("dateTime", "0000-12-31T23:59:59.999-00:01"),
            ("dateTime", "1970-01-01T00:00:60Z"),
            ("dateTime", "1970-01-01T00:00:00+14:01"),
            ("int", "2147483648"),
            ("boolean", "True"),
        ] {
            refused.push(String::from_utf8(correlation_definition(
                "$Default",
                "",
                &correlation_property("value", kind, text),
            ))?);
        }
        for &(left, right) in COLLIDING_CORRELATION_KEYS {
            refused.push(String::from_utf8(correlation_definition(
                "$Default",
                "",
                &format!(
                    "{}{}",
                    correlation_property(left, "int", "1"),
                    correlation_property(right, "long", "2")
                ),
            ))?);
        }
        let conditions: String = (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
            .map(|index| correlation_property(&format!("property-{index}"), "int", "1"))
            .collect();
        refused.push(String::from_utf8(correlation_definition(
            "$Default",
            "<CorrelationId>counts-too</CorrelationId>",
            &conditions,
        ))?);
        for request in refused {
            let (status, body) = exchange(
                &node,
                Method::PUT,
                &member("$Default"),
                Some(management_token()),
                request.as_bytes(),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(
                node.effects(),
                effects,
                "invalid correlation XML must precede duplicate owner work"
            );
        }
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let opaque = RuleName::new("native-uuid")?;
        node.handle()
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateRule {
                    subscription: subscription.clone(),
                    name: opaque.clone(),
                    filter: RuleFilter::Correlation(CorrelationFilter {
                        properties: [("value".into(), MessageValue::Uuid([1; 16]))]
                            .into_iter()
                            .collect(),
                        ..CorrelationFilter::default()
                    }),
                },
            )
            .await?;
        for (index, &(left, right)) in COLLIDING_CORRELATION_KEYS.iter().enumerate() {
            node.handle()
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::CreateRule {
                        subscription: subscription.clone(),
                        name: RuleName::new(format!("collision-{index}"))?,
                        filter: RuleFilter::Correlation(correlation_keys(left, right)),
                    },
                )
                .await?;
        }
        assert!(
            StateMachine::new(node.store.as_ref().unwrap().inner.clone())
                .rules(&namespace, &topic, &subscription)
                .is_ok()
        );
        let healthy = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, _) = exchange(
            &node,
            Method::GET,
            &member("$Default"),
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        for path in [
            member("native-uuid"),
            collection(0, 1),
            collection(1_000, 1),
        ]
        .into_iter()
        .chain(
            (0..COLLIDING_CORRELATION_KEYS.len())
                .map(|index| member(&format!("collision-{index}"))),
        ) {
            let (status, _) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(node.snapshot()?, healthy);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        let corrupt = domain::keys::rule(
            &namespace,
            &topic,
            &subscription,
            &RuleName::new("corrupt")?,
        );
        node.store.as_ref().unwrap().inner.apply(
            WriteBatch::default().put(
                corrupt,
                domain::codec::encode(&domain::RuleDefinition {
                    name: RuleName::new("corrupt")?,
                    filter: RuleFilter::Correlation(CorrelationFilter {
                        correlation_id: Some("system-counts-too".into()),
                        properties: (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
                            .map(|index| {
                                (format!("property-{index}"), MessageValue::Int(index as i32))
                            })
                            .collect(),
                        ..CorrelationFilter::default()
                    }),
                    created_at: Timestamp::from_millis(1_000),
                    action: None,
                })?,
            ),
        )?;
        let stored = node.snapshot()?;
        for path in [member("$Default"), member("Absent"), collection(1_000, 1)] {
            let (status, body) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!std::str::from_utf8(&body)?.contains("system-counts-too"));
            assert_eq!(node.snapshot()?, stored);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        for (method, body) in [
            (Method::PUT, correlation_definition("$Default", "", "")),
            (Method::DELETE, Vec::new()),
        ] {
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, body) = exchange(
                &node,
                method,
                &member("$Default"),
                Some(management_token()),
                &body,
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!std::str::from_utf8(&body)?.contains("system-counts-too"));
            assert_eq!(node.snapshot()?, stored);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        }
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

const SQL_ACTION_SOURCE: &str = " \r\nREMOVE user.[ Drop ]; REMOVE Plain; SET user.[Text-Case] = ' Red & <\u{03BB}> ''quoted'' \r\n'; SET user.flag = TRUE; SET user.minimum = -9223372036854775808; SET user.maximum = +9223372036854775807; ";

fn with_sql_action(body: Vec<u8>, source: &str) -> Vec<u8> {
    let source = quick_xml::escape::escape(source);
    String::from_utf8(body).unwrap().replace("</RuleDescription>", &format!(
        r#"<Action xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="SqlRuleAction"><SqlExpression>{source}</SqlExpression><Parameters /></Action></RuleDescription>"#,
    )).into_bytes()
}

fn action_dto(name: &str, filter: RuleFilter, action: &SqlAction) -> AtomRuleDefinition {
    AtomRuleDefinition {
        name: RuleName::new(name).unwrap(),
        filter,
        action: Some(action.clone()),
    }
}

fn action_put_batch(
    namespace: &NamespaceName,
    topic: &EntityPath,
    subscription: &SubscriptionName,
    value: &AtomRuleDefinition,
) -> TestResult<WriteBatch> {
    Ok(WriteBatch::default()
        .put(
            domain::keys::rule(namespace, topic, subscription, &value.name),
            domain::codec::encode(&domain::RuleDefinition {
                name: value.name.clone(),
                filter: value.filter.clone(),
                created_at: Timestamp::from_millis(1_000),
                action: value.action.clone(),
            })?,
        )
        .put(
            domain::keys::clock(),
            domain::codec::encode(&Timestamp::from_millis(1_000))?,
        ))
}

async fn sql_action_rule_crud_preserves_typed_source_and_retained_tls_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    use protocol_amqp::Broker as _;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let child = topic.subscription(&subscription)?;
        let native_action = SqlAction::new("REMOVE user.marker;")?;
        node.handle()
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateRuleWithAction {
                    subscription: subscription.clone(),
                    name: RuleName::new("native-supported")?,
                    filter: RuleFilter::False,
                    action: native_action.clone(),
                },
            )
            .await?;
        node.handle()
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("sibling")?,
                    config: configured(),
                },
            )
            .await?;
        for sequence in 1..=3 {
            node.handle()
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::Send {
                        message_id: format!("action-retained-{sequence}"),
                        body: vec![sequence as u8; 32],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )
                .await?;
            if sequence < 3 {
                let CommandOutcome::Received(Some(delivery)) = node
                    .handle()
                    .submit(
                        namespace.clone(),
                        child.clone(),
                        CommandKind::Receive {
                            mode: domain::ReceiveMode::PeekLock,
                            lock_duration_millis: None,
                            session: None,
                        },
                    )
                    .await?
                else {
                    panic!("trusted TLS action retention seed must be deliverable");
                };
                if sequence == 1 {
                    node.handle()
                        .submit(
                            namespace.clone(),
                            child.clone(),
                            CommandKind::DeadLetter {
                                sequence: delivery.sequence,
                                lock_token: delivery.lock.unwrap().token,
                                reason: "action-retention".into(),
                                description: "trusted seed".into(),
                            },
                        )
                        .await?;
                }
            }
        }
        let shadow = child.dead_letter_queue()?;
        {
            let machine = StateMachine::new(node.store.as_ref().unwrap().inner.clone());
            let ready = machine
                .message(&namespace, &child, domain::SequenceNumber::new(3))?
                .unwrap();
            assert_eq!(ready.state, domain::MessageState::Ready);
            assert_eq!(ready.body, vec![3; 32]);
            assert_eq!(ready.expires_at, Some(Timestamp::from_millis(61_000)));
            let locked = machine
                .message(&namespace, &child, domain::SequenceNumber::new(2))?
                .unwrap();
            assert!(matches!(locked.state, domain::MessageState::Locked { .. }));
            let dead = machine
                .message(&namespace, &shadow, domain::SequenceNumber::new(1))?
                .unwrap();
            assert_eq!(dead.body, vec![1; 32]);
            assert_eq!(
                dead.dead_letter.unwrap().reason,
                domain::DeadLetterReason::Application("action-retention".into())
            );
        }
        let handle = node.handle();
        let mut child_wait = Box::pin(handle.deliverable(&namespace, &child));
        let mut shadow_wait = Box::pin(handle.deliverable(&namespace, &shadow));
        poll_fn(|cx| {
            assert!(child_wait.as_mut().poll(cx).is_pending());
            assert!(shadow_wait.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let action = SqlAction::new(SQL_ACTION_SOURCE)?;
        let (fields, properties, correlation) = correlation_wire();
        let shapes = [
            (RuleFilter::True, definition("Action-0", true)),
            (RuleFilter::False, definition("Action-1", false)),
            (
                RuleFilter::Sql(SqlFilter::new(" 1=1 ")?),
                sql_definition("Action-2", " 1=1 "),
            ),
            (
                RuleFilter::Correlation(correlation),
                correlation_definition("Action-3", &fields, &properties),
            ),
        ];
        for (index, (filter, body)) in shapes.into_iter().enumerate() {
            let name = format!("Action-{index}");
            let value = action_dto(&name, filter, &action);
            let key = domain::keys::rule(&namespace, &topic, &subscription, &value.name);
            let mut wire = with_sql_action(body, SQL_ACTION_SOURCE);
            if index == 1 {
                wire = String::from_utf8(wire)?
                    .replace("<Parameters /></Action>", "</Action>")
                    .into_bytes();
            }
            let before = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            let (status, created) = exchange(
                &node,
                Method::PUT,
                &member(&name),
                Some(management_token()),
                &wire,
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);
            let text = std::str::from_utf8(&created)?;
            assert!(text.contains("i:type=\"SqlRuleAction\""));
            assert!(text.contains(&format!(
                "<SqlExpression>{}</SqlExpression>",
                quick_xml::escape::escape(SQL_ACTION_SOURCE)
            )));
            assert!(!text.contains("CreatedAt"));
            assert_sql_image(
                &before,
                &node.snapshot()?,
                action_put_batch(&namespace, &topic, &subscription, &value)?,
            )?;
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
            let retained = node.snapshot()?;
            let (status, read) = exchange(
                &node,
                Method::GET,
                &member(&name),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(read, created);
            assert_eq!(
                node.handle()
                    .get_atom_rule(
                        namespace.clone(),
                        topic.clone(),
                        subscription.clone(),
                        value.name.clone()
                    )
                    .await?,
                Some(value.clone())
            );
            let (status, feed) = exchange(
                &node,
                Method::GET,
                &collection(0, 100),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            let feed = std::str::from_utf8(&feed)?;
            assert_eq!(feed.matches("<entry").count(), 3);
            assert_eq!(feed.matches("i:type=\"SqlRuleAction\"").count(), 2);
            assert!(feed.contains(text));
            let mut listed = vec![
                AtomRuleDefinition {
                    name: RuleName::new("$Default")?,
                    filter: RuleFilter::True,
                    action: None,
                },
                value.clone(),
                action_dto("native-supported", RuleFilter::False, &native_action),
            ];
            listed.sort_by(|left, right| left.name.cmp(&right.name));
            assert_eq!(
                node.handle()
                    .list_atom_rules(
                        namespace.clone(),
                        topic.clone(),
                        subscription.clone(),
                        0,
                        100
                    )
                    .await?,
                listed
            );
            let (status, page) = exchange(
                &node,
                Method::GET,
                &collection(1_000, 1),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(std::str::from_utf8(&page)?.matches("<entry").count(), 0);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
            assert_eq!(node.snapshot()?, retained);
            let (status, _) = exchange(
                &node,
                Method::PUT,
                &member(&name),
                Some(management_token()),
                &wire,
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 2);
            assert_eq!(node.snapshot()?, retained);
            let (status, deleted) = exchange(
                &node,
                Method::DELETE,
                &member(&name),
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert!(deleted.is_empty());
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 3);
            assert_sql_image(
                &retained,
                &node.snapshot()?,
                WriteBatch::default().delete(key).put(
                    domain::keys::clock(),
                    domain::codec::encode(&Timestamp::from_millis(1_000))?,
                ),
            )?;
            assert_eq!(node.snapshot()?, before);
        }
        poll_fn(|cx| {
            assert!(child_wait.as_mut().poll(cx).is_pending());
            assert!(shadow_wait.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(child_wait);
        drop(shadow_wait);
        drop(handle);
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn sql_action_wire_refusals_and_stored_health_keep_http_priorities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed(&node, "orders", "worker").await?;
        let before = node.snapshot()?;
        let effects = node.effects();
        let token_limit = format!("{}REMOVE user.value;", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS));
        let statement_limit = "REMOVE[x];".repeat(33);
        let source_limit = format!("SET user.value = '{}';", "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS));
        let invalid_sources = [
            "", "REMOVE", "SET user.value =", "SET user.value = NULL;",
            "SET user.value = 1.0;", "SET user.value = user.other;", "SET user.value = 1 + 2;",
            "SET sys.message_id = 'secret';", "SET user.value = 9223372036854775808;",
            "SET user.value = -9223372036854775809;", token_limit.as_str(),
            statement_limit.as_str(), source_limit.as_str(),
        ];
        let mut refused = invalid_sources.into_iter().map(|source| {
            assert!(SqlAction::new(source).is_err());
            with_sql_action(definition("$Default", true), source)
        }).collect::<Vec<_>>();
        let valid = String::from_utf8(with_sql_action(definition("$Default", true), "REMOVE user.secret;"))?;
        for request in [
            valid.replace("i:type=\"SqlRuleAction\"", ""),
            valid.replace("i:type=\"SqlRuleAction\"", "i:type=\"UnknownAction\""),
            valid.replace("i:type=\"SqlRuleAction\"", "i:type=\"sb:SqlRuleAction\""),
            valid.replace("<Parameters /></Action>", "<Parameters>secret</Parameters></Action>"),
            valid.replace("<Parameters /></Action>", "<Parameters><Parameter>secret</Parameter></Parameters></Action>"),
            valid.replace("<Parameters /></Action>", "<Parameters /><Parameters /></Action>"),
            valid.replace("<SqlExpression>REMOVE user.secret;</SqlExpression>", ""),
            valid.replace("<SqlExpression>REMOVE user.secret;</SqlExpression>", "<SqlExpression>REMOVE user.secret;</SqlExpression><SqlExpression>REMOVE user.secret;</SqlExpression>"),
            valid.replace("</Action>", "</Action><Action />"),
            valid.replace("<Action xmlns:i=", "<Action xmlns=\"urn:foreign\" xmlns:i="),
            valid.replace("<Parameters /></Action>", "<SemanticVersion>2</SemanticVersion></Action>"),
        ] {
            refused.push(request.into_bytes());
        }
        for request in &refused {
            let (status, body) = exchange(&node, Method::PUT, &member("$Default"), Some(management_token()), request, false).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert_eq!(node.snapshot()?, before);
            assert_eq!(node.effects(), effects, "invalid action XML must precede duplicate owner work");
        }
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let subscription = SubscriptionName::new("worker")?;
        let supported = action_dto("supported", RuleFilter::Sql(SqlFilter::new(" 1=0 ")?), &SqlAction::new(SQL_ACTION_SOURCE)?);
        let request = with_sql_action(sql_definition("supported", " 1=0 "), SQL_ACTION_SOURCE);
        let before = node.snapshot()?;
        let (status, created) = exchange(&node, Method::PUT, &member("supported"), Some(management_token()), &request, false).await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_sql_image(&before, &node.snapshot()?, action_put_batch(&namespace, &topic, &subscription, &supported)?)?;
        for (name, action) in [
            ("v1-remove", SqlAction::with_semantic_version("REMOVE user.secret;", 1)?),
            ("xml-illegal-action", SqlAction::new("SET user.value = '\u{FFFE}';")?),
        ] {
            assert_eq!(node.handle().submit(namespace.clone(), topic.clone(), CommandKind::CreateRuleWithAction {
                subscription: subscription.clone(), name: RuleName::new(name)?,
                filter: RuleFilter::False, action,
            }).await?, CommandOutcome::RuleCreated);
        }
        assert!(StateMachine::new(node.store.as_ref().unwrap().inner.clone()).rules(&namespace, &topic, &subscription).is_ok());
        let healthy = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        for (name, expected_status) in [("$Default", StatusCode::OK), ("supported", StatusCode::OK), ("absent", StatusCode::NOT_FOUND)] {
            let (status, body) = exchange(&node, Method::GET, &member(name), Some(management_token()), b"", false).await?;
            assert_eq!(status, expected_status);
            if name == "supported" {
                assert_eq!(body, created);
                assert_eq!(node.handle().get_atom_rule(namespace.clone(), topic.clone(), subscription.clone(), RuleName::new(name)?).await?, Some(supported.clone()));
            }
            assert_eq!(node.snapshot()?, healthy);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        for path in [member("v1-remove"), member("xml-illegal-action"), collection(0, 1), collection(1_000, 1)] {
            let (status, body) = exchange(&node, Method::GET, &path, Some(management_token()), b"", false).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&body)?.contains("secret"));
            assert!(!std::str::from_utf8(&body)?.contains("SqlRuleAction"));
            assert_eq!(node.snapshot()?, healthy);
            assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
        }
        let transient = action_dto("transient", RuleFilter::True, &SqlAction::new("SET user.flag = FALSE;")?);
        let (status, _) = exchange(&node, Method::PUT, &member("transient"), Some(management_token()), &with_sql_action(definition("transient", true), "SET user.flag = FALSE;"), false).await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        let retained = node.snapshot()?;
        assert_sql_image(&healthy, &retained, action_put_batch(&namespace, &topic, &subscription, &transient)?)?;
        let (status, _) = exchange(&node, Method::DELETE, &member("transient"), Some(management_token()), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 2);
        assert_sql_image(&retained, &node.snapshot()?, WriteBatch::default().delete(
            domain::keys::rule(&namespace, &topic, &subscription, &transient.name),
        ).put(domain::keys::clock(), domain::codec::encode(&Timestamp::from_millis(1_000))?))?;
        assert_eq!(node.snapshot()?, healthy);
        let (status, _) = exchange(&node, Method::DELETE, &member("v1-remove"), Some(management_token()), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 3);
        assert_sql_image(&healthy, &node.snapshot()?, WriteBatch::default().delete(
            domain::keys::rule(&namespace, &topic, &subscription, &RuleName::new("v1-remove")?),
        ).put(domain::keys::clock(), domain::codec::encode(&Timestamp::from_millis(1_000))?))?;
        let key = domain::keys::rule(&namespace, &topic, &subscription, &RuleName::new("corrupt-action")?);
        let mut corrupted = Vec::new();
        for source in ["secret =", "SET sys.message_id = 'secret';", "SET user.value = 1.0;"] {
            let action = domain::codec::decode::<SqlAction>(&domain::codec::encode(&(2_u32, source))?)?;
            assert!(SqlAction::new(action.expression()).is_err());
            corrupted.push(domain::codec::encode(&domain::RuleDefinition {
                name: RuleName::new("corrupt-action")?, filter: RuleFilter::False,
                created_at: Timestamp::from_millis(1_000), action: Some(action),
            })?);
        }
        // Postcard tuples retain the field layout while bypassing the typed version constructor.
        let unknown = domain::codec::encode(&(RuleName::new("corrupt-action")?, RuleFilter::False,
            Timestamp::from_millis(1_000), Some((3_u32, "REMOVE user.secret;"))))?;
        assert!(domain::RuleDefinition::decode(&unknown).is_err());
        corrupted.push(unknown);
        for bytes in corrupted {
            node.store.as_ref().unwrap().inner.apply(WriteBatch::default().put(key.clone(), bytes))?;
            assert!(StateMachine::new(node.store.as_ref().unwrap().inner.clone()).rules(&namespace, &topic, &subscription).is_err());
            let stored = node.snapshot()?;
            let clock = node.clock.0.load(Ordering::SeqCst);
            let effects = node.effects();
            let (status, _) = exchange(&node, Method::PUT, &member("$Default"), Some(management_token()), &refused[0], false).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(node.effects(), effects, "invalid desired action must precede stored health work");
            assert_eq!(node.snapshot()?, stored);
            for path in [member("$Default"), member("supported"), member("absent"), collection(0, 1), collection(1_000, 1)] {
                let (status, body) = exchange(&node, Method::GET, &path, Some(management_token()), b"", false).await?;
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert!(!std::str::from_utf8(&body)?.contains("secret"));
                assert!(!std::str::from_utf8(&body)?.contains("SqlRuleAction"));
                assert_eq!(node.snapshot()?, stored);
                assert_eq!(node.clock.0.load(Ordering::SeqCst), clock);
            }
            for (method, body) in [
                (Method::PUT, with_sql_action(definition("safe-action", false), "REMOVE user.value;")),
                (Method::DELETE, Vec::new()),
            ] {
                let name = if method == Method::PUT { "safe-action" } else { "$Default" };
                let clock = node.clock.0.load(Ordering::SeqCst);
                let (status, body) = exchange(&node, method, &member(name), Some(management_token()), &body, false).await?;
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert!(!std::str::from_utf8(&body)?.contains("secret"));
                assert_eq!(node.snapshot()?, stored);
                assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
            }
        }
        expected = Some(node.snapshot()?);
        Ok(())
    }).catch_unwind().await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

subscription_transport_backends! {
    rule_crud_empty_feed_and_default_recreation_reopen,
    rule_marker_authorization_preserves_literal_names,
    rule_auth_closed_xml_and_conditions_have_no_owner_effects,
    rule_listing_never_hides_opaque_rules_outside_the_page,
    rule_native_count_limit_returns_service_busy_and_preserves_healthy_reads,
    sql_rule_crud_preserves_exact_source_and_retained_tls_state,
    sql_wire_refusals_and_stored_compile_health_keep_http_priorities,
    correlation_rule_crud_preserves_typed_source_and_retained_tls_state,
    correlation_wire_refusals_and_stored_health_keep_http_priorities,
    sql_action_rule_crud_preserves_typed_source_and_retained_tls_state,
    sql_action_wire_refusals_and_stored_health_keep_http_priorities,
}
