use super::*;

use domain::{RuleFilter, RuleName, SqlAction, SqlFilter};
use server::AtomRuleDefinition;

fn definition(name: &str, keep: bool) -> Vec<u8> {
    let filter = if keep { "True" } else { "False" };
    let expression = if keep { "1=1" } else { "1=0" };
    format!(r#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="{filter}Filter"><SqlExpression>{expression}</SqlExpression><Parameters /></Filter><Name>{name}</Name></RuleDescription></content></entry>"#).into_bytes()
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
                filter: RuleFilter::False
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
                    action: SqlAction::new("SET user.marker = 'native';")?,
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

subscription_transport_backends! {
    rule_crud_empty_feed_and_default_recreation_reopen,
    rule_marker_authorization_preserves_literal_names,
    rule_auth_closed_xml_and_conditions_have_no_owner_effects,
    rule_listing_never_hides_opaque_rules_outside_the_page,
    rule_native_count_limit_returns_service_busy_and_preserves_healthy_reads,
    sql_rule_crud_preserves_exact_source_and_retained_tls_state,
    sql_wire_refusals_and_stored_compile_health_keep_http_priorities,
}
