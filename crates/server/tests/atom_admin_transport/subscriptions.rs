use super::*;

use domain::{Command, CommandOutcome, SubscriptionConfig, SubscriptionName, TopicConfig};

const DEFINITION: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT5S</LockDuration><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>PT60S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>false</DeadLetteringOnFilterEvaluationExceptions><DefaultRuleDescription><Filter xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><Parameters /></Filter><Name>$Default</Name></DefaultRuleDescription><MaxDeliveryCount>4</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><Status>Active</Status></SubscriptionDescription></content></entry>"#;

async fn seed_topic<P: StoreProvider>(node: &Node<P>, topic: &str) -> TestResult {
    assert_eq!(
        node.handle()
            .submit(
                NamespaceName::new("tenant")?,
                EntityPath::new(topic)?,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )
            .await?,
        CommandOutcome::TopicCreated
    );
    Ok(())
}

fn configured() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: 5_000,
        max_delivery_count: 4,
        default_time_to_live_millis: Some(60_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

async fn subscription_crud_returns_leaf_descriptions_and_reopens<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let token = management_token();
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let (status, created) = exchange(
            &node,
            Method::PUT,
            path,
            Some(token.clone()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let text = std::str::from_utf8(&created)?;
        assert!(text.contains("<title>worker</title>"));
        assert!(!text.contains("DefaultRuleDescription"));
        for absent in [
            "MaxSizeInMegabytes",
            "MaxMessageSizeInKilobytes",
            "MessageCount",
            "SizeInBytes",
            "CreatedAt",
        ] {
            assert!(!text.contains(absent));
        }
        let (status, read) =
            exchange(&node, Method::GET, path, Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(read, created);
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        let name = SubscriptionName::new("worker")?;
        assert_eq!(
            node.handle()
                .get_atom_subscription(namespace.clone(), topic.clone(), name.clone())
                .await?,
            Some(configured())
        );
        let oracle_store = MemoryStore::default();
        let oracle = StateMachine::new(oracle_store.clone());
        oracle.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?;
        oracle.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: configured(),
            },
        ))?;
        assert_eq!(
            node.snapshot()?,
            oracle_store.snapshot()?,
            "same-domain canonical replay, not an independent oracle"
        );
        let before = node.snapshot()?;
        let (status, _) = exchange(
            &node,
            Method::PUT,
            path,
            Some(token.clone()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(node.snapshot()?, before);
        let (status, deleted) =
            exchange(&node, Method::DELETE, path, Some(token.clone()), b"", false).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(deleted.is_empty());
        oracle.apply(&Command::new(
            namespace,
            topic,
            Timestamp::from_millis(1_000),
            CommandKind::DeleteEntity {
                target: domain::DeleteEntityTarget::Subscription { name },
            },
        ))?;
        assert_eq!(node.snapshot()?, oracle_store.snapshot()?);
        let (status, body) = exchange(&node, Method::GET, path, Some(token), b"", false).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(std::str::from_utf8(&body)?.contains("subscription"));
        expected = Some(node.snapshot()?);
        Ok(())
    })
    .catch_unwind()
    .await;
    let provider = node.finish(outcome).await?;
    assert_eq!(provider.open()?.snapshot()?, expected.unwrap());
    Ok(())
}

async fn marker_scope_mapping_preserves_literals_and_old_queue_paths<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "Orders%2Fbranch").await?;
        let canonical = format!("https://{HOST}/Orders%252Fbranch/subscriptions/worker");
        let alias = format!("https://{HOST}/Orders%252Fbranch/Subscriptions/worker");
        let path = "/Orders%252Fbranch/Subscriptions/worker?api-version=2024-05";
        let before = node.snapshot()?;
        let effects = node.effects();
        let (status, _) = exchange(
            &node,
            Method::PUT,
            path,
            Some(token(&alias, "manage", epoch() + 300)),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.effects(), effects);
        let scoped = token(&canonical, "manage", epoch() + 300);
        let (status, _) = exchange(
            &node,
            Method::PUT,
            path,
            Some(scoped.clone()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = exchange(
            &node,
            Method::GET,
            "/Orders%252Fbranch/subscriptions/worker?api-version=2024-05",
            Some(scoped),
            b"",
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            node.handle()
                .get_atom_subscription(
                    NamespaceName::new("tenant")?,
                    EntityPath::new("Orders%2Fbranch")?,
                    SubscriptionName::new("worker")?
                )
                .await?,
            Some(configured())
        );
        for primary in ["existing/Subscriptions", "other/subscriptions"] {
            let path = format!("/{primary}?api-version=2024-05");
            let (status, _) = exchange(
                &node,
                Method::PUT,
                &path,
                Some(management_token()),
                CREATE,
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::CREATED);
            let (status, _) = exchange(
                &node,
                Method::GET,
                &path,
                Some(management_token()),
                b"",
                false,
            )
            .await?;
            assert_eq!(
                status,
                StatusCode::OK,
                "no member means this remains an ordinary queue route"
            );
        }
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn subscription_auth_xml_and_unsupported_operations_have_no_mutations<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let before = node.snapshot()?;
        let effects = node.effects();
        for authorization in [
            None,
            Some(token(&format!("https://{HOST}"), "send", epoch() + 300)),
            Some(token(
                &format!("https://{HOST}/orders/Subscriptions/worker"),
                "manage",
                epoch() + 300,
            )),
        ] {
            let (status, body) = exchange(
                &node,
                Method::PUT,
                path,
                authorization,
                b"<secret producer-payload",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            let body = std::str::from_utf8(&body)?;
            assert!(!body.contains("secret"));
            assert!(!body.contains(KEY));
            assert!(!body.contains("InvalidXml"));
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let definition = std::str::from_utf8(DEFINITION)?;
        let refused = [
            definition.replace("<RequiresSession>false", "<RequiresSession>true"),
            definition.replace("<Name>$Default", "<Name>custom"),
            definition.replace("TrueFilter", "SqlFilter"),
            definition.replace("<SqlExpression>1=1", "<SqlExpression>1=0"),
            definition.replace("<Parameters />", "<Action />"),
            definition.replace("<LockDuration>PT5S", "<LockDuration>PT4.999S"),
            definition.replace("<Status>Active", "<Status>Disabled"),
            definition.replace(
                "</SubscriptionDescription>",
                "<MaxSizeInMegabytes>1</MaxSizeInMegabytes></SubscriptionDescription>",
            ),
        ];
        for body in refused {
            let (status, public) = exchange(
                &node,
                Method::PUT,
                path,
                Some(management_token()),
                body.as_bytes(),
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&public)?.contains(KEY));
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let (status, _) = exchange(
            &node,
            Method::PUT,
            path,
            Some(management_token()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let before = node.snapshot()?;
        let effects = node.effects();
        for (method, path, update) in [
            (
                Method::PUT,
                "/orders/Subscriptions/worker?api-version=2024-05",
                true,
            ),
            (
                Method::GET,
                "/orders/Subscriptions?api-version=2024-05&enrich=False&$skip=0&$top=100",
                false,
            ),
            (
                Method::GET,
                "/orders/Subscriptions/worker?api-version=2024-05&enrich=True",
                false,
            ),
            (
                Method::GET,
                "/orders/Subscriptions/worker/Rules/$Default?api-version=2024-05&enrich=True",
                false,
            ),
        ] {
            let (status, _) = exchange(
                &node,
                method,
                path,
                Some(management_token()),
                if update { DEFINITION } else { b"" },
                update,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

async fn subscription_tls_profile_and_topology_limits_keep_healthy_controls<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let (status, _) = exchange(
            &node,
            Method::PUT,
            path,
            Some(management_token()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        let before = node.snapshot()?;
        let effects = node.effects();
        for (trust, name) in [(false, "localhost"), (true, "wrong.example")] {
            let tls = connector(if trust { Some(&node.certificate) } else { None })?;
            let stream = timeout(DEADLINE, TcpStream::connect(node.address)).await??;
            let error = match timeout(
                DEADLINE,
                tls.connect(ServerName::try_from(name.to_owned())?, stream),
            )
            .await?
            {
                Err(error) => error,
                Ok(_) => return Err("invalid subscription TLS trust or name was accepted".into()),
            };
            let cause = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<rustls::Error>());
            if trust {
                assert!(matches!(
                    cause,
                    Some(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::NotValidForNameContext { .. }
                    ))
                ));
            } else {
                assert!(matches!(
                    cause,
                    Some(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::UnknownIssuer
                    ))
                ));
            }
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let (status, _) = exchange(
            &node,
            Method::GET,
            path,
            Some(management_token()),
            b"",
            false,
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "same fixture remains healthy after concrete TLS negatives"
        );
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("orders")?;
        for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS - 1 {
            node.handle()
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new(format!("other{index}"))?,
                        config: SubscriptionConfig::default(),
                    },
                )
                .await?;
        }
        let before = node.snapshot()?;
        let clock = node.clock.0.load(Ordering::SeqCst);
        let (status, body) = exchange(
            &node,
            Method::PUT,
            "/orders/Subscriptions/overflow?api-version=2024-05",
            Some(management_token()),
            DEFINITION,
            false,
        )
        .await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let body = std::str::from_utf8(&body)?;
        assert!(body.contains("ServiceBusy"));
        assert!(!body.contains("queue capacity"));
        assert_eq!(node.clock.0.load(Ordering::SeqCst), clock + 1);
        assert_eq!(node.snapshot()?, before);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

macro_rules! subscription_transport_backends {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::default()).await })+ }
        mod durable { $(#[tokio::test] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

subscription_transport_backends! {
    subscription_crud_returns_leaf_descriptions_and_reopens,
    marker_scope_mapping_preserves_literals_and_old_queue_paths,
    subscription_auth_xml_and_unsupported_operations_have_no_mutations,
    subscription_tls_profile_and_topology_limits_keep_healthy_controls,
}

#[path = "subscriptions/updates.rs"]
mod updates;

#[path = "subscriptions/rules.rs"]
mod rules;

#[path = "subscriptions/topic_mode.rs"]
mod topic_mode;
