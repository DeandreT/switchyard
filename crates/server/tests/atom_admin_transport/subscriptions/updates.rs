use super::*;

const UPDATE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT30S</LockDuration><MaxDeliveryCount>6</MaxDeliveryCount><DefaultMessageTimeToLive>PT90S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>false</DeadLetteringOnFilterEvaluationExceptions></SubscriptionDescription></content></entry>"#;
const RESET: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" /></content></entry>"#;

fn changed() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 6,
        default_time_to_live_millis: Some(90_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

async fn tls_full_update_resets_omissions_and_noop_keeps_snapshot<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut expected = None;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let canonical = format!("https://{HOST}/orders/subscriptions/worker");
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
        for (body, config) in [(UPDATE, changed()), (RESET, SubscriptionConfig::default())] {
            let (status, updated) =
                exchange(&node, Method::PUT, path, Some(scoped.clone()), body, true).await?;
            assert_eq!(status, StatusCode::OK);
            let text = std::str::from_utf8(&updated)?;
            assert!(text.contains("<title>worker</title>"));
            for absent in [
                "DefaultRuleDescription",
                "MaxSizeInMegabytes",
                "MaxMessageSizeInKilobytes",
                "MessageCount",
                "SizeInBytes",
                "CreatedAt",
            ] {
                assert!(!text.contains(absent));
            }
            assert_eq!(
                node.handle()
                    .get_atom_subscription(
                        NamespaceName::new("tenant")?,
                        EntityPath::new("orders")?,
                        SubscriptionName::new("worker")?,
                    )
                    .await?,
                Some(config)
            );
            let (status, read) = exchange(
                &node,
                Method::GET,
                "/orders/subscriptions/worker?api-version=2024-05",
                Some(scoped.clone()),
                b"",
                false,
            )
            .await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(read, updated);
            let before = node.snapshot()?;
            let (status, repeated) =
                exchange(&node, Method::PUT, path, Some(scoped.clone()), body, true).await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(repeated, updated);
            assert_eq!(node.snapshot()?, before);
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

async fn tls_update_auth_and_unsupported_rule_definitions_have_no_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let path = "/orders/Subscriptions/worker?api-version=2024-05";
        let (status, _) = exchange(&node, Method::PUT, path, Some(management_token()), DEFINITION, false).await?;
        assert_eq!(status, StatusCode::CREATED);
        let before = node.snapshot()?;
        let effects = node.effects();
        for authorization in [
            None,
            Some(token(&format!("https://{HOST}"), "send", epoch() + 300)),
            Some(token(&format!("https://{HOST}"), "unknown", epoch() + 300)),
            Some(token(&format!("https://{HOST}"), "manage", epoch() - 300)),
            Some(token(&format!("https://{HOST}/orders/Subscriptions/worker"), "manage", epoch() + 300)),
        ] {
            let (status, public) = exchange(&node, Method::PUT, path, authorization, b"<secret producer-payload", true).await?;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            let text = std::str::from_utf8(&public)?;
            assert!(!text.contains("secret"));
            assert!(!text.contains(KEY));
            assert!(!text.contains("InvalidXml"));
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let definition = std::str::from_utf8(UPDATE)?;
        let reset = std::str::from_utf8(RESET)?;
        for body in [
            std::str::from_utf8(DEFINITION)?.to_owned(),
            reset.replace(" />", "><DefaultRuleDescription /></SubscriptionDescription>"),
            reset.replace(" />", "><DefaultRuleDescription><Name>custom</Name></DefaultRuleDescription></SubscriptionDescription>"),
            definition.replace("</SubscriptionDescription>", "<RequiresSession>true</RequiresSession></SubscriptionDescription>"),
            definition.replace("<LockDuration>PT30S", "<LockDuration>PT4.999S"),
            definition.replace("<DefaultMessageTimeToLive>PT90S", "<DefaultMessageTimeToLive>PT0.999S"),
            definition.replace("<MaxDeliveryCount>6", "<MaxDeliveryCount>0"),
            definition.replace("</SubscriptionDescription>", "<MaxSizeInMegabytes>1</MaxSizeInMegabytes></SubscriptionDescription>"),
        ] {
            let (status, public) = exchange(&node, Method::PUT, path, Some(management_token()), body.as_bytes(), true).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(!std::str::from_utf8(&public)?.contains(KEY));
            assert_eq!(node.effects(), effects);
            assert_eq!(node.snapshot()?, before);
        }
        let (status, _) = exchange(&node, Method::PUT, path, Some(management_token()), UPDATE, true).await?;
        assert_eq!(status, StatusCode::OK);
        Ok(())
    }).catch_unwind().await;
    node.finish(outcome).await?;
    Ok(())
}

async fn tls_missing_update_never_creates_and_stamps_original_planner<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let outcome = AssertUnwindSafe(async {
        seed_topic(&node, "orders").await?;
        let before = node.snapshot()?;
        let effects = node.effects();
        for path in [
            "/orders/Subscriptions/missing?api-version=2024-05",
            "/missing/Subscriptions/worker?api-version=2024-05",
        ] {
            let reads = node.effects().1;
            let (status, _) = exchange(
                &node,
                Method::PUT,
                path,
                Some(management_token()),
                UPDATE,
                true,
            )
            .await?;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(node.effects().1, reads + 1);
            assert_eq!(node.snapshot()?, before);
        }
        assert!(node.effects().0 > effects.0);
        Ok(())
    })
    .catch_unwind()
    .await;
    node.finish(outcome).await?;
    Ok(())
}

subscription_transport_backends! {
    tls_full_update_resets_omissions_and_noop_keeps_snapshot,
    tls_update_auth_and_unsupported_rule_definitions_have_no_effects,
    tls_missing_update_never_creates_and_stamps_original_planner,
}
