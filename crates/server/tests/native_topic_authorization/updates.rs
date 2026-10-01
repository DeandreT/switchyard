use admin_api::v1::{SubscriptionConfiguration, TopicConfiguration, UpdateEntityRequest};

use super::*;

fn topic_patch(path: &str) -> UpdateEntityRequest {
    UpdateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        topic_config: Some(TopicConfiguration {
            max_message_bytes: Some(32_768),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn subscription_patch(path: &str) -> UpdateEntityRequest {
    UpdateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        subscription_config: Some(SubscriptionConfiguration {
            max_delivery_count: Some(7),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn exact_child_manage_updates_only_its_canonical_member<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let path = "Orders/subscriptions/Accounting";
    let node = Node::start(provider, Some(path))?;
    let token = sas(path, "manage", EXPIRY);
    node.clock.set(11_000);
    let before_update = node
        .service
        .get_entity(authorized(get(path), &token))
        .await?
        .into_inner();
    let updated = node
        .service
        .update_entity(authorized(
            subscription_patch("Orders/SUBSCRIPTIONS/Accounting"),
            &token,
        ))
        .await?
        .into_inner();
    assert_eq!(updated.path, path);
    assert_eq!(updated.kind, EntityKind::Subscription as i32);
    assert_ne!(updated, before_update);
    let config = updated.subscription_config.as_ref().expect("subscription");
    assert_eq!(config.max_delivery_count, Some(7));
    assert_eq!(
        config.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    node.clock.set(0);
    let before = node.guard()?;
    assert_eq!(
        node.service
            .get_entity(authorized(get(path), &token))
            .await?
            .into_inner(),
        updated
    );
    node.unchanged(&before, true)?;
    for denied in [
        topic_patch("Orders"),
        topic_patch("Orders-old"),
        subscription_patch("Orders/subscriptions/Billing"),
        subscription_patch("orders/subscriptions/Accounting"),
        subscription_patch("Orders/subscriptions/accounting"),
        subscription_patch("Orders/subscriptions/Missing"),
        UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders-old".into(),
            ..Default::default()
        },
    ] {
        let before = node.guard()?;
        code(
            node.service.update_entity(authorized(denied, &token)).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    for (audience, expected) in [
        ("Orders/Subscriptions/Accounting", Code::Unauthenticated),
        (
            "Orders/subscriptions/Accounting/$management",
            Code::PermissionDenied,
        ),
        (
            "Orders/subscriptions/Accounting/$deadletterqueue",
            Code::PermissionDenied,
        ),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .update_entity(authorized(
                    subscription_patch(path),
                    &sas(audience, "manage", EXPIRY),
                ))
                .await,
            expected,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    let mut patch = subscription_patch(path);
    patch
        .subscription_config
        .as_mut()
        .expect("patch")
        .max_delivery_count = Some(8);
    code(
        node.service.update_entity(authorized(patch, &token)).await,
        Code::Unavailable,
    );
    assert_eq!(node.store.snapshot()?, before.snapshot);
    assert_eq!(
        node.observations.writes.load(Ordering::SeqCst),
        before.writes
    );
    assert_eq!(
        node.observations.clock_reads.load(Ordering::SeqCst),
        before.clock_reads + 1
    );
    assert_eq!(
        node.broker.handle().last_applied_blocking()?,
        before.applied
    );
    node.clock.set(11_001);
    let mut patch = subscription_patch(path);
    patch
        .subscription_config
        .as_mut()
        .expect("patch")
        .max_delivery_count = Some(8);
    assert_eq!(
        node.service
            .update_entity(authorized(patch, &token))
            .await?
            .into_inner()
            .subscription_config
            .expect("subscription")
            .max_delivery_count,
        Some(8)
    );
    Ok(())
}

async fn topic_manage_updates_parent_and_children_without_neighbor_aliases<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some("Orders"))?;
    let token = sas("Orders", "manage", EXPIRY);
    node.clock.set(11_000);
    let parent = node
        .service
        .update_entity(authorized(topic_patch("Orders"), &token))
        .await?
        .into_inner();
    assert_eq!(parent.kind, EntityKind::Topic as i32);
    assert_eq!(
        parent.topic_config.expect("topic").max_message_bytes,
        Some(32_768)
    );
    for path in [
        "Orders/Subscriptions/Accounting",
        "Orders/subscriptions/Billing",
    ] {
        let updated = node
            .service
            .update_entity(authorized(subscription_patch(path), &token))
            .await?
            .into_inner();
        assert_eq!(updated.kind, EntityKind::Subscription as i32);
        assert_eq!(
            updated
                .subscription_config
                .expect("subscription")
                .max_delivery_count,
            Some(7)
        );
    }
    node.clock.set(0);
    for request in [
        topic_patch("orders"),
        topic_patch("Orders-old"),
        subscription_patch("orders/subscriptions/Accounting"),
        subscription_patch("Orders-old/subscriptions/Accounting"),
        topic_patch("Ordinary"),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .update_entity(authorized(request, &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    Ok(())
}

async fn update_permissions_bad_credentials_and_control_tokens_refuse_before_owner_access<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, None)?;
    node.clock.set(0);
    for (token, expected) in [
        (sas("", "send", EXPIRY), Code::PermissionDenied),
        (sas("", "listen", EXPIRY), Code::PermissionDenied),
        (sas("", "manage", 1), Code::Unauthenticated),
        (
            sas("", "manage", EXPIRY).replace("sig=", "sig=bad"),
            Code::Unauthenticated,
        ),
        (
            signed_resource("amqps://foreign.servicebus.windows.net", "manage", EXPIRY),
            Code::Unauthenticated,
        ),
        (
            sas("Orders/$management", "manage", EXPIRY),
            Code::PermissionDenied,
        ),
        (
            sas(
                "Orders/subscriptions/Accounting/$management",
                "manage",
                EXPIRY,
            ),
            Code::PermissionDenied,
        ),
        (
            sas(
                "Orders/subscriptions/Accounting/$deadletterqueue/$management",
                "manage",
                EXPIRY,
            ),
            Code::PermissionDenied,
        ),
    ] {
        let before = node.guard()?;
        for mut request in [
            topic_patch("Orders"),
            subscription_patch("Orders/subscriptions/Accounting"),
        ] {
            // Deliberately invalid patch families must not outrank authorization.
            request.queue_config = Some(admin_api::v1::QueueConfiguration::default());
            code(
                node.service
                    .update_entity(authorized(request, &token))
                    .await,
                expected,
            );
        }
        node.unchanged(&before, false)?;
    }
    let token = sas("", "manage", EXPIRY);
    let before = node.guard()?;
    for mut request in [
        topic_patch("Orders"),
        subscription_patch("Orders/subscriptions/Accounting"),
    ] {
        request.namespace = "foreign".into();
        code(
            node.service
                .update_entity(authorized(request, &token))
                .await,
            Code::PermissionDenied,
        );
    }
    code(
        node.service
            .update_entity(Request::new(UpdateEntityRequest::default()))
            .await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn literal_control_parent_names_remain_exact_during_updates<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let parent = "Events/$Management";
    let node = Node::start(provider, Some(parent))?;
    let token = sas(parent, "manage", EXPIRY);
    node.clock.set(11_000);
    let updated = node
        .service
        .update_entity(authorized(topic_patch(parent), &token))
        .await?
        .into_inner();
    assert_eq!(updated.path, parent);
    let updated = node
        .service
        .update_entity(authorized(
            subscription_patch("Events/$Management/SUBSCRIPTIONS/Accounting"),
            &token,
        ))
        .await?
        .into_inner();
    assert_eq!(updated.path, "Events/$Management/subscriptions/Accounting");
    node.clock.set(0);
    for request in [
        topic_patch("Events/$management"),
        subscription_patch("Events/$management/subscriptions/Accounting"),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .update_entity(authorized(request, &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await?
            })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await?
            })+ }
    };
}

for_each_backend! {
    exact_child_manage_updates_only_its_canonical_member,
    topic_manage_updates_parent_and_children_without_neighbor_aliases,
    update_permissions_bad_credentials_and_control_tokens_refuse_before_owner_access,
    literal_control_parent_names_remain_exact_during_updates,
}
