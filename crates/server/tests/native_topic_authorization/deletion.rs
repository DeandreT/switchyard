use admin_api::v1::{DeleteEntityRequest, Operation};

use super::*;

fn request(path: &str, kind: EntityKind) -> DeleteEntityRequest {
    DeleteEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
    }
}

async fn remove<P: StoreProvider>(
    node: &Node<P>,
    path: &str,
    kind: EntityKind,
    token: &str,
) -> Result<Operation, tonic::Status> {
    Ok(tokio::time::timeout(
        DEADLINE,
        node.service
            .delete_entity(authorized(request(path, kind), token)),
    )
    .await
    .expect("bounded authorized delete")?
    .into_inner())
}

fn completed(operation: Operation) {
    assert_eq!(operation.state, "completed");
    assert!(operation.operation_id.is_empty());
    assert!(operation.error.is_empty());
}

async fn exact_child_deletion_canonicalizes_structure_without_widening_scope<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let path = "Orders/subscriptions/Accounting";
    let node = Node::start(provider, Some(path))?;
    let token = sas(path, "manage", EXPIRY);
    for denied in [
        "Orders",
        "Orders/subscriptions/accounting",
        "Orders/subscriptions/Billing",
        "orders/subscriptions/Accounting",
        "Orders-old/subscriptions/Accounting",
        "Ordinary",
    ] {
        let before = node.guard()?;
        code(
            remove(&node, denied, EntityKind::Unspecified, &token).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    let noncanonical = sas("Orders/SUBSCRIPTIONS/Accounting", "manage", EXPIRY);
    code(
        remove(&node, path, EntityKind::Subscription, &noncanonical).await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    node.clock.set(11_000);
    let writes = node.observations.writes.load(Ordering::SeqCst);
    completed(
        remove(
            &node,
            "Orders/SuBsCrIpTiOnS/Accounting",
            EntityKind::Subscription,
            &token,
        )
        .await?,
    );
    assert_eq!(node.observations.writes.load(Ordering::SeqCst), writes + 1);
    code(
        node.service.get_entity(authorized(get(path), &token)).await,
        Code::NotFound,
    );
    assert!(
        StateMachine::new(node.store.clone())
            .subscription_config(
                &NamespaceName::new("tenant")?,
                &EntityPath::new("Orders")?,
                &SubscriptionName::new("accounting")?
            )?
            .is_some()
    );
    let before = node.guard()?;
    code(
        remove(&node, path, EntityKind::Unspecified, &token).await,
        Code::NotFound,
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
    Ok(())
}

async fn parent_manage_deletion_inherits_only_its_children_and_cascade<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some("Orders"))?;
    let token = sas("Orders", "manage", EXPIRY);
    for denied in [
        "orders",
        "Orders-old",
        "Orders-old/subscriptions/Accounting",
        "Ordinary",
    ] {
        let before = node.guard()?;
        code(
            remove(&node, denied, EntityKind::Unspecified, &token).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    node.clock.set(11_000);
    completed(
        remove(
            &node,
            "Orders/subscriptions/Accounting",
            EntityKind::Unspecified,
            &token,
        )
        .await?,
    );
    completed(remove(&node, "Orders", EntityKind::Topic, &token).await?);
    let namespace = NamespaceName::new("tenant")?;
    let machine = StateMachine::new(node.store.clone());
    assert!(
        machine
            .topic_config(&namespace, &EntityPath::new("Orders")?)?
            .is_none()
    );
    assert!(
        machine
            .topic_config(&namespace, &EntityPath::new("orders")?)?
            .is_some()
    );
    assert!(
        machine
            .topic_config(&namespace, &EntityPath::new("Orders-old")?)?
            .is_some()
    );
    let before = node.guard()?;
    code(
        node.service
            .get_entity(authorized(get("Orders"), &token))
            .await,
        Code::NotFound,
    );
    node.unchanged(&before, true)?;
    Ok(())
}

async fn deletion_authentication_and_permission_checks_precede_invalid_input<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, None)?;
    for (token, expected) in [
        (sas("Orders", "send", EXPIRY), Code::PermissionDenied),
        (sas("Orders", "listen", EXPIRY), Code::PermissionDenied),
        (sas("Orders", "manage", 1), Code::Unauthenticated),
        (
            signed_resource(
                "amqps://other.servicebus.windows.net/Orders",
                "manage",
                EXPIRY,
            ),
            Code::Unauthenticated,
        ),
        (
            sas("Orders/$management", "manage", EXPIRY),
            Code::PermissionDenied,
        ),
        (
            sas("Orders/$deadletterqueue", "manage", EXPIRY),
            Code::PermissionDenied,
        ),
    ] {
        let before = node.guard()?;
        let mut invalid = request("Orders", EntityKind::Unspecified);
        invalid.kind = 99;
        code(
            node.service
                .delete_entity(authorized(invalid, &token))
                .await,
            expected,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    code(
        node.service
            .delete_entity(Request::new(DeleteEntityRequest::default()))
            .await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    let token = sas("Orders", "manage", EXPIRY);
    let mut foreign = request("Orders", EntityKind::Topic);
    foreign.namespace = "other".into();
    let before = node.guard()?;
    code(
        node.service
            .delete_entity(authorized(foreign, &token))
            .await,
        Code::PermissionDenied,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn literal_control_parent_deletion_preserves_case_and_regressed_clock_refuses<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let path = "Events/$Management";
    let node = Node::start(provider, Some(path))?;
    let token = sas(path, "manage", EXPIRY);
    let before = node.guard()?;
    code(
        remove(&node, "Events/$management", EntityKind::Topic, &token).await,
        Code::PermissionDenied,
    );
    node.unchanged(&before, false)?;
    node.clock.set(0);
    let before = node.guard()?;
    code(
        remove(&node, path, EntityKind::Topic, &token).await,
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
    let before = node.guard()?;
    node.service
        .get_entity(authorized(get(path), &token))
        .await?;
    node.unchanged(&before, true)?;
    node.clock.set(11_000);
    completed(remove(&node, path, EntityKind::Unspecified, &token).await?);
    assert!(
        StateMachine::new(node.store.clone())
            .topic_config(
                &NamespaceName::new("tenant")?,
                &EntityPath::new("Events/$management")?
            )?
            .is_some()
    );
    Ok(())
}

macro_rules! backends {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> TestResult {
            tokio::time::timeout(DEADLINE * 6, super::$case(testkit::MemoryProvider::new())).await?
        })+ use super::*; }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> TestResult {
            tokio::time::timeout(DEADLINE * 6, super::$case(testkit::DurableProvider::temporary()?)).await?
        })+ use super::*; }
    };
}

backends!(
    exact_child_deletion_canonicalizes_structure_without_widening_scope,
    parent_manage_deletion_inherits_only_its_children_and_cascade,
    deletion_authentication_and_permission_checks_precede_invalid_input,
    literal_control_parent_deletion_preserves_case_and_regressed_clock_refuses,
);
