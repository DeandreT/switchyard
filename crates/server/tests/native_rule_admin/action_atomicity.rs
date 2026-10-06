use serde::Serialize;

use super::*;

pub(super) async fn action_mutations_retry_without_partial_state_and_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let source = " /* exact retry source */ REMOVE user.[audit]; REMOVE missing; ";
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.manual.set(2_000);
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    let error = code(
        node.create_action(
            PATH,
            "retry-action",
            true_filter(),
            sql_action(source, None),
        )
        .await,
        Code::Internal,
    );
    assert!(!error.message().contains("private-rule-store-detail"));
    assert!(!error.message().contains(source));
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let node = node.reopen()?;
    node.unchanged(&before, 0, 0)?;
    assert_eq!(node.list_actions(PATH).await?.len(), 1);
    node.clock.manual.set(2_000);
    node.create_action(
        PATH,
        "retry-action",
        true_filter(),
        sql_action(source, Some(1)),
    )
    .await?;
    let created = node.get_actions(PATH, "retry-action").await?;
    assert_eq!(created.created_at_unix_millis, 2_000);
    assert_eq!(created.action, sql_action(source, Some(1)));
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.manual.set(3_000);
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    code(node.delete(PATH, "retry-action").await, Code::Internal);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let node = node.reopen()?;
    node.clock.manual.set(0);
    assert_eq!(node.get_actions(PATH, "retry-action").await?, created);
    code(node.list(PATH).await, Code::Unimplemented);
    node.unchanged(&before, 0, 0)?;
    node.clock.manual.set(3_000);
    node.delete(PATH, "retry-action").await?;
    assert_eq!(node.list(PATH).await?.len(), 1);
    let before = node.snapshot()?;
    let node = node.reopen()?;
    code(node.get_actions(PATH, "retry-action").await, Code::NotFound);
    assert!(
        node.list_actions(PATH)
            .await?
            .iter()
            .all(|rule| rule.action.is_none())
    );
    node.unchanged(&before, 0, 0)?;
    Ok(())
}

#[derive(Serialize)]
struct StoredAction {
    semantic_version: u32,
    expression: String,
}

#[derive(Serialize)]
struct StoredRule {
    name: RuleName,
    filter: domain::RuleFilter,
    created_at: Timestamp,
    action: Option<StoredAction>,
}

fn corrupt_rule(source: &str, version: u32) -> TestResult<Vec<u8>> {
    Ok(codec::encode(&StoredRule {
        name: RuleName::new("z-corrupt")?,
        filter: domain::RuleFilter::True,
        created_at: Timestamp::from_millis(1_000),
        action: Some(StoredAction {
            semantic_version: version,
            expression: source.into(),
        }),
    })?)
}

pub(super) async fn corrupt_actions_refuse_complete_reads_without_repairs<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    tokio::time::timeout(
        DEADLINE,
        node.broker.handle().submit(
            namespace()?,
            EntityPath::new("Orders")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Healthy")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )
    .await??;
    node.create_action(
        PATH,
        "a-healthy",
        true_filter(),
        sql_action("REMOVE [healthy]", None),
    )
    .await?;
    let key = keys::rule(
        &namespace()?,
        &EntityPath::new("Orders")?,
        &SubscriptionName::new("Alpha")?,
        &RuleName::new("z-corrupt")?,
    );
    let clean = node.snapshot()?;
    for bytes in [
        corrupt_rule("REMOVE", 1)?,
        corrupt_rule("SET private-field = 'private-source'", 1)?,
        corrupt_rule("REMOVE private-source", 3)?,
        corrupt_rule(&"x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1), 1)?,
    ] {
        let mut batch = WriteBatch::default();
        batch.push_put(key.clone(), bytes);
        node.store.apply(batch)?;
        let before = node.snapshot()?;
        let writes = node.writes();
        let clocks = node.clocks();
        node.clock.manual.set(0);
        for error in [
            code(node.get(PATH, "$Default").await, Code::Internal),
            code(node.get_actions(PATH, "$Default").await, Code::Internal),
            code(node.get_actions(PATH, "a-healthy").await, Code::Internal),
            code(node.list(PATH).await, Code::Internal),
            code(node.list_actions(PATH).await, Code::Internal),
        ] {
            assert_eq!(error.message(), "rule operation failed");
            assert!(!error.message().contains("private-source"));
        }
        assert_eq!(
            node.list_actions("Orders/subscriptions/Healthy")
                .await?
                .len(),
            1
        );
        node.unchanged(&before, writes, clocks)?;
        node.clock.manual.set(1_000);
        code(
            node.create_action(PATH, "new", true_filter(), sql_action("REMOVE x", None))
                .await,
            Code::Internal,
        );
        code(node.delete(PATH, "a-healthy").await, Code::Internal);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.writes(), writes);
        let mut cleanup = WriteBatch::default();
        cleanup.push_delete(key.clone());
        node.store.apply(cleanup)?;
        assert_eq!(node.snapshot()?, clean);
    }
    let mut batch = WriteBatch::default();
    batch.push_put(
        key,
        corrupt_rule("SET private-field = 'private-source'", 1)?,
    );
    node.store.apply(batch)?;
    let before = node.snapshot()?;
    let node = node.reopen()?;
    node.clock.manual.set(0);
    code(node.list_actions(PATH).await, Code::Internal);
    code(node.get_actions(PATH, "$Default").await, Code::Internal);
    node.unchanged(&before, 0, 0)?;
    Ok(())
}

pub(super) async fn captured_action_bindings_fence_recreated_children<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let namespace = namespace()?;
    let parent = EntityPath::new("Orders")?;
    let subscription = SubscriptionName::new("Alpha")?;
    let child = parent.subscription(&subscription)?;
    for action in 0..3 {
        node.create_action(
            PATH,
            "a-action",
            true_filter(),
            sql_action("REMOVE old", None),
        )
        .await?;
        let gate = node.gate(keys::entity_incarnation(&namespace, &child));
        let operation = async {
            match action {
                0 => {
                    node.create_action(
                        PATH,
                        "old-generation",
                        true_filter(),
                        sql_action("REMOVE private", None),
                    )
                    .await
                }
                1 => node.get_actions(PATH, "a-action").await.map(|_| ()),
                _ => node.list_actions(PATH).await.map(|_| ()),
            }
        };
        tokio::pin!(operation);
        pending_once(operation.as_mut()).await;
        gate.started().await?;
        gate.release();
        // Capture admission before replacing the child; the RPC stays unpolled.
        tokio::time::timeout(
            DEADLINE,
            node.broker
                .handle()
                .queue_config(namespace.clone(), parent.clone()),
        )
        .await??;
        node.clock.manual.set(2_000 + action);
        tokio::time::timeout(
            DEADLINE,
            node.broker.handle().submit(
                namespace.clone(),
                parent.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Subscription {
                        name: subscription.clone(),
                    },
                },
            ),
        )
        .await??;
        tokio::time::timeout(
            DEADLINE,
            node.broker.handle().submit(
                namespace.clone(),
                parent.clone(),
                CommandKind::CreateSubscription {
                    name: subscription.clone(),
                    config: SubscriptionConfig::default(),
                },
            ),
        )
        .await??;
        node.create_action(
            PATH,
            "a-action",
            true_filter(),
            sql_action("REMOVE replacement", None),
        )
        .await?;
        let before = node.snapshot()?;
        let writes = node.writes();
        let clocks = node.clocks();
        node.clock.manual.set(0);
        code(
            tokio::time::timeout(DEADLINE, operation).await?,
            Code::NotFound,
        );
        let rules = node.list_actions(PATH).await?;
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].name, "$Default");
        assert!(rules[0].action.is_none());
        assert_eq!(rules[0].created_at_unix_millis, 2_000 + action);
        assert_eq!(rules[1].name, "a-action");
        assert_eq!(rules[1].action, sql_action("REMOVE replacement", Some(2)));
        node.unchanged(&before, writes, clocks)?;
        node.clock.manual.set(2_000 + action);
        node.delete(PATH, "a-action").await?;
    }
    node.create_action(
        PATH,
        "fresh",
        true_filter(),
        sql_action("REMOVE current", None),
    )
    .await?;
    assert_eq!(
        node.get_actions(PATH, "fresh").await?.action,
        sql_action("REMOVE current", Some(2))
    );
    Ok(())
}
