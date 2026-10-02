use serde::Serialize;

use super::*;

pub(super) async fn failed_mutations_retry_and_reopen_without_partial_rules<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.manual.set(2_000);
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    let error = code(
        node.create(PATH, "retry", sql("color = 'blue'", None))
            .await,
        Code::Internal,
    );
    assert!(!error.message().contains("private-rule-store-detail"));
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.list(PATH).await?.len(), 1);
    node.clock.manual.set(2_000);
    node.create(PATH, "retry", sql("color = 'blue'", Some(1)))
        .await?;
    let created = node.get(PATH, "retry").await?;
    assert_eq!(created.created_at_unix_millis, 2_000);
    assert_eq!(created.filter, sql("color = 'blue'", Some(1)));
    let before = node.snapshot()?;
    let writes = node.writes();
    node.clock.manual.set(3_000);
    node.store
        .observations
        .fail_next
        .store(true, Ordering::SeqCst);
    let error = code(node.delete(PATH, "retry").await, Code::Internal);
    assert!(!error.message().contains("private-rule-store-detail"));
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes + 1);
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.get(PATH, "retry").await?, created);
    node.clock.manual.set(3_000);
    node.delete(PATH, "retry").await?;
    assert_eq!(node.list(PATH).await?.len(), 1);
    let before = node.snapshot()?;
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    code(node.get(PATH, "retry").await, Code::NotFound);
    Ok(())
}

#[derive(Serialize)]
struct StoredRule {
    name: RuleName,
    filter: StoredFilter,
    created_at: Timestamp,
    action: Option<domain::SqlAction>,
}

// Mirror only the stored enum ordinals to inject a syntactically corrupt source.
#[derive(Serialize)]
#[allow(dead_code)]
enum StoredFilter {
    True,
    False,
    Correlation(CorrelationFilter),
    Sql(StoredSql),
}

#[derive(Serialize)]
struct StoredSql {
    semantic_version: u32,
    expression: String,
}

pub(super) async fn corrupt_sets_refuse_without_partial_responses_or_repairs<P: StoreProvider>(
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
    let rule_key = |name: &str| -> TestResult<Key> {
        Ok(keys::rule(
            &namespace()?,
            &EntityPath::new("Orders")?,
            &SubscriptionName::new("Alpha")?,
            &RuleName::new(name)?,
        ))
    };
    let clean = node.snapshot()?;
    let mut cases = Vec::new();
    cases.push(vec![(rule_key("opaque")?, vec![10, 255])]);
    cases.push(vec![(
        rule_key("key-name")?,
        codec::encode(&RuleDefinition {
            name: RuleName::new("other-name")?,
            filter: domain::RuleFilter::False,
            created_at: Timestamp::from_millis(1_000),
            action: None,
        })?,
    )]);
    cases.push(vec![(
        rule_key("stored-syntax")?,
        codec::encode(&StoredRule {
            name: RuleName::new("stored-syntax")?,
            filter: StoredFilter::Sql(StoredSql {
                semantic_version: 1,
                expression: "broken =".into(),
            }),
            created_at: Timestamp::from_millis(1_000),
            action: None,
        })?,
    )]);
    cases.push(
        (0..domain::MAX_SUBSCRIPTION_RULES)
            .map(|index| {
                let name = format!("extra{index:02}");
                Ok((
                    rule_key(&name)?,
                    codec::encode(&RuleDefinition {
                        name: RuleName::new(name)?,
                        filter: domain::RuleFilter::False,
                        created_at: Timestamp::from_millis(1_000),
                        action: None,
                    })?,
                ))
            })
            .collect::<TestResult<Vec<_>>>()?,
    );

    for injected in cases {
        let mutation_code = if injected.len() == domain::MAX_SUBSCRIPTION_RULES {
            Code::ResourceExhausted
        } else {
            Code::Internal
        };
        let mut batch = WriteBatch::default();
        for (key, value) in &injected {
            batch.push_put(key.clone(), value.clone());
        }
        node.store.apply(batch)?;
        let before = node.snapshot()?;
        let writes = node.writes();
        let clocks = node.clocks();
        for error in [
            code(node.list(PATH).await, Code::Internal),
            code(node.get(PATH, "$Default").await, Code::Internal),
        ] {
            assert!(!error.message().contains("broken"));
            assert!(!error.message().contains("stored-syntax"));
            assert!(!error.message().contains("private-rule-store-detail"));
        }
        node.unchanged(&before, writes, clocks)?;
        code(node.create(PATH, "new", true_filter()).await, mutation_code);
        code(node.delete(PATH, "$Default").await, mutation_code);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.writes(), writes);
        let clocks = node.clocks();
        assert_eq!(node.list("Orders/subscriptions/Healthy").await?.len(), 1);
        node.unchanged(&before, writes, clocks)?;
        let mut cleanup = WriteBatch::default();
        for (key, _) in injected {
            cleanup.push_delete(key);
        }
        node.store.apply(cleanup)?;
        assert_eq!(node.snapshot()?, clean);
    }
    let mut batch = WriteBatch::default();
    batch.push_put(rule_key("opaque")?, vec![10, 255]);
    node.store.apply(batch)?;
    let before = node.snapshot()?;
    let node = node.reopen()?;
    code(node.list(PATH).await, Code::Internal);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), 0);
    assert_eq!(node.clocks(), 0);
    Ok(())
}

pub(super) async fn captured_subscription_generation_fences_inflight_rpcs<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let namespace = namespace()?;
    let parent = EntityPath::new("Orders")?;
    let subscription = SubscriptionName::new("Alpha")?;
    let child = parent.subscription(&subscription)?;
    for action in 0..4 {
        let gate = node.gate(keys::entity_incarnation(&namespace, &child));
        let operation = async {
            match action {
                0 => node.get(PATH, "$Default").await.map(|_| ()),
                1 => node.list(PATH).await.map(|_| ()),
                2 => node.create(PATH, "old-generation", true_filter()).await,
                _ => node.delete(PATH, "$Default").await,
            }
        };
        tokio::pin!(operation);
        pending_once(operation.as_mut()).await;
        gate.started().await?;
        gate.release();
        // The owner barrier captures the old binding while the RPC stays unpolled.
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
        let before = node.snapshot()?;
        let writes = node.writes();
        let clocks = node.clocks();
        node.clock.manual.set(0);
        code(
            tokio::time::timeout(DEADLINE, operation).await?,
            Code::NotFound,
        );
        node.unchanged(&before, writes, clocks)?;
        let rules = node.list(PATH).await?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "$Default");
        assert_eq!(rules[0].created_at_unix_millis, 2_000 + action);
        node.unchanged(&before, writes, clocks)?;
        node.clock.manual.set(2_000 + action);
    }
    node.create(PATH, "fresh-request", true_filter()).await?;
    assert_eq!(node.list(PATH).await?.len(), 2);
    Ok(())
}
