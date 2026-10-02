use super::*;

pub(super) async fn crud_default_sorting_and_clock_free_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    let default = node.get(PATH, "$Default").await?;
    assert_eq!(default.namespace, "tenant");
    assert_eq!(default.subscription_path, PATH);
    assert_eq!(default.name, "$Default");
    assert_eq!(default.filter, true_filter());
    assert_eq!(default.created_at_unix_millis, 1_000);
    assert_eq!(node.list(PATH).await?, vec![default.clone()]);

    node.clock.manual.set(2_000);
    let writes = node.writes();
    let clocks = node.clocks();
    node.create(PATH, "z-last", false_filter()).await?;
    assert_eq!(node.writes(), writes + 1);
    assert_eq!(node.clocks(), clocks + 1);
    let batch = node
        .store
        .observations
        .batches
        .lock()
        .expect("batches")
        .last()
        .expect("create batch")
        .clone();
    let mut expected = vec![
        keys::clock(),
        keys::rule(
            &namespace()?,
            &EntityPath::new("Orders")?,
            &SubscriptionName::new("Alpha")?,
            &RuleName::new("z-last")?,
        ),
    ];
    expected.sort();
    let mut puts = batch
        .mutations()
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } => key.clone(),
            Mutation::Delete { .. } => panic!("create must contain only puts"),
        })
        .collect::<Vec<_>>();
    puts.sort();
    assert_eq!(puts, expected);
    node.create(PATH, "a-first", true_filter()).await?;
    node.create(PATH, "Sql", sql("color = 'blue' AND priority >= 3", None))
        .await?;
    let saved = node.list(PATH).await?;
    assert_eq!(
        saved
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "Sql", "a-first", "z-last"]
    );
    assert_eq!(
        saved[1].filter,
        sql(
            "color = 'blue' AND priority >= 3",
            Some(domain::SQL_FILTER_SEMANTIC_VERSION)
        )
    );
    assert!(
        saved
            .iter()
            .skip(1)
            .all(|rule| rule.created_at_unix_millis == 2_000)
    );
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    code(
        node.create(PATH, "Sql", sql("broken =", None)).await,
        Code::InvalidArgument,
    );
    node.unchanged(&before, writes, clocks)?;
    code(
        node.create(PATH, "Sql", true_filter()).await,
        Code::AlreadyExists,
    );
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);
    let clocks = node.clocks();
    node.clock.manual.set(0);
    assert_eq!(node.list("Orders/SuBsCrIpTiOnS/Alpha").await?, saved);
    for rule in &saved {
        assert_eq!(node.get(PATH, &rule.name).await?, *rule);
    }
    code(node.get(PATH, "sql").await, Code::NotFound);
    node.unchanged(&before, writes, clocks)?;

    let node = node.reopen()?;
    node.clock.manual.set(0);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.list(PATH).await?, saved);
    assert_eq!(node.clocks(), 0);
    assert_eq!(node.writes(), 0);
    node.clock.manual.set(3_000);
    node.delete(PATH, "$Default").await?;
    node.delete(PATH, "z-last").await?;
    let rules = node.list(PATH).await?;
    assert_eq!(
        rules
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["Sql", "a-first"]
    );
    let before = node.snapshot()?;
    let writes = node.writes();
    code(node.delete(PATH, "z-last").await, Code::NotFound);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), writes);
    Ok(())
}

pub(super) async fn literal_names_paths_and_empty_sets_are_isolated<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (parent, child) in [
        ("Orders", "Alpha"),
        ("Orders", "alpha"),
        ("orders", "Alpha"),
        ("/Orders/$Management", "Subscriptions"),
    ] {
        if child != "alpha" {
            node.topology(parent, child).await?;
        } else {
            tokio::time::timeout(
                DEADLINE,
                node.broker.handle().submit(
                    namespace()?,
                    EntityPath::new(parent)?,
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new(child)?,
                        config: SubscriptionConfig::default(),
                    },
                ),
            )
            .await??;
        }
    }
    node.delete(PATH, "$Default").await?;
    assert!(node.list(PATH).await?.is_empty());
    code(node.get(PATH, "$Default").await, Code::NotFound);
    for name in ["Priority", "priority", "priority orders", "$Other"] {
        node.create("Orders/SUBSCRIPTIONS/Alpha", name, false_filter())
            .await?;
    }
    let saved = node.list(PATH).await?;
    assert_eq!(
        saved
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Other", "Priority", "priority", "priority orders"]
    );
    for path in [
        "Orders/subscriptions/alpha",
        "orders/subscriptions/Alpha",
        "/Orders/$Management/subscriptions/Subscriptions",
    ] {
        let rules = node
            .list(path)
            .await
            .map_err(|error| format!("listing literal sibling {path}: {error}"))?;
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "$Default");
        assert_eq!(rules[0].subscription_path, path);
        code(node.get(path, "Priority").await, Code::NotFound);
    }
    let literal = "/Orders/$Management/subscriptions/Subscriptions";
    let literal_name = " Literal Rule ";
    node.create(literal, literal_name, false_filter()).await?;
    let created = node.get(literal, literal_name).await?;
    assert_eq!(created.namespace, "tenant");
    assert_eq!(created.subscription_path, literal);
    assert_eq!(created.name, literal_name);
    assert_eq!(created.filter, false_filter());
    assert_eq!(created.created_at_unix_millis, 1_000);
    assert_eq!(
        node.list("/Orders/$Management/SUBSCRIPTIONS/Subscriptions")
            .await?
            .iter()
            .find(|rule| rule.name == literal_name),
        Some(&created)
    );
    node.delete(literal, literal_name).await?;
    code(node.get(literal, literal_name).await, Code::NotFound);
    assert_eq!(node.list(literal).await?.len(), 1);

    // Native binding does not widen the AMQP address grammar.
    assert!(matches!(
        tokio::time::timeout(
            DEADLINE,
            node.broker.handle().bind(
                namespace()?,
                protocol_amqp::Attachment::Subscription {
                    topic: EntityPath::new("/Orders/$Management")?,
                    subscription: SubscriptionName::new("Subscriptions")?,
                },
            ),
        )
        .await?,
        Err(server::SubmitError::Propose(server::ProposeError::Broker(
            domain::BrokerError::DanglingEntityMetadata
        )))
    ));
    let admission = tokio::time::timeout(
        DEADLINE,
        node.broker.handle().bind(
            namespace()?,
            protocol_amqp::Attachment::Subscription {
                topic: EntityPath::new("Orders")?,
                subscription: SubscriptionName::new("Alpha")?,
            },
        ),
    )
    .await??
    .expect("ordinary AMQP subscription admission");
    assert_eq!(admission.binding.target().as_str(), PATH);
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    for path in [
        "Orders/subscriptions/Missing",
        "Missing/subscriptions/Alpha",
    ] {
        code(node.list(path).await, Code::NotFound);
        code(node.get(path, "x").await, Code::NotFound);
        code(node.create(path, "x", true_filter()).await, Code::NotFound);
        code(node.delete(path, "x").await, Code::NotFound);
    }
    node.unchanged(&before, writes, clocks)?;
    let reads = node.reads();
    for path in [
        "Orders",
        "Orders/$DeadLetterQueue",
        "Orders/subscriptions/Alpha/$deadletterqueue",
        "Orders/subscriptions/Alpha/extra",
        "Orders/subscriptions/-bad",
    ] {
        code(node.list(path).await, Code::InvalidArgument);
        code(
            node.create(path, "x", true_filter()).await,
            Code::InvalidArgument,
        );
    }
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    let node = node.reopen()?;
    node.clock.manual.set(0);
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.list(PATH).await?, saved);
    let literal_rules = node.list(literal).await?;
    assert_eq!(literal_rules.len(), 1);
    assert_eq!(literal_rules[0].namespace, "tenant");
    assert_eq!(literal_rules[0].subscription_path, literal);
    assert_eq!(literal_rules[0].name, "$Default");
    assert_eq!(literal_rules[0].filter, true_filter());
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.clocks(), 0);
    assert_eq!(node.writes(), 0);
    Ok(())
}
