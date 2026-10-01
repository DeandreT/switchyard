use super::*;

const SQL_FILTER_CODE: u64 = 0x000001370000006;

fn condition(response: &Message, expected: &str) {
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from(expected)))
    );
}

async fn sql_selection_enumeration_and_overlap_preserve_session_content<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "sql-publisher", "Orders"),
    )
    .await??;
    let mut first = RulesClient::attach(
        &mut session,
        "alpha-sql",
        "Orders/SUBSCRIPTIONS/Alpha/$MANAGEMENT",
        16_384,
    )
    .await?;
    let mut second = RulesClient::attach(
        &mut session,
        "beta-sql",
        "Orders/subscriptions/beta/$management",
        16_384,
    )
    .await?;
    status(&first.remove("$Default").await?, 200);
    status(&second.remove("$Default").await?, 200);
    let original_source =
        " sys.SessionId='A' AND member IN (1,3) AND nullable IS NULL AND NOT EXISTS(blocked) ";
    status(
        &first
            .add("a-filter", "sql-filter", sql(original_source))
            .await?,
        200,
    );
    status(
        &first
            .add(
                "z-overlap",
                "sql-filter",
                sql("member=3 AND sys.SessionId='A'"),
            )
            .await?,
        200,
    );
    status(
        &second
            .add("numeric", "sql-filter", sql("member>=2"))
            .await?,
        200,
    );
    let rules = listed(&first.list(100, 0).await?);
    assert_eq!(
        rules.iter().map(|rule| rule.0.as_str()).collect::<Vec<_>>(),
        ["a-filter", "z-overlap"]
    );
    assert_eq!(
        described(&rules[0].1, SQL_FILTER_CODE),
        [Value::String(original_source.into()), Value::Int(20)]
    );
    assert_eq!(rules[0].2, 1_000);
    let mut originals = Vec::new();
    for (index, group) in [(0, None), (1, Some("A")), (2, Some("B")), (3, Some("A"))] {
        let mut message = rich(index);
        message.properties.as_mut().expect("properties").group_id = group.map(str::to_owned);
        accepted(timeout(DEADLINE, publisher.send(message.clone())).await??);
        originals.push(message);
    }
    assert_eq!(
        node.peek(&alpha)
            .await?
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        [2, 4]
    );
    assert_eq!(
        node.peek(&beta)
            .await?
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        [3, 4]
    );
    for (name, entity, indices) in [
        ("alpha-sql-copy", &alpha, [1, 3]),
        ("beta-sql-copy", &beta, [2, 3]),
    ] {
        let mut receiver = timeout(
            DEADLINE,
            ClientReceiver::attach(&mut session, name, entity.as_str()),
        )
        .await??;
        for index in indices {
            let delivery = recv(&mut receiver).await?;
            assert_eq!(sequence(delivery.message()), index as i64 + 1);
            content(delivery.message(), &originals[index]);
            assert_eq!(
                delivery
                    .message()
                    .properties
                    .as_ref()
                    .expect("properties")
                    .group_id,
                originals[index]
                    .properties
                    .as_ref()
                    .expect("properties")
                    .group_id
            );
            timeout(DEADLINE, receiver.accept(&delivery)).await??;
        }
        node.wait_len(entity, 0).await?;
        timeout(DEADLINE, receiver.close()).await??;
    }
    assert_eq!(listed(&first.list(1, 1).await?)[0].0, "z-overlap");
    status(&first.remove("a-filter").await?, 200);
    status(&first.remove("z-overlap").await?, 200);
    assert!(listed(&first.list(100, 0).await?).is_empty());
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn sql_refusals_and_evaluation_resource_limits_are_atomic_and_recoverable<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut rules = RulesClient::attach(
        &mut session,
        "sql-refusals",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    status(&rules.remove("$Default").await?, 200);
    for (name, source, expected, expected_condition) in [
        (
            "syntax",
            "member=".to_owned(),
            400,
            protocol_amqp::INVALID_FIELD,
        ),
        (
            "function",
            "newid()=NULL".to_owned(),
            501,
            protocol_amqp::NOT_IMPLEMENTED,
        ),
        (
            "scope",
            "sys.NoSuchProperty IS NULL".to_owned(),
            501,
            protocol_amqp::NOT_IMPLEMENTED,
        ),
        (
            "tokens",
            format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS)),
            403,
            protocol_amqp::RESOURCE_LIMIT_EXCEEDED,
        ),
        (
            "source",
            "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            403,
            protocol_amqp::RESOURCE_LIMIT_EXCEEDED,
        ),
    ] {
        let before = node.snapshot()?;
        let refused = rules.add(name, "sql-filter", sql(&source)).await?;
        status(&refused, expected);
        condition(&refused, expected_condition);
        assert_eq!(node.snapshot()?, before);
        assert!(listed(&rules.list(100, 0).await?).is_empty());
    }
    let before = node.snapshot()?;
    let wrong_type = rules
        .add(
            "type",
            "sql-filter",
            Value::Map(map([("expression", Value::Int(1))])),
        )
        .await?;
    status(&wrong_type, 400);
    condition(&wrong_type, protocol_amqp::INVALID_FIELD);
    assert_eq!(node.snapshot()?, before);
    status(
        &rules
            .add("bounded", "sql-filter", sql("sys.MessageId LIKE pattern"))
            .await?,
        200,
    );
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "sql-resource-publisher", "Orders"),
    )
    .await??;
    let mut too_large = rich(7);
    too_large
        .application_properties
        .as_mut()
        .expect("properties")
        .0
        .insert(
            "pattern".into(),
            Value::String("x".repeat(domain::MAX_SQL_LIKE_PATTERN_BYTES + 1)),
        );
    let before = node.snapshot()?;
    let Outcome::Rejected(refused) = timeout(DEADLINE, publisher.send(too_large)).await?? else {
        panic!("resource refusal");
    };
    assert_eq!(
        refused
            .error
            .as_ref()
            .map(|error| error.condition.as_symbol()),
        Some(Symbol::from(protocol_amqp::RESOURCE_LIMIT_EXCEEDED))
    );
    assert_eq!(node.snapshot()?, before);
    assert!(node.peek(&alpha).await?.is_empty());
    assert!(node.peek(&beta).await?.is_empty());
    assert!(node.peek(&alpha.dead_letter_queue()?).await?.is_empty());
    let mut healthy = rich(8);
    healthy
        .application_properties
        .as_mut()
        .expect("properties")
        .0
        .insert("pattern".into(), Value::String("member-%".into()));
    healthy.properties.as_mut().expect("properties").group_id = Some("A".into());
    accepted(timeout(DEADLINE, publisher.send(healthy.clone())).await??);
    for (name, entity) in [("sql-resource-alpha", &alpha), ("sql-resource-beta", &beta)] {
        let mut receiver = timeout(
            DEADLINE,
            ClientReceiver::attach(&mut session, name, entity.as_str()),
        )
        .await??;
        let delivery = recv(&mut receiver).await?;
        assert_eq!(sequence(delivery.message()), 1);
        content(delivery.message(), &healthy);
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
        node.wait_len(entity, 0).await?;
        timeout(DEADLINE, receiver.close()).await??;
    }
    assert_eq!(listed(&rules.list(100, 0).await?)[0].0, "bounded");
    status(&rules.remove("bounded").await?, 200);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn sql_pages_are_clock_free_and_fail_whole_when_the_peer_budget_is_small<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut normal = RulesClient::attach(
        &mut session,
        "sql-full-page",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    status(&normal.remove("$Default").await?, 200);
    status(
        &normal.add("a-small", "sql-filter", sql("member=1")).await?,
        200,
    );
    let source = format!("description='{}'", "x".repeat(950));
    status(
        &normal.add("z-large", "sql-filter", sql(&source)).await?,
        200,
    );
    let mut small = RulesClient::attach(
        &mut session,
        "sql-small-page",
        "Orders/subscriptions/Alpha/$management",
        1_024,
    )
    .await?;
    node.clock.set(0);
    let before = node.snapshot()?;
    assert_eq!(listed(&small.list(1, 0).await?)[0].0, "a-small");
    let refused = small.list(100, 0).await?;
    status(&refused, 403);
    condition(&refused, protocol_amqp::MESSAGE_SIZE_EXCEEDED);
    assert_eq!(refused.body, Body::Value(Value::Null));
    assert_eq!(node.snapshot()?, before);
    let full = listed(&normal.list(100, 0).await?);
    assert_eq!(
        full.iter().map(|rule| rule.0.as_str()).collect::<Vec<_>>(),
        ["a-small", "z-large"]
    );
    assert_eq!(
        described(&full[1].1, SQL_FILTER_CODE),
        [Value::String(source), Value::Int(20)]
    );
    assert_eq!(node.snapshot()?, before);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

for_each_backend! {
    sql_selection_enumeration_and_overlap_preserve_session_content,
    sql_refusals_and_evaluation_resource_limits_are_atomic_and_recoverable,
    sql_pages_are_clock_free_and_fail_whole_when_the_peer_budget_is_small,
}
