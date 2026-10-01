use super::*;

fn original_source_is_stored_listed_and_reopened_without_normalization<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "child", "$Default", 1)?;
    let source = "  UsEr.COLOR = 'Red' /* preserve this comment */ ";
    let filter = SqlFilter::new(source)?;
    assert_eq!(filter.expression(), source);
    assert_eq!(filter.semantic_version(), 1);
    assert_eq!(codec::encode(&RuleFilter::Sql(filter.clone()))?[1], 3);
    add(&fixture, "child", "sql", RuleFilter::Sql(filter), 2)?;
    let expected = RuleDefinition {
        name: RuleName::new("sql")?,
        filter: sql(source)?,
        created_at: Timestamp::from_millis(2),
    };
    let key = keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &SubscriptionName::new("child")?,
        &expected.name,
    );
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(codec::encode(&expected)?)
    );
    assert_eq!(rules(&fixture, "child")?, vec![expected.clone()]);
    let snapshot = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(rules(&fixture, "child")?, vec![expected]);
    let application = publish(&fixture, 3, vec![member("selected")])?;
    effects(&application, std::slice::from_ref(&child));
    assert_eq!(peek(&fixture, &child, 3)?.len(), 1);
    assert_eq!(counters(&fixture, &child)?, None);
    Ok(())
}

fn correlation_and_sql_union_produces_one_shared_sequence_copy<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "Alpha", "$Default", 1)?;
    add(
        &fixture,
        "Alpha",
        "correlation",
        RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([("color".into(), MessageValue::String("Red".into()))]),
            ..CorrelationFilter::default()
        }),
        1,
    )?;
    add(
        &fixture,
        "Alpha",
        "sql",
        sql("color = 'Red' AND sys.MessageId = 'selected'")?,
        1,
    )?;
    let red = member("selected");
    let mut blue = member("unselected");
    blue.envelope
        .application_properties
        .insert("color".into(), MessageValue::String("Blue".into()));
    let application = publish(&fixture, 2, vec![red.clone(), blue.clone()])?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        }
    );
    effects(&application, &[alpha.clone(), beta.clone()]);
    assert_eq!(peek(&fixture, &alpha, 2)?.len(), 1);
    assert_eq!(peek(&fixture, &beta, 2)?.len(), 2);
    for child in [&alpha, &beta] {
        let copy = record(&fixture, child, 1)?.expect("shared selected copy");
        assert_eq!(copy.sequence, SequenceNumber::new(1));
        assert_eq!(copy.envelope.as_deref(), Some(&red.envelope));
        assert_eq!(copy.body, red.body);
        assert_eq!(counters(&fixture, child)?, None);
    }
    assert!(record(&fixture, &alpha, 2)?.is_none());
    assert_eq!(
        record(&fixture, &beta, 2)?
            .expect("ordinary default")
            .envelope
            .as_deref(),
        Some(&blue.envelope)
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic sequence")
            .next_sequence,
        3
    );
    Ok(())
}

fn finite_errors_override_matching_rules_and_route_only_enabled_subscription_shadows<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let early = subscribe(&fixture, "early", SubscriptionConfig::default(), 0)?;
    let late = subscribe(&fixture, "late", SubscriptionConfig::default(), 0)?;
    let disabled = subscribe(
        &fixture,
        "disabled",
        SubscriptionConfig {
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let sessions = subscribe(
        &fixture,
        "sessions",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let healthy = subscribe(&fixture, "healthy", SubscriptionConfig::default(), 0)?;
    for name in ["early", "late"] {
        remove(&fixture, name, "$Default", 1)?;
    }
    add(&fixture, "early", "a-failure", sql("1 / 0 = 1")?, 1)?;
    add(&fixture, "early", "z-match", RuleFilter::True, 1)?;
    add(&fixture, "late", "a-match", RuleFilter::True, 1)?;
    add(&fixture, "late", "z-failure", sql("1 / 0 = 1")?, 1)?;
    for name in ["disabled", "sessions"] {
        add(&fixture, name, "failure", sql("1 / 0 = 1")?, 1)?;
    }
    let mut first = member("secret-message-one");
    first.time_to_live_millis = Some(25);
    first.session_id = Some(SessionId::new("secret-session")?);
    first.envelope.application_properties.insert(
        "secret".into(),
        MessageValue::String("secret-producer-value".into()),
    );
    let mut second = member("secret-message-two");
    second.time_to_live_millis = Some(25);
    second.envelope.application_properties.insert(
        "secret".into(),
        MessageValue::String("different-secret".into()),
    );
    let application = publish(&fixture, 10, vec![first.clone(), second.clone()])?;
    let shadows = [&early, &late, &sessions]
        .into_iter()
        .map(EntityPath::dead_letter_queue)
        .collect::<Result<Vec<_>, _>>()?;
    let mut destinations = shadows.clone();
    destinations.push(healthy.clone());
    effects(&application, &destinations);
    let mut description = None;
    for (sequence, original) in [(1, &first), (2, &second)] {
        for (child, shadow) in [&early, &late, &sessions].into_iter().zip(&shadows) {
            assert!(record(&fixture, child, sequence)?.is_none());
            let copy = record(&fixture, shadow, sequence)?.expect("one filter-error copy");
            assert_eq!(copy.envelope.as_deref(), Some(&original.envelope));
            assert_eq!(copy.body, original.body);
            assert_eq!(copy.session_id, None);
            assert_eq!(copy.expires_at, None);
            assert_eq!(copy.state, MessageState::Ready);
            let dead = copy.dead_letter.expect("canonical filter-error metadata");
            assert_eq!(
                dead.reason,
                DeadLetterReason::Application(SQL_ERROR_REASON.into())
            );
            assert_eq!(dead.dead_lettered_at, Timestamp::from_millis(10));
            assert!(!dead.description.is_empty());
            assert_eq!(dead.description, SQL_DIVISION_DESCRIPTION);
            assert!(!dead.description.contains("secret"));
            assert!(!dead.description.contains("1 / 0"));
            if let Some(expected) = &description {
                assert_eq!(&dead.description, expected);
            } else {
                description = Some(dead.description);
            }
        }
        let healthy_copy = record(&fixture, &healthy, sequence)?.expect("unrelated healthy copy");
        assert_eq!(healthy_copy.expires_at, Some(Timestamp::from_millis(35)));
        assert_eq!(healthy_copy.session_id, original.session_id);
        assert!(healthy_copy.dead_letter.is_none());
        assert!(record(&fixture, &disabled, sequence)?.is_none());
        assert!(record(&fixture, &disabled.dead_letter_queue()?, sequence)?.is_none());
    }
    for shadow in &shadows {
        assert_eq!(peek(&fixture, shadow, 10)?.len(), 2);
    }
    assert_eq!(peek(&fixture, &healthy, 10)?.len(), 2);
    let config = fixture
        .machine
        .subscription_config(
            &fixture.namespace,
            &fixture.entity,
            &SubscriptionName::new("late")?,
        )?
        .expect("default config");
    assert!(config.dead_lettering_on_filter_evaluation_exceptions);
    Ok(())
}

fn unmatched_unknown_and_disabled_errors_still_share_topic_deduplication<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    let disabled = subscribe(
        &fixture,
        "disabled",
        SubscriptionConfig {
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    remove(&fixture, "child", "$Default", 0)?;
    add(&fixture, "child", "unknown", sql("missing = 1")?, 0)?;
    add(&fixture, "disabled", "failure", sql("1 / 0 = 1")?, 0)?;
    let first = publish(
        &fixture,
        1,
        vec![member("known"), member("known"), member("")],
    )?;
    effects(&first, &[]);
    assert_eq!(
        first.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(3)
            ]
        }
    );
    let history = keys::duplicate_history(&fixture.namespace, &fixture.entity, "known");
    assert_eq!(
        fixture.machine.store().get(&history)?,
        Some(codec::encode(&Timestamp::from_millis(20_001))?)
    );
    remove(&fixture, "child", "unknown", 2)?;
    add(&fixture, "child", "true", sql("TRUE")?, 2)?;
    let next = publish(
        &fixture,
        3,
        vec![member("known"), member("fresh"), member("")],
    )?;
    effects(&next, std::slice::from_ref(&child));
    assert_eq!(
        next.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![
                SequenceNumber::new(4),
                SequenceNumber::new(5),
                SequenceNumber::new(6)
            ]
        }
    );
    assert!(record(&fixture, &child, 4)?.is_none());
    assert_eq!(
        peek(&fixture, &child, 3)?
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        vec![SequenceNumber::new(5), SequenceNumber::new(6)]
    );
    assert!(peek(&fixture, &disabled, 3)?.is_empty());
    assert!(peek(&fixture, &disabled.dead_letter_queue()?, 3)?.is_empty());
    assert_eq!(
        fixture.machine.store().get(&history)?,
        Some(codec::encode(&Timestamp::from_millis(20_001))?)
    );
    effects(&publish(&fixture, 4, vec![])?, &[]);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all ingress sequences")
            .next_sequence,
        7
    );
    Ok(())
}

fn finite_error_classes_have_bounded_static_descriptions_and_unknown_is_not_an_error<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for (index, (expression, description)) in [
        (
            "number = 'secret'",
            "SQL filter operands have incompatible types.",
        ),
        (
            "decimal = 1",
            "SQL filter references an unsupported message value.",
        ),
        (
            "9223372036854775807 + 1 = 0",
            "SQL filter integer arithmetic overflowed.",
        ),
        ("1 / 0 = 1", SQL_DIVISION_DESCRIPTION),
        (
            "color LIKE '!' ESCAPE '!'",
            "SQL filter LIKE escape is invalid.",
        ),
        (
            "ambiguous = 1",
            "SQL filter property names collide under lowercase comparison.",
        ),
        (
            "color < 'secret'",
            "SQL filter string ordering is unsupported.",
        ),
        ("number", "SQL filter result is not a Boolean predicate."),
    ]
    .into_iter()
    .enumerate()
    {
        fixture.entity = EntityPath::new(format!("finite-{index}"))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 10)?;
        add(&fixture, "child", "finite", sql(expression)?, 10)?;
        let mut input = member("secret-id");
        input.envelope.application_properties.extend([
            ("decimal".into(), MessageValue::Decimal32([0; 4])),
            ("ambiguous".into(), MessageValue::Int(1)),
            ("AMBIGUOUS".into(), MessageValue::Int(1)),
        ]);
        let application = publish(&fixture, 10, vec![input.clone()])?;
        effects(
            &application,
            std::slice::from_ref(&child.dead_letter_queue()?),
        );
        let copy = record(&fixture, &child.dead_letter_queue()?, 1)?.expect("finite error copy");
        let dead = copy.dead_letter.expect("typed canonical reason");
        assert_eq!(dead.reason.as_str(), SQL_ERROR_REASON);
        assert_eq!(dead.description, description);
        assert!(!dead.description.contains("secret"));
        assert_eq!(copy.envelope.as_deref(), Some(&input.envelope));
        assert!(record(&fixture, &child, 1)?.is_none());
        remove(&fixture, "child", "$Default", 10)?;
        remove(&fixture, "child", "finite", 10)?;
        add(&fixture, "child", "unknown", sql("missing = 1")?, 10)?;
        effects(&publish(&fixture, 10, vec![member("unknown")])?, &[]);
        assert!(record(&fixture, &child.dead_letter_queue()?, 2)?.is_none());
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    original_source_is_stored_listed_and_reopened_without_normalization,
    correlation_and_sql_union_produces_one_shared_sequence_copy,
    finite_errors_override_matching_rules_and_route_only_enabled_subscription_shadows,
    unmatched_unknown_and_disabled_errors_still_share_topic_deduplication,
    finite_error_classes_have_bounded_static_descriptions_and_unknown_is_not_an_error,
}
