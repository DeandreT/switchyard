use super::*;
use domain::{
    IngressBatchLimit, MAX_SQL_COMPILE_SOURCE_BYTES, MAX_SQL_COMPILE_TOKENS,
    MAX_SQL_EXPRESSION_TOKENS, MAX_SQL_EXPRESSION_UTF16_UNITS, MAX_SQL_LIKE_PATTERN_BYTES,
    MAX_SQL_REGEX_ENGINE_BYTES, MAX_SUBSCRIPTION_RULES, MAX_TOPIC_FANOUT_CONTENT_BYTES,
    MAX_TOPIC_FANOUT_COPIES, MAX_TOPIC_FANOUT_VALUE_ITEMS, MAX_TOPIC_RULE_COMPARISON_BYTES,
    MAX_TOPIC_RULE_MATCH_WORK, RuleMatchLimit,
};

fn seed_rules<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    subscription: &str,
    filters: impl IntoIterator<Item = RuleFilter>,
) -> TestResult {
    let subscription = SubscriptionName::new(subscription)?;
    let mut batch = WriteBatch::default().delete(keys::rule(
        &fixture.namespace,
        &fixture.entity,
        &subscription,
        &RuleName::new("$Default")?,
    ));
    for (index, filter) in filters.into_iter().enumerate() {
        let rule = RuleDefinition {
            name: RuleName::new(format!("rule{index:02}"))?,
            filter,
            created_at: Timestamp::from_millis(1),
        };
        batch.push_put(
            keys::rule(
                &fixture.namespace,
                &fixture.entity,
                &subscription,
                &rule.name,
            ),
            codec::encode(&rule)?,
        );
    }
    fixture.machine.store().apply(batch)?;
    Ok(())
}

fn whole_topic_compilation_has_one_allowance_even_for_empty_and_duplicate_ingress<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for (index, kind) in [
        SqlCompileLimit::AggregateTokens,
        SqlCompileLimit::AggregateSourceBytes,
    ]
    .into_iter()
    .enumerate()
    {
        let base = 3 * index as u64 + 1;
        fixture.entity = EntityPath::new(format!("compile-{kind:?}"))?;
        fixture.at(
            base,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        let source = if kind == SqlCompileLimit::AggregateTokens {
            format!("TRUE{}", " ".repeat(MAX_SQL_EXPRESSION_TOKENS - 1))
        } else {
            format!(
                "TRUE/*{}*/",
                "\u{754c}".repeat(MAX_SQL_EXPRESSION_UTF16_UNITS - 8)
            )
        };
        let metrics = SqlProgram::compile(&source)?.metrics();
        let (maximum, per_rule) = if kind == SqlCompileLimit::AggregateTokens {
            (MAX_SQL_COMPILE_TOKENS, metrics.tokens)
        } else {
            (MAX_SQL_COMPILE_SOURCE_BYTES, metrics.source_bytes)
        };
        let full_rules = maximum / per_rule;
        let remainder = maximum % per_rule;
        let total_rules = full_rules + usize::from(remainder != 0);
        let mut targets = Vec::new();
        for index in 0..total_rules.div_ceil(MAX_SUBSCRIPTION_RULES) {
            let name = format!("sub{index:02}");
            targets.push(subscribe(
                &fixture,
                &name,
                SubscriptionConfig::default(),
                base,
            )?);
            let mut filters = Vec::new();
            for offset in 0..MAX_SUBSCRIPTION_RULES {
                let rule_index = index * MAX_SUBSCRIPTION_RULES + offset;
                if rule_index >= total_rules {
                    break;
                }
                let expression = if rule_index == full_rules {
                    assert!(remainder >= 8);
                    format!("TRUE/*{}*/", "x".repeat(remainder - 8))
                } else {
                    source.clone()
                };
                filters.push(sql(&expression)?);
            }
            seed_rules(&fixture, &name, filters)?;
            assert!(!rules(&fixture, &name)?.is_empty());
        }
        let accepted = publish(&fixture, base, vec![member("known")])?;
        effects(&accepted, &targets);
        let late = subscribe(&fixture, "z-late", SubscriptionConfig::default(), base + 1)?;
        add(&fixture, "z-late", "over", sql(&source)?, base + 1)?;
        assert_eq!(rules(&fixture, "z-late")?.len(), 2);
        let expected = BrokerError::SqlRuleCompilation(SqlCompileError::Limit { kind, maximum });
        for command in [
            CommandKind::SendBatch { messages: vec![] },
            CommandKind::SendBatch {
                messages: vec![member("known")],
            },
            CommandKind::ScheduleEnvelopes { messages: vec![] },
            CommandKind::ActivateScheduled,
        ] {
            reject(&fixture, base + 2, command, expected.clone())?;
        }
        assert!(peek(&fixture, &late, base + 1)?.is_empty());
        assert!(peek(&fixture, &late.dead_letter_queue()?, base + 1)?.is_empty());
    }
    Ok(())
}

fn correlation_and_sql_share_evaluation_work_and_bytes_even_after_finite_errors<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for limit in [RuleMatchLimit::WorkUnits, RuleMatchLimit::ComparisonBytes] {
        fixture.entity = EntityPath::new(format!("evaluation-{limit:?}"))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        let failed = subscribe(&fixture, "A-finite", SubscriptionConfig::default(), 10)?;
        add(&fixture, "A-finite", "failure", sql("1 / 0 = 1")?, 10)?;
        for index in 0..8 {
            let sql_name = format!("sql{index}");
            let correlation_name = format!("correlation{index}");
            subscribe(&fixture, &sql_name, SubscriptionConfig::default(), 10)?;
            subscribe(
                &fixture,
                &correlation_name,
                SubscriptionConfig::default(),
                10,
            )?;
            let (sql_filter, correlation) = if limit == RuleMatchLimit::WorkUnits {
                (
                    sql("x = 1")?,
                    RuleFilter::Correlation(CorrelationFilter {
                        properties: (0..32)
                            .map(|index| (format!("p{index:02}"), MessageValue::Int(1)))
                            .collect(),
                        ..CorrelationFilter::default()
                    }),
                )
            } else {
                (
                    sql(&format!("value = '{}'", "z".repeat(400)))?,
                    RuleFilter::Correlation(CorrelationFilter {
                        properties: BTreeMap::from([(
                            "value".into(),
                            MessageValue::String("z".repeat(4_096)),
                        )]),
                        ..CorrelationFilter::default()
                    }),
                )
            };
            seed_rules(&fixture, &sql_name, std::iter::repeat_n(sql_filter, 32))?;
            seed_rules(
                &fixture,
                &correlation_name,
                std::iter::repeat_n(correlation, 32),
            )?;
        }
        let input = |id: &str| {
            let mut message = member(id);
            message.envelope.application_properties = if limit == RuleMatchLimit::WorkUnits {
                (0..32)
                    .map(|index| (format!("p{index:02}"), MessageValue::Int(0)))
                    .collect()
            } else {
                BTreeMap::from([("value".into(), MessageValue::String("x".repeat(32_000)))])
            };
            message
        };
        let count = if limit == RuleMatchLimit::WorkUnits {
            2
        } else {
            1
        };
        let accepted = publish(
            &fixture,
            10,
            (0..count)
                .map(|index| input(&format!("known{index}")))
                .collect(),
        )?;
        effects(
            &accepted,
            std::slice::from_ref(&failed.dead_letter_queue()?),
        );
        let maximum = if limit == RuleMatchLimit::WorkUnits {
            MAX_TOPIC_RULE_MATCH_WORK
        } else {
            MAX_TOPIC_RULE_COMPARISON_BYTES
        };
        reject(
            &fixture,
            11,
            CommandKind::SendBatch {
                messages: (0..=count)
                    .map(|index| input(&format!("known{index}")))
                    .collect(),
            },
            BrokerError::TopicRuleMatchTooLarge { limit, maximum },
        )?;
        assert_eq!(
            peek(&fixture, &failed.dead_letter_queue()?, 10)?.len(),
            count
        );
        assert!(record(&fixture, &failed, 1)?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    &format!("known{count}")
                ))?
                .is_none()
        );
    }
    Ok(())
}

fn later_independent_like_resources_override_an_earlier_finite_error<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for (index, (limit, pattern, maximum)) in [
        (
            RuleMatchLimit::LikePatternBytes,
            "x".repeat(MAX_SQL_LIKE_PATTERN_BYTES - 3),
            MAX_SQL_LIKE_PATTERN_BYTES,
        ),
        (
            RuleMatchLimit::RegexEngineBytes,
            "_".repeat(16_000),
            MAX_SQL_REGEX_ENGINE_BYTES,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        fixture.entity = EntityPath::new(format!("like-{index}"))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 10)?;
        add(&fixture, "child", "a-finite", sql("1 / 0 = 1")?, 10)?;
        add(
            &fixture,
            "child",
            "z-resource",
            sql("input LIKE pattern")?,
            10,
        )?;
        let mut message = member("bounded");
        message.envelope.application_properties = BTreeMap::from([
            ("input".into(), MessageValue::String(String::new())),
            ("pattern".into(), MessageValue::String(pattern.clone())),
        ]);
        reject(
            &fixture,
            11,
            CommandKind::SendBatch {
                messages: vec![message.clone()],
            },
            BrokerError::TopicRuleMatchTooLarge { limit, maximum },
        )?;
        remove(&fixture, "child", "a-finite", 10)?;
        remove(&fixture, "child", "z-resource", 10)?;
        add(
            &fixture,
            "child",
            "one-program",
            sql("(1 / 0 = 1) OR input LIKE pattern")?,
            10,
        )?;
        reject(
            &fixture,
            11,
            CommandKind::SendBatch {
                messages: vec![message],
            },
            BrokerError::TopicRuleMatchTooLarge { limit, maximum },
        )?;
        assert!(peek(&fixture, &child, 10)?.is_empty());
        assert!(peek(&fixture, &child.dead_letter_queue()?, 10)?.is_empty());
    }
    Ok(())
}

fn error_copy_projected_values_and_stripped_session_bytes_are_budgeted_exactly<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    add(&fixture, "child", "failure", sql("1 / 0 = 1")?, 0)?;
    let mut exact = member("values");
    exact.body.clear();
    exact.envelope = MessageEnvelope {
        body: MessageBody::Value(MessageValue::Array(vec![
            MessageValue::Null;
            MAX_TOPIC_FANOUT_VALUE_ITEMS - 3
        ])),
        ..MessageEnvelope::default()
    };
    let accepted = publish(&fixture, 1, vec![exact.clone()])?;
    effects(&accepted, std::slice::from_ref(&child.dead_letter_queue()?));
    let stored =
        record(&fixture, &child.dead_letter_queue()?, 1)?.expect("exact projected value budget");
    assert_eq!(stored.envelope.as_deref(), Some(&exact.envelope));
    let MessageBody::Value(MessageValue::Array(values)) = &mut exact.envelope.body else {
        panic!("array")
    };
    values.push(MessageValue::Null);
    reject(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: vec![exact],
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ValueItems,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS,
        },
    )?;
    fixture.entity = EntityPath::new("exact-bytes")?;
    fixture.at(
        2,
        CommandKind::CreateTopic {
            config: TopicConfig {
                max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
                ..TopicConfig::default()
            },
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..SubscriptionConfig::default()
        },
        2,
    )?;
    add(&fixture, "child", "failure", sql("1 / 0 = 1")?, 2)?;
    let mut exact = member("bytes");
    exact.session_id = Some(SessionId::new("session-stripped")?);
    exact.envelope = MessageEnvelope::default();
    let overhead = exact.envelope.content_size()
        + exact.message_id.len()
        + SQL_ERROR_REASON.len()
        + SQL_DIVISION_DESCRIPTION.len();
    exact.body = vec![b'x'; MAX_TOPIC_FANOUT_CONTENT_BYTES - overhead];
    let accepted = publish(&fixture, 3, vec![exact.clone()])?;
    effects(&accepted, std::slice::from_ref(&child.dead_letter_queue()?));
    assert_eq!(
        record(&fixture, &child.dead_letter_queue()?, 1)?
            .expect("stripped session fits exact bytes")
            .session_id,
        None
    );
    exact.body.push(b'x');
    reject(
        &fixture,
        4,
        CommandKind::SendBatch {
            messages: vec![exact],
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    Ok(())
}

fn active_and_error_routes_share_the_copy_limit_and_exact_sixty_four_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let mut targets = Vec::new();
    for index in 0..32 {
        let name = format!("sub{index:02}");
        let child = subscribe(&fixture, &name, SubscriptionConfig::default(), 0)?;
        add(&fixture, &name, "sql", sql("1 / divisor = 1")?, 0)?;
        targets.push(child.clone());
        targets.push(child.dead_letter_queue()?);
    }
    let input = |index: usize| {
        let mut message = member(&format!("m{index}"));
        message.envelope.application_properties =
            BTreeMap::from([("divisor".into(), MessageValue::Int((index % 2) as i32))]);
        message
    };
    let accepted = publish(
        &fixture,
        1,
        (0..MAX_TOPIC_FANOUT_COPIES / 32).map(input).collect(),
    )?;
    effects(&accepted, &targets);
    assert_eq!(
        accepted
            .subscription_enqueues
            .as_ref()
            .expect("all routes")
            .len(),
        64
    );
    for index in 0..32 {
        let child = fixture
            .entity
            .subscription(&SubscriptionName::new(format!("sub{index:02}"))?)?;
        assert!(record(&fixture, &child, 1)?.is_none());
        assert!(record(&fixture, &child.dead_letter_queue()?, 1)?.is_some());
        assert!(record(&fixture, &child, 2)?.is_some());
        assert!(record(&fixture, &child.dead_letter_queue()?, 2)?.is_none());
        assert_eq!(counters(&fixture, &child)?, None);
        assert_eq!(counters(&fixture, &child.dead_letter_queue()?)?, None);
    }
    reject(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: (0..=MAX_TOPIC_FANOUT_COPIES / 32).map(input).collect(),
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::Messages,
            maximum: MAX_TOPIC_FANOUT_COPIES,
        },
    )?;
    Ok(())
}

fn activation_commits_fitting_rule_work_prefix_and_late_error_head_remains_cancelable<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    let mut children = Vec::new();
    for index in 0..8 {
        let sql_name = format!("sql{index}");
        let corr_name = format!("corr{index}");
        children.push(subscribe(
            &fixture,
            &sql_name,
            SubscriptionConfig::default(),
            0,
        )?);
        children.push(subscribe(
            &fixture,
            &corr_name,
            SubscriptionConfig::default(),
            0,
        )?);
        seed_rules(&fixture, &sql_name, std::iter::repeat_n(sql("x = 1")?, 32))?;
        seed_rules(
            &fixture,
            &corr_name,
            std::iter::repeat_n(
                RuleFilter::Correlation(CorrelationFilter {
                    properties: (0..32)
                        .map(|index| (format!("p{index:02}"), MessageValue::Int(1)))
                        .collect(),
                    ..CorrelationFilter::default()
                }),
                32,
            ),
        )?;
    }
    let input = |index: usize| {
        let mut message = member(&format!("future{index}"));
        message.envelope.application_properties = (0..32)
            .map(|index| (format!("p{index:02}"), MessageValue::Int(0)))
            .collect();
        scheduled(message, 100)
    };
    for index in 0..3 {
        apply(
            &fixture,
            1,
            CommandKind::ScheduleEnvelopes {
                messages: vec![input(index)],
            },
        )?;
    }
    let first = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        first.outcome,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    effects(&first, &[]);
    assert!(record(&fixture, &fixture.entity, 1)?.is_none());
    assert!(record(&fixture, &fixture.entity, 2)?.is_none());
    assert!(matches!(
        record(&fixture, &fixture.entity, 3)?
            .expect("unselected fitting-prefix tail")
            .state,
        MessageState::Scheduled { .. }
    ));
    assert_eq!(
        apply(&fixture, 100, CommandKind::ActivateScheduled)?.outcome,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    for child in &children {
        assert!(peek(&fixture, child, 100)?.is_empty());
        assert!(peek(&fixture, &child.dead_letter_queue()?, 100)?.is_empty());
    }
    fixture.entity = EntityPath::new("late-error-head")?;
    fixture.at(
        100,
        CommandKind::CreateTopic {
            config: TopicConfig {
                max_message_bytes: 2_000_000,
                requires_duplicate_detection: true,
                ..TopicConfig::default()
            },
        },
    )?;
    for index in 0..3 {
        let name = format!("sub{index}");
        subscribe(
            &fixture,
            &name,
            SubscriptionConfig {
                max_message_bytes: 2_000_000,
                ..SubscriptionConfig::default()
            },
            100,
        )?;
        remove(&fixture, &name, "$Default", 100)?;
        add(&fixture, &name, "sql", sql("FALSE")?, 100)?;
    }
    let mut message = member("cancelable");
    message.envelope = MessageEnvelope::default();
    message.body = vec![b'x'; 1_500_000];
    apply(
        &fixture,
        101,
        CommandKind::ScheduleEnvelopes {
            messages: vec![scheduled(message, 200)],
        },
    )?;
    let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "cancelable");
    let history = fixture.machine.store().get(&history_key)?;
    for index in 0..3 {
        let name = format!("sub{index}");
        remove(&fixture, &name, "sql", 102)?;
        add(&fixture, &name, "sql", sql("1 / 0 = 1")?, 102)?;
    }
    reject(
        &fixture,
        200,
        CommandKind::ActivateScheduled,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    assert!(matches!(
        record(&fixture, &fixture.entity, 1)?
            .expect("still pending")
            .state,
        MessageState::Scheduled { .. }
    ));
    effects(
        &apply(
            &fixture,
            201,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1)],
            },
        )?,
        &[],
    );
    assert_eq!(fixture.machine.store().get(&history_key)?, history);
    assert!(record(&fixture, &fixture.entity, 1)?.is_none());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    whole_topic_compilation_has_one_allowance_even_for_empty_and_duplicate_ingress,
    correlation_and_sql_share_evaluation_work_and_bytes_even_after_finite_errors,
    later_independent_like_resources_override_an_earlier_finite_error,
    error_copy_projected_values_and_stripped_session_bytes_are_budgeted_exactly,
    active_and_error_routes_share_the_copy_limit_and_exact_sixty_four_effects,
    activation_commits_fitting_rule_work_prefix_and_late_error_head_remains_cancelable,
}
