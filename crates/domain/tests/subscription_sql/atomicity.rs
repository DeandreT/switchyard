use super::*;

#[derive(serde::Serialize)]
struct StoredSql {
    semantic_version: u32,
    expression: String,
}

#[allow(dead_code)]
#[derive(serde::Serialize)]
enum StoredFilter {
    True,
    False,
    Correlation(CorrelationFilter),
    Sql(StoredSql),
}

#[derive(serde::Serialize)]
struct StoredRule {
    name: RuleName,
    filter: StoredFilter,
    created_at: Timestamp,
    action: Option<domain::SqlAction>,
}

fn replayed_sql_rule_is_compiled_by_the_owner_before_any_metadata_is_staged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    subscribe(&fixture, "child", SubscriptionConfig::default(), 0)?;
    assert_eq!(SqlFilter::new("broken ="), Err(SqlCompileError::Syntax));

    // Replicated commands can deserialize a bounded source without passing
    // through the public compiling constructor.
    let filter = codec::decode::<RuleFilter>(&codec::encode(&StoredFilter::Sql(StoredSql {
        semantic_version: 1,
        expression: "broken =".into(),
    }))?)?;
    let RuleFilter::Sql(sql_filter) = &filter else {
        panic!("decoded SQL filter");
    };
    assert_eq!(sql_filter.expression(), "broken =");
    filter.validate()?;
    let before = fixture.machine.store().snapshot()?;
    let applied = fixture.machine.last_applied_time()?;
    let original_rules = rules(&fixture, "child")?;
    reject(
        &fixture,
        17,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("child")?,
            name: RuleName::new("replayed")?,
            filter,
        },
        BrokerError::SqlRuleCompilation(SqlCompileError::Syntax),
    )?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    assert_eq!(rules(&fixture, "child")?, original_rules);

    let accepted = apply(
        &fixture,
        17,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("child")?,
            name: RuleName::new("replayed")?,
            filter: sql("TRUE")?,
        },
    )?;
    assert_eq!(accepted.outcome, CommandOutcome::RuleCreated);
    assert_eq!(accepted.subscription_enqueues, None);
    assert_eq!(rules(&fixture, "child")?.len(), 2);
    Ok(())
}

fn corrupt_late_sql_versions_sources_and_topology_refuse_all_ingress_atomically<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    for damage in 0..7 {
        fixture.entity = EntityPath::new(format!("damage-{damage}"))?;
        fixture.at(
            10,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        let healthy = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 10)?;
        let late = subscribe(&fixture, "zulu", SubscriptionConfig::default(), 10)?;
        add(&fixture, "zulu", "sql", sql("TRUE")?, 10)?;
        publish(&fixture, 10, vec![member("known")])?;
        let name = RuleName::new("sql")?;
        let subscription = SubscriptionName::new("zulu")?;
        let key = keys::rule(&fixture.namespace, &fixture.entity, &subscription, &name);
        let (version, source, expected) = match damage {
            0 => (
                2,
                "TRUE".to_owned(),
                BrokerError::Codec(codec::CodecError::Decode),
            ),
            1 => (1, "broken =".to_owned(), BrokerError::DanglingRuleMetadata),
            2 => (
                1,
                "CAST(1 AS BIGINT)=1".to_owned(),
                BrokerError::DanglingRuleMetadata,
            ),
            3 => (
                1,
                "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
                BrokerError::Codec(codec::CodecError::Decode),
            ),
            4 => (
                1,
                format!("TRUE{}", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS)),
                BrokerError::DanglingRuleMetadata,
            ),
            5 => (1, "TRUE".to_owned(), BrokerError::MalformedIndexKey),
            6 => (
                1,
                "TRUE".to_owned(),
                BrokerError::DanglingSubscriptionMetadata,
            ),
            _ => unreachable!(),
        };
        let bytes = codec::encode(&StoredRule {
            name,
            filter: StoredFilter::Sql(StoredSql {
                semantic_version: version,
                expression: source,
            }),
            created_at: Timestamp::from_millis(10),
            action: None,
        })?;
        let mutation = if damage == 6 {
            WriteBatch::default().delete(keys::queue_config(
                &fixture.namespace,
                &late.dead_letter_queue()?,
            ))
        } else {
            let mut damaged_key = key;
            if damage == 5 {
                damaged_key.push(b'x');
            }
            WriteBatch::default().put(damaged_key, bytes)
        };
        fixture.machine.store().apply(mutation)?;
        let before = fixture.machine.store().snapshot()?;
        let applied = fixture.machine.last_applied_time()?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.entity, &subscription),
            Err(expected.clone())
        );
        assert_eq!(rules(&fixture, "Alpha")?.len(), 1);
        for kind in [
            CommandKind::SendBatch {
                messages: vec![member("fresh")],
            },
            CommandKind::SendBatch {
                messages: vec![member("known")],
            },
            CommandKind::SendBatch { messages: vec![] },
            CommandKind::ScheduleEnvelopes {
                messages: vec![scheduled(member("future"), 200)],
            },
            CommandKind::ScheduleEnvelopes { messages: vec![] },
            CommandKind::ActivateScheduled,
        ] {
            reject(&fixture, 100, kind, expected.clone())?;
        }
        assert_eq!(fixture.machine.store().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, applied);
        assert_eq!(peek(&fixture, &healthy, 10)?.len(), 1);
        assert!(record(&fixture, &healthy, 2)?.is_none());
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::duplicate_history(
                    &fixture.namespace,
                    &fixture.entity,
                    "fresh"
                ))?
                .is_none()
        );
    }
    Ok(())
}

fn one_commit_failure_preserves_rules_history_and_both_routes_then_retries_after_reopen<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
        },
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let healthy = subscribe(&fixture, "healthy", SubscriptionConfig::default(), 0)?;
    let failed = subscribe(&fixture, "failed", SubscriptionConfig::default(), 0)?;
    add(&fixture, "failed", "sql", sql("1 / 0 = 1")?, 1)?;
    let before = fixture.machine.store().snapshot()?;
    let applied = fixture.machine.last_applied_time()?;
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    assert!(matches!(
        publish(&fixture, 2, vec![member("known"), member("known")]),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, applied);
    assert_eq!(observations.lock().expect("observations").commits, 1);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    let application = publish(&fixture, 2, vec![member("known"), member("known")])?;
    let shadow = failed.dead_letter_queue()?;
    effects(&application, &[healthy.clone(), shadow.clone()]);
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
        }
    );
    assert_eq!(peek(&fixture, &healthy, 2)?.len(), 1);
    assert_eq!(peek(&fixture, &shadow, 2)?.len(), 1);
    assert!(record(&fixture, &failed, 1)?.is_none());
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.commits, 1);
        for key in [
            keys::message(&fixture.namespace, &healthy, SequenceNumber::new(1)),
            keys::message(&fixture.namespace, &shadow, SequenceNumber::new(1)),
            keys::queue_counters(&fixture.namespace, &fixture.entity),
            keys::duplicate_history(&fixture.namespace, &fixture.entity, "known"),
        ] {
            assert_eq!(
                observed
                    .puts
                    .iter()
                    .filter(|written| **written == key)
                    .count(),
                1
            );
        }
    }
    let before = fixture.machine.store().snapshot()?;
    fail_next.store(true, Ordering::Relaxed);
    assert!(matches!(
        fixture.at(
            3,
            CommandKind::CreateRule {
                subscription: SubscriptionName::new("healthy")?,
                name: RuleName::new("extra")?,
                filter: sql("TRUE")?
            }
        ),
        Err(BrokerError::Storage(StorageError::Backend { .. }))
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    add(&fixture, "healthy", "extra", sql("TRUE")?, 3)?;
    assert_eq!(rules(&fixture, "healthy")?.len(), 2);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    replayed_sql_rule_is_compiled_by_the_owner_before_any_metadata_is_staged,
    corrupt_late_sql_versions_sources_and_topology_refuse_all_ingress_atomically,
    one_commit_failure_preserves_rules_history_and_both_routes_then_retries_after_reopen,
}
