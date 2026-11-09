//! Controlled full-topic setup; raw rows are fixture input, not concurrent writes.
use super::*;
use domain::{MAX_TOPIC_SQL_RULES, SqlRuleAction, SqlRuleFilter};

fn sibling() -> EntityPath {
    topic()
        .subscription(&SubscriptionName::new("other").unwrap())
        .unwrap()
}

fn seed<P: StoreProvider>(
    fixture: &Fixture<P>,
    writer: &mut IndexedWriter<Observed<P::Store>>,
) -> TestResult {
    writer.apply(
        1,
        &proposal(
            &topic(),
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    for (index, name) in [(2, "worker"), (3, "other")] {
        writer.apply(
            index,
            &proposal(
                &topic(),
                index,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            ),
        )?;
    }
    let mut batch = WriteBatch::default();
    for (entity, actions) in [(subscription(), false), (sibling(), true)] {
        for index in 0..MAX_TOPIC_SQL_RULES / 2 {
            let definition = RuleDefinition {
                name: RuleName::new(format!("q{index:04}"))?,
                filter: if actions {
                    RuleFilter::True
                } else {
                    RuleFilter::Sql(SqlRuleFilter::new("1 = 1"))
                },
                action: actions.then(|| SqlRuleAction::new("REMOVE user.old")),
                created_at: Timestamp::from_millis(3),
            };
            batch.push_put(
                keys::subscription_rule(&namespace(), &entity, &definition.name),
                codec::rule::encode(&definition)?,
            );
        }
    }
    fixture.raw(batch);
    Ok(())
}

fn creation(action: bool) -> DurableProposal {
    proposal(
        &subscription(),
        10,
        if action {
            CommandKind::CreateRuleWithAction {
                name: RuleName::new("new-action").unwrap(),
                filter: RuleFilter::True,
                action: SqlRuleAction::new("REMOVE user.old"),
            }
        } else {
            CommandKind::CreateRule {
                name: RuleName::new("new-sql").unwrap(),
                filter: RuleFilter::Sql(SqlRuleFilter::new("1 = 1")),
            }
        },
    )
}

fn healthy<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    seed(&fixture, &mut writer)?;
    let before = domain_rows(&fixture.snapshot());
    for (index, action) in [(4, false), (5, true)] {
        let original = DurableProposal::decode(&creation(action).encode()?)?;
        fixture.reset();
        assert_eq!(
            writer.apply(index, &original)?,
            IndexedApplyOutcome::Refused(BrokerError::SqlRuleLimitExceeded {
                maximum: MAX_TOPIC_SQL_RULES
            })
        );
        assert_eq!(
            domain_rows(&fixture.snapshot()),
            before,
            "refusal writes no domain/clock/counter state"
        );
        let trace = fixture.trace();
        assert_eq!(trace.batches.len(), 1);
        assert!(
            effects(&trace.batches[0]).is_empty(),
            "only the indexed checkpoint may change"
        );
        assert_eq!(writer.applied_index()?, index);
    }
    let checkpointed = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    let mut writer = fixture.writer();
    assert_eq!(writer.applied_index()?, 5);
    assert_eq!(fixture.snapshot(), checkpointed);
    assert_eq!(
        writer.apply(5, &creation(true))?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert_eq!(fixture.snapshot(), checkpointed);
    assert_eq!(
        writer.apply(
            6,
            &proposal(
                &subscription(),
                11,
                CommandKind::CreateRule {
                    name: RuleName::new("later-valid")?,
                    filter: RuleFilter::False
                }
            )
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::RuleCreated)
    );
    assert_eq!(
        fixture.machine().last_applied_time()?,
        Timestamp::from_millis(11)
    );
    drop(writer);
    fixture.reopen();
    assert_eq!(fixture.writer().applied_index()?, 6);
    Ok(())
}

fn corrupt<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for case in 0..3 {
        fixture.clear();
        let mut writer = fixture.writer();
        seed(&fixture, &mut writer)?;
        let name = RuleName::new("q0000")?;
        let key = keys::subscription_rule(&namespace(), &sibling(), &name);
        let mut bad = RuleDefinition {
            name,
            filter: RuleFilter::True,
            action: Some(SqlRuleAction::new("REMOVE user.old")),
            created_at: Timestamp::from_millis(3),
        };
        match case {
            0 => {
                bad.action = Some(SqlRuleAction::new("SET sys.Label = 'unsupported'"));
            }
            1 => {
                bad.name = RuleName::new("wrong-key")?;
            }
            _ => {
                bad.name = RuleName::new("extra-sql")?;
            }
        }
        let key = if case == 2 {
            keys::subscription_rule(&namespace(), &sibling(), &bad.name)
        } else {
            key
        };
        fixture.raw(WriteBatch::default().put(key, codec::rule::encode(&bad)?));
        let before = fixture.snapshot();
        fixture.reset();
        assert!(
            matches!(
                writer.apply(4, &creation(true)),
                Err(IndexedApplyError::Domain(_))
            ),
            "corrupt/overfull case {case} must not mint a healthy refusal"
        );
        assert_eq!(writer.applied_index()?, 3);
        assert_eq!(fixture.snapshot(), before);
        assert!(fixture.trace().batches.is_empty());
        drop(writer);
        fixture.reopen();
        let mut writer = fixture.writer();
        assert_eq!(writer.applied_index()?, 3);
        assert!(matches!(
            writer.apply(4, &creation(true)),
            Err(IndexedApplyError::Domain(_))
        ));
        assert_eq!(fixture.snapshot(), before);
    }
    Ok(())
}

#[test]
fn memory_healthy_sql_quota_refusal_checkpoints_and_later_entry_progresses() -> TestResult {
    healthy(MemoryProvider::new())
}
#[test]
fn fjall_healthy_sql_quota_refusal_checkpoints_and_later_entry_progresses() -> TestResult {
    healthy(DurableProvider::temporary()?)
}
#[test]
fn memory_corrupt_sql_quota_never_checkpoints() -> TestResult {
    corrupt(MemoryProvider::new())
}
#[test]
fn fjall_corrupt_sql_quota_never_checkpoints() -> TestResult {
    corrupt(DurableProvider::temporary()?)
}

fn overfull_proof_stops<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    let mut writer = fixture.writer();
    writer.apply(
        1,
        &proposal(
            &topic(),
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    for (index, name) in ["worker", "a", "b", "c", "d"].into_iter().enumerate() {
        let index = index as u64 + 2;
        writer.apply(
            index,
            &proposal(
                &topic(),
                index,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            ),
        )?;
    }
    let mut rows = WriteBatch::default();
    for (group, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let entity = topic().subscription(&SubscriptionName::new(name)?)?;
        // Every child stays legally below its own cap. Distinct sources make
        // any unnecessary global proof retain distinct compiled programs.
        for index in 0..=MAX_TOPIC_SQL_RULES / 2 {
            let definition = RuleDefinition {
                name: RuleName::new(format!("q{index:04}"))?,
                filter: RuleFilter::Sql(SqlRuleFilter::new(format!("{group} = {index}"))),
                action: (group % 2 == 1)
                    .then(|| SqlRuleAction::new(format!("SET user.flag = {index}"))),
                created_at: Timestamp::from_millis(6),
            };
            rows.push_put(
                keys::subscription_rule(&namespace(), &entity, &definition.name),
                codec::rule::encode(&definition)?,
            );
        }
    }
    fixture.raw(rows);
    let before = fixture.snapshot();
    for action in [false, true] {
        fixture.reset();
        assert_eq!(
            writer.apply(7, &creation(action)),
            Err(IndexedApplyError::Domain(
                BrokerError::SqlRuleLimitExceeded {
                    maximum: MAX_TOPIC_SQL_RULES
                }
            ))
        );
        assert_eq!(writer.applied_index()?, 6);
        assert_eq!(
            fixture.snapshot(),
            before,
            "overfull refusal cannot change clock, counters or checkpoint"
        );
        let trace = fixture.trace();
        assert!(trace.batches.is_empty());
        for name in ["worker", "a", "b", "c", "d"] {
            let entity = topic().subscription(&SubscriptionName::new(name)?)?;
            let prefix = keys::subscription_rule_prefix(&namespace(), &entity);
            let reads = trace.scans.iter().filter(|key| **key == prefix).count();
            let expected = usize::from(matches!(name, "worker" | "a" | "b"));
            assert_eq!(
                reads, expected,
                "bounded rule reads for {name}; no second scan may populate a global program cache"
            );
        }
    }

    // Existing-name refusal still wins before any rule/proof scans, and is
    // checkpointed normally without moving the domain clock.
    fixture.reset();
    assert_eq!(
        writer.apply(
            7,
            &proposal(
                &subscription(),
                10,
                CommandKind::CreateRuleWithAction {
                    name: RuleName::new(domain::DEFAULT_RULE_NAME)?,
                    filter: RuleFilter::True,
                    action: SqlRuleAction::new("REMOVE user.old")
                }
            )
        )?,
        IndexedApplyOutcome::Refused(BrokerError::RuleAlreadyExists {
            name: RuleName::new(domain::DEFAULT_RULE_NAME)?
        })
    );
    assert_eq!(domain_rows(&fixture.snapshot()), domain_rows(&before));
    assert!(fixture.trace().scans.is_empty());
    assert_eq!(
        fixture.machine().last_applied_time()?,
        Timestamp::from_millis(6)
    );
    let checkpointed = fixture.snapshot();
    drop(writer);
    fixture.reopen();
    let mut writer = fixture.writer();
    assert_eq!(writer.applied_index()?, 7);
    assert_eq!(
        writer.apply(8, &creation(true)),
        Err(IndexedApplyError::Domain(
            BrokerError::SqlRuleLimitExceeded {
                maximum: MAX_TOPIC_SQL_RULES
            }
        ))
    );
    assert_eq!(writer.applied_index()?, 7);
    assert_eq!(fixture.snapshot(), checkpointed);
    Ok(())
}

#[test]
fn memory_overfull_sql_proof_stops_before_later_siblings_and_cache_growth() -> TestResult {
    overfull_proof_stops(MemoryProvider::new())
}
#[test]
fn fjall_overfull_sql_proof_stops_before_later_siblings_and_cache_growth() -> TestResult {
    overfull_proof_stops(DurableProvider::temporary()?)
}
