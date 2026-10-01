use super::*;

fn default_rule_is_an_atomic_persisted_member_and_reads_remain_bounded<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: Arc::new(AtomicBool::new(false)),
            observations: observations.clone(),
        },
        TopicConfig::default(),
    )?;
    *observations.lock().expect("observations") = Observations::default();
    let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 5)?;
    let subscription = SubscriptionName::new("Alpha")?;
    let default = RuleName::new("$Default")?;
    let rule_key = keys::rule(&fixture.namespace, &fixture.entity, &subscription, &default);
    let definition = RuleDefinition {
        name: default,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(5),
    };
    assert_eq!(read_rules(&fixture, "Alpha")?, vec![definition.clone()]);
    assert_eq!(
        fixture.machine.store().get(&rule_key)?,
        Some(codec::encode(&definition)?)
    );
    let observed = observations.lock().expect("observations");
    assert_eq!(observed.commits, 1);
    for key in [
        keys::subscription(&fixture.namespace, &fixture.entity, &subscription),
        keys::queue_config(&fixture.namespace, &child),
        keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
        rule_key,
        keys::clock(),
    ] {
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|actual| **actual == key)
                .count(),
            1
        );
    }
    assert!(
        !observed
            .puts
            .contains(&keys::queue_counters(&fixture.namespace, &child))
    );
    drop(observed);
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(read_rules(&fixture, "Alpha")?, vec![definition.clone()]);
    let prefix = keys::rule_prefix(&fixture.namespace, &fixture.entity, &subscription);
    let observed = observations.lock().expect("observations");
    assert_eq!(observed.commits, 0);
    assert_eq!(
        observed
            .scans
            .iter()
            .filter(|(actual, _, _)| *actual == prefix)
            .map(|(_, limit, rows)| (*limit, *rows))
            .collect::<Vec<_>>(),
        vec![(domain::MAX_SUBSCRIPTION_RULES + 1, 1)]
    );
    drop(observed);
    let fixture = fixture.restart()?;
    assert_eq!(read_rules(&fixture, "Alpha")?, vec![definition]);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(5)
    );
    Ok(())
}

fn true_false_default_removal_and_exact_names_never_create_duplicate_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    add(&fixture, "Alpha", "Alpha", RuleFilter::False, 1)?;
    effects(
        &publish(&fixture, 2, vec![member("default-masks-false")])?,
        std::slice::from_ref(&child),
    );
    remove(&fixture, "Alpha", "$Default", 3)?;
    effects(&publish(&fixture, 4, vec![member("all-false")])?, &[]);
    assert!(record(&fixture, &child, 2)?.is_none());
    add(&fixture, "Alpha", "$default", RuleFilter::True, 5)?;
    add(&fixture, "Alpha", "z", RuleFilter::True, 6)?;
    let rules = read_rules(&fixture, "Alpha")?;
    assert_eq!(
        rules
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        vec!["$default", "Alpha", "z"]
    );
    assert_eq!(rules[0].created_at, Timestamp::from_millis(5));
    reject(
        &fixture,
        7,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("z")?,
            filter: RuleFilter::False,
        },
        BrokerError::RuleAlreadyExists,
    )?;
    effects(
        &publish(&fixture, 7, vec![member("two-matches-one-copy")])?,
        std::slice::from_ref(&child),
    );
    assert_eq!(
        peek(&fixture, &child, 7)?
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    remove(&fixture, "Alpha", "$default", 8)?;
    remove(&fixture, "Alpha", "Alpha", 9)?;
    remove(&fixture, "Alpha", "z", 10)?;
    assert!(read_rules(&fixture, "Alpha")?.is_empty());
    effects(&publish(&fixture, 11, vec![member("no-rule")])?, &[]);
    assert!(record(&fixture, &child, 4)?.is_none());
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("ingress sequences")
            .next_sequence,
        5
    );
    reject(
        &fixture,
        12,
        CommandKind::DeleteRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("$Default")?,
        },
        BrokerError::RuleNotFound,
    )?;
    let fixture = fixture.restart()?;
    assert!(read_rules(&fixture, "Alpha")?.is_empty());
    add(
        &fixture,
        "Alpha",
        "revived",
        RuleFilter::Correlation(CorrelationFilter::default()),
        12,
    )?;
    effects(
        &publish(&fixture, 13, vec![member("revived")])?,
        std::slice::from_ref(&child),
    );
    assert!(record(&fixture, &child, 5)?.is_some());
    Ok(())
}

fn names_are_validated_on_deserialization_and_rule_scopes_are_exact<P: StoreProvider>(
    provider: P,
) -> TestResult {
    for invalid in [
        "", " \t", "a/b", "a\\b", "a@b", "a?b", "a#b", "a*b", "a\0b", "a\nb",
    ] {
        assert!(RuleName::new(invalid).is_err(), "invalid name {invalid:?}");
        assert!(codec::decode::<RuleName>(&codec::encode(&invalid.to_string())?).is_err());
    }
    assert!(RuleName::new("x".repeat(domain::MAX_RULE_NAME_LENGTH)).is_ok());
    assert!(RuleName::new("x".repeat(domain::MAX_RULE_NAME_LENGTH + 1)).is_err());
    assert!(RuleName::new("\u{1f600}".repeat(25)).is_ok());
    assert!(RuleName::new("\u{1f600}".repeat(26)).is_err());
    for valid in ["$Default", "$default", "$DefaultX", ".", " a ", "a.b-c_d"] {
        let name = RuleName::new(valid)?;
        assert_eq!(name.as_str(), valid);
        assert_eq!(codec::decode::<RuleName>(&codec::encode(&name)?)?, name);
    }
    let mut fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let lower = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "Alpha", "$Default", 1)?;
    add(&fixture, "Alpha", "same", RuleFilter::False, 2)?;
    add(&fixture, "alpha", "same", RuleFilter::True, 3)?;
    assert_eq!(read_rules(&fixture, "Alpha")?.len(), 1);
    assert_eq!(read_rules(&fixture, "alpha")?.len(), 2);
    effects(
        &publish(&fixture, 4, vec![member("scope")])?,
        std::slice::from_ref(&lower),
    );
    assert!(record(&fixture, &alpha, 1)?.is_none());
    let original_namespace = fixture.namespace.clone();
    let original_entity = fixture.entity.clone();
    fixture.namespace = NamespaceName::new("other")?;
    fixture.at(
        4,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let other = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 4)?;
    add(&fixture, "Alpha", "same", RuleFilter::True, 5)?;
    effects(
        &publish(&fixture, 6, vec![member("other-scope")])?,
        std::slice::from_ref(&other),
    );
    assert!(record(&fixture, &other, 1)?.is_some());
    fixture.namespace = original_namespace;
    fixture.entity = EntityPath::new("Orders")?;
    fixture.at(
        6,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 6)?;
    add(&fixture, "Alpha", "same", RuleFilter::True, 7)?;
    fixture.entity = original_entity;
    assert_eq!(read_rules(&fixture, "Alpha")?.len(), 1);
    assert_eq!(read_rules(&fixture, "Alpha")?[0].filter, RuleFilter::False);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    default_rule_is_an_atomic_persisted_member_and_reads_remain_bounded,
    true_false_default_removal_and_exact_names_never_create_duplicate_copies,
    names_are_validated_on_deserialization_and_rule_scopes_are_exact,
}
