use std::collections::BTreeSet;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterInfo, DeadLetterReason, EntityIncarnation,
    EntityIncarnationKind, EntityPath, MessageRecord, MessageState, NamespaceName, QueueConfig,
    ReceiveMode, RuleDefinition, RuleFilter, RuleName, SequenceNumber, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use server::{AtomSubscriptionOwnerError, BrokerHandle, ProposeError, SubmitError};
use storage::{Mutation, StateStore, StoreSnapshot, WriteBatch};

use super::{
    super::{postconditions::SUFFIXES, process::AtomScenario},
    TestResult,
};

const TOPIC: &str = "sdk-atom-subscriptions";
const SIBLING: &str = "NativeSibling";
const FALSE_RULE: &str = "NativeFalse";
const DLQ_REASON: &str = "subscription-retained-proof";
const DLQ_DESCRIPTION: &str = "trusted fixture seed";

fn topic() -> EntityPath {
    EntityPath::new(TOPIC).expect("fixed SDK subscription topic")
}

fn name(kind: &str, suffix: &str) -> SubscriptionName {
    SubscriptionName::new(format!("{kind}-{suffix}")).expect("fixed SDK subscription name")
}

fn definition() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: 15_000,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(45_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

pub(super) fn seed_parent(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            },
        )?,
        CommandOutcome::TopicCreated
    );
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(SIBLING)?,
                config: SubscriptionConfig::default(),
            },
        )?,
        CommandOutcome::SubscriptionCreated
    );
    Ok(())
}

pub(super) fn advance(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    scenario: AtomScenario,
) -> TestResult {
    match scenario {
        AtomScenario::SubscriptionsEmpty => {
            for _ in SUFFIXES {
                missing_delete(
                    handle,
                    namespace,
                    SubscriptionName::new("sdk-atom-missing")?,
                )?;
            }
        }
        AtomScenario::SubscriptionsCreate => {
            for suffix in SUFFIXES {
                for (kind, config) in [
                    ("Default", SubscriptionConfig::default()),
                    ("Definition", definition()),
                ] {
                    assert_eq!(
                        handle.create_atom_subscription_blocking(
                            namespace.clone(),
                            topic(),
                            name(kind, suffix),
                            config,
                        )?,
                        config
                    );
                }
            }
        }
        AtomScenario::SubscriptionsRefusals => {
            for suffix in SUFFIXES {
                let error = handle
                    .create_atom_subscription_blocking(
                        namespace.clone(),
                        topic(),
                        name("Default", suffix),
                        SubscriptionConfig::default(),
                    )
                    .expect_err("duplicate subscription must fail");
                assert!(matches!(
                    error,
                    AtomSubscriptionOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(
                        BrokerError::SubscriptionAlreadyExists
                    )))
                ));
            }
        }
        AtomScenario::SubscriptionsUpdate => {
            for suffix in SUFFIXES {
                for (kind, config) in [
                    ("Default", updated_default()),
                    ("Definition", SubscriptionConfig::default()),
                ] {
                    for _ in 0..2 {
                        assert_eq!(
                            handle.update_atom_subscription_blocking(
                                namespace.clone(),
                                topic(),
                                name(kind, suffix),
                                config,
                            )?,
                            config
                        );
                    }
                }
            }
        }
        AtomScenario::SubscriptionsDelete => {
            for suffix in SUFFIXES {
                for kind in ["Default", "Definition"] {
                    let subscription = name(kind, suffix);
                    assert_eq!(
                        handle.delete_atom_subscription_blocking(
                            namespace.clone(),
                            topic(),
                            subscription.clone(),
                        )?,
                        CommandOutcome::SubscriptionDeleted
                    );
                    missing_delete(handle, namespace, subscription)?;
                }
            }
        }
        AtomScenario::SubscriptionsRecreate => {
            for suffix in SUFFIXES {
                assert_eq!(
                    handle.create_atom_subscription_blocking(
                        namespace.clone(),
                        topic(),
                        name("Default", suffix),
                        SubscriptionConfig::default(),
                    )?,
                    SubscriptionConfig::default()
                );
            }
        }
        AtomScenario::SubscriptionsInspect
        | AtomScenario::SubscriptionsDenied
        | AtomScenario::SubscriptionsTlsRefused => {}
        _ => return Err("queue scenario cannot use subscription replay".into()),
    }
    Ok(())
}

fn missing_delete(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    subscription: SubscriptionName,
) -> TestResult {
    let error = handle
        .delete_atom_subscription_blocking(namespace.clone(), topic(), subscription)
        .expect_err("absent subscription deletion must fail");
    assert!(matches!(
        error,
        AtomSubscriptionOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(
            BrokerError::SubscriptionNotFound
        )))
    ));
    Ok(())
}

pub(super) fn seed_retention(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    for sequence in 1..=2 {
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::Send {
                    message_id: format!("sdk-subscription-retained-{sequence}"),
                    body: vec![0x30 + sequence as u8; 1536],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(sequence)
            }
        );
    }
    for suffix in SUFFIXES {
        let subscription = name("Default", suffix);
        let entity = topic().subscription(&subscription)?;
        let CommandOutcome::Received(Some(delivery)) = handle.submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("trusted default subscription retained seed was not received");
        };
        assert_eq!(delivery.sequence, SequenceNumber::new(1));
        assert_eq!(delivery.message_id, "sdk-subscription-retained-1");
        assert_eq!(delivery.body, vec![0x31; 1536]);
        let lock = delivery.lock.expect("trusted PeekLock seed");
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                entity,
                CommandKind::DeadLetter {
                    sequence: delivery.sequence,
                    lock_token: lock.token,
                    reason: DLQ_REASON.into(),
                    description: DLQ_DESCRIPTION.into(),
                },
            )?,
            CommandOutcome::DeadLettered
        );
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::DeleteRule {
                    subscription: subscription.clone(),
                    name: RuleName::new("$Default")?,
                },
            )?,
            CommandOutcome::RuleDeleted
        );
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::CreateRule {
                    subscription,
                    name: RuleName::new(FALSE_RULE)?,
                    filter: RuleFilter::False,
                },
            )?,
            CommandOutcome::RuleCreated
        );
    }
    Ok(())
}

fn incarnation<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
    generation: u64,
    kind: EntityIncarnationKind,
    retired: bool,
) -> TestResult {
    let bytes = store
        .get(&keys::entity_incarnation(namespace, entity))?
        .expect("raw retained owner incarnation");
    assert_eq!(
        codec::decode::<EntityIncarnation>(&bytes)?,
        EntityIncarnation::new(generation, kind, retired)?
    );
    Ok(())
}

fn excluded_capacity<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> TestResult {
    assert!(
        store
            .get(&keys::queue_capacity_mode(namespace, entity))?
            .is_none()
    );
    assert!(
        store
            .get(&keys::queue_capacity_usage(namespace, entity))?
            .is_none()
    );
    assert!(
        store
            .scan_prefix(&keys::message_charge_prefix(namespace, entity), 1)?
            .is_empty()
    );
    Ok(())
}

fn live<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    subscription: &SubscriptionName,
    config: SubscriptionConfig,
    generation: u64,
    rule_name: &str,
    filter: RuleFilter,
) -> TestResult {
    let parent = topic();
    let entity = parent.subscription(subscription)?;
    let shadow = entity.dead_letter_queue()?;
    let bytes = store
        .get(&keys::subscription(namespace, &parent, subscription))?
        .expect("raw subscription membership config");
    assert_eq!(SubscriptionConfig::decode(&bytes)?, config);
    for (path, expected) in [
        (&entity, config.to_queue_config()),
        (&shadow, config.to_queue_config().dead_letter_shadow()),
    ] {
        let bytes = store
            .get(&keys::queue_config(namespace, path))?
            .expect("raw subscription backing/shadow config");
        assert_eq!(QueueConfig::decode(&bytes)?, expected);
        excluded_capacity(store, namespace, path)?;
    }
    incarnation(
        store,
        namespace,
        &entity,
        generation,
        EntityIncarnationKind::Subscription,
        false,
    )?;
    assert!(
        store
            .get(&keys::entity_incarnation(namespace, &shadow))?
            .is_none()
    );
    let rule_name = RuleName::new(rule_name)?;
    let expected_key = keys::rule(namespace, &parent, subscription, &rule_name);
    let rows = store.scan_prefix(&keys::rule_prefix(namespace, &parent, subscription), 2)?;
    assert_eq!(rows.len(), 1, "unexpected complete stored rule set");
    assert_eq!(rows[0].0, expected_key);
    assert_eq!(
        RuleDefinition::decode(&rows[0].1)?,
        RuleDefinition {
            name: rule_name,
            filter,
            created_at: Timestamp::from_millis(1000),
            action: None
        }
    );
    Ok(())
}

fn membership<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    expected: &[SubscriptionName],
) -> TestResult {
    let expected = expected
        .iter()
        .map(|subscription| keys::subscription(namespace, &topic(), subscription))
        .collect::<BTreeSet<_>>();
    let rows = store.scan_prefix(
        &keys::subscription_prefix(namespace, &topic()),
        expected.len() + 1,
    )?;
    assert_eq!(rows.len(), expected.len());
    assert_eq!(
        rows.iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>(),
        expected
    );
    Ok(())
}

fn index<S: StateStore>(
    store: &S,
    prefix: Vec<u8>,
    expected: BTreeSet<Vec<u8>>,
    marker: bool,
) -> TestResult {
    let rows = store.scan_prefix(&prefix, expected.len() + 1)?;
    assert_eq!(rows.len(), expected.len());
    assert_eq!(
        rows.iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>(),
        expected
    );
    assert!(rows.iter().all(|(_, value)| if marker {
        value.is_empty()
    } else {
        !value.is_empty()
    }));
    Ok(())
}

fn messages<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequences: &[u64],
    ttl: Option<u64>,
    dead_letter: bool,
) -> TestResult {
    let numbers = sequences
        .iter()
        .copied()
        .map(SequenceNumber::new)
        .collect::<Vec<_>>();
    index(
        store,
        keys::message_prefix(namespace, entity),
        numbers
            .iter()
            .map(|&sequence| keys::message(namespace, entity, sequence))
            .collect(),
        false,
    )?;
    index(
        store,
        keys::ready_prefix(namespace, entity),
        numbers
            .iter()
            .map(|&sequence| keys::ready(namespace, entity, sequence))
            .collect(),
        true,
    )?;
    let expiry = ttl.map(|millis| Timestamp::from_millis(1000 + millis));
    index(
        store,
        keys::expiry_prefix(namespace, entity),
        numbers
            .iter()
            .filter_map(|&sequence| expiry.map(|at| keys::expiry(namespace, entity, at, sequence)))
            .collect(),
        true,
    )?;
    for sequence in numbers {
        let bytes = store
            .get(&keys::message(namespace, entity, sequence))?
            .expect("raw retained subscription message");
        assert_eq!(
            MessageRecord::decode(&bytes)?,
            MessageRecord {
                sequence,
                message_id: format!("sdk-subscription-retained-{}", sequence.as_u64()),
                body: vec![0x30 + sequence.as_u64() as u8; 1536],
                enqueued_at: Timestamp::from_millis(1000),
                expires_at: expiry,
                delivery_count: if dead_letter { 1 } else { 0 },
                state: MessageState::Ready,
                session_id: None,
                dead_letter: dead_letter.then(|| DeadLetterInfo {
                    reason: DeadLetterReason::Application(DLQ_REASON.into()),
                    description: DLQ_DESCRIPTION.into(),
                    dead_lettered_at: Timestamp::from_millis(1000),
                }),
                scheduled_enqueue_time: None,
                envelope: None,
            }
        );
    }
    for prefix in [
        keys::lock_prefix(namespace, entity),
        keys::scheduled_prefix(namespace, entity),
        keys::session_lock_prefix(namespace, entity),
        keys::entity_session_prefix(namespace, entity),
        keys::duplicate_history_prefix(namespace, entity),
        keys::duplicate_history_expiry_prefix(namespace, entity),
    ] {
        assert!(store.scan_prefix(&prefix, 1)?.is_empty());
    }
    excluded_capacity(store, namespace, entity)?;
    Ok(())
}

pub(super) fn check_parent<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    retained: bool,
) -> TestResult {
    let parent = topic();
    let bytes = store
        .get(&keys::topic_config(namespace, &parent))?
        .expect("raw native parent topic");
    assert_eq!(
        codec::decode::<TopicConfig>(&bytes)?,
        TopicConfig::default()
    );
    assert!(
        store
            .get(&keys::queue_config(namespace, &parent))?
            .is_none()
    );
    incarnation(
        store,
        namespace,
        &parent,
        1,
        EntityIncarnationKind::Topic,
        false,
    )?;
    excluded_capacity(store, namespace, &parent)?;
    let sibling = SubscriptionName::new(SIBLING)?;
    live(
        store,
        namespace,
        &sibling,
        SubscriptionConfig::default(),
        1,
        "$Default",
        RuleFilter::True,
    )?;
    let entity = parent.subscription(&sibling)?;
    messages(
        store,
        namespace,
        &entity,
        if retained { &[1, 2] } else { &[] },
        None,
        false,
    )?;
    messages(
        store,
        namespace,
        &entity.dead_letter_queue()?,
        &[],
        None,
        false,
    )?;
    Ok(())
}

pub(super) fn check_created<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    check_parent(store, namespace, false)?;
    let mut expected = vec![SubscriptionName::new(SIBLING)?];
    for suffix in SUFFIXES {
        for (kind, config) in [
            ("Default", SubscriptionConfig::default()),
            ("Definition", definition()),
        ] {
            let subscription = name(kind, suffix);
            live(
                store,
                namespace,
                &subscription,
                config,
                1,
                "$Default",
                RuleFilter::True,
            )?;
            let entity = topic().subscription(&subscription)?;
            messages(store, namespace, &entity, &[], None, false)?;
            messages(
                store,
                namespace,
                &entity.dead_letter_queue()?,
                &[],
                None,
                false,
            )?;
            expected.push(subscription);
        }
    }
    membership(store, namespace, &expected)
}

pub(super) fn check_retained<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    check_parent(store, namespace, true)?;
    let mut expected = vec![SubscriptionName::new(SIBLING)?];
    for suffix in SUFFIXES {
        for (kind, config, rule_name, filter) in [
            (
                "Default",
                SubscriptionConfig::default(),
                FALSE_RULE,
                RuleFilter::False,
            ),
            ("Definition", definition(), "$Default", RuleFilter::True),
        ] {
            let subscription = name(kind, suffix);
            live(
                store,
                namespace,
                &subscription,
                config,
                1,
                rule_name,
                filter,
            )?;
            let entity = topic().subscription(&subscription)?;
            if kind == "Default" {
                messages(store, namespace, &entity, &[2], None, false)?;
                messages(
                    store,
                    namespace,
                    &entity.dead_letter_queue()?,
                    &[1],
                    None,
                    true,
                )?;
            } else {
                messages(store, namespace, &entity, &[1, 2], Some(45_000), false)?;
                messages(
                    store,
                    namespace,
                    &entity.dead_letter_queue()?,
                    &[],
                    None,
                    false,
                )?;
            }
            expected.push(subscription);
        }
    }
    membership(store, namespace, &expected)
}

fn owned() -> TestResult<Vec<(SubscriptionName, EntityPath, EntityPath)>> {
    let mut paths = Vec::new();
    for suffix in SUFFIXES {
        for kind in ["Default", "Definition"] {
            let subscription = name(kind, suffix);
            let entity = topic().subscription(&subscription)?;
            let shadow = entity.dead_letter_queue()?;
            paths.push((subscription, entity, shadow));
        }
    }
    Ok(paths)
}

pub(super) fn unaffected(
    namespace: &NamespaceName,
    before: &StoreSnapshot,
    after: &StoreSnapshot,
) -> TestResult {
    let paths = owned()?;
    let unchanged = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| {
                key != &keys::clock()
                    && !paths.iter().any(|(subscription, entity, shadow)| {
                        keys::entity_scope_parts(key).is_some_and(|(ns, path)| {
                            ns == namespace.as_str()
                                && (path == entity.as_str() || path == shadow.as_str())
                        }) || key == &keys::subscription(namespace, &topic(), subscription)
                            || key.starts_with(&keys::rule_prefix(
                                namespace,
                                &topic(),
                                subscription,
                            ))
                    })
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        unchanged(before),
        unchanged(after),
        "subscription deletion changed parent, sibling or unrelated rows"
    );
    Ok(())
}

fn only_fences<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
    shadow: &EntityPath,
) -> TestResult {
    let allowed = BTreeSet::from([
        keys::entity_incarnation(namespace, entity),
        keys::queue_counters(namespace, entity),
        keys::queue_counters(namespace, shadow),
    ]);
    for (key, _) in store.snapshot()?.entries() {
        if keys::entity_scope_parts(key).is_some_and(|(ns, path)| {
            ns == namespace.as_str() && (path == entity.as_str() || path == shadow.as_str())
        }) {
            assert!(
                allowed.contains(key),
                "deleted subscription retained an owned metadata/runtime row"
            );
        }
    }
    excluded_capacity(store, namespace, entity)?;
    excluded_capacity(store, namespace, shadow)?;
    Ok(())
}

pub(super) fn check_deleted<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    retained: &StoreSnapshot,
) -> TestResult {
    check_parent(store, namespace, true)?;
    membership(store, namespace, &[SubscriptionName::new(SIBLING)?])?;
    for (subscription, entity, shadow) in owned()? {
        incarnation(
            store,
            namespace,
            &entity,
            1,
            EntityIncarnationKind::Subscription,
            true,
        )?;
        assert!(
            store
                .get(&keys::subscription(namespace, &topic(), &subscription))?
                .is_none()
        );
        assert!(
            store
                .scan_prefix(&keys::rule_prefix(namespace, &topic(), &subscription), 1)?
                .is_empty()
        );
        only_fences(store, namespace, &entity, &shadow)?;
        for path in [&entity, &shadow] {
            let key = keys::queue_counters(namespace, path);
            let before = retained
                .entries()
                .iter()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, value)| value.clone());
            assert_eq!(
                store.get(&key)?,
                before,
                "subscription deletion changed a retained counter fence"
            );
        }
    }
    unaffected(namespace, retained, &store.snapshot()?)
}

pub(super) fn check_final<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    check_parent(store, namespace, true)?;
    let mut expected = vec![SubscriptionName::new(SIBLING)?];
    for suffix in SUFFIXES {
        let subscription = name("Default", suffix);
        live(
            store,
            namespace,
            &subscription,
            SubscriptionConfig::default(),
            2,
            "$Default",
            RuleFilter::True,
        )?;
        let entity = topic().subscription(&subscription)?;
        messages(store, namespace, &entity, &[], None, false)?;
        messages(
            store,
            namespace,
            &entity.dead_letter_queue()?,
            &[],
            None,
            false,
        )?;
        expected.push(subscription);
        let deleted = name("Definition", suffix);
        let entity = topic().subscription(&deleted)?;
        let shadow = entity.dead_letter_queue()?;
        incarnation(
            store,
            namespace,
            &entity,
            1,
            EntityIncarnationKind::Subscription,
            true,
        )?;
        assert!(
            store
                .get(&keys::subscription(namespace, &topic(), &deleted))?
                .is_none()
        );
        assert!(
            store
                .scan_prefix(&keys::rule_prefix(namespace, &topic(), &deleted), 1)?
                .is_empty()
        );
        only_fences(store, namespace, &entity, &shadow)?;
    }
    membership(store, namespace, &expected)
}

fn updated_default() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 4,
        default_time_to_live_millis: Some(60_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

pub(super) fn check_updated<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    before: &StoreSnapshot,
    batches: &[WriteBatch],
) -> TestResult {
    check_parent(store, namespace, true)?;
    let mut expected = vec![SubscriptionName::new(SIBLING)?];
    let mut allowed = BTreeSet::from([keys::clock()]);
    let mut expected_batches = BTreeSet::new();
    for suffix in SUFFIXES {
        for (kind, config, rule_name, filter) in [
            ("Default", updated_default(), FALSE_RULE, RuleFilter::False),
            (
                "Definition",
                SubscriptionConfig::default(),
                "$Default",
                RuleFilter::True,
            ),
        ] {
            let subscription = name(kind, suffix);
            live(
                store,
                namespace,
                &subscription,
                config,
                1,
                rule_name,
                filter,
            )?;
            let entity = topic().subscription(&subscription)?;
            let shadow = entity.dead_letter_queue()?;
            let config_keys = BTreeSet::from([
                keys::subscription(namespace, &topic(), &subscription),
                keys::queue_config(namespace, &entity),
                keys::queue_config(namespace, &shadow),
            ]);
            for key in &config_keys {
                let original = before
                    .entries()
                    .iter()
                    .find(|(candidate, _)| candidate == key)
                    .expect("original retained subscription config");
                assert_ne!(
                    store.get(key)?.expect("updated subscription config"),
                    original.1,
                    "changed subscription config retained its original bytes"
                );
            }
            let mut batch_keys = config_keys.clone();
            batch_keys.insert(keys::clock());
            expected_batches.insert(batch_keys);
            allowed.extend(config_keys);
            if kind == "Default" {
                // The new finite default TTL does not rewrite pre-existing Unlimited records.
                messages(store, namespace, &entity, &[2], None, false)?;
                messages(store, namespace, &shadow, &[1], None, true)?;
            } else {
                // Unlimited configuration reset preserves the original finite expiry and index.
                messages(store, namespace, &entity, &[1, 2], Some(45_000), false)?;
                messages(store, namespace, &shadow, &[], None, false)?;
            }
            expected.push(subscription);
        }
    }
    membership(store, namespace, &expected)?;
    let unchanged = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !allowed.contains(key))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        unchanged(before),
        unchanged(&store.snapshot()?),
        "subscription update changed retained rows, rules, identities, counters, parent or sibling"
    );
    assert_eq!(
        batches.len(),
        4,
        "changed/no-op SDK updates committed wrong batch count"
    );
    let mut actual_batches = BTreeSet::new();
    for batch in batches {
        assert_eq!(batch.mutations().len(), 4);
        let mut changed = BTreeSet::new();
        for mutation in batch.mutations() {
            let Mutation::Put { key, .. } = mutation else {
                panic!("subscription update deleted a retained row");
            };
            assert!(
                changed.insert(key.clone()),
                "duplicate subscription update mutation"
            );
        }
        assert!(
            actual_batches.insert(changed),
            "duplicate changed subscription batch"
        );
    }
    assert_eq!(
        actual_batches, expected_batches,
        "subscription updates did not commit exactly their config triple and clock"
    );
    Ok(())
}
