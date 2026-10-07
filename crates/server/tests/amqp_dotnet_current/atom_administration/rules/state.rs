use std::collections::BTreeSet;

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeadLetterInfo, DeadLetterReason, EntityIncarnation,
    EntityIncarnationKind, EntityPath, LockToken, MessageRecord, MessageState, NamespaceName,
    QueueConfig, QueueCounters, ReceiveMode, RuleDefinition, RuleFilter, RuleName, SequenceNumber,
    SqlAction, SqlFilter, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec,
    keys,
};
use server::{AtomRuleDefinition, AtomRuleOwnerError, BrokerHandle, ProposeError, SubmitError};
use storage::{MemoryStore, Mutation, StateStore, StoreSnapshot, WriteBatch};

use super::{
    super::{postconditions::SUFFIXES, process::AtomScenario},
    TestResult,
};

const TOPIC: &str = "sdk-atom-rules";
const SIBLING: &str = "NativeSibling";
const OPAQUE: &str = "NativeOpaque";
const DLQ_REASON: &str = "rule-retention";
const DLQ_DESCRIPTION: &str = "trusted fixture seed";

#[derive(Clone, Copy)]
pub(super) enum OwnedRules {
    Empty,
    Two,
    DefaultOnly,
}

fn topic() -> EntityPath {
    EntityPath::new(TOPIC).expect("fixed SDK rule topic")
}

fn owned(suffix: &str) -> SubscriptionName {
    SubscriptionName::new(format!("Rules-{suffix}")).expect("fixed SDK rule subscription")
}

fn config() -> SubscriptionConfig {
    SubscriptionConfig {
        default_time_to_live_millis: Some(45_000),
        dead_lettering_on_message_expiration: true,
        ..SubscriptionConfig::default()
    }
}

fn definition(name: &str, filter: RuleFilter) -> TestResult<AtomRuleDefinition> {
    Ok(AtomRuleDefinition {
        name: RuleName::new(name)?,
        filter,
    })
}

fn stored(name: &str, filter: RuleFilter, action: Option<SqlAction>) -> TestResult<RuleDefinition> {
    Ok(RuleDefinition {
        name: RuleName::new(name)?,
        filter,
        action,
        created_at: Timestamp::from_millis(1_000),
    })
}

pub(super) fn seed(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateTopic {
                config: TopicConfig::default()
            }
        )?,
        CommandOutcome::TopicCreated
    );
    for subscription in [
        owned("named"),
        owned("connection"),
        SubscriptionName::new(SIBLING)?,
        SubscriptionName::new(OPAQUE)?,
    ] {
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::CreateSubscription {
                    name: subscription,
                    config: config()
                }
            )?,
            CommandOutcome::SubscriptionCreated
        );
    }
    let opaque = SubscriptionName::new(OPAQUE)?;
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateRule {
                subscription: opaque.clone(),
                name: RuleName::new("NativeSql")?,
                filter: RuleFilter::Sql(SqlFilter::new("1=0")?),
            }
        )?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateRuleWithAction {
                subscription: opaque,
                name: RuleName::new("NativeAction")?,
                filter: RuleFilter::False,
                action: SqlAction::new("SET user.marker = 'native';")?,
            }
        )?,
        CommandOutcome::RuleCreated
    );
    for sequence in 1..=3 {
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::Send {
                    message_id: format!("sdk-rule-retained-{sequence}"),
                    body: vec![0x40 + sequence as u8; 1536],
                    time_to_live_millis: None,
                    session_id: None,
                }
            )?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(sequence)
            }
        );
    }
    for suffix in SUFFIXES {
        let subscription = owned(suffix);
        let entity = topic().subscription(&subscription)?;
        for sequence in 1..=2 {
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
                panic!("trusted rule retention seed must be deliverable");
            };
            assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
            assert_eq!(delivery.message_id, format!("sdk-rule-retained-{sequence}"));
            assert_eq!(delivery.body, vec![0x40 + sequence as u8; 1536]);
            if sequence == 1 {
                assert_eq!(
                    handle.submit_blocking(
                        namespace.clone(),
                        entity.clone(),
                        CommandKind::DeadLetter {
                            sequence: delivery.sequence,
                            lock_token: delivery.lock.expect("trusted PeekLock seed").token,
                            reason: DLQ_REASON.into(),
                            description: DLQ_DESCRIPTION.into(),
                        }
                    )?,
                    CommandOutcome::DeadLettered
                );
            }
        }
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::DeleteRule {
                    subscription,
                    name: RuleName::new("$Default")?,
                }
            )?,
            CommandOutcome::RuleDeleted
        );
    }
    Ok(())
}

fn create(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    subscription: SubscriptionName,
    name: &str,
    filter: RuleFilter,
) -> TestResult {
    let desired = definition(name, filter)?;
    assert_eq!(
        handle.create_atom_rule_blocking(
            namespace.clone(),
            topic(),
            subscription,
            desired.clone()
        )?,
        desired
    );
    Ok(())
}

fn delete(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    subscription: SubscriptionName,
    name: &str,
) -> TestResult {
    assert_eq!(
        handle.delete_atom_rule_blocking(
            namespace.clone(),
            topic(),
            subscription,
            RuleName::new(name)?
        )?,
        CommandOutcome::RuleDeleted
    );
    Ok(())
}

fn missing_delete(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    subscription: SubscriptionName,
    name: &str,
) -> TestResult {
    let error = handle
        .delete_atom_rule_blocking(
            namespace.clone(),
            topic(),
            subscription,
            RuleName::new(name)?,
        )
        .expect_err("absent rule deletion must fail");
    assert!(matches!(
        error,
        AtomRuleOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(
            BrokerError::RuleNotFound
        )))
    ));
    Ok(())
}

pub(super) fn advance(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    scenario: AtomScenario,
) -> TestResult {
    for suffix in SUFFIXES {
        let subscription = owned(suffix);
        match scenario {
            AtomScenario::RulesEmpty => missing_delete(handle, namespace, subscription, "Missing")?,
            AtomScenario::RulesCreate => {
                create(
                    handle,
                    namespace,
                    subscription.clone(),
                    "$Default",
                    RuleFilter::True,
                )?;
                create(handle, namespace, subscription, "Rules", RuleFilter::False)?;
            }
            AtomScenario::RulesRefusals => {
                let error = handle
                    .create_atom_rule_blocking(
                        namespace.clone(),
                        topic(),
                        subscription,
                        definition("$Default", RuleFilter::True)?,
                    )
                    .expect_err("duplicate rule must fail");
                assert!(matches!(
                    error,
                    AtomRuleOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(
                        BrokerError::RuleAlreadyExists
                    )))
                ));
            }
            AtomScenario::RulesDelete => {
                for name in ["$Default", "Rules"] {
                    delete(handle, namespace, subscription.clone(), name)?;
                    missing_delete(handle, namespace, subscription.clone(), name)?;
                }
            }
            AtomScenario::RulesRecreate => create(
                handle,
                namespace,
                subscription,
                "$Default",
                RuleFilter::True,
            )?,
            AtomScenario::RulesOpaque => {
                let opaque = SubscriptionName::new(OPAQUE)?;
                let name = format!("Transient-{suffix}");
                create(handle, namespace, opaque.clone(), &name, RuleFilter::False)?;
                delete(handle, namespace, opaque, &name)?;
            }
            AtomScenario::RulesInspect
            | AtomScenario::RulesDenied
            | AtomScenario::RulesTlsRefused => {}
            _ => return Err("non-rule scenario cannot use rule replay".into()),
        }
    }
    Ok(())
}

fn exclusion<S: StateStore>(
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
    owned: bool,
    shadow: bool,
) -> TestResult {
    let sequences: &[u64] = match (owned, shadow) {
        (true, false) => &[2, 3],
        (true, true) => &[1],
        (false, false) => &[1, 2, 3],
        (false, true) => &[],
    };
    let expiry = (!shadow).then(|| Timestamp::from_millis(46_000));
    index(
        store,
        keys::message_prefix(namespace, entity),
        sequences
            .iter()
            .map(|&n| keys::message(namespace, entity, SequenceNumber::new(n)))
            .collect(),
        false,
    )?;
    index(
        store,
        keys::ready_prefix(namespace, entity),
        sequences
            .iter()
            .filter(|&&n| !(owned && !shadow && n == 2))
            .map(|&n| keys::ready(namespace, entity, SequenceNumber::new(n)))
            .collect(),
        true,
    )?;
    index(
        store,
        keys::expiry_prefix(namespace, entity),
        sequences
            .iter()
            .filter(|&&n| !(owned && !shadow && n == 2))
            .filter_map(|&n| {
                expiry.map(|at| keys::expiry(namespace, entity, at, SequenceNumber::new(n)))
            })
            .collect(),
        true,
    )?;
    index(
        store,
        keys::lock_prefix(namespace, entity),
        if owned && !shadow {
            BTreeSet::from([keys::lock(
                namespace,
                entity,
                Timestamp::from_millis(61_000),
                SequenceNumber::new(2),
            )])
        } else {
            BTreeSet::new()
        },
        true,
    )?;
    for &n in sequences {
        let bytes = store
            .get(&keys::message(namespace, entity, SequenceNumber::new(n)))?
            .expect("raw retained rule message");
        assert_eq!(
            MessageRecord::decode(&bytes)?,
            MessageRecord {
                sequence: SequenceNumber::new(n),
                message_id: format!("sdk-rule-retained-{n}"),
                body: vec![0x40 + n as u8; 1536],
                enqueued_at: Timestamp::from_millis(1_000),
                expires_at: expiry,
                delivery_count: u32::from(owned && (shadow || n == 2)),
                state: if owned && !shadow && n == 2 {
                    MessageState::Locked {
                        token: LockToken::new(2),
                        locked_until: Timestamp::from_millis(61_000),
                    }
                } else {
                    MessageState::Ready
                },
                session_id: None,
                scheduled_enqueue_time: None,
                envelope: None,
                dead_letter: shadow.then(|| DeadLetterInfo {
                    reason: DeadLetterReason::Application(DLQ_REASON.into()),
                    description: DLQ_DESCRIPTION.into(),
                    dead_lettered_at: Timestamp::from_millis(1_000),
                }),
            }
        );
    }
    for prefix in [
        keys::scheduled_prefix(namespace, entity),
        keys::session_lock_prefix(namespace, entity),
        keys::entity_session_prefix(namespace, entity),
        keys::duplicate_history_prefix(namespace, entity),
        keys::duplicate_history_expiry_prefix(namespace, entity),
    ] {
        assert!(store.scan_prefix(&prefix, 1)?.is_empty());
    }
    exclusion(store, namespace, entity)
}

fn rules<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    subscription: &SubscriptionName,
    expected: Vec<RuleDefinition>,
) -> TestResult {
    let mut expected = expected
        .into_iter()
        .map(|rule| {
            Ok((
                keys::rule(namespace, &topic(), subscription, &rule.name),
                codec::encode(&rule)?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        store.scan_prefix(
            &keys::rule_prefix(namespace, &topic(), subscription),
            expected.len() + 1
        )?,
        expected
    );
    Ok(())
}

pub(super) fn check<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    owned_rules: OwnedRules,
) -> TestResult {
    let parent = topic();
    assert_eq!(
        codec::decode::<TopicConfig>(
            &store
                .get(&keys::topic_config(namespace, &parent))?
                .expect("raw rule parent topic")
        )?,
        TopicConfig::default()
    );
    assert_eq!(
        codec::decode::<EntityIncarnation>(
            &store
                .get(&keys::entity_incarnation(namespace, &parent))?
                .expect("raw rule parent incarnation")
        )?,
        EntityIncarnation::new(1, EntityIncarnationKind::Topic, false)?
    );
    assert!(
        store
            .get(&keys::queue_config(namespace, &parent))?
            .is_none()
    );
    exclusion(store, namespace, &parent)?;
    let subscriptions = [
        owned("named"),
        owned("connection"),
        SubscriptionName::new(SIBLING)?,
        SubscriptionName::new(OPAQUE)?,
    ];
    index(
        store,
        keys::subscription_prefix(namespace, &parent),
        subscriptions
            .iter()
            .map(|name| keys::subscription(namespace, &parent, name))
            .collect(),
        false,
    )?;
    for subscription in subscriptions {
        let entity = parent.subscription(&subscription)?;
        let shadow = entity.dead_letter_queue()?;
        assert_eq!(
            SubscriptionConfig::decode(
                &store
                    .get(&keys::subscription(namespace, &parent, &subscription))?
                    .expect("raw rule subscription config")
            )?,
            config()
        );
        assert_eq!(
            codec::decode::<EntityIncarnation>(
                &store
                    .get(&keys::entity_incarnation(namespace, &entity))?
                    .expect("raw rule child incarnation")
            )?,
            EntityIncarnation::new(1, EntityIncarnationKind::Subscription, false)?
        );
        assert!(
            store
                .get(&keys::entity_incarnation(namespace, &shadow))?
                .is_none()
        );
        for (path, expected) in [
            (&entity, config().to_queue_config()),
            (&shadow, config().to_queue_config().dead_letter_shadow()),
        ] {
            assert_eq!(
                QueueConfig::decode(
                    &store
                        .get(&keys::queue_config(namespace, path))?
                        .expect("raw rule backing/shadow config")
                )?,
                expected
            );
        }
        let is_owned = subscription.as_str().starts_with("Rules-");
        if is_owned {
            let counters = codec::decode::<QueueCounters>(
                &store
                    .get(&keys::queue_counters(namespace, &entity))?
                    .expect("raw retained rule lock counter"),
            )?;
            assert_eq!(counters.next_lock_token, 3);
        }
        messages(store, namespace, &entity, is_owned, false)?;
        messages(store, namespace, &shadow, is_owned, true)?;
        let expected = if is_owned {
            match owned_rules {
                OwnedRules::Empty => vec![],
                OwnedRules::Two => vec![
                    stored("$Default", RuleFilter::True, None)?,
                    stored("Rules", RuleFilter::False, None)?,
                ],
                OwnedRules::DefaultOnly => vec![stored("$Default", RuleFilter::True, None)?],
            }
        } else if subscription.as_str() == OPAQUE {
            vec![
                stored("$Default", RuleFilter::True, None)?,
                stored("NativeSql", RuleFilter::Sql(SqlFilter::new("1=0")?), None)?,
                stored(
                    "NativeAction",
                    RuleFilter::False,
                    Some(SqlAction::new("SET user.marker = 'native';")?),
                )?,
            ]
        } else {
            vec![stored("$Default", RuleFilter::True, None)?]
        };
        rules(store, namespace, &subscription, expected)?;
    }
    Ok(())
}

pub(super) fn check_final<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    check(store, namespace, OwnedRules::DefaultOnly)
}

pub(super) fn check_batches(
    namespace: &NamespaceName,
    scenario: AtomScenario,
    before: &StoreSnapshot,
    after: &StoreSnapshot,
    batches: &[WriteBatch],
) -> TestResult {
    let mut expected = Vec::new();
    for suffix in SUFFIXES {
        let subscription = owned(suffix);
        let mut changes = Vec::new();
        match scenario {
            AtomScenario::RulesCreate => {
                for (name, filter) in [("$Default", RuleFilter::True), ("Rules", RuleFilter::False)]
                {
                    let rule = stored(name, filter, None)?;
                    changes.push(Mutation::Put {
                        key: keys::rule(namespace, &topic(), &subscription, &rule.name),
                        value: codec::encode(&rule)?,
                    });
                }
            }
            AtomScenario::RulesDelete => {
                for name in ["$Default", "Rules"] {
                    changes.push(Mutation::Delete {
                        key: keys::rule(namespace, &topic(), &subscription, &RuleName::new(name)?),
                    });
                }
            }
            AtomScenario::RulesRecreate => {
                let rule = stored("$Default", RuleFilter::True, None)?;
                changes.push(Mutation::Put {
                    key: keys::rule(namespace, &topic(), &subscription, &rule.name),
                    value: codec::encode(&rule)?,
                });
            }
            AtomScenario::RulesOpaque => {
                let name = format!("Transient-{suffix}");
                let rule = stored(&name, RuleFilter::False, None)?;
                let key = keys::rule(
                    namespace,
                    &topic(),
                    &SubscriptionName::new(OPAQUE)?,
                    &rule.name,
                );
                changes.push(Mutation::Put {
                    key: key.clone(),
                    value: codec::encode(&rule)?,
                });
                changes.push(Mutation::Delete { key });
            }
            AtomScenario::RulesEmpty | AtomScenario::RulesInspect | AtomScenario::RulesRefusals => {
            }
            _ => return Err("unexpected measured rule stage".into()),
        }
        for mutation in changes {
            expected.push(vec![
                mutation,
                Mutation::Put {
                    key: keys::clock(),
                    value: codec::encode(&Timestamp::from_millis(1_000))?,
                },
            ]);
        }
    }
    assert_eq!(
        batches.len(),
        expected.len(),
        "wrong exact successful-stage rule batch count"
    );
    let projected = MemoryStore::default();
    projected.apply(
        before
            .entries()
            .iter()
            .fold(WriteBatch::default(), |batch, (key, value)| {
                batch.put(key.clone(), value.clone())
            }),
    )?;
    for (batch, expected) in batches.iter().zip(expected) {
        assert_eq!(
            batch.mutations(),
            expected,
            "rule stage changed more than its exact rule and Clock"
        );
        projected.apply(
            expected
                .into_iter()
                .fold(WriteBatch::default(), |batch, mutation| match mutation {
                    Mutation::Put { key, value } => batch.put(key, value),
                    Mutation::Delete { key } => batch.delete(key),
                }),
        )?;
    }
    assert_eq!(
        after,
        &projected.snapshot()?,
        "rule stage changed retained messages, expiry/DLQ/config/rules/identities/counters or unrelated rows"
    );
    Ok(())
}
