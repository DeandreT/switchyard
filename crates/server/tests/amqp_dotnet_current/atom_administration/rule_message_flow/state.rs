use std::collections::{BTreeMap, BTreeSet};

use domain::{
    AnnotationKey, CommandKind, CommandOutcome, CorrelationFilter, EntityPath, MessageBody,
    MessageHeader, MessageIdentifier, MessageRecord, MessageState, MessageValue, NamespaceName,
    QueueConfig, QueueCounters, RuleDefinition, RuleFilter, RuleName, SequenceNumber, SqlAction,
    SqlFilter, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec,
    keys,
};
use server::BrokerHandle;
use storage::{Mutation, StateStore, StoreSnapshot, WriteBatch};

use super::TestResult;

pub(super) const TOPIC: &str = "sdk-atom-rule-flow";
const RETAINED: &str = "bridge-unrelated-retained";
const SUBJECT: &str = "bridge caf\u{e9} & <\u{3bb}>\nsubject";
const FILTER_SOURCE: &str =
    " colour = 'red' AND sys.Label = 'bridge caf\u{e9} & <\u{3bb}>\nsubject' ";
const ACTION_SOURCE: &str = " /* bridge v2 */ REMOVE audit; REMOVE user.[drop];\nSET [MiXeD Target]=' caf\u{e9} & <\u{3bb}>\nO''Brien '; SET user.enabled=FALSE; SET added=+23; SET number=-7; SET [RuleName]='ignored'; ";
const BODY: &str = "bridge-body-caf\u{e9}-<\u{3bb}>\n";
const SUFFIXES: [&str; 2] = ["named", "connection"];
const COPY_SEQUENCES: [(u64, u64); 2] = [(2, 3), (5, 6)];

fn copy_sequences(cycle: usize) -> (u64, u64) {
    COPY_SEQUENCES[cycle]
}

fn topic() -> EntityPath {
    EntityPath::new(TOPIC).expect("fixed bridge topic")
}
fn subscription(name: &str) -> SubscriptionName {
    SubscriptionName::new(name).expect("fixed bridge subscription")
}
fn config() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
        default_time_to_live_millis: None,
        ..SubscriptionConfig::default()
    }
}

fn correlation() -> CorrelationFilter {
    CorrelationFilter {
        subject: Some(SUBJECT.into()),
        properties: BTreeMap::from([("colour".into(), MessageValue::String("red".into()))]),
        ..CorrelationFilter::default()
    }
}

fn expected_rule(suffix: &str, alpha: bool, issued_at: Timestamp) -> TestResult<RuleDefinition> {
    Ok(RuleDefinition {
        name: RuleName::new(format!(
            "{}-{suffix}",
            if alpha { "Bridge" } else { "Sibling" }
        ))?,
        filter: if alpha {
            RuleFilter::Sql(SqlFilter::new(FILTER_SOURCE)?)
        } else {
            RuleFilter::Correlation(correlation())
        },
        action: if alpha {
            Some(SqlAction::new(ACTION_SOURCE)?)
        } else {
            None
        },
        created_at: issued_at,
    })
}

pub(super) fn seed(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            topic(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            }
        )?,
        CommandOutcome::TopicCreated
    );
    for name in ["Alpha", "beta"] {
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::CreateSubscription {
                    name: subscription(name),
                    config: config(),
                }
            )?,
            CommandOutcome::SubscriptionCreated
        );
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                topic(),
                CommandKind::DeleteRule {
                    subscription: subscription(name),
                    name: RuleName::new("$Default")?,
                }
            )?,
            CommandOutcome::RuleDeleted
        );
    }
    let retained = EntityPath::new(RETAINED)?;
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            retained.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            }
        )?,
        CommandOutcome::QueueCreated
    );
    assert_eq!(
        handle.submit_blocking(
            namespace.clone(),
            retained,
            CommandKind::Send {
                message_id: "bridge-unrelated-message".into(),
                body: vec![0x5a; 1536],
                time_to_live_millis: None,
                session_id: None,
            }
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    Ok(())
}

pub(super) struct Oracle {
    initial: BTreeMap<Vec<u8>, Vec<u8>>,
    first_batch: usize,
    first_reading: usize,
    started_at: Timestamp,
}

impl Oracle {
    pub(super) fn new(
        snapshot: &StoreSnapshot,
        first_batch: usize,
        first_reading: usize,
    ) -> TestResult<Self> {
        let initial: BTreeMap<_, _> = snapshot.entries().iter().cloned().collect();
        let started_at = codec::decode(initial.get(&keys::clock()).expect("seed Clock"))?;
        Ok(Self {
            initial,
            first_batch,
            first_reading,
            started_at,
        })
    }

    pub(super) fn check<S: StateStore>(
        &self,
        store: &S,
        namespace: &NamespaceName,
        all_batches: &[WriteBatch],
        readings: &[Timestamp],
        finished_at: Timestamp,
    ) -> TestResult {
        let batches = &all_batches[self.first_batch..];
        let readings = &readings[self.first_reading..];
        assert!(
            !batches.is_empty() && !readings.is_empty(),
            "joint clock/store probes were not used"
        );
        let earliest = Timestamp::from_millis(self.started_at.as_millis().saturating_sub(500));
        let latest = finished_at.saturating_add_millis(500);
        assert!(
            readings.iter().all(|reading| *reading >= earliest
                && *reading <= latest
                && reading.as_millis() > 1_600_000_000_000
                && reading.as_millis() < u64::MAX),
            "instrumented real clock observations are implausible"
        );
        let observed: BTreeSet<_> = readings.iter().copied().collect();
        let mut previous = self.started_at;
        let mut rule_batches = Vec::new();
        let mut publications = Vec::new();
        let mut originals = BTreeMap::<(String, u64), MessageRecord>::new();
        let mut acquired = BTreeSet::new();
        let mut completed = BTreeSet::new();
        let rule_scope = keys::topic_rule_prefix(namespace, &topic());
        for batch in batches {
            let issued_at = batch_clock(batch)?;
            assert!(
                issued_at >= previous
                    && issued_at >= earliest
                    && issued_at <= latest
                    && issued_at.as_millis() > 1_600_000_000_000
                    && issued_at.as_millis() < u64::MAX,
                "committed bridge Clock is regressed or implausible"
            );
            assert!(
                observed.contains(&issued_at) || issued_at == previous,
                "committed Clock has neither a real reading nor the permitted prior-time clamp"
            );
            previous = issued_at;
            if batch
                .mutations()
                .iter()
                .any(|mutation| mutation_key(mutation).starts_with(&rule_scope))
            {
                rule_batches.push((batch, issued_at));
            }
            let mut ready = Vec::new();
            for mutation in batch.mutations() {
                if let Mutation::Put { key, value } = mutation {
                    for name in ["Alpha", "beta"] {
                        let entity = topic().subscription(&subscription(name))?;
                        if key.starts_with(&keys::message_prefix(namespace, &entity)) {
                            let message: MessageRecord = codec::decode(value)?;
                            assert_eq!(key, &keys::message(namespace, &entity, message.sequence));
                            let identity = (name.to_owned(), message.sequence.as_u64());
                            if message.state == MessageState::Ready {
                                assert!(
                                    originals.insert(identity, message.clone()).is_none(),
                                    "bridge repeated a native publication or unexpectedly abandoned a copy"
                                );
                                ready.push((name, message));
                            } else if let MessageState::Locked {
                                token,
                                locked_until,
                            } = message.state
                            {
                                let mut expected = originals
                                    .get(&identity)
                                    .expect("receive must follow publication")
                                    .clone();
                                assert!(
                                    acquired.insert(identity.clone()),
                                    "bridge reacquired a completed or already-held copy"
                                );
                                assert_eq!(token.as_u64(), if identity.1 < 4 { 1 } else { 2 });
                                assert_eq!(
                                    locked_until,
                                    issued_at
                                        .saturating_add_millis(domain::MAX_LOCK_DURATION_MILLIS)
                                );
                                expected.delivery_count = 1;
                                expected.state = MessageState::Locked {
                                    token,
                                    locked_until,
                                };
                                assert_eq!(
                                    message, expected,
                                    "bridge native receive changed retained content or lifetime"
                                );
                            } else {
                                panic!("bridge produced an unexpected native message state");
                            }
                        }
                    }
                } else if let Mutation::Delete { key } = mutation {
                    for name in ["Alpha", "beta"] {
                        let entity = topic().subscription(&subscription(name))?;
                        if key.starts_with(&keys::message_prefix(namespace, &entity)) {
                            let sequence = keys::trailing_sequence(key)
                                .expect("bridge message key sequence")
                                .as_u64();
                            assert_eq!(
                                key,
                                &keys::message(namespace, &entity, SequenceNumber::new(sequence))
                            );
                            let identity = (name.to_owned(), sequence);
                            assert!(
                                acquired.contains(&identity) && completed.insert(identity),
                                "bridge settlement removed an unacquired or already-completed copy"
                            );
                        }
                    }
                }
            }
            if !ready.is_empty() {
                publications.push((ready, issued_at));
            }
        }
        assert_eq!(
            originals.len(),
            4,
            "bridge needs four observed native ready publications"
        );
        assert_eq!(
            acquired.len(),
            4,
            "bridge needs four observed native acquisitions"
        );
        assert_eq!(
            completed, acquired,
            "bridge did not settle every acquired native copy exactly once"
        );
        check_rule_batches(namespace, &rule_batches)?;
        assert_eq!(
            publications.len(),
            2,
            "bridge needs two captured matching publication batches"
        );
        for (cycle, (ready, issued_at)) in publications.iter().enumerate() {
            assert_eq!(
                ready.len(),
                2,
                "matching publication needs precisely two native ready copies"
            );
            let by_name: BTreeMap<_, _> =
                ready.iter().map(|(name, record)| (*name, record)).collect();
            assert_eq!(by_name.len(), 2);
            let original = by_name["beta"];
            let (base_sequence, action_sequence) = copy_sequences(cycle);
            check_original(
                original,
                SUFFIXES[cycle],
                base_sequence,
                *issued_at,
                earliest,
                latest,
            )?;
            let changed = by_name["Alpha"];
            let mut expected = original.clone();
            expected.sequence = SequenceNumber::new(action_sequence);
            let properties = &mut expected
                .envelope
                .as_mut()
                .expect("bridge base envelope")
                .application_properties;
            properties.remove("audit");
            properties.remove("drop");
            properties.insert(
                "MiXeD Target".into(),
                MessageValue::String(" caf\u{e9} & <\u{3bb}>\nO'Brien ".into()),
            );
            properties.insert("enabled".into(), MessageValue::Bool(false));
            properties.insert("added".into(), MessageValue::Long(23));
            properties.insert("number".into(), MessageValue::Long(-7));
            properties.insert(
                "RuleName".into(),
                MessageValue::String(format!("Bridge-{}", SUFFIXES[cycle])),
            );
            assert_eq!(
                changed, &expected,
                "HTTPS action did not produce the exact native v2 transformed record"
            );
        }
        let final_snapshot = store.snapshot()?;
        let actual: BTreeMap<_, _> = final_snapshot.entries().iter().cloned().collect();
        let mut expected = self.initial.clone();
        expected.insert(keys::clock(), codec::encode(&previous)?);
        expected.insert(
            keys::queue_counters(namespace, &topic()),
            codec::encode(&QueueCounters {
                next_sequence: 7,
                next_lock_token: 1,
            })?,
        );
        for name in ["Alpha", "beta"] {
            expected.insert(
                keys::queue_counters(namespace, &topic().subscription(&subscription(name))?),
                codec::encode(&QueueCounters {
                    next_sequence: 1,
                    next_lock_token: 3,
                })?,
            );
        }
        assert_eq!(
            actual, expected,
            "bridge changed unrelated bytes or retained unexpected owned runtime state"
        );
        check_final(store, namespace)?;
        Ok(())
    }
}

fn mutation_key(mutation: &Mutation) -> &[u8] {
    match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key } => key,
    }
}

fn batch_clock(batch: &WriteBatch) -> TestResult<Timestamp> {
    let clocks: Vec<_> = batch
        .mutations()
        .iter()
        .filter(|mutation| mutation_key(mutation) == keys::clock())
        .collect();
    assert_eq!(
        clocks.len(),
        1,
        "every committed bridge batch must have one Clock"
    );
    let Mutation::Put { value, .. } = clocks[0] else {
        panic!("Clock must be a Put")
    };
    assert_eq!(
        batch.mutations().last(),
        Some(clocks[0]),
        "Clock must end its committed batch"
    );
    Ok(codec::decode(value)?)
}

fn check_rule_batches(
    namespace: &NamespaceName,
    batches: &[(&WriteBatch, Timestamp)],
) -> TestResult {
    assert_eq!(
        batches.len(),
        8,
        "two constructors must commit four creates and four deletes"
    );
    for (index, (batch, stamp)) in batches.iter().enumerate() {
        let cycle = index / 4;
        let position = index % 4;
        let alpha = position % 2 == 0;
        let expected = expected_rule(SUFFIXES[cycle], alpha, *stamp)?;
        let key = keys::rule(
            namespace,
            &topic(),
            &subscription(if alpha { "Alpha" } else { "beta" }),
            &expected.name,
        );
        let first = if position < 2 {
            let Some(Mutation::Put { value, .. }) = batch.mutations().first() else {
                panic!("bridge rule create needs a Put")
            };
            let actual: RuleDefinition = codec::decode(value)?;
            assert_eq!(
                actual, expected,
                "decoded bridge RulePut lost exact source, native types or created_at"
            );
            assert_eq!(actual.created_at, *stamp);
            if let RuleFilter::Sql(sql) = &actual.filter {
                assert_eq!(sql.semantic_version(), 1);
            }
            if let Some(action) = &actual.action {
                assert_eq!(action.semantic_version(), 2);
            }
            Mutation::Put {
                key,
                value: codec::encode(&expected)?,
            }
        } else {
            Mutation::Delete { key }
        };
        assert_eq!(
            batch.mutations(),
            &[
                first,
                Mutation::Put {
                    key: keys::clock(),
                    value: codec::encode(stamp)?
                }
            ],
            "bridge rule CRUD must commit exactly its ordered Rule mutation and matching Clock"
        );
    }
    Ok(())
}

fn source_properties() -> BTreeMap<String, MessageValue> {
    BTreeMap::from([
        ("colour".into(), MessageValue::String("red".into())),
        ("number".into(), MessageValue::Long(42)),
        ("enabled".into(), MessageValue::Bool(true)),
        ("drop".into(), MessageValue::String("remove-drop".into())),
        ("audit".into(), MessageValue::String("remove-audit".into())),
        ("Audit".into(), MessageValue::String("case-retained".into())),
        (
            "RuleName".into(),
            MessageValue::String("publisher-rule".into()),
        ),
        (
            "rulename".into(),
            MessageValue::String("lowercase-retained".into()),
        ),
    ])
}

fn check_original(
    record: &MessageRecord,
    suffix: &str,
    sequence: u64,
    stamp: Timestamp,
    earliest: Timestamp,
    latest: Timestamp,
) -> TestResult {
    assert_eq!(record.sequence, SequenceNumber::new(sequence));
    assert_eq!(record.message_id, format!("bridge-{suffix}-match"));
    assert_eq!(record.body, BODY.as_bytes());
    assert_eq!(record.enqueued_at, stamp);
    assert_eq!(
        record.expires_at,
        Some(stamp.saturating_add_millis(120_000))
    );
    assert_eq!(record.delivery_count, 0);
    assert_eq!(record.state, MessageState::Ready);
    assert!(
        record.session_id.is_none()
            && record.dead_letter.is_none()
            && record.scheduled_enqueue_time.is_none()
    );
    let envelope = record
        .envelope
        .as_ref()
        .expect("SDK bridge native envelope");
    assert_eq!(envelope.header, Some(MessageHeader::default()));
    assert_eq!(
        envelope.body,
        MessageBody::Data(vec![BODY.as_bytes().to_vec()])
    );
    assert_eq!(envelope.application_properties, source_properties());
    assert!(envelope.message_annotations.is_empty());
    assert_eq!(
        envelope.footer,
        BTreeMap::from([(
            AnnotationKey::Symbol("producer-checksum".into()),
            MessageValue::String("bridge-checksum".into()),
        )])
    );
    let properties = &envelope.properties;
    let creation = properties.creation_time.expect("SDK TTL creation time");
    let expiry = properties
        .absolute_expiry_time
        .expect("SDK TTL absolute expiry");
    assert_eq!(expiry.checked_sub(creation), Some(120_000));
    assert!(
        u64::try_from(creation)? >= earliest.as_millis()
            && u64::try_from(creation)? <= latest.as_millis()
    );
    let expected = domain::MessageProperties {
        message_id: Some(MessageIdentifier::String(format!("bridge-{suffix}-match"))),
        correlation_id: Some(MessageIdentifier::String("bridge-correlation".into())),
        subject: Some(SUBJECT.into()),
        content_type: Some("text/plain".into()),
        content_encoding: Some("utf-8".into()),
        to: Some("bridge-destination".into()),
        reply_to: Some("bridge-reply".into()),
        reply_to_group_id: Some("bridge-reply-session".into()),
        creation_time: Some(creation),
        absolute_expiry_time: Some(expiry),
        ..domain::MessageProperties::default()
    };
    assert_eq!(
        properties, &expected,
        "native sibling lost a fixed system field or retained an extra property"
    );
    Ok(())
}

pub(super) fn check_final<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    let machine = StateMachine::new(store.clone());
    assert_eq!(
        machine.topic_config(namespace, &topic())?,
        Some(TopicConfig::default())
    );
    let subscriptions = machine.subscriptions(namespace, &topic())?;
    assert_eq!(
        subscriptions
            .iter()
            .map(|item| item.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Alpha", "beta"])
    );
    for name in ["Alpha", "beta"] {
        let name = subscription(name);
        assert_eq!(
            machine.subscription_config(namespace, &topic(), &name)?,
            Some(config())
        );
        assert!(machine.rules(namespace, &topic(), &name)?.is_empty());
        let entity = topic().subscription(&name)?;
        for entity in [entity.clone(), entity.dead_letter_queue()?] {
            check_empty_runtime(store, namespace, &entity)?;
        }
    }
    check_empty_runtime(store, namespace, &topic())?;
    Ok(())
}

fn check_empty_runtime<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> TestResult {
    for prefix in [
        keys::message_prefix(namespace, entity),
        keys::ready_prefix(namespace, entity),
        keys::lock_prefix(namespace, entity),
        keys::expiry_prefix(namespace, entity),
        keys::scheduled_prefix(namespace, entity),
        keys::duplicate_history_prefix(namespace, entity),
        keys::duplicate_history_expiry_prefix(namespace, entity),
        keys::entity_session_prefix(namespace, entity),
        keys::entity_session_ready_prefix(namespace, entity),
        keys::session_lock_prefix(namespace, entity),
        keys::session_message_lock_reverse_prefix(namespace, entity),
        keys::session_message_lock_summary_prefix(namespace, entity),
        keys::queue_capacity_usage(namespace, entity),
        keys::message_charge_prefix(namespace, entity),
    ] {
        assert!(
            store.scan_prefix(&prefix, 1)?.is_empty(),
            "bridge retained runtime rows in {entity}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_bridge_declarations_preserve_native_versions_types_and_sequence_plan() -> TestResult {
        let csharp =
            include_str!("../../../../../conformance/dotnet-current/AtomRuleMessageFlowCases.cs");
        assert!(csharp.contains("private const string Topic = \"sdk-atom-rule-flow\";"));
        assert!(csharp.contains("private const string ActionSource = \" /* bridge v2 */ REMOVE audit; REMOVE user.[drop];\\nSET [MiXeD Target]=' caf\\u00E9 & <\\u03BB>\\nO''Brien '; SET user.enabled=FALSE; SET added=+23; SET number=-7; SET [RuleName]='ignored'; \";"));
        for declaration in [
            "private const string Subject = \"bridge caf\\u00E9 & <\\u03BB>\\nsubject\";",
            "private const string FilterSource = \" colour = 'red' AND sys.Label = 'bridge caf\\u00E9 & <\\u03BB>\\nsubject' \";",
            "new ServiceBusMessage(\"bridge-body-caf\\u00E9-<\\u03BB>\\n\")",
            "message.ApplicationProperties[\"number\"] = 42L;",
            "message.ApplicationProperties[\"enabled\"] = true;",
            "message.GetRawAmqpMessage().Header.Durable = false;",
            "message.GetRawAmqpMessage().Header.Priority = 4;",
            "message.GetRawAmqpMessage().Header.FirstAcquirer = false;",
            "message.GetRawAmqpMessage().Footer[\"producer-checksum\"] = \"bridge-checksum\";",
            "long baseSequence = 2 + cycle * 3;",
        ] {
            assert!(
                csharp.contains(declaration),
                "bridge fixed declaration changed: {declaration}"
            );
        }
        let stamp = Timestamp::from_millis(1_700_000_000_000);
        let mut batches = Vec::new();
        for suffix in SUFFIXES {
            for creating in [true, false] {
                for alpha in [true, false] {
                    let rule = expected_rule(suffix, alpha, stamp)?;
                    let key = keys::rule(
                        &NamespaceName::new("tenant")?,
                        &topic(),
                        &subscription(if alpha { "Alpha" } else { "beta" }),
                        &rule.name,
                    );
                    let batch = if creating {
                        WriteBatch::default().put(key, codec::encode(&rule)?)
                    } else {
                        WriteBatch::default().delete(key)
                    };
                    batches.push(batch.put(keys::clock(), codec::encode(&stamp)?));
                }
            }
        }
        let refs: Vec<_> = batches.iter().map(|batch| (batch, stamp)).collect();
        check_rule_batches(&NamespaceName::new("tenant")?, &refs)?;
        assert_eq!(source_properties()["number"], MessageValue::Long(42));
        assert_eq!(source_properties()["enabled"], MessageValue::Bool(true));
        assert_eq!(copy_sequences(0), (2, 3));
        assert_eq!(copy_sequences(1), (5, 6));
        assert_eq!(batch_clock(&batches[0])?, stamp);
        let mut missing = batches.clone();
        missing[0] = WriteBatch::default().put(keys::clock(), codec::encode(&stamp)?);
        let mut extra = batches.clone();
        extra[0].push_delete(b"unexpected-rule-mutation".to_vec());
        let first = batches[0].mutations()[0].clone();
        let Mutation::Put { key, value } = first else {
            panic!("positive create fixture")
        };
        let mut reordered = batches.clone();
        reordered[0] = WriteBatch::default()
            .put(keys::clock(), codec::encode(&stamp)?)
            .put(key.clone(), value);
        let mut changed_created = batches.clone();
        let mut rule = expected_rule(SUFFIXES[0], true, stamp)?;
        rule.created_at = Timestamp::from_millis(stamp.as_millis() - 1);
        changed_created[0] = WriteBatch::default()
            .put(key.clone(), codec::encode(&rule)?)
            .put(keys::clock(), codec::encode(&stamp)?);
        let mut missing_clock = batches.clone();
        missing_clock[0] = WriteBatch::default().put(
            key.clone(),
            codec::encode(&expected_rule(SUFFIXES[0], true, stamp)?)?,
        );
        let mut malformed_clock = batches.clone();
        malformed_clock[0] = WriteBatch::default()
            .put(
                key,
                codec::encode(&expected_rule(SUFFIXES[0], true, stamp)?)?,
            )
            .put(keys::clock(), vec![0xff]);
        for malformed in [
            missing,
            missing_clock,
            extra,
            reordered,
            changed_created,
            malformed_clock,
        ] {
            let refs: Vec<_> = malformed.iter().map(|batch| (batch, stamp)).collect();
            assert!(
                !matches!(
                    std::panic::catch_unwind(|| check_rule_batches(
                        &NamespaceName::new("tenant").expect("fixture namespace"),
                        &refs
                    )),
                    Ok(Ok(()))
                ),
                "malformed Rule+Clock batch was not rejected"
            );
        }
        let fewer: Vec<_> = batches[..7].iter().map(|batch| (batch, stamp)).collect();
        assert!(!matches!(
            std::panic::catch_unwind(|| check_rule_batches(
                &NamespaceName::new("tenant").expect("fixture namespace"),
                &fewer
            )),
            Ok(Ok(()))
        ));
        Ok(())
    }
}
