//! Selected rule integrity, not a global rule catalog health check or repair API.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, CorrelationFilter,
    CorrelationValue, DurableProposal, EntityPath, FilterProperties, IndexedApplyError,
    IndexedApplyOutcome, IndexedWriter, MAX_CORRELATION_FILTER_BYTES, MAX_RULE_PAGE,
    MAX_SUBSCRIPTION_RULES, MessageEnvelope, MessageInput, NamespaceName, QueueConfig, ReceiveMode,
    RuleConfigError, RuleDefinition, RuleFilter, RuleName, SequenceNumber, SessionId, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Scan {
    prefix: Key,
    start: Key,
    limit: usize,
    returned: usize,
}

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    scans: Vec<Scan>,
    batches: Vec<WriteBatch>,
    snapshots: usize,
}

#[derive(Debug, Default)]
struct Controls {
    trace: Trace,
    fail_scan: Option<Key>,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    controls: Arc<Mutex<Controls>>,
}

impl<S> Observed<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            controls: Arc::new(Mutex::new(Controls::default())),
        }
    }
    fn reset(&self) {
        self.controls.lock().unwrap().trace = Trace::default();
    }
    fn trace(&self) -> Trace {
        self.controls.lock().unwrap().trace.clone()
    }
}

fn scan_failure() -> StorageError {
    StorageError::Backend {
        operation: "scan selected subscription rules",
        detail: String::from("injected rule scan failure"),
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.controls.lock().unwrap().trace.gets.push(key.to_vec());
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls
            .lock()
            .unwrap()
            .trace
            .batches
            .push(batch.clone());
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.controls.lock().unwrap().trace.snapshots += 1;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let fail = self.controls.lock().unwrap().fail_scan.as_deref() == Some(prefix);
        let result = if fail {
            Err(scan_failure())
        } else {
            self.inner.scan_from(prefix, start, limit)
        };
        self.controls.lock().unwrap().trace.scans.push(Scan {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
            returned: result.as_ref().map_or(0, Vec::len),
        });
        result
    }
}

struct Fixture<P: StoreProvider> {
    machine: StateMachine<Observed<P::Store>>,
    namespace: NamespaceName,
    topic: EntityPath,
    subscription: EntityPath,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("events")?;
        let name = SubscriptionName::new("audit")?;
        let subscription = topic.subscription(&name)?;
        let fixture = Self {
            machine: StateMachine::new(Observed::new(provider.open()?)),
            namespace,
            topic,
            subscription,
            provider,
        };
        assert_eq!(
            fixture.at(
                &fixture.topic,
                0,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated
        );
        assert_eq!(
            fixture.at(
                &fixture.topic,
                10,
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig::default(),
                }
            )?,
            CommandOutcome::SubscriptionCreated {
                entity: fixture.subscription.clone()
            }
        );
        Ok(fixture)
    }
    fn command(&self, entity: &EntityPath, at: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    }
    fn at(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&self.command(entity, at, kind))
    }
    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }
    fn put(&self, key: Key, value: Value) -> Result<(), StorageError> {
        self.raw().apply(WriteBatch::default().put(key, value))
    }
    fn row(&self, name: &str, filter: RuleFilter) -> Result<RuleDefinition, Box<dyn Error>> {
        Ok(RuleDefinition {
            name: RuleName::new(name)?,
            filter,
            created_at: Timestamp::from_millis(u64::MAX),
        })
    }
    fn rule_key(&self, name: &RuleName) -> Key {
        keys::subscription_rule(&self.namespace, &self.subscription, name)
    }
    fn install(&self, definition: &RuleDefinition) -> Result<(), Box<dyn Error>> {
        self.put(self.rule_key(&definition.name), codec::encode(definition)?)?;
        Ok(())
    }
    fn reset(&self) {
        self.machine.store().reset();
    }
    fn trace(&self) -> Trace {
        self.machine.store().trace()
    }
    fn unchanged(&self, before: &StoreSnapshot) -> Result<(), Box<dyn Error>> {
        let trace = self.trace();
        assert!(trace.batches.is_empty(), "refusal must not attempt a write");
        assert_eq!(trace.snapshots, 0, "no operational snapshot fallback");
        assert_eq!(
            &self.raw().snapshot()?,
            before,
            "every byte, Clock and sequence stays exact"
        );
        Ok(())
    }
    fn reject(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
        expected: BrokerError,
    ) -> Result<Trace, Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(self.at(entity, at, kind), Err(expected));
        let trace = self.trace();
        assert_eq!(trace.gets.first(), Some(&keys::clock()));
        self.unchanged(&before)?;
        Ok(trace)
    }
    fn reject_list(
        &self,
        skip: u32,
        max: u32,
        expected: BrokerError,
    ) -> Result<Trace, Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        self.reset();
        assert_eq!(
            self.machine
                .rules(&self.namespace, &self.subscription, skip, max),
            Err(expected)
        );
        let trace = self.trace();
        assert!(!trace.gets.contains(&keys::clock()));
        self.unchanged(&before)?;
        Ok(trace)
    }
    fn restore(&self, before: &StoreSnapshot) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for (key, _) in self.raw().snapshot()?.entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in before.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        self.raw().apply(batch)
    }
    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            machine,
            namespace,
            topic,
            subscription,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            machine: StateMachine::new(Observed::new(provider.open()?)),
            namespace,
            topic,
            subscription,
            provider,
        })
    }
    fn assert_rule_scan(&self, trace: &Trace, returned: usize) {
        let prefix = keys::subscription_rule_prefix(&self.namespace, &self.subscription);
        let scans: Vec<_> = trace
            .scans
            .iter()
            .filter(|scan| scan.prefix == prefix)
            .cloned()
            .collect();
        assert_eq!(
            scans,
            vec![Scan {
                start: prefix.clone(),
                prefix,
                limit: MAX_SUBSCRIPTION_RULES + 1,
                returned
            }]
        );
        assert_eq!(
            trace
                .scans
                .iter()
                .filter(|scan| scan.prefix.first() == Some(&0x10))
                .count(),
            1
        );
        assert!(
            !trace
                .gets
                .contains(&keys::queue_counters(&self.namespace, &self.topic))
        );
        assert!(
            !trace
                .gets
                .contains(&keys::queue_counters(&self.namespace, &self.subscription))
        );
    }
    fn assert_no_rules(&self, trace: &Trace) {
        assert!(
            trace
                .scans
                .iter()
                .all(|scan| scan.prefix.first() != Some(&0x10))
        );
    }
    fn schedule(&self) -> Result<(), Box<dyn Error>> {
        assert_eq!(
            self.at(&self.topic, 20, send(input("scheduled", Some(50))))?,
            published(&[1], &[])
        );
        Ok(())
    }
    fn reject_surfaces(&self, returned: usize) -> Result<(), Box<dyn Error>> {
        for skip in [0, u32::MAX] {
            let trace = self.reject_list(skip, 1, BrokerError::EntityMetadataCorrupt)?;
            self.assert_rule_scan(&trace, returned);
        }
        for (entity, kind) in [
            (
                &self.subscription,
                CommandKind::ListRules {
                    skip: 0,
                    max_rules: 1,
                },
            ),
            (&self.subscription, create("new-rule", RuleFilter::True)?),
            (&self.topic, send(input("immediate", None))),
            (
                &self.topic,
                CommandKind::SendBatch {
                    messages: vec![input("one", None), input("two", None)],
                },
            ),
            (
                &self.topic,
                CommandKind::SendBatch {
                    messages: vec![input("later", Some(200)), input("now", None)],
                },
            ),
            (&self.topic, CommandKind::ActivateScheduled),
        ] {
            let trace = self.reject(entity, 100, kind, BrokerError::EntityMetadataCorrupt)?;
            self.assert_rule_scan(&trace, returned);
        }
        Ok(())
    }
    fn healthy_companion(&self, names: &[&str]) -> Result<(), Box<dyn Error>> {
        let before = self.raw().snapshot()?;
        let rules = self
            .machine
            .rules(&self.namespace, &self.subscription, 0, MAX_RULE_PAGE)?;
        assert_eq!(
            rules
                .iter()
                .map(|rule| rule.name.display_name())
                .collect::<Vec<_>>(),
            names
        );
        assert_eq!(
            self.raw().snapshot()?,
            before,
            "listing does not normalize stored bytes"
        );
        assert_eq!(
            self.at(
                &self.subscription,
                30,
                create("new-rule", RuleFilter::False)?
            )?,
            CommandOutcome::RuleCreated
        );
        assert_eq!(
            self.at(&self.topic, 40, send(input("immediate", None)))?,
            published(&[2], std::slice::from_ref(&self.subscription))
        );
        assert_eq!(
            self.at(
                &self.topic,
                50,
                CommandKind::SendBatch {
                    messages: vec![input("one", None), input("two", None)]
                }
            )?,
            published(&[3, 4], std::slice::from_ref(&self.subscription))
        );
        assert_eq!(
            self.at(&self.topic, 60, CommandKind::ActivateScheduled)?,
            CommandOutcome::ScheduledActivated {
                activated: 1,
                deliverable_entities: vec![self.subscription.clone()],
            }
        );
        for (sequence, id) in [(2, "immediate"), (3, "one"), (4, "two"), (5, "scheduled")] {
            let CommandOutcome::Received(Some(delivery)) = self.at(
                &self.subscription,
                70,
                CommandKind::Receive {
                    mode: ReceiveMode::ReceiveAndDelete,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("actual routed copy");
            };
            assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
            assert_eq!(delivery.message_id, id);
            assert_eq!(delivery.body, b"payload");
        }
        Ok(())
    }
}

fn input(id: &str, due: Option<u64>) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: b"payload".to_vec(),
        scheduled_enqueue_at: due.map(Timestamp::from_millis),
        ..MessageInput::default()
    }
}

fn send(input: MessageInput) -> CommandKind {
    CommandKind::Send {
        message_id: input.message_id,
        body: input.body,
        time_to_live_millis: input.time_to_live_millis,
        session_id: input.session_id,
        scheduled_enqueue_at: input.scheduled_enqueue_at,
        envelope: input.envelope,
    }
}

fn create(name: &str, filter: RuleFilter) -> Result<CommandKind, Box<dyn Error>> {
    Ok(CommandKind::CreateRule {
        name: RuleName::new(name)?,
        filter,
    })
}

fn published(sequences: &[u64], subscriptions: &[EntityPath]) -> CommandOutcome {
    CommandOutcome::Published {
        sequences: sequences
            .iter()
            .map(|sequence| SequenceNumber::new(*sequence))
            .collect(),
        subscriptions: subscriptions.to_vec(),
    }
}

fn key_integrity<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new(provider)?;
    fixture.schedule()?;
    let baseline = fixture.raw().snapshot()?;
    let definition = fixture.row("MiXeD", RuleFilter::True)?;
    let prefix = keys::subscription_rule_prefix(&fixture.namespace, &fixture.subscription);
    let mut uppercase = prefix.clone();
    uppercase.extend_from_slice(b"MiXeD");
    let mut extra = fixture.rule_key(&definition.name);
    extra.push(0);
    let mut malformed = prefix.clone();
    malformed.push(0xFF);
    for key in [
        fixture.rule_key(&RuleName::new("different")?),
        uppercase,
        extra,
        prefix,
        malformed,
    ] {
        fixture.restore(&baseline)?;
        fixture.put(key, codec::encode(&definition)?)?;
        let corrupt = fixture.raw().snapshot()?;
        fixture.reject_surfaces(2)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.raw().snapshot()?, corrupt);
        fixture.reject_surfaces(2)?;
        // Controlled fixture repair, not a domain recovery or normalization API.
        fixture.restore(&baseline)?;
        fixture.install(&definition)?;
        let default = fixture.row("$DeFaUlT", RuleFilter::True)?;
        fixture.install(&default)?;
        fixture.healthy_companion(&["$DeFaUlT", "MiXeD"])?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.subscription, 1, 1)?[0],
            definition
        );
    }
    Ok(())
}

fn invalid_filters() -> Result<Vec<RuleFilter>, Box<dyn Error>> {
    let value = CorrelationValue::new(vec![0xA1, 0, 0xFF])?;
    let oversized: CorrelationValue = codec::decode(&codec::encode(&vec![
        0u8;
        MAX_CORRELATION_FILTER_BYTES
            + 1
    ])?)?;
    Ok(vec![
        RuleFilter::Correlation(CorrelationFilter::default()),
        RuleFilter::Correlation(CorrelationFilter {
            session_id: Some(String::from("unsupported")),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(String::from("Color"), value.clone())]),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([
                (String::from("Color"), value.clone()),
                (String::from("color"), value),
            ]),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            subject: Some("x".repeat(MAX_CORRELATION_FILTER_BYTES + 1)),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(String::from("color"), oversized)]),
            ..CorrelationFilter::default()
        }),
    ])
}

fn valid_correlation() -> Result<CorrelationFilter, Box<dyn Error>> {
    Ok(CorrelationFilter {
        subject: Some(String::from("CaseSensitive")),
        application_properties: BTreeMap::from([(
            String::from("color"),
            CorrelationValue::new(vec![0xA1, 0, 0xFF])?,
        )]),
        ..CorrelationFilter::default()
    })
}

fn filter_integrity<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new(provider)?;
    fixture.schedule()?;
    let baseline = fixture.raw().snapshot()?;
    for filter in invalid_filters()? {
        fixture.restore(&baseline)?;
        fixture.install(&fixture.row("z-bad", filter)?)?;
        let corrupt = fixture.raw().snapshot()?;
        fixture.reject_surfaces(2)?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.raw().snapshot()?, corrupt);
        fixture.reject_surfaces(2)?;
        fixture.restore(&baseline)?;
        fixture.install(&fixture.row("z-valid", RuleFilter::Correlation(valid_correlation()?))?)?;
        fixture.healthy_companion(&["$default", "z-valid"])?;
    }
    for filter in [
        RuleFilter::True,
        RuleFilter::False,
        RuleFilter::Correlation(CorrelationFilter {
            subject: Some("x".repeat(MAX_CORRELATION_FILTER_BYTES)),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(
                String::new(),
                CorrelationValue::new(vec![0; MAX_CORRELATION_FILTER_BYTES])?,
            )]),
            ..CorrelationFilter::default()
        }),
    ] {
        fixture.restore(&baseline)?;
        let definition = fixture.row("z-valid", filter)?;
        fixture.install(&definition)?;
        assert_eq!(
            fixture
                .machine
                .rules(&fixture.namespace, &fixture.subscription, 1, 1)?[0],
            definition
        );
        fixture.healthy_companion(&["$default", "z-valid"])?;
    }
    fixture.restore(&baseline)?;
    fixture.install(&fixture.row("$DeFaUlT", RuleFilter::False)?)?;
    let filter = valid_correlation()?;
    fixture.install(&fixture.row("MiXeD", RuleFilter::Correlation(filter.clone()))?)?;
    let rule_bytes = fixture
        .raw()
        .get(&fixture.rule_key(&RuleName::new("MiXeD")?))?
        .unwrap();
    let rules = fixture
        .machine
        .rules(&fixture.namespace, &fixture.subscription, 0, 2)?;
    assert_eq!(rules[1].filter, RuleFilter::Correlation(filter));
    assert_eq!(rules[1].created_at, Timestamp::from_millis(u64::MAX));
    for (id, subject, opaque, destinations) in [
        (
            "subject-case",
            "casesensitive",
            vec![0xA1, 0, 0xFF],
            Vec::new(),
        ),
        (
            "opaque-type",
            "CaseSensitive",
            vec![0xA2, 0, 0xFF],
            Vec::new(),
        ),
        (
            "exact",
            "CaseSensitive",
            vec![0xA1, 0, 0xFF],
            vec![fixture.subscription.clone()],
        ),
    ] {
        let mut message = input(id, None);
        message.envelope = Some(
            MessageEnvelope::new(b"payload".to_vec()).with_filter_properties(FilterProperties {
                subject: Some(subject.to_owned()),
                application_properties: BTreeMap::from([(
                    String::from("CoLoR"),
                    CorrelationValue::new(opaque)?,
                )]),
                ..FilterProperties::default()
            }),
        );
        let CommandOutcome::Published { subscriptions, .. } =
            fixture.at(&fixture.topic, 30, send(message))?
        else {
            panic!("publication");
        };
        assert_eq!(subscriptions, destinations);
    }
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        &fixture.subscription,
        40,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("only the exact system/opaque match routes");
    };
    assert_eq!(delivery.message_id, "exact");
    assert_eq!(delivery.sequence, SequenceNumber::new(4));
    assert_eq!(
        fixture.at(
            &fixture.subscription,
            40,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            }
        )?,
        CommandOutcome::Received(None)
    );
    assert_eq!(
        fixture
            .raw()
            .get(&fixture.rule_key(&RuleName::new("MiXeD")?))?,
        Some(rule_bytes)
    );
    Ok(())
}

fn bounded_order<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new(provider)?;
    fixture.schedule()?;
    let mut batch = WriteBatch::default();
    for offset in 1..MAX_SUBSCRIPTION_RULES {
        let rule = fixture.row(&format!("rule-{offset:04}"), RuleFilter::True)?;
        batch.push_put(fixture.rule_key(&rule.name), codec::encode(&rule)?);
    }
    fixture.raw().apply(batch)?;
    let full = fixture.raw().snapshot()?;
    let cap = BrokerError::RuleLimitExceeded {
        maximum: MAX_SUBSCRIPTION_RULES,
    };
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create("new-rule", RuleFilter::True)?,
        cap.clone(),
    )?;
    fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES);
    assert_eq!(
        fixture
            .machine
            .rules(&fixture.namespace, &fixture.subscription, 0, 1)?
            .len(),
        1
    );
    assert_eq!(
        fixture.at(&fixture.topic, 30, send(input("full-valid", None)))?,
        published(&[2], &[fixture.subscription.clone()])
    );
    fixture.restore(&full)?;
    let last = RuleName::new(format!("rule-{:04}", MAX_SUBSCRIPTION_RULES - 1))?;
    for corrupt in [
        fixture.row("wrong-name", RuleFilter::True)?,
        fixture.row(
            last.display_name(),
            RuleFilter::Correlation(CorrelationFilter::default()),
        )?,
    ] {
        fixture.restore(&full)?;
        fixture.put(fixture.rule_key(&last), codec::encode(&corrupt)?)?;
        fixture.reject_surfaces(MAX_SUBSCRIPTION_RULES)?;
        let snapshot = fixture.raw().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.raw().snapshot()?, snapshot);
        fixture.reject_surfaces(MAX_SUBSCRIPTION_RULES)?;
        let overfull = fixture.row(
            "zz-extra",
            RuleFilter::Correlation(CorrelationFilter::default()),
        )?;
        fixture.install(&overfull)?;
        let trace = fixture.reject_list(0, 1, cap.clone())?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
        let trace = fixture.reject(
            &fixture.subscription,
            100,
            create("new-rule", RuleFilter::True)?,
            cap.clone(),
        )?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
        // The next row is outside the unchanged MAX+1 materialization bound.
        let unread = fixture.rule_key(&RuleName::new("zzz-unread")?);
        fixture.put(unread, Vec::new())?;
        let trace = fixture.reject_list(0, 1, cap.clone())?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
        let codec_error = BrokerError::from(codec::decode::<RuleDefinition>(&[]).unwrap_err());
        fixture.put(fixture.rule_key(&overfull.name), Vec::new())?;
        let trace = fixture.reject_list(0, 1, codec_error.clone())?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
        let trace = fixture.reject(
            &fixture.subscription,
            100,
            create("new-rule", RuleFilter::True)?,
            codec_error,
        )?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
        fixture.put(fixture.rule_key(&RuleName::new("$default")?), vec![255])?;
        let first_codec = BrokerError::from(codec::decode::<RuleDefinition>(&[255]).unwrap_err());
        let trace = fixture.reject_list(0, 1, first_codec)?;
        fixture.assert_rule_scan(&trace, MAX_SUBSCRIPTION_RULES + 1);
    }
    fixture.restore(&full)?;
    assert_eq!(
        fixture.at(
            &fixture.topic,
            30,
            CommandKind::SendBatch {
                messages: vec![input("one", None), input("two", None)]
            }
        )?,
        published(&[2, 3], &[fixture.subscription.clone()])
    );
    assert_eq!(
        fixture.at(&fixture.topic, 60, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![fixture.subscription.clone()],
        }
    );
    Ok(())
}

fn priority<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider)?;
    fixture.schedule()?;
    fixture.install(&fixture.row(
        "z-bad",
        RuleFilter::Correlation(CorrelationFilter::default()),
    )?)?;
    let baseline = fixture.raw().snapshot()?;
    for (max, error) in [
        (0, BrokerError::EmptyRulePage),
        (
            MAX_RULE_PAGE + 1,
            BrokerError::RulePageTooLarge {
                requested: MAX_RULE_PAGE + 1,
                maximum: MAX_RULE_PAGE,
            },
        ),
    ] {
        let trace = fixture.reject_list(0, max, error.clone())?;
        assert!(trace.gets.is_empty());
        assert!(trace.scans.is_empty());
        let trace = fixture.reject(
            &fixture.subscription,
            100,
            CommandKind::ListRules {
                skip: 0,
                max_rules: max,
            },
            error,
        )?;
        assert_eq!(trace.gets, vec![keys::clock()]);
        assert!(trace.scans.is_empty());
    }
    let trace = fixture.reject(
        &fixture.subscription,
        19,
        create("new-rule", RuleFilter::True)?,
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(20),
            proposed: Timestamp::from_millis(19),
        },
    )?;
    assert_eq!(trace.gets, vec![keys::clock()]);
    let invalid = RuleFilter::Correlation(CorrelationFilter::default());
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create("$Default", invalid)?,
        BrokerError::RuleConfig(RuleConfigError::EmptyCorrelationFilter),
    )?;
    fixture.assert_no_rules(&trace);
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create("$Default", RuleFilter::True)?,
        BrokerError::RuleAlreadyExists {
            name: RuleName::new("$Default")?,
        },
    )?;
    fixture.assert_no_rules(&trace);
    fixture.put(fixture.rule_key(&RuleName::new("$default")?), Vec::new())?;
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create("$Default", RuleFilter::True)?,
        BrokerError::RuleAlreadyExists {
            name: RuleName::new("$Default")?,
        },
    )?;
    fixture.assert_no_rules(&trace);
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create(
            "$Default",
            RuleFilter::Correlation(CorrelationFilter::default()),
        )?,
        BrokerError::RuleConfig(RuleConfigError::EmptyCorrelationFilter),
    )?;
    fixture.assert_no_rules(&trace);
    fixture.restore(&baseline)?;
    for (kind, error) in [
        (
            CommandKind::SendBatch {
                messages: Vec::new(),
            },
            BrokerError::EmptyMessageBatch,
        ),
        (
            send(MessageInput {
                session_id: Some(SessionId::new("session")?),
                ..input("session", None)
            }),
            BrokerError::TopicSessionNotSupported,
        ),
        (
            send(input(
                &"x".repeat(domain::MAX_MESSAGE_ID_CHARACTERS + 1),
                None,
            )),
            BrokerError::MessageIdTooLong {
                characters: domain::MAX_MESSAGE_ID_CHARACTERS + 1,
                maximum: domain::MAX_MESSAGE_ID_CHARACTERS,
            },
        ),
        (
            send(MessageInput {
                body: vec![0; TopicConfig::default().max_message_bytes + 1],
                ..input("large", None)
            }),
            BrokerError::MessageTooLarge {
                body_bytes: TopicConfig::default().max_message_bytes + 1,
                maximum_bytes: TopicConfig::default().max_message_bytes,
            },
        ),
    ] {
        let trace = fixture.reject(&fixture.topic, 100, kind, error)?;
        fixture.assert_no_rules(&trace);
    }
    let mut message = input("invalid-projection", None);
    let value = CorrelationValue::new(vec![1])?;
    message.envelope = Some(
        MessageEnvelope::new(b"payload".to_vec()).with_filter_properties(FilterProperties {
            application_properties: BTreeMap::from([
                (String::from("Color"), value.clone()),
                (String::from("color"), value),
            ]),
            ..FilterProperties::default()
        }),
    );
    let trace = fixture.reject(
        &fixture.topic,
        100,
        send(message),
        BrokerError::RuleConfig(RuleConfigError::DuplicateApplicationProperty),
    )?;
    fixture.assert_no_rules(&trace);
    let binding = fixture
        .machine
        .bind_entity(&fixture.namespace, &fixture.subscription)?;
    fixture.put(
        keys::entity_metadata(&fixture.namespace, &fixture.subscription),
        Vec::new(),
    )?;
    fixture.put(keys::clock(), Vec::new())?;
    let before = fixture.raw().snapshot()?;
    fixture.reset();
    let command = fixture.command(
        &fixture.subscription,
        100,
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
    );
    assert_eq!(
        fixture
            .machine
            .apply_bound(&BoundCommand::new(binding.clone(), command)),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    fixture.unchanged(&before)?;
    assert!(!fixture.trace().gets.contains(&keys::clock()));
    // The existing subscription kind is tag 2; verify this controlled head shape against the actual-created row.
    let head_key = keys::entity_metadata(&fixture.namespace, &fixture.subscription);
    let actual_head = baseline
        .entries()
        .iter()
        .find_map(|(key, value)| (key == &head_key).then_some(value.clone()))
        .unwrap();
    assert_eq!(
        codec::encode(&(binding.generation(), 2u32, false))?,
        actual_head
    );
    fixture.put(
        head_key,
        codec::encode(&(binding.generation() + 1, 2u32, false))?,
    )?;
    let changed_generation = fixture.raw().snapshot()?;
    fixture.reset();
    let command = fixture.command(
        &fixture.subscription,
        100,
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
    );
    assert_eq!(
        fixture
            .machine
            .apply_bound(&BoundCommand::new(binding.clone(), command)),
        Err(BrokerError::StaleEntityBinding)
    );
    fixture.unchanged(&changed_generation)?;
    assert!(!fixture.trace().gets.contains(&keys::clock()));
    fixture.reset();
    let wrong = fixture.command(
        &fixture.topic,
        100,
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
    );
    assert_eq!(
        fixture
            .machine
            .apply_bound(&BoundCommand::new(binding, wrong)),
        Err(BrokerError::InvalidEntityBinding)
    );
    fixture.unchanged(&changed_generation)?;
    assert!(fixture.trace().gets.is_empty());
    fixture.restore(&baseline)?;
    fixture.put(
        keys::entity_metadata(&fixture.namespace, &fixture.subscription),
        Vec::new(),
    )?;
    let trace = fixture.reject_list(0, 0, BrokerError::EmptyRulePage)?;
    assert!(trace.gets.is_empty() && trace.scans.is_empty());
    let trace = fixture.reject_list(0, 1, BrokerError::EntityMetadataCorrupt)?;
    fixture.assert_no_rules(&trace);
    assert!(trace.gets.contains(&keys::entity_metadata(
        &fixture.namespace,
        &fixture.subscription
    )));
    let trace = fixture.reject(
        &fixture.subscription,
        100,
        create(
            "new-rule",
            RuleFilter::Correlation(CorrelationFilter::default()),
        )?,
        BrokerError::EntityMetadataCorrupt,
    )?;
    fixture.assert_no_rules(&trace);
    fixture.restore(&baseline)?;
    fixture.put(
        keys::message(&fixture.namespace, &fixture.topic, SequenceNumber::new(1)),
        Vec::new(),
    )?;
    let trace = fixture.reject(
        &fixture.topic,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::EntityMetadataCorrupt,
    )?;
    fixture.assert_rule_scan(&trace, 2);
    fixture
        .raw()
        .apply(WriteBatch::default().delete(fixture.rule_key(&RuleName::new("z-bad")?)))?;
    let trace = fixture.reject(
        &fixture.topic,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::from(codec::decode::<domain::MessageRecord>(&[]).unwrap_err()),
    )?;
    fixture.assert_rule_scan(&trace, 1);
    fixture.restore(&baseline)?;
    fixture.put(
        keys::queue_config(&fixture.namespace, &fixture.subscription),
        codec::encode(&QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        })?,
    )?;
    let trace = fixture.reject_list(0, 1, BrokerError::EntityMetadataCorrupt)?;
    fixture.assert_no_rules(&trace);
    let trace = fixture.reject(
        &fixture.topic,
        100,
        send(input("invalid-backing", None)),
        BrokerError::TopicTopologyCorrupt,
    )?;
    fixture.assert_no_rules(&trace);
    fixture.restore(&baseline)?;
    fixture.put(
        keys::queue_config(&fixture.namespace, &fixture.subscription),
        Vec::new(),
    )?;
    let error = BrokerError::from(codec::decode::<QueueConfig>(&[]).unwrap_err());
    let trace = fixture.reject_list(0, 1, error.clone())?;
    fixture.assert_no_rules(&trace);
    let trace = fixture.reject(
        &fixture.topic,
        100,
        send(input("bad-raw-profile", None)),
        error,
    )?;
    fixture.assert_no_rules(&trace);
    fixture.restore(&baseline)?;
    let missing = fixture
        .topic
        .subscription(&SubscriptionName::new("missing")?)?;
    let trace = fixture.reject(
        &missing,
        100,
        create("new-rule", RuleFilter::True)?,
        BrokerError::SubscriptionNotFound,
    )?;
    fixture.assert_no_rules(&trace);
    let trace = fixture.reject(
        &fixture.subscription.dead_letter_queue()?,
        100,
        create("new-rule", RuleFilter::True)?,
        BrokerError::SubscriptionNotFound,
    )?;
    fixture.assert_no_rules(&trace);
    fixture.machine.store().controls.lock().unwrap().fail_scan = Some(
        keys::subscription_rule_prefix(&fixture.namespace, &fixture.subscription),
    );
    let trace = fixture.reject_list(0, 1, BrokerError::Storage(scan_failure()))?;
    fixture.assert_rule_scan(&trace, 0);
    fixture.machine.store().controls.lock().unwrap().fail_scan = None;
    fixture.reject_surfaces(2)?;
    Ok(())
}

fn unselected<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new(provider)?;
    let bad = fixture.row(
        "z-bad",
        RuleFilter::Correlation(CorrelationFilter::default()),
    )?;
    fixture.install(&bad)?;
    fixture.reset();
    assert_eq!(
        fixture.at(&fixture.topic, 20, send(input("scheduled", Some(100))))?,
        published(&[1], &[])
    );
    fixture.assert_no_rules(&fixture.trace());
    fixture.reset();
    assert_eq!(
        fixture.at(
            &fixture.topic,
            30,
            CommandKind::SendBatch {
                messages: vec![input("one", Some(100)), input("two", Some(100))]
            }
        )?,
        published(&[2, 3], &[])
    );
    fixture.assert_no_rules(&fixture.trace());
    let before = fixture.raw().snapshot()?;
    fixture.reset();
    assert_eq!(
        fixture.at(&fixture.topic, 99, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new(),
        }
    );
    fixture.unchanged(&before)?;
    fixture.assert_no_rules(&fixture.trace());
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(30)
    );
    let trace = fixture.reject(
        &fixture.topic,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::EntityMetadataCorrupt,
    )?;
    fixture.assert_rule_scan(&trace, 2);
    fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, before);
    fixture.put(fixture.rule_key(&bad.name), Vec::new())?;
    fixture.reset();
    assert_eq!(
        fixture.at(
            &fixture.subscription,
            40,
            CommandKind::DeleteRule {
                name: bad.name.clone()
            }
        )?,
        CommandOutcome::RuleDeleted
    );
    fixture.assert_no_rules(&fixture.trace());
    assert!(fixture.raw().get(&fixture.rule_key(&bad.name))?.is_none());
    // Presence-only deletion is existing behavior, not a new integrity repair.
    let other_namespace = NamespaceName::new("other")?;
    fixture.put(
        keys::subscription_rule(&other_namespace, &fixture.subscription, &bad.name),
        Vec::new(),
    )?;
    let name = SubscriptionName::new("other")?;
    let other = fixture.topic.subscription(&name)?;
    assert_eq!(
        fixture.at(
            &fixture.topic,
            50,
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default()
            }
        )?,
        CommandOutcome::SubscriptionCreated {
            entity: other.clone()
        }
    );
    fixture.put(
        keys::subscription_rule(&fixture.namespace, &other, &bad.name),
        Vec::new(),
    )?;
    fixture.reset();
    assert_eq!(
        fixture
            .machine
            .rules(&fixture.namespace, &fixture.subscription, 0, 1)?
            .len(),
        1
    );
    fixture.assert_rule_scan(&fixture.trace(), 1);
    let trace = fixture.reject(
        &fixture.topic,
        100,
        CommandKind::ActivateScheduled,
        BrokerError::from(codec::decode::<RuleDefinition>(&[]).unwrap_err()),
    )?;
    assert!(
        trace
            .scans
            .iter()
            .any(|scan| scan.prefix == keys::subscription_rule_prefix(&fixture.namespace, &other))
    );
    fixture
        .raw()
        .apply(WriteBatch::default().delete(keys::subscription_rule(
            &fixture.namespace,
            &other,
            &bad.name,
        )))?;
    assert_eq!(
        fixture.at(&fixture.topic, 100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 3,
            deliverable_entities: vec![fixture.subscription.clone(), other],
        }
    );
    Ok(())
}

fn indexed_integrity<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("audit")?;
    let subscription = topic.subscription(&name)?;
    let store = Observed::new(provider.open()?);
    let mut writer = IndexedWriter::open(store.clone())?;
    let proposal = |entity: &EntityPath, at, kind| {
        DurableProposal::unbound(Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    };
    assert_eq!(
        writer.apply(
            1,
            &proposal(
                &topic,
                0,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::TopicCreated)
    );
    assert_eq!(
        writer.apply(
            2,
            &proposal(
                &topic,
                10,
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig::default()
                }
            )
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::SubscriptionCreated {
            entity: subscription.clone()
        })
    );
    let latest = proposal(&topic, 20, send(input("scheduled", Some(50))));
    assert_eq!(
        writer.apply(3, &latest)?,
        IndexedApplyOutcome::Applied(published(&[1], &[]))
    );
    let bad = RuleDefinition {
        name: RuleName::new("z-bad")?,
        filter: RuleFilter::Correlation(CorrelationFilter::default()),
        created_at: Timestamp::from_millis(u64::MAX),
    };
    let bad_key = keys::subscription_rule(&namespace, &subscription, &bad.name);
    let binding = StateMachine::new(store.clone()).bind_entity(&namespace, &subscription)?;
    let next_proposals = [
        proposal(
            &subscription,
            100,
            CommandKind::ListRules {
                skip: 0,
                max_rules: 1,
            },
        ),
        proposal(&subscription, 100, create("new-rule", RuleFilter::True)?),
        proposal(&topic, 100, send(input("immediate", None))),
        proposal(
            &topic,
            100,
            CommandKind::SendBatch {
                messages: vec![input("one", None), input("two", None)],
            },
        ),
        proposal(&topic, 100, CommandKind::ActivateScheduled),
        DurableProposal::bound(BoundCommand::new(
            binding,
            Command::new(
                namespace.clone(),
                subscription.clone(),
                Timestamp::from_millis(100),
                CommandKind::ListRules {
                    skip: 0,
                    max_rules: 1,
                },
            ),
        ))?,
    ];
    for definition in [
        RuleDefinition {
            name: RuleName::new("wrong-name")?,
            filter: RuleFilter::True,
            ..bad.clone()
        },
        bad,
    ] {
        store
            .inner
            .apply(WriteBatch::default().put(bad_key.clone(), codec::encode(&definition)?))?;
        let corrupt = store.inner.snapshot()?;
        for next in &next_proposals {
            store.reset();
            assert_eq!(
                writer.apply(4, next),
                Err(IndexedApplyError::Domain(
                    BrokerError::EntityMetadataCorrupt
                ))
            );
            assert_eq!(writer.applied_index()?, 3);
            assert_eq!(
                store.inner.snapshot()?,
                corrupt,
                "no effect, Clock or checkpoint advance"
            );
            assert!(store.trace().batches.is_empty());
            assert_eq!(store.trace().snapshots, 0);
        }
    }
    let good_clock = store.inner.get(&keys::clock())?.unwrap();
    store
        .inner
        .apply(WriteBatch::default().put(keys::clock(), Vec::new()))?;
    let bad_clock = store.inner.snapshot()?;
    store.reset();
    assert_eq!(
        writer.apply(3, &latest)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    let trace = store.trace();
    assert!(trace.gets.is_empty() && trace.scans.is_empty() && trace.batches.is_empty());
    assert_eq!(trace.snapshots, 0);
    assert_eq!(store.inner.snapshot()?, bad_clock);
    store.reset();
    assert_eq!(
        writer.apply(4, &next_proposals[0]),
        Err(IndexedApplyError::Domain(BrokerError::from(
            codec::decode::<Timestamp>(&[]).unwrap_err()
        )))
    );
    assert_eq!(store.trace().gets, vec![keys::clock()]);
    assert!(store.trace().scans.is_empty() && store.trace().batches.is_empty());
    assert_eq!(store.inner.snapshot()?, bad_clock);
    drop(writer);
    drop(store);
    // All database handles are gone before the actual Fjall reopen. Memory is retained reopen only.
    let store = Observed::new(provider.open()?);
    let mut writer = IndexedWriter::open(store.clone())?;
    assert_eq!(writer.applied_index()?, 3);
    assert_eq!(store.inner.snapshot()?, bad_clock);
    store.reset();
    assert_eq!(
        writer.apply(3, &latest)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    let trace = store.trace();
    assert!(trace.gets.is_empty() && trace.scans.is_empty() && trace.batches.is_empty());
    assert_eq!(trace.snapshots, 0);
    store
        .inner
        .apply(WriteBatch::default().put(keys::clock(), good_clock))?;
    let before_repair = store.inner.snapshot()?;
    store.reset();
    assert_eq!(
        writer.apply(4, &next_proposals[0]),
        Err(IndexedApplyError::Domain(
            BrokerError::EntityMetadataCorrupt
        ))
    );
    assert_eq!(store.inner.snapshot()?, before_repair);
    assert!(store.trace().batches.is_empty());
    assert_eq!(writer.applied_index()?, 3);
    store.inner.apply(WriteBatch::default().delete(bad_key))?;
    let next = proposal(&topic, 30, send(input("after-repair", None)));
    assert_eq!(
        writer.apply(4, &next)?,
        IndexedApplyOutcome::Applied(published(&[2], &[subscription]))
    );
    assert_eq!(writer.applied_index()?, 4);
    assert_eq!(
        StateMachine::new(store.clone()).last_applied_time()?,
        Timestamp::from_millis(30)
    );
    Ok(())
}

#[test]
fn memory_touched_rule_keys_preserve_display_and_reject_mismatches() -> Result<(), Box<dyn Error>> {
    key_integrity(MemoryProvider::new())
}
#[test]
fn fjall_touched_rule_keys_preserve_display_and_reject_mismatches() -> Result<(), Box<dyn Error>> {
    key_integrity(DurableProvider::temporary()?)
}
#[test]
fn memory_touched_rule_filters_reject_policy_and_canonicality_violations()
-> Result<(), Box<dyn Error>> {
    filter_integrity(MemoryProvider::new())
}
#[test]
fn fjall_touched_rule_filters_reject_policy_and_canonicality_violations()
-> Result<(), Box<dyn Error>> {
    filter_integrity(DurableProvider::temporary()?)
}
#[test]
fn memory_rule_reader_preserves_full_cap_decode_and_scan_order() -> Result<(), Box<dyn Error>> {
    bounded_order(MemoryProvider::new())
}
#[test]
fn fjall_rule_reader_preserves_full_cap_decode_and_scan_order() -> Result<(), Box<dyn Error>> {
    bounded_order(DurableProvider::temporary()?)
}
#[test]
fn memory_rule_integrity_preserves_earlier_refusal_priorities() -> Result<(), Box<dyn Error>> {
    priority(MemoryProvider::new())
}
#[test]
fn fjall_rule_integrity_preserves_earlier_refusal_priorities() -> Result<(), Box<dyn Error>> {
    priority(DurableProvider::temporary()?)
}
#[test]
fn memory_rule_integrity_only_checks_operationally_selected_rows() -> Result<(), Box<dyn Error>> {
    unselected(MemoryProvider::new())
}
#[test]
fn fjall_rule_integrity_only_checks_operationally_selected_rows() -> Result<(), Box<dyn Error>> {
    unselected(DurableProvider::temporary()?)
}
#[test]
fn memory_indexed_rule_corruption_never_advances_the_next_checkpoint() -> Result<(), Box<dyn Error>>
{
    indexed_integrity(MemoryProvider::new())
}
#[test]
fn fjall_indexed_rule_corruption_never_advances_the_next_checkpoint() -> Result<(), Box<dyn Error>>
{
    indexed_integrity(DurableProvider::temporary()?)
}
