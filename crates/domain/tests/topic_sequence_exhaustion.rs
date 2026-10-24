//! Raw near-MAX seeds followed by actual topic operations, not natural histories.
//! RED captures returned ACKs/MAX records before the intended refusal assertion.
//! Later conservation/reopen assertions are unreached when that assertion fails.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, CorrelationValue,
    DEFAULT_RULE_NAME, DurableProposal, EntityPath, FilterProperties, IndexedApplyError,
    IndexedApplyOutcome, IndexedWriter, MAX_MESSAGE_ID_CHARACTERS, MessageEnvelope, MessageInput,
    MessageRecord, MessageState, NamespaceName, QueueConfig, QueueCounters, RuleConfigError,
    RuleDefinition, RuleFilter, RuleName, SequenceNumber, SessionId, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;
const END: u64 = u64::MAX;
const LAST: u64 = END - 1;

#[derive(Default)]
struct ReadProbe {
    all: AtomicBool,
    forbidden: Mutex<Vec<Key>>,
    attempts: AtomicUsize,
    applies: AtomicUsize,
}

impl ReadProbe {
    fn block(&self, prefixes: Vec<Key>) {
        *self.forbidden.lock().unwrap() = prefixes;
        self.attempts.store(0, Ordering::SeqCst);
    }

    fn clear(&self) {
        self.all.store(false, Ordering::SeqCst);
        self.forbidden.lock().unwrap().clear();
    }

    fn check(&self, key: &[u8]) -> Result<(), StorageError> {
        if self.all.load(Ordering::SeqCst)
            || self
                .forbidden
                .lock()
                .unwrap()
                .iter()
                .any(|prefix| key.starts_with(prefix) || prefix.starts_with(key))
        {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::Backend {
                operation: "qualified topic read exclusion",
                detail: format!("unexpected key/prefix {key:?}"),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Observed<S> {
    inner: S,
    probe: Arc<ReadProbe>,
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.probe.check(key)?;
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.probe.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.probe.check(&[])?;
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.probe.check(prefix)?;
        self.inner.scan_from(prefix, start, limit)
    }
}

struct TopicFixture<P: StoreProvider> {
    namespace: NamespaceName,
    topic: EntityPath,
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
    probe: Arc<ReadProbe>,
}

impl<P: StoreProvider> TopicFixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let probe = Arc::new(ReadProbe::default());
        let mut fixture = Self {
            namespace: NamespaceName::new("t")?,
            topic: EntityPath::new("initial")?,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                probe: probe.clone(),
            }),
            provider,
            probe,
        };
        fixture.case("initial", 0, TopicConfig::default())?;
        Ok(fixture)
    }

    fn command(&self, at: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            self.topic.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    }

    fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&self.command(at, kind))
    }

    fn case(&mut self, name: &str, at: u64, config: TopicConfig) -> TestResult {
        self.topic = EntityPath::new(name)?;
        assert_eq!(
            self.at(at, CommandKind::CreateTopic { config })?,
            CommandOutcome::TopicCreated
        );
        Ok(())
    }

    fn subscribe(
        &self,
        at: u64,
        name: &str,
        ttl: Option<u64>,
    ) -> Result<EntityPath, Box<dyn Error>> {
        let kind = CommandKind::CreateSubscription {
            name: SubscriptionName::new(name)?,
            config: SubscriptionConfig {
                default_time_to_live_millis: ttl,
                ..SubscriptionConfig::default()
            },
        };
        let CommandOutcome::SubscriptionCreated { entity } = self.at(at, kind)? else {
            panic!("expected subscription creation");
        };
        Ok(entity)
    }

    fn child(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }

    // 0: no subscribers; 1: only False; 2: two matching rules per child.
    fn fanout(&self, at: u64, mode: usize) -> Result<Vec<EntityPath>, Box<dyn Error>> {
        let mut children = Vec::new();
        for (index, (name, ttl)) in [("a", 60), ("b", 100)].into_iter().take(mode).enumerate() {
            let child = self.subscribe(at + index as u64 * 3, name, Some(ttl))?;
            if mode == 1 {
                assert_eq!(
                    self.child(
                        &child,
                        at + 1,
                        CommandKind::DeleteRule {
                            name: RuleName::new(DEFAULT_RULE_NAME)?
                        }
                    )?,
                    CommandOutcome::RuleDeleted
                );
            }
            assert_eq!(
                self.child(
                    &child,
                    at + index as u64 * 3 + 2,
                    CommandKind::CreateRule {
                        name: RuleName::new("extra")?,
                        filter: if mode == 1 {
                            RuleFilter::False
                        } else {
                            RuleFilter::True
                        },
                    }
                )?,
                CommandOutcome::RuleCreated
            );
            children.push(child);
        }
        Ok(children)
    }

    fn image(&self) -> Result<StoreSnapshot, StorageError> {
        self.machine.store().inner.snapshot()
    }

    fn raw(&self, key: Key, value: Option<Value>) -> TestResult {
        let mut batch = WriteBatch::default();
        match value {
            Some(value) => batch.push_put(key, value),
            None => batch.push_delete(key),
        }
        self.machine.store().inner.apply(batch)?;
        Ok(())
    }

    fn counters(&self) -> Result<QueueCounters, Box<dyn Error>> {
        match self
            .machine
            .store()
            .inner
            .get(&keys::queue_counters(&self.namespace, &self.topic))?
        {
            Some(value) => Ok(codec::decode(&value)?),
            None => Ok(QueueCounters::default()),
        }
    }

    fn seed(&self, next: u64) -> TestResult {
        let mut counters = self.counters()?;
        counters.next_sequence = next;
        self.raw(
            keys::queue_counters(&self.namespace, &self.topic),
            Some(codec::encode(&counters)?),
        )
    }

    fn excluded(&self) -> Vec<Key> {
        // 0x10 is the existing complete subscription-rule tag, including broad scans.
        vec![
            keys::queue_counters(&self.namespace, &self.topic),
            keys::topic_subscription_prefix(&self.namespace, &self.topic),
            vec![0x10],
        ]
    }

    fn records(
        &self,
        children: &[EntityPath],
        sequence: u64,
    ) -> Result<Vec<Option<MessageRecord>>, BrokerError> {
        std::iter::once(&self.topic)
            .chain(children)
            .map(|entity| {
                self.machine
                    .message(&self.namespace, entity, SequenceNumber::new(sequence))
            })
            .collect()
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let before = self.image()?;
        let Self {
            namespace,
            topic,
            machine,
            provider,
            probe,
        } = self;
        probe.clear();
        drop(machine);
        // No backend clone survives: Memory is logical; Fjall opens after all handles drop.
        let fixture = Self {
            namespace,
            topic,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                probe: probe.clone(),
            }),
            provider,
            probe,
        };
        assert_eq!(fixture.image()?, before);
        Ok(fixture)
    }
}

fn input(id: &str, due: Option<u64>, ttl: Option<u64>) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        scheduled_enqueue_at: due.map(Timestamp::from_millis),
        time_to_live_millis: ttl,
        ..MessageInput::default()
    }
}

fn send(message: MessageInput) -> CommandKind {
    CommandKind::Send {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        scheduled_enqueue_at: message.scheduled_enqueue_at,
        envelope: message.envelope,
    }
}

fn published<P: StoreProvider>(
    f: &TopicFixture<P>,
    at: u64,
    kind: CommandKind,
    sequences: &[u64],
    children: &[EntityPath],
) -> TestResult {
    assert_eq!(
        f.at(at, kind)?,
        CommandOutcome::Published {
            sequences: sequences.iter().copied().map(SequenceNumber::new).collect(),
            subscriptions: children.to_vec(),
        }
    );
    Ok(())
}

fn activated<P: StoreProvider>(
    f: &TopicFixture<P>,
    at: u64,
    count: u32,
    children: &[EntityPath],
) -> TestResult {
    assert_eq!(
        f.at(at, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: count,
            deliverable_entities: children.to_vec(),
        }
    );
    Ok(())
}

fn refuse<P: StoreProvider>(
    f: &TopicFixture<P>,
    at: u64,
    kind: CommandKind,
    children: &[EntityPath],
    repeat: Option<MessageInput>,
) -> TestResult {
    let before = f.image()?;
    let clock = f.machine.last_applied_time()?;
    let counters = f.counters()?;
    let result = f.at(at, kind);
    let first_image = f.image()?;
    let first_max = f.records(children, END)?;
    let repeated = if result.is_ok() {
        if let Some(message) = repeat {
            let next = f.at(at + 1, send(message));
            Some((
                next,
                f.records(children, END)?,
                f.counters()?,
                f.machine.last_applied_time()?,
            ))
        } else {
            None
        }
    } else {
        None
    };
    assert_eq!(
        result,
        Err(BrokerError::SequenceNumberExhausted),
        "actual MAX records: {first_max:?}; first image changed: {}; repeated operation: {repeated:?}",
        first_image != before
    );
    // These witnesses are unreached when the intended RED assertion above fails.
    assert_eq!(first_image, before);
    assert_eq!(f.image()?, before);
    assert_eq!(f.machine.last_applied_time()?, clock);
    assert_eq!(f.counters()?, counters);
    Ok(())
}

fn unchanged_error<P: StoreProvider>(
    f: &TopicFixture<P>,
    at: u64,
    kind: CommandKind,
    error: BrokerError,
) -> TestResult {
    let before = f.image()?;
    let clock = f.machine.last_applied_time()?;
    assert_eq!(f.at(at, kind), Err(error));
    assert_eq!(f.image()?, before);
    assert_eq!(f.machine.last_applied_time()?, clock);
    Ok(())
}

fn immediate<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = TopicFixture::new(provider)?;
    for (mode_index, mode) in [2_usize, 0, 1].into_iter().enumerate() {
        for batch in [false, true] {
            let base = 1_000 + (mode_index as u64 * 2 + u64::from(batch)) * 1_000;
            f.case(
                &format!("immediate-{mode}-{batch}"),
                base,
                TopicConfig {
                    default_time_to_live_millis: Some(150),
                    ..TopicConfig::default()
                },
            )?;
            let children = f.fanout(base + 1, mode)?;
            let matched = if mode == 2 { children.as_slice() } else { &[] };
            let child_counters: Vec<_> = children
                .iter()
                .map(|child| {
                    f.machine
                        .store()
                        .inner
                        .get(&keys::queue_counters(&f.namespace, child))
                })
                .collect::<Result<_, _>>()?;
            f.seed(LAST)?;
            let last = input("last", None, Some(80));
            let kind = if batch {
                CommandKind::SendBatch {
                    messages: vec![last],
                }
            } else {
                send(last)
            };
            published(&f, base + 10, kind, &[LAST], matched)?;
            assert_eq!(f.counters()?.next_sequence, END);
            assert!(
                f.machine
                    .message(&f.namespace, &f.topic, SequenceNumber::new(LAST))?
                    .is_none()
            );
            for (index, child) in matched.iter().enumerate() {
                let record = f
                    .machine
                    .message(&f.namespace, child, SequenceNumber::new(LAST))?
                    .unwrap();
                assert_eq!(record.body, b"last");
                assert_eq!(
                    record.expires_at,
                    Some(Timestamp::from_millis(base + 10 + [60, 80][index]))
                );
                assert_eq!(
                    f.machine.ready_sequences(&f.namespace, child, 10)?,
                    vec![SequenceNumber::new(LAST)]
                );
            }
            for (child, original) in children.iter().zip(child_counters) {
                assert_eq!(
                    f.machine
                        .store()
                        .inner
                        .get(&keys::queue_counters(&f.namespace, child))?,
                    original
                );
            }
            let legal_records = f.records(&children, LAST)?;
            let terminal = input("terminal", None, Some(70));
            let kind = if batch {
                CommandKind::SendBatch {
                    messages: vec![terminal],
                }
            } else {
                send(terminal)
            };
            refuse(
                &f,
                base + 11,
                kind,
                &children,
                Some(input("repeated-max", None, Some(50))),
            )?;
            assert_eq!(f.records(&children, LAST)?, legal_records);
        }
    }
    let f = f.restart()?;
    refuse(&f, 7_100, send(input("after-open", None, None)), &[], None)
}

fn mixed_batches<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = TopicFixture::new(provider)?;
    let history = EntityPath::new("history")?;
    assert_eq!(
        f.child(
            &history,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_duplicate_detection: true,
                    ..QueueConfig::default()
                }
            }
        )?,
        CommandOutcome::QueueCreated
    );
    assert_eq!(
        f.child(&history, 2, send(input("retained-id", None, None)))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    assert!(
        f.image()?
            .entries()
            .iter()
            .any(|(key, _)| key.first() == Some(&0x0C))
    );
    for (index, scheduled_first) in [false, true].into_iter().enumerate() {
        let base = 1_000 + index as u64 * 1_000;
        f.case(&format!("mixed-fit-{index}"), base, TopicConfig::default())?;
        let children = f.fanout(base + 1, 2)?;
        let pair = |prefix: &str, due: u64| {
            let immediate = input(&format!("{prefix}-now"), None, Some(80));
            let scheduled = input(&format!("{prefix}-later"), Some(due), Some(90));
            if scheduled_first {
                vec![scheduled, immediate]
            } else {
                vec![immediate, scheduled]
            }
        };
        f.seed(END - 2)?;
        published(
            &f,
            base + 10,
            CommandKind::SendBatch {
                messages: pair("fit", base + 100),
            },
            &[END - 2, LAST],
            &children,
        )?;
        assert_eq!(f.counters()?.next_sequence, END);
        let scheduled_sequence = if scheduled_first { END - 2 } else { LAST };
        assert_eq!(
            f.machine.scheduled_sequences(&f.namespace, &f.topic, 10)?,
            vec![SequenceNumber::new(scheduled_sequence)]
        );
        f.case(
            &format!("mixed-cross-{index}"),
            base + 200,
            TopicConfig::default(),
        )?;
        let children = f.fanout(base + 201, 2)?;
        published(
            &f,
            base + 210,
            send(input("retained-placeholder", Some(base + 500), Some(90))),
            &[1],
            &[],
        )?;
        f.seed(LAST)?;
        refuse(
            &f,
            base + 220,
            CommandKind::SendBatch {
                messages: pair("cross", base + 500),
            },
            &children,
            None,
        )?;
        assert_eq!(
            f.machine.scheduled_sequences(&f.namespace, &f.topic, 10)?,
            vec![SequenceNumber::new(1)]
        );
    }
    f.case("all-scheduled", 4_000, TopicConfig::default())?;
    let children = f.fanout(4_001, 2)?;
    f.seed(END - 2)?;
    f.probe.block(f.excluded().into_iter().skip(1).collect());
    published(
        &f,
        4_010,
        CommandKind::SendBatch {
            messages: vec![input("a", Some(4_100), None), input("b", Some(4_100), None)],
        },
        &[END - 2, LAST],
        &[],
    )?;
    refuse(
        &f,
        4_011,
        send(input("c", Some(4_100), None)),
        &children,
        None,
    )?;
    assert_eq!(f.probe.attempts.load(Ordering::SeqCst), 0);
    f.probe.clear();
    let f = f.restart()?;
    refuse(
        &f,
        4_012,
        send(input("d", Some(4_100), None)),
        &children,
        None,
    )
}

fn activation<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = TopicFixture::new(provider)?;
    for (mode_index, mode) in [2_usize, 0, 1].into_iter().enumerate() {
        for (phase, next, count) in [
            ("fit", LAST, 1_u32),
            ("terminal", END, 2),
            ("cross", LAST, 2),
        ] {
            let phase_index: u64 = match phase {
                "fit" => 0,
                "terminal" => 1,
                _ => 2,
            };
            let base = 1_000 + (mode_index as u64 * 3 + phase_index) * 1_000;
            f.case(
                &format!("activation-{mode}-{phase}"),
                base,
                TopicConfig {
                    default_time_to_live_millis: Some(90),
                    ..TopicConfig::default()
                },
            )?;
            let children = f.fanout(base + 1, mode)?;
            let matched = if mode == 2 { children.as_slice() } else { &[] };
            let inputs: Vec<_> = (0..count)
                .map(|index| {
                    input(
                        &format!("item-{index}"),
                        Some(base + 100),
                        Some([80, 120][index as usize]),
                    )
                })
                .collect();
            published(
                &f,
                base + 10,
                CommandKind::SendBatch { messages: inputs },
                &(1..=u64::from(count)).collect::<Vec<_>>(),
                &[],
            )?;
            f.seed(next)?;
            if phase == "fit" {
                activated(&f, base + 120, 1, matched)?;
                assert_eq!(f.counters()?.next_sequence, END);
                assert!(
                    f.machine
                        .message(&f.namespace, &f.topic, SequenceNumber::new(LAST))?
                        .is_none()
                );
                assert!(
                    f.machine
                        .scheduled_sequences(&f.namespace, &f.topic, 10)?
                        .is_empty()
                );
                for (index, child) in matched.iter().enumerate() {
                    let record = f
                        .machine
                        .message(&f.namespace, child, SequenceNumber::new(LAST))?
                        .unwrap();
                    assert_eq!(record.sequence, SequenceNumber::new(LAST));
                    assert_eq!(record.body, b"item-0");
                    assert_eq!(record.enqueued_at, Timestamp::from_millis(base + 120));
                    assert_eq!(
                        record.scheduled_enqueue_at,
                        Some(Timestamp::from_millis(base + 100))
                    );
                    assert_eq!(
                        record.expires_at,
                        Some(Timestamp::from_millis(base + 120 + [60, 80][index]))
                    );
                    assert!(
                        f.machine
                            .store()
                            .inner
                            .get(&keys::queue_counters(&f.namespace, child))?
                            .is_none()
                    );
                }
                let before = f.image()?;
                activated(&f, base + 121, 0, &[])?;
                assert_eq!(f.image()?, before);
            } else {
                refuse(
                    &f,
                    base + 120,
                    CommandKind::ActivateScheduled,
                    &children,
                    None,
                )?;
                assert_eq!(
                    f.machine.scheduled_sequences(&f.namespace, &f.topic, 10)?,
                    vec![SequenceNumber::new(1), SequenceNumber::new(2)]
                );
                f = f.restart()?;
                refuse(
                    &f,
                    base + 121,
                    CommandKind::ActivateScheduled,
                    &children,
                    None,
                )?;
            }
        }
    }
    Ok(())
}

fn bounded_resume<P: StoreProvider>(provider: P) -> TestResult {
    let f = TopicFixture::new(provider)?;
    let children: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| f.subscribe(index as u64 + 1, name, Some(60)))
        .collect::<Result<_, _>>()?;
    let messages = (0..65)
        .map(|index| input(&format!("item-{index}"), Some(100), Some(80)))
        .collect();
    published(
        &f,
        10,
        CommandKind::SendBatch { messages },
        &(1..=65).collect::<Vec<_>>(),
        &[],
    )?;
    let final_key = keys::message(&f.namespace, &f.topic, SequenceNumber::new(65));
    let final_bytes = f.machine.store().inner.get(&final_key)?.unwrap();
    f.seed(END - 64)?;
    activated(&f, 100, 64, &children)?;
    assert_eq!(f.counters()?.next_sequence, END);
    assert_eq!(
        f.machine.scheduled_sequences(&f.namespace, &f.topic, 10)?,
        vec![SequenceNumber::new(65)]
    );
    assert_eq!(
        f.machine.store().inner.get(&final_key)?,
        Some(final_bytes.clone())
    );
    for child in &children {
        assert_eq!(
            f.machine.ready_sequences(&f.namespace, child, 100)?,
            (END - 64..END).map(SequenceNumber::new).collect::<Vec<_>>()
        );
        let last = f
            .machine
            .message(&f.namespace, child, SequenceNumber::new(LAST))?
            .unwrap();
        assert_eq!(last.body, b"item-63");
        assert_eq!(last.enqueued_at, Timestamp::from_millis(100));
        assert_eq!(last.expires_at, Some(Timestamp::from_millis(160)));
        assert_eq!(last.scheduled_enqueue_at, Some(Timestamp::from_millis(100)));
        assert!(
            f.machine
                .store()
                .inner
                .get(&keys::queue_counters(&f.namespace, child))?
                .is_none()
        );
    }
    let f = f.restart()?;
    refuse(&f, 101, CommandKind::ActivateScheduled, &children, None)?;
    assert_eq!(f.machine.store().inner.get(&final_key)?, Some(final_bytes));
    assert_eq!(
        f.at(
            102,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(65)]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(f.counters()?.next_sequence, END);
    Ok(())
}

fn idle_cancel<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = TopicFixture::new(provider)?;
    let counter_key = keys::queue_counters(&f.namespace, &f.topic);
    assert!(f.machine.store().inner.get(&counter_key)?.is_none());
    for raw_counter in [
        None,
        Some(codec::encode(&QueueCounters {
            next_sequence: END,
            ..QueueCounters::default()
        })?),
        Some(vec![0]),
    ] {
        f.raw(counter_key.clone(), raw_counter)?;
        let before = f.image()?;
        f.probe.block(f.excluded());
        activated(&f, 50, 0, &[])?;
        assert_eq!(f.image()?, before);
        assert_eq!(f.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
        assert_eq!(f.probe.attempts.load(Ordering::SeqCst), 0);
        f.probe.clear();
    }
    f.raw(counter_key, None)?;
    let children = f.fanout(1, 2)?;
    published(&f, 10, send(input("later", Some(100), None)), &[1], &[])?;
    f.seed(END)?;
    let counter_key = keys::queue_counters(&f.namespace, &f.topic);
    let original = f.machine.store().inner.get(&counter_key)?.unwrap();
    f.raw(counter_key.clone(), Some(vec![0]))?;
    let before = f.image()?;
    f.probe.block(f.excluded());
    activated(&f, 50, 0, &[])?;
    assert_eq!(f.image()?, before);
    assert_eq!(f.machine.last_applied_time()?, Timestamp::from_millis(10));
    assert_eq!(f.probe.attempts.load(Ordering::SeqCst), 0);
    f.probe.clear();
    f.raw(counter_key, Some(original))?;
    f.probe.block(f.excluded());
    assert_eq!(
        f.at(
            51,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1)]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(f.probe.attempts.load(Ordering::SeqCst), 0);
    f.probe.clear();
    assert_eq!(f.machine.last_applied_time()?, Timestamp::from_millis(51));
    assert_eq!(f.counters()?.next_sequence, END);
    unchanged_error(
        &f,
        52,
        CommandKind::CancelScheduled { sequences: vec![] },
        BrokerError::EmptyScheduledCancellation,
    )?;
    unchanged_error(
        &f,
        52,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(1)],
        },
        BrokerError::DuplicateScheduledSequence {
            sequence: SequenceNumber::new(1),
        },
    )?;
    unchanged_error(
        &f,
        52,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(999)],
        },
        BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999),
        },
    )?;
    let before = f.image()?;
    activated(&f, 100, 0, &[])?;
    assert_eq!(f.image()?, before);
    assert!(f.records(&children, END)?.iter().all(Option::is_none));
    f.case("idle-not-unmatched", 200, TopicConfig::default())?;
    let children = f.fanout(201, 1)?;
    published(
        &f,
        210,
        send(input("unmatched", Some(300), None)),
        &[1],
        &[],
    )?;
    f.seed(END)?;
    refuse(&f, 300, CommandKind::ActivateScheduled, &children, None)?;
    let f = f.restart()?;
    refuse(&f, 301, CommandKind::ActivateScheduled, &children, None)
}

fn invalid_projection() -> Result<MessageEnvelope, Box<dyn Error>> {
    let value = CorrelationValue::new(vec![1])?;
    Ok(
        MessageEnvelope::new(vec![1]).with_filter_properties(FilterProperties {
            application_properties: BTreeMap::from([
                ("A".to_owned(), value.clone()),
                ("a".to_owned(), value),
            ]),
            ..FilterProperties::default()
        }),
    )
}

fn priorities<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = TopicFixture::new(provider)?;
    f.case(
        "priorities",
        100,
        TopicConfig {
            max_message_bytes: 5,
            ..TopicConfig::default()
        },
    )?;
    let child = f.subscribe(101, "a", None)?;
    published(
        &f,
        110,
        CommandKind::SendBatch {
            messages: vec![
                input("a", Some(300), Some(80)),
                input("b", Some(300), Some(90)),
            ],
        },
        &[1, 2],
        &[],
    )?;
    f.seed(END)?;
    unchanged_error(
        &f,
        109,
        CommandKind::ActivateScheduled,
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(110),
            proposed: Timestamp::from_millis(109),
        },
    )?;
    let binding = f.machine.bind_entity(&f.namespace, &f.topic)?;
    let before = f.image()?;
    let mut wrong_scope = f.command(1, CommandKind::ActivateScheduled);
    wrong_scope.entity = EntityPath::new("elsewhere")?;
    assert_eq!(
        f.machine
            .apply_bound(&BoundCommand::new(binding, wrong_scope)),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(f.image()?, before);
    unchanged_error(
        &f,
        111,
        CommandKind::SendBatch { messages: vec![] },
        BrokerError::EmptyMessageBatch,
    )?;
    let mut session = input("a", None, None);
    session.session_id = Some(SessionId::new("S")?);
    unchanged_error(
        &f,
        111,
        send(session),
        BrokerError::TopicSessionNotSupported,
    )?;
    let long = input(&"m".repeat(MAX_MESSAGE_ID_CHARACTERS + 1), None, None);
    unchanged_error(
        &f,
        111,
        send(long),
        BrokerError::MessageIdTooLong {
            characters: MAX_MESSAGE_ID_CHARACTERS + 1,
            maximum: MAX_MESSAGE_ID_CHARACTERS,
        },
    )?;
    unchanged_error(
        &f,
        111,
        CommandKind::SendBatch {
            messages: vec![input("a", None, None), input("123456", None, None)],
        },
        BrokerError::MessageTooLarge {
            body_bytes: 6,
            maximum_bytes: 5,
        },
    )?;
    let mut projection = input("a", None, None);
    projection.envelope = Some(invalid_projection()?);
    unchanged_error(
        &f,
        111,
        CommandKind::SendBatch {
            messages: vec![input("a", None, None), projection.clone()],
        },
        BrokerError::RuleConfig(RuleConfigError::DuplicateApplicationProperty),
    )?;
    let rule_key =
        keys::subscription_rule(&f.namespace, &child, &RuleName::new(DEFAULT_RULE_NAME)?);
    let rule_raw = f.machine.store().inner.get(&rule_key)?.unwrap();
    let rule_error = codec::decode::<RuleDefinition>(&[0]).unwrap_err();
    f.raw(rule_key.clone(), Some(vec![0]))?;
    unchanged_error(
        &f,
        111,
        send(input("123456", None, None)),
        BrokerError::MessageTooLarge {
            body_bytes: 6,
            maximum_bytes: 5,
        },
    )?;
    unchanged_error(
        &f,
        111,
        send(projection.clone()),
        BrokerError::RuleConfig(RuleConfigError::DuplicateApplicationProperty),
    )?;
    unchanged_error(
        &f,
        111,
        send(input("a", None, None)),
        BrokerError::Codec(rule_error),
    )?;
    unchanged_error(
        &f,
        300,
        CommandKind::ActivateScheduled,
        BrokerError::Codec(rule_error),
    )?;
    f.raw(rule_key, Some(rule_raw))?;
    let member_key = keys::topic_subscription(&f.namespace, &f.topic, &SubscriptionName::new("a")?);
    let member_raw = f.machine.store().inner.get(&member_key)?.unwrap();
    f.raw(
        member_key.clone(),
        Some(codec::encode(&EntityPath::new("wrong")?)?),
    )?;
    unchanged_error(
        &f,
        111,
        send(input("a", None, None)),
        BrokerError::TopicTopologyCorrupt,
    )?;
    f.raw(member_key, Some(member_raw))?;
    let config_key = keys::topic_config(&f.namespace, &f.topic);
    let config_raw = f.machine.store().inner.get(&config_key)?.unwrap();
    f.raw(
        config_key.clone(),
        Some(codec::encode(&TopicConfig {
            max_message_bytes: 0,
            ..TopicConfig::default()
        })?),
    )?;
    unchanged_error(
        &f,
        111,
        CommandKind::SendBatch { messages: vec![] },
        BrokerError::TopicTopologyCorrupt,
    )?;
    f.raw(config_key, Some(config_raw))?;
    let counter_key = keys::queue_counters(&f.namespace, &f.topic);
    let counter_raw = f.machine.store().inner.get(&counter_key)?.unwrap();
    let counter_error = codec::decode::<QueueCounters>(&[0]).unwrap_err();
    let second = SequenceNumber::new(2);
    let message_key = keys::message(&f.namespace, &f.topic, second);
    let message_raw = f.machine.store().inner.get(&message_key)?.unwrap();
    let retained: MessageRecord = codec::decode(&message_raw)?;
    let message_error = codec::decode::<MessageRecord>(&[0]).unwrap_err();
    for counter_bytes in [counter_raw.clone(), vec![0]] {
        f.raw(counter_key.clone(), Some(counter_bytes))?;
        f.raw(message_key.clone(), None)?;
        unchanged_error(
            &f,
            300,
            CommandKind::ActivateScheduled,
            BrokerError::DanglingIndexEntry { sequence: second },
        )?;
        f.raw(message_key.clone(), Some(vec![0]))?;
        unchanged_error(
            &f,
            300,
            CommandKind::ActivateScheduled,
            BrokerError::Codec(message_error),
        )?;
        for (record, error) in [
            (
                MessageRecord {
                    state: MessageState::Ready,
                    ..retained.clone()
                },
                BrokerError::MessageNotScheduled { sequence: second },
            ),
            (
                MessageRecord {
                    scheduled_enqueue_at: None,
                    ..retained.clone()
                },
                BrokerError::ScheduledEnqueueTimeMissing { sequence: second },
            ),
            (
                MessageRecord {
                    scheduled_enqueue_at: Some(Timestamp::from_millis(301)),
                    ..retained.clone()
                },
                BrokerError::MalformedIndexKey,
            ),
            (
                MessageRecord {
                    envelope: Some(invalid_projection()?),
                    ..retained.clone()
                },
                BrokerError::RuleConfig(RuleConfigError::DuplicateApplicationProperty),
            ),
        ] {
            f.raw(message_key.clone(), Some(codec::encode(&record)?))?;
            unchanged_error(&f, 300, CommandKind::ActivateScheduled, error)?;
        }
        f.raw(message_key.clone(), Some(message_raw.clone()))?;
    }
    unchanged_error(
        &f,
        300,
        CommandKind::ActivateScheduled,
        BrokerError::Codec(counter_error),
    )?;
    unchanged_error(
        &f,
        111,
        send(input("a", None, None)),
        BrokerError::Codec(counter_error),
    )?;
    f.raw(counter_key, Some(counter_raw))?;
    f.case("p", 400, TopicConfig::default())?;
    published(&f, 410, send(input("a", Some(500), None)), &[1], &[])?;
    f.seed(END)?;
    let malformed_key = keys::scheduled_prefix(&f.namespace, &f.topic);
    assert!(
        malformed_key.len() < 16,
        "this injected key lacks the deadline/sequence tail"
    );
    f.raw(malformed_key.clone(), Some(vec![]))?;
    unchanged_error(
        &f,
        450,
        CommandKind::ActivateScheduled,
        BrokerError::MalformedIndexKey,
    )?;
    f.raw(malformed_key, None)?;
    let f = f.restart()?;
    let before = f.image()?;
    activated(&f, 450, 0, &[])?;
    assert_eq!(f.image()?, before);
    Ok(())
}

fn domain_rows(image: &StoreSnapshot) -> Vec<(Key, Value)> {
    image
        .entries()
        .iter()
        .filter(|(key, _)| key.first() != Some(&0xF1))
        .cloned()
        .collect()
}

fn indexed<P: StoreProvider>(provider: P) -> TestResult {
    let store = provider.open()?;
    let probe = Arc::new(ReadProbe::default());
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        probe: probe.clone(),
    })?;
    let namespace = NamespaceName::new("t")?;
    let topic = EntityPath::new("indexed-topic")?;
    let command = |at, kind| {
        Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    };
    assert_eq!(
        writer.apply(
            1,
            &DurableProposal::unbound(command(
                10,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::TopicCreated)
    );
    let name = SubscriptionName::new("a")?;
    let child = topic.subscription(&name)?;
    assert_eq!(
        writer.apply(
            2,
            &DurableProposal::unbound(command(
                20,
                CommandKind::CreateSubscription {
                    name,
                    config: SubscriptionConfig::default()
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::SubscriptionCreated {
            entity: child.clone()
        })
    );
    assert_eq!(
        writer.apply(
            3,
            &DurableProposal::unbound(command(
                30,
                CommandKind::SendBatch {
                    messages: vec![
                        input("first", Some(100), Some(80)),
                        input("second", Some(100), Some(90)),
                        input("ready", None, Some(100))
                    ]
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::Published {
            sequences: vec![
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(3)
            ],
            subscriptions: vec![child.clone()]
        })
    );
    let machine = StateMachine::new(store.clone());
    let binding = machine.bind_entity(&namespace, &topic)?;
    drop(machine);
    // Bootstrap establishes an existing owner; all subsequent raw/writer access is serial.
    let key = keys::queue_counters(&namespace, &topic);
    let mut counters: QueueCounters = codec::decode(&store.get(&key)?.unwrap())?;
    counters.next_sequence = END;
    store.apply(WriteBatch::default().put(key, codec::encode(&counters)?))?;
    let seeded = store.snapshot()?;
    let mut latest = None;
    for (offset, kind) in [
        send(input("terminal", None, None)),
        CommandKind::SendBatch {
            messages: vec![input("one", None, None), input("two", Some(200), None)],
        },
        CommandKind::ActivateScheduled,
    ]
    .into_iter()
    .enumerate()
    {
        let index = 4 + offset as u64;
        let at = 100 + offset as u64;
        let proposal = DurableProposal::bound(BoundCommand::new(
            binding.clone(),
            command(at, kind.clone()),
        ))?;
        let before = store.snapshot()?;
        let result = writer.apply(index, &proposal);
        let max_record = StateMachine::new(store.clone()).message(
            &namespace,
            &child,
            SequenceNumber::new(END),
        )?;
        assert_eq!(
            result,
            Ok(IndexedApplyOutcome::Refused(
                BrokerError::SequenceNumberExhausted
            )),
            "actual indexed outcome above; MAX copy: {max_record:?}"
        );
        // RED failing here does not prove any checkpoint/read-free/reopen assertion below.
        assert_eq!(writer.applied_index()?, index);
        let after = store.snapshot()?;
        assert_eq!(domain_rows(&after), domain_rows(&seeded));
        let old_metadata: Vec<_> = before
            .entries()
            .iter()
            .filter(|(key, _)| key.first() == Some(&0xF1))
            .collect();
        let metadata: Vec<_> = after
            .entries()
            .iter()
            .filter(|(key, _)| key.first() == Some(&0xF1))
            .collect();
        assert_eq!(old_metadata.len(), 2);
        assert_eq!(metadata.len(), 2);
        assert_eq!(
            metadata[0], old_metadata[0],
            "the original owner row is unchanged"
        );
        assert_ne!(
            metadata[1], old_metadata[1],
            "only the F1 checkpoint intentionally changes"
        );
        let applies = probe.applies.load(Ordering::SeqCst);
        probe.attempts.store(0, Ordering::SeqCst);
        probe.all.store(true, Ordering::SeqCst);
        assert_eq!(
            writer.apply(index, &proposal)?,
            IndexedApplyOutcome::AlreadyApplied
        );
        let conflict =
            DurableProposal::bound(BoundCommand::new(binding.clone(), command(at + 1, kind)))?;
        assert_eq!(
            writer.apply(index, &conflict),
            Err(IndexedApplyError::ConflictingProposal { index })
        );
        assert_eq!(probe.applies.load(Ordering::SeqCst), applies);
        assert_eq!(probe.attempts.load(Ordering::SeqCst), 0);
        probe.clear();
        assert_eq!(store.snapshot()?, after);
        latest = Some(proposal);
    }
    let latest = latest.unwrap();
    let after = store.snapshot()?;
    drop(writer);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, after);
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        probe: probe.clone(),
    })?;
    let applies = probe.applies.load(Ordering::SeqCst);
    probe.attempts.store(0, Ordering::SeqCst);
    probe.all.store(true, Ordering::SeqCst);
    assert_eq!(
        writer.apply(6, &latest)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert_eq!(probe.attempts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.applies.load(Ordering::SeqCst), applies);
    probe.clear();
    let second_key = keys::message(&namespace, &topic, SequenceNumber::new(2));
    let second_raw = store.get(&second_key)?.unwrap();
    let error = codec::decode::<MessageRecord>(&[0]).unwrap_err();
    store.apply(WriteBatch::default().put(second_key.clone(), vec![0]))?;
    let corrupt = store.snapshot()?;
    assert_eq!(
        writer.apply(
            7,
            &DurableProposal::bound(BoundCommand::new(
                binding.clone(),
                command(103, CommandKind::ActivateScheduled)
            ))?
        ),
        Err(IndexedApplyError::Domain(BrokerError::Codec(error)))
    );
    assert_eq!(writer.applied_index()?, 6);
    assert_eq!(store.snapshot()?, corrupt);
    store.apply(WriteBatch::default().put(second_key, second_raw))?;
    assert_eq!(store.snapshot()?, after);
    assert_eq!(
        writer.apply(
            7,
            &DurableProposal::bound(BoundCommand::new(
                binding.clone(),
                command(
                    104,
                    CommandKind::CancelScheduled {
                        sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)]
                    }
                )
            ))?
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::ScheduledCancelled { cancelled: 2 })
    );
    let domain = domain_rows(&store.snapshot()?);
    assert_eq!(
        writer.apply(
            8,
            &DurableProposal::bound(BoundCommand::new(
                binding,
                command(105, CommandKind::ActivateScheduled)
            ))?
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: vec![]
        })
    );
    assert_eq!(domain_rows(&store.snapshot()?), domain);
    let final_image = store.snapshot()?;
    drop(writer);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, final_image);
    drop(store);
    Ok(())
}

#[test]
fn memory_qualified_topic_offers_keep_slot_identity_and_refuse_terminal() -> TestResult {
    immediate(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_offers_keep_slot_identity_and_refuse_terminal() -> TestResult {
    immediate(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_mixed_batches_refuse_atomically_without_topology_rereads() -> TestResult {
    mixed_batches(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_mixed_batches_refuse_atomically_without_topology_rereads() -> TestResult {
    mixed_batches(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_activation_allocates_per_placeholder_not_copy() -> TestResult {
    activation(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_activation_allocates_per_placeholder_not_copy() -> TestResult {
    activation(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_fanout_budget_reopens_before_final_refusal() -> TestResult {
    bounded_resume(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_fanout_budget_reopens_before_final_refusal() -> TestResult {
    bounded_resume(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_idle_and_cancel_need_no_sequence() -> TestResult {
    idle_cancel(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_idle_and_cancel_need_no_sequence() -> TestResult {
    idle_cancel(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_validation_priorities_precede_exhaustion() -> TestResult {
    priorities(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_validation_priorities_precede_exhaustion() -> TestResult {
    priorities(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_topic_indexed_refusal_advances_only_existing_checkpoint() -> TestResult {
    indexed(MemoryProvider::new())
}

#[test]
fn fjall_qualified_topic_indexed_refusal_advances_only_existing_checkpoint() -> TestResult {
    indexed(DurableProvider::temporary()?)
}
