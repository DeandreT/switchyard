//! Identical controls on Memory and a genuinely closed/reopened Fjall owner.
use std::error::Error;

use domain::{
    BrokerError, CodecError, Command, CommandKind, CommandOutcome, EntityPath, MessageEnvelope,
    MessageInput, NamespaceName, QueueConfig, QueueCounters, QueueTimeToLiveUpdate, ReceiveMode,
    RuleFilter, RuleName, SequenceNumber, SessionId, StateMachine, SubscriptionConfig,
    SubscriptionConfigUpdate, SubscriptionName, TIMER_SCAN_LIMIT, Timestamp, TopicConfig,
    TopicConfigUpdate, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;

type ResultTest = Result<(), Box<dyn Error>>;
const WINDOW: u64 = 20_000;
fn config() -> TopicConfig {
    TopicConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: WINDOW,
        ..TopicConfig::default()
    }
}
fn input(id: &str) -> MessageInput {
    MessageInput {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        ..MessageInput::default()
    }
}
fn scheduled(id: &str, at: u64) -> MessageInput {
    MessageInput {
        scheduled_enqueue_at: Some(Timestamp::from_millis(at)),
        ..input(id)
    }
}

struct Fixture<P: StoreProvider> {
    machine: StateMachine<P::Store>,
    provider: P,
    ns: NamespaceName,
    topic: EntityPath,
}
impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P, config: TopicConfig) -> Result<Self, Box<dyn Error>> {
        let f = Self {
            machine: StateMachine::new(provider.open()?),
            provider,
            ns: NamespaceName::new("n")?,
            topic: EntityPath::new("t")?,
        };
        f.at(0, CommandKind::CreateTopic { config })?;
        Ok(f)
    }
    fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.on(&self.topic, at, kind)
    }
    fn on(
        &self,
        path: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.ns.clone(),
            path.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }
    fn offer(
        &self,
        at: u64,
        inputs: Vec<MessageInput>,
    ) -> Result<(Vec<u64>, Vec<EntityPath>), BrokerError> {
        match self.at(at, CommandKind::SendBatch { messages: inputs })? {
            CommandOutcome::Published {
                sequences,
                subscriptions,
            } => Ok((
                sequences.into_iter().map(SequenceNumber::as_u64).collect(),
                subscriptions,
            )),
            other => panic!("not published: {other:?}"),
        }
    }
    fn single(
        &self,
        at: u64,
        input: MessageInput,
    ) -> Result<(Vec<u64>, Vec<EntityPath>), BrokerError> {
        match self.at(
            at,
            CommandKind::Send {
                message_id: input.message_id,
                body: input.body,
                time_to_live_millis: input.time_to_live_millis,
                session_id: input.session_id,
                scheduled_enqueue_at: input.scheduled_enqueue_at,
                envelope: input.envelope,
            },
        )? {
            CommandOutcome::Published {
                sequences,
                subscriptions,
            } => Ok((
                sequences.into_iter().map(SequenceNumber::as_u64).collect(),
                subscriptions,
            )),
            other => panic!("not published: {other:?}"),
        }
    }
    fn child(&self, name: &str) -> Result<EntityPath, Box<dyn Error>> {
        match self.at(
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )? {
            CommandOutcome::SubscriptionCreated { entity } => Ok(entity),
            other => panic!("not subscribed: {other:?}"),
        }
    }
    fn deadline(&self, id: &str) -> Option<Timestamp> {
        self.machine
            .duplicate_history_deadline(&self.ns, &self.topic, id)
            .unwrap()
    }
    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            machine,
            provider,
            ns,
            topic,
        } = self;
        drop(machine); // No cloned store survives this point.
        Ok(Self {
            machine: StateMachine::new(provider.open()?),
            provider,
            ns,
            topic,
        })
    }
    fn counters(&self, path: &EntityPath) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        Ok(self
            .machine
            .store()
            .get(&keys::queue_counters(&self.ns, path))?)
    }
    fn ready(&self, path: &EntityPath) -> Vec<u64> {
        self.machine
            .ready_sequences(&self.ns, path, 100)
            .unwrap()
            .into_iter()
            .map(SequenceNumber::as_u64)
            .collect()
    }
}

fn immediate<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let a = f.child("a")?;
    let b = f.child("b")?;
    let children_before = (f.counters(&a)?, f.counters(&b)?);
    assert_eq!(
        f.offer(
            10,
            vec![input("ID"), input("ID"), input("id"), input(""), input("")]
        )?,
        (vec![1, 2, 3, 4, 5], vec![a.clone(), b.clone()])
    );
    assert_eq!(f.ready(&a), vec![1, 3, 4, 5]);
    assert_eq!(f.ready(&b), vec![1, 3, 4, 5]);
    assert_eq!(f.deadline("ID"), Some(Timestamp::from_millis(20_010)));
    assert_eq!(f.deadline(""), None);
    assert_eq!(
        f.offer(11, vec![input("ID"), scheduled("id", 50_000)])?,
        (vec![6, 7], vec![])
    );
    assert!(
        f.machine
            .message(&f.ns, &f.topic, SequenceNumber::new(7))?
            .is_none()
    );
    assert_eq!((f.counters(&a)?, f.counters(&b)?), children_before);
    for child in [&a, &b, &a.dead_letter_queue()?, &b.dead_letter_queue()?] {
        assert!(
            f.machine
                .duplicate_history_deadline(&f.ns, child, "ID")?
                .is_none()
        );
        assert!(
            f.machine
                .store()
                .scan_prefix(&keys::duplicate_expiry_prefix(&f.ns, child), 1)?
                .is_empty()
        );
        assert!(
            !f.machine
                .queue_config(&f.ns, child)?
                .unwrap()
                .requires_duplicate_detection
        );
    }
    let f = f.restart()?;
    assert_eq!(f.offer(20_009, vec![input("ID")])?, (vec![8], vec![]));
    assert_eq!(f.deadline("ID"), Some(Timestamp::from_millis(20_010)));
    assert_eq!(
        f.offer(20_010, vec![input("ID"), input("ID")])?,
        (vec![9, 10], vec![a.clone(), b])
    );
    assert_eq!(f.ready(&a), vec![1, 3, 4, 5, 9]);
    assert_eq!(f.deadline("ID"), Some(Timestamp::from_millis(40_010)));
    let other = EntityPath::new("other")?;
    f.on(
        &other,
        20_011,
        CommandKind::CreateTopic { config: config() },
    )?;
    f.on(
        &other,
        20_012,
        CommandKind::SendBatch {
            messages: vec![input("ID")],
        },
    )?;
    let other_ns = NamespaceName::new("other")?;
    f.machine.apply(&Command::new(
        other_ns.clone(),
        f.topic.clone(),
        Timestamp::from_millis(20_012),
        CommandKind::CreateTopic { config: config() },
    ))?;
    f.machine.apply(&Command::new(
        other_ns.clone(),
        f.topic.clone(),
        Timestamp::from_millis(20_013),
        CommandKind::SendBatch {
            messages: vec![input("ID")],
        },
    ))?;
    assert_eq!(
        f.machine.duplicate_history_deadline(&f.ns, &other, "ID")?,
        Some(Timestamp::from_millis(40_012))
    );
    assert_eq!(
        f.machine
            .duplicate_history_deadline(&other_ns, &f.topic, "ID")?,
        Some(Timestamp::from_millis(40_013))
    );
    Ok(())
}

fn scheduled_lifecycle<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let child = f.child("a")?;
    assert_eq!(
        f.offer(
            2,
            vec![scheduled("cancel", 30_000), scheduled("activate", 30_000)]
        )?
        .0,
        vec![1, 2]
    );
    f.at(
        3,
        CommandKind::CancelScheduled {
            sequences: vec![SequenceNumber::new(1)],
        },
    )?;
    assert_eq!(
        f.offer(4, vec![scheduled("cancel", 30_000), input("activate")])?,
        (vec![3, 4], vec![])
    );
    let f = f.restart()?;
    assert_eq!(
        f.at(30_000, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![child.clone()]
        }
    );
    assert_eq!(f.ready(&child), vec![5]);
    assert_eq!(f.deadline("activate"), Some(Timestamp::from_millis(20_002)));
    // Activation neither rechecks elapsed history nor refreshes it.
    assert_eq!(
        f.offer(30_001, vec![input("activate")])?,
        (vec![6], vec![child.clone()])
    );
    let delivery = match f.on(
        &child,
        30_002,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )? {
        CommandOutcome::Received(Some(delivery)) => delivery,
        other => panic!("not received: {other:?}"),
    };
    f.on(
        &child,
        30_003,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.unwrap().token,
        },
    )?;
    assert_eq!(f.offer(30_004, vec![input("activate")])?, (vec![7], vec![]));
    f.offer(
        30_005,
        vec![MessageInput {
            time_to_live_millis: Some(0),
            ..input("ttl")
        }],
    )?;
    f.on(&child, 30_005, CommandKind::ExpireMessages)?;
    assert_eq!(f.ready(&child.dead_letter_queue()?), vec![8]);
    assert_eq!(f.offer(30_006, vec![input("ttl")])?, (vec![9], vec![]));
    assert_eq!(f.deadline("ttl"), Some(Timestamp::from_millis(50_005)));
    assert!(
        f.machine
            .duplicate_history_deadline(&f.ns, &child.dead_letter_queue()?, "ttl")?
            .is_none()
    );
    Ok(())
}

fn live_scheduled_history<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let child = f.child("a")?;
    assert_eq!(f.single(2, scheduled("live", 100))?, (vec![1], vec![]));
    assert_eq!(f.single(2, scheduled("live", 100))?, (vec![2], vec![]));
    let f = f.restart()?;
    assert_eq!(
        f.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![child.clone()]
        }
    );
    assert_eq!(f.ready(&child), vec![3]);
    assert_eq!(f.deadline("live"), Some(Timestamp::from_millis(20_002)));
    assert_eq!(f.single(101, input("live"))?, (vec![4], vec![]));
    Ok(())
}

fn no_matches_and_updates<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    assert_eq!(f.single(0, input("no-children"))?, (vec![1], vec![]));
    let child = f.child("a")?;
    f.on(
        &child,
        2,
        CommandKind::DeleteRule {
            name: RuleName::new("$Default")?,
        },
    )?;
    f.on(
        &child,
        3,
        CommandKind::CreateRule {
            name: RuleName::new("false")?,
            filter: RuleFilter::False,
        },
    )?;
    assert_eq!(
        f.offer(4, vec![input("no-rules"), input("no-children")])?,
        (vec![2, 3], vec![])
    );
    f.at(
        5,
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate {
                max_message_bytes: Some(100),
                ..TopicConfigUpdate::default()
            },
        },
    )?;
    f.at(
        6,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("a")?,
            update: SubscriptionConfigUpdate {
                max_delivery_count: Some(2),
                ..SubscriptionConfigUpdate::default()
            },
        },
    )?;
    assert_eq!(
        f.machine.topic_config(&f.ns, &f.topic)?,
        Some(TopicConfig {
            max_message_bytes: 100,
            ..config()
        })
    );
    assert_eq!(
        f.deadline("no-children"),
        Some(Timestamp::from_millis(WINDOW))
    );
    assert_eq!(
        f.deadline("no-rules"),
        Some(Timestamp::from_millis(WINDOW + 4))
    );
    f.on(
        &child,
        7,
        CommandKind::DeleteRule {
            name: RuleName::new("false")?,
        },
    )?;
    f.on(
        &child,
        8,
        CommandKind::CreateRule {
            name: RuleName::new("true")?,
            filter: RuleFilter::True,
        },
    )?;
    assert_eq!(f.single(9, input("no-rules"))?, (vec![4], vec![]));
    assert!(f.ready(&child).is_empty());
    let disabled = EntityPath::new("disabled")?;
    f.on(
        &disabled,
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let disabled_child = match f.on(
        &disabled,
        10,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("a")?,
            config: SubscriptionConfig::default(),
        },
    )? {
        CommandOutcome::SubscriptionCreated { entity } => entity,
        other => panic!("not subscribed: {other:?}"),
    };
    f.on(
        &disabled,
        11,
        CommandKind::SendBatch {
            messages: vec![input("same"), input("same")],
        },
    )?;
    assert!(
        f.machine
            .duplicate_history_deadline(&f.ns, &disabled, "same")?
            .is_none()
    );
    assert_eq!(f.ready(&disabled_child), vec![1, 2]);
    Ok(())
}

fn disabled_nondefault_window<P: StoreProvider>(provider: P) -> ResultTest {
    let config = TopicConfig {
        requires_duplicate_detection: false,
        duplicate_detection_history_millis: WINDOW,
        ..TopicConfig::default()
    };
    assert_ne!(
        config.duplicate_detection_history_millis,
        TopicConfig::default().duplicate_detection_history_millis
    );
    let f = Fixture::new(provider, config)?;
    let child = f.child("a")?;
    let assert_disabled_offers =
        |f: &Fixture<P>, expected: TopicConfig, at: u64, sequences: Vec<u64>| -> ResultTest {
            let stored = f
                .machine
                .store()
                .get(&keys::topic_config(&f.ns, &f.topic))?
                .unwrap();
            assert_eq!(stored.first(), Some(&codec::topic::VALUE_FORMAT_V2));
            assert_eq!(codec::topic::decode(&stored)?, expected);
            assert_eq!(f.machine.topic_config(&f.ns, &f.topic)?, Some(expected));
            // Exercise the unchanged V1-only generic version check used by old
            // readers, not an old binary. The tuple matches the old topic DTO.
            assert_eq!(
                codec::decode::<(Option<u64>, usize)>(&stored),
                Err(CodecError::UnsupportedVersion {
                    version: codec::topic::VALUE_FORMAT_V2
                })
            );
            assert_eq!(
                f.offer(at, vec![input("same"), input("same")])?,
                (sequences, vec![child.clone()])
            );
            assert_eq!(f.deadline("same"), None);
            for owner in [&f.topic, &child, &child.dead_letter_queue()?] {
                assert!(
                    f.machine
                        .store()
                        .get(&keys::duplicate_id(&f.ns, owner, "same"))?
                        .is_none()
                );
                assert!(
                    f.machine
                        .store()
                        .scan_prefix(&keys::duplicate_expiry_prefix(&f.ns, owner), 1)?
                        .is_empty()
                );
            }
            Ok(())
        };
    assert_disabled_offers(&f, config, 2, vec![1, 2])?;
    assert_eq!(
        f.at(
            3,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite {
                        millis: 60_000,
                    }),
                    max_message_bytes: Some(100),
                },
            },
        )?,
        CommandOutcome::TopicUpdated
    );
    let updated = TopicConfig {
        default_time_to_live_millis: Some(60_000),
        max_message_bytes: 100,
        ..config
    };
    assert_disabled_offers(&f, updated, 4, vec![3, 4])?;
    // Consumes and drops the sole machine/store handle before Fjall reopens.
    let f = f.restart()?;
    assert_disabled_offers(&f, updated, 6, vec![5, 6])?;
    assert_eq!(f.ready(&child), vec![1, 2, 3, 4, 5, 6]);
    Ok(())
}

fn atomic_refusals<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let child = f.child("a")?;
    f.offer(2, vec![input("hit")])?;
    let bad = [
        MessageInput {
            session_id: Some(SessionId::new("s")?),
            ..input("hit")
        },
        input(&"x".repeat(domain::MAX_MESSAGE_ID_CHARACTERS + 1)),
        MessageInput {
            body: vec![0; config().max_message_bytes + 1],
            ..input("hit")
        },
        MessageInput {
            envelope: Some(MessageEnvelope::new(vec![
                0;
                config().max_message_bytes + 1
            ])),
            ..input("hit")
        },
    ];
    for invalid in bad {
        let before = f.machine.store().snapshot()?;
        assert!(f.offer(3, vec![input("new"), invalid]).is_err());
        assert_eq!(f.machine.store().snapshot()?, before);
    }
    let value = domain::CorrelationValue::new(vec![1])?;
    let invalid_filter = MessageInput {
        envelope: Some(MessageEnvelope::new(vec![]).with_filter_properties(
            domain::FilterProperties {
                application_properties: std::collections::BTreeMap::from([
                    ("Color".into(), value.clone()),
                    ("color".into(), value),
                ]),
                ..domain::FilterProperties::default()
            },
        )),
        ..scheduled("hit", 10)
    };
    let before = f.machine.store().snapshot()?;
    assert!(f.offer(3, vec![input("new"), invalid_filter]).is_err());
    assert_eq!(f.machine.store().snapshot()?, before);
    let membership = keys::topic_subscription(&f.ns, &f.topic, &SubscriptionName::new("a")?);
    let raw = f.machine.store().get(&membership)?.unwrap();
    f.machine
        .store()
        .apply(WriteBatch::default().put(membership.clone(), vec![255]))?;
    let before = f.machine.store().snapshot()?;
    assert!(f.offer(3, vec![input("hit")]).is_err());
    assert_eq!(f.machine.store().snapshot()?, before);
    // All-scheduled offers still defer topology proof until activation.
    assert_eq!(f.offer(3, vec![scheduled("later", 10)])?, (vec![2], vec![]));
    f.machine
        .store()
        .apply(WriteBatch::default().put(membership, raw))?;
    for next in [u64::MAX - 1, u64::MAX] {
        f.machine.store().apply(WriteBatch::default().put(
            keys::queue_counters(&f.ns, &f.topic),
            codec::encode(&QueueCounters {
                next_sequence: next,
                next_lock_token: 1,
            })?,
        ))?;
        for messages in [
            vec![input("hit"), input("new")],
            vec![scheduled("new", 10), input("hit")],
            vec![scheduled("hit", 10), scheduled("new", 10)],
        ] {
            let before = f.machine.store().snapshot()?;
            assert_eq!(
                f.offer(4, messages),
                Err(BrokerError::SequenceNumberExhausted)
            );
            assert_eq!(f.machine.store().snapshot()?, before);
        }
    }
    f.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&f.ns, &f.topic),
        codec::encode(&QueueCounters {
            next_sequence: u64::MAX - 1,
            next_lock_token: 1,
        })?,
    ))?;
    let before = f.counters(&child)?;
    assert_eq!(
        f.offer(4, vec![input("hit")])?,
        (vec![u64::MAX - 1], vec![])
    );
    assert_eq!(f.counters(&child)?, before);
    assert!(f.deadline("new").is_none());
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(10, CommandKind::ActivateScheduled),
        Err(BrokerError::SequenceNumberExhausted)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    assert_eq!(f.deadline("later"), Some(Timestamp::from_millis(20_003)));
    Ok(())
}

fn cleanup<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let inputs = (0..TIMER_SCAN_LIMIT + 3)
        .map(|n| input(&format!("id-{n:05}")))
        .collect();
    f.offer(0, inputs)?;
    f.offer(WINDOW, vec![input("id-00000")])?;
    assert!(
        f.machine
            .store()
            .get(&keys::duplicate_expiry(
                &f.ns,
                &f.topic,
                Timestamp::from_millis(WINDOW),
                "id-00000"
            ))?
            .is_none()
    );
    // Simulate an older ordered generation left by a compatible writer.
    f.machine.store().apply(WriteBatch::default().put(
        keys::duplicate_expiry(&f.ns, &f.topic, Timestamp::from_millis(WINDOW), "id-00000"),
        vec![],
    ))?;
    let f = f.restart()?;
    assert_eq!(
        f.at(WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired {
            removed: TIMER_SCAN_LIMIT as u32
        }
    );
    assert_eq!(
        f.deadline("id-00000"),
        Some(Timestamp::from_millis(2 * WINDOW))
    );
    assert_eq!(
        f.at(WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 3 }
    );
    assert_eq!(
        f.at(WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 0 }
    );
    let f = f.restart()?;
    assert_eq!(
        f.deadline("id-00000"),
        Some(Timestamp::from_millis(2 * WINDOW))
    );
    assert!(f.deadline("id-00001").is_none());
    assert_eq!(
        f.at(2 * WINDOW, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 1 }
    );
    Ok(())
}

fn saturated_time<P: StoreProvider>(provider: P) -> ResultTest {
    let f = Fixture::new(provider, config())?;
    let child = f.child("a")?;
    assert_eq!(
        f.offer(u64::MAX - 1, vec![input("same"), input("same")])?,
        (vec![1, 2], vec![child.clone()])
    );
    assert_eq!(f.deadline("same"), Some(Timestamp::from_millis(u64::MAX)));
    assert_eq!(
        f.offer(u64::MAX, vec![input("same"), input("same")])?,
        (vec![3, 4], vec![child.clone()])
    );
    assert_eq!(f.ready(&child), vec![1, 3, 4]);
    let queue = EntityPath::new("q")?;
    f.on(
        &queue,
        u64::MAX,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                ..QueueConfig::default()
            },
        },
    )?;
    assert_eq!(
        f.on(
            &queue,
            u64::MAX,
            CommandKind::SendBatch {
                messages: vec![input("same"), input("same")]
            }
        )?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
            stored: 1
        }
    );
    assert_eq!(f.ready(&queue), vec![1]);
    Ok(())
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $case:ident) => {
        #[test]
        fn $memory() -> ResultTest {
            $case(testkit::MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> ResultTest {
            $case(testkit::DurableProvider::temporary()?)
        }
    };
}
paired!(immediate_memory, immediate_fjall, immediate);
paired!(scheduled_memory, scheduled_fjall, scheduled_lifecycle);
paired!(
    live_scheduled_memory,
    live_scheduled_fjall,
    live_scheduled_history
);
paired!(updates_memory, updates_fjall, no_matches_and_updates);
paired!(
    disabled_nondefault_window_memory,
    disabled_nondefault_window_fjall,
    disabled_nondefault_window
);
paired!(refusals_memory, refusals_fjall, atomic_refusals);
paired!(cleanup_memory, cleanup_fjall, cleanup);
paired!(saturated_memory, saturated_fjall, saturated_time);
