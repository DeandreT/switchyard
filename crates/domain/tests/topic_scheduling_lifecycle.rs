//! Scheduled topic fanout semantics over every storage backend.

use std::error::Error;

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, CorrelationFilter, DEFAULT_RULE_NAME,
    Delivery, DeliveryOrigin, EntityPath, FilterProperties, MessageEnvelope, MessageInput,
    NamespaceName, QueueConfig, RuleFilter, RuleName, SequenceNumber, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
};
use testkit::StoreProvider;

struct TopicFixture<P: StoreProvider> {
    namespace: NamespaceName,
    topic: EntityPath,
    machine: StateMachine<P::Store>,
    provider: P,
}

impl<P: StoreProvider> TopicFixture<P> {
    fn new(provider: P, config: TopicConfig) -> Result<Self, Box<dyn Error>> {
        let fixture = Self {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new("scheduled-events")?,
            machine: StateMachine::new(provider.open()?),
            provider,
        };
        assert_eq!(
            fixture.topic_at(0, CommandKind::CreateTopic { config })?,
            CommandOutcome::TopicCreated
        );
        Ok(fixture)
    }

    fn topic_at(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.namespace, &self.topic, millis, kind)
    }

    fn at_entity(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }

    fn subscribe(
        &self,
        millis: u64,
        name: &str,
        config: SubscriptionConfig,
    ) -> Result<EntityPath, Box<dyn Error>> {
        let name = SubscriptionName::new(name)?;
        let CommandOutcome::SubscriptionCreated { entity } =
            self.topic_at(millis, CommandKind::CreateSubscription { name, config })?
        else {
            panic!("expected subscription creation");
        };
        Ok(entity)
    }

    fn publish(&self, millis: u64, message: MessageInput) -> Result<CommandOutcome, BrokerError> {
        self.topic_at(
            millis,
            CommandKind::Send {
                message_id: message.message_id,
                body: message.body,
                time_to_live_millis: message.time_to_live_millis,
                session_id: message.session_id,
                scheduled_enqueue_at: message.scheduled_enqueue_at,
                envelope: message.envelope,
            },
        )
    }

    fn peek(&self, entity: &EntityPath, millis: u64) -> Result<Vec<Delivery>, BrokerError> {
        match self.at_entity(
            &self.namespace,
            entity,
            millis,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 250,
                session: None,
            },
        )? {
            CommandOutcome::Peeked(messages) => Ok(messages),
            other => panic!("expected peek, got {other:?}"),
        }
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            namespace,
            topic,
            machine,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            namespace,
            topic,
            machine: StateMachine::new(provider.open()?),
            provider,
        })
    }
}

fn message(id: &str, body: &[u8], scheduled_at: Option<u64>, ttl: Option<u64>) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: body.to_vec(),
        time_to_live_millis: ttl,
        scheduled_enqueue_at: scheduled_at.map(Timestamp::from_millis),
        ..MessageInput::default()
    }
}

fn filtered_message(id: &str, subject: &str, scheduled_at: u64) -> MessageInput {
    MessageInput {
        envelope: Some(
            MessageEnvelope::new(format!("wire-{id}").into_bytes()).with_filter_properties(
                FilterProperties {
                    subject: Some(subject.to_owned()),
                    ..FilterProperties::default()
                },
            ),
        ),
        ..message(id, id.as_bytes(), Some(scheduled_at), None)
    }
}

fn sequence_range(first: u64, last: u64) -> Vec<SequenceNumber> {
    (first..=last).map(SequenceNumber::new).collect()
}

fn delete_default<P: StoreProvider>(
    fixture: &TopicFixture<P>,
    subscription: &EntityPath,
    millis: u64,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        fixture.at_entity(
            &fixture.namespace,
            subscription,
            millis,
            CommandKind::DeleteRule {
                name: RuleName::new(DEFAULT_RULE_NAME)?,
            },
        )?,
        CommandOutcome::RuleDeleted
    );
    Ok(())
}

fn add_subject_rule<P: StoreProvider>(
    fixture: &TopicFixture<P>,
    subscription: &EntityPath,
    millis: u64,
    name: &str,
    subject: &str,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        fixture.at_entity(
            &fixture.namespace,
            subscription,
            millis,
            CommandKind::CreateRule {
                name: RuleName::new(name)?,
                filter: RuleFilter::Correlation(CorrelationFilter {
                    subject: Some(subject.to_owned()),
                    ..CorrelationFilter::default()
                }),
            },
        )?,
        CommandOutcome::RuleCreated
    );
    Ok(())
}

fn scheduled_placeholders_are_topic_owned_cancellable_and_durable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let subscription = fixture.subscribe(1, "receiver", SubscriptionConfig::default())?;
    assert_eq!(
        fixture.publish(10, message("first", b"first", Some(100), None))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: Vec::new(),
        }
    );
    assert_eq!(
        fixture.publish(11, message("second", b"second", Some(200), None))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(2)],
            subscriptions: Vec::new(),
        }
    );
    assert!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &subscription, 10)?
            .is_empty()
    );
    assert!(fixture.peek(&subscription, 20)?.is_empty());
    let topic_peek = fixture.peek(&fixture.topic, 20)?;
    assert_eq!(
        topic_peek
            .iter()
            .map(|delivery| (delivery.sequence, delivery.origin))
            .collect::<Vec<_>>(),
        vec![
            (SequenceNumber::new(1), DeliveryOrigin::Scheduled),
            (SequenceNumber::new(2), DeliveryOrigin::Scheduled),
        ]
    );
    assert_eq!(
        fixture.topic_at(99, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new(),
        }
    );

    assert_eq!(
        fixture.topic_at(
            99,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1), SequenceNumber::new(999)],
            },
        ),
        Err(BrokerError::MessageNotFound {
            sequence: SequenceNumber::new(999),
        })
    );
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 10)?,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    assert_eq!(
        fixture.topic_at(
            99,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(2)],
            },
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.machine.topics(10)?,
        vec![(fixture.namespace.clone(), fixture.topic.clone())]
    );
    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![subscription.clone()],
        }
    );
    assert!(fixture.peek(&fixture.topic, 101)?.is_empty());
    let copy = fixture.peek(&subscription, 101)?;
    assert_eq!(copy.len(), 1);
    assert_eq!(copy[0].sequence, SequenceNumber::new(3));
    assert_eq!(copy[0].message_id, "first");
    assert_eq!(copy[0].enqueued_at, Timestamp::from_millis(100));
    assert_eq!(
        copy[0].scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );
    Ok(())
}

fn activation_batches_one_rule_snapshot_and_later_due_messages_see_changes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    assert!(matches!(
        fixture.publish(10, filtered_message("red-a", "selected", 100))?,
        CommandOutcome::Published { .. }
    ));
    assert!(matches!(
        fixture.publish(11, filtered_message("red-b", "selected", 100))?,
        CommandOutcome::Published { .. }
    ));
    assert!(matches!(
        fixture.publish(12, filtered_message("later", "selected", 200))?,
        CommandOutcome::Published { .. }
    ));

    // Both subscriptions and both matching rules are created after scheduling.
    let all = fixture.subscribe(20, "all", SubscriptionConfig::default())?;
    let filtered = fixture.subscribe(21, "filtered", SubscriptionConfig::default())?;
    delete_default(&fixture, &filtered, 22)?;
    add_subject_rule(&fixture, &filtered, 23, "selected-a", "selected")?;
    add_subject_rule(&fixture, &filtered, 24, "selected-b", "selected")?;

    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 2,
            deliverable_entities: vec![all.clone(), filtered.clone()],
        }
    );
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 10)?,
        vec![SequenceNumber::new(3)],
        "the later publication must remain a placeholder"
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &filtered, 10)?,
        vec![SequenceNumber::new(4), SequenceNumber::new(5)],
        "every due publication uses the same current rule snapshot, while two matching rules still materialize one copy per publication"
    );

    // Remove both rules before the distinctly later publication becomes due.
    // Routing uses the later command's current durable rule set, not either the
    // schedule-time snapshot or the preceding activation's snapshot.
    for (millis, name) in [(101, "selected-a"), (102, "selected-b")] {
        assert_eq!(
            fixture.at_entity(
                &fixture.namespace,
                &filtered,
                millis,
                CommandKind::DeleteRule {
                    name: RuleName::new(name)?,
                },
            )?,
            CommandOutcome::RuleDeleted
        );
    }
    assert_eq!(
        fixture.topic_at(199, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new(),
        }
    );
    assert_eq!(
        fixture.topic_at(200, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![all.clone()],
        }
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &all, 10)?,
        vec![
            SequenceNumber::new(4),
            SequenceNumber::new(5),
            SequenceNumber::new(6),
        ]
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &filtered, 10)?,
        vec![SequenceNumber::new(4), SequenceNumber::new(5)]
    );
    Ok(())
}

fn low_fanout_activation_drains_more_than_eight_due_publications<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let subscription = fixture.subscribe(1, "receiver", SubscriptionConfig::default())?;
    let messages = (0..12)
        .map(|index| message(&format!("scheduled-{index}"), b"body", Some(100), None))
        .collect();
    assert_eq!(
        fixture.topic_at(10, CommandKind::SendBatch { messages })?,
        CommandOutcome::Published {
            sequences: sequence_range(1, 12),
            subscriptions: Vec::new(),
        }
    );

    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 12,
            deliverable_entities: vec![subscription.clone()],
        }
    );
    assert!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 20)?
            .is_empty()
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &subscription, 20)?,
        sequence_range(13, 24)
    );
    Ok(())
}

fn fanout_aware_activation_budget_is_bounded_and_resumable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let mut subscriptions = Vec::new();
    for (millis, name) in [(1, "a"), (2, "b"), (3, "c"), (4, "d")] {
        subscriptions.push(fixture.subscribe(millis, name, SubscriptionConfig::default())?);
    }
    let messages = (0..65)
        .map(|index| message(&format!("scheduled-{index}"), b"body", Some(100), None))
        .collect();
    assert_eq!(
        fixture.topic_at(10, CommandKind::SendBatch { messages })?,
        CommandOutcome::Published {
            sequences: sequence_range(1, 65),
            subscriptions: Vec::new(),
        }
    );

    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 64,
            deliverable_entities: subscriptions.clone(),
        },
        "four-way fanout limits one command to 256 subscription copies"
    );
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 10)?,
        vec![SequenceNumber::new(65)]
    );
    for subscription in &subscriptions {
        assert_eq!(
            fixture
                .machine
                .ready_sequences(&fixture.namespace, subscription, 100)?,
            sequence_range(66, 129)
        );
    }

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: subscriptions.clone(),
        }
    );
    assert!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 10)?
            .is_empty()
    );
    for subscription in &subscriptions {
        assert_eq!(
            fixture
                .machine
                .ready_sequences(&fixture.namespace, subscription, 100)?,
            sequence_range(66, 130)
        );
    }
    Ok(())
}

fn mixed_batches_validate_atomically_and_ttl_rebases_on_activation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(90),
            max_message_bytes: 5,
        },
    )?;
    let long = fixture.subscribe(
        1,
        "long",
        SubscriptionConfig {
            default_time_to_live_millis: Some(200),
            ..SubscriptionConfig::default()
        },
    )?;
    let short = fixture.subscribe(
        2,
        "short",
        SubscriptionConfig {
            default_time_to_live_millis: Some(60),
            ..SubscriptionConfig::default()
        },
    )?;

    assert_eq!(
        fixture.topic_at(
            10,
            CommandKind::SendBatch {
                messages: vec![
                    message("later", b"12345", Some(100), Some(80)),
                    message("invalid", b"123456", None, None),
                ],
            },
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 6,
            maximum_bytes: 5,
        })
    );
    assert!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.topic, 10)?
            .is_empty()
    );

    assert_eq!(
        fixture.topic_at(
            11,
            CommandKind::SendBatch {
                messages: vec![
                    message("later", b"12345", Some(100), Some(80)),
                    message("now", b"now", None, None),
                ],
            },
        )?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
            subscriptions: vec![long.clone(), short.clone()],
        }
    );
    assert_eq!(
        fixture.topic_at(120, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![long.clone(), short.clone()],
        }
    );
    for (subscription, expected_expiry) in [(&long, 200), (&short, 180)] {
        let record = fixture
            .machine
            .message(&fixture.namespace, subscription, SequenceNumber::new(3))?
            .expect("activation writes a subscription copy");
        assert_eq!(record.enqueued_at, Timestamp::from_millis(120));
        assert_eq!(
            record.expires_at,
            Some(Timestamp::from_millis(expected_expiry))
        );
        assert_eq!(
            record.scheduled_enqueue_at,
            Some(Timestamp::from_millis(100))
        );
    }
    Ok(())
}

fn an_unmatched_activation_drops_atomically_and_advances_topic_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let filtered = fixture.subscribe(1, "filtered", SubscriptionConfig::default())?;
    delete_default(&fixture, &filtered, 2)?;
    add_subject_rule(&fixture, &filtered, 3, "wanted", "wanted")?;
    assert_eq!(
        fixture.publish(10, filtered_message("ignored", "other", 100))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: Vec::new(),
        }
    );
    assert_eq!(
        fixture.topic_at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: Vec::new(),
        }
    );
    assert!(fixture.peek(&fixture.topic, 101)?.is_empty());
    assert!(fixture.peek(&filtered, 101)?.is_empty());
    assert_eq!(
        fixture.publish(102, message("after", b"after", None, None))?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(3)],
            subscriptions: Vec::new(),
        },
        "the dropped activation still owns its fresh active topic sequence"
    );
    Ok(())
}

fn topic_catalog_pages_only_topics_and_survives_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let other_namespace = NamespaceName::new("other")?;
    let other_topic = EntityPath::new("alpha")?;
    assert_eq!(
        fixture.at_entity(
            &other_namespace,
            &other_topic,
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?,
        CommandOutcome::TopicCreated
    );
    let queue = EntityPath::new("not-a-topic")?;
    assert_eq!(
        fixture.at_entity(
            &fixture.namespace,
            &queue,
            2,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?,
        CommandOutcome::QueueCreated
    );

    let first = fixture.machine.topics(1)?;
    assert_eq!(first, vec![(other_namespace.clone(), other_topic.clone())]);
    assert_eq!(
        fixture.machine.topics_after(first.first(), 10)?,
        vec![(fixture.namespace.clone(), fixture.topic.clone())]
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.machine.topics(10)?,
        vec![
            (other_namespace, other_topic),
            (fixture.namespace.clone(), fixture.topic.clone()),
        ]
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    scheduled_placeholders_are_topic_owned_cancellable_and_durable,
    activation_batches_one_rule_snapshot_and_later_due_messages_see_changes,
    low_fanout_activation_drains_more_than_eight_due_publications,
    fanout_aware_activation_budget_is_bounded_and_resumable,
    mixed_batches_validate_atomically_and_ttl_rebases_on_activation,
    an_unmatched_activation_drops_atomically_and_advances_topic_order,
    topic_catalog_pages_only_topics_and_survives_restart,
}
