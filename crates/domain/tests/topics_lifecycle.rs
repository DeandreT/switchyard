//! Immediate match-all topic fanout on every storage backend.

use std::error::Error;

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, Delivery, EntityPath, LockToken,
    MessageEnvelope, MessageInput, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber,
    SessionId, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
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
            topic: EntityPath::new("events")?,
            machine: StateMachine::new(provider.open()?),
            provider,
        };
        assert_eq!(
            fixture.at(0, CommandKind::CreateTopic { config })?,
            CommandOutcome::TopicCreated
        );
        Ok(fixture)
    }

    fn at(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            self.topic.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }

    fn at_entity(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
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
        match self.at(
            millis,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config,
            },
        )? {
            CommandOutcome::SubscriptionCreated { entity } => {
                assert_eq!(entity, self.topic.subscription(&name)?);
                Ok(entity)
            }
            other => panic!("expected subscription creation, got {other:?}"),
        }
    }

    fn publish(
        &self,
        millis: u64,
        input: MessageInput,
    ) -> Result<(Vec<SequenceNumber>, Vec<EntityPath>), BrokerError> {
        match self.at(
            millis,
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
            } => Ok((sequences, subscriptions)),
            other => panic!("expected a publish outcome, got {other:?}"),
        }
    }

    fn receive(
        &self,
        entity: &EntityPath,
        millis: u64,
        mode: ReceiveMode,
    ) -> Result<Option<Delivery>, BrokerError> {
        match self.at_entity(
            entity,
            millis,
            CommandKind::Receive {
                mode,
                lock_duration_millis: None,
                session: None,
            },
        )? {
            CommandOutcome::Received(delivery) => Ok(delivery),
            other => panic!("expected a receive outcome, got {other:?}"),
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

fn input(id: &str, body: &[u8]) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: body.to_vec(),
        ..MessageInput::default()
    }
}

fn lock(delivery: &Delivery) -> (SequenceNumber, LockToken) {
    (
        delivery.sequence,
        delivery.lock.expect("peek-lock delivery").token,
    )
}

fn immediate_fanout_is_durable_and_each_copy_settles_independently<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let accounting = fixture.subscribe(1, "accounting", SubscriptionConfig::default())?;
    let analytics = fixture.subscribe(2, "analytics", SubscriptionConfig::default())?;
    let envelope = MessageEnvelope::new(vec![0, 0x53, 0x77, 0xa1, 3, b'a', b'm', b'q']);

    let (sequences, subscriptions) = fixture.publish(
        10,
        MessageInput {
            envelope: Some(envelope.clone()),
            ..input("invoice-1", b"invoice")
        },
    )?;
    assert_eq!(sequences, vec![SequenceNumber::new(1)]);
    assert_eq!(subscriptions, vec![accounting.clone(), analytics.clone()]);

    let first = fixture
        .receive(&accounting, 11, ReceiveMode::PeekLock)?
        .expect("accounting copy");
    let second = fixture
        .receive(&analytics, 11, ReceiveMode::PeekLock)?
        .expect("analytics copy");
    for copy in [&first, &second] {
        assert_eq!(copy.sequence, SequenceNumber::new(1));
        assert_eq!(copy.body, b"invoice".to_vec());
        assert_eq!(copy.envelope, Some(envelope.clone()));
        assert_eq!(copy.delivery_count, 1);
    }

    let (sequence, token) = lock(&first);
    assert_eq!(
        fixture.at_entity(
            &accounting,
            12,
            CommandKind::Complete {
                sequence,
                lock_token: token,
            },
        )?,
        CommandOutcome::Completed
    );
    let (sequence, token) = lock(&second);
    assert_eq!(
        fixture.at_entity(
            &analytics,
            12,
            CommandKind::Abandon {
                sequence,
                lock_token: token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false,
        }
    );
    assert_eq!(
        fixture.receive(&accounting, 13, ReceiveMode::ReceiveAndDelete)?,
        None
    );
    let redelivered = fixture
        .receive(&analytics, 13, ReceiveMode::ReceiveAndDelete)?
        .expect("abandoned copy returns");
    assert_eq!(redelivered.sequence, SequenceNumber::new(1));
    assert_eq!(redelivered.delivery_count, 2);
    Ok(())
}

fn batches_validate_before_atomic_fanout<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(
        provider,
        TopicConfig {
            max_message_bytes: 4,
            ..TopicConfig::default()
        },
    )?;
    let first = fixture.subscribe(1, "first", SubscriptionConfig::default())?;
    let second = fixture.subscribe(2, "second", SubscriptionConfig::default())?;

    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: vec![input("valid", b"1234"), input("invalid", b"12345")],
            },
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 5,
            maximum_bytes: 4,
        })
    );
    for subscription in [&first, &second] {
        assert!(
            fixture
                .machine
                .ready_sequences(&fixture.namespace, subscription, 10)?
                .is_empty()
        );
    }

    let outcome = fixture.at(
        11,
        CommandKind::SendBatch {
            messages: vec![input("one", b"1"), input("two", b"22")],
        },
    )?;
    let CommandOutcome::Published {
        sequences,
        subscriptions,
    } = outcome
    else {
        panic!("expected a publish outcome");
    };
    assert_eq!(
        sequences,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    assert_eq!(subscriptions, vec![first.clone(), second.clone()]);
    for subscription in [&first, &second] {
        assert_eq!(
            fixture
                .machine
                .ready_sequences(&fixture.namespace, subscription, 10)?,
            sequences
        );
    }
    Ok(())
}

fn zero_and_late_subscriptions_observe_log_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    assert_eq!(
        fixture.publish(1, input("before", b"before"))?,
        (vec![SequenceNumber::new(1)], Vec::new())
    );

    let late = fixture.subscribe(2, "late", SubscriptionConfig::default())?;
    assert_eq!(
        fixture.receive(&late, 3, ReceiveMode::ReceiveAndDelete)?,
        None,
        "a new subscription must not receive topic history"
    );
    assert_eq!(
        fixture.publish(4, input("after", b"after"))?,
        (vec![SequenceNumber::new(2)], vec![late.clone()])
    );
    let received = fixture
        .receive(&late, 5, ReceiveMode::ReceiveAndDelete)?
        .expect("future publication");
    assert_eq!(received.sequence, SequenceNumber::new(2));
    assert_eq!(received.body, b"after".to_vec());
    Ok(())
}

fn topic_subscription_metadata_and_ttl_survive_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(100),
            ..TopicConfig::default()
        },
    )?;
    let subscription_capped = fixture.subscribe(
        1,
        "subscription-cap",
        SubscriptionConfig {
            default_time_to_live_millis: Some(60),
            ..SubscriptionConfig::default()
        },
    )?;
    let topic_capped = fixture.subscribe(
        2,
        "topic-cap",
        SubscriptionConfig {
            default_time_to_live_millis: Some(200),
            ..SubscriptionConfig::default()
        },
    )?;
    fixture.publish(
        10,
        MessageInput {
            time_to_live_millis: Some(900),
            ..input("durable", b"payload")
        },
    )?;

    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.topic)?,
        Some(TopicConfig {
            default_time_to_live_millis: Some(100),
            ..TopicConfig::default()
        })
    );
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.topic, 10)?,
        vec![subscription_capped.clone(), topic_capped.clone()]
    );
    let subscription_capped_record = fixture
        .machine
        .message(
            &fixture.namespace,
            &subscription_capped,
            SequenceNumber::new(1),
        )?
        .expect("durable fanout copy");
    assert_eq!(
        subscription_capped_record.expires_at,
        Some(Timestamp::from_millis(70)),
        "the subscription default further caps the topic lifetime"
    );
    assert_eq!(subscription_capped_record.body, b"payload".to_vec());

    let topic_capped_record = fixture
        .machine
        .message(&fixture.namespace, &topic_capped, SequenceNumber::new(1))?
        .expect("durable fanout copy");
    assert_eq!(
        topic_capped_record.expires_at,
        Some(Timestamp::from_millis(110)),
        "the topic default caps a longer explicit message lifetime"
    );
    assert_eq!(topic_capped_record.body, b"payload".to_vec());
    Ok(())
}

fn subscription_copies_use_the_existing_dead_letter_lifecycle<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    let subscription = fixture.subscribe(
        1,
        "limited",
        SubscriptionConfig {
            max_delivery_count: 1,
            ..SubscriptionConfig::default()
        },
    )?;
    fixture.publish(2, input("limited", b"payload"))?;
    let delivery = fixture
        .receive(&subscription, 3, ReceiveMode::PeekLock)?
        .expect("subscription copy");
    let (sequence, lock_token) = lock(&delivery);
    assert_eq!(
        fixture.at_entity(
            &subscription,
            4,
            CommandKind::Abandon {
                sequence,
                lock_token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
        }
    );

    let dead_letter_queue = subscription.dead_letter_queue()?;
    let dead_letter = fixture
        .receive(&dead_letter_queue, 5, ReceiveMode::ReceiveAndDelete)?
        .expect("subscription DLQ copy");
    assert_eq!(dead_letter.sequence, sequence);
    assert_eq!(
        dead_letter
            .dead_letter
            .expect("dead-letter metadata")
            .reason
            .as_str(),
        "MaxDeliveryCountExceeded"
    );
    Ok(())
}

fn entity_conflicts_reserved_paths_and_deferred_shapes_are_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(provider, TopicConfig::default())?;
    assert_eq!(
        fixture.at(
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        Err(BrokerError::TopicAlreadyExists)
    );
    assert_eq!(
        fixture.at(
            1,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
        Err(BrokerError::EntityAlreadyExists)
    );
    let subscription = fixture.subscribe(2, "only", SubscriptionConfig::default())?;
    assert_eq!(
        fixture.at(
            3,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("only")?,
                config: SubscriptionConfig::default(),
            },
        ),
        Err(BrokerError::SubscriptionAlreadyExists)
    );
    assert_eq!(
        fixture.at_entity(
            &subscription,
            3,
            CommandKind::Send {
                message_id: String::from("direct"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ),
        Err(BrokerError::SubscriptionSendNotAllowed)
    );

    assert_eq!(
        fixture.at(
            4,
            CommandKind::Send {
                message_id: String::from("scheduled"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: Some(Timestamp::from_millis(100)),
                envelope: None,
            },
        ),
        Err(BrokerError::TopicSchedulingNotSupported)
    );
    assert_eq!(
        fixture.at(4, CommandKind::ActivateScheduled),
        Err(BrokerError::TopicSchedulingNotSupported)
    );
    assert_eq!(
        fixture.receive(&fixture.topic, 4, ReceiveMode::ReceiveAndDelete),
        Err(BrokerError::TopicReceiveNotSupported)
    );
    assert_eq!(
        fixture.at(
            4,
            CommandKind::Send {
                message_id: String::from("session"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: Some(SessionId::new("session-1")?),
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ),
        Err(BrokerError::TopicSessionNotSupported)
    );
    assert_eq!(
        fixture.at(
            4,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(1)],
            },
        ),
        Err(BrokerError::TopicSchedulingNotSupported)
    );
    assert_eq!(
        fixture.publish(5, input("first-valid", b"valid"))?.0,
        vec![SequenceNumber::new(1)],
        "rejected topic sends cannot consume sequence numbers"
    );
    Ok(())
}

fn queue_and_missing_topic_conflicts_are_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("orders")?;
    let machine = StateMachine::new(provider.open()?);
    let at = |millis, entity: &EntityPath, kind| {
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    assert_eq!(
        at(
            0,
            &queue,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?,
        CommandOutcome::QueueCreated
    );
    assert_eq!(
        at(
            1,
            &queue,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        Err(BrokerError::EntityAlreadyExists)
    );
    assert_eq!(
        at(
            1,
            &EntityPath::new("missing")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("orphan")?,
                config: SubscriptionConfig::default(),
            },
        ),
        Err(BrokerError::TopicNotFound)
    );
    for reserved in [
        "orders/subscriptions/forged",
        "orders/Subscriptions",
        "orders/$management",
    ] {
        assert_eq!(
            at(
                1,
                &EntityPath::new(reserved)?,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ),
            Err(BrokerError::EntityPathReserved),
            "reserved queue path {reserved:?} was accepted"
        );
    }
    for reserved in ["events/Subscriptions", "events/$Management"] {
        assert_eq!(
            at(
                1,
                &EntityPath::new(reserved)?,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            ),
            Err(BrokerError::EntityPathReserved),
            "reserved topic path {reserved:?} was accepted"
        );
    }
    Ok(())
}

fn maximum_topic_and_subscription_names_remain_addressable<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("t".repeat(domain::MAX_ENTITY_PATH_BYTES))?;
    let name = SubscriptionName::new("s".repeat(domain::MAX_SUBSCRIPTION_NAME_CHARACTERS))?;
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(0),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?,
        CommandOutcome::TopicCreated
    );
    let CommandOutcome::SubscriptionCreated {
        entity: subscription,
    } = machine.apply(&Command::new(
        namespace.clone(),
        topic.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateSubscription {
            name,
            config: SubscriptionConfig::default(),
        },
    ))?
    else {
        panic!("expected subscription creation");
    };
    let dead_letters = subscription.dead_letter_queue()?;
    assert!(subscription.as_str().len() > domain::MAX_ENTITY_PATH_BYTES);
    assert!(dead_letters.as_str().len() > subscription.as_str().len());
    drop(machine);

    // Timer discovery rehydrates queue-config keys after restart. Both the
    // maximum composite subscription and its DLQ must survive that path.
    let machine = StateMachine::new(provider.open()?);
    let queues = machine.queues(4)?;
    assert!(
        queues.contains(&(namespace.clone(), subscription.clone())),
        "the maximum subscription is discoverable after restart"
    );
    assert!(
        queues.contains(&(namespace.clone(), dead_letters)),
        "the maximum subscription DLQ is discoverable after restart"
    );
    assert!(
        machine.queue_config(&namespace, &subscription)?.is_some(),
        "the maximum subscription remains addressable"
    );
    Ok(())
}

#[test]
fn a_topic_enforces_the_standard_subscription_limit() -> Result<(), Box<dyn Error>> {
    let fixture = TopicFixture::new(testkit::MemoryProvider::new(), TopicConfig::default())?;
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        fixture.subscribe(
            index as u64 + 1,
            &format!("subscription-{index}"),
            SubscriptionConfig::default(),
        )?;
    }
    assert_eq!(
        fixture.at(
            domain::MAX_TOPIC_SUBSCRIPTIONS as u64 + 1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("one-too-many")?,
                config: SubscriptionConfig::default(),
            },
        ),
        Err(BrokerError::SubscriptionLimitExceeded {
            maximum: domain::MAX_TOPIC_SUBSCRIPTIONS,
        })
    );
    assert_eq!(
        fixture
            .machine
            .subscriptions(
                &fixture.namespace,
                &fixture.topic,
                domain::MAX_TOPIC_SUBSCRIPTIONS + 1,
            )?
            .len(),
        domain::MAX_TOPIC_SUBSCRIPTIONS
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
    immediate_fanout_is_durable_and_each_copy_settles_independently,
    batches_validate_before_atomic_fanout,
    zero_and_late_subscriptions_observe_log_order,
    topic_subscription_metadata_and_ttl_survive_restart,
    subscription_copies_use_the_existing_dead_letter_lifecycle,
    entity_conflicts_reserved_paths_and_deferred_shapes_are_rejected,
    queue_and_missing_topic_conflicts_are_rejected,
    maximum_topic_and_subscription_names_remain_addressable,
}
