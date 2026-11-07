//! Content sends: derivation at apply time and V2 records through every move,
//! over every storage backend and across reopen.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CONTENT_LIMITS, Command, CommandKind, CommandOutcome, CorrelationFilter,
    CorrelationValue, DEFAULT_RULE_NAME, DeadLetterReason, Delivery, DurableProposal, EntityPath,
    FilterProperties, IndexedApplyOutcome, IndexedWriter, MessageContent, MessageContentError,
    MessageRecord, MessageState, NamespaceName, QueueConfig, RawMessage, ReceiveMode, RuleFilter,
    RuleName, SequenceNumber, SessionId, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, keys, snapshot_validation::validate_complete_state,
};
use message_content::{
    AnnotationKey, Binary, Body, Content, Field, Header, MessageId, Properties, Symbol, Text,
    Value, Width, encode_content,
};
use storage::{MemoryStore, StateStore};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(DurableProvider::temporary()?)
        }
    };
}

fn text(value: &str) -> Text {
    Text {
        text: value.into(),
        width: Width::One,
    }
}

fn symbol(value: &str) -> Symbol {
    Symbol {
        text: value.into(),
        width: Width::One,
    }
}

#[derive(Default)]
struct Message<'a> {
    id: &'a str,
    session: Option<&'a str>,
    ttl: Option<u32>,
    scheduled: Option<i64>,
    region: Option<&'a str>,
}

fn amqp(message: Message<'_>) -> Vec<u8> {
    let content = Content {
        header: message.ttl.map(|ttl| Header {
            durable: Field::Null,
            priority: Field::Null,
            ttl: Field::Value(ttl),
            ..Header::default()
        }),
        message_annotations: message.scheduled.map(|millis| {
            vec![(
                AnnotationKey::Symbol(symbol("x-opt-scheduled-enqueue-time")),
                Value::Timestamp(millis),
            )]
        }),
        properties: Some(match message.session {
            None => Properties {
                message_id: Field::Value(MessageId::String(text(message.id))),
                ..Properties::default()
            },
            // A present field needs explicit nulls before it.
            Some(session) => Properties {
                message_id: Field::Value(MessageId::String(text(message.id))),
                user_id: Field::Null,
                to: Field::Null,
                subject: Field::Null,
                reply_to: Field::Null,
                correlation_id: Field::Null,
                content_type: Field::Null,
                content_encoding: Field::Null,
                absolute_expiry_time: Field::Null,
                creation_time: Field::Null,
                group_id: Field::Value(text(session)),
                ..Properties::default()
            },
        }),
        application_properties: message
            .region
            .map(|region| vec![(text("region"), Value::String(text(region)))]),
        body: Body::Data(vec![Binary {
            bytes: message.id.as_bytes().to_vec(),
            width: Width::One,
        }]),
        ..Content::default()
    };
    encode_content(&content, CONTENT_LIMITS).expect("test content encodes")
}

fn send(content: Vec<u8>) -> CommandKind {
    CommandKind::SendContent {
        content: MessageContent::Amqp(content),
    }
}

/// The row's leading version byte and its record, which must re-encode exactly.
fn row<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Option<(u8, MessageRecord)> {
    let raw = machine
        .store()
        .get(&keys::message(namespace, entity, sequence))
        .unwrap()?;
    let record = MessageRecord::decode(&raw).unwrap();
    assert_eq!(domain::codec::message::encode(&record).unwrap(), raw);
    Some((raw[0], record))
}

fn v2<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> MessageRecord {
    let (version, record) = row(machine, namespace, entity, sequence).expect("stored row");
    assert_eq!(version, 2, "{entity} {sequence}");
    record
}

fn validated<S: StateStore>(machine: &StateMachine<S>) -> usize {
    let image = machine.store().snapshot().unwrap().entries().to_vec();
    validate_complete_state(&image).unwrap().message_rows()
}

fn received(outcome: CommandOutcome) -> Delivery {
    match outcome {
        CommandOutcome::Received(Some(delivery)) => delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

fn lock<P: StoreProvider>(fixture: &QueueFixture<P>, at: u64) -> Delivery {
    received(
        fixture
            .at(
                at,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .unwrap(),
    )
}

fn queue_lifecycle<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let (namespace, queue) = (fixture.namespace.clone(), fixture.entity.clone());
    let dead_letters = queue.dead_letter_queue()?;
    let original = amqp(Message {
        id: "m-1",
        ttl: Some(60_000),
        region: Some("West"),
        ..Message::default()
    });
    assert_eq!(
        fixture.at(1, send(original.clone()))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let raw = MessageContent::Raw(RawMessage {
        message_id: "raw-1".into(),
        body: b"raw".to_vec(),
        ..RawMessage::default()
    });
    fixture.at(2, CommandKind::SendContent { content: raw })?;
    let scheduled = amqp(Message {
        id: "later",
        scheduled: Some(100),
        ..Message::default()
    });
    fixture.at(3, send(scheduled.clone()))?;

    let record = v2(&fixture.machine, &namespace, &queue, SequenceNumber::new(1));
    assert_eq!(record.message_id, "m-1");
    assert!(record.body.is_empty(), "AMQP content is stored once");
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(60_001)));
    let envelope = record.envelope.expect("AMQP content keeps its envelope");
    assert_eq!(envelope.as_bytes(), original);
    assert!(envelope.overlay().is_empty());
    assert_eq!(
        envelope.filter_properties().message_id.as_deref(),
        Some("m-1")
    );
    let record = v2(&fixture.machine, &namespace, &queue, SequenceNumber::new(2));
    assert_eq!(
        (record.body.as_slice(), record.envelope),
        (&b"raw"[..], None)
    );
    let placeholder = v2(&fixture.machine, &namespace, &queue, SequenceNumber::new(3));
    assert_eq!(placeholder.state, MessageState::Scheduled);
    assert_eq!(
        placeholder.scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );

    let fixture = fixture.restart()?;
    let delivery = lock(&fixture, 4);
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(
        delivery
            .envelope
            .as_ref()
            .map(|envelope| envelope.as_bytes()),
        Some(original.as_slice())
    );
    let token = delivery.lock.expect("peek-lock").token;
    assert!(matches!(
        v2(&fixture.machine, &namespace, &queue, delivery.sequence).state,
        MessageState::Locked { .. }
    ));
    fixture.at(
        5,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: token,
            replacement_envelope: None,
        },
    )?;
    assert_eq!(
        v2(&fixture.machine, &namespace, &queue, delivery.sequence).state,
        MessageState::Deferred
    );
    let CommandOutcome::DeferredReceived(deferred) = fixture.at(
        6,
        CommandKind::ReceiveDeferred {
            sequences: vec![delivery.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected deferred receive");
    };
    fixture.at(
        7,
        CommandKind::DeadLetter {
            sequence: delivery.sequence,
            lock_token: deferred[0].lock.expect("peek-lock").token,
            reason: "Rejected".into(),
            description: "test".into(),
            replacement_envelope: None,
        },
    )?;
    assert!(row(&fixture.machine, &namespace, &queue, delivery.sequence).is_none());
    let dead = v2(
        &fixture.machine,
        &namespace,
        &dead_letters,
        delivery.sequence,
    );
    assert_eq!(dead.envelope.expect("kept").as_bytes(), original);

    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![queue.clone()],
        }
    );
    let fixture = fixture.restart()?;
    let raw_delivery = lock(&fixture, 101);
    assert_eq!(raw_delivery.body, b"raw");
    let activated = lock(&fixture, 102);
    assert_eq!(activated.message_id, "later");
    let record = v2(&fixture.machine, &namespace, &queue, activated.sequence);
    assert_eq!(
        record.envelope.expect("kept").as_bytes(),
        scheduled.as_slice()
    );
    assert_eq!(validated(&fixture.machine), 3);
    Ok(())
}
paired!(
    queue_lifecycle_keeps_v2_records_on_memory,
    queue_lifecycle_keeps_v2_records_on_fjall,
    queue_lifecycle
);

fn topic_lifecycle<P: StoreProvider>(provider: P) -> TestResult {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let mut machine = StateMachine::new(provider.open()?);
    let at = |machine: &StateMachine<P::Store>, entity: &EntityPath, millis, kind| {
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    at(
        &machine,
        &topic,
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let mut subscriptions = Vec::new();
    for name in ["all", "west"] {
        let CommandOutcome::SubscriptionCreated { entity } = at(
            &machine,
            &topic,
            0,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )?
        else {
            panic!("expected subscription");
        };
        subscriptions.push(entity);
    }
    let (all, west) = (subscriptions[0].clone(), subscriptions[1].clone());
    // The rule value is the canonical encoding of the AMQP string "West".
    at(
        &machine,
        &west,
        0,
        CommandKind::DeleteRule {
            name: RuleName::new(DEFAULT_RULE_NAME)?,
        },
    )?;
    at(
        &machine,
        &west,
        0,
        CommandKind::CreateRule {
            name: RuleName::new("west")?,
            filter: RuleFilter::Correlation(CorrelationFilter {
                application_properties: BTreeMap::from([(
                    "region".to_owned(),
                    CorrelationValue::new(*b"\xa1\x04West")?,
                )]),
                ..CorrelationFilter::default()
            }),
        },
    )?;

    let messages = [
        amqp(Message {
            id: "west-1",
            region: Some("West"),
            ..Message::default()
        }),
        amqp(Message {
            id: "east-1",
            region: Some("East"),
            ..Message::default()
        }),
        amqp(Message {
            id: "west-later",
            region: Some("West"),
            scheduled: Some(50),
            ..Message::default()
        }),
    ];
    assert_eq!(
        at(
            &machine,
            &topic,
            1,
            CommandKind::SendContentBatch {
                messages: messages.iter().cloned().map(MessageContent::Amqp).collect(),
            },
        )?,
        CommandOutcome::Published {
            sequences: (1..=3).map(SequenceNumber::new).collect(),
            subscriptions: vec![all.clone(), west.clone()],
        }
    );
    for (entity, sequence) in [(&all, 1), (&all, 2), (&west, 1)] {
        let record = v2(&machine, &namespace, entity, SequenceNumber::new(sequence));
        assert_eq!(
            record.envelope.expect("kept").as_bytes(),
            messages[sequence as usize - 1]
        );
    }
    assert!(row(&machine, &namespace, &west, SequenceNumber::new(2)).is_none());
    assert_eq!(
        v2(&machine, &namespace, &topic, SequenceNumber::new(3)).state,
        MessageState::Scheduled
    );

    drop(machine);
    machine = StateMachine::new(provider.open()?);
    assert_eq!(
        at(&machine, &topic, 50, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![all.clone(), west.clone()],
        }
    );
    for entity in [&all, &west] {
        let record = v2(&machine, &namespace, entity, SequenceNumber::new(4));
        assert_eq!(record.message_id, "west-later");
        assert_eq!(record.state, MessageState::Ready);
    }
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(validated(&machine), 5);
    Ok(())
}
paired!(
    topic_fanout_and_activation_keep_v2_records_on_memory,
    topic_fanout_and_activation_keep_v2_records_on_fjall,
    topic_lifecycle
);

fn batch_refusal<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let valid = |id: &str| {
        MessageContent::Amqp(amqp(Message {
            id,
            ..Message::default()
        }))
    };
    let invalid = MessageContent::Amqp(vec![0x00, 0x53, 0x75, 0xa0, 0x09]);
    fixture.at(
        1,
        CommandKind::SendContent {
            content: valid("before"),
        },
    )?;
    for position in 0..3 {
        let before = fixture.machine.store().snapshot()?;
        let mut messages = vec![valid("a"), valid("b")];
        messages.insert(position, invalid.clone());
        assert_eq!(
            fixture.at(2, CommandKind::SendContentBatch { messages }),
            Err(BrokerError::MessageContent(MessageContentError::Amqp(
                message_content::Error::Truncated
            ))),
            "invalid child at {position}"
        );
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    assert_eq!(
        fixture.at(
            3,
            CommandKind::SendContent {
                content: valid("after")
            }
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );
    let fixture = fixture.restart()?;
    for (sequence, id) in [(1, "before"), (2, "after")] {
        let record = v2(
            &fixture.machine,
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(sequence),
        );
        assert_eq!(record.message_id, id);
    }
    assert_eq!(validated(&fixture.machine), 2);
    Ok(())
}
paired!(
    an_invalid_batch_child_refuses_the_batch_on_memory,
    an_invalid_batch_child_refuses_the_batch_on_fjall,
    batch_refusal
);

#[test]
fn derived_fields_follow_the_content_and_invalid_content_refuses() {
    let session = amqp(Message {
        id: "m",
        session: Some("cart-1"),
        ttl: Some(5),
        scheduled: Some(9),
        ..Message::default()
    });
    let input = MessageContent::Amqp(session).derive().unwrap();
    assert_eq!(input.message_id, "m");
    assert_eq!(input.session_id, Some(SessionId::new("cart-1").unwrap()));
    assert_eq!(input.time_to_live_millis, Some(5));
    assert_eq!(input.scheduled_enqueue_at, Some(Timestamp::from_millis(9)));
    assert!(input.body.is_empty());
    assert_eq!(
        input.envelope.unwrap().filter_properties(),
        &FilterProperties {
            message_id: Some("m".into()),
            session_id: Some("cart-1".into()),
            ..FilterProperties::default()
        }
    );

    for (message, expected) in [
        (
            Message {
                id: "m",
                session: Some(""),
                ..Message::default()
            },
            MessageContentError::InvalidSessionId,
        ),
        (
            Message {
                id: "m",
                scheduled: Some(-1),
                ..Message::default()
            },
            MessageContentError::ScheduledEnqueueTimeBeforeEpoch,
        ),
    ] {
        assert_eq!(MessageContent::Amqp(amqp(message)).derive(), Err(expected));
    }
    let not_timestamp = Content {
        message_annotations: Some(vec![(
            AnnotationKey::Symbol(symbol("x-opt-scheduled-enqueue-time")),
            Value::Long(9),
        )]),
        ..Content::default()
    };
    assert_eq!(
        MessageContent::Amqp(encode_content(&not_timestamp, CONTENT_LIMITS).unwrap()).derive(),
        Err(MessageContentError::ScheduledEnqueueTimeNotTimestamp)
    );
    let oversized = Content {
        application_properties: Some(vec![(
            text("big"),
            Value::Binary(Binary {
                bytes: vec![0; domain::MAX_CORRELATION_VALUE_BYTES],
                width: Width::Four,
            }),
        )]),
        ..Content::default()
    };
    assert_eq!(
        MessageContent::Amqp(encode_content(&oversized, CONTENT_LIMITS).unwrap()).derive(),
        Err(MessageContentError::PropertyValueTooLarge {
            maximum: domain::MAX_CORRELATION_VALUE_BYTES
        })
    );
    // Nesting beyond the fixed profile refuses rather than recursing further.
    let mut nested = vec![0x45];
    for _ in 0..200 {
        let mut outer = vec![0xd0];
        outer.extend((nested.len() as u32 + 4).to_be_bytes());
        outer.extend(1_u32.to_be_bytes());
        outer.extend(nested);
        nested = outer;
    }
    let mut deep = vec![0x00, 0x53, 0x77];
    deep.extend(nested);
    assert_eq!(
        MessageContent::Amqp(deep).derive(),
        Err(MessageContentError::Amqp(
            message_content::Error::DepthLimit
        ))
    );
}

#[test]
fn refused_content_checkpoints_through_the_indexed_writer_without_effects() {
    let mut writer = IndexedWriter::open(MemoryStore::default()).unwrap();
    let proposal = |millis, kind| {
        DurableProposal::unbound(Command::new(
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("orders").unwrap(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    writer
        .apply(
            1,
            &proposal(
                0,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ),
        )
        .unwrap();
    let invalid = proposal(1, send(vec![0xff]));
    let decoded = DurableProposal::decode(&invalid.encode().unwrap()).unwrap();
    assert_eq!(
        writer.apply(2, &decoded).unwrap(),
        IndexedApplyOutcome::Refused(BrokerError::MessageContent(MessageContentError::Amqp(
            message_content::Error::InvalidConstructor
        )))
    );
    assert_eq!(writer.applied_index().unwrap(), 2);
    assert_eq!(
        writer
            .apply(
                3,
                &proposal(
                    2,
                    send(amqp(Message {
                        id: "m",
                        ..Message::default()
                    }))
                )
            )
            .unwrap(),
        IndexedApplyOutcome::Applied(CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        })
    );
}

/// A bare data section only: AMQP content with no properties, so the message
/// identifier derives to the empty string.
fn amqp_without_id() -> Vec<u8> {
    encode_content(
        &Content {
            body: Body::Data(vec![Binary {
                bytes: vec![1],
                width: Width::One,
            }]),
            ..Content::default()
        },
        CONTENT_LIMITS,
    )
    .expect("bare content encodes")
}

fn sent(outcome: CommandOutcome) -> SequenceNumber {
    match outcome {
        CommandOutcome::Sent { sequence } => sequence,
        other => panic!("expected a send, got {other:?}"),
    }
}

#[test]
fn duplicate_detection_suppresses_repeated_content_sends_on_a_queue() -> TestResult {
    let fixture = QueueFixture::new(
        MemoryProvider::new(),
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_millis: 60_000,
            ..QueueConfig::default()
        },
    )?;
    let identified = send(amqp(Message {
        id: "m-1",
        ..Message::default()
    }));
    assert_eq!(
        fixture.at(1, identified.clone())?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    assert_eq!(
        fixture.at(2, identified)?,
        CommandOutcome::DuplicateSuppressed {
            sequence: SequenceNumber::new(2)
        }
    );
    // An AMQP message with no message-id derives "". An absent identifier acts
    // like a broker-assigned unique one, so anonymous messages never collapse
    // onto a shared empty-string history entry.
    let bare = CommandKind::SendContent {
        content: MessageContent::Amqp(amqp_without_id()),
    };
    assert_eq!(
        fixture.at(3, bare.clone())?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(3)
        }
    );
    assert_eq!(
        fixture.at(4, bare)?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(4)
        }
    );
    v2(
        &fixture.machine,
        &fixture.namespace,
        &fixture.entity,
        SequenceNumber::new(1),
    );
    assert_eq!(
        v2(
            &fixture.machine,
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(3),
        )
        .message_id,
        ""
    );
    Ok(())
}

#[test]
fn duplicate_detection_suppresses_repeated_content_publications_on_a_topic() -> TestResult {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let machine = StateMachine::new(MemoryProvider::new().open()?);
    let at = |millis, entity: &EntityPath, kind| {
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    at(
        0,
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 60_000,
                ..TopicConfig::default()
            },
        },
    )?;
    let CommandOutcome::SubscriptionCreated { entity: all } = at(
        0,
        &topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("all")?,
            config: SubscriptionConfig::default(),
        },
    )?
    else {
        panic!("expected subscription");
    };
    let publish = send(amqp(Message {
        id: "m",
        ..Message::default()
    }));
    assert_eq!(
        at(1, &topic, publish.clone())?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: vec![all.clone()],
        }
    );
    assert_eq!(
        at(2, &topic, publish)?,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(2)],
            subscriptions: vec![],
        }
    );
    // Exactly one copy reached the subscription; the duplicate stored nothing.
    assert!(row(&machine, &namespace, &all, SequenceNumber::new(1)).is_some());
    assert!(row(&machine, &namespace, &all, SequenceNumber::new(2)).is_none());
    Ok(())
}

#[test]
fn session_and_size_rules_apply_to_derived_content() -> TestResult {
    let sessionful = QueueFixture::new(
        MemoryProvider::new(),
        "tenant",
        "sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let with_session = |id, session| {
        send(amqp(Message {
            id,
            session: Some(session),
            ..Message::default()
        }))
    };
    assert_eq!(
        sessionful.at(1, with_session("m-1", "cart-1"))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    // No group-id derives no session, which a session queue refuses.
    assert_eq!(
        sessionful.at(
            2,
            send(amqp(Message {
                id: "m-2",
                ..Message::default()
            }))
        ),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(
        sessionful.at(
            3,
            CommandKind::SendContentBatch {
                messages: vec![
                    MessageContent::Amqp(amqp(Message {
                        id: "a",
                        session: Some("cart-1"),
                        ..Message::default()
                    })),
                    MessageContent::Amqp(amqp(Message {
                        id: "b",
                        session: Some("cart-2"),
                        ..Message::default()
                    })),
                ],
            },
        ),
        Err(BrokerError::MessageBatchSessionMismatch)
    );

    // A group-id carried to a topic is a session the topic path refuses.
    let namespace = NamespaceName::new("tenant")?;
    let events = EntityPath::new("events")?;
    let machine = StateMachine::new(MemoryProvider::new().open()?);
    machine.apply(&Command::new(
        namespace.clone(),
        events.clone(),
        Timestamp::from_millis(0),
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    ))?;
    assert_eq!(
        machine.apply(&Command::new(
            namespace,
            events,
            Timestamp::from_millis(1),
            send(amqp(Message {
                id: "m",
                session: Some("cart-1"),
                ..Message::default()
            })),
        )),
        Err(BrokerError::TopicSessionNotSupported)
    );

    // The size limit measures the retained original bytes of AMQP content.
    let small = QueueFixture::new(
        MemoryProvider::new(),
        "tenant",
        "tiny",
        QueueConfig {
            max_message_bytes: 4,
            ..QueueConfig::default()
        },
    )?;
    let content = amqp(Message {
        id: "oversized",
        ..Message::default()
    });
    assert_eq!(
        small.at(1, send(content.clone())),
        Err(BrokerError::MessageTooLarge {
            body_bytes: content.len(),
            maximum_bytes: 4,
        })
    );
    Ok(())
}

fn survival<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 100,
            max_delivery_count: 1,
            ..QueueConfig::default()
        },
    )?;
    let (namespace, queue) = (fixture.namespace.clone(), fixture.entity.clone());
    let dlq = queue.dead_letter_queue()?;

    // An AMQP record deferred with a replacement envelope, a raw record
    // dead-lettered with a replacement envelope, a max-delivery-count victim
    // and a TTL victim all stay V2.
    let deferred = sent(fixture.at(
        1,
        send(amqp(Message {
            id: "deferred",
            ..Message::default()
        })),
    )?);
    let raw = sent(fixture.at(
        1,
        CommandKind::SendContent {
            content: MessageContent::Raw(RawMessage {
                message_id: "raw".into(),
                body: b"raw".to_vec(),
                ..RawMessage::default()
            }),
        },
    )?);
    let exhausted = sent(fixture.at(
        1,
        send(amqp(Message {
            id: "exhausted",
            ..Message::default()
        })),
    )?);
    let expiring = sent(fixture.at(
        1,
        CommandKind::SendContent {
            content: MessageContent::Amqp(amqp(Message {
                id: "expiring",
                ttl: Some(50),
                ..Message::default()
            })),
        },
    )?);

    let replacement = domain::MessageEnvelope::new(amqp(Message {
        id: "replaced",
        ..Message::default()
    }));
    let delivery = lock(&fixture, 2);
    assert_eq!(delivery.sequence, deferred);
    fixture.at(
        3,
        CommandKind::Defer {
            sequence: deferred,
            lock_token: delivery.lock.expect("peek-lock").token,
            replacement_envelope: Some(replacement.clone()),
        },
    )?;
    let record = v2(&fixture.machine, &namespace, &queue, deferred);
    assert_eq!(record.state, MessageState::Deferred);
    assert_eq!(record.envelope.as_ref(), Some(&replacement));
    assert!(record.body.is_empty());

    let raw_delivery = lock(&fixture, 4);
    assert_eq!(raw_delivery.sequence, raw);
    fixture.at(
        5,
        CommandKind::DeadLetter {
            sequence: raw,
            lock_token: raw_delivery.lock.expect("peek-lock").token,
            reason: "Rejected".into(),
            description: String::new(),
            replacement_envelope: Some(replacement.clone()),
        },
    )?;
    // A raw V2 record given a replacement envelope becomes AMQP V2 content.
    let dead_raw = v2(&fixture.machine, &namespace, &dlq, raw);
    assert_eq!(dead_raw.envelope.as_ref(), Some(&replacement));
    assert!(dead_raw.body.is_empty());

    // Abandon past the delivery limit dead-letters the record, still V2.
    let exhausted_delivery = lock(&fixture, 6);
    assert_eq!(exhausted_delivery.sequence, exhausted);
    assert_eq!(
        fixture.at(
            7,
            CommandKind::Abandon {
                sequence: exhausted,
                lock_token: exhausted_delivery.lock.expect("peek-lock").token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );

    let fixture = fixture.restart()?;
    for (sequence, reason) in [
        (exhausted, DeadLetterReason::MaxDeliveryCountExceeded),
        (expiring, DeadLetterReason::TimeToLiveExpired),
    ] {
        assert!(row(&fixture.machine, &namespace, &queue, sequence).is_none());
        let dead = v2(&fixture.machine, &namespace, &dlq, sequence);
        assert_eq!(dead.dead_letter.expect("dead-lettered").reason, reason);
    }
    assert_eq!(validated(&fixture.machine), 4);
    Ok(())
}
paired!(
    v2_content_records_survive_replacement_and_dead_letter_paths_on_memory,
    v2_content_records_survive_replacement_and_dead_letter_paths_on_fjall,
    survival
);

#[test]
fn an_application_property_array_of_compound_elements_refuses_at_derive() {
    let content = Content {
        application_properties: Some(vec![(
            Text {
                text: "nested".into(),
                width: Width::One,
            },
            Value::Array(message_content::Array {
                constructor: message_content::Constructor::Primitive(
                    message_content::PrimitiveConstructor::List32,
                ),
                elements: vec![Value::List(vec![Value::Int(1)])],
            }),
        )]),
        ..Content::default()
    };
    let bytes = encode_content(&content, CONTENT_LIMITS).expect("content encodes");
    assert_eq!(
        MessageContent::Amqp(bytes).derive(),
        Err(MessageContentError::UnsupportedArrayElement)
    );
}

/// An application-properties section with one `uint0` element array that
/// re-encodes four bytes per element, so a tiny input yields a wide projection.
fn zero_width_array_message(count: u32) -> Vec<u8> {
    let mut array = vec![0xf0, 0, 0, 0, 5];
    array.extend(count.to_be_bytes());
    array.push(0x43);
    let mut map = vec![0xa1, 1, b'k'];
    map.extend(&array);
    let mut section = vec![0x00, 0x53, 0x74, 0xc1, (map.len() + 1) as u8, 2];
    section.extend(map);
    section
}

fn batch_budget<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert_eq!(
        fixture.at(
            1,
            send(amqp(Message {
                id: "before",
                ..Message::default()
            })),
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    // Each child is tiny and individually under the per-value ceiling, but they
    // share one command-wide budget, so a batch of them cannot each claim a
    // fresh allowance. A per-child budget would have accepted this.
    let child = MessageContent::Amqp(zero_width_array_message(60_000));
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            2,
            CommandKind::SendContentBatch {
                messages: vec![child; 50],
            },
        ),
        Err(BrokerError::MessageContent(
            MessageContentError::ProjectionTooLarge
        ))
    );
    assert_eq!(
        fixture.machine.store().snapshot()?,
        before,
        "the refused batch has no effects"
    );
    assert_eq!(
        fixture.at(
            3,
            send(amqp(Message {
                id: "after",
                ..Message::default()
            })),
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        },
        "a send after the refused batch is unaffected"
    );

    // A realistic batch of ordinary small properties derives and stores.
    let ids: Vec<String> = (0..20).map(|index| format!("m-{index}")).collect();
    let messages: Vec<MessageContent> = ids
        .iter()
        .map(|id| {
            MessageContent::Amqp(amqp(Message {
                id,
                region: Some("West"),
                ..Message::default()
            }))
        })
        .collect();
    let CommandOutcome::BatchSent { sequences, stored } =
        fixture.at(4, CommandKind::SendContentBatch { messages })?
    else {
        panic!("expected a batch send");
    };
    assert_eq!(stored, 20);
    let fixture = fixture.restart()?;
    for (index, sequence) in sequences.into_iter().enumerate() {
        let record = v2(
            &fixture.machine,
            &fixture.namespace,
            &fixture.entity,
            sequence,
        );
        assert_eq!(record.message_id, ids[index]);
        assert_eq!(
            record
                .envelope
                .expect("AMQP content keeps its envelope")
                .filter_properties()
                .application_properties
                .get("region")
                .map(|value| value.as_bytes().to_vec()),
            Some(b"\xa1\x04West".to_vec()),
        );
    }
    assert_eq!(validated(&fixture.machine), 22);
    Ok(())
}
paired!(
    one_projection_budget_spans_a_whole_batch_on_memory,
    one_projection_budget_spans_a_whole_batch_on_fjall,
    batch_budget
);

#[test]
fn an_application_property_empty_list_array_refuses_at_derive() {
    // application-properties { "k": [list0, list0] }: ingress accepts and
    // re-encodes this, but V4 cannot render it as spec AMQP, so it refuses.
    let bytes = [
        0x00, 0x53, 0x74, // application-properties
        0xc1, 0x08, 0x02, // map8, size 8, count 2
        0xa1, 0x01, b'k', // key "k"
        0xe0, 0x02, 0x02, 0x45, // [list0, list0]
    ];
    assert_eq!(
        MessageContent::Amqp(bytes.to_vec()).derive(),
        Err(MessageContentError::UnsupportedArrayElement)
    );
}
