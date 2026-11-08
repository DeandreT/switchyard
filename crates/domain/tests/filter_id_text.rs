//! Azure SDK text IDs are a filter projection, not a new duplicate identity.
//! Both backends exercise persistence, old projections and scheduled fanout.

use std::error::Error;

use domain::{
    CONTENT_LIMITS, Command, CommandKind, CommandOutcome, CorrelationFilter, DEFAULT_RULE_NAME,
    EntityPath, MessageContent, MessageRecord, MessageRecordFormat, NamespaceName, OverlayProperty,
    QueueConfig, ReceiveMode, RuleFilter, RuleName, SequenceNumber, SqlRuleFilter, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    legacy_content::validate_legacy_content, snapshot_validation::validate_complete_state,
};
use message_content::{
    AnnotationKey, Binary, Content, Field, MessageId, Properties, Symbol, Text, Value, Width,
    encode_content,
};
use storage::StateStore;
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

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

struct Case {
    message: MessageId,
    correlation: MessageId,
    message_text: &'static str,
    correlation_text: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            // A string resembling the legacy UUID debug representation is
            // still just a string: no heuristic reinterpretation is permitted.
            message: MessageId::String(text(
                "Uuid([0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255])",
            )),
            correlation: MessageId::String(text("string-correlation")),
            message_text: "Uuid([0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255])",
            correlation_text: "string-correlation",
        },
        Case {
            message: MessageId::ULong(u64::MAX),
            correlation: MessageId::ULong(0),
            message_text: "18446744073709551615",
            correlation_text: "0",
        },
        Case {
            message: MessageId::Uuid([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]),
            correlation: MessageId::Uuid([
                0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22,
                0x11, 0x00,
            ]),
            message_text: "00112233-4455-6677-8899-aabbccddeeff",
            correlation_text: "ffeeddcc-bbaa-9988-7766-554433221100",
        },
        Case {
            message: MessageId::Binary(Binary {
                bytes: vec![0, 0xab, 0xcd, 0xef, 0xff, 0x10],
                width: Width::One,
            }),
            correlation: MessageId::Binary(Binary {
                bytes: vec![],
                width: Width::One,
            }),
            message_text: "00ABCDEFFF10",
            correlation_text: "",
        },
        Case {
            message: MessageId::String(text("00112233-4455-6677-8899-aabbccddeeff")),
            correlation: MessageId::String(text("ffeeddcc-bbaa-9988-7766-554433221100")),
            message_text: "00112233-4455-6677-8899-aabbccddeeff",
            correlation_text: "ffeeddcc-bbaa-9988-7766-554433221100",
        },
    ]
}

fn encoded(case: &Case, scheduled: bool) -> Vec<u8> {
    encode_content(
        &Content {
            properties: Some(Properties {
                message_id: Field::Value(case.message.clone()),
                user_id: Field::Null,
                to: Field::Null,
                subject: Field::Null,
                reply_to: Field::Null,
                correlation_id: Field::Value(case.correlation.clone()),
                ..Properties::default()
            }),
            message_annotations: scheduled.then(|| {
                vec![(
                    AnnotationKey::Symbol(Symbol {
                        text: "x-opt-scheduled-enqueue-time".into(),
                        width: Width::One,
                    }),
                    Value::Timestamp(100),
                )]
            }),
            ..Content::default()
        },
        CONTENT_LIMITS,
    )
    .unwrap()
}

fn apply<S: StateStore>(
    machine: &StateMachine<S>,
    entity: &EntityPath,
    at: u64,
    kind: CommandKind,
) -> CommandOutcome {
    let at = at.max(machine.last_applied_time().unwrap().as_millis());
    machine
        .apply(&Command::new(
            NamespaceName::new("n").unwrap(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
        .unwrap()
}

fn send(content: MessageContent) -> CommandKind {
    CommandKind::SendContent { content }
}

fn row<S: StateStore>(
    machine: &StateMachine<S>,
    entity: &EntityPath,
    sequence: u64,
) -> (Vec<u8>, MessageRecord) {
    let bytes = machine
        .store()
        .get(&keys::message(
            &NamespaceName::new("n").unwrap(),
            entity,
            SequenceNumber::new(sequence),
        ))
        .unwrap()
        .unwrap();
    let record = MessageRecord::decode(&bytes).unwrap();
    assert_eq!(codec::message::encode(&record).unwrap(), bytes);
    (bytes, record)
}

fn matching<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let mut retained = Vec::new();
    for (index, case) in cases().iter().enumerate() {
        let topic = EntityPath::new(format!("t{index}"))?;
        apply(
            &machine,
            &topic,
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        );
        let filters = [
            (
                "sm",
                RuleFilter::Sql(SqlRuleFilter::new(format!(
                    "sys.MessageId = '{}'",
                    case.message_text
                ))),
            ),
            (
                "sc",
                RuleFilter::Sql(SqlRuleFilter::new(format!(
                    "sys.CorrelationId = '{}'",
                    case.correlation_text
                ))),
            ),
            (
                "cm",
                RuleFilter::Correlation(CorrelationFilter {
                    message_id: Some(case.message_text.into()),
                    ..CorrelationFilter::default()
                }),
            ),
            (
                "cc",
                RuleFilter::Correlation(CorrelationFilter {
                    correlation_id: Some(case.correlation_text.into()),
                    ..CorrelationFilter::default()
                }),
            ),
            (
                "no-sql",
                RuleFilter::Sql(SqlRuleFilter::new(format!(
                    "sys.MessageId = '{}'",
                    case.correlation_text
                ))),
            ),
            (
                "no-corr",
                RuleFilter::Correlation(CorrelationFilter {
                    correlation_id: Some(case.message_text.into()),
                    ..CorrelationFilter::default()
                }),
            ),
        ];
        let mut expected = Vec::new();
        for (name, filter) in filters {
            let name = SubscriptionName::new(name)?;
            apply(
                &machine,
                &topic,
                0,
                CommandKind::CreateSubscription {
                    name: name.clone(),
                    config: SubscriptionConfig::default(),
                },
            );
            let child = topic.subscription(&name)?;
            apply(
                &machine,
                &child,
                0,
                CommandKind::DeleteRule {
                    name: RuleName::new(DEFAULT_RULE_NAME)?,
                },
            );
            apply(
                &machine,
                &child,
                0,
                CommandKind::CreateRule {
                    name: RuleName::new("id")?,
                    filter,
                },
            );
            if !name.as_str().starts_with("no-") {
                expected.push(child);
            }
        }
        expected.sort();
        for scheduled in [false, true] {
            let bytes = encoded(case, scheduled);
            let new = MessageContent::AmqpTextIds(bytes.clone());
            let legacy = MessageContent::Amqp(bytes.clone()).derive()?;
            let derived = new.derive()?;
            assert_eq!(derived.message_id, legacy.message_id, "duplicate identity");
            let projection = derived.envelope.as_ref().unwrap().filter_properties();
            assert_eq!(projection.message_id.as_deref(), Some(case.message_text));
            assert_eq!(
                projection.correlation_id.as_deref(),
                Some(case.correlation_text)
            );
            assert_eq!(derived.envelope.as_ref().unwrap().as_bytes(), bytes);
            let outcome = apply(&machine, &topic, 1, send(new));
            let CommandOutcome::Published {
                sequences,
                mut subscriptions,
            } = outcome
            else {
                panic!("{outcome:?}")
            };
            let sequence = sequences[0].as_u64();
            if scheduled {
                assert!(subscriptions.is_empty());
                let (_, placeholder) = row(&machine, &topic, sequence);
                assert_eq!(placeholder.format, MessageRecordFormat::V3);
                retained.push((topic.clone(), sequence, bytes));
            } else {
                subscriptions.sort();
                assert_eq!(subscriptions, expected);
                for child in &expected {
                    retained.push((child.clone(), sequence, bytes.clone()));
                }
            }
        }
    }
    let before = machine.store().snapshot()?.entries().to_vec();
    validate_legacy_content(&before)?;
    validate_complete_state(&before)?;
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(machine.store().snapshot()?.entries(), before);
    for (entity, sequence, original) in retained {
        let (_, record) = row(&machine, &entity, sequence);
        assert_eq!(record.format, MessageRecordFormat::V3);
        assert_eq!(record.envelope.as_ref().unwrap().as_bytes(), original);
    }
    // Scheduled routing after reopen must use projection text, not dedup IDs.
    for index in 0..cases().len() {
        let topic = EntityPath::new(format!("t{index}"))?;
        let outcome = apply(&machine, &topic, 100, CommandKind::ActivateScheduled);
        let CommandOutcome::ScheduledActivated {
            activated,
            mut deliverable_entities,
        } = outcome
        else {
            panic!("{outcome:?}")
        };
        assert_eq!(activated, 1);
        deliverable_entities.sort();
        let mut expected = ["sm", "sc", "cm", "cc"].map(|name| {
            topic
                .subscription(&SubscriptionName::new(name).unwrap())
                .unwrap()
        });
        expected.sort();
        assert_eq!(deliverable_entities, expected);
        for child in expected {
            assert_eq!(row(&machine, &child, 3).1.format, MessageRecordFormat::V3);
        }
    }
    validate_complete_state(machine.store().snapshot()?.entries())?;
    Ok(())
}

paired!(
    azure_ids_match_sql_and_correlation_memory,
    azure_ids_match_sql_and_correlation_fjall,
    matching
);

fn duplicate_identity<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let mut unchanged = Vec::new();
    for (index, case) in cases().iter().enumerate() {
        let bytes = encoded(case, false);
        let legacy = MessageContent::Amqp(bytes.clone());
        let current = MessageContent::AmqpTextIds(bytes);
        let identity = legacy.derive()?.message_id;
        for reverse in [false, true] {
            let queue = EntityPath::new(format!("q{index}-{reverse}"))?;
            apply(
                &machine,
                &queue,
                0,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_duplicate_detection: true,
                        ..QueueConfig::default()
                    },
                },
            );
            let (first, second) = if reverse {
                (current.clone(), legacy.clone())
            } else {
                (legacy.clone(), current.clone())
            };
            assert!(matches!(
                apply(&machine, &queue, 1, send(first)),
                CommandOutcome::Sent { .. }
            ));
            let (stored, record) = row(&machine, &queue, 1);
            assert_eq!(record.message_id, identity);
            assert!(matches!(
                apply(&machine, &queue, 2, send(second)),
                CommandOutcome::DuplicateSuppressed { .. }
            ));
            let alias = Case {
                message: MessageId::String(text(&identity)),
                correlation: case.correlation.clone(),
                message_text: "",
                correlation_text: "",
            };
            assert!(matches!(
                apply(
                    &machine,
                    &queue,
                    3,
                    send(MessageContent::AmqpTextIds(encoded(&alias, false)))
                ),
                CommandOutcome::DuplicateSuppressed { .. }
            ));
            // The Azure filter spelling must NOT become an identity alias.
            if identity != case.message_text {
                let alias = Case {
                    message: MessageId::String(text(case.message_text)),
                    correlation: case.correlation.clone(),
                    message_text: "",
                    correlation_text: "",
                };
                assert!(matches!(
                    apply(
                        &machine,
                        &queue,
                        4,
                        send(MessageContent::AmqpTextIds(encoded(&alias, false)))
                    ),
                    CommandOutcome::Sent { .. }
                ));
            }
            unchanged.push((queue, stored, current.clone()));
        }
        let topic = EntityPath::new(format!("t{index}"))?;
        apply(
            &machine,
            &topic,
            0,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        );
        let child_name = SubscriptionName::new("all")?;
        apply(
            &machine,
            &topic,
            0,
            CommandKind::CreateSubscription {
                name: child_name.clone(),
                config: SubscriptionConfig::default(),
            },
        );
        let child = topic.subscription(&child_name)?;
        apply(&machine, &topic, 1, send(legacy));
        let original = row(&machine, &child, 1).0;
        let outcome = apply(&machine, &topic, 2, send(current));
        assert!(
            matches!(outcome, CommandOutcome::Published { subscriptions, .. } if subscriptions.is_empty())
        );
        assert_eq!(row(&machine, &child, 1).0, original);
    }
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    for (queue, bytes, current) in unchanged {
        assert_eq!(row(&machine, &queue, 1).0, bytes);
        assert!(matches!(
            apply(&machine, &queue, 5, send(current)),
            CommandOutcome::DuplicateSuppressed { .. }
        ));
        assert_eq!(row(&machine, &queue, 1).0, bytes);
    }
    validate_complete_state(machine.store().snapshot()?.entries())?;
    Ok(())
}

paired!(
    id_text_keeps_duplicate_identity_memory,
    id_text_keeps_duplicate_identity_fjall,
    duplicate_identity
);

fn patch_semantics<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let queue = EntityPath::new("q")?;
    apply(
        &machine,
        &queue,
        0,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    let case = cases().remove(2);
    let bytes = encoded(&case, false);
    apply(
        &machine,
        &queue,
        1,
        send(MessageContent::Amqp(bytes.clone())),
    );
    apply(
        &machine,
        &queue,
        2,
        send(MessageContent::AmqpTextIds(bytes.clone())),
    );
    for sequence in [1, 2] {
        let (_, before) = row(&machine, &queue, sequence);
        let CommandOutcome::Received(Some(delivery)) = apply(
            &machine,
            &queue,
            3,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ) else {
            panic!("locked copy")
        };
        let token = delivery.lock.as_ref().unwrap().token;
        apply(
            &machine,
            &queue,
            4,
            CommandKind::DeferWithPatch {
                sequence: SequenceNumber::new(sequence),
                lock_token: token,
                patch: vec![OverlayProperty {
                    name: "x".into(),
                    value: vec![0x41],
                }],
            },
        );
        let (_, after) = row(&machine, &queue, sequence);
        assert_eq!(after.format, before.format);
        assert_eq!(after.message_id, before.message_id);
        let old_projection = before.envelope.as_ref().unwrap().filter_properties();
        let envelope = after.envelope.unwrap();
        assert_eq!(envelope.as_bytes(), bytes);
        assert_eq!(
            envelope.filter_properties().message_id,
            old_projection.message_id
        );
        assert_eq!(
            envelope.filter_properties().correlation_id,
            old_projection.correlation_id
        );
    }
    let before = machine.store().snapshot()?.entries().to_vec();
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(machine.store().snapshot()?.entries(), before);
    validate_legacy_content(&before)?;
    validate_complete_state(&before)?;
    Ok(())
}

paired!(
    patch_keeps_id_projection_semantics_memory,
    patch_keeps_id_projection_semantics_fjall,
    patch_semantics
);

fn literal_uuid_strings<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let queue = EntityPath::new("literal-ids")?;
    apply(
        &machine,
        &queue,
        0,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    let cases = cases();
    let uuid = encoded(&cases[2], false);
    let debug_string = encoded(&cases[0], false);
    let standard_string = encoded(&cases[4], false);
    let identity = MessageContent::Amqp(uuid.clone()).derive()?.message_id;
    assert_eq!(
        identity, cases[0].message_text,
        "literal string exactly equals UUID debug text"
    );
    for content in [
        MessageContent::Amqp(uuid.clone()),
        MessageContent::AmqpTextIds(uuid),
        MessageContent::Amqp(debug_string.clone()),
        MessageContent::AmqpTextIds(debug_string),
        MessageContent::AmqpTextIds(standard_string),
    ] {
        apply(&machine, &queue, 1, send(content));
    }
    let before = machine.store().snapshot()?.entries().to_vec();
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(machine.store().snapshot()?.entries(), before);
    for (sequence, filter_text, duplicate_id) in [
        (1, identity.as_str(), identity.as_str()),
        (2, cases[2].message_text, identity.as_str()),
        (3, identity.as_str(), identity.as_str()),
        (4, identity.as_str(), identity.as_str()),
        (5, cases[4].message_text, cases[4].message_text),
    ] {
        let (_, record) = row(&machine, &queue, sequence);
        assert_eq!(record.message_id, duplicate_id);
        assert_eq!(
            record
                .envelope
                .unwrap()
                .filter_properties()
                .message_id
                .as_deref(),
            Some(filter_text)
        );
    }
    validate_complete_state(&before)?;
    Ok(())
}

paired!(
    literal_debug_and_standard_uuid_strings_memory,
    literal_debug_and_standard_uuid_strings_fjall,
    literal_uuid_strings
);

fn v3_replacement<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    let queue = EntityPath::new("replacement")?;
    apply(
        &machine,
        &queue,
        0,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                ..QueueConfig::default()
            },
        },
    );
    let cases = cases();
    let original = encoded(&cases[2], false);
    let identity = MessageContent::Amqp(original.clone()).derive()?.message_id;
    apply(
        &machine,
        &queue,
        1,
        send(MessageContent::AmqpTextIds(original.clone())),
    );
    assert_eq!(
        row(&machine, &queue, 1).1.envelope.unwrap().as_bytes(),
        original
    );
    let CommandOutcome::Received(Some(delivery)) = apply(
        &machine,
        &queue,
        2,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ) else {
        panic!("locked original")
    };

    // Replace UUID IDs with binary IDs and retain complete canonical content,
    // including its body/footer. Caller-supplied legacy projection is not V3.
    let mut replacement =
        message_content::decode_content(&encoded(&cases[3], false), CONTENT_LIMITS)?;
    replacement.body = message_content::Body::Data(vec![Binary {
        bytes: vec![0, 0xff, 0x41],
        width: Width::Four,
    }]);
    replacement.footer = Some(vec![(
        AnnotationKey::Symbol(Symbol {
            text: "footer".into(),
            width: Width::One,
        }),
        Value::UInt(7),
    )]);
    let replacement = encode_content(&replacement, CONTENT_LIMITS)?;
    let projection = MessageContent::AmqpTextIds(replacement.clone())
        .derive()?
        .envelope
        .unwrap()
        .filter_properties()
        .clone();
    let envelope = domain::MessageEnvelope::new(replacement.clone()).with_filter_properties(
        domain::FilterProperties {
            message_id: Some("forged legacy ID".into()),
            correlation_id: Some("Uuid([0, 1])".into()),
            ..domain::FilterProperties::default()
        },
    );
    apply(
        &machine,
        &queue,
        3,
        CommandKind::Defer {
            sequence: SequenceNumber::new(1),
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: Some(envelope.clone()),
        },
    );
    let (retained, record) = row(&machine, &queue, 1);
    assert_eq!(record.format, MessageRecordFormat::V3);
    assert_eq!(record.message_id, identity);
    assert!(record.body.is_empty());
    let stored = record.envelope.unwrap();
    assert_eq!(stored.as_bytes(), replacement);
    assert_eq!(stored.filter_properties(), &projection);
    assert_eq!(
        projection.message_id.as_deref(),
        Some(cases[3].message_text)
    );
    assert_eq!(
        projection.correlation_id.as_deref(),
        Some(cases[3].correlation_text)
    );
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(row(&machine, &queue, 1).0, retained);
    assert!(matches!(
        apply(
            &machine,
            &queue,
            4,
            send(MessageContent::AmqpTextIds(original))
        ),
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    let literal_alias = Case {
        message: MessageId::String(text(&identity)),
        correlation: cases[2].correlation.clone(),
        message_text: "",
        correlation_text: "",
    };
    assert!(matches!(
        apply(
            &machine,
            &queue,
            5,
            send(MessageContent::AmqpTextIds(encoded(&literal_alias, false)))
        ),
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    // Replacement never overwrites the send's retained duplicate identity or
    // creates history for the replacement's new ID.
    assert!(matches!(
        apply(
            &machine,
            &queue,
            6,
            send(MessageContent::AmqpTextIds(replacement.clone()))
        ),
        CommandOutcome::Sent { .. }
    ));

    let receive = || CommandKind::ReceiveDeferred {
        sequences: vec![SequenceNumber::new(1)],
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: None,
    };
    let CommandOutcome::DeferredReceived(mut deliveries) = apply(&machine, &queue, 7, receive())
    else {
        panic!("deferred delivery")
    };
    let delivery = deliveries.remove(0);
    apply(
        &machine,
        &queue,
        8,
        CommandKind::DeferWithPatch {
            sequence: SequenceNumber::new(1),
            lock_token: delivery.lock.unwrap().token,
            patch: vec![OverlayProperty {
                name: "patched".into(),
                value: vec![0x41],
            }],
        },
    );
    let CommandOutcome::DeferredReceived(mut deliveries) = apply(&machine, &queue, 9, receive())
    else {
        panic!("patched delivery")
    };
    let token = deliveries.remove(0).lock.unwrap().token;
    let before = machine.store().snapshot()?.entries().to_vec();
    for kind in [
        CommandKind::Defer {
            sequence: SequenceNumber::new(1),
            lock_token: token,
            replacement_envelope: Some(envelope.clone()),
        },
        CommandKind::Abandon {
            sequence: SequenceNumber::new(1),
            lock_token: token,
            replacement_envelope: Some(envelope.clone()),
        },
        CommandKind::DeadLetter {
            sequence: SequenceNumber::new(1),
            lock_token: token,
            reason: "test".into(),
            description: String::new(),
            replacement_envelope: Some(envelope.clone()),
        },
    ] {
        assert_eq!(
            machine.apply(&Command::new(
                NamespaceName::new("n")?,
                queue.clone(),
                Timestamp::from_millis(10),
                kind
            )),
            Err(domain::BrokerError::MessageContent(
                domain::MessageContentError::ReplacementWouldDropOverlay
            ))
        );
        assert_eq!(machine.store().snapshot()?.entries(), before);
    }
    let (_, patched) = row(&machine, &queue, 1);
    assert_eq!(patched.format, MessageRecordFormat::V3);
    assert_eq!(patched.message_id, identity);
    assert_eq!(patched.envelope.as_ref().unwrap().as_bytes(), replacement);
    assert_eq!(
        patched
            .envelope
            .as_ref()
            .unwrap()
            .filter_properties()
            .message_id,
        projection.message_id
    );
    validate_legacy_content(&before)?;
    validate_complete_state(&before)?;
    Ok(())
}

paired!(
    v3_whole_envelope_replacement_memory,
    v3_whole_envelope_replacement_fjall,
    v3_replacement
);
